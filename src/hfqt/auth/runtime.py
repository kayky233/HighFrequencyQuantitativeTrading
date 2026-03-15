from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.serialization import load_pem_public_key

from hfqt.auth.machine import MachineFingerprintCollector
from hfqt.auth.schemas import AuthorizationStatus, LicensePayload, SessionTokenPayload, SignedEnvelope
from hfqt.config import AppConfig
from hfqt.runtime_logging import get_logger


logger = get_logger("auth_runtime")


class AuthorizationError(RuntimeError):
    pass


def _pad_b64(value: str) -> str:
    return value + "=" * (-len(value) % 4)


def _decode_signature(value: str) -> bytes:
    return base64.urlsafe_b64decode(_pad_b64(value))


def _parse_version(value: str | None) -> tuple[int, ...]:
    if not value:
        return ()
    parts: list[int] = []
    for raw in value.replace("-", ".").split("."):
        raw = raw.strip()
        if not raw:
            continue
        digits = "".join(ch for ch in raw if ch.isdigit())
        if digits:
            parts.append(int(digits))
        else:
            parts.append(0)
    return tuple(parts)


class AuthorizationRuntime:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.collector = MachineFingerprintCollector(config)

    def _load_json(self, path: Path) -> dict[str, Any]:
        return json.loads(path.read_text(encoding="utf-8"))

    def _load_envelope(self, path: Path) -> SignedEnvelope:
        return SignedEnvelope.model_validate(self._load_json(path))

    def _verify_signed_payload(self, path: Path) -> dict[str, Any]:
        envelope = self._load_envelope(path)
        if envelope.algorithm.lower() != "ed25519":
            raise AuthorizationError(f"Unsupported auth algorithm: {envelope.algorithm}")
        key_path = Path(self.config.auth_public_key_path)
        if not key_path.exists():
            raise AuthorizationError("Public key file not found.")
        public_key = load_pem_public_key(key_path.read_bytes())
        signature = _decode_signature(envelope.signature)
        message = json.dumps(envelope.payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        public_key.verify(signature, message)
        return envelope.payload

    def _check_version_window(self, min_version: str | None, max_version: str | None) -> tuple[bool, str | None]:
        current = _parse_version(self.config.auth_client_version)
        minimum = _parse_version(min_version)
        maximum = _parse_version(max_version)
        if minimum and current < minimum:
            return False, f"Client version {self.config.auth_client_version} is below minimum {min_version}."
        if maximum and current > maximum:
            return False, f"Client version {self.config.auth_client_version} is above maximum {max_version}."
        return True, None

    def _grace_deadline(
        self,
        license_payload: LicensePayload,
        token_payload: SessionTokenPayload | None,
    ) -> datetime | None:
        grace_hours = max(1, license_payload.offline_grace_hours or self.config.auth_offline_grace_hours)
        reference = token_payload.expire_at if token_payload else None
        if reference is None:
            return None
        return reference + timedelta(hours=grace_hours)

    def status(self) -> AuthorizationStatus:
        machine_hash = self.collector.machine_hash()
        base = AuthorizationStatus(
            enabled=self.config.auth_enabled,
            mode=self.config.auth_mode,
            product=self.config.auth_product_code,
            client_version=self.config.auth_client_version,
            machine_hash_short=machine_hash[:12],
            license_path=str(self.config.auth_license_path),
            token_cache_path=str(self.config.auth_token_cache_path),
            public_key_path=str(self.config.auth_public_key_path),
            can_run=not self.config.auth_enabled,
            reason=None if not self.config.auth_enabled else "Authorization not evaluated yet.",
        )
        if not self.config.auth_enabled or self.config.auth_mode == "off":
            base.reason = "Authorization disabled."
            return base

        license_path = Path(self.config.auth_license_path)
        token_path = Path(self.config.auth_token_cache_path)
        key_path = Path(self.config.auth_public_key_path)
        base.public_key_present = key_path.exists()
        base.license_present = license_path.exists()
        base.token_present = token_path.exists()

        if not base.public_key_present:
            base.can_run = False
            base.reason = "Authorization public key missing."
            return base
        if not base.license_present:
            base.can_run = False
            base.reason = "License file missing."
            return base

        try:
            license_payload = LicensePayload.model_validate(self._verify_signed_payload(license_path))
            base.license_valid = True
            base.license_id = license_payload.license_id
            base.edition = license_payload.edition
            base.features = list(license_payload.features)
            base.limits = dict(license_payload.limits)
            base.expires_at = license_payload.expire_at
        except Exception as exc:  # noqa: BLE001
            base.can_run = False
            base.reason = f"License verification failed: {exc}"
            return base

        now = datetime.now(UTC)
        if license_payload.product != self.config.auth_product_code:
            base.can_run = False
            base.reason = "License product mismatch."
            return base
        if license_payload.expire_at <= now:
            base.can_run = False
            base.reason = "License expired."
            return base

        version_ok, version_reason = self._check_version_window(
            license_payload.min_client_version,
            license_payload.max_client_version,
        )
        if not version_ok:
            base.can_run = False
            base.reason = version_reason
            return base

        expected_binding = (license_payload.machine_binding_mode or self.config.auth_machine_binding_mode).lower()
        if expected_binding in {"single", "locked"}:
            base.machine_match = bool(license_payload.machine_hash and license_payload.machine_hash == machine_hash)
            if not base.machine_match:
                base.can_run = False
                base.reason = "Machine binding mismatch."
                return base
        else:
            base.machine_match = True

        token_payload: SessionTokenPayload | None = None
        if base.token_present:
            try:
                token_payload = SessionTokenPayload.model_validate(self._verify_signed_payload(token_path))
                base.token_valid = True
                base.token_expires_at = token_payload.expire_at
                if token_payload.license_id != license_payload.license_id:
                    raise AuthorizationError("Token license mismatch.")
                if token_payload.machine_hash and token_payload.machine_hash != machine_hash:
                    raise AuthorizationError("Token machine mismatch.")
                if token_payload.expire_at <= now:
                    raise AuthorizationError("Token expired.")
                base.features = list(token_payload.session_rights or base.features)
            except Exception as exc:  # noqa: BLE001
                base.token_valid = False
                base.reason = f"Token verification failed: {exc}"

        deadline = self._grace_deadline(license_payload, token_payload)
        base.offline_grace_deadline = deadline
        if token_payload is not None and not base.token_valid and deadline is not None and now <= deadline:
            base.in_offline_grace = True
        elif token_payload is None and self.config.auth_mode in {"local_only", "offline"}:
            base.in_offline_grace = True

        if self.config.auth_mode in {"local_only", "offline"}:
            base.can_run = base.license_valid and base.machine_match
            base.reason = "License-only mode active."
            return base

        if base.token_valid:
            base.can_run = True
            base.reason = "License and session token valid."
            return base

        if base.in_offline_grace:
            base.can_run = True
            base.reason = "Running within offline grace window."
            return base

        base.can_run = False
        if base.reason is None:
            base.reason = "Session token missing or invalid."
        return base

    def require_feature(self, feature: str) -> AuthorizationStatus:
        status = self.status()
        if not status.can_run:
            raise AuthorizationError(status.reason or "Authorization blocked.")
        if feature not in status.features:
            raise AuthorizationError(f"Feature not licensed: {feature}")
        return status
