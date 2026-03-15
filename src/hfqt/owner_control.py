from __future__ import annotations

import hmac
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from hfqt.config import AppConfig
from hfqt.runtime_logging import get_logger


logger = get_logger("owner_control")
OWNER_HEADER_NAME = "X-HFQT-Owner-Token"


class OwnerControlError(RuntimeError):
    def __init__(self, detail: str, status_code: int = 403) -> None:
        super().__init__(detail)
        self.detail = detail
        self.status_code = status_code


class OwnerState(BaseModel):
    trading_locked: bool = False
    note: str | None = None
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class OwnerControl:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.state_path = Path(config.owner_state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def enabled(self) -> bool:
        return bool(self.config.owner_control_enabled or self.config.owner_token)

    @property
    def requires_token_for_write(self) -> bool:
        return bool(self.enabled and self.config.require_owner_token_for_write)

    def get_status(self) -> dict[str, Any]:
        state = self.load_state()
        return {
            "enabled": self.enabled,
            "require_token_for_write": self.requires_token_for_write,
            "trading_locked": state.trading_locked,
            "note": state.note,
            "updated_at": state.updated_at.isoformat(),
            "header_name": OWNER_HEADER_NAME,
            "token_configured": bool(self.config.owner_token),
        }

    def load_state(self) -> OwnerState:
        if not self.state_path.exists():
            return OwnerState(trading_locked=self.config.trading_locked_default)
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            return OwnerState.model_validate(data)
        except Exception:  # noqa: BLE001
            logger.exception(
                "failed to read owner control state",
                extra={"event": "owner_state_read_failed", "state_path": str(self.state_path)},
            )
            return OwnerState(trading_locked=self.config.trading_locked_default)

    def save_state(self, state: OwnerState) -> OwnerState:
        state.updated_at = datetime.now(UTC)
        tmp_path = self.state_path.with_suffix(".tmp")
        tmp_path.write_text(
            json.dumps(state.model_dump(mode="json"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(self.state_path)
        return state

    def verify_token(self, provided_token: str | None) -> bool:
        expected = self.config.owner_token or ""
        candidate = (provided_token or "").strip()
        if not expected:
            return False
        return hmac.compare_digest(expected, candidate)

    def require_owner_token(self, provided_token: str | None) -> None:
        if not self.requires_token_for_write:
            return
        if self.verify_token(provided_token):
            return
        logger.warning(
            "owner token rejected",
            extra={
                "event": "owner_token_rejected",
                "state_path": str(self.state_path),
                "token_present": bool((provided_token or "").strip()),
            },
        )
        raise OwnerControlError("Owner token invalid or missing.", status_code=403)

    def require_trade_access(self, provided_token: str | None) -> None:
        if not self.enabled:
            return
        self.require_owner_token(provided_token)
        state = self.load_state()
        if state.trading_locked:
            logger.warning(
                "trade blocked by owner lock",
                extra={
                    "event": "owner_trade_blocked",
                    "state_path": str(self.state_path),
                    "note": state.note,
                },
            )
            raise OwnerControlError("Trading is locked by owner control.", status_code=423)

    def lock_trading(self, provided_token: str | None, note: str | None = None) -> OwnerState:
        self.require_owner_token(provided_token)
        state = OwnerState(trading_locked=True, note=note or "Locked by owner.")
        saved = self.save_state(state)
        logger.warning(
            "trading locked by owner",
            extra={
                "event": "owner_locked_trading",
                "state_path": str(self.state_path),
                "note": saved.note,
            },
        )
        return saved

    def unlock_trading(self, provided_token: str | None, note: str | None = None) -> OwnerState:
        self.require_owner_token(provided_token)
        state = OwnerState(trading_locked=False, note=note or "Unlocked by owner.")
        saved = self.save_state(state)
        logger.warning(
            "trading unlocked by owner",
            extra={
                "event": "owner_unlocked_trading",
                "state_path": str(self.state_path),
                "note": saved.note,
            },
        )
        return saved
