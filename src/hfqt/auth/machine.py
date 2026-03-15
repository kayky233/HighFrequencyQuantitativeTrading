from __future__ import annotations

import hashlib
import os
import platform
import secrets
import uuid
from pathlib import Path

from hfqt.config import AppConfig


def _read_windows_machine_guid() -> str | None:
    try:
        import winreg

        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Cryptography") as key:
            value, _ = winreg.QueryValueEx(key, "MachineGuid")
            return str(value).strip() or None
    except Exception:  # noqa: BLE001
        return None


class MachineFingerprintCollector:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.device_secret_path = Path(config.auth_device_secret_path)

    def _ensure_device_secret(self) -> str:
        self.device_secret_path.parent.mkdir(parents=True, exist_ok=True)
        if self.device_secret_path.exists():
            value = self.device_secret_path.read_text(encoding="utf-8").strip()
            if value:
                return value
        secret = secrets.token_hex(16)
        self.device_secret_path.write_text(secret, encoding="utf-8")
        return secret

    def _parts(self) -> list[str]:
        hostname = platform.node().strip().lower()
        machine_guid = (_read_windows_machine_guid() or "").strip().lower()
        mac = f"{uuid.getnode():012x}"
        machine = platform.machine().strip().lower()
        processor = platform.processor().strip().lower()
        system = platform.system().strip().lower()
        release = platform.release().strip().lower()
        return [
            hostname,
            machine_guid,
            mac,
            machine,
            processor,
            system,
            release,
        ]

    def machine_hash(self) -> str:
        secret = self._ensure_device_secret()
        normalized = "|".join(part for part in self._parts() if part)
        payload = f"{normalized}|{secret}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def machine_hash_short(self) -> str:
        return self.machine_hash()[:12]
