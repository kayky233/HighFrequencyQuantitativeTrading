from __future__ import annotations

import base64
import json
import random
import time
from typing import Any

import httpx
from Crypto.Cipher import PKCS1_v1_5
from Crypto.Hash import MD5
from Crypto.PublicKey import RSA
from Crypto.Signature.pkcs1_15 import PKCS115_SigScheme

from hfqt.config import AppConfig


def _wrap_pem(value: str, label: str) -> str:
    stripped = value.strip()
    if "BEGIN " in stripped:
        return stripped
    return f"-----BEGIN {label}-----\n{stripped}\n-----END {label}-----"


class USmartCrypto:
    def __init__(self, public_key: str, private_key: str) -> None:
        self.public_key = self._import_public_key(public_key)
        self.private_key = self._import_private_key(private_key)

    @staticmethod
    def _import_public_key(value: str) -> RSA.RsaKey:
        candidates = [
            _wrap_pem(value, "PUBLIC KEY"),
            _wrap_pem(value, "PRIVATE KEY"),
        ]
        for candidate in candidates:
            try:
                imported = RSA.import_key(candidate.encode("utf-8"))
                if imported.has_private():
                    return imported.publickey()
                return imported
            except (ValueError, IndexError, TypeError):
                continue
        raise ValueError("Unable to import uSmart public key.")

    @staticmethod
    def _import_private_key(value: str) -> RSA.RsaKey:
        candidates = [
            _wrap_pem(value, "PRIVATE KEY"),
            _wrap_pem(value, "RSA PRIVATE KEY"),
        ]
        for candidate in candidates:
            try:
                imported = RSA.import_key(candidate.encode("utf-8"))
                if imported.has_private():
                    return imported
            except (ValueError, IndexError, TypeError):
                continue
        raise ValueError("Unable to import uSmart private key.")

    def rsa_encrypt_urlsafe_b64(self, value: str) -> str:
        cipher = PKCS1_v1_5.new(self.public_key)
        encrypted = cipher.encrypt(value.encode("utf-8"))
        return base64.urlsafe_b64encode(encrypted).decode("utf-8")

    def sign_b64(self, content: str) -> str:
        signer = PKCS115_SigScheme(self.private_key)
        digest = MD5.new(content.encode("utf-8"))
        signature = signer.sign(digest)
        return base64.b64encode(signature).decode("utf-8")

    def sign_urlsafe_b64(self, content: str) -> str:
        signer = PKCS115_SigScheme(self.private_key)
        digest = MD5.new(content.encode("utf-8"))
        signature = signer.sign(digest)
        return base64.urlsafe_b64encode(signature).decode("utf-8")

    @staticmethod
    def gen_request_id() -> str:
        return str(int(time.time() * 10**6)) + str(random.randint(0, 999)).zfill(3)

    @staticmethod
    def gen_unix_time_str(length: int = 10) -> str:
        return str(int(time.time() * 10 ** (length - 10)))


class USmartApiClient:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.crypto = USmartCrypto(
            public_key=config.usmart_public_key or "",
            private_key=config.usmart_private_key or "",
        )

    def _json(self, payload: dict[str, Any]) -> str:
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    def _trade_headers(self, params_json: str, token: str | None = None, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "X-Lang": self.config.usmart_x_lang,
            "X-Channel": self.config.usmart_x_channel or "",
            "X-Sign": self.crypto.sign_b64(params_json),
        }
        if token:
            headers["Authorization"] = token
        if self.config.usmart_x_dt:
            headers["X-Dt"] = self.config.usmart_x_dt
        if self.config.usmart_x_type:
            headers["X-Type"] = self.config.usmart_x_type
        if extra:
            headers.update({key: value for key, value in extra.items() if value})
        return headers

    def _quote_headers(self, params_json: str, token: str | None = None) -> dict[str, str]:
        request_id = self.crypto.gen_request_id()
        unix_time = self.crypto.gen_unix_time_str(10)
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "X-Channel": self.config.usmart_x_channel or "",
            "X-Lang": self.config.usmart_x_lang,
            "X-Request-Id": request_id,
            "X-Time": unix_time,
        }
        authorization = token or ""
        if authorization:
            headers["Authorization"] = authorization
        row_content = authorization + headers["X-Channel"] + headers["X-Lang"] + request_id + unix_time + params_json
        headers["X-Sign"] = self.crypto.sign_urlsafe_b64(row_content)
        return headers

    async def _post(self, url: str, headers: dict[str, str], payload: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=15, trust_env=True) as client:
            response = await client.post(url, content=self._json(payload).encode("utf-8"), headers=headers)
            response.raise_for_status()
            try:
                return response.json()
            except ValueError:
                return {"raw_text": response.text, "status_code": response.status_code}

    async def login(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "password": self.crypto.rsa_encrypt_urlsafe_b64(self.config.usmart_login_password or ""),
        }
        login_type = (self.config.usmart_login_type or "phone").strip().lower()
        if login_type == "email":
            payload["email"] = self.crypto.rsa_encrypt_urlsafe_b64(self.config.usmart_email or "")
        else:
            payload["phoneNumber"] = self.crypto.rsa_encrypt_urlsafe_b64(self.config.usmart_phone_number or "")
            payload["areaCode"] = self.config.usmart_area_code or ""
        params_json = self._json(payload)
        headers = self._trade_headers(params_json=params_json)
        return await self._post(f"{self.config.usmart_trade_host.rstrip('/')}{self.config.usmart_login_path}", headers, payload)

    async def trade_login(self, token: str) -> dict[str, Any]:
        payload = {
            "password": self.crypto.rsa_encrypt_urlsafe_b64(self.config.usmart_trade_password or ""),
        }
        params_json = self._json(payload)
        headers = self._trade_headers(
            params_json=params_json,
            token=token,
            extra={
                "X-Type": self.config.usmart_x_type or "",
                "X-Request-Id": self.crypto.gen_unix_time_str(16),
            },
        )
        return await self._post(
            f"{self.config.usmart_trade_host.rstrip('/')}{self.config.usmart_trade_login_path}",
            headers,
            payload,
        )

    async def marketstate(self, market: str, token: str | None = None) -> dict[str, Any]:
        payload = {"market": market}
        params_json = self._json(payload)
        headers = self._quote_headers(params_json=params_json, token=token)
        return await self._post(
            f"{self.config.usmart_quote_host.rstrip('/')}{self.config.usmart_marketstate_path}",
            headers,
            payload,
        )

    @staticmethod
    def extract_token(payload: dict[str, Any]) -> str | None:
        data = payload.get("data") or {}
        token = data.get("token")
        return str(token) if token else None
