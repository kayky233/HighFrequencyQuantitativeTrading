from __future__ import annotations

from typing import Any

from hfqt.brokers.base import BrokerAdapter
from hfqt.brokers.usmart_client import USmartApiClient
from hfqt.config import AppConfig
from hfqt.runtime_logging import get_logger
from hfqt.schemas import AccountInfo, OrderRecord, OrderRequest


logger = get_logger("brokers.usmart")


class USmartBrokerAdapter(BrokerAdapter):
    name = "usmart"

    def __init__(self, config: AppConfig) -> None:
        self.config = config

    async def connect(self) -> None:
        logger.info("uSmart adapter connect called", extra={"event": "broker_connect_called", "broker": self.name})
        return None

    async def healthcheck(self) -> dict[str, Any]:
        missing_credentials = [
            key
            for key, value in {
                "USMART_X_CHANNEL": self.config.usmart_x_channel,
                "USMART_PUBLIC_KEY": self.config.usmart_public_key,
                "USMART_PRIVATE_KEY": self.config.usmart_private_key,
                "USMART_LOGIN_PASSWORD": self.config.usmart_login_password,
            }.items()
            if not value
        ]
        smoke_supported = not missing_credentials
        return {
            "broker": self.name,
            "ready": not missing_credentials,
            "message": "uSmart adapter is scaffold-only until account credentials and permissions arrive.",
            "env": self.config.usmart_env,
            "trade_host": self.config.usmart_trade_host,
            "quote_host": self.config.usmart_quote_host,
            "ws_host": self.config.usmart_ws_host,
            "login_path": self.config.usmart_login_path,
            "trade_login_path": self.config.usmart_trade_login_path,
            "marketstate_path": self.config.usmart_marketstate_path,
            "public_headers": {
                "X-Channel": bool(self.config.usmart_x_channel),
                "X-Lang": self.config.usmart_x_lang,
                "X-Dt": self.config.usmart_x_dt,
                "X-Type": self.config.usmart_x_type,
                "X-Request-Id": "generated at request time",
                "X-Time": "generated at request time",
                "X-Sign": "generated from RSA private key at request time",
            },
            "missing_credentials": missing_credentials,
            "smoke_supported": smoke_supported,
            "supported_smoke_steps": ["login", "marketstate", "trade-login"],
        }

    async def smoke(self, market: str = "us", include_trade_login: bool = False) -> dict[str, Any]:
        health = await self.healthcheck()
        if not health["ready"]:
            return {
                "status": "blocked",
                "reason": "missing_credentials",
                "health": health,
            }

        client = USmartApiClient(self.config)
        login_payload = await client.login()
        token = client.extract_token(login_payload)
        result: dict[str, Any] = {
            "status": "ok",
            "env": self.config.usmart_env,
            "hosts": {
                "trade": self.config.usmart_trade_host,
                "quote": self.config.usmart_quote_host,
                "ws": self.config.usmart_ws_host,
            },
            "login": login_payload,
            "token_received": bool(token),
        }
        if token:
            result["marketstate"] = await client.marketstate(market=market, token=token)
            if include_trade_login and self.config.usmart_trade_password:
                result["trade_login"] = await client.trade_login(token=token)
        return result

    async def get_accounts(self) -> list[AccountInfo]:
        raise NotImplementedError("uSmart adapter is not active yet.")

    async def submit_order(self, request: OrderRequest) -> OrderRecord:
        raise NotImplementedError("uSmart adapter is not active yet.")

    async def get_order(self, broker_order_id: str) -> OrderRecord | None:
        raise NotImplementedError("uSmart adapter is not active yet.")

    async def list_orders(self) -> list[OrderRecord]:
        raise NotImplementedError("uSmart adapter is not active yet.")

    async def close(self) -> None:
        logger.info("uSmart adapter closed", extra={"event": "broker_closed", "broker": self.name})
        return None
