from __future__ import annotations

import importlib
import socket
from typing import Any

from hfqt.brokers.base import BrokerAdapter
from hfqt.config import AppConfig
from hfqt.runtime_logging import get_logger
from hfqt.schemas import AccountInfo, OrderRecord, OrderRequest, OrderStatus, OrderType, TradeAction


logger = get_logger("brokers.futu_sim")


class FutuSimBrokerAdapter(BrokerAdapter):
    name = "futu_sim"

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.sdk = None
        self.ctx = None

    def _load_sdk(self):
        if self.sdk:
            return self.sdk
        for module_name in ("futu", "moomoo"):
            try:
                self.sdk = importlib.import_module(module_name)
                return self.sdk
            except ModuleNotFoundError:
                continue
        raise RuntimeError("Neither 'futu' nor 'moomoo' SDK is installed.")

    def _security_firm(self):
        sdk = self._load_sdk()
        if not hasattr(sdk, "SecurityFirm"):
            return None
        if self.config.futu_security_firm and hasattr(sdk.SecurityFirm, self.config.futu_security_firm):
            return getattr(sdk.SecurityFirm, self.config.futu_security_firm)
        for fallback in ("FUTUSECURITIES", "FUTUINC"):
            if hasattr(sdk.SecurityFirm, fallback):
                return getattr(sdk.SecurityFirm, fallback)
        return None

    async def connect(self) -> None:
        sdk = self._load_sdk()
        kwargs = {
            "filter_trdmarket": getattr(sdk.TrdMarket, self.config.futu_market),
            "host": self.config.futu_host,
            "port": self.config.futu_port,
        }
        security_firm = self._security_firm()
        if security_firm is not None:
            kwargs["security_firm"] = security_firm
        self.ctx = sdk.OpenSecTradeContext(**kwargs)
        logger.info(
            "futu simulated broker connected",
            extra={
                "event": "broker_connected",
                "broker": self.name,
                "host": self.config.futu_host,
                "port": self.config.futu_port,
                "market": self.config.futu_market,
            },
        )

    async def healthcheck(self) -> dict[str, Any]:
        tcp_reachable = False
        error = None
        try:
            with socket.create_connection((self.config.futu_host, self.config.futu_port), timeout=1.5):
                tcp_reachable = True
        except Exception as exc:  # noqa: BLE001
            error = str(exc)

        sdk_loaded = True
        try:
            self._load_sdk()
        except Exception as exc:  # noqa: BLE001
            sdk_loaded = False
            error = error or str(exc)

        return {
            "broker": self.name,
            "sdk_loaded": sdk_loaded,
            "tcp_reachable": tcp_reachable,
            "host": self.config.futu_host,
            "port": self.config.futu_port,
            "error": error,
        }

    async def get_accounts(self) -> list[AccountInfo]:
        sdk = self._load_sdk()
        if not self.ctx:
            await self.connect()
        ret, data = self.ctx.get_acc_list()
        if ret != sdk.RET_OK:
            raise RuntimeError(str(data))
        accounts: list[AccountInfo] = []
        for _, row in data.iterrows():
            raw = row.to_dict()
            accounts.append(
                AccountInfo(
                    broker=self.name,
                    account_id=str(raw.get("acc_id") or raw.get("accID") or raw.get("id")),
                    account_name=str(raw.get("acc_type") or raw.get("card_num") or "Futu Account"),
                    trd_env=str(raw.get("trd_env")),
                    market=str(raw.get("trdmarket") or raw.get("market") or self.config.futu_market),
                    security_firm=str(raw.get("security_firm")) if raw.get("security_firm") is not None else None,
                    raw=raw,
                )
            )
        logger.info(
            "futu accounts loaded",
            extra={
                "event": "broker_accounts_loaded",
                "broker": self.name,
                "count": len(accounts),
            },
        )
        return accounts

    async def submit_order(self, request: OrderRequest) -> OrderRecord:
        sdk = self._load_sdk()
        if not self.ctx:
            await self.connect()
        kwargs = {
            "price": request.price,
            "qty": request.quantity,
            "code": request.symbol,
            "trd_side": getattr(sdk.TrdSide, request.side.value),
            "trd_env": sdk.TrdEnv.SIMULATE,
            "order_type": getattr(sdk.OrderType, request.order_type.value),
        }
        ret, data = self.ctx.place_order(**kwargs)
        if ret != sdk.RET_OK:
            raise RuntimeError(str(data))
        row = data.iloc[0].to_dict()
        record = self._map_order_record(request, row)
        logger.info(
            "futu order submitted",
            extra={
                "event": "broker_order_submitted",
                "broker": self.name,
                "request_id": request.request_id,
                "broker_order_id": record.broker_order_id,
                "symbol": record.symbol,
                "status": record.status.value,
            },
        )
        return record

    async def get_order(self, broker_order_id: str) -> OrderRecord | None:
        sdk = self._load_sdk()
        if not self.ctx:
            await self.connect()
        query_kwargs = {"trd_env": sdk.TrdEnv.SIMULATE, "order_id": broker_order_id}
        if hasattr(self.ctx, "order_list_query"):
            ret, data = self.ctx.order_list_query(**query_kwargs)
        else:
            ret, data = self.ctx.history_order_list_query(**query_kwargs)
        if ret != sdk.RET_OK:
            raise RuntimeError(str(data))
        if data.empty:
            return None
        row = data.iloc[0].to_dict()
        request = OrderRequest(
            request_id=str(row.get("order_id") or broker_order_id),
            intent_id=str(row.get("order_id") or broker_order_id),
            broker=self.name,
            symbol=str(row.get("code")),
            side=TradeAction.BUY,
            quantity=float(row.get("qty", 0)),
            order_type=OrderType.LIMIT,
            price=float(row.get("price")) if row.get("price") is not None else None,
        )
        record = self._map_order_record(request, row)
        logger.info(
            "futu order queried",
            extra={
                "event": "broker_order_queried",
                "broker": self.name,
                "broker_order_id": broker_order_id,
                "symbol": record.symbol,
                "status": record.status.value,
                "filled_qty": record.filled_qty,
            },
        )
        return record

    async def list_orders(self) -> list[OrderRecord]:
        sdk = self._load_sdk()
        if not self.ctx:
            await self.connect()
        query_kwargs = {"trd_env": sdk.TrdEnv.SIMULATE}
        if hasattr(self.ctx, "order_list_query"):
            ret, data = self.ctx.order_list_query(**query_kwargs)
        else:
            ret, data = self.ctx.history_order_list_query(**query_kwargs)
        if ret != sdk.RET_OK:
            raise RuntimeError(str(data))
        records: list[OrderRecord] = []
        for _, row in data.iterrows():
            raw = row.to_dict()
            request = OrderRequest(
                request_id=str(raw.get("order_id") or raw.get("id")),
                intent_id=str(raw.get("order_id") or raw.get("id")),
                broker=self.name,
                symbol=str(raw.get("code")),
                side=TradeAction.BUY,
                quantity=float(raw.get("qty", 0)),
                order_type=OrderType.LIMIT,
                price=float(raw.get("price")) if raw.get("price") is not None else None,
            )
            records.append(self._map_order_record(request, raw))
        return records

    def _map_order_record(self, request: OrderRequest, raw: dict[str, Any]) -> OrderRecord:
        status = self._map_status(raw)
        avg_value = raw.get("dealt_avg_price", raw.get("avg_fill_price"))
        return OrderRecord(
            request_id=request.request_id,
            broker=self.name,
            broker_order_id=str(raw.get("order_id") or raw.get("id") or request.request_id),
            symbol=request.symbol,
            side=request.side,
            quantity=request.quantity,
            order_type=request.order_type,
            price=request.price,
            status=status,
            filled_qty=float(raw.get("dealt_qty", raw.get("filled_qty", 0)) or 0),
            avg_fill_price=float(avg_value) if avg_value not in {None, ""} else None,
            message=str(raw.get("order_status") or raw.get("remark") or ""),
            metadata={"raw": raw},
        )

    @staticmethod
    def _map_status(raw: dict[str, Any]) -> OrderStatus:
        value = str(raw.get("order_status", "")).upper()
        if "FILLED" in value or "ALL" in value:
            return OrderStatus.FILLED
        if "SUBMIT" in value or "WAIT" in value or "PARTIAL" in value:
            return OrderStatus.SUBMITTED
        if "CANCEL" in value:
            return OrderStatus.CANCELLED
        if "FAIL" in value or "REJECT" in value:
            return OrderStatus.REJECTED
        return OrderStatus.UNKNOWN

    async def close(self) -> None:
        if self.ctx:
            self.ctx.close()
            self.ctx = None
        logger.info("futu simulated broker closed", extra={"event": "broker_closed", "broker": self.name})
