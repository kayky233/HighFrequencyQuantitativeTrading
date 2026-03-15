from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from hfqt.brokers.base import BrokerAdapter
from hfqt.runtime_logging import get_logger
from hfqt.schemas import AccountInfo, OrderRecord, OrderRequest, OrderStatus


logger = get_logger("brokers.paper_local")


class LocalPaperBroker(BrokerAdapter):
    name = "local_paper"

    def __init__(self) -> None:
        self.connected = False
        self.orders: dict[str, OrderRecord] = {}
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        self.connected = True
        logger.info("local paper broker connected", extra={"event": "broker_connected", "broker": self.name})

    async def healthcheck(self) -> dict[str, Any]:
        return {"broker": self.name, "connected": self.connected, "mode": "SIMULATE"}

    async def get_accounts(self) -> list[AccountInfo]:
        return [
            AccountInfo(
                broker=self.name,
                account_id="LOCAL-SIM-001",
                account_name="Local Paper Account",
                trd_env="SIMULATE",
                market="MULTI",
            )
        ]

    async def submit_order(self, request: OrderRequest) -> OrderRecord:
        async with self._lock:
            broker_order_id = f"LOCAL-{uuid4().hex[:12].upper()}"
            record = OrderRecord(
                request_id=request.request_id,
                broker=self.name,
                broker_order_id=broker_order_id,
                symbol=request.symbol,
                side=request.side,
                quantity=request.quantity,
                order_type=request.order_type,
                price=request.price,
                status=OrderStatus.SUBMITTED,
                message="Order accepted by local paper broker.",
            )
            self.orders[broker_order_id] = record
            logger.info(
                "local paper order accepted",
                extra={
                    "event": "broker_order_accepted",
                    "broker": self.name,
                    "request_id": request.request_id,
                    "broker_order_id": broker_order_id,
                    "symbol": request.symbol,
                    "side": request.side.value,
                    "quantity": request.quantity,
                    "price": request.price,
                },
            )
            return record

    async def get_order(self, broker_order_id: str) -> OrderRecord | None:
        async with self._lock:
            existing = self.orders.get(broker_order_id)
            if not existing:
                logger.warning(
                    "local paper order not found",
                    extra={
                        "event": "broker_order_missing",
                        "broker": self.name,
                        "broker_order_id": broker_order_id,
                    },
                )
                return None
            if existing.status == OrderStatus.SUBMITTED:
                filled = existing.model_copy(
                    update={
                        "record_id": str(uuid4()),
                        "status": OrderStatus.FILLED,
                        "filled_qty": existing.quantity,
                        "avg_fill_price": existing.price,
                        "message": "Order filled by local paper broker.",
                        "ts": datetime.now(UTC),
                    }
                )
                self.orders[broker_order_id] = filled
                logger.info(
                    "local paper order filled",
                    extra={
                        "event": "broker_order_filled",
                        "broker": self.name,
                        "broker_order_id": broker_order_id,
                        "symbol": filled.symbol,
                        "filled_qty": filled.filled_qty,
                        "avg_fill_price": filled.avg_fill_price,
                    },
                )
                return filled
            return existing

    async def list_orders(self) -> list[OrderRecord]:
        async with self._lock:
            return list(self.orders.values())

    async def close(self) -> None:
        self.connected = False
        logger.info("local paper broker closed", extra={"event": "broker_closed", "broker": self.name})
