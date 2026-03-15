from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from hfqt.schemas import AccountInfo, OrderRecord, OrderRequest


class BrokerAdapter(ABC):
    name: str = "broker"

    @abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def healthcheck(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def get_accounts(self) -> list[AccountInfo]:
        raise NotImplementedError

    @abstractmethod
    async def submit_order(self, request: OrderRequest) -> OrderRecord:
        raise NotImplementedError

    @abstractmethod
    async def get_order(self, broker_order_id: str) -> OrderRecord | None:
        raise NotImplementedError

    @abstractmethod
    async def list_orders(self) -> list[OrderRecord]:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError
