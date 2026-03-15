from __future__ import annotations

from abc import ABC, abstractmethod

from hfqt.schemas import MarketSnapshot


class MarketDataAdapter(ABC):
    @abstractmethod
    async def get_snapshot(self, symbol: str) -> MarketSnapshot:
        raise NotImplementedError
