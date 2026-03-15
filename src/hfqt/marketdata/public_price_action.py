from __future__ import annotations

from hfqt.config import AppConfig
from hfqt.marketdata.yahoo_chart import YahooChartAdapter
from hfqt.marketdata.yfinance_chart import YFinanceChartAdapter
from hfqt.schemas import PriceActionFeatures


class PublicPriceActionAdapter:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.adapters = (
            YahooChartAdapter(config),
            YFinanceChartAdapter(config),
        )

    async def get_intraday_features(self, symbol: str) -> PriceActionFeatures:
        errors: list[str] = []
        for adapter in self.adapters:
            try:
                return await adapter.get_intraday_features(symbol)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{adapter.__class__.__name__}: {exc}")
        raise RuntimeError("; ".join(errors))

    async def get_intraday_bars(self, symbol: str, bar_limit: int = 90) -> list[dict[str, float]]:
        errors: list[str] = []
        for adapter in self.adapters:
            get_bars = getattr(adapter, "get_intraday_bars", None)
            if not callable(get_bars):
                continue
            try:
                return await get_bars(symbol, bar_limit=bar_limit)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{adapter.__class__.__name__}: {exc}")
        raise RuntimeError("; ".join(errors) if errors else f"No market data adapter available for {symbol}.")
