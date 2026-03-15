from __future__ import annotations

import logging
import math
from statistics import pstdev
from time import time

from hfqt.config import AppConfig
from hfqt.schemas import PriceActionFeatures


class YFinanceChartAdapter:
    _shared_cache: dict[str, tuple[float, dict[str, object]]] = {}

    def __init__(self, config: AppConfig) -> None:
        self.config = config

    async def get_intraday_features(self, symbol: str) -> PriceActionFeatures:
        dataset = await self._get_dataset(symbol)
        return dataset["features"]  # type: ignore[return-value]

    async def get_intraday_bars(self, symbol: str, bar_limit: int = 90) -> list[dict[str, float]]:
        dataset = await self._get_dataset(symbol)
        bars = dataset["bars"]  # type: ignore[assignment]
        return [dict(bar) for bar in bars[-max(10, bar_limit):]]

    async def _get_dataset(self, symbol: str) -> dict[str, object]:
        cached = self._shared_cache.get(symbol)
        if cached and time() - cached[0] <= 45:
            return cached[1]

        try:
            import yfinance as yf
        except ModuleNotFoundError as exc:
            raise RuntimeError("yfinance is not installed.") from exc
        logging.getLogger("yfinance").setLevel(logging.CRITICAL)

        yahoo_symbol = self._to_yahoo_symbol(symbol)
        try:
            ticker = yf.Ticker(yahoo_symbol)
            history = ticker.history(period="5d", interval="1m", prepost=True, auto_adjust=False)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"yfinance history fetch failed for {symbol}: {exc}") from exc
        if history is None or history.empty:
            raise RuntimeError(f"No yfinance history returned for {symbol}.")

        bars: list[dict[str, float]] = []
        for index, row in history.tail(max(self.config.intraday_feature_window_minutes, 40)).iterrows():
            close = row.get("Close")
            if close is None or math.isnan(float(close)):
                continue
            open_price = row.get("Open", close)
            high = row.get("High", close)
            low = row.get("Low", close)
            volume = row.get("Volume", 0.0)
            bars.append(
                {
                    "ts": index.timestamp(),
                    "open": float(open_price if not math.isnan(float(open_price)) else close),
                    "high": float(high if not math.isnan(float(high)) else close),
                    "low": float(low if not math.isnan(float(low)) else close),
                    "close": float(close),
                    "volume": float(0.0 if volume is None or math.isnan(float(volume)) else volume),
                }
            )

        if len(bars) < 6:
            raise RuntimeError(f"Not enough yfinance bars returned for {symbol}.")

        window = min(self.config.intraday_feature_window_minutes, len(bars) - 1)
        recent = bars[-window:]
        last_bar = recent[-1]
        first_bar = recent[0]
        previous_close = float(last_bar["close"])
        if "Close" in history.columns and len(history["Close"]) > window:
            previous_close = float(history["Close"].iloc[-(window + 1)])
        volumes = [float(bar.get("volume") or 0.0) for bar in recent]
        avg_volume_30 = sum(volumes) / max(len(volumes), 1)
        avg_volume_5 = sum(volumes[-5:]) / max(min(5, len(volumes)), 1)
        returns_1m = []
        for prev, curr in zip(recent[:-1], recent[1:], strict=False):
            if prev["close"] and curr["close"]:
                returns_1m.append((curr["close"] / prev["close"]) - 1.0)

        features = PriceActionFeatures(
            symbol=symbol,
            source="yfinance",
            interval="1m",
            lookback_minutes=window,
            last_price=float(last_bar["close"]),
            previous_close=previous_close,
            return_5m_pct=self._return_pct(bars, 5),
            return_15m_pct=self._return_pct(bars, 15),
            return_30m_pct=self._return_pct(bars, 30),
            intraday_range_pct_30m=(max(bar["high"] for bar in recent) - min(bar["low"] for bar in recent))
            / max(first_bar["close"], 1e-6),
            volatility_1m_std_30m=pstdev(returns_1m) if len(returns_1m) >= 2 else 0.0,
            volume_ratio_5m_vs_30m=(avg_volume_5 / avg_volume_30) if avg_volume_30 > 0 else None,
            raw={
                "provider_symbol": yahoo_symbol,
                "points": len(bars),
                "window_points": len(recent),
            },
        )
        dataset: dict[str, object] = {
            "features": features,
            "bars": bars,
        }
        self._shared_cache[symbol] = (time(), dataset)
        return dataset

    @staticmethod
    def _return_pct(bars: list[dict[str, float]], lookback_minutes: int) -> float | None:
        if len(bars) <= lookback_minutes:
            return None
        current = bars[-1]["close"]
        previous = bars[-(lookback_minutes + 1)]["close"]
        if math.isclose(previous, 0.0):
            return None
        return (current / previous) - 1.0

    @staticmethod
    def _to_yahoo_symbol(symbol: str) -> str:
        market, _, code = symbol.upper().partition(".")
        if not code:
            return symbol.upper()
        if market == "US":
            return code
        if market == "HK":
            return f"{code.zfill(4)}.HK"
        return code
