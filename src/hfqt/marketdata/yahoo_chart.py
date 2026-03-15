from __future__ import annotations

import math
from statistics import pstdev
from time import time
from typing import Any

import httpx

from hfqt.config import AppConfig
from hfqt.schemas import PriceActionFeatures


class YahooChartAdapter:
    _shared_cache: dict[str, tuple[float, dict[str, Any]]] = {}

    def __init__(self, config: AppConfig) -> None:
        self.config = config

    async def get_intraday_features(self, symbol: str) -> PriceActionFeatures:
        dataset = await self._get_dataset(symbol)
        return dataset["features"]

    async def get_intraday_bars(self, symbol: str, bar_limit: int = 90) -> list[dict[str, float]]:
        dataset = await self._get_dataset(symbol)
        bars = dataset["bars"]
        return [dict(bar) for bar in bars[-max(10, bar_limit):]]

    async def _get_dataset(self, symbol: str) -> dict[str, Any]:
        cached = self._shared_cache.get(symbol)
        if cached and time() - cached[0] <= 45:
            return cached[1]

        yahoo_symbol = self._to_yahoo_symbol(symbol)
        urls = [
            f"{self.config.yahoo_chart_base_url.rstrip('/')}/v8/finance/chart/{yahoo_symbol}",
            f"https://query2.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}",
        ]
        params = {
            "interval": "1m",
            "range": "5d",
            "includePrePost": "true",
            "events": "div,splits",
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://finance.yahoo.com/",
        }

        last_error: Exception | None = None
        payload = None
        async with httpx.AsyncClient(timeout=12, trust_env=True, headers=headers) as client:
            for url in urls:
                try:
                    response = await client.get(url, params=params)
                    response.raise_for_status()
                    payload = response.json()
                    break
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
        if payload is None:
            raise RuntimeError(str(last_error) if last_error else f"Failed to fetch chart for {symbol}.")

        result = ((payload.get("chart") or {}).get("result") or [None])[0]
        if not result:
            raise RuntimeError(f"No chart payload returned for {symbol}.")

        bars = self._parse_bars(result)
        if len(bars) < 6:
            raise RuntimeError(f"Not enough intraday bars returned for {symbol}.")

        window = min(self.config.intraday_feature_window_minutes, len(bars) - 1)
        recent = bars[-window:]
        last_bar = recent[-1]
        first_bar = recent[0]
        previous_close = float((result.get("meta") or {}).get("previousClose") or first_bar["close"])
        last_price = float(last_bar["close"])

        volumes = [float(bar.get("volume") or 0.0) for bar in recent]
        avg_volume_30 = sum(volumes) / max(len(volumes), 1)
        avg_volume_5 = sum(volumes[-5:]) / max(min(5, len(volumes)), 1)
        returns_1m = []
        for prev, curr in zip(recent[:-1], recent[1:], strict=False):
            if prev["close"] and curr["close"]:
                returns_1m.append((curr["close"] / prev["close"]) - 1.0)

        features = PriceActionFeatures(
            symbol=symbol,
            source="yahoo_chart",
            interval="1m",
            lookback_minutes=window,
            last_price=last_price,
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
                "currency": (result.get("meta") or {}).get("currency"),
                "exchange_name": (result.get("meta") or {}).get("exchangeName"),
            },
        )
        dataset = {
            "features": features,
            "bars": bars,
        }
        self._shared_cache[symbol] = (time(), dataset)
        return dataset

    @staticmethod
    def _parse_bars(result: dict[str, Any]) -> list[dict[str, float]]:
        timestamps = result.get("timestamp") or []
        quote = (((result.get("indicators") or {}).get("quote")) or [{}])[0]
        opens = quote.get("open") or []
        highs = quote.get("high") or []
        lows = quote.get("low") or []
        closes = quote.get("close") or []
        volumes = quote.get("volume") or []
        bars: list[dict[str, float]] = []
        for index, ts in enumerate(timestamps):
            close = closes[index] if index < len(closes) else None
            if close is None:
                continue
            open_price = opens[index] if index < len(opens) and opens[index] is not None else close
            high = highs[index] if index < len(highs) and highs[index] is not None else close
            low = lows[index] if index < len(lows) and lows[index] is not None else close
            volume = volumes[index] if index < len(volumes) and volumes[index] is not None else 0.0
            bars.append(
                {
                    "ts": float(ts),
                    "open": float(open_price),
                    "high": float(high),
                    "low": float(low),
                    "close": float(close),
                    "volume": float(volume),
                }
            )
        return bars

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
