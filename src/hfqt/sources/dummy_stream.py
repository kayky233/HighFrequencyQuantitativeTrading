from __future__ import annotations

import math
import random
from collections import deque
from dataclasses import dataclass, field

from hfqt.config import AppConfig
from hfqt.marketdata import PublicPriceActionAdapter
from hfqt.schemas import EventType, InputEvent


@dataclass
class _DummySymbolState:
    price: float
    previous_close: float
    prices: deque[float] = field(default_factory=lambda: deque(maxlen=30))
    volumes: deque[float] = field(default_factory=lambda: deque(maxlen=30))


class DummyStreamSource:
    def __init__(self, config: AppConfig, seed: int = 42) -> None:
        self.config = config
        self.random = random.Random(seed)
        self.marketdata = PublicPriceActionAdapter(config)
        self.state: dict[str, _DummySymbolState] = {}

    async def prime(self, symbols: list[str]) -> None:
        for symbol in symbols:
            if symbol in self.state:
                continue
            base_price = 100.0
            previous_close = base_price
            try:
                features = await self.marketdata.get_intraday_features(symbol)
                if features.last_price:
                    base_price = features.last_price
                if features.previous_close:
                    previous_close = features.previous_close
            except Exception:  # noqa: BLE001
                pass
            history = deque([base_price] * 30, maxlen=30)
            volumes = deque([1000.0] * 30, maxlen=30)
            self.state[symbol] = _DummySymbolState(
                price=base_price,
                previous_close=previous_close,
                prices=history,
                volumes=volumes,
            )

    async def next_event(self, symbol: str, sequence: int) -> InputEvent:
        if symbol not in self.state:
            await self.prime([symbol])
        state = self.state[symbol]

        wave = math.sin(sequence / 3.0) * 0.0045
        shock = self.random.uniform(-0.003, 0.003)
        drift = wave + shock
        next_price = max(1.0, state.price * (1.0 + drift))
        volume = max(100.0, 1000.0 + self.random.uniform(-500.0, 1800.0) + abs(drift) * 200000)
        state.price = round(next_price, 4)
        state.prices.append(state.price)
        state.volumes.append(volume)

        sentiment = max(-0.95, min(0.95, drift * 90 + self.random.uniform(-0.12, 0.12)))
        momentum_5 = self._return_pct(state.prices, 5)
        momentum_15 = self._return_pct(state.prices, 15)
        momentum_30 = self._return_pct(state.prices, 30)
        intraday_range = (max(state.prices) - min(state.prices)) / max(state.prices[0], 1e-6)
        avg_volume_30 = sum(state.volumes) / max(len(state.volumes), 1)
        avg_volume_5 = sum(list(state.volumes)[-5:]) / max(min(5, len(state.volumes)), 1)
        volatility = self._volatility(state.prices)

        if sentiment >= 0.18:
            headline = f"{symbol} buy-wall thickens as synthetic demand pulse accelerates"
        elif sentiment <= -0.18:
            headline = f"{symbol} synthetic sell pressure rises as momentum deteriorates"
        else:
            headline = f"{symbol} synthetic tape remains range-bound with mixed micro-signals"

        return InputEvent(
            event_type=EventType.NEWS,
            source="dummy-stream",
            symbol=symbol,
            headline=headline,
            body=(
                f"dummy gateway pulse #{sequence}; drift={drift:+.4%}; "
                f"momentum5={momentum_5 or 0:+.4%}; momentum15={momentum_15 or 0:+.4%}"
            ),
            sentiment=sentiment,
            price=state.price,
            quantity=1.0,
            metadata={
                "trigger": "dummy_stream",
                "price_action": {
                    "symbol": symbol,
                    "source": "dummy_gateway",
                    "interval": "1m",
                    "lookback_minutes": min(len(state.prices), 30),
                    "last_price": state.price,
                    "previous_close": state.previous_close,
                    "return_5m_pct": momentum_5,
                    "return_15m_pct": momentum_15,
                    "return_30m_pct": momentum_30,
                    "intraday_range_pct_30m": intraday_range,
                    "volatility_1m_std_30m": volatility,
                    "volume_ratio_5m_vs_30m": (avg_volume_5 / avg_volume_30) if avg_volume_30 > 0 else None,
                },
                "source_quality": {
                    "overall_score": 0.82,
                    "news_count": 0,
                    "social_count": 0,
                    "trusted_social_count": 0,
                    "freshness_score": 1.0,
                    "diversity_score": 0.65,
                    "mode": "dummy_gateway",
                },
                "history_match": [],
            },
        )

    @staticmethod
    def _return_pct(prices: deque[float], lookback: int) -> float | None:
        if len(prices) <= lookback:
            return None
        current = prices[-1]
        previous = list(prices)[-(lookback + 1)]
        if previous == 0:
            return None
        return (current / previous) - 1.0

    @staticmethod
    def _volatility(prices: deque[float]) -> float:
        points = list(prices)
        if len(points) < 3:
            return 0.0
        returns = []
        for prev, curr in zip(points[:-1], points[1:], strict=False):
            if prev:
                returns.append((curr / prev) - 1.0)
        if len(returns) < 2:
            return 0.0
        mean = sum(returns) / len(returns)
        variance = sum((item - mean) ** 2 for item in returns) / len(returns)
        return variance**0.5
