from __future__ import annotations

from hfqt.features.market_features import MarketFeatureBuilder
from hfqt.features.sentiment_features import SentimentFeatureBuilder
from hfqt.schemas import FeatureSnapshot, InputEvent


class FeatureEngine:
    def build(self, event: InputEvent) -> FeatureSnapshot:
        market = MarketFeatureBuilder.build(event)
        sentiment = SentimentFeatureBuilder.build(event)

        volume_ratio = float(market.get("volume_ratio_5m_vs_30m") or 0.0)
        intraday_range = float(market.get("intraday_range_30m") or 0.0)
        source_quality = float(market.get("source_quality_score") or 0.0)
        diversity = float(market.get("diversity_score") or 0.0)
        news_count = int(market.get("news_count") or 0)
        social_count = int(market.get("social_count") or 0)

        liquidity_score = self._clip(
            0.34
            + min(max(volume_ratio, 0.0), 2.5) * 0.18
            + (0.12 if market.get("last_price") else 0.0)
            - min(intraday_range * 8.0, 0.24),
            0.05,
            0.98,
        )
        event_density_score = self._clip(
            0.18 + min(news_count, 4) * 0.12 + min(social_count, 4) * 0.08 + diversity * 0.18,
            0.0,
            1.0,
        )

        return FeatureSnapshot(
            event_id=event.event_id,
            symbol=event.symbol,
            sentiment_score=float(sentiment["sentiment_score"]),
            sentiment_momentum=float(sentiment["sentiment_momentum"]),
            momentum_5m=market.get("momentum_5m"),
            momentum_15m=market.get("momentum_15m"),
            momentum_30m=market.get("momentum_30m"),
            volatility_30m=market.get("volatility_30m"),
            intraday_range_30m=market.get("intraday_range_30m"),
            volume_ratio_5m_vs_30m=market.get("volume_ratio_5m_vs_30m"),
            source_quality_score=float(market["source_quality_score"]),
            freshness_score=float(market["freshness_score"]),
            diversity_score=float(market["diversity_score"]),
            news_count=news_count,
            social_count=social_count,
            history_bias=float(sentiment["history_bias"]),
            history_support=float(market["history_support"]),
            liquidity_score=liquidity_score,
            event_density_score=event_density_score,
            last_price=market.get("last_price"),
            metadata={
                "event_source": event.source,
                "event_type": event.event_type.value,
                "has_history_match": bool(event.metadata.get("history_match")),
            },
        )

    @staticmethod
    def _clip(value: float, low: float, high: float) -> float:
        return max(low, min(high, value))
