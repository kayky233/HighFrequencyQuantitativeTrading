from __future__ import annotations

from hfqt.schemas import InputEvent


class MarketFeatureBuilder:
    @staticmethod
    def build(event: InputEvent) -> dict[str, float | int | None]:
        metadata = event.metadata or {}
        price_action = metadata.get("price_action") or {}
        source_quality = metadata.get("source_quality") or {}
        history_match = metadata.get("history_match") or []

        return {
            "momentum_5m": MarketFeatureBuilder._safe_float(price_action.get("return_5m_pct")),
            "momentum_15m": MarketFeatureBuilder._safe_float(price_action.get("return_15m_pct")),
            "momentum_30m": MarketFeatureBuilder._safe_float(price_action.get("return_30m_pct")),
            "volatility_30m": MarketFeatureBuilder._safe_float(price_action.get("volatility_1m_std_30m")),
            "intraday_range_30m": MarketFeatureBuilder._safe_float(price_action.get("intraday_range_pct_30m")),
            "volume_ratio_5m_vs_30m": MarketFeatureBuilder._safe_float(price_action.get("volume_ratio_5m_vs_30m")),
            "last_price": MarketFeatureBuilder._safe_float(price_action.get("last_price")) or event.price,
            "source_quality_score": MarketFeatureBuilder._safe_float(source_quality.get("overall_score"), 0.0),
            "freshness_score": MarketFeatureBuilder._safe_float(source_quality.get("freshness_score"), 0.0),
            "diversity_score": MarketFeatureBuilder._safe_float(source_quality.get("diversity_score"), 0.0),
            "news_count": int(source_quality.get("news_count") or 0),
            "social_count": int(source_quality.get("social_count") or 0),
            "history_support": min(float(len(history_match)), 3.0) / 3.0,
        }

    @staticmethod
    def _safe_float(value, default: float | None = None) -> float | None:
        if value in {None, ""}:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
