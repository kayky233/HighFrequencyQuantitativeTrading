from __future__ import annotations

from hfqt.schemas import FeatureSnapshot, RegimeType


class RegimeEngine:
    def detect(self, features: FeatureSnapshot) -> RegimeType:
        volatility = float(features.volatility_30m or 0.0)
        intraday_range = float(features.intraday_range_30m or 0.0)
        momentum = abs(float(features.momentum_30m or 0.0))
        sentiment = features.sentiment_score

        if (volatility >= 0.006 or intraday_range >= 0.018) and sentiment < -0.15:
            return RegimeType.RISK_OFF
        if momentum < 0.0025 and volatility < 0.003 and intraday_range < 0.009:
            return RegimeType.MEAN_REVERSION
        return RegimeType.TREND
