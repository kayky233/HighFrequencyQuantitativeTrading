from __future__ import annotations

from hfqt.schemas import AlphaSignal, FeatureSnapshot, RegimeType, TradeAction


class SignalEngine:
    def generate(self, features: FeatureSnapshot, regime: RegimeType) -> AlphaSignal:
        momentum_5m = self._scaled(features.momentum_5m, 18.0)
        momentum_15m = self._scaled(features.momentum_15m, 14.0)
        momentum_30m = self._scaled(features.momentum_30m, 10.0)
        sentiment_direction = self._signed(features.sentiment_score * 0.7 + features.sentiment_momentum * 0.3)
        price_direction = self._signed(momentum_5m * 0.45 + momentum_15m * 0.35 + momentum_30m * 0.20)
        alignment = sentiment_direction * price_direction if sentiment_direction and price_direction else 0.0
        volume_support = self._clip(float(features.volume_ratio_5m_vs_30m or 1.0) - 1.0, -0.45, 0.85)
        volatility_penalty = min(float(features.volatility_30m or 0.0) * 20.0, 0.22)
        freshness_bonus = max(features.freshness_score - 0.45, 0.0) * 0.10
        price_confirmation = self._clip(
            0.48
            + alignment * 0.22
            + volume_support * 0.18
            + max(abs(momentum_5m), abs(momentum_15m)) * 0.12
            + freshness_bonus
            - volatility_penalty * 0.45,
            0.0,
            1.0,
        )
        execution_quality = self._clip(
            features.liquidity_score * 0.42
            + price_confirmation * 0.33
            + features.source_quality_score * 0.15
            + features.freshness_score * 0.10,
            0.0,
            1.0,
        )
        sentiment_term = features.sentiment_score * 0.34
        source_term = (features.source_quality_score - 0.5) * 0.12
        history_term = features.history_bias * 0.11
        liquidity_term = (features.liquidity_score - 0.5) * 0.11
        density_term = (features.event_density_score - 0.45) * 0.10
        momentum_term = momentum_5m * 0.10 + momentum_15m * 0.18 + momentum_30m * 0.12
        confirmation_term = (price_confirmation - 0.5) * 0.24
        execution_term = (execution_quality - 0.5) * 0.10
        score = sentiment_term + source_term + history_term + liquidity_term + density_term + momentum_term + confirmation_term + execution_term

        if regime == RegimeType.RISK_OFF and score > 0:
            score -= 0.12
        elif regime == RegimeType.TREND:
            score += 0.04 if score > 0 else -0.04 if score < 0 else 0.0

        direction = TradeAction.HOLD
        neutral_band = 0.16
        if price_confirmation < 0.42:
            neutral_band += 0.04
        elif price_confirmation > 0.68:
            neutral_band -= 0.03
        if abs(score) >= neutral_band and features.source_quality_score >= 0.2:
            direction = TradeAction.BUY if score > 0 else TradeAction.SELL

        confidence = self._clip(
            0.32
            + abs(score) * 0.42
            + abs(features.sentiment_score) * 0.12
            + features.source_quality_score * 0.12
            + features.liquidity_score * 0.10
            + features.history_support * 0.04
            + price_confirmation * 0.08
            + execution_quality * 0.07
            - volatility_penalty,
            0.20,
            0.97,
        )
        ranking_score = abs(score) * (0.54 + execution_quality * 0.28 + price_confirmation * 0.18)
        if direction == TradeAction.HOLD:
            ranking_score -= 0.20

        rationale = (
            f"score={score:.2f}; sentiment={features.sentiment_score:.2f}; "
            f"m15={float(features.momentum_15m or 0.0):+.2%}; "
            f"confirm={price_confirmation:.2f}; exec={execution_quality:.2f}; regime={regime.value}"
        )
        return AlphaSignal(
            event_id=features.event_id,
            symbol=features.symbol,
            direction=direction,
            score=round(score, 6),
            confidence=round(confidence, 6),
            regime=regime,
            ranking_score=round(ranking_score, 6),
            rationale=rationale,
            metadata={
                "price_confirmation": round(price_confirmation, 6),
                "execution_quality": round(execution_quality, 6),
                "alignment": round(alignment, 6),
                "volume_support": round(volume_support, 6),
                "neutral_band": round(neutral_band, 6),
                "momentum_term": round(momentum_term, 6),
                "sentiment_term": round(sentiment_term, 6),
                "history_term": round(history_term, 6),
                "source_term": round(source_term, 6),
                "liquidity_term": round(liquidity_term, 6),
                "density_term": round(density_term, 6),
                "confirmation_term": round(confirmation_term, 6),
                "execution_term": round(execution_term, 6),
            },
        )

    @staticmethod
    def _scaled(value: float | None, factor: float) -> float:
        if value is None:
            return 0.0
        return max(-1.0, min(1.0, float(value) * factor))

    @staticmethod
    def _clip(value: float, low: float, high: float) -> float:
        return max(low, min(high, value))

    @staticmethod
    def _signed(value: float) -> float:
        if value > 0:
            return 1.0
        if value < 0:
            return -1.0
        return 0.0
