from __future__ import annotations

from hfqt.config import AppConfig
from hfqt.schemas import AlphaSignal, FeatureSnapshot, RegimeType


class PositionSizingEngine:
    def __init__(self, config: AppConfig) -> None:
        self.config = config

    def size(self, signal: AlphaSignal, features: FeatureSnapshot, fallback_price: float | None) -> float:
        price = float(features.last_price or fallback_price or 0.0)
        risk_scalar = 0.7 if signal.regime == RegimeType.RISK_OFF else 1.0
        liquidity_boost = max(features.liquidity_score - 0.45, 0.0) * 2.0
        execution_quality = float((signal.metadata or {}).get("execution_quality") or 0.5)
        price_confirmation = float((signal.metadata or {}).get("price_confirmation") or 0.5)
        volatility_penalty = min(float(features.volatility_30m or 0.0) * 16.0, 0.55)
        conviction = max(signal.confidence - self.config.min_confidence, 0.0)
        raw_quantity = self.config.default_quantity * (
            1.0
            + signal.confidence * 1.8
            + conviction * 1.6
            + liquidity_boost
            + execution_quality * 1.25
            + price_confirmation * 0.95
            - volatility_penalty
        ) * risk_scalar
        quantity = max(1.0, round(raw_quantity))

        if price > 0:
            max_qty_by_notional = max(int(self.config.max_notional_per_order // price), 1)
            quantity = min(quantity, float(max_qty_by_notional))
        return float(quantity)
