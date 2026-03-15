from __future__ import annotations

from hfqt.alpha import RegimeEngine, SignalEngine
from hfqt.config import AppConfig
from hfqt.features import FeatureEngine
from hfqt.portfolio import PositionSizingEngine, RankingEngine
from hfqt.schemas import AlphaSignal, FeatureSnapshot, InputEvent, TradeIntent


class QuantPipeline:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.feature_engine = FeatureEngine()
        self.regime_engine = RegimeEngine()
        self.signal_engine = SignalEngine()
        self.ranking_engine = RankingEngine()
        self.sizing_engine = PositionSizingEngine(config)

    def analyze(self, event: InputEvent) -> tuple[FeatureSnapshot, AlphaSignal]:
        features = self.feature_engine.build(event)
        regime = self.regime_engine.detect(features)
        signal = self.signal_engine.generate(features, regime)
        signal = signal.model_copy(
            update={
                "ranking_score": self.ranking_engine.score(signal),
            }
        )
        return features, signal

    def build_trade_intent(
        self,
        event: InputEvent,
        features: FeatureSnapshot,
        signal: AlphaSignal,
        rationale: str | None = None,
        strategy_id: str = "deterministic-factor-v1",
    ) -> TradeIntent:
        quantity = event.quantity or self.sizing_engine.size(signal, features, event.price)
        limit_price = event.price or features.last_price
        return TradeIntent(
            event_id=event.event_id,
            symbol=event.symbol,
            action=signal.direction,
            quantity=quantity,
            limit_price=limit_price,
            confidence=signal.confidence,
            score=signal.score,
            regime=signal.regime,
            rationale=rationale or signal.rationale,
            strategy_id=strategy_id,
            metadata={
                "headline": event.headline,
                "source": event.source,
                "event_type": event.event_type.value,
                "feature_snapshot": features.model_dump(mode="json"),
                "alpha_signal": signal.model_dump(mode="json"),
            },
        )
