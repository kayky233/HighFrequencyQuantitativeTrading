from __future__ import annotations

from hfqt.config import AppConfig
from hfqt.pipeline import QuantPipeline
from hfqt.schemas import InputEvent, TradeIntent


class MockReasoner:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.pipeline = QuantPipeline(config)
        self.last_feature_snapshot = None
        self.last_alpha_signal = None

    async def generate(self, event: InputEvent) -> TradeIntent:
        features, signal = self.pipeline.analyze(event)
        self.last_feature_snapshot = features
        self.last_alpha_signal = signal
        rationale = (
            f"score={signal.score:.2f}; regime={signal.regime.value}; "
            f"liq={features.liquidity_score:.2f}; source={features.source_quality_score:.2f}; "
            f"m15={float(features.momentum_15m or 0.0):+.2%}"
        )
        return self.pipeline.build_trade_intent(
            event=event,
            features=features,
            signal=signal,
            rationale=rationale,
            strategy_id="deterministic-factor-v2",
        )
