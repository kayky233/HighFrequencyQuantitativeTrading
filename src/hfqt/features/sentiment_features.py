from __future__ import annotations

from hfqt.schemas import InputEvent


class SentimentFeatureBuilder:
    @staticmethod
    def build(event: InputEvent) -> dict[str, float]:
        sentiment_score = max(-1.0, min(1.0, float(event.sentiment or 0.0)))
        history_match = event.metadata.get("history_match") or []
        history_bias = SentimentFeatureBuilder._history_bias(history_match)
        source_quality = event.metadata.get("source_quality") or {}
        freshness = float(source_quality.get("freshness_score") or 0.0)
        layer_weight = float((source_quality.get("signal_layers") or {}).get("weighted_score") or 0.0)
        sentiment_momentum = max(-1.0, min(1.0, sentiment_score * 0.6 + freshness * 0.25 + layer_weight * 0.15))
        return {
            "sentiment_score": sentiment_score,
            "sentiment_momentum": sentiment_momentum,
            "history_bias": history_bias,
        }

    @staticmethod
    def _history_bias(history_matches: list[dict]) -> float:
        if not history_matches:
            return 0.0
        total_weight = 0.0
        weighted_signal = 0.0
        for match in history_matches[:3]:
            similarity = float(match.get("similarity") or 0.0)
            action = str(match.get("prior_action") or "HOLD").upper()
            action_score = 1.0 if action == "BUY" else -1.0 if action == "SELL" else 0.0
            total_weight += similarity
            weighted_signal += similarity * action_score
        if total_weight <= 0:
            return 0.0
        return max(-1.0, min(1.0, weighted_signal / total_weight))
