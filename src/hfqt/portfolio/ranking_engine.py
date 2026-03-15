from __future__ import annotations

from hfqt.schemas import AlphaSignal, TradeAction


class RankingEngine:
    def score(self, signal: AlphaSignal) -> float:
        score = float(signal.ranking_score or abs(signal.score))
        if signal.direction == TradeAction.HOLD:
            score -= 0.15
        return round(score, 6)
