from __future__ import annotations


class UniverseSelector:
    @staticmethod
    def _bucket_priority(item: dict) -> int:
        order = {
            "execute": 0,
            "queued": 1,
            "watch": 2,
            "setup": 3,
            "observe": 4,
        }
        return order.get(str(item.get("candidate_bucket") or "observe"), 9)

    @staticmethod
    def _sort_key(item: dict) -> tuple:
        return (
            UniverseSelector._bucket_priority(item),
            item.get("action") == "HOLD",
            item.get("execution_status") == "ALLOW" and item.get("risk_status") != "ALLOW",
            -(float(item.get("ranking_score") or 0.0)),
            -float(item.get("confidence") or 0.0),
            -float(item.get("freshness_score") or 0.0),
        )

    @staticmethod
    def filter_allowed(symbols: list[str], allowed_symbols: list[str]) -> list[str]:
        allowed = {item.upper() for item in allowed_symbols}
        return [symbol for symbol in symbols if symbol.upper() in allowed]

    @staticmethod
    def sort_candidates(items: list[dict]) -> list[dict]:
        return sorted(items, key=UniverseSelector._sort_key)

    @staticmethod
    def select_top(items: list[dict], limit: int = 3) -> list[dict]:
        ordered = UniverseSelector.sort_candidates(items)
        return ordered[: max(limit, 1)]

    @staticmethod
    def select_actionable(items: list[dict], limit: int = 3) -> list[dict]:
        ordered = UniverseSelector.sort_candidates(items)
        actionable = [
            item
            for item in ordered
            if item.get("candidate_bucket") in {"execute", "queued"}
        ]
        if actionable:
            return actionable[: max(limit, 1)]

        directional = [item for item in ordered if item.get("candidate_bucket") == "watch"]
        if directional:
            return directional[: max(limit, 1)]

        setups = [item for item in ordered if item.get("candidate_bucket") == "setup"]
        if setups:
            return setups[: max(limit, 1)]

        return ordered[: max(limit, 1)]
