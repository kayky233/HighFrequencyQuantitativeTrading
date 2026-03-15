from __future__ import annotations

from datetime import UTC, datetime

from hfqt.config import AppConfig
from hfqt.marketdata import PublicPriceActionAdapter
from hfqt.schemas import InputEvent
from hfqt.store.sqlite_store import SQLiteAuditStore


class EventEnricher:
    def __init__(self, config: AppConfig, store: SQLiteAuditStore) -> None:
        self.config = config
        self.store = store
        self.marketdata = PublicPriceActionAdapter(config)

    async def enrich(self, event: InputEvent) -> InputEvent:
        metadata = dict(event.metadata)
        enrichment_errors = dict(metadata.get("enrichment_errors") or {})
        updated_price = event.price

        if "price_action" not in metadata:
            try:
                features = await self.marketdata.get_intraday_features(event.symbol)
                metadata["price_action"] = features.model_dump(mode="json")
                updated_price = updated_price or features.last_price
            except Exception as exc:  # noqa: BLE001
                enrichment_errors["price_action"] = str(exc)

        if "source_quality" not in metadata:
            metadata["source_quality"] = self._build_source_quality(event, metadata)

        if "history_match" not in metadata:
            try:
                history_matches = await self.store.find_similar_events(
                    event,
                    limit=self.config.history_match_limit,
                    lookback_days=self.config.history_match_lookback_days,
                )
                metadata["history_match"] = [match.model_dump(mode="json") for match in history_matches]
            except Exception as exc:  # noqa: BLE001
                enrichment_errors["history_match"] = str(exc)

        if enrichment_errors:
            metadata["enrichment_errors"] = enrichment_errors

        return event.model_copy(update={"metadata": metadata, "price": updated_price})

    def _build_source_quality(self, event: InputEvent, metadata: dict) -> dict:
        items = metadata.get("items") or []
        if not items:
            return {
                "overall_score": 0.35 if event.source == "demo-ui" else 0.25,
                "news_count": 0,
                "social_count": 0,
                "trusted_social_count": 0,
                "freshness_score": 0.5,
                "diversity_score": 0.3,
                "mode": "manual_or_sparse",
                "signal_layers": {},
                "dedup_count": 0,
            }

        news_sources = {
            "google_news",
            "yfinance_news",
            "alpha_vantage_news",
            "financial_datasets",
            "btc_etf_flow",
            "macro_event",
        }
        social_sources = {"xreach", "x_monitor", "x_link", "whale_alert"}
        news_count = sum(1 for item in items if item.get("source") in news_sources)
        social_count = sum(1 for item in items if item.get("source") in social_sources)
        trusted_social_count = 0
        item_scores: list[float] = []
        freshness_scores: list[float] = []
        present_sources: set[str] = set()
        signal_layers, deduped_items = self._summarize_signal_layers(items)

        for item in deduped_items:
            raw = item.get("raw") or {}
            source = item.get("source")
            base_score = self._source_base_score(source)
            if source in {"xreach", "x_monitor", "x_link", "whale_alert"}:
                view_count = float(raw.get("viewCount") or 0.0)
                like_count = float(raw.get("likeCount") or 0.0)
                user = raw.get("user") or {}
                if user.get("isBlueVerified") or view_count >= 1000 or like_count >= 20:
                    base_score += 0.18
                    trusted_social_count += 1
            if source:
                present_sources.add(str(source))
            item_scores.append(min(base_score, 0.95))

            freshness_scores.append(self._freshness_score(item.get("published_at")))

        diversity_score = min(1.0, len(present_sources) / 4.0)
        avg_item_score = sum(item_scores) / max(len(item_scores), 1)
        avg_freshness = sum(freshness_scores) / max(len(freshness_scores), 1)
        layer_weight = float(signal_layers.get("weighted_score") or 0.0)
        overall_score = min(0.99, 0.40 * avg_item_score + 0.30 * avg_freshness + 0.15 * diversity_score + 0.15 * layer_weight)

        return {
            "overall_score": round(overall_score, 4),
            "news_count": news_count,
            "social_count": social_count,
            "trusted_social_count": trusted_social_count,
            "freshness_score": round(avg_freshness, 4),
            "diversity_score": round(diversity_score, 4),
            "mode": "network_intel",
            "signal_layers": signal_layers,
            "dedup_count": max(len(items) - len(deduped_items), 0),
        }

    def _summarize_signal_layers(self, items: list[dict]) -> tuple[dict, list[dict]]:
        layer_weights = {
            "macro_event": 1.1,
            "btc_etf_flow": 1.0,
            "financial_datasets": 0.9,
            "alpha_vantage_news": 0.85,
            "yfinance_news": 0.7,
            "google_news": 0.6,
            "whale_alert": 0.75,
            "x_monitor": 0.65,
            "x_link": 0.6,
            "xreach": 0.4,
        }
        deduped = self._deduplicate_items(items)
        layer_stats: dict[str, dict] = {}
        weighted_score = 0.0
        for item in deduped:
            source = str(item.get("source") or "unknown")
            weight = layer_weights.get(source, 0.45)
            if source not in layer_stats:
                layer_stats[source] = {"count": 0, "weight": weight}
            layer_stats[source]["count"] += 1
            weighted_score += weight

        total = sum(stat["count"] for stat in layer_stats.values())
        coverage = min(1.0, len(layer_stats) / 5.0)
        weighted_score = weighted_score / max(total, 1)
        return {
            "layer_counts": {key: value["count"] for key, value in layer_stats.items()},
            "layer_weights": {key: value["weight"] for key, value in layer_stats.items()},
            "coverage_score": round(coverage, 4),
            "weighted_score": round(min(1.0, weighted_score), 4),
        }, deduped

    @staticmethod
    def _deduplicate_items(items: list[dict]) -> list[dict]:
        seen: set[str] = set()
        deduped: list[dict] = []
        for item in items:
            title = str(item.get("title") or "").strip().lower()
            url = str(item.get("url") or "").strip().lower()
            key = url or title
            if not key:
                continue
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    @staticmethod
    def _source_base_score(source: str | None) -> float:
        if source == "alpha_vantage_news":
            return 0.84
        if source == "financial_datasets":
            return 0.93
        if source == "btc_etf_flow":
            return 0.92
        if source == "macro_event":
            return 0.88
        if source == "yfinance_news":
            return 0.76
        if source == "google_news":
            return 0.7
        if source == "whale_alert":
            return 0.82
        if source == "x_monitor":
            return 0.74
        if source == "x_link":
            return 0.62
        if source == "xreach":
            return 0.38
        return 0.45

    @staticmethod
    def _freshness_score(published_at: str | None) -> float:
        if not published_at:
            return 0.35

        parsed: datetime | None = None
        for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a %b %d %H:%M:%S %z %Y"):
            try:
                parsed = datetime.strptime(published_at, fmt)
                break
            except ValueError:
                continue

        if parsed is None:
            return 0.35

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        else:
            parsed = parsed.astimezone(UTC)

        age_minutes = max((datetime.now(UTC) - parsed).total_seconds() / 60.0, 0.0)
        if age_minutes <= 30:
            return 1.0
        if age_minutes <= 120:
            return 0.82
        if age_minutes <= 360:
            return 0.65
        if age_minutes <= 1440:
            return 0.45
        return 0.25
