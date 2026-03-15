from __future__ import annotations

import json
import re
import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

from hfqt.schemas import (
    AlphaSignal,
    DailyStats,
    FeatureSnapshot,
    HistoricalEventMatch,
    InputEvent,
    OrderRecord,
    OrderRequest,
    ReviewDecision,
    RiskDecision,
    TradeIntent,
)


class SQLiteAuditStore:
    def __init__(self, database_path: Path) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.database_path)

    def _table_exists(self, conn: sqlite3.Connection, table: str) -> bool:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        )
        return cursor.fetchone() is not None

    async def initialize(self) -> None:
        schema = {
            "input_events": "event_id",
            "feature_snapshots": "feature_id",
            "alpha_signals": "signal_id",
            "trade_intents": "intent_id",
            "review_decisions": "review_id",
            "risk_decisions": "decision_id",
            "order_requests": "request_id",
            "order_records": "record_id",
        }
        with closing(self._connect()) as conn:
            for table, key in schema.items():
                conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        {key} TEXT PRIMARY KEY,
                        created_at TEXT NOT NULL,
                        payload TEXT NOT NULL
                    )
                    """
                )
            conn.commit()

    @staticmethod
    def _payload(model) -> str:
        return json.dumps(model.model_dump(mode="json"), ensure_ascii=False, sort_keys=True)

    def _save(self, table: str, id_column: str, row_id: str, payload: str) -> None:
        created_at = datetime.now(UTC).isoformat()
        with closing(self._connect()) as conn:
            conn.execute(
                f"INSERT OR REPLACE INTO {table} ({id_column}, created_at, payload) VALUES (?, ?, ?)",
                (row_id, created_at, payload),
            )
            conn.commit()

    async def save_input_event(self, model: InputEvent) -> None:
        self._save("input_events", "event_id", model.event_id, self._payload(model))

    async def save_feature_snapshot(self, model: FeatureSnapshot) -> None:
        self._save("feature_snapshots", "feature_id", model.feature_id, self._payload(model))

    async def save_alpha_signal(self, model: AlphaSignal) -> None:
        self._save("alpha_signals", "signal_id", model.signal_id, self._payload(model))

    async def save_trade_intent(self, model: TradeIntent) -> None:
        self._save("trade_intents", "intent_id", model.intent_id, self._payload(model))

    async def save_review_decision(self, model: ReviewDecision) -> None:
        self._save("review_decisions", "review_id", model.review_id, self._payload(model))

    async def save_risk_decision(self, model: RiskDecision) -> None:
        self._save("risk_decisions", "decision_id", model.decision_id, self._payload(model))

    async def save_order_request(self, model: OrderRequest) -> None:
        self._save("order_requests", "request_id", model.request_id, self._payload(model))

    async def save_order_record(self, model: OrderRecord) -> None:
        self._save("order_records", "record_id", model.record_id, self._payload(model))

    async def count_order_requests_for_day(self, day: datetime | None = None) -> DailyStats:
        target = (day or datetime.now(UTC)).date().isoformat()
        with closing(self._connect()) as conn:
            cursor = conn.execute(
                "SELECT COUNT(1) FROM order_requests WHERE substr(created_at, 1, 10) = ?",
                (target,),
            )
            count = int(cursor.fetchone()[0])
        return DailyStats(day=datetime.fromisoformat(target).date(), order_requests=count)

    async def load_order_records(
        self,
        broker: str | None = None,
        status: str | None = None,
        symbol: str | None = None,
    ) -> list[OrderRecord]:
        with closing(self._connect()) as conn:
            if not self._table_exists(conn, "order_records"):
                return []
            rows = conn.execute("SELECT payload FROM order_records ORDER BY created_at ASC").fetchall()

        records: list[OrderRecord] = []
        for row in rows:
            payload = json.loads(row[0])
            if broker and payload.get("broker") != broker:
                continue
            if status and payload.get("status") != status:
                continue
            if symbol and payload.get("symbol") != symbol:
                continue
            records.append(OrderRecord.model_validate(payload))
        return records

    async def load_recent_input_events(self, limit: int = 8, symbol: str | None = None) -> list[InputEvent]:
        return [
            InputEvent.model_validate(payload)
            for payload in self._load_recent_payloads("input_events", limit=limit, symbol=symbol)
        ]

    async def load_recent_trade_intents(self, limit: int = 8, symbol: str | None = None) -> list[TradeIntent]:
        return [
            TradeIntent.model_validate(payload)
            for payload in self._load_recent_payloads("trade_intents", limit=limit, symbol=symbol)
        ]

    async def load_trade_intents_by_ids(self, intent_ids: list[str]) -> list[TradeIntent]:
        return [
            TradeIntent.model_validate(payload)
            for payload in self._load_payloads_by_ids("trade_intents", "intent_id", intent_ids)
        ]

    async def load_input_events_by_ids(self, event_ids: list[str]) -> list[InputEvent]:
        return [
            InputEvent.model_validate(payload)
            for payload in self._load_payloads_by_ids("input_events", "event_id", event_ids)
        ]

    async def load_recent_order_requests(
        self,
        limit: int = 8,
        broker: str | None = None,
        symbol: str | None = None,
    ) -> list[OrderRequest]:
        requests = [
            OrderRequest.model_validate(payload)
            for payload in self._load_recent_payloads("order_requests", limit=limit * 3, symbol=symbol)
        ]
        filtered: list[OrderRequest] = []
        for request in requests:
            if broker and request.broker != broker:
                continue
            filtered.append(request)
            if len(filtered) >= limit:
                break
        return filtered

    async def load_recent_review_decisions(self, limit: int = 8, symbol: str | None = None) -> list[ReviewDecision]:
        return [
            ReviewDecision.model_validate(payload)
            for payload in self._load_recent_payloads("review_decisions", limit=limit, symbol=symbol)
        ]

    async def load_recent_order_records(
        self,
        limit: int = 8,
        broker: str | None = None,
        symbol: str | None = None,
    ) -> list[OrderRecord]:
        records = [
            OrderRecord.model_validate(payload)
            for payload in self._load_recent_payloads("order_records", limit=limit * 3, symbol=symbol)
        ]
        filtered: list[OrderRecord] = []
        for record in records:
            if broker and record.broker != broker:
                continue
            filtered.append(record)
            if len(filtered) >= limit:
                break
        return filtered

    async def find_similar_events(
        self,
        event: InputEvent,
        limit: int = 3,
        lookback_days: int = 30,
    ) -> list[HistoricalEventMatch]:
        with closing(self._connect()) as conn:
            if not self._table_exists(conn, "input_events"):
                return []

            since = (datetime.now(UTC) - timedelta(days=lookback_days)).isoformat()
            event_rows = conn.execute(
                "SELECT payload FROM input_events WHERE created_at >= ? ORDER BY created_at DESC LIMIT 200",
                (since,),
            ).fetchall()
            intent_rows = (
                conn.execute("SELECT payload FROM trade_intents ORDER BY created_at DESC LIMIT 300").fetchall()
                if self._table_exists(conn, "trade_intents")
                else []
            )
            order_request_rows = (
                conn.execute("SELECT payload FROM order_requests ORDER BY created_at DESC LIMIT 300").fetchall()
                if self._table_exists(conn, "order_requests")
                else []
            )
            order_record_rows = (
                conn.execute("SELECT payload FROM order_records ORDER BY created_at DESC LIMIT 500").fetchall()
                if self._table_exists(conn, "order_records")
                else []
            )

        current_text = self._event_text(event.model_dump(mode="json"))
        current_tokens = self._tokenize(current_text)

        intents_by_event_id: dict[str, dict] = {}
        for row in intent_rows:
            payload = json.loads(row[0])
            event_id = payload.get("event_id")
            if event_id and event_id not in intents_by_event_id:
                intents_by_event_id[event_id] = payload

        requests_by_intent_id: dict[str, dict] = {}
        for row in order_request_rows:
            payload = json.loads(row[0])
            intent_id = payload.get("intent_id")
            if intent_id and intent_id not in requests_by_intent_id:
                requests_by_intent_id[intent_id] = payload

        latest_order_by_request_id: dict[str, dict] = {}
        for row in order_record_rows:
            payload = json.loads(row[0])
            request_id = payload.get("request_id")
            if request_id and request_id not in latest_order_by_request_id:
                latest_order_by_request_id[request_id] = payload

        matches: list[HistoricalEventMatch] = []
        for row in event_rows:
            payload = json.loads(row[0])
            if payload.get("event_id") == event.event_id or payload.get("symbol") != event.symbol:
                continue
            similarity = self._similarity_score(event, payload, current_tokens)
            if similarity < 0.12:
                continue

            intent_payload = intents_by_event_id.get(payload.get("event_id") or "")
            request_payload = requests_by_intent_id.get((intent_payload or {}).get("intent_id") or "")
            order_payload = latest_order_by_request_id.get((request_payload or {}).get("request_id") or "")
            ts_value = payload.get("ts")
            parsed_ts = None
            if ts_value:
                try:
                    parsed_ts = datetime.fromisoformat(ts_value.replace("Z", "+00:00"))
                except ValueError:
                    parsed_ts = None

            matches.append(
                HistoricalEventMatch(
                    event_id=payload["event_id"],
                    symbol=payload.get("symbol") or event.symbol,
                    similarity=round(similarity, 4),
                    ts=parsed_ts,
                    headline=payload.get("headline"),
                    sentiment=payload.get("sentiment"),
                    prior_action=(intent_payload or {}).get("action"),
                    prior_confidence=(intent_payload or {}).get("confidence"),
                    prior_order_status=(order_payload or {}).get("status"),
                    rationale=(intent_payload or {}).get("rationale"),
                )
            )

        matches.sort(key=lambda item: item.similarity, reverse=True)
        return matches[:limit]

    @staticmethod
    def _event_text(payload: dict) -> str:
        headline = str(payload.get("headline") or "")
        body = str(payload.get("body") or "")
        return f"{headline} {body}".strip()

    @staticmethod
    def _tokenize(text: str) -> set[str]:
        return {token for token in re.findall(r"[A-Za-z0-9]{3,}", text.lower())}

    def _similarity_score(self, current_event: InputEvent, prior_payload: dict, current_tokens: set[str]) -> float:
        prior_tokens = self._tokenize(self._event_text(prior_payload))
        overlap = len(current_tokens & prior_tokens) / max(len(current_tokens | prior_tokens), 1)
        prior_sentiment = float(prior_payload.get("sentiment") or 0.0)
        sentiment_similarity = max(0.0, 1.0 - abs(current_event.sentiment - prior_sentiment))
        same_type = 1.0 if prior_payload.get("event_type") == current_event.event_type.value else 0.0

        recency_score = 0.4
        ts_value = prior_payload.get("ts")
        if ts_value:
            try:
                parsed_ts = datetime.fromisoformat(str(ts_value).replace("Z", "+00:00"))
                age_days = max((datetime.now(UTC) - parsed_ts.astimezone(UTC)).total_seconds() / 86400.0, 0.0)
                recency_score = max(0.1, 1.0 - min(age_days / 30.0, 0.9))
            except ValueError:
                recency_score = 0.4

        return 0.45 * overlap + 0.25 * sentiment_similarity + 0.15 * same_type + 0.15 * recency_score

    def _load_recent_payloads(self, table: str, limit: int, symbol: str | None = None) -> list[dict]:
        with closing(self._connect()) as conn:
            if not self._table_exists(conn, table):
                return []
            rows = conn.execute(
                f"SELECT payload FROM {table} ORDER BY created_at DESC LIMIT ?",
                (max(limit, 1) * 5,),
            ).fetchall()

        payloads: list[dict] = []
        for row in rows:
            payload = json.loads(row[0])
            if symbol and payload.get("symbol") != symbol:
                continue
            payloads.append(payload)
            if len(payloads) >= limit:
                break
        return payloads

    def _load_payloads_by_ids(self, table: str, id_column: str, ids: list[str]) -> list[dict]:
        normalized_ids = [row_id for row_id in ids if row_id]
        if not normalized_ids:
            return []
        placeholders = ",".join("?" for _ in normalized_ids)
        with closing(self._connect()) as conn:
            if not self._table_exists(conn, table):
                return []
            rows = conn.execute(
                f"SELECT payload FROM {table} WHERE {id_column} IN ({placeholders})",
                normalized_ids,
            ).fetchall()
        return [json.loads(row[0]) for row in rows]
