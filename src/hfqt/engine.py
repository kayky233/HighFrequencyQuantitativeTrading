from __future__ import annotations

import inspect
from time import perf_counter

from hfqt.brokers.base import BrokerAdapter
from hfqt.config import AppConfig
from hfqt.runtime_logging import get_logger, log_decision_event, log_trade_event
from hfqt.schemas import OrderRequest, OrderType, ProcessResult, ReviewStatus, RiskStatus, TradeAction
from hfqt.store.sqlite_store import SQLiteAuditStore


logger = get_logger("engine")


class TradingEngine:
    def __init__(
        self,
        config: AppConfig,
        reasoner,
        risk_engine,
        broker: BrokerAdapter,
        store: SQLiteAuditStore,
        event_enricher=None,
        reviewer=None,
    ) -> None:
        self.config = config
        self.reasoner = reasoner
        self.risk_engine = risk_engine
        self.broker = broker
        self.store = store
        self.event_enricher = event_enricher
        self.reviewer = reviewer

    async def initialize(self) -> None:
        await self.store.initialize()
        await self.broker.connect()
        logger.info(
            "engine initialized",
            extra={
                "event": "engine_initialized",
                "broker": self.broker.name,
                "database_path": str(self.store.database_path),
            },
        )

    async def close(self) -> None:
        close_reasoner = getattr(self.reasoner, "aclose", None)
        if callable(close_reasoner):
            result = close_reasoner()
            if inspect.isawaitable(result):
                await result
        close_reviewer = getattr(self.reviewer, "aclose", None)
        if callable(close_reviewer):
            result = close_reviewer()
            if inspect.isawaitable(result):
                await result
        close_enricher = getattr(self.event_enricher, "aclose", None)
        if callable(close_enricher):
            result = close_enricher()
            if inspect.isawaitable(result):
                await result
        await self.broker.close()
        logger.info(
            "engine closed",
            extra={
                "event": "engine_closed",
                "broker": self.broker.name,
            },
        )

    async def process_event(self, event) -> ProcessResult:
        started_at = perf_counter()
        logger.info(
            "processing event",
            extra={
                "event": "process_event_started",
                "event_id": event.event_id,
                "symbol": event.symbol,
                "source": event.source,
                "event_type": event.event_type.value,
                "broker": self.broker.name,
            },
        )
        log_trade_event(
            "event_received",
            event_id=event.event_id,
            symbol=event.symbol,
            source=event.source,
            event_type=event.event_type.value,
            broker=self.broker.name,
            headline=event.headline,
        )
        log_decision_event(
            "event_received",
            event_id=event.event_id,
            symbol=event.symbol,
            source=event.source,
            event_type=event.event_type.value,
            broker=self.broker.name,
            headline=event.headline,
        )

        try:
            if self.event_enricher is not None:
                event = await self.event_enricher.enrich(event)
                logger.info(
                    "event enriched",
                    extra={
                        "event": "event_enriched",
                        "event_id": event.event_id,
                        "symbol": event.symbol,
                        "has_price_action": bool((event.metadata or {}).get("price_action")),
                        "history_matches": len((event.metadata or {}).get("history_match") or []),
                    },
                )

            await self.store.save_input_event(event)
            intent = await self.reasoner.generate(event)
            feature_snapshot = getattr(self.reasoner, "last_feature_snapshot", None)
            alpha_signal = getattr(self.reasoner, "last_alpha_signal", None)
            review_decision = None
            if self.reviewer is not None:
                review_decision = await self.reviewer.review(event, intent)
                if review_decision is not None:
                    await self.store.save_review_decision(review_decision)
                    logger.info(
                        "review decision completed",
                        extra={
                            "event": "review_decision_completed",
                            "event_id": event.event_id,
                            "intent_id": intent.intent_id,
                            "symbol": intent.symbol,
                            "review_status": review_decision.status.value,
                            "reviewer": review_decision.reviewer,
                        },
                    )
                    log_trade_event(
                        "review_decision_completed",
                        event_id=event.event_id,
                        intent_id=intent.intent_id,
                        symbol=intent.symbol,
                        review_status=review_decision.status.value,
                        reviewer=review_decision.reviewer,
                        rationale=review_decision.rationale,
                        metadata=review_decision.metadata,
                    )
                    log_decision_event(
                        "review_decision_completed",
                        event_id=event.event_id,
                        intent_id=intent.intent_id,
                        symbol=intent.symbol,
                        review_status=review_decision.status.value,
                        reviewer=review_decision.reviewer,
                        rationale=review_decision.rationale,
                        metadata=review_decision.metadata,
                    )
                    intent = self._apply_review_decision(intent, review_decision)
            if feature_snapshot is not None:
                await self.store.save_feature_snapshot(feature_snapshot)
            if alpha_signal is not None:
                await self.store.save_alpha_signal(alpha_signal)
            await self.store.save_trade_intent(intent)
            logger.info(
                "trade intent generated",
                extra={
                    "event": "trade_intent_generated",
                    "event_id": event.event_id,
                    "intent_id": intent.intent_id,
                    "symbol": intent.symbol,
                    "action": intent.action.value,
                    "confidence": intent.confidence,
                    "score": intent.score,
                    "strategy_id": intent.strategy_id,
                },
            )
            log_trade_event(
                "trade_intent_generated",
                event_id=event.event_id,
                intent_id=intent.intent_id,
                symbol=intent.symbol,
                action=intent.action.value,
                confidence=intent.confidence,
                score=intent.score,
                limit_price=intent.limit_price,
                quantity=intent.quantity,
                strategy_id=intent.strategy_id,
            )
            log_decision_event(
                "trade_intent_generated",
                event_id=event.event_id,
                intent_id=intent.intent_id,
                symbol=intent.symbol,
                action=intent.action.value,
                confidence=intent.confidence,
                score=intent.score,
                limit_price=intent.limit_price,
                quantity=intent.quantity,
                strategy_id=intent.strategy_id,
                rationale=intent.rationale,
                metadata=intent.metadata,
            )

            stats = await self.store.count_order_requests_for_day()
            risk_decision = await self.risk_engine.evaluate(intent, stats.order_requests, event=event)
            await self.store.save_risk_decision(risk_decision)
            logger.info(
                "risk decision completed",
                extra={
                    "event": "risk_decision_completed",
                    "event_id": event.event_id,
                    "intent_id": intent.intent_id,
                    "symbol": intent.symbol,
                    "risk_status": risk_decision.status.value,
                    "reasons": risk_decision.reasons,
                    "notional": risk_decision.notional,
                },
            )
            log_trade_event(
                "risk_decision_completed",
                event_id=event.event_id,
                intent_id=intent.intent_id,
                symbol=intent.symbol,
                risk_status=risk_decision.status.value,
                reasons=risk_decision.reasons,
                notional=risk_decision.notional,
                metadata=risk_decision.metadata,
            )
            log_decision_event(
                "risk_decision_completed",
                event_id=event.event_id,
                intent_id=intent.intent_id,
                symbol=intent.symbol,
                risk_status=risk_decision.status.value,
                reasons=risk_decision.reasons,
                notional=risk_decision.notional,
                metadata=risk_decision.metadata,
            )

            result = ProcessResult(
                event=event,
                feature_snapshot=feature_snapshot,
                alpha_signal=alpha_signal,
                intent=intent,
                review_decision=review_decision,
                risk_decision=risk_decision,
            )
            if risk_decision.status == RiskStatus.REJECT:
                elapsed_ms = round((perf_counter() - started_at) * 1000.0, 3)
                logger.info(
                    "event rejected by risk engine",
                    extra={
                        "event": "process_event_rejected",
                        "event_id": event.event_id,
                        "intent_id": intent.intent_id,
                        "symbol": intent.symbol,
                        "elapsed_ms": elapsed_ms,
                    },
                )
                log_trade_event(
                    "order_blocked",
                    event_id=event.event_id,
                    intent_id=intent.intent_id,
                    symbol=intent.symbol,
                    action=intent.action.value,
                    reasons=risk_decision.reasons,
                    elapsed_ms=elapsed_ms,
                )
                log_decision_event(
                    "order_blocked",
                    event_id=event.event_id,
                    intent_id=intent.intent_id,
                    symbol=intent.symbol,
                    action=intent.action.value,
                    reasons=risk_decision.reasons,
                    elapsed_ms=elapsed_ms,
                )
                return result

            order_request = OrderRequest(
                intent_id=intent.intent_id,
                broker=self.broker.name,
                symbol=intent.symbol,
                side=TradeAction.BUY if intent.action == TradeAction.BUY else TradeAction.SELL,
                quantity=intent.quantity,
                order_type=OrderType.LIMIT,
                price=intent.limit_price,
            )
            await self.store.save_order_request(order_request)
            result.order_request = order_request
            log_trade_event(
                "order_request_created",
                event_id=event.event_id,
                intent_id=intent.intent_id,
                request_id=order_request.request_id,
                broker=order_request.broker,
                symbol=order_request.symbol,
                side=order_request.side.value,
                quantity=order_request.quantity,
                price=order_request.price,
                order_type=order_request.order_type.value,
            )

            submitted = await self.broker.submit_order(order_request)
            await self.store.save_order_record(submitted)
            result.order_records.append(submitted)
            logger.info(
                "order submitted",
                extra={
                    "event": "order_submitted",
                    "event_id": event.event_id,
                    "intent_id": intent.intent_id,
                    "request_id": order_request.request_id,
                    "broker_order_id": submitted.broker_order_id,
                    "symbol": submitted.symbol,
                    "status": submitted.status.value,
                },
            )
            log_trade_event(
                "order_submitted",
                event_id=event.event_id,
                intent_id=intent.intent_id,
                request_id=order_request.request_id,
                broker_order_id=submitted.broker_order_id,
                symbol=submitted.symbol,
                status=submitted.status.value,
                filled_qty=submitted.filled_qty,
                avg_fill_price=submitted.avg_fill_price,
                message=submitted.message,
            )

            latest = await self.broker.get_order(submitted.broker_order_id)
            if latest and latest.record_id != submitted.record_id:
                await self.store.save_order_record(latest)
                result.order_records.append(latest)
                logger.info(
                    "order status updated",
                    extra={
                        "event": "order_status_updated",
                        "event_id": event.event_id,
                        "intent_id": intent.intent_id,
                        "request_id": order_request.request_id,
                        "broker_order_id": latest.broker_order_id,
                        "symbol": latest.symbol,
                        "status": latest.status.value,
                        "filled_qty": latest.filled_qty,
                        "avg_fill_price": latest.avg_fill_price,
                    },
                )
                log_trade_event(
                    "order_status_updated",
                    event_id=event.event_id,
                    intent_id=intent.intent_id,
                    request_id=order_request.request_id,
                    broker_order_id=latest.broker_order_id,
                    symbol=latest.symbol,
                    status=latest.status.value,
                    filled_qty=latest.filled_qty,
                    avg_fill_price=latest.avg_fill_price,
                    message=latest.message,
                )

            elapsed_ms = round((perf_counter() - started_at) * 1000.0, 3)
            logger.info(
                "event completed",
                extra={
                    "event": "process_event_completed",
                    "event_id": event.event_id,
                    "intent_id": intent.intent_id,
                    "symbol": intent.symbol,
                    "broker": self.broker.name,
                    "elapsed_ms": elapsed_ms,
                    "order_records": len(result.order_records),
                },
            )
            log_trade_event(
                "process_event_completed",
                event_id=event.event_id,
                intent_id=intent.intent_id,
                symbol=intent.symbol,
                broker=self.broker.name,
                elapsed_ms=elapsed_ms,
                order_records=[record.status.value for record in result.order_records],
            )
            log_decision_event(
                "process_event_completed",
                event_id=event.event_id,
                intent_id=intent.intent_id,
                symbol=intent.symbol,
                broker=self.broker.name,
                elapsed_ms=elapsed_ms,
                order_records=[record.status.value for record in result.order_records],
            )
            return result
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "processing event failed",
                extra={
                    "event": "process_event_failed",
                    "event_id": event.event_id,
                    "symbol": event.symbol,
                    "source": event.source,
                    "broker": self.broker.name,
                },
            )
            log_trade_event(
                "process_event_failed",
                event_id=event.event_id,
                symbol=event.symbol,
                source=event.source,
                broker=self.broker.name,
                error=str(exc),
            )
            raise

    @staticmethod
    def _apply_review_decision(intent, review_decision):
        if review_decision.status != ReviewStatus.REJECT:
            if review_decision.confidence_cap is None:
                return intent
            metadata = dict(intent.metadata or {})
            metadata["review_decision"] = review_decision.model_dump(mode="json")
            return intent.model_copy(
                update={
                    "confidence": min(intent.confidence, review_decision.confidence_cap),
                    "metadata": metadata,
                }
            )

        metadata = dict(intent.metadata or {})
        metadata["review_decision"] = review_decision.model_dump(mode="json")
        updated_action = review_decision.action_override or TradeAction.HOLD
        updated_confidence = (
            min(intent.confidence, review_decision.confidence_cap)
            if review_decision.confidence_cap is not None
            else min(intent.confidence, 0.25)
        )
        updated_rationale = f"{intent.rationale} | Review: {review_decision.rationale}".strip(" |")
        return intent.model_copy(
            update={
                "action": updated_action,
                "confidence": updated_confidence,
                "rationale": updated_rationale,
                "metadata": metadata,
            }
        )
