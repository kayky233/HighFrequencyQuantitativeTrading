from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic, perf_counter
from zoneinfo import ZoneInfo

import os

from fastapi import FastAPI, Header
from fastapi.responses import HTMLResponse, JSONResponse, Response
import uvicorn

from hfqt.agents import ReviewAgent
from hfqt.analytics import PortfolioAnalyzer
from hfqt.auth import AuthorizationRuntime
from hfqt.brokers.futu_sim import FutuSimBrokerAdapter
from hfqt.brokers.paper_local import LocalPaperBroker
from hfqt.brokers.usmart import USmartBrokerAdapter
from hfqt.config import AppConfig
from hfqt.config_catalog import build_config_catalog
from hfqt.engine import TradingEngine
from hfqt.enrichers import EventEnricher
from hfqt.marketdata import PublicPriceActionAdapter
from hfqt.marketdata.futu_quote import FutuQuoteAdapter
from hfqt.owner_control import OWNER_HEADER_NAME, OwnerControl, OwnerControlError
from hfqt.pipeline import QuantPipeline
from hfqt.portfolio import UniverseSelector
from hfqt.reasoners.llm import LLMReasoner
from hfqt.reasoners.mock import MockReasoner
from hfqt.risk.basic import BasicRiskEngine
from hfqt.runtime_logging import get_logger, read_recent_trade_logs, resolve_log_paths, setup_logging
from hfqt.schemas import InputEvent, LatencyBudgetReport, OrderRecord, OrderStatus, ReviewStatus, RiskStatus, StressRunSummary, TradeAction
from hfqt.sources.manual import ManualEventSource
from hfqt.sources.dummy_stream import DummyStreamSource
from hfqt.sources.network_intel import NetworkIntelSource
from hfqt.sources.replay import ReplayEventSource
from hfqt.store.sqlite_store import SQLiteAuditStore

logger = get_logger("app")
_SCAN_RESULT_CACHE: dict[str, dict] = {}


def _scan_cache_key(
    symbols: list[str],
    use_llm: bool,
    translate_to_zh: bool | None,
    llm_mode: str | None,
    query_template: str | None,
) -> str:
    payload = {
        "symbols": [symbol.upper() for symbol in symbols],
        "use_llm": bool(use_llm),
        "translate_to_zh": bool(translate_to_zh),
        "llm_mode": llm_mode or "",
        "query_template": query_template or "",
    }
    return json.dumps(payload, sort_keys=True, ensure_ascii=False)


def _clone_scan_result(payload: dict) -> dict:
    return json.loads(json.dumps(payload, ensure_ascii=False))


def resolve_web_root() -> Path:
    module_root = Path(__file__).resolve().parent
    bundled_root = Path(getattr(sys, "_MEIPASS", module_root))
    candidates = [
        module_root / "web",
        bundled_root / "hfqt" / "web",
        bundled_root / "web",
    ]
    for candidate in candidates:
        if (candidate / "index.html").exists():
            return candidate
    return module_root / "web"


WEB_ROOT = resolve_web_root()


def resolve_owner_token(explicit_token: str | None = None) -> str | None:
    return explicit_token or os.environ.get("HFQT_OWNER_SESSION_TOKEN")


def ensure_trade_access(config: AppConfig, owner_token: str | None = None) -> OwnerControl:
    control = OwnerControl(config)
    control.require_trade_access(resolve_owner_token(owner_token))
    return control


def with_translation_override(config: AppConfig, translate_to_zh: bool | None) -> AppConfig:
    if translate_to_zh is None:
        return config
    return config.model_copy(update={"translate_to_zh": translate_to_zh})


def with_llm_mode_override(config: AppConfig, llm_mode: str | None) -> AppConfig:
    if not llm_mode:
        return config
    return config.model_copy(update={"llm_mode": llm_mode.strip().lower()})


def with_runtime_overrides(
    config: AppConfig,
    translate_to_zh: bool | None = None,
    llm_mode: str | None = None,
) -> AppConfig:
    updated = with_translation_override(config, translate_to_zh)
    return with_llm_mode_override(updated, llm_mode)


def public_agent_settings(config: AppConfig, role: str) -> dict[str, str | bool | None]:
    settings = dict(config.agent_settings(role))
    api_key = settings.pop("api_key", None)
    settings["api_key_configured"] = bool(api_key)
    return settings


def parse_public_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None

    raw = str(value).strip()
    if not raw:
        return None

    candidates = [raw]
    if raw.endswith("Z"):
        candidates.append(raw[:-1] + "+00:00")
    if " " in raw and "T" not in raw:
        candidates.append(raw.replace(" ", "T"))

    for candidate in candidates:
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        else:
            parsed = parsed.astimezone(UTC)
        return parsed

    for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a %b %d %H:%M:%S %z %Y", "%Y-%m-%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(raw, fmt)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        else:
            parsed = parsed.astimezone(UTC)
        return parsed
    return None


def age_minutes_from_timestamp(value: str | None) -> float | None:
    parsed = parse_public_timestamp(value)
    if parsed is None:
        return None
    return max((datetime.now(UTC) - parsed).total_seconds() / 60.0, 0.0)


def extract_primary_item(metadata: dict | None) -> dict | None:
    items = ((metadata or {}).get("items") or [])
    for item in items:
        if isinstance(item, dict):
            return item
    return None


def _parse_time_hhmm(value: str) -> tuple[int, int] | None:
    try:
        parts = value.strip().split(":")
        if len(parts) != 2:
            return None
        hour = int(parts[0])
        minute = int(parts[1])
    except (ValueError, AttributeError):
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return hour, minute


def resolve_settlement_windows(config: AppConfig, now: datetime | None = None) -> list[dict]:
    now = now or datetime.now(UTC)
    active: list[dict] = []
    for window in config.settlement_windows:
        tz_name = str(window.get("tz") or "UTC")
        try:
            tz = ZoneInfo(tz_name)
        except Exception:  # noqa: BLE001
            tz = UTC
        local_now = now.astimezone(tz)
        days = window.get("days") or []
        if isinstance(days, str):
            days = [int(item) for item in days.split(",") if item.strip().isdigit()]
        if days and local_now.weekday() not in days:
            continue
        start = _parse_time_hhmm(str(window.get("start") or ""))
        end = _parse_time_hhmm(str(window.get("end") or ""))
        if not start or not end:
            continue
        start_dt = local_now.replace(hour=start[0], minute=start[1], second=0, microsecond=0)
        end_dt = local_now.replace(hour=end[0], minute=end[1], second=0, microsecond=0)
        if end_dt < start_dt:
            end_dt = end_dt + timedelta(days=1)
        if start_dt <= local_now <= end_dt:
            active.append(
                {
                    "name": window.get("name"),
                    "tz": tz_name,
                    "start": window.get("start"),
                    "end": window.get("end"),
                    "local_time": local_now.isoformat(),
                }
            )
    return active


def build_broker(config: AppConfig, broker_name: str):
    if broker_name == "futu_sim":
        return FutuSimBrokerAdapter(config)
    if broker_name == "usmart":
        return USmartBrokerAdapter(config)
    return LocalPaperBroker()


def build_engine(config: AppConfig, broker_name: str, use_llm: bool = False, use_review: bool | None = None) -> TradingEngine:
    setup_logging(config)
    store = SQLiteAuditStore(config.database_path)
    reasoner = LLMReasoner(config) if use_llm else MockReasoner(config)
    risk_engine = BasicRiskEngine(config)
    broker = build_broker(config, broker_name)
    event_enricher = EventEnricher(config, store)
    reviewer = ReviewAgent(config) if (config.review_agent_enabled and (use_llm if use_review is None else use_review)) else None
    return TradingEngine(
        config=config,
        reasoner=reasoner,
        risk_engine=risk_engine,
        broker=broker,
        store=store,
        event_enricher=event_enricher,
        reviewer=reviewer,
    )


async def run_replay(
    fixture: str,
    broker_name: str,
    use_llm: bool = False,
    llm_mode: str | None = None,
    owner_token: str | None = None,
) -> dict:
    config = with_runtime_overrides(AppConfig.from_env(), llm_mode=llm_mode)
    ensure_trade_access(config, owner_token=owner_token)
    engine = build_engine(config, broker_name=broker_name, use_llm=use_llm)
    source = ReplayEventSource()
    event = await source.load(fixture)
    await engine.initialize()
    try:
        result = await engine.process_event(event)
        return result.model_dump(mode="json")
    finally:
        await engine.close()


async def run_manual_event(
    payload: dict,
    broker_name: str,
    use_llm: bool = False,
    translate_to_zh: bool | None = None,
    llm_mode: str | None = None,
    owner_token: str | None = None,
) -> dict:
    config = with_runtime_overrides(AppConfig.from_env(), translate_to_zh=translate_to_zh, llm_mode=llm_mode)
    ensure_trade_access(config, owner_token=owner_token)
    engine = build_engine(config, broker_name=broker_name, use_llm=use_llm)
    source = ManualEventSource()
    event = await source.load(payload)
    await engine.initialize()
    try:
        result = await engine.process_event(event)
        return result.model_dump(mode="json")
    finally:
        await engine.close()


async def run_network_event(
    symbol: str,
    query: str | None,
    x_url: str | None,
    broker_name: str,
    use_llm: bool = False,
    translate_to_zh: bool | None = None,
    llm_mode: str | None = None,
    owner_token: str | None = None,
) -> dict:
    config = with_runtime_overrides(AppConfig.from_env(), translate_to_zh=translate_to_zh, llm_mode=llm_mode)
    ensure_trade_access(config, owner_token=owner_token)
    engine = build_engine(config, broker_name=broker_name, use_llm=use_llm)
    try:
        source = NetworkIntelSource(config, use_intel_agent=use_llm)
        try:
            event = await source.load(symbol=symbol, query=query, x_urls=[x_url] if x_url else None)
            await engine.initialize()
            result = await engine.process_event(event)
            return result.model_dump(mode="json")
        finally:
            await source.aclose()
    finally:
        await engine.close()


async def fetch_network_event(
    symbol: str,
    query: str | None,
    x_url: str | None = None,
    translate_to_zh: bool | None = None,
) -> dict:
    config = with_runtime_overrides(AppConfig.from_env(), translate_to_zh=translate_to_zh)
    source = NetworkIntelSource(config, use_intel_agent=True)
    try:
        event = await source.load(symbol=symbol, query=query, x_urls=[x_url] if x_url else None)
        return event.model_dump(mode="json")
    finally:
        await source.aclose()


async def run_healthcheck(broker_name: str) -> dict:
    config = AppConfig.from_env()
    setup_logging(config)
    broker = build_broker(config, broker_name)
    try:
        health = await broker.healthcheck()
        return {
            "status": "ok",
            "database_path": str(config.database_path),
            "default_broker": config.default_broker,
            "broker": health,
        }
    finally:
        await broker.close()


async def run_usmart_smoke(market: str = "us", include_trade_login: bool = False) -> dict:
    config = AppConfig.from_env()
    setup_logging(config)
    broker = USmartBrokerAdapter(config)
    try:
        return await broker.smoke(market=market, include_trade_login=include_trade_login)
    finally:
        await broker.close()


async def run_list_accounts(broker_name: str) -> dict:
    config = AppConfig.from_env()
    setup_logging(config)
    broker = build_broker(config, broker_name)
    await broker.connect()
    try:
        accounts = await broker.get_accounts()
        return {
            "broker": broker_name,
            "accounts": [account.model_dump(mode="json") for account in accounts],
        }
    finally:
        await broker.close()


async def run_list_orders(broker_name: str) -> dict:
    config = AppConfig.from_env()
    setup_logging(config)
    broker = build_broker(config, broker_name)
    await broker.connect()
    try:
        orders = await broker.list_orders()
        return {
            "broker": broker_name,
            "orders": [order.model_dump(mode="json") for order in orders],
        }
    finally:
        await broker.close()


async def run_quote(symbol: str) -> dict:
    config = AppConfig.from_env()
    setup_logging(config)
    adapter = FutuQuoteAdapter(config)
    snapshot = await adapter.get_snapshot(symbol)
    return {
        "broker": "futu_sim",
        "snapshot": snapshot.model_dump(mode="json"),
    }

async def run_scan_watchlist(
    symbols: list[str],
    use_llm: bool = False,
    translate_to_zh: bool | None = None,
    llm_mode: str | None = None,
    query_template: str | None = None,
) -> dict:
    config = with_runtime_overrides(AppConfig.from_env(), translate_to_zh=translate_to_zh, llm_mode=llm_mode)
    setup_logging(config)
    cache_key = _scan_cache_key(symbols, use_llm, translate_to_zh, llm_mode, query_template)
    cache_ttl = max(0, int(config.scan_cache_ttl_seconds))
    incremental_ttl = max(0, int(config.scan_incremental_ttl_seconds))
    cached_entry = _SCAN_RESULT_CACHE.get(cache_key)
    if cache_ttl > 0 and cached_entry:
        age_seconds = monotonic() - float(cached_entry.get("stored_at") or 0.0)
        if age_seconds <= cache_ttl:
            cached_payload = _clone_scan_result(cached_entry["payload"])
            cached_payload["cache"] = {
                "hit": True,
                "age_seconds": round(age_seconds, 3),
                "ttl_seconds": cache_ttl,
                "mode": "full",
            }
            return cached_payload
        if incremental_ttl > 0 and age_seconds <= cache_ttl + incremental_ttl:
            cached_payload = _clone_scan_result(cached_entry["payload"])
            cached_payload["cache"] = {
                "hit": True,
                "age_seconds": round(age_seconds, 3),
                "ttl_seconds": incremental_ttl,
                "mode": "incremental",
            }
            cached_payload["scan_mode"] = "incremental"
            return cached_payload

    store = SQLiteAuditStore(config.database_path)
    await store.initialize()
    reasoner = LLMReasoner(config) if use_llm else MockReasoner(config)
    reviewer = ReviewAgent(config) if (use_llm and config.review_agent_enabled) else None
    risk_engine = BasicRiskEngine(config)
    enricher = EventEnricher(config, store)
    source = NetworkIntelSource(config, use_intel_agent=use_llm)
    pipeline = QuantPipeline(config)
    symbols = UniverseSelector.filter_allowed(symbols, config.allowed_symbols) or symbols

    try:
        stats = await store.count_order_requests_for_day()
        semaphore = asyncio.Semaphore(max(1, min(len(symbols), config.scan_concurrency)))

        async def analyze_symbol(symbol: str) -> dict:
            async with semaphore:
                ticker = symbol.split(".")[-1]
                query = query_template.format(symbol=symbol, ticker=ticker) if query_template else None
                event = await source.load(symbol=symbol, query=query)
                event = await enricher.enrich(event)
                feature_snapshot, alpha_signal = pipeline.analyze(event)
                intent = await reasoner.generate(event)
                review_decision = await reviewer.review(event, intent) if reviewer is not None else None
                if review_decision is not None and review_decision.status == ReviewStatus.REJECT:
                    metadata = dict(intent.metadata or {})
                    metadata["review_decision"] = review_decision.model_dump(mode="json")
                    intent = intent.model_copy(
                        update={
                            "action": review_decision.action_override or TradeAction.HOLD,
                            "confidence": min(intent.confidence, review_decision.confidence_cap or 0.25),
                            "rationale": f"{intent.rationale} | Review: {review_decision.rationale}".strip(" |"),
                            "metadata": metadata,
                        }
                    )
                elif review_decision is not None and review_decision.confidence_cap is not None:
                    metadata = dict(intent.metadata or {})
                    metadata["review_decision"] = review_decision.model_dump(mode="json")
                    intent = intent.model_copy(
                        update={
                            "confidence": min(intent.confidence, review_decision.confidence_cap),
                            "metadata": metadata,
                        }
                    )
                signal_risk_decision = await risk_engine.evaluate(intent, 0, event=event)
                risk_decision = await risk_engine.evaluate(intent, stats.order_requests, event=event)
                if signal_risk_decision.status == RiskStatus.ALLOW and risk_decision.status == RiskStatus.REJECT:
                    logger.warning(
                        "execution risk rejected",
                        extra={
                            "event": "execution_risk_rejected",
                            "symbol": intent.symbol,
                            "intent_id": intent.intent_id,
                            "signal_status": signal_risk_decision.status.value,
                            "execution_status": risk_decision.status.value,
                            "execution_reasons": list(risk_decision.reasons or []),
                            "confidence": intent.confidence,
                            "quantity": intent.quantity,
                            "limit_price": intent.limit_price,
                        },
                    )
                alpha_meta = dict(alpha_signal.metadata or {})
                quantity = float(intent.quantity or 0.0)
                limit_price = (
                    float(intent.limit_price)
                    if intent.limit_price is not None
                    else float(event.price or feature_snapshot.last_price or 0.0)
                )
                estimated_notional = quantity * limit_price if quantity > 0 and limit_price > 0 else None
                candidate_bucket = "observe"
                setup_ready = (
                    intent.action == TradeAction.HOLD
                    and float(alpha_signal.score or 0.0) > 0.0
                    and float(intent.confidence or 0.0)
                    >= max(float(risk_decision.metadata.get("resolved_min_confidence") or config.min_confidence) - 0.03, 0.0)
                    and float(alpha_meta.get("price_confirmation") or 0.0) >= 0.65
                )
                if setup_ready:
                    candidate_bucket = "setup"
                if intent.action != TradeAction.HOLD:
                    candidate_bucket = "watch"
                if signal_risk_decision.status == RiskStatus.ALLOW and intent.action != TradeAction.HOLD:
                    candidate_bucket = "queued"
                if (
                    signal_risk_decision.status == RiskStatus.ALLOW
                    and risk_decision.status == RiskStatus.ALLOW
                    and intent.action != TradeAction.HOLD
                ):
                    candidate_bucket = "execute"
                primary_item = extract_primary_item(event.metadata)
                primary_published_at = primary_item.get("published_at") if isinstance(primary_item, dict) else None
                return {
                    "symbol": symbol,
                    "headline": event.headline,
                    "primary_link": next(
                        (
                            item.get("url")
                            for item in (event.metadata.get("items") or [])
                            if isinstance(item, dict) and item.get("url")
                        ),
                        None,
                    ),
                    "primary_published_at": primary_published_at,
                    "primary_age_minutes": age_minutes_from_timestamp(primary_published_at),
                    "event_id": event.event_id,
                    "source": event.source,
                    "regime": alpha_signal.regime.value,
                    "alpha_score": alpha_signal.score,
                    "ranking_score": alpha_signal.ranking_score,
                    "action": intent.action.value,
                    "confidence": intent.confidence,
                    "limit_price": intent.limit_price,
                    "risk_status": signal_risk_decision.status.value,
                    "risk_threshold": signal_risk_decision.metadata.get("resolved_min_confidence"),
                    "review_status": review_decision.status.value if review_decision is not None else None,
                    "review_model": (review_decision.metadata or {}).get("model") if review_decision is not None else None,
                    "risk_agent_status": ((signal_risk_decision.metadata or {}).get("risk_agent") or {}).get("status"),
                    "risk_agent_model": ((signal_risk_decision.metadata or {}).get("risk_agent") or {}).get("model"),
                    "risk_agent_rationale": ((signal_risk_decision.metadata or {}).get("risk_agent") or {}).get("rationale"),
                    "risk_reasons": list(signal_risk_decision.reasons or []),
                    "execution_status": risk_decision.status.value,
                    "execution_threshold": risk_decision.metadata.get("resolved_min_confidence"),
                    "execution_reasons": list(risk_decision.reasons or []),
                    "rationale": intent.rationale,
                    "model": intent.metadata.get("model"),
                    "model_profile": intent.metadata.get("model_profile"),
                    "strategy_id": intent.strategy_id,
                    "quantity": quantity,
                    "estimated_notional": estimated_notional,
                    "candidate_bucket": candidate_bucket,
                    "liquidity_score": feature_snapshot.liquidity_score,
                    "source_quality": (event.metadata.get("source_quality") or {}).get("overall_score"),
                    "news_count": (event.metadata.get("source_quality") or {}).get("news_count"),
                    "social_count": (event.metadata.get("source_quality") or {}).get("social_count"),
                    "freshness_score": (event.metadata.get("source_quality") or {}).get("freshness_score"),
                    "diversity_score": (event.metadata.get("source_quality") or {}).get("diversity_score"),
                    "last_price": (event.metadata.get("price_action") or {}).get("last_price"),
                    "momentum_15m_pct": (event.metadata.get("price_action") or {}).get("return_15m_pct"),
                    "momentum_30m_pct": (event.metadata.get("price_action") or {}).get("return_30m_pct"),
                    "intraday_range_pct": (event.metadata.get("price_action") or {}).get("intraday_range_pct_30m"),
                    "volume_ratio_5m": (event.metadata.get("price_action") or {}).get("volume_ratio_5m_vs_30m"),
                    "price_confirmation": alpha_meta.get("price_confirmation"),
                    "execution_quality": alpha_meta.get("execution_quality"),
                    "alignment": alpha_meta.get("alignment"),
                    "volume_support": alpha_meta.get("volume_support"),
                }

        candidates = []
        failed_symbols: dict[str, str] = {}
        gathered = await asyncio.gather(*(analyze_symbol(symbol) for symbol in symbols), return_exceptions=True)
        for symbol, outcome in zip(symbols, gathered, strict=False):
            if isinstance(outcome, Exception):
                failed_symbols[symbol] = str(outcome)
                logger.warning(
                    "watchlist analysis failed",
                    extra={
                        "event": "watchlist_analysis_failed",
                        "symbol": symbol,
                        "error": str(outcome),
                    },
                )
                continue
            candidates.append(outcome)
        for symbol in symbols:
            if any(candidate["symbol"] == symbol for candidate in candidates):
                continue
            failure_reason = failed_symbols.get(symbol) or "该标的本轮抓取或分析失败，已自动降级为跳过。"
            candidates.append(
                {
                    "symbol": symbol,
                    "headline": "抓取失败",
                    "regime": "RISK_OFF",
                    "alpha_score": -1.0,
                    "ranking_score": -1.0,
                    "action": "HOLD",
                    "confidence": 0.0,
                    "limit_price": None,
                    "risk_status": "REJECT",
                    "risk_threshold": None,
                    "review_status": None,
                    "review_model": None,
                    "risk_reasons": [failure_reason],
                    "execution_status": "REJECT",
                    "execution_threshold": None,
                    "execution_reasons": [failure_reason],
                    "rationale": failure_reason,
                    "model": None,
                    "model_profile": None,
                    "strategy_id": None,
                    "quantity": 0.0,
                    "estimated_notional": None,
                    "candidate_bucket": "observe",
                    "liquidity_score": 0.0,
                    "source_quality": 0.0,
                    "last_price": None,
                    "price_confirmation": 0.0,
                    "execution_quality": 0.0,
                    "alignment": 0.0,
                    "volume_support": 0.0,
                    "error": failure_reason,
                }
            )
        candidates = UniverseSelector.sort_candidates(candidates)
        shortlist = UniverseSelector.select_actionable(
            candidates,
            limit=max(1, min(len(candidates), min(config.watchlist_top_n, 3))),
        )
        selected = shortlist[0] if shortlist else (candidates[0] if candidates else None)
        result = {
            "symbols_considered": symbols,
            "candidates": UniverseSelector.select_top(candidates, limit=max(1, min(len(candidates), config.watchlist_top_n))),
            "shortlist": shortlist,
            "selected": selected,
        }
        result["cache"] = {
            "hit": False,
            "age_seconds": 0.0,
            "ttl_seconds": cache_ttl,
            "mode": "full",
        }
        if cache_ttl > 0 and len(candidates) > len(failed_symbols):
            _SCAN_RESULT_CACHE[cache_key] = {
                "stored_at": monotonic(),
                "payload": _clone_scan_result(result),
            }
        return result
    finally:
        close_reasoner = getattr(reasoner, "aclose", None)
        if callable(close_reasoner):
            result = close_reasoner()
            if inspect.isawaitable(result):
                await result
        close_reviewer = getattr(reviewer, "aclose", None)
        if callable(close_reviewer):
            result = close_reviewer()
            if inspect.isawaitable(result):
                await result
        await source.aclose()


def _iter_ranked_trade_candidates(scan: dict | None) -> list[dict]:
    if not scan:
        return []
    ranked: list[dict] = []
    seen: set[str] = set()
    for pool_name in ("shortlist", "candidates"):
        for candidate in scan.get(pool_name) or []:
            symbol = str(candidate.get("symbol") or "").upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            ranked.append(candidate)
    return ranked


async def _find_recent_symbol_order(
    store: SQLiteAuditStore,
    broker_name: str,
    symbol: str,
    cooldown_minutes: int,
) -> tuple[bool, str | None]:
    recent_orders = await store.load_recent_order_records(limit=24, broker=broker_name, symbol=symbol)
    if not recent_orders:
        return False, None

    cutoff = datetime.now(UTC) - timedelta(minutes=max(1, cooldown_minutes))
    active_statuses = {
        OrderStatus.SUBMITTED.value,
        OrderStatus.FILLED.value,
        OrderStatus.UNKNOWN.value,
    }
    for order in recent_orders:
        order_ts = order.ts if order.ts.tzinfo is not None else order.ts.replace(tzinfo=UTC)
        if order.status.value not in active_statuses:
            continue
        if order_ts >= cutoff:
            return True, f"{symbol} 在冷却窗口内已有 {order.status.value} 订单。"
    return False, None


async def run_watchlist_auto_trade(
    symbols: list[str],
    broker_name: str,
    use_llm: bool = False,
    translate_to_zh: bool | None = None,
    llm_mode: str | None = None,
    owner_token: str | None = None,
    scan: dict | None = None,
) -> dict:
    config = with_runtime_overrides(AppConfig.from_env(), translate_to_zh=translate_to_zh, llm_mode=llm_mode)
    setup_logging(config)
    ensure_trade_access(config, owner_token=owner_token)

    scan_result = scan or await run_scan_watchlist(
        symbols=symbols,
        use_llm=use_llm,
        translate_to_zh=translate_to_zh,
        llm_mode=llm_mode,
    )
    candidates = _iter_ranked_trade_candidates(scan_result)
    if not candidates:
        return {
            "enabled": True,
            "triggered": False,
            "reason": "当前没有可分析的候选标的。",
            "selected_symbol": None,
            "attempts": [],
        }

    store = SQLiteAuditStore(config.database_path)
    await store.initialize()
    attempts: list[dict] = []
    max_orders = max(1, int(config.auto_trade_max_orders_per_cycle))

    for candidate in candidates:
        symbol = str(candidate.get("symbol") or "").upper()
        bucket = str(candidate.get("candidate_bucket") or "")
        action = str(candidate.get("action") or "")
        execution_status = str(candidate.get("execution_status") or "")
        if bucket != "execute" or action == TradeAction.HOLD.value or execution_status != RiskStatus.ALLOW.value:
            continue

        blocked, reason = await _find_recent_symbol_order(
            store=store,
            broker_name=broker_name,
            symbol=symbol,
            cooldown_minutes=config.auto_trade_cooldown_minutes,
        )
        if blocked:
            attempts.append(
                {
                    "symbol": symbol,
                    "status": "cooldown_skip",
                    "reason": reason,
                    "candidate_bucket": bucket,
                    "action": action,
                }
            )
            continue

        result = await run_network_event(
            symbol=symbol,
            query=None,
            x_url=None,
            broker_name=broker_name,
            use_llm=use_llm,
            translate_to_zh=translate_to_zh,
            llm_mode=llm_mode,
            owner_token=owner_token,
        )
        attempts.append(
            {
                "symbol": symbol,
                "status": "submitted" if result.get("order_records") else "blocked",
                "reason": None if result.get("order_records") else "本轮自动执行进入引擎，但未形成订单。",
                "candidate_bucket": bucket,
                "action": action,
                "result": result,
            }
        )
        if len([item for item in attempts if item.get("status") == "submitted"]) >= max_orders:
            break

    if not attempts:
        selected = scan_result.get("selected") or {}
        return {
            "enabled": True,
            "triggered": False,
            "reason": "当前候选里没有进入 execute 桶的标的。",
            "selected_symbol": selected.get("symbol"),
            "selected_bucket": selected.get("candidate_bucket"),
            "selected_execution_status": selected.get("execution_status"),
            "selected_execution_reasons": selected.get("execution_reasons") or selected.get("risk_reasons") or [],
            "attempts": [],
        }

    submitted = [item for item in attempts if item.get("status") == "submitted"]
    latest = submitted[-1] if submitted else attempts[-1]
    return {
        "enabled": True,
        "triggered": bool(submitted),
        "reason": latest.get("reason"),
        "selected_symbol": latest.get("symbol"),
        "attempts": attempts,
        "executed_count": len(submitted),
    }


async def run_portfolio_summary(broker_name: str, symbols: list[str] | None = None) -> dict:
    config = AppConfig.from_env()
    setup_logging(config)
    store = SQLiteAuditStore(config.database_path)
    await store.initialize()
    analyzer = PortfolioAnalyzer(store, PublicPriceActionAdapter(config))
    summary = await analyzer.summarize(broker_name, symbols=symbols)
    return summary.model_dump(mode="json")


def _select_chart_symbols(
    portfolio: dict,
    scan: dict,
    fallback_symbols: list[str],
    limit: int = 4,
) -> list[str]:
    selected: list[str] = []

    def add_symbol(symbol: str | None) -> None:
        if not symbol or symbol in selected:
            return
        selected.append(symbol)

    positions = list(portfolio.get("positions") or [])
    profitable = sorted(
        (position for position in positions if float(position.get("total_pnl") or 0.0) > 0),
        key=lambda item: float(item.get("total_pnl") or 0.0),
        reverse=True,
    )
    for position in profitable:
        add_symbol(position.get("symbol"))
        if len(selected) >= limit:
            return selected

    ranked_positions = sorted(
        positions,
        key=lambda item: (
            -float(item.get("total_pnl") or 0.0),
            -abs(float(item.get("market_value") or 0.0)),
        ),
    )
    for position in ranked_positions:
        add_symbol(position.get("symbol"))
        if len(selected) >= limit:
            return selected

    selected_scan = scan.get("selected") or {}
    add_symbol(selected_scan.get("symbol"))
    for candidate in scan.get("shortlist") or []:
        add_symbol(candidate.get("symbol"))
        if len(selected) >= limit:
            return selected
    for candidate in scan.get("candidates") or []:
        add_symbol(candidate.get("symbol"))
        if len(selected) >= limit:
            return selected

    for symbol in fallback_symbols:
        add_symbol(symbol)
        if len(selected) >= limit:
            break

    return selected


async def run_chart_panels(
    symbols: list[str],
    broker_name: str,
    scan: dict | None = None,
    portfolio: dict | None = None,
    limit: int = 4,
    bar_limit: int = 90,
) -> dict:
    config = AppConfig.from_env()
    setup_logging(config)
    store = SQLiteAuditStore(config.database_path)
    await store.initialize()
    marketdata = PublicPriceActionAdapter(config)
    portfolio_data = portfolio or await run_portfolio_summary(broker_name=broker_name, symbols=symbols or None)
    scan_data = scan or {"selected": None, "shortlist": [], "candidates": []}
    target_symbols = _select_chart_symbols(portfolio_data, scan_data, symbols, limit=limit)
    filled_orders = await store.load_order_records(broker=broker_name, status=OrderStatus.FILLED.value)
    recent_order_requests = await store.load_recent_order_requests(limit=200, broker=broker_name)
    requests_by_id = {request.request_id: request for request in recent_order_requests}
    linked_intents = await store.load_trade_intents_by_ids(
        [request.intent_id for request in recent_order_requests if request.intent_id]
    )
    intents_by_id = {intent.intent_id: intent for intent in linked_intents}
    linked_events = await store.load_input_events_by_ids(
        [intent.event_id for intent in linked_intents if intent.event_id]
    )
    events_by_id = {event.event_id: event for event in linked_events}

    position_map = {
        position.get("symbol"): position
        for position in (portfolio_data.get("positions") or [])
        if position.get("symbol")
    }
    candidate_map = {
        candidate.get("symbol"): candidate
        for candidate in (scan_data.get("candidates") or [])
        if candidate.get("symbol")
    }
    selected_candidate = scan_data.get("selected") or {}

    async def build_panel(symbol: str) -> dict | None:
        chart_error: str | None = None
        try:
            bars = await marketdata.get_intraday_bars(symbol, bar_limit=bar_limit)
        except Exception as exc:  # noqa: BLE001
            chart_error = str(exc)
            logger.warning(
                "chart panel fetch failed",
                extra={"event": "chart_panel_fetch_failed", "symbol": symbol, "error": chart_error},
            )
            bars = []

        position = position_map.get(symbol) or {}
        signal = candidate_map.get(symbol)
        if signal is None and selected_candidate.get("symbol") == symbol:
            signal = selected_candidate

        trades = []
        for order in filled_orders:
            if order.symbol != symbol:
                continue
            trade_price = order.avg_fill_price or order.price
            if trade_price is None:
                continue
            order_request = requests_by_id.get(order.request_id)
            intent = intents_by_id.get(order_request.intent_id) if order_request is not None else None
            event = events_by_id.get(intent.event_id) if intent is not None else None
            price_action = (event.metadata or {}).get("price_action") if event is not None else {}
            primary_item = extract_primary_item(event.metadata if event is not None else None)
            primary_published_at = primary_item.get("published_at") if isinstance(primary_item, dict) else None
            primary_link = next(
                (
                    item.get("url")
                    for item in ((event.metadata or {}).get("items") or [])
                    if isinstance(item, dict) and item.get("url")
                ),
                None,
            ) if event is not None else None
            trades.append(
                {
                    "ts": order.ts.isoformat(),
                    "side": order.side.value,
                    "price": float(trade_price),
                    "quantity": float(order.filled_qty or order.quantity or 0.0),
                    "request_id": order.request_id,
                    "broker_order_id": order.broker_order_id,
                    "status": order.status.value,
                    "trigger_summary": (intent.rationale if intent is not None else None) or order.message,
                    "headline": event.headline if event is not None else None,
                    "news_url": primary_link,
                    "published_at": primary_published_at,
                    "age_minutes": age_minutes_from_timestamp(primary_published_at),
                    "momentum_15m_pct": price_action.get("return_15m_pct") if isinstance(price_action, dict) else None,
                    "momentum_30m_pct": price_action.get("return_30m_pct") if isinstance(price_action, dict) else None,
                }
            )
        trades = trades[-12:]

        return {
            "symbol": symbol,
            "priority_pnl": float(position.get("total_pnl") or 0.0),
            "position": position,
            "signal": signal,
            "bars": bars,
            "trades": trades,
            "chart_error": chart_error,
        }

    panels = [
        panel
        for panel in await asyncio.gather(*(build_panel(symbol) for symbol in target_symbols))
        if panel is not None
    ]
    panels.sort(key=lambda item: float(item.get("priority_pnl") or 0.0), reverse=True)
    return {
        "symbols": target_symbols,
        "panels": panels,
    }


async def run_live_snapshot(
    symbols: list[str],
    broker_name: str,
    use_llm: bool = False,
    translate_to_zh: bool | None = None,
    llm_mode: str | None = None,
    auto_trade: bool = False,
    owner_token: str | None = None,
) -> dict:
    config = with_runtime_overrides(AppConfig.from_env(), translate_to_zh=translate_to_zh, llm_mode=llm_mode)
    setup_logging(config)
    store = SQLiteAuditStore(config.database_path)
    await store.initialize()
    symbol_filter = symbols[0] if len(symbols) == 1 else None
    scan_error: str | None = None
    try:
        scan = await run_scan_watchlist(
            symbols=symbols,
            use_llm=use_llm,
            translate_to_zh=translate_to_zh,
            llm_mode=llm_mode,
        )
    except Exception as exc:  # noqa: BLE001
        scan = {
            "symbols_considered": symbols,
            "candidates": [],
            "shortlist": [],
            "selected": None,
        }
        scan_error = str(exc)

    auto_trade_result: dict | None = None
    if auto_trade:
        auto_trade_result = await run_watchlist_auto_trade(
            symbols=symbols,
            broker_name=broker_name,
            use_llm=use_llm,
            translate_to_zh=translate_to_zh,
            llm_mode=llm_mode,
            owner_token=owner_token,
            scan=scan,
        )

    settlement_windows = resolve_settlement_windows(config)

    portfolio, recent_events, recent_intents, recent_reviews, recent_order_requests, recent_orders = await asyncio.gather(
        run_portfolio_summary(broker_name=broker_name, symbols=symbols or None),
        store.load_recent_input_events(limit=16, symbol=symbol_filter),
        store.load_recent_trade_intents(limit=16, symbol=symbol_filter),
        store.load_recent_review_decisions(limit=12, symbol=symbol_filter),
        store.load_recent_order_requests(limit=12, broker=broker_name, symbol=symbol_filter),
        store.load_recent_order_records(limit=8, broker=broker_name, symbol=symbol_filter),
    )
    order_intent_ids = [request.intent_id for request in recent_order_requests if request.intent_id]
    linked_intents = await store.load_trade_intents_by_ids(order_intent_ids)
    merged_intents = {intent.intent_id: intent for intent in [*recent_intents, *linked_intents]}
    recent_intents = sorted(merged_intents.values(), key=lambda item: item.ts, reverse=True)

    linked_event_ids = [intent.event_id for intent in recent_intents if intent.event_id]
    linked_events = await store.load_input_events_by_ids(linked_event_ids)
    merged_events = {event.event_id: event for event in [*recent_events, *linked_events]}
    recent_events = sorted(merged_events.values(), key=lambda item: item.ts, reverse=True)
    chart_panels = await run_chart_panels(
        symbols=symbols,
        broker_name=broker_name,
        scan=scan,
        portfolio=portfolio,
    )
    broker = build_broker(config, broker_name)
    try:
        await broker.connect()
        refreshed_orders: list[OrderRecord] = []
        refreshed_by_id: dict[str, OrderRecord] = {}
        for order in recent_orders:
            latest = await broker.get_order(order.broker_order_id)
            if latest and latest.record_id != order.record_id:
                await store.save_order_record(latest)
                refreshed_orders.append(latest)
                refreshed_by_id[order.broker_order_id] = latest
        if refreshed_by_id:
            recent_orders = [
                refreshed_by_id.get(order.broker_order_id, order)
                for order in recent_orders
            ]
        health = await broker.healthcheck()
    except Exception as exc:  # noqa: BLE001
        health = {
            "broker": broker_name,
            "connected": False,
            "mode": "UNKNOWN",
            "error": str(exc),
        }
    finally:
        await broker.close()
    return {
        "ts": datetime.now(UTC).isoformat(),
        "symbols": symbols,
        "broker": broker_name,
        "health": health,
        "errors": {"scan": scan_error},
        "auto_trade": auto_trade_result
        or {
            "enabled": bool(config.auto_trade_enabled),
            "triggered": False,
            "reason": "自动执行当前关闭。",
            "attempts": [],
        },
        "settlement_windows": settlement_windows,
        "scan": scan,
        "portfolio": portfolio,
        "chart_panels": chart_panels,
        "recent_events": [event.model_dump(mode="json") for event in recent_events],
        "recent_intents": [intent.model_dump(mode="json") for intent in recent_intents],
        "recent_reviews": [review.model_dump(mode="json") for review in recent_reviews],
        "recent_order_requests": [request.model_dump(mode="json") for request in recent_order_requests],
        "recent_orders": [order.model_dump(mode="json") for order in recent_orders],
    }


async def run_latency_budget_test(
    symbols: list[str],
    use_llm: bool = False,
    llm_mode: str | None = None,
    translate_to_zh: bool | None = None,
    budget_ms: int | None = None,
) -> dict:
    config = with_runtime_overrides(AppConfig.from_env(), translate_to_zh=translate_to_zh, llm_mode=llm_mode)
    started = perf_counter()
    scan = await run_scan_watchlist(
        symbols=symbols,
        use_llm=use_llm,
        translate_to_zh=translate_to_zh,
        llm_mode=llm_mode,
    )
    elapsed_ms = round((perf_counter() - started) * 1000.0, 3)
    report = LatencyBudgetReport(
        budget_ms=float(budget_ms or config.latency_budget_ms),
        symbol_count=len(symbols),
        use_llm=use_llm,
        review_enabled=bool(use_llm and config.review_agent_enabled),
        llm_mode=llm_mode or config.llm_mode,
        elapsed_ms=elapsed_ms,
        within_budget=elapsed_ms <= float(budget_ms or config.latency_budget_ms),
        selected_symbol=(scan.get("selected") or {}).get("symbol"),
        candidate_count=len(scan.get("candidates") or []),
        breakdown={
            "symbols": symbols,
            "selected": scan.get("selected"),
        },
    )
    return report.model_dump(mode="json")


async def run_dummy_stream(
    symbols: list[str],
    events: int,
    concurrency: int,
    broker_name: str,
    use_llm: bool = False,
    interval_ms: int = 0,
    llm_mode: str | None = None,
    owner_token: str | None = None,
) -> dict:
    config = with_runtime_overrides(AppConfig.from_env(), llm_mode=llm_mode)
    setup_logging(config)
    ensure_trade_access(config, owner_token=owner_token)
    store = SQLiteAuditStore(config.database_path)
    engine = build_engine(config, broker_name=broker_name, use_llm=use_llm)
    source = DummyStreamSource(config)
    analyzer = PortfolioAnalyzer(store, PublicPriceActionAdapter(config))
    await source.prime(symbols)
    await engine.initialize()
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def worker(index: int, symbol: str):
        async with semaphore:
            event = await source.next_event(symbol, index)
            started = perf_counter()
            result = await engine.process_event(event)
            latency_ms = (perf_counter() - started) * 1000.0
            return result, latency_ms

    try:
        tasks = []
        for index in range(events):
            symbol = symbols[index % len(symbols)]
            tasks.append(asyncio.create_task(worker(index, symbol)))
            if interval_ms > 0:
                await asyncio.sleep(interval_ms / 1000.0)

        completed = await asyncio.gather(*tasks)
        latencies = [latency for _, latency in completed]
        p95_latency = 0.0
        if latencies:
            ordered = sorted(latencies)
            p95_index = min(len(ordered) - 1, max(0, int(len(ordered) * 0.95) - 1))
            p95_latency = ordered[p95_index]

        summary = StressRunSummary(
            broker=broker_name,
            symbols=symbols,
            events_requested=events,
            events_processed=len(completed),
            buy_intents=sum(1 for result, _ in completed if result.intent.action == TradeAction.BUY),
            sell_intents=sum(1 for result, _ in completed if result.intent.action == TradeAction.SELL),
            hold_intents=sum(1 for result, _ in completed if result.intent.action == TradeAction.HOLD),
            risk_allowed=sum(1 for result, _ in completed if result.risk_decision.status.value == "ALLOW"),
            orders_filled=sum(
                1
                for result, _ in completed
                for order in result.order_records
                if order.status.value == "FILLED"
            ),
            avg_latency_ms=(sum(latencies) / len(latencies)) if latencies else 0.0,
            p95_latency_ms=p95_latency,
            portfolio=await analyzer.summarize(broker_name, symbols=symbols),
        )
        return summary.model_dump(mode="json")
    finally:
        await engine.close()


async def run_trade_log(limit: int = 30) -> dict:
    config = AppConfig.from_env()
    setup_logging(config)
    return {
        "log_paths": {key: str(value) for key, value in resolve_log_paths(config).items()},
        "items": read_recent_trade_logs(config, limit=limit),
    }


async def run_owner_status() -> dict:
    config = AppConfig.from_env()
    setup_logging(config)
    return {"owner_control": OwnerControl(config).get_status()}


async def run_auth_status() -> dict:
    config = AppConfig.from_env()
    setup_logging(config)
    return {"authorization": AuthorizationRuntime(config).status().model_dump(mode="json")}


async def run_owner_lock(locked: bool, note: str | None = None, owner_token: str | None = None) -> dict:
    config = AppConfig.from_env()
    setup_logging(config)
    control = OwnerControl(config)
    resolved_token = resolve_owner_token(owner_token)
    state = control.lock_trading(resolved_token, note=note) if locked else control.unlock_trading(resolved_token, note=note)
    return {
        "owner_control": control.get_status(),
        "state": state.model_dump(mode="json"),
    }


def create_fastapi_app() -> FastAPI:
    config = AppConfig.from_env()
    setup_logging(config)
    app = FastAPI(title="HFQT Ignite API")

    @app.exception_handler(OwnerControlError)
    async def owner_control_exception_handler(_, exc: OwnerControlError):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return (WEB_ROOT / "index.html").read_text(encoding="utf-8")

    @app.get("/favicon.ico")
    async def favicon() -> Response:
        return Response(status_code=204)

    @app.get("/demo/config")
    async def demo_config() -> dict:
        config = AppConfig.from_env()
        owner_control = OwnerControl(config)
        auth_status = AuthorizationRuntime(config).status()
        return {
            "app_name": "HFQT Ignite Demo",
            "default_broker": config.default_broker,
            "database_path": str(config.database_path),
            "llm_ready": bool(config.llm_base_url and (config.llm_model_primary or config.llm_model)),
            "llm_provider": config.llm_provider or "mock",
            "llm_model": config.llm_model,
            "llm_mode": config.llm_mode,
            "llm_model_primary": config.llm_model_primary,
            "llm_model_fallback": config.llm_model_fallback,
            "llm_base_url": config.llm_base_url,
            "translate_to_zh": config.translate_to_zh,
            "allowed_symbols": config.allowed_symbols,
            "watchlist_top_n": config.watchlist_top_n,
            "auto_trade_enabled": config.auto_trade_enabled,
            "auto_trade_cooldown_minutes": config.auto_trade_cooldown_minutes,
            "auto_trade_max_orders_per_cycle": config.auto_trade_max_orders_per_cycle,
            "latency_budget_ms": config.latency_budget_ms,
            "agent_stack": {
                "intel": public_agent_settings(config, "intel"),
                "analysis": public_agent_settings(config, "analysis"),
                "review": public_agent_settings(config, "review"),
                "risk": public_agent_settings(config, "risk"),
            },
            "authorization": auth_status.model_dump(mode="json"),
            "auth_settings": config.auth_settings_public(),
            "owner_control": owner_control.get_status(),
            "sample_event": {
                "event_type": "news",
                "source": "demo-ui",
                "symbol": "US.IBIT",
                "headline": "Bitcoin ETF inflows accelerate as macro risk appetite improves",
                "body": "Fresh ETF flow data and crypto market commentary point to renewed spot BTC demand, improving near-term sentiment for IBIT.",
                "sentiment": 0.72,
                "price": 52.4,
                "quantity": 1,
                "metadata": {"preset": "sample"},
            },
        }

    @app.get("/demo/config-catalog")
    async def demo_config_catalog() -> dict:
        config = AppConfig.from_env()
        return build_config_catalog(config)

    @app.get("/demo/fetch-event")
    async def demo_fetch_event(
        symbol: str = "US.IBIT",
        query: str | None = None,
        x_url: str | None = None,
        translate: bool | None = None,
    ) -> dict:
        return await fetch_network_event(symbol=symbol, query=query, x_url=x_url, translate_to_zh=translate)

    @app.get("/demo/fetch-and-analyze")
    async def demo_fetch_and_analyze(
        symbol: str = "US.IBIT",
        query: str | None = None,
        x_url: str | None = None,
        broker: str | None = None,
        llm: bool = True,
        llm_mode: str | None = None,
        translate: bool | None = None,
        x_hfqt_owner_token: str | None = Header(default=None, alias=OWNER_HEADER_NAME),
    ) -> dict:
        config = AppConfig.from_env()
        return await run_network_event(
            symbol=symbol,
            query=query,
            x_url=x_url,
            broker_name=broker or config.default_broker,
            use_llm=llm,
            translate_to_zh=translate,
            llm_mode=llm_mode,
            owner_token=x_hfqt_owner_token,
        )

    @app.get("/health")
    async def health() -> dict:
        config = AppConfig.from_env()
        engine = build_engine(config, broker_name=config.default_broker)
        try:
            broker_health = await engine.broker.healthcheck()
        finally:
            await engine.close()
        return {"status": "ok", "broker": broker_health, "database_path": str(config.database_path)}

    @app.get("/demo/portfolio")
    async def demo_portfolio(broker: str = "local_paper", symbols: str = "") -> dict:
        symbol_list = [item.strip() for item in symbols.split(",") if item.strip()]
        return await run_portfolio_summary(broker, symbols=symbol_list or None)

    @app.get("/demo/chart-panels")
    async def demo_chart_panels(
        symbols: str = "US.IBIT,US.MSTR,US.COIN",
        broker: str = "local_paper",
    ) -> dict:
        symbol_list = [item.strip() for item in symbols.split(",") if item.strip()]
        return await run_chart_panels(symbols=symbol_list or ["US.IBIT"], broker_name=broker)

    @app.get("/demo/scan-watchlist")
    async def demo_scan_watchlist(
        symbols: str = "US.IBIT,US.MSTR,US.COIN",
        llm: bool = False,
        llm_mode: str | None = None,
        translate: bool | None = None,
    ) -> dict:
        symbol_list = [item.strip() for item in symbols.split(",") if item.strip()]
        return await run_scan_watchlist(symbol_list, use_llm=llm, translate_to_zh=translate, llm_mode=llm_mode)

    @app.get("/demo/live-snapshot")
    async def demo_live_snapshot(
        symbols: str = "US.IBIT,US.MSTR,US.COIN",
        broker: str = "local_paper",
        llm: bool = False,
        llm_mode: str | None = None,
        translate: bool | None = None,
        auto_trade: bool | None = None,
        x_hfqt_owner_token: str | None = Header(default=None, alias=OWNER_HEADER_NAME),
    ) -> dict:
        config = with_runtime_overrides(AppConfig.from_env(), translate_to_zh=translate, llm_mode=llm_mode)
        symbol_list = [item.strip() for item in symbols.split(",") if item.strip()]
        return await run_live_snapshot(
            symbols=symbol_list or ["US.IBIT"],
            broker_name=broker,
            use_llm=llm,
            translate_to_zh=translate,
            llm_mode=llm_mode,
            auto_trade=config.auto_trade_enabled if auto_trade is None else auto_trade,
            owner_token=x_hfqt_owner_token,
        )

    @app.get("/demo/latency-budget")
    async def demo_latency_budget(
        symbols: str = "US.IBIT,US.MSTR,US.COIN",
        llm: bool = False,
        llm_mode: str | None = None,
        translate: bool | None = None,
        budget_ms: int | None = None,
    ) -> dict:
        symbol_list = [item.strip() for item in symbols.split(",") if item.strip()]
        return await run_latency_budget_test(
            symbols=symbol_list or ["US.IBIT"],
            use_llm=llm,
            llm_mode=llm_mode,
            translate_to_zh=translate,
            budget_ms=budget_ms,
        )

    @app.get("/demo/trade-log")
    async def demo_trade_log(limit: int = 30) -> dict:
        return await run_trade_log(limit=limit)

    @app.get("/demo/auth-status")
    async def demo_auth_status() -> dict:
        return await run_auth_status()

    @app.get("/owner/status")
    async def owner_status() -> dict:
        return await run_owner_status()

    @app.post("/owner/lock")
    async def owner_lock(
        payload: dict | None = None,
        x_hfqt_owner_token: str | None = Header(default=None, alias=OWNER_HEADER_NAME),
    ) -> dict:
        return await run_owner_lock(True, note=(payload or {}).get("note"), owner_token=x_hfqt_owner_token)

    @app.post("/owner/unlock")
    async def owner_unlock(
        payload: dict | None = None,
        x_hfqt_owner_token: str | None = Header(default=None, alias=OWNER_HEADER_NAME),
    ) -> dict:
        return await run_owner_lock(False, note=(payload or {}).get("note"), owner_token=x_hfqt_owner_token)

    @app.post("/signals/manual")
    async def manual_signal(
        event: InputEvent,
        broker: str | None = None,
        llm: bool = False,
        llm_mode: str | None = None,
        translate: bool | None = None,
        x_hfqt_owner_token: str | None = Header(default=None, alias=OWNER_HEADER_NAME),
    ) -> dict:
        config = with_runtime_overrides(AppConfig.from_env(), translate_to_zh=translate, llm_mode=llm_mode)
        return await run_manual_event(
            event.model_dump(mode="json"),
            broker_name=broker or config.default_broker,
            use_llm=llm,
            translate_to_zh=translate,
            llm_mode=llm_mode,
            owner_token=x_hfqt_owner_token,
        )

    return app


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="HFQT ignition CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    health = sub.add_parser("healthcheck", help="Check broker reachability and local database path.")
    health.add_argument("--broker", default="local_paper", choices=["local_paper", "futu_sim", "usmart"])

    usmart_smoke = sub.add_parser("usmart-smoke", help="Run a minimal uSmart read-only smoke flow.")
    usmart_smoke.add_argument("--market", default="us")
    usmart_smoke.add_argument("--trade-login", action="store_true")

    accounts = sub.add_parser("list-accounts", help="List broker accounts.")
    accounts.add_argument("--broker", default="local_paper", choices=["local_paper", "futu_sim", "usmart"])

    orders = sub.add_parser("list-orders", help="List broker orders.")
    orders.add_argument("--broker", default="local_paper", choices=["local_paper", "futu_sim", "usmart"])

    quote = sub.add_parser("quote", help="Get one market snapshot through Futu OpenD.")
    quote.add_argument("--symbol", default="US.AAPL")

    scan = sub.add_parser("scan-watchlist", help="Analyze a watchlist and rank current trade targets.")
    scan.add_argument("--symbols", default="")
    scan.add_argument("--llm", action="store_true")
    scan.add_argument("--llm-mode", choices=["primary", "fallback", "auto"], default=None)
    scan.add_argument("--translate", action="store_true")
    scan.add_argument("--query-template", default="")

    portfolio = sub.add_parser("portfolio-summary", help="Summarize filled orders and PnL for one broker.")
    portfolio.add_argument("--broker", default="local_paper", choices=["local_paper", "futu_sim", "usmart"])
    portfolio.add_argument("--symbols", default="")

    latency = sub.add_parser("latency-budget", help="Measure one full scan cycle against the configured latency budget.")
    latency.add_argument("--symbols", default="")
    latency.add_argument("--llm", action="store_true")
    latency.add_argument("--llm-mode", choices=["primary", "fallback", "auto"], default=None)
    latency.add_argument("--translate", action="store_true")
    latency.add_argument("--budget-ms", type=int, default=None)

    fetch_network = sub.add_parser("fetch-network-event", help="Fetch one network event using xreach and Google News.")
    fetch_network.add_argument("--symbol", default="US.IBIT")
    fetch_network.add_argument("--query")
    fetch_network.add_argument("--translate", action="store_true", help="Translate fetched intel to Chinese.")
    fetch_network.add_argument("--x-url", default=None)

    run_network = sub.add_parser("run-network-event", help="Fetch network intel and run the full pipeline.")
    run_network.add_argument("--symbol", default="US.IBIT")
    run_network.add_argument("--query")
    run_network.add_argument("--broker", default="local_paper", choices=["local_paper", "futu_sim", "usmart"])
    run_network.add_argument("--llm", action="store_true")
    run_network.add_argument("--llm-mode", choices=["primary", "fallback", "auto"], default=None)
    run_network.add_argument("--translate", action="store_true", help="Translate fetched intel and rationale to Chinese.")
    run_network.add_argument("--x-url", default=None)
    run_network.add_argument("--owner-token", default=None)

    replay = sub.add_parser("run-replay", help="Run one replay fixture through the full pipeline.")
    replay.add_argument("--fixture", required=True)
    replay.add_argument("--broker", default="local_paper", choices=["local_paper", "futu_sim", "usmart"])
    replay.add_argument("--llm", action="store_true")
    replay.add_argument("--llm-mode", choices=["primary", "fallback", "auto"], default=None)
    replay.add_argument("--owner-token", default=None)

    manual = sub.add_parser("run-manual-event", help="Run one manual event through the full pipeline.")
    manual.add_argument("--symbol", default="US.IBIT")
    manual.add_argument("--sentiment", type=float, default=0.8)
    manual.add_argument("--price", type=float, default=None)
    manual.add_argument("--qty", type=float, default=1.0)
    manual.add_argument("--headline", default="Manual ignition event")
    manual.add_argument("--broker", default="local_paper", choices=["local_paper", "futu_sim", "usmart"])
    manual.add_argument("--llm", action="store_true")
    manual.add_argument("--llm-mode", choices=["primary", "fallback", "auto"], default=None)
    manual.add_argument("--owner-token", default=None)

    dummy = sub.add_parser("run-dummy-stream", help="Push a continuous synthetic stream through the engine.")
    dummy.add_argument("--symbols", default="")
    dummy.add_argument("--events", type=int, default=30)
    dummy.add_argument("--concurrency", type=int, default=4)
    dummy.add_argument("--interval-ms", type=int, default=50)
    dummy.add_argument("--broker", default="local_paper", choices=["local_paper", "futu_sim", "usmart"])
    dummy.add_argument("--llm", action="store_true")
    dummy.add_argument("--llm-mode", choices=["primary", "fallback", "auto"], default=None)
    dummy.add_argument("--owner-token", default=None)

    trade_log = sub.add_parser("trade-log", help="Show recent structured trade log entries.")
    trade_log.add_argument("--limit", type=int, default=30)

    sub.add_parser("auth-status", help="Show current authorization runtime status.")

    sub.add_parser("owner-status", help="Show current owner control status.")

    owner_lock = sub.add_parser("owner-lock", help="Lock trading until owner unlocks it.")
    owner_lock.add_argument("--note", default="Locked from CLI")
    owner_lock.add_argument("--owner-token", default=None)

    owner_unlock = sub.add_parser("owner-unlock", help="Unlock trading.")
    owner_unlock.add_argument("--note", default="Unlocked from CLI")
    owner_unlock.add_argument("--owner-token", default=None)

    serve = sub.add_parser("serve-api", help="Run the FastAPI service.")
    serve.add_argument("--host", default=AppConfig.from_env().api_host)
    serve.add_argument("--port", type=int, default=AppConfig.from_env().api_port)
    serve.add_argument("--reload", action="store_true")

    return parser


async def _main_async(args: argparse.Namespace) -> dict:
    if args.command == "healthcheck":
        return await run_healthcheck(broker_name=args.broker)
    if args.command == "usmart-smoke":
        return await run_usmart_smoke(market=args.market, include_trade_login=args.trade_login)
    if args.command == "list-accounts":
        return await run_list_accounts(broker_name=args.broker)
    if args.command == "list-orders":
        return await run_list_orders(broker_name=args.broker)
    if args.command == "quote":
        return await run_quote(symbol=args.symbol)
    if args.command == "scan-watchlist":
        config = AppConfig.from_env()
        symbols = [item.strip() for item in args.symbols.split(",") if item.strip()] or config.allowed_symbols
        return await run_scan_watchlist(
            symbols=symbols,
            use_llm=args.llm,
            llm_mode=args.llm_mode,
            translate_to_zh=args.translate,
            query_template=args.query_template or None,
        )
    if args.command == "portfolio-summary":
        symbols = [item.strip() for item in args.symbols.split(",") if item.strip()]
        return await run_portfolio_summary(broker_name=args.broker, symbols=symbols or None)
    if args.command == "latency-budget":
        config = AppConfig.from_env()
        symbols = [item.strip() for item in args.symbols.split(",") if item.strip()] or config.allowed_symbols[:3]
        return await run_latency_budget_test(
            symbols=symbols,
            use_llm=args.llm,
            llm_mode=args.llm_mode,
            translate_to_zh=args.translate,
            budget_ms=args.budget_ms,
        )
    if args.command == "fetch-network-event":
        return await fetch_network_event(
            symbol=args.symbol,
            query=args.query,
            x_url=args.x_url,
            translate_to_zh=args.translate,
        )
    if args.command == "run-network-event":
        return await run_network_event(
            symbol=args.symbol,
            query=args.query,
            x_url=args.x_url,
            broker_name=args.broker,
            use_llm=args.llm,
            llm_mode=args.llm_mode,
            translate_to_zh=args.translate,
            owner_token=args.owner_token,
        )
    if args.command == "run-replay":
        return await run_replay(
            args.fixture,
            broker_name=args.broker,
            use_llm=args.llm,
            llm_mode=args.llm_mode,
            owner_token=args.owner_token,
        )
    if args.command == "run-dummy-stream":
        config = AppConfig.from_env()
        symbols = [item.strip() for item in args.symbols.split(",") if item.strip()] or config.allowed_symbols
        return await run_dummy_stream(
            symbols=symbols,
            events=args.events,
            concurrency=args.concurrency,
            broker_name=args.broker,
            use_llm=args.llm,
            llm_mode=args.llm_mode,
            interval_ms=args.interval_ms,
            owner_token=args.owner_token,
        )
    if args.command == "trade-log":
        return await run_trade_log(limit=args.limit)
    if args.command == "auth-status":
        return await run_auth_status()
    if args.command == "owner-status":
        return await run_owner_status()
    if args.command == "owner-lock":
        return await run_owner_lock(True, note=args.note, owner_token=args.owner_token)
    if args.command == "owner-unlock":
        return await run_owner_lock(False, note=args.note, owner_token=args.owner_token)

    payload = {
        "event_type": "manual",
        "source": "cli",
        "symbol": args.symbol,
        "headline": args.headline,
        "sentiment": args.sentiment,
        "price": args.price,
        "quantity": args.qty,
        "metadata": {"trigger": "cli"},
    }
    return await run_manual_event(
        payload,
        broker_name=args.broker,
        use_llm=args.llm,
        llm_mode=args.llm_mode,
        owner_token=args.owner_token,
    )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    setup_logging(AppConfig.from_env())
    if args.command == "serve-api":
        uvicorn.run(app, host=args.host, port=args.port, reload=args.reload)
        return
    try:
        result = asyncio.run(_main_async(args))
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception:  # noqa: BLE001
        logger.exception("CLI command failed", extra={"event": "cli_command_failed", "command": args.command})
        raise


app = create_fastapi_app()


if __name__ == "__main__":
    main()
