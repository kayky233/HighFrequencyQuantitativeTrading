from __future__ import annotations

from datetime import UTC, date, datetime
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


def utc_now() -> datetime:
    return datetime.now(UTC)


class EventType(str, Enum):
    NEWS = "news"
    PRICE = "price"
    MANUAL = "manual"


class TradeAction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class RegimeType(str, Enum):
    TREND = "TREND"
    MEAN_REVERSION = "MEAN_REVERSION"
    RISK_OFF = "RISK_OFF"


class RiskStatus(str, Enum):
    ALLOW = "ALLOW"
    REJECT = "REJECT"


class ReviewStatus(str, Enum):
    APPROVE = "APPROVE"
    REJECT = "REJECT"


class OrderType(str, Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"


class OrderStatus(str, Enum):
    SUBMITTED = "SUBMITTED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"


class InputEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: EventType = Field(default=EventType.NEWS)
    source: str = Field(default="manual")
    symbol: str = Field(default="US.AAPL")
    headline: str | None = None
    body: str | None = None
    sentiment: float = 0.0
    price: float | None = None
    quantity: float | None = None
    ts: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)


class MarketSnapshot(BaseModel):
    symbol: str
    last_price: float
    bid_price: float | None = None
    ask_price: float | None = None
    ts: datetime = Field(default_factory=utc_now)
    raw: dict[str, Any] = Field(default_factory=dict)


class PriceActionFeatures(BaseModel):
    symbol: str
    source: str = "yahoo_chart"
    interval: str = "1m"
    lookback_minutes: int = 30
    last_price: float | None = None
    previous_close: float | None = None
    return_5m_pct: float | None = None
    return_15m_pct: float | None = None
    return_30m_pct: float | None = None
    intraday_range_pct_30m: float | None = None
    volatility_1m_std_30m: float | None = None
    volume_ratio_5m_vs_30m: float | None = None
    ts: datetime = Field(default_factory=utc_now)
    raw: dict[str, Any] = Field(default_factory=dict)


class HistoricalEventMatch(BaseModel):
    event_id: str
    symbol: str
    similarity: float
    ts: datetime | None = None
    headline: str | None = None
    sentiment: float | None = None
    prior_action: TradeAction | None = None
    prior_confidence: float | None = None
    prior_order_status: OrderStatus | None = None
    rationale: str | None = None


class FeatureSnapshot(BaseModel):
    feature_id: str = Field(default_factory=lambda: str(uuid4()))
    event_id: str
    symbol: str
    feature_set: str = "event_factor_v1"
    sentiment_score: float = 0.0
    sentiment_momentum: float = 0.0
    momentum_5m: float | None = None
    momentum_15m: float | None = None
    momentum_30m: float | None = None
    volatility_30m: float | None = None
    intraday_range_30m: float | None = None
    volume_ratio_5m_vs_30m: float | None = None
    source_quality_score: float = 0.0
    freshness_score: float = 0.0
    diversity_score: float = 0.0
    news_count: int = 0
    social_count: int = 0
    history_bias: float = 0.0
    history_support: float = 0.0
    liquidity_score: float = 0.0
    event_density_score: float = 0.0
    last_price: float | None = None
    ts: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)


class AlphaSignal(BaseModel):
    signal_id: str = Field(default_factory=lambda: str(uuid4()))
    event_id: str
    symbol: str
    direction: TradeAction
    score: float
    confidence: float
    regime: RegimeType = RegimeType.TREND
    ranking_score: float | None = None
    rationale: str = ""
    ts: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)


class TradeIntent(BaseModel):
    intent_id: str = Field(default_factory=lambda: str(uuid4()))
    event_id: str
    symbol: str
    action: TradeAction
    quantity: float
    limit_price: float | None = None
    confidence: float = 0.5
    score: float | None = None
    regime: RegimeType | None = None
    rationale: str = ""
    strategy_id: str = "mock-news-v1"
    ts: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)


class RiskDecision(BaseModel):
    decision_id: str = Field(default_factory=lambda: str(uuid4()))
    intent_id: str
    status: RiskStatus
    reasons: list[str] = Field(default_factory=list)
    notional: float | None = None
    ts: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ReviewDecision(BaseModel):
    review_id: str = Field(default_factory=lambda: str(uuid4()))
    event_id: str
    intent_id: str
    status: ReviewStatus = ReviewStatus.APPROVE
    reviewer: str = "review-agent"
    action_override: TradeAction | None = None
    confidence_cap: float | None = None
    rationale: str = ""
    ts: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)


class OrderRequest(BaseModel):
    request_id: str = Field(default_factory=lambda: str(uuid4()))
    intent_id: str
    broker: str
    symbol: str
    side: TradeAction
    quantity: float
    order_type: OrderType = OrderType.LIMIT
    price: float | None = None
    trd_env: str = "SIMULATE"
    ts: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)


class OrderRecord(BaseModel):
    record_id: str = Field(default_factory=lambda: str(uuid4()))
    request_id: str
    broker: str
    broker_order_id: str
    symbol: str
    side: TradeAction
    quantity: float
    order_type: OrderType
    price: float | None = None
    status: OrderStatus = OrderStatus.UNKNOWN
    filled_qty: float = 0.0
    avg_fill_price: float | None = None
    message: str | None = None
    ts: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)


class AccountInfo(BaseModel):
    broker: str
    account_id: str
    account_name: str | None = None
    trd_env: str
    market: str | None = None
    security_firm: str | None = None
    raw: dict[str, Any] = Field(default_factory=dict)


class ProcessResult(BaseModel):
    event: InputEvent
    feature_snapshot: FeatureSnapshot | None = None
    alpha_signal: AlphaSignal | None = None
    intent: TradeIntent
    review_decision: ReviewDecision | None = None
    risk_decision: RiskDecision
    order_request: OrderRequest | None = None
    order_records: list[OrderRecord] = Field(default_factory=list)


class DailyStats(BaseModel):
    day: date
    order_requests: int = 0


class PositionSummary(BaseModel):
    symbol: str
    net_quantity: float = 0.0
    avg_cost: float = 0.0
    last_price: float | None = None
    market_value: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    total_pnl: float = 0.0


class PortfolioSummary(BaseModel):
    broker: str
    positions: list[PositionSummary] = Field(default_factory=list)
    filled_orders: int = 0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    total_pnl: float = 0.0
    cost_basis: float = 0.0
    gross_market_value: float = 0.0
    net_market_value: float = 0.0
    return_pct: float = 0.0
    winning_positions: int = 0
    losing_positions: int = 0
    ts: datetime = Field(default_factory=utc_now)


class StressRunSummary(BaseModel):
    broker: str
    symbols: list[str]
    events_requested: int
    events_processed: int
    buy_intents: int = 0
    sell_intents: int = 0
    hold_intents: int = 0
    risk_allowed: int = 0
    orders_filled: int = 0
    avg_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    portfolio: PortfolioSummary | None = None
    ts: datetime = Field(default_factory=utc_now)


class LatencyBudgetReport(BaseModel):
    budget_ms: float
    symbol_count: int
    use_llm: bool
    review_enabled: bool
    llm_mode: str | None = None
    elapsed_ms: float
    within_budget: bool
    selected_symbol: str | None = None
    candidate_count: int = 0
    breakdown: dict[str, Any] = Field(default_factory=dict)
    ts: datetime = Field(default_factory=utc_now)
