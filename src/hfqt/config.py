from __future__ import annotations

import json
import os
from pathlib import Path

from pydantic import BaseModel, Field


DEFAULT_ALLOWED_SYMBOLS = [
    "US.IBIT",
    "US.MSTR",
    "US.COIN",
    "US.MARA",
    "US.RIOT",
    "US.BITO",
    "US.NVDA",
    "US.AAPL",
    "US.MSFT",
    "US.AMZN",
    "US.META",
    "HK.00700",
]
DEFAULT_BTC_PROXY_SYMBOLS = [
    "US.IBIT",
    "US.BITO",
    "US.MSTR",
    "US.COIN",
    "US.MARA",
    "US.RIOT",
]
DEFAULT_BTC_ETF_FUNDS = [
    "IBIT",
    "FBTC",
    "GBTC",
    "ARKB",
    "BITB",
    "BTCO",
    "EZBC",
    "HODL",
    "BRRR",
    "BTCW",
    "DEFI",
]
DEFAULT_X_MONITOR_ACCOUNTS = [
    "saylor",
    "elonmusk",
    "BlackRock",
    "EricBalchunas",
    "NateGeraci",
    "JSeyff",
    "whale_alert",
]
_LOADED_ENV_FILES: set[str] = set()


def _split_csv(value: str | None, default: list[str]) -> list[str]:
    if not value:
        return default
    return [item.strip() for item in value.split(",") if item.strip()]


def _split_json_list(value: str | None, default: list[dict]) -> list[dict]:
    if not value:
        return default
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return default
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    return default


def _first_present(env: os._Environ[str], *keys: str) -> str | None:
    for key in keys:
        value = env.get(key)
        if value:
            return value
    return None


def _strip_wrapping_quotes(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return stripped


def _load_env_file(path_value: str | None) -> None:
    if not path_value:
        return
    path = Path(path_value)
    if not path.is_absolute():
        path = Path.cwd() / path
    try:
        resolved_key = str(path.resolve())
    except OSError:
        resolved_key = str(path)
    if resolved_key in _LOADED_ENV_FILES or not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        os.environ.setdefault(key, _strip_wrapping_quotes(value))
    _LOADED_ENV_FILES.add(resolved_key)


def _load_default_env_files() -> None:
    _load_env_file(os.environ.get("HFQT_ENV_FILE", ".env"))


class AppConfig(BaseModel):
    database_path: Path = Field(default=Path("var/hfqt.sqlite3"))
    log_dir: Path = Field(default=Path("var/logs"))
    log_level: str = Field(default="INFO")
    log_max_bytes: int = Field(default=5_242_880)
    log_backup_count: int = Field(default=5)
    app_log_filename: str = Field(default="app.log")
    trade_log_filename: str = Field(default="trades.jsonl")
    error_log_filename: str = Field(default="error.log")
    decision_log_filename: str = Field(default="decision_chain.jsonl")
    owner_control_enabled: bool = Field(default=False)
    require_owner_token_for_write: bool = Field(default=True)
    owner_token: str | None = Field(default=None)
    owner_state_path: Path = Field(default=Path("var/owner_state.json"))
    trading_locked_default: bool = Field(default=False)
    auth_enabled: bool = Field(default=False)
    auth_mode: str = Field(default="off")
    auth_product_code: str = Field(default="HFQT")
    auth_client_version: str = Field(default="0.4.0")
    auth_license_path: Path = Field(default=Path("config/license.dat"))
    auth_token_cache_path: Path = Field(default=Path("cache/token.cache"))
    auth_device_secret_path: Path = Field(default=Path("config/device.secret"))
    auth_public_key_path: Path = Field(default=Path("config/license_public.pem"))
    auth_server_base_url: str | None = Field(default=None)
    auth_machine_binding_mode: str = Field(default="single")
    auth_offline_grace_hours: int = Field(default=72)
    auth_token_refresh_minutes: int = Field(default=720)
    auth_heartbeat_minutes: int = Field(default=30)
    default_broker: str = Field(default="local_paper")
    dry_run: bool = Field(default=False)
    allowed_symbols: list[str] = Field(default_factory=lambda: DEFAULT_ALLOWED_SYMBOLS.copy())
    watchlist_top_n: int = Field(default=8)
    scan_concurrency: int = Field(default=4)
    scan_cache_ttl_seconds: int = Field(default=180)
    scan_incremental_ttl_seconds: int = Field(default=30)
    settlement_windows: list[dict] = Field(default_factory=lambda: [
        {
            "name": "US equities open",
            "tz": "America/New_York",
            "days": [0, 1, 2, 3, 4],
            "start": "09:30",
            "end": "16:00",
        },
        {
            "name": "US equities close",
            "tz": "America/New_York",
            "days": [0, 1, 2, 3, 4],
            "start": "15:50",
            "end": "16:10",
        },
        {
            "name": "HK equities open",
            "tz": "Asia/Hong_Kong",
            "days": [0, 1, 2, 3, 4],
            "start": "09:30",
            "end": "12:00",
        },
        {
            "name": "HK equities close",
            "tz": "Asia/Hong_Kong",
            "days": [0, 1, 2, 3, 4],
            "start": "15:50",
            "end": "16:10",
        },
    ])
    auto_trade_enabled: bool = Field(default=False)
    auto_trade_cooldown_minutes: int = Field(default=15)
    auto_trade_max_orders_per_cycle: int = Field(default=1)
    max_notional_per_order: float = Field(default=100_000.0)
    max_orders_per_day: int = Field(default=20)
    min_confidence: float = Field(default=0.55)
    default_quantity: float = Field(default=1.0)
    intraday_feature_window_minutes: int = Field(default=30)
    network_intel_limit: int = Field(default=100)
    network_intel_primary_age_hours: float = Field(default=2.0)
    network_intel_max_news_age_hours: float = Field(default=4.0)
    network_intel_max_social_age_hours: float = Field(default=2.0)
    network_intel_important_event_age_hours: float = Field(default=72.0)
    network_intel_ignore_query: bool = Field(default=True)
    yfinance_news_limit: int = Field(default=100)
    btc_proxy_symbols: list[str] = Field(default_factory=lambda: DEFAULT_BTC_PROXY_SYMBOLS.copy())
    x_monitor_enabled: bool = Field(default=True)
    x_monitor_accounts: list[str] = Field(default_factory=lambda: DEFAULT_X_MONITOR_ACCOUNTS.copy())
    x_monitor_posts_limit: int = Field(default=100)
    xreach_enabled: bool = Field(default=True)
    whale_alert_enabled: bool = Field(default=True)
    whale_alert_handle: str = Field(default="whale_alert")
    whale_alert_min_btc: float = Field(default=500.0)
    whale_alert_min_usd: float = Field(default=5_000_000.0)
    btc_etf_flow_enabled: bool = Field(default=True)
    btc_etf_flow_jina_url: str = Field(default="https://r.jina.ai/http://farside.co.uk/btc/")
    btc_etf_flow_lookback_days: int = Field(default=5)
    btc_etf_funds: list[str] = Field(default_factory=lambda: DEFAULT_BTC_ETF_FUNDS.copy())
    macro_event_enabled: bool = Field(default=True)
    macro_fed_rss_url: str = Field(default="https://www.federalreserve.gov/feeds/speeches.xml")
    macro_cpi_release_url: str = Field(default="https://r.jina.ai/http://www.bls.gov/news.release/cpi.nr0.htm")
    macro_nfp_release_url: str = Field(default="https://r.jina.ai/http://www.bls.gov/news.release/empsit.nr0.htm")
    macro_event_limit: int = Field(default=100)
    macro_calendar_sources: list[str] = Field(default_factory=lambda: [
        "https://r.jina.ai/http://www.investing.com/economic-calendar/",
        "https://r.jina.ai/http://www.forexfactory.com/calendar",
    ])
    macro_calendar_keywords: list[str] = Field(default_factory=lambda: [
        "CPI",
        "CPI y/y",
        "Core CPI",
        "Core CPI y/y",
        "Non-Farm Employment Change",
        "Unemployment Rate",
        "FOMC Statement",
        "Federal Funds Rate",
        "ISM Manufacturing PMI",
        "Retail Sales",
    ])
    macro_rss_sources: list[str] = Field(default_factory=lambda: [
        "https://www.federalreserve.gov/feeds/press_all.xml",
        "https://www.bls.gov/feed/news_release/",
        "https://home.treasury.gov/press-releases/rss",
    ])
    etf_news_sources: list[str] = Field(default_factory=lambda: [
        "https://www.sec.gov/rss/press-release.xml",
        "https://www.etf.com/sections/news/news.xml",
    ])
    announcement_sources: list[str] = Field(default_factory=lambda: [
        "https://www.sec.gov/rss/press-release.xml",
        "https://www.sec.gov/rss/litigation/litreleases.xml",
        "https://www.nasdaq.com/feed/rssoutbound?category=Corporate%20News",
    ])
    alpha_vantage_enabled: bool = Field(default=True)
    alpha_vantage_api_key: str | None = Field(default=None)
    alpha_vantage_base_url: str = Field(default="https://www.alphavantage.co/query")
    alpha_vantage_news_limit: int = Field(default=3)
    alpha_vantage_timeout_seconds: float = Field(default=10.0)
    financial_datasets_enabled: bool = Field(default=False)
    financial_datasets_api_key: str | None = Field(default=None)
    financial_datasets_base_url: str = Field(default="https://api.financialdatasets.ai")
    financial_datasets_mcp_url: str = Field(default="https://mcp.financialdatasets.ai/api")
    financial_datasets_statement_limit: int = Field(default=4)
    yahoo_chart_base_url: str = Field(default="https://query1.finance.yahoo.com")
    history_match_limit: int = Field(default=3)
    history_match_lookback_days: int = Field(default=30)
    dynamic_threshold_low_vol_pct: float = Field(default=0.004)
    dynamic_threshold_high_vol_pct: float = Field(default=0.012)
    dynamic_threshold_low_vol_bump: float = Field(default=0.03)
    dynamic_threshold_high_vol_discount: float = Field(default=0.08)
    futu_host: str = Field(default="127.0.0.1")
    futu_port: int = Field(default=11111)
    futu_market: str = Field(default="US")
    futu_security_firm: str | None = Field(default=None)
    llm_base_url: str | None = Field(default=None)
    llm_api_key: str | None = Field(default=None)
    llm_model: str | None = Field(default=None)
    llm_mode: str = Field(default="primary")
    llm_model_primary: str | None = Field(default=None)
    llm_model_fallback: str | None = Field(default=None)
    llm_fallback_body_chars: int = Field(default=3800)
    llm_fallback_xreach_count: int = Field(default=4)
    llm_fallback_item_count: int = Field(default=6)
    llm_auto_retry_with_fallback: bool = Field(default=True)
    llm_provider: str | None = Field(default=None)
    llm_timeout_seconds: float = Field(default=15.0)
    llm_temperature: float = Field(default=0.1)
    intel_agent_enabled: bool = Field(default=True)
    intel_agent_provider: str | None = Field(default=None)
    intel_agent_base_url: str | None = Field(default=None)
    intel_agent_api_key: str | None = Field(default=None)
    intel_agent_model: str | None = Field(default=None)
    review_agent_enabled: bool = Field(default=True)
    review_agent_provider: str | None = Field(default=None)
    review_agent_base_url: str | None = Field(default=None)
    review_agent_api_key: str | None = Field(default=None)
    review_agent_model: str | None = Field(default=None)
    review_agent_secondary_enabled: bool = Field(default=False)
    review_agent_secondary_provider: str | None = Field(default=None)
    review_agent_secondary_base_url: str | None = Field(default=None)
    review_agent_secondary_api_key: str | None = Field(default=None)
    review_agent_secondary_model: str | None = Field(default=None)
    review_conflict_action: str = Field(default="hold")
    risk_agent_enabled: bool = Field(default=True)
    risk_agent_provider: str | None = Field(default=None)
    risk_agent_base_url: str | None = Field(default=None)
    risk_agent_api_key: str | None = Field(default=None)
    risk_agent_model: str | None = Field(default=None)
    latency_budget_ms: int = Field(default=5_000)
    translate_to_zh: bool = Field(default=False)
    translation_provider_order: tuple[str, ...] = Field(default=("deepseek", "glm", "openai"))
    translation_timeout_seconds: float = Field(default=4.0)
    translation_max_tokens: int = Field(default=120)
    usmart_env: str = Field(default="prod")
    usmart_trade_host: str = Field(default="https://open-jy.yxzq.com")
    usmart_quote_host: str = Field(default="https://open-hz.yxzq.com:8443")
    usmart_ws_host: str = Field(default="wss://open-hz.yxzq.com:8443/wss/v1")
    usmart_ws_origin: str = Field(default="https://open-hz.yxzq.com:8443")
    usmart_login_path: str = Field(default="/user-server/open-api/login")
    usmart_trade_login_path: str = Field(default="/user-server/open-api/trade-login")
    usmart_marketstate_path: str = Field(default="/quotes-openservice/api/v1/marketstate")
    usmart_x_channel: str | None = Field(default=None)
    usmart_x_lang: str = Field(default="1")
    usmart_x_dt: str | None = Field(default=None)
    usmart_x_type: str | None = Field(default=None)
    usmart_public_key: str | None = Field(default=None)
    usmart_private_key: str | None = Field(default=None)
    usmart_login_type: str = Field(default="phone")
    usmart_area_code: str | None = Field(default=None)
    usmart_phone_number: str | None = Field(default=None)
    usmart_email: str | None = Field(default=None)
    usmart_login_password: str | None = Field(default=None)
    usmart_trade_password: str | None = Field(default=None)
    api_host: str = Field(default="127.0.0.1")
    api_port: int = Field(default=8000)

    @classmethod
    def from_env(cls) -> "AppConfig":
        _load_default_env_files()
        env = os.environ
        llm_api_key = _first_present(env, "HFQT_LLM_API_KEY", "DEEPSEEK_API_KEY", "DEEPSEEK_KEY")
        llm_base_url = _first_present(env, "HFQT_LLM_BASE_URL", "DEEPSEEK_BASE_URL")
        llm_model = _first_present(env, "HFQT_LLM_MODEL", "DEEPSEEK_MODEL")
        llm_model_primary = _first_present(env, "HFQT_LLM_MODEL_PRIMARY", "HFQT_LLM_MODEL", "DEEPSEEK_MODEL")
        llm_model_fallback = _first_present(env, "HFQT_LLM_MODEL_FALLBACK")
        llm_provider = env.get("HFQT_LLM_PROVIDER")

        if not llm_base_url and llm_api_key:
            llm_base_url = "https://api.deepseek.com"
        if not llm_model and llm_api_key:
            llm_model = "deepseek-chat"
        if not llm_model_primary and llm_model:
            llm_model_primary = llm_model
        if not llm_provider:
            normalized_base_url = (llm_base_url or "").strip().lower()
            if "11434" in normalized_base_url or "ollama" in normalized_base_url:
                llm_provider = "ollama"
            elif llm_api_key:
                llm_provider = "deepseek"
            elif llm_base_url:
                llm_provider = "openai_compatible"

        return cls(
            database_path=Path(env.get("HFQT_DATABASE_PATH", "var/hfqt.sqlite3")),
            log_dir=Path(env.get("HFQT_LOG_DIR", "var/logs")),
            log_level=env.get("HFQT_LOG_LEVEL", "INFO"),
            log_max_bytes=int(env.get("HFQT_LOG_MAX_BYTES", 5_242_880)),
            log_backup_count=int(env.get("HFQT_LOG_BACKUP_COUNT", 5)),
            app_log_filename=env.get("HFQT_APP_LOG_FILENAME", "app.log"),
            trade_log_filename=env.get("HFQT_TRADE_LOG_FILENAME", "trades.jsonl"),
            error_log_filename=env.get("HFQT_ERROR_LOG_FILENAME", "error.log"),
            decision_log_filename=env.get("HFQT_DECISION_LOG_FILENAME", "decision_chain.jsonl"),
            owner_control_enabled=env.get("HFQT_OWNER_CONTROL_ENABLED", "false").lower() in {"1", "true", "yes", "on"},
            require_owner_token_for_write=env.get("HFQT_REQUIRE_OWNER_TOKEN_FOR_WRITE", "true").lower() in {"1", "true", "yes", "on"},
            owner_token=env.get("HFQT_OWNER_TOKEN") or None,
            owner_state_path=Path(env.get("HFQT_OWNER_STATE_PATH", "var/owner_state.json")),
            trading_locked_default=env.get("HFQT_TRADING_LOCKED_DEFAULT", "false").lower() in {"1", "true", "yes", "on"},
            auth_enabled=env.get("HFQT_AUTH_ENABLED", "false").lower() in {"1", "true", "yes", "on"},
            auth_mode=(env.get("HFQT_AUTH_MODE", "off").strip().lower() or "off"),
            auth_product_code=env.get("HFQT_AUTH_PRODUCT_CODE", "HFQT"),
            auth_client_version=env.get("HFQT_AUTH_CLIENT_VERSION", "0.4.0"),
            auth_license_path=Path(env.get("HFQT_AUTH_LICENSE_PATH", "config/license.dat")),
            auth_token_cache_path=Path(env.get("HFQT_AUTH_TOKEN_CACHE_PATH", "cache/token.cache")),
            auth_device_secret_path=Path(env.get("HFQT_AUTH_DEVICE_SECRET_PATH", "config/device.secret")),
            auth_public_key_path=Path(env.get("HFQT_AUTH_PUBLIC_KEY_PATH", "config/license_public.pem")),
            auth_server_base_url=env.get("HFQT_AUTH_SERVER_BASE_URL") or None,
            auth_machine_binding_mode=env.get("HFQT_AUTH_MACHINE_BINDING_MODE", "single"),
            auth_offline_grace_hours=max(1, int(env.get("HFQT_AUTH_OFFLINE_GRACE_HOURS", 72))),
            auth_token_refresh_minutes=max(5, int(env.get("HFQT_AUTH_TOKEN_REFRESH_MINUTES", 720))),
            auth_heartbeat_minutes=max(1, int(env.get("HFQT_AUTH_HEARTBEAT_MINUTES", 30))),
            default_broker=env.get("HFQT_DEFAULT_BROKER", "local_paper"),
            dry_run=env.get("HFQT_DRY_RUN", "false").lower() in {"1", "true", "yes", "on"},
            allowed_symbols=_split_csv(env.get("HFQT_ALLOWED_SYMBOLS"), DEFAULT_ALLOWED_SYMBOLS.copy()),
            watchlist_top_n=max(3, int(env.get("HFQT_WATCHLIST_TOP_N", 8))),
            scan_concurrency=max(1, int(env.get("HFQT_SCAN_CONCURRENCY", 4))),
            scan_cache_ttl_seconds=max(0, int(env.get("HFQT_SCAN_CACHE_TTL_SECONDS", 180))),
            scan_incremental_ttl_seconds=max(0, int(env.get("HFQT_SCAN_INCREMENTAL_TTL_SECONDS", 30))),
            settlement_windows=_split_json_list(env.get("HFQT_SETTLEMENT_WINDOWS"), [
                {
                    "name": "US equities open",
                    "tz": "America/New_York",
                    "days": [0, 1, 2, 3, 4],
                    "start": "09:30",
                    "end": "16:00",
                },
                {
                    "name": "US equities close",
                    "tz": "America/New_York",
                    "days": [0, 1, 2, 3, 4],
                    "start": "15:50",
                    "end": "16:10",
                },
                {
                    "name": "HK equities open",
                    "tz": "Asia/Hong_Kong",
                    "days": [0, 1, 2, 3, 4],
                    "start": "09:30",
                    "end": "12:00",
                },
                {
                    "name": "HK equities close",
                    "tz": "Asia/Hong_Kong",
                    "days": [0, 1, 2, 3, 4],
                    "start": "15:50",
                    "end": "16:10",
                },
            ]),
            auto_trade_enabled=env.get("HFQT_AUTO_TRADE_ENABLED", "false").lower() in {"1", "true", "yes", "on"},
            auto_trade_cooldown_minutes=max(1, int(env.get("HFQT_AUTO_TRADE_COOLDOWN_MINUTES", 15))),
            auto_trade_max_orders_per_cycle=max(1, int(env.get("HFQT_AUTO_TRADE_MAX_ORDERS_PER_CYCLE", 1))),
            max_notional_per_order=float(env.get("HFQT_MAX_NOTIONAL_PER_ORDER", 100_000)),
            max_orders_per_day=int(env.get("HFQT_MAX_ORDERS_PER_DAY", 20)),
            min_confidence=float(env.get("HFQT_MIN_CONFIDENCE", 0.55)),
            default_quantity=float(env.get("HFQT_DEFAULT_QUANTITY", 1.0)),
            intraday_feature_window_minutes=int(env.get("HFQT_INTRADAY_FEATURE_WINDOW_MINUTES", 30)),
            network_intel_limit=max(2, int(env.get("HFQT_NETWORK_INTEL_LIMIT", 100))),
            network_intel_primary_age_hours=max(0.5, float(env.get("HFQT_NETWORK_INTEL_PRIMARY_AGE_HOURS", 2.0))),
            network_intel_max_news_age_hours=max(1.0, float(env.get("HFQT_NETWORK_INTEL_MAX_NEWS_AGE_HOURS", 4.0))),
            network_intel_max_social_age_hours=max(0.5, float(env.get("HFQT_NETWORK_INTEL_MAX_SOCIAL_AGE_HOURS", 2.0))),
            network_intel_important_event_age_hours=max(
                4.0,
                float(env.get("HFQT_NETWORK_INTEL_IMPORTANT_EVENT_AGE_HOURS", 72.0)),
            ),
            network_intel_ignore_query=env.get("HFQT_NETWORK_INTEL_IGNORE_QUERY", "true").lower() in {"1", "true", "yes", "on"},
            yfinance_news_limit=max(1, int(env.get("HFQT_YFINANCE_NEWS_LIMIT", 100))),
            btc_proxy_symbols=_split_csv(env.get("HFQT_BTC_PROXY_SYMBOLS"), DEFAULT_BTC_PROXY_SYMBOLS.copy()),
            x_monitor_enabled=env.get("HFQT_X_MONITOR_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
            x_monitor_accounts=_split_csv(env.get("HFQT_X_MONITOR_ACCOUNTS"), DEFAULT_X_MONITOR_ACCOUNTS.copy()),
            x_monitor_posts_limit=max(1, int(env.get("HFQT_X_MONITOR_POSTS_LIMIT", 100))),
            xreach_enabled=env.get("HFQT_XREACH_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
            whale_alert_enabled=env.get("HFQT_WHALE_ALERT_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
            whale_alert_handle=env.get("HFQT_WHALE_ALERT_HANDLE", "whale_alert").strip() or "whale_alert",
            whale_alert_min_btc=max(0.0, float(env.get("HFQT_WHALE_ALERT_MIN_BTC", 500.0))),
            whale_alert_min_usd=max(0.0, float(env.get("HFQT_WHALE_ALERT_MIN_USD", 5_000_000.0))),
            btc_etf_flow_enabled=env.get("HFQT_BTC_ETF_FLOW_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
            btc_etf_flow_jina_url=env.get("HFQT_BTC_ETF_FLOW_JINA_URL", "https://r.jina.ai/http://farside.co.uk/btc/"),
            btc_etf_flow_lookback_days=max(1, int(env.get("HFQT_BTC_ETF_FLOW_LOOKBACK_DAYS", 5))),
            btc_etf_funds=_split_csv(env.get("HFQT_BTC_ETF_FUNDS"), DEFAULT_BTC_ETF_FUNDS.copy()),
            macro_event_enabled=env.get("HFQT_MACRO_EVENT_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
            macro_fed_rss_url=env.get("HFQT_MACRO_FED_RSS_URL", "https://www.federalreserve.gov/feeds/speeches.xml"),
            macro_cpi_release_url=env.get("HFQT_MACRO_CPI_RELEASE_URL", "https://r.jina.ai/http://www.bls.gov/news.release/cpi.nr0.htm"),
            macro_nfp_release_url=env.get("HFQT_MACRO_NFP_RELEASE_URL", "https://r.jina.ai/http://www.bls.gov/news.release/empsit.nr0.htm"),
            macro_event_limit=max(1, int(env.get("HFQT_MACRO_EVENT_LIMIT", 100))),
            macro_calendar_sources=_split_csv(env.get("HFQT_MACRO_CALENDAR_SOURCES"), [
                "https://r.jina.ai/http://www.investing.com/economic-calendar/",
                "https://r.jina.ai/http://www.forexfactory.com/calendar",
            ]),
            macro_calendar_keywords=_split_csv(env.get("HFQT_MACRO_CALENDAR_KEYWORDS"), [
                "CPI",
                "CPI y/y",
                "Core CPI",
                "Core CPI y/y",
                "Non-Farm Employment Change",
                "Unemployment Rate",
                "FOMC Statement",
                "Federal Funds Rate",
                "ISM Manufacturing PMI",
                "Retail Sales",
            ]),
            macro_rss_sources=_split_csv(env.get("HFQT_MACRO_RSS_SOURCES"), [
                "https://www.federalreserve.gov/feeds/press_all.xml",
                "https://www.bls.gov/feed/news_release/",
                "https://home.treasury.gov/press-releases/rss",
            ]),
            etf_news_sources=_split_csv(env.get("HFQT_ETF_NEWS_SOURCES"), [
                "https://www.sec.gov/rss/press-release.xml",
                "https://www.etf.com/sections/news/news.xml",
            ]),
            announcement_sources=_split_csv(env.get("HFQT_ANNOUNCEMENT_SOURCES"), [
                "https://www.sec.gov/rss/press-release.xml",
                "https://www.sec.gov/rss/litigation/litreleases.xml",
                "https://www.nasdaq.com/feed/rssoutbound?category=Corporate%20News",
            ]),
            alpha_vantage_enabled=env.get("HFQT_ALPHA_VANTAGE_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
            alpha_vantage_api_key=env.get("HFQT_ALPHA_VANTAGE_API_KEY") or None,
            alpha_vantage_base_url=env.get("HFQT_ALPHA_VANTAGE_BASE_URL", "https://www.alphavantage.co/query"),
            alpha_vantage_news_limit=max(1, int(env.get("HFQT_ALPHA_VANTAGE_NEWS_LIMIT", 3))),
            alpha_vantage_timeout_seconds=max(2.0, float(env.get("HFQT_ALPHA_VANTAGE_TIMEOUT_SECONDS", 10.0))),
            financial_datasets_enabled=env.get("HFQT_FINANCIAL_DATASETS_ENABLED", "false").lower() in {"1", "true", "yes", "on"},
            financial_datasets_api_key=env.get("HFQT_FINANCIAL_DATASETS_API_KEY") or None,
            financial_datasets_base_url=env.get("HFQT_FINANCIAL_DATASETS_BASE_URL", "https://api.financialdatasets.ai"),
            financial_datasets_mcp_url=env.get("HFQT_FINANCIAL_DATASETS_MCP_URL", "https://mcp.financialdatasets.ai/api"),
            financial_datasets_statement_limit=max(1, int(env.get("HFQT_FINANCIAL_DATASETS_STATEMENT_LIMIT", 4))),
            yahoo_chart_base_url=env.get("HFQT_YAHOO_CHART_BASE_URL", "https://query1.finance.yahoo.com"),
            history_match_limit=int(env.get("HFQT_HISTORY_MATCH_LIMIT", 3)),
            history_match_lookback_days=int(env.get("HFQT_HISTORY_MATCH_LOOKBACK_DAYS", 30)),
            dynamic_threshold_low_vol_pct=float(env.get("HFQT_DYNAMIC_THRESHOLD_LOW_VOL_PCT", 0.004)),
            dynamic_threshold_high_vol_pct=float(env.get("HFQT_DYNAMIC_THRESHOLD_HIGH_VOL_PCT", 0.012)),
            dynamic_threshold_low_vol_bump=float(env.get("HFQT_DYNAMIC_THRESHOLD_LOW_VOL_BUMP", 0.03)),
            dynamic_threshold_high_vol_discount=float(env.get("HFQT_DYNAMIC_THRESHOLD_HIGH_VOL_DISCOUNT", 0.08)),
            futu_host=env.get("HFQT_FUTU_HOST", "127.0.0.1"),
            futu_port=int(env.get("HFQT_FUTU_PORT", 11111)),
            futu_market=env.get("HFQT_FUTU_MARKET", "US"),
            futu_security_firm=env.get("HFQT_FUTU_SECURITY_FIRM") or None,
            llm_base_url=llm_base_url,
            llm_api_key=llm_api_key,
            llm_model=llm_model_primary or llm_model,
            llm_mode=env.get("HFQT_LLM_MODE", "primary").strip().lower() or "primary",
            llm_model_primary=llm_model_primary or llm_model,
            llm_model_fallback=llm_model_fallback,
            llm_fallback_body_chars=max(800, int(env.get("HFQT_LLM_FALLBACK_BODY_CHARS", 3800))),
            llm_fallback_xreach_count=max(1, int(env.get("HFQT_LLM_FALLBACK_XREACH_COUNT", 4))),
            llm_fallback_item_count=max(2, int(env.get("HFQT_LLM_FALLBACK_ITEM_COUNT", 6))),
            llm_auto_retry_with_fallback=env.get("HFQT_LLM_AUTO_RETRY_WITH_FALLBACK", "true").lower() in {"1", "true", "yes", "on"},
            llm_provider=llm_provider,
            llm_timeout_seconds=float(env.get("HFQT_LLM_TIMEOUT_SECONDS", 15)),
            llm_temperature=float(env.get("HFQT_LLM_TEMPERATURE", 0.1)),
            intel_agent_enabled=env.get("HFQT_INTEL_AGENT_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
            intel_agent_provider=env.get("HFQT_INTEL_AGENT_PROVIDER") or None,
            intel_agent_base_url=env.get("HFQT_INTEL_AGENT_BASE_URL") or None,
            intel_agent_api_key=env.get("HFQT_INTEL_AGENT_API_KEY") or None,
            intel_agent_model=env.get("HFQT_INTEL_AGENT_MODEL") or None,
            review_agent_enabled=env.get("HFQT_REVIEW_AGENT_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
            review_agent_provider=env.get("HFQT_REVIEW_AGENT_PROVIDER") or None,
            review_agent_base_url=env.get("HFQT_REVIEW_AGENT_BASE_URL") or None,
            review_agent_api_key=env.get("HFQT_REVIEW_AGENT_API_KEY") or None,
            review_agent_model=env.get("HFQT_REVIEW_AGENT_MODEL") or None,
            review_agent_secondary_enabled=env.get("HFQT_REVIEW_AGENT_SECONDARY_ENABLED", "false").lower() in {"1", "true", "yes", "on"},
            review_agent_secondary_provider=env.get("HFQT_REVIEW_AGENT_SECONDARY_PROVIDER") or None,
            review_agent_secondary_base_url=env.get("HFQT_REVIEW_AGENT_SECONDARY_BASE_URL") or None,
            review_agent_secondary_api_key=env.get("HFQT_REVIEW_AGENT_SECONDARY_API_KEY") or None,
            review_agent_secondary_model=env.get("HFQT_REVIEW_AGENT_SECONDARY_MODEL") or None,
            review_conflict_action=env.get("HFQT_REVIEW_CONFLICT_ACTION", "hold").strip().lower() or "hold",
            risk_agent_enabled=env.get("HFQT_RISK_AGENT_ENABLED", "true").lower() in {"1", "true", "yes", "on"},
            risk_agent_provider=env.get("HFQT_RISK_AGENT_PROVIDER") or None,
            risk_agent_base_url=env.get("HFQT_RISK_AGENT_BASE_URL") or None,
            risk_agent_api_key=env.get("HFQT_RISK_AGENT_API_KEY") or None,
            risk_agent_model=env.get("HFQT_RISK_AGENT_MODEL") or None,
            latency_budget_ms=max(1000, int(env.get("HFQT_LATENCY_BUDGET_MS", 5000))),
            translate_to_zh=env.get("HFQT_TRANSLATE_TO_ZH", "false").lower() in {"1", "true", "yes", "on"},
            translation_provider_order=tuple(
                item.strip().lower()
                for item in env.get("HFQT_TRANSLATION_PROVIDER_ORDER", "deepseek,glm,openai").split(",")
                if item.strip()
            ),
            translation_timeout_seconds=float(env.get("HFQT_TRANSLATION_TIMEOUT_SECONDS", 4.0)),
            translation_max_tokens=max(int(env.get("HFQT_TRANSLATION_MAX_TOKENS", 120)), 32),
            usmart_env=env.get("USMART_ENV", "prod"),
            usmart_trade_host=env.get("USMART_TRADE_HOST", "https://open-jy.yxzq.com"),
            usmart_quote_host=env.get("USMART_QUOTE_HOST", "https://open-hz.yxzq.com:8443"),
            usmart_ws_host=env.get("USMART_WS_HOST", "wss://open-hz.yxzq.com:8443/wss/v1"),
            usmart_ws_origin=env.get("USMART_WS_ORIGIN", "https://open-hz.yxzq.com:8443"),
            usmart_login_path=env.get("USMART_LOGIN_PATH", "/user-server/open-api/login"),
            usmart_trade_login_path=env.get("USMART_TRADE_LOGIN_PATH", "/user-server/open-api/trade-login"),
            usmart_marketstate_path=env.get("USMART_MARKETSTATE_PATH", "/quotes-openservice/api/v1/marketstate"),
            usmart_x_channel=env.get("USMART_X_CHANNEL") or None,
            usmart_x_lang=env.get("USMART_X_LANG", "1"),
            usmart_x_dt=env.get("USMART_X_DT") or None,
            usmart_x_type=env.get("USMART_X_TYPE") or None,
            usmart_public_key=env.get("USMART_PUBLIC_KEY") or None,
            usmart_private_key=env.get("USMART_PRIVATE_KEY") or None,
            usmart_login_type=env.get("USMART_LOGIN_TYPE", "phone"),
            usmart_area_code=env.get("USMART_AREA_CODE") or None,
            usmart_phone_number=env.get("USMART_PHONE_NUMBER") or None,
            usmart_email=env.get("USMART_EMAIL") or None,
            usmart_login_password=env.get("USMART_LOGIN_PASSWORD") or None,
            usmart_trade_password=env.get("USMART_TRADE_PASSWORD") or None,
            api_host=env.get("HFQT_API_HOST", "127.0.0.1"),
            api_port=int(env.get("HFQT_API_PORT", 8000)),
        )

    def agent_settings(self, role: str) -> dict[str, str | bool | None]:
        role_key = role.strip().lower()
        if role_key == "analysis":
            return {
                "role": "analysis",
                "enabled": bool(self.llm_base_url and (self.llm_model_primary or self.llm_model)),
                "provider": self.llm_provider or "deepseek",
                "base_url": self.llm_base_url,
                "api_key": self.llm_api_key,
                "model": self.llm_model_primary or self.llm_model,
                "fallback_model": self.llm_model_fallback,
                "mode": self.llm_mode,
            }
        if role_key == "intel":
            return {
                "role": "intel",
                "enabled": self.intel_agent_enabled and bool((self.intel_agent_base_url or self.llm_base_url) and (self.intel_agent_model or self.llm_model_primary or self.llm_model)),
                "provider": self.intel_agent_provider or self.llm_provider or "deepseek",
                "base_url": self.intel_agent_base_url or self.llm_base_url,
                "api_key": self.intel_agent_api_key or self.llm_api_key,
                "model": self.intel_agent_model or self.llm_model_primary or self.llm_model,
                "fallback_model": None,
                "mode": "single",
            }
        if role_key == "review":
            return {
                "role": "review",
                "enabled": self.review_agent_enabled and bool((self.review_agent_base_url or self.llm_base_url) and (self.review_agent_model or self.llm_model_primary or self.llm_model)),
                "provider": self.review_agent_provider or self.llm_provider or "deepseek",
                "base_url": self.review_agent_base_url or self.llm_base_url,
                "api_key": self.review_agent_api_key or self.llm_api_key,
                "model": self.review_agent_model or self.llm_model_primary or self.llm_model,
                "fallback_model": None,
                "mode": "single",
            }
        if role_key == "review_secondary":
            return {
                "role": "review",
                "enabled": self.review_agent_secondary_enabled and bool((self.review_agent_secondary_base_url or self.llm_base_url) and (self.review_agent_secondary_model or self.llm_model_primary or self.llm_model)),
                "provider": self.review_agent_secondary_provider or self.llm_provider or "deepseek",
                "base_url": self.review_agent_secondary_base_url or self.llm_base_url,
                "api_key": self.review_agent_secondary_api_key or self.llm_api_key,
                "model": self.review_agent_secondary_model or self.llm_model_primary or self.llm_model,
                "fallback_model": None,
                "mode": "single",
            }
        if role_key == "risk":
            return {
                "role": "risk",
                "enabled": self.risk_agent_enabled and bool((self.risk_agent_base_url or self.llm_base_url) and (self.risk_agent_model or self.llm_model_primary or self.llm_model)),
                "provider": self.risk_agent_provider or self.llm_provider or "deepseek",
                "base_url": self.risk_agent_base_url or self.llm_base_url,
                "api_key": self.risk_agent_api_key or self.llm_api_key,
                "model": self.risk_agent_model or self.llm_model_primary or self.llm_model,
                "fallback_model": None,
                "mode": "single",
            }
        raise ValueError(f"Unsupported agent role: {role}")

    def auth_settings_public(self) -> dict[str, str | bool | int | None]:
        return {
            "enabled": self.auth_enabled,
            "mode": self.auth_mode,
            "product_code": self.auth_product_code,
            "client_version": self.auth_client_version,
            "license_path": str(self.auth_license_path),
            "token_cache_path": str(self.auth_token_cache_path),
            "public_key_path": str(self.auth_public_key_path),
            "server_base_url": self.auth_server_base_url,
            "machine_binding_mode": self.auth_machine_binding_mode,
            "offline_grace_hours": self.auth_offline_grace_hours,
            "token_refresh_minutes": self.auth_token_refresh_minutes,
            "heartbeat_minutes": self.auth_heartbeat_minutes,
        }
