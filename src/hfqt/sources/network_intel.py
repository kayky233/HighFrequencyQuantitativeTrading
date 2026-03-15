from __future__ import annotations

import asyncio
import html
import json
import re
import subprocess
import urllib.parse
import xml.etree.ElementTree as ET
import math
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Awaitable, Callable

import httpx
import yfinance as yf

from hfqt.agents import IntelAgent
from hfqt.config import AppConfig
from hfqt.schemas import EventType, InputEvent
from hfqt.translation import ChineseTranslator


@dataclass
class NetworkIntelItem:
    source: str
    title: str
    snippet: str | None = None
    url: str | None = None
    published_at: str | None = None
    score: float = 0.0
    raw: dict | None = None
    title_zh: str | None = None
    snippet_zh: str | None = None


class NetworkIntelSource:
    def __init__(self, config: AppConfig, limit: int | None = None, use_intel_agent: bool | None = None) -> None:
        self.config = config
        self.limit = limit or config.network_intel_limit
        self.translator = ChineseTranslator(config)
        self.intel_agent = IntelAgent(config)
        self.use_intel_agent = config.intel_agent_enabled if use_intel_agent is None else use_intel_agent
        self._shared_tasks: dict[str, asyncio.Task[Any]] = {}

    async def aclose(self) -> None:
        await self.translator.aclose()

    async def load(self, symbol: str, query: str | None = None, x_urls: list[str] | None = None) -> InputEvent:
        normalized_symbol = symbol.upper()
        ticker = normalized_symbol.split(".")[-1]
        direct_x_urls = self._normalize_x_urls(x_urls or self._extract_x_urls(query))
        query_text = self._normalize_query_text(normalized_symbol, query, direct_x_urls)
        if self.config.network_intel_ignore_query:
            query_text = "market OR earnings OR guidance OR outlook"

        google_result, yfinance_result, xreach_result, direct_x_result, alpha_vantage_result, financial_datasets_result, monitored_x_result, whale_alert_result, etf_flow_result, macro_result, etf_news_result, announcement_result = await asyncio.gather(
            self._fetch_google_news(query_text),
            asyncio.to_thread(self._fetch_yfinance_news, ticker),
            asyncio.to_thread(self._fetch_xreach_posts, query_text),
            asyncio.to_thread(self._fetch_direct_x_urls, direct_x_urls),
            self._fetch_alpha_vantage_snapshot(ticker),
            self._fetch_financial_datasets_snapshot(normalized_symbol),
            self._fetch_monitored_x_snapshot(normalized_symbol),
            self._fetch_whale_alert_snapshot(normalized_symbol),
            self._fetch_btc_etf_flow_snapshot(normalized_symbol),
            self._fetch_macro_event_snapshot(normalized_symbol),
            self._fetch_etf_news_snapshot(normalized_symbol),
            self._fetch_announcement_snapshot(normalized_symbol),
        )
        google_items, google_error = google_result
        yfinance_items, yfinance_error = yfinance_result
        xreach_items, xreach_error = xreach_result
        direct_x_items, direct_x_meta, direct_x_error = direct_x_result
        alpha_vantage_items, alpha_vantage_meta, alpha_vantage_error = alpha_vantage_result
        financial_datasets_items, financial_datasets_meta, financial_datasets_error = financial_datasets_result
        monitored_x_items, monitored_x_meta, monitored_x_error = monitored_x_result
        whale_alert_items, whale_alert_meta, whale_alert_error = whale_alert_result
        etf_flow_items, etf_flow_meta, etf_flow_error = etf_flow_result
        macro_items, macro_meta, macro_error = macro_result
        etf_news_items, etf_news_meta, etf_news_error = etf_news_result
        announcement_items, announcement_meta, announcement_error = announcement_result

        etf_news_items = self._apply_source_topic_filter(etf_news_items, normalized_symbol)
        announcement_items = self._apply_source_topic_filter(announcement_items, normalized_symbol)

        all_items = [
            *etf_flow_items,
            *macro_items,
            *whale_alert_items,
            *direct_x_items,
            *monitored_x_items,
            *financial_datasets_items,
            *alpha_vantage_items,
            *yfinance_items,
            *google_items,
            *xreach_items,
            *etf_news_items,
            *announcement_items,
        ]
        merged_items = self._merge_ranked_items(all_items)
        if not merged_items:
            raise RuntimeError(
                "没有抓到任何网络情报。"
                f"etf_flow_error={etf_flow_error or 'none'}; "
                f"macro_error={macro_error or 'none'}; "
                f"whale_alert_error={whale_alert_error or 'none'}; "
                f"monitored_x_error={monitored_x_error or 'none'}; "
                f"alpha_vantage_error={alpha_vantage_error or 'none'}; "
                f"yfinance_error={yfinance_error or 'none'}; "
                f"google_error={google_error or 'none'}; "
                f"xreach_error={xreach_error or 'none'}"
            )

        await self._translate_items(merged_items[: self.limit * 2])
        agent_summary = None
        if self.use_intel_agent and self.intel_agent.enabled:
            agent_summary = await self.intel_agent.summarize(
                symbol=normalized_symbol,
                query=query_text,
                items=merged_items[: self.limit * 2],
                translate_to_zh=self.config.translate_to_zh,
            )
        headline = self._display_title(merged_items[0])
        body_parts: list[str] = []
        sentiment = 0.0
        if agent_summary:
            headline = str(agent_summary.get("headline") or headline)
            body_parts.append(str(agent_summary.get("body") or ""))
            key_points = agent_summary.get("key_points") or []
            if isinstance(key_points, list):
                body_parts.extend(f"- {point}" for point in key_points[:5] if str(point).strip())
            sentiment = max(-1.0, min(1.0, float(agent_summary.get("sentiment", 0.0))))
        else:
            sentiment = self._fallback_sentiment(alpha_vantage_meta)

        alpha_vantage_brief = self._render_alpha_vantage_brief(alpha_vantage_meta)
        if alpha_vantage_brief:
            body_parts.append(alpha_vantage_brief)
        financial_datasets_brief = self._render_financial_datasets_brief(financial_datasets_meta)
        if financial_datasets_brief:
            body_parts.append(financial_datasets_brief)
        for item in merged_items[: self.limit * 2]:
            body_parts.append(f"[{self._source_label(item.source)}] {self._display_title(item)}")
            if item.snippet and item.snippet != item.title:
                body_parts.append(self._display_snippet(item))
            if item.url:
                body_parts.append(item.url)

        return InputEvent(
            event_type=EventType.NEWS,
            source="network-intel",
            symbol=normalized_symbol,
            headline=headline,
            body="\n".join(body_parts),
            sentiment=sentiment,
            metadata={
                "query": query_text,
                "direct_x_urls": direct_x_urls,
                "original_headline": merged_items[0].title,
                "items": [asdict(item) for item in merged_items[: self.limit * 2]],
                "sources": {
                    "btc_etf_flow_count": len(etf_flow_items),
                    "macro_event_count": len(macro_items),
                    "whale_alert_count": len(whale_alert_items),
                    "direct_x_count": len(direct_x_items),
                    "monitored_x_count": len(monitored_x_items),
                    "financial_datasets_count": len(financial_datasets_items),
                    "alpha_vantage_news_count": len(alpha_vantage_items),
                    "yfinance_news_count": len(yfinance_items),
                    "google_news_count": len(google_items),
                    "xreach_count": len(xreach_items),
                    "etf_news_count": len(etf_news_items),
                    "announcement_count": len(announcement_items),
                    "merged_count": len(merged_items),
                },
                "errors": {
                    "btc_etf_flow": etf_flow_error,
                    "macro_event": macro_error,
                    "whale_alert": whale_alert_error,
                    "direct_x": direct_x_error,
                    "monitored_x": monitored_x_error,
                    "financial_datasets": financial_datasets_error,
                    "alpha_vantage": alpha_vantage_error,
                    "yfinance_news": yfinance_error,
                    "google_news": google_error,
                    "xreach": xreach_error,
                    "etf_news": etf_news_error,
                    "announcement": announcement_error,
                },
                "btc_etf_flow": etf_flow_meta,
                "macro_event": macro_meta,
                "whale_alert": whale_alert_meta,
                "direct_x": direct_x_meta,
                "monitored_x": monitored_x_meta,
                "financial_datasets": financial_datasets_meta,
                "alpha_vantage": alpha_vantage_meta,
                "etf_news": etf_news_meta,
                "announcement": announcement_meta,
                "intel_agent": agent_summary,
            },
        )

    def _default_query_for_symbol(self, symbol: str) -> str:
        overrides = {
            "US.IBIT": "IBIT bitcoin ETF inflow OR bitcoin ETF news",
            "US.BITO": "BITO bitcoin ETF flow OR bitcoin futures ETF",
            "US.MSTR": "MicroStrategy bitcoin treasury OR MSTR bitcoin news",
            "US.COIN": "Coinbase crypto exchange OR COIN bitcoin trading news",
            "US.MARA": "MARA bitcoin mining OR MARA hash rate news",
            "US.RIOT": "RIOT bitcoin mining OR RIOT energy bitcoin news",
            "US.NVDA": "NVDA AI chips datacenter demand",
            "US.AAPL": "AAPL stock earnings OR Apple supply chain",
            "US.MSFT": "MSFT Azure AI earnings",
            "US.AMZN": "AMZN AWS AI retail earnings",
            "US.META": "META AI advertising earnings",
            "HK.00700": "Tencent gaming AI earnings",
        }
        base = overrides.get(symbol)
        if base:
            return self._expand_query(base, self._intel_theme_for_symbol(symbol))
        ticker = symbol.split(".")[-1]
        return self._expand_query(f"{ticker} stock", "generic")

    @staticmethod
    def _extract_x_urls(query: str | None) -> list[str]:
        if not query:
            return []
        matches = re.findall(r"https?://(?:www\.)?(?:x\.com|twitter\.com)/\S+", query, flags=re.IGNORECASE)
        return [item.rstrip(").,]}>\"'") for item in matches]

    @staticmethod
    def _normalize_x_urls(urls: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for raw in urls:
            value = str(raw).strip()
            if not value:
                continue
            value = value.rstrip(").,]}>\"'")
            if value in seen:
                continue
            seen.add(value)
            normalized.append(value)
        return normalized

    def _normalize_query_text(self, symbol: str, query: str | None, x_urls: list[str]) -> str:
        default_query = self._default_query_for_symbol(symbol)
        if not query:
            return default_query
        clean_query = str(query)
        for url in x_urls:
            clean_query = clean_query.replace(url, " ")
        clean_query = re.sub(r"\s+", " ", clean_query).strip()
        if not clean_query:
            return default_query
        # Always expand user queries to avoid overly narrow results.
        return self._expand_query(clean_query, self._intel_theme_for_symbol(symbol))

    def _expand_query(self, base_query: str, theme: str) -> str:
        terms = self._theme_terms(theme, base_query)
        if not terms:
            return base_query
        term_query = " OR ".join(terms)
        return f"({base_query}) OR ({term_query})"

    @staticmethod
    def _theme_terms(theme: str, base_query: str) -> list[str]:
        ticker = base_query.split()[0].strip()
        themes = {
            "btc_etf": [
                "bitcoin",
                "btc",
                "spot etf",
                "bitcoin etf",
                "etf inflow",
                "etf outflow",
                "blackrock",
                "ibit",
                "bito",
            ],
            "mstr": ["microstrategy", "strategy", "bitcoin treasury", "mstr"],
            "coin": ["coinbase", "crypto exchange", "custody", "btc"],
            "miners": ["bitcoin mining", "hashrate", "miner", "energy", "mara", "riot"],
            "ai_infra": ["ai", "gpu", "datacenter", "nvidia", "microsoft", "amazon", "meta"],
            "consumer_tech": ["iphone", "apple", "services", "supply chain", "app store"],
            "china_tech": ["tencent", "wechat", "gaming", "cloud", "china tech"],
            "generic": [ticker, "earnings", "guidance", "upgrade", "downgrade"],
        }
        terms = themes.get(theme, themes["generic"]).copy()
        macro_terms = ["fed", "cpi", "nfp", "rate decision", "inflation", "central bank"]
        for term in macro_terms:
            if term not in terms:
                terms.append(term)
        return [item for item in terms if item]

    async def _shared_result(self, key: str, factory: Callable[[], Awaitable[Any]]) -> Any:
        task = self._shared_tasks.get(key)
        if task is None:
            task = asyncio.create_task(factory())
            self._shared_tasks[key] = task
        try:
            return await task
        except Exception:
            self._shared_tasks.pop(key, None)
            raise

    def _is_btc_proxy_symbol(self, symbol: str) -> bool:
        return symbol.upper() in {item.upper() for item in self.config.btc_proxy_symbols}

    def _intel_theme_for_symbol(self, symbol: str) -> str:
        normalized = symbol.upper()
        if normalized in {"US.IBIT", "US.BITO"}:
            return "btc_etf"
        if normalized == "US.MSTR":
            return "mstr"
        if normalized == "US.COIN":
            return "coin"
        if normalized in {"US.MARA", "US.RIOT"}:
            return "miners"
        if normalized in {"US.NVDA", "US.MSFT", "US.AMZN", "US.META"}:
            return "ai_infra"
        if normalized == "US.AAPL":
            return "consumer_tech"
        if normalized == "HK.00700":
            return "china_tech"
        return "generic"

    def _x_monitor_query_for_symbol(self, symbol: str) -> str:
        handles = [handle.strip().lstrip("@") for handle in self.config.x_monitor_accounts if handle.strip()]
        if not handles:
            return ""
        handles_query = " OR ".join(f"from:{handle}" for handle in handles)
        return f"({handles_query})"

    def _merge_ranked_items(self, items: list[NetworkIntelItem], enforce_age: bool = True) -> list[NetworkIntelItem]:
        merged_items: list[NetworkIntelItem] = []
        for item in items:
            prepared = self._prepare_item_for_merge(NetworkIntelItem(**asdict(item)), enforce_age=enforce_age)
            if prepared is not None:
                merged_items.append(prepared)
        merged_items.sort(key=lambda item: item.score, reverse=True)
        return merged_items

    def _prepare_item_for_merge(self, item: NetworkIntelItem, enforce_age: bool = True) -> NetworkIntelItem | None:
        age_minutes = self._age_minutes(item.published_at)
        explicit_link = bool((item.raw or {}).get("explicit_link"))
        reference_only = False
        reference_reason: str | None = None
        if enforce_age and age_minutes is not None and not explicit_link:
            if not self._is_within_age_limit(item.source, age_minutes):
                reference_reason = self._stale_reference_reason(item, age_minutes)
                if reference_reason is None:
                    return None
                reference_only = True

        recency_bonus = self._recency_bonus(age_minutes)
        if age_minutes is None:
            recency_bonus -= 18.0
        if reference_only and age_minutes is not None:
            item.score -= self._stale_reference_penalty(age_minutes)
        item.score += recency_bonus
        if item.raw is None:
            item.raw = {}
        item.raw["age_minutes"] = age_minutes
        item.raw["recency_bonus"] = round(recency_bonus, 4)
        item.raw["reference_only"] = reference_only
        if reference_reason:
            item.raw["reference_reason"] = reference_reason
        return item

    def _is_within_age_limit(self, source: str, age_minutes: float) -> bool:
        if source in {"xreach", "x_monitor", "whale_alert"}:
            return age_minutes <= self.config.network_intel_max_social_age_hours * 60.0
        if source == "financial_datasets":
            return age_minutes <= max(self.config.network_intel_max_news_age_hours * 60.0, 400.0 * 24.0 * 60.0)
        return age_minutes <= self.config.network_intel_max_news_age_hours * 60.0

    def _stale_reference_reason(self, item: NetworkIntelItem, age_minutes: float) -> str | None:
        if item.source == "financial_datasets":
            return "structured_reference"
        if age_minutes > self.config.network_intel_important_event_age_hours * 60.0:
            return None
        text = " ".join(part for part in [item.title, item.snippet] if part).lower()
        important_keywords = (
            "war",
            "warfare",
            "military",
            "missile",
            "airstrike",
            "attack",
            "invasion",
            "conflict",
            "ceasefire",
            "sanction",
            "sanctions",
            "tariff",
            "embargo",
            "nuclear",
            "terror",
            "bankruptcy",
            "chapter 11",
            "default",
            "liquidation",
            "fraud investigation",
            "state of emergency",
            "emergency meeting",
            "开战",
            "战争",
            "冲突",
            "袭击",
            "空袭",
            "导弹",
            "入侵",
            "制裁",
            "关税",
            "禁运",
            "核",
            "恐袭",
            "破产",
            "违约",
            "紧急状态",
        )
        if any(keyword in text for keyword in important_keywords):
            return "important_event"
        return None

    def _stale_reference_penalty(self, age_minutes: float) -> float:
        age_hours = max(age_minutes / 60.0, 0.0)
        return 80.0 + min(age_hours, self.config.network_intel_important_event_age_hours) * 2.5

    @staticmethod
    def _recency_bonus(age_minutes: float | None) -> float:
        if age_minutes is None:
            return 0.0
        if age_minutes <= 15:
            return 42.0
        if age_minutes <= 30:
            return 34.0
        if age_minutes <= 60:
            return 24.0
        if age_minutes <= 120:
            return 14.0
        if age_minutes <= 240:
            return 6.0
        if age_minutes <= 480:
            return -4.0
        return -18.0

    @classmethod
    def _age_minutes(cls, published_at: str | None) -> float | None:
        parsed = cls._parse_published_at(published_at)
        if parsed is None:
            return None
        return max((datetime.now(UTC) - parsed).total_seconds() / 60.0, 0.0)

    @staticmethod
    def _parse_published_at(published_at: str | None) -> datetime | None:
        if not published_at:
            return None

        value = str(published_at).strip()
        if not value:
            return None

        candidates = [value]
        if value.endswith("Z"):
            candidates.append(value[:-1] + "+00:00")
        if " " in value and "T" not in value:
            candidates.append(value.replace(" ", "T"))

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
                parsed = datetime.strptime(value, fmt)
            except ValueError:
                continue
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            else:
                parsed = parsed.astimezone(UTC)
            return parsed
        return None

    async def _translate_items(self, items: list[NetworkIntelItem]) -> None:
        if not items or not self.config.translate_to_zh:
            return
        texts: list[str] = []
        indexes: list[tuple[int, str]] = []
        for index, item in enumerate(items):
            texts.append(item.title)
            indexes.append((index, "title_zh"))
            if item.snippet:
                texts.append(item.snippet)
                indexes.append((index, "snippet_zh"))

        translated = await self.translator.translate_many(texts)
        for (index, field_name), value in zip(indexes, translated, strict=False):
            setattr(items[index], field_name, value)

    async def _fetch_google_news(self, query: str) -> tuple[list[NetworkIntelItem], str | None]:
        params = urllib.parse.urlencode(
            {
                "q": f"{query} {self._google_news_search_window()}",
                "hl": "en-US",
                "gl": "US",
                "ceid": "US:en",
            }
        )
        url = f"https://news.google.com/rss/search?{params}"

        try:
            async with httpx.AsyncClient(timeout=20, trust_env=True) as client:
                response = await client.get(url)
                response.raise_for_status()
            root = ET.fromstring(response.text)
        except Exception as exc:  # noqa: BLE001
            return [], str(exc)

        items: list[NetworkIntelItem] = []
        for node in root.findall("./channel/item")[: self.limit]:
            title = (node.findtext("title") or "").strip()
            link = (node.findtext("link") or "").strip() or None
            description = self._clean_html(node.findtext("description") or "")
            pub_date = (node.findtext("pubDate") or "").strip() or None
            if not title:
                continue
            items.append(
                NetworkIntelItem(
                    source="google_news",
                    title=title,
                    snippet=description or None,
                    url=link,
                    published_at=pub_date,
                    score=float(len(items) + 1) * -1.0,
                    raw={"title": title, "link": link, "description": description, "pub_date": pub_date},
                )
            )
        items.reverse()
        for index, item in enumerate(items):
            item.score = 100.0 - index
        return items, None

    def _google_news_search_window(self) -> str:
        hours = max(
            self.config.network_intel_max_news_age_hours,
            min(self.config.network_intel_important_event_age_hours, 72.0),
        )
        if hours >= 24.0:
            return f"when:{max(1, math.ceil(hours / 24.0))}d"
        return f"when:{max(1, math.ceil(hours))}h"

    def _fetch_yfinance_news(self, ticker: str) -> tuple[list[NetworkIntelItem], str | None]:
        try:
            rows = list((yf.Ticker(ticker).news or [])[: self.config.yfinance_news_limit])
        except Exception as exc:  # noqa: BLE001
            return [], str(exc)

        items: list[NetworkIntelItem] = []
        for row in rows:
            content = row.get("content") or {}
            title = str(content.get("title") or "").strip()
            if not title:
                continue
            summary = str(content.get("summary") or content.get("description") or "").strip() or None
            provider = (content.get("provider") or {}).get("displayName")
            canonical_url = (content.get("canonicalUrl") or {}).get("url")
            click_url = (content.get("clickThroughUrl") or {}).get("url")
            published_at = str(content.get("pubDate") or content.get("displayTime") or "").strip() or None
            items.append(
                NetworkIntelItem(
                    source="yfinance_news",
                    title=title if not provider else f"{title} - {provider}",
                    snippet=summary,
                    url=click_url or canonical_url,
                    published_at=published_at,
                    score=120.0 - len(items),
                    raw=row,
                )
            )
        return items, None

    async def _fetch_alpha_vantage_snapshot(
        self, ticker: str
    ) -> tuple[list[NetworkIntelItem], dict[str, Any] | None, str | None]:
        if not self.config.alpha_vantage_enabled:
            return [], {"enabled": False, "status": "disabled"}, None
        if not self.config.alpha_vantage_api_key:
            return [], {"enabled": False, "status": "skipped", "reason": "missing_api_key"}, None

        try:
            async with httpx.AsyncClient(
                timeout=self.config.alpha_vantage_timeout_seconds,
                trust_env=True,
            ) as client:
                market_task = client.get(
                    self.config.alpha_vantage_base_url,
                    params={
                        "function": "MARKET_STATUS",
                        "apikey": self.config.alpha_vantage_api_key,
                    },
                )
                news_task = client.get(
                    self.config.alpha_vantage_base_url,
                    params={
                        "function": "NEWS_SENTIMENT",
                        "tickers": ticker,
                        "limit": self.config.alpha_vantage_news_limit,
                        "sort": "LATEST",
                        "apikey": self.config.alpha_vantage_api_key,
                    },
                )
                market_response, news_response = await asyncio.gather(market_task, news_task)
                market_response.raise_for_status()
                news_response.raise_for_status()
                market_payload = market_response.json()
                news_payload = news_response.json()
        except Exception as exc:  # noqa: BLE001
            return [], None, str(exc)

        if market_payload.get("Information") or news_payload.get("Information"):
            info = str(news_payload.get("Information") or market_payload.get("Information"))
            return [], {"enabled": True, "status": "limited", "info": info}, info
        if market_payload.get("Error Message") or news_payload.get("Error Message"):
            message = str(news_payload.get("Error Message") or market_payload.get("Error Message"))
            return [], {"enabled": True, "status": "error"}, message

        market_status = self._extract_alpha_vantage_market_status(market_payload)
        items: list[NetworkIntelItem] = []
        sentiment_scores: list[float] = []
        relevance_scores: list[float] = []

        for row in news_payload.get("feed", [])[: self.config.alpha_vantage_news_limit]:
            title = str(row.get("title") or "").strip()
            if not title:
                continue
            item_sentiment = self._extract_alpha_vantage_ticker_sentiment(row, ticker)
            relevance = self._extract_alpha_vantage_relevance(row, ticker)
            sentiment_scores.append(item_sentiment)
            relevance_scores.append(relevance)
            items.append(
                NetworkIntelItem(
                    source="alpha_vantage_news",
                    title=title,
                    snippet=str(row.get("summary") or "").strip() or None,
                    url=str(row.get("url") or "").strip() or None,
                    published_at=self._normalize_alpha_vantage_time(str(row.get("time_published") or "").strip() or None),
                    score=125.0 - len(items) + relevance * 12.0 + abs(item_sentiment) * 8.0,
                    raw=row,
                )
            )

        sentiment_average = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 0.0
        relevance_average = sum(relevance_scores) / len(relevance_scores) if relevance_scores else 0.0

        meta = {
            "enabled": True,
            "status": "ok",
            "market_status": market_status,
            "news_sentiment": {
                "article_count": len(items),
                "average_ticker_sentiment": round(sentiment_average, 4),
                "average_relevance": round(relevance_average, 4),
            },
        }
        return items, meta, None

    async def _fetch_financial_datasets_snapshot(
        self, symbol: str
    ) -> tuple[list[NetworkIntelItem], dict[str, Any] | None, str | None]:
        if not self.config.financial_datasets_enabled:
            return [], {"enabled": False, "status": "disabled"}, None
        if not self.config.financial_datasets_api_key:
            return [], {"enabled": False, "status": "skipped", "reason": "missing_api_key"}, None
        if not symbol.upper().startswith("US."):
            return [], {"enabled": False, "status": "skipped", "reason": "non_us_symbol"}, None

        ticker = symbol.split(".")[-1]
        headers = {"X-API-KEY": self.config.financial_datasets_api_key}
        try:
            async with httpx.AsyncClient(timeout=18, trust_env=True, headers=headers) as client:
                facts_task = client.get(
                    f"{self.config.financial_datasets_base_url.rstrip('/')}/company/facts",
                    params={"ticker": ticker},
                )
                metrics_task = client.get(
                    f"{self.config.financial_datasets_base_url.rstrip('/')}/financial-metrics/snapshot",
                    params={"ticker": ticker},
                )
                facts_response, metrics_response = await asyncio.gather(facts_task, metrics_task)
                facts_response.raise_for_status()
                metrics_response.raise_for_status()
                facts_payload = facts_response.json()
                metrics_payload = metrics_response.json()
        except Exception as exc:  # noqa: BLE001
            return [], None, str(exc)

        company_facts = facts_payload.get("company_facts") or facts_payload.get("companyFacts") or facts_payload
        metrics = metrics_payload.get("financial_metrics") or metrics_payload.get("financialMetrics") or metrics_payload
        if not isinstance(company_facts, dict) and not isinstance(metrics, dict):
            return [], None, "financial_datasets_empty_payload"

        market_cap = self._fd_float(company_facts, "market_cap", "marketCap")
        company_name = str(company_facts.get("company_name") or company_facts.get("name") or ticker).strip()
        sector = str(company_facts.get("sector") or "").strip() or None
        industry = str(company_facts.get("industry") or "").strip() or None
        price_to_earnings = self._fd_float(metrics, "price_to_earnings_ratio", "pe_ratio", "priceToEarningsRatio")
        price_to_sales = self._fd_float(metrics, "price_to_sales_ratio", "ps_ratio", "priceToSalesRatio")
        gross_margin = self._fd_float(metrics, "gross_margin", "grossMargin")
        operating_margin = self._fd_float(metrics, "operating_margin", "operatingMargin")
        net_margin = self._fd_float(metrics, "net_margin", "netMargin")
        revenue_growth = self._fd_float(metrics, "revenue_growth", "revenueGrowth")
        earnings_growth = self._fd_float(metrics, "earnings_growth", "earningsGrowth")
        return_on_equity = self._fd_float(metrics, "return_on_equity", "returnOnEquity")
        as_of = (
            str(metrics.get("report_period") or metrics.get("reportPeriod") or company_facts.get("updated_at") or "")
            .strip()
            or None
        )

        metric_parts: list[str] = []
        if sector:
            metric_parts.append(f"sector={sector}")
        if industry:
            metric_parts.append(f"industry={industry}")
        if market_cap is not None:
            metric_parts.append(f"market_cap={market_cap/1_000_000_000:.1f}B")
        if revenue_growth is not None:
            metric_parts.append(f"revenue_growth={revenue_growth:+.2%}")
        if earnings_growth is not None:
            metric_parts.append(f"earnings_growth={earnings_growth:+.2%}")
        if gross_margin is not None:
            metric_parts.append(f"gross_margin={gross_margin:.2%}")
        if operating_margin is not None:
            metric_parts.append(f"operating_margin={operating_margin:.2%}")
        if net_margin is not None:
            metric_parts.append(f"net_margin={net_margin:.2%}")
        if return_on_equity is not None:
            metric_parts.append(f"roe={return_on_equity:.2%}")
        if price_to_earnings is not None:
            metric_parts.append(f"pe={price_to_earnings:.1f}")
        if price_to_sales is not None:
            metric_parts.append(f"ps={price_to_sales:.1f}")

        title_suffix = f" | as_of={as_of}" if as_of else ""
        item = NetworkIntelItem(
            source="financial_datasets",
            title=f"Financial Datasets | {company_name}{title_suffix}",
            snippet="; ".join(metric_parts) or f"Structured fundamentals snapshot for {ticker}",
            url="https://financialdatasets.ai/",
            published_at=as_of,
            score=198.0 + abs(revenue_growth or 0.0) * 80.0 + abs(earnings_growth or 0.0) * 65.0,
            raw={
                "ticker": ticker,
                "company_facts": company_facts,
                "financial_metrics": metrics,
                "mcp_url": self.config.financial_datasets_mcp_url,
                "statement_limit": self.config.financial_datasets_statement_limit,
            },
        )
        meta = {
            "enabled": True,
            "status": "ok",
            "ticker": ticker,
            "company_name": company_name,
            "sector": sector,
            "industry": industry,
            "market_cap": market_cap,
            "revenue_growth": revenue_growth,
            "earnings_growth": earnings_growth,
            "gross_margin": gross_margin,
            "operating_margin": operating_margin,
            "net_margin": net_margin,
            "return_on_equity": return_on_equity,
            "price_to_earnings_ratio": price_to_earnings,
            "price_to_sales_ratio": price_to_sales,
            "as_of": as_of,
            "base_url": self.config.financial_datasets_base_url,
            "mcp_url": self.config.financial_datasets_mcp_url,
            "coverage_note": "vendor coverage includes roughly 17k stocks and up to 30 years of statements",
        }
        return [item], meta, None

    async def _fetch_monitored_x_snapshot(
        self, symbol: str
    ) -> tuple[list[NetworkIntelItem], dict[str, Any] | None, str | None]:
        if not self.config.x_monitor_enabled:
            return [], {"enabled": False, "status": "disabled"}, None
        if not self.config.x_monitor_accounts:
            return [], {"enabled": False, "status": "skipped", "reason": "missing_accounts"}, None

        return await self._shared_result(
            "x_monitor:accounts",
            lambda: asyncio.to_thread(self._fetch_monitored_x_snapshot_sync),
        )

    def _fetch_monitored_x_snapshot_sync(self) -> tuple[list[NetworkIntelItem], dict[str, Any] | None, str | None]:
        handles = [handle.strip().lstrip("@") for handle in self.config.x_monitor_accounts if handle.strip()]
        if not handles:
            return [], {"enabled": False, "status": "skipped", "reason": "empty_accounts"}, None
        items: list[NetworkIntelItem] = []
        errors: list[str] = []
        for index, handle in enumerate(handles):
            query = f"from:{handle}"
            fetched, error = self._fetch_xreach_search(
                query=query,
                source="x_monitor",
                limit=max(3, int(self.config.x_monitor_posts_limit / max(len(handles), 1))),
                score_boost=210.0 - index * 4.0,
            )
            if fetched:
                items.extend(fetched)
            if error:
                errors.append(f"{handle}: {error}")
        items.sort(key=lambda item: item.score, reverse=True)
        meta = {
            "enabled": True,
            "status": "ok" if not errors else "partial",
            "accounts": handles,
            "query": " | ".join(f"from:{handle}" for handle in handles),
            "count": len(items),
        }
        return items[: max(self.limit, self.config.x_monitor_posts_limit)], meta, "; ".join(errors) or None

    def _apply_source_topic_filter(self, items: list[NetworkIntelItem], symbol: str) -> list[NetworkIntelItem]:
        theme = self._intel_theme_for_symbol(symbol)
        terms = {term.lower() for term in self._theme_terms(theme, symbol)}
        if not terms:
            return items
        filtered: list[NetworkIntelItem] = []
        for item in items:
            text = f"{item.title} {item.snippet or ''}".lower()
            if any(term in text for term in terms):
                filtered.append(item)
        return filtered or items

    async def _fetch_whale_alert_snapshot(
        self, symbol: str
    ) -> tuple[list[NetworkIntelItem], dict[str, Any] | None, str | None]:
        if not self.config.whale_alert_enabled:
            return [], {"enabled": False, "status": "disabled"}, None

        return await self._shared_result(
            "whale_alert:btc",
            lambda: asyncio.to_thread(self._fetch_whale_alert_snapshot_sync),
        )

    def _fetch_whale_alert_snapshot_sync(self) -> tuple[list[NetworkIntelItem], dict[str, Any] | None, str | None]:
        handle = self.config.whale_alert_handle.strip().lstrip("@") or "whale_alert"
        query = f"from:{handle}"
        items, error = self._fetch_xreach_search(
            query=query,
            source="whale_alert",
            limit=max(self.limit, 10),
            score_boost=240.0,
        )

        filtered: list[NetworkIntelItem] = []
        net_exchange_bias = 0.0
        for item in items:
            transfer = self._parse_whale_alert_transfer(item.title or item.snippet or "")
            if transfer is None:
                continue
            amount_btc = float(transfer.get("amount_btc") or 0.0)
            usd_value = float(transfer.get("usd_value") or 0.0)
            if amount_btc < self.config.whale_alert_min_btc and usd_value < self.config.whale_alert_min_usd:
                continue
            transfer["exchange_bias"] = self._whale_exchange_bias(transfer)
            net_exchange_bias += float(transfer["exchange_bias"])
            item.raw = {**(item.raw or {}), "whale_transfer": transfer}
            filtered.append(item)

        meta = {
            "enabled": True,
            "status": "ok" if not error else "error",
            "handle": handle,
            "query": query,
            "count": len(filtered),
            "min_btc": self.config.whale_alert_min_btc,
            "min_usd": self.config.whale_alert_min_usd,
            "net_exchange_bias": round(net_exchange_bias, 4),
        }
        return filtered, meta, error

    async def _fetch_btc_etf_flow_snapshot(
        self, symbol: str
    ) -> tuple[list[NetworkIntelItem], dict[str, Any] | None, str | None]:
        if not self._is_btc_proxy_symbol(symbol):
            return [], {"enabled": False, "status": "skipped", "reason": "not_btc_proxy"}, None
        if not self.config.btc_etf_flow_enabled:
            return [], {"enabled": False, "status": "disabled"}, None

        parsed, error = await self._shared_result(
            "btc_etf_flow:table",
            self._fetch_btc_etf_flow_table,
        )
        if error or parsed is None:
            return [], None, error or "failed_to_parse_farside_btc_etf_flow"

        latest_date = parsed["latest_date"]
        latest_total = float(parsed["latest_total"] or 0.0)
        latest_flows = parsed["latest_flows"]
        tracked_funds = {fund.upper() for fund in self.config.btc_etf_funds}
        if tracked_funds:
            latest_flows = {fund: flow for fund, flow in latest_flows.items() if fund.upper() in tracked_funds}
            latest_total = sum(latest_flows.values())
        ticker = symbol.split(".")[-1]
        symbol_flow = float(latest_flows.get(ticker) or 0.0)
        top_positive = sorted(
            ((fund, flow) for fund, flow in latest_flows.items() if flow > 0.0),
            key=lambda item: item[1],
            reverse=True,
        )[:3]
        top_negative = sorted(
            ((fund, flow) for fund, flow in latest_flows.items() if flow < 0.0),
            key=lambda item: item[1],
        )[:2]

        snippet_parts = [
            f"latest_total={latest_total:+.1f} US$m on {latest_date}",
            f"{ticker}={symbol_flow:+.1f} US$m" if ticker in latest_flows else "symbol_flow=n/a",
        ]
        if top_positive:
            snippet_parts.append(
                "top_inflows=" + ", ".join(f"{fund}{flow:+.1f}" for fund, flow in top_positive)
            )
        if top_negative:
            snippet_parts.append(
                "top_outflows=" + ", ".join(f"{fund}{flow:+.1f}" for fund, flow in top_negative)
            )

        published_at = self._parse_farside_date(latest_date)
        item = NetworkIntelItem(
            source="btc_etf_flow",
            title=f"BTC ETF flow {latest_total:+.1f} US$m | {latest_date}",
            snippet="; ".join(snippet_parts),
            url="https://farside.co.uk/btc/",
            published_at=published_at,
            score=260.0 + abs(latest_total) * 0.12 + abs(symbol_flow) * 0.08,
            raw={
                "latest_date": latest_date,
                "latest_total": latest_total,
                "latest_flows": latest_flows,
                "top_positive": top_positive,
                "top_negative": top_negative,
                "tracked_funds": sorted(tracked_funds),
            },
        )
        meta = {
            "enabled": True,
            "status": "ok",
            "latest_date": latest_date,
            "latest_total": latest_total,
            "latest_flows": latest_flows,
            "top_positive": top_positive,
            "top_negative": top_negative,
            "symbol": symbol,
            "symbol_flow": symbol_flow,
            "lookback_days": self.config.btc_etf_flow_lookback_days,
            "source_url": "https://farside.co.uk/btc/",
        }
        return [item], meta, None

    async def _fetch_btc_etf_flow_table(self) -> tuple[dict[str, Any] | None, str | None]:
        try:
            async with httpx.AsyncClient(timeout=20, trust_env=True) as client:
                response = await client.get(self.config.btc_etf_flow_jina_url)
                response.raise_for_status()
                text = response.text
        except Exception as exc:  # noqa: BLE001
            return None, str(exc)

        parsed = self._parse_btc_etf_flow_markdown(text)
        if parsed is None:
            return None, "failed_to_parse_farside_btc_etf_flow"
        return parsed, None

    async def _fetch_macro_event_snapshot(
        self, symbol: str
    ) -> tuple[list[NetworkIntelItem], dict[str, Any] | None, str | None]:
        if not self.config.macro_event_enabled:
            return [], {"enabled": False, "status": "disabled"}, None

        fed_result, cpi_result, nfp_result, calendar_result, rss_result = await asyncio.gather(
            self._shared_result("macro:fed", self._fetch_fed_speeches),
            self._shared_result("macro:cpi", self._fetch_cpi_release),
            self._shared_result("macro:nfp", self._fetch_nfp_release),
            self._shared_result("macro:calendar", self._fetch_macro_calendar_snapshot),
            self._shared_result("macro:rss", self._fetch_macro_rss_snapshot),
        )
        fed_items, fed_meta, fed_error = fed_result
        cpi_items, cpi_meta, cpi_error = cpi_result
        nfp_items, nfp_meta, nfp_error = nfp_result
        calendar_items, calendar_meta, calendar_error = calendar_result
        rss_items, rss_meta, rss_error = rss_result
        merged = [*calendar_items, *rss_items, *fed_items, *cpi_items, *nfp_items]
        meta = {
            "enabled": True,
            "status": "ok" if not any([fed_error, cpi_error, nfp_error, calendar_error, rss_error]) else "partial",
            "fed": fed_meta,
            "cpi": cpi_meta,
            "nfp": nfp_meta,
            "calendar": calendar_meta,
            "rss": rss_meta,
        }
        errors = "; ".join(
            f"{name}={error}"
            for name, error in [
                ("calendar", calendar_error),
                ("rss", rss_error),
                ("fed", fed_error),
                ("cpi", cpi_error),
                ("nfp", nfp_error),
            ]
            if error
        )
        return merged[: self.config.macro_event_limit], meta, errors or None

    async def _fetch_fed_speeches(self) -> tuple[list[NetworkIntelItem], dict[str, Any] | None, str | None]:
        try:
            async with httpx.AsyncClient(timeout=20, trust_env=True) as client:
                response = await client.get(self.config.macro_fed_rss_url)
                response.raise_for_status()
            root = ET.fromstring(response.text)
        except Exception as exc:  # noqa: BLE001
            return [], None, str(exc)

        items: list[NetworkIntelItem] = []
        for index, node in enumerate(root.findall("./channel/item")[: self.config.macro_event_limit]):
            title = (node.findtext("title") or "").strip()
            link = (node.findtext("link") or "").strip() or None
            description = self._clean_html(node.findtext("description") or "")
            pub_date = (node.findtext("pubDate") or "").strip() or None
            if not title:
                continue
            items.append(
                NetworkIntelItem(
                    source="macro_event",
                    title=f"Fed speech | {title}",
                    snippet=description or None,
                    url=link,
                    published_at=pub_date,
                    score=225.0 - index * 4.0,
                    raw={"macro_type": "fed_speech", "title": title, "description": description, "link": link},
                )
            )
        return items, {"rss_url": self.config.macro_fed_rss_url, "count": len(items)}, None

    async def _fetch_macro_calendar_snapshot(
        self,
    ) -> tuple[list[NetworkIntelItem], dict[str, Any] | None, str | None]:
        sources = [url.strip() for url in self.config.macro_calendar_sources if url.strip()]
        keywords = [item.strip() for item in self.config.macro_calendar_keywords if item.strip()]
        if not sources or not keywords:
            return [], {"enabled": False, "status": "skipped", "reason": "missing_sources_or_keywords"}, None

        items: list[NetworkIntelItem] = []
        errors: list[str] = []
        for url in sources:
            try:
                async with httpx.AsyncClient(timeout=25, trust_env=True) as client:
                    response = await client.get(url)
                    response.raise_for_status()
                    text = response.text
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{url}: {exc}")
                continue

            extracted = self._extract_macro_calendar_items(text, url, keywords)
            items.extend(extracted)

        items.sort(key=lambda item: item.score, reverse=True)
        meta = {
            "enabled": True,
            "status": "ok" if not errors else "partial",
            "sources": sources,
            "keywords": keywords,
            "count": len(items),
        }
        return items[: self.config.macro_event_limit], meta, "; ".join(errors) or None

    async def _fetch_macro_rss_snapshot(
        self,
    ) -> tuple[list[NetworkIntelItem], dict[str, Any] | None, str | None]:
        sources = [url.strip() for url in self.config.macro_rss_sources if url.strip()]
        return self._fetch_rss_bundle(
            sources=sources,
            source_key="macro_rss",
            title_prefix="Macro RSS",
            score_base=220.0,
            limit=self.config.macro_event_limit,
        )

    async def _fetch_etf_news_snapshot(
        self, symbol: str
    ) -> tuple[list[NetworkIntelItem], dict[str, Any] | None, str | None]:
        sources = [url.strip() for url in self.config.etf_news_sources if url.strip()]
        if not sources:
            return [], {"enabled": False, "status": "skipped", "reason": "missing_sources"}, None
        return self._fetch_rss_bundle(
            sources=sources,
            source_key="etf_news",
            title_prefix="ETF news",
            score_base=215.0,
            limit=max(self.limit, 6),
            extra_raw={"symbol": symbol},
        )

    async def _fetch_announcement_snapshot(
        self, symbol: str
    ) -> tuple[list[NetworkIntelItem], dict[str, Any] | None, str | None]:
        sources = [url.strip() for url in self.config.announcement_sources if url.strip()]
        if not sources:
            return [], {"enabled": False, "status": "skipped", "reason": "missing_sources"}, None
        return self._fetch_rss_bundle(
            sources=sources,
            source_key="announcement",
            title_prefix="Announcement",
            score_base=205.0,
            limit=max(self.limit, 6),
            extra_raw={"symbol": symbol},
        )

    def _fetch_rss_bundle(
        self,
        sources: list[str],
        source_key: str,
        title_prefix: str,
        score_base: float,
        limit: int,
        extra_raw: dict[str, Any] | None = None,
    ) -> tuple[list[NetworkIntelItem], dict[str, Any] | None, str | None]:
        if not sources:
            return [], {"enabled": False, "status": "skipped", "reason": "missing_sources"}, None

        items: list[NetworkIntelItem] = []
        errors: list[str] = []
        for url in sources:
            try:
                response = httpx.get(url, timeout=20, follow_redirects=True)
                response.raise_for_status()
                root = ET.fromstring(response.text)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{url}: {exc}")
                continue

            for index, node in enumerate(root.findall("./channel/item")[: limit]):
                title = (node.findtext("title") or "").strip()
                link = (node.findtext("link") or "").strip() or None
                description = self._clean_html(node.findtext("description") or "")
                pub_date = (node.findtext("pubDate") or "").strip() or None
                if not title:
                    continue
                raw_meta = {"title": title, "link": link, "feed": url}
                if extra_raw:
                    raw_meta.update(extra_raw)
                items.append(
                    NetworkIntelItem(
                        source=source_key,
                        title=f"{title_prefix} | {title}",
                        snippet=description or None,
                        url=link,
                        published_at=pub_date,
                        score=score_base - index * 2.0,
                        raw=raw_meta,
                    )
                )

        items.sort(key=lambda item: item.score, reverse=True)
        meta = {
            "enabled": True,
            "status": "ok" if not errors else "partial",
            "sources": sources,
            "count": len(items),
        }
        return items[: limit], meta, "; ".join(errors) or None

    def _extract_macro_calendar_items(
        self, text: str, url: str, keywords: list[str]
    ) -> list[NetworkIntelItem]:
        lowered = text.lower()
        results: list[NetworkIntelItem] = []
        for keyword in keywords:
            if keyword.lower() not in lowered:
                continue
            results.append(
                NetworkIntelItem(
                    source="macro_calendar",
                    title=f"Macro calendar | {keyword}",
                    snippet=f"Detected keyword '{keyword}' in {url}",
                    url=url.replace("r.jina.ai/http://", "http://"),
                    published_at=datetime.now(UTC).isoformat(),
                    score=210.0,
                    raw={"macro_type": "calendar", "keyword": keyword, "source_url": url},
                )
            )
        return results

    async def _fetch_cpi_release(self) -> tuple[list[NetworkIntelItem], dict[str, Any] | None, str | None]:
        return await self._fetch_bls_release_summary(
            url=self.config.macro_cpi_release_url,
            label="CPI",
            summary_prefix="The Consumer Price Index for All Urban Consumers",
        )

    async def _fetch_nfp_release(self) -> tuple[list[NetworkIntelItem], dict[str, Any] | None, str | None]:
        return await self._fetch_bls_release_summary(
            url=self.config.macro_nfp_release_url,
            label="NFP",
            summary_prefix="Total nonfarm payroll employment",
        )

    async def _fetch_bls_release_summary(
        self,
        url: str,
        label: str,
        summary_prefix: str,
    ) -> tuple[list[NetworkIntelItem], dict[str, Any] | None, str | None]:
        try:
            async with httpx.AsyncClient(timeout=25, trust_env=True) as client:
                response = await client.get(url)
                response.raise_for_status()
                text = response.text
        except Exception as exc:  # noqa: BLE001
            return [], None, str(exc)

        parsed = self._parse_bls_release_text(text, label=label, summary_prefix=summary_prefix)
        if parsed is None:
            return [], None, f"failed_to_parse_{label.lower()}_release"

        item = NetworkIntelItem(
            source="macro_event",
            title=f"{label} release | {parsed['title']}",
            snippet=parsed["summary"],
            url=parsed["url"],
            published_at=parsed["published_at"],
            score=235.0,
            raw={"macro_type": label.lower(), **parsed},
        )
        meta = {
            "title": parsed["title"],
            "published_at": parsed["published_at"],
            "next_release": parsed.get("next_release"),
            "url": parsed["url"],
        }
        return [item], meta, None

    def _fetch_xreach_posts(self, query: str) -> tuple[list[NetworkIntelItem], str | None]:
        if not self.config.xreach_enabled:
            return [], "xreach_disabled"
        return self._fetch_xreach_search(
            query=query,
            source="xreach",
            limit=self.limit,
        )

    def _fetch_direct_x_urls(
        self, urls: list[str]
    ) -> tuple[list[NetworkIntelItem], dict[str, Any] | None, str | None]:
        if not urls:
            return [], {"enabled": False, "status": "skipped", "reason": "no_urls"}, None

        items: list[NetworkIntelItem] = []
        errors: list[str] = []
        for index, url in enumerate(urls):
            item, error = self._fetch_xreach_tweet(url, score_boost=230.0 - index * 3.0)
            if item is not None:
                items.append(item)
            elif error:
                errors.append(error)

        meta = {
            "enabled": True,
            "status": "ok" if items else "error",
            "count": len(items),
            "urls": urls,
        }
        return items, meta, "; ".join(errors) or None

    def _fetch_xreach_search(
        self,
        query: str,
        source: str,
        limit: int,
        score_boost: float = 0.0,
    ) -> tuple[list[NetworkIntelItem], str | None]:
        safe_query = query.replace("\n", " ").strip()
        safe_query = safe_query.replace("'", "''")
        command = (
            "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; "
            "$OutputEncoding = [System.Text.Encoding]::UTF8; "
            f"xreach search '{safe_query}' -n {max(limit * 2, limit + 2)} --json"
        )

        try:
            completed = subprocess.run(
                ["powershell", "-NoProfile", "-Command", command],
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=40,
            )
            payload = json.loads(completed.stdout.lstrip("\ufeff"))
        except Exception as exc:  # noqa: BLE001
            return [], str(exc)

        items: list[NetworkIntelItem] = []
        for row in payload.get("items", []):
            text = str(row.get("text") or "").strip()
            created_at = str(row.get("createdAt") or "")
            if not text or self._looks_like_spam(text) or not self._is_recent_x_post(created_at, self.config):
                continue
            raw = dict(row)
            raw["query"] = query
            items.append(
                NetworkIntelItem(
                    source=source,
                    title=text.splitlines()[0][:220],
                    snippet=text[:600],
                    url=f"https://x.com/i/status/{row.get('id')}" if row.get("id") else None,
                    published_at=created_at,
                    score=score_boost
                    + float(row.get("likeCount") or 0)
                    + float(row.get("retweetCount") or 0) * 2
                    + float(row.get("viewCount") or 0) / 1000,
                    raw=raw,
                )
            )
            if len(items) >= limit:
                break

        return items, None

    def _fetch_xreach_tweet(self, url: str, score_boost: float = 230.0) -> tuple[NetworkIntelItem | None, str | None]:
        safe_url = str(url).replace("\n", " ").strip()
        if not safe_url:
            return None, "empty_x_url"
        command = (
            "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; "
            "$OutputEncoding = [System.Text.Encoding]::UTF8; "
            f"xreach tweet '{safe_url}' --json"
        )
        try:
            completed = subprocess.run(
                ["powershell", "-NoProfile", "-Command", command],
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=35,
            )
            payload = json.loads(completed.stdout.lstrip("\ufeff"))
        except Exception as exc:  # noqa: BLE001
            return None, f"{safe_url}: {exc}"

        text = str(payload.get("text") or "").strip()
        if not text:
            return None, f"{safe_url}: empty_text"

        created_at = str(payload.get("createdAt") or "").strip() or None
        tweet_id = str(payload.get("id") or "").strip() or None
        item_url = f"https://x.com/i/status/{tweet_id}" if tweet_id else safe_url
        user = payload.get("user") or {}
        screen_name = str(user.get("screenName") or "").strip()
        author_prefix = f"@{screen_name}: " if screen_name else ""
        payload["explicit_link"] = True
        payload["input_url"] = safe_url
        return (
            NetworkIntelItem(
                source="x_link",
                title=(author_prefix + text.splitlines()[0])[:220],
                snippet=text[:900],
                url=item_url,
                published_at=created_at,
                score=score_boost
                + float(payload.get("likeCount") or 0.0)
                + float(payload.get("retweetCount") or 0.0) * 2.0
                + float(payload.get("viewCount") or 0.0) / 1000.0,
                raw=payload,
            ),
            None,
        )

    @staticmethod
    def _clean_html(value: str) -> str:
        text = re.sub(r"<[^>]+>", " ", value)
        text = html.unescape(text)
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _parse_whale_alert_transfer(text: str) -> dict[str, Any] | None:
        pattern = re.compile(
            r"(?P<amount_btc>[\d,]+(?:\.\d+)?)\s+#BTC\s+\((?P<usd_value>[\d,]+(?:\.\d+)?)\s+USD\)\s+transferred from\s+(?P<from_entity>.+?)\s+to\s+(?P<to_entity>.+)",
            re.IGNORECASE,
        )
        match = pattern.search(text.replace("\n", " "))
        if not match:
            return None

        def _to_float(value: str) -> float:
            return float(value.replace(",", "").strip())

        return {
            "amount_btc": _to_float(match.group("amount_btc")),
            "usd_value": _to_float(match.group("usd_value")),
            "from_entity": match.group("from_entity").strip(),
            "to_entity": match.group("to_entity").strip(),
        }

    @staticmethod
    def _whale_exchange_bias(transfer: dict[str, Any]) -> float:
        exchange_keywords = ("coinbase", "kraken", "binance", "bitfinex", "okx", "okex", "kucoin", "bybit", "institutional")
        from_entity = str(transfer.get("from_entity") or "").lower()
        to_entity = str(transfer.get("to_entity") or "").lower()
        if any(keyword in to_entity for keyword in exchange_keywords):
            return -1.0
        if any(keyword in from_entity for keyword in exchange_keywords):
            return 1.0
        return 0.0

    def _parse_btc_etf_flow_markdown(self, text: str) -> dict[str, Any] | None:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        header_line = next((line for line in lines if line.startswith("|  |") and "IBIT" not in line and "Blackrock" in line), None)
        codes_line = next((line for line in lines if line.startswith("|  | IBIT")), None)
        if header_line is None or codes_line is None:
            return None

        codes = [cell.strip() for cell in codes_line.strip("|").split("|")]
        fund_codes = codes[1:-1]
        latest_row: list[str] | None = None
        row_pattern = re.compile(r"^\|\s\d{2}\s[A-Za-z]{3}\s\d{4}\s\|")
        for line in lines:
            if row_pattern.match(line):
                latest_row = [cell.strip() for cell in line.strip("|").split("|")]
        if latest_row is None or len(latest_row) < len(fund_codes) + 2:
            return None

        latest_date = latest_row[0]
        values = latest_row[1:]
        latest_total = self._parse_parenthesized_number(values[-1])
        latest_flows: dict[str, float] = {}
        for code, raw_value in zip(fund_codes, values[:-1], strict=False):
            latest_flows[code] = self._parse_parenthesized_number(raw_value)

        return {
            "latest_date": latest_date,
            "latest_total": latest_total,
            "latest_flows": latest_flows,
        }

    @staticmethod
    def _parse_parenthesized_number(value: str) -> float:
        stripped = value.strip()
        negative = stripped.startswith("(") and stripped.endswith(")")
        cleaned = stripped.strip("()").replace(",", "")
        try:
            parsed = float(cleaned)
        except ValueError:
            return 0.0
        return -parsed if negative else parsed

    @staticmethod
    def _parse_farside_date(value: str) -> str | None:
        try:
            parsed = datetime.strptime(value, "%d %b %Y")
        except ValueError:
            return None
        return parsed.replace(tzinfo=UTC).isoformat()

    def _parse_bls_release_text(
        self,
        text: str,
        label: str,
        summary_prefix: str,
    ) -> dict[str, Any] | None:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        title_line = next((line for line in lines if line.startswith("Title:")), None)
        url_line = next((line for line in lines if line.startswith("URL Source:")), None)
        published_line = next(
            (line for line in lines if "8:30 a.m. (ET)" in line and any(month in line for month in ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"))),
            None,
        )
        summary_line = next((line for line in lines if summary_prefix in line), None)
        next_release_line = next((line for line in lines if "scheduled to be released on" in line.lower()), None)
        if title_line is None or url_line is None or summary_line is None:
            return None

        published_at = None
        if published_line:
            match = re.search(
                r"(Monday|Tuesday|Wednesday|Thursday|Friday),\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})",
                published_line,
            )
            if match:
                try:
                    published_at = datetime.strptime(match.group(2), "%B %d, %Y").replace(tzinfo=UTC).isoformat()
                except ValueError:
                    published_at = None

        next_release = None
        if next_release_line:
            match = re.search(r"released on\s+(Monday|Tuesday|Wednesday|Thursday|Friday),\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", next_release_line, re.IGNORECASE)
            if match:
                next_release = f"{match.group(1)}, {match.group(2)}"

        return {
            "title": title_line.replace("Title:", "").strip(),
            "url": url_line.replace("URL Source:", "").strip(),
            "published_at": published_at,
            "summary": summary_line,
            "next_release": next_release,
            "label": label,
        }

    @staticmethod
    def _looks_like_spam(text: str) -> bool:
        lowered = text.lower()
        blocked_keywords = [
            "join now",
            "discord",
            "free options",
            "copy trade",
            "paid group",
            "whatsapp",
            "telegram",
        ]
        return any(keyword in lowered for keyword in blocked_keywords)

    def _source_label(self, source: str) -> str:
        if self.config.translate_to_zh:
            return {
                "btc_etf_flow": "ETF 资金流",
                "macro_event": "宏观事件",
                "macro_rss": "宏观 RSS",
                "macro_calendar": "宏观日历",
                "whale_alert": "Whale Alert",
                "x_link": "指定 X 链接",
                "x_monitor": "指定 X 账号",
                "financial_datasets": "Financial Datasets",
                "alpha_vantage_news": "Alpha Vantage",
                "yfinance_news": "Yahoo 财经",
                "google_news": "谷歌新闻",
                "xreach": "X 舆情",
                "etf_news": "ETF 新闻",
                "announcement": "公告",
            }.get(source, source)
        return {
            "btc_etf_flow": "ETF Flow",
            "macro_event": "Macro Event",
            "macro_rss": "Macro RSS",
            "macro_calendar": "Macro Calendar",
            "whale_alert": "Whale Alert",
            "x_link": "Direct X Link",
            "x_monitor": "Monitored X",
            "financial_datasets": "Financial Datasets",
            "alpha_vantage_news": "Alpha Vantage",
            "yfinance_news": "Yahoo Finance",
            "google_news": "Google News",
            "xreach": "X Sentiment",
            "etf_news": "ETF News",
            "announcement": "Announcements",
        }.get(source, source)

    def _display_title(self, item: NetworkIntelItem) -> str:
        if self.config.translate_to_zh:
            return item.title_zh or item.title
        return item.title

    def _display_snippet(self, item: NetworkIntelItem) -> str:
        if self.config.translate_to_zh:
            return item.snippet_zh or item.snippet or ""
        return item.snippet or ""

    @staticmethod
    def _extract_alpha_vantage_market_status(payload: dict[str, Any]) -> dict[str, Any] | None:
        for row in payload.get("markets", []) or []:
            if (
                str(row.get("market_type") or "").strip().lower() == "equity"
                and str(row.get("region") or "").strip().lower() == "united states"
            ):
                return {
                    "market_type": row.get("market_type"),
                    "region": row.get("region"),
                    "current_status": row.get("current_status"),
                    "local_open": row.get("local_open"),
                    "local_close": row.get("local_close"),
                    "notes": row.get("notes"),
                }
        return None

    @staticmethod
    def _extract_alpha_vantage_ticker_sentiment(row: dict[str, Any], ticker: str) -> float:
        for entry in row.get("ticker_sentiment", []) or []:
            if str(entry.get("ticker") or "").strip().upper() == ticker.upper():
                try:
                    return float(entry.get("ticker_sentiment_score") or 0.0)
                except (TypeError, ValueError):
                    return 0.0
        try:
            return float(row.get("overall_sentiment_score") or 0.0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _extract_alpha_vantage_relevance(row: dict[str, Any], ticker: str) -> float:
        for entry in row.get("ticker_sentiment", []) or []:
            if str(entry.get("ticker") or "").strip().upper() == ticker.upper():
                try:
                    return float(entry.get("relevance_score") or 0.0)
                except (TypeError, ValueError):
                    return 0.0
        return 0.0

    @staticmethod
    def _normalize_alpha_vantage_time(value: str | None) -> str | None:
        if not value:
            return None
        try:
            parsed = datetime.strptime(value, "%Y%m%dT%H%M%S")
        except ValueError:
            return value
        return parsed.replace(tzinfo=UTC).isoformat()

    def _fallback_sentiment(self, alpha_vantage_meta: dict[str, Any] | None) -> float:
        if not alpha_vantage_meta:
            return 0.0
        news_sentiment = alpha_vantage_meta.get("news_sentiment") or {}
        try:
            score = float(news_sentiment.get("average_ticker_sentiment") or 0.0)
        except (TypeError, ValueError):
            return 0.0
        return max(-1.0, min(1.0, score))

    def _render_alpha_vantage_brief(self, alpha_vantage_meta: dict[str, Any] | None) -> str | None:
        if not alpha_vantage_meta or alpha_vantage_meta.get("status") != "ok":
            return None
        news_sentiment = alpha_vantage_meta.get("news_sentiment") or {}
        market_status = alpha_vantage_meta.get("market_status") or {}
        article_count = int(news_sentiment.get("article_count") or 0)
        avg_sentiment = float(news_sentiment.get("average_ticker_sentiment") or 0.0)
        avg_relevance = float(news_sentiment.get("average_relevance") or 0.0)
        status = str(market_status.get("current_status") or "unknown").strip() or "unknown"
        return (
            f"[Alpha Vantage] US market={status}; article_count={article_count}; "
            f"avg_ticker_sentiment={avg_sentiment:.2f}; avg_relevance={avg_relevance:.2f}"
        )

    def _render_financial_datasets_brief(self, meta: dict[str, Any] | None) -> str | None:
        if not meta or meta.get("status") != "ok":
            return None
        parts = [f"[Financial Datasets] {meta.get('ticker') or 'n/a'}"]
        if meta.get("company_name"):
            parts.append(str(meta["company_name"]))
        if meta.get("market_cap") is not None:
            parts.append(f"market_cap={float(meta['market_cap'])/1_000_000_000:.1f}B")
        if meta.get("revenue_growth") is not None:
            parts.append(f"revenue_growth={float(meta['revenue_growth']):+.2%}")
        if meta.get("earnings_growth") is not None:
            parts.append(f"earnings_growth={float(meta['earnings_growth']):+.2%}")
        if meta.get("price_to_earnings_ratio") is not None:
            parts.append(f"pe={float(meta['price_to_earnings_ratio']):.1f}")
        if meta.get("as_of"):
            parts.append(f"as_of={meta['as_of']}")
        return "; ".join(parts)

    @staticmethod
    def _fd_float(payload: dict[str, Any], *keys: str) -> float | None:
        for key in keys:
            if key not in payload:
                continue
            value = payload.get(key)
            if value in (None, ""):
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _x_search_window_hours(config: AppConfig) -> float:
        return max(
            config.network_intel_primary_age_hours,
            config.network_intel_max_news_age_hours,
            config.network_intel_max_social_age_hours,
        )

    @classmethod
    def _is_recent_x_post(cls, created_at: str, config: AppConfig) -> bool:
        if not created_at:
            return False
        try:
            created = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
        except ValueError:
            return False
        return created >= datetime.now(UTC) - timedelta(hours=cls._x_search_window_hours(config))
