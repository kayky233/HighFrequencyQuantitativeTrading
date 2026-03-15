from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlparse

import httpx

from hfqt.agents.client import parse_json_from_llm_content
from hfqt.config import AppConfig
from hfqt.pipeline import QuantPipeline
from hfqt.schemas import InputEvent, TradeAction, TradeIntent
from hfqt.translation import ChineseTranslator


class LLMReasoner:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.pipeline = QuantPipeline(config)
        self.translator = ChineseTranslator(config)
        self.system_prompt = self._build_system_prompt(config.translate_to_zh)
        self.last_feature_snapshot = None
        self.last_alpha_signal = None
        self.last_model_selection: dict[str, Any] | None = None

    async def aclose(self) -> None:
        await self.translator.aclose()

    async def generate(self, event: InputEvent) -> TradeIntent:
        primary_model = self.config.llm_model_primary or self.config.llm_model
        if not self.config.llm_base_url or not primary_model:
            raise RuntimeError(
                "LLMReasoner requires HFQT_LLM_BASE_URL and HFQT_LLM_MODEL_PRIMARY/HFQT_LLM_MODEL. "
                "Use MockReasoner for ignition if no compatible model endpoint is ready."
            )
        features, alpha_signal = self.pipeline.analyze(event)
        self.last_feature_snapshot = features
        self.last_alpha_signal = alpha_signal

        route = self._resolve_model_route(event)
        self.last_model_selection = route
        try:
            parsed = await self._request_completion(event, route["model"], features, alpha_signal)
        except Exception as exc:  # noqa: BLE001
            retry_route = self._resolve_retry_route(route, exc)
            if retry_route is None:
                raise
            self.last_model_selection = retry_route
            parsed = await self._request_completion(event, retry_route["model"], features, alpha_signal)
            route = retry_route

        action = self._coerce_action(parsed.get("action"))
        quantity_value = parsed.get("quantity")
        if action == TradeAction.HOLD:
            quantity = 0.0
        elif quantity_value in {None, ""}:
            quantity = float(event.quantity or self.pipeline.sizing_engine.size(alpha_signal, features, event.price))
        else:
            quantity = float(quantity_value)
        limit_price = parsed.get("limit_price")
        if limit_price in {None, ""}:
            limit_price = None
        else:
            limit_price = float(limit_price)
            if limit_price <= 0:
                limit_price = None
        if limit_price is None:
            limit_price = event.price or ((event.metadata or {}).get("price_action") or {}).get("last_price")

        confidence = max(0.0, min(1.0, float(parsed.get("confidence", 0.5))))
        rationale = str(parsed.get("rationale") or self._default_rationale(self.config.translate_to_zh))
        if self.config.translate_to_zh and rationale and not self._contains_cjk(rationale):
            rationale = await self.translator.translate_to_zh(rationale)

        return TradeIntent(
            event_id=event.event_id,
            symbol=event.symbol,
            action=action,
            quantity=quantity,
            limit_price=limit_price,
            confidence=confidence,
            score=alpha_signal.score,
            regime=alpha_signal.regime,
            rationale=rationale,
            strategy_id="llm-openai-compatible-v3",
            metadata={
                "provider": self.config.llm_provider or "openai_compatible",
                "model": route["model"],
                "model_profile": route["profile"],
                "model_reason": route["reason"],
                "llm_mode": self.config.llm_mode,
                "fallback_used": bool(route.get("fallback_used")),
                "headline": event.headline,
                "source": event.source,
                "event_type": event.event_type.value,
                "price_action": (event.metadata or {}).get("price_action"),
                "source_quality": (event.metadata or {}).get("source_quality"),
                "history_match_count": len((event.metadata or {}).get("history_match") or []),
                "feature_snapshot": features.model_dump(mode="json"),
                "alpha_signal": alpha_signal.model_dump(mode="json"),
            },
        )

    async def _request_completion(self, event: InputEvent, model_name: str, features, alpha_signal) -> dict[str, Any]:
        payload = {
            "model": model_name,
            "temperature": self.config.llm_temperature,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": self.system_prompt,
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "symbol": event.symbol,
                            "headline": event.headline,
                            "body": event.body,
                            "sentiment": event.sentiment,
                            "price": event.price,
                            "quantity_hint": event.quantity,
                            "metadata": event.metadata,
                            "derived_features": features.model_dump(mode="json"),
                            "alpha_baseline": alpha_signal.model_dump(mode="json"),
                            "required_output_schema": {
                                "action": "BUY | SELL | HOLD",
                                "confidence": "0.0-1.0",
                                "quantity": "number, HOLD must be 0",
                                "limit_price": "positive number or null",
                            },
                        },
                        ensure_ascii=False,
                    ),
                },
            ],
        }
        headers = {"Content-Type": "application/json"}
        if self.config.llm_api_key:
            headers["Authorization"] = f"Bearer {self.config.llm_api_key}"

        parsed_url = urlparse(self.config.llm_base_url)
        trust_env = parsed_url.hostname not in {"127.0.0.1", "localhost"}

        async with httpx.AsyncClient(timeout=self.config.llm_timeout_seconds, trust_env=trust_env) as client:
            response = await client.post(
                f"{self.config.llm_base_url.rstrip('/')}/chat/completions",
                json=payload,
                headers=headers,
            )
            response.raise_for_status()
            content = self._extract_content(response.json())
            return parse_json_from_llm_content(content)

    def _resolve_model_route(self, event: InputEvent) -> dict[str, Any]:
        primary_model = self.config.llm_model_primary or self.config.llm_model
        fallback_model = self.config.llm_model_fallback
        mode = (self.config.llm_mode or "primary").strip().lower()
        if mode == "fallback" and fallback_model:
            return {
                "profile": "fallback",
                "model": fallback_model,
                "reason": "forced_fallback_mode",
                "fallback_used": True,
            }
        if mode == "auto" and fallback_model and self._should_prefer_fallback(event):
            return {
                "profile": "fallback",
                "model": fallback_model,
                "reason": "auto_fallback_prefilter",
                "fallback_used": True,
            }
        if mode == "fallback" and not fallback_model:
            return {
                "profile": "primary",
                "model": primary_model,
                "reason": "fallback_requested_but_not_configured",
                "fallback_used": False,
            }
        return {
            "profile": "primary",
            "model": primary_model,
            "reason": "default_primary_mode" if mode != "auto" else "auto_primary_mode",
            "fallback_used": False,
        }

    def _resolve_retry_route(self, current_route: dict[str, Any], exc: Exception) -> dict[str, Any] | None:
        fallback_model = self.config.llm_model_fallback
        if current_route.get("profile") == "fallback" or not fallback_model:
            return None
        if not self.config.llm_auto_retry_with_fallback:
            return None
        if not self._should_retry_with_fallback(exc):
            return None
        return {
            "profile": "fallback",
            "model": fallback_model,
            "reason": f"retry_after_primary_error:{type(exc).__name__}",
            "fallback_used": True,
        }

    def _should_prefer_fallback(self, event: InputEvent) -> bool:
        metadata = event.metadata or {}
        sources = metadata.get("sources") or {}
        body_chars = len((event.body or "").strip())
        item_count = len(metadata.get("items") or [])
        xreach_count = int(sources.get("xreach_count") or 0)
        return (
            body_chars >= self.config.llm_fallback_body_chars
            or item_count >= self.config.llm_fallback_item_count
            or xreach_count >= self.config.llm_fallback_xreach_count
        )

    @staticmethod
    def _should_retry_with_fallback(exc: Exception) -> bool:
        if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)):
            return True
        lowered = str(exc).lower()
        retry_keywords = (
            "out of memory",
            "cuda",
            "vram",
            "kv cache",
            "context length",
            "maximum context",
            "overloaded",
            "503",
            "429",
            "timeout",
            "timed out",
            "no healthy upstream",
        )
        return any(keyword in lowered for keyword in retry_keywords)

    @staticmethod
    def _extract_content(payload: dict[str, Any]) -> str:
        choices = payload.get("choices") or []
        if not choices:
            raise RuntimeError("No choices returned by LLM endpoint.")
        message = choices[0].get("message") or {}
        content = message.get("content")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    parts.append(str(item.get("text", "")))
            if parts:
                return "\n".join(parts)
        raise RuntimeError("Unsupported LLM response format.")

    @staticmethod
    def _strip_json_fence(content: str) -> str:
        text = content.strip()
        if text.startswith("```"):
            lines = text.splitlines()
            if lines and lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].startswith("```"):
                lines = lines[:-1]
            text = "\n".join(lines).strip()
        return text

    @staticmethod
    def _coerce_action(value: Any) -> TradeAction:
        normalized = str(value or "HOLD").strip().upper()
        if normalized == TradeAction.BUY.value:
            return TradeAction.BUY
        if normalized == TradeAction.SELL.value:
            return TradeAction.SELL
        return TradeAction.HOLD

    @staticmethod
    def _contains_cjk(text: str) -> bool:
        return any("\u4e00" <= char <= "\u9fff" for char in text)

    @staticmethod
    def _build_system_prompt(translate_to_zh: bool) -> str:
        if translate_to_zh:
            return (
                "你是一个美股事件驱动交易意图生成器。"
                "除了新闻文本，你还必须结合 metadata 里的 price_action、source_quality、history_match 共同判断。"
                "如果文本方向和量价特征一致，可以提高 confidence。"
                "如果文本方向和 price_action 明显冲突，不要强行给 BUY 或 SELL，应降低 confidence，必要时返回 HOLD。"
                "如果 source_quality 很低、来源单一，或 history_match 为空，也应降低 confidence。"
                "只返回 JSON。必须始终包含 action、confidence、quantity、limit_price 这 4 个字段；rationale 可选。"
                "action 只能是 BUY、SELL、HOLD。confidence 必须是 0 到 1 之间的小数。"
                "BUY/SELL 时 quantity 必须大于 0；HOLD 时 quantity 必须是 0。"
                "limit_price 必须是正数，或在无法确定时返回 null。"
                "rationale 必须使用自然、简洁的简体中文，并明确提到新闻、量价或历史对标中的关键依据。"
            )
        return (
            "You are an event-driven US equities trade-intent generator. "
            "You must combine the text signal with metadata.price_action, metadata.source_quality, and metadata.history_match. "
            "If text and price action align, confidence can increase. "
            "If text and price action conflict, reduce confidence and return HOLD when needed. "
            "If source quality is weak, sources are one-sided, or history_match is missing, reduce confidence. "
            "Return JSON only. You must always include action, confidence, quantity, and limit_price; rationale is optional. "
            "action must be BUY, SELL, or HOLD. confidence must be a decimal between 0 and 1. "
            "For BUY or SELL, quantity must be greater than 0. For HOLD, quantity must be 0. "
            "limit_price must be a positive number or null if unavailable. "
            "rationale must be concise, plain English and mention the key evidence."
        )

    @staticmethod
    def _default_rationale(translate_to_zh: bool) -> str:
        if translate_to_zh:
            return "模型已基于新闻、量价与历史相似事件生成交易意图。"
        return "Model generated a trade intent using text, price action, and prior-event context."
