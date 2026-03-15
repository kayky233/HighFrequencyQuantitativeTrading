from __future__ import annotations

from hfqt.agents.client import AgentRuntimeConfig, OpenAIJsonClient
from hfqt.config import AppConfig
from hfqt.schemas import InputEvent, TradeIntent


class RiskAgent:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        settings = config.agent_settings("risk")
        self.agent = AgentRuntimeConfig(**settings)
        self.client = OpenAIJsonClient(config.llm_timeout_seconds, config.llm_temperature)

    @property
    def enabled(self) -> bool:
        return self.agent.ready

    async def assess(
        self,
        event: InputEvent,
        intent: TradeIntent,
        prior_order_count: int,
        resolved_min_confidence: float,
        estimated_notional: float | None,
    ) -> dict | None:
        if not self.enabled:
            return None

        system_prompt = (
            "你是本地交易风控代理。你必须独立审查已有交易意图，并结合新闻、量价、来源质量、历史相似事件、"
            "当前置信度、数量、名义金额、当日订单数和基础风控阈值来判断是否放行。"
            "返回 JSON，只允许字段：approved, confidence_cap, quantity_cap, max_notional, risk_level, rationale。"
            "approved 为布尔值；confidence_cap、quantity_cap、max_notional 为数字或 null；"
            "risk_level 只能是 LOW、MEDIUM、HIGH；rationale 必须简洁说明放行或拦截原因。"
            "如果新闻与曲线冲突、来源弱、数量偏大、名义金额偏大，或者动作过激，请拒绝或收紧上限。"
        )
        if not self.config.translate_to_zh:
            system_prompt = (
                "You are a local trade risk agent. Independently review the trade intent using the news, price action, "
                "source quality, history match, confidence, quantity, notional, prior order count, and base thresholds. "
                "Return JSON only with approved, confidence_cap, quantity_cap, max_notional, risk_level, and rationale."
            )

        payload = {
            "event": event.model_dump(mode="json"),
            "intent": intent.model_dump(mode="json"),
            "risk_context": {
                "prior_order_count": prior_order_count,
                "resolved_min_confidence": resolved_min_confidence,
                "estimated_notional": estimated_notional,
                "max_notional_per_order": self.config.max_notional_per_order,
                "max_orders_per_day": self.config.max_orders_per_day,
            },
        }
        result = await self.client.complete_json(self.agent, system_prompt, payload)
        result["agent_model"] = self.agent.model
        result["agent_provider"] = self.agent.provider or "openai_compatible"
        return result
