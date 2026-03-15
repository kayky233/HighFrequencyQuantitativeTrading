from __future__ import annotations

from hfqt.agents.client import AgentRuntimeConfig, OpenAIJsonClient
from hfqt.config import AppConfig
from hfqt.schemas import InputEvent, ReviewDecision, ReviewStatus, TradeAction, TradeIntent


class ReviewAgent:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        primary_settings = config.agent_settings("review")
        self.agent = AgentRuntimeConfig(**primary_settings)
        self.client = OpenAIJsonClient(config.llm_timeout_seconds, config.llm_temperature)

        secondary_settings = config.agent_settings("review_secondary")
        self.secondary_agent = AgentRuntimeConfig(**secondary_settings)
        self.secondary_client = OpenAIJsonClient(config.llm_timeout_seconds, config.llm_temperature)

    @property
    def enabled(self) -> bool:
        return self.agent.ready

    @property
    def secondary_enabled(self) -> bool:
        return self.secondary_agent.ready

    async def review(self, event: InputEvent, intent: TradeIntent) -> ReviewDecision | None:
        if not self.enabled:
            return None

        system_prompt = (
            "你是独立风控复核代理。你要复核已有交易意图是否与新闻、量价、来源质量一致。"
            "返回 JSON，只允许：approved, action_override, confidence_cap, rationale。"
            "approved 为布尔值；action_override 只能是 BUY/SELL/HOLD/null；confidence_cap 为 0 到 1 之间的小数或 null。"
            "如果文本和量价冲突、来源质量过弱或历史相似事件不支持，请拒绝或降置信度。"
        )
        if not self.config.translate_to_zh:
            system_prompt = (
                "You are an independent trade review agent. Review whether the existing trade intent is consistent with "
                "the news, price action, source quality, and history match. Return JSON only with approved, action_override, "
                "confidence_cap, and rationale."
            )

        payload = {
            "event": event.model_dump(mode="json"),
            "intent": intent.model_dump(mode="json"),
        }

        primary_result = await self.client.complete_json(self.agent, system_prompt, payload)
        primary_decision = self._build_decision(event, intent, primary_result, reviewer="llm-review-agent")

        if not self.secondary_enabled:
            return primary_decision

        secondary_result = await self.secondary_client.complete_json(self.secondary_agent, system_prompt, payload)
        secondary_decision = self._build_decision(event, intent, secondary_result, reviewer="llm-review-agent-secondary")

        return self._merge_decisions(event, intent, primary_decision, secondary_decision)

    def _build_decision(
        self,
        event: InputEvent,
        intent: TradeIntent,
        result: dict,
        reviewer: str,
    ) -> ReviewDecision:
        approved = bool(result.get("approved", True))
        action_override = result.get("action_override")
        normalized_action = None
        if action_override is not None:
            value = str(action_override).strip().upper()
            normalized_action = value if value in {"BUY", "SELL", "HOLD"} else None

        confidence_cap = result.get("confidence_cap")
        if confidence_cap in {None, ""}:
            confidence_cap_value = None
        else:
            confidence_cap_value = max(0.0, min(1.0, float(confidence_cap)))

        return ReviewDecision(
            event_id=event.event_id,
            intent_id=intent.intent_id,
            status=ReviewStatus.APPROVE if approved else ReviewStatus.REJECT,
            reviewer=reviewer,
            action_override=TradeAction(normalized_action) if normalized_action else None,
            confidence_cap=confidence_cap_value,
            rationale=str(result.get("rationale") or ""),
            metadata={
                "provider": self.agent.provider if reviewer == "llm-review-agent" else self.secondary_agent.provider,
                "model": self.agent.model if reviewer == "llm-review-agent" else self.secondary_agent.model,
                "raw": result,
            },
        )

    def _merge_decisions(
        self,
        event: InputEvent,
        intent: TradeIntent,
        primary: ReviewDecision,
        secondary: ReviewDecision,
    ) -> ReviewDecision:
        conflict = primary.status != secondary.status or primary.action_override != secondary.action_override
        if not conflict:
            merged = primary.model_copy(deep=True)
            merged.metadata = {
                **(primary.metadata or {}),
                "secondary": secondary.model_dump(mode="json"),
                "conflict": False,
            }
            return merged

        action = (self.config.review_conflict_action or "hold").strip().lower()
        if action not in {"hold", "reject"}:
            action = "hold"
        conflict_status = ReviewStatus.REJECT
        updated_action = TradeAction.HOLD
        updated_confidence = min(
            primary.confidence_cap or 1.0,
            secondary.confidence_cap or 1.0,
            0.25,
        )
        rationale = "Review conflict: primary and secondary reviewers disagree."
        if self.config.translate_to_zh:
            rationale = "复核冲突：双审结论不一致，已回滚为 HOLD。"
        return ReviewDecision(
            event_id=event.event_id,
            intent_id=intent.intent_id,
            status=conflict_status,
            reviewer="llm-review-agent-dual",
            action_override=updated_action,
            confidence_cap=updated_confidence,
            rationale=rationale,
            metadata={
                "conflict": True,
                "conflict_action": action,
                "primary": primary.model_dump(mode="json"),
                "secondary": secondary.model_dump(mode="json"),
            },
        )
