from __future__ import annotations

from dataclasses import asdict

from hfqt.agents.client import AgentRuntimeConfig, OpenAIJsonClient
from hfqt.config import AppConfig


class IntelAgent:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        settings = config.agent_settings("intel")
        self.agent = AgentRuntimeConfig(**settings)
        self.client = OpenAIJsonClient(config.llm_timeout_seconds, config.llm_temperature)

    @property
    def enabled(self) -> bool:
        return self.agent.ready

    async def summarize(self, symbol: str, query: str, items: list[object], translate_to_zh: bool) -> dict | None:
        if not self.enabled or not items:
            return None

        system_prompt = (
            "你是金融情报抓取代理。请从多源新闻和社交舆情中去噪、去重，只保留交易最相关的核心信息。"
            "返回 JSON，字段只允许：headline, body, sentiment, key_points。"
            "headline 要简短；body 要总结成适合交易决策的摘要；sentiment 在 -1 到 1 之间；key_points 为字符串数组。"
        )
        if not translate_to_zh:
            system_prompt = (
                "You are a financial intelligence intake agent. Dedupe and denoise multi-source news and social posts. "
                "Return JSON only with headline, body, sentiment, and key_points. "
                "headline should be concise, body should be a trade-ready summary, sentiment must be between -1 and 1."
            )

        payload = {
            "symbol": symbol,
            "query": query,
            "items": [asdict(item) if hasattr(item, "__dict__") else item for item in items[:8]],
        }
        result = await self.client.complete_json(self.agent, system_prompt, payload)
        result["agent_model"] = self.agent.model
        result["agent_provider"] = self.agent.provider or "openai_compatible"
        return result
