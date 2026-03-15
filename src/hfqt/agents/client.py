from __future__ import annotations

import json
import re
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx


@dataclass(slots=True)
class AgentRuntimeConfig:
    role: str
    enabled: bool
    provider: str | None
    base_url: str | None
    api_key: str | None
    model: str | None
    fallback_model: str | None = None
    mode: str | None = None

    @property
    def ready(self) -> bool:
        return bool(self.enabled and self.base_url and self.model)


class OpenAIJsonClient:
    def __init__(self, timeout_seconds: float, temperature: float) -> None:
        self.timeout_seconds = timeout_seconds
        self.temperature = temperature

    async def complete_json(
        self,
        agent: AgentRuntimeConfig,
        system_prompt: str,
        user_payload: dict,
    ) -> dict:
        if not agent.ready:
            raise RuntimeError(f"Agent {agent.role} is not configured.")

        headers = {"Content-Type": "application/json"}
        if agent.api_key:
            headers["Authorization"] = f"Bearer {agent.api_key}"

        parsed_url = urlparse(agent.base_url or "")
        trust_env = parsed_url.hostname not in {"127.0.0.1", "localhost"}
        payload = {
            "model": agent.model,
            "temperature": self.temperature,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
        }

        async with httpx.AsyncClient(timeout=self.timeout_seconds, trust_env=trust_env) as client:
            response = await client.post(
                f"{agent.base_url.rstrip('/')}/chat/completions",
                json=payload,
                headers=headers,
            )
            response.raise_for_status()
            body = response.json()
            choices = body.get("choices") or []
            if not choices:
                raise RuntimeError(f"Agent {agent.role} returned no choices.")
            message = choices[0].get("message") or {}
            content = message.get("content")
            if isinstance(content, list):
                content = "\n".join(
                    str(item.get("text", ""))
                    for item in content
                    if isinstance(item, dict) and item.get("type") == "text"
                )
            if not isinstance(content, str) or not content.strip():
                raise RuntimeError(f"Agent {agent.role} returned empty content.")
            return parse_json_from_llm_content(content)

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


def parse_json_from_llm_content(content: str) -> dict:
    text = OpenAIJsonClient._strip_json_fence(content)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        payload = json.loads(_extract_first_json_object(text))
    if not isinstance(payload, dict):
        raise RuntimeError("LLM endpoint did not return a JSON object.")
    return _canonicalize_object_keys(payload)


def _extract_first_json_object(text: str) -> str:
    decoder = json.JSONDecoder()
    candidates = [match.start() for match in re.finditer(r"\{", text)]
    for start in candidates:
        try:
            obj, _ = decoder.raw_decode(text[start:])
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            return json.dumps(obj, ensure_ascii=False)
    raise json.JSONDecodeError("No JSON object found in model response.", text, 0)


def _canonicalize_object_keys(value):
    if isinstance(value, dict):
        normalized = {}
        for key, item in value.items():
            normalized[str(key).strip().lower()] = _canonicalize_object_keys(item)
        return normalized
    if isinstance(value, list):
        return [_canonicalize_object_keys(item) for item in value]
    return value
