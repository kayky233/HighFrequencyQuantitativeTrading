from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import httpx

from hfqt.config import AppConfig
from hfqt.runtime_logging import get_logger


HTTP_ERROR_THRESHOLD = 400
logger = get_logger("translation.zh")


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    api_key: str
    base_url: str
    model: str

    @property
    def endpoint(self) -> str:
        if self.base_url.rstrip("/").endswith("/chat/completions"):
            return self.base_url.rstrip("/")
        return f"{self.base_url.rstrip('/')}/chat/completions"


def _read_env(*names: str) -> str | None:
    import os

    for name in names:
        value = os.getenv(name)
        if value and value.strip():
            return value.strip()
    return None


def _extract_text(data: dict[str, Any]) -> str:
    choices = data.get("choices") or []
    if not choices:
        return ""

    message = choices[0].get("message") or {}
    content = message.get("content")
    if isinstance(content, str):
        return content.strip()

    if isinstance(content, list):
        text_parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                text_parts.append(item)
                continue
            if not isinstance(item, dict):
                continue
            if item.get("type") in {"text", "output_text"} and item.get("text"):
                text_parts.append(str(item["text"]))
        return "".join(text_parts).strip()

    return ""


def _extract_error_text(response: httpx.Response) -> str:
    try:
        data = response.json()
    except ValueError:
        return response.text[:200]

    error = data.get("error")
    if isinstance(error, dict):
        message = error.get("message")
        if message:
            return str(message)
    return str(data)[:200]


class ChineseTranslator:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.providers = self._build_providers()
        self._cache: OrderedDict[str, str] = OrderedDict()
        self._cache_limit = 256
        self._provider_failures: dict[str, int] = {}
        self._provider_cooldown_until: dict[str, float] = {}
        self._client = (
            httpx.AsyncClient(
                timeout=httpx.Timeout(config.translation_timeout_seconds),
                limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
            )
            if self.providers and config.translate_to_zh
            else None
        )

    def _build_providers(self) -> list[ProviderConfig]:
        provider_specs = {
            "deepseek": {
                "key_envs": ("DEEPSEEK_API_KEY", "DEEPSEEK_KEY"),
                "base_url_env": "DEEPSEEK_BASE_URL",
                "default_base_url": "https://api.deepseek.com",
                "model_env": "DEEPSEEK_MODEL",
                "default_model": "deepseek-chat",
            },
            "openai": {
                "key_envs": ("OPENAI_API_KEY",),
                "base_url_env": "OPENAI_BASE_URL",
                "default_base_url": "https://api.openai.com/v1",
                "model_env": "OPENAI_MODEL",
                "default_model": "gpt-4o-mini",
            },
            "glm": {
                "key_envs": ("GLM_API_KEY", "ZAI_API_KEY"),
                "base_url_env": "GLM_BASE_URL",
                "default_base_url": "https://open.bigmodel.cn/api/paas/v4",
                "model_env": "GLM_MODEL",
                "default_model": "glm-4.7-flash",
            },
        }

        import os

        providers: list[ProviderConfig] = []
        for name in self.config.translation_provider_order:
            spec = provider_specs.get(name)
            if spec is None:
                continue
            api_key = _read_env(*spec["key_envs"])
            if not api_key:
                continue
            base_url = os.getenv(spec["base_url_env"], spec["default_base_url"]).strip()
            model = os.getenv(spec["model_env"], spec["default_model"]).strip()
            providers.append(ProviderConfig(name=name, api_key=api_key, base_url=base_url, model=model))
        return providers

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()

    def describe(self) -> str:
        return " -> ".join(f"{provider.name}({provider.model})" for provider in self.providers)

    async def translate_to_zh(self, text: str) -> str:
        clean_text = (text or "").strip()
        if not clean_text:
            return ""
        if not self.config.translate_to_zh:
            return clean_text

        cached = self._cache_get(clean_text)
        if cached:
            return cached

        fast = self._quick_translate(clean_text)
        if fast:
            self._cache_put(clean_text, fast)
            return fast

        if self._client is None:
            return clean_text

        messages = [
            {
                "role": "system",
                "content": (
                    "请将输入的财经新闻标题、摘要、推文或舆情内容翻译成自然、简洁的简体中文。"
                    "保留股票代码、公司名、数字、时间、涨跌方向和关键术语。"
                    "如果原文已经是中文，直接返回中文。只返回中文结果，不要解释。"
                ),
            },
            {"role": "user", "content": clean_text},
        ]

        last_error = ""
        for provider in self._provider_sequence():
            parsed_url = urlparse(provider.base_url)
            trust_env = parsed_url.hostname not in {"127.0.0.1", "localhost"}
            try:
                response = await self._client.post(
                    provider.endpoint,
                    headers={
                        "Authorization": f"Bearer {provider.api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": provider.model,
                        "messages": messages,
                        "temperature": 0.0,
                        "stream": False,
                        "max_tokens": self.config.translation_max_tokens,
                    },
                    extensions={"trust_env": trust_env},
                )
            except httpx.HTTPError as error:
                self._mark_provider_failure(provider.name)
                last_error = str(error)
                continue

            if response.status_code >= HTTP_ERROR_THRESHOLD:
                self._mark_provider_failure(provider.name)
                last_error = _extract_error_text(response)
                continue

            try:
                data = response.json()
            except ValueError:
                self._mark_provider_failure(provider.name)
                last_error = response.text[:200]
                continue

            translated = _extract_text(data).strip()
            if translated:
                self._mark_provider_success(provider.name)
                self._cache_put(clean_text, translated)
                return translated

        if last_error:
            logger.warning(
                "translation fallback to original text",
                extra={
                    "event": "translation_fallback",
                    "provider_chain": self.describe(),
                    "error": last_error,
                    "text_preview": clean_text[:120],
                },
            )
        return clean_text

    async def translate_many(self, texts: list[str]) -> list[str]:
        results: list[str] = []
        for text in texts:
            results.append(await self.translate_to_zh(text))
        return results

    def _quick_translate(self, text: str) -> str:
        if self._contains_cjk(text):
            return text

        normalized = " ".join(text.strip().split())
        if not normalized:
            return ""

        quick_map = {
            "breaking": "突发",
            "after market close": "盘后",
            "before market open": "盘前",
            "hot stock alert": "热门股票提醒",
            "strong buy": "强烈买入",
            "buy": "买入",
            "sell": "卖出",
            "hold": "观望",
        }
        lowered = normalized.lower()
        if lowered in quick_map:
            return quick_map[lowered]
        return ""

    def _provider_sequence(self) -> list[ProviderConfig]:
        now = time.time()
        available = [
            provider
            for provider in self.providers
            if self._provider_cooldown_until.get(provider.name, 0.0) <= now
        ]
        return available or self.providers

    def _mark_provider_failure(self, provider_name: str) -> None:
        failures = self._provider_failures.get(provider_name, 0) + 1
        self._provider_failures[provider_name] = failures
        cooldown_seconds = min(4.0 * failures, 20.0)
        self._provider_cooldown_until[provider_name] = time.time() + cooldown_seconds

    def _mark_provider_success(self, provider_name: str) -> None:
        self._provider_failures.pop(provider_name, None)
        self._provider_cooldown_until.pop(provider_name, None)

    def _cache_get(self, text: str) -> str:
        key = " ".join(text.split()).strip()
        if not key:
            return ""
        cached = self._cache.get(key, "")
        if cached:
            self._cache.move_to_end(key)
        return cached

    def _cache_put(self, text: str, translated: str) -> None:
        key = " ".join(text.split()).strip()
        value = translated.strip()
        if not key or not value:
            return
        self._cache[key] = value
        self._cache.move_to_end(key)
        while len(self._cache) > self._cache_limit:
            self._cache.popitem(last=False)

    @staticmethod
    def _contains_cjk(text: str) -> bool:
        return any("\u4e00" <= char <= "\u9fff" for char in text)
