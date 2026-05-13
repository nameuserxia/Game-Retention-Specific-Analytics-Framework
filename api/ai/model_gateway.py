# -*- coding: utf-8 -*-
"""Optional OpenAI-compatible model gateway with environment-based config."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List, Optional


class LLMUnavailableError(RuntimeError):
    """Raised when optional LLM configuration or runtime is unavailable."""


@dataclass
class AIModelConfig:
    provider: str = ""
    api_key: str = ""
    base_url: str = ""
    model: str = ""
    timeout: int = 60
    temperature: float = 0.2
    max_tokens: int = 1600

    @classmethod
    def from_env(cls) -> "AIModelConfig":
        provider = os.getenv("RETENTION_AI_PROVIDER", "").strip().lower()
        base_url = os.getenv("RETENTION_AI_BASE_URL", "").strip()
        model = os.getenv("RETENTION_AI_MODEL", "").strip()
        api_key = os.getenv("RETENTION_AI_API_KEY", "").strip()

        if provider == "deepseek":
            base_url = base_url or "https://api.deepseek.com"
            model = model or "deepseek-chat"
        elif provider in {"openai", "openai-compatible"}:
            base_url = base_url or "https://api.openai.com/v1"
            model = model or "gpt-4o-mini"
        elif provider == "ollama":
            base_url = base_url or "http://localhost:11434/v1"
            model = model or "llama3.1"
            api_key = api_key or "ollama"

        return cls(
            provider=provider,
            api_key=api_key,
            base_url=base_url,
            model=model,
            timeout=int(os.getenv("RETENTION_AI_TIMEOUT", "60")),
            temperature=float(os.getenv("RETENTION_AI_TEMPERATURE", "0.2")),
            max_tokens=int(os.getenv("RETENTION_AI_MAX_TOKENS", "1600")),
        )

    @property
    def enabled(self) -> bool:
        return bool(self.provider and self.base_url and self.model and self.api_key)


class ModelGateway:
    def __init__(self, config: Optional[AIModelConfig] = None):
        self.config = config or AIModelConfig.from_env()

    def chat(self, messages: List[Dict[str, str]]) -> str:
        if not self.config.enabled:
            raise LLMUnavailableError("AI provider is not configured")

        try:
            from openai import OpenAI
        except ImportError as exc:
            raise LLMUnavailableError("openai package is not installed") from exc

        try:
            client = OpenAI(
                api_key=self.config.api_key,
                base_url=self.config.base_url,
                timeout=self.config.timeout,
            )
            response = client.chat.completions.create(
                model=self.config.model,
                messages=messages,
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens,
                response_format={"type": "json_object"},
            )
            return response.choices[0].message.content or ""
        except Exception as exc:
            raise LLMUnavailableError(str(exc)) from exc
