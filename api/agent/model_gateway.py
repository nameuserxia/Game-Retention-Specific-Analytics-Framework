# -*- coding: utf-8 -*-
"""
ModelGateway — DeepSeek / OpenAI 兼容流式调用封装。
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import AsyncIterator, Dict, List, Optional, Tuple

import yaml

logger = logging.getLogger(__name__)


@dataclass
class LLMConfig:
    provider: str
    api_key: str
    base_url: str
    model: str
    temperature: float
    max_tokens: int
    top_p: float
    stream: bool
    timeout: int
    fallback_on_error: bool

    @classmethod
    def from_yaml(cls, path: str) -> "LLMConfig":
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        return cls(
            provider=str(raw.get("provider", "") or "").strip(),
            api_key=str(raw.get("api_key", "") or "").strip(),
            base_url=str(raw.get("base_url", "https://api.deepseek.com") or "").strip(),
            model=str(raw.get("model", "deepseek-chat") or "").strip(),
            temperature=float(raw.get("temperature", 0.3)),
            max_tokens=int(raw.get("max_tokens", 2048)),
            top_p=float(raw.get("top_p", 1.0)),
            stream=bool(raw.get("stream", True)),
            timeout=int(raw.get("timeout", 60)),
            fallback_on_error=bool(raw.get("fallback_on_error", True)),
        )

    @property
    def is_enabled(self) -> bool:
        """provider 和 api_key 都不为空时才启用 LLM。"""
        return bool(self.provider and self.api_key)

    @property
    def extra_headers(self) -> Dict[str, str]:
        """部分服务商需要额外 header（如 DeepSeek 用 Content-Type）。"""
        if self.provider == "deepseek":
            return {"Content-Type": "application/json"}
        return {}


class ModelGateway:
    """
    调用 OpenAI 兼容接口，支持流式迭代。

    用法（同步，非 SSE 路由）：
        gateway = ModelGateway(config)
        text = gateway.chat_sync("你是谁")

    用法（SSE 路由）：
        async for chunk in gateway.chat_stream(messages):
            yield chunk          # str，每块 token
    """

    def __init__(self, config: LLMConfig):
        self.config = config
        self._client: Optional["OpenAI"] = None

    # ── 内部：懒加载 client ────────────────────────────────────
    def _get_client(self) -> "OpenAI":
        if self._client is None:
            try:
                from openai import OpenAI
            except ImportError:
                raise RuntimeError(
                    "请先安装 openai 库: pip install openai"
                )
            self._client = OpenAI(
                api_key=self.config.api_key,
                base_url=self.config.base_url,
                timeout=self.config.timeout,
                default_headers=self.config.extra_headers,
            )
        return self._client

    # ── 同步单轮（非流）───────────────────────────────────────
    def chat_sync(self, prompt: str, system_prompt: str = "") -> str:
        """
        同步调用，适合不需要流式的场景。
        返回模型输出的完整文本。
        """
        messages: List[Dict[str, str]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        client = self._get_client()
        resp = client.chat.completions.create(
            model=self.config.model,
            messages=messages,
            temperature=self.config.temperature,
            max_tokens=self.config.max_tokens,
            top_p=self.config.top_p,
        )
        return resp.choices[0].message.content or ""

    # ── 异步流式（yield str）─────────────────────────────────
    async def chat_stream(
        self, messages: List[Dict[str, str]]
    ) -> AsyncIterator[str]:
        """
        异步流式调用，yield 每块 token（字符串）。

        messages 格式:
            [{"role": "system", "content": "..."}, {"role": "user", "content": "..."}]
        """
        try:
            client = self._get_client()
            # OpenAI Python SDK >= 1.0 支持 async with
            import openai
            from openai import AsyncOpenAI

            async_client = AsyncOpenAI(
                api_key=self.config.api_key,
                base_url=self.config.base_url,
                timeout=self.config.timeout,
                default_headers=self.config.extra_headers,
            )

            stream = await async_client.chat.completions.create(
                model=self.config.model,
                messages=messages,
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens,
                top_p=self.config.top_p,
                stream=True,
            )

            async for chunk in stream:
                delta = chunk.choices[0].delta.content
                if delta:
                    yield delta

        except Exception as exc:
            logger.warning("[ModelGateway] LLM 调用失败: %s", exc)
            yield ""  # 空串让调用方知道出错了

    # ── 同步流式（fetch + EventSource）────────────────────────
    def chat_stream_sync(self, messages: List[Dict[str, str]]) -> SyncStream:
        """
        同步流式：内部用 httpx 构造 SSE 请求，返回一个可迭代对象。
        适合 FastAPI 的 StreamingResponse 同步生成器场景。
        """
        try:
            import httpx
        except ImportError:
            raise RuntimeError("请先安装 httpx: pip install httpx")

        return SyncStream(messages=messages, config=self.config)


class SyncStream:
    """
    基于 httpx 的同步 SSE 迭代器。
    供 FastAPI StreamingResponse 使用。
    """

    def __init__(self, messages: List[Dict[str, str]], config: LLMConfig):
        self.messages = messages
        self.config = config
        self._iterator: Optional[httpx.Response] = None

    def _build_payload(self) -> Dict:
        return {
            "model": self.config.model,
            "messages": self.messages,
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens,
            "top_p": self.config.top_p,
            "stream": True,
        }

    def __iter__(self):
        import httpx

        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.config.api_key}"}
        headers.update(self.config.extra_headers)

        with httpx.Client(timeout=self.config.timeout) as client:
            with client.stream(
                "POST",
                f"{self.config.base_url.rstrip('/')}/chat/completions",
                json=self._build_payload(),
                headers=headers,
            ) as resp:
                resp.raise_for_status()
                for line in resp.iter_lines():
                    if not line or not line.startswith("data: "):
                        continue
                    data = line[6:].strip()
                    if data == "[DONE]":
                        break
                    import json
                    try:
                        chunk = json.loads(data)
                        content = chunk.get("choices", [{}])[0].get("delta", {}).get("content", "")
                        if content:
                            yield f"data: {json.dumps({'text': content}, ensure_ascii=False)}\n\n"
                    except json.JSONDecodeError:
                        continue
