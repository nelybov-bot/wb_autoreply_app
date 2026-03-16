"""
OpenAI клиент через Responses API (https://api.openai.com/v1/responses).

- Модель по умолчанию: gpt-5.2 (качество).
- Ретраи на 429/5xx через retry().
"""
from __future__ import annotations

import json
import logging
from typing import Optional

import aiohttp

from .net import HttpStatusError, retry

log = logging.getLogger("openai")

class OpenAIClient:
    def __init__(self, api_key: str, model: str = "gpt-5.2", timeout_s: float = 40.0) -> None:
        self.api_key = api_key.strip()
        self.model = model
        self.timeout = aiohttp.ClientTimeout(connect=10, total=timeout_s)

    @staticmethod
    def _extract_text(resp_json: dict) -> str:
        # Responses API: output[].content[].text
        out = resp_json.get("output", [])
        parts: list[str] = []
        for o in out:
            for c in o.get("content", []) or []:
                if c.get("type") == "output_text" and "text" in c:
                    parts.append(str(c["text"]))
        return "\n".join([p for p in parts if p]).strip()

    async def generate(self, system_prompt: str, user_prompt: str) -> str:
        url = "https://api.openai.com/v1/responses"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "input": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }

        async def _do():
            async with aiohttp.ClientSession(timeout=self.timeout) as s:
                async with s.post(url, headers=headers, json=payload) as resp:
                    txt = await resp.text()
                    if resp.status >= 400:
                        raise HttpStatusError(resp.status, txt)
                    data = json.loads(txt) if txt else {}
                    text = self._extract_text(data)
                    return text

        text = await retry(_do)
        return text
