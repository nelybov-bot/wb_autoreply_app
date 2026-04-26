"""
OpenAI клиент через Responses API (https://api.openai.com/v1/responses).

- Модель по умолчанию: gpt-5.2 (качество).
- Ретраи только на 5xx (без 429): квота/лимит OpenAI не дублируем бессмысленными повторами.
"""
from __future__ import annotations

import json
import logging

import aiohttp

from .net import HttpStatusError, retry

log = logging.getLogger("openai")


def _openai_error_summary(status: int, body: str) -> str:
    """Короткое сообщение для логов и HTTP вместо JSON простыни."""
    if status == 401:
        return "OpenAI: неверный или пустой API-ключ (401)."
    if status == 403:
        return "OpenAI: доступ запрещён (403). Проверьте ключ и регион."
    try:
        j = json.loads(body) if body else {}
        err = j.get("error") if isinstance(j, dict) else None
        if isinstance(err, dict):
            code = str(err.get("code") or "")
            typ = str(err.get("type") or "")
            if code == "insufficient_quota" or typ == "insufficient_quota":
                return (
                    "OpenAI: закончилась квота или нет оплаты (insufficient_quota). "
                    "Откройте https://platform.openai.com/account/billing"
                )
            if code == "rate_limit_exceeded" or typ == "rate_limit_exceeded":
                return "OpenAI: слишком много запросов (rate_limit). Подождите минуту."
            msg = (err.get("message") or "").strip()
            if msg and len(msg) < 220:
                return f"OpenAI ({status}): {msg}"
    except Exception:
        pass
    if status == 429:
        return "OpenAI: 429 — лимит или квота. Проверьте биллинг и лимиты на platform.openai.com."
    return f"OpenAI: ошибка HTTP {status}"


class OpenAIClient:
    def __init__(self, api_key: str, model: str = "gpt-5.2", timeout_s: float = 40.0) -> None:
        self.api_key = api_key.strip()
        self.model = model
        self.timeout = aiohttp.ClientTimeout(connect=10, total=timeout_s)

    @staticmethod
    def _extract_text(resp_json: dict) -> str:
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
                        short = _openai_error_summary(resp.status, txt)
                        log.warning("OpenAI responses %s: %s", resp.status, short)
                        raise HttpStatusError(resp.status, short)
                    data = json.loads(txt) if txt else {}
                    text = self._extract_text(data)
                    return text

        return await retry(_do, retry_on_status=(500, 502, 503, 504), retries=4)
