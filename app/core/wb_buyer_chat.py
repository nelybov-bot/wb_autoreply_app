"""
Клиент Wildberries «Чат с покупателями» (buyer-chat-api).

Тот же API-ключ, что и для отзывов/вопросов (Authorization: Bearer …).

Лимиты WB: жёстко сериализуем все запросы к buyer-chat-api в одном процессе
(один RateLimiter + Lock), иначе «список чатов» + «события» давали двойную частоту и 429.

При 429: пауза по X-Ratelimit-Retry / Retry-After и одна повторная попытка.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import socket
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from .net import HttpStatusError, RateLimiter, USER_AGENT, retry

log = logging.getLogger("wb_chat")

BASE = "https://buyer-chat-api.wildberries.ru"

# Один лимитер на весь процесс: не чаще ~1 запроса к buyer-chat-api в 4.5 с (ниже лимита 10/10 с).
_wb_buyer_rl = RateLimiter(0.22)
_wb_buyer_serial = asyncio.Lock()


def _wb_retry_after_seconds(headers: Any) -> int:
    for key in ("X-Ratelimit-Retry", "Retry-After", "X-RateLimit-Reset"):
        v = headers.get(key)
        if v is None:
            continue
        try:
            return max(8, min(120, int(float(str(v))) + 3))
        except (ValueError, TypeError):
            continue
    return 15


@asynccontextmanager
async def _wb_buyer_slot():
    async with _wb_buyer_serial:
        await _wb_buyer_rl.wait()
        yield


class WbBuyerChatClient:
    def __init__(self, api_key: str, *, timeout_s: float = 45.0) -> None:
        self.api_key = api_key.strip()
        self.timeout = aiohttp.ClientTimeout(connect=15, total=timeout_s)

    def _headers_json(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        }

    def _headers_form(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "User-Agent": USER_AGENT,
        }

    async def _http_get_json(self, path: str, *, params: Optional[dict] = None) -> Any:
        url = BASE + path
        for attempt in range(3):
            async with _wb_buyer_slot():
                connector = aiohttp.TCPConnector(force_close=True, family=socket.AF_INET)
                async with aiohttp.ClientSession(timeout=self.timeout, connector=connector) as s:
                    async with s.get(url, headers=self._headers_json(), params=params or None) as resp:
                        txt = await resp.text()
                        st = resp.status
                        hdrs = resp.headers
            if st == 429 and attempt < 2:
                wait = _wb_retry_after_seconds(hdrs)
                log.warning("WB buyer-chat GET %s: 429, sleep %ss", path, wait)
                await asyncio.sleep(wait)
                continue
            if st >= 400:
                raise HttpStatusError(st, txt)
            if st == 204 or not txt:
                return None
            try:
                return json.loads(txt)
            except Exception as e:
                log.warning("WB buyer-chat invalid JSON: %s", e)
                raise HttpStatusError(502, f"Invalid JSON: {str(e)[:200]}") from e
        raise HttpStatusError(429, "Too many requests")

    async def _http_post_multipart(self, path: str, form: aiohttp.FormData) -> Any:
        url = BASE + path
        for attempt in range(3):
            async with _wb_buyer_slot():
                connector = aiohttp.TCPConnector(force_close=True, family=socket.AF_INET)
                async with aiohttp.ClientSession(timeout=self.timeout, connector=connector) as s:
                    async with s.post(url, headers=self._headers_form(), data=form) as resp:
                        txt = await resp.text()
                        st = resp.status
                        hdrs = resp.headers
            if st == 429 and attempt < 2:
                wait = _wb_retry_after_seconds(hdrs)
                log.warning("WB buyer-chat POST %s: 429, sleep %ss", path, wait)
                await asyncio.sleep(wait)
                continue
            if st >= 400:
                raise HttpStatusError(st, txt)
            if not txt:
                return {}
            try:
                return json.loads(txt)
            except Exception as e:
                log.warning("WB buyer-chat send: invalid JSON: %s", e)
                return {}
        raise HttpStatusError(429, "Too many requests")

    async def list_chats(self) -> list[dict]:
        async def _do():
            data = await self._http_get_json("/api/v1/seller/chats")
            if not isinstance(data, dict):
                return []
            res = data.get("result")
            if isinstance(res, list):
                return res
            log.warning("WB buyer chat chats: unexpected result shape: %s", type(res))
            return []

        return await retry(_do, retry_on_status=(500, 502, 503, 504), retries=3)

    async def get_events(self, *, next_cursor: Optional[int] = None) -> dict:
        params: dict[str, str] = {}
        if next_cursor is not None:
            params["next"] = str(int(next_cursor))

        async def _do():
            data = await self._http_get_json("/api/v1/seller/events", params=params or None)
            if not isinstance(data, dict):
                return {}
            res = data.get("result")
            if isinstance(res, dict):
                return res
            return {}

        return await retry(_do, retry_on_status=(500, 502, 503, 504), retries=3)

    async def send_message(self, reply_sign: str, message: str) -> dict:
        reply_sign = (reply_sign or "").strip()
        message = (message or "").strip()
        if not reply_sign or not message:
            raise ValueError("reply_sign и message обязательны")
        msg_cut = message[:1000]

        async def _do():
            form = aiohttp.FormData()
            form.add_field("replySign", reply_sign)
            form.add_field("message", msg_cut)
            return await self._http_post_multipart("/api/v1/seller/message", form)

        return await retry(_do, retry_on_status=(500, 502, 503, 504), retries=3)


async def collect_global_events_by_chat(
    client: WbBuyerChatClient,
    *,
    max_pages: int = 12,
) -> Dict[str, List[dict]]:
    """
    Один проход по ленте /seller/events с пагинацией next; события сгруппированы по chatID.
    """
    by_chat: Dict[str, List[dict]] = {}
    next_cursor: Optional[int] = None
    for _ in range(max_pages):
        block = await client.get_events(next_cursor=next_cursor)
        events = block.get("events") or []
        if not isinstance(events, list):
            events = []
        for ev in events:
            if not isinstance(ev, dict):
                continue
            cid = str(ev.get("chatID") or "").strip()
            if cid:
                by_chat.setdefault(cid, []).append(ev)
        if int(block.get("totalEvents") or 0) == 0:
            break
        raw_next = block.get("next")
        if raw_next is None:
            break
        try:
            next_cursor = int(raw_next)
        except (TypeError, ValueError):
            break
    return by_chat


_GOOD_TITLE_KEYS = ("productName", "name", "title", "subject", "imtName", "supplierArticle", "brandName")


def product_title_from_wb_chat(good_card: Any, message_texts: List[str]) -> str:
    """
    Название товара из ответа WB: поля goodCard, иначе типичная формулировка в тексте сообщения.
    """
    if isinstance(good_card, dict):
        for k in _GOOD_TITLE_KEYS:
            v = good_card.get(k)
            if v is not None and str(v).strip():
                return str(v).strip()
    for text in message_texts:
        if not text:
            continue
        m = re.search(r"по\s+товару\s*\"([^\"]+)\"", text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
        m = re.search(r"товару\s*«([^»]+)»", text)
        if m:
            return m.group(1).strip()
    if isinstance(good_card, dict):
        nm = good_card.get("nmID")
        if nm is not None and str(nm).strip():
            return f"Товар nmID {nm}"
    return "Товар (название не указано в данных чата)"


def _event_text(ev: dict) -> str:
    msg = ev.get("message") or {}
    if isinstance(msg, dict):
        return str(msg.get("text") or "").strip()
    return ""


def merge_good_card(chat_row: dict, events: List[dict]) -> dict:
    """Объединяет goodCard из списка чатов и из вложений событий."""
    gc: Dict[str, Any] = {}
    raw = chat_row.get("goodCard") if isinstance(chat_row, dict) else None
    if isinstance(raw, dict):
        gc.update(raw)
    for ev in events:
        if not isinstance(ev, dict):
            continue
        msg = ev.get("message")
        if not isinstance(msg, dict):
            continue
        att = msg.get("attachments")
        if not isinstance(att, dict):
            continue
        g = att.get("goodCard")
        if isinstance(g, dict):
            for k, v in g.items():
                if v is not None and str(v).strip() != "":
                    gc[k] = v
    return gc


def collect_thread_lines(events: List[dict], chat_id: str) -> List[Tuple[str, str, int]]:
    """Сообщения одного чата: (role, text, addTimestamp)."""
    out: List[Tuple[str, str, int]] = []
    cid = (chat_id or "").strip()
    for ev in events:
        if not isinstance(ev, dict):
            continue
        if str(ev.get("chatID") or "").strip() != cid:
            continue
        if str(ev.get("eventType") or "") != "message":
            continue
        text = _event_text(ev)
        if not text:
            continue
        sender = str(ev.get("sender") or "").strip().lower()
        if sender == "client":
            role = "client"
        elif sender in ("seller", "seller-public-api"):
            role = "seller"
        else:
            role = sender or "other"
        ts = int(ev.get("addTimestamp") or 0)
        out.append((role, text, ts))
    out.sort(key=lambda x: x[2])
    return out


async def fetch_events_for_chat(
    client: WbBuyerChatClient,
    chat_id: str,
    *,
    max_wb_requests: int = 8,
) -> Tuple[List[dict], Optional[int]]:
    """
    Подтягивает страницы /seller/events и отбирает события выбранного чата.
    """
    merged: List[dict] = []
    next_cursor: Optional[int] = None
    last_next: Optional[int] = None
    for _ in range(max_wb_requests):
        block = await client.get_events(next_cursor=next_cursor)
        events = block.get("events") or []
        if not isinstance(events, list):
            events = []
        for ev in events:
            if isinstance(ev, dict) and str(ev.get("chatID") or "").strip() == (chat_id or "").strip():
                merged.append(ev)
        total = int(block.get("totalEvents") or 0)
        if total == 0:
            last_next = None
            break
        raw_next = block.get("next")
        if raw_next is None:
            last_next = None
            break
        try:
            last_next = int(raw_next)
        except (TypeError, ValueError):
            last_next = None
            break
        next_cursor = last_next
    return merged, last_next
