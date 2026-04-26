"""
Клиент Wildberries «Чат с покупателями» (buyer-chat-api).

Ключ: в кабинете WB API нужна категория «Чат с покупателями» (buyer-chat-api);
ключ «Вопросы и отзывы» к feedbacks-api не подходит для этого хоста.

Лимиты WB: для персонального/сервисного ключа — 10 запросов / 10 с к buyer-chat-api.
Тариф «Базовый» в доке — 1 запрос / час: тогда в логах будут паузы до часа; без такого ключа
частый опрос чатов невозможен.

Сериализация: один asyncio.Lock на весь цикл «запрос → при 429 пауза → повтор».
После 429 пауза берётся из Retry-After / X-Ratelimit-Retry (не обрезается до ~35 с).
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import socket
import time
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from .net import HttpStatusError, RateLimiter, USER_AGENT, retry

log = logging.getLogger("wb_chat")

BASE = "https://buyer-chat-api.wildberries.ru"

# ~0.9 rps ≈ 1.1 с между запросами — укладывается в 10 запросов / 10 с (личный/сервисный тариф WB).
_wb_buyer_rl = RateLimiter(0.9)
_wb_buyer_serial = asyncio.Lock()


def _wb_buyer_429_sleep_seconds(headers: Any) -> tuple[int, str]:
    """
    Пауза после 429 по заголовкам WB.

    Раньше всё резали до ~40 с: при тарифе API «Базовый» (1 запрос/час к buyer-chat)
    Retry-After может быть тысячи секунд — мы просыпались слишком рано и снова ловили 429.
    """
    max_sleep = 7200  # один sleep не дольше 2 ч (воркер не «мёртвый» навсегда)
    min_sleep = 8
    default = 28
    now = time.time()

    for key in ("Retry-After", "X-Ratelimit-Retry"):
        v = headers.get(key)
        if v is None:
            continue
        try:
            raw = float(str(v).strip())
        except (ValueError, TypeError):
            continue

        if raw > 1e12:
            target_s = raw / 1000.0
            wait = int(max(0.0, target_s - now)) + 1
            note = f"{key}=unix_ms"
        elif raw > 1e9:
            wait = int(max(0.0, raw - now)) + 1
            note = f"{key}=unix_s"
        else:
            wait = int(raw) + 1
            note = f"{key}=Δs"

        wait = max(min_sleep, min(max_sleep, wait))
        return wait, note

    return default, "no_retry_header"


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
        async with _wb_buyer_serial:
            for attempt in range(3):
                await _wb_buyer_rl.wait()
                connector = aiohttp.TCPConnector(force_close=True, family=socket.AF_INET)
                async with aiohttp.ClientSession(timeout=self.timeout, connector=connector) as s:
                    async with s.get(url, headers=self._headers_json(), params=params or None) as resp:
                        txt = await resp.text()
                        st = resp.status
                        hdrs = resp.headers
                if st == 429 and attempt < 2:
                    wait, why = _wb_buyer_429_sleep_seconds(hdrs)
                    if wait >= 180:
                        log.warning(
                            "WB buyer-chat GET %s: 429, пауза %ss (%s), попытка %s/3. "
                            "Долгая пауза часто значит тариф WB API «Базовый» (до 1 запроса/ч к чату) — "
                            "нужен персональный или сервисный ключ «Чат с покупателями».",
                            path,
                            wait,
                            why,
                            attempt + 1,
                        )
                    else:
                        log.warning(
                            "WB buyer-chat GET %s: 429, пауза %ss (%s), попытка %s/3",
                            path,
                            wait,
                            why,
                            attempt + 1,
                        )
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

    async def _http_post_multipart(self, path: str, form: aiohttp.FormData) -> Any:
        url = BASE + path
        async with _wb_buyer_serial:
            for attempt in range(3):
                await _wb_buyer_rl.wait()
                connector = aiohttp.TCPConnector(force_close=True, family=socket.AF_INET)
                async with aiohttp.ClientSession(timeout=self.timeout, connector=connector) as s:
                    async with s.post(url, headers=self._headers_form(), data=form) as resp:
                        txt = await resp.text()
                        st = resp.status
                        hdrs = resp.headers
                if st == 429 and attempt < 2:
                    wait, why = _wb_buyer_429_sleep_seconds(hdrs)
                    if wait >= 180:
                        log.warning(
                            "WB buyer-chat POST %s: 429, пауза %ss (%s), попытка %s/3. "
                            "Проверьте тариф ключа «Чат с покупателями» (не «Базовый» для частого опроса).",
                            path,
                            wait,
                            why,
                            attempt + 1,
                        )
                    else:
                        log.warning(
                            "WB buyer-chat POST %s: 429, пауза %ss (%s), попытка %s/3",
                            path,
                            wait,
                            why,
                            attempt + 1,
                        )
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
