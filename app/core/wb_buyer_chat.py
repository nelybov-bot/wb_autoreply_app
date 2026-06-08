"""
Клиент Wildberries «Чат с покупателями» (buyer-chat-api).

Ключ: в кабинете WB API нужна категория «Чат с покупателями» (buyer-chat-api);
ключ «Вопросы и отзывы» к feedbacks-api не подходит для этого хоста.

Лимиты WB: для персонального/сервисного ключа — ориентир 10 запросов / 10 с к buyer-chat-api.
429 с длинным Retry-After бывает и при корректном ключе с «Чат с покупателем»: перегруз по частоте
(несколько магазинов в приложении, автозапуск, UI) и отдельные лимиты WB на аккаунт.

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

from .chat_common import parse_api_error_detail
from .net import HttpStatusError, RateLimiter, USER_AGENT, retry

log = logging.getLogger("wb_chat")

BASE = "https://buyer-chat-api.wildberries.ru"

# Префикс исходящих сообщений продавца — по нему в переписке видно, кто писал последним.
WB_SELLER_REPLY_PREFIX = "Ответ продавца:"


def wb_chat_error_message(status: int, body: str) -> str:
    """Человекочитаемое сообщение по коду и телу ответа buyer-chat-api."""
    detail = parse_api_error_detail(body)
    if status == 401:
        return (
            "WB buyer-chat: 401 — не авторизован. Проверьте API-ключ и категорию токена "
            "«Чат с покупателями» (buyer-chat-api)."
            + (f" {detail}" if detail and detail not in (body or "")[:80] else "")
        )
    if status == 402:
        return (
            "WB buyer-chat: 402 — требуется платёж или подписка."
            + (f" {detail}" if detail else "")
        )
    if status == 429:
        return (
            "WB чаты: 429 — лимит buyer-chat-api (10 запросов / 10 с). "
            "Подождите 1–2 минуты. Не открывайте вкладку чатов параллельно с автозапуском."
            + (f" {detail}" if detail else "")
        )
    if status == 400:
        return f"WB buyer-chat: 400 — неверный запрос. {detail}"
    return f"WB buyer-chat: HTTP {status}. {detail}"

# ~0.9 rps ≈ 1.1 с между запросами — укладывается в 10 запросов / 10 с (личный/сервисный тариф WB).
_wb_buyer_rl = RateLimiter(0.9)
_wb_buyer_serial = asyncio.Lock()


def _wb_buyer_429_sleep_seconds(headers: Any) -> tuple[int, str]:
    """
    Пауза после 429 по заголовкам WB.

    Раньше паузу искусственно резали до ~40 с; WB мог отдавать Retry-After на тысячи секунд —
    просыпались рано и снова получали 429.
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
    def __init__(self, api_key: str, *, timeout_s: float = 60.0) -> None:
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
                            "Долгую паузу задаёт WB; при персональном ключе с «Чат» это часто суммарная "
                            "нагрузка (несколько магазинов, автозапуск, вкладка чатов) и лимит на аккаунт.",
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
                            "Долгую паузу задаёт WB; при персональном ключе с «Чат» это часто суммарная "
                            "нагрузка (несколько магазинов, автозапуск, вкладка чатов) и лимит на аккаунт.",
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
            api_errs = data.get("errors")
            if isinstance(api_errs, list) and api_errs:
                raise HttpStatusError(400, json.dumps({"errors": api_errs}))
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
            api_errs = data.get("errors")
            if isinstance(api_errs, list) and api_errs:
                raise HttpStatusError(400, json.dumps({"errors": api_errs}))
            res = data.get("result")
            if isinstance(res, dict):
                return res
            return {}

        return await retry(_do, retry_on_status=(500, 502, 503, 504), retries=3)

    async def send_message(self, reply_sign: str, message: str) -> dict:
        reply_sign = (reply_sign or "").strip()
        message = format_wb_seller_outgoing(message)
        if not reply_sign or not message:
            raise ValueError("reply_sign и message обязательны")
        msg_cut = message[:1000]

        async def _do():
            form = aiohttp.FormData()
            form.add_field("replySign", reply_sign)
            form.add_field("message", msg_cut)
            data = await self._http_post_multipart("/api/v1/seller/message", form)
            if isinstance(data, dict):
                api_errs = data.get("errors")
                if isinstance(api_errs, list) and api_errs:
                    raise HttpStatusError(400, json.dumps({"errors": api_errs}))
            return data if isinstance(data, dict) else {}

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


def format_wb_seller_outgoing(message: str) -> str:
    """Текст для отправки в WB с узнаваемым префиксом продавца."""
    body = (message or "").strip()
    if not body:
        return ""
    low = body.lower()
    if low.startswith(WB_SELLER_REPLY_PREFIX.lower()):
        return body[:1000]
    quoted = f'{WB_SELLER_REPLY_PREFIX} "{body}"'
    if len(quoted) <= 1000:
        return quoted
    plain = f"{WB_SELLER_REPLY_PREFIX} {body}"
    return plain[:1000]


def role_from_prefixed_text(text: str) -> Optional[str]:
    t = (text or "").lstrip()
    if t.lower().startswith(WB_SELLER_REPLY_PREFIX.lower()):
        return "seller"
    return None


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


def _bool_flag(val: Any) -> Optional[bool]:
    if val is True or val == 1:
        return True
    if val is False or val == 0:
        return False
    if isinstance(val, str):
        s = val.strip().lower()
        if s in ("true", "1", "yes"):
            return True
        if s in ("false", "0", "no"):
            return False
    return None


_CLIENT_SOURCES = frozenset({"rusite", "client", "buyer", "customer", "mobile", "ios", "android", "site"})


def _event_role(ev: dict) -> str:
    """Роль автора события: client / seller / other."""
    if not isinstance(ev, dict):
        return "other"
    seller_flag = _bool_flag(ev.get("isSeller"))
    if seller_flag is True:
        return "seller"
    if seller_flag is False:
        return "client"
    sender = str(ev.get("sender") or "").strip().lower()
    if sender == "client":
        return "client"
    if sender in ("seller", "seller-public-api", "seller-portal", "seller-web"):
        return "seller"
    source = str(ev.get("source") or "").strip().lower()
    if source in _CLIENT_SOURCES:
        return "client"
    if source.startswith("seller") or "seller" in source:
        return "seller"
    msg = ev.get("message")
    if isinstance(msg, dict):
        ms = str(msg.get("sender") or "").strip().lower()
        if ms == "client":
            return "client"
        if ms in ("seller", "seller-public-api", "seller-portal", "seller-web"):
            return "seller"
    # У сообщений покупателя в events часто есть clientName; у ответов продавца — нет.
    client_name = str(ev.get("clientName") or "").strip()
    if client_name and sender != "seller" and seller_flag is not True:
        return "client"
    if not client_name and source and source not in _CLIENT_SOURCES and "seller" in source:
        return "seller"
    if not client_name and sender in ("", "seller") and source and "seller" in source:
        return "seller"
    return sender or source or "other"


def _resolve_last_message_role(
    lm: dict,
    chat_row: Optional[dict] = None,
    events: Optional[List[dict]] = None,
    chat_id: str = "",
) -> str:
    """Роль lastMessage: в списке чатов WB часто только text + addTimestamp без sender."""
    if isinstance(lm, dict) and lm:
        role = _event_role(lm)
        if role in ("client", "seller"):
            return role
    cid = (chat_id or (chat_row or {}).get("chatID") or "").strip()
    if events and cid:
        ts = int(lm.get("addTimestamp") or 0)
        text = str(lm.get("text") or "").strip()
        for ev in events:
            if str(ev.get("chatID") or "").strip() != cid:
                continue
            if str(ev.get("eventType") or "") != "message":
                continue
            if int(ev.get("addTimestamp") or 0) != ts:
                continue
            if text and _event_text(ev) != text:
                continue
            role = _event_role(ev)
            if role in ("client", "seller"):
                return role
        lines = collect_thread_lines(events, cid)
        if lines:
            _r, _t, last_ts, _mk = lines[-1]
            if abs(int(lm.get("addTimestamp") or 0) - last_ts) <= 3000:
                if _r in ("client", "seller"):
                    return _r
    if isinstance(chat_row, dict):
        role = _event_role(chat_row)
        if role in ("client", "seller"):
            return role
    return "other"


def collect_thread_lines(events: List[dict], chat_id: str) -> List[Tuple[str, str, int, str]]:
    """Сообщения одного чата: (role, text, addTimestamp, message_key)."""
    out: List[Tuple[str, str, int, str]] = []
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
        role = role_from_prefixed_text(text) or _event_role(ev)
        ts = int(ev.get("addTimestamp") or 0)
        event_id = str(ev.get("eventID") or ev.get("id") or "").strip()
        msg_key = event_id if event_id else str(ts)
        out.append((role, text, ts, msg_key))
    out.sort(key=lambda x: x[2])
    return out


def fallback_line_from_chat_row(
    chat_row: dict,
    events: Optional[List[dict]] = None,
) -> List[Tuple[str, str, int, str]]:
    """Если в /events нет сообщений — одна строка из lastMessage списка чатов."""
    if not isinstance(chat_row, dict):
        return []
    lm = chat_row.get("lastMessage") or {}
    if not isinstance(lm, dict):
        return []
    t = str(lm.get("text") or "").strip()
    if not t:
        return []
    ts = int(lm.get("addTimestamp") or 0)
    cid = str(chat_row.get("chatID") or "").strip()
    role = role_from_prefixed_text(t) or _resolve_last_message_role(lm, chat_row, events, cid)
    return [(role, t, ts, str(ts))]


def build_wb_thread_lines(
    events: List[dict],
    chat_id: str,
    chat_row: Optional[dict] = None,
) -> List[Tuple[str, str, int, str]]:
    """Переписка: events + lastMessage из списка чатов, если в ленте не хватает строк."""
    cid = (chat_id or "").strip()
    lines = collect_thread_lines(events, cid)
    if not isinstance(chat_row, dict):
        return lines
    lm = chat_row.get("lastMessage") or {}
    if not isinstance(lm, dict):
        return lines
    text = str(lm.get("text") or "").strip()
    if not text:
        return lines
    ts = int(lm.get("addTimestamp") or 0)
    if any(abs(ts - ts_) <= 3000 and t.strip() == text for _, t, ts_, __ in lines):
        return lines
    role = role_from_prefixed_text(text) or _resolve_last_message_role(lm, chat_row, events, cid)
    lines = list(lines)
    lines.append((role, text, ts, str(ts)))
    lines.sort(key=lambda x: x[2])
    return lines


def last_client_message_info(lines_ts: List[Tuple[str, str, int, str]]) -> Optional[Tuple[str, int]]:
    """Ключ и timestamp последнего сообщения покупателя."""
    for role, _text, ts, msg_key in reversed(lines_ts):
        if role == "client":
            return msg_key, ts
    return None


async def fetch_events_for_chat(
    client: WbBuyerChatClient,
    chat_id: str,
    *,
    max_wb_requests: int = 8,
) -> Tuple[List[dict], Optional[int]]:
    """
    Подтягивает страницы /seller/events и отбирает события выбранного чата.
    Лента общая для всех чатов — для старых диалогов нужно больше страниц.
    """
    merged: List[dict] = []
    seen_event_ids: set[str] = set()
    next_cursor: Optional[int] = None
    last_next: Optional[int] = None
    for _ in range(max_wb_requests):
        block = await client.get_events(next_cursor=next_cursor)
        events = block.get("events") or []
        if not isinstance(events, list):
            events = []
        for ev in events:
            if not isinstance(ev, dict):
                continue
            if str(ev.get("chatID") or "").strip() != (chat_id or "").strip():
                continue
            eid = str(ev.get("eventID") or ev.get("id") or "").strip()
            if eid:
                if eid in seen_event_ids:
                    continue
                seen_event_ids.add(eid)
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
