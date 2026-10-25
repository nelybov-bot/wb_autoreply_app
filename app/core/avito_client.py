"""
Клиент Avito Business API (OAuth client_credentials).

Хранение в магазине:
  client_id  → Avito client_id
  api_key    → Avito client_secret
  business_id → Avito user_id (кэш после /core/v1/accounts/self)
"""
from __future__ import annotations

import logging
import time
import json
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from .net import HttpStatusError, USER_AGENT, retry

log = logging.getLogger("avito")

BASE = "https://api.avito.ru"
TOKEN_URL = f"{BASE}/token"


class AvitoClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        *,
        user_id: Optional[int] = None,
        timeout_s: float = 25.0,
    ) -> None:
        self.client_id = (client_id or "").strip()
        self.client_secret = (client_secret or "").strip()
        self.user_id = int(user_id) if user_id else None
        self.timeout = aiohttp.ClientTimeout(connect=15, total=timeout_s)
        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0

    async def _fetch_token(self) -> str:
        if not self.client_id or not self.client_secret:
            raise HttpStatusError(401, "нет client_id / client_secret Avito")

        async def _do() -> str:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.post(
                    TOKEN_URL,
                    data={
                        "grant_type": "client_credentials",
                        "client_id": self.client_id,
                        "client_secret": self.client_secret,
                    },
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "User-Agent": USER_AGENT,
                    },
                ) as resp:
                    text = await resp.text()
                    if resp.status >= 400:
                        raise HttpStatusError(int(resp.status), text[:800])
                    try:
                        data = json.loads(text)
                    except Exception as e:
                        raise HttpStatusError(resp.status, f"bad token JSON: {e}; {text[:400]}")
                    token = str((data or {}).get("access_token") or "").strip()
                    if not token:
                        raise HttpStatusError(401, f"нет access_token: {text[:400]}")
                    expires_in = int((data or {}).get("expires_in") or 86400)
                    # Обновляем чуть раньше истечения.
                    self._token = token
                    self._token_expires_at = time.time() + max(60, expires_in - 120)
                    return token

        return await retry(_do, retries=3, retry_on_status=(429, 500, 502, 503, 504))

    async def _ensure_token(self, *, force: bool = False) -> str:
        if (
            not force
            and self._token
            and time.time() < self._token_expires_at
        ):
            return self._token
        return await self._fetch_token()

    def _auth_headers(self, token: str) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        }

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[dict] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        retry_auth: bool = True,
    ) -> Any:
        url = BASE + path

        async def _do(force_token: bool = False) -> Any:
            token = await self._ensure_token(force=force_token)
            headers = self._auth_headers(token)
            if extra_headers:
                headers.update(extra_headers)
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    json=json_body,
                ) as resp:
                    text = await resp.text()
                    if resp.status in (401, 403) and retry_auth and not force_token:
                        # Avito часто отдаёт 403 на просроченный токен.
                        body_l = (text or "").lower()
                        if "token" in body_l or "unauthorized" in body_l or resp.status == 401:
                            raise _AuthRetryNeeded(text)
                    if resp.status >= 400:
                        raise HttpStatusError(int(resp.status), text[:1200])
                    if not text.strip():
                        return {}
                    try:
                        return json.loads(text)
                    except Exception:
                        return {"_raw": text[:1200]}

        try:
            return await retry(lambda: _do(False), retries=4, retry_on_status=(429, 500, 502, 503, 504))
        except _AuthRetryNeeded:
            return await retry(lambda: _do(True), retries=3, retry_on_status=(429, 500, 502, 503, 504))

    async def get_self(self) -> dict:
        data = await self._request("GET", "/core/v1/accounts/self")
        return data if isinstance(data, dict) else {}

    async def resolve_user_id(self) -> int:
        if self.user_id:
            return int(self.user_id)
        me = await self.get_self()
        uid = me.get("id") or me.get("user_id")
        if uid is None:
            raise HttpStatusError(400, f"не удалось получить Avito user_id: {me!r}"[:400])
        self.user_id = int(uid)
        return int(self.user_id)

    async def list_orders(
        self,
        *,
        statuses: Optional[List[str]] = None,
        date_from: Optional[int] = None,
        page: int = 1,
        limit: int = 20,
        ids: Optional[List[str]] = None,
    ) -> dict:
        params: Dict[str, Any] = {
            "page": max(1, int(page)),
            "limit": max(1, min(20, int(limit))),
        }
        if statuses:
            params["statuses"] = statuses
        if date_from is not None:
            params["dateFrom"] = int(date_from)
        if ids:
            params["ids"] = ids
        data = await self._request("GET", "/order-management/1/orders", params=params)
        return data if isinstance(data, dict) else {"orders": [], "hasMore": False}

    async def list_orders_all(
        self,
        *,
        statuses: Optional[List[str]] = None,
        date_from: Optional[int] = None,
        max_pages: int = 10,
    ) -> List[dict]:
        out: List[dict] = []
        page = 1
        while page <= max_pages:
            data = await self.list_orders(
                statuses=statuses,
                date_from=date_from,
                page=page,
                limit=20,
            )
            batch = data.get("orders") or []
            if not isinstance(batch, list):
                break
            for row in batch:
                if isinstance(row, dict):
                    out.append(row)
            if not data.get("hasMore") or not batch:
                break
            page += 1
        return out

    async def list_chats(
        self,
        *,
        unread_only: bool = False,
        limit: int = 50,
        offset: int = 0,
        user_id: Optional[int] = None,
    ) -> List[dict]:
        uid = int(user_id) if user_id else await self.resolve_user_id()
        params: Dict[str, Any] = {
            "limit": max(1, min(100, int(limit))),
            "offset": max(0, int(offset)),
        }
        if unread_only:
            # Avito OpenAPI: unreadOnly; часть клиентов шлёт unread_only.
            params["unreadOnly"] = "true"
            params["unread_only"] = "true"
        data = await self._request(
            "GET",
            f"/messenger/v2/accounts/{uid}/chats",
            params=params,
        )
        if isinstance(data, dict):
            chats = data.get("chats")
            if isinstance(chats, list):
                return [c for c in chats if isinstance(c, dict)]
        if isinstance(data, list):
            return [c for c in data if isinstance(c, dict)]
        return []

    async def get_chat(
        self,
        chat_id: str,
        *,
        user_id: Optional[int] = None,
    ) -> dict:
        """Один чат: context (товар) + users. GET /messenger/v2/accounts/{id}/chats/{chat_id}"""
        uid = int(user_id) if user_id else await self.resolve_user_id()
        cid = (chat_id or "").strip()
        if not cid:
            return {}
        data = await self._request(
            "GET",
            f"/messenger/v2/accounts/{uid}/chats/{cid}",
        )
        return data if isinstance(data, dict) else {}

    async def list_chat_messages(
        self,
        chat_id: str,
        *,
        limit: int = 20,
        offset: int = 0,
        user_id: Optional[int] = None,
    ) -> List[dict]:
        uid = int(user_id) if user_id else await self.resolve_user_id()
        cid = (chat_id or "").strip()
        if not cid:
            return []
        params = {
            "limit": max(1, min(100, int(limit))),
            "offset": max(0, int(offset)),
        }
        data = await self._request(
            "GET",
            f"/messenger/v3/accounts/{uid}/chats/{cid}/messages/",
            params=params,
        )
        if isinstance(data, dict):
            msgs = data.get("messages")
            if isinstance(msgs, list):
                return [m for m in msgs if isinstance(m, dict)]
        if isinstance(data, list):
            return [m for m in data if isinstance(m, dict)]
        return []

    async def get_balance(self, *, user_id: Optional[int] = None) -> dict:
        """Кошелёк ЛК: real + bonus (рубли). GET /core/v1/accounts/{user_id}/balance/"""
        uid = int(user_id) if user_id else await self.resolve_user_id()
        data = await self._request("GET", f"/core/v1/accounts/{uid}/balance/")
        return data if isinstance(data, dict) else {}

    async def get_ads_balance(self, *, user_id: Optional[int] = None) -> dict:
        """Рекламный баланс: balance + bonusBalance (рубли)."""
        uid = int(user_id) if user_id else await self.resolve_user_id()
        data = await self._request("GET", f"/ads/v1/account/{uid}/balance")
        return data if isinstance(data, dict) else {}

    async def get_cpa_balance(self) -> dict:
        """CPA-кошелёк в копейках. POST /cpa/v3/balanceInfo (лимит ~1/мин)."""
        data = await self._request(
            "POST",
            "/cpa/v3/balanceInfo",
            json_body={},
            extra_headers={"X-Source": "MarketAI"},
        )
        return data if isinstance(data, dict) else {}

    async def upload_image(
        self,
        image_bytes: bytes,
        *,
        filename: str = "photo.jpg",
        content_type: str = "image/jpeg",
        user_id: Optional[int] = None,
    ) -> str:
        """Загрузка одного изображения. Возвращает image_id."""
        uid = int(user_id) if user_id else await self.resolve_user_id()
        if not image_bytes:
            raise HttpStatusError(400, "пустой файл изображения")
        # Лимит Avito — 24 МБ.
        if len(image_bytes) > 24 * 1024 * 1024:
            raise HttpStatusError(400, "файл больше 24 МБ")

        url = f"{BASE}/messenger/v1/accounts/{uid}/uploadImages"

        async def _do(force_token: bool = False) -> str:
            token = await self._ensure_token(force=force_token)
            headers = {
                "Authorization": f"Bearer {token}",
                "User-Agent": USER_AGENT,
            }
            form = aiohttp.FormData()
            form.add_field(
                "uploadfile[]",
                image_bytes,
                filename=filename or "photo.jpg",
                content_type=content_type or "image/jpeg",
            )
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.post(url, data=form, headers=headers) as resp:
                    text = await resp.text()
                    if resp.status in (401, 403) and not force_token:
                        body_l = (text or "").lower()
                        if "token" in body_l or "unauthorized" in body_l or resp.status == 401:
                            raise _AuthRetryNeeded(text)
                    if resp.status >= 400:
                        raise HttpStatusError(int(resp.status), text[:1200])
                    try:
                        data = json.loads(text) if text.strip() else {}
                    except Exception as e:
                        raise HttpStatusError(resp.status, f"bad upload JSON: {e}; {text[:400]}")
                    if not isinstance(data, dict) or not data:
                        raise HttpStatusError(400, f"нет image_id в ответе: {text[:400]}")
                    # Ответ: { "<image_id>": { "1280x960": "url", ... } }
                    image_id = next(iter(data.keys()))
                    if not image_id:
                        raise HttpStatusError(400, f"пустой image_id: {text[:400]}")
                    return str(image_id)

        try:
            return await retry(lambda: _do(False), retries=3, retry_on_status=(429, 500, 502, 503, 504))
        except _AuthRetryNeeded:
            return await retry(lambda: _do(True), retries=2, retry_on_status=(429, 500, 502, 503, 504))

    async def send_image_message(
        self,
        chat_id: str,
        image_id: str,
        *,
        user_id: Optional[int] = None,
    ) -> dict:
        uid = int(user_id) if user_id else await self.resolve_user_id()
        cid = (chat_id or "").strip()
        iid = (image_id or "").strip()
        if not cid or not iid:
            raise HttpStatusError(400, "нужны chat_id и image_id")
        data = await self._request(
            "POST",
            f"/messenger/v1/accounts/{uid}/chats/{cid}/messages/image",
            json_body={"image_id": iid},
        )
        return data if isinstance(data, dict) else {"ok": True}

    async def send_text_message(
        self,
        chat_id: str,
        text: str,
        *,
        user_id: Optional[int] = None,
    ) -> dict:
        uid = int(user_id) if user_id else await self.resolve_user_id()
        cid = (chat_id or "").strip()
        body = (text or "").strip()
        if not cid or not body:
            raise HttpStatusError(400, "нужны chat_id и текст")
        if len(body) > 1000:
            body = body[:997] + "…"
        data = await self._request(
            "POST",
            f"/messenger/v1/accounts/{uid}/chats/{cid}/messages",
            json_body={"type": "text", "message": {"text": body}},
        )
        return data if isinstance(data, dict) else {"ok": True}

    async def subscribe_webhook(self, url: str) -> dict:
        data = await self._request(
            "POST",
            "/messenger/v3/webhook",
            json_body={"url": (url or "").strip()},
        )
        return data if isinstance(data, dict) else {"ok": True}

    async def unsubscribe_webhook(self, url: str) -> dict:
        data = await self._request(
            "POST",
            "/messenger/v1/webhook/unsubscribe",
            json_body={"url": (url or "").strip()},
        )
        return data if isinstance(data, dict) else {"ok": True}


class _AuthRetryNeeded(Exception):
    def __init__(self, body: str) -> None:
        self.body = body
        super().__init__(body)


def order_display_id(order: dict) -> str:
    for key in ("marketplaceId", "id"):
        val = order.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    return ""


def order_item_titles(order: dict, *, max_items: int = 3) -> str:
    items = order.get("items") or []
    titles: List[str] = []
    if isinstance(items, list):
        for it in items[:max_items]:
            if not isinstance(it, dict):
                continue
            t = str(it.get("title") or "").strip()
            if t:
                titles.append(t)
    if not titles:
        return "—"
    extra = len(items) - len(titles) if isinstance(items, list) else 0
    s = "; ".join(titles)
    if extra > 0:
        s += f" (+ ещё {extra})"
    return s


def order_total_rub(order: dict) -> str:
    prices = order.get("prices") or {}
    if not isinstance(prices, dict):
        return "—"
    for key in ("total", "price"):
        if prices.get(key) is not None:
            try:
                n = float(prices[key])
                s = f"{n:,.0f}".replace(",", " ")
                return f"{s} ₽"
            except (TypeError, ValueError):
                return f"{prices[key]} ₽"
    return "—"


def chat_id_of(chat: dict) -> str:
    for key in ("id", "chat_id"):
        val = chat.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    return ""


def chat_last_message(chat: dict) -> Optional[dict]:
    lm = chat.get("last_message") or chat.get("lastMessage")
    return lm if isinstance(lm, dict) else None


def message_text_preview(msg: dict, *, max_len: int = 280) -> str:
    if not isinstance(msg, dict):
        return ""
    mtype = str(msg.get("type") or "").strip().lower()
    content = msg.get("content") if isinstance(msg.get("content"), dict) else {}
    text = ""
    if mtype in ("text", "") or content.get("text"):
        raw = content.get("text") if content else msg.get("text")
        text = str(raw or "").strip()
    elif mtype == "image":
        text = "[изображение]"
    elif mtype == "voice":
        text = "[голосовое]"
    elif mtype == "link":
        link = content.get("link") if isinstance(content.get("link"), dict) else {}
        text = str(link.get("url") or link.get("text") or "[ссылка]").strip()
    elif mtype == "item":
        item = content.get("item") if isinstance(content.get("item"), dict) else {}
        text = str(item.get("title") or "[объявление]").strip()
    elif mtype == "system":
        text = "[системное сообщение]"
    elif mtype == "call":
        text = "[звонок]"
    elif mtype == "location":
        text = "[геометка]"
    elif mtype == "deleted":
        text = "[удалено]"
    else:
        text = f"[{mtype or 'сообщение'}]" if mtype else "[сообщение]"
    text = " ".join(text.split())
    if len(text) > max_len:
        return text[: max_len - 1] + "…"
    return text


def message_image_url(msg: dict) -> str:
    """Лучший URL картинки из сообщения типа image (предпочитаем больший размер)."""
    if not isinstance(msg, dict):
        return ""
    content = msg.get("content") if isinstance(msg.get("content"), dict) else {}
    image = content.get("image") if isinstance(content.get("image"), dict) else {}
    sizes = image.get("sizes") if isinstance(image.get("sizes"), dict) else {}
    if not sizes:
        return ""
    best_url = ""
    best_area = -1
    for key, url in sizes.items():
        if not url:
            continue
        area = 0
        try:
            parts = str(key).lower().replace("×", "x").split("x")
            if len(parts) == 2:
                area = int(parts[0]) * int(parts[1])
        except (TypeError, ValueError):
            area = 0
        if area >= best_area:
            best_area = area
            best_url = str(url).strip()
    return best_url


def _as_float(val: Any) -> Optional[float]:
    if val is None or val is False:
        return None
    if isinstance(val, bool):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _pick_number(data: dict, *keys: str) -> Optional[float]:
    for key in keys:
        if key in data:
            num = _as_float(data.get(key))
            if num is not None:
                return num
    return None


def parse_wallet_balance(balance: dict) -> tuple[float, float, float]:
    """Кошелёк ЛК → (real, bonus, total) в рублях."""
    if not isinstance(balance, dict):
        return 0.0, 0.0, 0.0
    raw = balance
    nested = balance.get("balance")
    if isinstance(nested, dict) and ("real" in nested or "bonus" in nested):
        raw = nested
    result = balance.get("result")
    if isinstance(result, dict) and ("real" in result or "bonus" in result):
        raw = result
    real = _pick_number(raw, "real", "amountRub", "amount_rub") or 0.0
    bonus = _pick_number(raw, "bonus", "amountBonus", "amount_bonus") or 0.0
    return real, bonus, real + bonus


def parse_ads_balance(balance: dict) -> tuple[float, float, float]:
    """Рекламный аккаунт → (balance, bonus, total) в рублях."""
    if not isinstance(balance, dict):
        return 0.0, 0.0, 0.0
    raw = balance
    nested = balance.get("balance")
    # Иногда обёртка { "balance": { "balance": 1, "bonusBalance": 2 } }
    if isinstance(nested, dict) and (
        "balance" in nested or "bonusBalance" in nested or "balanceKopeks" in nested
    ):
        raw = nested
    rub = _pick_number(raw, "balance", "amount")
    bonus = _pick_number(raw, "bonusBalance", "bonus_balance", "bonus")
    if rub is None:
        kopeks = _pick_number(raw, "balanceKopeks", "balance_kopeks")
        rub = (kopeks / 100.0) if kopeks is not None else 0.0
    if bonus is None:
        b_kopeks = _pick_number(raw, "bonusBalanceKopeks", "bonus_balance_kopeks")
        bonus = (b_kopeks / 100.0) if b_kopeks is not None else 0.0
    return float(rub), float(bonus), float(rub) + float(bonus)


def parse_cpa_balance(balance: dict) -> tuple[float, float]:
    """CPA → (total_rub, kopeks). Баланс в копейках."""
    if not isinstance(balance, dict):
        return 0.0, 0.0
    raw = balance
    result = balance.get("result")
    if isinstance(result, dict) and "balance" in result:
        raw = result
    kopeks = _pick_number(raw, "balance")
    if kopeks is None:
        return 0.0, 0.0
    return float(kopeks) / 100.0, float(kopeks)


def balance_total_rub(balance: dict) -> float:
    """Обратная совместимость: сумма кошелька ЛК."""
    _real, _bonus, total = parse_wallet_balance(balance)
    return total


def message_id_of(msg: dict) -> str:
    for key in ("id", "message_id"):
        val = msg.get(key) if isinstance(msg, dict) else None
        if val is not None and str(val).strip():
            return str(val).strip()
    return ""


def chat_item_title(chat: dict) -> str:
    if not isinstance(chat, dict):
        return "—"
    ctx = chat.get("context") if isinstance(chat.get("context"), dict) else {}
    value = ctx.get("value") if isinstance(ctx.get("value"), dict) else {}
    title = value.get("title") or value.get("name")
    if title:
        return str(title).strip()
    # Иногда title лежит прямо в context.
    title = ctx.get("title") or ctx.get("name")
    if title:
        return str(title).strip()
    item = chat.get("item") if isinstance(chat.get("item"), dict) else {}
    title = item.get("title") or item.get("name")
    if title:
        return str(title).strip()
    return "—"


def chat_has_meta(chat: dict) -> bool:
    """Есть ли у объекта чата товар и/или участники."""
    if not isinstance(chat, dict):
        return False
    if chat_item_title(chat) not in ("", "—"):
        return True
    users = chat.get("users")
    return isinstance(users, list) and any(isinstance(u, dict) for u in users)


def chat_buyer_name(
    chat: dict,
    *,
    author_id: Any = None,
    our_user_id: Optional[int] = None,
) -> str:
    """Имя собеседника из users чата (без сырого ID)."""
    users = chat.get("users")
    if not isinstance(users, list):
        return ""

    def _name_of(u: dict) -> str:
        for key in ("name", "public_name", "username"):
            val = u.get(key)
            if val and str(val).strip():
                return str(val).strip()
        profile = u.get("public_user_profile") or u.get("profile")
        if isinstance(profile, dict):
            for key in ("name", "public_name", "username"):
                val = profile.get(key)
                if val and str(val).strip():
                    return str(val).strip()
        return ""

    want: Optional[int] = None
    try:
        if author_id is not None:
            want = int(author_id)
    except (TypeError, ValueError):
        want = None

    our: Optional[int] = None
    try:
        if our_user_id is not None:
            our = int(our_user_id)
    except (TypeError, ValueError):
        our = None

    if want is not None:
        for u in users:
            if not isinstance(u, dict):
                continue
            try:
                uid = int(u.get("id"))
            except (TypeError, ValueError):
                continue
            if uid == want:
                return _name_of(u)

    for u in users:
        if not isinstance(u, dict):
            continue
        try:
            uid = int(u.get("id"))
        except (TypeError, ValueError):
            uid = None
        if our is not None and uid == our:
            continue
        name = _name_of(u)
        if name:
            return name
    return ""
