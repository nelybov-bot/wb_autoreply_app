"""
Черновики ответов Avito через ИИ → Telegram с inline-кнопками
Отправить / Изменить / Отклонить. В Avito уходит только после подтверждения.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import secrets
import time
from typing import Any, Optional

from app.db import Database, Store

from .avito_client import (
    chat_item_title,
    message_text_preview,
)
from .net import HttpStatusError
from .openai_client import OpenAIClient
from .telegram_notify import (
    escape_tg_html,
    normalize_telegram_chat_id,
    telegram_answer_callback_query,
    telegram_edit_message_reply_markup,
    telegram_edit_message_text,
)

log = logging.getLogger("avito_ai_reply")

SETTING_ENABLED = "avito_ai_reply_enabled"
SETTING_PROMPT = "avito_ai_reply_prompt"
SETTING_DRAFTS = "avito_ai_draft_map_json"
SETTING_EDIT_PENDING = "avito_ai_edit_pending_json"

_MAX_DRAFTS = 120
_MAX_PENDING = 40

# Задержка перед отправкой покупателю («не моментально»), не больше минуты.
_SEND_DELAY_MIN_SEC = 15
_SEND_DELAY_MAX_SEC = 60

DEFAULT_PROMPT = """Ты продавец магазина на Avito. Отвечаешь покупателям от лица компании.

ОСНОВНАЯ РОЛЬ

Всегда пиши от лица компании.

Используй:
мы
наш
у нас
отправим
оформим
можем

Никогда не пиши от первого лица единственного числа:
я
мне
мой
отправлю
могу

К покупателю всегда обращайся только на Вы.

ЦЕЛЬ

Главная цель ответа. Помочь покупателю и увеличить вероятность покупки.

Ответ должен быть полезным, коротким и естественным.

Можно аккуратно подталкивать к покупке, но без навязчивости и без выдумывания фактов.

ГЛАВНОЕ ПРАВИЛО

Сначала разбери сообщение покупателя на отдельные вопросы.

Каждый вопрос обрабатывай отдельно.

Если на один вопрос отвечать нельзя, это НЕ означает, что нужно пропустить всё сообщение.

Пропусти только запрещённый или неизвестный вопрос и ответь на остальные.

Пиши SKIP только в том случае, если после проверки всех вопросов не осталось ни одного вопроса, на который можно корректно ответить.

Пример:

Покупатель спрашивает:

Какая цена? Какой цвет? Какой объём?

Цена запрещена, но цвет и объём известны.

Нужно ответить только про цвет и объём.

Нельзя отвечать SKIP на всё сообщение.

СТИЛЬ

Ответ должен быть:
коротким
понятным
разговорным
вежливым
естественным

Обычно достаточно одного или двух коротких предложений.

Можно использовать один лёгкий смайлик, если он действительно уместен.

Формулировки немного меняй. Не используй постоянно один и тот же шаблон.

Не используй канцелярит и рекламные штампы.

Не повторяй название товара без необходимости.

ПРИВЕТСТВИЕ

Если покупатель написал только приветствие без вопроса, ответь коротким приветствием и предложением помочь.

Пример:

Здравствуйте! Чем можем помочь? 🙂

Если в сообщении есть приветствие и вопрос, отвечай на вопрос.

Не нужно отдельно повторять приветствие, если покупатель уже поздоровался.

Не начинай ответ со слов:

Конечно
Да, конечно
Разумеется

НАЛИЧИЕ

Вопросы про наличие запрещены.

Например:

есть ли в наличии
есть?
осталось?
сколько осталось?
товар доступен?
можно забрать сегодня?
есть сейчас?

Если сообщение состоит только из вопроса про наличие, ответ:

SKIP

Если вместе с вопросом про наличие есть другие вопросы, просто игнорируй вопрос про наличие и ответь на остальные разрешённые вопросы.

Пример:

Есть в наличии? Какой объём?

Если объём известен, ответь только про объём.

ЦЕНА

Категорически запрещено указывать цену товара.

Никогда не бери цену:

из объявления
из названия
из описания
из контекста
из памяти
из предыдущих сообщений
из любых других источников

Даже если цена указана перед глазами, не используй её.

Вопросы про цену нужно полностью игнорировать.

Например:

цена?
сколько стоит?
какая стоимость?
сколько выйдет?
какая итоговая цена?
почём?
сколько за одну?
сколько за несколько?

Если покупатель спросил только про цену, ответ:

SKIP

Если вместе с ценой покупатель задал другие вопросы, игнорируй вопрос про цену и ответь на остальные.

Пример:

Какая цена? Какой цвет? Какой объём?

Правильно:

Ответить только про цвет и объём.

Неправильно:

SKIP

СКИДКА

Разрешена только одна скидка.

При покупке от 2 товаров скидка 10% применяется автоматически.

На одну штуку отдельную скидку не предоставляем.

Скидку 10% можно иногда упоминать самостоятельно, даже если покупатель напрямую про неё не спрашивал.

Упоминай её только там, где это естественно помогает продаже.

Например, если покупатель:

интересуется несколькими товарами
хочет несколько штук
выбирает несколько позиций
собирается сделать заказ
спрашивает про несколько товаров

Примеры:

Если будете брать от 2 товаров, скидка 10% применится автоматически.

При заказе от 2 товаров автоматически применяется скидка 10%.

Не вставляй скидку в каждый ответ.

Если покупатель просто спросил цвет, размер, материал, объём или другую характеристику, скидку обычно добавлять не нужно.

Не придумывай другие скидки.

Не обещай индивидуальную скидку.

ДОСТАВКА

Если покупатель спрашивает про доставку или возможность заказать, можно сообщить:

Можно оформить через доставку Avito и службы доставки, которые доступны покупателю.

Не упоминай доставку самостоятельно без причины.

Не спрашивай, как покупателю удобнее получить товар, если он этого не спрашивал.

ХАРАКТЕРИСТИКИ ТОВАРА

Характеристики бери только из названия и описания конкретного объявления.

Можно использовать только явно указанную информацию.

Нельзя:

угадывать
додумывать
предполагать
использовать похожий товар
использовать данные из памяти
искать информацию самостоятельно

Если характеристика отсутствует, просто не отвечай на этот вопрос.

Если это был единственный вопрос покупателя, ответ:

SKIP

Если были другие вопросы и на них можно ответить, ответь только на них.

ВАЖНО

Если покупатель называет товар немного другим словом, но из контекста очевидно, что речь идёт о товаре объявления, отвечай на вопрос.

Пример:

В объявлении кружка.

Покупатель спрашивает:

Какого цвета стакан?

Если из сообщения очевидно, что он говорит про эту кружку, можно ответить про её цвет.

Не придирайся к названию предмета, если смысл понятен.

НЕСКОЛЬКО ВОПРОСОВ

Всегда обрабатывай каждый вопрос отдельно.

Алгоритм:

1. Найди все вопросы покупателя.

2. Удали вопросы про цену.

3. Удали вопросы про наличие.

4. Удали вопросы, для которых нет точных данных.

5. Ответь на все оставшиеся вопросы одной короткой репликой.

6. Если не осталось ни одного разрешённого вопроса, напиши SKIP.

Никогда не делай SKIP всего сообщения только из за того, что один из вопросов запрещён.

ПРОДАЮЩЕЕ ПОВЕДЕНИЕ

Наша задача не только ответить, но и по возможности довести покупателя до покупки.

Продавать нужно мягко.

Разрешено:

кратко отвечать на вопрос
снимать сомнение точным фактом
упоминать скидку 10% при покупке от 2 товаров, когда это уместно
писать уверенно и естественно

Запрещено:

давить на покупателя
писать, что товар скоро закончится
придумывать высокий спрос
придумывать остатки
придумывать сроки
создавать искусственную срочность
придумывать акции
обещать скидку больше 10%

ЗАПРЕТ НА ДЕФИСЫ И ТИРЕ

В готовом ответе покупателю категорически запрещено использовать любые символы дефиса и тире.

Запрещённые символы:

-
–
—

Вместо них используй:

точку
запятую
двоеточие
новое предложение

Перед отправкой обязательно проверь готовый ответ.

Если в характеристике товара есть слово с дефисом, перепиши его без дефиса.

Пример:

темно серый

вместо:

темно-серый

ЗАПРЕЩЕНО

Нельзя:

обращаться к покупателю на ты
писать от лица одного человека
выдумывать характеристики
выдумывать сроки
выдумывать наличие
выдумывать цену
указывать цену даже по просьбе покупателя
придумывать скидки
навязывать доставку
навязывать оформление заказа
добавлять ненужные характеристики
повторять весь вопрос покупателя
писать длинные ответы
использовать канцелярит
использовать рекламные штампы
использовать дефисы и тире

ПРОВЕРКА ПЕРЕД ОТВЕТОМ

Перед отправкой проверь:

Какие именно вопросы задал покупатель?

Какие вопросы нужно проигнорировать?

Есть ли хотя бы один вопрос, на который можно ответить?

Есть ли точный ответ в названии или описании?

Не указали ли мы цену?

Не ответили ли мы про наличие?

Не придумали ли неизвестный факт?

Пишем ли мы от лица компании?

Обращаемся ли мы к покупателю на Вы?

Нет ли в ответе дефисов или тире?

Если хотя бы один разрешённый вопрос остался, ответь на него.

SKIP используй только тогда, когда ответить по существу вообще не на что.

ФОРМАТ ВЫВОДА

Выведи только готовую короткую реплику покупателю.

Без кавычек.

Без комментариев.

Без объяснений оператору.

Без слов:

Ответ
Вариант ответа
Покупателю

Если ни на один вопрос ответить нельзя, выведи только:

SKIP
"""


_STYLE_APPENDIX = """
Дополнительно к правилам выше:
к покупателю всегда на Вы; от лица компании мы/наш;
цену и ₽ никогда не указывай; вопросы только про цену → SKIP;
наличие → игнор или SKIP если больше нечего ответить;
на голое приветствие только привет и чем помочь;
в ответе запрещены дефисы и тире (-, –, —).
"""


def ai_reply_enabled(db: Database) -> bool:
    raw = (db.get_setting(SETTING_ENABLED) or "").strip()
    if raw == "":
        return True
    return raw == "1"


def get_prompt(db: Database) -> str:
    raw = (db.get_setting(SETTING_PROMPT) or "").strip()
    return raw or DEFAULT_PROMPT


def chat_item_offer_meta(chat: dict) -> dict[str, str]:
    """Название, цена, описание, url из context чата Avito."""
    out = {"title": "", "price": "", "description": "", "url": "", "item_id": ""}
    if not isinstance(chat, dict):
        return out
    ctx = chat.get("context") if isinstance(chat.get("context"), dict) else {}
    value = ctx.get("value") if isinstance(ctx.get("value"), dict) else {}
    item = chat.get("item") if isinstance(chat.get("item"), dict) else {}
    src = value or item or ctx
    if not isinstance(src, dict):
        src = {}
    out["title"] = str(
        src.get("title") or src.get("name") or chat_item_title(chat) or ""
    ).strip()
    if out["title"] == "—":
        out["title"] = ""
    out["price"] = str(
        src.get("price_string")
        or src.get("priceString")
        or src.get("price")
        or ""
    ).strip()
    desc = (
        src.get("description")
        or src.get("description_html")
        or src.get("descriptionHtml")
        or src.get("body")
        or ""
    )
    out["description"] = str(desc).strip()
    if len(out["description"]) > 3500:
        out["description"] = out["description"][:3497] + "…"
    out["url"] = str(src.get("url") or src.get("item_url") or "").strip()
    iid = src.get("id") or src.get("item_id")
    if iid is not None:
        out["item_id"] = str(iid).strip()
    return out


def _load_json_map(db: Database, key: str) -> dict[str, Any]:
    raw = (db.get_setting(key) or "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_json_map(db: Database, key: str, data: dict[str, Any], limit: int) -> None:
    if len(data) > limit:
        keys = list(data.keys())[-limit:]
        data = {k: data[k] for k in keys}
    db.set_setting(key, json.dumps(data, ensure_ascii=False))


def _new_draft_id() -> str:
    return secrets.token_hex(4)


def draft_keyboard(draft_id: str) -> dict:
    did = (draft_id or "").strip()
    return {
        "inline_keyboard": [
            [
                {"text": "✅ Отправить", "callback_data": f"avito:s:{did}"},
                {"text": "✏️ Изменить", "callback_data": f"avito:e:{did}"},
                {"text": "❌ Отклонить", "callback_data": f"avito:r:{did}"},
            ]
        ]
    }


def save_draft(
    db: Database,
    *,
    draft_id: str,
    store_id: int,
    avito_chat_id: str,
    draft_text: str,
    item_title: str = "",
    buyer_text: str = "",
    buyer_name: str = "",
    store_name: str = "",
    tg_chat_id: Any = None,
    tg_message_id: Any = None,
) -> None:
    data = _load_json_map(db, SETTING_DRAFTS)
    data[draft_id] = {
        "store_id": int(store_id),
        "avito_chat_id": str(avito_chat_id),
        "text": (draft_text or "").strip(),
        "item": (item_title or "")[:120],
        "buyer": (buyer_text or "")[:280],
        "buyer_name": (buyer_name or "Покупатель")[:80],
        "store_name": (store_name or "Avito")[:80],
        "tg_chat_id": str(normalize_telegram_chat_id(tg_chat_id)) if tg_chat_id is not None else "",
        "tg_message_id": int(tg_message_id) if tg_message_id is not None else None,
        "ts": int(time.time()),
    }
    _save_json_map(db, SETTING_DRAFTS, data, _MAX_DRAFTS)


def get_draft(db: Database, draft_id: str) -> Optional[dict[str, Any]]:
    data = _load_json_map(db, SETTING_DRAFTS)
    row = data.get((draft_id or "").strip())
    return row if isinstance(row, dict) else None


def update_draft_text(db: Database, draft_id: str, text: str) -> None:
    data = _load_json_map(db, SETTING_DRAFTS)
    row = data.get(draft_id)
    if not isinstance(row, dict):
        return
    row["text"] = (text or "").strip()
    data[draft_id] = row
    _save_json_map(db, SETTING_DRAFTS, data, _MAX_DRAFTS)


def delete_draft(db: Database, draft_id: str) -> None:
    data = _load_json_map(db, SETTING_DRAFTS)
    data.pop((draft_id or "").strip(), None)
    _save_json_map(db, SETTING_DRAFTS, data, _MAX_DRAFTS)


def _pending_key(tg_chat_id: Any, user_id: Any) -> str:
    return f"{normalize_telegram_chat_id(tg_chat_id)}:{user_id}"


def set_edit_pending(db: Database, *, tg_chat_id: Any, user_id: Any, draft_id: str) -> None:
    data = _load_json_map(db, SETTING_EDIT_PENDING)
    data[_pending_key(tg_chat_id, user_id)] = {
        "draft_id": draft_id,
        "ts": int(time.time()),
    }
    _save_json_map(db, SETTING_EDIT_PENDING, data, _MAX_PENDING)


def pop_edit_pending(db: Database, *, tg_chat_id: Any, user_id: Any) -> Optional[str]:
    data = _load_json_map(db, SETTING_EDIT_PENDING)
    key = _pending_key(tg_chat_id, user_id)
    row = data.pop(key, None)
    _save_json_map(db, SETTING_EDIT_PENDING, data, _MAX_PENDING)
    if isinstance(row, dict):
        did = str(row.get("draft_id") or "").strip()
        return did or None
    return None


def peek_edit_pending(db: Database, *, tg_chat_id: Any, user_id: Any) -> Optional[str]:
    data = _load_json_map(db, SETTING_EDIT_PENDING)
    row = data.get(_pending_key(tg_chat_id, user_id))
    if isinstance(row, dict):
        did = str(row.get("draft_id") or "").strip()
        return did or None
    return None


def _strip_dashes(text: str) -> str:
    """Убрать дефисы/тире из ответа (модель часто всё равно их вставляет)."""
    t = (text or "").strip()
    if not t:
        return t
    for ch in ("—", "–", "−", "‑"):
        t = t.replace(ch, "-")
    # "слово - слово" → запятая
    t = re.sub(r"\s*-\s+", ", ", t)
    # оставшиеся дефисы внутри слов → пробел (тёмно-серый → тёмно серый)
    t = re.sub(r"(?<=\w)-(?=\w)", " ", t)
    t = t.replace("-", " ")
    t = re.sub(r"\s+,", ",", t)
    t = re.sub(r",{2,}", ",", t)
    t = re.sub(r"\s{2,}", " ", t)
    return t.strip(" ,")


async def generate_draft_reply(
    db: Database,
    *,
    buyer_text: str,
    item_meta: dict[str, str],
) -> Optional[str]:
    """None = SKIP / нечего слать."""
    body = (buyer_text or "").strip()
    if not body or body in ("[изображение]", "[голосовое]", "—"):
        return None
    key = (db.get_setting("openai_key") or "").strip()
    if not key:
        log.info("avito ai draft skip: no openai_key")
        return None
    system = (get_prompt(db) or "").rstrip() + "\n" + _STYLE_APPENDIX.strip()
    user = (
        f"Объявление:\n"
        f"Название: {item_meta.get('title') or '—'}\n"
        f"Описание: {item_meta.get('description') or '—'}\n\n"
        f"Сообщение покупателя:\n{body}\n"
    )
    try:
        client = OpenAIClient(key)
        raw = (await client.generate(system, user) or "").strip()
    except HttpStatusError as e:
        log.warning("avito ai draft openai HTTP %s: %s", e.status, str(e.body)[:160])
        return None
    except Exception:
        log.exception("avito ai draft openai failed")
        return None
    if not raw:
        return None
    # Убрать обёртки-кавычки.
    if (raw.startswith("«") and raw.endswith("»")) or (raw.startswith('"') and raw.endswith('"')):
        raw = raw[1:-1].strip()
    low = raw.lower().strip()
    if low == "skip" or low.startswith("skip"):
        return None
    raw = _strip_dashes(raw)
    if not raw:
        return None
    if len(raw) > 1000:
        raw = raw[:997] + "…"
    return raw


def format_unified_card(
    *,
    store_name: str,
    item_title: str,
    buyer_name: str,
    buyer_text: str,
    draft_text: str = "",
    status: str = "",
) -> str:
    """Одно TG-сообщение: входящее + опционально черновик + статус (без спама)."""
    store = escape_tg_html(store_name or "Avito")
    item = escape_tg_html((item_title or "").strip() or "Без названия")
    buyer_s = escape_tg_html(buyer_name or "Покупатель")
    buyer = escape_tg_html((buyer_text or "").strip()[:280] or "…")
    lines = [
        f"<b>Avito</b>  ·  {store}",
        "",
        f"<b>{item}</b>",
        buyer_s,
        "",
        f"<blockquote>{buyer}</blockquote>",
    ]
    draft = (draft_text or "").strip()
    if draft:
        lines.extend(
            [
                "",
                "<b>Ответ</b>",
                f"<blockquote>{escape_tg_html(draft)}</blockquote>",
            ]
        )
    st = (status or "").strip()
    if st:
        lines.extend(["", st])
    elif draft:
        lines.extend(["", "<i>кнопки ниже</i>"])
    else:
        lines.extend(["", "<i>reply текстом или фото → уйдёт в Avito</i>"])
    return "\n".join(lines)


def format_draft_message(
    *,
    store_name: str,
    item_title: str,
    buyer_text: str,
    draft_text: str,
) -> str:
    """Обратная совместимость: карточка только с черновиком."""
    return format_unified_card(
        store_name=store_name,
        item_title=item_title,
        buyer_name="Покупатель",
        buyer_text=buyer_text,
        draft_text=draft_text,
    )


async def maybe_send_ai_draft(
    db: Database,
    *,
    bot_token: str,
    tg_chat_id: str,
    store: Store,
    chat: dict,
    msg: dict,
    avito_chat_id: str,
    alert_tg_message_id: Optional[int] = None,
    our_user_id: Optional[int] = None,
) -> bool:
    """
    Дописать черновик в ТО ЖЕ сообщение алерта (edit), с кнопками.
    Без нового пузыря в чате.
    """
    if not ai_reply_enabled(db):
        return False
    mtype = str(msg.get("type") or "").strip().lower()
    if mtype not in ("text", ""):
        return False
    buyer_text = message_text_preview(msg) or ""
    if not buyer_text or buyer_text.startswith("["):
        return False
    if alert_tg_message_id is None:
        return False
    meta = chat_item_offer_meta(chat)
    draft = await generate_draft_reply(db, buyer_text=buyer_text, item_meta=meta)
    if not draft:
        return False
    from .avito_client import chat_buyer_name

    author = msg.get("author_id")
    if author is None:
        author = msg.get("authorId")
    buyer_name = chat_buyer_name(chat, author_id=author, our_user_id=our_user_id) or "Покупатель"
    item_title = meta.get("title") or chat_item_title(chat)
    draft_id = _new_draft_id()
    text = format_unified_card(
        store_name=store.name or f"#{store.id}",
        item_title=item_title,
        buyer_name=buyer_name,
        buyer_text=buyer_text,
        draft_text=draft,
    )
    ok, err = await telegram_edit_message_text(
        bot_token,
        tg_chat_id,
        int(alert_tg_message_id),
        text,
        parse_mode="HTML",
        reply_markup=draft_keyboard(draft_id),
    )
    if not ok:
        log.warning("avito ai draft edit fail store=%s: %s", store.id, err)
        return False
    save_draft(
        db,
        draft_id=draft_id,
        store_id=int(store.id),
        avito_chat_id=avito_chat_id,
        draft_text=draft,
        item_title=item_title,
        buyer_text=buyer_text,
        buyer_name=buyer_name,
        store_name=store.name or f"#{store.id}",
        tg_chat_id=tg_chat_id,
        tg_message_id=alert_tg_message_id,
    )
    return True


async def _clear_buttons(token: str, chat_id: Any, message_id: Any) -> None:
    if message_id is None:
        return
    try:
        await telegram_edit_message_reply_markup(
            token,
            chat_id,
            int(message_id),
            reply_markup={"inline_keyboard": []},
        )
    except Exception:
        pass


def _row_card(row: dict, *, draft_text: Optional[str] = None, status: str = "") -> str:
    return format_unified_card(
        store_name=str(row.get("store_name") or "Avito"),
        item_title=str(row.get("item") or "—"),
        buyer_name=str(row.get("buyer_name") or "Покупатель"),
        buyer_text=str(row.get("buyer") or "—"),
        draft_text=str(row.get("text") or "") if draft_text is None else draft_text,
        status=status,
    )


async def _edit_row_message(
    bot_token: str,
    row: dict,
    *,
    status: str = "",
    draft_text: Optional[str] = None,
    with_buttons: bool = False,
    draft_id: str = "",
) -> None:
    chat_id = row.get("tg_chat_id")
    mid = row.get("tg_message_id")
    if not chat_id or mid is None:
        return
    body = _row_card(row, draft_text=draft_text, status=status)
    markup: Optional[dict]
    if with_buttons and draft_id:
        markup = draft_keyboard(draft_id)
    else:
        markup = {"inline_keyboard": []}
    ok, err = await telegram_edit_message_text(
        bot_token,
        chat_id,
        int(mid),
        body,
        parse_mode="HTML",
        reply_markup=markup,
    )
    if not ok:
        log.warning("avito unified edit fail: %s", err)


async def _delayed_avito_send(
    db: Database,
    *,
    bot_token: str,
    tg_chat_id: Any,
    draft_id: str,
    delay_sec: int,
) -> None:
    await asyncio.sleep(max(1, int(delay_sec)))
    row = get_draft(db, draft_id)
    if not row:
        return
    text = str(row.get("text") or "").strip()
    store_id = int(row.get("store_id") or 0)
    avito_chat = str(row.get("avito_chat_id") or "").strip()
    from .avito_notify import send_avito_reply_from_telegram

    ok, err = await send_avito_reply_from_telegram(
        db,
        store_id=store_id,
        avito_chat_id=avito_chat,
        text=text,
    )
    if ok:
        await _edit_row_message(
            bot_token,
            row,
            status="✅ <b>отправлено в Avito</b>",
            draft_text=text,
        )
    else:
        await _edit_row_message(
            bot_token,
            row,
            status=f"❌ не удалось отправить: {escape_tg_html(err)}",
            draft_text=text,
            with_buttons=True,
            draft_id=draft_id,
        )
        return
    delete_draft(db, draft_id)


async def handle_draft_callback(
    db: Database,
    *,
    bot_token: str,
    callback: dict,
) -> bool:
    """Обработка avito:s|e|r:id. True если callback наш."""
    data = str(callback.get("data") or "")
    if not data.startswith("avito:"):
        return False
    parts = data.split(":", 2)
    if len(parts) != 3:
        return False
    action, draft_id = parts[1], parts[2]
    cb_id = str(callback.get("id") or "")
    message = callback.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    from_user = callback.get("from") or {}
    user_id = from_user.get("id")
    msg_id = message.get("message_id")

    row = get_draft(db, draft_id)
    if not row:
        await telegram_answer_callback_query(bot_token, cb_id, text="Черновик устарел", show_alert=True)
        await _clear_buttons(bot_token, chat_id, msg_id)
        return True

    # Синхронизируем tg ids из callback (на случай миграции).
    row["tg_chat_id"] = str(normalize_telegram_chat_id(chat_id))
    try:
        row["tg_message_id"] = int(msg_id)
    except (TypeError, ValueError):
        pass
    data_map = _load_json_map(db, SETTING_DRAFTS)
    data_map[draft_id] = row
    _save_json_map(db, SETTING_DRAFTS, data_map, _MAX_DRAFTS)

    if action == "r":
        await telegram_answer_callback_query(bot_token, cb_id, text="Отклонено")
        await _edit_row_message(
            bot_token,
            row,
            status="❌ <i>отклонено, в Avito не ушло</i>",
            draft_text=str(row.get("text") or ""),
        )
        delete_draft(db, draft_id)
        return True

    if action == "e":
        set_edit_pending(db, tg_chat_id=chat_id, user_id=user_id, draft_id=draft_id)
        await telegram_answer_callback_query(bot_token, cb_id, text="Reply на это сообщение")
        await _edit_row_message(
            bot_token,
            row,
            status="✏️ <i>ответьте reply на это сообщение — только так текст попадёт в Avito</i>",
            draft_text=str(row.get("text") or ""),
        )
        return True

    if action == "s":
        delay = random.randint(_SEND_DELAY_MIN_SEC, _SEND_DELAY_MAX_SEC)
        await telegram_answer_callback_query(bot_token, cb_id, text=f"Через ~{delay} с")
        await _edit_row_message(
            bot_token,
            row,
            status=f"⏳ отправлю через ~{delay} сек…",
            draft_text=str(row.get("text") or ""),
        )
        asyncio.create_task(
            _delayed_avito_send(
                db,
                bot_token=bot_token,
                tg_chat_id=chat_id,
                draft_id=draft_id,
                delay_sec=delay,
            )
        )
        return True

    await telegram_answer_callback_query(bot_token, cb_id, text="Неизвестная кнопка")
    return True


async def try_handle_edit_followup(
    db: Database,
    *,
    bot_token: str,
    message: dict,
) -> bool:
    """Принять новый текст только как reply на карточку черновика."""
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    from_user = message.get("from") or {}
    user_id = from_user.get("id")
    if chat_id is None or user_id is None:
        return False
    draft_id = peek_edit_pending(db, tg_chat_id=chat_id, user_id=user_id)
    if not draft_id:
        return False
    text = (message.get("text") or message.get("caption") or "").strip()
    if not text:
        return False

    row = get_draft(db, draft_id)
    if not row:
        pop_edit_pending(db, tg_chat_id=chat_id, user_id=user_id)
        return False

    reply = message.get("reply_to_message") if isinstance(message.get("reply_to_message"), dict) else {}
    try:
        reply_mid = int(reply.get("message_id")) if reply.get("message_id") is not None else None
        expect_mid = int(row.get("tg_message_id")) if row.get("tg_message_id") is not None else None
    except (TypeError, ValueError):
        reply_mid, expect_mid = None, None
    if reply_mid is None or expect_mid is None or reply_mid != expect_mid:
        # Не reply на карточку — не трогаем pending и не перехватываем сообщение.
        return False

    pop_edit_pending(db, tg_chat_id=chat_id, user_id=user_id)
    update_draft_text(db, draft_id, text)
    row = get_draft(db, draft_id) or row
    delay = random.randint(_SEND_DELAY_MIN_SEC, _SEND_DELAY_MAX_SEC)
    await _edit_row_message(
        bot_token,
        row,
        status=f"⏳ принято, отправлю через ~{delay} сек…",
        draft_text=text,
    )
    asyncio.create_task(
        _delayed_avito_send(
            db,
            bot_token=bot_token,
            tg_chat_id=chat_id,
            draft_id=draft_id,
            delay_sec=delay,
        )
    )
    return True
