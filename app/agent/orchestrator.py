"""Оркестратор AI-агента: планирование, подтверждение, вызов инструментов."""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Optional

from app.agent.session import AgentSession, PendingAction, append_message
from app.agent.tools import (
    AgentContext,
    TOOL_BY_NAME,
    format_tool_result,
    run_tool,
    tools_catalog_for_prompt,
)
from app.core.openai_client import OpenAIClient
from app.core.net import HttpStatusError

log = logging.getLogger("agent")

_CONFIRM_WORDS = frozenset({
    "да", "yes", "ok", "ок", "подтверждаю", "подтвердить", "выполни", "выполнить",
    "согласен", "согласна", "go", "confirm",
})
_DENY_WORDS = frozenset({
    "нет", "no", "отмена", "отменить", "cancel", "стоп", "stop",
})

_SYSTEM = """Ты AI-ассистент MarketAI — помогаешь управлять маркетплейсами Wildberries, Ozon и Яндекс Маркет.

Ты можешь только вызывать перечисленные инструменты. Не выдумывай данные — если нужна информация, вызови read-инструмент.

Правила:
1. Если магазин не указан и их несколько — спроси уточнение (type=clarify), не угадывай.
2. Опасные операции (risk=write) — только через tool; система сама запросит подтверждение у пользователя.
3. Перед generate_answers или send_answers сначала используй list_queue_items чтобы получить item_ids.
4. Отвечай на русском, кратко и по делу.
5. Не показывай сырой JSON пользователю — text должен быть человекочитаемым.

Инструменты:
{tools}

Формат ответа — ТОЛЬКО JSON без markdown:
{{
  "type": "message" | "tool" | "clarify",
  "text": "сообщение пользователю",
  "tool": "имя_инструмента или null",
  "args": {{}}
}}
"""


def _parse_agent_json(raw: str) -> Optional[dict[str, Any]]:
    raw = (raw or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except json.JSONDecodeError:
        m = re.search(r"\{[\s\S]*\}", raw)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                pass
    return None


def _is_confirm(text: str) -> bool:
    t = text.strip().lower().rstrip(".!")
    return t in _CONFIRM_WORDS or t.startswith("да,") or t.startswith("да ")


def _is_deny(text: str) -> bool:
    t = text.strip().lower().rstrip(".!")
    return t in _DENY_WORDS


def _build_user_prompt(session: AgentSession, user_message: str) -> str:
    history = ""
    for msg in session.messages[-12:]:
        role = "Пользователь" if msg["role"] == "user" else "Ассистент"
        history += f"{role}: {msg['content']}\n"
    history += f"Пользователь: {user_message}\n\nОтветь JSON:"
    return history


async def handle_agent_message(
    *,
    session: AgentSession,
    user_message: str,
    ctx: AgentContext,
    openai_key: str,
    force_confirm: bool = False,
) -> dict[str, Any]:
    user_message = (user_message or "").strip()
    if not user_message:
        return {"reply": "Напишите, что нужно сделать.", "session": session.session_id}

    append_message(session, "user", user_message)

    if session.pending and not force_confirm:
        if _is_deny(user_message):
            session.pending = None
            reply = "Действие отменено."
            append_message(session, "assistant", reply)
            return {"reply": reply, "session": session.session_id, "pending": None}
        if _is_confirm(user_message):
            pending = session.pending
            session.pending = None
            result = await run_tool(ctx, pending.tool, pending.args)
            formatted = format_tool_result(pending.tool, result)
            reply = f"Готово.\n\n{formatted}"
            append_message(session, "assistant", reply)
            return {"reply": reply, "session": session.session_id, "tool_used": pending.tool}

    if not openai_key.strip():
        reply = "Не задан ключ OpenAI в настройках. Добавьте его в разделе «Настройки → OpenAI»."
        append_message(session, "assistant", reply)
        return {"reply": reply, "session": session.session_id}

    system = _SYSTEM.format(tools=tools_catalog_for_prompt())
    client = OpenAIClient(openai_key)
    try:
        raw = await client.generate(system, _build_user_prompt(session, user_message))
    except HttpStatusError as e:
        reply = f"OpenAI недоступен: {e.body or e.status}"
        append_message(session, "assistant", reply)
        return {"reply": reply, "session": session.session_id}

    plan = _parse_agent_json(raw)
    if not plan:
        reply = raw[:2000] if raw else "Не удалось разобрать ответ AI. Попробуйте переформулировать."
        append_message(session, "assistant", reply)
        return {"reply": reply, "session": session.session_id}

    ptype = str(plan.get("type") or "message").lower()
    text = str(plan.get("text") or "").strip()
    tool_name = plan.get("tool")
    args = plan.get("args") if isinstance(plan.get("args"), dict) else {}

    if ptype in ("message", "clarify") or not tool_name:
        reply = text or "Чем могу помочь?"
        append_message(session, "assistant", reply)
        return {"reply": reply, "session": session.session_id}

    spec = TOOL_BY_NAME.get(str(tool_name))
    if not spec:
        reply = text or f"Неизвестный инструмент: {tool_name}"
        append_message(session, "assistant", reply)
        return {"reply": reply, "session": session.session_id}

    if spec.risk == "write" and not force_confirm:
        summary = text or f"Выполнить «{spec.name}»?"
        session.pending = PendingAction(tool=spec.name, args=args, summary=summary)
        reply = f"{summary}\n\nПодтвердите действие: напишите «да» или «отмена»."
        append_message(session, "assistant", reply)
        return {
            "reply": reply,
            "session": session.session_id,
            "pending": {"tool": spec.name, "summary": summary},
            "needs_confirm": True,
        }

    result = await run_tool(ctx, spec.name, args)
    formatted = format_tool_result(spec.name, result)
    reply = f"{text}\n\n{formatted}".strip() if text else formatted
    append_message(session, "assistant", reply)
    return {"reply": reply, "session": session.session_id, "tool_used": spec.name}
