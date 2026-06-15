"""Оркестратор AI-агента: планирование, подтверждение, вызов инструментов."""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Optional

from app.agent.formatting import compose_reply, strip_leaked_json
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

_MAX_READ_TOOL_ROUNDS = 5

_CONFIRM_WORDS = frozenset({
    "да", "yes", "ok", "ок", "подтверждаю", "подтвердить", "выполни", "выполнить",
    "согласен", "согласна", "go", "confirm",
})
_DENY_WORDS = frozenset({
    "нет", "no", "отмена", "отменить", "cancel", "стоп", "stop",
})

_SYSTEM = """Ты оператор MarketAI — помощник по управлению маркетплейсами Wildberries, Ozon и Яндекс.Маркет.

Ты НЕ ChatGPT. Ты управляешь MarketAI только через инструменты.

## Сценарии (цепочки — один инструмент, НЕ разбивай на шаги вручную)
- «ответить на отзывы», «обработать отзывы» → pipeline_answer_reviews
- «ответить на вопросы» → pipeline_answer_questions
- «проверь автоакции», «акции озон» → check_ozon_promotions
- «удали из акций», «сними товары» (после проверки) → remove_ozon_promotions (use_last_check=true)
- «как успехи?» → get_task_status

pipeline_* сам: загрузка → генерация → отправка. Одно подтверждение на весь цикл.

## Правила
1. Факты — только через инструменты.
2. write — один вызов; система запросит подтверждение.
3. Точечно: load_new_items, generate_answers, send_answers — по отдельности.
4. export_dialog — история диалога.
5. send_telegram_broadcast — Telegram-чаты MarketAI.
6. После check_ozon_promotions для удаления — remove_ozon_promotions.
7. Русский, кратко, без JSON и жаргона.

Инструменты:
{tools}

Формат — ТОЛЬКО один JSON:
{{
  "type": "message" | "tool" | "clarify",
  "text": "текст",
  "tool": "имя или null",
  "args": {{}}
}}
"""

_SYNTHESIS_SUFFIX = """

[Система: инструмент выполнен. Результат ниже.]
{tool_result}

Сформулируй понятный ответ пользователю на русском. type=message, tool=null. Не вызывай инструмент снова."""


def _parse_agent_json(raw: str) -> Optional[dict[str, Any]]:
    raw = (raw or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    # Несколько JSON подряд — берём первый валидный
    candidates = [raw]
    candidates.extend(m.group(0) for m in re.finditer(r"\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}", raw))
    seen: set[str] = set()
    for cand in candidates:
        cand = cand.strip()
        if not cand or cand in seen:
            continue
        seen.add(cand)
        try:
            obj = json.loads(cand)
            if isinstance(obj, dict) and ("type" in obj or "tool" in obj or "text" in obj):
                return obj
        except json.JSONDecodeError:
            continue
    return None


def _is_confirm(text: str) -> bool:
    t = text.strip().lower().rstrip(".!")
    return t in _CONFIRM_WORDS or t.startswith("да,") or t.startswith("да ")


def _is_deny(text: str) -> bool:
    t = text.strip().lower().rstrip(".!")
    return t in _DENY_WORDS


def _build_history_prompt(session: AgentSession, extra: str = "") -> str:
    history = ""
    for msg in session.messages[-18:]:
        role = "Пользователь" if msg["role"] == "user" else "Ассистент"
        content = strip_leaked_json(msg.get("content") or "")
        if content:
            history += f"{role}: {content}\n"
    if extra:
        history += extra.rstrip() + "\n"
    history += "\nОтветь JSON:"
    return history


def _remember_task_id(session: AgentSession, result: Any) -> None:
    if isinstance(result, dict) and result.get("task_id"):
        session.last_task_id = str(result["task_id"])


async def _call_llm(client: OpenAIClient, system: str, prompt: str) -> str:
    return await client.generate(system, prompt)


async def _synthesize_reply(
    client: OpenAIClient,
    system: str,
    session: AgentSession,
    tool_name: str,
    tool_result: str,
) -> str:
    extra = _SYNTHESIS_SUFFIX.format(tool_result=tool_result[:3500])
    raw = await _call_llm(client, system, _build_history_prompt(session, extra))
    plan = _parse_agent_json(raw)
    if plan:
        text = strip_leaked_json(str(plan.get("text") or ""))
        if text:
            return text
    return strip_leaked_json(raw) or tool_result


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

    ctx.session = session
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
            _remember_task_id(session, result)
            formatted = format_tool_result(pending.tool, result)
            reply = compose_reply("", formatted, done_label="✅ Готово")
            append_message(session, "assistant", reply)
            return {"reply": reply, "session": session.session_id, "tool_used": pending.tool}

    if not openai_key.strip():
        reply = "Не задан ключ OpenAI в настройках. Добавьте его в разделе «Настройки → OpenAI»."
        append_message(session, "assistant", reply)
        return {"reply": reply, "session": session.session_id}

    system = _SYSTEM.format(tools=tools_catalog_for_prompt())
    client = OpenAIClient(openai_key)

    read_rounds = 0
    tool_notes = ""

    while read_rounds <= _MAX_READ_TOOL_ROUNDS:
        try:
            raw = await _call_llm(client, system, _build_history_prompt(session, tool_notes))
        except HttpStatusError as e:
            reply = f"OpenAI недоступен: {e.body or e.status}"
            append_message(session, "assistant", reply)
            return {"reply": reply, "session": session.session_id}

        plan = _parse_agent_json(raw)
        if not plan:
            reply = strip_leaked_json(raw[:2000]) if raw else "Не удалось разобрать ответ. Переформулируйте запрос."
            append_message(session, "assistant", reply)
            return {"reply": reply, "session": session.session_id}

        ptype = str(plan.get("type") or "message").lower()
        text = strip_leaked_json(str(plan.get("text") or ""))
        tool_name = plan.get("tool")
        args = plan.get("args") if isinstance(plan.get("args"), dict) else {}

        if ptype in ("message", "clarify") or not tool_name:
            if tool_notes and not text:
                try:
                    text = await _synthesize_reply(client, system, session, "combined", tool_notes)
                except HttpStatusError:
                    text = tool_notes.strip()
            reply = text or "Чем помочь? Могу показать магазины, статистику, очередь, запустить загрузку или автозапуск."
            append_message(session, "assistant", reply)
            return {"reply": reply, "session": session.session_id}

        spec = TOOL_BY_NAME.get(str(tool_name))
        if not spec:
            reply = text or f"Неизвестная команда: {tool_name}. Напишите /help или «покажи магазины»."
            append_message(session, "assistant", reply)
            return {"reply": reply, "session": session.session_id}

        if spec.risk == "write" and not force_confirm:
            default_summaries = {
                "pipeline_answer_reviews": "Запустить полный цикл по отзывам: загрузка → генерация ответов → отправка на маркетплейсы.",
                "pipeline_answer_questions": "Запустить полный цикл по вопросам: загрузка → генерация → отправка.",
                "remove_ozon_promotions": "Удалить товары из акций Ozon.",
                "load_new_items": "Загрузить новые отзывы и вопросы с маркетплейсов.",
                "send_telegram_broadcast": "Разослать сообщение во все настроенные Telegram-чаты.",
            }
            summary = text or default_summaries.get(spec.name) or f"Выполнить «{spec.name}»?"
            session.pending = PendingAction(tool=spec.name, args=args, summary=summary)
            reply = f"{summary}\n\nПодтвердите: «да» или «отмена»."
            append_message(session, "assistant", reply)
            return {
                "reply": reply,
                "session": session.session_id,
                "pending": {"tool": spec.name, "summary": summary},
                "needs_confirm": True,
            }

        result = await run_tool(ctx, spec.name, args)
        _remember_task_id(session, result)
        formatted = format_tool_result(spec.name, result)

        if spec.risk == "write":
            reply = compose_reply(text, formatted, done_label="✅ Готово")
            append_message(session, "assistant", reply)
            return {"reply": reply, "session": session.session_id, "tool_used": spec.name}

        read_rounds += 1
        tool_notes += f"\n[Результат {spec.name}]:\n{formatted}\n"
        if read_rounds > _MAX_READ_TOOL_ROUNDS:
            try:
                reply = await _synthesize_reply(client, system, session, spec.name, tool_notes)
            except HttpStatusError:
                reply = tool_notes.strip()
            append_message(session, "assistant", reply)
            return {"reply": reply, "session": session.session_id, "tool_used": spec.name}

    reply = "Не удалось обработать запрос. Попробуйте проще, например: «статистика»."
    append_message(session, "assistant", reply)
    return {"reply": reply, "session": session.session_id}
