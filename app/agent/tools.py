"""Инструменты агента — обёртки над существующими задачами MarketAI."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional, TYPE_CHECKING

from app.agent.playbooks import (
    check_ozon_promotions,
    pipeline_answer_items,
    remove_ozon_promotions,
)
from app.core.quality_metrics import fetch_all_quality
from app.core.telegram_notify import (
    normalize_telegram_bot_token,
    normalize_telegram_chat_id,
    send_telegram_message,
)
from app.db import Database
from app.web import tasks as web_tasks
from app.web.store_locks import StoreBusyError

if TYPE_CHECKING:
    from app.agent.session import AgentSession

log = logging.getLogger("agent.tools")

ToolFn = Callable[["AgentContext", dict[str, Any]], Any]


@dataclass
class ToolSpec:
    name: str
    description: str
    risk: str  # "read" | "write"
    parameters: dict[str, str]
    execute: ToolFn


@dataclass
class AgentContext:
    db: Database
    username: str
    user_id: int
    get_auto_status: Callable[[], dict[str, Any]]
    run_auto_now: Optional[Callable[[], Any]] = None
    stop_auto: Optional[Callable[[], Any]] = None
    session: Optional["AgentSession"] = None
    send_telegram_report: Optional[Callable[[], Any]] = None


def _resolve_store_id(db: Database, *, store_id: Optional[int], store_name: Optional[str]) -> tuple[Optional[int], str]:
    stores = [s for s in db.list_stores() if s.active]
    if store_id is not None:
        for s in stores:
            if int(s.id) == int(store_id):
                return int(s.id), s.name
        return None, f"Магазин id={store_id} не найден или неактивен."
    if store_name:
        q = store_name.strip().lower()
        matches = [s for s in stores if q in (s.name or "").lower()]
        if len(matches) == 1:
            return int(matches[0].id), matches[0].name
        if len(matches) > 1:
            names = ", ".join(f"{s.name} (id={s.id})" for s in matches[:8])
            return None, f"Несколько магазинов подходят под «{store_name}»: {names}. Уточните id или точное имя."
        return None, f"Магазин «{store_name}» не найден."
    return None, "Укажите store_id или store_name."


def _tool_list_stores(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    rows = []
    for s in ctx.db.list_stores():
        rows.append({
            "id": s.id,
            "name": s.name,
            "marketplace": s.marketplace,
            "active": bool(s.active),
        })
    return {"stores": rows, "count": len(rows)}


def _tool_get_stats(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    return ctx.db.get_stats()


async def _tool_list_queue(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    item_type = str(args.get("item_type") or "review").strip().lower()
    if item_type not in ("review", "question"):
        return {"error": "item_type должен быть review или question"}
    status = args.get("status")
    statuses = [status] if status else None
    has_answer = args.get("has_answer")
    if has_answer is not None:
        has_answer = bool(has_answer)
    limit = min(int(args.get("limit") or 20), 50)
    sid, err = _resolve_store_id(
        ctx.db,
        store_id=args.get("store_id"),
        store_name=args.get("store_name"),
    )
    if sid is None and (args.get("store_id") is not None or args.get("store_name")):
        return {"error": err}
    items = ctx.db.list_items_filtered(
        item_type=item_type,
        store_id=sid,
        statuses=statuses,
        has_answer=has_answer,
        limit=limit,
    )
    store_names = {s.id: s.name for s in ctx.db.list_stores()}
    return {
        "item_type": item_type,
        "store_id": sid,
        "items": [
            {
                "id": r.id,
                "store_id": r.store_id,
                "store_name": store_names.get(r.store_id, ""),
                "status": r.status,
                "rating": r.rating,
                "product_title": (r.product_title or "")[:80],
                "text_preview": (r.text or "")[:120],
                "has_answer": bool((r.generated_text or "").strip()),
            }
            for r in items
        ],
        "count": len(items),
    }


def _tool_auto_status(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    return ctx.get_auto_status()


async def _tool_quality_summary(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    stores = ctx.db.list_stores()
    data = await fetch_all_quality(stores, use_cache=True)
    wb = []
    for row in data.get("wb") or []:
        m = (row.get("metrics") or [{}])[0] if row.get("metrics") else {}
        wb.append({
            "store_id": row.get("store_id"),
            "store_name": row.get("store_name"),
            "ok": row.get("ok"),
            "rating": m.get("value"),
            "error": row.get("error") or "",
        })
    ozon = []
    for row in data.get("ozon") or []:
        metrics = {m.get("key"): m.get("value") for m in (row.get("metrics") or [])}
        ozon.append({
            "store_id": row.get("store_id"),
            "store_name": row.get("store_name"),
            "ok": row.get("ok"),
            "metrics": metrics,
            "error": row.get("error") or "",
        })
    return {"wb": wb, "ozon": ozon}


async def _tool_load_new(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    store_ids = args.get("store_ids")
    if store_ids is not None:
        store_ids = [int(x) for x in store_ids]
    try:
        task_id = await web_tasks.run_load_new(ctx.db, store_ids)
    except StoreBusyError as e:
        return {"error": str(e)}
    return {"task_id": task_id, "message": "Загрузка новых отзывов/вопросов запущена."}


async def _tool_generate(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    item_ids = [int(x) for x in (args.get("item_ids") or [])]
    if not item_ids:
        return {"error": "Нужен список item_ids"}
    key = (ctx.db.get_setting("openai_key") or "").strip()
    if not key:
        return {"error": "Не задан OpenAI ключ в настройках"}
    try:
        task_id = await web_tasks.run_generate(ctx.db, item_ids, key)
    except StoreBusyError as e:
        return {"error": str(e)}
    return {"task_id": task_id, "item_count": len(item_ids)}


async def _tool_send(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    item_ids = [int(x) for x in (args.get("item_ids") or [])]
    if not item_ids:
        return {"error": "Нужен список item_ids"}
    try:
        task_id = await web_tasks.run_send(ctx.db, item_ids)
    except StoreBusyError as e:
        return {"error": str(e)}
    return {"task_id": task_id, "item_count": len(item_ids)}


async def _tool_run_auto(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    if not ctx.run_auto_now:
        return {"error": "Запуск автозапуска недоступен"}
    try:
        return await ctx.run_auto_now()
    except Exception as e:
        return {"error": str(e)}


async def _tool_stop_auto(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    if not ctx.stop_auto:
        return {"error": "Остановка автозапуска недоступна"}
    try:
        return await ctx.stop_auto()
    except Exception as e:
        return {"error": str(e)}


def _configured_telegram_chat_ids(db: Database) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for key in (
        "telegram_chat_id",
        "telegram_report_chat_id",
        "telegram_card_error_chat_id",
        "ozon_alerts_telegram_chat_id",
        "telegram_agent_chat_id",
    ):
        raw = (db.get_setting(key) or "").strip()
        if not raw:
            continue
        cid = str(normalize_telegram_chat_id(raw))
        if cid and cid not in seen:
            seen.add(cid)
            out.append(cid)
    return out


async def _tool_telegram_broadcast(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    text = str(args.get("text") or "").strip()
    if not text:
        return {"error": "Нужен текст сообщения (text)"}
    token = normalize_telegram_bot_token(ctx.db.get_setting("telegram_bot_token") or "")
    if not token:
        return {"error": "Не задан токен Telegram-бота"}
    chat_ids = _configured_telegram_chat_ids(ctx.db)
    if not chat_ids:
        return {"error": "Не настроены Telegram chat_id"}
    sent = 0
    errors: list[str] = []
    for cid in chat_ids:
        ok, err = await send_telegram_message(token, cid, text, db=ctx.db)
        if ok:
            sent += 1
        else:
            errors.append(f"{cid}: {err[:80]}")
    return {
        "message": f"Отправлено в {sent} из {len(chat_ids)} чатов.",
        "sent": sent,
        "total": len(chat_ids),
        "errors": errors[:5],
    }


async def _tool_telegram_report(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    if not ctx.send_telegram_report:
        return {"error": "Отправка отчёта недоступна"}
    try:
        result = await ctx.send_telegram_report()
        if isinstance(result, dict) and result.get("error"):
            return {"error": str(result["error"])}
        return {"message": "Периодический отчёт отправлен в Telegram."}
    except Exception as e:
        return {"error": str(e)}


async def _tool_get_task(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    task_id = str(args.get("task_id") or "").strip()
    if not task_id and ctx.session and ctx.session.last_task_id:
        task_id = ctx.session.last_task_id
    if not task_id:
        return {"error": "Укажите task_id или сначала запустите задачу (загрузка/генерация/отправка)"}
    state = await web_tasks.get_task(task_id)
    if not state:
        return {"error": f"Задача {task_id} не найдена (возможно, уже завершилась и удалена из памяти)"}
    return {"task_id": task_id, **state}


async def _tool_list_tasks(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    status = args.get("status")
    limit = min(int(args.get("limit") or 10), 20)
    rows = await web_tasks.list_tasks(status=str(status) if status else None, limit=limit)
    return {"tasks": rows, "count": len(rows)}


def _tool_export_dialog(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    if not ctx.session:
        return {"error": "Сессия недоступна"}
    lines = []
    for msg in ctx.session.messages:
        role = "Вы" if msg.get("role") == "user" else "Ассистент"
        content = (msg.get("content") or "").strip()
        if content:
            lines.append(f"{role}: {content}")
    return {"lines": lines, "count": len(lines)}


def _tool_list_operations(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    limit = min(int(args.get("limit") or 15), 40)
    action = args.get("action")
    rows = ctx.db.list_audit_events(action=str(action) if action else None, limit=limit)
    store_names = {s.id: s.name for s in ctx.db.list_stores()}
    items = []
    for r in rows:
        store = store_names.get(r.store_id, "") if r.store_id else ""
        items.append({
            "ts": r.ts,
            "actor": r.actor,
            "action": r.action,
            "store": store,
            "result": r.result,
        })
    return {"operations": items, "count": len(items)}


def _tool_list_card_errors(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    limit = min(int(args.get("limit") or 10), 30)
    status = args.get("status") or "new"
    sid, err = _resolve_store_id(
        ctx.db,
        store_id=args.get("store_id"),
        store_name=args.get("store_name"),
    )
    if sid is None and (args.get("store_id") is not None or args.get("store_name")):
        return {"error": err}
    rows = ctx.db.list_card_error_alerts(store_id=sid, status=str(status) if status else None, limit=limit)
    store_names = {s.id: s.name for s in ctx.db.list_stores()}
    return {
        "alerts": [
            {
                "id": r.id,
                "store": store_names.get(r.store_id, ""),
                "product": (r.product_title or "")[:60],
                "kind": r.error_kind,
                "status": r.status,
                "ts": r.ts,
            }
            for r in rows
        ],
        "count": len(rows),
    }


def _tool_list_ozon_alerts(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    limit = min(int(args.get("limit") or 10), 30)
    sid, err = _resolve_store_id(
        ctx.db,
        store_id=args.get("store_id"),
        store_name=args.get("store_name"),
    )
    if sid is None and (args.get("store_id") is not None or args.get("store_name")):
        return {"error": err}
    status = str(args.get("status") or "new").strip() or "new"
    rows = ctx.db.list_ozon_important_alerts(store_id=sid, status=status, limit=limit)
    store_names = {s.id: s.name for s in ctx.db.list_stores()}
    return {
        "alerts": [
            {
                "id": r.id,
                "store": store_names.get(r.store_id, ""),
                "summary": (r.summary or "")[:100],
                "threat": r.threat_type,
                "status": r.status,
                "ts": r.ts,
            }
            for r in rows
        ],
        "count": len(rows),
    }


def _tool_apply_template(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    item_ids = [int(x) for x in (args.get("item_ids") or [])]
    text = str(args.get("template_text") or "").strip()
    if not item_ids:
        return {"error": "Нужен список item_ids"}
    if not text:
        return {"error": "Нужен template_text"}
    applied = 0
    skipped = 0
    for item_id in item_ids:
        row = ctx.db.get_item_by_id(int(item_id))
        if not row or row.item_type != "review" or row.status != "new" or (row.generated_text or "").strip():
            skipped += 1
            continue
        ctx.db.set_generated(int(item_id), text)
        applied += 1
    return {"message": f"Шаблон применён к {applied} отзывам, пропущено {skipped}.", "applied": applied, "skipped": skipped}


async def _tool_pipeline_reviews(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    store_ids = args.get("store_ids")
    if store_ids is not None:
        store_ids = [int(x) for x in store_ids]
    key = (ctx.db.get_setting("openai_key") or "").strip()
    if not key:
        return {"error": "Не задан OpenAI ключ"}
    return await pipeline_answer_items(ctx.db, item_type="review", store_ids=store_ids, openai_key=key)


async def _tool_pipeline_questions(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    store_ids = args.get("store_ids")
    if store_ids is not None:
        store_ids = [int(x) for x in store_ids]
    key = (ctx.db.get_setting("openai_key") or "").strip()
    if not key:
        return {"error": "Не задан OpenAI ключ"}
    return await pipeline_answer_items(ctx.db, item_type="question", store_ids=store_ids, openai_key=key)


async def _tool_check_ozon_promotions(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    sid, err = _resolve_store_id(
        ctx.db,
        store_id=args.get("store_id"),
        store_name=args.get("store_name"),
    )
    if sid is None and (args.get("store_id") is not None or args.get("store_name")):
        return {"error": err}
    only_auto = args.get("only_auto_add")
    if only_auto is not None:
        only_auto = bool(only_auto)
    session_ctx = ctx.session.context if ctx.session else None
    return await check_ozon_promotions(
        ctx.db,
        store_id=sid,
        only_auto_add=only_auto,
        session_context=session_ctx,
    )


async def _tool_remove_ozon_promotions(ctx: AgentContext, args: dict[str, Any]) -> dict[str, Any]:
    sid, err = _resolve_store_id(
        ctx.db,
        store_id=args.get("store_id"),
        store_name=args.get("store_name"),
    )
    if sid is None and (args.get("store_id") is not None or args.get("store_name")):
        return {"error": err}
    action_ids = args.get("action_ids")
    if action_ids is not None:
        action_ids = [int(x) for x in action_ids]
    session_ctx = ctx.session.context if ctx.session else None
    return await remove_ozon_promotions(
        ctx.db,
        store_id=sid,
        action_ids=action_ids,
        only_auto_add=bool(args.get("only_auto_add", True)),
        session_context=session_ctx,
        use_last_check=bool(args.get("use_last_check", False)),
    )


async def _execute_tool(ctx: AgentContext, name: str, args: dict[str, Any]) -> Any:
    spec = TOOL_BY_NAME.get(name)
    if not spec:
        return {"error": f"Неизвестный инструмент: {name}"}
    import asyncio
    result = spec.execute(ctx, args)
    if asyncio.iscoroutine(result):
        result = await result
    try:
        ctx.db.add_audit_event(
            actor=f"agent:{ctx.username}",
            action=f"agent_tool_{name}",
            item_type="agent",
            result="ok" if not (isinstance(result, dict) and result.get("error")) else "error",
            meta={"args": args, "result_preview": str(result)[:500]},
        )
    except Exception:
        pass
    return result


TOOL_SPECS: list[ToolSpec] = [
    ToolSpec(
        name="list_stores",
        description="Список всех магазинов (id, название, маркетплейс, активен).",
        risk="read",
        parameters={"active_only": "bool, optional"},
        execute=_tool_list_stores,
    ),
    ToolSpec(
        name="get_stats",
        description="Операционная сводка: очередь отзывов/вопросов, отправлено сегодня, магазины.",
        risk="read",
        parameters={},
        execute=_tool_get_stats,
    ),
    ToolSpec(
        name="list_queue_items",
        description="Список отзывов или вопросов в очереди.",
        risk="read",
        parameters={
            "item_type": "review | question (обязательно)",
            "store_id": "int, optional",
            "store_name": "str, optional — часть названия",
            "status": "new | generated | sent, optional",
            "has_answer": "bool — есть ли сгенерированный текст",
            "limit": "int, max 50, default 20",
        },
        execute=_tool_list_queue,
    ),
    ToolSpec(
        name="get_auto_schedule_status",
        description="Статус автозапуска: выполняется ли сейчас, этап, следующий слот, подсказки.",
        risk="read",
        parameters={},
        execute=_tool_auto_status,
    ),
    ToolSpec(
        name="get_quality_summary",
        description="Показатели качества WB (рейтинг) и Ozon (отмены, просрочки, индекс) по магазинам.",
        risk="read",
        parameters={},
        execute=_tool_quality_summary,
    ),
    ToolSpec(
        name="load_new_items",
        description="Загрузить новые отзывы и вопросы с маркетплейсов. Опасная операция.",
        risk="write",
        parameters={"store_ids": "list[int] или null = все активные магазины"},
        execute=_tool_load_new,
    ),
    ToolSpec(
        name="generate_answers",
        description="Сгенерировать AI-ответы для указанных item_ids. Опасная операция (OpenAI).",
        risk="write",
        parameters={"item_ids": "list[int] — id из list_queue_items"},
        execute=_tool_generate,
    ),
    ToolSpec(
        name="send_answers",
        description="Отправить ответы на маркетплейс для указанных item_ids. Опасная операция.",
        risk="write",
        parameters={"item_ids": "list[int]"},
        execute=_tool_send,
    ),
    ToolSpec(
        name="run_auto_schedule_now",
        description="Запустить цикл автозапуска сейчас (без ожидания расписания). Опасная операция.",
        risk="write",
        parameters={},
        execute=_tool_run_auto,
    ),
    ToolSpec(
        name="stop_auto_schedule",
        description="Остановить текущий автозапуск и выключить его в настройках. Опасная операция.",
        risk="write",
        parameters={},
        execute=_tool_stop_auto,
    ),
    ToolSpec(
        name="get_task_status",
        description="Статус фоновой задачи (загрузка/генерация/отправка). Без task_id — последняя задача в диалоге.",
        risk="read",
        parameters={"task_id": "str, optional"},
        execute=_tool_get_task,
    ),
    ToolSpec(
        name="list_active_tasks",
        description="Список недавних фоновых задач MarketAI.",
        risk="read",
        parameters={"status": "running | done | error | cancelled, optional", "limit": "int, default 10"},
        execute=_tool_list_tasks,
    ),
    ToolSpec(
        name="export_dialog",
        description="Экспорт истории текущего диалога с ассистентом (текстом).",
        risk="read",
        parameters={},
        execute=_tool_export_dialog,
    ),
    ToolSpec(
        name="list_recent_operations",
        description="Последние операции из журнала MarketAI (загрузки, отправки, автозапуск).",
        risk="read",
        parameters={"action": "load_new | generate | send | auto_run, optional", "limit": "int, default 15"},
        execute=_tool_list_operations,
    ),
    ToolSpec(
        name="list_card_errors",
        description="Ошибки в карточках товаров (подозрения из отзывов/вопросов/чатов).",
        risk="read",
        parameters={"store_id": "int, optional", "store_name": "str, optional", "status": "new | resolved, default new", "limit": "int"},
        execute=_tool_list_card_errors,
    ),
    ToolSpec(
        name="list_ozon_alerts",
        description="Важные уведомления Ozon (штрафы, документы, снятие с продажи).",
        risk="read",
        parameters={"store_id": "int, optional", "store_name": "str, optional", "status": "new, optional", "limit": "int"},
        execute=_tool_list_ozon_alerts,
    ),
    ToolSpec(
        name="apply_template",
        description="Применить текстовый шаблон к отзывам без ответа (без OpenAI). Опасная операция.",
        risk="write",
        parameters={"item_ids": "list[int]", "template_text": "str"},
        execute=_tool_apply_template,
    ),
    ToolSpec(
        name="send_telegram_broadcast",
        description="Отправить текст во все настроенные Telegram-чаты MarketAI (основной, отчёты, карточки, Ozon). Опасная операция.",
        risk="write",
        parameters={"text": "str — текст сообщения"},
        execute=_tool_telegram_broadcast,
    ),
    ToolSpec(
        name="send_telegram_report_now",
        description="Отправить сводный периодический отчёт MarketAI в Telegram сейчас. Опасная операция.",
        risk="write",
        parameters={},
        execute=_tool_telegram_report,
    ),
    ToolSpec(
        name="pipeline_answer_reviews",
        description=(
            "Полный цикл по отзывам: загрузить с маркетплейсов → сгенерировать ответы ИИ → отправить. "
            "Используй при «ответить на отзывы», «обработать отзывы», «отзывы в работу»."
        ),
        risk="write",
        parameters={"store_ids": "list[int] или null = все магазины"},
        execute=_tool_pipeline_reviews,
    ),
    ToolSpec(
        name="pipeline_answer_questions",
        description=(
            "Полный цикл по вопросам: загрузить → сгенерировать → отправить. "
            "Используй при «ответить на вопросы», «обработать вопросы»."
        ),
        risk="write",
        parameters={"store_ids": "list[int] или null = все магазины"},
        execute=_tool_pipeline_questions,
    ),
    ToolSpec(
        name="check_ozon_promotions",
        description=(
            "Проверить акции Ozon по магазинам: название, id, сколько товаров участвует, автоакция ли. "
            "Используй при «проверь автоакции», «какие акции на Ozon», «сколько товаров в акциях»."
        ),
        risk="read",
        parameters={
            "store_id": "int, optional",
            "store_name": "str, optional",
            "only_auto_add": "bool — только автоакции, optional",
        },
        execute=_tool_check_ozon_promotions,
    ),
    ToolSpec(
        name="remove_ozon_promotions",
        description=(
            "Удалить товары из акций Ozon. После check_ozon_promotions можно use_last_check=true. "
            "Или укажи action_ids. Используй при «удали товары из акций», «сними с акций»."
        ),
        risk="write",
        parameters={
            "store_id": "int, optional",
            "store_name": "str, optional",
            "action_ids": "list[int], optional",
            "only_auto_add": "bool, default true — если action_ids не указаны",
            "use_last_check": "bool — удалить из акций последней проверки",
        },
        execute=_tool_remove_ozon_promotions,
    ),
]

TOOL_BY_NAME: dict[str, ToolSpec] = {t.name: t for t in TOOL_SPECS}


def tools_catalog_for_prompt() -> str:
    lines = []
    for t in TOOL_SPECS:
        params = ", ".join(f"{k}: {v}" for k, v in t.parameters.items()) or "нет"
        lines.append(f"- {t.name} [{t.risk}]: {t.description} Параметры: {params}")
    return "\n".join(lines)


async def run_tool(ctx: AgentContext, name: str, args: dict[str, Any]) -> Any:
    return await _execute_tool(ctx, name, args)


def format_tool_result(name: str, result: Any) -> str:
    if not isinstance(result, dict):
        return str(result)[:2000]
    if result.get("error"):
        return f"❌ {result['error']}"
    if name == "list_stores":
        stores = result.get("stores") or []
        if not stores:
            return "Магазинов нет."
        lines = [f"• {s['name']} — {s['marketplace'].upper()}, id={s['id']}, {'✅ активен' if s['active'] else '⏸ выкл'}" for s in stores]
        return f"📦 Магазины ({len(stores)}):\n" + "\n".join(lines)
    if name == "get_stats":
        q = result.get("queue") or {}
        stores = result.get("stores") or {}
        return (
            "📊 Сводка:\n"
            f"• Отправлено сегодня: {result.get('sent_today', 0)}\n"
            f"• Новые отзывы: {q.get('new_reviews', 0)} | вопросы: {q.get('new_questions', 0)}\n"
            f"• Сгенерировано: отзывы {q.get('generated_reviews', 0)} | вопросы {q.get('generated_questions', 0)}\n"
            f"• Магазины: {stores.get('active', 0)} активных из {stores.get('total', 0)}"
        )
    if name == "list_queue_items":
        items = result.get("items") or []
        if not items:
            return "Очередь пуста по заданным фильтрам."
        lines = []
        for it in items:
            ans = "✅ ответ есть" if it.get("has_answer") else "⏳ без ответа"
            rating = f" ⭐{it['rating']}" if it.get("rating") else ""
            lines.append(
                f"• #{it['id']} [{it['status']}]{rating} {it.get('store_name', '')}\n"
                f"  {(it.get('product_title') or '')[:55]} — {ans}"
            )
        return f"📋 Найдено {len(items)}:\n" + "\n".join(lines)
    if name == "get_auto_schedule_status":
        running = "🔄 выполняется" if result.get("running") else "⏸ ожидание"
        enabled = "включён" if result.get("enabled") else "выключен"
        hint = result.get("schedule_hint") or ""
        body = (
            f"⏰ Автозапуск ({enabled}):\n"
            f"• Статус: {running}\n"
            f"• Этап: {result.get('phase', '—')}\n"
            f"• Следующий слот: {result.get('next_slot') or '—'}"
        )
        if hint:
            body += f"\n• {hint}"
        return body
    if name == "get_quality_summary":
        lines = ["📈 Качество:"]
        for row in result.get("wb") or []:
            if row.get("ok") and row.get("rating") is not None:
                lines.append(f"• WB {row['store_name']}: {row['rating']} ★")
            elif row.get("error"):
                lines.append(f"• WB {row['store_name']}: ⚠️ {row['error'][:60]}")
        for row in result.get("ozon") or []:
            m = row.get("metrics") or {}
            if row.get("ok"):
                parts = [f"{k}={v}" for k, v in m.items() if v is not None]
                lines.append(f"• Ozon {row['store_name']}: " + ", ".join(parts[:4]))
            elif row.get("error"):
                lines.append(f"• Ozon {row['store_name']}: ⚠️ {row['error'][:60]}")
        return "\n".join(lines) if len(lines) > 1 else "Нет данных качества."
    if name == "get_task_status":
        st = str(result.get("status") or "—")
        action = result.get("action") or "—"
        detail = result.get("detail") or ""
        prog = result.get("progress") or [0, 1]
        icon = {"running": "🔄", "done": "✅", "error": "❌", "cancelled": "⏹"}.get(st, "•")
        lines = [
            f"{icon} Задача {result.get('task_id', '—')}",
            f"• Действие: {action}",
            f"• Статус: {st}",
            f"• Прогресс: {prog[0]}/{prog[1]}",
        ]
        if detail:
            lines.append(f"• {detail}")
        if result.get("error"):
            lines.append(f"• Ошибка: {result['error']}")
        return "\n".join(lines)
    if name == "list_active_tasks":
        tasks = result.get("tasks") or []
        if not tasks:
            return "Активных задач нет."
        lines = []
        for t in tasks:
            icon = {"running": "🔄", "done": "✅", "error": "❌", "cancelled": "⏹"}.get(t.get("status"), "•")
            lines.append(f"{icon} {t.get('task_id')} — {t.get('action')} ({t.get('status')})")
        return "📋 Задачи:\n" + "\n".join(lines)
    if name == "export_dialog":
        lines = result.get("lines") or []
        if not lines:
            return "История диалога пуста."
        return "💬 История диалога:\n\n" + "\n\n".join(lines[-40:])
    if name == "list_recent_operations":
        ops = result.get("operations") or []
        if not ops:
            return "Записей в журнале нет."
        lines = [f"• {o['ts'][:16]} {o['action']} — {o['actor']}" + (f" ({o['store']})" if o.get("store") else "") for o in ops]
        return "📜 Последние операции:\n" + "\n".join(lines)
    if name == "list_card_errors":
        alerts = result.get("alerts") or []
        if not alerts:
            return "Ошибок карточек не найдено."
        lines = [f"• #{a['id']} {a['store']}: {a['product']} [{a['kind']}]" for a in alerts]
        return f"⚠️ Ошибки карточек ({len(alerts)}):\n" + "\n".join(lines)
    if name == "list_ozon_alerts":
        alerts = result.get("alerts") or []
        if not alerts:
            return "Важных уведомлений Ozon нет."
        lines = [f"• {a['store']}: {a['summary']}" for a in alerts]
        return f"🔔 Ozon ({len(alerts)}):\n" + "\n".join(lines)
    if name in ("pipeline_answer_reviews", "pipeline_answer_questions"):
        steps = result.get("steps") or []
        body = result.get("message") or ""
        if steps:
            body = body + "\n\n" + "\n".join(f"• {s}" for s in steps) if body else "\n".join(f"• {s}" for s in steps)
        return body or "Цикл выполнен."
    if name == "check_ozon_promotions":
        lines = ["🏷 Акции Ozon:"]
        for block in result.get("stores") or []:
            if block.get("error"):
                lines.append(f"\n📦 {block.get('store_name', '?')}: ⚠️ {block['error']}")
                continue
            lines.append(f"\n📦 {block.get('store_name', '?')} ({block.get('actions_count', 0)} акций):")
            for a in (block.get("actions") or [])[:15]:
                auto = " 🤖авто" if a.get("is_auto_add") else ""
                lines.append(
                    f"  • id={a.get('id')} — {a.get('title', '')[:45]}: "
                    f"{a.get('participating_products_count', 0)} тов.{auto}"
                )
            rest = len(block.get("actions") or []) - 15
            if rest > 0:
                lines.append(f"  … ещё {rest} акций")
        lines.append(f"\nИтого: {result.get('total_actions', 0)} акций, {result.get('total_products', 0)} товаров")
        return "\n".join(lines)
    if name == "remove_ozon_promotions":
        if result.get("message"):
            lines = [f"✅ {result['message']}"]
            for row in result.get("per_store") or []:
                if row.get("products_removed"):
                    lines.append(
                        f"• {row.get('store_name')}: снято {row.get('products_removed')} "
                        f"из {row.get('actions_processed')} акций"
                    )
            return "\n".join(lines)
    if result.get("message"):
        return str(result["message"])
    if result.get("task_id"):
        return f"✅ Задача запущена (id: {result['task_id']}). Спросите «как успехи?» для статуса."
    import json
    try:
        return json.dumps(result, ensure_ascii=False, indent=2)[:2500]
    except Exception:
        return str(result)[:2000]
