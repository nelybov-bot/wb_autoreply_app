"""Инструменты агента — обёртки над существующими задачами MarketAI."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional

from app.core.quality_metrics import fetch_all_quality
from app.db import Database
from app.web import tasks as web_tasks
from app.web.store_locks import StoreBusyError

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
        return f"Ошибка: {result['error']}"
    if name == "list_stores":
        stores = result.get("stores") or []
        if not stores:
            return "Магазинов нет."
        lines = [f"• {s['name']} (id={s['id']}, {s['marketplace']}, {'активен' if s['active'] else 'выкл'})" for s in stores]
        return f"Магазины ({len(stores)}):\n" + "\n".join(lines)
    if name == "get_stats":
        q = result.get("queue") or {}
        stores = result.get("stores") or {}
        return (
            f"Отправлено сегодня: {result.get('sent_today', 0)}\n"
            f"Новых отзывов: {q.get('new_reviews', 0)}, вопросов: {q.get('new_questions', 0)}\n"
            f"Сгенерировано отзывов: {q.get('generated_reviews', 0)}, вопросов: {q.get('generated_questions', 0)}\n"
            f"Магазины: {stores.get('active', 0)} активных из {stores.get('total', 0)}"
        )
    if name == "list_queue_items":
        items = result.get("items") or []
        if not items:
            return "Записей не найдено."
        lines = []
        for it in items:
            ans = "✓ ответ" if it.get("has_answer") else "— без ответа"
            lines.append(
                f"• id={it['id']} [{it['status']}] {it.get('store_name', '')}: "
                f"{it.get('product_title', '')[:50]} ({ans})"
            )
        return f"Найдено {len(items)}:\n" + "\n".join(lines)
    if name == "get_auto_schedule_status":
        running = "выполняется" if result.get("running") else "ожидание"
        hint = result.get("schedule_hint") or ""
        return (
            f"Автозапуск: {running}, этап: {result.get('phase', '—')}, "
            f"слот: {result.get('slot') or '—'}, следующий: {result.get('next_slot') or '—'}"
            + (f"\n{hint}" if hint else "")
        )
    if name == "get_quality_summary":
        lines = []
        for row in result.get("wb") or []:
            if row.get("ok") and row.get("rating") is not None:
                lines.append(f"WB {row['store_name']}: {row['rating']} ★")
            elif row.get("error"):
                lines.append(f"WB {row['store_name']}: {row['error'][:80]}")
        for row in result.get("ozon") or []:
            m = row.get("metrics") or {}
            if row.get("ok"):
                parts = [f"{k}={v}" for k, v in m.items() if v is not None]
                lines.append(f"Ozon {row['store_name']}: " + ", ".join(parts[:4]))
            elif row.get("error"):
                lines.append(f"Ozon {row['store_name']}: {row['error'][:80]}")
        return "\n".join(lines) if lines else "Нет данных качества."
    if result.get("task_id"):
        return result.get("message") or f"Задача запущена, task_id={result['task_id']}. Следите в интерфейсе отзывов/вопросов."
    import json
    try:
        return json.dumps(result, ensure_ascii=False, indent=2)[:2500]
    except Exception:
        return str(result)[:2000]
