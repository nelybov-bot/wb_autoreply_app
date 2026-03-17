"""
Фоновые задачи для веб-API: загрузка, генерация, отправка.
Состояние хранится в памяти, опрос через GET /api/tasks/{id}.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, Optional

from app.db import Database, Store
from app.core.workflows import load_new_all, generate_mass, send_mass_all
from app.core.net import UnauthorizedStoreError

log = logging.getLogger("web.tasks")

# task_id -> { "status": "running"|"done"|"error", "progress": [current, total], "result": Any, "error": str }
_tasks: dict[str, dict[str, Any]] = {}
_tasks_lock = asyncio.Lock()


def _make_id() -> str:
    return uuid.uuid4().hex[:12]


async def run_load_new(db: Database, store_ids: Optional[list[int]]) -> str:
    """Запускает load_new_all в фоне, возвращает task_id."""
    stores = db.list_stores()
    if store_ids is not None:
        stores = [s for s in stores if s.id in store_ids]
    task_id = _make_id()
    async with _tasks_lock:
        _tasks[task_id] = {"status": "running", "progress": [0, max(len(stores), 1)], "result": None, "error": None}

    async def _set_progress(cur: int, tot: int) -> None:
        safe_tot = max(int(tot or 0), 1)
        safe_cur = max(0, min(int(cur or 0), safe_tot))
        async with _tasks_lock:
            if task_id in _tasks:
                _tasks[task_id]["progress"] = [safe_cur, safe_tot]

    def _progress(cur: int, tot: int) -> None:
        asyncio.create_task(_set_progress(cur, tot))

    async def _run() -> None:
        try:
            total = len(stores)
            n = await load_new_all(db, stores, progress_queue=None, progress_cb=_progress)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "done"
                _tasks[task_id]["result"] = n
                _tasks[task_id]["progress"] = [total, total]
            try:
                db.add_audit_event(actor="system", action="load_new", item_type="", result="ok", meta={"added": n, "stores": total})
            except Exception:
                pass
        except UnauthorizedStoreError as e:
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = f"Магазин «{e.store_name}»: неверный ключ или доступ запрещён."
            try:
                db.add_audit_event(actor="system", action="load_new", item_type="", result="error", meta={"error": str(e), "store": e.store_name})
            except Exception:
                pass
        except Exception as e:
            log.exception("load_new task %s failed: %s", task_id, e)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = str(e)
            try:
                db.add_audit_event(actor="system", action="load_new", item_type="", result="error", meta={"error": str(e)})
            except Exception:
                pass

    asyncio.create_task(_run())
    return task_id


async def run_generate(db: Database, item_ids: list[int], openai_key: str) -> str:
    """Запускает generate_mass в фоне, возвращает task_id."""
    if not openai_key or not openai_key.strip():
        raise ValueError("OpenAI ключ не задан")
    task_id = _make_id()
    async with _tasks_lock:
        _tasks[task_id] = {"status": "running", "progress": [0, len(item_ids)], "result": None, "error": None}

    async def _set_progress(cur: int, tot: int) -> None:
        safe_tot = max(int(tot or 0), 1)
        safe_cur = max(0, min(int(cur or 0), safe_tot))
        async with _tasks_lock:
            if task_id in _tasks:
                _tasks[task_id]["progress"] = [safe_cur, safe_tot]

    def _progress(cur: int, tot: int) -> None:
        asyncio.create_task(_set_progress(cur, tot))

    async def _run() -> None:
        try:
            ok, failed = await generate_mass(db, item_ids, openai_key, model="gpt-5.2", progress_queue=None, progress_cb=_progress)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "done"
                _tasks[task_id]["result"] = {"ok": ok, "failed": failed}
                _tasks[task_id]["progress"] = [len(item_ids), len(item_ids)]
            try:
                # item_type can be mixed; mark as 'mixed' for ops log
                db.add_audit_event(
                    actor="system",
                    action="generate",
                    item_type="mixed",
                    result="ok",
                    meta={"ok": ok, "failed": failed, "count": len(item_ids), "item_ids": item_ids[:50]},
                )
            except Exception:
                pass
        except Exception as e:
            log.exception("generate task %s failed: %s", task_id, e)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = str(e)
            try:
                db.add_audit_event(actor="system", action="generate", item_type="mixed", result="error", meta={"error": str(e), "count": len(item_ids)})
            except Exception:
                pass

    asyncio.create_task(_run())
    return task_id


async def run_send(db: Database, item_ids: list[int]) -> str:
    """Запускает send_mass_all в фоне, возвращает task_id."""
    task_id = _make_id()
    async with _tasks_lock:
        _tasks[task_id] = {"status": "running", "progress": [0, 1], "result": None, "error": None}

    async def _set_progress(cur: int, tot: int) -> None:
        safe_tot = max(int(tot or 0), 1)
        safe_cur = max(0, min(int(cur or 0), safe_tot))
        async with _tasks_lock:
            if task_id in _tasks:
                _tasks[task_id]["progress"] = [safe_cur, safe_tot]

    def _progress(cur: int, tot: int) -> None:
        asyncio.create_task(_set_progress(cur, tot))

    async def _run() -> None:
        try:
            sent_ok, skipped, failed = await send_mass_all(db, item_ids, progress_queue=None, progress_cb=_progress)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "done"
                _tasks[task_id]["result"] = {"sent_ok": sent_ok, "skipped": skipped, "failed": failed}
                _tasks[task_id]["progress"] = [1, 1]
            try:
                db.add_audit_event(
                    actor="system",
                    action="send",
                    item_type="mixed",
                    result="ok",
                    meta={"sent_ok": sent_ok, "skipped": skipped, "failed": failed, "count": len(item_ids), "item_ids": item_ids[:50]},
                )
            except Exception:
                pass
        except UnauthorizedStoreError as e:
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = f"Магазин «{e.store_name}»: неверный ключ или доступ запрещён."
            try:
                db.add_audit_event(actor="system", action="send", item_type="mixed", result="error", meta={"error": str(e), "store": e.store_name})
            except Exception:
                pass
        except Exception as e:
            log.exception("send task %s failed: %s", task_id, e)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = str(e)
            try:
                db.add_audit_event(actor="system", action="send", item_type="mixed", result="error", meta={"error": str(e), "count": len(item_ids)})
            except Exception:
                pass

    asyncio.create_task(_run())
    return task_id


async def get_task(task_id: str) -> Optional[dict[str, Any]]:
    """Возвращает состояние задачи или None."""
    async with _tasks_lock:
        return _tasks.get(task_id)
