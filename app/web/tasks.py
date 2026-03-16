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

    async def _run() -> None:
        try:
            total = len(stores)
            n = await load_new_all(db, stores, progress_queue=None)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "done"
                _tasks[task_id]["result"] = n
                _tasks[task_id]["progress"] = [total, total]
        except UnauthorizedStoreError as e:
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = f"Магазин «{e.store_name}»: неверный ключ или доступ запрещён."
        except Exception as e:
            log.exception("load_new task %s failed: %s", task_id, e)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = str(e)

    asyncio.create_task(_run())
    return task_id


async def run_generate(db: Database, item_ids: list[int], openai_key: str) -> str:
    """Запускает generate_mass в фоне, возвращает task_id."""
    if not openai_key or not openai_key.strip():
        raise ValueError("OpenAI ключ не задан")
    task_id = _make_id()
    async with _tasks_lock:
        _tasks[task_id] = {"status": "running", "progress": [0, len(item_ids)], "result": None, "error": None}

    async def _run() -> None:
        try:
            ok, failed = await generate_mass(db, item_ids, openai_key, model="gpt-5.2", progress_queue=None)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "done"
                _tasks[task_id]["result"] = {"ok": ok, "failed": failed}
                _tasks[task_id]["progress"] = [len(item_ids), len(item_ids)]
        except Exception as e:
            log.exception("generate task %s failed: %s", task_id, e)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = str(e)

    asyncio.create_task(_run())
    return task_id


async def run_send(db: Database, item_ids: list[int]) -> str:
    """Запускает send_mass_all в фоне, возвращает task_id."""
    task_id = _make_id()
    async with _tasks_lock:
        _tasks[task_id] = {"status": "running", "progress": [0, 1], "result": None, "error": None}

    async def _run() -> None:
        try:
            sent_ok, skipped, failed = await send_mass_all(db, item_ids, progress_queue=None)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "done"
                _tasks[task_id]["result"] = {"sent_ok": sent_ok, "skipped": skipped, "failed": failed}
                _tasks[task_id]["progress"] = [1, 1]
        except UnauthorizedStoreError as e:
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = f"Магазин «{e.store_name}»: неверный ключ или доступ запрещён."
        except Exception as e:
            log.exception("send task %s failed: %s", task_id, e)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = str(e)

    asyncio.create_task(_run())
    return task_id


async def get_task(task_id: str) -> Optional[dict[str, Any]]:
    """Возвращает состояние задачи или None."""
    async with _tasks_lock:
        return _tasks.get(task_id)
