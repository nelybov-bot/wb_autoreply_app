"""
Фоновые задачи для веб-API: загрузка, генерация, отправка.
Состояние хранится в памяти, опрос через GET /api/tasks/{id}.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import Any, Optional

from app.db import Database, Store
from app.core.net import OzonApiAccessError, UnauthorizedStoreError
from app.core.workflows import load_new_all, generate_mass, send_mass_all
from app.web.store_locks import StoreBusyError, store_locks
from app.web.task_control import TaskControl

log = logging.getLogger("web.tasks")

_TASK_TTL_SEC = 3600
_MAX_FINISHED_TASKS = 120

# task_id -> state dict
_tasks: dict[str, dict[str, Any]] = {}
_tasks_lock = asyncio.Lock()
_handles: dict[str, asyncio.Task] = {}
_controls: dict[str, TaskControl] = {}


def _make_id() -> str:
    return uuid.uuid4().hex[:12]


def _store_names(db: Database, store_ids: list[int]) -> dict[int, str]:
    want = {int(x) for x in store_ids}
    return {s.id: s.name for s in db.list_stores() if s.id in want}


def _store_ids_for_items(db: Database, item_ids: list[int]) -> list[int]:
    out: set[int] = set()
    for iid in item_ids:
        row = db.get_item_by_id(int(iid))
        if row:
            out.add(int(row.store_id))
    return sorted(out)


async def _prune_tasks() -> None:
    now = time.time()
    async with _tasks_lock:
        finished = [
            (tid, st)
            for tid, st in _tasks.items()
            if st.get("status") in ("done", "error", "cancelled")
        ]
        finished.sort(key=lambda x: float(x[1].get("finished_at") or 0))
        for tid, st in finished:
            finished_at = float(st.get("finished_at") or 0)
            if now - finished_at > _TASK_TTL_SEC:
                _tasks.pop(tid, None)
                _handles.pop(tid, None)
                _controls.pop(tid, None)
        while len(finished) > _MAX_FINISHED_TASKS:
            tid, _ = finished.pop(0)
            _tasks.pop(tid, None)
            _handles.pop(tid, None)
            _controls.pop(tid, None)


def _mark_finished(task_id: str, status: str) -> None:
    async def _do() -> None:
        async with _tasks_lock:
            if task_id in _tasks:
                _tasks[task_id]["status"] = status
                _tasks[task_id]["finished_at"] = time.time()
        await _prune_tasks()

    asyncio.create_task(_do())


async def _init_task(task_id: str, action: str, detail: str, total: int) -> TaskControl:
    ctrl = TaskControl()
    async with _tasks_lock:
        _tasks[task_id] = {
            "status": "running",
            "action": action,
            "detail": detail,
            "progress": [0, max(total, 1)],
            "result": None,
            "error": None,
            "store_ids": [],
            "finished_at": None,
        }
        _controls[task_id] = ctrl
    return ctrl


async def run_load_new(db: Database, store_ids: Optional[list[int]]) -> str:
    stores = db.list_stores()
    if store_ids is not None:
        stores = [s for s in stores if s.id in store_ids]
    sids = [s.id for s in stores]
    task_id = _make_id()
    try:
        await store_locks.acquire(sids, "load", task_id, store_names=_store_names(db, sids))
    except StoreBusyError:
        await store_locks.release_all_for_owner(task_id)
        raise

    ctrl = await _init_task(task_id, "load_new", "Подготовка…", len(stores))
    async with _tasks_lock:
        _tasks[task_id]["store_ids"] = sids

    async def _set_progress(cur: int, tot: int) -> None:
        safe_tot = max(int(tot or 0), 1)
        safe_cur = max(0, min(int(cur or 0), safe_tot))
        async with _tasks_lock:
            if task_id in _tasks:
                _tasks[task_id]["progress"] = [safe_cur, safe_tot]

    def _progress(cur: int, tot: int) -> None:
        if _tasks.get(task_id, {}).get("status") == "running":
            asyncio.create_task(_set_progress(cur, tot))

    async def _run() -> None:
        try:
            total = len(stores)
            added_total = 0
            if total == 0:
                async with _tasks_lock:
                    _tasks[task_id]["status"] = "done"
                    _tasks[task_id]["result"] = 0
                    _tasks[task_id]["progress"] = [0, 1]
                    _tasks[task_id]["detail"] = "Нет выбранных магазинов"
                _mark_finished(task_id, "done")
                return
            for i, s in enumerate(stores):
                ctrl.raise_if_cancelled()
                async with _tasks_lock:
                    if task_id in _tasks:
                        _tasks[task_id]["detail"] = f"Магазин: {s.name} ({s.marketplace})"
                n = await load_new_all(
                    db, [s], progress_queue=None, progress_cb=None, cancel=ctrl,
                )
                added_total += int(n or 0)
                _progress(i + 1, total)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "done"
                _tasks[task_id]["result"] = added_total
                _tasks[task_id]["progress"] = [total, total]
                _tasks[task_id]["detail"] = "Готово" if added_total else "Новых записей нет"
            _mark_finished(task_id, "done")
            try:
                db.add_audit_event(
                    actor="system", action="load_new", item_type="", result="ok",
                    meta={"added": added_total, "stores": total},
                )
            except Exception:
                pass
        except asyncio.CancelledError:
            async with _tasks_lock:
                if task_id in _tasks and _tasks[task_id].get("status") == "running":
                    _tasks[task_id]["status"] = "cancelled"
                    _tasks[task_id]["error"] = "Остановлено пользователем"
                    _tasks[task_id]["detail"] = "Остановлено"
            _mark_finished(task_id, "cancelled")
        except UnauthorizedStoreError as e:
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = f"Магазин «{e.store_name}»: неверный ключ или доступ запрещён."
            _mark_finished(task_id, "error")
        except OzonApiAccessError as e:
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = str(e.message or e)
            _mark_finished(task_id, "error")
        except Exception as e:
            log.exception("load_new task %s failed: %s", task_id, e)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = str(e)
            _mark_finished(task_id, "error")
        finally:
            await store_locks.release(sids, task_id)

    _handles[task_id] = asyncio.create_task(_run())
    return task_id


async def run_generate(db: Database, item_ids: list[int], openai_key: str) -> str:
    if not openai_key or not openai_key.strip():
        raise ValueError("OpenAI ключ не задан")
    sids = _store_ids_for_items(db, item_ids)
    task_id = _make_id()
    try:
        await store_locks.acquire(sids, "generate", task_id, store_names=_store_names(db, sids))
    except StoreBusyError:
        await store_locks.release_all_for_owner(task_id)
        raise

    ctrl = await _init_task(task_id, "generate", "Генерация…", len(item_ids))
    async with _tasks_lock:
        _tasks[task_id]["store_ids"] = sids

    def _progress(cur: int, tot: int) -> None:
        if _tasks.get(task_id, {}).get("status") != "running":
            return
        safe_tot = max(int(tot or 0), 1)
        safe_cur = max(0, min(int(cur or 0), safe_tot))
        async def _set() -> None:
            async with _tasks_lock:
                if task_id in _tasks:
                    _tasks[task_id]["progress"] = [safe_cur, safe_tot]
        asyncio.create_task(_set())

    async def _run() -> None:
        try:
            ok, failed, _card_errors = await generate_mass(
                db, item_ids, openai_key, model="gpt-5.2",
                progress_queue=None, progress_cb=_progress, cancel=ctrl,
            )
            async with _tasks_lock:
                _tasks[task_id]["status"] = "done"
                _tasks[task_id]["result"] = {"ok": ok, "failed": failed}
                _tasks[task_id]["progress"] = [len(item_ids), len(item_ids)]
                _tasks[task_id]["detail"] = "Готово"
            _mark_finished(task_id, "done")
            try:
                db.add_audit_event(
                    actor="system", action="generate", item_type="mixed", result="ok",
                    meta={"ok": ok, "failed": failed, "count": len(item_ids), "item_ids": item_ids[:50]},
                )
            except Exception:
                pass
        except asyncio.CancelledError:
            async with _tasks_lock:
                if task_id in _tasks and _tasks[task_id].get("status") == "running":
                    _tasks[task_id]["status"] = "cancelled"
                    _tasks[task_id]["error"] = "Остановлено пользователем"
            _mark_finished(task_id, "cancelled")
        except Exception as e:
            log.exception("generate task %s failed: %s", task_id, e)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = str(e)
            _mark_finished(task_id, "error")
        finally:
            await store_locks.release(sids, task_id)

    _handles[task_id] = asyncio.create_task(_run())
    return task_id


async def run_send(db: Database, item_ids: list[int]) -> str:
    sids = _store_ids_for_items(db, item_ids)
    task_id = _make_id()
    try:
        await store_locks.acquire(sids, "send", task_id, store_names=_store_names(db, sids))
    except StoreBusyError:
        await store_locks.release_all_for_owner(task_id)
        raise

    safe_total = max(len(item_ids), 1)
    ctrl = await _init_task(task_id, "send", "Отправка…", safe_total)
    async with _tasks_lock:
        _tasks[task_id]["store_ids"] = sids

    def _progress(cur: int, tot: int) -> None:
        if _tasks.get(task_id, {}).get("status") != "running":
            return
        safe_tot = max(int(tot or 0), 1)
        safe_cur = max(0, min(int(cur or 0), safe_tot))
        async def _set() -> None:
            async with _tasks_lock:
                if task_id in _tasks:
                    _tasks[task_id]["progress"] = [safe_cur, safe_tot]
        asyncio.create_task(_set())

    async def _run() -> None:
        try:
            sent_ok, skipped, failed = await send_mass_all(
                db, item_ids, progress_queue=None, progress_cb=_progress, cancel=ctrl,
            )
            async with _tasks_lock:
                _tasks[task_id]["status"] = "done"
                _tasks[task_id]["result"] = {"sent_ok": sent_ok, "skipped": skipped, "failed": failed}
                _tasks[task_id]["progress"] = [safe_total, safe_total]
                _tasks[task_id]["detail"] = "Готово"
            _mark_finished(task_id, "done")
            try:
                db.add_audit_event(
                    actor="system", action="send", item_type="mixed", result="ok",
                    meta={"sent_ok": sent_ok, "skipped": skipped, "failed": failed, "count": len(item_ids), "item_ids": item_ids[:50]},
                )
            except Exception:
                pass
        except asyncio.CancelledError:
            async with _tasks_lock:
                if task_id in _tasks and _tasks[task_id].get("status") == "running":
                    _tasks[task_id]["status"] = "cancelled"
                    _tasks[task_id]["error"] = "Остановлено пользователем"
            _mark_finished(task_id, "cancelled")
        except UnauthorizedStoreError as e:
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = f"Магазин «{e.store_name}»: неверный ключ или доступ запрещён."
            _mark_finished(task_id, "error")
        except Exception as e:
            log.exception("send task %s failed: %s", task_id, e)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = str(e)
            _mark_finished(task_id, "error")
        finally:
            await store_locks.release(sids, task_id)

    _handles[task_id] = asyncio.create_task(_run())
    return task_id


async def run_card_links_ai_suggest(
    *,
    rows: list,
    groups: list,
    marketplace: str,
    openai_key: str,
    include_linked: bool = True,
    scope: str = "all",
    batch_size: int = 60,
    max_products: int = 0,
    max_ai_batches: int = 12,
    deterministic_packs: bool = True,
    split_oversized: bool = True,
    system_prompt: str = "",
) -> str:
    from app.core.card_links import ai_suggest_card_links
    from app.core.net import HttpStatusError

    task_id = _make_id()
    ctrl = await _init_task(task_id, "card_links_ai", "Подготовка ИИ…", 1)

    def _progress(cur: int, tot: int, detail: str) -> None:
        if _tasks.get(task_id, {}).get("status") != "running":
            return
        safe_tot = max(int(tot or 0), 1)
        safe_cur = max(0, min(int(cur or 0), safe_tot))

        async def _set() -> None:
            async with _tasks_lock:
                if task_id in _tasks:
                    _tasks[task_id]["progress"] = [safe_cur, safe_tot]
                    _tasks[task_id]["detail"] = detail

        asyncio.create_task(_set())

    async def _run() -> None:
        try:
            ai_rows, ai_bundles, ai_meta = await ai_suggest_card_links(
                list(rows),
                list(groups),
                marketplace=marketplace,
                openai_key=openai_key,
                include_linked=include_linked,
                scope=scope,
                batch_size=batch_size,
                max_products=max_products,
                max_ai_batches=max_ai_batches,
                deterministic_packs=deterministic_packs,
                split_oversized=split_oversized,
                system_prompt=system_prompt or None,
                progress_cb=_progress,
                cancel=ctrl,
            )
            async with _tasks_lock:
                _tasks[task_id]["status"] = "done"
                _tasks[task_id]["result"] = {
                    "ai_suggestions": ai_rows,
                    "ai_bundles": ai_bundles,
                    "count": len(ai_bundles),
                    "ai_meta": ai_meta,
                }
                planned = int(ai_meta.get("batches_planned") or 0)
                run_n = int(ai_meta.get("batches_run") or 0)
                skipped = int(ai_meta.get("batches_skipped") or 0)
                detail = f"Готово · {len(ai_bundles)} связок"
                if planned:
                    detail += f" · ИИ-запросов {run_n}/{planned}"
                if skipped:
                    detail += f" · пропущено {skipped} (лимит батчей)"
                _tasks[task_id]["progress"] = [max(run_n, 1), max(planned, run_n, 1)]
                _tasks[task_id]["detail"] = detail
            _mark_finished(task_id, "done")
        except asyncio.CancelledError:
            async with _tasks_lock:
                if task_id in _tasks and _tasks[task_id].get("status") == "running":
                    _tasks[task_id]["status"] = "cancelled"
                    _tasks[task_id]["error"] = "Остановлено пользователем"
            _mark_finished(task_id, "cancelled")
        except HttpStatusError as e:
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = str(e.body or e)[:400]
            _mark_finished(task_id, "error")
        except Exception as e:
            log.exception("card_links_ai task %s failed: %s", task_id, e)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = str(e)[:400]
            _mark_finished(task_id, "error")

    _handles[task_id] = asyncio.create_task(_run())
    return task_id


async def run_card_links_master_step(
    db: Any,
    *,
    store_id: int,
    step: str,
    api_key: str,
    openai_key: str = "",
    max_pages: int = 100,
    bundle_ids: Optional[list] = None,
) -> str:
    from app.core.card_links import wb_content_api_error_message
    from app.core.card_links_master import run_master_step
    from app.core.net import HttpStatusError, UnauthorizedStoreError

    task_id = _make_id()
    labels = {
        "load": "Загрузка WB",
        "brands": "Бренды",
        "segment": "Сегмент",
        "classify": "Тип / модель",
        "plan": "План связок",
        "apply": "Применение",
    }
    sid = int(store_id)
    sids = [sid]
    lock_steps = {"load", "apply"}
    if step in lock_steps:
        try:
            await store_locks.acquire(
                sids, "card_links", task_id, store_names=_store_names(db, sids),
            )
        except StoreBusyError:
            await store_locks.release_all_for_owner(task_id)
            raise

    ctrl = await _init_task(task_id, "card_links_master", labels.get(step, step), 1)
    async with _tasks_lock:
        _tasks[task_id]["store_ids"] = sids

    def _progress(cur: int, tot: int, detail: str) -> None:
        if _tasks.get(task_id, {}).get("status") != "running":
            return
        safe_tot = max(int(tot or 0), 1)
        safe_cur = max(0, min(int(cur or 0), safe_tot))

        async def _set() -> None:
            async with _tasks_lock:
                if task_id in _tasks:
                    _tasks[task_id]["progress"] = [safe_cur, safe_tot]
                    _tasks[task_id]["detail"] = detail

        asyncio.create_task(_set())

    async def _run() -> None:
        try:
            ctrl.raise_if_cancelled()
            run_coro = run_master_step(
                db,
                sid,
                step,
                api_key=api_key,
                openai_key=openai_key,
                max_pages=max_pages,
                bundle_ids=bundle_ids,
                progress_cb=_progress,
            )
            if step == "load":
                result = await asyncio.wait_for(run_coro, timeout=600.0)
            else:
                result = await run_coro
            async with _tasks_lock:
                _tasks[task_id]["status"] = "done"
                _tasks[task_id]["result"] = result
                _tasks[task_id]["progress"] = [1, 1]
                _tasks[task_id]["detail"] = f"Шаг «{labels.get(step, step)}» завершён"
            _mark_finished(task_id, "done")
        except asyncio.CancelledError:
            async with _tasks_lock:
                if task_id in _tasks and _tasks[task_id].get("status") == "running":
                    _tasks[task_id]["status"] = "cancelled"
                    _tasks[task_id]["error"] = "Остановлено пользователем"
            _mark_finished(task_id, "cancelled")
        except UnauthorizedStoreError as e:
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = f"Магазин «{e.store_name}»: неверный ключ или доступ запрещён."
            _mark_finished(task_id, "error")
        except HttpStatusError as e:
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = wb_content_api_error_message(e.status, e.body or "")[:500]
            _mark_finished(task_id, "error")
        except asyncio.TimeoutError:
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = (
                    "Загрузка каталога WB превысила 10 минут — уменьшите «Страниц каталога» или повторите позже."
                )
            _mark_finished(task_id, "error")
        except Exception as e:
            log.exception("card_links_master step %s failed: %s", step, e)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = str(e)[:400]
            _mark_finished(task_id, "error")
        finally:
            if step in lock_steps:
                await store_locks.release(sids, task_id)

    _handles[task_id] = asyncio.create_task(_run())
    return task_id


async def get_task(task_id: str) -> Optional[dict[str, Any]]:
    await _prune_tasks()
    async with _tasks_lock:
        st = _tasks.get(task_id)
        return dict(st) if st else None


async def cancel_task(task_id: str) -> bool:
    async with _tasks_lock:
        state = _tasks.get(task_id)
        if state is None:
            return False
        if state.get("status") != "running":
            return True
        state["status"] = "cancelled"
        state["error"] = "Остановлено пользователем"
        state["detail"] = "Остановлено"
    ctrl = _controls.get(task_id)
    if ctrl:
        ctrl.request_cancel()
    t = _handles.get(task_id)
    if t and not t.done():
        t.cancel()
    return True


async def cancel_all_running() -> None:
    async with _tasks_lock:
        ids = [tid for tid, st in _tasks.items() if st.get("status") == "running"]
    for tid in ids:
        await cancel_task(tid)


async def list_tasks(*, status: Optional[str] = None, limit: int = 15) -> list[dict[str, Any]]:
    """Список задач в памяти (load/generate/send)."""
    await _prune_tasks()
    safe_limit = max(1, min(int(limit), 30))
    async with _tasks_lock:
        rows = [(tid, dict(st)) for tid, st in _tasks.items()]
    rows.sort(key=lambda x: float(x[1].get("finished_at") or 0), reverse=True)
    out: list[dict[str, Any]] = []
    for tid, st in rows:
        st_status = str(st.get("status") or "")
        if status and st_status != status:
            continue
        out.append({
            "task_id": tid,
            "status": st_status,
            "action": st.get("action"),
            "detail": st.get("detail"),
            "progress": st.get("progress"),
            "error": st.get("error"),
            "result": st.get("result"),
        })
        if len(out) >= safe_limit:
            break
    return out


async def run_wb_certificates_apply(
    db: Any,
    *,
    store_ids: list[int],
    text: str = "",
    vendor_codes: Optional[list[str]] = None,
    dry_run: bool = False,
) -> str:
    from app.core.wb_certificates import (
        apply_certificates_multi_store,
        filter_cert_rows,
        parse_certificates_text,
    )
    from app.core.net import HttpStatusError, UnauthorizedStoreError

    rows, parse_warnings = parse_certificates_text(text)
    if not rows:
        raise ValueError(parse_warnings[0] if parse_warnings else "Нет данных для обработки")

    if vendor_codes:
        rows, filter_warnings = filter_cert_rows(rows, vendor_codes)
        parse_warnings = list(parse_warnings) + filter_warnings
        if not rows:
            raise ValueError(
                filter_warnings[0] if filter_warnings else "Ни один из выбранных артикулов не найден в таблице"
            )

    sids = sorted({int(x) for x in store_ids if int(x) > 0})
    if not sids:
        raise ValueError("Выберите хотя бы один магазин WB")

    stores_payload: list[tuple[int, str, str]] = []
    by_id = {s.id: s for s in db.list_stores()}
    for sid in sids:
        st = by_id.get(sid)
        if not st or str(st.marketplace or "").lower() != "wb":
            raise ValueError(f"Магазин {sid} не найден или не WB")
        if not (st.api_key or "").strip():
            raise ValueError(f"У магазина «{st.name}» нет API-ключа")
        stores_payload.append((sid, st.name, st.api_key.strip()))

    task_id = _make_id()
    try:
        await store_locks.acquire(
            sids, "wb_certificates", task_id,
            store_names=_store_names(db, sids),
        )
    except StoreBusyError:
        await store_locks.release_all_for_owner(task_id)
        raise

    total_steps = max(len(rows) * len(sids), 1)
    await _init_task(task_id, "wb_certificates", "Сертификаты WB", total_steps)
    async with _tasks_lock:
        _tasks[task_id]["store_ids"] = sids
        _tasks[task_id]["parse_warnings"] = parse_warnings

    def _progress(cur: int, tot: int, detail: str) -> None:
        if _tasks.get(task_id, {}).get("status") != "running":
            return
        safe_tot = max(int(tot or 0), 1)
        safe_cur = max(0, min(int(cur or 0), safe_tot))

        async def _set() -> None:
            async with _tasks_lock:
                if task_id in _tasks:
                    _tasks[task_id]["progress"] = [safe_cur, safe_tot]
                    _tasks[task_id]["detail"] = detail

        asyncio.create_task(_set())

    async def _run() -> None:
        try:
            result = await apply_certificates_multi_store(
                stores_payload,
                rows=rows,
                dry_run=dry_run,
                progress_cb=_progress,
            )
            if parse_warnings:
                result["parse_warnings"] = parse_warnings
            async with _tasks_lock:
                _tasks[task_id]["status"] = "done"
                _tasks[task_id]["result"] = result
                sent = sum(int(s.get("sent") or 0) for s in result.get("stores") or [])
                prepared = sum(int(s.get("prepared") or 0) for s in result.get("stores") or [])
                if dry_run:
                    _tasks[task_id]["detail"] = f"Проверка: {prepared} карточек в {len(sids)} магазинах"
                else:
                    _tasks[task_id]["detail"] = f"Отправлено {sent} обновлений в {len(sids)} магазинах"
                _tasks[task_id]["progress"] = [total_steps, total_steps]
            _mark_finished(task_id, "done")
        except asyncio.CancelledError:
            async with _tasks_lock:
                if task_id in _tasks and _tasks[task_id].get("status") == "running":
                    _tasks[task_id]["status"] = "cancelled"
                    _tasks[task_id]["error"] = "Остановлено пользователем"
            _mark_finished(task_id, "cancelled")
        except UnauthorizedStoreError as e:
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = str(e)[:400]
            _mark_finished(task_id, "error")
        except HttpStatusError as e:
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = str(e.body or e)[:400]
            _mark_finished(task_id, "error")
        except Exception as e:
            log.exception("wb_certificates task %s failed: %s", task_id, e)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = str(e)[:400]
            _mark_finished(task_id, "error")
        finally:
            await store_locks.release(sids, task_id)

    _handles[task_id] = asyncio.create_task(_run())
    return task_id


async def run_ozon_certificates_apply(
    db: Any,
    *,
    store_ids: list[int],
    text: str = "",
    vendor_codes: Optional[list[str]] = None,
    dry_run: bool = False,
    fsa_only: bool = False,
) -> str:
    from app.core.compliance_docs import filter_cert_rows, parse_certificates_text
    from app.core.net import HttpStatusError, UnauthorizedStoreError
    from app.core.ozon_certificates import apply_ozon_certificates_multi_store

    rows, parse_warnings = parse_certificates_text(text)
    if not rows:
        raise ValueError(parse_warnings[0] if parse_warnings else "Нет данных для обработки")

    if vendor_codes:
        rows, filter_warnings = filter_cert_rows(rows, vendor_codes)
        parse_warnings = list(parse_warnings) + filter_warnings
        if not rows:
            raise ValueError(
                filter_warnings[0] if filter_warnings else "Ни один из выбранных артикулов не найден в таблице"
            )

    stores_payload: list[tuple[int, str, str, str]] = []
    sids: list[int] = []
    if not fsa_only:
        sids = sorted({int(x) for x in store_ids if int(x) > 0})
        if not sids:
            raise ValueError("Выберите хотя бы один магазин Ozon")
        by_id = {s.id: s for s in db.list_stores()}
        for sid in sids:
            st = by_id.get(sid)
            if not st or str(st.marketplace or "").lower() != "ozon":
                raise ValueError(f"Магазин {sid} не найден или не Ozon")
            cid = str(st.client_id or "").strip()
            key = str(st.api_key or "").strip()
            if not cid or not key:
                raise ValueError(f"У магазина «{st.name}» нет Client-Id / Api-Key")
            stores_payload.append((sid, st.name, cid, key))

    unique_docs = len({str(r.doc_number or "").strip() for r in rows if str(r.doc_number or "").strip()})
    total_steps = max(unique_docs + (0 if fsa_only else len(rows) * len(sids)), 1)

    task_id = _make_id()
    if sids:
        try:
            await store_locks.acquire(
                sids, "ozon_certificates", task_id,
                store_names=_store_names(db, sids),
            )
        except StoreBusyError:
            await store_locks.release_all_for_owner(task_id)
            raise

    label = "ФСА" if fsa_only else ("Проверка Ozon" if dry_run else "Документы Ozon")
    await _init_task(task_id, "ozon_certificates", label, total_steps)
    async with _tasks_lock:
        _tasks[task_id]["store_ids"] = sids
        _tasks[task_id]["parse_warnings"] = parse_warnings

    def _progress(cur: int, tot: int, detail: str) -> None:
        if _tasks.get(task_id, {}).get("status") != "running":
            return
        safe_tot = max(int(tot or 0), 1)
        safe_cur = max(0, min(int(cur or 0), safe_tot))

        async def _set() -> None:
            async with _tasks_lock:
                if task_id in _tasks:
                    _tasks[task_id]["progress"] = [safe_cur, safe_tot]
                    _tasks[task_id]["detail"] = detail

        asyncio.create_task(_set())

    async def _run() -> None:
        try:
            result = await apply_ozon_certificates_multi_store(
                stores_payload,
                rows=rows,
                dry_run=dry_run,
                fsa_only=fsa_only,
                progress_cb=_progress,
            )
            if parse_warnings:
                result["parse_warnings"] = parse_warnings
            async with _tasks_lock:
                _tasks[task_id]["status"] = "done"
                _tasks[task_id]["result"] = result
                if fsa_only:
                    found = sum(
                        1 for st in (result.get("stores") or [])
                        for r in (st.get("rows") or [])
                        if r.get("fsa_found")
                    )
                    _tasks[task_id]["detail"] = f"ФСА: найдено {found} из {len(rows)}"
                elif dry_run:
                    prepared = sum(int(s.get("prepared") or 0) for s in result.get("stores") or [])
                    _tasks[task_id]["detail"] = f"Проверка: {prepared} строк в {len(sids)} магазинах"
                else:
                    bound = sum(int(s.get("bound") or 0) for s in result.get("stores") or [])
                    _tasks[task_id]["detail"] = f"Привязано {bound} товаров в {len(sids)} магазинах"
                _tasks[task_id]["progress"] = [total_steps, total_steps]
            _mark_finished(task_id, "done")
        except asyncio.CancelledError:
            async with _tasks_lock:
                if task_id in _tasks and _tasks[task_id].get("status") == "running":
                    _tasks[task_id]["status"] = "cancelled"
                    _tasks[task_id]["error"] = "Остановлено пользователем"
            _mark_finished(task_id, "cancelled")
        except HttpStatusError as e:
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = str(e.body or e)[:400]
            _mark_finished(task_id, "error")
        except Exception as e:
            log.exception("ozon_certificates task %s failed: %s", task_id, e)
            async with _tasks_lock:
                _tasks[task_id]["status"] = "error"
                _tasks[task_id]["error"] = str(e)[:400]
            _mark_finished(task_id, "error")
        finally:
            if sids:
                await store_locks.release(sids, task_id)

    _handles[task_id] = asyncio.create_task(_run())
    return task_id
