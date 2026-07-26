"""
Блокировки тяжёлых операций по магазину: load / generate / send / auto_run — по одной на store_id.
"""
from __future__ import annotations

import asyncio
from typing import Iterable, Optional

_OP_LABELS = {
    "load": "загрузка",
    "generate": "генерация",
    "send": "отправка",
    "auto_run": "автозапуск",
    "card_links": "связки карточек",
    "wb_certificates": "сертификаты WB",
    "wb_bulk_chars": "характеристики WB",
    "wb_card_drafts": "черновики WB",
    "ozon_certificates": "документы Ozon",
    "packaging_dims": "габариты WB",
}


class StoreBusyError(Exception):
    def __init__(
        self,
        store_id: int,
        store_name: str,
        operation: str,
        *,
        owner: str = "",
    ) -> None:
        self.store_id = store_id
        self.store_name = store_name
        self.operation = operation
        self.owner = str(owner or "")
        busy_ru = _OP_LABELS.get(operation, operation)
        super().__init__(
            f"Магазин «{store_name}» занят: выполняется «{busy_ru}». "
            f"Дождитесь завершения или остановите текущую задачу."
        )

    def as_dict(self) -> dict:
        return {
            "message": str(self),
            "store_id": int(self.store_id),
            "store_name": self.store_name,
            "operation": self.operation,
            "task_id": self.owner or None,
        }


class StoreLockManager:
    def __init__(self) -> None:
        self._held: dict[int, tuple[str, str]] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _store_name(store_id: int, names: Optional[dict[int, str]] = None) -> str:
        if names and int(store_id) in names:
            return names[int(store_id)]
        return str(store_id)

    async def acquire(
        self,
        store_ids: Iterable[int],
        operation: str,
        owner: str,
        *,
        store_names: Optional[dict[int, str]] = None,
    ) -> None:
        ids = sorted({int(x) for x in store_ids if int(x) > 0})
        async with self._lock:
            for sid in ids:
                if sid in self._held:
                    op, held_owner = self._held[sid]
                    raise StoreBusyError(
                        sid,
                        self._store_name(sid, store_names),
                        op,
                        owner=held_owner,
                    )
            for sid in ids:
                self._held[sid] = (operation, owner)

    async def acquire_or_skip(self, store_ids: Iterable[int], operation: str, owner: str) -> list[int]:
        """Возвращает store_id, которые удалось заблокировать; остальные пропускаются."""
        ids = sorted({int(x) for x in store_ids if int(x) > 0})
        acquired: list[int] = []
        async with self._lock:
            for sid in ids:
                if sid in self._held:
                    continue
                self._held[sid] = (operation, owner)
                acquired.append(sid)
        return acquired

    async def release(self, store_ids: Iterable[int], owner: str) -> None:
        ids = {int(x) for x in store_ids if int(x) > 0}
        async with self._lock:
            for sid in list(ids):
                if self._held.get(sid, (None, None))[1] == owner:
                    del self._held[sid]

    async def release_all_for_owner(self, owner: str) -> None:
        async with self._lock:
            for sid, (_, o) in list(self._held.items()):
                if o == owner:
                    del self._held[sid]

    def is_busy(self, store_id: int) -> bool:
        return int(store_id) in self._held

    def snapshot(self) -> list[dict]:
        out: list[dict] = []
        for sid, (op, owner) in sorted(self._held.items()):
            out.append({
                "store_id": int(sid),
                "operation": op,
                "operation_label": _OP_LABELS.get(op, op),
                "task_id": owner,
            })
        return out

    async def force_release(
        self,
        store_ids: Optional[Iterable[int]] = None,
    ) -> list[dict]:
        """Снять блокировки без проверки owner (для UI «сбросить»)."""
        want: Optional[set[int]] = None
        if store_ids is not None:
            want = {int(x) for x in store_ids if int(x) > 0}
        released: list[dict] = []
        async with self._lock:
            for sid, (op, owner) in list(self._held.items()):
                if want is not None and sid not in want:
                    continue
                released.append({
                    "store_id": int(sid),
                    "operation": op,
                    "operation_label": _OP_LABELS.get(op, op),
                    "task_id": owner,
                })
                del self._held[sid]
        return released


store_locks = StoreLockManager()
