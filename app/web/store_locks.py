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
}


class StoreBusyError(Exception):
    def __init__(self, store_id: int, store_name: str, operation: str) -> None:
        self.store_id = store_id
        self.store_name = store_name
        self.operation = operation
        op_ru = _OP_LABELS.get(operation, operation)
        busy_ru = _OP_LABELS.get(operation, operation)
        super().__init__(
            f"Магазин «{store_name}» занят: выполняется «{busy_ru}». "
            f"Дождитесь завершения или остановите текущую задачу."
        )


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
                    op, _ = self._held[sid]
                    raise StoreBusyError(sid, self._store_name(sid, store_names), op)
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


store_locks = StoreLockManager()
