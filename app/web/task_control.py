"""Cooperative cancellation для фоновых задач."""
from __future__ import annotations

import asyncio


class TaskControl:
    def __init__(self) -> None:
        self._cancelled = asyncio.Event()

    def request_cancel(self) -> None:
        self._cancelled.set()

    @property
    def cancelled(self) -> bool:
        return self._cancelled.is_set()

    def raise_if_cancelled(self) -> None:
        if self.cancelled:
            raise asyncio.CancelledError()
