"""
Запуск asyncio в отдельном потоке, чтобы UI не лагал.

При остановке сначала отменяются все задачи, затем закрывается loop.
Во время shutdown уровень лога asyncio поднимаем, чтобы не сыпало "Task was destroyed".

На macOS с Python 3.13 asyncio при создании SSL-транспорта вызывает setsockopt(TCP_NODELAY),
что может дать OSError [Errno 22] Invalid argument. Патчим _set_nodelay, чтобы игнорировать EINVAL.
"""
from __future__ import annotations

import asyncio
import logging
import sys
import threading
from concurrent.futures import Future
from typing import Any, Coroutine, Optional

_asyncio_log = logging.getLogger("asyncio")


def _patch_asyncio_tcp_nodelay_macos() -> None:
    """Игнорировать EINVAL при установке TCP_NODELAY на macOS (Python 3.13 + aiohttp)."""
    if sys.platform != "darwin":
        return
    try:
        import asyncio.base_events as base_events
        _orig = base_events._set_nodelay

        def _patched(sock: Any) -> None:
            try:
                _orig(sock)
            except OSError as e:
                if e.errno != 22:  # EINVAL
                    raise

        base_events._set_nodelay = _patched
    except Exception:
        pass


class AsyncRunner:
    def __init__(self) -> None:
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._ready = threading.Event()
        self._thread.start()
        self._ready.wait()

    def _run(self) -> None:
        _patch_asyncio_tcp_nodelay_macos()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._ready.set()
        loop.run_forever()

    def submit(self, coro: Coroutine[Any, Any, Any]) -> Future:
        if not self._loop:
            raise RuntimeError("Async loop not ready")
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def stop(self) -> None:
        if not self._loop:
            return
        loop = self._loop
        self._loop = None
        old_level = _asyncio_log.level
        _asyncio_log.setLevel(logging.CRITICAL)

        def _ignore_exceptions(loop: asyncio.AbstractEventLoop, ctx: dict) -> None:
            pass

        async def _shutdown() -> None:
            loop.set_exception_handler(_ignore_exceptions)
            tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
            for t in tasks:
                t.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(0.5)
            loop.stop()

        try:
            asyncio.run_coroutine_threadsafe(_shutdown(), loop).result(timeout=5.0)
        except Exception:
            pass
        _asyncio_log.setLevel(old_level)
        if self._thread.is_alive():
            self._thread.join(timeout=2.0)
