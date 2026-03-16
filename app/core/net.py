"""
Сетевые утилиты: retry с exponential backoff и простая rate-limit пауза.
"""
from __future__ import annotations

import asyncio
import ssl
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

# User-Agent для API-запросов (некоторые сервера сбрасывают соединение без него)
USER_AGENT = "AutoReply/1.0 (https)"

_RETRY_ON = (
    asyncio.TimeoutError,
    ConnectionResetError,
    BrokenPipeError,
    ssl.SSLError,
    OSError,
)
try:
    from aiohttp import ClientConnectorError, ClientError
    _RETRY_ON = _RETRY_ON + (ClientConnectorError, ClientError)
except ImportError:
    pass

@dataclass
class HttpStatusError(Exception):
    status: int
    body: str
    def __str__(self) -> str:
        return f"HTTP {self.status}: {self.body}"


class UnauthorizedStoreError(Exception):
    """401 при работе с магазином WB — в сообщении показываем название магазина."""
    def __init__(self, store_id: int, store_name: str, message: str) -> None:
        self.store_id = store_id
        self.store_name = store_name
        self.message = message
        super().__init__(message)

# Backoff в секундах: после попытки 1 → 1s, 2 → 2s, 3 → 4s, 4 → 8s (всего 5 попыток)
_RETRY_DELAYS = (1, 2, 4, 8)


async def retry(
    fn: Callable[[], Awaitable],
    *,
    retries: int = 5,
    retry_on_status: tuple[int, ...] = (429, 500, 502, 503, 504),
) -> Any:
    last_exc: Optional[BaseException] = None
    for attempt in range(retries):
        try:
            return await fn()
        except HttpStatusError as e:
            last_exc = e
            if e.status not in retry_on_status or attempt >= retries - 1:
                raise
        except _RETRY_ON as e:
            last_exc = e
            if attempt >= retries - 1:
                raise
        delay = _RETRY_DELAYS[attempt] if attempt < len(_RETRY_DELAYS) else _RETRY_DELAYS[-1]
        await asyncio.sleep(delay)
    if last_exc:
        raise last_exc

class RateLimiter:
    """
    Простой лимитер: гарантирует минимальный интервал между запросами.
    """
    def __init__(self, rps: float) -> None:
        self.min_interval = 1.0 / max(rps, 0.001)
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def wait(self) -> None:
        async with self._lock:
            now = asyncio.get_event_loop().time()
            sleep_for = self.min_interval - (now - self._last)
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            self._last = asyncio.get_event_loop().time()
