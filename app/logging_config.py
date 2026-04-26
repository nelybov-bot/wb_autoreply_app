# Настройка логирования приложения.
# RotatingFileHandler: 2 MB × 5 файлов. Формат: локальное время с миллисекундами, одна строка на событие.
from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


class AppLogFormatter(logging.Formatter):
    """Одна строка на запись; traceback — сжатый, без простыней в логе."""

    def formatException(self, ei) -> str:
        text = super().formatException(ei)
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        joined = " → ".join(lines)
        if len(joined) > 1200:
            return joined[:1200] + "…"
        return joined

    def format(self, record: logging.LogRecord) -> str:
        s = super().format(record)
        if "\n" in s:
            s = s.replace("\n", " ↳ ")
        return s


def setup_logging(log_path: str) -> None:
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)

    fmt = "%(asctime)s.%(msecs)03d | %(levelname)-5s | %(name)-18s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = AppLogFormatter(fmt, datefmt=datefmt)

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=2 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Убираем дубли и старые обработчики при повторном вызове (web + uvicorn)
    root.handlers.clear()
    root.addHandler(file_handler)
    root.addHandler(stream_handler)

    # Меньше шума от библиотек
    for name in (
        "aiohttp.access",
        "aiohttp.client",
        "asyncio",
        "httpx",
        "httpcore",
        "urllib3",
        "multipart",
    ):
        logging.getLogger(name).setLevel(logging.WARNING)

    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
