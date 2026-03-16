# Настройка логирования приложения.
# RotatingFileHandler ограничивает размер лога (2 MB, 5 файлов), чтобы не заполнять диск при 24/7.
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logging(log_path: str) -> None:
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    handlers = [
        RotatingFileHandler(
            log_path,
            maxBytes=2 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ]
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=handlers)
