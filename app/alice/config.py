"""Конфигурация модуля Alice Warehouse из environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class AliceWarehouseConfig:
    tms_base_url: str
    tms_api_token: str
    tms_operator: str
    dry_run: bool
    max_stock_change: int
    session_ttl_seconds: int
    pending_ttl_seconds: int
    dedup_ttl_seconds: int
    skill_id: str
    webhook_secret: str


def load_config() -> AliceWarehouseConfig:
    return AliceWarehouseConfig(
        tms_base_url=(os.getenv("TMS_BASE_URL") or "https://tmsdesk.ru").strip().rstrip("/"),
        tms_api_token=(os.getenv("TMS_API_TOKEN") or "").strip(),
        tms_operator=(os.getenv("TMS_OPERATOR") or "Alice-Warehouse").strip()[:64] or "Alice-Warehouse",
        dry_run=_env_bool("ALICE_WAREHOUSE_DRY_RUN", True),
        max_stock_change=max(1, _env_int("ALICE_MAX_STOCK_CHANGE", 1000)),
        session_ttl_seconds=max(60, _env_int("ALICE_SESSION_TTL_SECONDS", 1800)),
        pending_ttl_seconds=max(10, _env_int("ALICE_PENDING_TTL_SECONDS", 60)),
        dedup_ttl_seconds=max(30, _env_int("ALICE_DEDUP_TTL_SECONDS", 300)),
        skill_id=(os.getenv("ALICE_SKILL_ID") or "").strip(),
        webhook_secret=(os.getenv("ALICE_WEBHOOK_SECRET") or "").strip(),
    )
