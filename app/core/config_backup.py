"""Экспорт и импорт настроек приложения (магазины, ключи, Telegram, расписание, промпты)."""
from __future__ import annotations

import datetime as dt
import json
from typing import Any, Optional

from app.db import Database
from app.core.secret_mask import MASK_BULLETS, SECRET_SETTING_KEYS, is_masked_display
from app.core.telegram_notify import normalize_telegram_bot_token

BACKUP_VERSION = 1
APP_NAME = "wb_autoreply"

# Ключи, которые не переносим — служебные метки времени на конкретном сервере.
RUNTIME_SETTING_KEYS = frozenset({
    "auto_schedule_last_run_at",
    "telegram_report_last_sent",
    "telegram_agent_update_offset",
})

# Все настройки, которые обычно задаются через веб-интерфейс.
SETTING_KEYS = (
    "openai_key",
    "telegram_bot_token",
    "telegram_chat_id",
    "telegram_report_chat_id",
    "telegram_card_error_chat_id",
    "telegram_enabled",
    "telegram_report_enabled",
    "telegram_report_interval",
    "telegram_agent_enabled",
    "telegram_agent_chat_id",
    "telegram_agent_user_id",
    "card_check_enabled",
    "card_check_telegram_enabled",
    "card_check_include_in_periodic_report",
    "card_check_telegram_template",
    "ozon_alerts_enabled",
    "ozon_alerts_telegram_enabled",
    "ozon_alerts_check_from_date",
    "ozon_alerts_telegram_template",
    "ozon_alerts_telegram_chat_id",
    "theme",
    "buyer_chat_reply_from_date",
    "buyer_chat_auto_max_age_days",
)


def store_key(marketplace: str, name: str) -> str:
    return f"{(marketplace or '').strip().lower()}:{(name or '').strip()}"


def parse_store_key(key: str) -> tuple[str, str]:
    mp, _, name = (key or "").partition(":")
    return mp.strip().lower(), name.strip()


def _store_to_export(s) -> dict:
    has_key = bool((s.api_key or "").strip())
    return {
        "marketplace": s.marketplace,
        "name": s.name,
        "api_key": "",
        "active": bool(s.active),
        "business_id": s.business_id,
        "client_id": s.client_id or "",
        "api_key_set": has_key,
        "api_key_redacted": has_key,
    }


def _build_store_id_map(db: Database) -> dict[str, int]:
    return {store_key(s.marketplace, s.name): s.id for s in db.list_stores()}


def _export_auto_schedule(db: Database, id_to_key: dict[int, str]) -> dict:
    raw = (db.get_setting("auto_schedule_json") or "").strip()
    cfg: dict = {
        "enabled": False,
        "slots": [],
        "store_keys": [],
        "schedule_mode": "slots",
        "interval_hours": 1,
        "run_reviews_wb": True,
        "run_reviews_yam": True,
        "run_reviews_ozon": False,
        "run_questions_wb": True,
        "run_questions_yam": True,
        "run_questions_ozon": True,
        "run_wb_chats": False,
        "run_ozon_chats": False,
        "run_ozon_alerts": False,
        "run_ozon_actions_remove": False,
    }
    if not raw:
        return cfg
    try:
        obj = json.loads(raw)
    except Exception:
        return cfg
    store_keys: list[str] = []
    for sid in obj.get("store_ids") or []:
        try:
            key = id_to_key.get(int(sid))
        except (TypeError, ValueError):
            continue
        if key:
            store_keys.append(key)
    cfg["enabled"] = bool(obj.get("enabled"))
    cfg["slots"] = list(obj.get("slots") or [])
    cfg["store_keys"] = store_keys
    mode = str(obj.get("schedule_mode") or "slots").strip().lower()
    cfg["schedule_mode"] = mode if mode in ("slots", "interval") else "slots"
    try:
        cfg["interval_hours"] = max(1, min(int(obj.get("interval_hours") or 1), 24))
    except (TypeError, ValueError):
        cfg["interval_hours"] = 1
    _mp_keys = (
        "run_reviews_wb", "run_reviews_yam", "run_reviews_ozon",
        "run_questions_wb", "run_questions_yam", "run_questions_ozon",
    )
    if not any(k in obj for k in _mp_keys[:3]):
        legacy_r = bool(obj.get("run_reviews", True))
        legacy_q = bool(obj.get("run_questions", True))
        ozon_r = bool(obj.get("run_ozon_reviews", False)) if "run_ozon_reviews" in obj else False
        cfg["run_reviews_wb"] = legacy_r
        cfg["run_reviews_yam"] = legacy_r
        cfg["run_reviews_ozon"] = ozon_r
        cfg["run_questions_wb"] = legacy_q
        cfg["run_questions_yam"] = legacy_q
        cfg["run_questions_ozon"] = legacy_q
    else:
        cfg["run_reviews_wb"] = bool(obj.get("run_reviews_wb", True))
        cfg["run_reviews_yam"] = bool(obj.get("run_reviews_yam", True))
        cfg["run_reviews_ozon"] = bool(obj.get("run_reviews_ozon", False))
        cfg["run_questions_wb"] = bool(obj.get("run_questions_wb", True))
        cfg["run_questions_yam"] = bool(obj.get("run_questions_yam", True))
        cfg["run_questions_ozon"] = bool(obj.get("run_questions_ozon", True))
    cfg["run_wb_chats"] = bool(obj.get("run_wb_chats", False))
    cfg["run_ozon_chats"] = bool(obj.get("run_ozon_chats", False))
    cfg["run_ozon_alerts"] = bool(obj.get("run_ozon_alerts", False))
    cfg["run_ozon_actions_remove"] = bool(obj.get("run_ozon_actions_remove", False))
    return cfg


def _export_ozon_actions(db: Database, id_to_key: dict[int, str]) -> dict:
    raw = (db.get_setting("ozon_actions_settings_json") or "").strip()
    cfg: dict = {"auto_remove_on_schedule": False, "only_auto_add": True, "stores": {}}
    if not raw:
        return cfg
    try:
        obj = json.loads(raw)
    except Exception:
        return cfg
    cfg["auto_remove_on_schedule"] = bool(obj.get("auto_remove_on_schedule"))
    cfg["only_auto_add"] = bool(obj.get("only_auto_add", True))
    stores_out: dict[str, dict] = {}
    stores_in = obj.get("stores") or {}
    if isinstance(stores_in, dict):
        for sid_s, ent in stores_in.items():
            try:
                sid = int(sid_s)
            except (TypeError, ValueError):
                continue
            key = id_to_key.get(sid)
            if not key or not isinstance(ent, dict):
                continue
            watched: list[int] = []
            for x in ent.get("watched_action_ids") or []:
                try:
                    watched.append(int(x))
                except (TypeError, ValueError):
                    continue
            exclude: list[int] = []
            for x in ent.get("exclude_action_ids") or []:
                try:
                    exclude.append(int(x))
                except (TypeError, ValueError):
                    continue
            stores_out[key] = {
                "watched_action_ids": watched,
                "sync_mode": ent.get("sync_mode"),
                "discount_threshold_percent": ent.get("discount_threshold_percent"),
                "sync_enable_remove": ent.get("sync_enable_remove"),
                "sync_enable_add": ent.get("sync_enable_add"),
                "exclude_voucher_actions": ent.get("exclude_voucher_actions"),
                "exclude_action_ids": exclude,
            }
    cfg["stores"] = stores_out
    return cfg


def export_config(db: Database) -> dict:
    stores = db.list_stores()
    id_to_key = {s.id: store_key(s.marketplace, s.name) for s in stores}
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    settings = {}
    for k in SETTING_KEYS:
        raw = db.get_setting(k) or ""
        if k in SECRET_SETTING_KEYS:
            settings[k] = ""
            settings[f"{k}_set"] = bool(raw.strip())
            settings[f"{k}_redacted"] = bool(raw.strip())
        else:
            settings[k] = raw
    prompts = [
        {
            "item_type": p.item_type,
            "rating_group": p.rating_group,
            "prompt_text": p.prompt_text,
        }
        for p in db.list_prompts()
    ]
    return {
        "version": BACKUP_VERSION,
        "app": APP_NAME,
        "exported_at": now,
        "secrets_included": False,
        "stores": [_store_to_export(s) for s in stores],
        "settings": settings,
        "auto_schedule": _export_auto_schedule(db, id_to_key),
        "ozon_actions": _export_ozon_actions(db, id_to_key),
        "prompts": prompts,
    }


def _upsert_store_from_backup(db: Database, row: dict) -> tuple[str, bool]:
    """Возвращает (store_key, created)."""
    mp = (row.get("marketplace") or "").strip().lower()
    name = (row.get("name") or "").strip()
    api_key = (row.get("api_key") or "").strip()
    active = bool(row.get("active", True))
    if mp not in ("wb", "yam", "ozon") or not name:
        raise ValueError(f"Некорректный магазин: {row!r}")
    key = store_key(mp, name)
    existing = {store_key(s.marketplace, s.name): s for s in db.list_stores()}
    if key in existing:
        s = existing[key]
        if row.get("api_key_redacted") or row.get("api_key_set") or api_key == MASK_BULLETS or not api_key:
            api_key = (s.api_key or "").strip()
        business_id = row.get("business_id")
        if business_id is not None and business_id != "":
            try:
                business_id = int(business_id)
            except (TypeError, ValueError):
                business_id = s.business_id
        else:
            business_id = s.business_id
        client_id = (row.get("client_id") or "").strip() or (s.client_id or "")
        db.update_store(s.id, name, api_key, active, business_id=business_id, client_id=client_id)
        return key, False
    if mp == "wb":
        db.upsert_store_wb(name, api_key, active)
    elif mp == "yam":
        bid = row.get("business_id")
        if bid is None or bid == "":
            raise ValueError(f"Для Яндекс.Маркета нужен business_id: {name}")
        db.upsert_store_yam(name, api_key, int(bid), active)
    else:
        client_id = (row.get("client_id") or "").strip()
        if not client_id:
            raise ValueError(f"Для Ozon нужен client_id: {name}")
        db.upsert_store_ozon(name, api_key, client_id, active)
    return key, True


def _import_settings(db: Database, settings: dict) -> int:
    n = 0
    if not isinstance(settings, dict):
        return n
    for k, v in settings.items():
        key = str(k).strip()
        if not key or key in RUNTIME_SETTING_KEYS or key.endswith("_redacted") or key.endswith("_set"):
            continue
        val = str(v) if v is not None else ""
        if key in SECRET_SETTING_KEYS:
            if settings.get(f"{key}_redacted") or settings.get(f"{key}_set") or val == MASK_BULLETS or is_masked_display(val) or not val.strip():
                continue
        if key == "telegram_bot_token":
            val = normalize_telegram_bot_token(val)
        db.set_setting(key, val)
        n += 1
    return n


def _import_auto_schedule(db: Database, data: dict, key_to_id: dict[str, int]) -> None:
    if not isinstance(data, dict):
        return
    store_ids: list[int] = []
    for sk in data.get("store_keys") or []:
        sid = key_to_id.get(str(sk).strip())
        if sid is not None:
            store_ids.append(int(sid))
    mode = str(data.get("schedule_mode") or "slots").strip().lower()
    if mode not in ("slots", "interval"):
        mode = "slots"
    try:
        interval_hours = max(1, min(int(data.get("interval_hours") or 1), 24))
    except (TypeError, ValueError):
        interval_hours = 1
    cfg = {
        "enabled": bool(data.get("enabled")),
        "slots": list(data.get("slots") or []),
        "store_ids": store_ids,
        "schedule_mode": mode,
        "interval_hours": interval_hours,
        "run_reviews_wb": bool(data.get("run_reviews_wb", True)),
        "run_reviews_yam": bool(data.get("run_reviews_yam", True)),
        "run_reviews_ozon": bool(data.get("run_reviews_ozon", False)),
        "run_questions_wb": bool(data.get("run_questions_wb", True)),
        "run_questions_yam": bool(data.get("run_questions_yam", True)),
        "run_questions_ozon": bool(data.get("run_questions_ozon", True)),
        "run_wb_chats": bool(data.get("run_wb_chats", False)),
        "run_ozon_chats": bool(data.get("run_ozon_chats", False)),
        "run_ozon_alerts": bool(data.get("run_ozon_alerts", False)),
        "run_ozon_actions_remove": bool(data.get("run_ozon_actions_remove", False)),
    }
    db.set_setting("auto_schedule_json", json.dumps(cfg, ensure_ascii=False))


def _import_ozon_actions(db: Database, data: dict, key_to_id: dict[str, int]) -> None:
    if not isinstance(data, dict):
        return
    stores_out: dict[str, dict] = {}
    stores_in = data.get("stores") or {}
    if isinstance(stores_in, dict):
        for sk, ent in stores_in.items():
            sid = key_to_id.get(str(sk).strip())
            if sid is None or not isinstance(ent, dict):
                continue
            watched: list[int] = []
            for x in ent.get("watched_action_ids") or []:
                try:
                    watched.append(int(x))
                except (TypeError, ValueError):
                    continue
            exclude: list[int] = []
            for x in ent.get("exclude_action_ids") or []:
                try:
                    exclude.append(int(x))
                except (TypeError, ValueError):
                    continue
            mode = str(ent.get("sync_mode") or "discount_threshold").strip()
            if mode not in ("discount_threshold", "legacy_auto_remove"):
                mode = "discount_threshold"
            try:
                threshold = float(ent.get("discount_threshold_percent", 3.0))
            except (TypeError, ValueError):
                threshold = 3.0
            stores_out[str(int(sid))] = {
                "watched_action_ids": watched,
                "sync_mode": mode,
                "discount_threshold_percent": max(0.0, min(threshold, 99.0)),
                "sync_enable_remove": bool(ent.get("sync_enable_remove", True)),
                "sync_enable_add": bool(ent.get("sync_enable_add", True)),
                "exclude_voucher_actions": bool(ent.get("exclude_voucher_actions", False)),
                "exclude_action_ids": exclude,
            }
    cfg = {
        "auto_remove_on_schedule": bool(data.get("auto_remove_on_schedule")),
        "only_auto_add": bool(data.get("only_auto_add", True)),
        "stores": stores_out,
    }
    db.set_setting("ozon_actions_settings_json", json.dumps(cfg, ensure_ascii=False))


def _import_prompts(db: Database, prompts: list) -> tuple[int, int]:
    updated = 0
    added = 0
    if not isinstance(prompts, list):
        return updated, added
    existing = {(p.item_type, p.rating_group): p for p in db.list_prompts()}
    for row in prompts:
        if not isinstance(row, dict):
            continue
        item_type = (row.get("item_type") or "").strip()
        rating_group = (row.get("rating_group") or "").strip()
        prompt_text = str(row.get("prompt_text") or "")
        if not item_type or not rating_group:
            continue
        hit = existing.get((item_type, rating_group))
        if hit:
            db.update_prompt(hit.id, prompt_text)
            updated += 1
        else:
            db.add_prompt(item_type, rating_group, prompt_text)
            added += 1
    return updated, added


def validate_backup_payload(data: Any) -> Optional[str]:
    if not isinstance(data, dict):
        return "Ожидается JSON-объект"
    if int(data.get("version") or 0) != BACKUP_VERSION:
        return f"Неподдерживаемая версия файла (нужна {BACKUP_VERSION})"
    if (data.get("app") or APP_NAME) != APP_NAME:
        return "Файл не от этого приложения"
    if "stores" not in data:
        return "В файле нет списка магазинов"
    return None


def import_config(db: Database, data: dict) -> dict:
    err = validate_backup_payload(data)
    if err:
        raise ValueError(err)

    stores_created = 0
    stores_updated = 0
    store_errors: list[str] = []

    for row in data.get("stores") or []:
        try:
            _, created = _upsert_store_from_backup(db, row)
            if created:
                stores_created += 1
            else:
                stores_updated += 1
        except ValueError as e:
            store_errors.append(str(e))

    key_to_id = _build_store_id_map(db)
    settings_count = _import_settings(db, data.get("settings") or {})
    _import_auto_schedule(db, data.get("auto_schedule") or {}, key_to_id)
    _import_ozon_actions(db, data.get("ozon_actions") or {}, key_to_id)
    prompts_updated, prompts_added = _import_prompts(db, data.get("prompts") or [])

    return {
        "ok": True,
        "stores_created": stores_created,
        "stores_updated": stores_updated,
        "store_errors": store_errors,
        "settings_count": settings_count,
        "prompts_updated": prompts_updated,
        "prompts_added": prompts_added,
    }
