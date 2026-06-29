"""
SQLite слой: хранение магазинов, элементов (отзывы/вопросы), промптов.

Особенности:
- check_same_thread=False + глобальный RLock, чтобы безопасно использовать БД из фонового потока.
- Все операции БД должны проходить через методы класса Database.
"""
from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple
import datetime as dt
import json

_DB_LOCK = threading.RLock()


def utc_now_iso() -> str:
    """Единый формат меток времени в БД (UTC, ISO)."""
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def iso_to_unix(iso: str) -> float:
    """Парсинг ISO-даты для сравнения в отчётах (учитывает смещение TZ)."""
    if iso is None:
        return 0.0
    s = (iso or "").strip()
    if not s:
        return 0.0
    try:
        d = dt.datetime.fromisoformat(s)
    except ValueError:
        return 0.0
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.timestamp()


def _empty_activity_stats() -> dict:
    return {
        "reviews_sent": 0,
        "questions_sent": 0,
        "ozon_products_removed": 0,
        "ozon_products_added": 0,
        "ozon_promo_kept": 0,
        "ozon_promo_skipped_data": 0,
        "wb_chat_replies": 0,
        "ozon_chat_replies": 0,
        "chat_replies_total": 0,
        "card_errors": 0,
        "ozon_alerts": 0,
        "ozon_cert_requests_products": 0,
        "ozon_hidden_products": 0,
        "ozon_threat_hide_products": 0,
        "ozon_threat_fine_products": 0,
        "ozon_threat_fine_by_amount": {},
        "reviews_by_rating": {},
    }


@dataclass(frozen=True)
class Store:
    id: int
    marketplace: str
    name: str
    api_key: str
    active: bool
    business_id: Optional[int] = None
    client_id: Optional[str] = None

@dataclass(frozen=True)
class PromptRow:
    id: int
    item_type: str   # 'review' | 'question'
    rating_group: str
    prompt_text: str

@dataclass(frozen=True)
class ItemRow:
    id: int
    store_id: int
    external_id: str
    item_type: str   # 'review'|'question'
    date: str        # ISO
    rating: Optional[int]
    text: str
    author: str
    product_title: str
    status: str      # 'new'|'generated'|'sent'|'ignored'
    generated_text: str
    was_viewed: bool
    extra_json: str = ""  # JSON для marketplace-specific данных (напр. Ozon sku)

@dataclass(frozen=True)
class UserRow:
    id: int
    username: str
    password_hash: str
    role: str  # 'admin' | 'guest'

@dataclass(frozen=True)
class AuditEventRow:
    id: int
    ts: str
    actor: str
    action: str
    item_type: str
    store_id: Optional[int]
    result: str
    meta_json: str


@dataclass(frozen=True)
class OzonImportantAlertRow:
    id: int
    ts: str
    store_id: int
    chat_id: str
    message_id: str
    chat_type: str
    message_at: str
    message_text: str
    threat_type: str
    amount: str
    product_ref: str
    summary: str
    action_needed: str
    status: str
    telegram_sent: bool
    alert_category: str = ""
    product_skus: str = ""


@dataclass(frozen=True)
class CardErrorAlertRow:
    id: int
    ts: str
    store_id: int
    source_type: str
    source_ref: str
    product_title: str
    customer_text: str
    error_kind: str
    explanation: str
    status: str
    telegram_sent: bool

class Database:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.create_function("iso_to_unix", 1, iso_to_unix)
        self._migrate()

    def close(self) -> None:
        with _DB_LOCK:
            self._conn.close()

    def _migrate(self) -> None:
        with _DB_LOCK:
            c = self._conn.cursor()
            c.execute("""
            CREATE TABLE IF NOT EXISTS stores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                marketplace TEXT NOT NULL,
                name TEXT NOT NULL,
                client_id TEXT,
                api_key TEXT NOT NULL,
                business_id TEXT,
                active INTEGER NOT NULL DEFAULT 1
            )
            """)
            c.execute("""
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                store_id INTEGER NOT NULL,
                external_id TEXT NOT NULL,
                item_type TEXT NOT NULL,
                date TEXT NOT NULL,
                rating INTEGER,
                text TEXT,
                author TEXT,
                product_title TEXT,
                status TEXT NOT NULL,
                generated_text TEXT,
                sent_at TEXT,
                was_viewed INTEGER NOT NULL DEFAULT 0,
                UNIQUE(store_id, item_type, external_id)
            )
            """)
            c.execute("""
            CREATE TABLE IF NOT EXISTS prompts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_type TEXT NOT NULL,
                rating_group TEXT NOT NULL,
                prompt_text TEXT NOT NULL
            )
            """)
            c.execute("""
            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            )
            """)
            c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'guest'
            )
            """)
            c.execute("""
            CREATE TABLE IF NOT EXISTS user_permissions (
                user_id INTEGER NOT NULL,
                permission TEXT NOT NULL,
                PRIMARY KEY (user_id, permission),
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """)
            c.execute("""
            CREATE TABLE IF NOT EXISTS audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                actor TEXT NOT NULL,
                action TEXT NOT NULL,
                item_type TEXT NOT NULL DEFAULT '',
                store_id INTEGER,
                result TEXT NOT NULL DEFAULT '',
                meta_json TEXT NOT NULL DEFAULT ''
            )
            """)
            c.execute("CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_events(ts)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_events(action)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_audit_actor ON audit_events(actor)")
            # Индексы
            c.execute("CREATE INDEX IF NOT EXISTS idx_items_status ON items(status)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_items_type ON items(item_type)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_items_store ON items(store_id)")
            # Lazy migration: extra_json для Ozon sku и др.
            info_items = c.execute("PRAGMA table_info(items)").fetchall()
            if "extra_json" not in [row[1] for row in info_items]:
                c.execute("ALTER TABLE items ADD COLUMN extra_json TEXT")
            if "send_error" not in [row[1] for row in info_items]:
                c.execute("ALTER TABLE items ADD COLUMN send_error TEXT")
            # Зависшие sending после рестарта → generated (ответ сохранён)
            c.execute(
                """UPDATE items SET status='generated', send_error=COALESCE(send_error,'')
                   WHERE status='sending' AND trim(COALESCE(generated_text,'')) <> ''"""
            )
            c.execute(
                "UPDATE items SET status='new', send_error='' WHERE status='sending' AND trim(COALESCE(generated_text,'')) = ''"
            )
            # Lazy migration: client_id в stores (для Ozon)
            info_stores = c.execute("PRAGMA table_info(stores)").fetchall()
            if "client_id" not in [row[1] for row in info_stores]:
                c.execute("ALTER TABLE stores ADD COLUMN client_id TEXT")
            c.execute("""
                CREATE TABLE IF NOT EXISTS ozon_sku_cache (
                    sku INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS buyer_chat_replies (
                    store_id INTEGER NOT NULL,
                    marketplace TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    client_message_key TEXT NOT NULL,
                    replied_at TEXT NOT NULL,
                    PRIMARY KEY (store_id, marketplace, chat_id, client_message_key)
                )
            """)
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_buyer_chat_replies_store "
                "ON buyer_chat_replies(store_id, marketplace)"
            )
            c.execute("""
                CREATE TABLE IF NOT EXISTS card_error_alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    store_id INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    source_ref TEXT NOT NULL,
                    product_title TEXT NOT NULL DEFAULT '',
                    customer_text TEXT NOT NULL DEFAULT '',
                    error_kind TEXT NOT NULL DEFAULT '',
                    explanation TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'new',
                    telegram_sent INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(store_id, source_type, source_ref)
                )
            """)
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_card_errors_ts ON card_error_alerts(ts)"
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_card_errors_status ON card_error_alerts(status)"
            )
            c.execute("""
                CREATE TABLE IF NOT EXISTS ozon_important_alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    store_id INTEGER NOT NULL,
                    chat_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    chat_type TEXT NOT NULL DEFAULT '',
                    message_at TEXT NOT NULL DEFAULT '',
                    message_text TEXT NOT NULL DEFAULT '',
                    threat_type TEXT NOT NULL DEFAULT '',
                    amount TEXT NOT NULL DEFAULT '',
                    product_ref TEXT NOT NULL DEFAULT '',
                    summary TEXT NOT NULL DEFAULT '',
                    action_needed TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'new',
                    telegram_sent INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(store_id, chat_id, message_id)
                )
            """)
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_ozon_alerts_ts ON ozon_important_alerts(ts)"
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_ozon_alerts_status ON ozon_important_alerts(status)"
            )
            c.execute("""
                CREATE TABLE IF NOT EXISTS card_links_master_items (
                    store_id INTEGER NOT NULL,
                    nm_id INTEGER NOT NULL,
                    vendor_code TEXT NOT NULL DEFAULT '',
                    title TEXT NOT NULL DEFAULT '',
                    subject_id INTEGER NOT NULL DEFAULT 0,
                    subject_name TEXT NOT NULL DEFAULT '',
                    parent_name TEXT NOT NULL DEFAULT '',
                    imt_id INTEGER NOT NULL DEFAULT 0,
                    linked INTEGER NOT NULL DEFAULT 0,
                    brand TEXT NOT NULL DEFAULT '',
                    segment TEXT NOT NULL DEFAULT '',
                    subtype TEXT NOT NULL DEFAULT '',
                    phone_model TEXT NOT NULL DEFAULT '',
                    bundle_id TEXT NOT NULL DEFAULT '',
                    group_key TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    row_json TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (store_id, nm_id)
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS card_links_master_bundles (
                    store_id INTEGER NOT NULL,
                    bundle_id TEXT NOT NULL,
                    segment TEXT NOT NULL DEFAULT '',
                    category_label TEXT NOT NULL DEFAULT '',
                    brand TEXT NOT NULL DEFAULT '',
                    subtype_label TEXT NOT NULL DEFAULT '',
                    item_count INTEGER NOT NULL DEFAULT 0,
                    target_imt INTEGER NOT NULL DEFAULT 0,
                    sort_size INTEGER NOT NULL DEFAULT 0,
                    bundle_json TEXT NOT NULL DEFAULT '',
                    apply_status TEXT NOT NULL DEFAULT 'pending',
                    updated_at TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (store_id, bundle_id)
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS card_links_master_state (
                    store_id INTEGER PRIMARY KEY,
                    steps_json TEXT NOT NULL DEFAULT '{}',
                    catalog_at TEXT NOT NULL DEFAULT '',
                    log_json TEXT NOT NULL DEFAULT '[]',
                    updated_at TEXT NOT NULL DEFAULT ''
                )
            """)
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_clm_items_store_seg "
                "ON card_links_master_items(store_id, segment)"
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_clm_items_store_bundle "
                "ON card_links_master_items(store_id, bundle_id)"
            )
            for col, ddl in (
                ("alert_category", "TEXT NOT NULL DEFAULT ''"),
                ("product_skus", "TEXT NOT NULL DEFAULT ''"),
            ):
                try:
                    c.execute(f"ALTER TABLE ozon_important_alerts ADD COLUMN {col} {ddl}")
                except sqlite3.OperationalError:
                    pass
            self._conn.commit()
            self._seed_prompts_if_empty()
            self._seed_missing_prompts()

    # ---------- Users ----------
    def count_users(self) -> int:
        with _DB_LOCK:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()
            return int(row["n"]) if row else 0

    def get_user_by_username(self, username: str) -> Optional[UserRow]:
        u = (username or "").strip()
        if not u:
            return None
        with _DB_LOCK:
            row = self._conn.execute(
                "SELECT id, username, password_hash, role FROM users WHERE username=?",
                (u,),
            ).fetchone()
            if not row:
                return None
            return UserRow(
                id=int(row["id"]),
                username=str(row["username"]),
                password_hash=str(row["password_hash"]),
                role=str(row["role"] or "guest"),
            )

    def list_users(self) -> list[UserRow]:
        with _DB_LOCK:
            rows = self._conn.execute(
                "SELECT id, username, password_hash, role FROM users ORDER BY id DESC"
            ).fetchall()
            return [
                UserRow(
                    id=int(r["id"]),
                    username=str(r["username"]),
                    password_hash=str(r["password_hash"]),
                    role=str(r["role"] or "guest"),
                )
                for r in rows
            ]

    def create_user(self, username: str, password_hash: str, role: str = "guest") -> int:
        u = (username or "").strip()
        if not u:
            raise ValueError("username пустой")
        r = (role or "guest").strip() or "guest"
        with _DB_LOCK:
            cur = self._conn.execute(
                "INSERT INTO users(username, password_hash, role) VALUES(?,?,?)",
                (u, password_hash, r),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def delete_user(self, user_id: int) -> None:
        with _DB_LOCK:
            self._conn.execute("DELETE FROM users WHERE id=?", (int(user_id),))
            self._conn.commit()

    def update_user_password(self, username: str, password_hash: str) -> None:
        u = (username or "").strip()
        if not u:
            raise ValueError("username пустой")
        with _DB_LOCK:
            self._conn.execute(
                "UPDATE users SET password_hash=? WHERE username=?",
                (password_hash, u),
            )
            self._conn.commit()

    def get_user_permissions(self, user_id: int) -> list[str]:
        with _DB_LOCK:
            rows = self._conn.execute(
                "SELECT permission FROM user_permissions WHERE user_id=?",
                (int(user_id),),
            ).fetchall()
            return [str(r["permission"]) for r in rows]

    def set_user_permissions(self, user_id: int, permissions: list[str]) -> None:
        with _DB_LOCK:
            self._conn.execute("DELETE FROM user_permissions WHERE user_id=?", (int(user_id),))
            for p in permissions:
                p = (p or "").strip()
                if p:
                    self._conn.execute(
                        "INSERT INTO user_permissions(user_id, permission) VALUES(?,?)",
                        (int(user_id), p),
                    )
            self._conn.commit()

    # ---------- Audit events (ops log) ----------
    def add_audit_event(
        self,
        *,
        actor: str,
        action: str,
        item_type: str = "",
        store_id: Optional[int] = None,
        result: str = "",
        meta: Optional[dict] = None,
    ) -> int:
        ts = utc_now_iso()
        meta_json = ""
        if meta is not None:
            try:
                meta_json = json.dumps(meta, ensure_ascii=False, separators=(",", ":"))
            except Exception:
                meta_json = ""
        with _DB_LOCK:
            cur = self._conn.execute(
                "INSERT INTO audit_events(ts, actor, action, item_type, store_id, result, meta_json) VALUES(?,?,?,?,?,?,?)",
                (ts, actor or "", action or "", item_type or "", store_id, result or "", meta_json),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def list_audit_events(
        self,
        *,
        action: Optional[str] = None,
        item_type: Optional[str] = None,
        store_id: Optional[int] = None,
        result: Optional[str] = None,
        q: Optional[str] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[AuditEventRow]:
        where = []
        params = []
        if action:
            where.append("action=?")
            params.append(action)
        if item_type:
            where.append("item_type=?")
            params.append(item_type)
        if store_id is not None:
            where.append("store_id=?")
            params.append(int(store_id))
        if result:
            where.append("result=?")
            params.append(result)
        if q:
            where.append("(actor LIKE ? OR meta_json LIKE ?)")
            like = f"%{q}%"
            params.extend([like, like])
        w = ("WHERE " + " AND ".join(where)) if where else ""
        safe_limit = max(1, min(int(limit), 500))
        safe_offset = max(0, int(offset))
        with _DB_LOCK:
            rows = self._conn.execute(
                f"SELECT id, ts, actor, action, item_type, store_id, result, meta_json FROM audit_events {w} ORDER BY id DESC LIMIT ? OFFSET ?",
                params + [safe_limit, safe_offset],
            ).fetchall()
            out = []
            for r in rows:
                out.append(
                    AuditEventRow(
                        id=int(r["id"]),
                        ts=str(r["ts"]),
                        actor=str(r["actor"] or ""),
                        action=str(r["action"] or ""),
                        item_type=str(r["item_type"] or ""),
                        store_id=(int(r["store_id"]) if r["store_id"] is not None else None),
                        result=str(r["result"] or ""),
                        meta_json=str(r["meta_json"] or ""),
                    )
                )
            return out

    def _seed_prompts_if_empty(self) -> None:
        with _DB_LOCK:
            cur = self._conn.execute("SELECT COUNT(*) AS n FROM prompts")
            n = int(cur.fetchone()["n"])
            if n > 0:
                return
            # Промпты дефолтные: короткие, без воды, на русском.
            prompts = [
                ("review","1","Ты отвечаешь на отзыв (1/5). Вежливо извинись за ситуацию и поблагодари за обратную связь. Сообщи что мы учитываем замечания и работаем над улучшениями. Русский, 2-4 предложения, без эмодзи. Не уточняй в чем проблема, просто извинись"),
                ("review","2","Ты отвечаешь на отзыв (2/5). Вежливо извинись за ситуацию и поблагодари за обратную связь. Сообщи что мы учитываем замечания и работаем над улучшениями. Русский, 2-4 предложения, без эмодзи. Не уточняй в чем проблема, просто извинись"),
                ("review","3","Ты отвечаешь на отзыв (3/5). Вежливо извинись за ситуацию и поблагодари за обратную связь. Сообщи что мы учитываем замечания и работаем над улучшениями. Русский, 2-4 предложения, без эмодзи. Не уточняй в чем проблема, просто извинись"),
                ("review","4-5","Ты отвечаешь на положительный отзыв (4-5/5). Поблагодари, коротко, 1-3 предложения. Русский, без эмодзи. Не повторять название товара в ответе. Не уточняй в чем проблема"),
                ("review","general","Ты отвечаешь на отзыв. Русский, 2-4 предложения, без эмодзи. Не повторять название товара в ответе. Не уточняй в чем проблема"),
                ("question","general","Ты отвечаешь на вопрос покупателя по товару. Учитывай название товара, но не повторяй его в ответе.  Русский, по делу, 1-4 предложения, без эмодзи, Если модель товара не подходит, то кратко пиши что не подойдет. Не проси покупателя писать еще раз для уточнения. Категорически нельзя никого винить в ошибке. Всегда все признаем и говорим что будем работать над улучшениями. Не предлагай компенсации или обращения в поддержку. Не уточняй в чем проблема, просто извинись. Не рекомендуй оформить замену "),
                ("buyer_chat", "general", "Ты отвечаешь покупателю в чате по товару. Учитывай контекст переписки. Русский, 2–4 предложения, без эмодзи. Не повторяй полное название товара. Не предлагай компенсации и обращения в поддержку. Не задавай вопросов покупателю."),
                ("card_check", "general", "Дополнительно проверь, указывает ли текст покупателя на вероятную ошибку в карточке товара на маркетплейсе: неверное описание, характеристики, комплектация, размерная сетка, совместимость, цвет или модель в названии. НЕ считай ошибкой карточки: доставку, упаковку, пересорт, брак при транспортировке, субъективное «не понравилось». card_error.suspected = true только при явном несоответствии карточки."),
            ]
            self._conn.executemany(
                "INSERT INTO prompts(item_type, rating_group, prompt_text) VALUES(?,?,?)",
                prompts
            )
            self._conn.commit()

    def _seed_missing_prompts(self) -> None:
        from app.core.card_check import DEFAULT_BUYER_CHAT_PROMPT, DEFAULT_CARD_CHECK_PROMPT

        from app.core.ozon_alerts import DEFAULT_PROMPT as DEFAULT_OZON_ALERT_PROMPT

        extra = [
            ("buyer_chat", "general", DEFAULT_BUYER_CHAT_PROMPT),
            ("card_check", "general", DEFAULT_CARD_CHECK_PROMPT),
            ("ozon_important_alert", "general", DEFAULT_OZON_ALERT_PROMPT),
        ]
        with _DB_LOCK:
            for item_type, rating_group, text in extra:
                row = self._conn.execute(
                    "SELECT id FROM prompts WHERE item_type=? AND rating_group=?",
                    (item_type, rating_group),
                ).fetchone()
                if not row:
                    self._conn.execute(
                        "INSERT INTO prompts(item_type, rating_group, prompt_text) VALUES(?,?,?)",
                        (item_type, rating_group, text),
                    )
            self._conn.commit()

    # ---------- Ozon important alerts ----------
    def has_recent_ozon_alert_telegram(
        self,
        store_id: int,
        alert_category: str,
        product_skus: str,
        *,
        hours: int = 24,
    ) -> bool:
        """Уже отправляли в Telegram похожий инцидент (магазин + категория + SKU)."""
        cat = (alert_category or "").strip().lower()
        if not cat:
            return False
        skus = [s.strip() for s in (product_skus or "").split(",") if s.strip()]
        if not skus:
            return False
        cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=max(1, int(hours)))
        cutoff_iso = cutoff.isoformat(timespec="seconds")
        with _DB_LOCK:
            rows = self._conn.execute(
                """SELECT product_skus FROM ozon_important_alerts
                   WHERE store_id=? AND alert_category=? AND telegram_sent=1
                     AND status!='ignored' AND ts >= ?
                   ORDER BY id DESC LIMIT 40""",
                (int(store_id), cat, cutoff_iso),
            ).fetchall()
        sku_set = set(skus)
        for row in rows:
            raw = row["product_skus"] if isinstance(row, dict) else row[0]
            other = {s.strip() for s in str(raw or "").split(",") if s.strip()}
            if sku_set & other:
                return True
        return False

    def list_ozon_alerts_pending_telegram(self, store_id: int, *, limit: int = 20) -> list[dict]:
        with _DB_LOCK:
            rows = self._conn.execute(
                """SELECT id, chat_type, message_at, message_text, threat_type, amount,
                          product_ref, summary, action_needed, alert_category, product_skus
                   FROM ozon_important_alerts
                   WHERE store_id=? AND telegram_sent=0 AND status='new'
                   ORDER BY id ASC LIMIT ?""",
                (int(store_id), int(limit)),
            ).fetchall()
        out: list[dict] = []
        for r in rows:
            out.append({k: r[k] for k in r.keys()})
        return out

    def has_ozon_important_alert(self, store_id: int, chat_id: str, message_id: str) -> bool:
        with _DB_LOCK:
            row = self._conn.execute(
                """SELECT 1 FROM ozon_important_alerts
                   WHERE store_id=? AND chat_id=? AND message_id=?""",
                (int(store_id), (chat_id or "").strip(), (message_id or "").strip()),
            ).fetchone()
            return row is not None

    def add_ozon_important_alert(
        self,
        *,
        store_id: int,
        chat_id: str,
        message_id: str,
        chat_type: str,
        message_at: str,
        message_text: str,
        threat_type: str,
        amount: str,
        product_ref: str,
        summary: str,
        action_needed: str,
        status: str = "new",
        alert_category: str = "",
        product_skus: str = "",
    ) -> int:
        st = (status or "new").strip().lower()
        if st not in ("new", "resolved", "ignored"):
            st = "new"
        ts = utc_now_iso()
        with _DB_LOCK:
            cur = self._conn.execute(
                """INSERT INTO ozon_important_alerts(
                       ts, store_id, chat_id, message_id, chat_type, message_at, message_text,
                       threat_type, amount, product_ref, summary, action_needed, status,
                       alert_category, product_skus, telegram_sent
                   ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)""",
                (
                    ts,
                    int(store_id),
                    (chat_id or "").strip(),
                    (message_id or "").strip(),
                    (chat_type or "").strip(),
                    (message_at or "").strip(),
                    (message_text or "").strip(),
                    (threat_type or "").strip(),
                    (amount or "").strip(),
                    (product_ref or "").strip(),
                    (summary or "").strip(),
                    (action_needed or "").strip(),
                    st,
                    (alert_category or "").strip(),
                    (product_skus or "").strip(),
                ),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def clear_ozon_ignored_alerts(self, store_id: int) -> int:
        """Удалить записи «не важно» — чтобы пересканировать сообщения заново."""
        with _DB_LOCK:
            cur = self._conn.execute(
                "DELETE FROM ozon_important_alerts WHERE store_id=? AND status='ignored'",
                (int(store_id),),
            )
            self._conn.commit()
            return int(cur.rowcount or 0)

    def mark_ozon_message_alert_processed(
        self,
        *,
        store_id: int,
        chat_id: str,
        message_id: str,
        chat_type: str = "",
        message_at: str = "",
        message_text: str = "",
    ) -> None:
        """Пометить сообщение как проверенное (не важное) — повторно не анализировать."""
        if self.has_ozon_important_alert(store_id, chat_id, message_id):
            return
        self.add_ozon_important_alert(
            store_id=store_id,
            chat_id=chat_id,
            message_id=message_id,
            chat_type=chat_type,
            message_at=message_at,
            message_text=message_text,
            threat_type="—",
            amount="—",
            product_ref="—",
            summary="(не важно)",
            action_needed="—",
            status="ignored",
        )

    def mark_ozon_important_alert_telegram_sent(self, alert_id: int) -> None:
        with _DB_LOCK:
            self._conn.execute(
                "UPDATE ozon_important_alerts SET telegram_sent=1 WHERE id=?",
                (int(alert_id),),
            )
            self._conn.commit()

    def update_ozon_important_alert_status(self, alert_id: int, status: str) -> bool:
        st = (status or "").strip().lower()
        if st not in ("new", "resolved", "ignored"):
            return False
        with _DB_LOCK:
            cur = self._conn.execute(
                "UPDATE ozon_important_alerts SET status=? WHERE id=?",
                (st, int(alert_id)),
            )
            self._conn.commit()
            return cur.rowcount > 0

    def list_ozon_alerts_for_product_report(self) -> list[dict]:
        """Все значимые уведомления Ozon для подсчёта уникальных SKU в отчётах."""
        with _DB_LOCK:
            rows = self._conn.execute(
                """SELECT store_id, ts, threat_type, amount, product_ref, summary, message_text,
                          alert_category, product_skus, status
                   FROM ozon_important_alerts
                   WHERE status IN ('new', 'resolved')
                   ORDER BY id ASC"""
            ).fetchall()
        out: list[dict] = []
        for r in rows:
            out.append(
                {
                    "store_id": int(r["store_id"]),
                    "ts": str(r["ts"] or ""),
                    "threat_type": str(r["threat_type"] or ""),
                    "amount": str(r["amount"] or ""),
                    "product_ref": str(r["product_ref"] or ""),
                    "summary": str(r["summary"] or ""),
                    "message_text": str(r["message_text"] or ""),
                    "alert_category": str(r["alert_category"] or ""),
                    "product_skus": str(r["product_skus"] or ""),
                    "status": str(r["status"] or ""),
                }
            )
        return out

    def list_ozon_important_alerts(
        self,
        *,
        store_id: Optional[int] = None,
        status: Optional[str] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[OzonImportantAlertRow]:
        where = []
        params: list = []
        if store_id is not None:
            where.append("store_id=?")
            params.append(int(store_id))
        if status:
            where.append("status=?")
            params.append(status.strip())
        w = ("WHERE " + " AND ".join(where)) if where else ""
        safe_limit = max(1, min(int(limit), 500))
        safe_offset = max(0, int(offset))
        with _DB_LOCK:
            rows = self._conn.execute(
                f"""SELECT id, ts, store_id, chat_id, message_id, chat_type, message_at, message_text,
                           threat_type, amount, product_ref, summary, action_needed, status, telegram_sent
                    FROM ozon_important_alerts {w}
                    ORDER BY id DESC LIMIT ? OFFSET ?""",
                params + [safe_limit, safe_offset],
            ).fetchall()
            return [
                OzonImportantAlertRow(
                    id=int(r["id"]),
                    ts=str(r["ts"] or ""),
                    store_id=int(r["store_id"]),
                    chat_id=str(r["chat_id"] or ""),
                    message_id=str(r["message_id"] or ""),
                    chat_type=str(r["chat_type"] or ""),
                    message_at=str(r["message_at"] or ""),
                    message_text=str(r["message_text"] or ""),
                    threat_type=str(r["threat_type"] or ""),
                    amount=str(r["amount"] or ""),
                    product_ref=str(r["product_ref"] or ""),
                    summary=str(r["summary"] or ""),
                    action_needed=str(r["action_needed"] or ""),
                    status=str(r["status"] or "new"),
                    telegram_sent=bool(r["telegram_sent"]),
                )
                for r in rows
            ]

    # ---------- Card error alerts ----------
    def has_card_error_alert(self, store_id: int, source_type: str, source_ref: str) -> bool:
        with _DB_LOCK:
            row = self._conn.execute(
                """SELECT 1 FROM card_error_alerts
                   WHERE store_id=? AND source_type=? AND source_ref=?""",
                (int(store_id), (source_type or "").strip(), (source_ref or "").strip()),
            ).fetchone()
            return row is not None

    def add_card_error_alert(
        self,
        *,
        store_id: int,
        source_type: str,
        source_ref: str,
        product_title: str,
        customer_text: str,
        error_kind: str,
        explanation: str,
    ) -> int:
        ts = utc_now_iso()
        with _DB_LOCK:
            cur = self._conn.execute(
                """INSERT INTO card_error_alerts(
                       ts, store_id, source_type, source_ref, product_title,
                       customer_text, error_kind, explanation, status, telegram_sent
                   ) VALUES(?,?,?,?,?,?,?,?, 'new', 0)""",
                (
                    ts,
                    int(store_id),
                    (source_type or "").strip(),
                    (source_ref or "").strip(),
                    (product_title or "").strip(),
                    (customer_text or "").strip(),
                    (error_kind or "").strip(),
                    (explanation or "").strip(),
                ),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def mark_card_error_telegram_sent(self, alert_id: int) -> None:
        with _DB_LOCK:
            self._conn.execute(
                "UPDATE card_error_alerts SET telegram_sent=1 WHERE id=?",
                (int(alert_id),),
            )
            self._conn.commit()

    def update_card_error_status(self, alert_id: int, status: str) -> bool:
        st = (status or "").strip().lower()
        if st not in ("new", "resolved"):
            return False
        with _DB_LOCK:
            cur = self._conn.execute(
                "UPDATE card_error_alerts SET status=? WHERE id=?",
                (st, int(alert_id)),
            )
            self._conn.commit()
            return cur.rowcount > 0

    def list_card_error_alerts(
        self,
        *,
        store_id: Optional[int] = None,
        status: Optional[str] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[CardErrorAlertRow]:
        where = []
        params: list = []
        if store_id is not None:
            where.append("store_id=?")
            params.append(int(store_id))
        if status:
            where.append("status=?")
            params.append(status.strip())
        w = ("WHERE " + " AND ".join(where)) if where else ""
        safe_limit = max(1, min(int(limit), 500))
        safe_offset = max(0, int(offset))
        with _DB_LOCK:
            rows = self._conn.execute(
                f"""SELECT id, ts, store_id, source_type, source_ref, product_title,
                           customer_text, error_kind, explanation, status, telegram_sent
                    FROM card_error_alerts {w}
                    ORDER BY id DESC LIMIT ? OFFSET ?""",
                params + [safe_limit, safe_offset],
            ).fetchall()
            return [
                CardErrorAlertRow(
                    id=int(r["id"]),
                    ts=str(r["ts"] or ""),
                    store_id=int(r["store_id"]),
                    source_type=str(r["source_type"] or ""),
                    source_ref=str(r["source_ref"] or ""),
                    product_title=str(r["product_title"] or ""),
                    customer_text=str(r["customer_text"] or ""),
                    error_kind=str(r["error_kind"] or ""),
                    explanation=str(r["explanation"] or ""),
                    status=str(r["status"] or "new"),
                    telegram_sent=bool(r["telegram_sent"]),
                )
                for r in rows
            ]

    def count_card_error_alerts_since(
        self,
        since_iso: str,
        until_iso: Optional[str] = None,
    ) -> int:
        since_u = iso_to_unix(since_iso)
        if since_u <= 0:
            return 0
        until_u = iso_to_unix(until_iso) if until_iso else None
        with _DB_LOCK:
            if until_u is not None and until_u > since_u:
                row = self._conn.execute(
                    """SELECT COUNT(*) AS n FROM card_error_alerts
                       WHERE iso_to_unix(ts) >= ? AND iso_to_unix(ts) < ?""",
                    (since_u, until_u),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT COUNT(*) AS n FROM card_error_alerts WHERE iso_to_unix(ts) >= ?",
                    (since_u,),
                ).fetchone()
            return int(row["n"]) if row else 0

    # ---------- Stores ----------
    def list_stores(self) -> list[Store]:
        with _DB_LOCK:
            rows = self._conn.execute(
                "SELECT id, marketplace, name, api_key, active, business_id, client_id FROM stores ORDER BY id DESC"
            ).fetchall()
            def _norm_key(k: str) -> str:
                return (k or "").strip().replace("\n", "").replace("\r", "").replace(" ", "")

            def _parse_business_id(v) -> Optional[int]:
                if v is None or v == "":
                    return None
                try:
                    return int(v)
                except (ValueError, TypeError):
                    return None

            def _norm_client_id(v) -> Optional[str]:
                s = (v or "").strip() or None
                return s if s else None

            return [
                Store(
                    int(r["id"]),
                    r["marketplace"],
                    r["name"],
                    _norm_key(str(r["api_key"] or "")),
                    bool(r["active"]),
                    _parse_business_id(r["business_id"]),
                    _norm_client_id(r["client_id"]),
                )
                for r in rows
            ]

    def upsert_store_wb(self, name: str, api_key: str, active: bool=True) -> int:
        key_clean = (api_key or "").strip().replace("\n", "").replace("\r", "").replace(" ", "")
        with _DB_LOCK:
            cur = self._conn.execute(
                "INSERT INTO stores(marketplace,name,api_key,active) VALUES('wb',?,?,?)",
                (name, key_clean, 1 if active else 0)
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def upsert_store_yam(self, name: str, api_key: str, business_id: int, active: bool = True) -> int:
        key_clean = (api_key or "").strip().replace("\n", "").replace("\r", "").replace(" ", "")
        with _DB_LOCK:
            cur = self._conn.execute(
                "INSERT INTO stores(marketplace,name,api_key,business_id,active) VALUES('yam',?,?,?,?)",
                (name, key_clean, str(business_id), 1 if active else 0),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def upsert_store_ozon(self, name: str, api_key: str, client_id: str, active: bool = True) -> int:
        key_clean = (api_key or "").strip().replace("\n", "").replace("\r", "").replace(" ", "")
        client_clean = (client_id or "").strip().replace("\n", "").replace("\r", "").replace(" ", "")
        with _DB_LOCK:
            cur = self._conn.execute(
                "INSERT INTO stores(marketplace,name,api_key,client_id,active) VALUES('ozon',?,?,?,?)",
                (name, key_clean, client_clean, 1 if active else 0),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def update_store(
        self,
        store_id: int,
        name: str,
        api_key: str,
        active: bool,
        *,
        business_id: Optional[int] = None,
        client_id: Optional[str] = None,
    ) -> None:
        key_clean = (api_key or "").strip().replace("\n", "").replace("\r", "").replace(" ", "")
        with _DB_LOCK:
            cur_row = self._conn.execute(
                "SELECT business_id, client_id FROM stores WHERE id=?", (store_id,)
            ).fetchone()
            bid_val = str(business_id) if business_id is not None else (cur_row["business_id"] if cur_row else None)
            cid_val = (client_id or "").strip() if client_id is not None else (cur_row["client_id"] if cur_row else None)
            if cid_val is None:
                cid_val = ""
            self._conn.execute(
                "UPDATE stores SET name=?, api_key=?, active=?, business_id=?, client_id=? WHERE id=?",
                (name, key_clean, 1 if active else 0, bid_val, cid_val, store_id),
            )
            self._conn.commit()

    def delete_store(self, store_id: int) -> None:
        with _DB_LOCK:
            self._conn.execute("DELETE FROM stores WHERE id=?", (store_id,))
            self._conn.execute("DELETE FROM items WHERE store_id=?", (store_id,))
            self._conn.commit()

    def clear_items(self, store_ids: Optional[list[int]] = None, item_types: Optional[list[str]] = None) -> int:
        """
        Удаляет элементы (отзывы/вопросы) из items.
        - store_ids=None: очищает все items
        - store_ids=[...]: очищает только по указанным магазинам
        - item_types=["review"|"question", ...]: ограничивает по типам
        Возвращает количество удалённых строк.
        """
        with _DB_LOCK:
            where: list[str] = []
            params: list = []
            if store_ids is None:
                pass
            else:
                ids = [int(x) for x in (store_ids or [])]
                if not ids:
                    return 0
                placeholders = ",".join("?" * len(ids))
                where.append(f"store_id IN ({placeholders})")
                params.extend(ids)
            tps = [str(x).strip() for x in (item_types or []) if str(x).strip() in ("review", "question")]
            if tps:
                placeholders = ",".join("?" * len(tps))
                where.append(f"item_type IN ({placeholders})")
                params.extend(tps)
            sql = "DELETE FROM items"
            if where:
                sql += " WHERE " + " AND ".join(where)
            cur = self._conn.execute(sql, params)
            self._conn.commit()
            return int(cur.rowcount or 0)

    # ---------- Ozon SKU cache (названия товаров по sku) ----------
    def get_ozon_sku_names(self, skus: list[int]) -> dict[int, str]:
        """Возвращает {sku: name} только для тех sku, что есть в кэше."""
        if not skus:
            return {}
        with _DB_LOCK:
            placeholders = ",".join("?" * len(skus))
            rows = self._conn.execute(
                f"SELECT sku, name FROM ozon_sku_cache WHERE sku IN ({placeholders})",
                list(skus),
            ).fetchall()
            return {int(r["sku"]): str(r["name"]) for r in rows}

    def upsert_ozon_sku_names(self, names: dict[int, str]) -> None:
        """Записывает/обновляет пары sku->name в кэше."""
        if not names:
            return
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with _DB_LOCK:
            self._conn.executemany(
                "INSERT OR REPLACE INTO ozon_sku_cache(sku, name, updated_at) VALUES(?,?,?)",
                [(sku, name, now) for sku, name in names.items()],
            )
            self._conn.commit()

    # ---------- App settings (Telegram и т.д.) ----------
    def get_setting(self, key: str) -> str:
        with _DB_LOCK:
            row = self._conn.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
            return str(row["value"]) if row else ""

    def set_setting(self, key: str, value: str) -> None:
        with _DB_LOCK:
            self._conn.execute(
                "INSERT INTO app_settings(key, value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            self._conn.commit()

    # ---------- Buyer chat deduplication ----------
    def is_buyer_chat_replied(
        self,
        store_id: int,
        marketplace: str,
        chat_id: str,
        client_message_key: str,
    ) -> bool:
        mp = (marketplace or "").strip().lower()
        cid = (chat_id or "").strip()
        mk = (client_message_key or "").strip()
        if not mp or not cid or not mk:
            return False
        with _DB_LOCK:
            row = self._conn.execute(
                """SELECT 1 FROM buyer_chat_replies
                   WHERE store_id=? AND marketplace=? AND chat_id=? AND client_message_key=?""",
                (int(store_id), mp, cid, mk),
            ).fetchone()
            return row is not None

    def mark_buyer_chat_replied(
        self,
        store_id: int,
        marketplace: str,
        chat_id: str,
        client_message_key: str,
    ) -> None:
        mp = (marketplace or "").strip().lower()
        cid = (chat_id or "").strip()
        mk = (client_message_key or "").strip()
        if not mp or not cid or not mk:
            return
        now = dt.datetime.now().isoformat(timespec="seconds")
        with _DB_LOCK:
            self._conn.execute(
                """INSERT INTO buyer_chat_replies(store_id, marketplace, chat_id, client_message_key, replied_at)
                   VALUES(?,?,?,?,?)
                   ON CONFLICT(store_id, marketplace, chat_id, client_message_key) DO UPDATE SET replied_at=excluded.replied_at""",
                (int(store_id), mp, cid, mk, now),
            )
            self._conn.commit()

    # ---------- Prompts ----------
    def list_prompts(self) -> list[PromptRow]:
        with _DB_LOCK:
            rows = self._conn.execute(
                "SELECT id, item_type, rating_group, prompt_text FROM prompts ORDER BY item_type, rating_group"
            ).fetchall()
            return [
                PromptRow(
                    id=int(r["id"]),
                    item_type=str(r["item_type"]),
                    rating_group=str(r["rating_group"]),
                    prompt_text=str(r["prompt_text"] or ""),
                )
                for r in rows
            ]

    def get_prompt(self, item_type: str, rating_group: str) -> str:
        with _DB_LOCK:
            row = self._conn.execute(
                "SELECT prompt_text FROM prompts WHERE item_type=? AND rating_group=?",
                (item_type, rating_group)
            ).fetchone()
            if row:
                return str(row["prompt_text"])
            row = self._conn.execute(
                "SELECT prompt_text FROM prompts WHERE item_type=? AND rating_group='general'",
                (item_type,)
            ).fetchone()
            return str(row["prompt_text"]) if row else ""

    def update_prompt(self, prompt_id: int, prompt_text: str) -> None:
        with _DB_LOCK:
            self._conn.execute(
                "UPDATE prompts SET prompt_text=? WHERE id=?",
                (prompt_text, prompt_id)
            )
            self._conn.commit()

    def add_prompt(self, item_type: str, rating_group: str, prompt_text: str) -> int:
        with _DB_LOCK:
            cur = self._conn.execute(
                "INSERT INTO prompts(item_type, rating_group, prompt_text) VALUES(?,?,?)",
                (item_type, rating_group.strip(), prompt_text.strip())
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def delete_prompt(self, prompt_id: int) -> None:
        with _DB_LOCK:
            self._conn.execute("DELETE FROM prompts WHERE id=?", (prompt_id,))
            self._conn.commit()

    # ---------- Items ----------
    def upsert_item(
        self,
        store_id: int,
        external_id: str,
        item_type: str,
        date: str,
        rating: Optional[int],
        text: str,
        author: str,
        product_title: str,
        was_viewed: bool,
        status_if_new: str = "new",
        *,
        extra_json: Optional[str] = None,
    ) -> Tuple[Optional[int], bool]:
        """
        Возвращает (item_id, was_inserted). item_id при insert — id новой строки; при update — id существующей.
        was_inserted True только при новой записи (для отправки в Telegram и т.д.).
        extra_json — опционально (напр. Ozon: {"sku": 123}).
        """
        with _DB_LOCK:
            row = self._conn.execute(
                "SELECT id, status FROM items WHERE store_id=? AND item_type=? AND external_id=?",
                (store_id, item_type, external_id)
            ).fetchone()
            extra = (extra_json or "").strip() or ""
            if row:
                if extra_json is not None:
                    self._conn.execute(
                        "UPDATE items SET date=?, rating=?, text=?, author=?, product_title=?, was_viewed=?, extra_json=? WHERE id=?",
                        (date, rating, text, author, product_title, 1 if was_viewed else 0, extra, int(row["id"])),
                    )
                else:
                    self._conn.execute(
                        "UPDATE items SET date=?, rating=?, text=?, author=?, product_title=?, was_viewed=? WHERE id=?",
                        (date, rating, text, author, product_title, 1 if was_viewed else 0, int(row["id"])),
                    )
                self._conn.commit()
                return int(row["id"]), False
            cur = self._conn.execute(
                """INSERT INTO items(store_id,external_id,item_type,date,rating,text,author,product_title,status,generated_text,was_viewed,extra_json)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (store_id, external_id, item_type, date, rating, text, author, product_title, status_if_new, "", 1 if was_viewed else 0, extra),
            )
            self._conn.commit()
            return int(cur.lastrowid), True

    def list_items_for_ui(self, store_id: int, item_type: str) -> list[ItemRow]:
        with _DB_LOCK:
            rows = self._conn.execute(
                """SELECT id, store_id, external_id, item_type, date, rating, text, author, product_title,
                          status, COALESCE(generated_text,'') AS generated_text, was_viewed,
                          COALESCE(extra_json,'') AS extra_json
                   FROM items
                   WHERE store_id=? AND item_type=? AND status IN ('new','generated')
                   ORDER BY date DESC
                   LIMIT 10000
                """,
                (store_id, item_type)
            ).fetchall()
            out: list[ItemRow] = []
            for r in rows:
                out.append(ItemRow(
                    id=int(r["id"]),
                    store_id=int(r["store_id"]),
                    external_id=str(r["external_id"]),
                    item_type=str(r["item_type"]),
                    date=str(r["date"]),
                    rating=(int(r["rating"]) if r["rating"] is not None else None),
                    text=str(r["text"] or ""),
                    author=str(r["author"] or ""),
                    product_title=str(r["product_title"] or ""),
                    status=str(r["status"]),
                    generated_text=str(r["generated_text"] or ""),
                    was_viewed=bool(r["was_viewed"]),
                    extra_json=str(r["extra_json"] or ""),
                ))
            return out

    def list_items_for_ui_all(self, item_type: str) -> list[ItemRow]:
        """Список элементов по всем магазинам (для режима «Все магазины»)."""
        with _DB_LOCK:
            rows = self._conn.execute(
                """SELECT id, store_id, external_id, item_type, date, rating, text, author, product_title,
                          status, COALESCE(generated_text,'') AS generated_text, was_viewed,
                          COALESCE(extra_json,'') AS extra_json
                   FROM items
                   WHERE item_type=? AND status IN ('new','generated')
                   ORDER BY date DESC
                   LIMIT 10000
                """,
                (item_type,)
            ).fetchall()
            return [
                ItemRow(
                    id=int(r["id"]),
                    store_id=int(r["store_id"]),
                    external_id=str(r["external_id"]),
                    item_type=str(r["item_type"]),
                    date=str(r["date"]),
                    rating=(int(r["rating"]) if r["rating"] is not None else None),
                    text=str(r["text"] or ""),
                    author=str(r["author"] or ""),
                    product_title=str(r["product_title"] or ""),
                    status=str(r["status"]),
                    generated_text=str(r["generated_text"] or ""),
                    was_viewed=bool(r["was_viewed"]),
                    extra_json=str(r["extra_json"] or ""),
                )
                for r in rows
            ]

    def find_item_id(self, store_id: int, item_type: str, external_id: str) -> Optional[int]:
        with _DB_LOCK:
            row = self._conn.execute(
                "SELECT id FROM items WHERE store_id=? AND item_type=? AND external_id=?",
                (int(store_id), str(item_type), str(external_id)),
            ).fetchone()
            return int(row["id"]) if row else None

    def set_status(self, item_id: int, status: str) -> None:
        with _DB_LOCK:
            self._conn.execute("UPDATE items SET status=? WHERE id=?", (str(status), int(item_id)))
            self._conn.commit()

    def list_items_filtered(
        self,
        *,
        item_type: str,
        store_id: Optional[int] = None,
        statuses: Optional[list[str]] = None,
        has_answer: Optional[bool] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[ItemRow]:
        where = ["item_type=?"]
        params: list = [str(item_type)]
        if store_id is not None:
            where.append("store_id=?")
            params.append(int(store_id))
        if statuses:
            placeholders = ",".join("?" * len(statuses))
            where.append(f"status IN ({placeholders})")
            params.extend([str(s) for s in statuses])
        if has_answer is True:
            where.append("COALESCE(generated_text,'') <> ''")
        elif has_answer is False:
            where.append("COALESCE(generated_text,'') = ''")
        w = " AND ".join(where)
        safe_limit = max(1, min(int(limit), 500))
        safe_offset = max(0, int(offset))
        with _DB_LOCK:
            rows = self._conn.execute(
                f"""SELECT id, store_id, external_id, item_type, date, rating, text, author, product_title,
                          status, COALESCE(generated_text,'') AS generated_text, was_viewed,
                          COALESCE(extra_json,'') AS extra_json
                   FROM items
                   WHERE {w}
                   ORDER BY date DESC
                   LIMIT ? OFFSET ?""",
                params + [safe_limit, safe_offset],
            ).fetchall()
            return [
                ItemRow(
                    id=int(r["id"]),
                    store_id=int(r["store_id"]),
                    external_id=str(r["external_id"]),
                    item_type=str(r["item_type"]),
                    date=str(r["date"]),
                    rating=(int(r["rating"]) if r["rating"] is not None else None),
                    text=str(r["text"] or ""),
                    author=str(r["author"] or ""),
                    product_title=str(r["product_title"] or ""),
                    status=str(r["status"]),
                    generated_text=str(r["generated_text"] or ""),
                    was_viewed=bool(r["was_viewed"]),
                    extra_json=str(r["extra_json"] or ""),
                )
                for r in rows
            ]

    def get_item_by_id(self, item_id: int) -> Optional[ItemRow]:
        with _DB_LOCK:
            r = self._conn.execute(
                """SELECT id, store_id, external_id, item_type, date, rating, text, author, product_title,
                          status, COALESCE(generated_text,'') AS generated_text, was_viewed,
                          COALESCE(extra_json,'') AS extra_json
                   FROM items WHERE id=?""",
                (item_id,)
            ).fetchone()
            if not r:
                return None
            return ItemRow(
                id=int(r["id"]),
                store_id=int(r["store_id"]),
                external_id=str(r["external_id"]),
                item_type=str(r["item_type"]),
                date=str(r["date"]),
                rating=(int(r["rating"]) if r["rating"] is not None else None),
                text=str(r["text"] or ""),
                author=str(r["author"] or ""),
                product_title=str(r["product_title"] or ""),
                status=str(r["status"]),
                generated_text=str(r["generated_text"] or ""),
                was_viewed=bool(r["was_viewed"]),
                extra_json=str(r["extra_json"] or ""),
            )

    def set_generated(self, item_id: int, text: str) -> None:
        with _DB_LOCK:
            self._conn.execute(
                "UPDATE items SET status='generated', generated_text=?, send_error='' WHERE id=?",
                (text, item_id)
            )
            self._conn.commit()

    def update_generated_text(self, item_id: int, text: str) -> bool:
        """Сохранить отредактированный ответ. Нельзя менять уже отправленные."""
        t = (text or "").strip()
        if not t:
            return False
        with _DB_LOCK:
            row = self._conn.execute(
                "SELECT status FROM items WHERE id=?", (int(item_id),)
            ).fetchone()
            if not row:
                return False
            st = str(row["status"] or "")
            if st in ("sent", "sending"):
                return False
            self._conn.execute(
                "UPDATE items SET status='generated', generated_text=?, send_error='' WHERE id=?",
                (t, int(item_id)),
            )
            self._conn.commit()
            return True

    def try_claim_for_send(self, item_id: int) -> Optional[ItemRow]:
        """Атомарно переводит item в sending, если можно отправить."""
        with _DB_LOCK:
            cur = self._conn.execute(
                """UPDATE items SET status='sending', send_error=''
                   WHERE id=? AND status IN ('new','generated')
                   AND trim(COALESCE(generated_text,'')) <> ''""",
                (int(item_id),),
            )
            if int(cur.rowcount or 0) == 0:
                self._conn.commit()
                return None
            self._conn.commit()
            r = self._conn.execute(
                """SELECT id, store_id, external_id, item_type, date, rating, text, author, product_title,
                          status, COALESCE(generated_text,'') AS generated_text, was_viewed,
                          COALESCE(extra_json,'') AS extra_json
                   FROM items WHERE id=?""",
                (int(item_id),),
            ).fetchone()
            if not r:
                return None
            return ItemRow(
                id=int(r["id"]),
                store_id=int(r["store_id"]),
                external_id=str(r["external_id"]),
                item_type=str(r["item_type"]),
                date=str(r["date"]),
                rating=(int(r["rating"]) if r["rating"] is not None else None),
                text=str(r["text"] or ""),
                author=str(r["author"] or ""),
                product_title=str(r["product_title"] or ""),
                status=str(r["status"]),
                generated_text=str(r["generated_text"] or ""),
                was_viewed=bool(r["was_viewed"]),
                extra_json=str(r["extra_json"] or ""),
            )

    def release_send_claim(self, item_id: int, error: str = "") -> None:
        with _DB_LOCK:
            err = (error or "")[:500]
            self._conn.execute(
                """UPDATE items SET status='generated', send_error=?
                   WHERE id=? AND status='sending'""",
                (err, int(item_id)),
            )
            self._conn.commit()

    def set_sent(self, item_id: int, sent_at_iso: str) -> None:
        with _DB_LOCK:
            self._conn.execute(
                "UPDATE items SET status='sent', sent_at=?, send_error='' WHERE id=?",
                (sent_at_iso, item_id)
            )
            self._conn.commit()

    def set_ignored(self, item_id: int) -> None:
        with _DB_LOCK:
            self._conn.execute(
                "UPDATE items SET status='ignored' WHERE id=?",
                (item_id,)
            )
            self._conn.commit()

    def get_activity_stats_since(
        self,
        since_iso: str,
        until_iso: Optional[str] = None,
    ) -> dict:
        """Счётчики действий за интервал [since, until) по unix-времени (корректно с TZ)."""
        since_u = iso_to_unix(since_iso)
        if since_u <= 0:
            return _empty_activity_stats()
        until_u = iso_to_unix(until_iso) if until_iso else None
        ts_rng = "iso_to_unix(ts) >= ?"
        sent_rng = "iso_to_unix(sent_at) >= ?"
        ae_params: list[float] = [since_u]
        item_params: list[float] = [since_u]
        if until_u is not None and until_u > since_u:
            ts_rng += " AND iso_to_unix(ts) < ?"
            sent_rng += " AND iso_to_unix(sent_at) < ?"
            ae_params.append(until_u)
            item_params.append(until_u)
        with _DB_LOCK:
            rev = self._conn.execute(
                f"""SELECT COUNT(*) AS n FROM items
                   WHERE status='sent' AND item_type='review' AND {sent_rng}""",
                tuple(item_params),
            ).fetchone()
            qu = self._conn.execute(
                f"""SELECT COUNT(*) AS n FROM items
                   WHERE status='sent' AND item_type='question' AND {sent_rng}""",
                tuple(item_params),
            ).fetchone()
            wb = self._conn.execute(
                f"""SELECT COUNT(*) AS n FROM audit_events
                   WHERE action='wb_buyer_chat_send' AND result='ok' AND {ts_rng}""",
                tuple(ae_params),
            ).fetchone()
            oz = self._conn.execute(
                f"""SELECT COUNT(*) AS n FROM audit_events
                   WHERE action='ozon_buyer_chat_send' AND result='ok' AND {ts_rng}""",
                tuple(ae_params),
            ).fetchone()
            oz_alerts = self._conn.execute(
                f"""SELECT COUNT(*) AS n FROM audit_events
                   WHERE action='ozon_alert_detected' AND result='ok' AND {ts_rng}""",
                tuple(ae_params),
            ).fetchone()
            audit_rows = self._conn.execute(
                f"""SELECT meta_json FROM audit_events
                   WHERE action IN (
                     'ozon_actions_auto_remove',
                     'ozon_actions_remove',
                     'ozon_actions_discount_sync'
                   )
                     AND result IN ('ok', 'skipped')
                     AND {ts_rng}""",
                tuple(ae_params),
            ).fetchall()
            rating_rows = self._conn.execute(
                f"""SELECT rating, COUNT(*) AS n FROM items
                   WHERE status='sent' AND item_type='review' AND rating BETWEEN 1 AND 5
                     AND {sent_rng}
                   GROUP BY rating""",
                tuple(item_params),
            ).fetchall()
        products_removed = 0
        products_added = 0
        participants_kept = 0
        skipped_data_count = 0
        for row in audit_rows:
            raw = row["meta_json"] if isinstance(row, dict) else row[0]
            if not raw:
                continue
            try:
                meta = json.loads(str(raw))
                products_removed += int(
                    meta.get("products_removed") or meta.get("participants_removed") or 0
                )
                products_added += int(
                    meta.get("products_added") or meta.get("candidates_added") or 0
                )
                participants_kept += int(meta.get("participants_kept") or 0)
                skipped_data_count += int(meta.get("skipped_data_count") or 0)
            except Exception:
                continue
        reviews_by_rating: dict[int, int] = {}
        for row in rating_rows:
            try:
                r = int(row["rating"] if isinstance(row, dict) else row[0])
                n = int(row["n"] if isinstance(row, dict) else row[1])
            except (TypeError, ValueError, IndexError):
                continue
            if 1 <= r <= 5 and n > 0:
                reviews_by_rating[r] = n
        wb_chats = int(wb["n"]) if wb else 0
        oz_chats = int(oz["n"]) if oz else 0
        card_errors = self.count_card_error_alerts_since(since_iso, until_iso)
        stats = {
            "reviews_sent": int(rev["n"]) if rev else 0,
            "reviews_by_rating": reviews_by_rating,
            "questions_sent": int(qu["n"]) if qu else 0,
            "ozon_products_removed": products_removed,
            "ozon_products_added": products_added,
            "ozon_promo_kept": participants_kept,
            "ozon_promo_skipped_data": skipped_data_count,
            "wb_chat_replies": wb_chats,
            "ozon_chat_replies": oz_chats,
            "chat_replies_total": wb_chats + oz_chats,
            "card_errors": card_errors,
            "ozon_alerts": int(oz_alerts["n"]) if oz_alerts else 0,
            "ozon_cert_requests_products": 0,
            "ozon_hidden_products": 0,
            "ozon_threat_hide_products": 0,
            "ozon_threat_fine_products": 0,
            "ozon_threat_fine_by_amount": {},
        }
        try:
            from app.core.ozon_alerts import ozon_product_stats_for_period

            stats.update(ozon_product_stats_for_period(self, since_iso, until_iso))
        except Exception:
            pass
        return stats

    def get_stats(self) -> dict:
        """Операционная сводка: отправки + текущая очередь + активные магазины."""
        with _DB_LOCK:
            total = self._conn.execute(
                "SELECT COUNT(*) AS n FROM items WHERE status='sent'"
            ).fetchone()
            total_sent = int(total["n"]) if total else 0

            today = self._conn.execute(
                """SELECT COUNT(*) AS n FROM items WHERE status='sent'
                   AND DATE(sent_at) = DATE('now', 'localtime')"""
            ).fetchone()
            sent_today = int(today["n"]) if today else 0

            by_type_rows = self._conn.execute(
                """SELECT item_type, COUNT(*) AS n FROM items WHERE status='sent'
                   GROUP BY item_type"""
            ).fetchall()
            by_type = {str(r["item_type"]): int(r["n"]) for r in by_type_rows}

            by_store_rows = self._conn.execute(
                """SELECT i.store_id, s.name, COUNT(*) AS n
                   FROM items i LEFT JOIN stores s ON s.id = i.store_id
                   WHERE i.status='sent'
                   GROUP BY i.store_id"""
            ).fetchall()
            by_store = [
                {"store_id": int(r["store_id"]), "name": str(r["name"] or ""), "count": int(r["n"])}
                for r in by_store_rows
            ]
            q_rows = self._conn.execute(
                """SELECT status, item_type, COUNT(*) AS n
                   FROM items
                   GROUP BY status, item_type"""
            ).fetchall()
            queue = {
                "new_reviews": 0,
                "new_questions": 0,
                "generated_reviews": 0,
                "generated_questions": 0,
                "sent_reviews": 0,
                "sent_questions": 0,
            }
            for r in q_rows:
                st = str(r["status"] or "")
                tp = str(r["item_type"] or "")
                n = int(r["n"] or 0)
                if st == "new" and tp == "review":
                    queue["new_reviews"] = n
                elif st == "new" and tp == "question":
                    queue["new_questions"] = n
                elif st == "generated" and tp == "review":
                    queue["generated_reviews"] = n
                elif st == "generated" and tp == "question":
                    queue["generated_questions"] = n
                elif st == "sent" and tp == "review":
                    queue["sent_reviews"] = n
                elif st == "sent" and tp == "question":
                    queue["sent_questions"] = n

            active_stores_row = self._conn.execute("SELECT COUNT(*) AS n FROM stores WHERE active=1").fetchone()
            total_stores_row = self._conn.execute("SELECT COUNT(*) AS n FROM stores").fetchone()
            active_stores = int(active_stores_row["n"]) if active_stores_row else 0
            total_stores = int(total_stores_row["n"]) if total_stores_row else 0

            wb_chat_row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM audit_events WHERE action=? AND result=?",
                ("wb_buyer_chat_send", "ok"),
            ).fetchone()
            wb_chat_sent = int(wb_chat_row["n"]) if wb_chat_row else 0
            today_prefix = dt.datetime.now().strftime("%Y-%m-%d")
            wb_today_row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM audit_events WHERE action=? AND result=? AND substr(ts,1,10)=?",
                ("wb_buyer_chat_send", "ok", today_prefix),
            ).fetchone()
            wb_chat_sent_today = int(wb_today_row["n"]) if wb_today_row else 0

            ozon_chat_row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM audit_events WHERE action=? AND result=?",
                ("ozon_buyer_chat_send", "ok"),
            ).fetchone()
            ozon_chat_sent = int(ozon_chat_row["n"]) if ozon_chat_row else 0
            ozon_today_row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM audit_events WHERE action=? AND result=? AND substr(ts,1,10)=?",
                ("ozon_buyer_chat_send", "ok", today_prefix),
            ).fetchone()
            ozon_chat_sent_today = int(ozon_today_row["n"]) if ozon_today_row else 0

        return {
            "total_sent": total_sent,
            "sent_today": sent_today,
            "by_type": by_type,
            "by_store": by_store,
            "queue": queue,
            "stores": {"active": active_stores, "total": total_stores},
            "wb_chat_sent": wb_chat_sent,
            "wb_chat_sent_today": wb_chat_sent_today,
            "ozon_chat_sent": ozon_chat_sent,
            "ozon_chat_sent_today": ozon_chat_sent_today,
        }

    # ---------- Card links master (WB) ----------
    def clm_clear_store(self, store_id: int) -> None:
        """Полный сброс кэша мастера: карточки, план, шаги и журнал."""
        with _DB_LOCK:
            sid = int(store_id)
            self._conn.execute("DELETE FROM card_links_master_items WHERE store_id=?", (sid,))
            self._conn.execute("DELETE FROM card_links_master_bundles WHERE store_id=?", (sid,))
            self._conn.execute("DELETE FROM card_links_master_state WHERE store_id=?", (sid,))
            self._conn.commit()

    def clm_save_items(self, store_id: int, rows: list[dict]) -> None:
        ts = utc_now_iso()
        sid = int(store_id)
        with _DB_LOCK:
            self._conn.execute("DELETE FROM card_links_master_items WHERE store_id=?", (sid,))
            for r in rows:
                nid = int(r.get("nm_id") or 0)
                if not nid:
                    continue
                self._conn.execute(
                    """INSERT INTO card_links_master_items(
                        store_id, nm_id, vendor_code, title, subject_id, subject_name, parent_name,
                        imt_id, linked, brand, segment, subtype, phone_model, bundle_id,
                        group_key, status, row_json, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        sid,
                        nid,
                        str(r.get("vendor_code") or ""),
                        str(r.get("title") or "")[:240],
                        int(r.get("subject_id") or 0),
                        str(r.get("subject_name") or ""),
                        str(r.get("parent_name") or ""),
                        int(r.get("imt_id") or 0),
                        1 if r.get("linked") else 0,
                        str(r.get("brand") or ""),
                        str(r.get("segment") or ""),
                        str(r.get("subtype") or ""),
                        str(r.get("phone_model") or ""),
                        str(r.get("bundle_id") or ""),
                        str(r.get("group_key") or ""),
                        str(r.get("status") or "pending"),
                        json.dumps(r, ensure_ascii=False, default=str),
                        ts,
                    ),
                )
            self._conn.commit()

    def clm_load_items(self, store_id: int) -> list[dict]:
        with _DB_LOCK:
            cur = self._conn.execute(
                "SELECT row_json FROM card_links_master_items WHERE store_id=? ORDER BY subject_name, brand, title",
                (int(store_id),),
            )
            out: list[dict] = []
            for row in cur.fetchall():
                try:
                    out.append(json.loads(row["row_json"] or "{}"))
                except Exception:
                    pass
            return out

    def clm_save_bundles(self, store_id: int, bundles: list[dict]) -> None:
        ts = utc_now_iso()
        sid = int(store_id)
        with _DB_LOCK:
            self._conn.execute("DELETE FROM card_links_master_bundles WHERE store_id=?", (sid,))
            for b in bundles:
                bid = str(b.get("bundle_id") or "")
                if not bid:
                    continue
                self._conn.execute(
                    """INSERT INTO card_links_master_bundles(
                        store_id, bundle_id, segment, category_label, brand, subtype_label,
                        item_count, target_imt, sort_size, bundle_json, apply_status, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        sid,
                        bid,
                        str(b.get("segment") or ""),
                        str(b.get("category_label") or ""),
                        str(b.get("brand") or ""),
                        str(b.get("subtype_label") or ""),
                        int(b.get("item_count") or 0),
                        int(b.get("target_imt") or 0),
                        int(b.get("sort_size") or b.get("item_count") or 0),
                        json.dumps(b, ensure_ascii=False, default=str),
                        str(b.get("apply_status") or "pending"),
                        ts,
                    ),
                )
            self._conn.commit()

    def _clm_bundle_where(
        self,
        store_id: int,
        *,
        segment: str = "",
        brand: str = "",
        category: str = "",
        min_bundles_in_category: int = 0,
    ) -> tuple[list[str], list]:
        where = ["store_id=?"]
        params: list = [int(store_id)]
        if segment:
            where.append("segment=?")
            params.append(segment)
        if brand:
            where.append("brand=?")
            params.append(brand)
        if category:
            where.append("category_label=?")
            params.append(category)
        mc = int(min_bundles_in_category or 0)
        if mc >= 2:
            where.append(
                """category_label IN (
                    SELECT category_label FROM card_links_master_bundles
                    WHERE store_id=? GROUP BY category_label HAVING COUNT(*) >= ?
                )"""
            )
            params.extend([int(store_id), mc])
        return where, params

    def clm_category_bundle_counts(self, store_id: int) -> dict[str, int]:
        with _DB_LOCK:
            cur = self._conn.execute(
                """SELECT category_label, COUNT(*) AS n FROM card_links_master_bundles
                   WHERE store_id=? AND category_label<>'' GROUP BY category_label""",
                (int(store_id),),
            )
            return {str(r[0]): int(r[1]) for r in cur.fetchall() if r and r[0]}

    def clm_dense_categories(self, store_id: int, *, min_count: int = 3) -> list[dict]:
        mc = max(2, int(min_count or 3))
        counts = self.clm_category_bundle_counts(store_id)
        rows = [
            {"category_label": k, "bundle_count": v}
            for k, v in counts.items()
            if v >= mc
        ]
        rows.sort(key=lambda x: (-int(x["bundle_count"]), str(x["category_label"])))
        return rows

    def clm_load_bundles(
        self,
        store_id: int,
        *,
        segment: str = "",
        brand: str = "",
        category: str = "",
        min_bundles_in_category: int = 0,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict], int]:
        where, params = self._clm_bundle_where(
            store_id,
            segment=segment,
            brand=brand,
            category=category,
            min_bundles_in_category=min_bundles_in_category,
        )
        w = " AND ".join(where)
        with _DB_LOCK:
            cnt = self._conn.execute(
                f"SELECT COUNT(*) AS n FROM card_links_master_bundles WHERE {w}",
                params,
            ).fetchone()
            total = int(cnt["n"]) if cnt else 0
            cur = self._conn.execute(
                f"""SELECT bundle_json, apply_status FROM card_links_master_bundles
                    WHERE {w} ORDER BY sort_size DESC, category_label, bundle_id
                    LIMIT ? OFFSET ?""",
                params + [int(limit), int(offset)],
            )
            out: list[dict] = []
            for row in cur.fetchall():
                try:
                    b = json.loads(row["bundle_json"] or "{}")
                    st = str(row["apply_status"] or b.get("apply_status") or "pending")
                    b["apply_status"] = st
                    out.append(b)
                except Exception:
                    pass
            return out, total

    def clm_list_bundle_ids(
        self,
        store_id: int,
        *,
        segment: str = "",
        brand: str = "",
        category: str = "",
        min_bundles_in_category: int = 0,
    ) -> list[str]:
        where, params = self._clm_bundle_where(
            store_id,
            segment=segment,
            brand=brand,
            category=category,
            min_bundles_in_category=min_bundles_in_category,
        )
        w = " AND ".join(where)
        with _DB_LOCK:
            cur = self._conn.execute(
                f"""SELECT bundle_id FROM card_links_master_bundles
                    WHERE {w} ORDER BY sort_size DESC, category_label, bundle_id""",
                params,
            )
            return [str(r[0]) for r in cur.fetchall() if r and r[0]]

    def clm_set_bundle_apply_statuses(
        self,
        store_id: int,
        *,
        applied: Optional[list] = None,
        skipped: Optional[list] = None,
        failed: Optional[list] = None,
    ) -> None:
        sid = int(store_id)
        mapping: list[tuple[str, str]] = []
        for bid in applied or []:
            b = str(bid or "").strip()
            if b:
                mapping.append((b, "applied"))
        for bid in skipped or []:
            b = str(bid or "").strip()
            if b:
                mapping.append((b, "skipped"))
        for bid in failed or []:
            b = str(bid or "").strip()
            if b:
                mapping.append((b, "failed"))
        if not mapping:
            return
        ts = utc_now_iso()
        with _DB_LOCK:
            for bid, status in mapping:
                self._conn.execute(
                    """UPDATE card_links_master_bundles
                       SET apply_status=?, updated_at=?
                       WHERE store_id=? AND bundle_id=?""",
                    (status, ts, sid, bid),
                )
            self._conn.commit()

    def clm_get_state(self, store_id: int) -> dict:
        with _DB_LOCK:
            row = self._conn.execute(
                "SELECT steps_json, catalog_at, log_json, updated_at FROM card_links_master_state WHERE store_id=?",
                (int(store_id),),
            ).fetchone()
            if not row:
                return {"steps": {}, "catalog_at": "", "log": [], "updated_at": ""}
            try:
                steps = json.loads(row["steps_json"] or "{}")
            except Exception:
                steps = {}
            try:
                log = json.loads(row["log_json"] or "[]")
            except Exception:
                log = []
            return {
                "steps": steps,
                "catalog_at": str(row["catalog_at"] or ""),
                "log": log,
                "updated_at": str(row["updated_at"] or ""),
            }

    def clm_set_state(
        self,
        store_id: int,
        *,
        steps: Optional[dict] = None,
        catalog_at: Optional[str] = None,
        log: Optional[list] = None,
    ) -> None:
        ts = utc_now_iso()
        sid = int(store_id)
        cur = self.clm_get_state(sid)
        if steps is not None:
            cur["steps"] = steps
        if catalog_at is not None:
            cur["catalog_at"] = catalog_at
        if log is not None:
            cur["log"] = log[-200:]
        with _DB_LOCK:
            self._conn.execute(
                """INSERT INTO card_links_master_state(store_id, steps_json, catalog_at, log_json, updated_at)
                   VALUES (?,?,?,?,?)
                   ON CONFLICT(store_id) DO UPDATE SET
                     steps_json=excluded.steps_json,
                     catalog_at=excluded.catalog_at,
                     log_json=excluded.log_json,
                     updated_at=excluded.updated_at""",
                (
                    sid,
                    json.dumps(cur.get("steps") or {}, ensure_ascii=False),
                    str(cur.get("catalog_at") or ""),
                    json.dumps(cur.get("log") or [], ensure_ascii=False),
                    ts,
                ),
            )
            self._conn.commit()

    def clm_append_log(self, store_id: int, message: str, *, level: str = "info") -> None:
        st = self.clm_get_state(store_id)
        log = list(st.get("log") or [])
        log.append({"ts": utc_now_iso(), "level": level, "message": message[:500]})
        self.clm_set_state(store_id, log=log[-200:])

    def clm_filter_options(self, store_id: int) -> dict:
        with _DB_LOCK:
            sid = int(store_id)
            brands = [
                str(r[0])
                for r in self._conn.execute(
                    "SELECT DISTINCT brand FROM card_links_master_items WHERE store_id=? AND brand<>'' ORDER BY brand",
                    (sid,),
                ).fetchall()
            ]
            categories = [
                str(r[0])
                for r in self._conn.execute(
                    "SELECT DISTINCT subject_name FROM card_links_master_items WHERE store_id=? AND subject_name<>'' ORDER BY subject_name",
                    (sid,),
                ).fetchall()
            ]
            segments = [
                str(r[0])
                for r in self._conn.execute(
                    "SELECT DISTINCT segment FROM card_links_master_items WHERE store_id=? AND segment<>'' ORDER BY segment",
                    (sid,),
                ).fetchall()
            ]
            subtypes = [
                str(r[0])
                for r in self._conn.execute(
                    """SELECT DISTINCT subtype FROM card_links_master_items
                       WHERE store_id=? AND subtype<>'' ORDER BY subtype""",
                    (sid,),
                ).fetchall()
            ]
            return {
                "brands": brands,
                "categories": categories,
                "segments": segments,
                "subtypes": subtypes,
            }

    def clm_coverage(self, store_id: int) -> dict:
        with _DB_LOCK:
            sid = int(store_id)
            total = self._conn.execute(
                "SELECT COUNT(*) AS n FROM card_links_master_items WHERE store_id=?",
                (sid,),
            ).fetchone()
            planned = self._conn.execute(
                "SELECT COUNT(*) AS n FROM card_links_master_items WHERE store_id=? AND status='planned'",
                (sid,),
            ).fetchone()
            solo = self._conn.execute(
                "SELECT COUNT(*) AS n FROM card_links_master_items WHERE store_id=? AND status='solo'",
                (sid,),
            ).fetchone()
            bundles = self._conn.execute(
                "SELECT COUNT(*) AS n FROM card_links_master_bundles WHERE store_id=?",
                (sid,),
            ).fetchone()
            return {
                "total": int(total["n"]) if total else 0,
                "planned_items": int(planned["n"]) if planned else 0,
                "singles": int(solo["n"]) if solo else 0,
                "bundles": int(bundles["n"]) if bundles else 0,
            }
