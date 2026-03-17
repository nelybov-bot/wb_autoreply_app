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

_DB_LOCK = threading.RLock()

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

class Database:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
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
            # Индексы
            c.execute("CREATE INDEX IF NOT EXISTS idx_items_status ON items(status)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_items_type ON items(item_type)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_items_store ON items(store_id)")
            # Lazy migration: extra_json для Ozon sku и др.
            info_items = c.execute("PRAGMA table_info(items)").fetchall()
            if "extra_json" not in [row[1] for row in info_items]:
                c.execute("ALTER TABLE items ADD COLUMN extra_json TEXT")
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
            self._conn.commit()
            self._seed_prompts_if_empty()

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
            ]
            self._conn.executemany(
                "INSERT INTO prompts(item_type, rating_group, prompt_text) VALUES(?,?,?)",
                prompts
            )
            self._conn.commit()

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
                "UPDATE items SET status='generated', generated_text=? WHERE id=?",
                (text, item_id)
            )
            self._conn.commit()

    def set_sent(self, item_id: int, sent_at_iso: str) -> None:
        with _DB_LOCK:
            self._conn.execute(
                "UPDATE items SET status='sent', sent_at=? WHERE id=?",
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

    def get_stats(self) -> dict:
        """Статистика по отправленным: всего, за сегодня, по типам, по магазинам."""
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

        return {
            "total_sent": total_sent,
            "sent_today": sent_today,
            "by_type": by_type,
            "by_store": by_store,
        }
