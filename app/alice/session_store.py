"""Хранение сессий Алисы, pending-операций, dedup и аудита в SQLite."""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional

from app.db import Database, utc_now_iso


@dataclass
class AliceSession:
    session_id: str
    current_sku: Optional[int]
    updated_at: float


@dataclass
class PendingOperation:
    session_id: str
    operation: str  # decrease | set
    sku: int
    quantity: int
    created_at: float
    operation_id: str


class AliceSessionStore:
    def __init__(
        self,
        db: Database,
        *,
        session_ttl: int = 1800,
        pending_ttl: int = 60,
        dedup_ttl: int = 300,
    ) -> None:
        self.db = db
        self.session_ttl = session_ttl
        self.pending_ttl = pending_ttl
        self.dedup_ttl = dedup_ttl

    def get_current_sku(self, session_id: str) -> Optional[int]:
        self.db.alice_cleanup_expired(
            session_ttl=self.session_ttl,
            pending_ttl=self.pending_ttl,
            dedup_ttl=self.dedup_ttl,
        )
        row = self.db.alice_get_session(session_id)
        if not row:
            return None
        updated = float(row.get("updated_at") or 0)
        if time.time() - updated > self.session_ttl:
            self.db.alice_clear_session(session_id)
            return None
        sku = row.get("current_sku")
        return int(sku) if sku is not None else None

    def set_current_sku(self, session_id: str, sku: int) -> None:
        self.db.alice_upsert_session(session_id, int(sku))

    def clear_pending(self, session_id: str) -> None:
        self.db.alice_clear_pending(session_id)

    def set_pending(
        self,
        session_id: str,
        *,
        operation: str,
        sku: int,
        quantity: int,
        operation_id: Optional[str] = None,
    ) -> PendingOperation:
        op_id = operation_id or f"alice-{uuid.uuid4().hex[:24]}"
        created = time.time()
        self.db.alice_set_pending(
            session_id,
            operation=operation,
            sku=int(sku),
            quantity=int(quantity),
            operation_id=op_id,
            created_at=created,
        )
        return PendingOperation(
            session_id=session_id,
            operation=operation,
            sku=int(sku),
            quantity=int(quantity),
            created_at=created,
            operation_id=op_id,
        )

    def get_pending(self, session_id: str) -> Optional[PendingOperation]:
        row = self.db.alice_get_pending(session_id)
        if not row:
            return None
        created = float(row.get("created_at") or 0)
        if time.time() - created > self.pending_ttl:
            self.db.alice_clear_pending(session_id)
            return None
        return PendingOperation(
            session_id=session_id,
            operation=str(row["operation"]),
            sku=int(row["sku"]),
            quantity=int(row["quantity"]),
            created_at=created,
            operation_id=str(row.get("operation_id") or ""),
        )

    def peek_pending_status(self, session_id: str) -> str:
        """none | active | expired"""
        row = self.db.alice_get_pending(session_id)
        if not row:
            return "none"
        created = float(row.get("created_at") or 0)
        if time.time() - created > self.pending_ttl:
            self.db.alice_clear_pending(session_id)
            return "expired"
        return "active"

    def try_claim_dedup(self, dedup_key: str) -> bool:
        """True если ключ новый (можно выполнять mutation). False если дубликат."""
        return self.db.alice_try_claim_dedup(dedup_key, ttl_seconds=self.dedup_ttl)

    def release_dedup(self, dedup_key: str) -> None:
        self.db.alice_release_dedup(dedup_key)

    def write_audit(
        self,
        *,
        session_id: str,
        request_id: str,
        sku: Optional[int],
        operation: str,
        quantity: Optional[int],
        stock_before: Optional[int],
        stock_after: Optional[int],
        status: str,
        error_code: str = "",
        dry_run: bool = False,
        meta: Optional[dict[str, Any]] = None,
    ) -> None:
        self.db.alice_write_audit(
            ts=utc_now_iso(),
            session_id=session_id,
            request_id=request_id,
            sku=sku,
            operation=operation,
            quantity=quantity,
            stock_before=stock_before,
            stock_after=stock_after,
            status=status,
            error_code=error_code or "",
            dry_run=1 if dry_run else 0,
            meta=meta or {},
        )
