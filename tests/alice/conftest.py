from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any, Optional

import pytest

from app.alice.config import AliceWarehouseConfig
from app.alice.session_store import AliceSessionStore
from app.alice.tms_client import TmsError
from app.alice.warehouse_service import WarehouseService
from app.db import Database


class FakeTms:
    def __init__(self, products: Optional[dict[int, dict]] = None) -> None:
        self.products = products or {}
        self.patch_calls: list[dict[str, Any]] = []
        self.get_calls: list[int] = []
        self.fail_patch_with: Optional[TmsError] = None
        self.stock_override_after_get: Optional[dict[int, int]] = None

    async def get_product_by_sku(self, sku: int) -> dict[str, Any]:
        self.get_calls.append(int(sku))
        if self.stock_override_after_get and int(sku) in self.stock_override_after_get:
            # emulate race: change stock before returning
            p = dict(self.products[int(sku)])
            p["stock"] = self.stock_override_after_get[int(sku)]
            self.products[int(sku)] = p
            self.stock_override_after_get.pop(int(sku), None)
        if int(sku) not in self.products:
            raise TmsError("NOT_FOUND", "Товар не найден", 404)
        return dict(self.products[int(sku)])

    async def patch_stock_absolute(self, sku: int, stock: int) -> dict[str, Any]:
        self.patch_calls.append({"sku": int(sku), "stock": int(stock), "mode": "absolute"})
        if self.fail_patch_with:
            raise self.fail_patch_with
        p = dict(self.products[int(sku)])
        p["stock"] = int(stock)
        self.products[int(sku)] = p
        return p

    async def patch_stock_add(self, sku: int, add: int, operation_id: str) -> dict[str, Any]:
        self.patch_calls.append(
            {"sku": int(sku), "add": int(add), "operation_id": operation_id, "mode": "add"}
        )
        if self.fail_patch_with:
            raise self.fail_patch_with
        p = dict(self.products[int(sku)])
        cur = p.get("stock")
        if cur is None:
            raise TmsError("STOCK_ROW_NOT_FOUND", "нет записи", 409)
        new = int(cur) + int(add)
        if new < 0:
            raise TmsError(
                "STOCK_INSUFFICIENT",
                "недостаточно",
                409,
                details={"current": int(cur), "add": int(add)},
            )
        p["stock"] = new
        self.products[int(sku)] = p
        return p


@pytest.fixture
def tmp_db():
    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / "test.db")
        db = Database(path)
        yield db
        db.close()


@pytest.fixture
def cfg_dry():
    return AliceWarehouseConfig(
        tms_base_url="https://tmsdesk.ru",
        tms_api_token="test-token",
        tms_operator="Alice-Warehouse",
        dry_run=True,
        max_stock_change=1000,
        session_ttl_seconds=1800,
        pending_ttl_seconds=60,
        dedup_ttl_seconds=300,
        skill_id="",
        webhook_secret="",
    )


@pytest.fixture
def cfg_live(cfg_dry):
    return AliceWarehouseConfig(
        tms_base_url=cfg_dry.tms_base_url,
        tms_api_token=cfg_dry.tms_api_token,
        tms_operator=cfg_dry.tms_operator,
        dry_run=False,
        max_stock_change=cfg_dry.max_stock_change,
        session_ttl_seconds=cfg_dry.session_ttl_seconds,
        pending_ttl_seconds=cfg_dry.pending_ttl_seconds,
        dedup_ttl_seconds=cfg_dry.dedup_ttl_seconds,
        skill_id="",
        webhook_secret="",
    )


_DEFAULT_ADDRESS = {"rack": "А", "shelf": "5(ПОЛ)", "place": "13", "cell": "", "string": "А5(ПОЛ) 13"}
_NO_ADDRESS = object()


def make_product(
    sku: int = 789868,
    stock: Any = 74,
    *,
    is_kit: bool = False,
    address: Any = _DEFAULT_ADDRESS,
    name: str = "Тестовый товар",
) -> dict:
    if address is _NO_ADDRESS:
        address = None
    elif address is _DEFAULT_ADDRESS:
        address = dict(_DEFAULT_ADDRESS)
    return {
        "sku": sku,
        "name": name,
        "stock": stock,
        "is_kit": is_kit,
        "address": address,
        "barcodes": [],
        "address_source": "own" if address else None,
        "inherited_from_sku": None,
        "is_active": True,
        "matched_by": "sku",
    }


def make_service(db, cfg, products=None) -> tuple[WarehouseService, FakeTms, AliceSessionStore]:
    tms = FakeTms(products or {789868: make_product()})
    store = AliceSessionStore(
        db,
        session_ttl=cfg.session_ttl_seconds,
        pending_ttl=cfg.pending_ttl_seconds,
        dedup_ttl=cfg.dedup_ttl_seconds,
    )
    svc = WarehouseService(config=cfg, store=store, tms=tms)  # type: ignore[arg-type]
    return svc, tms, store
