from __future__ import annotations

import time

import pytest

from app.alice.config import AliceWarehouseConfig
from app.alice.tms_client import TmsError
from tests.alice.conftest import _NO_ADDRESS, make_product, make_service


@pytest.mark.asyncio
async def test_get_stock(tmp_db, cfg_dry):
    svc, tms, _ = make_service(tmp_db, cfg_dry)
    r = await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="остаток 789868")
    assert "74" in r.text
    assert tms.patch_calls == []


@pytest.mark.asyncio
async def test_how_many_left(tmp_db, cfg_dry):
    svc, tms, _ = make_service(tmp_db, cfg_dry)
    r = await svc.handle(
        session_id="s1", message_id=1, is_new=False, utterance="сколько осталось 789868"
    )
    assert "74" in r.text
    assert not tms.patch_calls


@pytest.mark.asyncio
async def test_location(tmp_db, cfg_dry):
    svc, tms, store = make_service(tmp_db, cfg_dry)
    r = await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="где лежит 789868")
    assert "стеллаж" in r.text.lower() or "Стеллаж" in r.text
    assert "А" in r.text
    assert "13" in r.text
    # context: where without sku
    r2 = await svc.handle(session_id="s1", message_id=2, is_new=False, utterance="где лежит?")
    assert "А" in r2.text
    assert store.get_current_sku("s1") == 789868


@pytest.mark.asyncio
async def test_increase_dry_run(tmp_db, cfg_dry):
    svc, tms, _ = make_service(tmp_db, cfg_dry)
    await svc.handle(session_id="s1", message_id=0, is_new=False, utterance="остаток 789868")
    r = await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="добавь одну")
    assert "Тестовый режим" in r.text
    assert "75" in r.text
    assert tms.patch_calls == []


@pytest.mark.asyncio
async def test_plusani_two_live(tmp_db, cfg_live):
    svc, tms, _ = make_service(tmp_db, cfg_live)
    await svc.handle(session_id="s1", message_id=0, is_new=False, utterance="остаток 789868")
    r = await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="плюсани две")
    assert "76" in r.text
    assert len(tms.patch_calls) == 1
    assert tms.patch_calls[0]["add"] == 2


@pytest.mark.asyncio
async def test_decrease_requires_confirm(tmp_db, cfg_live):
    svc, tms, store = make_service(tmp_db, cfg_live)
    await svc.handle(session_id="s1", message_id=0, is_new=False, utterance="остаток 789868")
    r = await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="спиши одну")
    assert "Списать" in r.text
    assert tms.patch_calls == []
    assert store.get_pending("s1") is not None


@pytest.mark.asyncio
async def test_confirm_executes_pending(tmp_db, cfg_live):
    svc, tms, _ = make_service(tmp_db, cfg_live)
    await svc.handle(session_id="s1", message_id=0, is_new=False, utterance="остаток 789868")
    await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="спиши три")
    r = await svc.handle(session_id="s1", message_id=2, is_new=False, utterance="да")
    assert "71" in r.text or "Готово" in r.text
    assert tms.products[789868]["stock"] == 71
    assert len(tms.patch_calls) == 1
    assert tms.patch_calls[0]["add"] == -3


@pytest.mark.asyncio
async def test_cancel_pending(tmp_db, cfg_live):
    svc, tms, store = make_service(tmp_db, cfg_live)
    await svc.handle(session_id="s1", message_id=0, is_new=False, utterance="остаток 789868")
    await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="спиши одну")
    r = await svc.handle(session_id="s1", message_id=2, is_new=False, utterance="нет")
    assert "отменено" in r.text.lower()
    assert store.get_pending("s1") is None
    assert tms.patch_calls == []


@pytest.mark.asyncio
async def test_dedup_no_second_patch(tmp_db, cfg_live):
    svc, tms, _ = make_service(tmp_db, cfg_live)
    await svc.handle(session_id="s1", message_id=0, is_new=False, utterance="остаток 789868")
    r1 = await svc.handle(
        session_id="s1", message_id=1, is_new=False, utterance="добавь одну", request_id="s1:1"
    )
    assert "Готово" in r1.text
    r2 = await svc.handle(
        session_id="s1", message_id=1, is_new=False, utterance="добавь одну", request_id="s1:1"
    )
    assert "уже обработана" in r2.text.lower()
    assert len(tms.patch_calls) == 1


@pytest.mark.asyncio
async def test_stock_null(tmp_db, cfg_dry):
    svc, tms, _ = make_service(
        tmp_db, cfg_dry, products={789868: make_product(stock=None)}
    )
    r = await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="остаток 789868")
    assert "нет данных об остатке" in r.text.lower()
    r2 = await svc.handle(session_id="s1", message_id=2, is_new=False, utterance="добавь одну")
    assert "нет данных об остатке" in r2.text.lower()
    assert not tms.patch_calls


@pytest.mark.asyncio
async def test_not_found(tmp_db, cfg_dry):
    svc, tms, _ = make_service(tmp_db, cfg_dry, products={})
    r = await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="остаток 999999")
    assert "не найден" in r.text.lower()


@pytest.mark.asyncio
async def test_kit(tmp_db, cfg_live):
    svc, tms, _ = make_service(
        tmp_db, cfg_live, products={555001: make_product(sku=555001, stock=5, is_kit=True)}
    )
    await svc.handle(session_id="s1", message_id=0, is_new=False, utterance="остаток 555001")
    r = await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="добавь одну")
    assert "комплект" in r.text.lower()
    assert not tms.patch_calls


@pytest.mark.asyncio
async def test_insufficient_stock(tmp_db, cfg_live):
    svc, tms, _ = make_service(
        tmp_db, cfg_live, products={789868: make_product(stock=2)}
    )
    await svc.handle(session_id="s1", message_id=0, is_new=False, utterance="остаток 789868")
    r = await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="спиши 5")
    assert "невозможно" in r.text.lower()
    assert "2" in r.text
    assert not tms.patch_calls


@pytest.mark.asyncio
async def test_race_condition_on_confirm(tmp_db, cfg_live):
    """Остаток изменился между командой и подтверждением — используем свежий GET."""
    svc, tms, _ = make_service(tmp_db, cfg_live)
    await svc.handle(session_id="s1", message_id=0, is_new=False, utterance="остаток 789868")
    await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="спиши 3")
    # пока ждали «да», остаток стал 70
    tms.products[789868]["stock"] = 70
    r = await svc.handle(session_id="s1", message_id=2, is_new=False, utterance="да")
    assert tms.products[789868]["stock"] == 67
    assert tms.patch_calls[0]["add"] == -3
    assert "67" in r.text


@pytest.mark.asyncio
async def test_twenty_five_decrease(tmp_db, cfg_live):
    svc, tms, _ = make_service(
        tmp_db, cfg_live, products={789868: make_product(stock=100)}
    )
    await svc.handle(session_id="s1", message_id=0, is_new=False, utterance="остаток 789868")
    r = await svc.handle(
        session_id="s1", message_id=1, is_new=False, utterance="спиши двадцать пять"
    )
    assert "25" in r.text
    await svc.handle(session_id="s1", message_id=2, is_new=False, utterance="да")
    assert tms.products[789868]["stock"] == 75


@pytest.mark.asyncio
async def test_decrease_with_explicit_sku(tmp_db, cfg_live):
    svc, tms, _ = make_service(tmp_db, cfg_live)
    r = await svc.handle(
        session_id="s1",
        message_id=1,
        is_new=False,
        utterance="спиши 2 с артикула 789868",
    )
    assert "2" in r.text and "789868" in r.text
    await svc.handle(session_id="s1", message_id=2, is_new=False, utterance="да")
    assert tms.products[789868]["stock"] == 72


@pytest.mark.asyncio
async def test_increase_to_sku(tmp_db, cfg_live):
    svc, tms, _ = make_service(tmp_db, cfg_live)
    r = await svc.handle(
        session_id="s1", message_id=1, is_new=False, utterance="добавь к 789868 три"
    )
    assert "Готово" in r.text
    assert tms.products[789868]["stock"] == 77


@pytest.mark.asyncio
async def test_explicit_sku_replaces_current(tmp_db, cfg_dry):
    products = {
        111111: make_product(sku=111111, stock=10, name="A"),
        222222: make_product(sku=222222, stock=20, name="B", address=None),
    }
    svc, tms, store = make_service(tmp_db, cfg_dry, products=products)
    await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="остаток 111111")
    assert store.get_current_sku("s1") == 111111
    r = await svc.handle(session_id="s1", message_id=2, is_new=False, utterance="остаток 222222")
    assert store.get_current_sku("s1") == 222222
    assert "20" in r.text


@pytest.mark.asyncio
async def test_sessions_isolated(tmp_db, cfg_dry):
    products = {
        111111: make_product(sku=111111, stock=10),
        222222: make_product(sku=222222, stock=20),
    }
    svc, tms, store = make_service(tmp_db, cfg_dry, products=products)
    await svc.handle(session_id="a", message_id=1, is_new=False, utterance="остаток 111111")
    await svc.handle(session_id="b", message_id=1, is_new=False, utterance="остаток 222222")
    assert store.get_current_sku("a") == 111111
    assert store.get_current_sku("b") == 222222


@pytest.mark.asyncio
async def test_pending_expired(tmp_db, cfg_live):
    cfg = AliceWarehouseConfig(
        tms_base_url="https://tmsdesk.ru",
        tms_api_token="t",
        tms_operator="Alice-Warehouse",
        dry_run=False,
        max_stock_change=1000,
        session_ttl_seconds=1800,
        pending_ttl_seconds=1,
        dedup_ttl_seconds=300,
        skill_id="",
        webhook_secret="",
    )
    svc, tms, store = make_service(tmp_db, cfg)
    await svc.handle(session_id="s1", message_id=0, is_new=False, utterance="остаток 789868")
    await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="спиши одну")
    # протушить pending
    row = tmp_db.alice_get_pending("s1")
    tmp_db.alice_set_pending(
        "s1",
        operation=row["operation"],
        sku=row["sku"],
        quantity=row["quantity"],
        operation_id=row["operation_id"],
        created_at=time.time() - 10,
    )
    r = await svc.handle(session_id="s1", message_id=2, is_new=False, utterance="да")
    assert "истекло" in r.text.lower()
    assert not tms.patch_calls


@pytest.mark.asyncio
async def test_one_more_clarifies(tmp_db, cfg_live):
    svc, tms, _ = make_service(tmp_db, cfg_live)
    await svc.handle(session_id="s1", message_id=0, is_new=False, utterance="остаток 789868")
    r = await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="ещё одну")
    assert "Уточните" in r.text
    assert not tms.patch_calls


@pytest.mark.asyncio
async def test_set_stock_requires_confirm(tmp_db, cfg_live):
    svc, tms, store = make_service(tmp_db, cfg_live)
    await svc.handle(session_id="s1", message_id=0, is_new=False, utterance="остаток 789868")
    r = await svc.handle(
        session_id="s1", message_id=1, is_new=False, utterance="поставь остаток 20"
    )
    assert "Установить" in r.text
    assert store.get_pending("s1") is not None
    assert not tms.patch_calls
    r2 = await svc.handle(session_id="s1", message_id=2, is_new=False, utterance="да")
    assert tms.products[789868]["stock"] == 20
    assert any(c.get("mode") == "absolute" for c in tms.patch_calls)


@pytest.mark.asyncio
async def test_max_stock_change(tmp_db, cfg_live):
    cfg = AliceWarehouseConfig(
        tms_base_url="https://tmsdesk.ru",
        tms_api_token="t",
        tms_operator="Alice-Warehouse",
        dry_run=False,
        max_stock_change=5,
        session_ttl_seconds=1800,
        pending_ttl_seconds=60,
        dedup_ttl_seconds=300,
        skill_id="",
        webhook_secret="",
    )
    svc, tms, _ = make_service(tmp_db, cfg)
    await svc.handle(session_id="s1", message_id=0, is_new=False, utterance="остаток 789868")
    r = await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="добавь 100")
    assert "Слишком большое" in r.text
    assert not tms.patch_calls


@pytest.mark.asyncio
async def test_unknown_no_patch(tmp_db, cfg_live):
    svc, tms, _ = make_service(tmp_db, cfg_live)
    r = await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="включи свет")
    assert "Не понял" in r.text
    assert not tms.patch_calls


@pytest.mark.asyncio
async def test_no_sku_for_decrease(tmp_db, cfg_live):
    svc, tms, _ = make_service(tmp_db, cfg_live)
    r = await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="спиши одну")
    assert "артикул" in r.text.lower()
    assert not tms.patch_calls


@pytest.mark.asyncio
async def test_new_command_cancels_pending(tmp_db, cfg_live):
    products = {
        789868: make_product(stock=74),
        123456: make_product(sku=123456, stock=5, address=None),
    }
    svc, tms, store = make_service(tmp_db, cfg_live, products=products)
    await svc.handle(session_id="s1", message_id=0, is_new=False, utterance="остаток 789868")
    await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="спиши 3")
    assert store.get_pending("s1") is not None
    r = await svc.handle(session_id="s1", message_id=2, is_new=False, utterance="остаток 123456")
    assert store.get_pending("s1") is None
    assert "5" in r.text
    # старое подтверждение не должно сработать
    r2 = await svc.handle(session_id="s1", message_id=3, is_new=False, utterance="да")
    assert "Нечего подтверждать" in r2.text
    assert tms.products[789868]["stock"] == 74


@pytest.mark.asyncio
async def test_greeting(tmp_db, cfg_dry):
    svc, _, _ = make_service(tmp_db, cfg_dry)
    r = await svc.handle(session_id="s1", message_id=0, is_new=True, utterance="")
    assert "Склад готов" in r.text


@pytest.mark.asyncio
async def test_address_null(tmp_db, cfg_dry):
    svc, _, _ = make_service(
        tmp_db, cfg_dry, products={789868: make_product(address=_NO_ADDRESS)}
    )
    r = await svc.handle(session_id="s1", message_id=1, is_new=False, utterance="где 789868")
    assert "не указан" in r.text.lower()
