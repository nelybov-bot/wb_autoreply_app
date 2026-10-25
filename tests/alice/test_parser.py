from __future__ import annotations

import pytest

from app.alice.command_parser import parse_command
from app.alice.models import Intent
from app.alice.numbers_ru import parse_russian_number


@pytest.mark.parametrize(
    "text,intent,sku,qty",
    [
        ("остаток 789868", Intent.GET_STOCK, 789868, None),
        ("сколько осталось 789868", Intent.GET_STOCK, 789868, None),
        ("где лежит 789868", Intent.GET_LOCATION, 789868, None),
        ("добавь одну", Intent.INCREASE_STOCK, None, 1),
        ("плюсани две", Intent.INCREASE_STOCK, None, 2),
        ("спиши одну", Intent.DECREASE_STOCK, None, 1),
        ("да", Intent.CONFIRM, None, None),
        ("нет", Intent.CANCEL, None, None),
        ("двадцать пять", Intent.UNKNOWN, None, None),  # само по себе не команда
        ("спиши двадцать пять", Intent.DECREASE_STOCK, None, 25),
        ("спиши 2 с артикула 789868", Intent.DECREASE_STOCK, 789868, 2),
        ("добавь к 789868 три", Intent.INCREASE_STOCK, 789868, 3),
        ("ещё одну", Intent.CLARIFY, None, 1),
        ("поставь остаток 20", Intent.SET_STOCK, None, 20),
        ("установи остаток 20", Intent.SET_STOCK, None, 20),
        ("че там по 789868", Intent.GET_STOCK, 789868, None),
        ("789868", Intent.SELECT_PRODUCT, 789868, None),
        ("блаблабла", Intent.UNKNOWN, None, None),
    ],
)
def test_parse_commands(text, intent, sku, qty):
    p = parse_command(text)
    assert p.intent == intent
    assert p.sku == sku
    assert p.quantity == qty


def test_russian_numbers():
    assert parse_russian_number("двадцать пять") == 25
    assert parse_russian_number("сто") == 100
    assert parse_russian_number("одиннадцать") == 11
    assert parse_russian_number("две") == 2


def test_spoken_sku_without_thousands():
    """«пятьсот сорок девять семьсот сорок восемь» → 549748, без слова тысяч."""
    from app.alice.numbers_ru import extract_spoken_sku

    sku, _ = extract_spoken_sku("остаток пятьсот сорок девять семьсот сорок восемь")
    assert sku == 549748

    p = parse_command("остаток пятьсот сорок девять семьсот сорок восемь")
    assert p.intent == Intent.GET_STOCK
    assert p.sku == 549748


def test_spoken_sku_with_thousands():
    from app.alice.numbers_ru import extract_spoken_sku

    sku, _ = extract_spoken_sku("пятьсот сорок девять тысяч семьсот сорок восемь")
    assert sku == 549748


def test_spoken_sku_digit_by_digit():
    from app.alice.numbers_ru import extract_spoken_sku

    sku, _ = extract_spoken_sku("пять четыре девять семь четыре восемь")
    assert sku == 549748


def test_nlu_number_groups():
    p = parse_command("остаток артикула", nlu_numbers=[549, 748])
    assert p.intent == Intent.GET_STOCK
    assert p.sku == 549748
