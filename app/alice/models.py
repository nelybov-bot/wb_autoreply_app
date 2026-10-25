"""Модели интентов и результатов парсера команд Алисы."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


class Intent(str, Enum):
    GET_STOCK = "GET_STOCK"
    GET_LOCATION = "GET_LOCATION"
    SELECT_PRODUCT = "SELECT_PRODUCT"
    INCREASE_STOCK = "INCREASE_STOCK"
    DECREASE_STOCK = "DECREASE_STOCK"
    SET_STOCK = "SET_STOCK"
    CONFIRM = "CONFIRM"
    CANCEL = "CANCEL"
    CLARIFY = "CLARIFY"
    UNKNOWN = "UNKNOWN"


@dataclass
class ParsedCommand:
    intent: Intent
    sku: Optional[int] = None
    quantity: Optional[int] = None
    raw_text: str = ""
    ambiguous: bool = False


@dataclass
class VoiceReply:
    text: str
    end_session: bool = False
    session_state: Optional[dict[str, Any]] = None


@dataclass
class ProductView:
    sku: int
    name: str
    stock: Optional[int]
    address: Optional[dict[str, Any]]
    is_kit: bool
    raw: dict[str, Any]
