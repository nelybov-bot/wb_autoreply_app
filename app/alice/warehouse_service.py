"""Бизнес-логика голосовых операций склада."""
from __future__ import annotations

import logging
import re
import uuid
from typing import Any, Optional

from .command_parser import parse_command
from .config import AliceWarehouseConfig
from .models import Intent, ParsedCommand, ProductView, VoiceReply
from .session_store import AliceSessionStore
from .tms_client import TmsClient, TmsError

log = logging.getLogger("alice")

_ERROR_SPEECH = {
    "UNAUTHORIZED": "Нет доступа к складу. Проверьте настройки.",
    "FORBIDDEN_SCOPE": "Недостаточно прав для этой операции на складе.",
    "BAD_REQUEST": "Некорректный запрос к складу.",
    "NOT_FOUND": "Товар не найден.",
    "AMBIGUOUS_BARCODE": "Штрихкод неоднозначен. Укажите артикул.",
    "PRODUCT_WITHOUT_SKU": "У товара нет артикула.",
    "KIT_INHERITED": "Это комплект. Его остаток нельзя изменить напрямую.",
    "KIT_NOT_WRITABLE": "Это комплект. Его остаток нельзя изменить напрямую.",
    "STOCK_INVALID": "Некорректное значение остатка.",
    "STOCK_ROW_NOT_FOUND": "Для этого товара нет записи остатка на складе.",
    "STOCK_ROW_AMBIGUOUS": "Запись остатка неоднозначна. Нужна правка в TMS.",
    "STOCK_CLIENT_WITHOUT_USER": "Запись остатка не настроена на стороне склада.",
    "STOCK_INSUFFICIENT": "Списание невозможно: на складе недостаточно товара.",
    "STOCK_OPERATION_ID_INVALID": "Ошибка ключа операции. Повторите команду.",
    "RATE_LIMITED": "Склад временно перегружен. Повторите через минуту.",
    "INTERNAL_ERROR": "Не удалось выполнить операцию со складом.",
}


def _pieces_word(n: int) -> str:
    n_abs = abs(int(n)) % 100
    n1 = n_abs % 10
    if 11 <= n_abs <= 14:
        return "штук"
    if n1 == 1:
        return "штука"
    if 2 <= n1 <= 4:
        return "штуки"
    return "штук"


def _stock_phrase(n: int) -> str:
    return f"{n} {_pieces_word(n)}"


def _short_name(name: str, limit: int = 40) -> str:
    name = (name or "").strip()
    if not name:
        return ""
    if len(name) <= limit:
        return name
    return name[: limit - 1].rstrip() + "…"


def _format_address(address: Optional[dict]) -> str:
    if address is None:
        return "Адрес хранения не указан."
    parts = []
    rack = str(address.get("rack") or "").strip()
    shelf = str(address.get("shelf") or "").strip()
    place = str(address.get("place") or "").strip()
    cell = str(address.get("cell") or "").strip()
    # убрать служебные хвосты вроде (ПОЛ) для голоса
    shelf_voice = re.sub(r"\(.*?\)", "", shelf).strip() or shelf
    if rack:
        parts.append(f"стеллаж {rack}")
    if shelf_voice:
        parts.append(f"полка {shelf_voice}")
    if place:
        parts.append(f"место {place}")
    if cell:
        parts.append(f"ячейка {cell}")
    if not parts:
        return "Адрес хранения не указан."
    text = ", ".join(parts)
    return text[0].upper() + text[1:] + "."


def _product_from_data(data: dict[str, Any]) -> ProductView:
    return ProductView(
        sku=int(data["sku"]),
        name=str(data.get("name") or ""),
        stock=data.get("stock") if data.get("stock") is None else int(data["stock"]),
        address=data.get("address") if isinstance(data.get("address"), dict) else None,
        is_kit=bool(data.get("is_kit")),
        raw=data,
    )


def _speech_for_tms_error(err: TmsError) -> str:
    if err.code in ("KIT_INHERITED", "KIT_NOT_WRITABLE"):
        base = _ERROR_SPEECH[err.code]
        comp = None
        if err.details and err.details.get("component_sku") is not None:
            comp = err.details.get("component_sku")
        if comp is not None:
            return f"{base} Компонент — артикул {comp}."
        return base
    if err.code == "STOCK_INSUFFICIENT":
        current = (err.details or {}).get("current")
        if current is not None:
            return f"Списание невозможно. На складе только {_stock_phrase(int(current))}."
    return _ERROR_SPEECH.get(err.code, "Не удалось выполнить операцию со складом.")


class WarehouseService:
    def __init__(
        self,
        *,
        config: AliceWarehouseConfig,
        store: AliceSessionStore,
        tms: TmsClient,
    ) -> None:
        self.cfg = config
        self.store = store
        self.tms = tms

    def _reply(self, text: str, *, end: bool = False, state: Optional[dict] = None) -> VoiceReply:
        return VoiceReply(text=text, end_session=end, session_state=state)

    def _dedup_key(self, session_id: str, message_id: Any, request_id: str) -> str:
        return f"{session_id}:{message_id}:{request_id}"

    def _make_operation_id(self, session_id: str, request_id: str) -> str:
        # Только [A-Za-z0-9._:-], <=64
        raw = f"alice-{session_id[:12]}-{request_id[:20]}"
        cleaned = re.sub(r"[^A-Za-z0-9._:-]", "-", raw)[:64]
        return cleaned or f"alice-{uuid.uuid4().hex[:20]}"

    async def handle(
        self,
        *,
        session_id: str,
        message_id: Any,
        is_new: bool,
        utterance: str,
        request_id: Optional[str] = None,
        nlu_numbers: Optional[list[int]] = None,
    ) -> VoiceReply:
        rid = (request_id or f"{session_id}-{message_id}").strip()
        if is_new and not (utterance or "").strip():
            return self._reply("Склад готов. Назовите артикул или команду.")

        if is_new and not utterance.strip():
            return self._reply("Склад готов. Назовите артикул или команду.")

        # Приветствие новой сессии с пустой/служебной командой
        cmd_norm = (utterance or "").strip().lower()
        if is_new and cmd_norm in ("", "запусти навык", "запустить навык"):
            return self._reply("Склад готов. Назовите артикул или команду.")

        parsed = parse_command(utterance, nlu_numbers=nlu_numbers or [])
        log.info(
            "alice_command session=%s request_id=%s intent=%s sku=%s quantity=%s",
            session_id,
            rid,
            parsed.intent.value,
            parsed.sku,
            parsed.quantity,
        )

        # Новая команда при pending (не confirm/cancel) — отменить pending.
        # Confirm/cancel не трогаем здесь: иначе истёкший pending очистится до проверки TTL.
        if parsed.intent not in (Intent.CONFIRM, Intent.CANCEL):
            pending = self.store.get_pending(session_id)
            if pending:
                self.store.clear_pending(session_id)
                log.info("alice_pending_cancelled_by_new_command session=%s", session_id)

        try:
            if parsed.intent == Intent.CONFIRM:
                return await self._handle_confirm(session_id, rid, message_id)
            if parsed.intent == Intent.CANCEL:
                return self._handle_cancel(session_id)
            if parsed.intent == Intent.CLARIFY:
                qty = parsed.quantity or 1
                return self._reply(
                    f"Уточните: добавить или списать {_stock_phrase(qty)}?"
                )
            if parsed.intent == Intent.UNKNOWN:
                return self._reply(
                    "Не понял команду. Например: остаток 789868 или спиши две."
                )

            sku = parsed.sku if parsed.sku is not None else self.store.get_current_sku(session_id)
            if parsed.sku is not None:
                self.store.set_current_sku(session_id, parsed.sku)
                sku = parsed.sku

            if parsed.intent == Intent.SELECT_PRODUCT:
                if sku is None:
                    return self._reply("Сначала назовите артикул.")
                return await self._handle_get_stock(session_id, rid, sku)

            if parsed.intent == Intent.GET_STOCK:
                if sku is None:
                    return self._reply("Сначала назовите артикул.")
                return await self._handle_get_stock(session_id, rid, sku)

            if parsed.intent == Intent.GET_LOCATION:
                if sku is None:
                    return self._reply("Сначала назовите артикул.")
                return await self._handle_get_location(session_id, rid, sku)

            if parsed.intent == Intent.INCREASE_STOCK:
                return await self._handle_increase(session_id, rid, message_id, sku, parsed)

            if parsed.intent == Intent.DECREASE_STOCK:
                return await self._handle_decrease_request(session_id, rid, sku, parsed)

            if parsed.intent == Intent.SET_STOCK:
                return await self._handle_set_request(session_id, rid, sku, parsed)

            return self._reply(
                "Не понял команду. Например: остаток 789868 или спиши две."
            )
        except TmsError as e:
            log.warning(
                "alice_tms_error session=%s request_id=%s code=%s http=%s",
                session_id,
                rid,
                e.code,
                e.http_status,
            )
            self.store.write_audit(
                session_id=session_id,
                request_id=rid,
                sku=parsed.sku,
                operation=parsed.intent.value,
                quantity=parsed.quantity,
                stock_before=None,
                stock_after=None,
                status="error",
                error_code=e.code,
                dry_run=self.cfg.dry_run,
            )
            return self._reply(_speech_for_tms_error(e))
        except Exception:
            log.exception("alice_unhandled session=%s request_id=%s", session_id, rid)
            return self._reply("Не удалось выполнить операцию со складом.")

    async def _fetch_product(self, sku: int) -> ProductView:
        data = await self.tms.get_product_by_sku(sku)
        return _product_from_data(data)

    async def _handle_get_stock(self, session_id: str, rid: str, sku: int) -> VoiceReply:
        product = await self._fetch_product(sku)
        self.store.set_current_sku(session_id, product.sku)
        if product.stock is None:
            return self._reply("Для этого товара нет данных об остатке.")
        name = _short_name(product.name)
        stock_txt = _stock_phrase(product.stock)
        if name and len(name) <= 30:
            return self._reply(f"{name}. Остаток {stock_txt}.")
        return self._reply(f"Остаток {stock_txt}.")

    async def _handle_get_location(self, session_id: str, rid: str, sku: int) -> VoiceReply:
        product = await self._fetch_product(sku)
        self.store.set_current_sku(session_id, product.sku)
        return self._reply(_format_address(product.address))

    def _need_sku(self, sku: Optional[int]) -> Optional[VoiceReply]:
        if sku is None:
            return self._reply("Сначала назовите артикул.")
        return None

    def _need_qty(self, qty: Optional[int]) -> Optional[VoiceReply]:
        if qty is None or qty <= 0:
            return self._reply("Укажите количество. Например: добавь две.")
        return None

    async def _handle_increase(
        self,
        session_id: str,
        rid: str,
        message_id: Any,
        sku: Optional[int],
        parsed: ParsedCommand,
    ) -> VoiceReply:
        miss = self._need_sku(sku)
        if miss:
            return miss
        miss = self._need_qty(parsed.quantity)
        if miss:
            return miss
        assert sku is not None and parsed.quantity is not None
        qty = int(parsed.quantity)
        if qty > self.cfg.max_stock_change:
            return self._reply("Слишком большое изменение. Выполните его вручную.")

        product = await self._fetch_product(sku)
        self.store.set_current_sku(session_id, product.sku)
        if product.is_kit:
            return self._reply("Это комплект. Его остаток нельзя изменить напрямую.")
        if product.stock is None:
            return self._reply("Для этого товара нет данных об остатке.")

        before = int(product.stock)
        after = before + qty
        return await self._apply_delta(
            session_id=session_id,
            request_id=rid,
            message_id=message_id,
            sku=product.sku,
            delta=qty,
            stock_before=before,
            operation="increase",
        )

    async def _handle_decrease_request(
        self,
        session_id: str,
        rid: str,
        sku: Optional[int],
        parsed: ParsedCommand,
    ) -> VoiceReply:
        miss = self._need_sku(sku)
        if miss:
            return miss
        miss = self._need_qty(parsed.quantity)
        if miss:
            return miss
        assert sku is not None and parsed.quantity is not None
        qty = int(parsed.quantity)
        if qty > self.cfg.max_stock_change:
            return self._reply("Слишком большое изменение. Выполните его вручную.")

        product = await self._fetch_product(sku)
        self.store.set_current_sku(session_id, product.sku)
        if product.is_kit:
            return self._reply("Это комплект. Его остаток нельзя изменить напрямую.")
        if product.stock is None:
            return self._reply("Для этого товара нет данных об остатке.")

        before = int(product.stock)
        if before < qty:
            return self._reply(
                f"Списание невозможно. На складе только {_stock_phrase(before)}."
            )

        op_id = self._make_operation_id(session_id, rid)
        self.store.set_pending(
            session_id,
            operation="decrease",
            sku=product.sku,
            quantity=qty,
            operation_id=op_id,
        )
        return self._reply(
            f"Списать {_stock_phrase(qty)} артикула {product.sku}? Сейчас остаток {before}."
        )

    async def _handle_set_request(
        self,
        session_id: str,
        rid: str,
        sku: Optional[int],
        parsed: ParsedCommand,
    ) -> VoiceReply:
        miss = self._need_sku(sku)
        if miss:
            return miss
        if parsed.quantity is None or parsed.quantity < 0:
            return self._reply("Укажите новый остаток. Например: поставь остаток 20.")
        assert sku is not None
        new_stock = int(parsed.quantity)

        product = await self._fetch_product(sku)
        self.store.set_current_sku(session_id, product.sku)
        if product.is_kit:
            return self._reply("Это комплект. Его остаток нельзя изменить напрямую.")
        if product.stock is None:
            return self._reply("Для этого товара нет данных об остатке.")

        before = int(product.stock)
        delta = abs(new_stock - before)
        if delta > self.cfg.max_stock_change:
            return self._reply("Слишком большое изменение. Выполните его вручную.")

        op_id = self._make_operation_id(session_id, rid)
        self.store.set_pending(
            session_id,
            operation="set",
            sku=product.sku,
            quantity=new_stock,
            operation_id=op_id,
        )
        return self._reply(
            f"Установить остаток артикула {product.sku} равным {new_stock}? Сейчас {before}."
        )

    def _handle_cancel(self, session_id: str) -> VoiceReply:
        pending = self.store.get_pending(session_id)
        if not pending:
            return self._reply("Нечего отменять.")
        self.store.clear_pending(session_id)
        if pending.operation == "set":
            return self._reply("Установка остатка отменена.")
        return self._reply("Списание отменено.")

    async def _handle_confirm(
        self,
        session_id: str,
        rid: str,
        message_id: Any,
    ) -> VoiceReply:
        status = self.store.peek_pending_status(session_id)
        if status == "none":
            return self._reply("Нечего подтверждать.")
        if status == "expired":
            return self._reply("Время подтверждения истекло. Повторите команду.")
        pending = self.store.get_pending(session_id)
        if pending is None:
            return self._reply("Время подтверждения истекло. Повторите команду.")

        sku = pending.sku
        qty = pending.quantity
        operation = pending.operation
        op_id = pending.operation_id or self._make_operation_id(session_id, rid)

        # Свежий GET перед любой записью
        product = await self._fetch_product(sku)
        if product.is_kit:
            self.store.clear_pending(session_id)
            return self._reply("Это комплект. Его остаток нельзя изменить напрямую.")
        if product.stock is None:
            self.store.clear_pending(session_id)
            return self._reply("Для этого товара нет данных об остатке.")

        fresh = int(product.stock)

        if operation == "decrease":
            if fresh < qty:
                self.store.clear_pending(session_id)
                return self._reply(
                    f"Списание невозможно. На складе только {_stock_phrase(fresh)}."
                )
            if qty > self.cfg.max_stock_change:
                self.store.clear_pending(session_id)
                return self._reply("Слишком большое изменение. Выполните его вручную.")
            self.store.clear_pending(session_id)
            return await self._apply_delta(
                session_id=session_id,
                request_id=rid,
                message_id=message_id,
                sku=sku,
                delta=-qty,
                stock_before=fresh,
                operation="decrease",
                operation_id=op_id,
            )

        if operation == "set":
            delta = abs(qty - fresh)
            if delta > self.cfg.max_stock_change:
                self.store.clear_pending(session_id)
                return self._reply("Слишком большое изменение. Выполните его вручную.")
            self.store.clear_pending(session_id)
            return await self._apply_absolute(
                session_id=session_id,
                request_id=rid,
                message_id=message_id,
                sku=sku,
                new_stock=qty,
                stock_before=fresh,
                operation_id=op_id,
            )

        self.store.clear_pending(session_id)
        return self._reply("Нечего подтверждать.")

    async def _apply_delta(
        self,
        *,
        session_id: str,
        request_id: str,
        message_id: Any,
        sku: int,
        delta: int,
        stock_before: int,
        operation: str,
        operation_id: Optional[str] = None,
    ) -> VoiceReply:
        dedup = self._dedup_key(session_id, message_id, request_id)
        if not self.store.try_claim_dedup(dedup):
            log.info("alice_dedup_hit session=%s request_id=%s", session_id, request_id)
            return self._reply("Команда уже обработана.")

        after_expected = stock_before + delta
        op_id = operation_id or self._make_operation_id(session_id, request_id)

        if self.cfg.dry_run:
            log.info(
                "alice_stock_change sku=%s operation=%s before=%s after=%s operator=%s dry_run=1",
                sku,
                operation,
                stock_before,
                after_expected,
                self.cfg.tms_operator,
            )
            self.store.write_audit(
                session_id=session_id,
                request_id=request_id,
                sku=sku,
                operation=operation,
                quantity=abs(delta),
                stock_before=stock_before,
                stock_after=after_expected,
                status="dry_run",
                dry_run=True,
            )
            return self._reply(
                f"Тестовый режим. Остаток был бы изменён с {stock_before} на {after_expected}."
            )

        try:
            # Приращение — race-safe на стороне TMS (docs § stock.add)
            updated = await self.tms.patch_stock_add(sku, delta, op_id)
            stock_after = updated.get("stock")
            if stock_after is None:
                stock_after = after_expected
            else:
                stock_after = int(stock_after)
            log.info(
                "alice_stock_change sku=%s operation=%s before=%s after=%s operator=%s",
                sku,
                operation,
                stock_before,
                stock_after,
                self.cfg.tms_operator,
            )
            self.store.write_audit(
                session_id=session_id,
                request_id=request_id,
                sku=sku,
                operation=operation,
                quantity=abs(delta),
                stock_before=stock_before,
                stock_after=stock_after,
                status="ok",
                dry_run=False,
            )
            return self._reply(f"Готово. Теперь {_stock_phrase(stock_after)}.")
        except TmsError as e:
            # не отпускаем dedup при успехе; при ошибке можно отпустить, чтобы повтор имел смысл
            self.store.release_dedup(dedup)
            raise

    async def _apply_absolute(
        self,
        *,
        session_id: str,
        request_id: str,
        message_id: Any,
        sku: int,
        new_stock: int,
        stock_before: int,
        operation_id: str,
    ) -> VoiceReply:
        dedup = self._dedup_key(session_id, message_id, request_id)
        if not self.store.try_claim_dedup(dedup):
            return self._reply("Команда уже обработана.")

        if self.cfg.dry_run:
            log.info(
                "alice_stock_change sku=%s operation=set before=%s after=%s operator=%s dry_run=1",
                sku,
                stock_before,
                new_stock,
                self.cfg.tms_operator,
            )
            self.store.write_audit(
                session_id=session_id,
                request_id=request_id,
                sku=sku,
                operation="set",
                quantity=new_stock,
                stock_before=stock_before,
                stock_after=new_stock,
                status="dry_run",
                dry_run=True,
            )
            return self._reply(
                f"Тестовый режим. Остаток был бы изменён с {stock_before} на {new_stock}."
            )

        try:
            updated = await self.tms.patch_stock_absolute(sku, new_stock)
            stock_after = updated.get("stock")
            stock_after = int(stock_after) if stock_after is not None else new_stock
            log.info(
                "alice_stock_change sku=%s operation=set before=%s after=%s operator=%s",
                sku,
                stock_before,
                stock_after,
                self.cfg.tms_operator,
            )
            self.store.write_audit(
                session_id=session_id,
                request_id=request_id,
                sku=sku,
                operation="set",
                quantity=new_stock,
                stock_before=stock_before,
                stock_after=stock_after,
                status="ok",
                dry_run=False,
            )
            return self._reply(f"Готово. Остаток {_stock_phrase(stock_after)}.")
        except TmsError:
            self.store.release_dedup(dedup)
            raise
