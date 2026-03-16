"""
UI диалоги: магазин WB, OpenAI ключ, редактирование промптов.
"""
from __future__ import annotations

from dataclasses import dataclass
from tkinter import filedialog, messagebox
from typing import Optional
import tkinter.ttk as ttk
import customtkinter as ctk

from ..db import Database

@dataclass
class StoreFormResult:
    name: str
    api_key: str
    active: bool
    marketplace: str = "wb"
    business_id: Optional[int] = None
    client_id: Optional[str] = None

def pick_openai_key_file(parent) -> str | None:
    path = filedialog.askopenfilename(
        parent=parent,
        title="Выберите файл с OpenAI API ключом",
        filetypes=[("Text", "*.txt"), ("All", "*.*")]
    )
    return path or None

def read_openai_key_from_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        content = f.read().strip()
    if not content:
        raise ValueError("Файл пустой")
    # поддержка формата OPENAI_API_KEY=...
    if "OPENAI_API_KEY" in content and "=" in content:
        for line in content.splitlines():
            line = line.strip()
            if line.startswith("OPENAI_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return content.strip().strip('"').strip("'")

class StoreDialog(ctk.CTkToplevel):
    def __init__(
        self,
        parent,
        *,
        title: str,
        initial_name: str = "",
        initial_key: str = "",
        initial_active: bool = True,
        initial_marketplace: str = "wb",
        initial_business_id: Optional[int] = None,
        initial_client_id: Optional[str] = None,
    ):
        super().__init__(parent)
        self.title(title)
        self.geometry("520x360")
        self.resizable(False, False)
        self.result: StoreFormResult | None = None

        self.grid_columnconfigure(1, weight=1)

        row = 0
        ctk.CTkLabel(self, text="Маркетплейс").grid(row=row, column=0, padx=16, pady=(16, 8), sticky="w")
        _mp_map = {"wb": "WB", "yam": "Яндекс Маркет", "ozon": "Ozon"}
        _mp_val = _mp_map.get((initial_marketplace or "wb"), "WB")
        self.var_marketplace = ctk.StringVar(value=_mp_val)
        self.combo_marketplace = ctk.CTkComboBox(
            self,
            variable=self.var_marketplace,
            values=["WB", "Яндекс Маркет", "Ozon"],
            command=self._on_marketplace_change,
            width=200,
        )
        self.combo_marketplace.grid(row=row, column=1, padx=16, pady=(16, 8), sticky="w")
        row += 1

        ctk.CTkLabel(self, text="Название магазина").grid(row=row, column=0, padx=16, pady=8, sticky="w")
        self.e_name = ctk.CTkEntry(self)
        self.e_name.grid(row=row, column=1, padx=16, pady=8, sticky="ew")
        self.e_name.insert(0, initial_name)
        row += 1

        self.lbl_key = ctk.CTkLabel(self, text="API ключ")
        self.lbl_key.grid(row=row, column=0, padx=16, pady=8, sticky="w")
        self.e_key = ctk.CTkEntry(self, show="*")
        self.e_key.grid(row=row, column=1, padx=16, pady=8, sticky="ew")
        self.e_key.insert(0, initial_key)
        row += 1

        self.frame_business = ctk.CTkFrame(self, fg_color="transparent")
        self.lbl_business = ctk.CTkLabel(self.frame_business, text="Business ID (кабинет)")
        self.lbl_business.grid(row=0, column=0, padx=16, pady=8, sticky="w")
        self.e_business_id = ctk.CTkEntry(self.frame_business, placeholder_text="Только цифры")
        self.e_business_id.grid(row=0, column=1, padx=16, pady=8, sticky="ew")
        if initial_business_id is not None and initial_business_id != 0:
            self.e_business_id.insert(0, str(initial_business_id))
        self.frame_business.grid(row=row, column=0, columnspan=2, sticky="ew")
        self.frame_business.grid_columnconfigure(1, weight=1)
        row += 1

        self.frame_client = ctk.CTkFrame(self, fg_color="transparent")
        ctk.CTkLabel(self.frame_client, text="Client-Id").grid(row=0, column=0, padx=16, pady=8, sticky="w")
        self.e_client_id = ctk.CTkEntry(self.frame_client, placeholder_text="Client-Id из личного кабинета Ozon")
        self.e_client_id.grid(row=0, column=1, padx=16, pady=8, sticky="ew")
        if initial_client_id:
            self.e_client_id.insert(0, initial_client_id)
        self.frame_client.grid(row=row, column=0, columnspan=2, sticky="ew")
        self.frame_client.grid_columnconfigure(1, weight=1)
        self.frame_client.grid_remove()
        row += 1

        self.var_active = ctk.BooleanVar(value=initial_active)
        ctk.CTkCheckBox(self, text="Активен", variable=self.var_active).grid(row=row, column=1, padx=16, pady=8, sticky="w")
        row += 1

        btns = ctk.CTkFrame(self, fg_color="transparent")
        btns.grid(row=row, column=0, columnspan=2, padx=16, pady=(16, 16), sticky="ew")
        btns.grid_columnconfigure((0, 1), weight=1)
        ctk.CTkButton(btns, text="Отмена", command=self._cancel).grid(row=0, column=0, padx=(0, 8), sticky="ew")
        ctk.CTkButton(btns, text="Сохранить", command=self._save).grid(row=0, column=1, padx=(8, 0), sticky="ew")

        self._on_marketplace_change(None)

        self.grab_set()
        self.focus_force()

    def _on_marketplace_change(self, _) -> None:
        mp = self.var_marketplace.get()
        if mp == "Яндекс Маркет":
            self.lbl_key.configure(text="API ключ (Яндекс Маркет)")
            self.frame_business.grid()
            self.frame_client.grid_remove()
        elif mp == "Ozon":
            self.lbl_key.configure(text="Api-Key (Ozon)")
            self.frame_business.grid_remove()
            self.frame_client.grid()
        else:
            self.lbl_key.configure(text="WB API ключ")
            self.frame_business.grid_remove()
            self.frame_client.grid_remove()

    def _cancel(self):
        self.result = None
        self.destroy()

    def _save(self):
        name = self.e_name.get().strip()
        key = self.e_key.get().strip().replace("\n", "").replace("\r", "").replace(" ", "")
        active = bool(self.var_active.get())
        mp_display = self.var_marketplace.get().strip() or "WB"
        if mp_display == "Яндекс Маркет":
            marketplace = "yam"
        elif mp_display == "Ozon":
            marketplace = "ozon"
        else:
            marketplace = "wb"

        if not name or not key:
            messagebox.showerror("Ошибка", "Название и ключ обязательны.", parent=self)
            return

        business_id: Optional[int] = None
        client_id: Optional[str] = None
        if marketplace == "yam":
            bid_str = self.e_business_id.get().strip().replace(" ", "")
            if not bid_str:
                messagebox.showerror("Ошибка", "Для Яндекс Маркета укажите Business ID (ID кабинета). Только цифры.", parent=self)
                return
            if not bid_str.isdigit():
                messagebox.showerror("Ошибка", "Business ID должен состоять только из цифр.", parent=self)
                return
            business_id = int(bid_str)
        elif marketplace == "ozon":
            client_id = self.e_client_id.get().strip().replace("\n", "").replace("\r", "")
            if not client_id:
                messagebox.showerror("Ошибка", "Для Ozon укажите Client-Id из личного кабинета продавца.", parent=self)
                return

        if marketplace == "wb":
            if key.count(".") != 2:
                messagebox.showwarning(
                    "Формат ключа WB",
                    "Ключ WB должен быть в формате JWT (три части через точку).\n\n"
                    "Сейчас формат не совпадает — возможно, ключ обрезан или скопирован неверно.\n\n"
                    "Где взять: Личный кабинет продавца WB → Настройки → Доступ к API → "
                    "токен с доступом к отзывам/вопросам. Копируйте ключ целиком, без пробелов и переносов.",
                    parent=self,
                )
        self.result = StoreFormResult(name=name, api_key=key, active=active, marketplace=marketplace, business_id=business_id, client_id=client_id)
        self.destroy()


# ---------- Telegram ----------
class TelegramDialog(ctk.CTkToplevel):
    """Настройки бота Telegram: токен и ID чата. Отзывы 1–3 звёзд отправляются в этот чат."""
    def __init__(self, parent, db: Database):
        super().__init__(parent)
        self.title("Telegram — уведомления об отзывах 1–3")
        self.geometry("560x280")
        self.resizable(False, False)
        self._db = db

        self.grid_columnconfigure(1, weight=1)

        ctk.CTkLabel(
            self,
            text="Сюда отправляются новые отзывы с оценкой 1, 2 или 3 (название товара + текст).\nЗаполните оба поля или оставьте пустыми, чтобы отключить.",
            font=ctk.CTkFont(size=12),
            anchor="w",
        ).grid(row=0, column=0, columnspan=2, padx=16, pady=(16, 12), sticky="ew")

        ctk.CTkLabel(self, text="Токен бота").grid(row=1, column=0, padx=16, pady=8, sticky="w")
        self.e_token = ctk.CTkEntry(self, show="*", placeholder_text="123456:ABC...")
        self.e_token.grid(row=1, column=1, padx=16, pady=8, sticky="ew")
        self.e_token.insert(0, db.get_setting("telegram_bot_token"))

        ctk.CTkLabel(self, text="ID чата (беседы)").grid(row=2, column=0, padx=16, pady=8, sticky="w")
        self.e_chat = ctk.CTkEntry(self, placeholder_text="-1001234567890 или 123456789")
        self.e_chat.grid(row=2, column=1, padx=16, pady=8, sticky="ew")
        self.e_chat.insert(0, db.get_setting("telegram_chat_id"))

        btns = ctk.CTkFrame(self, fg_color="transparent")
        btns.grid(row=3, column=0, columnspan=2, padx=16, pady=(16, 16), sticky="ew")
        btns.grid_columnconfigure((0, 1), weight=1)
        ctk.CTkButton(btns, text="Отмена", command=self.destroy).grid(row=0, column=0, padx=(0, 8), sticky="ew")
        ctk.CTkButton(btns, text="Сохранить", command=self._save).grid(row=0, column=1, padx=(8, 0), sticky="ew")

        self.grab_set()
        self.focus_force()

    def _save(self) -> None:
        token = self.e_token.get().strip().replace("\n", "").replace("\r", "")
        chat_id = self.e_chat.get().strip().replace("\n", "").replace("\r", "")
        self._db.set_setting("telegram_bot_token", token)
        self._db.set_setting("telegram_chat_id", chat_id)
        messagebox.showinfo("Готово", "Настройки Telegram сохранены.", parent=self)
        self.destroy()


# ---------- Промпты ----------
@dataclass
class PromptFormResult:
    item_type: str
    rating_group: str
    prompt_text: str


class PromptEditDialog(ctk.CTkToplevel):
    """Диалог добавления/редактирования одного промпта."""
    def __init__(
        self,
        parent,
        *,
        title: str,
        initial_type: str = "review",
        initial_group: str = "general",
        initial_text: str = "",
    ):
        super().__init__(parent)
        self.title(title)
        self.geometry("640x420")
        self.resizable(True, True)
        self.result: PromptFormResult | None = None

        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(2, weight=1)

        ctk.CTkLabel(self, text="Тип").grid(row=0, column=0, padx=16, pady=(16, 8), sticky="w")
        self.type_var = ctk.StringVar(value=initial_type)
        self.combo_type = ctk.CTkComboBox(
            self, variable=self.type_var, values=["review", "question"], width=180
        )
        self.combo_type.grid(row=0, column=1, padx=16, pady=(16, 8), sticky="w")

        ctk.CTkLabel(self, text="Группа оценки").grid(row=1, column=0, padx=16, pady=8, sticky="w")
        self.e_group = ctk.CTkEntry(self, placeholder_text="1, 2, 3, 4-5, general...")
        self.e_group.grid(row=1, column=1, padx=16, pady=8, sticky="ew")
        self.e_group.insert(0, initial_group)

        ctk.CTkLabel(self, text="Текст промпта").grid(row=2, column=0, padx=16, pady=(8, 4), sticky="nw")
        self.txt_prompt = ctk.CTkTextbox(self)
        self.txt_prompt.grid(row=2, column=1, padx=16, pady=(8, 8), sticky="nsew")
        self.txt_prompt.insert("1.0", initial_text)

        btns = ctk.CTkFrame(self, fg_color="transparent")
        btns.grid(row=3, column=0, columnspan=2, padx=16, pady=(0, 16), sticky="ew")
        btns.grid_columnconfigure((0, 1), weight=1)
        ctk.CTkButton(btns, text="Отмена", command=self._cancel).grid(row=0, column=0, padx=(0, 8), sticky="ew")
        ctk.CTkButton(btns, text="Сохранить", command=self._save).grid(row=0, column=1, padx=(8, 0), sticky="ew")

        self.grab_set()
        self.focus_force()

    def _cancel(self) -> None:
        self.result = None
        self.destroy()

    def _save(self) -> None:
        item_type = self.type_var.get().strip() or "review"
        rating_group = self.e_group.get().strip() or "general"
        prompt_text = self.txt_prompt.get("1.0", "end").strip()
        if not prompt_text:
            messagebox.showerror("Ошибка", "Текст промпта не может быть пустым.", parent=self)
            return
        self.result = PromptFormResult(item_type=item_type, rating_group=rating_group, prompt_text=prompt_text)
        self.destroy()


class PromptsWindow(ctk.CTkToplevel):
    """Окно списка промптов: просмотр, добавление, редактирование, удаление."""
    def __init__(self, parent, db):
        super().__init__(parent)
        self.title("Промпты")
        self.geometry("820x480")
        self.resizable(True, True)
        self._db = db

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.grid(row=0, column=0, padx=16, pady=(16, 8), sticky="ew")
        btn_row.grid_columnconfigure(0, weight=1)
        ctk.CTkButton(btn_row, text="Добавить", command=self._add).grid(row=0, column=0, padx=(0, 8), sticky="w")
        ctk.CTkButton(btn_row, text="Редактировать", command=self._edit).grid(row=0, column=1, padx=8, sticky="w")
        ctk.CTkButton(btn_row, text="Удалить", command=self._delete).grid(row=0, column=2, padx=8, sticky="w")

        cols = ("id", "type", "group", "text_preview")
        self.tree = ttk.Treeview(self, columns=cols, show="headings", selectmode="browse", height=20)
        self.tree.heading("id", text="ID")
        self.tree.heading("type", text="Тип")
        self.tree.heading("group", text="Группа")
        self.tree.heading("text_preview", text="Промпт (начало)")
        self.tree.column("id", width=60)
        self.tree.column("type", width=80)
        self.tree.column("group", width=100)
        self.tree.column("text_preview", width=520)
        self.tree.grid(row=1, column=0, padx=16, pady=(0, 16), sticky="nsew")

        self._reload()

    def _reload(self) -> None:
        for iid in self.tree.get_children():
            self.tree.delete(iid)
        for row in self._db.list_prompts():
            preview = (row.prompt_text or "").replace("\n", " ")[:80]
            self.tree.insert("", "end", iid=str(row.id), values=(
                row.id, row.item_type, row.rating_group, preview
            ))

    def _selected_id(self) -> int | None:
        sel = self.tree.selection()
        if not sel:
            return None
        return int(sel[0])

    def _add(self) -> None:
        d = PromptEditDialog(self, title="Добавить промпт", initial_type="review", initial_group="general", initial_text="")
        self.wait_window(d)
        if d.result:
            self._db.add_prompt(d.result.item_type, d.result.rating_group, d.result.prompt_text)
            self._reload()

    def _edit(self) -> None:
        pid = self._selected_id()
        if pid is None:
            messagebox.showinfo("Нет выбора", "Выберите строку в таблице.", parent=self)
            return
        prompts = [p for p in self._db.list_prompts() if p.id == pid]
        if not prompts:
            return
        row = prompts[0]
        d = PromptEditDialog(
            self,
            title="Редактировать промпт",
            initial_type=row.item_type,
            initial_group=row.rating_group,
            initial_text=row.prompt_text,
        )
        self.wait_window(d)
        if d.result:
            self._db.update_prompt(pid, d.result.prompt_text)
            self._reload()

    def _delete(self) -> None:
        pid = self._selected_id()
        if pid is None:
            messagebox.showinfo("Нет выбора", "Выберите строку в таблице.", parent=self)
            return
        if not messagebox.askyesno("Удалить", "Удалить этот промпт?", parent=self):
            return
        self._db.delete_prompt(pid)
        self._reload()
