"""
Главное окно приложения.

Функции:
- Магазины WB: добавить/редактировать/удалить.
- Ручная загрузка новых: WB -> SQLite.
- Вкладки: Отзывы / Вопросы.
- Массовая генерация ответов (выбранные/все видимые) через OpenAI.
- Массовая отправка (выбранные/все видимые) на WB.
- Список показывает только статусы: Новый / Сгенерирован.
- Столбцы в таблице на русском языке.
"""
from __future__ import annotations

import asyncio
import logging
import re
import sys
from concurrent.futures import Future
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from tkinter import messagebox
import tkinter as tk
import tkinter.ttk as ttk

import customtkinter as ctk

from .logging_config import setup_logging
from .db import Database, Store, ItemRow
from .core.async_runner import AsyncRunner
from .core.net import UnauthorizedStoreError
from .core.workflows import load_new_items, load_new_all, generate_mass, send_mass, send_mass_all
from .ui.dialogs import StoreDialog, PromptsWindow, TelegramDialog, pick_openai_key_file, read_openai_key_from_file

log = logging.getLogger("ui")

APP_DIR = Path(__file__).resolve().parent.parent
DB_PATH = str(APP_DIR / "data" / "reviews.db")
LOG_PATH = str(APP_DIR / "logs" / "app.log")

STATUS_RU = {
    "new": "Новый",
    "generated": "Сгенерирован",
    "sent": "Отправлен",
    "ignored": "Игнор",
}

# Темы оформления (mode, accent, hover, border; опционально theme_path — JSON для set_default_color_theme)
THEMES = {
    "dark_purple": {"mode": "dark", "accent": "#7C3AED", "hover": "#6D28D9", "border": "#374151", "name": "Тёмная (фиолетовый)"},
    "dark_blue": {"mode": "dark", "accent": "#2563EB", "hover": "#1D4ED8", "border": "#1E3A5F", "name": "Тёмная (синий)"},
    "dark_emerald": {"mode": "dark", "accent": "#059669", "hover": "#047857", "border": "#064E3B", "name": "Тёмная (изумруд)"},
    "custom_json": {
        "mode": "dark",
        "accent": "#7C3AED",
        "hover": "#6D28D9",
        "border": "#374151",
        "name": "Тёмная (из файла)",
        "theme_path": "app/assets/themes/dark-purple.json",
    },
    "light": {"mode": "light", "accent": "#7C3AED", "hover": "#6D28D9", "border": "#D1D5DB", "name": "Светлая (фиолетовый)"},
}


def _format_date(iso_str: str | None) -> str:
    """Из ISO даты делаем человекопонятную: 12.03.2025, 03:56."""
    if not iso_str or not iso_str.strip():
        return "—"
    s = iso_str.strip().replace("Z", "+00:00")
    # убрать дробные секунды если мешают
    s = re.sub(r"\.\d+", "", s)
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo:
            dt = dt.astimezone()
        return dt.strftime("%d.%m.%Y, %H:%M")
    except (ValueError, TypeError):
        return iso_str[:16] if len(iso_str) > 16 else iso_str

class App(ctk.CTk):
    def __init__(self) -> None:
        super().__init__()
        setup_logging(LOG_PATH)

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")

        self.title("WB Автоответчик")
        self.geometry("1200x740")
        self.minsize(1100, 680)
        try:
            self.configure(fg_color="#0F0F14")
        except (tk.TclError, TypeError):
            pass

        self.db = Database(DB_PATH)
        self.runner = AsyncRunner()
        self._progress_queue: Queue = Queue()
        self._pending_ui_updates: Queue = Queue()  # (action, future[, extra]) — обрабатывается в main thread
        self._current_future: Future | None = None

        self.openai_key_path: str | None = None
        self.openai_key_value: str | None = None

        # Тема из настроек (применится в _build_ui)
        theme_id = self.db.get_setting("theme") or "dark_purple"
        if theme_id not in THEMES:
            theme_id = "dark_purple"
        th = THEMES[theme_id]
        ctk.set_appearance_mode(th["mode"])
        theme_path = th.get("theme_path")
        if theme_path:
            path = APP_DIR / theme_path
            if path.exists():
                ctk.set_default_color_theme(str(path))
        else:
            ctk.set_default_color_theme("dark-blue")
        self._accent = th["accent"]
        self._accent_hover = th["hover"]
        self._border = th["border"]
        self._hover_secondary = ("gray75", "gray30")  # единый hover для второстепенных кнопок

        self._build_ui()
        self._reload_stores()
        self._setup_focus_and_keys()
        self._tab_slide_after_id: str | None = None
        # Плавное появление окна (лёгкая анимация)
        try:
            self.attributes("-alpha", 0.0)
            self.after(20, self._fade_in)
        except tk.TclError:
            pass
        self.after(400, self._take_focus)

        self.protocol("WM_DELETE_WINDOW", self._on_close)

    # ---------------- UI ----------------
    def _build_ui(self) -> None:
        # Шрифты: заголовки и моно для читаемости
        font_heading = ctk.CTkFont(size=15, weight="bold")
        font_title = ctk.CTkFont(size=18, weight="bold")
        font_mono = ctk.CTkFont(family=("Consolas" if sys.platform == "win32" else "Menlo"), size=12)
        _pad, _rad = 22, 16  # отступы и скругления

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        top = ctk.CTkFrame(self, corner_radius=_rad, border_width=1, border_color=self._border, fg_color=("#E2E8F0", "#16161E"))
        top.grid(row=0, column=0, padx=_pad, pady=(_pad, 10), sticky="ew")
        top.grid_columnconfigure(2, weight=1)
        top.grid_rowconfigure(0, weight=0)
        top.grid_rowconfigure(1, weight=0)
        # Акцентная полоска под шапкой (толще и заметнее)
        _accent_line = ctk.CTkFrame(top, height=5, fg_color=self._accent, corner_radius=0)
        _accent_line.grid(row=1, column=0, columnspan=10, sticky="ew")

        # Блок названия приложения слева
        _app_title = ctk.CTkFrame(top, fg_color="transparent")
        _app_title.grid(row=0, column=0, padx=(_pad, 16), pady=_pad, sticky="w")
        ctk.CTkFrame(_app_title, width=5, height=26, fg_color=self._accent, corner_radius=3).grid(row=0, column=0, padx=(0, 12), pady=0, sticky="w")
        ctk.CTkLabel(_app_title, text="WB Автоответчик", font=font_title, text_color=("#0F172A", "#E2E8F0")).grid(row=0, column=1, sticky="w")

        _hdr = ctk.CTkFrame(top, fg_color="transparent")
        _hdr.grid(row=0, column=1, padx=(0, 8), pady=_pad, sticky="w")
        ctk.CTkLabel(_hdr, text="Магазин", font=font_heading, text_color=("#475569", "#94A3B8")).grid(row=0, column=0, padx=(0, 8), sticky="w")
        self.store_var = ctk.StringVar(value="")
        self.store_combo = ctk.CTkComboBox(top, variable=self.store_var, values=[], command=lambda _: self._refresh_items(), width=220)
        self.store_combo.grid(row=0, column=2, padx=(0, 8), pady=_pad, sticky="ew")

        ctk.CTkButton(top, text="Добавить", command=self._add_store, width=90).grid(row=0, column=3, padx=4, pady=_pad)
        ctk.CTkButton(top, text="Редакт.", command=self._edit_store, width=80).grid(row=0, column=4, padx=4, pady=_pad)
        ctk.CTkButton(top, text="Удалить", command=self._delete_store, width=80).grid(row=0, column=5, padx=4, pady=_pad)
        ctk.CTkButton(top, text="Настройки", command=self._open_settings_window, width=100).grid(row=0, column=6, padx=(4, _pad), pady=_pad)

        # Основной контент: вкладки «Главная» и «Лог»
        body = ctk.CTkFrame(self, corner_radius=_rad, border_width=1, border_color=self._border, fg_color=("gray90", "#16161E"))
        body.grid(row=1, column=0, padx=_pad, pady=(0, _pad), sticky="nsew")
        body.grid_columnconfigure(0, weight=1)
        body.grid_rowconfigure(0, weight=0)   # строка с табами Главная|Лог
        body.grid_rowconfigure(1, weight=1)  # контент

        body_tab_bar = ctk.CTkFrame(body, fg_color="transparent")
        body_tab_bar.grid(row=0, column=0, columnspan=2, sticky="ew", padx=_pad, pady=(_pad, 6))
        body_tab_bar.grid_columnconfigure(0, weight=1)
        main_stack = ctk.CTkFrame(body, fg_color="transparent")
        main_stack.grid(row=1, column=0, columnspan=2, padx=0, pady=0, sticky="nsew")
        main_stack.grid_rowconfigure(0, weight=1)
        main_stack.grid_columnconfigure(0, weight=1)

        main_frame = ctk.CTkFrame(main_stack, fg_color="transparent")
        main_frame.grid(row=0, column=0, sticky="nsew")
        main_frame.grid_columnconfigure(0, weight=5)
        main_frame.grid_columnconfigure(1, weight=3)
        main_frame.grid_rowconfigure(0, weight=1)
        main_frame.grid_rowconfigure(1, weight=0)
        main_frame.grid_rowconfigure(2, weight=0)

        # Левая панель: акцентная полоска слева + переключатель вкладок
        left_panel = ctk.CTkFrame(main_frame, corner_radius=_rad, border_width=1, border_color=self._border, fg_color=("gray90", "#16161E"))
        left_panel.grid(row=0, column=0, padx=(_pad, 8), pady=_pad, sticky="nsew")
        left_panel.grid_rowconfigure(1, weight=1)
        left_panel.grid_columnconfigure(0, minsize=4)
        left_panel.grid_columnconfigure(1, weight=1)
        ctk.CTkFrame(left_panel, width=4, fg_color=self._accent, corner_radius=2).grid(row=0, column=0, rowspan=2, sticky="ns", padx=(0, 0), pady=0)

        tab_bar = ctk.CTkFrame(left_panel, fg_color="transparent")
        tab_bar.grid(row=0, column=1, sticky="ew", padx=(_pad, _pad), pady=(_pad, 0))
        tab_bar.grid_columnconfigure(0, weight=1)
        self._tab_var = "Отзывы"

        def _on_tab_change(value: str) -> None:
            if value == "Отзывы":
                self._switch_to_reviews()
            else:
                self._switch_to_questions()

        self._tab_segmented = ctk.CTkSegmentedButton(
            tab_bar,
            values=["Отзывы", "Вопросы"],
            command=_on_tab_change,
        )
        self._tab_segmented.set("Отзывы")
        self._tab_segmented.grid(row=0, column=0, sticky="ew", pady=8)

        self.status_filter_var = ctk.StringVar(value="Все")
        # Контейнер для анимации слайда при смене вкладки
        tab_holder = ctk.CTkFrame(left_panel, fg_color="transparent")
        tab_holder.grid(row=1, column=1, padx=(_pad, _pad), pady=(_pad, _pad), sticky="nsew")
        tab_holder.grid_rowconfigure(0, weight=1)
        tab_holder.grid_columnconfigure(0, weight=1)
        self._tab_slide = ctk.CTkFrame(tab_holder, fg_color="transparent")
        self._tab_slide.place(x=0, y=0, relwidth=1, relheight=1)
        self._tab_slide.grid_rowconfigure(1, weight=1)
        self._tab_slide.grid_columnconfigure(0, weight=1)

        self.tab_reviews = ctk.CTkFrame(self._tab_slide, fg_color="transparent")
        self.tab_reviews.grid(row=1, column=0, padx=0, pady=0, sticky="nsew")
        self.tab_reviews.grid_rowconfigure(1, weight=1)
        self.tab_reviews.grid_columnconfigure(0, weight=1)
        self.tab_questions = ctk.CTkFrame(self._tab_slide, fg_color="transparent")
        self.tab_questions.grid(row=1, column=0, padx=0, pady=0, sticky="nsew")
        self.tab_questions.grid_rowconfigure(1, weight=1)
        self.tab_questions.grid_columnconfigure(0, weight=1)
        self.tab_questions.grid_remove()

        self._add_status_filter_to_tab(self.tab_reviews)
        self._add_status_filter_to_tab(self.tab_questions)

        # Правая панель: превью и редактор с акцентными заголовками
        right = ctk.CTkFrame(main_frame, corner_radius=_rad, border_width=1, border_color=self._border, fg_color=("gray90", "#16161E"))
        right.grid(row=0, column=1, padx=(8, _pad), pady=_pad, sticky="nsew")
        right.grid_rowconfigure(2, weight=1)
        right.grid_columnconfigure(0, weight=1)

        def _section_header(parent: ctk.CTkFrame, text: str, row: int) -> None:
            f = ctk.CTkFrame(parent, fg_color="transparent")
            f.grid(row=row, column=0, padx=(_pad, 0), pady=(_pad if row == 0 else 0, 6), sticky="w")
            ctk.CTkFrame(f, width=4, height=20, fg_color=self._accent, corner_radius=2).grid(row=0, column=0, padx=(0, 10), sticky="w")
            ctk.CTkLabel(f, text=text, font=font_heading).grid(row=0, column=1, sticky="w")

        _section_header(right, "Текст", 0)
        self.txt_source = ctk.CTkTextbox(right, height=160, font=font_mono, corner_radius=12, border_width=1)
        self.txt_source.grid(row=1, column=0, padx=_pad, pady=(0, _pad), sticky="nsew")

        _section_header(right, "Ответ (сгенерированный / редактируемый)", 2)
        self.txt_answer = ctk.CTkTextbox(right, font=font_mono, corner_radius=12, border_width=1)
        self.txt_answer.grid(row=3, column=0, padx=_pad, pady=(0, _pad), sticky="nsew")

        btn_row = ctk.CTkFrame(right, fg_color="transparent")
        btn_row.grid(row=4, column=0, padx=_pad, pady=(0, _pad), sticky="ew")
        btn_row.grid_columnconfigure((0, 1, 2), weight=1)
        _h = self._hover_secondary
        ctk.CTkButton(btn_row, text="Сохранить", command=self._save_answer_edit, hover_color=_h).grid(row=0, column=0, padx=(0, 6), sticky="ew")
        ctk.CTkButton(btn_row, text="Игнор", command=self._ignore_selected, hover_color=_h).grid(row=0, column=1, padx=6, sticky="ew")
        ctk.CTkButton(btn_row, text="Отправить", command=self._send_selected_single, hover_color=_h).grid(row=0, column=2, padx=(6, 0), sticky="ew")

        # Нижняя панель действий (action bar)
        bottom = ctk.CTkFrame(main_frame, corner_radius=_rad, border_width=1, border_color=self._border, fg_color=("gray92", "#1C1C26"))
        bottom.grid(row=1, column=0, columnspan=2, padx=_pad, pady=(0, _pad), sticky="ew")
        bottom.grid_columnconfigure((0,1,2,3,4,5), weight=1)

        self._btn_load = ctk.CTkButton(
            bottom, text="Загрузить новые", command=self._load_new,
            fg_color=self._accent, hover_color=self._accent_hover,
        )
        self._btn_load.grid(row=0, column=0, padx=8, pady=12, sticky="ew")
        self._btn_gen_sel = ctk.CTkButton(
            bottom, text="Сгенерировать выбранные", command=self._generate_selected,
            fg_color=self._accent, hover_color=self._accent_hover,
        )
        self._btn_gen_sel.grid(row=0, column=1, padx=8, pady=12, sticky="ew")
        self._btn_gen_all = ctk.CTkButton(
            bottom, text="Сгенерировать все", command=self._generate_all,
            fg_color=self._accent, hover_color=self._accent_hover,
        )
        self._btn_gen_all.grid(row=0, column=2, padx=8, pady=12, sticky="ew")
        self._btn_send_sel = ctk.CTkButton(
            bottom, text="Отправить выбранные", command=self._send_selected,
            hover_color=self._hover_secondary,
        )
        self._btn_send_sel.grid(row=0, column=3, padx=8, pady=12, sticky="ew")
        self._btn_send_all = ctk.CTkButton(
            bottom, text="Отправить все", command=self._send_all,
            hover_color=self._hover_secondary,
        )
        self._btn_send_all.grid(row=0, column=4, padx=8, pady=12, sticky="ew")

        self.progress = ctk.CTkProgressBar(bottom, progress_color=self._accent)
        self.progress.grid(row=0, column=5, padx=8, pady=12, sticky="ew")
        self.progress.set(0)

        # Trees (под фильтром в каждой вкладке)
        self.tree_reviews = self._make_tree(self.tab_reviews, is_reviews=True)
        self.tree_questions = self._make_tree(self.tab_questions, is_reviews=False)

        # Строка статуса внизу (в отдельном блоке)
        self.status_var = ctk.StringVar(value="Показано: 0 | Выбрано: 0")
        status_wrap = ctk.CTkFrame(main_frame, fg_color="transparent")
        status_wrap.grid(row=2, column=0, columnspan=2, padx=_pad, pady=(0, _pad), sticky="w")
        status_bar = ctk.CTkLabel(status_wrap, textvariable=self.status_var, font=font_mono, text_color=("#64748B", "#94A3B8"))
        status_bar.pack(side="left")

        # Вкладка «Лог»: отдельный экран с логом
        log_frame = ctk.CTkFrame(main_stack, fg_color="transparent")
        log_frame.grid(row=0, column=0, sticky="nsew")
        log_frame.grid_rowconfigure(1, weight=1)
        log_frame.grid_columnconfigure(0, weight=1)
        log_header = ctk.CTkFrame(log_frame, fg_color="transparent")
        log_header.grid(row=0, column=0, padx=_pad, pady=(_pad, 8), sticky="w")
        ctk.CTkFrame(log_header, width=4, height=18, fg_color=self._accent, corner_radius=2).grid(row=0, column=0, padx=(0, 8), sticky="w")
        ctk.CTkLabel(log_header, text="Лог", font=font_heading).grid(row=0, column=1, sticky="w")
        self.log_box = ctk.CTkTextbox(log_frame, font=font_mono, corner_radius=10)
        self.log_box.grid(row=1, column=0, padx=_pad, pady=(0, _pad), sticky="nsew")
        self.log_box.insert("1.0", "Лог…\n")
        self.log_box.configure(state="disabled")
        log_frame.grid_remove()

        def _on_body_tab(value: str) -> None:
            if value == "Главная":
                log_frame.grid_remove()
                main_frame.grid(row=0, column=0, sticky="nsew")
            else:
                main_frame.grid_remove()
                log_frame.grid(row=0, column=0, sticky="nsew")

        body_tab_seg = ctk.CTkSegmentedButton(
            body_tab_bar,
            values=["Главная", "Лог"],
            command=_on_body_tab,
        )
        body_tab_seg.set("Главная")
        body_tab_seg.grid(row=0, column=0, sticky="w", pady=0)
        self.after(800, self._tail_log)

        self._selected_item_id: int | None = None
        self._tooltip_data: dict[str, dict[str, str]] = {}
        self._tooltip_after_id: str | None = None
        self._tooltip_win: tk.Toplevel | None = None

        self._setup_tree_tooltips()
        self._setup_hotkeys()

    def _setup_tree_tooltips(self) -> None:
        """Подсказка при наведении на ячейки «Товар» и «Текст»."""
        for tree in (self.tree_reviews, self.tree_questions):
            tree.bind("<Motion>", self._on_tree_motion)
            tree.bind("<Leave>", self._on_tree_leave)

    def _on_tree_motion(self, event: tk.Event) -> None:
        tree = event.widget
        if not isinstance(tree, ttk.Treeview):
            return
        self._on_tree_leave()
        region = tree.identify_region(event.x, event.y)
        if region != "cell":
            return
        row_id = tree.identify_row(event.y)
        col = tree.identify_column(event.x)
        if not row_id or col not in ("#5", "#6"):
            return
        data = self._tooltip_data.get(row_id, {})
        key = "product" if col == "#5" else "text"
        content = (data.get(key) or "").strip()
        if not content:
            return
        self._tooltip_after_id = self.after(450, lambda: self._show_tooltip(content, event.x_root, event.y_root))

    def _show_tooltip(self, content: str, x_root: int, y_root: int) -> None:
        self._tooltip_after_id = None
        if self._tooltip_win:
            try:
                self._tooltip_win.destroy()
            except tk.TclError:
                pass
        self._tooltip_win = tk.Toplevel(self)
        self._tooltip_win.wm_overrideredirect(True)
        self._tooltip_win.wm_geometry("+0+0")
        max_len = 600
        if len(content) > max_len:
            content = content[:max_len] + "…"
        lbl = tk.Label(
            self._tooltip_win,
            text=content,
            justify="left",
            background="#1C1C26",
            foreground="#E2E8F0",
            font=("Helvetica Neue", 12),
            padx=14,
            pady=12,
            wraplength=420,
            relief="flat",
            borderwidth=1,
            highlightbackground="#3D3D52",
        )
        lbl.pack()
        self._tooltip_win.update_idletasks()
        tw, th = self._tooltip_win.winfo_reqwidth(), self._tooltip_win.winfo_reqheight()
        # Позиция: чуть справа и ниже курсора, не выходить за экран
        dx, dy = 20, 20
        sw = self.winfo_screenwidth()
        sh = self.winfo_screenheight()
        x = min(x_root + dx, sw - tw - 10)
        y = min(y_root + dy, sh - th - 10)
        if x < 10:
            x = 10
        if y < 10:
            y = 10
        self._tooltip_win.wm_geometry(f"+{x}+{y}")

    def _on_tree_leave(self, event: tk.Event | None = None) -> None:
        if self._tooltip_after_id:
            self.after_cancel(self._tooltip_after_id)
            self._tooltip_after_id = None
        if self._tooltip_win:
            try:
                self._tooltip_win.destroy()
            except tk.TclError:
                pass
            self._tooltip_win = None

    def _setup_hotkeys(self) -> None:
        """Горячие клавиши: Ctrl+L загрузка, Ctrl+G генерация, Ctrl+S отправить."""
        def do_load(_e=None):
            if self._btn_load.cget("state") == "normal":
                self._load_new()
            return "break"
        def do_generate(_e=None):
            if self._btn_gen_sel.cget("state") == "normal":
                ids = self._selected_ids()
                if ids:
                    self._generate(ids)
                else:
                    self._generate_all()
            return "break"
        def do_send(_e=None):
            if self._btn_send_sel.cget("state") == "normal":
                ids = self._selected_ids()
                if ids:
                    self._send(ids)
                else:
                    self._send_all()
            return "break"
        self.bind_all("<Control-l>", do_load)
        self.bind_all("<Control-g>", do_generate)
        self.bind_all("<Control-s>", do_send)

    def _open_settings_window(self) -> None:
        """Открывает окно настроек (одно место для OpenAI, промптов, Telegram, темы)."""
        win = ctk.CTkToplevel(self)
        win.title("Настройки")
        win.geometry("520x320")
        win.resizable(True, True)
        font_heading = ctk.CTkFont(size=13, weight="bold")
        win.grid_columnconfigure(0, weight=1)
        row = 0

        # OpenAI ключ
        openai_f = ctk.CTkFrame(win, fg_color="transparent")
        openai_f.grid(row=row, column=0, padx=24, pady=(24, 12), sticky="w")
        openai_f.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(openai_f, text="OpenAI", font=font_heading).grid(row=0, column=0, padx=(0, 12), pady=8, sticky="w")
        ctk.CTkButton(openai_f, text="Выбрать файл с ключом", command=self._pick_openai_key).grid(row=0, column=1, padx=(0, 12), pady=8, sticky="w")
        self.lbl_key = ctk.CTkLabel(openai_f, text=f"ключ: {Path(self.openai_key_path).name}" if self.openai_key_path else "ключ: не выбран", text_color="#9ca3af")
        self.lbl_key.grid(row=0, column=2, padx=0, pady=8, sticky="w")
        row += 1

        # Промпты
        prom_f = ctk.CTkFrame(win, fg_color="transparent")
        prom_f.grid(row=row, column=0, padx=24, pady=12, sticky="w")
        ctk.CTkLabel(prom_f, text="Промпты", font=font_heading).grid(row=0, column=0, padx=(0, 12), pady=8, sticky="w")
        ctk.CTkButton(prom_f, text="Открыть редактор промптов", command=self._open_prompts).grid(row=0, column=1, pady=8, sticky="w")
        row += 1

        # Telegram
        tg_f = ctk.CTkFrame(win, fg_color="transparent")
        tg_f.grid(row=row, column=0, padx=24, pady=12, sticky="w")
        ctk.CTkLabel(tg_f, text="Telegram", font=font_heading).grid(row=0, column=0, padx=(0, 12), pady=8, sticky="w")
        ctk.CTkButton(tg_f, text="Настроить бота и чат", command=self._open_telegram).grid(row=0, column=1, pady=8, sticky="w")
        row += 1

        # Тема
        theme_f = ctk.CTkFrame(win, fg_color="transparent")
        theme_f.grid(row=row, column=0, padx=24, pady=12, sticky="w")
        theme_f.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(theme_f, text="Тема", font=font_heading).grid(row=0, column=0, padx=(0, 12), pady=8, sticky="w")
        theme_id = self.db.get_setting("theme") or "dark_purple"
        if theme_id not in THEMES:
            theme_id = "dark_purple"
        theme_names = [THEMES[t]["name"] for t in THEMES]
        theme_var = ctk.StringVar(value=THEMES[theme_id]["name"])

        def on_theme_change(choice: str) -> None:
            for tid, data in THEMES.items():
                if data["name"] == choice:
                    self.db.set_setting("theme", tid)
                    messagebox.showinfo("Тема", "Тема сохранена. Перезапустите приложение, чтобы применить.", parent=win)
                    return

        ctk.CTkComboBox(theme_f, values=theme_names, variable=theme_var, width=220, command=on_theme_change).grid(row=0, column=1, pady=8, sticky="w")
        ctk.CTkLabel(theme_f, text="(применится после перезапуска)", text_color="#9ca3af", font=ctk.CTkFont(size=12)).grid(row=0, column=2, padx=(12, 0), pady=8, sticky="w")
        row += 1

        win.transient(self)

    def _setup_focus_and_keys(self) -> None:
        self._take_focus()
        # Не вешаем FocusIn на root: на macOS при клике по кнопке фокус приходит на root,
        # мы забирали его focus_force() — из-за этого кнопка не получала клик и command не срабатывал.
        self._bind_copy_paste_by_keycode()

    def _take_focus(self) -> None:
        try:
            self.focus_force()
        except tk.TclError:
            self.focus_set()

    def _bind_copy_paste_by_keycode(self) -> None:
        # Только латинские c, v, x (ASCII) — Tcl не принимает кириллицу в <Command-keysym>
        for ch in (0x63, 0x76, 0x78):  # 'c', 'v', 'x'
            keysym = chr(ch)
            try:
                self.bind_all(f"<Command-{keysym}>", self._make_cmd_key_handler(keysym), add=True)
                self.bind_all(f"<Mod1-{keysym}>", self._make_cmd_key_handler(keysym), add=True)
            except tk.TclError:
                pass
        self.bind_all("<KeyPress>", self._on_keypress_cmd_copy_paste, add=True)

    def _on_keypress_cmd_copy_paste(self, event: tk.Event) -> str:
        st = getattr(event, "state", 0)
        if (st & (0x8 | 0x10 | 0x80000)) == 0:
            return ""
        # Только когда keysym не латинские c/v/x — иначе сработает привязка <Command-c> и будет двойное срабатывание
        ks = getattr(event, "keysym", "").lower()
        if ks in ("c", "v", "x"):
            return ""
        kc = getattr(event, "keycode", 0)
        action = {8: "copy", 9: "paste", 7: "cut"}.get(kc)
        if not action:
            return ""
        w = self.focus_get()
        text = self._focused_text_widget(w)
        if text is None:
            return ""
        try:
            if action == "copy":
                sel = text.selection_get()
                self.clipboard_clear()
                self.clipboard_append(sel)
            elif action == "paste":
                text.insert(tk.INSERT, self.clipboard_get())
            else:
                sel = text.selection_get()
                self.clipboard_clear()
                self.clipboard_append(sel)
                text.delete(tk.SEL_FIRST, tk.SEL_LAST)
        except tk.TclError:
            return ""
        return "break"

    def _make_cmd_key_handler(self, keysym: str) -> tk.Callable[[tk.Event], str]:
        action = {"c": "copy", "v": "paste", "x": "cut"}.get(keysym.lower())
        if not action:
            return lambda e: ""

        def handler(event: tk.Event) -> str:
            w = self.focus_get()
            text = self._focused_text_widget(w)
            if text is None:
                return ""
            try:
                if action == "copy":
                    sel = text.selection_get()
                    self.clipboard_clear()
                    self.clipboard_append(sel)
                elif action == "paste":
                    text.insert(tk.INSERT, self.clipboard_get())
                else:
                    sel = text.selection_get()
                    self.clipboard_clear()
                    self.clipboard_append(sel)
                    text.delete(tk.SEL_FIRST, tk.SEL_LAST)
            except tk.TclError:
                return ""
            return "break"
        return handler

    def _focused_text_widget(self, w: tk.Widget | None) -> tk.Text | None:
        if w is None:
            return None
        if isinstance(w, tk.Text):
            return w
        if hasattr(w, "_textbox") and isinstance(getattr(w, "_textbox"), tk.Text):
            return getattr(w, "_textbox")
        return None

    def _add_status_filter_to_tab(self, parent: ctk.CTkFrame) -> None:
        """Добавляет в верх вкладки строку с фильтром по статусу."""
        f = ctk.CTkFrame(parent, fg_color="transparent")
        f.pack(side="top", fill="x", padx=12, pady=(12, 0))
        ctk.CTkLabel(f, text="Статус:", font=ctk.CTkFont(size=13, weight="bold")).pack(side="left", padx=(0, 8), pady=4)
        ctk.CTkComboBox(
            f,
            variable=self.status_filter_var,
            values=["Все", "Только новые", "Только сгенерированные"],
            width=200,
            command=lambda _: self._refresh_items(),
        ).pack(side="left", pady=4)

    def _make_tree(self, parent, *, is_reviews: bool = False) -> ttk.Treeview:
        # Стиль таблицы под тёмную тему: контрастный фон, акцентные заголовки
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        style.configure("Treeview", rowheight=28, font=("Helvetica Neue", 12),
                        background="#16161E", foreground="#E2E8F0", fieldbackground="#16161E")
        style.configure("Treeview.Heading", font=("Helvetica Neue", 12, "bold"),
                        background="#2D2D3D", foreground="#F5F3FF")
        style.map("Treeview", background=[("selected", "#3D3D52")], foreground=[("selected", "#FFFFFF")])
        style.map("Treeview.Heading", background=[("active", "#3D3D52")])

        cols = ("id", "date", "rating", "store", "product", "text", "status")
        tree = ttk.Treeview(parent, columns=cols, show="headings", selectmode="extended", height=18)
        tree.heading("id", text="ID WB")
        tree.heading("date", text="Дата")
        tree.heading("rating", text="Оценка")
        tree.heading("store", text="Магазин")
        tree.heading("product", text="Товар")
        tree.heading("text", text="Текст")
        tree.heading("status", text="Статус")

        tree.column("id", width=70, anchor="w")
        tree.column("date", width=170, anchor="w")
        tree.column("rating", width=70, anchor="center")
        tree.column("store", width=140, anchor="w")
        tree.column("product", width=220, anchor="w")
        tree.column("text", width=400, anchor="w")
        tree.column("status", width=130, anchor="w")

        if is_reviews:
            # Подсветка строк с оценкой 1–3 (тёмная тема)
            tree.tag_configure("low_rating", background="#2D2528")

        tree.pack(fill="both", expand=True, padx=12, pady=12)

        tree.bind("<<TreeviewSelect>>", lambda e, t=tree: self._on_select(t))
        return tree

    # ---------------- Stores ----------------
    def _reload_stores(self) -> None:
        stores = self.db.list_stores()
        self._stores = stores
        supported = [s for s in stores if s.marketplace in ("wb", "yam", "ozon")]
        values = ["Все магазины"] + [f"{s.id}: {s.name}" for s in supported]
        self.store_combo.configure(values=values if values else [""])
        cur = self.store_var.get().strip()
        if cur not in values:
            self.store_var.set(values[0] if values else "")
        self._refresh_items()

    def _current_store(self) -> Store | None:
        v = self.store_var.get().strip()
        if not v or v == "Все магазины" or ":" not in v:
            return None
        try:
            sid = int(v.split(":", 1)[0])
        except (ValueError, TypeError):
            return None
        for s in self._stores:
            if s.id == sid:
                return s
        return None

    def _store_name_by_id(self, store_id: int) -> str:
        for s in self._stores:
            if s.id == store_id:
                return s.name
        return str(store_id)

    def _add_store(self) -> None:
        d = StoreDialog(self, title="Добавить магазин")
        self.wait_window(d)
        if d.result:
            if d.result.marketplace == "wb":
                self.db.upsert_store_wb(d.result.name, d.result.api_key, d.result.active)
            elif d.result.marketplace == "yam":
                bid = d.result.business_id
                if bid is None:
                    messagebox.showerror("Ошибка", "Укажите Business ID для Яндекс Маркета.")
                    return
                self.db.upsert_store_yam(d.result.name, d.result.api_key, bid, d.result.active)
            elif d.result.marketplace == "ozon":
                cid = (d.result.client_id or "").strip()
                if not cid:
                    messagebox.showerror("Ошибка", "Укажите Client-Id для Ozon.")
                    return
                self.db.upsert_store_ozon(d.result.name, d.result.api_key, cid, d.result.active)
            self._reload_stores()

    def _edit_store(self) -> None:
        s = self._current_store()
        if not s:
            messagebox.showerror("Ошибка", "Сначала выбери магазин.")
            return
        d = StoreDialog(
            self,
            title="Редактировать магазин",
            initial_name=s.name,
            initial_key=s.api_key,
            initial_active=s.active,
            initial_marketplace=s.marketplace,
            initial_business_id=s.business_id,
            initial_client_id=s.client_id or "",
        )
        self.wait_window(d)
        if d.result:
            self.db.update_store(
                s.id,
                d.result.name,
                d.result.api_key,
                d.result.active,
                business_id=d.result.business_id,
                client_id=d.result.client_id,
            )
            self._reload_stores()

    def _delete_store(self) -> None:
        s = self._current_store()
        if not s:
            return
        if not messagebox.askyesno("Удалить", f"Удалить магазин '{s.name}' и все его данные?"):
            return
        self.db.delete_store(s.id)
        self._reload_stores()

    # ---------------- OpenAI key ----------------
    def _open_prompts(self) -> None:
        PromptsWindow(self, self.db)

    def _open_telegram(self) -> None:
        TelegramDialog(self, self.db)

    def _pick_openai_key(self) -> None:
        path = pick_openai_key_file(self)
        if not path:
            return
        try:
            key = read_openai_key_from_file(path)
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не прочитал ключ: {e}")
            return
        self.openai_key_path = path
        self.openai_key_value = key
        self.lbl_key.configure(text=f"ключ: {Path(path).name}")

    # ---------------- Items UI ----------------
    def _fade_in(self, step: int = 0) -> None:
        """Плавное появление окна: 4 шага по ~40 ms."""
        steps = [0.0, 0.35, 0.65, 1.0]
        try:
            self.attributes("-alpha", steps[min(step, len(steps) - 1)])
            if step < len(steps) - 1:
                self.after(45, lambda: self._fade_in(step + 1))
        except tk.TclError:
            pass

    def _tab_slide_animate(self, target_x: int) -> None:
        if self._tab_slide_after_id:
            self.after_cancel(self._tab_slide_after_id)
            self._tab_slide_after_id = None
        try:
            self._tab_slide.place(x=target_x, y=0, relwidth=1, relheight=1)
        except tk.TclError:
            pass

    def _switch_to_reviews(self) -> None:
        if self._tab_slide_after_id:
            self.after_cancel(self._tab_slide_after_id)
            self._tab_slide_after_id = None
        self._tab_var = "Отзывы"
        self.tab_questions.grid_remove()
        self.tab_reviews.grid(row=1, column=0, padx=0, pady=0, sticky="nsew")
        self._tab_segmented.set("Отзывы")
        self._refresh_items()
        self._tab_slide.place(x=24, y=0, relwidth=1, relheight=1)
        self._tab_slide_after_id = self.after(40, lambda: self._tab_slide_animate(10))
        self.after(85, lambda: self._cancel_tab_slide_then_place(0))

    def _switch_to_questions(self) -> None:
        if self._tab_slide_after_id:
            self.after_cancel(self._tab_slide_after_id)
            self._tab_slide_after_id = None
        self._tab_var = "Вопросы"
        self.tab_reviews.grid_remove()
        self.tab_questions.grid(row=1, column=0, padx=0, pady=0, sticky="nsew")
        self._tab_segmented.set("Вопросы")
        self._refresh_items()
        self._tab_slide.place(x=24, y=0, relwidth=1, relheight=1)
        self._tab_slide_after_id = self.after(40, lambda: self._tab_slide_animate(10))
        self.after(85, lambda: self._cancel_tab_slide_then_place(0))

    def _cancel_tab_slide_then_place(self, x: int) -> None:
        if self._tab_slide_after_id:
            self.after_cancel(self._tab_slide_after_id)
            self._tab_slide_after_id = None
        try:
            self._tab_slide.place(x=x, y=0, relwidth=1, relheight=1)
        except tk.TclError:
            pass

    def _active_tree(self) -> ttk.Treeview:
        return self.tree_reviews if self._tab_var == "Отзывы" else self.tree_questions

    def _active_type(self) -> str:
        return "review" if self._tab_var == "Отзывы" else "question"

    def _refresh_items(self) -> None:
        s = self._current_store()
        t = self._active_tree()
        for iid in t.get_children():
            t.delete(iid)
        self._selected_item_id = None
        self._set_preview(None)

        if s:
            rows = self.db.list_items_for_ui(s.id, self._active_type())
            store_name = s.name
        else:
            rows = self.db.list_items_for_ui_all(self._active_type())
            store_name = None

        # Фильтр по статусу
        status_filter = getattr(self, "status_filter_var", None) and self.status_filter_var.get()
        if status_filter == "Только новые":
            rows = [r for r in rows if r.status == "new"]
        elif status_filter == "Только сгенерированные":
            rows = [r for r in rows if r.status == "generated"]

        self._tooltip_data = {str(r.id): {"text": r.text or "", "product": r.product_title or ""} for r in rows}
        is_reviews = self._active_type() == "review"
        for r in rows:
            # Для вопросов оценки нет — показываем прочерк, чтобы колонка не выглядела пустой
            rating = "—" if r.rating is None else str(r.rating)
            sn = store_name if store_name else self._store_name_by_id(r.store_id)
            text = (r.text or "").replace("\n", " ")[:220]
            tags = ("low_rating",) if (is_reviews and r.rating is not None and r.rating in (1, 2, 3)) else ()
            t.insert("", "end", iid=str(r.id), values=(
                r.external_id,
                _format_date(r.date),
                rating,
                (sn or "")[:50],
                (r.product_title or "")[:60],
                text,
                STATUS_RU.get(r.status, r.status),
            ), tags=tags)

        # Обновить строку статуса (сохраняем для _update_status_selected)
        sf = getattr(self, "status_filter_var", None) and self.status_filter_var.get()
        self._status_shown = len(rows)
        self._status_filter = sf or "Все"
        if getattr(self, "status_var", None):
            self._set_status_line(0)

    def _on_select(self, tree: ttk.Treeview) -> None:
        sel = tree.selection()
        self._update_status_selected()
        if not sel:
            self._selected_item_id = None
            self._set_preview(None)
            return
        # preview first selected
        item_id = int(sel[0])
        self._selected_item_id = item_id
        row = self.db.get_item_by_id(item_id)
        self._set_preview(row)
        self._update_status_selected()

    def _set_status_line(self, selected_count: int) -> None:
        shown = getattr(self, "_status_shown", 0)
        sf = getattr(self, "_status_filter", "Все")
        line = f"Показано: {shown} | Выбрано: {selected_count}"
        if sf and sf != "Все":
            line += f" | Фильтр: {sf}"
        line += "  ·  Ctrl+L загрузка  ·  Ctrl+G генерация  ·  Ctrl+S отправить"
        if getattr(self, "status_var", None):
            self.status_var.set(line)

    def _update_status_selected(self) -> None:
        """Обновить «Выбрано: N» в строке статуса."""
        t = self._active_tree()
        self._set_status_line(len(t.selection()))

    def _set_preview(self, row: ItemRow | None) -> None:
        self.txt_source.delete("1.0", "end")
        self.txt_answer.delete("1.0", "end")
        if not row:
            return
        head = f"Товар: {row.product_title}\n"
        if row.item_type == "review":
            head += f"Оценка: {row.rating}\n"
        head += f"Дата: {_format_date(row.date)}\n\n"
        self.txt_source.insert("1.0", head + (row.text or ""))
        self.txt_answer.insert("1.0", (row.generated_text or "").strip())

    def _selected_ids(self) -> list[int]:
        t = self._active_tree()
        return [int(x) for x in t.selection()]

    def _all_visible_ids(self) -> list[int]:
        t = self._active_tree()
        return [int(x) for x in t.get_children()]

    # ---------------- Actions ----------------
    def _load_new(self) -> None:
        s = self._current_store()
        stores_to_load = [x for x in self._stores if x.marketplace in ("wb", "yam", "ozon")]
        if s and (
            (s.marketplace == "wb")
            or (s.marketplace == "yam" and s.business_id is not None)
            or (s.marketplace == "ozon" and (s.client_id or "").strip())
        ):
            store_list = [s]
        elif stores_to_load:
            store_list = [
                x for x in stores_to_load
                if (x.marketplace == "wb")
                or (x.marketplace == "yam" and x.business_id is not None)
                or (x.marketplace == "ozon" and (x.client_id or "").strip())
            ]
        else:
            store_list = []
        if not store_list:
            messagebox.showerror("Ошибка", "Добавьте хотя бы один магазин (для Яндекс Маркета укажите Business ID).")
            return
        self._set_busy(True)
        self._progress_queue = Queue()
        if len(store_list) > 1:
            coro = load_new_all(self.db, store_list, progress_queue=self._progress_queue)
        else:
            coro = load_new_items(self.db, store_list[0], progress_queue=self._progress_queue)
        fut = self.runner.submit(coro)
        self._current_future = fut
        self._poll_progress()
        def done_cb(f):
            self._current_future = None
            self._pending_ui_updates.put(("load_done", f))
        fut.add_done_callback(done_cb)

    def _poll_progress(self) -> None:
        """Опрос очереди прогресса каждые ~100 мс, обновление progress bar. Обработка завершённых задач (в main thread)."""
        while True:
            try:
                msg = self._progress_queue.get_nowait()
            except Empty:
                break
            if isinstance(msg, tuple) and len(msg) == 3 and msg[0] == "progress":
                _, current, total = msg
                if total and total > 0:
                    self.progress.set(min(1.0, current / total))
        while True:
            try:
                item = self._pending_ui_updates.get_nowait()
            except Empty:
                break
            self._process_pending_ui(item)
        if self._current_future is not None and not self._current_future.done():
            self.after(100, self._poll_progress)
        else:
            self._current_future = None

    def _process_pending_ui(self, item: tuple) -> None:
        """Обработка результата фоновой задачи (вызывается только из main thread)."""
        action = item[0]
        fut = item[1]
        try:
            result = fut.result()
        except asyncio.CancelledError:
            self._set_busy(False)
            return
        except UnauthorizedStoreError as e:
            msg = f"Проблема с ключом в магазине «{e.store_name}». Неверный или истёкший API-ключ WB. Проверьте ключ в настройках магазина (Редакт.)."
            prefix = {"load_done": "Загрузка", "generate_done": "Генерация", "send_done": "Отправка"}.get(action, "Операция")
            messagebox.showerror("Ошибка", f"{prefix} не удалась: {msg}")
            self._set_busy(False)
            return
        except Exception as e:
            log.exception("%s failed: %s", action, e)
            msg = str(e)
            if "401" in msg or "unauthorized" in msg.lower() or "token" in msg.lower():
                msg = "Неверный или истёкший API-ключ WB. Проверьте ключ в настройках магазина (Редакт.)."
            prefix = {"load_done": "Загрузка", "generate_done": "Генерация", "send_done": "Отправка"}.get(action, "Операция")
            messagebox.showerror("Ошибка", f"{prefix} не удалась: {msg}")
            self._set_busy(False)
            return
        if action == "load_done":
            self._on_loaded(result)
        elif action == "generate_done":
            self._on_generated(result[0], result[1])
        elif action == "send_done":
            extra_skipped = item[2] if len(item) > 2 else 0
            self._on_sent(result[0], result[1] + extra_skipped, result[2])

    def _on_loaded(self, added: int) -> None:
        self._current_future = None
        self.progress.set(0)
        self._append_log(f"Загружено/обновлено записей: {added}")
        self._refresh_items()
        self._set_busy(False)

    def _generate_selected(self) -> None:
        ids = self._selected_ids()
        if not ids:
            messagebox.showinfo("Нет выбора", "Выбери строки в таблице.")
            return
        self._generate(ids)

    def _generate_all(self) -> None:
        ids = self._all_visible_ids()
        if not ids:
            return
        self._generate(ids)

    def _generate(self, ids: list[int]) -> None:
        if not self.openai_key_value:
            messagebox.showerror("Ошибка", "Сначала выбери файл с OpenAI ключом.")
            return
        self._set_busy(True)
        self._progress_queue = Queue()
        fut = self.runner.submit(
            generate_mass(self.db, ids, self.openai_key_value, model="gpt-5.2", progress_queue=self._progress_queue)
        )
        self._current_future = fut
        self._poll_progress()

        def done_cb(f):
            self._current_future = None
            self._pending_ui_updates.put(("generate_done", f))
        fut.add_done_callback(done_cb)

    def _on_generated(self, ok: int, failed: int) -> None:
        self._current_future = None
        self.progress.set(0)
        self._append_log(f"Генерация: OK={ok}, Ошибок={failed}")
        self._refresh_items()
        self._set_busy(False)

    def _send_selected(self) -> None:
        ids = self._selected_ids()
        if not ids:
            messagebox.showinfo("Нет выбора", "Выбери строки в таблице.")
            return
        self._send(ids)

    def _send_all(self) -> None:
        ids = self._all_visible_ids()
        if not ids:
            return
        self._send(ids)

    def _send(self, ids: list[int]) -> None:
        to_send = []
        skipped = 0
        for item_id in ids:
            row = self.db.get_item_by_id(item_id)
            if row and (row.generated_text or "").strip():
                to_send.append(item_id)
            else:
                skipped += 1
        if not to_send:
            messagebox.showinfo("Нечего отправлять", f"Нет ответов для отправки. Пропущено: {skipped}")
            return

        s = self._current_store()
        self._progress_queue = Queue()
        if s:
            coro = send_mass(self.db, s, to_send, progress_queue=self._progress_queue)
        else:
            coro = send_mass_all(self.db, to_send, progress_queue=self._progress_queue)
        self._set_busy(True)
        fut = self.runner.submit(coro)
        self._current_future = fut
        self._poll_progress()

        def done_cb(f):
            self._current_future = None
            self._pending_ui_updates.put(("send_done", f, skipped))
        fut.add_done_callback(done_cb)

    def _on_sent(self, sent_ok: int, skipped: int, failed: int) -> None:
        self._current_future = None
        self.progress.set(0)
        self._append_log(f"Отправка: OK={sent_ok}, Пропущено(no text)={skipped}, Ошибок={failed}")
        self._refresh_items()
        self._set_busy(False)

    def _send_selected_single(self) -> None:
        if not self._selected_item_id:
            return
        self._send([self._selected_item_id])

    def _save_answer_edit(self) -> None:
        if not self._selected_item_id:
            return
        txt = self.txt_answer.get("1.0","end").strip()
        if not txt:
            messagebox.showerror("Ошибка", "Ответ пустой.")
            return
        self.db.set_generated(self._selected_item_id, txt)
        self._append_log(f"Ответ сохранён для item_id={self._selected_item_id}")
        self._refresh_items()

    def _ignore_selected(self) -> None:
        if not self._selected_item_id:
            return
        self.db.set_ignored(self._selected_item_id)
        self._append_log(f"Помечено как игнор item_id={self._selected_item_id}")
        self._refresh_items()

    def _set_busy(self, busy: bool) -> None:
        self.progress.set(0.2 if busy else 0)
        state = "disabled" if busy else "normal"
        for btn in (self._btn_load, self._btn_gen_sel, self._btn_gen_all, self._btn_send_sel, self._btn_send_all):
            btn.configure(state=state)

    # ---------------- Log view ----------------
    def _append_log(self, line: str) -> None:
        self.log_box.configure(state="normal")
        self.log_box.insert("end", line + "\n")
        self.log_box.see("end")
        self.log_box.configure(state="disabled")

    def _tail_log(self) -> None:
        try:
            p = Path(LOG_PATH)
            if p.exists():
                size = p.stat().st_size
                max_read = 100 * 1024
                with p.open("rb") as f:
                    if size > max_read:
                        f.seek(size - max_read)
                        blob = f.read()
                    else:
                        blob = f.read()
                txt = blob.decode("utf-8", errors="ignore")
                tail = "\n".join(txt.splitlines()[-30:])
                self.log_box.configure(state="normal")
                self.log_box.delete("1.0","end")
                self.log_box.insert("1.0", tail + "\n")
                self.log_box.see("end")
                self.log_box.configure(state="disabled")
        except Exception:
            pass
        self.after(1200, self._tail_log)

    def _on_close(self) -> None:
        try:
            self.runner.stop()
        except Exception:
            pass
        try:
            self.db.close()
        except Exception:
            pass
        self.destroy()

def main() -> None:
    App().mainloop()

if __name__ == "__main__":
    main()
