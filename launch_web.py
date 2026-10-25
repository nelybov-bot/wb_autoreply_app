#!/usr/bin/env python3
"""
Окно с одной кнопкой «Запустить»: поднимает веб-сервер и открывает браузер.
Закройте окно — сервер остановится.
"""
import subprocess
import sys
import time
import webbrowser
from pathlib import Path

# Запуск из корня проекта
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

try:
    import tkinter as tk
    from tkinter import font as tkfont
except ImportError:
    # Без GUI — просто запуск в консоли
    subprocess.run([sys.executable, str(ROOT / "run_web.py")], cwd=ROOT)
    sys.exit(0)

URL = "http://127.0.0.1:8000"
PROC = None


def ensure_deps():
    try:
        import uvicorn  # noqa: F401
        return True
    except ImportError:
        pass
    subprocess.check_call([
        sys.executable, "-m", "pip", "install", "-q", "fastapi", "uvicorn[standard]"
    ], cwd=ROOT)
    return True


def start_server():
    global PROC
    if PROC is not None:
        webbrowser.open(URL)
        return
    ensure_deps()
    PROC = subprocess.Popen(
        [sys.executable, str(ROOT / "run_web.py")],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1.2)
    webbrowser.open(URL)
    btn = win.nametowidget("run_btn")
    btn.config(text="Сервер запущен\n(закройте окно для остановки)", state="disabled")
    label.config(text="Откройте в браузере: " + URL)


def on_close():
    global PROC
    if PROC is not None:
        PROC.terminate()
        PROC.wait(timeout=5)
    win.destroy()


win = tk.Tk()
win.title("WB Автоответчик")
win.resizable(False, False)
win.protocol("WM_DELETE_WINDOW", on_close)

f = tkfont.nametofont("TkDefaultFont")
f.configure(size=11)
win.option_add("*Font", f)

frame = tk.Frame(win, padx=32, pady=24)
frame.pack()

label = tk.Label(frame, text="Нажмите кнопку — откроется браузер с веб-интерфейсом.")
label.pack(pady=(0, 16))

btn = tk.Button(
    frame,
    name="run_btn",
    text="Запустить",
    command=start_server,
    width=28,
    height=2,
    cursor="hand2",
    font=("", 12, "bold"),
    bg="#00d4aa",
    fg="#08080c",
    activebackground="#00f5c4",
    activeforeground="#08080c",
    relief="flat",
)
btn.pack()
btn.bind("<Enter>", lambda e: btn.config(bg="#00f5c4"))
btn.bind("<Leave>", lambda e: btn.config(bg="#00d4aa") if btn["state"] != "disabled" else None)

win.eval("tk::PlaceWindow . center")
win.mainloop()
