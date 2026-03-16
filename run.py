#!/usr/bin/env python3
"""Запуск приложения WB Автоответчик. Запуск: python3 run.py  или  ./run.py"""
import sys
import os
import subprocess

def _is_macos_gui_unsafe() -> bool:
    """На macOS Tk падает при запуске из терминала Cursor/IDE. Проверяем такой контекст."""
    if sys.platform != "darwin":
        return False
    term = os.environ.get("TERM_PROGRAM", "").lower()
    if "cursor" in term or "vscode" in term or "code" in term:
        return True
    try:
        ppid = os.getppid()
        out = subprocess.run(
            ["ps", "-p", str(ppid), "-o", "comm="],
            capture_output=True,
            timeout=2,
            text=True,
        )
        if out.returncode == 0 and out.stdout:
            name = out.stdout.strip().lower()
            if "cursor" in name or "electron" in name:
                return True
    except Exception:
        pass
    return False

# Добавляем корень проекта в путь
_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _root)
os.chdir(_root)

if __name__ == "__main__":
    if _is_macos_gui_unsafe():
        print(
            "Ошибка: на macOS приложение нельзя запускать из терминала Cursor/VS Code — "
            "Tk падает при регистрации окна.\n\n"
            "Запускайте одним из способов:\n"
            "  1. Двойной клик по файлу  run.command  в папке проекта\n"
            "  2. В терминале:  ./run.command  или  python3 run.py\n",
            file=sys.stderr,
        )
        sys.exit(1)
    from app.main import main
    main()
