#!/bin/bash
# Скрипт запуска WB Автоответчик (macOS/Linux): двойной клик или в терминале: ./run.command
cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
    echo "Виртуальное окружение не найдено. Создаём и устанавливаем зависимости..."
    python3 -m venv .venv
    .venv/bin/pip install -r requirements.txt
fi
source .venv/bin/activate
exec python3 run.py
