#!/bin/bash
# Запуск сервера с доступом по сети (для телефона в той же Wi‑Fi)
cd "$(dirname "$0")"
echo "Сервер запускается. Для доступа с телефона открой в Safari: http://$(ipconfig getifaddr en0 2>/dev/null || hostname):8000"
echo "Нажми Ctrl+C для остановки."
python3 -m uvicorn app.web.server:app --host 0.0.0.0 --port 8000
