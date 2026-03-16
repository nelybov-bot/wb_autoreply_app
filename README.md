# WB AutoReply App

Локальное macOS desktop-приложение (Python 3.10+) для ручной загрузки новых отзывов/вопросов Wildberries, массовой генерации ответов через OpenAI и массовой отправки.

## Запуск на macOS

**Важно:** приложение нельзя запускать из встроенного терминала Cursor или VS Code — на macOS в таком случае Tk падает при создании окна (`_RegisterApplication`). Запускайте только так:

1. **Двойной клик** по файлу `run.command` в папке проекта (откроется Терминал и приложение).
2. **Из приложения Terminal (Терминал):** откройте Терминал, перейдите в папку проекта и выполните:
   ```bash
   cd /путь/к/wb_autoreply_app
   python3 run.py
   ```
   Если используете виртуальное окружение:
   ```bash
   source .venv/bin/activate
   python run.py
   ```

При попытке запуска из Cursor/VS Code скрипт выведет сообщение с этими инструкциями и завершится без краша.

## Первая установка (одной строкой)
```bash
cd ~/Downloads/wb_autoreply_app && python3 -m venv .venv && source .venv/bin/activate && pip install -U pip && pip install -r requirements.txt
```
После этого запуск: двойной клик по `run.command` или `python3 run.py` из Терминала.

## Файлы
- DB: `data/reviews.db`
- Лог: `logs/app.log`
