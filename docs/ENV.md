# Переменные окружения и настройки

## Переменные окружения (сервер)

Используются в `app/web/server.py` и при деплое.

| Переменная | Обязательность | Описание |
|------------|----------------|----------|
| `SESSION_SECRET` | **Да** (production) | Секрет для HMAC-подписи cookie `wb_session`. Без него сессии небезопасны. |
| `ADMIN_INIT_PASSWORD` | При первом старте | Пароль первого пользователя `admin`, если таблица `users` пуста |
| `ADMIN_RESET_TOKEN` | Для `/reset` | Токен для `POST /api/auth/admin-reset` |
| `CORS_ORIGINS` | Опционально | Разрешённые origins через запятую (если фронт на другом домене) |
| `COOKIE_SECURE` | Опционально | Если задано непустое значение — cookie с флагом `Secure` (HTTPS) |
| `PYTHON_VERSION` | Render | `3.10.15` (см. `render.yaml`) |
| `PORT` | Production | Порт uvicorn (задаёт Render автоматически) |

### Пример `.env` (локально, не коммитить)

```bash
SESSION_SECRET=change-me-long-random-string
ADMIN_INIT_PASSWORD=your-admin-password
ADMIN_RESET_TOKEN=reset-token-for-emergency
# COOKIE_SECURE=1
# CORS_ORIGINS=https://your-domain.com
```

На VPS (Timeweb): `/etc/wb_autoreply.env` — см. `deploy/timeweb/setup_vps.sh`.

---

## Настройки в SQLite (`app_settings`)

Секреты магазинов и OpenAI хранятся в **БД** (`data/reviews.db`), не в env.  
API `GET /api/settings` возвращает секреты **замаскированными** (`app/core/secret_mask.py`).

### OpenAI

| Ключ | Описание |
|------|----------|
| `openai_key` | API-ключ OpenAI (write-only в UI) |

Модель по умолчанию в коде: `gpt-5.2` (`app/core/openai_client.py`), отдельного ключа в настройках нет.

### Telegram

| Ключ | Описание |
|------|----------|
| `telegram_bot_token` | Токен бота |
| `telegram_chat_id` | Основной чат уведомлений |
| `telegram_report_chat_id` | Чат для периодических отчётов |
| `telegram_card_error_chat_id` | Чат для ошибок карточек |
| `telegram_enabled` | `1` / `0` — уведомления при отправке ответов |
| `telegram_report_enabled` | `1` — периодические отчёты |
| `telegram_report_interval` | `hour` \| `day` и др. |
| `telegram_report_last_sent` | Служебная метка (не экспортируется) |
| `telegram_agent_enabled` | `1` — AI-агент в Telegram |
| `telegram_agent_chat_id` | Чат для агента |
| `telegram_agent_user_id` | Разрешённый Telegram user id |
| `telegram_agent_update_offset` | Служебный offset long polling |

### Проверка карточек (`card_check.py`)

| Ключ | По умолчанию | Описание |
|------|--------------|----------|
| `card_check_enabled` | `1` | Анализ отзывов/вопросов на ошибки карточки |
| `card_check_telegram_enabled` | `1` | Уведомления в Telegram |
| `card_check_include_in_periodic_report` | `1` | Включать в периодический отчёт |
| `card_check_telegram_template` | встроенный | Шаблон сообщения |

### Уведомления Ozon (`ozon_alerts.py`)

| Ключ | Описание |
|------|----------|
| `ozon_alerts_enabled` | `1` — сканирование поддержки |
| `ozon_alerts_telegram_enabled` | Уведомления в Telegram |
| `ozon_alerts_check_from_date` | Дата начала проверки (ISO) |
| `ozon_alerts_telegram_template` | Шаблон сообщения |
| `ozon_alerts_telegram_chat_id` | Отдельный чат (опционально) |

### Чаты покупателей (`chat_common.py`)

| Ключ | Описание |
|------|----------|
| `buyer_chat_reply_from_date` | Не обрабатывать чаты старше даты (ISO) |
| `buyer_chat_auto_max_age_days` | Макс. возраст чата для автоответа (1–30, default 3) |

### Прочее

| Ключ | Описание |
|------|----------|
| `theme` | Тема UI (`light` / `dark`) |
| `auto_schedule_json` | JSON расписания автозапуска |
| `auto_schedule_last_run_at` | Служебная метка последнего запуска |
| `ozon_actions_settings_json` | Глобальные настройки автоудаления из акций |

### Экспорт/импорт

`app/core/config_backup.py` — `GET /api/config/export`, `POST /api/config/import`.  
Экспорт **не включает** API-ключи (только флаги `api_key_set`). Служебные ключи (`*_last_*`, `telegram_agent_update_offset`) не переносятся.

---

## Permissions (роль `guest`)

Admin имеет все права неявно. Для guest настраиваются в `user_permissions`:

| Permission | Доступ |
|------------|--------|
| `view_settings` | Настройки, экспорт конфига, Telegram test |
| `view_log` | Dev-журнал (`/api/log`, `/api/log/dev`) |
| `view_ops_log` | Журнал операций (`/api/log/ops`) |

---

## Данные на диске

| Путь | Описание |
|------|----------|
| `data/reviews.db` | SQLite (магазины, items, users, settings) |
| `logs/app.log` | Файловый лог приложения |

Оба в `.gitignore`. На Render без persistent disk данные теряются при рестарте.

---

## Связанные документы

- [PROJECT.md](./PROJECT.md) — обзор и деплой
- [API.md](./API.md) — REST endpoints
- [DEPLOY.md](../DEPLOY.md) — Render Disk, пошаговый деплой
