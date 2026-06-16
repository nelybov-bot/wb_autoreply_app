# WB AutoReply App (MarketAI)

## Краткое описание

**WB AutoReply App** — приложение для автоматизации работы селлера на маркетплейсах **Wildberries**, **Ozon** и (частично) **Яндекс.Маркет**. Изначально — desktop-приложение на macOS (CustomTkinter). Сейчас основной режим разработки и деплоя — **веб-интерфейс** (FastAPI + статический SPA на vanilla JS).

Основные возможности:

- Загрузка и обработка **отзывов** и **вопросов** покупателей
- Генерация ответов через **OpenAI** и массовая отправка на площадки
- **Чаты покупателей** WB и Ozon (просмотр, черновик ИИ, отправка, массовые операции)
- **Автозапуск** по расписанию (слоты MSK): отзывы, вопросы, чаты, Ozon-акции
- **Акции Ozon**: просмотр, ручное и авто-удаление товаров
- **Связки карточек** (WB imtID / Ozon «Название модели»): каталог, предложения, пулы, ИИ-подсказки
- **Ozon TMS qty-table**: связывание по таблице количеств в упаковке
- **Алерты Ozon** (штрафы, угрозы скрытия) и **ошибки карточек** в отзывах/чатах
- **Telegram**: отчёты, уведомления, AI-агент в боте
- **AI-агент** в веб-UI: оркестрация pipeline через инструменты
- **Пользователи и роли**: admin / guest, гранулярные permissions
- **Аудит** операций и журналы

## Репозиторий и деплой

| Параметр | Значение |
|----------|----------|
| GitHub | `nelybov-bot/wb_autoreply_app` (ветка `main`) |
| Python | 3.10+ (Render: 3.10.15) |
| Production | [Render.com](https://render.com) — `render.yaml`, `requirements-web.txt` |
| Альтернатива | VPS (Timeweb и др.) — `deploy/timeweb/setup_vps.sh` |
| Локальный веб | `python3 run_web.py` → `http://127.0.0.1:8000` |
| Desktop (legacy) | `python3 run.py` / `run.command` (только macOS, не из Cursor IDE) |

## Структура каталогов

```
wb_autoreply_app/
├── app/                    # Исходный код приложения
│   ├── main.py             # Desktop GUI (CustomTkinter), ~1190 строк
│   ├── db.py               # SQLite слой, схема, CRUD (~1690 строк)
│   ├── logging_config.py   # Настройка логирования
│   ├── core/               # Бизнес-логика, API-клиенты, workflows (19 модулей)
│   ├── web/                # FastAPI сервер, задачи, статика
│   ├── agent/              # AI-агент (оркестратор, tools, Telegram)
│   ├── ui/                 # Диалоги для desktop (dialogs.py)
│   └── assets/themes/      # JSON-темы для CustomTkinter
├── docs/                   # Документация (читать перед задачами)
│   ├── WORKFLOW.md         # Канон процесса разработки
│   ├── PROJECT.md          # Этот файл
│   ├── ARCHITECTURE.md     # Слои, потоки, модули
│   ├── API.md              # Справочник REST API
│   ├── ENV.md              # Env и ключи app_settings
│   ├── AI_CONTEXT.md       # Правила для AI
│   ├── SESSION.md          # Текущее состояние
│   ├── TASKS.md            # Бэклог
│   ├── CHANGELOG.md
│   └── BUGS.md
├── data/                   # SQLite БД (reviews.db) — в .gitignore
├── logs/                   # app.log — в .gitignore
├── deploy/timeweb/         # nginx, systemd, setup VPS
├── run.py                  # Точка входа desktop
├── run_web.py              # Точка входа веб-сервера
├── launch_web.py           # Tk-лаунчер веба с кнопкой
├── requirements.txt        # Desktop: customtkinter + fastapi + aiohttp
├── requirements-web.txt    # Web-only (без customtkinter)
├── render.yaml             # Blueprint Render
├── README.md               # Устаревший фокус на desktop (см. заметку ниже)
└── DEPLOY.md               # Инструкция деплоя на Render
```

## Зависимости

### requirements-web.txt (production)

- `aiohttp==3.9.5` — HTTP к WB/Ozon/OpenAI
- `fastapi==0.115.6` — REST API
- `uvicorn[standard]==0.32.1` — ASGI-сервер

### requirements.txt (desktop)

- Всё из web + `customtkinter==5.2.2`

### Внешние сервисы (ключи в БД / env)

| Сервис | Назначение |
|--------|------------|
| Wildberries API | Отзывы, вопросы, чаты, Content API (связки) |
| Ozon Seller API | Отзывы, вопросы, чаты, акции, каталог |
| Яндекс.Маркет | Частичная поддержка (`yam_client.py`) |
| OpenAI API | Генерация ответов, AI-агент, card-links AI |
| Telegram Bot API | Отчёты, алерты, агент |

## Переменные окружения (сервер)

| Переменная | Обязательность | Назначение |
|------------|----------------|------------|
| `SESSION_SECRET` | **Да** (prod) | Подпись cookie сессии |
| `ADMIN_INIT_PASSWORD` | При первом старте без users | Создание admin |
| `ADMIN_RESET_TOKEN` | Для `/reset` | Сброс пароля admin |
| `CORS_ORIGINS` | Опционально | Разрешённые origins через запятую |
| `COOKIE_SECURE` | Опционально | Cookie с флагом Secure (HTTPS) |
| `PYTHON_VERSION` | Render | 3.10.15 |

Полный список env и ключей `app_settings`: [ENV.md](./ENV.md).  
Секреты магазинов и OpenAI хранятся в **SQLite** (`data/reviews.db`), не в env.

## Данные

- **БД:** `data/reviews.db` (SQLite, `check_same_thread=False`, RLock)
- **Лог:** `logs/app.log`
- **Не коммитить:** `.gitignore` исключает `*.db`, `logs/`, `.env`

На Render без persistent disk данные **сбрасываются** при деплое/рестарте. Рекомендуется Render Disk или VPS.

## Быстрый старт для нового разработчика

1. Прочитать [WORKFLOW.md](./WORKFLOW.md) — обязательный процесс
2. [PROJECT.md](./PROJECT.md) — что за проект, как запустить
3. [ARCHITECTURE.md](./ARCHITECTURE.md) — слои и потоки данных
4. [API.md](./API.md) + [ENV.md](./ENV.md) — справочники при работе с сервером
5. [AI_CONTEXT.md](./AI_CONTEXT.md) — грабли и соглашения
6. Локально: `pip install -r requirements-web.txt && python3 run_web.py` → http://127.0.0.1:8000
7. Первый вход: задать `ADMIN_INIT_PASSWORD` или создать admin через env при пустой БД

Корневой `README.md` описывает только desktop — для веб-разработки ориентироваться на `docs/`.

## Режимы запуска

| Команда | Режим |
|---------|-------|
| `python3 run.py` | Desktop GUI |
| `python3 run_web.py` | Uvicorn localhost:8000 |
| `uvicorn app.web.server:app --host 0.0.0.0 --port $PORT` | Production |
| `./run.command` | macOS: desktop через Terminal |

## Веб-интерфейс (панели)

SPA в `app/web/static/index.html` (~1250 строк) + `app.js` (~5900) + `styles.css` (~4150):

| Панель (`data-tab`) | Назначение |
|---------------------|------------|
| `summary` | Сводка, метрики качества Ozon |
| `stores` | CRUD магазинов WB / Ozon / YAM |
| `reviews`, `questions` | Очередь, генерация, отправка |
| `wb-chats` | Чаты покупателей WB |
| `ozon-chats` | Чаты Ozon (покупатели / поддержка / все) |
| `ozon-actions` | Акции: список, автоудаление, ручное снятие |
| `card-links` | Связки карточек: предложения, перепроверка, каталог |
| `auto` | Автозапуск по расписанию MSK |
| `agent` | AI-агент в веб-UI |
| `settings` | OpenAI, Telegram, чаты, карточки, Ozon, UI, users, промпты, экспорт |
| `card-errors` | Ошибки в карточках из отзывов/чатов |
| `ozon-alerts` | Важные сообщения Ozon |
| `log` | Dev- и ops-журналы |

REST API: [API.md](./API.md).

Аутентификация: cookie `wb_session`, страницы `/login`, `/reset`.

## Обязательный процесс разработки

Все изменения в проекте (люди и AI) ведутся по единому workflow.  
**Полная версия:** [WORKFLOW.md](./WORKFLOW.md).

### Перед любой задачей

Читать **по порядку**:

1. [PROJECT.md](./PROJECT.md) — этот файл  
2. [ARCHITECTURE.md](./ARCHITECTURE.md)  
3. [AI_CONTEXT.md](./AI_CONTEXT.md)  
4. [SESSION.md](./SESSION.md)  
5. [TASKS.md](./TASKS.md)  

### Перед изменением кода

- Кратко описать **что** и **зачем** меняется  
- Перечислить **файлы**, которые будут затронуты  
- Указать **риски** (регрессии, API-лимиты, секреты, desktop/web)

### После изменения кода

| Всегда | По необходимости |
|--------|------------------|
| `SESSION.md` | `ARCHITECTURE.md` — если менялась архитектура |
| `CHANGELOG.md` | `AI_CONTEXT.md` — новые договорённости |
| | `BUGS.md` — новый или исправленный баг |
| | `TASKS.md` — новая или закрытая задача |

### Завершение задачи

В `SESSION.md` — блок **Handoff**: сделано, осталось, следующий шаг.

### Длинная сессия

При переполнении контекста или смене направления — обновить `SESSION.md` полностью и начать новый чат с чтения docs.

## Заметки о зрелости проекта

- README в корне описывает в основном **desktop**; фактическая активная разработка — **web + card-links + agent**
- Нет видимого набора автотестов (`tests/` отсутствует)
- Крупные монолитные файлы: `server.py` (~3980), `app.js` (~5900), `card_links.py` (~2325), `workflows.py` (~2030)
- Desktop и web **делят** `db.py`, `core/workflows.py`, клиенты — изменения затрагивают оба режима

## Связанные документы

- [WORKFLOW.md](./WORKFLOW.md) — **обязательный процесс разработки**
- [ARCHITECTURE.md](./ARCHITECTURE.md) — слои и потоки данных
- [API.md](./API.md) — справочник REST API (~70 endpoints)
- [ENV.md](./ENV.md) — переменные окружения и настройки БД
- [AI_CONTEXT.md](./AI_CONTEXT.md) — контекст для AI-ассистентов
- [TASKS.md](./TASKS.md) — бэклог
- [BUGS.md](./BUGS.md) — известные проблемы
- [CHANGELOG.md](./CHANGELOG.md) — история изменений
- [SESSION.md](./SESSION.md) — текущее состояние сессии разработки
