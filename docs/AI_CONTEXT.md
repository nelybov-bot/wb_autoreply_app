# AI Context — WB AutoReply App

Документ для AI-ассистентов (Cursor, Codex и др.).  
**Этот файл обязателен к прочтению перед любой задачей** (после PROJECT.md и ARCHITECTURE.md).

Полный процесс: [WORKFLOW.md](./WORKFLOW.md).

---

## Обязательный workflow (всегда соблюдать)

### Шаг A — Перед любой задачей

Читать **строго в порядке**:

1. `docs/PROJECT.md`
2. `docs/ARCHITECTURE.md`
3. `docs/AI_CONTEXT.md` (этот файл)
4. `docs/SESSION.md`
5. `docs/TASKS.md`

При работе с багами — также `docs/BUGS.md`. При необходимости истории — `docs/CHANGELOG.md`.

### Шаг B — Перед изменением кода

В ответе пользователю **кратко** указать:

1. **Что** собираешься изменить и зачем  
2. **Список файлов**, которые будут изменены  
3. **Риски**: регрессии, WB/Ozon rate limits, desktop+web, секреты в БД, кэш `?v=` статики  

Не вносить правки в код приложения, пока это не сформулировано (исключение: явный однострочный фикс по прямому запросу).

### Шаг C — После любого изменения кода

Обновить в **той же сессии**:

| Файл | Когда |
|------|-------|
| `docs/SESSION.md` | **Всегда** |
| `docs/CHANGELOG.md` | **Всегда** |
| `docs/ARCHITECTURE.md` | Изменилась архитектура, API, структура модулей |
| `docs/AI_CONTEXT.md` | Новые договорённости, грабли, соглашения |
| `docs/BUGS.md` | Найден или исправлен баг |
| `docs/TASKS.md` | Добавлена или закрыта задача |

Задачи **только по документации** (без кода приложения) — обновлять SESSION, CHANGELOG и при необходимости PROJECT / AI_CONTEXT / WORKFLOW.

### Шаг D — Завершение задачи (handoff)

В `docs/SESSION.md` добавить секцию **Handoff**:

- Что сделано  
- Что осталось  
- Рекомендуемый следующий шаг  

### Шаг E — Длинный диалог или смена задачи

Если контекст переполнен или направление **существенно** изменилось:

1. Предложить пользователю завершить сессию  
2. Полностью обновить `docs/SESSION.md`  
3. Подготовить проект к новому чату (актуальные TASKS, BUGS)  
4. В новом чате снова выполнить **Шаг A**

### Чеклист AI

```
[ ] PROJECT → ARCHITECTURE → AI_CONTEXT → SESSION → TASKS прочитаны
[ ] Перед кодом: план + файлы + риски озвучены
[ ] После кода: SESSION + CHANGELOG обновлены
[ ] Handoff в SESSION при завершении задачи
[ ] Предложена новая сессия, если контекст раздут
```

---

## Что это за проект

Веб-приложение **MarketAI / WB AutoReply** для селлеров WB и Ozon: отзывы, вопросы, чаты, акции, связки карточек, автозапуск, Telegram, AI-агент.

**Основной стек:** Python 3.10, FastAPI, aiohttp, SQLite, vanilla JS SPA.

**Production:** Render.com (`main` branch auto-deploy). Репозиторий: `nelybov-bot/wb_autoreply_app`.

## Правила для AI

1. **Не коммитить** `data/reviews.db`, `logs/`, `.env` — там API-ключи.
2. **Секреты write-only:** `secret_mask.py` — не возвращать полные ключи в API; UI шлёт маску `••••••••1234` при неизменённом поле.
3. **Минимальный diff** — проект большой, не рефакторить без запроса.
4. **Соглашения:** async в `core/`, sync обёртки через `tasks.py` или `AsyncRunner` (desktop).
5. **Кэш статики:** при изменении `app.js` / `styles.css` — увеличить `?v=` в `index.html`.
6. **WB rate limits:** card-links операции с cooldown (`CARD_LINKS_ACTION_COOLDOWN_MS` в app.js).
7. **Не ломать desktop** при правках `workflows.py` / `db.py` без необходимости.

## Ключевые файлы (приоритет при поиске)

| Задача | Файлы |
|--------|-------|
| REST API | `app/web/server.py` |
| UI логика | `app/web/static/app.js` |
| UI разметка | `app/web/static/index.html` |
| Стили | `app/web/static/styles.css` |
| БД | `app/db.py` |
| Бизнес-процессы | `app/core/workflows.py` |
| Связки карточек | `app/core/card_links.py` |
| Фоновые задачи | `app/web/tasks.py` |
| AI-агент | `app/agent/orchestrator.py`, `tools.py` |
| Маскирование секретов | `app/core/secret_mask.py` |
| Экспорт конфига | `app/core/config_backup.py` |
| Качество Ozon | `app/core/quality_metrics.py` |

## Card Links — доменная модель

### Типы предложений (`kind`)

| kind | Смысл |
|------|-------|
| `new_link` | Создать новую связку из похожих одиночек |
| `attach` | Добавить 1 товар в существующую связку |
| `attach_batch` | Пул: несколько attach в одну связку |
| `combine_suggestions` | Объединить несколько new_link в одну |
| `merge_groups` | Объединить две существующие связки (перепроверка) |
| `relocate` | Переместить товар в более крупную связку |

### UI вкладки

- **Предложения** (`candidates`) — combine + attach + new + ai
- **Перепроверка** (`review`) — merge_groups, relocate
- **Каталог** (`catalog`) — все карточки, фильтр, ручной merge

### API

Полный справочник: [API.md](./API.md). Ключевые card-links routes:

```
GET  /api/card-links/{wb|ozon}/{store_id}/catalog
POST /api/card-links/wb/{store_id}/merge
POST /api/card-links/wb/{store_id}/disconnect
POST /api/card-links/ozon/{store_id}/link
POST /api/card-links/ozon/{store_id}/unlink
POST /api/card-links/ozon/{store_id}/link-qty-table
POST /api/card-links/{wb|ozon}/{store_id}/ai-suggest
```

UI cooldown между merge-операциями: `CARD_LINKS_ACTION_COOLDOWN_MS = 3000` в `app.js`.

### Важные функции Python

- `group_attach_suggestions()` — **обязательна**; была сломана (тело после return) — исправлено в `73aefbc`
- `suggest_attach_to_groups()` — использует `_item_matches_group_attach` (мягче чем review)
- `sort_catalog_rows()` — сортировка на бэкенде
- Frontend: `cardLinksSortCatalogRows()`, `sortCardLinksCandidates()`

## Аутентификация

- Cookie `wb_session` (HMAC, `SESSION_SECRET`)
- Роли: `admin`, `guest`
- Guest permissions: `view_settings`, `view_log`, `view_ops_log`
- Сброс admin: `POST /api/auth/admin-reset` + `ADMIN_RESET_TOKEN`, страница `/reset`

## Env (production)

См. полный справочник [ENV.md](./ENV.md). Минимум:

```
SESSION_SECRET=...          # обязательно
ADMIN_INIT_PASSWORD=...     # первый запуск
ADMIN_RESET_TOKEN=...       # сброс пароля
COOKIE_SECURE=1             # опционально, HTTPS
CORS_ORIGINS=https://...    # опционально
```

## Паттерны кода

### Добавление API endpoint

1. Pydantic model в `server.py`
2. Route + `Depends(require_user)` или `require_admin`
3. Вызов `core` функции
4. Обработка `HttpStatusError` → `_card_links_http_error` (для card-links)

### Добавление UI панели

1. `<section id="panel-...">` в `index.html`
2. Nav link в sidebar
3. Логика в `app.js` (поиск по `panel-` prefix)
4. Стили в `styles.css`

### Фоновая операция

1. `web_tasks.run_*` → возвращает `task_id`
2. UI: `pollTask()` или аналог
3. `TaskControl` для отмены

## Известные грабли

- **Progress bar** (`#card-links-loading`): управляется `setPanelLoading()` через атрибут `hidden`; дублирующий CSS `.progress-wrap` без `display:none` ломал скрытие
- **Checkboxes card-links**: класс `card-links-row-check`, отдельная колонка `col-check`
- **Панели bulk**: `#card-links-apply-bar` (предложения), `#card-links-review-bar` (перепроверка), `#card-links-combine-bar` (объединение new_link)
- **`data-cl-view`** на `#panel-card-links` — CSS скрывает чужие панели
- **Render без disk** — БД пустая после деплоя
- **macOS desktop** — не запускать Tk из Cursor terminal

## Что не трогать без явного запроса

- Массовый рефакторинг `server.py` / `app.js`
- Миграция на PostgreSQL
- Удаление desktop (`main.py`)
- Изменение схемы БД без миграции (`db.py` `_migrate` inline)

## Полезные команды

```bash
# Локальный веб
python3 run_web.py

# Синтаксис JS
node --check app/web/static/app.js

# Импорт card_links
python3 -c "from app.core.card_links import group_attach_suggestions"

# Git (не пушить без запроса)
git status && git diff
```

## Язык

- UI и сообщения пользователю: **русский**
- Код, комментарии, commit messages: русский или английский (в репо смешанно; новые commits — английский краткий subject)

## Документация

- [WORKFLOW.md](./WORKFLOW.md) — обязательный процесс (канон)
- [PROJECT.md](./PROJECT.md)
- [ARCHITECTURE.md](./ARCHITECTURE.md)
- [API.md](./API.md) — REST endpoints
- [ENV.md](./ENV.md) — env и app_settings
- [BUGS.md](./BUGS.md)
- [TASKS.md](./TASKS.md)
- [SESSION.md](./SESSION.md)
- [CHANGELOG.md](./CHANGELOG.md)
