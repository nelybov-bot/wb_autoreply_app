# SESSION — текущее состояние разработки

> Обновлено: 2026-06-06

## Активная ветка

- `main` — production, auto-deploy Render
- Последний commit кода: `73aefbc` — Fix card-links loading bar, catalog bundle order, and attach pools
- Документация: полный аудит `docs/` выполнен (см. Handoff ниже), **не закоммичено**

---

## Handoff (2026-06-06) — аудит документации

### Сделано

- Прочитаны все 8 файлов `docs/` и сверен код (`server.py`, `core/*`, `agent/*`, `db.py`, static UI)
- Исправлены несоответствия: сломанная структура каталогов в PROJECT.md, устаревшие оценки строк, пропущенные API и модули
- Созданы **API.md** (справочник ~70 endpoints) и **ENV.md** (env + ключи `app_settings`)
- Обновлены: PROJECT, ARCHITECTURE, AI_CONTEXT, WORKFLOW, CHANGELOG, TASKS, BUGS, SESSION
- Код приложения **не изменялся**

### Осталось

- Закоммитить `docs/` в git (T-000b)
- Верификация card-links на production после `73aefbc`
- Обновить корневой `README.md` (T-004) — вне scope аудита docs
- Диагностика «пропущенных» одиночек (нужны nmID от пользователя)

### Следующий рекомендуемый шаг

1. Закоммитить обновлённую документацию `docs/`
2. Новому разработчику: читать PROJECT → ARCHITECTURE → API/ENV → AI_CONTEXT → SESSION → TASKS

---

## Недавняя работа по коду (контекст)

### Card Links (`6551696` … `73aefbc`)

- Пулы attach, чекбоксы, категории, сортировка каталога
- Fix `group_attach_suggestions`, progress bar, attach matching

### Безопасность (`5e443b9`)

- `secret_mask.py`, write-only ключи, `ADMIN_RESET_TOKEN`

### Деплой

- Render (основной), Timeweb VPS (скрипты в `deploy/timeweb/`)

## Открытые вопросы (production)

| Проблема | Статус на `73aefbc` |
|----------|---------------------|
| Card-links UI после деплоя | Требует верификации |
| Одиночки не в предложениях | Частично исправлено; нужны примеры |

## Как продолжить в новом чате

1. Прочитать по порядку: **PROJECT → ARCHITECTURE → AI_CONTEXT → SESSION → TASKS** ([WORKFLOW.md](./WORKFLOW.md))
2. Справочники: [API.md](./API.md), [ENV.md](./ENV.md)
3. `git log -5 --oneline` и `git status`
4. Следовать workflow: план перед кодом, обновление docs после кода
5. Не пушить без явного запроса
