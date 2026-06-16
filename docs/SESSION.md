# SESSION — текущее состояние разработки

> Обновлено: 2026-06-16

## Активная ветка

- Локально: fx-widgets (лампа в шапке, корова) + UI-фиксы

---

## Handoff (2026-06-16) — корова SVG

### Сделано

- Экран «Нет соединения»: CSS-корова заменена на SVG (гольштинка, Wi‑Fi с крестом, анимация)
- `fx-widgets.css?v=5`

---

## Handoff (2026-06-16) — loading убран

### Сделано

- Удалена анимация бегущей точки в progress-барах (`fx-dot-loading`)
- Прогресс снова обычным текстом (`textContent`)
- `fx-widgets.css?v=4`, `fx-widgets.js?v=4`, `app.js?v=44`

---

## Handoff (2026-06-16) — лампа и логин

### Сделано

- Лампочка перенесена в `header-right` (SVG, компактный шнур) — без плавающего виджета справа/снизу
- Свечи на странице логина удалены; вход снова сразу через POST `/api/auth/login`
- `fx-widgets.css?v=3`, `fx-widgets.js?v=3`

---

## Handoff (2026-06-16) — fx-widgets

### Сделано

- Лампочка: шнур переключает `theme-dark` (синхрон с настройками)
- Корова: полноэкран при потере соединения (Failed to fetch), кнопки Повторить / Закрыть
- Loading: точка по тексту в progress-барах

### Файлы

- `app/web/static/fx-widgets.css`, `fx-widgets.js`
- `index.html`, `app.js?v=42`

---

## Handoff (2026-06-16) — аудит и исправление UI

### Сделано

- Восстановлены отступы в `server.py` (SyntaxError блокировал запуск сервера)
- Сохранён `sort_catalog_rows` в API каталога card-links (WB/Ozon)
- `app.js`: dev-журнал `join('\n')`, импорт настроек обновляет магазины на активной вкладке, маска секретов после сохранения (`loadSettings`), фильтр `log-level`, скрытие Dev без `view_log`, подсветка вкладок card-links
- `styles.css`: `.card-links-catalog-filter[hidden]`
- Кэш-бастинг: `app.js?v=41`, `styles.css?v=23`

### Осталось

- Закоммитить изменения по запросу пользователя
- Верификация на production (card-links, журнал Dev)
- T-000b: закоммитить `docs/` если ещё не в git

### Следующий шаг

Ручная проверка: запуск `python3 run_web.py`, журнал Dev, импорт настроек на вкладке «Магазины», сохранение OpenAI-ключа.

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
