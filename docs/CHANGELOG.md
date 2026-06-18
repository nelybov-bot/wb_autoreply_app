# CHANGELOG

Формат: дата, commit (если есть), краткое описание.  
Полная история: `git log --oneline`.

---

## 2026-06-18 — WB каталог >1000 карточек

### Backend
- `wb_content_client.list_cards_all`: корректная пагинация по размеру страницы (не `cursor.total`), дедуп nmID, одна HTTP-сессия на весь каталог
- `card_links`: ускорены эвристики предложений на больших каталогах (лимиты O(n²) кластеризации и attach/review)
- `build_wb_catalog_payload`, таймаут 10 мин на `/api/card-links/wb/.../catalog`

### Frontend
- Загрузка каталога card-links: `timeoutMs: 600000` (`app.js?v=45`)

---

## 2026-06-16 — Корова: SVG вместо CSS

### Frontend
- Экран offline: нормальная SVG-корова вместо сломанного CSS-блоба; анимация хвоста, ушей, моргания

---

## 2026-06-16 — Loading-анимация убрана

### Frontend
- Удалён `fx-dot-loading`; progress-бары и poll задач снова показывают простой текст

---

## 2026-06-16 — Лампа в шапке, свечи убраны

### Frontend
- Лампочка: компактный SVG в `header-right`, шнур переключает тему (без fixed-виджета у края экрана)
- Логин: удалены свечи и `playLoginCandles`; прямой запрос auth

---

## 2026-06-16 — UI-виджеты (лампа, корова, loading)

### Frontend
- `fx-widgets.css` / `fx-widgets.js`: лампочка со шнуром (тема), корова при потере сети, loading с точкой
- Лампочка в `/app` — переключение темы без настроек
- Корова: полноэкран при Failed to fetch
- Прогресс-бары: бегущая точка по тексту (цвет accent)

---

## 2026-06-16 — Исправление UI-багов и server.py

### Backend
- Восстановлены отступы в `server.py` (SyntaxError в telegram report, auto scheduler, API tasks)
- `sort_catalog_rows` в ответах `/api/card-links/{wb|ozon}/catalog`

### Frontend
- Dev-журнал: корректные переносы строк (`join('\n')`)
- Импорт настроек: обновление списка магазинов при активной вкладке «Магазины»
- Настройки: очистка secret-полей после сохранения через `loadSettings()`
- Журнал: слушатель `log-level`; опция Dev скрыта без `view_log`
- Card-links: подсветка активной вкладки; CSS `[hidden]` для фильтра каталога
- Кэш: `app.js?v=41`, `styles.css?v=23`

---

## 2026-06-06 — Полный аудит документации (без изменений кода приложения)

### Документация
- Сверка всех файлов `docs/` с актуальным кодом
- Созданы `docs/API.md` (~70 REST endpoints) и `docs/ENV.md` (env + app_settings)
- Исправлена сломанная структура каталогов в PROJECT.md
- ARCHITECTURE: добавлены пропущенные модули core (chat_common, card_check, quality_metrics, config_backup и др.), 25 инструментов агента
- AI_CONTEXT, WORKFLOW: ссылки на API.md и ENV.md
- Обновлены SESSION, TASKS, BUGS

---

## 2026-06-06 — Документация и workflow (без изменений кода приложения)

### Документация проекта
- Создана папка `docs/` с PROJECT, ARCHITECTURE, AI_CONTEXT, SESSION, TASKS, CHANGELOG, BUGS
- Добавлен `docs/WORKFLOW.md` — обязательный процесс разработки
- В `PROJECT.md` и `AI_CONTEXT.md` зафиксированы правила: чтение docs перед задачей, план перед кодом, обновление docs после кода, handoff в SESSION

---

## 2026-06 (card-links стабилизация)

### `73aefbc` — Fix card-links loading bar, catalog bundle order, and attach pools
- Восстановлена функция `group_attach_suggestions` (была сломана: dead code после return)
- Смягчено сопоставление одиночек `_item_matches_group_attach`
- Progress bar: `setPanelLoading` через `hidden`, CSS fix
- Каталог: сортировка блоками по связке, заголовки связок
- Backend: `sort_catalog_rows` в API catalog

### `b431c31` — Fix card-links checkboxes, category labels, and suggestion ordering
- Категории на товарах, связках, разделители по категориям
- Колонка «Категория» в каталоге
- Сортировка предложений: категория → пул → attach → new
- `group_label` WB/Ozon с категорией

### `65dd216` — Fix card-links bulk selection checkboxes and action bars
- Единые чекбоксы `card-links-row-check` для всех типов предложений
- Панель `#card-links-apply-bar` для вкладки Предложения
- Исправлено пересечение review/combine панелей

### `0628528` — Group WB attach suggestions into pools and fix card-links UI
- `group_attach_suggestions()` — пулы attach в одну связку
- Badge «Пул», кнопка «Связать все (N)»

### `f804d5f` / `3fa91df` — Login mascot
- Анимированный кот (замена зайца) на `login.html`

### `5e443b9` — Prevent API keys from being read back through the web UI
- `secret_mask.py`, write-only UI, redaction в логах

---

## 2026-05 — Card Links (начальная реализация)

### `a35973e` — Ozon singles + catalog search
### `23dccc0` — Select-all и bulk apply на вкладках предложений
### `67fcdbe` … `9f6d2f3` — Ozon TMS qty-table linking
### `6ad0ae5` — Ozon card-link suggestions UX
### `ffbf2e6` / `2703fe5` / `e64d9a7` — Review, combine, bulk apply перепроверки
### `d447aaf` / `b68d7ef` / `ed928b5` — Category groups, attach hints, AI
### `6551696` — **Initial** WB and Ozon product card linking

---

## Ранее (автоматизация, чаты, Ozon)

### `0ca47ae` — Fix Telegram getUpdates timeouts flooding ops log

(Более ранние commits — см. `git log`; в этой документации не инвентаризированы полностью.)

---

## Версионирование

Проект **не использует** semver-теги. Версия UI-кэша: `app.js?v=N`, `styles.css?v=N` в `index.html`.

| Файл | Текущая v (на момент документации) |
|------|-------------------------------------|
| app.js | 40 |
| styles.css | 22 |

---

## Шаблон записи

```markdown
### `abcdef0` — Краткий заголовок
- Пункт изменения
- Пункт изменения
```
