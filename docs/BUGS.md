# BUGS — известные проблемы и ограничения

Статусы: `open` | `fixed` | `wontfix` | `monitoring`

---

## Критические / исправленные недавно

### BUG-001: `group_attach_suggestions` отсутствовала как функция
- **Статус:** `fixed` (`73aefbc`)
- **Симптом:** ImportError на сервере ИЛИ пулы attach не создавались; одиночки не группировались
- **Причина:** Тело функции оказалось после `return` внутри `suggest_attach_to_groups` (dead code)
- **Проверка:** `python3 -c "from app.core.card_links import group_attach_suggestions"`

### BUG-002: Progress bar «Загрузка…» не скрывался после загрузки каталога
- **Статус:** `fixed` (`73aefbc`)
- **Симптом:** Статус «Загружено N карточек», но бар виден
- **Причина:** `setPanelLoading` не управлял `hidden`; дублирующий CSS `.progress-wrap` без `display:none`
- **Файлы:** `app.js` (`setPanelLoading`), `styles.css`

### BUG-003: Чекбоксы bulk-выбора не отображались у предложений
- **Статус:** `fixed` (`65dd216`, `b431c31`)
- **Симптом:** Только кнопки «Связать все», нет чекбоксов
- **Причина:** Чекбоксы рендерились только для `new_link`; спрятаны в заголовке; thead скрыт
- **Файлы:** `app.js`, `index.html`, `styles.css`

### BUG-004: Панель «Применить выбранные» (review) на вкладке Предложения
- **Статус:** `fixed` (`65dd216`)
- **Причина:** CSS `display:flex` перебивал `[hidden]` на `.card-links-review-wrap`

---

## Открытые — функциональность

### BUG-010: Не все одиночные товары попадают в предложения связок
- **Статус:** `open` / `monitoring`
- **Симптом:** Пользователь видит одиночки в каталоге, но нет attach/new_link предложения
- **Известные причины:**
  1. Название не проходит `_titles_related_enough` / `_item_matches_group_attach`
  2. Разный `subject_id` / `parent_id` (WB) или `category_key` (Ozon)
  3. Целевая связка уже `MAX_LINK_ITEMS` (30)
  4. Для `new_link` нужно минимум 2 похожих одиночки в категории
  5. Товар без `subject_id` (WB) пропускается в `suggest_link_candidates`
- **Нужно:** конкретные nmID для воспроизведения
- **Файлы:** `card_links.py` — `suggest_attach_to_groups`, `suggest_link_candidates`

### BUG-011: Дублирование логики сортировки каталога
- **Статус:** `open`
- **Симптом:** Расхождение порядка при фильтрации на клиенте vs сырой ответ API
- **Файлы:** `sort_catalog_rows` (Python), `cardLinksSortCatalogRows` (JS)
- **Риск:** Регрессии при правке только одной стороны

### BUG-012: Progress bar без реального прогресса
- **Статус:** `open`
- **Симптом:** Indeterminate анимация при загрузке каталога WB (до N страниц API)
- **Ограничение:** Endpoint синхронный, нет streaming progress
- **Улучшение:** task-based catalog load с poll (см. TASKS T-011)

### BUG-039: WB каталог не загружался при >1000 карточек
- **Статус:** `fixed` (2026-06-18)
- **Симптом:** «Загрузить» на вкладке Связки зависает или падает по таймауту на крупных магазинах WB
- **Причина:** O(n²) эвристики предложений (кластеризация по названию в одной категории) + долгая пагинация WB без reuse сессии; неверный критерий `cursor.total < 100` (total — размер всего кабинета)
- **Файлы:** `wb_content_client.py`, `card_links.py`, `server.py`, `app.js`
- **Проверка:** магазин WB с 1500+ карточек, hard refresh `app.js?v=45`

---

## Открытые — инфраструктура

### BUG-020: Потеря данных на Render Free без Disk
- **Статус:** `open` (by design)
- **Симптом:** После деплоя/рестарта пустые магазины и настройки
- **Mitigation:** Render Disk, VPS, не коммитить `reviews.db`
- **Док:** `DEPLOY.md`

### BUG-021: In-memory tasks и agent sessions
- **Статус:** `open`
- **Симптом:** Рестарт сервера обрывает фоновые задачи и сессии агента
- **Файлы:** `tasks.py`, `agent/session.py`

### BUG-022: Нет автотестов
- **Статус:** `open`
- **Риск:** Регрессии в card_links (уже случались: BUG-001, BUG-003)

---

## Открытые — UX / UI

### BUG-034: Dev-журнал без переносов строк
- **Статус:** `fixed` (2026-06-16)
- **Симптом:** Строки лога склеивались в одну
- **Причина:** `join('\\n')` вместо `join('\n')` в `app.js`
- **Файлы:** `app/web/static/app.js`

### BUG-035: Импорт настроек не обновлял список магазинов
- **Статус:** `fixed` (2026-06-16)
- **Симптом:** На вкладке «Магазины» список устаревал после импорта JSON
- **Причина:** Проверка `panel.hidden` вместо `classList.contains('active')`
- **Файлы:** `app/web/static/app.js`

### BUG-036: Секреты видны после сохранения настроек
- **Статус:** `fixed` (2026-06-16)
- **Симптом:** OpenAI/Telegram ключи оставались в полях после «Сохранить»
- **Причина:** `saveServerSettings()` не вызывал `loadSettings()`
- **Файлы:** `app/web/static/app.js`

### BUG-037: Фильтр каталога card-links виден на других вкладках
- **Статус:** `fixed` (2026-06-16)
- **Причина:** `display:flex` перебивал `[hidden]` без `!important`
- **Файлы:** `app/web/static/styles.css`

### BUG-038: server.py SyntaxError (сломанные отступы)
- **Статус:** `fixed` (2026-06-16)
- **Симптом:** Сервер не запускался
- **Файлы:** `app/web/server.py`

### BUG-030: Корневой README устарел
- **Статус:** `open`
- **Симптом:** Описан только desktop macOS; не упомянуты web, card-links, agent
- **Файл:** `README.md`

### BUG-031: `app.js` монолит ~5900 строк
- **Статус:** `open` (tech debt)
- **Симптом:** Сложно сопровождать, риск конфликтов, нет typecheck

### BUG-032: `server.py` монолит ~3980 строк
- **Статус:** `open` (tech debt)

### BUG-033: Кэш браузера после деплоя
- **Статус:** `monitoring`
- **Симптом:** Пользователь видит старый UI без чекбоксов
- **Mitigation:** Увеличивать `?v=` в index.html; hard refresh

---

## Ограничения API (не баги)

| ID | Ограничение |
|----|-------------|
| LIM-001 | WB Content API rate limit — cooldown между merge операциями |
| LIM-002 | Макс. 30 товаров в связке (`MAX_LINK_ITEMS`) |
| LIM-003 | Каталог WB: пагинация `max_pages` (1–150); усечение предложений: `suggest_link_candidates` [:150], `suggest_attach_to_groups` [:200], `group_attach_suggestions` [:120], `suggest_review_linked_groups` [:120] |
| LIM-004 | Ozon catalog: `max_pages` default 30, max 100 |
| LIM-005 | Render free tier sleep после 15 мин неактивности |

---

## Desktop-specific

### BUG-040: Tk crash из терминала Cursor/VS Code на macOS
- **Статус:** `wontfix` (documented)
- **Workaround:** `run.command` или внешний Terminal
- **Файл:** `run.py` — `_is_macos_gui_unsafe()`

---

## Как добавлять баг

```markdown
### BUG-XXX: Краткий заголовок
- **Статус:** open
- **Симптом:** ...
- **Причина:** ...
- **Файлы:** ...
- **Воспроизведение:** ...
```

При исправлении: статус → `fixed`, commit hash, дата.
