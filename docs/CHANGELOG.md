# CHANGELOG

Формат: дата, commit (если есть), краткое описание.  
Полная история: `git log --oneline`.

---

## 2026-06-18 — Точечная развязка перед перепривязкой

### Backend
- `wb_merge_cards`: `disconnect_first` (по умолчанию true) — развязка только moving nm_id перед merge
- `wb_disconnect_cards`: батчи по 30 nm_id
- `ozon_link_by_model`: `unlink_first` — развязка offer с другой моделью перед link

### API / Frontend
- `POST .../merge`: `disconnect_first`; `POST .../link`: `unlink_first`
- Подтверждение: «Сначала развязать N карточек, затем объединить…»

---

## 2026-06-18 — Покрытие bin-pack и метрики uncovered

### Backend
- `brand_general`, `balm_unspecified`, `cream_unspecified` в bin-pack (не только косметика с точным use)
- `bin_covered` — только артикулы из реальных предложений (не «уже ок»)
- meta: `already_optimal`, `uncovered_unlinked`, `needs_attention`

### Frontend
- Статус ИИ: «в предложениях» / «уже в норме» / «без связки» вместо одного «без предложения: 5300»

---

## 2026-06-18 — Bin-pack по всему пулу (CARD_LINKING_REVIEW шаг 1)

### Backend (`card_links.py`)
- `deterministic_bin_pack_from_pool` — первичная упаковка пула по subject + бренд + назначение
- `_facet_aware_pack_bins` — фасовки 1/2/3 как атомы; мягкий приоритет title_base при добивке до 30
- Pack-артикулы больше не исключаются из `ai_pool`
- `_merge_ai_suggestions_by_use_bucket`: без гейта `len(cluster)<2`, facet-aware split
- `default_ai_system_prompt`: роль ИИ — дополнение bin-pack; не дублировать бренд+назначение

---

## 2026-06-18 — ИИ-связки: применение, выбор, пагинация

### Backend
- `_merged_apply_candidate`: несколько товаров с kind `relocate`/`attach` → `merge_groups` (не ломает «Применить связку»)

### Frontend
- Нормализация кандидата перед apply; bulk-select по галочке слева (не «Склейка 2»)
- Селектор размера страницы: 50 / 100 / 200 / Все
- Лимит 30: в «добавляете N» только товары не из целевой связки

### Backend
- `_external_moving_items` + `_split_target_merge_chunks`: перенос в существующую связку >30 режется на части

---

## 2026-06-18 — Убрано деление по линейкам

### Backend
- Склейка только по **бренд + назначение** в subjectID (не линейка, не imtID)
- ISANA Urea + Cream & Care + Traubenkernöl — одна связка (если одно назначение)
- Промпт: не делить по линейкам/вкусам/мл

---

### Backend
- `hair_gel` в классификаторе («Гель для волос»)
- `_USE_MERGE_BRAND_SCOPE` + UI consolidate: Balea MEN Wet Look + Ultra Strong в одной связке
- Гели для душа (ISANA Urea / Cream) — по линейке, не весь бренд

---

### Backend
- `_bundle_bucket_key`: для губ/блесков — один UI-блок на бренд (не на imtID)
- lavera 03 + 04 → 6 товаров в одной связке; Balea отдельно
- apply: target imtID выбирается по числу карточек в группе

---

### Backend
- `_STRICT_SEPARATE_BRANDS` (Labello, Balea, ISANA, lavera, …) — детект в названии
- `_bundle_bucket_key` с брендом — UI не склеивает «для губ» из разных брендов
- ISANA гели: склейка по линейке, не весь бренд; объём мл/г не влияет
- Заголовок связки: «Balea · для губ…» вместо «Нет бренда»

---

### Backend
- use-merge: subjectID + назначение + **бренд** (не смешивать Labello и Balea)
- `_row_brand_key` из названия; валидатор «разные бренды»
- «Жидкая помада» / «Блески» → lipstick; оттенки lavera в одной связке

---

### Backend
- «Гигиеническая помада» → `lips`; склейка `lip_care` (lips + lipstick) в subjectID
- Fix `_title_base_key` для объёмов 4.8 г и фасовок 5 шт
- `_row_use_bucket`, DE/EN паттерны, `_use_merge_category_key` по subjectID

---

## 2026-06-18 — UI вкладки ИИ: убрать «колхоз»

### Frontend
- Фикс скрытия пагинации (`[hidden]` + `display:flex`)
- Фильтры в сворачиваемый блок; на ИИ закрыты по умолчанию
- Панель ИИ: lead + actions; пагинация внутри панели
- Скрыт thead каталога на ИИ; короткий empty-state
- `app.js?v=60`

---

## 2026-06-18 — ИИ-связки: пагинация, порядок категорий, склейка 2 связок

### Backend
- `consolidate_ai_bundle_previews`: сортировка по категории, затем названию связки

### Frontend
- Вкладка ИИ: пагинация ~100 товаров на страницу (связка целиком, не режется)
- Категории строго по порядку, без перемешивания
- Галочка «Склейка» (макс. 2) + кнопка «Склеить 2 связки» в одной категории
- «Применить связку»: связка сразу исчезает со списка (запрос в WB/Ozon в фоне)
- `app.js?v=59`

---

## 2026-06-18 — Редактор промпта ИИ-связок в UI

### Backend
- `default_ai_system_prompt`, `resolve_ai_system_prompt` — встроенный и кастомный промпт
- Хранение в `app_settings`: `card_links_ai_prompt_wb`, `card_links_ai_prompt_ozon`
- GET/PUT `/api/card-links/ai-prompt/{marketplace}`; `ai-suggest` читает сохранённый промпт
- Плейсхолдер `{max_link_items}` в кастомном тексте

### Frontend
- Textarea в «Настройки ИИ-связок»: сохранить / вернуть встроенный
- Отдельный промпт для WB и Ozon (по выбранному маркетплейсу)
- `app.js?v=57`

---

## 2026-06-18 — Откат brand-merge (fix 332 товаров в связке)

### Backend
- Убрано слияние всего бренда в категории
- Связка = только фасовки 1/2/3 одной линейки (до 30)
- consolidate: разбиение на части по 30 в UI

### Frontend
- `app.js?v=56`

---

## 2026-06-18 — Фасовки 1/2/3 не дробить

### Backend
- Авто-фасовки: `merge_groups` если 1/2/3 шт в разных imtID
- `_merge_ai_suggestions_by_pack_key` перед brand-merge
- Промпт ИИ: главное правило — фасовки всегда в одной связке

---

## 2026-06-18 — Слияние ИИ-связок по бренду в категории

### Backend
- `_merge_ai_suggestions_by_brand_category` после pack/AI, до consolidate
- Balea/ARTDECO в одной категории → одна итоговая связка (merge_groups в главный imtID)

### Frontend
- Статус: «объединено по бренду: N» · `app.js?v=55`

---

## 2026-06-18 — ИИ по категориям целиком

### Backend
- `_build_ai_category_batch_jobs`: 1 OpenAI-запрос на категорию, split только при превышении cap
- Промпт: `category`, `product_count`; meta: `categories_total`, `ai_mode: category`

### Frontend
- Настройки: «Лимит товаров в категории», «Макс. категорий за запуск»
- `app.js?v=54`

---

## 2026-06-18 — Процент прогресса ИИ + параллельные батчи

### Backend
- До **3 параллельных** вызовов OpenAI (`AI_SUGGEST_PARALLEL`)
- Общий клиент OpenAI на задачу; батч по умолчанию 60
- Прогресс: шаги prep → N батчей → сборка (cur/total для %)

### Frontend
- Determinate progress bar + метка «N%» для ИИ-задачи
- `app.js?v=53`, `styles.css?v=29`

---

## 2026-06-18 — Фоновый ИИ-связки (fix таймаута Render)

### Backend
- `ai-suggest` запускает фоновую задачу `card_links_ai`, ответ `{ task_id }`
- `max_ai_batches` (default 12), meta: batches_planned/run/skipped
- Батчи сортируются по размеру категории (сначала крупные)

### Frontend
- Опрос задачи, прогресс «ИИ: запрос N/M», без 10-мин HTTP hang
- Настройка лимита батчей; снятие cow-overlay при ошибке
- `app.js?v=52`

---

## 2026-06-18 — Итоговые связки ИИ + полный каталог

### Backend
- `consolidate_ai_bundle_previews`: операции ИИ → итоговые связки для UI
- `max_products=0` — весь загруженный каталог; meta: analyzed, uncovered, truncated

### Frontend
- Карточки итоговых связок вместо «кусочков» операций
- `app.js?v=51`, `styles.css?v=28`

---

## 2026-06-18 — ИИ-связки с редактированием

### Backend
- `ai_suggest_card_links`: весь каталог (linked + unlinked), батчи по категориям, промпт с правилами фасовок/IKEA/запчастей
- `deterministic_pack_suggestions`: авто-кластеры 1/2/3 шт
- POST body: `items`, `groups`, `options` (кэш каталога с фронта)
- Возврат `ai_meta` (batches, pack_clusters)

### Frontend
- Вкладка **ИИ**, настройки в «Дополнительно», редактирование предложений, пакетное применение
- `app.js?v=50`, `styles.css?v=27`

---

## 2026-06-18 — Ручной workflow связок (Справка, без автоперезагрузки)

### Backend
- `build_catalog_payload`: `suggestions=none|review|all` (по умолчанию `none` — без тяжёлых эвристик)
- Убрана проверка бренда при attach/merge и в `validate_ozon_link_rows`

### Frontend
- Вкладки: **Справка** | Перепроверка | Каталог (по умолчанию)
- Перепроверка и каталог не перезагружаются сами — только «Обновить» / «Запустить перепроверку»
- Галочка на всю связку в каталоге; снят запрет на разные бренды
- `app.js?v=49`, `styles.css?v=26`

---

## 2026-06-18 — Фильтры бренда и категории (Связки)

### Backend
- Предложения `new_link` и `attach` учитывают бренд внутри категории

### Frontend
- Панель фильтров: бренд, категория, скрытие категорий, одиночки, мелкие связки
- Колонка «Бренд», валидация ручной связки по бренду
- `app.js?v=47`, `styles.css?v=25`

---

## 2026-06-18 — Режим «только по списку артикулов» (Связки)

### Backend
- `articles_only` в `/api/card-links/{wb|ozon}/.../catalog` и `ai-suggest`
- Фильтр `filter_rows_by_articles`, мета `scope` / `missing_articles`
- Ozon: без related_sku вне списка; WB: точное совпадение vendor_code

### Frontend
- Чекбокс, textarea, загрузка из файла, localStorage
- `app.js?v=46`, `styles.css?v=24`

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
