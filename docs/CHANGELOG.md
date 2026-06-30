# CHANGELOG

Формат: дата, commit (если есть), краткое описание.  
Полная история: `git log --oneline`.

---

## 2026-06-30 — Документы Ozon: этап 2 (ФСА + PDF + привязка)

- `fsa_registry.py` — поиск деклараций/сертификатов в pub.fsa.gov.ru
- `pdf_registry.py` — PDF из файла реестра или сформированный из данных ФСА
- `ozon_certificates.py` — offer_id → product_id, create/bind в Ozon Certification API
- `OzonClient`: certificate types/list/create/bind
- `POST /api/ozon/certificates/apply` — режимы `fsa_only`, `dry_run`, загрузка
- UI Ozon: «Проверить в ФСА», «Только проверить», «Загрузить»; колонки ФСА/PDF в таблице

## 2026-06-30 — Документы WB + Ozon: этап 1 (единая панель)

- Раздел «Документы» (`compliance`): вкладки Wildberries / Ozon, общая таблица
- `compliance_docs.py` — общий парсер, определение типа (декларация / сертификат)
- `POST /api/compliance/parse` — разбор таблицы для обеих площадок
- WB: отправка без изменений (`/api/wb/certificates/apply`)
- Ozon: заглушка «следующий этап» (ФСА + PDF + привязка)

## 2026-06-30 — Сертификаты WB: выбор товаров и несколько магазинов

- Таблица товаров с чекбоксами после «Разобрать таблицу»
- `vendor_codes` в apply — только отмеченные артикулы
- `POST /api/wb/certificates/parse`
- Прогресс: магазин N/M, итог по всем магазинам в отчёте

## 2026-06-30 — Сертификаты WB: вставка таблицы и автоотправка

- Панель «Сертификаты» (`wb-certs`): вставка TSV/CSV, несколько магазинов WB
- `wb_certificates.py` — парсинг, charcs по subject, `cards/update` батчами
- `WbContentClient.update_cards`, `get_subject_charcs`
- `POST /api/wb/certificates/apply` + фоновая задача `wb_certificates`

## 2026-06-30 — Docs: правило прогресса для долгих UI-операций

- AI_CONTEXT.md — канон: три компонента, fakeProgress, CSS, чеклист
- WORKFLOW.md §3a, ARCHITECTURE.md (Frontend) — ссылки на канон

## 2026-06-30 — UI: кольцевой прогресс автозапуска и Ozon-алертов (шаг 4.5)

- `#auto-run-progress` + `watchAutoRunProgress` — кольцо с этапом и магазином из `/auto-schedule/status`
- `#ozon-alerts-scan-progress` / `#ozon-alerts-panel-scan-progress` — скан и перескан чатов Ozon
- Хелперы `startRingProgressUI` / `endRingProgressUI`

## 2026-06-30 — UI: прогресс мастера связок (шаг 4.4)

- `#card-links-master-progress` + `showStepProgress` в `pollTask` / `runStep` (шаги 1–6)
- Блокировка кнопок шагов на время задачи; детали из `task.detail` в подписи

## 2026-06-30 — UI: прогресс акций Ozon (шаг 4.3)

- `#ozon-actions-loading` → `.progress-container`; `startLinearProgress` для load/sync/remove/auto-remove
- После sync/remove — обновление списка отдельным прогрессом (без затирания индикатора)

## 2026-06-30 — UI: прогресс чатов WB/Ozon (шаг 4.2)

- `startLinearProgress` / `endLinearProgress` для списка, переписки, mass-send

## 2026-06-30 — UI: прогресс отзывов и вопросов (шаг 4.1)

- `pollItemsTask` + `showProgress` для load/generate/send/template

## 2026-06-30 — UI: компоненты прогресса (шаг 2)

- `showProgress`, `showStepProgress`, `showRingProgress`, `fakeProgress` в `app.js`
- CSS progress v2 в `styles.css`; экспорт `window.MarketAIProgress`

## 2026-06-30 — UI: разделы 4–7 (связки, автозапуск, магазины, настройки)

- Степпер мастера связок `.steps-row` / `.step-item` с прогрессом по API
- Единый `.switch` для всех toggle (автозапуск, настройки, магазины)
- Карточки магазинов: `.store-card-meta`, `.store-card-actions`, `.btn-danger` заливка
- Вкладки настроек: `.segment-tab`, контраст неактивных

## 2026-06-30 — UI: вкладки «Связки карточек»

- Единый сегмент-контрол `.view-tabs` / `.view-tab` вместо `.segmented`

## 2026-06-30 — UI: пустое состояние «Отзывы» и «Вопросы»

- Иконка `ti-inbox`, компактный flex-layout `.empty-state`, обычная `btn-primary`

## 2026-06-30 — UI: панель фильтров «Отзывы» и «Вопросы»

- Единая `.panel-toolbar` вместо двух блоков; `.toolbar-field` + `.toolbar-actions`
- Убран `.toolbar-divider`; кнопка «Загрузить новые» только в тулбаре

## 2026-06-18 — Автозапуск: выбор магазинов по площадкам

- `wb_store_ids`, `yam_store_ids`, `ozon_store_ids`, `ozon_actions_store_ids` в `auto_schedule_json`
- UI в автозапуске и в настройках акций Ozon

## 2026-06-18 — WB: дедупликация новостей между ЛК

- Один `news_id` — одна обработка ИИ, одна запись, одно сообщение в Telegram
- Счётчик `wb_alert_duplicate` при сканировании второго и далее магазина

## 2026-06-18 — WB: ИИ и Telegram для новостей портала

- Отдельный промпт `wb_important_alert` (Настройки → Промпты)
- ИИ отбирает важные новости после фильтра заказов/отмен
- Telegram: шаблон, отдельный chat_id, повторная отправка при сбое
- Настройки в «Telegram»: чат, toggle, шаблон для WB

## 2026-06-18 — Уведомления Wildberries

- API `GET /api/communications/v2/news` через `WbCommonClient`
- Таблица `wb_portal_alerts`, эндпоинты `/api/wb/alerts`, scan, patch status
- Фильтр шума: новые заказы, скорая отмена заказов
- UI: фильтр площадки в «Уведомления», настройки и автозапуск WB

## 2026-06-18 — Сводка и настройки акций Ozon

- Сводка: KPI «Сегодня» сверху, убран свёрнутый блок; очередь/автозапуск всегда видны
- Акции Ozon: карточки настроек с описаниями, крупные toggles, action-карточки для запуска
- Улучшены переключатели глобально (контрастный track + border)

## 2026-06-18 — Раскрывающийся сайдбар

- Кнопка «развернуть/свернуть» внизу `.sidebar`; подписи у всех пунктов
- Ширина 52px → 196px; `margin-left` у `.main` синхронизирован
- Состояние в `localStorage` (`ui_sidebar_expanded`)

## 2026-06-18 — Анимированный экран входа

- `login.html`: particle network canvas, логотип M с кольцами и квадратиками
- Stagger-анимация полей, кнопка со shine + стрелкой, пульсирующие glow-орбы
- Цвета согласованы с UI v2 (`#7F77DD`)

## 2026-06-18 — UI v2: stat-cards, таблицы, кнопки, тёмная тема

- Шаги 6–11: KPI-карточки, таблицы, flat-кнопки, focus-ring форм
- Тёмная тема: `--bg #0d0d0d`, `--surface #141414`, `--border #222`
- Cleanup: скрыты watermarks/marketplace-bg, `.app-nav`, глобально убраны box-shadow (кроме focus)
- Webkit scrollbar; класс `.status-dot` (.ok/.warn/.err)
- `styles.css?v=32`

## 2026-06-18 — UI v2 (в процессе): сайдбар и panel-toprow

- Tabler Icons CDN, новые CSS-токены (#7F77DD)
- Горизонтальный `.app-nav` заменён на фиксированный `.sidebar` 52px
- `.main` с `margin-left: 52px`; у панелей `padding: 20px 24px`
- Общий header убран; на каждой вкладке — `.panel-toprow` (eyebrow + heading + actions)
- Пользователь и «Выйти» — в шапке раздела «Настройки»

---

## 2026-06-18 — Ozon акции: синхронизация по порогу скидки

- Новый режим `discount_threshold` (по умолчанию): все акции, порог % (default 3), снять / добавить / оставить
- Пропуск SKU с нулевыми или некорректными ценами — в отчёт (`skipped_data_count`, `skipped_samples`)
- API: `POST /api/ozon/actions/{store_id}/sync-discount`; расширены настройки per-store
- Автозапуск: та же галочка, режим из настроек магазина (`discount_threshold` vs `legacy_auto_remove`)
- Telegram-отчёт: −/+, оставлено, пропусков данных
- Legacy `auto-remove` сохранён для ручного запуска и старого режима

---

## 2026-06-20 — Мастер: понятные ошибки WB при загрузке каталога

- `Internal error` и другие ответы WB Content API переводятся в текст с подсказкой (токен «Контент», повтор)
- Пагинация каталога: ретраи страницы на 5xx; при сбое после N страниц — частичная загрузка + запись в журнал
- Таймаут шага Load — 10 мин; мастер API — `require_user` (как POST step)

---

- **Правила:** новые типы косметики — дезодорант, маска, глаза, салфетки, воск, лак, сыворотка, очищение, мусс, зубы (+ подсказки из subject WB)
- **ИИ:** батчи **20** SKU (было до 80 в одном запросе), группировка subject + сегмент + бренд; title сжимается до ~72 символов
- **Fix:** карточки после 80-й в subject больше не пропускались (`batch[:80]`)
- Отдельные system prompt для косметики и запчастей; компактный JSON `{subject, items[]}`

---

- Группировка: кат+бренд+subject_id; запчасти — по модели телефона из названия
- При ошибке «Разные предметы WB» — разбивка пачки и повтор (только автосвязка)
- Лог: модель запчасти, строки перепроверки ↻

---

## 2026-06-20 — Fix мастер plan: NameError `out`

- `master_step_plan`: `return rows, bundles_out, meta` (было `return out`)

---

## 2026-06-20 — Fix мастер: полный сброс кэша при повторной загрузке

- `clm_clear_store` теперь удаляет и `card_links_master_state` (шаги, журнал)
- При шаге Load шаги 2–5 не сохраняются от прошлого прогона
- Сброс кэша сразу при POST load; UI очищает лог и список связок

---

## 2026-06-20 — Мастер: загрузка до 15 000 карточек (общий селектор страниц)

- Шаг 1 «Загрузить WB» использует «Страниц каталога WB» сверху (было жёстко 10 000)
- Подсказка: фильтры мастера не влияют на загрузку

---

## 2026-06-20 — Автосвязка: категория+бренд, подробный лог, не останавливаться

- Группировка по категории и бренду; панель лога; ошибки пропускаются, при 429 пауза 60 с

---

## 2026-06-20 — Fix автосвязка: subjectID + кэш мастера Apply

- Автосвязка: группировка по subjectID (не смешивать предметы с subject_id=0)
- Мастер Apply: восстановление items из плана; понятная ошибка после деплоя без Disk

---

## 2026-06-19 — Каталог: выбрать все + автосвязка одиночных

- Кнопки: выбрать все / одиночных, автосвязка пачками до 30 по категории WB

## 2026-06-19 — Мастер: объединение связок + фильтр дробных категорий

- POST `/merge-bundles` — склеить 2+ выбранных связки плана (до 29 SKU, один subject WB)
- Фильтр «категории с ≥N связками», бейдж «N в кат.», подписи в выпадающем списке категорий

---

## 2026-06-19 — Fix master plan: category label from first item

- `master_step_plan`: `_row_category_label(chunk[0])` вместо передачи всего списка (AttributeError на шаге Plan)

---

## 2026-06-19 — Fix master load: apply_link_status extra arg

- `card_links_master.py`: убран несуществующий `marketplace=` в вызове `apply_link_status` (TypeError на шаге Load)

---

## 2026-06-19 — Мастер связок WB (новая вкладка)

### Backend
- `app/core/card_links_master.py` — 6 шагов: load → brands → segment → classify → plan → apply
- SQLite-кэш: `card_links_master_items/bundles/state`, CRUD `clm_*` в `db.py`
- План: лимит **29**, сегменты cosmetic/home/ikea/parts, укрупнение мелких пачек 3–9
- Apply: moving-only disconnect, пропуск «уже в imtID», store lock `card_links`, статусы applied/skipped/failed

### API / UI
- `/api/card-links/master/{store_id}/*` — status, bundles, bundle-ids, step/{name}
- Вкладка «Мастер связок», `card_links_master.js`, стили `.clm-*`
- «Выбрать все» через `/bundle-ids` (весь план с учётом фильтров)

---

## 2026-06-19 — Категория только из WB (+ hotfix NameError)

- Откат подмены категории по названию; исправлен оставшийся вызов `_items_display_category_label` в `consolidate_ai_bundle_previews`

---

## 2026-06-19 — Ложный «лимит 30» и категория из title (откат display)

### Frontend
- `cardLinksBundleApplyBlockReason`: «уже в связке» vs реальный лимит 30

### Backend (откат части 046bff8)
- Категория в UI снова только из WB subject/parent
- `_row_household_line_key` остаётся только для разбиения bin-pack (не для подписи)

---

## 2026-06-19 — Fix apply for bundles already in target imtID

### Backend
- `consolidate_ai_bundle_previews`: не показывать is_new связки без реального переноса
- `_merged_apply_candidate`: is_new с известным imtID → `merge_groups`
- `_chunk_needs_link_action`: linked в разных imtID при tgt=0

### Frontend
- `new_link` + target → `merge_groups`, фильтр nm_id перед merge
- `cardLinksBundleMovingCount`: не давать применить «пустые» связки
- Bulk-тост с текстом последней ошибки

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
