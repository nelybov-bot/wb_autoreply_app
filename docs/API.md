# REST API — WB AutoReply App

Справочник HTTP API веб-сервера (`app/web/server.py`).  
Базовый URL: `http://127.0.0.1:8000` (локально) или URL Render/VPS.

**Аутентификация:** cookie `wb_session` (после `POST /api/auth/login`). Большинство `/api/*` требуют авторизации.  
**Роли:** `admin` — полный доступ; `guest` — только разрешённые permissions (см. [ENV.md](./ENV.md)).

---

## Страницы (HTML / статика)

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/` | Редирект на `/app` или SPA |
| GET | `/app` | Основное SPA (`index.html`) |
| GET | `/login` | Страница входа |
| GET | `/reset` | Сброс пароля admin |
| GET | `/static/*` | CSS, JS, изображения |
| GET | `/static/app.js` | Отдельный route с no-cache заголовками |
| GET | `/sw.js` | Service Worker (PWA) |

---

## Health

| Метод | Путь | Auth | Описание |
|-------|------|------|----------|
| GET | `/health` | Нет | Healthcheck для Render |
| GET | `/api/health` | Нет | То же |

---

## Auth (`/api/auth/*`)

| Метод | Путь | Auth | Описание |
|-------|------|------|----------|
| GET | `/api/auth/me` | Да | Текущий пользователь, роль, permissions |
| POST | `/api/auth/login` | Нет | Вход (`username`, `password`) → cookie |
| POST | `/api/auth/logout` | Да | Выход |
| POST | `/api/auth/admin-reset` | Нет | Сброс пароля admin (`ADMIN_RESET_TOKEN` в env) |

---

## Users (`/api/users/*`) — только admin

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/api/users` | Список пользователей |
| POST | `/api/users` | Создать пользователя |
| DELETE | `/api/users/{user_id}` | Удалить |
| GET | `/api/users/{user_id}/permissions` | Permissions гостя |
| PATCH | `/api/users/{user_id}/permissions` | Обновить permissions |

---

## Stores (`/api/stores/*`)

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/api/stores` | Список магазинов |
| POST | `/api/stores` | Создать (`marketplace`: `wb` \| `ozon` \| `yam`) |
| GET | `/api/stores/{store_id}` | Один магазин |
| PATCH | `/api/stores/{store_id}` | Обновить (ключи write-only через маску) |
| DELETE | `/api/stores/{store_id}` | Удалить |

Поля магазина: `name`, `marketplace`, `api_key`, `client_id` (Ozon), `business_id` (YAM), `active`.

---

## Items — отзывы и вопросы (`/api/items/*`)

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/api/items` | Очередь (`item_type`, `store_id`, `status`, …) |
| GET | `/api/items/{item_id}` | Одна запись |
| PATCH | `/api/items/{item_id}/answer` | Редактировать сгенерированный ответ |
| POST | `/api/items/bulk` | Массовое обновление статуса/текста |

Статусы: `new`, `generated`, `sent`. Типы: `review`, `question`.

---

## Settings, prompts, config

| Метод | Путь | Permission | Описание |
|-------|------|------------|----------|
| GET | `/api/settings` | `view_settings` | Настройки (секреты замаскированы) |
| POST | `/api/settings` | `view_settings` | Сохранить настройки |
| GET | `/api/prompts` | Да | Промпты по `item_type` + `rating_group` |
| PATCH | `/api/prompts/{prompt_id}` | Да | Изменить текст промпта |
| GET | `/api/config/export` | `view_settings` | JSON backup (без ключей) |
| POST | `/api/config/import` | admin | Импорт backup |

Ключи настроек — см. [ENV.md](./ENV.md).

---

## Telegram (из UI настроек)

| Метод | Путь | Permission | Описание |
|-------|------|------------|----------|
| POST | `/api/telegram/test` | `view_settings` | Тестовое сообщение в чат |
| POST | `/api/telegram/report-now` | `view_settings` | Отправить периодический отчёт сейчас |
| POST | `/api/telegram/wb-banned-cards-now` | `view_settings` | Сводка заблокированных карточек WB во все магазины → Telegram (включая 0) |

---

## Auto-schedule (`/api/auto-schedule/*`)

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/api/auto-schedule` | Текущее расписание (JSON в БД) |
| POST | `/api/auto-schedule` | Сохранить расписание |
| POST | `/api/auto-schedule/disable` | Выключить автозапуск |
| GET | `/api/auto-schedule/status` | Статус: выполняется ли сейчас, этап |
| POST | `/api/auto-schedule/run-now` | Запустить цикл вручную |
| POST | `/api/auto-schedule/stop` | Остановить текущий цикл |

Поля расписания: `enabled`, `schedule_mode` (`slots` \| `interval`), `slots` (MSK `HH:MM`), `interval_hours`, `store_ids` (общий), `wb_store_ids`, `yam_store_ids`, `ozon_store_ids`, `ozon_actions_store_ids`, флаги `run_reviews_wb/yam/ozon`, `run_questions_*`, `run_wb_chats`, `run_ozon_chats`, `run_ozon_alerts`, `run_wb_alerts`, `run_ozon_actions_remove`.

---

## AI Agent (`/api/agent/*`)

| Метод | Путь | Описание |
|-------|------|----------|
| POST | `/api/agent/chat` | Сообщение агенту (`session_id`, `message`, опционально подтверждение write) |
| GET | `/api/agent/session/{session_id}` | История сессии |
| DELETE | `/api/agent/session/{session_id}` | Удалить сессию |

Инструменты агента (25 шт.) — см. [ARCHITECTURE.md](./ARCHITECTURE.md#ai-агент-appagent).

---

## Фоновые задачи (`/api/load-new`, `/api/generate`, `/api/send`, `/api/tasks/*`)

| Метод | Путь | Описание |
|-------|------|----------|
| POST | `/api/load-new` | Загрузить новые отзывы/вопросы → `task_id` |
| POST | `/api/generate` | Сгенерировать ответы OpenAI → `task_id` |
| POST | `/api/send` | Отправить ответы на площадки → `task_id` |
| POST | `/api/apply-template` | Применить шаблон без OpenAI |
| GET | `/api/tasks/{task_id}` | Статус задачи (`progress`, `status`) |
| GET | `/api/tasks` | Список задач. Query: `status`, `action`, `limit` |
| POST | `/api/tasks/{task_id}/cancel` | Отмена |
| GET | `/api/store-locks` | Текущие блокировки магазинов |
| POST | `/api/store-locks/clear` | Снять блокировки (+ cancel owners). Body: `{ store_ids?: number[] }` |

Статусы задачи: `running`, `done`, `error`, `cancelled`. Хранение in-memory, TTL 1 ч.

---

## WB Buyer Chats (`/api/wb/buyer-chats/*`)

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/api/wb/buyer-chats/{store_id}` | Список чатов |
| GET | `/api/wb/buyer-chats/{store_id}/{chat_id}/thread` | Переписка |
| POST | `/api/wb/buyer-chats/{store_id}/generate-draft` | Черновик ИИ |
| POST | `/api/wb/buyer-chats/{store_id}/send` | Отправить сообщение |
| POST | `/api/wb/buyer-chats/{store_id}/mass-generate-send` | Массовая обработка |

---

## Ozon Buyer Chats (`/api/ozon/buyer-chats/*`)

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/api/ozon/buyer-chats/{store_id}` | Список (`filter`: buyers / support / all) |
| GET | `/api/ozon/buyer-chats/{store_id}/{chat_id}/thread` | Переписка |
| POST | `/api/ozon/buyer-chats/{store_id}/generate-draft` | Черновик ИИ |
| POST | `/api/ozon/buyer-chats/{store_id}/send` | Отправить |
| POST | `/api/ozon/buyer-chats/{store_id}/mass-generate-send` | Массовая обработка |

---

## Ozon Actions (`/api/ozon/actions/*`)

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/api/ozon/actions/settings/{store_id}` | Настройки синхронизации акций для магазина |
| POST | `/api/ozon/actions/settings/{store_id}` | Сохранить (режим `discount_threshold` / `legacy_auto_remove`, порог %, watched/exclude IDs) |
| GET | `/api/ozon/actions/{store_id}` | Список акций |
| GET | `/api/ozon/actions/{store_id}/{action_id}/products` | Товары в акции |
| POST | `/api/ozon/actions/{store_id}/remove` | Удалить товары из акций |
| POST | `/api/ozon/actions/{store_id}/sync-discount` | Синхронизация по порогу скидки (≤ порога — оставить/добавить, > — снять) |
| POST | `/api/ozon/actions/{store_id}/auto-remove` | Legacy-автоудаление по правилам |

Глобальные настройки акций хранятся в `app_settings.ozon_actions_settings_json`.

---

## Card Links (`/api/card-links/*`)

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/api/card-links/wb/{store_id}/catalog` | Каталог WB + предложения; после загрузки пишет кэш `clc_*` |
| GET | `/api/card-links/wb/{store_id}/catalog-cache` | Метаданные дискового кэша каталога |
| GET | `/api/card-links/ozon/{store_id}/catalog` | Каталог Ozon + предложения |
| POST | `/api/card-links/wb/{store_id}/check-singles` | Проверка одиночек WB → `{ task_id }` |
| POST | `/api/card-links/wb/{store_id}/merge` | Объединить карточки (imtID) |
| POST | `/api/card-links/wb/{store_id}/disconnect` | Разъединить |
| POST | `/api/card-links/ozon/{store_id}/link` | Связать по «Названию модели» |
| POST | `/api/card-links/ozon/{store_id}/unlink` | Разъединить |
| POST | `/api/card-links/ozon/{store_id}/link-qty-table` | Связка по таблице количеств (TMS) |
| POST | `/api/card-links/wb/{store_id}/ai-suggest` | ИИ-подсказки WB |
| POST | `/api/card-links/ozon/{store_id}/ai-suggest` | ИИ-подсказки Ozon |
| GET | `/api/card-links/master/{store_id}/status` | Статус мастера связок (шаги, покрытие, фильтры) |
| GET | `/api/card-links/master/{store_id}/bundles` | План связок (пагинация, фильтры) |
| GET | `/api/card-links/master/{store_id}/bundle-ids` | ID всех связок плана (для «выбрать все») |
| POST | `/api/card-links/master/{store_id}/merge-bundles` | Объединить 2+ связок плана в одну |
| POST | `/api/card-links/master/{store_id}/step/{name}` | Шаг: `load` / `brands` / `segment` / `classify` / `plan` / `apply` → `{ task_id }` |
| GET | `/api/card-links/ai-prompt/{marketplace}` | Системный промпт ИИ (`wb` / `ozon`) |
| PUT | `/api/card-links/ai-prompt/{marketplace}` | Сохранить промпт ИИ |

Query-параметры каталога:
- WB: `articles`, `q` (поиск), `max_pages` (default **150** = 15k, max 150), `articles_only` (bool), `from_cache` (bool — без запроса к WB)
- Ozon: `articles`, `max_pages` (default 30, max 100), `articles_only` (bool)

UI больше не даёт выбирать глубину — фронт всегда шлёт WB `max_pages=150`.

`POST …/check-singles` body: `{ force_refresh?: bool, max_pages?: int }` — по умолчанию из кэша; результат задачи: `candidates` (new_link), `attach_suggestions` (attach/attach_batch), `meta`, без relocate.

`articles_only=1` — загрузить только карточки из `articles`; предложения связок строятся только внутри этого списка (без перепроверки внешних групп Ozon/WB).

Ответ каталога: `items`, `groups`, `candidates`, `attach_suggestions`, `combine_suggestions`, `review_suggestions`, `catalog_meta`, `cache_meta`, счётчики (`count`, `unlinked_count`, `linked_groups`, `max_link_items`).

---

## Габариты и характеристики WB

| Метод | Путь | Описание |
|-------|------|----------|
| POST | `/api/packaging-dims/parse` | Разбор таблицы габаритов |
| POST | `/api/packaging-dims/compare` | Сравнение с WB → `{ task_id }` |
| POST | `/api/packaging-dims/apply` | Замена габаритов на WB → `{ task_id }` |
| POST | `/api/wb/bulk-chars/parse` | Разбор списка артикулов |
| POST | `/api/wb/bulk-chars/apply` | Массовая подстановка характеристики. Body: `{ store_ids, char_name, char_value, text?, vendor_codes?, all_catalog?, dry_run?, only_if_different?, refresh_catalog? }` → `{ task_id }` |
| POST | `/api/ozon/bulk-chars/apply` | Массовая замена ТН ВЭД / бренда на Ozon. Body: `{ store_ids, field: "tnved"|"brand", value, text?, offer_ids?, all_catalog?, dry_run?, only_if_different? }` → `{ task_id }` |

Прогресс/отмена: `GET/POST /api/tasks/{id}` (+ `/cancel`). После F5 UI восстанавливает задачу из `localStorage`.

## Документы (сертификаты / декларации)

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/api/compliance/fsa-status` | Диагностика ФСА: `proxy_configured`, `reachable`, `message` |
| POST | `/api/compliance/parse` | Общий разбор таблицы. Body: `{ text }` → `{ rows, warnings, count }`; в `rows` также `doc_type`, `doc_type_label` |

## Сертификаты WB

| Метод | Путь | Описание |
|-------|------|----------|
| POST | `/api/wb/certificates/parse` | То же, что `/api/compliance/parse` (совместимость) |
| POST | `/api/wb/certificates/apply` | Сопоставление + `cards/update`. Body: `{ store_ids, text, vendor_codes?: string[], dry_run?: bool }` → `{ task_id }` |
| POST | `/api/wb/certificates/drafts-scan` | Черновики WB: ошибки + пустые обязательные поля. Body: `{ store_ids, vendor_codes?: string[] }` → `{ task_id }` |
| POST | `/api/wb/certificates/drafts-fill` | ИИ-дозаполнение характеристик. Body: `{ store_ids, vendor_codes?, dry_run?, fills? }` → `{ task_id }` |

`drafts-fill`: `dry_run: true` — только подбор ИИ; `dry_run: false` + `fills` — отправка ранее подобранных значений на WB.

`vendor_codes` — если не пустой, обрабатываются только эти артикулы из таблицы. Несколько `store_ids` — последовательная проверка и отправка в каждый магазин.

Колонки в `text` (TSV/CSV, с заголовком или без): артикул продавца, номер сертификата/декларации, дата регистрации, действует до.

## Документы Ozon

| Метод | Путь | Описание |
|-------|------|----------|
| POST | `/api/ozon/certificates/apply` | PDF → Ozon create/bind. Body: `{ store_ids?, text, vendor_codes?, dry_run?, fsa_only?, pdf_source? }` → `{ task_id }` |

`pdf_source`: `fsa` (pub.fsa.gov.ru) | `mirror` (декларации-соответствия.рус).  
`fsa_only: true` — только проверка PDF (магазины не нужны). `dry_run: true` — без загрузки на Ozon.

---

## Алерты и ошибки карточек

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/api/card-errors` | Ошибки карточек из отзывов/чатов |
| PATCH | `/api/card-errors/{alert_id}` | Статус `new` / `resolved` |
| GET | `/api/ozon/alerts` | Важные сообщения Ozon |
| PATCH | `/api/ozon/alerts/{alert_id}` | Обновить статус |
| POST | `/api/ozon/alerts/{store_id}/scan` | Сканирование поддержки Ozon |
| GET | `/api/wb/alerts` | Новости портала WB (без заказов/отмен) |
| PATCH | `/api/wb/alerts/{alert_id}` | Обновить статус |
| POST | `/api/wb/alerts/{store_id}/scan` | Загрузка и ИИ-анализ; body: `{ rescan?, from_date? }` |

---

## Статистика и логи

| Метод | Путь | Permission | Описание |
|-------|------|------------|----------|
| GET | `/api/stats` | Да | Операционная сводка |
| GET | `/api/quality-metrics` | Да | Показатели качества Ozon (кэш 30 мин) |
| GET | `/api/log` | `view_log` | Хвост `logs/app.log` (legacy) |
| GET | `/api/log/dev` | `view_log` | Dev-журнал |
| GET | `/api/log/ops` | `view_ops_log` | Журнал операций (audit) |

---

## Коды ошибок

- `401` — не авторизован
- `403` — нет permission / не admin
- `404` — не найдено
- `409` — магазин занят (`StoreBusyError`)
- `429` / `502` — ошибки WB/Ozon API (часто через `HttpStatusError`)

Card-links: `_card_links_http_error()` преобразует исключения в понятные сообщения для UI.

---

## Связанные документы

- [ARCHITECTURE.md](./ARCHITECTURE.md) — потоки данных, модули
- [ENV.md](./ENV.md) — переменные окружения и ключи `app_settings`
- [AI_CONTEXT.md](./AI_CONTEXT.md) — card-links domain, грабли UI
