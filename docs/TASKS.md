# TASKS — бэклог разработки

Приоритеты ориентировочные. Статусы: `todo` | `in_progress` | `done` | `cancelled`.

---

## P0 — Стабильность и данные

| ID | Задача | Статус | Примечание |
|----|--------|--------|------------|
| T-000 | Внедрить обязательный workflow в docs | done | WORKFLOW.md, PROJECT.md, AI_CONTEXT.md |
| T-000b | Закоммитить папку `docs/` в git | todo | Ожидает запроса пользователя |
| T-000c | Полный аудит документации vs код | done | API.md, ENV.md, обновление всех docs |
| T-001 | Верификация card-links на production после `73aefbc` | todo | Hard refresh, проверить чекбоксы, пулы, progress bar, каталог |
| T-002 | Persistent storage на Render (Disk 1GB) | todo | Описано в DEPLOY.md; без диска БД сбрасывается |
| T-003 | Диагностика «пропущенных» одиночек в предложениях | todo | Нужны примеры nmID/артикулов от пользователя |
| T-004 | Обновить корневой README.md (web-first, не только desktop) | todo | Сейчас вводит в заблуждение |

---

## P1 — Card Links

| ID | Задача | Статус | Примечание |
|----|--------|--------|------------|
| T-010 | Унифицировать сортировку каталога (backend vs frontend) | todo | `sort_catalog_rows` + `cardLinksSortCatalogRows` — дублирование |
| T-011 | Прогресс загрузки каталога с реальным % | todo | Сейчас indeterminate; WB может грузиться минуты |
| T-012 | Фильтр каталога «только связки» / «только одиночки» | todo | Есть только «только без связки» |
| T-013 | Экспорт предложений связок (CSV) | todo | Запрос пользователей возможен |
| T-014 | Unit-тесты для `group_attach_suggestions`, `suggest_attach_to_groups` | todo | Регрессия уже была (сломанный return) |

---

## P1 — Качество кода

| ID | Задача | Статус | Примечание |
|----|--------|--------|------------|
| T-020 | Разбить `server.py` на роутеры (auth, stores, card-links, …) | todo | ~3980 строк |
| T-021 | Разбить `app.js` на ES-модули или сборку | todo | ~5900 строк |
| T-022 | Добавить `pytest` + CI (GitHub Actions) | todo | Тестов нет |
| T-023 | Типизация: mypy на `core/` | todo | Частичные type hints есть |

---

## P2 — Инфраструктура

| ID | Задача | Статус | Примечание |
|----|--------|--------|------------|
| T-030 | Миграция SQLite → PostgreSQL | todo | Упомянуто в DEPLOY.md |
| T-031 | Redis для tasks/sessions (multi-instance) | todo | Сейчас in-memory |
| T-032 | `.env.example` с документированными переменными | todo | Ключи описаны в docs/ENV.md; файл .env.example ещё не создан |
| T-033 | Healthcheck + метрики для Render | todo | Есть `/health`, нет metrics |

---

## P2 — Функциональность

| ID | Задача | Статус | Примечание |
|----|--------|--------|------------|
| T-040 | Полная поддержка Яндекс.Маркет | todo | `yam_client.py` — ограниченно |
| T-041 | История операций card-links (audit) | todo | Есть общий audit_events |
| T-042 | Откат связки (undo merge) | todo | Есть disconnect/unlink по отдельности |
| T-043 | PWA offline mode улучшения | todo | `sw.js` есть, покрытие неизвестно |
| T-044 | Desktop: паритет с web UI | todo | Desktop отстаёт по функциям |

---

## P3 — UX

| ID | Задача | Статус | Примечание |
|----|--------|--------|------------|
| T-050 | Тёмная тема: консистентность card-links панелей | todo | |
| T-051 | Мобильная вёрстка card-links | todo | |
| T-052 | Toast/ошибки WB 429 — единый компонент | todo | Разбросано по app.js |

---

## Done (недавно)

| ID | Задача | Commit / дата |
|----|--------|--------|
| D-000 | Docs + workflow process | 2026-06-06, docs/ |
| D-006 | Полный аудит документации | 2026-06-06, API.md + ENV.md |
| D-001 | Secret masking API keys | `5e443b9` |
| D-002 | Login mascot | `f804d5f` |
| D-003 | Card-links pools + UI | `0628528` … `73aefbc` |
| D-004 | Ozon TMS qty-table | `67fcdbe` … |
| D-005 | Fix group_attach_suggestions corruption | `73aefbc` |

---

## Как добавлять задачи

```markdown
| T-XXX | Краткое описание | todo | Контекст |
```

Обновлять статус при работе. Крупные завершённые — переносить в Done с commit hash.
