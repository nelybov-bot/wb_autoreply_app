# Развёртывание на бесплатном сервере (Render.com)

Приложение можно развернуть на **Render.com** (бесплатный тариф). Сервер будет доступен по ссылке 24/7 (после 15 мин без активности «засыпает», первый запрос разбудит за ~30 сек).

## Важно: база и ключи НЕ в Git

Файлы **`data/reviews.db`** и **`logs/`** не должны попадать в репозиторий — в них API-ключи магазинов.

Если `reviews.db` закоммичен, каждый деплой на Render **перезаписывает** живую базу старой из Git (устаревшие ключи и цифры в сводке).

После обновления кода один раз на Render: заново введите магазины и настройки в UI **или** подключите диск ниже.

## Постоянное хранилище на Render (Starter)

Диск описан в `render.yaml` (mount `data/`, 1 GB). Можно также добавить вручную:

1. **Dashboard** → сервис → **Disks** → Add Disk  
2. **Mount Path:** `/opt/render/project/src/data`  
3. **Size:** 1 GB (на Render нет 2 GB — только 1, 5, 10…)  
4. **Save** → **Manual Deploy**

База `data/reviews.db` сохранится между деплоями.

### Environment (заполнить в Dashboard)

| Переменная | Значение |
|------------|----------|
| `SESSION_SECRET` | длинная случайная строка (или **Generate** в Render) |
| `ADMIN_INIT_PASSWORD` | пароль первого `admin` (только если users пуста) |
| `ADMIN_RESET_TOKEN` | токен для экстренного сброса (опционально) |
| `COOKIE_SECURE` | `1` (HTTPS) |
| `FSA_PROXY_URL` | прокси для ФСА, если нужны документы Ozon |

После первого деплоя с диском: **Магазины** и **Настройки** в UI (или импорт `config/export`).

**Ограничения диска:** один инстанс, zero-downtime deploy отключён.

## Render + реестр ФСА (Ozon «Документы»)

**OpenAI** с Render работает; **pub.fsa.gov.ru** — только из РФ. Переносить всё на российский VPS не нужно.

Схема: **Render (приложение + OpenAI)** + **дешёвый VPS в РФ как HTTP-прокси только для ФСА**.

1. Арендуйте минимальный VPS в России (Timeweb и т.д.).
2. На VPS: `sudo bash deploy/fsa-proxy/setup_ru_proxy.sh`
3. В Render → **Environment** → `FSA_PROXY_URL` = строка из вывода скрипта.
4. Перезапуск сервиса → в UI **Документы → Ozon** — красная подсказка должна исчезнуть.

### Если в логах `Connection timeout to host https://pub.fsa.gov.ru/login`

1. **`FSA_PROXY_URL` не задан** в Render → Environment (самая частая причина).
2. Сделайте **Manual Deploy** последнего коммита.
3. Проверьте `PYTHON_VERSION` = `3.10.15` (в логах не должно быть Python 3.14).

Диагностика: `GET /api/compliance/fsa-status` → `reachable: true` если всё ок.

Подробно: [deploy/fsa-proxy/README.md](deploy/fsa-proxy/README.md)

## Ограничение бесплатного тарифа

- Без диска данные могут пропадать при перезапуске/деплое. Для постоянной базы — диск (выше) или PostgreSQL (позже).

## Шаги

### 1. Репозиторий на GitHub

Если проекта ещё нет в GitHub:

```bash
cd "/Users/ast/Desktop/Действующие коды/wb_autoreply_app"
git init
git add .
git commit -m "WB Автоответчик"
# Создай репозиторий на github.com, затем:
git remote add origin https://github.com/ТВОЙ_ЛОГИН/wb_autoreply_app.git
git branch -M main
git push -u origin main
```

### 2. Render.com

1. Зайди на [render.com](https://render.com), зарегистрируйся (можно через GitHub).
2. **Dashboard** → **New** → **Web Service**.
3. Подключи репозиторий **wb_autoreply_app** (или выбери свой форк).
4. Параметры:
   - **Build Command:** `pip install -r requirements-web.txt`
   - **Start Command:** `uvicorn app.web.server:app --host 0.0.0.0 --port $PORT`
5. **Create Web Service**.

Через несколько минут сервис поднимется. Ссылка будет вида: `https://wb-autoreply-xxxx.onrender.com`.

### 3. Открытие с телефона

В настройках приложения поле «Адрес API (ПК)» оставь **пустым** — открывай в браузере прямо ссылку Render (например с телефона), всё будет ходить на этот же адрес.

---

## Постоянная база (опционально)

Чтобы магазины и настройки не пропадали при перезапуске, можно позже подключить **бесплатную PostgreSQL** на Render и перевести приложение на неё (потребуется доработка кода). На бесплатном тарифе без БД данные считаются временными.
