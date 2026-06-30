# Прокси ФСА для Render

## Зачем

| Где | Что |
|-----|-----|
| **Render** (США/ЕС) | OpenAI, Ozon API, WB API — работают |
| **pub.fsa.gov.ru** | Доступен только из сети РФ |
| **Решение** | Дешёвый VPS в РФ = HTTP-прокси **только** для запросов ФСА |

Основное приложение остаётся на Render. Прокси не трогает OpenAI и остальные API.

## Шаг 1 — VPS в России

Подойдёт минимальный тариф (Timeweb, Selectel, Beget VPS и т.д.), Ubuntu 22.04/24.04.

1. Создайте VPS с публичным IPv4.
2. Склонируйте репозиторий или скопируйте скрипт на сервер.
3. Запустите:

```bash
sudo bash deploy/fsa-proxy/setup_ru_proxy.sh
```

Скрипт установит **squid** с логином/паролем и выведет готовую строку `FSA_PROXY_URL`.

Опционально задать свои значения до запуска:

```bash
export FSA_PROXY_USER=myuser
export FSA_PROXY_PASS='длинный_пароль'
export FSA_PROXY_PORT=3128
sudo -E bash deploy/fsa-proxy/setup_ru_proxy.sh
```

## Шаг 2 — Render

1. [Dashboard](https://dashboard.render.com) → ваш сервис **wb-autoreply**
2. **Environment** → Add Environment Variable
3. Имя: `FSA_PROXY_URL`
4. Значение: строка из вывода скрипта, например  
   `http://fsa_render:ПАРОЛЬ@123.45.67.89:3128`
5. **Save Changes** → дождаться перезапуска (или Manual Deploy)

## Шаг 3 — Проверка

В UI: **Документы** → вкладка **Ozon** → **Проверить в ФСА**.

Должны появиться статусы «Найден» / «не найден», а не «ошибка ФСА» с таймаутом.

## Безопасность

- Прокси с паролем; не открывайте squid без `auth`.
- На VPS можно ограничить порт файрволом (ufw), если знаете диапазоны IP Render — но у Free/Starter egress IP **не фиксирован**, поэтому пароль обязателен.
- Прокси используется **только** приложением для `pub.fsa.gov.ru`.

## Стоимость

Ориентир: VPS в РФ от ~150–400 ₽/мес + Render как сейчас. OpenAI по-прежнему с Render.

## Альтернатива без VPS

Локально `python3 run_web.py` на Mac в РФ — ФСА без прокси, но OpenAI с РФ может не работать без VPN. Для production на Render прокси — нормальная схема.
