# RetailCRM Telegram Analytics

Проект для синхронизации заказов из RetailCRM, расчета аналитики в Supabase и отображения дашборда с ручным запуском обновления и Telegram-уведомлениями по крупным заказам.

## Что было запрошено (история задач)

Ниже кратко зафиксированы основные запросы в процессе разработки:

1. Реализовать ETL слой по плану: `orders.raw -> dim/fact -> mart_hypotheses_signals`.
2. Разобраться с ошибками запуска (`Cannot find module 'dotenv'`, отсутствие таблиц в Supabase, FK-конфликты при перезаливке).
3. Починить запуск дашборда на `localhost`.
4. Добавить кнопку на сайт для запуска синка и пересчета аналитики.
5. Сменить валюту в UI с рублей на тенге.
6. Добавить Telegram-уведомление при новом заказе от `50 000 ₸`.
7. Подготовить репозиторий к публикации (hardening): `.gitignore`, `.env.example`, проверка на секреты.

## Где застряли и как решали

### 1) `npm run etl:marts` падал с `Cannot find module 'dotenv'`
- Причина: удален `node_modules`.
- Решение: `npm install`, затем повторный запуск ETL.

### 2) ETL падал: `Could not find the table 'public.dim_products'`
- Причина: не создана схема `dim/fact/mart` в Supabase.
- Решение: добавлен `etl-marts-schema.sql`, выполнен в Supabase SQL Editor.

### 3) ETL падал по внешнему ключу (`fact_order_items_product_key_fkey`)
- Причина: очищались `dim` таблицы до очистки `fact`.
- Решение: в `etl-marts.js` изменен порядок full refresh:
  `mart -> fact_order_items -> fact_orders -> dim_*`, потом `upsert`.

### 4) Сайт не открывался (`ERR_CONNECTION_REFUSED`)
- Причина: сервер не запущен или старый процесс занял порт.
- Решение: запуск `npm run dashboard`, перезапуск процесса на `3000`.

### 5) API отдавал `{"error":"Не удалось открыть HTML файл"}`
- Причина: сервер смотрел в `dashboard.html`, а файл назывался `orders-dashboard.html`.
- Решение: исправлен путь в `dashboard-server.js`.

### 6) Sync падал с ошибкой RetailCRM фильтра:
`updatedAtFrom: Filter does not exist`
- Причина: этот фильтр не поддерживался endpoint `/api/v5/orders` в данной инсталляции.
- Решение: убран фильтр, синк выполняется полным проходом с безопасным `upsert` по `retailcrm_id`.

### 7) Push в GitHub отклонялся (`fetch first`)
- Причина: удаленный `main` уже содержал commit.
- Решение: `git pull --allow-unrelated-histories`, merge, затем `git push`.

## Как работает проект

### Поток данных

1. `sync-orders.js`
   - читает заказы из RetailCRM;
   - маппит в структуру таблицы `orders` (raw);
   - делает `upsert` по `retailcrm_id` (без дублей);
   - для новых заказов с суммой >= порога отправляет Telegram-уведомление.

2. `etl-marts.js`
   - читает `orders` из Supabase;
   - строит:
     - `dim_products`
     - `dim_customers`
     - `dim_channels`
     - `fact_orders`
     - `fact_order_items`
     - `mart_hypotheses_signals`
   - выполняет full refresh в корректном порядке с учетом FK.

3. `dashboard-server.js`
   - отдает UI (`orders-dashboard.html`);
   - API аналитики: `GET /api/orders-analytics?days=...`;
   - API оркестрации: `POST /api/sync-and-refresh` (запускает sync + etl последовательно).

4. `orders-dashboard.html`
   - показывает KPI, графики и списки;
   - поддерживает выбор периода;
   - имеет кнопку "Синхронизировать и обновить";
   - отображает суммы в `KZT`.

## Функции и скрипты

`package.json`:

- `npm run sync` - синхронизация заказов из RetailCRM в `orders`.
- `npm run etl:marts` - пересчет `dim/fact/mart`.
- `npm run dashboard` - запуск веб-дашборда (`http://localhost:3000`).

### Telegram-уведомления

Логика в `sync-orders.js`:
- уведомление отправляется только для **новых** заказов;
- порог суммы задается через `TELEGRAM_NOTIFY_SUM_THRESHOLD` (по умолчанию `50000`).

## Переменные окружения

Используйте `.env.example` как шаблон и создайте локальный `.env`:

- `RETAILCRM_URL`
- `RETAILCRM_API_KEY`
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `TELEGRAM_NOTIFY_SUM_THRESHOLD`
- `PORT` (опционально)

## Быстрый старт

1. Установить зависимости:
   - `npm install`
2. Создать таблицы витрин:
   - выполнить `etl-marts-schema.sql` в Supabase.
3. Настроить `.env`.
4. Выполнить первичный цикл:
   - `npm run sync`
   - `npm run etl:marts`
5. Запустить UI:
   - `npm run dashboard`
   - открыть `http://localhost:3000`

## Ограничения и заметки

- Проект рассчитан на серверный ключ Supabase (`service role`) и запуск в доверенной среде.
- При большом объеме данных полный refresh ETL может занимать заметное время.
- Если нужно ускорение - следующий шаг: инкрементальный ETL и материализованные представления.