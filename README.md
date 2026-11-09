# Payment service

Небольшой сервис асинхронной обработки платежей на FastAPI, PostgreSQL и RabbitMQ. Платёжный шлюз эмулируется: обработка занимает 2–5 секунд, результат — 90% `succeeded` и 10% `failed`.

## Запуск

Нужны Docker и Docker Compose.

```bash
cp .env.example .env
chmod 600 .env
# Заполните POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD,
# RABBITMQ_USER, RABBITMQ_PASSWORD и API_KEY в .env
docker compose up --build
```

Все параметры подключения берутся из `.env`: адреса, порты, имена баз и пользователей, пароли и API-ключ. В репозитории есть только `.env.example` без секретов; `.env` исключён из Git и не попадает в Docker-образ. Приложение не стартует с пустыми обязательными значениями. Для доступа к RabbitMQ Management (`http://localhost:15672`) используйте `RABBITMQ_USER` и `RABBITMQ_PASSWORD` из своего `.env`. API доступно на `http://localhost:8000`. Миграция Alembic применяется при запуске API; consumer стартует после него.

Для примеров ниже задайте `API_KEY` в текущей оболочке тем же значением, что и в `.env`:

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: ${API_KEY}" \
  -H 'Idempotency-Key: order-123' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"1250.00","currency":"RUB","description":"Order 123","metadata":{"order_id":123},"webhook_url":"https://example.com/webhook"}'

curl -H "X-API-Key: ${API_KEY}" \
  http://localhost:8000/api/v1/payments/<payment_id>
```

`POST` отвечает `202` с `payment_id`, `status`, `created_at`. Повтор с тем же `Idempotency-Key` и тем же телом возвращает тот же платёж; с другим телом — `409`. Отсутствующий или неверный `X-API-Key` даёт `401`. `GET` возвращает `404` для неизвестного UUID.

## Доставка и сбои

Создание платежа и запись `outbox` происходят в одной транзакции. Фоновый publisher в API выбирает неопубликованные строки через `FOR UPDATE SKIP LOCKED`, публикует persistent-сообщение в durable-очередь `payments.new` с подтверждением брокера и только после этого отмечает строку опубликованной. При сбое между подтверждением и коммитом публикация повторится: доставка **at least once**.

Один обработчик consumer сохраняет итоговый статус только если платёж ещё `pending`. Повторное сообщение не запускает эмуляцию снова. После фиксации статуса он отправляет webhook с телом `payment_id`, `status`, `amount`, `currency`, `processed_at` и заголовком `X-Event-ID` (стабильный ID платежа). Успешная доставка фиксируется в БД.

Сетевые ошибки webhook, HTTP 408/429/5xx и временная недоступность БД получают до трёх попыток с паузами 1 и 2 секунды. Остальные HTTP 4xx, отсутствующий платёж и ошибки схемы БД сразу отклоняются. После исчерпания попыток RabbitMQ переносит сообщение в `payments.dead` через `payments.dlx`. Статус платежа при недоставленном webhook остаётся итоговым, `webhook_delivered_at` — `null`. Publisher outbox повторяет публикацию при ошибках соединения с RabbitMQ или БД; неожиданные ошибки не скрываются. При недоступности БД API отвечает `503` без текста внутренней ошибки. Очередь DLQ доступна для разбора в RabbitMQ Management; автоматического повтора из DLQ нет.

Webhook может прийти более одного раза, если процесс остановится после ответа получателя, но до записи `webhook_delivered_at`. Получателю следует дедуплицировать по `X-Event-ID`. В настоящей платёжной интеграции такой же ключ необходимо передавать внешнему шлюзу; эмулятор денежных операций не выполняет.

## Тесты

Тесты разделены на `tests/unittest` (валидация, API, consumer, outbox и топология RabbitMQ) и `tests/integrational_tests` (реальные PostgreSQL, RabbitMQ, HTTP API и webhook). Интеграционные тесты проверяют в том числе конкурентное создание с одним ключом, успешное уведомление и перенос в DLQ после трёх ошибок.

На текущей версии прошло **60 тестов**: 55 модульных и 5 интеграционных. Покрытие `app` с учётом ветвлений — **99,06%** (`pytest-cov` округляет до 99%). Метрика снимается с процесса, который запускает тесты; обработка внутри API и consumer контейнеров в неё не включена. Непокрыты точка запуска `consumer.py` через `python -m` (она проверяется запуском контейнера) и формальная ветка выхода из цикла retry, где каждая итерация завершается `return` или исключением.

Модульные тесты не требуют запущенных сервисов:

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[test]'
.venv/bin/pytest -q tests/unittest --cov=app --cov-branch --cov-report=term-missing --cov-fail-under=98
```

Для полного прогона сначала поднимите стек. Интеграционные тесты читают параметры доступа из `.env`:

```bash
cp .env.example .env
# Заполните обязательные значения в .env
docker compose up -d --build
.venv/bin/pytest -q tests --cov=app --cov-branch --cov-report=term-missing --cov-fail-under=98
docker compose down
```

Для ручной проверки DLQ укажите недоступный `webhook_url`, дождитесь трёх попыток и откройте очередь `payments.dead` в RabbitMQ Management. Для повторной доставки опубликуйте в `payments.new` JSON `{"payment_id":"<payment_id>"}`: статус обработанного платежа не изменится.
