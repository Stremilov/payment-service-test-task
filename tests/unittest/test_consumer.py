import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
import httpx
from faststream.exceptions import RejectMessage
from sqlalchemy.exc import NoResultFound, OperationalError, ProgrammingError

from app import consumer
from app.models import Payment


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def payment(status="pending", delivered=False):
    return Payment(
        id=uuid4(), amount=Decimal("10.00"), currency="RUB", description="Order",
        metadata_json={}, status=status, idempotency_key="key", request_hash="hash",
        webhook_url="https://example.com/hook", created_at=NOW,
        processed_at=NOW if status != "pending" else None,
        webhook_delivered_at=NOW if delivered else None,
    )


class Sessions:
    def __init__(self, item):
        self.item = item
        self.session = Mock()
        self.session.get = AsyncMock(return_value=item)
        self.session.execute = AsyncMock(side_effect=self.execute)
        self.begin_count = 0

    async def execute(self, statement):
        self.item.status = statement.compile().params["status"]
        self.item.processed_at = NOW

    @asynccontextmanager
    async def begin(self):
        self.begin_count += 1
        yield self.session

    @asynccontextmanager
    async def __call__(self):
        yield self.session


class Client:
    def __init__(self, response):
        self.post = AsyncMock(return_value=response)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass


@pytest.mark.parametrize("random_value,status", [(0.1, "succeeded"), (0.95, "failed")])
async def test_new_payment_is_processed_and_notified(monkeypatch, random_value, status):
    item = payment()
    sessions = Sessions(item)
    response = Mock()
    client = Client(response)
    sleep = AsyncMock()
    monkeypatch.setattr(consumer, "session_factory", sessions)
    monkeypatch.setattr(consumer.asyncio, "sleep", sleep)
    monkeypatch.setattr(consumer.random, "uniform", lambda *_: 3)
    monkeypatch.setattr(consumer.random, "random", lambda: random_value)
    monkeypatch.setattr(consumer.httpx, "AsyncClient", lambda **_: client)

    await consumer.handle_once(item.id)

    assert item.status == status
    assert item.webhook_delivered_at is not None
    assert sessions.begin_count == 2
    sleep.assert_awaited_once_with(3)
    response.raise_for_status.assert_called_once()
    _, kwargs = client.post.call_args
    assert kwargs["json"]["payment_id"] == str(item.id)
    assert kwargs["json"]["status"] == status
    assert kwargs["json"]["amount"] == "10.00"
    assert kwargs["headers"] == {"X-Event-ID": str(item.id)}


async def test_redelivery_skips_gateway_and_webhook(monkeypatch):
    item = payment(status="succeeded", delivered=True)
    sessions = Sessions(item)
    client = Client(Mock())
    monkeypatch.setattr(consumer, "session_factory", sessions)
    monkeypatch.setattr(consumer.httpx, "AsyncClient", lambda **_: client)
    await consumer.handle_once(item.id)
    assert sessions.begin_count == 1
    sessions.session.execute.assert_not_awaited()
    client.post.assert_not_awaited()


async def test_terminal_payment_retries_undelivered_webhook(monkeypatch):
    item = payment(status="failed")
    sessions = Sessions(item)
    client = Client(Mock())
    monkeypatch.setattr(consumer, "session_factory", sessions)
    monkeypatch.setattr(consumer.httpx, "AsyncClient", lambda **_: client)
    await consumer.handle_once(item.id)
    assert sessions.begin_count == 1
    sessions.session.execute.assert_not_awaited()
    client.post.assert_awaited_once()


async def test_missing_payment_is_an_error(monkeypatch):
    monkeypatch.setattr(consumer, "session_factory", Sessions(None))
    with pytest.raises(NoResultFound, match="not found"):
        await consumer.handle_once(uuid4())


async def test_webhook_failure_does_not_record_delivery(monkeypatch):
    item = payment(status="succeeded")
    sessions = Sessions(item)
    response = Mock()
    response.raise_for_status.side_effect = RuntimeError("HTTP 503")
    monkeypatch.setattr(consumer, "session_factory", sessions)
    monkeypatch.setattr(consumer.httpx, "AsyncClient", lambda **_: Client(response))
    with pytest.raises(RuntimeError, match="HTTP 503"):
        await consumer.handle_once(item.id)
    assert item.webhook_delivered_at is None


@pytest.mark.parametrize("event", [{}, {"payment_id": "invalid"}, {"payment_id": None}])
async def test_invalid_event_is_rejected(event):
    with pytest.raises(RejectMessage):
        await consumer.process_payment(event)


async def test_transient_failure_retries_then_succeeds(monkeypatch):
    handle = AsyncMock(side_effect=[httpx.ConnectError("temporary"), None])
    sleep = AsyncMock()
    monkeypatch.setattr(consumer, "handle_once", handle)
    monkeypatch.setattr(consumer.asyncio, "sleep", sleep)
    payment_id = uuid4()
    await consumer.process_payment({"payment_id": str(payment_id)})
    assert handle.await_count == 2
    sleep.assert_awaited_once_with(1)


async def test_three_failures_reject_message(monkeypatch):
    handle = AsyncMock(side_effect=httpx.ConnectError("unavailable"))
    sleep = AsyncMock()
    monkeypatch.setattr(consumer, "handle_once", handle)
    monkeypatch.setattr(consumer.asyncio, "sleep", sleep)
    with pytest.raises(RejectMessage):
        await consumer.process_payment({"payment_id": str(uuid4())})
    assert handle.await_count == 3
    assert [call.args[0] for call in sleep.await_args_list] == [1, 2]


@pytest.mark.parametrize("status,retries", [(400, False), (408, True), (429, True), (503, True)])
async def test_http_status_retry_policy(monkeypatch, status, retries):
    request = httpx.Request("POST", "https://example.com/hook")
    response = httpx.Response(status, request=request)
    error = httpx.HTTPStatusError("webhook rejected", request=request, response=response)
    handle = AsyncMock(side_effect=error)
    sleep = AsyncMock()
    monkeypatch.setattr(consumer, "handle_once", handle)
    monkeypatch.setattr(consumer.asyncio, "sleep", sleep)
    with pytest.raises(RejectMessage):
        await consumer.process_payment({"payment_id": str(uuid4())})
    assert handle.await_count == (3 if retries else 1)
    assert sleep.await_count == (2 if retries else 0)


async def test_transient_database_error_retries(monkeypatch):
    handle = AsyncMock(side_effect=[OperationalError("SELECT", {}, OSError("down")), None])
    sleep = AsyncMock()
    monkeypatch.setattr(consumer, "handle_once", handle)
    monkeypatch.setattr(consumer.asyncio, "sleep", sleep)
    await consumer.process_payment({"payment_id": str(uuid4())})
    assert handle.await_count == 2
    sleep.assert_awaited_once_with(1)


async def test_permanent_database_error_goes_directly_to_dlq(monkeypatch):
    handle = AsyncMock(side_effect=ProgrammingError("SELECT", {}, Exception("bad SQL")))
    monkeypatch.setattr(consumer, "handle_once", handle)
    with pytest.raises(RejectMessage):
        await consumer.process_payment({"payment_id": str(uuid4())})
    handle.assert_awaited_once()


async def test_missing_payment_goes_directly_to_dlq(monkeypatch):
    handle = AsyncMock(side_effect=NoResultFound("missing"))
    monkeypatch.setattr(consumer, "handle_once", handle)
    with pytest.raises(RejectMessage):
        await consumer.process_payment({"payment_id": str(uuid4())})
    handle.assert_awaited_once()


async def test_unexpected_error_is_not_retried(monkeypatch):
    handle = AsyncMock(side_effect=RuntimeError("bug"))
    monkeypatch.setattr(consumer, "handle_once", handle)
    with pytest.raises(RuntimeError, match="bug"):
        await consumer.process_payment({"payment_id": str(uuid4())})
    handle.assert_awaited_once()


async def test_main_declares_topology_starts_consumer_and_disposes_engine(monkeypatch):
    class Broker:
        start = AsyncMock()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

    class Event:
        async def wait(self):
            raise asyncio.CancelledError

    broker = Broker()
    declare = AsyncMock()
    dispose = AsyncMock()
    monkeypatch.setattr(consumer, "broker", broker)
    monkeypatch.setattr(consumer, "declare_topology", declare)
    monkeypatch.setattr(consumer.asyncio, "Event", Event)
    monkeypatch.setattr(consumer, "engine", SimpleNamespace(dispose=dispose))
    with pytest.raises(asyncio.CancelledError):
        await consumer.main()
    declare.assert_awaited_once_with(broker)
    broker.start.assert_awaited_once()
    dispose.assert_awaited_once()
