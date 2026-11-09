import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy.exc import OperationalError

from app import messaging, outbox


class StopPublisher(Exception):
    pass


class Broker:
    def __init__(self):
        self.publish = AsyncMock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass


class Sessions:
    def __init__(self, rows, *, stop_after_first=True):
        self.rows = rows
        self.calls = 0
        self.session = SimpleNamespace(
            scalars=AsyncMock(return_value=SimpleNamespace(all=lambda: self.rows))
        )
        self.stop_after_first = stop_after_first

    @asynccontextmanager
    async def begin(self):
        self.calls += 1
        if self.stop_after_first and self.calls > 1:
            raise StopPublisher
        yield self.session


async def test_declares_dead_letter_route_before_main_queue():
    broker = SimpleNamespace(
        declare_exchange=AsyncMock(return_value="exchange"),
        declare_queue=AsyncMock(),
    )
    dead = SimpleNamespace(bind=AsyncMock())
    broker.declare_queue.side_effect = [dead, None]

    await messaging.declare_topology(broker)

    broker.declare_exchange.assert_awaited_once_with(messaging.DLX)
    assert broker.declare_queue.await_args_list[0].args == (messaging.DEAD_QUEUE,)
    assert broker.declare_queue.await_args_list[1].args == (messaging.NEW_QUEUE,)
    dead.bind.assert_awaited_once_with("exchange", routing_key="payments.dead")
    assert messaging.NEW_QUEUE.arguments["x-dead-letter-exchange"] == "payments.dlx"


async def test_outbox_marks_row_only_after_confirmed_publish(monkeypatch):
    row = SimpleNamespace(id=uuid4(), payload={"payment_id": str(uuid4())}, published_at=None)
    sessions = Sessions([row])
    broker = Broker()
    monkeypatch.setattr(outbox, "RabbitBroker", lambda *_, **__: broker)
    monkeypatch.setattr(outbox, "declare_topology", AsyncMock())
    monkeypatch.setattr(outbox, "session_factory", sessions)

    with pytest.raises(StopPublisher):
        await outbox.publish_outbox()

    broker.publish.assert_awaited_once()
    kwargs = broker.publish.await_args.kwargs
    assert kwargs["persist"] is True
    assert kwargs["mandatory"] is True
    assert kwargs["message_id"] == str(row.id)
    assert row.published_at is not None


async def test_empty_outbox_waits_before_polling_again(monkeypatch):
    sessions = Sessions([], stop_after_first=False)
    sleep = AsyncMock(side_effect=StopPublisher)
    monkeypatch.setattr(outbox, "RabbitBroker", lambda *_, **__: Broker())
    monkeypatch.setattr(outbox, "declare_topology", AsyncMock())
    monkeypatch.setattr(outbox, "session_factory", sessions)
    monkeypatch.setattr(outbox.asyncio, "sleep", sleep)
    with pytest.raises(StopPublisher):
        await outbox.publish_outbox()
    sleep.assert_awaited_once_with(0.5)


async def test_publish_failure_keeps_row_unpublished(monkeypatch):
    row = SimpleNamespace(id=uuid4(), payload={"payment_id": str(uuid4())}, published_at=None)
    broker = Broker()
    broker.publish.side_effect = OSError("broker unavailable")
    sleep = AsyncMock(side_effect=StopPublisher)
    monkeypatch.setattr(outbox, "RabbitBroker", lambda *_, **__: broker)
    monkeypatch.setattr(outbox, "declare_topology", AsyncMock())
    monkeypatch.setattr(outbox, "session_factory", Sessions([row]))
    monkeypatch.setattr(outbox.asyncio, "sleep", sleep)

    with pytest.raises(StopPublisher):
        await outbox.publish_outbox()

    assert row.published_at is None
    sleep.assert_awaited_once_with(2)


async def test_connection_failure_retries(monkeypatch):
    sleep = AsyncMock(side_effect=StopPublisher)

    def cannot_connect(*_, **__):
        raise OSError("RabbitMQ offline")

    monkeypatch.setattr(outbox, "RabbitBroker", cannot_connect)
    monkeypatch.setattr(
        outbox,
        "session_factory",
        Sessions([SimpleNamespace(id=uuid4(), payload={}, published_at=None)]),
    )
    monkeypatch.setattr(outbox.asyncio, "sleep", sleep)
    with pytest.raises(StopPublisher):
        await outbox.publish_outbox()
    sleep.assert_awaited_once_with(2)


async def test_database_failure_retries(monkeypatch):
    sleep = AsyncMock(side_effect=StopPublisher)
    sessions = Sessions([])
    sessions.session.scalars.side_effect = OperationalError("SELECT", {}, OSError("down"))
    monkeypatch.setattr(outbox, "RabbitBroker", lambda *_, **__: Broker())
    monkeypatch.setattr(outbox, "declare_topology", AsyncMock())
    monkeypatch.setattr(outbox, "session_factory", sessions)
    monkeypatch.setattr(outbox.asyncio, "sleep", sleep)
    with pytest.raises(StopPublisher):
        await outbox.publish_outbox()
    sleep.assert_awaited_once_with(2)


async def test_broker_cancelled_publish_reconnects(monkeypatch):
    broker = Broker()
    broker.publish.side_effect = asyncio.CancelledError
    row = SimpleNamespace(id=uuid4(), payload={"payment_id": str(uuid4())}, published_at=None)
    sleep = AsyncMock(side_effect=StopPublisher)
    monkeypatch.setattr(outbox, "RabbitBroker", lambda *_, **__: broker)
    monkeypatch.setattr(outbox, "declare_topology", AsyncMock())
    monkeypatch.setattr(outbox, "session_factory", Sessions([row]))
    monkeypatch.setattr(outbox.asyncio, "sleep", sleep)
    with pytest.raises(StopPublisher):
        await outbox.publish_outbox()
    assert row.published_at is None
    sleep.assert_awaited_once_with(2)


async def test_shutdown_cancels_publisher(monkeypatch):
    started = asyncio.Event()
    broker = Broker()

    async def blocked_publish(*_, **__):
        started.set()
        await asyncio.Event().wait()

    broker.publish.side_effect = blocked_publish
    row = SimpleNamespace(id=uuid4(), payload={"payment_id": str(uuid4())}, published_at=None)
    monkeypatch.setattr(outbox, "RabbitBroker", lambda *_, **__: broker)
    monkeypatch.setattr(outbox, "declare_topology", AsyncMock())
    monkeypatch.setattr(outbox, "session_factory", Sessions([row]))

    stop_event = asyncio.Event()
    task = asyncio.create_task(outbox.publish_outbox(stop_event))
    await started.wait()
    stop_event.set()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert row.published_at is None


async def test_unexpected_publisher_error_is_not_silently_retried(monkeypatch):
    def broken(*_, **__):
        raise RuntimeError("configuration bug")

    monkeypatch.setattr(outbox, "RabbitBroker", broken)
    monkeypatch.setattr(
        outbox,
        "session_factory",
        Sessions([SimpleNamespace(id=uuid4(), payload={}, published_at=None)]),
    )
    with pytest.raises(RuntimeError, match="configuration bug"):
        await outbox.publish_outbox()
