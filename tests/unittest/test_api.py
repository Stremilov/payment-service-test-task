import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError, OperationalError

from app import api
from app.models import Outbox, Payment
from app.schemas import PaymentCreate


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class IdempotencyViolation(Exception):
    constraint_name = "uq_payments_idempotency_key"


def body(**changes):
    data = {
        "amount": "12.50",
        "currency": "RUB",
        "description": "Order",
        "metadata": {"order": 1},
        "webhook_url": "https://example.com/hook",
    }
    return PaymentCreate.model_validate(data | changes)


def saved_payment(request, key="key"):
    canonical = json.dumps(request.model_dump(mode="json", by_alias=True), sort_keys=True, separators=(",", ":"))
    return Payment(
        id=uuid4(),
        amount=request.amount,
        currency=request.currency,
        description=request.description,
        metadata_json=request.metadata_json,
        status="pending",
        idempotency_key=key,
        request_hash=hashlib.sha256(canonical.encode()).hexdigest(),
        webhook_url=str(request.webhook_url),
        created_at=NOW,
        processed_at=None,
        webhook_delivered_at=None,
    )


class Sessions:
    def __init__(self, *, existing=None, flush_error=None):
        self.write = Mock()
        self.write.add = Mock()
        self.write.flush = AsyncMock(side_effect=self.flush)
        self.read = Mock()
        self.read.get = AsyncMock(side_effect=self.get)
        self.read.scalar = AsyncMock(return_value=existing)
        self.existing = existing
        self.flush_error = flush_error

    async def flush(self):
        if self.flush_error:
            raise self.flush_error
        payment = self.write.add.call_args_list[0].args[0]
        payment.id = uuid4()
        payment.status = "pending"
        payment.created_at = NOW
        self.existing = payment

    async def get(self, *_):
        return self.existing

    @asynccontextmanager
    async def begin(self):
        yield self.write

    @asynccontextmanager
    async def __call__(self):
        yield self.read


def test_api_key(monkeypatch):
    monkeypatch.setattr(api, "API_KEY", "secret")
    for value in (None, "bad"):
        with pytest.raises(HTTPException) as error:
            api.require_api_key(value)
        assert error.value.status_code == 401
    assert api.require_api_key("secret") is None


@pytest.mark.parametrize("key", ["", "   "])
async def test_blank_idempotency_key(key):
    with pytest.raises(HTTPException) as error:
        await api.create_payment(body(), key)
    assert error.value.status_code == 400


async def test_create_payment_writes_payment_and_outbox_together(monkeypatch):
    sessions = Sessions()
    monkeypatch.setattr(api, "session_factory", sessions)

    response = await api.create_payment(body(), "key")

    payment, event = [call.args[0] for call in sessions.write.add.call_args_list]
    assert isinstance(payment, Payment)
    assert isinstance(event, Outbox)
    assert event.payment_id == response.payment_id == payment.id
    assert event.payload == {"payment_id": str(payment.id)}
    assert response.status == "pending"
    assert response.created_at == NOW


@pytest.mark.parametrize("same_body,expected_status", [(True, 202), (False, 409)])
async def test_reused_idempotency_key(monkeypatch, same_body, expected_status):
    request = body()
    existing = saved_payment(request)
    sessions = Sessions(
        existing=existing,
        flush_error=IntegrityError("INSERT", {}, IdempotencyViolation("duplicate")),
    )
    monkeypatch.setattr(api, "session_factory", sessions)

    if same_body:
        result = await api.create_payment(request, "key")
        assert result.payment_id == existing.id
    else:
        with pytest.raises(HTTPException) as error:
            await api.create_payment(body(amount="13.00"), "key")
        assert error.value.status_code == expected_status


async def test_unrelated_integrity_error_is_not_hidden(monkeypatch):
    sessions = Sessions(flush_error=IntegrityError("INSERT", {}, Exception("other")))
    monkeypatch.setattr(api, "session_factory", sessions)
    with pytest.raises(IntegrityError):
        await api.create_payment(body(), "key")


async def test_idempotency_violation_without_existing_payment_is_not_hidden(monkeypatch):
    sessions = Sessions(flush_error=IntegrityError("INSERT", {}, IdempotencyViolation("duplicate")))
    monkeypatch.setattr(api, "session_factory", sessions)
    with pytest.raises(IntegrityError):
        await api.create_payment(body(), "key")


async def test_database_failure_returns_503_without_details():
    response = await api.database_unavailable(None, OperationalError("SELECT secret", {}, OSError("down")))
    assert response.status_code == 503
    assert response.headers["retry-after"] == "2"
    assert b"secret" not in response.body


async def test_get_payment(monkeypatch):
    payment = saved_payment(body())
    sessions = Sessions(existing=payment)
    monkeypatch.setattr(api, "session_factory", sessions)
    result = await api.get_payment(payment.id)
    assert result.id == payment.id
    assert result.model_dump(by_alias=True)["metadata"] == {"order": 1}

    sessions.existing = None
    with pytest.raises(HTTPException) as error:
        await api.get_payment(payment.id)
    assert error.value.status_code == 404


async def test_lifespan_requires_key(monkeypatch):
    monkeypatch.setattr(api, "API_KEY", None)
    with pytest.raises(RuntimeError, match="API_KEY"):
        async with api.lifespan(api.app):
            pass


async def test_lifespan_stops_publisher_and_disposes_engine(monkeypatch):
    monkeypatch.setattr(api, "API_KEY", "secret")
    started = asyncio.Event()

    async def publisher(_):
        started.set()
        await asyncio.Event().wait()

    dispose = AsyncMock()
    monkeypatch.setattr(api, "publish_outbox", publisher)
    monkeypatch.setattr(api, "engine", SimpleNamespace(dispose=dispose))
    async with api.lifespan(api.app):
        await started.wait()
    dispose.assert_awaited_once()
