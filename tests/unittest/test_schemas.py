from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.schemas import PaymentCreate, PaymentDetails


def request(**changes):
    return {
        "amount": "123.45",
        "currency": "RUB",
        "description": "Order",
        "webhook_url": "https://example.com/webhook",
    } | changes


@pytest.mark.parametrize(
    "changes",
    [
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "1.001"},
        {"amount": "12345678901234567.00"},
        {"currency": "GBP"},
        {"description": ""},
        {"description": "x" * 501},
        {"webhook_url": "ftp://example.com/hook"},
    ],
)
def test_rejects_invalid_payment(changes):
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(request(**changes))


def test_metadata_defaults_and_amount_stays_decimal():
    payment = PaymentCreate.model_validate(request())
    assert payment.amount == Decimal("123.45")
    assert payment.metadata_json == {}
    assert payment.model_dump(mode="json", by_alias=True)["metadata"] == {}


def test_details_serialize_metadata_from_orm_attribute():
    data = SimpleNamespace(
        id=uuid4(), amount=Decimal("1.00"), currency="EUR", description="Order",
        metadata_json={"merchant": 5}, status="succeeded", idempotency_key="key",
        webhook_url="https://example.com/hook", created_at=datetime.now(timezone.utc),
        processed_at=None, webhook_delivered_at=None,
    )
    details = PaymentDetails.model_validate(data)
    assert details.model_dump(by_alias=True)["metadata"] == {"merchant": 5}
