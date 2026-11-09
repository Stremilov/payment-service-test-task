import asyncio
import logging
import random
from uuid import UUID

import httpx
from faststream.exceptions import RejectMessage
from faststream.rabbit import RabbitBroker
from sqlalchemy import func, update
from sqlalchemy.exc import InterfaceError, NoResultFound, OperationalError, SQLAlchemyError

from app.config import RABBITMQ_URL
from app.db import engine, session_factory
from app.messaging import NEW_QUEUE, declare_topology
from app.models import Payment

logger = logging.getLogger(__name__)
broker = RabbitBroker(RABBITMQ_URL)


async def handle_once(payment_id: UUID) -> None:
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
        if payment is None:
            raise NoResultFound(f"Payment {payment_id} not found")
        pending = payment.status == "pending"

    if pending:
        await asyncio.sleep(random.uniform(2, 5))
        status = "succeeded" if random.random() < 0.9 else "failed"
        async with session_factory.begin() as session:
            await session.execute(
                update(Payment)
                .where(Payment.id == payment_id, Payment.status == "pending")
                .values(status=status, processed_at=func.now())
            )

    # The row lock serializes duplicate deliveries for this payment.
    async with session_factory.begin() as session:
        payment = await session.get(Payment, payment_id, with_for_update=True)
        if payment.webhook_delivered_at is not None:
            return
        async with httpx.AsyncClient(timeout=5, follow_redirects=False) as client:
            response = await client.post(
                payment.webhook_url,
                json={
                    "payment_id": str(payment.id),
                    "status": payment.status,
                    "amount": str(payment.amount),
                    "currency": payment.currency,
                    "processed_at": payment.processed_at.isoformat(),
                },
                headers={"X-Event-ID": str(payment.id)},
            )
            response.raise_for_status()
        payment.webhook_delivered_at = func.now()


@broker.subscriber(NEW_QUEUE)
async def process_payment(event: dict) -> None:
    try:
        payment_id = UUID(event["payment_id"])
    except (KeyError, ValueError, TypeError) as exc:
        raise RejectMessage() from exc

    for attempt in range(3):
        try:
            await handle_once(payment_id)
            return
        except NoResultFound as exc:
            logger.error("Payment %s is missing; sending to DLQ", payment_id)
            raise RejectMessage() from exc
        except (httpx.HTTPStatusError, httpx.RequestError, OperationalError, InterfaceError, TimeoutError) as exc:
            if isinstance(exc, httpx.HTTPStatusError):
                status = exc.response.status_code
                if status not in (408, 429) and status < 500:
                    logger.warning("Webhook for payment %s returned HTTP %s; sending to DLQ", payment_id, status)
                    raise RejectMessage() from exc
            logger.warning("Payment %s attempt %s/3 failed: %s", payment_id, attempt + 1, type(exc).__name__)
            if attempt == 2:
                raise RejectMessage() from exc
            await asyncio.sleep(2**attempt)
        except SQLAlchemyError as exc:
            logger.error("Payment %s has a non-retryable database error: %s", payment_id, type(exc).__name__)
            raise RejectMessage() from exc


async def main() -> None:
    try:
        async with broker:
            await declare_topology(broker)
            await broker.start()
            await asyncio.Event().wait()
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
