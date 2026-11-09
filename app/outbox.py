import asyncio
import logging

from aio_pika.exceptions import AMQPException
from faststream.rabbit import Channel, RabbitBroker
from sqlalchemy import func, select
from sqlalchemy.exc import InterfaceError, OperationalError

from app.config import RABBITMQ_URL
from app.db import session_factory
from app.messaging import NEW_QUEUE, declare_topology
from app.models import Outbox

logger = logging.getLogger(__name__)


async def publish_outbox(stop_event: asyncio.Event | None = None) -> None:
    stop_event = stop_event or asyncio.Event()
    while True:
        try:
            async with session_factory.begin() as session:
                rows = (
                    await session.scalars(
                        select(Outbox)
                        .where(Outbox.published_at.is_(None))
                        .order_by(Outbox.created_at, Outbox.id)
                        .limit(20)
                        .with_for_update(skip_locked=True)
                    )
                ).all()
                if rows:
                    async with RabbitBroker(
                        RABBITMQ_URL,
                        default_channel=Channel(on_return_raises=True),
                        timeout=5,
                        graceful_timeout=2,
                    ) as broker:
                        await declare_topology(broker)
                        for row in rows:
                            await asyncio.wait_for(
                                broker.publish(
                                    row.payload,
                                    queue=NEW_QUEUE,
                                    persist=True,
                                    mandatory=True,
                                    message_id=str(row.id),
                                    timeout=10,
                                ),
                                timeout=15,
                            )
                            row.published_at = func.now()
            if not rows:
                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            if stop_event.is_set():
                raise
            logger.warning("RabbitMQ interrupted outbox publishing; reconnecting")
            await asyncio.sleep(2)
        except (AMQPException, OSError, TimeoutError) as exc:
            logger.warning("RabbitMQ unavailable (%s); retrying outbox", type(exc).__name__)
            await asyncio.sleep(2)
        except (OperationalError, InterfaceError) as exc:
            logger.warning("Database unavailable (%s); retrying outbox", type(exc).__name__)
            await asyncio.sleep(2)
