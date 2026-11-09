import asyncio
import hashlib
import json
import logging
import secrets
from contextlib import asynccontextmanager, suppress
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, InterfaceError, OperationalError

from app.config import API_KEY
from app.db import engine, session_factory
from app.models import Outbox, Payment
from app.outbox import publish_outbox
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetails

logger = logging.getLogger(__name__)


def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if not x_api_key or not secrets.compare_digest(x_api_key, API_KEY or ""):
        raise HTTPException(status_code=401, detail="Invalid API key")


@asynccontextmanager
async def lifespan(_: FastAPI):
    if not API_KEY:
        raise RuntimeError("API_KEY must be set")
    stop_event = asyncio.Event()
    publisher = asyncio.create_task(publish_outbox(stop_event))
    try:
        yield
    finally:
        stop_event.set()
        publisher.cancel()
        with suppress(asyncio.CancelledError):
            await publisher
        await engine.dispose()


app = FastAPI(
    title="Payment service",
    lifespan=lifespan,
    dependencies=[Depends(require_api_key)],
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


@app.exception_handler(OperationalError)
@app.exception_handler(InterfaceError)
async def database_unavailable(_: Request, exc: OperationalError | InterfaceError) -> JSONResponse:
    logger.warning("Database unavailable: %s", type(exc).__name__)
    return JSONResponse(
        status_code=503,
        content={"detail": "Database temporarily unavailable"},
        headers={"Retry-After": "2"},
    )


@app.post("/api/v1/payments", status_code=202, response_model=PaymentAccepted)
async def create_payment(
    body: PaymentCreate,
    idempotency_key: str = Header(min_length=1, max_length=255),
) -> PaymentAccepted:
    if not idempotency_key.strip():
        raise HTTPException(status_code=400, detail="Idempotency-Key must not be blank")

    canonical = json.dumps(body.model_dump(mode="json", by_alias=True), sort_keys=True, separators=(",", ":"))
    request_hash = hashlib.sha256(canonical.encode()).hexdigest()
    payment = Payment(
        amount=body.amount,
        currency=body.currency,
        description=body.description,
        metadata_json=body.metadata_json,
        webhook_url=str(body.webhook_url),
        idempotency_key=idempotency_key,
        request_hash=request_hash,
    )
    try:
        async with session_factory.begin() as session:
            session.add(payment)
            await session.flush()
            session.add(
                Outbox(
                    payment_id=payment.id,
                    event_type="payment.created",
                    payload={"payment_id": str(payment.id)},
                )
            )
        async with session_factory() as session:
            payment = await session.get(Payment, payment.id)
    except IntegrityError as exc:
        driver_error = getattr(exc, "driver_exception", exc.orig)
        constraint = getattr(driver_error, "constraint_name", None)
        if constraint is None:
            constraint = getattr(getattr(exc.orig, "__cause__", None), "constraint_name", None)
        if constraint != "uq_payments_idempotency_key":
            raise
        async with session_factory() as session:
            payment = await session.scalar(
                select(Payment).where(Payment.idempotency_key == idempotency_key)
            )
        if payment is None:
            raise
        if payment.request_hash != request_hash:
            raise HTTPException(status_code=409, detail="Idempotency-Key used with different payment")

    return PaymentAccepted(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


@app.get("/api/v1/payments/{payment_id}", response_model=PaymentDetails)
async def get_payment(payment_id: UUID) -> PaymentDetails:
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    return PaymentDetails.model_validate(payment)
