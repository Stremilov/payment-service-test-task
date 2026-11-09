"""Run against the stack started with docker compose up -d --build."""

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import httpx
import pytest

from app.config import settings

BASE_URL = os.getenv("TEST_API_URL", "http://localhost:8000")
WEBHOOK_HOST = os.getenv("TEST_WEBHOOK_HOST", "host.docker.internal")
RABBIT_MANAGEMENT_URL = os.getenv("TEST_RABBIT_MANAGEMENT_URL", "http://localhost:15672")
API_KEY = settings.api_key.get_secret_value()
RABBIT_AUTH = (settings.rabbitmq_user, settings.rabbitmq_password.get_secret_value())


class WebhookHandler(BaseHTTPRequestHandler):
    events = []

    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        self.events.append((self.path, json.loads(body), self.headers["X-Event-ID"]))
        self.send_response({"/fail": 503, "/bad-request": 400}.get(self.path, 204))
        self.end_headers()

    def log_message(self, *_):
        pass


@pytest.fixture(scope="module")
def webhook_server():
    server = ThreadingHTTPServer(("0.0.0.0", 0), WebhookHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def wait_until(check, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.3)
    raise AssertionError("Timed out waiting for payment processing")


def payment_body(webhook_url):
    return {
        "amount": "1250.00",
        "currency": "RUB",
        "description": "Order 123",
        "metadata": {"order_id": 123},
        "webhook_url": webhook_url,
    }


def test_payment_idempotency_and_webhook(webhook_server):
    body = payment_body(f"http://{WEBHOOK_HOST}:{webhook_server.server_port}/ok")
    headers = {"X-API-Key": API_KEY, "Idempotency-Key": str(uuid4())}

    with httpx.Client(base_url=BASE_URL) as client:
        assert client.post("/api/v1/payments", json=body).status_code == 401
        created = client.post("/api/v1/payments", json=body, headers=headers)
        assert created.status_code == 202, created.text
        payment_id = created.json()["payment_id"]

        repeated = client.post("/api/v1/payments", json=body, headers=headers)
        assert repeated.status_code == 202
        assert repeated.json()["payment_id"] == payment_id

        changed = {**body, "amount": "1251.00"}
        assert client.post("/api/v1/payments", json=changed, headers=headers).status_code == 409

        def finished():
            response = client.get(f"/api/v1/payments/{payment_id}", headers=headers)
            assert response.status_code == 200, response.text
            data = response.json()
            return data if data["webhook_delivered_at"] else None

        payment = wait_until(finished)

    assert payment["status"] in ("succeeded", "failed")
    assert payment["metadata"] == {"order_id": 123}
    assert any(event_id == payment_id and event["status"] == payment["status"]
               for _, event, event_id in WebhookHandler.events)


@pytest.mark.parametrize("path,attempts", [("/fail", 3), ("/bad-request", 1)])
def test_failed_webhook_goes_to_dlq(webhook_server, path, attempts):
    management = f"{RABBIT_MANAGEMENT_URL}/api/queues/%2F/payments.dead"
    with httpx.Client(auth=RABBIT_AUTH) as rabbit:
        before = rabbit.get(management)
        assert before.status_code == 200, before.text
        dead_count = before.json()["messages"]

    body = payment_body(f"http://{WEBHOOK_HOST}:{webhook_server.server_port}{path}")
    headers = {"X-API-Key": API_KEY, "Idempotency-Key": str(uuid4())}
    with httpx.Client(base_url=BASE_URL) as client:
        created = client.post("/api/v1/payments", json=body, headers=headers)
        assert created.status_code == 202, created.text
        payment_id = created.json()["payment_id"]

        def dead_lettered():
            with httpx.Client(auth=RABBIT_AUTH) as rabbit:
                response = rabbit.get(management)
                assert response.status_code == 200, response.text
                return response.json()["messages"] > dead_count

        wait_until(dead_lettered)
        payment = client.get(f"/api/v1/payments/{payment_id}", headers=headers).json()

    assert payment["status"] in ("succeeded", "failed")
    assert payment["webhook_delivered_at"] is None
    assert sum(event_id == payment_id for _, _, event_id in WebhookHandler.events) == attempts


def test_http_validation():
    headers = {"X-API-Key": API_KEY, "Idempotency-Key": str(uuid4())}
    body = payment_body("https://example.com/hook")
    with httpx.Client(base_url=BASE_URL) as client:
        assert client.post("/api/v1/payments", json={**body, "amount": "0"}, headers=headers).status_code == 422
        assert client.post("/api/v1/payments", json={**body, "currency": "GBP"}, headers=headers).status_code == 422
        assert client.post("/api/v1/payments", json=body, headers={"X-API-Key": API_KEY}).status_code == 422
        assert client.get(f"/api/v1/payments/{uuid4()}", headers=headers).status_code == 404
        assert client.get(f"/api/v1/payments/{uuid4()}").status_code == 401


def test_concurrent_requests_share_one_payment(webhook_server):
    body = payment_body(f"http://{WEBHOOK_HOST}:{webhook_server.server_port}/ok")
    headers = {"X-API-Key": API_KEY, "Idempotency-Key": str(uuid4())}

    def create():
        return httpx.post(f"{BASE_URL}/api/v1/payments", json=body, headers=headers, timeout=10)

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(lambda _: create(), range(2)))

    assert [response.status_code for response in responses] == [202, 202]
    assert responses[0].json()["payment_id"] == responses[1].json()["payment_id"]
