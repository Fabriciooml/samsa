import httpx
import pytest
from cryptography.fernet import Fernet
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.postgres import PostgresContainer

from telegram_kafka.app import Settings, create_app
from telegram_kafka.models import Base


@pytest.fixture(scope="module")
def postgres_url():
    with PostgresContainer("postgres:17-alpine", driver="asyncpg") as postgres:
        yield postgres.get_connection_url()


class FakeOffsets:
    async def capture(self, topic):
        if topic != "orders":
            raise ValueError("Topic not found")
        return {0: 5, 1: 10}


class CapturingSender:
    def __init__(self):
        self.calls = []

    async def send(self, token, chat_id, text):
        self.calls.append((token, chat_id, text))


@pytest.fixture
async def client(postgres_url):
    engine = create_async_engine(postgres_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()
    settings = Settings(
        database_url=postgres_url,
        kafka_bootstrap="kafka:9092",
        admin_password="test password",
        encryption_key=Fernet.generate_key().decode(),
        session_secret="test-session-secret-with-enough-length",
        start_workers=False,
        secure_cookies=False,
    )
    app = create_app(
        settings, offset_provider=FakeOffsets(), telegram_sender=CapturingSender()
    )
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as http_client:
            http_client.app = app
            yield http_client


@pytest.mark.asyncio
async def test_console_renders_template_examples_without_interpolation(client):
    response = await client.get("/")
    assert response.status_code == 200
    assert 'placeholder="*Order:* {{order.id}}"' in response.text
    assert "{{nested.field}}" in response.text


@pytest.mark.asyncio
async def test_admin_can_save_bot_but_api_never_returns_token(client):
    denied = await client.get("/api/bots")
    assert denied.status_code == 401

    login = await client.post("/api/login", json={"password": "test password"})
    assert login.status_code == 200
    csrf = login.json()["csrf_token"]

    created = await client.post(
        "/api/bots",
        json={"name": "alerts", "token": "123456:SECRET"},
        headers={"X-CSRF-Token": csrf},
    )
    assert created.status_code == 201
    assert created.json()["name"] == "alerts"
    assert "SECRET" not in created.text

    listed = await client.get("/api/bots")
    assert listed.status_code == 200
    assert len(listed.json()) == 1
    assert "SECRET" not in listed.text


@pytest.mark.asyncio
async def test_admin_can_create_route_with_saved_partition_boundaries(client):
    csrf = (await client.post("/api/login", json={"password": "test password"})).json()[
        "csrf_token"
    ]
    bot = (
        await client.post(
            "/api/bots",
            json={"name": "alerts", "token": "123456:SECRET"},
            headers={"X-CSRF-Token": csrf},
        )
    ).json()
    created = await client.post(
        "/api/routes",
        json={
            "topic": "orders",
            "bot_id": bot["id"],
            "chat_id": -100123456,
            "template": "*Order:* {{order.id}}",
        },
        headers={"X-CSRF-Token": csrf},
    )
    assert created.status_code == 201, created.text
    assert created.json()["start_offsets"] == {"0": 5, "1": 10}
    listed = await client.get("/api/routes")
    assert listed.json()[0]["topic"] == "orders"

    preview = await client.post(
        f"/api/routes/{created.json()['id']}/preview",
        json={"message": {"order": {"id": "A_1"}}},
        headers={"X-CSRF-Token": csrf},
    )
    assert preview.json() == {"text": "*Order:* A\\_1"}


@pytest.mark.asyncio
async def test_bot_and_route_can_be_updated_and_archived(client):
    csrf = (await client.post("/api/login", json={"password": "test password"})).json()[
        "csrf_token"
    ]
    headers = {"X-CSRF-Token": csrf}
    bot = (
        await client.post(
            "/api/bots", json={"name": "old", "token": "123456:SECRET"}, headers=headers
        )
    ).json()
    route = (
        await client.post(
            "/api/routes",
            json={
                "topic": "orders", "bot_id": bot["id"], "chat_id": -100123456,
                "template": "{{order.id}}",
            },
            headers=headers,
        )
    ).json()

    blocked = await client.delete(f"/api/bots/{bot['id']}", headers=headers)
    assert blocked.status_code == 409

    changed = await client.patch(
        f"/api/routes/{route['id']}",
        json={"template": "*ID:* {{order.id}}", "enabled": False},
        headers=headers,
    )
    assert changed.status_code == 200
    assert changed.json()["enabled"] is False
    assert changed.json()["start_offsets"] == route["start_offsets"]

    renamed = await client.patch(
        f"/api/bots/{bot['id']}",
        json={"name": "new", "token": "123456:NEWSECRET"},
        headers=headers,
    )
    assert renamed.status_code == 200
    assert renamed.json()["name"] == "new"
    assert "NEWSECRET" not in renamed.text

    assert (await client.delete(f"/api/routes/{route['id']}", headers=headers)).status_code == 204
    assert (await client.get("/api/routes")).json() == []
    assert (await client.delete(f"/api/bots/{bot['id']}", headers=headers)).status_code == 204
    replacement = await client.post(
        "/api/bots", json={"name": "new", "token": "123456:REPLACEMENT"}, headers=headers
    )
    assert replacement.status_code == 201, replacement.text
    another = (
        await client.post(
            "/api/bots", json={"name": "another", "token": "123456:ANOTHER"}, headers=headers
        )
    ).json()
    duplicate = await client.patch(
        f"/api/bots/{another['id']}", json={"name": "new"}, headers=headers
    )
    assert duplicate.status_code == 409


@pytest.mark.asyncio
async def test_invalid_kafka_value_is_saved_in_failure_history(client):
    from kafka_component import KafkaRecord

    from telegram_kafka.delivery import DeliveryHandler
    from telegram_kafka.store import SQLDeliveryStore

    csrf = (await client.post("/api/login", json={"password": "test password"})).json()[
        "csrf_token"
    ]
    headers = {"X-CSRF-Token": csrf}
    bot = (
        await client.post(
            "/api/bots", json={"name": "alerts", "token": "123456:SECRET"}, headers=headers
        )
    ).json()
    route = (
        await client.post(
            "/api/routes",
            json={"topic": "orders", "bot_id": bot["id"], "chat_id": -100123,
                  "template": "{{order.id}}"},
            headers=headers,
        )
    ).json()

    class NoSend:
        async def send(self, *args):
            raise AssertionError("Invalid JSON must not send")

    handler = DeliveryHandler(
        route["id"],
        SQLDeliveryStore(client.app.state.database, client.app.state.cipher),
        NoSend(),
    )
    record = KafkaRecord(topic="orders", partition=0, offset=5, value=b"bad")
    await handler(record)
    await handler(record)

    failures = await client.get("/api/failures")
    assert failures.status_code == 200
    assert len(failures.json()) == 1
    assert failures.json()[0]["category"] == "invalid_json"
    assert failures.json()[0]["offset"] == 5


@pytest.mark.asyncio
async def test_preview_can_be_test_sent_and_failed_send_retried(client):
    from telegram_kafka.store import SQLDeliveryStore

    csrf = (await client.post("/api/login", json={"password": "test password"})).json()[
        "csrf_token"
    ]
    headers = {"X-CSRF-Token": csrf}
    bot = (
        await client.post(
            "/api/bots", json={"name": "alerts", "token": "123456:SECRET"}, headers=headers
        )
    ).json()
    route = (
        await client.post(
            "/api/routes",
            json={"topic": "orders", "bot_id": bot["id"], "chat_id": -100123,
                  "template": "*ID:* {{order.id}}"},
            headers=headers,
        )
    ).json()
    sent = await client.post(
        f"/api/routes/{route['id']}/test",
        json={"message": {"order": {"id": "A_1"}}}, headers=headers,
    )
    assert sent.status_code == 200
    assert client.app.state.telegram_sender.calls == [
        ("123456:SECRET", -100123, "*ID:* A\\_1")
    ]

    store = SQLDeliveryStore(client.app.state.database, client.app.state.cipher)
    await store.record_failure(
        route_id=route["id"], topic="orders", partition=0, offset=9,
        category="telegram_exhausted", detail="Retry budget exhausted",
        rendered_text="*ID:* A\\_1",
    )
    failure_id = (await client.get("/api/failures")).json()[0]["id"]
    retried = await client.post(f"/api/failures/{failure_id}/retry", headers=headers)
    assert retried.status_code == 200
    assert retried.json()["retry_state"] == "sent"
    assert len(client.app.state.telegram_sender.calls) == 2
