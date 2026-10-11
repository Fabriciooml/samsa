import asyncio
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaProducer
from cryptography.fernet import Fernet
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.kafka import KafkaContainer
from testcontainers.community.postgres import PostgresContainer

from telegram_kafka.app import Settings, create_app
from telegram_kafka.models import Base
from telegram_kafka.offsets import KafkaOffsetProvider
from telegram_kafka.worker import RouteWorker
from telegram_kafka.delivery import DeliveryRoute


class Store:
    def __init__(self):
        self.failures = []

    async def load_route(self, route_id):
        return DeliveryRoute(route_id, "orders", -100123, "{{order.id}}", "123:SECRET")

    async def record_failure(self, **details):
        self.failures.append(details)


class Sender:
    def __init__(self):
        self.messages = []
        self.received = asyncio.Event()

    async def send(self, token, chat_id, text):
        self.messages.append(text)
        self.received.set()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_new_route_skips_history_and_catches_up_after_restart():
    with KafkaContainer("confluentinc/cp-kafka:7.9.0") as kafka:
        broker = kafka.get_bootstrap_server()
        producer = AIOKafkaProducer(bootstrap_servers=broker)
        await producer.start()
        try:
            await producer.send_and_wait("orders", b'{"order":{"id":"old"}}')
            starts = await KafkaOffsetProvider(broker).capture("orders")
            assert starts == {0: 1}
            store, sender = Store(), Sender()
            route_id = str(uuid4())
            worker = RouteWorker(route_id, "orders", broker, starts, store, sender)
            await worker.start()
            await producer.send_and_wait("orders", b'{"order":{"id":"new"}}')
            await asyncio.wait_for(sender.received.wait(), timeout=15)
            assert sender.messages == ["new"]
            await worker.stop()

            await producer.send_and_wait("orders", b'{"order":{"id":"during downtime"}}')
            sender.received.clear()
            worker = RouteWorker(route_id, "orders", broker, starts, store, sender)
            await worker.start()
            await asyncio.wait_for(sender.received.wait(), timeout=15)
            assert sender.messages == ["new", "during downtime"]
            await worker.stop()
        finally:
            await producer.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_route_crud_starts_consumer_and_sends_telegram_message():
    with KafkaContainer("confluentinc/cp-kafka:7.9.0") as kafka, PostgresContainer(
        "postgres:17-alpine", driver="asyncpg"
    ) as postgres:
        broker, database_url = kafka.get_bootstrap_server(), postgres.get_connection_url()
        engine = create_async_engine(database_url)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await engine.dispose()
        sender = Sender()
        settings = Settings(
            database_url=database_url, kafka_bootstrap=broker,
            admin_password="test password", encryption_key=Fernet.generate_key().decode(),
            session_secret="test-session-secret-with-enough-length", secure_cookies=False,
        )
        app = create_app(settings, telegram_sender=sender)
        producer = AIOKafkaProducer(bootstrap_servers=broker)
        await producer.start()
        try:
            await producer.send_and_wait("orders", b'{"order":{"id":"old"}}')
            await producer.send_and_wait("payments", b'{"order":{"id":"old payment"}}')
            async with app.router.lifespan_context(app):
                async with AsyncClient(
                    transport=ASGITransport(app=app), base_url="http://testserver"
                ) as client:
                    csrf = (
                        await client.post("/api/login", json={"password": "test password"})
                    ).json()["csrf_token"]
                    headers = {"X-CSRF-Token": csrf}
                    bot = (
                        await client.post(
                            "/api/bots", json={"name": "alerts", "token": "123:SECRET"},
                            headers=headers,
                        )
                    ).json()
                    route = await client.post(
                        "/api/routes",
                        json={"topic": "orders", "bot_id": bot["id"],
                              "chat_id": -100123, "template": "{{order.id}}"},
                        headers=headers,
                    )
                    assert route.status_code == 201, route.text
                    second_route = await client.post(
                        "/api/routes",
                        json={"topic": "payments", "bot_id": bot["id"],
                              "chat_id": -100456, "template": "{{order.id}}"},
                        headers=headers,
                    )
                    assert second_route.status_code == 201, second_route.text
                    await producer.send_and_wait("orders", b'{"order":{"id":"new"}}')
                    await producer.send_and_wait("payments", b'{"order":{"id":"new payment"}}')
                    async def both_delivered():
                        while len(sender.messages) < 2:
                            await asyncio.sleep(0.05)
                    await asyncio.wait_for(both_delivered(), timeout=15)
                    assert set(sender.messages) == {"new", "new payment"}
                    status = (await client.get("/api/status")).json()
                    assert status["workers"][route.json()["id"]]["last_processed"] == {"0": 1}
                    assert status["workers"][second_route.json()["id"]]["last_processed"] == {"0": 1}
        finally:
            await producer.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_unexpected_handler_error_retries_same_record_before_commit():
    with KafkaContainer("confluentinc/cp-kafka:7.9.0") as kafka:
        broker = kafka.get_bootstrap_server()
        producer = AIOKafkaProducer(bootstrap_servers=broker)
        await producer.start()
        try:
            await producer.send_and_wait("orders", b'{"order":{"id":"old"}}')
            starts = await KafkaOffsetProvider(broker).capture("orders")

            class FlakySender(Sender):
                def __init__(self):
                    super().__init__()
                    self.attempts = 0

                async def send(self, token, chat_id, text):
                    self.attempts += 1
                    if self.attempts == 1:
                        raise RuntimeError("temporary dependency error")
                    await super().send(token, chat_id, text)

            sender = FlakySender()
            worker = RouteWorker(str(uuid4()), "orders", broker, starts, Store(), sender)
            await worker.start()
            try:
                await producer.send_and_wait("orders", b'{"order":{"id":"A"}}')
                await asyncio.wait_for(sender.received.wait(), timeout=12)
                assert sender.messages == ["A"]
                assert sender.attempts == 2
            finally:
                await worker.stop()
        finally:
            await producer.stop()
