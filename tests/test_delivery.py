import pytest
from kafka_component import KafkaRecord

from telegram_kafka.delivery import DeliveryHandler, DeliveryRoute
from telegram_kafka.telegram import PermanentSendError


class FakeStore:
    def __init__(self):
        self.failures = []

    async def load_route(self, route_id):
        return DeliveryRoute(
            id=route_id, topic="orders", chat_id=-100123,
            template="*Order:* {{order.id}}", bot_token="123:SECRET",
        )

    async def record_failure(self, **details):
        self.failures.append(details)


class FakeSender:
    def __init__(self, error=None):
        self.calls = []
        self.error = error

    async def send(self, token, chat_id, text):
        self.calls.append((token, chat_id, text))
        if self.error:
            raise self.error


def record(value):
    return KafkaRecord(topic="orders", partition=2, offset=42, value=value)


@pytest.mark.asyncio
async def test_json_record_renders_and_sends():
    store, sender = FakeStore(), FakeSender()
    handler = DeliveryHandler("route-1", store, sender)
    await handler(record(b'{"order":{"id":"A_1"}}'))
    assert sender.calls == [("123:SECRET", -100123, "*Order:* A\\_1")]
    assert store.failures == []


@pytest.mark.asyncio
async def test_invalid_json_is_recorded_and_next_record_can_continue():
    store, sender = FakeStore(), FakeSender()
    handler = DeliveryHandler("route-1", store, sender)
    await handler(record(b"not-json"))
    assert sender.calls == []
    assert store.failures[0]["category"] == "invalid_json"
    assert store.failures[0]["partition"] == 2
    assert store.failures[0]["offset"] == 42


@pytest.mark.asyncio
async def test_permanent_send_failure_saves_rendered_text_for_manual_retry():
    store, sender = FakeStore(), FakeSender(PermanentSendError("rejected"))
    handler = DeliveryHandler("route-1", store, sender)
    await handler(record(b'{"order":{"id":"A_1"}}'))
    assert store.failures[0]["category"] == "telegram_permanent"
    assert store.failures[0]["rendered_text"] == "*Order:* A\\_1"
