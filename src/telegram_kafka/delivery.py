"""One Kafka record to one route's Telegram delivery."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Protocol

from kafka_component import KafkaRecord

from telegram_kafka.telegram import PermanentSendError, TransientSendError
from telegram_kafka.metrics import DELIVERIES, SEND_LATENCY
from telegram_kafka.template import InvalidTemplate, render_template


@dataclass(frozen=True)
class DeliveryRoute:
    id: str
    topic: str
    chat_id: int
    template: str
    bot_token: str


class DeliveryStore(Protocol):
    async def load_route(self, route_id: str) -> DeliveryRoute: ...

    async def record_failure(self, **details) -> None: ...


class Sender(Protocol):
    async def send(self, token: str, chat_id: int, text: str) -> None: ...


class DeliveryHandler:
    def __init__(self, route_id: str, store: DeliveryStore, sender: Sender):
        self.route_id = route_id
        self.store = store
        self.sender = sender

    async def _fail(
        self, record: KafkaRecord, category: str, detail: str, rendered_text: str | None = None
    ) -> None:
        await self.store.record_failure(
            route_id=self.route_id,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            category=category,
            detail=detail,
            rendered_text=rendered_text,
        )
        DELIVERIES.labels(outcome=category).inc()

    async def __call__(self, record: KafkaRecord) -> None:
        route = await self.store.load_route(self.route_id)
        try:
            if record.value is None:
                raise ValueError("Kafka tombstone has no JSON value")
            message = json.loads(record.value.decode("utf-8"))
            if not isinstance(message, dict):
                raise ValueError("JSON root must be an object")
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            await self._fail(record, "invalid_json", "Value is not a JSON object")
            return
        try:
            text = render_template(route.template, message)
        except InvalidTemplate as exc:
            await self._fail(record, "invalid_template_data", str(exc))
            return
        try:
            start = time.monotonic()
            await self.sender.send(route.bot_token, route.chat_id, text)
            SEND_LATENCY.observe(time.monotonic() - start)
            DELIVERIES.labels(outcome="sent").inc()
        except PermanentSendError:
            await self._fail(record, "telegram_permanent", "Telegram rejected message", text)
        except TransientSendError:
            await self._fail(record, "telegram_exhausted", "Telegram retry budget exhausted", text)
