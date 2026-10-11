"""Lifecycle wrapper around one route's Kafka consumer group."""

from __future__ import annotations

import asyncio
import logging

from kafka_component import KafkaConsumerComponent, PartitionAssignment

from telegram_kafka.delivery import DeliveryHandler, DeliveryStore, Sender

logger = logging.getLogger(__name__)


class RouteWorker:
    def __init__(
        self,
        route_id: str,
        topic: str,
        broker: str,
        start_offsets: dict[int, int],
        store: DeliveryStore,
        sender: Sender,
    ) -> None:
        self.last_handler_error: str | None = None
        self.last_processed: dict[int, int] = {}
        handler = DeliveryHandler(route_id, store, sender)

        async def guarded(record) -> None:
            delay = 1
            while True:
                try:
                    await handler(record)
                    self.last_processed[record.partition] = record.offset
                    self.last_handler_error = None
                    return
                except Exception as exc:
                    # Returning or raising to kafka-component's default
                    # SkipAndLogPolicy would commit this record. Keep it in-flight.
                    self.last_handler_error = type(exc).__name__
                    logger.error(
                        "Route delivery paused: route=%s topic=%s partition=%s offset=%s error=%s",
                        route_id, record.topic, record.partition, record.offset,
                        type(exc).__name__,
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30)

        async def resolve(assignment: PartitionAssignment) -> int:
            return start_offsets.get(assignment.partition, assignment.beginning_offset)

        self.component = KafkaConsumerComponent(
            bootstrap_servers=broker,
            group_id=f"telegram-route-{route_id}",
            topics=[topic],
            record_handler=guarded,
            start_offset_resolver=resolve,
            auto_offset_reset="earliest",
        )

    async def start(self) -> None:
        await self.component.start()

    async def stop(self) -> None:
        await self.component.shutdown()

    def status(self) -> dict:
        status = self.component.get_status()
        return {
            "connected": status["connected"],
            "last_error": self.last_handler_error or (
                "KafkaConsumerError" if status["last_error"] else None
            ),
            "last_processed": {
                str(partition): offset for partition, offset in self.last_processed.items()
            },
        }
