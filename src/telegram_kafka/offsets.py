from __future__ import annotations

from aiokafka import AIOKafkaConsumer, TopicPartition


class KafkaOffsetProvider:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers

    async def capture(self, topic: str) -> dict[int, int]:
        consumer = AIOKafkaConsumer(topic, bootstrap_servers=self.bootstrap_servers)
        await consumer.start()
        try:
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                raise ValueError(f"Kafka topic does not exist: {topic}")
            offsets = await consumer.end_offsets(
                [TopicPartition(topic, partition) for partition in sorted(partitions)]
            )
            return {tp.partition: offset for tp, offset in offsets.items()}
        finally:
            await consumer.stop()
