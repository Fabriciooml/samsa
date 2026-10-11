# Feature request: record metadata and per-partition start offsets

**Status: fulfilled in [`kafka-component` 0.2.0](https://pypi.org/project/kafka-component/).** This document records original request. Release exposes `record_handler`, `start_offset_resolver`, `KafkaRecord`, and `PartitionAssignment`. Its resolver must use `auto_offset_reset="earliest"` or `"latest"`; combining resolver with `"none"` is rejected.

## Summary

Please extend `KafkaConsumerComponent` with an **opt-in record handler** and an **offset resolver for newly assigned partitions**. Existing `handler(value)` users should keep their current behavior. These two additions would let an application create a consumer at a known Kafka offset, resume from committed offsets after downtime, and associate failures with the exact source record.

This request is for generic Kafka consumer capabilities. The application will own route CRUD, offset persistence, JSON validation, retries, and Telegram delivery.

## Use case

An administrator creates a route from one Kafka topic to a notification destination. At creation time, the application reads and stores the end offset of each current topic partition. The route must skip older records but process every record published after that boundary, including records published before its worker starts. After a restart, it must resume from its committed offset.

`auto_offset_reset="latest"` alone cannot provide that boundary. If a route is created at 12:00 and its consumer starts at 12:01, `latest` skips records produced during that minute. `earliest` replays records from before route creation. The application must supply the saved offset for each partition on first assignment.

The application also needs `topic`, `partition`, and `offset` to save a durable failure record before advancing the consumer. A decoded value alone cannot identify the failed Kafka record.

## Historical API gap in 0.1.0

As reviewed in [version 0.1.0](https://pypi.org/project/kafka-component/0.1.0/):

- Consumer receives topics at construction and passes only `msg.value` to `handler`.
- Consumer sets `auto_offset_reset="earliest"` internally.
- No public assignment callback or start-offset hook accepts an application's stored per-partition boundary.
- JSON decoding can fail before application handler sees record coordinates.

Static topics per consumer are acceptable for this use case: application can create one consumer component per route. Runtime route management does **not** need to be built into this package.

## Requested behavior

### 1. Opt-in full record handler

Add a handler mode that receives a record containing at least `topic: str`, `partition: int`, `offset: int`, and raw `value: bytes`. Preserve current decoded-value handler as default. Key, timestamp, and headers are useful if already available, but not required for first release.

In record-handler mode, pass raw bytes before JSON decoding. A malformed JSON value must be reportable without losing its topic/partition/offset. An optional decoded-value field can be added later.

### 2. First-assignment offset resolver

For each assigned partition **without a valid committed group offset**, invoke an optional async resolver. It receives topic, partition, and available offset bounds, then returns an absolute next offset to consume. The application will look up its saved route-creation boundary and return it. If a partition was added after route creation, application can choose its earliest available offset.

When a valid committed offset exists, use it by default; do not seek back to the route-creation boundary. Rebalance must apply the same rule. Reject or surface out-of-range requested offsets instead of silently resetting, so application can report retention loss.

An `auto_offset_reset` option (`earliest`, `latest`, `none`) would also remove the current hardcoded policy. It is helpful, but **not a substitute** for the per-partition resolver.

### 3. Keep commit semantics explicit

Keep auto-commit disabled for this handler mode. Commit `record.offset + 1` only after handler completes successfully. If handler raises, do not advance offset unless an explicitly selected error policy says to skip. Existing policies and default behavior should remain unchanged for legacy handler mode.

This lets an application write a durable failure record, return successfully, and advance past a permanently bad message; temporary failures can raise and remain uncommitted.

## Possible API shape

Names below are illustrative; maintainer can choose API consistent with package style.

```python
async def handle_record(record: KafkaRecord) -> None:
    # record.topic, record.partition, record.offset, record.value
    await deliver_or_store_failure(record)


async def resolve_start_offset(assignment: PartitionAssignment) -> int:
    # Called only when this consumer group has no valid committed offset.
    # assignment exposes topic, partition, beginning_offset, end_offset.
    return await route_start_offsets.lookup_or_earliest(assignment)


consumer = KafkaConsumerComponent(
    bootstrap_servers="kafka:9092",
    group_id="notification-route-123",
    topics=["orders"],
    record_handler=handle_record,
    start_offset_resolver=resolve_start_offset,
    auto_offset_reset="earliest",
)
```

Current `KafkaConsumerComponent(..., handler=handle_value)` continues to work without modification. Supplying both handler modes should raise a clear configuration error.

## Acceptance tests

1. Existing `handler(value)` tests pass unchanged.
2. Record handler sees correct topic, partition, offset, and value for records on two partitions.
3. With no committed offset, resolver selects saved offset `N`; records before `N` are skipped, record `N` is handled.
4. Records produced between offset capture and consumer startup are handled.
5. After handling and committing, restart resumes from committed offset without invoking resolver to rewind.
6. On rebalance, existing committed offset wins; a newly added partition invokes resolver.
7. Handler exception leaves failed record uncommitted; successful handler commits next offset.
8. Malformed JSON remains observable with record coordinates in raw record-handler mode.
9. Out-of-range resolver result surfaces a distinct error rather than silently starting at earliest/latest.

Use real Kafka for offset/rebalance integration tests; unit tests can cover callback and legacy API compatibility.

## Non-goals

- Database-backed offset storage inside `kafka-component`.
- Dynamic CRUD of topics, routes, or consumers inside `kafka-component`.
- Telegram-specific delivery, retry, or failure-record logic.
- Exactly-once delivery to external APIs.

## Why this matters

This feature supports any application that activates a subscription at a business-defined moment and must later resume reliably: notifications, audit exporters, and tenant-specific consumers. It keeps Kafka mechanics in `kafka-component` while allowing applications to own their own activation and failure policies.

## Reference

- [aiokafka consumer offset and commit behavior](https://aiokafka.readthedocs.io/en/stable/consumer.html#offsets-and-consumer-position)
- [Service requirement and TDD plan](../PLAN.md)
