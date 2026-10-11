---
status: proposed
---

# Independent consumer group per route

Each route has a stable Kafka consumer group and a stored partition start offset captured at route creation. This lets two routes on one topic progress independently, skip records predating each route, and catch up after downtime. A shared topic consumer would need separate per-route delivery cursors and more complex recovery logic. Independent groups cost one consumer per enabled route, so initial deployment targets modest route counts and must be revisited if load tests show that cost is too high.
