# Kafka to Telegram service: TDD implementation plan

## Goal and agreed scope

Build one Portainer-deployed service connected to one Kafka cluster and existing PostgreSQL instance. Administrators manage Telegram bots and routes through authenticated browser UI backed by HTTP API. Each enabled route maps one Kafka topic to one Telegram group using one saved bot and one MarkdownV2 template. Multiple routes may share topics or bots. Kafka record values are JSON. A route starts at topic's current end when created, then catches up after service downtime. Duplicate Telegram messages are acceptable.

No Telegram polling/webhooks, inbound commands, media forwarding, multi-cluster support, or public user accounts in first version.

## Python components

- Python 3.13; FastAPI and Jinja2 templates for small server-rendered UI; Uvicorn with exactly one worker process.
- [`python-components`](https://github.com/lucassant95/python-components) manages lifecycle/dependency order. [`fastapi-component`](https://github.com/lucassant95/fastapi-component) connects component system to FastAPI lifespan and routers. Use app-owned single-admin session auth; its optional JWT plugin adds refresh-token storage not needed here.
- [`postgres-component`](https://github.com/fabriciooml/postgres-component) manages async PostgreSQL engine/sessions and startup health. SQLAlchemy 2 defines models/repositories; Alembic owns migrations.
- `prometheus-client` exposes HTTP and delivery metrics in one process. [`prometheus-component`](https://github.com/lucassant95/prometheus-component) is not published on PyPI under that name as of implementation; use it later if packaged. Keep route ID out of metric labels to avoid unbounded cardinality.
- [`kafka-component` 0.2.0](https://pypi.org/project/kafka-component/) runs one `KafkaConsumerComponent` per enabled route. Its `record_handler` supplies raw bytes and Kafka coordinates; `start_offset_resolver` applies saved route-creation offsets when no committed offset exists. A small app-owned `aiokafka` metadata client captures topic end offsets when admin creates a route. App supervisor starts/stops route components after CRUD changes and coordinates a shared async per-chat rate limiter.
- HTTPX for Telegram Bot API calls; `cryptography` for token encryption; Argon2 password hashing.
- pytest for unit and API tests; Docker-backed Kafka/PostgreSQL integration tests; local fake Telegram HTTP server for transport and end-to-end tests. Pin dependency versions when implementation begins.

## Recommended decisions completing unspecified details

- Single admin account. Bootstrap password supplied through Portainer secret; store password hash in PostgreSQL. Use secure HTTP-only session cookie, CSRF protection for browser mutations, and login rate limiting. Serve UI through TLS reverse proxy.
- One app container and one process. Same process hosts API/UI and supervises Kafka route workers. One running replica; do not launch multiple web workers. PostgreSQL and Kafka remain separate services.
- Bot token enters through admin CRUD, is encrypted in PostgreSQL with app key supplied through Portainer secret, and is never returned in full by API or logs. Token replacement is allowed; deletion is blocked while routes reference bot unless reassigned.
- Route CRUD supports create, list, inspect, update template/bot/chat/enabled state, and delete. Topic is immutable; changing topic means creating a new route, so start point stays clear. Delete archives route and retains failure history. A disabled route retains its offset and catches up when re-enabled. An edit applies current route configuration to records delivered afterward, including backlog.
- A Kafka record may trigger every enabled route on its topic. Routes have independent consumer groups and offsets. Group ID derives from stable route UUID.
- Templates use `{{field}}` and `{{nested.field}}` JSON lookups only; no loops, expressions, code, or HTML. Placeholder syntax and paths are checked when saved. Static template text uses Telegram MarkdownV2. Placeholders inside code spans or link targets are rejected; dynamic values elsewhere are stringified and MarkdownV2 escaped as literal text. Missing/null required fields fail that delivery. Empty rendered text and text over Telegram's 4096-character post-parse limit fail with visible error; no silent truncation.
- Admin can preview a template with sample JSON without sending. Optional explicit test-send action sends preview to configured chat, with clear confirmation in UI.
- Malformed JSON or missing fields create durable failure records and do not stop consumption. Permanent Telegram errors create failure records. Temporary errors use bounded exponential backoff; honor Telegram `retry_after` on 429. After retry budget, save rendered text for manual retry. Manual retry can duplicate a message if earlier send succeeded without acknowledgment.
- Failure records show route, topic, partition, offset, timestamp, failure category, and sanitized detail. Retain rendered text only when needed for manual send retry; do not store bot tokens. Set configurable retention, default 30 days. Manual retry reuses current bot token and destination of failed delivery unless route was deleted; deletion disables retry.
- Expect modest route count and message volume (initial sizing target: up to 20 routes and 10 messages/second total). Apply per-chat pacing, bounded queues, and backpressure. Load-test and revisit worker model if actual needs exceed this.

## Data and runtime design

1. PostgreSQL tables: `admin_user`, `bot` (name, encrypted token, token fingerprint/last digits, timestamps), `route` (UUID, topic, bot FK, chat ID, template, enabled, archived timestamp, timestamps), `route_start_offset` (route, partition, start offset), and `delivery_failure` (route/record coordinates, category, sanitized reason, optional rendered text, retry state, timestamps). Schema migrations run before app accepts traffic.
2. Route creation validates topic exists, bot exists, chat ID shape, and template syntax. Capture `endOffsets` for every current topic partition and persist route plus start offsets before returning success. Route becomes active only after durable baseline exists. New partition discovered later starts at its earliest offset because it did not exist at route creation. Surface activation failure without leaving a half-active route.
3. App supervisor runs one `KafkaConsumerComponent` per enabled route, with stable group ID tied to route UUID. Configure `record_handler` and `start_offset_resolver`; resolver returns saved route start offset for an existing partition or beginning offset for a partition added later. Component uses committed offset when present. Use default `auto_offset_reset="earliest"`: release 0.2.0 rejects `"none"` combined with resolver. Handler returns only after Telegram success or after a permanent/exhausted failure is durably recorded; default `SkipAndLogPolicy` must not see a transient dependency error. For database outage, handler keeps current record pending with backoff until persistence returns, so later offsets cannot commit past it. Reconcile CRUD changes without process restart. Shutdown drains an in-flight send when dependencies are available; if persistence remains down, container stop timeout terminates process and Kafka replays uncommitted record on restart. If retention removed an unread offset, surface data-loss alert, update stored start to earliest available, and restart route component. Supervisor monitors `get_status()` and restarts recoverable dead consumers.
4. Telegram transport sends `sendMessage(chat_id, text, parse_mode=MarkdownV2)`. Escape substituted values per Telegram rules. Limit each chat's rate; honor HTTP 429 `retry_after`; retry timeouts and 5xx; classify 4xx as permanent except 429. Never include token in exception strings or request logs.
5. Expose `/health/live`, `/health/ready`, and `/metrics`. Readiness checks PostgreSQL and Kafka reachability; UI shows route state, last consumed offset, and recent failures. Metrics count sends, failures, and retries and observe send latency without token, chat ID, or route ID labels. Structured logs include route ID and Kafka coordinates, with no secrets or full event payload.
6. Portainer deployment example supplies internal Kafka broker address (container-reachable advertised listener), PostgreSQL connection, encryption key, admin bootstrap secret, public base URL, persistent database managed by existing PostgreSQL, and restart policy. Document Telegram bot membership and permission to post in target group.

## TDD sequence

Write each failing test before its smallest implementation. Keep unit tests fast; use real PostgreSQL and Kafka in integration tests, with local fake Telegram HTTP server. No test sends to a real group.

1. **Template engine.** Tests: nested lookup, JSON scalar/stringification, literal MarkdownV2 escaping for every reserved character, placeholder rejected in code/link target context, missing/null path, malformed placeholder, empty output, and post-parse length boundary. Implement renderer and preview service.
2. **Bot CRUD and security.** Tests: unauthenticated mutation rejected; admin login/session/CSRF; create/list/update/delete; encrypted token storage; token masked in API/logs; referenced bot deletion blocked. Implement auth and bot API/UI.
3. **Route CRUD and activation.** Tests: topic validation, multiple routes sharing topic/bot, baseline offsets captured per partition, atomic activation, topic immutability, disable/enable, config reload, and preview. Implement route API/UI and supervisor reconciliation through component lifecycle.
4. **Kafka delivery.** Integration tests against `kafka-component==0.2.0`: old records skipped on new route; records arriving after route creation delivered; service restart catches up; each route gets independent delivery; raw malformed JSON includes coordinates; offset committed only after success or durable failure; temporary PostgreSQL outage cannot advance offset past failed record; newly added partition handled; rebalance does not rewind behind committed offsets; out-of-range start raises alert and resumes from earliest available after supervisor recovery. Implement route workers and metadata capture.
5. **Telegram errors.** Fake-server tests: MarkdownV2 payload, valid chat ID, 429 `retry_after`, timeout/5xx retries, permanent 4xx failure, retry exhaustion, per-chat pacing, and manual retry. Implement sender and failure UI/API.
6. **End-to-end deployment gate.** Compose Kafka + PostgreSQL + app + fake Telegram. Create bot and route through API, publish JSON, assert one rendered message, restart app, publish another, assert catch-up and no replay of pre-creation record. Verify component shutdown, health/metrics endpoints, and secret redaction.

## Acceptance criteria

- Admin can create a bot, create a route for an existing topic, preview MarkdownV2 output, and see successful sends in target Telegram group.
- Two topics and two routes operate concurrently; two routes on same topic each send to their own destination.
- Pre-creation records stay unsent; records produced after route creation survive app downtime and are sent after restart.
- Bad JSON, missing fields, and Telegram errors appear in UI without stopping later valid records.
- Tokens remain encrypted at rest and masked in API, UI, and logs. CRUD requires admin authentication.
- Automated unit/integration/end-to-end suite passes in CI; Portainer deployment instructions permit running against internal Kafka and existing PostgreSQL.

## Known delivery limit

Kafka offset commit and Telegram send cannot share one transaction. Crash after Telegram accepts send but before offset commit may resend same message. This matches accepted duplicate policy. Failure record writes and Kafka commits also have a crash gap; repeated error records should be deduplicated by route/topic/partition/offset/category.

Kafka retention bounds catch-up. If downtime exceeds retention, expired records cannot be recovered by this service; report gap to admin. Also, route creation uses Kafka's current partition end offsets as activation boundary, so records produced during creation after offset capture are included.

## References

- [Telegram Bot API `sendMessage`](https://core.telegram.org/bots/api#sendmessage), [MarkdownV2](https://core.telegram.org/bots/api#markdownv2-style), [retry parameters](https://core.telegram.org/bots/api#responseparameters), [bot limits FAQ](https://core.telegram.org/bots/faq)
- [Apache Kafka consumer API](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)
- [kafka-component 0.2.0](https://pypi.org/project/kafka-component/), [aiokafka consumer](https://aiokafka.readthedocs.io/en/stable/consumer.html), [SQLAlchemy 2](https://docs.sqlalchemy.org/en/20/orm/quickstart.html), [Alembic](https://alembic.sqlalchemy.org/en/latest/), [HTTPX](https://www.python-httpx.org/)
