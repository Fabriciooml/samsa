# Samsa

![Black-and-white illustration of a giant insect lying in a narrow bed](assets/samsa-bed.webp)

In Franz Kafka's [*The Metamorphosis*](https://www.gutenberg.org/files/5200/5200-h/5200-h.htm), Gregor Samsa remains alert to his family's needs, but the words he speaks through his closed bedroom door become unintelligible to them. Something urgent is happening; those outside can hear him but cannot understand him. Samsa takes its name from that distance between a signal and its meaning. A Kafka record can hold an important event without telling the right people what happened. Routes select an audience, templates give the event context, and saved offsets carry it across downtime until it reaches a Telegram group.

Samsa is a configurable Kafka-to-Telegram notifier. One deployment connects to one Kafka cluster. Admins save BotFather tokens and create routes through a browser UI. New routes start at each partition's current end and catch up after downtime while Kafka retains the records.

## Run locally

Requires Python 3.13, Kafka, and PostgreSQL. Install dependencies with `uv sync --extra test`.

Set `DATABASE_URL=postgresql+asyncpg://user:password@host:5432/database`, `KAFKA_BOOTSTRAP=broker:9092`, `ADMIN_BOOTSTRAP_PASSWORD`, `TOKEN_ENCRYPTION_KEY`, and `SESSION_SECRET`. Generate encryption key with:

```sh
python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
```

Run migrations, then server:

```sh
uv run alembic upgrade head
uv run uvicorn telegram_kafka.app:create_app --factory --host 127.0.0.1 --port 8000
```

For local HTTP only, set `SECURE_COOKIES=false`. Keep default `true` behind HTTPS in production. Open `http://127.0.0.1:8000` and sign in with bootstrap password. Add bot, then route. Bot must be member of target Telegram group and allowed to post. Chat ID is signed numeric ID, often beginning `-100` for supergroups.

## Deploy using only Portainer UI (Docker Standalone)

1. In **Images → Build a new image**, name image `samsa:portainer`. If Portainer can fetch this source repository, select **URL** and enter repository URL with Dockerfile path `Dockerfile`. Otherwise select **Web editor**, paste [Dockerfile.portainer](Dockerfile.portainer), use **Select files** to upload [samsa-source.tar.gz](samsa-source.tar.gz), then click **Build the image**. This bundle contains application source and migrations; it contains no credentials. The repository URL method needs a repository URL that Portainer's image builder can access. For later updates using upload method, regenerate bundle with `scripts/make_portainer_bundle.py` during development and repeat image build in Portainer.
2. In **Stacks → Add stack → Web editor**, name stack `samsa` and paste [compose.portainer.standalone.yml](compose.portainer.standalone.yml). In **Environment variables**, set:

   | Variable | Value |
   | --- | --- |
   | `DATABASE_URL` | Existing PostgreSQL URL using its container name or network alias on `POSTGRES_NETWORK`, for example `postgresql+asyncpg://user:password@postgres:5432/samsa`. URL-encode special characters in password. |
   | `KAFKA_BOOTSTRAP` | Kafka's **container-reachable advertised listener** on `KAFKA_NETWORK`, for example `kafka:9092`; never `localhost`. |
   | `ADMIN_BOOTSTRAP_PASSWORD` | Strong password you choose for app login. No default password. |
   | `KAFKA_NETWORK` | Exact Docker network name already attached to Kafka. |
   | `POSTGRES_NETWORK` | Exact Docker network name already attached to PostgreSQL. |
   | `UI_BIND` | Host's private LAN address for direct access, or `127.0.0.1` when using a reverse proxy on the host. Default: `127.0.0.1`. |
   | `UI_PORT` | Host port. Default: `8765`. |
   | `SECURE_COOKIES` | `false` for direct HTTP access; `true` when accessed through HTTPS. Default: `false`. |
   | `SAMSA_KEYS_VOLUME` | Optional persistent key-volume name. Default keeps existing `telegram-kafka-notifier-keys` volume. |

3. Click **Deploy the stack**. Open `http://<UI_BIND>:<UI_PORT>` for direct HTTP access, or your HTTPS proxy URL. Sign in with `ADMIN_BOOTSTRAP_PASSWORD`, then add Bot and Route records. Bot must belong to target Telegram group and have permission to post.

Find exact network names under each container's **Networks** section in Portainer. Samsa joins both networks; Kafka and PostgreSQL can remain on separate networks. Portainer's Docker Standalone environment has no **Secrets** menu, so stack variables including database and admin passwords remain visible to Portainer administrators and through Docker container inspection. Restrict Portainer access and host port to trusted users/network. For HTTPS access, terminate TLS with your reverse proxy and set `SECURE_COOKIES=true`.

First startup generates token encryption key and session secret in named volume `telegram-kafka-notifier-keys`. Volume name stays stable across app renames so saved bot tokens remain readable. Keep that volume across stack updates; losing it makes saved bot tokens unreadable. `ADMIN_BOOTSTRAP_PASSWORD` initializes the admin account only when database has no admin row. Back up PostgreSQL and keys volume. Kafka retention determines maximum recoverable downtime. Run one app replica and one Uvicorn worker.

Docker Swarm users can instead use [compose.portainer.yml](compose.portainer.yml) and create four Docker secrets through Portainer's **Secrets** UI.

## Behavior

- Template syntax: `{{field}}`, `{{nested.field}}`; static text may use Telegram MarkdownV2. Inserted JSON values are escaped as literal MarkdownV2 text.
- Each enabled route has its own Kafka consumer group. Duplicate Telegram sends can occur if process stops after Telegram accepts send but before Kafka offset commit.
- Malformed JSON, missing template fields, and exhausted Telegram errors appear under Failures. Telegram send failures with rendered text may be manually retried.
- `/health/live`, `/health/ready`, and `/metrics` support operations. Keep UI and metrics behind trusted reverse proxy.

## Tests

```sh
uv run pytest -q
```

Integration tests start Kafka and PostgreSQL with Docker testcontainers.
