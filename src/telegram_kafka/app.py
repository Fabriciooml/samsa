from __future__ import annotations

import hmac
import asyncio
import os
import re
import secrets
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from argon2 import PasswordHasher
from aiokafka.admin import AIOKafkaAdminClient
from cryptography.fernet import Fernet
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from fastapi_component import system_lifespan
from postgres_component import PostgresComponent
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ConfigDict, Field
from python_components import System
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from starlette.middleware.sessions import SessionMiddleware

from telegram_kafka.models import AdminUser, Bot, DeliveryFailure, Route, RouteStartOffset
from telegram_kafka.offsets import KafkaOffsetProvider
from telegram_kafka.supervisor import RouteSupervisor
from telegram_kafka.telegram import PermanentSendError, TransientSendError
from telegram_kafka.template import InvalidTemplate, render_template, validate_template


@dataclass(frozen=True)
class Settings:
    database_url: str
    kafka_bootstrap: str
    admin_password: str
    encryption_key: str
    session_secret: str
    start_workers: bool = True
    secure_cookies: bool = True
    telegram_base_url: str = "https://api.telegram.org"

    @classmethod
    def from_env(cls) -> Settings:
        def secret(name: str) -> str:
            value = os.getenv(name)
            path = os.getenv(name + "_FILE")
            if value and path:
                raise ValueError(f"Set only one of {name} and {name}_FILE")
            if path:
                return Path(path).read_text().rstrip("\r\n")
            if value:
                return value
            raise ValueError(f"Missing {name} or {name}_FILE")

        return cls(
            database_url=secret("DATABASE_URL"),
            kafka_bootstrap=os.environ["KAFKA_BOOTSTRAP"],
            admin_password=secret("ADMIN_BOOTSTRAP_PASSWORD"),
            encryption_key=secret("TOKEN_ENCRYPTION_KEY"),
            session_secret=secret("SESSION_SECRET"),
            secure_cookies=os.getenv("SECURE_COOKIES", "true").lower() == "true",
            telegram_base_url=os.getenv("TELEGRAM_API_BASE", "https://api.telegram.org"),
        )


class LoginIn(BaseModel):
    password: str


class BotIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=120)
    token: str = Field(min_length=8)


class BotUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str | None = Field(default=None, min_length=1, max_length=120)
    token: str | None = Field(default=None, min_length=8)


class RouteIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    topic: str = Field(min_length=1, max_length=249)
    bot_id: str
    chat_id: int
    template: str = Field(min_length=1)


class RouteUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    bot_id: str | None = None
    chat_id: int | None = None
    template: str | None = Field(default=None, min_length=1)
    enabled: bool | None = None


class PreviewIn(BaseModel):
    message: dict


def create_app(settings: Settings | None = None, *, offset_provider=None, telegram_sender=None) -> FastAPI:
    settings = settings or Settings.from_env()
    cipher = Fernet(settings.encryption_key.encode())
    hasher = PasswordHasher()
    database = PostgresComponent(url=settings.database_url)
    offset_provider = offset_provider or KafkaOffsetProvider(settings.kafka_bootstrap)
    supervisor = (
        RouteSupervisor(
            database, cipher, settings.kafka_bootstrap,
            sender=telegram_sender, telegram_base_url=settings.telegram_base_url,
        )
        if settings.start_workers
        else None
    )
    components = {"database": database}
    if supervisor is not None:
        components["supervisor"] = supervisor
    system = System(components)

    @asynccontextmanager
    async def bootstrap(_: FastAPI):
        async with database.session() as session:
            user = await session.scalar(select(AdminUser).limit(1))
            if user is None:
                session.add(AdminUser(password_hash=hasher.hash(settings.admin_password)))
        yield

    app = FastAPI(lifespan=system_lifespan(system, wrap=bootstrap))
    app.add_middleware(
        SessionMiddleware,
        secret_key=settings.session_secret,
        same_site="strict",
        https_only=settings.secure_cookies,
    )
    app.state.database = database
    app.state.cipher = cipher
    app.state.supervisor = supervisor
    app.state.telegram_sender = telegram_sender
    templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
    failed_logins: dict[str, list[float]] = {}

    def active_sender():
        sender = telegram_sender or (supervisor.sender if supervisor is not None else None)
        if sender is None:
            raise HTTPException(503, "Telegram sender is not ready")
        return sender

    async def require_admin(request: Request) -> None:
        if request.session.get("admin") is not True:
            raise HTTPException(401, "Admin login required")

    async def require_csrf(request: Request, _: None = Depends(require_admin)) -> None:
        expected = request.session.get("csrf")
        supplied = request.headers.get("X-CSRF-Token")
        if not expected or not supplied or not hmac.compare_digest(expected, supplied):
            raise HTTPException(403, "Invalid CSRF token")

    @app.post("/api/login")
    async def login(body: LoginIn, request: Request):
        import time

        address = request.client.host if request.client else "unknown"
        now = time.monotonic()
        recent = [attempt for attempt in failed_logins.get(address, []) if now - attempt < 60]
        if len(recent) >= 5:
            raise HTTPException(429, "Too many login attempts")
        async with database.session() as session:
            user = await session.scalar(select(AdminUser).limit(1))
        try:
            valid = user is not None and hasher.verify(user.password_hash, body.password)
        except Exception:
            valid = False
        if not valid:
            recent.append(now)
            failed_logins[address] = recent
            raise HTTPException(401, "Invalid credentials")
        failed_logins.pop(address, None)
        request.session.clear()
        request.session["admin"] = True
        request.session["csrf"] = secrets.token_urlsafe(32)
        return {"csrf_token": request.session["csrf"]}

    @app.post("/api/logout", dependencies=[Depends(require_csrf)])
    async def logout(request: Request):
        request.session.clear()
        return {"ok": True}

    @app.get("/api/me", dependencies=[Depends(require_admin)])
    async def me(request: Request):
        return {"csrf_token": request.session["csrf"]}

    @app.get("/", response_class=HTMLResponse)
    async def home(request: Request):
        return templates.TemplateResponse(request, "console.html")

    @app.get("/health/live")
    async def live():
        return {"status": "alive"}

    @app.get("/health/ready")
    async def ready():
        try:
            async with database.session() as session:
                await session.execute(text("SELECT 1"))
        except Exception:
            return Response('{"status":"unready","reason":"database"}', 503, media_type="application/json")
        if supervisor is not None:
            worker_status = supervisor.status()
            if worker_status["last_error"] is not None or any(
                not worker["connected"] or worker["last_error"]
                for worker in worker_status["workers"].values()
            ):
                return Response('{"status":"unready","reason":"worker"}', 503, media_type="application/json")
            admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap)
            try:
                await asyncio.wait_for(admin.start(), timeout=3)
            except Exception:
                return Response('{"status":"unready","reason":"kafka"}', 503, media_type="application/json")
            finally:
                await admin.close()
        return {"status": "ready"}

    @app.get("/metrics")
    async def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/api/status", dependencies=[Depends(require_admin)])
    async def status():
        return supervisor.status() if supervisor is not None else {"workers": {}, "last_error": None}

    def bot_view(bot: Bot) -> dict[str, str]:
        return {"id": bot.id, "name": bot.name, "token_hint": bot.token_hint}

    @app.get("/api/bots", dependencies=[Depends(require_admin)])
    async def list_bots():
        async with database.session() as session:
            bots = (
                await session.scalars(
                    select(Bot).where(Bot.archived_at.is_(None)).order_by(Bot.name)
                )
            ).all()
            return [bot_view(bot) for bot in bots]

    @app.post("/api/bots", status_code=201, dependencies=[Depends(require_csrf)])
    async def create_bot(body: BotIn):
        if not re.fullmatch(r"\d+:[A-Za-z0-9_-]+", body.token):
            raise HTTPException(422, "Invalid Telegram bot token shape")
        bot = Bot(
            name=body.name,
            token_encrypted=cipher.encrypt(body.token.encode()).decode(),
            token_hint="****" + body.token[-4:],
        )
        try:
            async with database.session() as session:
                session.add(bot)
                await session.flush()
                view = bot_view(bot)
        except IntegrityError:
            raise HTTPException(409, "Bot name already exists") from None
        return view

    @app.patch("/api/bots/{bot_id}", dependencies=[Depends(require_csrf)])
    async def update_bot(bot_id: str, body: BotUpdate):
        try:
            async with database.session() as session:
                bot = await session.get(Bot, bot_id)
                if bot is None or bot.archived_at is not None:
                    raise HTTPException(404, "Bot not found")
                if body.name is not None:
                    bot.name = body.name
                if body.token is not None:
                    if not re.fullmatch(r"\d+:[A-Za-z0-9_-]+", body.token):
                        raise HTTPException(422, "Invalid Telegram bot token shape")
                    bot.token_encrypted = cipher.encrypt(body.token.encode()).decode()
                    bot.token_hint = "****" + body.token[-4:]
                await session.flush()
                return bot_view(bot)
        except IntegrityError:
            raise HTTPException(409, "Bot name already exists") from None

    @app.delete("/api/bots/{bot_id}", status_code=204, dependencies=[Depends(require_csrf)])
    async def delete_bot(bot_id: str):
        async with database.session() as session:
            bot = await session.get(Bot, bot_id)
            if bot is None or bot.archived_at is not None:
                raise HTTPException(404, "Bot not found")
            route = await session.scalar(
                select(Route.id)
                .where(Route.bot_id == bot_id, Route.archived_at.is_(None))
                .limit(1)
            )
            if route is not None:
                raise HTTPException(409, "Bot is used by a route")
            bot.archived_at = datetime.now(timezone.utc)

    async def route_view(session, route: Route) -> dict:
        offsets = (
            await session.scalars(
                select(RouteStartOffset).where(RouteStartOffset.route_id == route.id)
            )
        ).all()
        return {
            "id": route.id,
            "topic": route.topic,
            "bot_id": route.bot_id,
            "chat_id": route.chat_id,
            "template": route.template,
            "enabled": route.enabled,
            "start_offsets": {str(item.partition): item.start_offset for item in offsets},
        }

    @app.get("/api/routes", dependencies=[Depends(require_admin)])
    async def list_routes():
        async with database.session() as session:
            routes = (
                await session.scalars(
                    select(Route).where(Route.archived_at.is_(None)).order_by(Route.created_at)
                )
            ).all()
            return [await route_view(session, route) for route in routes]

    @app.post("/api/routes", status_code=201, dependencies=[Depends(require_csrf)])
    async def create_route(body: RouteIn):
        try:
            validate_template(body.template)
            starts = await offset_provider.capture(body.topic)
        except (InvalidTemplate, ValueError) as exc:
            raise HTTPException(422, str(exc)) from None
        async with database.session() as session:
            if await session.get(Bot, body.bot_id) is None:
                raise HTTPException(422, "Bot does not exist")
            route = Route(
                topic=body.topic,
                bot_id=body.bot_id,
                chat_id=body.chat_id,
                template=body.template,
            )
            session.add(route)
            await session.flush()
            session.add_all(
                RouteStartOffset(route_id=route.id, partition=partition, start_offset=offset)
                for partition, offset in starts.items()
            )
            await session.flush()
            view = await route_view(session, route)
        if supervisor is not None:
            await supervisor.refresh()
        return view

    @app.patch("/api/routes/{route_id}", dependencies=[Depends(require_csrf)])
    async def update_route(route_id: str, body: RouteUpdate):
        async with database.session() as session:
            route = await session.get(Route, route_id)
            if route is None or route.archived_at is not None:
                raise HTTPException(404, "Route not found")
            if body.bot_id is not None:
                bot = await session.get(Bot, body.bot_id)
                if bot is None or bot.archived_at is not None:
                    raise HTTPException(422, "Bot does not exist")
                route.bot_id = body.bot_id
            if body.chat_id is not None:
                route.chat_id = body.chat_id
            if body.template is not None:
                try:
                    validate_template(body.template)
                except InvalidTemplate as exc:
                    raise HTTPException(422, str(exc)) from None
                route.template = body.template
            if body.enabled is not None:
                route.enabled = body.enabled
            await session.flush()
            view = await route_view(session, route)
        if supervisor is not None:
            await supervisor.refresh()
        return view

    @app.delete("/api/routes/{route_id}", status_code=204, dependencies=[Depends(require_csrf)])
    async def delete_route(route_id: str):
        async with database.session() as session:
            route = await session.get(Route, route_id)
            if route is None or route.archived_at is not None:
                raise HTTPException(404, "Route not found")
            route.enabled = False
            route.archived_at = datetime.now(timezone.utc)
        if supervisor is not None:
            await supervisor.refresh()

    @app.post("/api/routes/{route_id}/preview", dependencies=[Depends(require_csrf)])
    async def preview_route(route_id: str, body: PreviewIn):
        async with database.session() as session:
            route = await session.get(Route, route_id)
            if route is None or route.archived_at is not None:
                raise HTTPException(404, "Route not found")
            try:
                return {"text": render_template(route.template, body.message)}
            except InvalidTemplate as exc:
                raise HTTPException(422, str(exc)) from None

    @app.post("/api/routes/{route_id}/test", dependencies=[Depends(require_csrf)])
    async def test_send(route_id: str, body: PreviewIn):
        async with database.session() as session:
            route = await session.get(Route, route_id)
            if route is None or route.archived_at is not None:
                raise HTTPException(404, "Route not found")
            bot = await session.get(Bot, route.bot_id)
            try:
                text = render_template(route.template, body.message)
            except InvalidTemplate as exc:
                raise HTTPException(422, str(exc)) from None
            token = cipher.decrypt(bot.token_encrypted.encode()).decode()
            chat_id = route.chat_id
        try:
            await active_sender().send(token, chat_id, text)
        except PermanentSendError:
            raise HTTPException(422, "Telegram rejected test message") from None
        except TransientSendError:
            raise HTTPException(503, "Telegram unavailable after retries") from None
        return {"sent": True, "text": text}

    @app.get("/api/failures", dependencies=[Depends(require_admin)])
    async def list_failures():
        async with database.session() as session:
            failures = (
                await session.scalars(
                    select(DeliveryFailure)
                    .order_by(DeliveryFailure.created_at.desc())
                    .limit(100)
                )
            ).all()
            return [
                {
                    "id": failure.id,
                    "route_id": failure.route_id,
                    "topic": failure.topic,
                    "partition": failure.partition,
                    "offset": failure.offset,
                    "category": failure.category,
                    "detail": failure.detail,
                    "retry_state": failure.retry_state,
                    "created_at": failure.created_at.isoformat(),
                }
                for failure in failures
            ]

    @app.post("/api/failures/{failure_id}/retry", dependencies=[Depends(require_csrf)])
    async def retry_failure(failure_id: str):
        async with database.session() as session:
            failure = await session.get(DeliveryFailure, failure_id)
            if failure is None:
                raise HTTPException(404, "Failure not found")
            if failure.retry_state != "open" or failure.rendered_text is None:
                raise HTTPException(409, "Failure cannot be retried")
            route = await session.get(Route, failure.route_id)
            if route is None or route.archived_at is not None:
                raise HTTPException(409, "Route no longer active")
            bot = await session.get(Bot, route.bot_id)
            if bot is None or bot.archived_at is not None:
                raise HTTPException(409, "Bot no longer active")
            token = cipher.decrypt(bot.token_encrypted.encode()).decode()
            chat_id, text = route.chat_id, failure.rendered_text
        try:
            await active_sender().send(token, chat_id, text)
        except PermanentSendError:
            raise HTTPException(422, "Telegram rejected retry") from None
        except TransientSendError:
            raise HTTPException(503, "Telegram unavailable after retries") from None
        async with database.session() as session:
            failure = await session.get(DeliveryFailure, failure_id)
            failure.retry_state = "sent"
        return {"retry_state": "sent"}

    return app
