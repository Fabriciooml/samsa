"""PostgreSQL adapter for Kafka delivery records."""

from __future__ import annotations

import asyncio

from cryptography.fernet import Fernet
from postgres_component import PostgresComponent
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from telegram_kafka.delivery import DeliveryRoute
from telegram_kafka.models import Bot, DeliveryFailure, Route


class SQLDeliveryStore:
    def __init__(self, database: PostgresComponent, cipher: Fernet):
        self.database = database
        self.cipher = cipher

    async def load_route(self, route_id: str) -> DeliveryRoute:
        delay = 1
        while True:
            try:
                async with self.database.session() as session:
                    route = await session.get(Route, route_id)
                    if route is None:
                        raise LookupError(f"Route {route_id} no longer exists")
                    bot = await session.get(Bot, route.bot_id)
                    if bot is None:
                        raise LookupError(f"Bot for route {route_id} no longer exists")
                    return DeliveryRoute(
                        id=route.id,
                        topic=route.topic,
                        chat_id=route.chat_id,
                        template=route.template,
                        bot_token=self.cipher.decrypt(bot.token_encrypted.encode()).decode(),
                    )
            except (SQLAlchemyError, OSError):
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def record_failure(self, **details) -> None:
        delay = 1
        while True:
            try:
                async with self.database.session() as session:
                    statement = (
                        insert(DeliveryFailure)
                        .values(**details)
                        .on_conflict_do_nothing(
                            index_elements=[
                                "route_id", "topic", "partition", "offset", "category"
                            ]
                        )
                    )
                    await session.execute(statement)
                return
            except (SQLAlchemyError, OSError):
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
