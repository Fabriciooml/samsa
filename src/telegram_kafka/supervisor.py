"""Reconcile saved routes with active Kafka consumers."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone

import httpx
from cryptography.fernet import Fernet
from postgres_component import PostgresComponent
from python_components import Component
from sqlalchemy import delete, select

from telegram_kafka.models import DeliveryFailure, Route, RouteStartOffset
from telegram_kafka.store import SQLDeliveryStore
from telegram_kafka.telegram import TelegramSender
from telegram_kafka.worker import RouteWorker

logger = logging.getLogger(__name__)


class RouteSupervisor(Component):
    def __init__(
        self,
        database: PostgresComponent,
        cipher: Fernet,
        broker: str,
        *,
        sender=None,
        telegram_base_url: str = "https://api.telegram.org",
    ):
        self.using(["database"])
        self.database = database
        self.cipher = cipher
        self.broker = broker
        self.sender = sender
        self.telegram_base_url = telegram_base_url
        self._client: httpx.AsyncClient | None = None
        self._workers: dict[str, RouteWorker] = {}
        self._lock = asyncio.Lock()
        self._stopping = asyncio.Event()
        self._loop_task: asyncio.Task | None = None
        self.last_error: str | None = None
        self._next_cleanup = 0.0

    async def start(self) -> None:
        self._stopping = asyncio.Event()
        if self.sender is None:
            self._client = httpx.AsyncClient(timeout=15)
            self.sender = TelegramSender(self._client, base_url=self.telegram_base_url)
        await self.refresh()
        self._loop_task = asyncio.create_task(self._loop())

    async def _loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=3)
            except TimeoutError:
                await self.refresh()

    async def refresh(self) -> None:
        async with self._lock:
            try:
                async with self.database.session() as session:
                    routes = (
                        await session.scalars(
                            select(Route).where(
                                Route.archived_at.is_(None), Route.enabled.is_(True)
                            )
                        )
                    ).all()
                    wanted = {}
                    for route in routes:
                        offsets = (
                            await session.scalars(
                                select(RouteStartOffset).where(
                                    RouteStartOffset.route_id == route.id
                                )
                            )
                        ).all()
                        wanted[route.id] = (
                            route.topic,
                            {item.partition: item.start_offset for item in offsets},
                        )
            except Exception as exc:
                self.last_error = type(exc).__name__
                logger.error("Route reconciliation database error: %s", type(exc).__name__)
                return
            if time.monotonic() >= self._next_cleanup:
                try:
                    async with self.database.session() as session:
                        await session.execute(
                            delete(DeliveryFailure).where(
                                DeliveryFailure.created_at
                                < datetime.now(timezone.utc) - timedelta(days=30)
                            )
                        )
                    self._next_cleanup = time.monotonic() + 86400
                except Exception as exc:
                    logger.error("Failure retention cleanup failed: %s", type(exc).__name__)
            for route_id, worker in list(self._workers.items()):
                if route_id not in wanted or not worker.status()["connected"]:
                    try:
                        await worker.stop()
                    except Exception as exc:
                        logger.error("Route worker stop failed: %s %s", route_id, type(exc).__name__)
                    self._workers.pop(route_id, None)
            store = SQLDeliveryStore(self.database, self.cipher)
            for route_id, (topic, starts) in wanted.items():
                if route_id in self._workers:
                    continue
                worker = RouteWorker(route_id, topic, self.broker, starts, store, self.sender)
                try:
                    await worker.start()
                except Exception as exc:
                    self.last_error = type(exc).__name__
                    logger.error("Route worker start failed: %s %s", route_id, type(exc).__name__)
                    continue
                self._workers[route_id] = worker
            if len(self._workers) == len(wanted):
                self.last_error = None

    async def shutdown(self) -> None:
        self._stopping.set()
        if self._loop_task is not None:
            await self._loop_task
        async with self._lock:
            for worker in list(self._workers.values()):
                await worker.stop()
            self._workers.clear()
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def status(self) -> dict:
        return {
            "workers": {route_id: worker.status() for route_id, worker in self._workers.items()},
            "last_error": self.last_error,
        }
