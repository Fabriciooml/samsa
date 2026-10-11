"""Telegram Bot API transport with bounded retries and per-chat pacing."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

import httpx

from telegram_kafka.metrics import RETRIES


class SendError(Exception):
    pass


class PermanentSendError(SendError):
    pass


class TransientSendError(SendError):
    pass


class TelegramSender:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        base_url: str = "https://api.telegram.org",
        chat_interval: float = 3.0,
        max_attempts: int = 5,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.client = client
        self.base_url = base_url.rstrip("/")
        self.chat_interval = chat_interval
        self.max_attempts = max_attempts
        self.sleep = sleep
        self._lock = asyncio.Lock()
        self._next_chat_send: dict[int, float] = {}

    async def _pace(self, chat_id: int) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._next_chat_send.get(chat_id, now) - now)
            self._next_chat_send[chat_id] = max(now, self._next_chat_send.get(chat_id, now)) + self.chat_interval
        if wait:
            await self.sleep(wait)

    async def send(self, token: str, chat_id: int, text: str) -> None:
        for attempt in range(1, self.max_attempts + 1):
            await self._pace(chat_id)
            try:
                response = await self.client.post(
                    f"{self.base_url}/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": text, "parse_mode": "MarkdownV2"},
                )
            except httpx.RequestError:
                if attempt == self.max_attempts:
                    raise TransientSendError("Telegram request failed after retries") from None
                RETRIES.labels(reason="network").inc()
                await self.sleep(min(2 ** (attempt - 1), 30))
                continue
            try:
                payload = response.json()
            except ValueError:
                payload = {}
            if response.status_code == 200 and payload.get("ok") is True:
                return
            if response.status_code == 429 or response.status_code >= 500:
                if attempt == self.max_attempts:
                    raise TransientSendError(
                        f"Telegram unavailable after {attempt} attempts (HTTP {response.status_code})"
                    )
                retry_after = payload.get("parameters", {}).get("retry_after")
                delay = retry_after if isinstance(retry_after, (int, float)) else min(2 ** (attempt - 1), 30)
                RETRIES.labels(reason="rate_limit" if response.status_code == 429 else "server").inc()
                await self.sleep(max(float(delay), 0))
                continue
            raise PermanentSendError(f"Telegram rejected message (HTTP {response.status_code})")
