"""Durable queue and authenticated, at-least-once Bot handoff."""

import asyncio
import logging

import aiosqlite
import httpx

from kcpc_daemon.config import BotConfig
from kcpc_daemon.events import NormalizedEvent

logger = logging.getLogger(__name__)


async def initialize(database: aiosqlite.Connection) -> None:
    await database.execute(
        "CREATE TABLE IF NOT EXISTS feed_cursors (feed_id TEXT PRIMARY KEY, token TEXT NOT NULL)"
    )
    await database.execute(
        "CREATE TABLE IF NOT EXISTS outbox (event_id TEXT PRIMARY KEY, community TEXT NOT NULL, payload TEXT NOT NULL)"
    )
    await database.commit()


async def record_event(
    database: aiosqlite.Connection,
    lock: asyncio.Lock,
    feed_id: str,
    token: str,
    event: NormalizedEvent | None,
    community: str | None,
) -> None:
    async with lock:
        try:
            if event is not None and community is not None:
                await database.execute(
                    "INSERT OR IGNORE INTO outbox (event_id, community, payload) VALUES (?, ?, ?)",
                    (event.event_id, community, event.model_dump_json()),
                )
            await database.execute(
                "INSERT INTO feed_cursors (feed_id, token) VALUES (?, ?) "
                "ON CONFLICT(feed_id) DO UPDATE SET token = excluded.token",
                (feed_id, token),
            )
            await database.commit()
        except BaseException:
            await database.rollback()
            raise


async def deliver(
    database: aiosqlite.Connection, lock: asyncio.Lock, bot: BotConfig
) -> None:
    delay = 1.0
    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            async with (
                lock,
                database.execute(
                    "SELECT event_id, community, payload FROM outbox ORDER BY rowid LIMIT 1"
                ) as cursor,
            ):
                row = await cursor.fetchone()
            if row is None:
                await asyncio.sleep(1)
                continue
            event_id, community, payload = row
            try:
                response = await client.post(
                    str(bot.url),
                    content=payload,
                    headers={
                        "Authorization": f"Bearer {bot.token.get_secret_value()}",
                        "Idempotency-Key": event_id,
                        "X-Community-Key": community,
                        "Content-Type": "application/json",
                    },
                )
                response.raise_for_status()
            except httpx.HTTPError as exc:
                logger.warning(
                    "Bot delivery failed event_id=%s community=%s error=%s",
                    event_id,
                    community,
                    type(exc).__name__,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60.0)
                continue
            async with lock:
                await database.execute(
                    "DELETE FROM outbox WHERE event_id = ?", (event_id,)
                )
                await database.commit()
            logger.info(
                "Bot delivery succeeded event_id=%s community=%s", event_id, community
            )
            delay = 1.0
