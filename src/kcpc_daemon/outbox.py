"""Durable queue and authenticated, at-least-once Bot handoff."""

import asyncio
import hashlib
import json
import logging
import time

import aiosqlite
import httpx

from kcpc_daemon.config import BotConfig
from kcpc_daemon.events import TELEMETRY_TYPES, NormalizedEvent
from kcpc_daemon.monitor import Monitor

logger = logging.getLogger(__name__)


async def initialize(database: aiosqlite.Connection) -> None:
    await database.execute(
        "CREATE TABLE IF NOT EXISTS feed_cursors (feed_id TEXT PRIMARY KEY, token TEXT NOT NULL)"
    )
    await database.execute(
        "CREATE TABLE IF NOT EXISTS outbox (event_id TEXT PRIMARY KEY, community TEXT NOT NULL, payload TEXT NOT NULL)"
    )
    async with database.execute("PRAGMA table_info(outbox)") as cursor:
        columns = {row[1] for row in await cursor.fetchall()}
    if "available_at" not in columns:
        await database.execute(
            "ALTER TABLE outbox ADD COLUMN available_at REAL NOT NULL DEFAULT 0"
        )
    if "aggregate_key" not in columns:
        await database.execute("ALTER TABLE outbox ADD COLUMN aggregate_key TEXT")
    async with database.execute("PRAGMA table_info(outbox)") as cursor:
        schema = await cursor.fetchall()
    if not any(row[1] == "delivery_key" for row in schema):
        await database.execute("BEGIN")
        try:
            await database.execute("ALTER TABLE outbox RENAME TO old_outbox")
            await database.execute(
                "CREATE TABLE outbox (event_id TEXT NOT NULL, community TEXT NOT NULL, payload TEXT NOT NULL, available_at REAL NOT NULL DEFAULT 0, aggregate_key TEXT, delivery_key TEXT NOT NULL, PRIMARY KEY(event_id, community))"
            )
            await database.execute(
                "INSERT INTO outbox SELECT event_id, community, payload, available_at, aggregate_key, event_id FROM old_outbox"
            )
            await database.execute("DROP TABLE old_outbox")
            await database.commit()
        except BaseException:
            await database.rollback()
            raise
    await database.commit()


def delivery_key(event_id: str, community: str) -> str:
    return hashlib.sha256(
        json.dumps([event_id, community], ensure_ascii=False).encode()
    ).hexdigest()


async def record_event(
    database: aiosqlite.Connection,
    lock: asyncio.Lock,
    feed_id: str,
    token: str,
    event: NormalizedEvent | None,
    communities: list[str],
    aggregation_seconds: int = 60,
) -> None:
    async with lock:
        try:
            for community in communities if event is not None else []:
                assert event is not None
                if event.source == "bachelier" and event.type in TELEMETRY_TYPES:
                    now = time.time()
                    key = json.dumps([feed_id, community, event.type])
                    async with database.execute(
                        "SELECT event_id, payload FROM outbox WHERE aggregate_key = ? AND available_at > ? ORDER BY rowid LIMIT 1",
                        (key, now),
                    ) as cursor:
                        existing = await cursor.fetchone()
                    if existing:
                        snapshot = NormalizedEvent.model_validate_json(existing[1])
                        assert snapshot.data is not None
                        items = snapshot.data["items"]
                        assert isinstance(items, dict)
                        items[event.object_id] = event.data
                        snapshot.data["count"] = int(snapshot.data["count"]) + 1  # type: ignore[call-overload]
                        await database.execute(
                            "UPDATE outbox SET payload = ? WHERE event_id = ? AND community = ?",
                            (snapshot.model_dump_json(), existing[0], community),
                        )
                    else:
                        snapshot = event.model_copy(
                            update={
                                "data": {
                                    "count": 1,
                                    "items": {event.object_id: event.data},
                                }
                            }
                        )
                        await database.execute(
                            "INSERT OR IGNORE INTO outbox (event_id, community, payload, available_at, aggregate_key, delivery_key) VALUES (?, ?, ?, ?, ?, ?)",
                            (
                                event.event_id,
                                community,
                                snapshot.model_dump_json(),
                                now + aggregation_seconds,
                                key,
                                delivery_key(event.event_id, community),
                            ),
                        )
                else:
                    await database.execute(
                        "INSERT OR IGNORE INTO outbox (event_id, community, payload, delivery_key) VALUES (?, ?, ?, ?)",
                        (
                            event.event_id,
                            community,
                            event.model_dump_json(),
                            delivery_key(event.event_id, community),
                        ),
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
    database: aiosqlite.Connection,
    lock: asyncio.Lock,
    bot: BotConfig,
    monitor: Monitor | None = None,
) -> None:
    retries: dict[str, float] = {}
    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            async with (
                lock,
                database.execute(
                    "SELECT event_id, community, payload, delivery_key FROM outbox WHERE available_at <= ? ORDER BY rowid LIMIT 1",
                    (time.time(),),
                ) as cursor,
            ):
                row = await cursor.fetchone()
            if row is None:
                await asyncio.sleep(1)
                continue
            event_id, community, payload, key = row
            try:
                response = await client.post(
                    str(bot.url),
                    content=payload,
                    headers={
                        "Authorization": f"Bearer {bot.token.get_secret_value()}",
                        "Idempotency-Key": key,
                        "X-Community-Key": community,
                        "Content-Type": "application/json",
                    },
                )
                response.raise_for_status()
            except httpx.HTTPError as exc:
                if monitor is not None:
                    monitor.handoff_failures += 1
                logger.warning(
                    "Bot delivery failed event_id=%s community=%s error=%s",
                    event_id,
                    community,
                    type(exc).__name__,
                )
                delay = min(retries.get(key, 0.5) * 2, 60.0)
                retries[key] = delay
                async with lock:
                    await database.execute(
                        "UPDATE outbox SET available_at = ?, aggregate_key = NULL WHERE event_id = ? AND community = ?",
                        (time.time() + delay, event_id, community),
                    )
                    await database.commit()
                continue
            async with lock:
                await database.execute(
                    "DELETE FROM outbox WHERE event_id = ? AND community = ?",
                    (event_id, community),
                )
                await database.commit()
            logger.info(
                "Bot delivery succeeded event_id=%s community=%s", event_id, community
            )
            retries.pop(key, None)
