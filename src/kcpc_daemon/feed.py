"""Consume CLICS-style feeds and persist their resume tokens."""

import asyncio
import json
import logging

import aiosqlite
import httpx

from kcpc_daemon.config import FeedConfig, RouteConfig
from kcpc_daemon.events import FeedEvent, normalize, route
from kcpc_daemon.monitor import Monitor
from kcpc_daemon.outbox import record_event

logger = logging.getLogger(__name__)


def reject_constant(value: str) -> None:
    raise ValueError(f"Invalid JSON constant: {value}")


async def run_feed(
    feed: FeedConfig,
    database: aiosqlite.Connection,
    routes: list[RouteConfig] | None = None,
    lock: asyncio.Lock | None = None,
    monitor: Monitor | None = None,
) -> None:
    """Reconnect forever; only validated events advance the durable cursor."""
    delay = 1.0
    lock = lock or asyncio.Lock()
    timeout = httpx.Timeout(10.0, read=feed.read_timeout_seconds)
    async with httpx.AsyncClient(timeout=timeout) as client:
        while True:
            async with (
                lock,
                database.execute(
                    "SELECT token FROM feed_cursors WHERE feed_id = ?", (feed.id,)
                ) as cursor,
            ):
                row = await cursor.fetchone()
            url = httpx.URL(str(feed.url))
            if row:
                url = url.copy_set_param("since_token", row[0])
            try:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    if monitor is not None:
                        monitor.connected.add(feed.id)
                        monitor.active.discard(feed.id)
                    buffer = b""
                    async for chunk in response.aiter_bytes():
                        if monitor is not None:
                            monitor.touch(feed.id)
                        buffer += chunk
                        while b"\n" in buffer:
                            line, buffer = buffer.split(b"\n", 1)
                            if not line.strip():
                                continue
                            event = FeedEvent.model_validate(
                                json.loads(
                                    line.decode("utf-8"), parse_constant=reject_constant
                                )
                            )
                            normalized = normalize(feed.id, event, feed.source)
                            communities = (
                                route(normalized, routes or []) if normalized else []
                            )
                            await record_event(
                                database,
                                lock,
                                feed.id,
                                event.token,
                                normalized,
                                communities,
                                feed.aggregation_seconds,
                            )
                            logger.info(
                                "Feed event received competition_id=%s object_id=%s type=%s community=%s event_id=%s",
                                feed.id,
                                event.id,
                                event.type,
                                communities,
                                normalized.event_id if normalized else None,
                            )
                            delay = 1.0
                logger.warning("Feed disconnected feed_id=%s", feed.id)
            except (httpx.HTTPError, UnicodeDecodeError, ValueError) as exc:
                logger.warning(
                    "Feed error feed_id=%s error=%s", feed.id, type(exc).__name__
                )
            finally:
                if monitor is not None:
                    monitor.connected.discard(feed.id)
                    monitor.active.discard(feed.id)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60.0)
