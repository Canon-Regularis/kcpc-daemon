"""Consume CLICS-style feeds and persist their resume tokens."""

import asyncio
import json
import logging

import aiosqlite
import httpx

from kcpc_daemon.config import FeedConfig, RouteConfig
from kcpc_daemon.events import FeedEvent, normalize, route

logger = logging.getLogger(__name__)


def reject_constant(value: str) -> None:
    raise ValueError(f"Invalid JSON constant: {value}")


async def run_feed(
    feed: FeedConfig,
    database: aiosqlite.Connection,
    routes: list[RouteConfig] | None = None,
) -> None:
    """Reconnect forever; only validated events advance the durable cursor."""
    delay = 1.0
    timeout = httpx.Timeout(10.0, read=feed.read_timeout_seconds)
    async with httpx.AsyncClient(timeout=timeout) as client:
        while True:
            async with database.execute(
                "SELECT token FROM feed_cursors WHERE feed_id = ?", (feed.id,)
            ) as cursor:
                row = await cursor.fetchone()
            url = httpx.URL(str(feed.url))
            if row:
                url = url.copy_set_param("since_token", row[0])
            try:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    buffer = b""
                    async for chunk in response.aiter_bytes():
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
                            normalized = normalize(feed.id, event)
                            community = (
                                route(normalized, routes or []) if normalized else None
                            )
                            await database.execute(
                                "INSERT INTO feed_cursors (feed_id, token) VALUES (?, ?) "
                                "ON CONFLICT(feed_id) DO UPDATE SET token = excluded.token",
                                (feed.id, event.token),
                            )
                            await database.commit()
                            logger.info(
                                "Feed event received competition_id=%s object_id=%s type=%s community=%s event_id=%s",
                                feed.id,
                                event.id,
                                event.type,
                                community,
                                normalized.event_id if normalized else None,
                            )
                            delay = 1.0
                logger.warning("Feed disconnected feed_id=%s", feed.id)
            except (httpx.HTTPError, UnicodeDecodeError, ValueError) as exc:
                logger.warning(
                    "Feed error feed_id=%s error=%s", feed.id, type(exc).__name__
                )
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60.0)
