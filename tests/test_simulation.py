"""Synthetic contest traffic through the full daemon pipeline."""

import asyncio
import json
import socket
from pathlib import Path

import aiosqlite
from aiohttp import web

from kcpc_daemon.config import BotConfig, Config, FeedConfig, RouteConfig
from kcpc_daemon.main import serve


async def test_synthetic_feed_delivers_without_loss_or_visible_duplicates(
    tmp_path: Path,
) -> None:
    total = 500
    deliveries: dict[str, dict] = {}
    attempts: dict[str, int] = {}
    complete = asyncio.Event()
    retry_succeeded = asyncio.Event()
    mismatches: list[str] = []

    async def feed(request: web.Request) -> web.Response:
        start = int(request.query.get("since_token", "0"))
        events = (
            json.dumps(
                {
                    "type": "submissions",
                    "id": str(i),
                    "data": {"number": i},
                    "token": str(i),
                }
            )
            + "\n"
            for i in range(start + 1, total + 1)
        )
        return web.Response(text="".join(events))

    async def bot(request: web.Request) -> web.Response:
        key = request.headers["Idempotency-Key"]
        event = await request.json()
        attempts[key] = attempts.get(key, 0) + 1
        if key in deliveries:
            if deliveries[key] != event:
                mismatches.append(key)
        else:
            deliveries[key] = event
        if len(deliveries) == total:
            complete.set()
        # Simulate accepting an event but losing its acknowledgement once.
        if event["object_id"] == "1" and attempts[key] == 1:
            return web.Response(status=503)
        if event["object_id"] == "1" and attempts[key] == 2:
            retry_succeeded.set()
        return web.Response(status=204)

    app = web.Application()
    app.router.add_get("/feed", feed)
    app.router.add_post("/bot", bot)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert site._server is not None
    port = site._server.sockets[0].getsockname()[1]
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        daemon_port = sock.getsockname()[1]
    config = Config(
        port=daemon_port,
        database_path=tmp_path / "db",
        feeds=[FeedConfig(id="contest", url=f"http://127.0.0.1:{port}/feed")],
        routes=[RouteConfig(competition_id="contest", community="club")],
        bot=BotConfig(url=f"http://127.0.0.1:{port}/bot", token="secret"),
    )
    stop = asyncio.Event()
    task = asyncio.create_task(serve(config, stop))
    try:
        await asyncio.wait_for(complete.wait(), 30)
        await asyncio.wait_for(retry_succeeded.wait(), 10)
        async with asyncio.timeout(10):
            while True:
                async with (
                    aiosqlite.connect(config.database_path) as db,
                    db.execute("SELECT count(*) FROM outbox") as cursor,
                ):
                    row = await cursor.fetchone()
                if row == (0,):
                    break
                await asyncio.sleep(0.01)
        assert {int(event["object_id"]) for event in deliveries.values()} == set(
            range(1, total + 1)
        )
        assert len(deliveries) == total
        assert mismatches == []
        assert sum(attempts.values()) == total + 1
    finally:
        stop.set()
        await asyncio.wait_for(task, 5)
        await runner.cleanup()
