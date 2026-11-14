import asyncio
import json
from pathlib import Path

import aiosqlite
import pytest
from aiohttp import web

from kcpc_daemon.config import Config, FeedConfig
from kcpc_daemon.feed import run_feed


async def test_feed_reconnect_and_restart(tmp_path: Path) -> None:
    requests: list[str | None] = []
    completed = asyncio.Event()

    async def feed(request: web.Request) -> web.Response:
        assert request.query.get("key") == "secret"
        token = request.query.get("since_token")
        requests.append(token)
        if token is None:
            return web.Response(
                text="\n"
                + json.dumps({"type": "teams", "id": "1", "data": {}, "token": "one"})
                + "\n"
            )
        if token == "one":
            completed.set()
            return web.Response(
                text=json.dumps(
                    {"type": "teams", "id": "2", "data": None, "token": "two"}
                )
                + "\n"
            )
        completed.set()
        return web.Response(text="\n")

    app = web.Application()
    app.router.add_get("/feed", feed)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert site._server is not None
    port = site._server.sockets[0].getsockname()[1]
    db_path = tmp_path / "db"
    config = FeedConfig(id="contest", url=f"http://127.0.0.1:{port}/feed?key=secret")
    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute(
                "CREATE TABLE feed_cursors (feed_id TEXT PRIMARY KEY, token TEXT NOT NULL)"
            )
            await db.commit()
            task = asyncio.create_task(run_feed(config, db))
            try:
                await asyncio.wait_for(completed.wait(), 4)
                for _ in range(100):
                    async with db.execute("SELECT token FROM feed_cursors") as cursor:
                        row = await cursor.fetchone()
                    if row == ("two",):
                        break
                    await asyncio.sleep(0.01)
                assert row == ("two",)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        completed.clear()
        async with aiosqlite.connect(db_path) as db:
            task = asyncio.create_task(run_feed(config, db))
            try:
                await asyncio.wait_for(completed.wait(), 3)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        assert requests[:3] == [None, "one", "two"]
    finally:
        await runner.cleanup()


def test_duplicate_feed_ids_rejected() -> None:
    with pytest.raises(ValueError, match="unique"):
        Config(
            feeds=[
                FeedConfig(id="x", url="http://localhost/feed"),
                FeedConfig(id="x", url="http://localhost/feed"),
            ]
        )


@pytest.mark.parametrize(
    "invalid",
    [
        b'{"type":"teams","token":"bad"}',
        b'{"type":"teams","id":"x","data":{"x":NaN},"token":"bad"}',
        b'{"type":"teams","id":"\xff","data":{},"token":"bad"}',
    ],
)
async def test_invalid_event_does_not_advance_cursor(
    tmp_path: Path, invalid: bytes
) -> None:
    requests: list[str | None] = []
    retried = asyncio.Event()

    async def feed(request: web.Request) -> web.Response:
        requests.append(request.query.get("since_token"))
        if len(requests) > 1:
            retried.set()
        return web.Response(
            body=b'{"type":"teams","id":"1","data":{},"token":"good"}\n'
            + invalid
            + b"\n"
        )

    app = web.Application()
    app.router.add_get("/", feed)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert site._server is not None
    port = site._server.sockets[0].getsockname()[1]
    try:
        async with aiosqlite.connect(tmp_path / "db") as db:
            await db.execute(
                "CREATE TABLE feed_cursors (feed_id TEXT PRIMARY KEY, token TEXT NOT NULL)"
            )
            await db.commit()
            task = asyncio.create_task(
                run_feed(FeedConfig(id="x", url=f"http://127.0.0.1:{port}/"), db)
            )
            try:
                await asyncio.wait_for(retried.wait(), 4)
                assert requests[:2] == [None, "good"]
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    finally:
        await runner.cleanup()


async def test_silent_feed_times_out_and_reconnects(tmp_path: Path) -> None:
    connected = 0
    reconnected = asyncio.Event()
    release = asyncio.Event()

    async def feed(request: web.Request) -> web.StreamResponse:
        nonlocal connected
        connected += 1
        if connected == 2:
            reconnected.set()
        response = web.StreamResponse()
        await response.prepare(request)
        await release.wait()
        return response

    app = web.Application()
    app.router.add_get("/", feed)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert site._server is not None
    port = site._server.sockets[0].getsockname()[1]
    try:
        async with aiosqlite.connect(tmp_path / "db") as db:
            await db.execute(
                "CREATE TABLE feed_cursors (feed_id TEXT PRIMARY KEY, token TEXT NOT NULL)"
            )
            await db.commit()
            task = asyncio.create_task(
                run_feed(
                    FeedConfig(
                        id="x",
                        url=f"http://127.0.0.1:{port}/",
                        read_timeout_seconds=0.1,
                    ),
                    db,
                )
            )
            try:
                await asyncio.wait_for(reconnected.wait(), 4)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    finally:
        release.set()
        await runner.cleanup()
