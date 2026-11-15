import asyncio
import json
from pathlib import Path

import aiosqlite
from aiohttp import web

from kcpc_daemon.config import FeedConfig, RouteConfig
from kcpc_daemon.events import FeedEvent, normalize
from kcpc_daemon.feed import run_feed
from kcpc_daemon.outbox import initialize, record_event, set_catch_up


async def test_catch_up_survives_restart_and_replay(tmp_path: Path) -> None:
    path = tmp_path / "db"
    lock = asyncio.Lock()
    async with aiosqlite.connect(path) as db:
        await initialize(db)
        await set_catch_up(db, lock, "trading", True)
        for token, price in [("1", 10), ("2", 11)]:
            event = normalize(
                "trading",
                FeedEvent(type="prices", id="ABC", data={"price": price}, token=token),
                "bachelier",
            )
            assert event is not None
            await record_event(
                db, lock, "trading", token, event, ["alpha", "beta"], 1, True
            )
        event = normalize(
            "trading",
            FeedEvent(type="market_events", id="m", data={"open": True}, token="3"),
            "bachelier",
        )
        assert event is not None
        await record_event(db, lock, "trading", "3", event, ["alpha", "beta"], 1, True)
        async with db.execute("SELECT count(*) FROM outbox") as cursor:
            assert await cursor.fetchone() == (2,)
    async with aiosqlite.connect(path) as db:
        await initialize(db)
        await set_catch_up(db, lock, "trading", True)
        duplicate = normalize(
            "trading",
            FeedEvent(type="prices", id="ABC", data={"price": 11}, token="2"),
            "bachelier",
        )
        fresh = normalize(
            "trading",
            FeedEvent(type="prices", id="ABC", data={"price": 12}, token="4"),
            "bachelier",
        )
        assert duplicate is not None and fresh is not None
        await record_event(
            db, lock, "trading", "2", duplicate, ["alpha", "beta"], 1, True
        )
        await record_event(db, lock, "trading", "4", fresh, ["alpha", "beta"], 1, True)
        await set_catch_up(db, lock, "trading", False)
        await set_catch_up(db, lock, "trading", False)
        async with db.execute(
            "SELECT community, payload FROM outbox ORDER BY rowid"
        ) as cursor:
            rows = await cursor.fetchall()
        assert len(rows) == 4
        assert [json.loads(payload)["type"] for _, payload in rows[:2]] == [
            "market_events",
            "market_events",
        ]
        assert {community for community, _ in rows[2:]} == {"alpha", "beta"}
        for _, payload in rows[2:]:
            assert json.loads(payload)["data"] == {
                "count": 3,
                "items": {"ABC": {"price": 12}},
            }
        async with db.execute("SELECT count(*) FROM catch_up_snapshots") as cursor:
            assert await cursor.fetchone() == (0,)


async def test_feed_markers_release_one_snapshot_after_reconnect(
    tmp_path: Path,
) -> None:
    requested = []
    finished = asyncio.Event()

    async def feed(request: web.Request) -> web.Response:
        token = request.query.get("since_token")
        requested.append(token)
        if token is None:
            lines = [
                {"type": "_kcpc_replay_start"},
                {"type": "prices", "id": "ABC", "data": {"price": 10}, "token": "1"},
            ]
        else:
            lines = [
                {"type": "prices", "id": "ABC", "data": {"price": 11}, "token": "2"},
                {"type": "_kcpc_replay_end"},
            ]
            finished.set()
        return web.Response(text="".join(json.dumps(line) + "\n" for line in lines))

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
            await initialize(db)
            task = asyncio.create_task(
                run_feed(
                    FeedConfig(
                        id="trading",
                        url=f"http://127.0.0.1:{port}/",
                        source="bachelier",
                        catch_up=True,
                    ),
                    db,
                    [RouteConfig(competition_id="trading", community="alpha")],
                )
            )
            try:
                await asyncio.wait_for(finished.wait(), 5)
                async with asyncio.timeout(5):
                    while True:
                        async with db.execute("SELECT payload FROM outbox") as cursor:
                            row = await cursor.fetchone()
                        if row:
                            break
                        await asyncio.sleep(0.01)
                assert json.loads(row[0])["data"] == {
                    "count": 2,
                    "items": {"ABC": {"price": 11}},
                }
                assert requested[:2] == [None, "1"]
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    finally:
        await runner.cleanup()


async def test_catch_up_through_running_daemon(tmp_path: Path) -> None:
    import socket

    from kcpc_daemon.config import BotConfig, Config
    from kcpc_daemon.main import serve

    release = asyncio.Event()
    received: list[dict] = []

    async def feed(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse()
        await response.prepare(request)
        for item in [
            {"type": "_kcpc_replay_start"},
            {"type": "prices", "id": "ABC", "data": {"price": 10}, "token": "1"},
            {"type": "market_events", "id": "open", "data": {}, "token": "2"},
            {"type": "prices", "id": "ABC", "data": {"price": 11}, "token": "3"},
        ]:
            await response.write((json.dumps(item) + "\n").encode())
        await release.wait()
        await response.write(b'{"type":"_kcpc_replay_end"}\n')
        await asyncio.sleep(3)
        return response

    async def bot(request: web.Request) -> web.Response:
        assert request.headers["Authorization"] == "Bearer secret"
        assert request.headers["X-Community-Key"] == "alpha"
        assert request.headers["Idempotency-Key"]
        received.append(await request.json())
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
        feeds=[
            FeedConfig(
                id="trading",
                url=f"http://127.0.0.1:{port}/feed",
                source="bachelier",
                catch_up=True,
            )
        ],
        routes=[RouteConfig(competition_id="trading", community="alpha")],
        bot=BotConfig(url=f"http://127.0.0.1:{port}/bot", token="secret"),
    )
    stop = asyncio.Event()
    task = asyncio.create_task(serve(config, stop))
    try:
        async with asyncio.timeout(10):
            while len(received) < 1:
                await asyncio.sleep(0.02)
        assert [event["type"] for event in received] == ["market_events"]
        release.set()
        async with asyncio.timeout(10):
            while len(received) < 2:
                await asyncio.sleep(0.02)
        assert received[1]["type"] == "prices"
        assert received[1]["data"] == {"count": 2, "items": {"ABC": {"price": 11}}}
    finally:
        release.set()
        stop.set()
        await asyncio.wait_for(task, 5)
        await runner.cleanup()
