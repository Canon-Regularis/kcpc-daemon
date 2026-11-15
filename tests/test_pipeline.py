"""Exercise the running daemon against a mock contest and mock Bot."""

import asyncio
import json
import socket
from pathlib import Path

import aiohttp
import aiosqlite
from aiohttp import web

from kcpc_daemon.config import BotConfig, Config, FeedConfig, RouteConfig
from kcpc_daemon.main import serve
from kcpc_daemon.outbox import delivery_key


async def test_contest_reaches_mock_bot(tmp_path: Path) -> None:
    received: list[tuple[dict, str, str]] = []
    completed = asyncio.Event()
    requests: list[str | None] = []
    replay_processed = asyncio.Event()

    async def feed(request: web.Request) -> web.Response:
        requests.append(request.query.get("since_token"))
        if len(requests) >= 3:
            replay_processed.set()
            return web.Response(text="\n")
        events = [
            {"type": "teams", "id": "team-1", "data": {"name": "Team"}, "token": "1"},
            {
                "type": "submissions",
                "id": "sub-1",
                "data": {"team_id": "team-1"},
                "token": "2",
            },
        ]
        return web.Response(text="".join(json.dumps(event) + "\n" for event in events))

    async def bot(request: web.Request) -> web.Response:
        assert request.headers["Authorization"] == "Bearer secret"
        received.append(
            (
                await request.json(),
                request.headers["Idempotency-Key"],
                request.headers["X-Community-Key"],
            )
        )
        if len(received) == 1:
            return web.Response(status=503)
        await asyncio.wait_for(replay_processed.wait(), 5)
        completed.set()
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
        database_path=tmp_path / "daemon.sqlite",
        feeds=[FeedConfig(id="contest", url=f"http://127.0.0.1:{port}/feed")],
        routes=[
            RouteConfig(
                competition_id="contest", community="club", event_types=["submissions"]
            )
        ],
        bot=BotConfig(url=f"http://127.0.0.1:{port}/bot", token="secret"),
    )
    stop = asyncio.Event()
    task = asyncio.create_task(serve(config, stop))
    try:
        await asyncio.wait_for(completed.wait(), 10)
        assert requests[:3] == [None, "2", "2"]
        event, key, community = received[0]
        assert (event["type"], event["object_id"], event["competition_id"]) == (
            "submissions",
            "sub-1",
            "contest",
        )
        assert key == delivery_key(event["event_id"], "club")
        assert community == "club"
        async with aiosqlite.connect(config.database_path) as database:
            async with asyncio.timeout(5):
                while True:
                    async with database.execute(
                        "SELECT count(*) FROM outbox"
                    ) as cursor:
                        if await cursor.fetchone() == (0,):
                            break
                    await asyncio.sleep(0.01)
        assert len(received) == 2
        assert received[0] == received[1]
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{daemon_port}/health") as response,
        ):
            assert response.status == 200
    finally:
        stop.set()
        await asyncio.wait_for(task, 5)
        await runner.cleanup()
