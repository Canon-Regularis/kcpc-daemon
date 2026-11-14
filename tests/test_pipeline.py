"""Exercise the running daemon against a mock contest and mock Bot."""

import asyncio
import json
import socket
from pathlib import Path

import aiohttp
from aiohttp import web

from kcpc_daemon.config import BotConfig, Config, FeedConfig, RouteConfig
from kcpc_daemon.main import serve


async def test_contest_reaches_mock_bot(tmp_path: Path) -> None:
    received: list[tuple[dict, str, str]] = []
    completed = asyncio.Event()
    requests: list[str | None] = []

    async def feed(request: web.Request) -> web.Response:
        requests.append(request.query.get("since_token"))
        if request.query.get("since_token"):
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
        await asyncio.wait_for(completed.wait(), 5)
        assert len(received) == 1
        event, key, community = received[0]
        assert (event["type"], event["object_id"], event["competition_id"]) == (
            "submissions",
            "sub-1",
            "contest",
        )
        assert key == event["event_id"]
        assert community == "club"
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{daemon_port}/health") as response,
        ):
            assert response.status == 200
    finally:
        stop.set()
        await asyncio.wait_for(task, 5)
        await runner.cleanup()
