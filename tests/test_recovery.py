"""Recovery of persisted events after a daemon process is killed."""

import asyncio
import json
import socket
import sys
from pathlib import Path

import aiosqlite
from aiohttp import web


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def test_daemon_kill_restart_during_bot_outage(tmp_path: Path) -> None:
    bot_up = False
    restarted = False
    delivered = asyncio.Event()
    failed_attempt = asyncio.Event()
    received: list[dict] = []
    requested: list[str | None] = []

    async def feed(request: web.Request) -> web.Response:
        token = request.query.get("since_token")
        requested.append(token)
        if token or restarted:
            return web.Response(text="\n")
        event = {"type": "submissions", "id": "s1", "data": {}, "token": "one"}
        return web.Response(text=json.dumps(event) + "\n")

    async def bot(request: web.Request) -> web.Response:
        if not bot_up:
            failed_attempt.set()
            return web.Response(status=503)
        received.append(await request.json())
        delivered.set()
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
    db_path = tmp_path / "daemon.db"
    config = tmp_path / "config.yaml"
    config.write_text(f"""port: {free_port()}
database_path: {db_path}
feeds:
  - id: contest
    url: http://127.0.0.1:{port}/feed
routes:
  - competition_id: contest
    community: club
bot:
  url: http://127.0.0.1:{port}/bot
  token: secret
""")
    process = None
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "kcpc_daemon.main",
            "--config",
            str(config),
            stderr=asyncio.subprocess.DEVNULL,
        )
        async with asyncio.timeout(5):
            while True:
                if process.returncode is not None:
                    raise AssertionError("daemon exited before event was queued")
                if db_path.exists():
                    async with aiosqlite.connect(db_path) as db:
                        try:
                            async with db.execute(
                                "SELECT count(*) FROM outbox"
                            ) as cursor:
                                row = await cursor.fetchone()
                            if row == (1,):
                                break
                        except aiosqlite.OperationalError:
                            pass
                await asyncio.sleep(0.02)
        await asyncio.wait_for(failed_attempt.wait(), 5)
        process.kill()
        await process.wait()
        requested.clear()
        restarted = True
        bot_up = True
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "kcpc_daemon.main",
            "--config",
            str(config),
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(delivered.wait(), 5)
        async with asyncio.timeout(5):
            while not requested:
                await asyncio.sleep(0.02)
        assert [event["object_id"] for event in received] == ["s1"]
        assert requested[0] == "one"
    finally:
        if process is not None and process.returncode is None:
            process.terminate()
            await asyncio.wait_for(process.wait(), 5)
        await runner.cleanup()
