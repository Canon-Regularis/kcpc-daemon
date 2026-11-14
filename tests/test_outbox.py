import asyncio
from pathlib import Path

import aiosqlite
import pytest
from aiohttp import web

from kcpc_daemon.config import BotConfig, RouteConfig
from kcpc_daemon.events import FeedEvent, normalize
from kcpc_daemon.outbox import deliver, initialize, record_event


@pytest.mark.parametrize("community", ["club café", "club\nother"])
def test_invalid_community_key_rejected(community: str) -> None:
    with pytest.raises(ValueError, match="printable ASCII"):
        RouteConfig(competition_id="contest", community=community)


@pytest.mark.parametrize("token", ["", "sécret", "secret\nother", "has space"])
def test_invalid_bot_token_rejected(token: str) -> None:
    with pytest.raises(ValueError, match="bearer token"):
        BotConfig(url="https://example.com/ingest", token=token)


async def test_atomic_record_and_retry_delivery(tmp_path: Path) -> None:
    attempts: list[tuple[str | None, str | None, str | None, dict]] = []
    delivered = asyncio.Event()

    async def bot(request: web.Request) -> web.Response:
        attempts.append(
            (
                request.headers.get("Authorization"),
                request.headers.get("Idempotency-Key"),
                request.headers.get("X-Community-Key"),
                await request.json(),
            )
        )
        if len(attempts) == 1:
            return web.Response(status=503)
        delivered.set()
        return web.Response(status=204)

    app = web.Application()
    app.router.add_post("/ingest", bot)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert site._server is not None
    port = site._server.sockets[0].getsockname()[1]
    try:
        async with aiosqlite.connect(tmp_path / "db") as db:
            await initialize(db)
            lock = asyncio.Lock()
            event = normalize(
                "contest", FeedEvent(type="teams", id="a", data={}, token="1")
            )
            assert event is not None
            await record_event(db, lock, "contest", "1", event, "club")
            await record_event(db, lock, "contest", "1", event, "club")
            async with db.execute("SELECT count(*) FROM outbox") as cursor:
                assert await cursor.fetchone() == (1,)
            task = asyncio.create_task(
                deliver(
                    db,
                    lock,
                    BotConfig(url=f"http://127.0.0.1:{port}/ingest", token="secret"),
                )
            )
            try:
                await asyncio.wait_for(delivered.wait(), 4)
                for _ in range(100):
                    async with (
                        lock,
                        db.execute("SELECT count(*) FROM outbox") as cursor,
                    ):
                        row = await cursor.fetchone()
                    if row == (0,):
                        break
                    await asyncio.sleep(0.01)
                assert row == (0,)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            assert len(attempts) == 2
            assert attempts[0] == attempts[1]
            assert attempts[0][:3] == ("Bearer secret", event.event_id, "club")
            assert attempts[0][3]["event_id"] == event.event_id
            async with db.execute(
                "SELECT token FROM feed_cursors WHERE feed_id='contest'"
            ) as cursor:
                assert await cursor.fetchone() == ("1",)
    finally:
        await runner.cleanup()


async def test_record_rolls_back_cursor_when_outbox_fails(tmp_path: Path) -> None:
    async with aiosqlite.connect(tmp_path / "db") as db:
        await initialize(db)
        await db.execute("DROP TABLE outbox")
        await db.commit()
        event = normalize(
            "contest", FeedEvent(type="teams", id="a", data={}, token="1")
        )
        with pytest.raises(aiosqlite.OperationalError):
            await record_event(db, asyncio.Lock(), "contest", "1", event, "club")
        async with db.execute("SELECT * FROM feed_cursors") as cursor:
            assert await cursor.fetchall() == []
