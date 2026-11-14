import asyncio
import json
from pathlib import Path

import aiosqlite

from kcpc_daemon.events import FeedEvent, normalize
from kcpc_daemon.outbox import initialize, record_event


async def test_bachelier_aggregation_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "db"
    lock = asyncio.Lock()
    async with aiosqlite.connect(path) as db:
        await initialize(db)
        for token, price in [("1", 10), ("2", 11)]:
            event = normalize(
                "trading",
                FeedEvent(type="prices", id="ABC", data={"price": price}, token=token),
                "bachelier",
            )
            assert event is not None
            await record_event(db, lock, "trading", token, event, ["club"], 60)
    async with aiosqlite.connect(path) as db:
        await initialize(db)
        async with db.execute("SELECT payload, available_at FROM outbox") as cursor:
            rows = await cursor.fetchall()
        assert len(rows) == 1
        payload = json.loads(rows[0][0])
        assert payload["source"] == "bachelier"
        assert payload["data"] == {"count": 2, "items": {"ABC": {"price": 11}}}
        assert rows[0][1] > 0
        async with db.execute(
            "SELECT token FROM feed_cursors WHERE feed_id = 'trading'"
        ) as cursor:
            assert await cursor.fetchone() == ("2",)


def test_bachelier_event_types() -> None:
    for kind in ("prices", "trades", "market_events", "leaderboard"):
        event = FeedEvent(type=kind, id="1", data={}, token="1")
        assert normalize("trading", event, "bachelier") is not None
        assert normalize("contest", event) is None
