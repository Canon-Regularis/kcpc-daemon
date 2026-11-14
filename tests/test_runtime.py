import asyncio
import os
import signal
import socket
import sys
from pathlib import Path

import aiohttp
import pytest
from pydantic import ValidationError

from kcpc_daemon.config import Config, FeedConfig, load_config
from kcpc_daemon.main import serve


def test_config_validation(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text("port: 1234\nlog_level: DEBUG\n")
    assert load_config(path).port == 1234
    path.write_text("port: 99999\n")
    with pytest.raises(ValidationError):
        load_config(path)
    path.write_text("unknown: true\n")
    with pytest.raises(ValidationError):
        load_config(path)
    path.write_text("[]\n")
    with pytest.raises(TypeError, match="mapping"):
        load_config(path)


async def test_health_and_shutdown(tmp_path: Path) -> None:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    config = Config(port=port, database_path=tmp_path / "data" / "db.sqlite")
    stop = asyncio.Event()
    task = asyncio.create_task(serve(config, stop))
    try:
        async with aiohttp.ClientSession() as session:
            for _ in range(100):
                if task.done():
                    await task
                try:
                    async with session.get(
                        f"http://127.0.0.1:{port}/health"
                    ) as response:
                        assert response.status == 200
                        assert await response.json() == {"status": "ok"}
                        break
                except aiohttp.ClientConnectorError:
                    await asyncio.sleep(0.01)
            else:
                pytest.fail("health server did not start")
        assert config.database_path.exists()
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/ready") as response:
                assert response.status == 200
            async with session.get(f"http://127.0.0.1:{port}/metrics") as response:
                metrics = await response.text()
                assert "kcpc_outbox_depth 0" in metrics
                assert "kcpc_handoff_failures_total 0" in metrics
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=2)
    async with aiohttp.ClientSession() as session:
        with pytest.raises(aiohttp.ClientConnectorError):
            await session.get(f"http://127.0.0.1:{port}/health")


async def test_unavailable_feed_is_not_ready(tmp_path: Path) -> None:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        feed_port = sock.getsockname()[1]
    config = Config(
        port=port,
        database_path=tmp_path / "db",
        feeds=[FeedConfig(id="contest", url=f"http://127.0.0.1:{feed_port}/feed")],
    )
    stop = asyncio.Event()
    task = asyncio.create_task(serve(config, stop))
    try:
        async with aiohttp.ClientSession() as session:
            async with asyncio.timeout(5):
                while True:
                    try:
                        async with session.get(
                            f"http://127.0.0.1:{port}/ready"
                        ) as response:
                            assert response.status == 503
                            break
                    except aiohttp.ClientConnectorError:
                        await asyncio.sleep(0.01)
            async with session.get(f"http://127.0.0.1:{port}/metrics") as response:
                assert (
                    'kcpc_feed_connected{feed_id="contest"} 0' in await response.text()
                )
    finally:
        stop.set()
        await asyncio.wait_for(task, 5)


async def test_database_failure_prevents_start(tmp_path: Path) -> None:
    blocker = tmp_path / "file"
    blocker.write_text("not a directory")
    with pytest.raises(OSError):
        await serve(Config(database_path=blocker / "db"), asyncio.Event())


async def test_sigterm_stops_process(tmp_path: Path) -> None:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    config = tmp_path / "config.yaml"
    config.write_text(f"port: {port}\ndatabase_path: {tmp_path / 'db.sqlite'}\n")
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "kcpc_daemon.main",
        "--config",
        str(config),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with aiohttp.ClientSession() as session:
            for _ in range(200):
                if process.returncode is not None:
                    pytest.fail(f"daemon exited early: {await process.stderr.read()}")
                try:
                    async with session.get(f"http://127.0.0.1:{port}/health"):
                        break
                except aiohttp.ClientConnectorError:
                    await asyncio.sleep(0.01)
            else:
                pytest.fail("daemon did not start")
        os.kill(process.pid, signal.SIGTERM)
        await asyncio.wait_for(process.communicate(), timeout=5)
        assert process.returncode == 0
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


async def test_silent_connected_feed_is_not_ready(tmp_path: Path) -> None:
    from aiohttp import web

    connected = asyncio.Event()
    release = asyncio.Event()

    async def silent(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse()
        await response.prepare(request)
        connected.set()
        await release.wait()
        return response

    app = web.Application()
    app.router.add_get("/feed", silent)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert site._server is not None
    feed_port = site._server.sockets[0].getsockname()[1]
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    config = Config(
        port=port,
        database_path=tmp_path / "db",
        feeds=[FeedConfig(id="contest", url=f"http://127.0.0.1:{feed_port}/feed")],
    )
    stop = asyncio.Event()
    task = asyncio.create_task(serve(config, stop))
    try:
        await asyncio.wait_for(connected.wait(), 5)
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/ready") as response:
                assert response.status == 503
            async with asyncio.timeout(5):
                while True:
                    async with session.get(
                        f"http://127.0.0.1:{port}/metrics"
                    ) as response:
                        if (
                            'kcpc_feed_connected{feed_id="contest"} 1'
                            in await response.text()
                        ):
                            break
                    await asyncio.sleep(0.01)
    finally:
        release.set()
        stop.set()
        await asyncio.wait_for(task, 5)
        await runner.cleanup()


def test_monitor_keeps_activity_when_connection_changes() -> None:
    from kcpc_daemon.monitor import Monitor

    monitor = Monitor({"contest": 60})
    monitor.connected.add("contest")
    monitor.touch("contest")
    last_activity = monitor.activity["contest"]
    assert monitor.alive("contest")
    monitor.connected.discard("contest")
    monitor.active.discard("contest")
    monitor.connected.add("contest")
    assert not monitor.alive("contest")
    assert monitor.activity["contest"] == last_activity
