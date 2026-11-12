import asyncio
import os
import signal
import socket
import sys
from pathlib import Path

import aiohttp
import pytest
from pydantic import ValidationError

from kcpc_daemon.config import Config, load_config
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
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=2)
    async with aiohttp.ClientSession() as session:
        with pytest.raises(aiohttp.ClientConnectorError):
            await session.get(f"http://127.0.0.1:{port}/health")


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
