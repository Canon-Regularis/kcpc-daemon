"""Daemon lifecycle and health endpoint."""

import argparse
import asyncio
import logging
import signal
from pathlib import Path

import aiosqlite
import yaml
from aiohttp import web

from kcpc_daemon.config import Config, load_config

logger = logging.getLogger(__name__)


async def health(_request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def serve(config: Config, stop: asyncio.Event) -> None:
    config.database_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(config.database_path) as database:
        # Validate access before advertising health. Future MVPs own their schemas.
        await database.execute("SELECT 1")
        app = web.Application()
        app.router.add_get("/health", health)
        runner = web.AppRunner(app, handle_signals=False)
        await runner.setup()
        try:
            site = web.TCPSite(runner, config.host, config.port)
            await site.start()
            logger.info("Daemon listening on %s:%s", config.host, config.port)
            await stop.wait()
        finally:
            logger.info("Shutting down")
            await runner.cleanup()


async def run(config: Config) -> None:
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    installed: list[signal.Signals] = []
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
            installed.append(sig)
        await serve(config, stop)
    finally:
        for sig in installed:
            loop.remove_signal_handler(sig)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the KCPC telemetry daemon")
    parser.add_argument("--config", type=Path, default=Path("config.yaml"))
    args = parser.parse_args()
    try:
        config = load_config(args.config)
    except (OSError, ValueError, TypeError, yaml.YAMLError) as exc:
        parser.exit(2, f"Invalid configuration: {exc}\n")
    logging.basicConfig(
        level=config.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run(config))


if __name__ == "__main__":
    main()
