# KCPC Daemon

KCPC Daemon is the planned bridge from competition event feeds to KCPC Bot. It can consume CLICS-style feeds and resume after disconnects; event routing and delivery are not implemented yet.

## Run locally

Install [uv](https://docs.astral.sh/uv/) and Python 3.13 or later, then:

```sh
cp config.example.yaml config.yaml
uv sync --locked
uv run kcpc-daemon --config config.yaml
```

Check the process with `curl http://127.0.0.1:8080/health`. Send Ctrl-C or SIGTERM for graceful shutdown. Configuration is a YAML mapping; all fields shown in the example have defaults, and unknown fields are rejected. The database directory is created on startup. Keep the SQLite file on persistent storage if its state needs to survive restarts. `/health` reports process liveness, not readiness of future feeds or bot delivery.

## Feeds

Add feeds to the configuration to enable ingestion (the default is no feeds):

```yaml
feeds:
  - id: mock-contest
    url: http://127.0.0.1:9000/event-feed
    read_timeout_seconds: 60
```

Each feed needs a unique, stable `id` and an HTTP(S) URL. The endpoint should stream newline-delimited JSON objects with string `type`, `id` and `token` fields and an object or null `data` field. Empty lines are keepalives. The daemon stores the last validated token in SQLite and reconnects with `since_token` in the query string after a disconnect, read timeout, invalid event, or restart. Retries use exponential backoff up to 60 seconds. An invalid event does not advance the cursor and must be fixed at the source before the feed can progress.

This milestone logs received events and persists their tokens; it does not retain event contents or deliver them to the Bot. Use a mock feed for MVP 1. Delivery and atomic outbox persistence are planned for a later milestone, so these cursors do not provide delivery guarantees.

## Docker

```sh
docker build -t kcpc-daemon .
docker run --rm -p 8080:8080 -v kcpc-data:/data kcpc-daemon
```

The image listens on all container interfaces and stores SQLite under `/data`. To change settings, mount a YAML file at `/app/config.yaml`; the container runs as UID 10001, which needs write access to the database directory.

## Development

```sh
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```
