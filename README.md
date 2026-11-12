# KCPC Daemon

KCPC Daemon is the planned bridge from competition event feeds to KCPC Bot. MVP 0 provides the runtime foundation; it does not yet ingest or deliver events.

## Run locally

Install [uv](https://docs.astral.sh/uv/) and Python 3.13 or later, then:

```sh
cp config.example.yaml config.yaml
uv sync --locked
uv run kcpc-daemon --config config.yaml
```

Check the process with `curl http://127.0.0.1:8080/health`. Send Ctrl-C or SIGTERM for graceful shutdown. Configuration is a YAML mapping; all fields shown in the example have defaults, and unknown fields are rejected. The database directory is created on startup. Keep the SQLite file on persistent storage if its state needs to survive restarts. `/health` reports process liveness, not readiness of future feeds or bot delivery.

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
