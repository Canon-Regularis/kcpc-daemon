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

For a Bachelier feed set `source: bachelier` and optionally `aggregation_seconds` (default 60) on the feed; the default source is `code_court`. Bachelier types are `prices`, `trades`, `market_events`, and `leaderboard`. Prices and trades are held in separate periodic snapshots per competition and community. A snapshot retains the latest data for each object ID and a count of updates in `data: {count, items}`. Its event ID is the first event's ID, and its type is the source type. Other Bachelier events are delivered individually. Pending snapshots and the cursor are stored together in SQLite and survive restart. The Bachelier feed and Bot support for this proposed snapshot format remain to be integrated.

Routed events are queued in SQLite together with their feed cursor, so keep the database on persistent storage. Events with no matching route are skipped. A configured Bot receives queued events; without one they remain in the outbox.

## Code Court events and routes

Code Court events of type `contests`, `state`, `problems`, `teams`, `submissions`, `judgements`, and `awards` are normalized. Other types are ignored. A normalized event contains `event_id` (a SHA-256 digest of the JSON-encoded pair of feed ID and token), `competition_id` (the feed ID), `source` (`code_court`), `type`, `object_id`, and `data` (the original object or null for deletion). The same feed token produces the same event ID across reconnects; different feeds have distinct IDs. Tokens must uniquely identify changes within a feed.

Routes select communities for each competition. Each competition and community pair may appear once, with its own event filter. An empty or omitted `event_types` list includes all supported types. For example:

```yaml
routes:
  - competition_id: mock-contest
    community: kcpc
    event_types: [submissions, judgements]
```

The community is a logical key, not a Discord channel ID. It must contain printable ASCII characters without spaces so it can be passed in an HTTP header.

## Bot handoff

Configure a Bot ingest URL and bearer token to enable delivery:

```yaml
bot:
  url: http://127.0.0.1:9001/internal/events
  token: replace-with-shared-secret
```

Protect the configuration file because it contains the shared secret. The daemon POSTs the normalized event as JSON with `Authorization: Bearer <token>`, `Idempotency-Key: <delivery_key>`, and `X-Community-Key: <community>`. Any 2xx response acknowledges the event; all other responses and network failures are retried with exponential backoff. New delivery keys are SHA-256 hashes of the JSON-encoded event ID and community pair; pending deliveries created before federation keep their original key. Delivery is at least once: the Bot must persistently deduplicate the idempotency key before acknowledging. The Bot endpoint is not yet available, so this contract is exercised with a mock and must be agreed with that project.

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
