# maifar-storage

Service ( **[Bun](https://bun.sh)** + TypeScript ) that subscribes to an MQTT broker, accepts **JSON** payloads only, batches rows in memory, flushes them to **PostgreSQL** in multi-row inserts, and exposes a read-only **HTTP** API (JSON listing, CSV / NDJSON export, OpenAPI, Swagger UI, Scalar, ReDoc).

**Default broker host:** `mqtt.maifar.actimi.com` (see **`MQTT_HOST`** / **`MQTT_PORT`** in [`.env.example`](.env.example)).

## Table of contents

- [Architecture](#architecture)
- [Requirements](#requirements)
- [Repository layout](#repository-layout)
- [Quick start](#quick-start)
- [Scripts](#scripts)
- [Environment variables](#environment-variables)
- [Data path and queue](#data-path-and-queue)
- [HTTP API](#http-api)
- [OpenAPI and docs UIs](#openapi-and-docs-uis)
- [Testing](#testing)
- [Operations](#operations)
- [Troubleshooting](#troubleshooting)
- [Test publish](#test-publish)
- [Contributing and legal](#contributing-and-legal)

## Architecture

```mermaid
flowchart LR
  Broker[MQTT broker]
  Sub[MQTT subscriber]
  Q[In-memory queue]
  Flush[Flush worker]
  PG[(PostgreSQL)]
  HTTP[HTTP API]

  Broker --> Sub
  Sub --> Q
  Q --> Flush
  Flush --> PG
  HTTP --> PG
```

1. **Ingest:** `on("message")` parses UTF-8 as JSON, resolves optional `device_id` (topic regex and/or JSON key), enqueues a row. No per-message `INSERT`.
2. **Flush:** When `queue.depth() >= BATCH_MAX` or every `FLUSH_INTERVAL_MS`, one serialized worker runs a batch insert (retries with backoff; failed batches are prepended back onto the queue).
3. **Query:** `Bun.serve` routes GET handlers that read from `device_messages` only.

## Requirements

- [Bun](https://bun.sh) 1.x
- PostgreSQL 14+ (`jsonb`)
- MQTT broker; `.env.example` uses **`MQTT_HOST=mqtt.maifar.actimi.com`** and **`MQTT_PORT=8883`** (TLS / `mqtts` by default). Local Mosquitto via Docker Compose is optional for development.

## Repository layout

| Path | Role |
|------|------|
| [`src/index.ts`](src/index.ts) | Process entry: `createApp()`, signals, logs |
| [`src/app.ts`](src/app.ts) | Wires DB, flush worker, MQTT, HTTP |
| [`src/mqtt-ingest.ts`](src/mqtt-ingest.ts) | MQTT connect, subscribe, enqueue |
| [`src/flush-worker.ts`](src/flush-worker.ts) | Batched insert + retry |
| [`src/queue.ts`](src/queue.ts) | In-memory buffer |
| [`src/api.ts`](src/api.ts) | REST handlers |
| [`src/api-docs.ts`](src/api-docs.ts) + [`src/openapi-spec.ts`](src/openapi-spec.ts) | OpenAPI + doc shells |
| [`src/config.ts`](src/config.ts) | Environment |
| [`src/db.ts`](src/db.ts) | `postgres` client |
| [`schema.sql`](schema.sql) | Table + indexes |
| [`scripts/migrate.ts`](scripts/migrate.ts) | Apply `schema.sql` |
| [`docker-compose.yml`](docker-compose.yml) | Postgres + optional Mosquitto |
| [`tests/`](tests/) | Unit, integration, smoke ([`bunfig.toml`](bunfig.toml) preloads [`tests/setup.ts`](tests/setup.ts)) |

## Quick start

1. **Environment**

   ```bash
   cp .env.example .env
   ```

   Edit `.env`: set `DATABASE_URL`, **`MQTT_HOST`** + **`MQTT_PORT`** (or `MQTT_URL`), and **`MQTT_TOPICS`** for your broker and ACLs.

2. **Dependencies**

   ```bash
   bun install
   ```

3. **Postgres** (local example: `docker compose up -d` for [`docker-compose.yml`](docker-compose.yml))

4. **Schema**

   ```bash
   bun run db:migrate
   ```

5. **Run**

   ```bash
   bun run dev
   ```

   Logs print the HTTP base URL and doc paths (`/docs`, `/scalar`, `/redoc`, `/openapi.json`).

## Scripts

| Command | Purpose |
|---------|---------|
| `bun run dev` | Watch mode, `src/index.ts` |
| `bun run start` | Production-style run |
| `bun run db:migrate` | Run `schema.sql` against `DATABASE_URL` |
| `bun test` | All tests |
| `bun run test:unit` | Unit only |
| `bun run test:integration` | Integration (needs DB) |
| `bun run test:smoke` | Smoke subset |
| `bun run test:coverage` | Coverage report |

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | yes | PostgreSQL connection URL |
| `MQTT_HOST` | yes* | Broker hostname (*required if `MQTT_URL` is unset) |
| `MQTT_PORT` | no | Broker port; default **8883** with TLS, **1883** when `MQTT_SSL=false` |
| `MQTT_SSL` / `MQTT_TLS` | no | Set to `false` for plain `mqtt://` (defaults to secured `mqtts://`) |
| `MQTT_URL` | no | Full broker URL; if set, **`MQTT_HOST` / `MQTT_PORT` / `MQTT_SSL` are ignored** |
| `MQTT_USERNAME` | no | MQTT username (pair with `MQTT_PASSWORD`; avoids userinfo in `MQTT_URL`) |
| `MQTT_PASSWORD` | no | MQTT password when `MQTT_USERNAME` is set; defaults to empty string if unset |
| `MQTT_CLIENT_ID` | no | Optional fixed MQTT client id (otherwise generated) |
| `MQTT_TOPICS` | yes | Comma-separated subscribe patterns (`+`, `#` per MQTT rules) |
| `HTTP_PORT` | no | Default `3000` |
| `BATCH_MAX` | no | Rows per flush (default `100`) |
| `FLUSH_INTERVAL_MS` | no | Timer flush interval ms (default `1000`, minimum `50`) |
| `DEVICE_ID_TOPIC_REGEX` | no | Regex with capture group 1 = device id (default `^devices/([^/]+)/`) |
| `DEVICE_ID_JSON_KEY` | no | JSON object key for device id when regex does not match |
| `TEST_DATABASE_URL` | tests only | Same shape as `DATABASE_URL`; enables integration + DB smoke tests |

Non-JSON MQTT bodies are **skipped** (logged). Only successfully parsed JSON is stored as `jsonb`.

**MQTT connection:** Either set **`MQTT_HOST`** (and optionally **`MQTT_PORT`**) or set **`MQTT_URL`** alone. With host/port, the URL is built as `mqtts://HOST:PORT` by default (port **8883**), or `mqtt://HOST:1883` when **`MQTT_SSL=false`**.

**MQTT authentication:** Use **`MQTT_USERNAME`** and **`MQTT_PASSWORD`** (with host/port or a host-only `MQTT_URL`). You can still put `user:pass` inside `MQTT_URL` if that fits your deployment.

## Data path and queue

- **Not durable:** buffered rows are lost if the process exits before a successful flush. Tune `BATCH_MAX` / `FLUSH_INTERVAL_MS` vs latency and risk.
- **Ordering:** inserts preserve enqueue order within each batch; cross-batch order follows flush timing.
- **Failure:** flush retries up to 8 times with exponential backoff; then the batch is prepended to the queue again.

## HTTP API

All data routes are **GET**.

| Route | Summary |
|-------|---------|
| `/health` | `ok`, `database` (`up` / `down`), `queue_depth` |
| `/messages` | Query: `from`, `to`, `device_id`, `topic_prefix`, `limit`, `cursor` → `{ items, next_cursor }` |
| `/export.csv` | Same filters; streamed CSV; `payload` cell is JSON text |
| `/export.json` | Same filters; streamed NDJSON |

Invalid query parameters return **400** with `{ "error": "..." }`.

## OpenAPI and docs UIs

| Path | Purpose |
|------|---------|
| `/openapi.json` | OpenAPI 3.0; `servers[0].url` = request origin |
| `/docs` | Swagger UI (CDN) |
| `/scalar` | Scalar (CDN) |
| `/redoc` | ReDoc (CDN) |

Browsers need access to those CDNs to render UIs.

## Testing

```bash
bun test
TEST_DATABASE_URL=postgres://user:pass@localhost:5432/maifar bun test
```

- **Unit** — `tests/unit/`: queue, config, format helpers, device id, flush worker mocks, API + OpenAPI routes.
- **Integration** — `tests/integration/postgres.test.ts`: real Postgres + HTTP + MQTT (Aedes) when `TEST_DATABASE_URL` is set; run `db:migrate` on that database first.
- **Smoke** — `tests/smoke/`: module load; full `createApp` health check when `TEST_DATABASE_URL` is set.

## Operations

- **Stop:** `SIGINT` / `SIGTERM` clear the flush timer, end the MQTT client, run a final flush, close DB connections, stop `Bun.serve`.
- **Health:** Use `/health` for liveness; `database` reflects a simple `SELECT 1`.
- **Production:** Run behind TLS termination (reverse proxy); keep `DATABASE_URL` and `MQTT_URL` in a secret store; restrict network access to Postgres and the MQTT broker.

## Troubleshooting

| Symptom | Things to check |
|---------|------------------|
| MQTT `ECONNREFUSED` / timeout | Host, port (`1883` vs `8883`), firewall, `mqtt://` vs `mqtts://` |
| `Not authorized` / connection closed | `MQTT_USERNAME` / `MQTT_PASSWORD`, broker ACLs, or credentials in `MQTT_URL` |
| TLS / certificate errors | Broker CA, or broker TLS config; Node/Bun trust store |
| `subscribe failed` | Broker ACLs for `MQTT_TOPICS` patterns |
| `database: down` in `/health` | `DATABASE_URL`, Postgres listening, SSL mode if required |
| No rows in `/messages` | Topic match, JSON payloads, flush interval, check `[mqtt]` / `[flush]` logs |

## Test publish

Topics must match `MQTT_TOPICS` (e.g. `devices/+/telemetry`).

**Maifar broker** (adjust TLS and auth to match `MQTT_URL`):

```bash
mosquitto_pub -h mqtt.maifar.actimi.com -p 8883 --capath /etc/ssl/certs \
  -t devices/acme-1/telemetry -m '{"temp":21.5,"deviceId":"acme-1"}'
```

Add `-u USER -P PASS` if the broker requires credentials.

**Local Mosquitto** (`MQTT_URL=mqtt://localhost:1883`):

```bash
mosquitto_pub -h localhost -p 1883 -t devices/acme-1/telemetry -m '{"temp":21.5,"deviceId":"acme-1"}'
```

Then open `http://127.0.0.1:3000/messages` (or your `HTTP_PORT`) after a flush interval.

## Contributing and legal

- [CONTRIBUTING.md](CONTRIBUTING.md) — tests and change workflow
- [SECURITY.md](SECURITY.md) — reporting security issues
- [LICENSE](LICENSE) — MIT
