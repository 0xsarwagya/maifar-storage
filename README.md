# maifar-storage

Service ( **[Bun](https://bun.sh)** + TypeScript ) that subscribes to an MQTT broker, accepts **JSON** payloads only, batches rows in memory, flushes them to **PostgreSQL** in multi-row inserts, and exposes a read-only **HTTP** API (JSON listing, CSV / NDJSON export, OpenAPI, Swagger UI, Scalar, ReDoc).

**Default broker host:** `mqtt.maifar.actimi.com` (see **`MQTT_HOST`** / **`MQTT_PORT`** / **`MQTT_SSL`** in [`.env.example`](.env.example)).

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
- MQTT broker; `.env.example` uses plain **`mqtt` on 1883** by default. Set **`MQTT_SSL=true`** (or **`MQTT_TLS=true`**) for **`mqtts`** and default port **8883**. Local Mosquitto via Docker Compose is optional for development.

## Repository layout

| Path | Role |
|------|------|
| [`src/index.ts`](src/index.ts) | Process entry: optional migrate, `createApp()`, signals |
| [`src/schema-migrate.ts`](src/schema-migrate.ts) | Apply `schema.sql` (shared with CLI migrate) |
| [`src/app.ts`](src/app.ts) | Wires DB, flush worker, MQTT, HTTP |
| [`src/mqtt-ingest.ts`](src/mqtt-ingest.ts) | MQTT connect, subscribe, enqueue |
| [`src/flush-worker.ts`](src/flush-worker.ts) | Batched insert + retry |
| [`src/queue.ts`](src/queue.ts) | In-memory buffer |
| [`src/api.ts`](src/api.ts) | REST handlers |
| [`src/api-docs.ts`](src/api-docs.ts) + [`src/openapi-spec.ts`](src/openapi-spec.ts) | OpenAPI + doc shells |
| [`src/cors.ts`](src/cors.ts) | CORS headers + `OPTIONS` preflight |
| [`src/config.ts`](src/config.ts) | Environment |
| [`src/db.ts`](src/db.ts) | `postgres` client |
| [`src/pg-tls.ts`](src/pg-tls.ts) | Optional Postgres `ssl` options from env |
| [`schema.sql`](schema.sql) | Table + indexes |
| [`scripts/migrate.ts`](scripts/migrate.ts) | Apply `schema.sql` |
| [`docker-compose.yml`](docker-compose.yml) | Postgres + optional Mosquitto |
| [`tests/`](tests/) | Unit, integration, smoke ([`bunfig.toml`](bunfig.toml) preloads [`tests/setup.ts`](tests/setup.ts)) |

## Quick start

1. **Environment**

   ```bash
   cp .env.example .env
   ```

   Edit `.env`: set `DATABASE_URL`, **`MQTT_HOST`** + **`MQTT_PORT`**, **`MQTT_SSL=true`** if the broker uses MQTTS (or use `MQTT_URL`), and **`MQTT_TOPICS`** for your broker and ACLs.

2. **Dependencies**

   ```bash
   bun install
   ```

3. **Postgres** (local example: `docker compose up -d` for [`docker-compose.yml`](docker-compose.yml))

4. **Schema**

   On **`bun run dev`** / **`bun run start`**, **`schema.sql`** is applied automatically by default (`AUTO_MIGRATE`, see [Environment variables](#environment-variables)). You can still run it manually:

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
| `DATABASE_TLS_CA_FILE` | no | PEM CA path for Postgres TLS (trust private CA without disabling verification) |
| `DATABASE_TLS_INSECURE` | no | If `true` / `1` / `yes` / `on`, **`ssl: { rejectUnauthorized: false }`** for Postgres (**insecure**) |
| `DATABASE_TLS_REJECT_UNAUTHORIZED` | no | Set to `false` / `0` / `no` / `off` to skip Postgres TLS cert verification |
| `AUTO_MIGRATE` | no | If not `false` / `0` / `no` / `off`, apply `schema.sql` once at process startup before MQTT/HTTP (default **on**) |
| `MQTT_HOST` | yes* | Broker hostname (*required if neither `MQTT_URL` nor `MQTT_SERVERS` is set) |
| `MQTT_PORT` | no | Broker port; default **1883** without TLS, **8883** when `MQTT_SSL` / `MQTT_TLS` is enabled |
| `MQTT_SSL` / `MQTT_TLS` | no | Set to `true` / `1` / `yes` / `on` for **`mqtts`** (TLS); default is **off** (plain **`mqtt`**) |
| `MQTT_TLS_CA_FILE` | no | Path to PEM CA bundle to trust for MQTTS (fixes private CA without disabling verification) |
| `MQTT_TLS_INSECURE` | no | If `true` / `1` / `yes` / `on`, set **`rejectUnauthorized: false`** for MQTTS (**insecure**; dev only) |
| `MQTT_TLS_REJECT_UNAUTHORIZED` | no | Set to `false` / `0` / `no` / `off` to skip TLS cert verification for MQTTS (same risk as `MQTT_TLS_INSECURE`) |
| `MQTT_SERVERS` | no | JSON array of `{ "host", "port", "protocol"? }`; MQTT.js iterates on reconnect; **`MQTT_HOST` / `MQTT_PORT` ignored** when set |
| `MQTT_URL` | no | Full broker URL; if set, **`MQTT_HOST` / `MQTT_PORT` / `MQTT_SSL` / `MQTT_SERVERS` are ignored** |
| `MQTT_USERNAME` | no | MQTT username (pair with `MQTT_PASSWORD`; avoids userinfo in `MQTT_URL`) |
| `MQTT_PASSWORD` | no | MQTT password when `MQTT_USERNAME` is set; defaults to empty string if unset |
| `MQTT_CLIENT_ID` | no | Optional fixed MQTT client id (otherwise generated) |
| `MQTT_TOPICS` | yes | Comma-separated subscribe patterns (`+`, `#` per MQTT rules) |
| `HTTP_PORT` | no | Default `3000` |
| `BATCH_MAX` | no | Rows per flush (default `100`) |
| `FLUSH_INTERVAL_MS` | no | Timer flush interval ms (default `1000`, minimum `50`) |
| `DEVICE_ID_TOPIC_REGEX` | no | Regex with capture group 1 = device id (default `^devices/([^/]+)/`) |
| `DEVICE_ID_JSON_KEY` | no | JSON object key for device id when regex does not match |
| `DISPLAY_TIMEZONE` | no | Default IANA timezone for `/devices` `*_local` fields when `timezone` query is omitted; falls back to `TZ`, then UTC |
| `CORS_ORIGIN` | no | `Access-Control-Allow-Origin` for all HTTP responses (default **`*`**). Set to your SPA origin if you do not want `*` |
| `TEST_DATABASE_URL` | tests only | Same shape as `DATABASE_URL`; enables integration + DB smoke tests |

Non-JSON MQTT bodies are **skipped** (logged). Only successfully parsed JSON is stored as `jsonb`.

**MQTT connection:** Use **`MQTT_URL`**, or **`MQTT_SERVERS`**, or **`MQTT_HOST`** with optional **`MQTT_PORT`**. Without TLS flags, the client uses plain **`mqtt`** on port **1883** by default. Set **`MQTT_SSL=true`** or **`MQTT_TLS=true`** for **`mqtts`** and default port **8883**. Options are passed as `protocol` + `servers` (no synthetic URL for the host/port path).

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
| `/devices` | Optional `timezone` (IANA). JSON: per-`device_id` `message_count`, `first_received_at` / `last_received_at` (ISO UTC), `*_local` (formatted in that timezone), plus `messages_without_device_id` |
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

Browsers need access to those CDNs to render UIs. All JSON/HTML responses include **CORS** headers (`Access-Control-Allow-Origin` defaults to `*`; override with **`CORS_ORIGIN`**). Doc pages load `/openapi.json` using an **absolute `http(s)` URL** so Swagger / Scalar / ReDoc avoid invalid relative fetch errors.

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
| TLS / certificate errors | Install broker CA on the host, or set **`MQTT_TLS_CA_FILE`** to a PEM bundle; last resort **`MQTT_TLS_INSECURE=true`** (dev only) |
| `SELF_SIGNED_CERT_IN_CHAIN` on startup | Often **Postgres** during **`AUTO_MIGRATE`** (runs before MQTT). Set **`DATABASE_TLS_INSECURE=true`** or **`DATABASE_TLS_CA_FILE`**. If it happens after connect, check **`MQTT_TLS_*`** for the broker |
| `unrecognized configuration parameter "sslrootcert"` | Hosted **`DATABASE_URL`** often includes `sslrootcert=…`; **`postgres.js`** used to forward that to the server. The app strips **`sslrootcert` / `sslcert` / `sslkey` / `sslcrl`** from the URL before connecting — pull latest; use **`DATABASE_TLS_CA_FILE`** if you need a custom CA |
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
