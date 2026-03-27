# Contributing

## Prerequisites

- [Bun](https://bun.sh) 1.x
- PostgreSQL for integration tests (optional; set `TEST_DATABASE_URL`)

## Setup

```bash
bun install
cp .env.example .env
```

## Checks before a PR

```bash
bunx tsc --noEmit
bun test
```

With a database available:

```bash
bun run db:migrate
TEST_DATABASE_URL=postgres://... bun test
```

## Guidelines

- Match existing style: TypeScript strict mode, minimal scope per change.
- Add or update tests for behavior changes (unit tests should not require Docker unless gated on `TEST_DATABASE_URL`).
- Do not commit `.env` or secrets.

## Project commands

See the [Scripts](README.md#scripts) table in the README.
