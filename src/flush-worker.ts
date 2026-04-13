import type { Sql } from "./db";
import * as queue from "./queue";
import type { QueuedRow } from "./types";

export const MAX_FLUSH_ATTEMPTS = 8;

async function defaultSleep(ms: number): Promise<void> {
  await new Promise((r) => setTimeout(r, ms));
}

const columns = ["received_at", "topic", "device_id", "payload"] as const;

export type FlushWorkerDeps = {
  sleep?: (ms: number) => Promise<void>;
  /** Override batch insert (used by tests). */
  insertBatch?: (sql: Sql, rows: QueuedRow[]) => Promise<void>;
};

type InsertOutcome = "inserted" | "dropped" | "requeued";

function getErrorCode(err: unknown): string | undefined {
  if (!err || typeof err !== "object") return undefined;
  const code = (err as { code?: unknown }).code;
  return typeof code === "string" ? code : undefined;
}

function isPoisonRowError(err: unknown): boolean {
  const code = getErrorCode(err);
  return code === "22P05" || code?.startsWith("22") === true;
}

async function defaultInsertBatch(sql: Sql, rows: QueuedRow[]): Promise<void> {
  const objects = rows.map((r) => ({
    received_at: r.receivedAt,
    topic: r.topic,
    device_id: r.deviceId,
    payload: r.payload,
  }));
  await sql`insert into device_messages ${sql(objects, [...columns])}`;
}

async function insertSingleRowWithRetries(
  sql: Sql,
  row: QueuedRow,
  insertBatch: (sql: Sql, rows: QueuedRow[]) => Promise<void>,
  sleep: (ms: number) => Promise<void>,
): Promise<InsertOutcome> {
  let attempt = 0;
  while (attempt < MAX_FLUSH_ATTEMPTS) {
    try {
      await insertBatch(sql, [row]);
      return "inserted";
    } catch (err) {
      if (isPoisonRowError(err)) {
        console.error("[flush] dropped poison row:", {
          topic: row.topic,
          device_id: row.deviceId,
          received_at: row.receivedAt.toISOString(),
          error_code: getErrorCode(err),
          error: err,
        });
        return "dropped";
      }

      attempt += 1;
      const wait = Math.min(30_000, 500 * 2 ** (attempt - 1));
      console.error(
        `[flush] isolated row attempt ${attempt}/${MAX_FLUSH_ATTEMPTS} failed:`,
        err,
      );
      if (attempt >= MAX_FLUSH_ATTEMPTS) {
        return "requeued";
      }
      await sleep(wait);
    }
  }
  return "requeued";
}

async function isolateBatchRows(
  sql: Sql,
  rows: QueuedRow[],
  insertBatch: (sql: Sql, rows: QueuedRow[]) => Promise<void>,
  sleep: (ms: number) => Promise<void>,
): Promise<void> {
  console.error(
    `[flush] isolating poison row inside batch of ${rows.length} rows`,
  );

  for (let i = 0; i < rows.length; i++) {
    const outcome = await insertSingleRowWithRetries(
      sql,
      rows[i]!,
      insertBatch,
      sleep,
    );
    if (outcome === "requeued") {
      const remaining = rows.slice(i);
      queue.prependBatch(remaining);
      console.error(
        "[flush] row isolation stopped; re-queued remaining rows",
        remaining.length,
      );
      return;
    }
  }
}

export function createFlushWorker(
  sql: Sql,
  batchMax: number,
  deps: FlushWorkerDeps = {},
) {
  const sleep = deps.sleep ?? defaultSleep;
  const insertBatch = deps.insertBatch ?? defaultInsertBatch;
  let chain: Promise<void> = Promise.resolve();

  async function flushOnce(): Promise<void> {
    const batch = queue.takeBatch(batchMax);
    if (batch.length === 0) return;

    let attempt = 0;
    while (attempt < MAX_FLUSH_ATTEMPTS) {
      try {
        await insertBatch(sql, batch);
        return;
      } catch (err) {
        if (isPoisonRowError(err)) {
          await isolateBatchRows(sql, batch, insertBatch, sleep);
          return;
        }
        attempt += 1;
        const wait = Math.min(30_000, 500 * 2 ** (attempt - 1));
        console.error(
          `[flush] attempt ${attempt}/${MAX_FLUSH_ATTEMPTS} failed:`,
          err,
        );
        if (attempt >= MAX_FLUSH_ATTEMPTS) break;
        await sleep(wait);
      }
    }
    queue.prependBatch(batch);
    console.error(
      "[flush] giving up after retries; batch re-queued at front, size",
      batch.length,
    );
  }

  function flush(): Promise<void> {
    chain = chain.then(flushOnce);
    return chain;
  }

  return { flush };
}
