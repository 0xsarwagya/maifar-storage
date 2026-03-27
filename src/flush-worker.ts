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

async function defaultInsertBatch(sql: Sql, rows: QueuedRow[]): Promise<void> {
  const objects = rows.map((r) => ({
    received_at: r.receivedAt,
    topic: r.topic,
    device_id: r.deviceId,
    payload: r.payload,
  }));
  await sql`insert into device_messages ${sql(objects, [...columns])}`;
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
