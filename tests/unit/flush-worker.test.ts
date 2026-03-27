import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import { createFlushWorker, MAX_FLUSH_ATTEMPTS } from "../../src/flush-worker";
import type { Sql } from "../../src/db";
import * as queue from "../../src/queue";
import type { QueuedRow } from "../../src/types";

function sampleRows(n: number): QueuedRow[] {
  return Array.from({ length: n }, (_, i) => ({
    receivedAt: new Date(),
    topic: `t/${i}`,
    deviceId: "d",
    payload: { i },
  }));
}

describe("createFlushWorker (unit, mocked insert)", () => {
  let errorSpy: ReturnType<typeof spyOn<typeof console, "error">>;

  beforeEach(() => {
    errorSpy = spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    errorSpy.mockRestore();
  });

  test("flush drains batch up to batchMax", async () => {
    const inserted: QueuedRow[][] = [];
    const sql = null as unknown as Sql;
    sampleRows(5).forEach((r) => queue.enqueue(r));

    const { flush } = createFlushWorker(sql, 2, {
      insertBatch: async (_s, rows) => {
        inserted.push(rows);
      },
      sleep: async () => {},
    });

    await flush();
    expect(inserted.length).toBe(1);
    expect(inserted[0]!.length).toBe(2);
    expect(queue.depth()).toBe(3);

    await flush();
    expect(inserted.length).toBe(2);
    expect(inserted[1]!.length).toBe(2);
    expect(queue.depth()).toBe(1);
  });

  test("retries then succeeds", async () => {
    const sql = null as unknown as Sql;
    queue.enqueue(sampleRows(1)[0]!);

    let calls = 0;
    const { flush } = createFlushWorker(sql, 10, {
      insertBatch: async () => {
        calls += 1;
        if (calls < 3) throw new Error("transient");
      },
      sleep: async () => {},
    });

    await flush();
    expect(calls).toBe(3);
    expect(queue.depth()).toBe(0);
  });

  test("re-queues batch after max failures", async () => {
    const sql = null as unknown as Sql;
    const row = sampleRows(1)[0]!;
    queue.enqueue(row);

    const { flush } = createFlushWorker(sql, 10, {
      insertBatch: async () => {
        throw new Error("always fail");
      },
      sleep: async () => {},
    });

    await flush();
    expect(queue.depth()).toBe(1);
    const pending = queue.takeBatch(10);
    expect(pending[0]!.topic).toBe(row.topic);
  });

  test("respects MAX_FLUSH_ATTEMPTS constant", () => {
    expect(MAX_FLUSH_ATTEMPTS).toBe(8);
  });
});
