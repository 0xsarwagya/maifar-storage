import { describe, expect, test } from "bun:test";
import * as queue from "../../src/queue";
import type { QueuedRow } from "../../src/types";

function row(topic: string): QueuedRow {
  return {
    receivedAt: new Date(),
    topic,
    deviceId: null,
    payload: { n: 1 },
  };
}

describe("queue", () => {
  test("enqueue increases depth", () => {
    expect(queue.depth()).toBe(0);
    queue.enqueue(row("a"));
    expect(queue.depth()).toBe(1);
    queue.enqueue(row("b"));
    expect(queue.depth()).toBe(2);
  });

  test("takeBatch removes up to max and preserves order", () => {
    queue.enqueue(row("1"));
    queue.enqueue(row("2"));
    queue.enqueue(row("3"));
    const a = queue.takeBatch(2);
    expect(a.map((r) => r.topic)).toEqual(["1", "2"]);
    expect(queue.depth()).toBe(1);
    const b = queue.takeBatch(10);
    expect(b.map((r) => r.topic)).toEqual(["3"]);
    expect(queue.depth()).toBe(0);
  });

  test("takeBatch returns empty for zero or negative max", () => {
    queue.enqueue(row("x"));
    expect(queue.takeBatch(0)).toEqual([]);
    expect(queue.takeBatch(-1)).toEqual([]);
    expect(queue.depth()).toBe(1);
  });

  test("prependBatch restores rows at front", () => {
    queue.enqueue(row("tail"));
    queue.prependBatch([row("a"), row("b")]);
    expect(queue.takeBatch(3).map((r) => r.topic)).toEqual(["a", "b", "tail"]);
  });

  test("prependBatch no-op for empty", () => {
    queue.prependBatch([]);
    expect(queue.depth()).toBe(0);
  });
});
