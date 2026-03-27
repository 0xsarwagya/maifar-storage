import type { QueuedRow } from "./types";

const buffer: QueuedRow[] = [];

export function enqueue(row: QueuedRow): void {
  buffer.push(row);
}

export function takeBatch(max: number): QueuedRow[] {
  if (max <= 0 || buffer.length === 0) return [];
  const n = Math.min(max, buffer.length);
  return buffer.splice(0, n);
}

export function prependBatch(rows: QueuedRow[]): void {
  if (rows.length === 0) return;
  buffer.unshift(...rows);
}

export function depth(): number {
  return buffer.length;
}

/** Clears the in-memory buffer (tests only). */
export function resetQueue(): void {
  buffer.length = 0;
}
