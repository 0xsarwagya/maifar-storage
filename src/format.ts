export const DEFAULT_LIMIT = 100;
export const MAX_LIMIT = 1000;

export type ParseFail = { ok: false; message: string };
export type ParseOk<T> = { ok: true; value: T };
export type ParseResult<T> = ParseOk<T> | ParseFail;

export type CursorPayload = { receivedAt: Date; id: bigint };

export function parseIsoDate(
  name: string,
  raw: string | null,
): ParseResult<Date | undefined> {
  if (raw === null || raw === "") return { ok: true, value: undefined };
  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) {
    return {
      ok: false,
      message: `Invalid ${name}: expected ISO 8601 date`,
    };
  }
  return { ok: true, value: d };
}

export function parseLimit(raw: string | null): ParseResult<number> {
  if (raw === null || raw === "") return { ok: true, value: DEFAULT_LIMIT };
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 1) {
    return { ok: false, message: "limit must be a positive integer" };
  }
  return { ok: true, value: Math.min(n, MAX_LIMIT) };
}

export function encodeCursor(c: CursorPayload): string {
  const payload = JSON.stringify({
    t: c.receivedAt.toISOString(),
    id: c.id.toString(),
  });
  return Buffer.from(payload, "utf8").toString("base64url");
}

export function decodeCursor(raw: string): ParseResult<CursorPayload> {
  try {
    const json = JSON.parse(Buffer.from(raw, "base64url").toString("utf8")) as {
      t: string;
      id: string;
    };
    const receivedAt = new Date(json.t);
    if (Number.isNaN(receivedAt.getTime())) {
      return { ok: false, message: "Invalid cursor: bad timestamp" };
    }
    const id = BigInt(json.id);
    return { ok: true, value: { receivedAt, id } };
  } catch {
    return { ok: false, message: "Invalid cursor" };
  }
}

export function escapeLikePrefix(prefix: string): string {
  return prefix.replace(/\\/g, "\\\\").replace(/%/g, "\\%").replace(/_/g, "\\_");
}

export function csvEscapeCell(value: string): string {
  if (/[",\n\r]/.test(value)) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

export function rowToApi(r: {
  id: bigint;
  received_at: Date;
  topic: string;
  device_id: string | null;
  payload: unknown;
}) {
  return {
    id: r.id.toString(),
    received_at: r.received_at.toISOString(),
    topic: r.topic,
    device_id: r.device_id,
    payload: r.payload,
  };
}
