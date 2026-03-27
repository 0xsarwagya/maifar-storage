import { describe, expect, test } from "bun:test";
import {
  csvEscapeCell,
  decodeCursor,
  DEFAULT_LIMIT,
  encodeCursor,
  escapeLikePrefix,
  MAX_LIMIT,
  parseIsoDate,
  parseLimit,
  rowToApi,
} from "../../src/format";

describe("format.parseIsoDate", () => {
  test("empty is undefined", () => {
    expect(parseIsoDate("from", null)).toEqual({ ok: true, value: undefined });
    expect(parseIsoDate("from", "")).toEqual({ ok: true, value: undefined });
  });

  test("valid ISO", () => {
    const r = parseIsoDate("from", "2024-01-15T12:00:00.000Z");
    expect(r.ok).toBe(true);
    if (r.ok) {
      expect(r.value?.toISOString()).toBe("2024-01-15T12:00:00.000Z");
    }
  });

  test("invalid", () => {
    const r = parseIsoDate("from", "not-a-date");
    expect(r.ok).toBe(false);
  });
});

describe("format.parseLimit", () => {
  test("default", () => {
    expect(parseLimit(null)).toEqual({ ok: true, value: DEFAULT_LIMIT });
    expect(parseLimit("")).toEqual({ ok: true, value: DEFAULT_LIMIT });
  });

  test("clamped to max", () => {
    expect(parseLimit("50000")).toEqual({ ok: true, value: MAX_LIMIT });
  });

  test("invalid", () => {
    expect(parseLimit("0").ok).toBe(false);
    expect(parseLimit("3.5").ok).toBe(false);
    expect(parseLimit("x").ok).toBe(false);
  });
});

describe("format.cursor", () => {
  test("roundtrip", () => {
    const c = {
      receivedAt: new Date("2024-06-01T00:00:00.000Z"),
      id: 42n,
    };
    const enc = encodeCursor(c);
    const dec = decodeCursor(enc);
    expect(dec.ok).toBe(true);
    if (dec.ok) {
      expect(dec.value.id).toBe(42n);
      expect(dec.value.receivedAt.toISOString()).toBe(c.receivedAt.toISOString());
    }
  });

  test("decode garbage", () => {
    expect(decodeCursor("@@@").ok).toBe(false);
  });

  test("decode bad timestamp inside json", () => {
    const bad = Buffer.from(JSON.stringify({ t: "x", id: "1" }), "utf8").toString(
      "base64url",
    );
    expect(decodeCursor(bad).ok).toBe(false);
  });
});

describe("format.escapeLikePrefix", () => {
  test("escapes special chars", () => {
    expect(escapeLikePrefix("a%b_c\\d")).toBe("a\\%b\\_c\\\\d");
  });
});

describe("format.csvEscapeCell", () => {
  test("plain passes through", () => {
    expect(csvEscapeCell("abc")).toBe("abc");
  });

  test("quotes and escapes", () => {
    expect(csvEscapeCell('say "hi"')).toBe('"say ""hi"""');
    expect(csvEscapeCell("a\nb")).toBe('"a\nb"');
    expect(csvEscapeCell("a,b")).toBe('"a,b"');
  });
});

describe("format.rowToApi", () => {
  test("serializes bigint id", () => {
    const out = rowToApi({
      id: 99n,
      received_at: new Date("2024-01-01T00:00:00.000Z"),
      topic: "t",
      device_id: "d",
      payload: { x: 1 },
    });
    expect(out.id).toBe("99");
    expect(out.device_id).toBe("d");
    expect(out.payload).toEqual({ x: 1 });
  });
});
