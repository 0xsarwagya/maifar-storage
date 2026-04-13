import { describe, expect, test } from "bun:test";
import {
  parsePayloadTextForStorage,
  sanitizePayloadForStorage,
} from "../../src/mqtt-ingest";

describe("mqtt ingest payload sanitization", () => {
  test("strips trailing NUL bytes before JSON parsing", () => {
    expect(
      parsePayloadTextForStorage('{"method":"lwt","online":"0"}\u0000'),
    ).toEqual({
      method: "lwt",
      online: "0",
    });
  });

  test("strips NUL characters from parsed JSON strings recursively", () => {
    expect(
      parsePayloadTextForStorage(
        '{"deviceId":"ab\\u0000cd","nested":["\\u0000x",{"value":"y\\u0000z"}]}',
      ),
    ).toEqual({
      deviceId: "abcd",
      nested: ["x", { value: "yz" }],
    });
  });

  test("strips NUL characters from non-JSON fallback strings", () => {
    expect(parsePayloadTextForStorage("plain\u0000text")).toBe("plaintext");
  });

  test("sanitizes arbitrary nested payload values", () => {
    expect(
      sanitizePayloadForStorage({
        a: "x\u0000y",
        b: ["\u0000z", { c: "m\u0000n" }],
      }),
    ).toEqual({
      a: "xy",
      b: ["z", { c: "mn" }],
    });
  });
});
