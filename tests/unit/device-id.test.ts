import { describe, expect, test } from "bun:test";
import { extractDeviceId } from "../../src/device-id";

const rx = /^devices\/([^/]+)\//;

describe("extractDeviceId", () => {
  test("from topic regex capture", () => {
    expect(extractDeviceId("devices/acme/telemetry", {}, rx)).toBe("acme");
  });

  test("regex mismatch uses json string key", () => {
    expect(
      extractDeviceId(
        "other/topic",
        { deviceId: "from-json" },
        rx,
        "deviceId",
      ),
    ).toBe("from-json");
  });

  test("json numeric key", () => {
    expect(
      extractDeviceId("x", { did: 7 }, /^nomatch/, "did"),
    ).toBe("7");
  });

  test("array payload skips json key", () => {
    expect(extractDeviceId("x", [1, 2], /^nomatch/, "id")).toBeNull();
  });

  test("no match", () => {
    expect(extractDeviceId("x/y", { a: 1 }, rx)).toBeNull();
  });
});
