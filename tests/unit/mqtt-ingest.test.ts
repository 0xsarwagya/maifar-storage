import { describe, expect, test } from "bun:test";
import {
  extractDeviceOnlineState,
  forwardNormalizedPayloadToOvok,
  normalizePayloadForStorage,
  parsePayloadTextForStorage,
  resolveOvokIngestUrl,
  sanitizePayloadForStorage,
} from "../../src/mqtt-ingest";
import {
  getOvokForwardMetricsSnapshot,
  resetOvokForwardMetricsForTests,
} from "../../src/ovok-forward-metrics";

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

describe("mqtt ingest online state parsing", () => {
  test("parses boolean-like online control payloads", () => {
    expect(extractDeviceOnlineState({ online: "0" })).toBe(false);
    expect(extractDeviceOnlineState({ online: 1 })).toBe(true);
    expect(extractDeviceOnlineState({ isOnline: false })).toBe(false);
    expect(extractDeviceOnlineState({ online: "offline" })).toBe(false);
    expect(extractDeviceOnlineState({ method: "lwt" })).toBeNull();
    expect(extractDeviceOnlineState("online=0")).toBeNull();
  });
});

describe("mqtt ingest presence normalization", () => {
  test("normalizes presence payload to observation format", () => {
    const out = normalizePayloadForStorage(
      "devices/acme-1/presence",
      {
        presence: "1",
        timestamp: "2025-08-20T16:53:44+00:00",
      },
      "2500005",
      new Date("2025-08-20T16:53:45.000Z"),
    ) as Record<string, unknown>;

    expect(out.resourceType).toBe("Observation");
    expect(out.status).toBe("final");
    expect(out.effectiveInstant).toBe("2025-08-20T16:53:44.000Z");
    expect((out.device as { reference: string }).reference).toBe(
      "Device/2500005",
    );
    expect(
      (
        out.valueSampledData as {
          data: string;
          period: number;
          dimensions: number;
        }
      ).data,
    ).toBe("1");
    expect(
      (out.valueSampledData as { period: number; dimensions: number }).period,
    ).toBe(30000);
    expect(
      (
        out.valueSampledData as { period: number; dimensions: number }
      ).dimensions,
    ).toBe(1);
  });

  test("leaves non-presence payload unchanged", () => {
    const payload = { method: "set", LightLimitGet: "[60,60,60,60]" };
    const out = normalizePayloadForStorage(
      "MC01/Server/2500005",
      payload,
      "2500005",
      new Date("2025-08-20T16:53:45.000Z"),
    );
    expect(out).toEqual(payload);
  });

  test("does not remap payload already in observation format", () => {
    const payload = {
      resourceType: "Observation",
      status: "final",
      valueSampledData: { data: "1" },
    };
    const out = normalizePayloadForStorage(
      "devices/acme-1/presence",
      payload,
      "2500005",
      new Date("2025-08-20T16:53:45.000Z"),
    );
    expect(out).toEqual(payload);
  });

  test("normalizes MC01 someoneExists presence field", () => {
    const out = normalizePayloadForStorage(
      "MC01/Client/200d3d2c0109",
      { method: "post", someoneExists: "1" },
      "200d3d2c0109",
      new Date("2026-04-18T14:00:00.000Z"),
    ) as Record<string, unknown>;
    expect(out.resourceType).toBe("Observation");
    expect((out.code as { coding: Array<{ code: string }> }).coding[0]?.code).toBe(
      "presence-detection",
    );
    expect((out.valueSampledData as { data: string }).data).toBe("1");
  });
});

describe("mqtt ingest sleep status normalization", () => {
  test("normalizes sleep status payload to observation format", () => {
    const out = normalizePayloadForStorage(
      "devices/acme-1/sleep-status",
      {
        sleepStatus: "1",
        timestamp: "2025-08-20T17:00:00+00:00",
      },
      "2500005",
      new Date("2025-08-20T17:00:01.000Z"),
    ) as Record<string, unknown>;

    expect(out.resourceType).toBe("Observation");
    expect(out.status).toBe("final");
    expect(out.valueString).toBe("awake");
    expect(
      (
        out.valueSampledData as {
          data: string;
          period: number;
          dimensions: number;
        }
      ).data,
    ).toBe("1");
    expect((out.code as { coding: Array<{ code: string }> }).coding[0]?.code).toBe(
      "107145-5",
    );
    expect((out.valueSampledData as { period: number }).period).toBe(30000);
  });

  test("maps asleep and invalid status values", () => {
    const asleep = normalizePayloadForStorage(
      "devices/acme-1/sleep",
      { sleep_status: 0 },
      "2500005",
      new Date("2025-08-20T17:00:01.000Z"),
    ) as Record<string, unknown>;
    expect(asleep.valueString).toBe("asleep");
    expect((asleep.valueSampledData as { data: string }).data).toBe("0");

    const invalid = normalizePayloadForStorage(
      "devices/acme-1/sleep",
      { sleep: "-1" },
      "2500005",
      new Date("2025-08-20T17:00:01.000Z"),
    ) as Record<string, unknown>;
    expect(invalid.valueString).toBe("invalid");
    expect((invalid.valueSampledData as { data: string }).data).toBe("-1");
  });
});

describe("mqtt ingest realtime HR/BR normalization", () => {
  test("normalizes realtime heart-rate payload", () => {
    const out = normalizePayloadForStorage(
      "devices/acme-1/realtime_hr",
      {
        hr: [86.4, 74.9, "E", 62.2],
        timestamp: "2025-08-20T16:27:03+00:00",
      },
      "2500001",
      new Date("2025-08-20T16:27:04.000Z"),
    ) as Record<string, unknown>;

    expect(out.resourceType).toBe("Observation");
    expect((out.code as { coding: Array<{ code: string }> }).coding[0]?.code).toBe(
      "8867-4",
    );
    expect(
      (
        out.valueSampledData as {
          origin: { unit: string };
          period: number;
          data: string;
        }
      ).origin.unit,
    ).toBe("beats/min");
    expect(
      (out.valueSampledData as { origin: { unit: string }; period: number; data: string })
        .period,
    ).toBe(5000);
    expect(
      (out.valueSampledData as { origin: { unit: string }; period: number; data: string })
        .data,
    ).toBe("86.4 74.9 E 62.2");
    expect(
      (
        out.component as Array<{
          valueQuantity: { value: number; unit: string };
        }>
      )[0]?.valueQuantity.unit,
    ).toBe("beats/min");
  });

  test("normalizes realtime breathing-rate payload", () => {
    const out = normalizePayloadForStorage(
      "devices/acme-1/realtime-br",
      {
        br: "17.7 E 12.7 15.8",
        timestamp: "2025-08-20T16:27:16+00:00",
      },
      "2500001",
      new Date("2025-08-20T16:27:17.000Z"),
    ) as Record<string, unknown>;

    expect(out.resourceType).toBe("Observation");
    expect((out.code as { coding: Array<{ code: string }> }).coding[0]?.code).toBe(
      "9279-1",
    );
    expect(
      (
        out.valueSampledData as {
          origin: { unit: string };
          period: number;
          data: string;
        }
      ).origin.unit,
    ).toBe("breaths/min");
    expect(
      (out.valueSampledData as { origin: { unit: string }; period: number; data: string })
        .data,
    ).toBe("17.7 E 12.7 15.8");
    expect(
      (
        out.device as {
          reference: string;
        }
      ).reference,
    ).toBe("Device/2500001");
  });

  test("normalizes MC01-style heartRateValue and breathValue payload keys", () => {
    const hr = normalizePayloadForStorage(
      "MC01/Client/200d3d2c0109",
      {
        method: "post",
        heartRateValue: 72,
      },
      "200d3d2c0109",
      new Date("2026-04-18T14:00:00.000Z"),
    ) as Record<string, unknown>;

    expect((hr.code as { coding: Array<{ code: string }> }).coding[0]?.code).toBe(
      "8867-4",
    );
    expect((hr.valueSampledData as { data: string }).data).toBe("72");

    const br = normalizePayloadForStorage(
      "MC01/Client/200d3d2c0109",
      {
        method: "post",
        breathValue: 15,
      },
      "200d3d2c0109",
      new Date("2026-04-18T14:00:05.000Z"),
    ) as Record<string, unknown>;

    expect((br.code as { coding: Array<{ code: string }> }).coding[0]?.code).toBe(
      "9279-1",
    );
    expect((br.valueSampledData as { data: string }).data).toBe("15");
  });
});

describe("mqtt ingest environment normalization", () => {
  test("normalizes room temperature from attributeName/value payloads", () => {
    const out = normalizePayloadForStorage(
      "MC01/Client/200d3d2c0109",
      {
        method: "post",
        attributeName: "temperature",
        value: 23.5,
        timestamp: 1713535200000,
      },
      "200d3d2c0109",
      new Date("2026-04-18T14:00:00.000Z"),
    ) as Record<string, unknown>;

    expect(out.resourceType).toBe("Observation");
    expect((out.code as { coding: Array<{ code: string }> }).coding[0]?.code).toBe(
      "room_temperature",
    );
    expect((out.valueQuantity as { value: number; code: string }).value).toBe(23.5);
    expect((out.valueQuantity as { value: number; code: string }).code).toBe("Cel");
    expect(out.effectiveInstant).toBe("2024-04-19T14:00:00.000Z");
  });

  test("normalizes ambient light with illuminance and RGB color", () => {
    const out = normalizePayloadForStorage(
      "devices/acme-1/ambient-light",
      {
        IR: 320,
        RGB: [18, 52, 86],
        timestamp: "2026-04-18T14:00:00.000Z",
      },
      "2500005",
      new Date("2026-04-18T14:00:01.000Z"),
    ) as Record<string, unknown>;

    expect(out.resourceType).toBe("Observation");
    expect((out.code as { coding: Array<{ code: string }> }).coding[0]?.code).toBe(
      "ambient_light",
    );
    expect(
      (
        out.component as Array<{
          valueQuantity?: { value: number; unit: string };
          valueString?: string;
        }>
      )[0]?.valueQuantity?.value,
    ).toBe(320);
    expect(
      (
        out.component as Array<{
          valueQuantity?: { value: number; unit: string };
          valueString?: string;
        }>
      )[1]?.valueString,
    ).toBe("#123456");
  });
});

describe("mqtt ingest batch normalization", () => {
  test("normalizes batch presence payload with effectivePeriod", () => {
    const out = normalizePayloadForStorage(
      "devices/acme-1/batch-presence",
      {
        batch_presence: "E 0 1 1",
        start: "2025-08-20T16:27:00+00:00",
        end: "2025-08-20T16:28:30+00:00",
      },
      "2500001",
      new Date("2025-08-20T16:29:00.000Z"),
    ) as Record<string, unknown>;

    expect(out.resourceType).toBe("Observation");
    expect((out.code as { coding: Array<{ code: string }> }).coding[0]?.code).toBe(
      "presence-detection",
    );
    expect(
      (
        out.effectivePeriod as {
          start: string;
          end: string;
        }
      ).start,
    ).toBe("2025-08-20T16:27:00.000Z");
    expect((out.valueSampledData as { data: string }).data).toBe("E 0 1 1");
  });

  test("normalizes batch sleep payload with loinc code", () => {
    const out = normalizePayloadForStorage(
      "devices/acme-1/batch-sleep",
      {
        batch_sleep: [1, null, 0, -1],
        start: "2025-08-20T16:49:55+00:00",
        end: "2025-08-20T16:51:25+00:00",
      },
      "2500002",
      new Date("2025-08-20T16:52:00.000Z"),
    ) as Record<string, unknown>;

    expect(out.resourceType).toBe("Observation");
    expect((out.code as { coding: Array<{ code: string }> }).coding[0]?.code).toBe(
      "107145-5",
    );
    expect((out.valueSampledData as { data: string }).data).toBe("1 E 0 -1");
    expect((out.device as { reference: string }).reference).toBe("Device/2500002");
  });

  test("defaults batch sleep sampled period to 30 seconds", () => {
    const out = normalizePayloadForStorage(
      "devices/acme-1/batch-sleep",
      {
        batch_sleep: [1, 0, -1],
        start: "2025-08-20T16:49:55+00:00",
        end: "2025-08-20T16:51:25+00:00",
      },
      "2500002",
      new Date("2025-08-20T16:52:00.000Z"),
    ) as Record<string, unknown>;

    expect((out.valueSampledData as { period: number }).period).toBe(30000);
  });

  test("normalizes batch breathing-rate payload without component", () => {
    const out = normalizePayloadForStorage(
      "devices/acme-1/batch-br",
      {
        batch_br: "17.7 E 12.7 15.8",
        start: "2025-08-20T16:27:16+00:00",
        end: "2025-08-20T16:27:31+00:00",
      },
      "2500001",
      new Date("2025-08-20T16:28:00.000Z"),
    ) as Record<string, unknown>;

    expect((out.code as { coding: Array<{ code: string }> }).coding[0]?.code).toBe(
      "9279-1",
    );
    expect((out.valueSampledData as { data: string }).data).toBe("17.7 E 12.7 15.8");
    expect(out.component).toBeUndefined();
    expect(out.effectivePeriod).toEqual({
      start: "2025-08-20T16:27:16.000Z",
      end: "2025-08-20T16:27:31.000Z",
    });
  });

  test("normalizes batch heart-rate payload without component", () => {
    const out = normalizePayloadForStorage(
      "devices/acme-1/batch-hr",
      {
        batch_hr: [86.4, 74.9, "E", 62.2],
        start: "2025-08-20T16:27:03+00:00",
        end: "2025-08-20T16:27:18+00:00",
      },
      "2500001",
      new Date("2025-08-20T16:28:00.000Z"),
    ) as Record<string, unknown>;

    expect((out.code as { coding: Array<{ code: string }> }).coding[0]?.code).toBe(
      "8867-4",
    );
    expect((out.valueSampledData as { data: string }).data).toBe("86.4 74.9 E 62.2");
    expect(out.component).toBeUndefined();
    expect(out.effectivePeriod).toEqual({
      start: "2025-08-20T16:27:03.000Z",
      end: "2025-08-20T16:27:18.000Z",
    });
  });

  test("uses 1 hour effective period for presence batch fallback", () => {
    const out = normalizePayloadForStorage(
      "devices/acme-1/batch-presence",
      { batch_presence: "1 0 1", period: 3600000 },
      "2500001",
      new Date("2025-08-20T16:30:00.000Z"),
    ) as Record<string, unknown>;
    expect(out.effectivePeriod).toEqual({
      start: "2025-08-20T15:30:00.000Z",
      end: "2025-08-20T16:30:00.000Z",
    });
    expect((out.valueSampledData as { period: number }).period).toBe(3600000);
  });

  test("defaults presence batch sampled period to 30 seconds", () => {
    const out = normalizePayloadForStorage(
      "devices/acme-1/batch-presence",
      {
        batch_presence: "1 0 1",
        start: "2025-08-20T15:30:00+00:00",
        end: "2025-08-20T16:30:00+00:00",
      },
      "2500001",
      new Date("2025-08-20T16:30:00.000Z"),
    ) as Record<string, unknown>;

    expect((out.valueSampledData as { period: number }).period).toBe(30000);
  });

  test("uses previous 9AM UTC window for vitals batch fallback", () => {
    const out = normalizePayloadForStorage(
      "devices/acme-1/batch-br",
      { batch_br: "12 13 14" },
      "2500001",
      new Date("2025-08-20T10:05:00.000Z"),
    ) as Record<string, unknown>;
    expect(out.effectivePeriod).toEqual({
      start: "2025-08-19T09:00:00.000Z",
      end: "2025-08-20T09:00:00.000Z",
    });
  });
});

describe("mqtt ingest batch statistics normalization", () => {
  test("builds batch-statistics bundle from raw stats payload", () => {
    const out = normalizePayloadForStorage(
      "devices/acme-1/batch-statistics",
      {
        timestamp: "2025-09-03T05:17:51+00:00",
        start: "2025-09-03T05:17:04+00:00",
        end: "2025-09-03T13:17:04+00:00",
        hrStats: {
          mean: 75,
          median: 75,
          max: 122.3,
          min: 32.8,
          p90: 87.8,
          p10: 62.2,
          coverage: 90.001,
        },
        brStats: {
          mean: 12.5,
          median: 12.5,
          max: 25.9,
          min: 0,
          p90: 16.3,
          p10: 8.7,
          coverage: 90.02,
        },
        sleepStats: {
          TST: 6.4,
          SE: 80,
          WASO: 44.5,
          TBT: 8,
          SleepStartTime: "2025-09-02T21:31:44+00:00",
          SleepEndTime: "2025-09-03T05:10:36+00:00",
          AwakeningCount: 7,
          AwakeningDuration: 5.2,
          SOL: 14.1,
          OBC: 0,
          WUL: 7.1,
        },
      },
      "250000",
      new Date("2025-09-03T05:17:51.000Z"),
    ) as Record<string, unknown>;

    expect(out.resourceType).toBe("Bundle");
    expect(out.type).toBe("collection");
    const entry = out.entry as Array<{ resource: Record<string, unknown> }>;
    expect(entry.length).toBe(3);
    const sleepObs = entry.find(
      (e) =>
        (
          (e.resource.code as { coding: Array<{ code: string }> }).coding[0] ?? {}
        ).code === "sleep-stats",
    )?.resource;
    expect(sleepObs).toBeDefined();
    const components = (sleepObs?.component ?? []) as Array<{
      code: { coding: Array<{ code: string }> };
      valueQuantity?: { value: number; unit: string };
    }>;
    const longest = components.find(
      (c) => (c.code.coding[0] ?? {}).code === "LongestSleepDuration",
    );
    expect(longest).toBeDefined();
    expect(longest?.valueQuantity?.unit).toBe("h");
    expect(longest?.valueQuantity?.value).toBeCloseTo(7.6478, 3);
  });

  test("overrides longest sleep duration using start/end in existing bundle", () => {
    const out = normalizePayloadForStorage(
      "devices/acme-1/batch-statistics",
      {
        resourceType: "Bundle",
        id: "bundle-1",
        type: "collection",
        entry: [
          {
            resource: {
              resourceType: "Observation",
              code: { coding: [{ code: "sleep-stats" }] },
              component: [
                {
                  code: { coding: [{ code: "SleepStartTime", display: "Sleep Start Time" }] },
                  valueDateTime: "2025-09-02T22:00:00+00:00",
                },
                {
                  code: { coding: [{ code: "SleepEndTime", display: "Sleep End Time" }] },
                  valueDateTime: "2025-09-03T04:30:00+00:00",
                },
                {
                  code: {
                    coding: [{ code: "LongestSleepDuration", display: "Longest Sleep Duration" }],
                  },
                  valueQuantity: { value: 1, unit: "h" },
                },
              ],
            },
          },
        ],
      },
      "250000",
      new Date("2025-09-03T05:17:51.000Z"),
    ) as Record<string, unknown>;

    const entry = out.entry as Array<{ resource: Record<string, unknown> }>;
    const sleepObs = entry[0]?.resource;
    const components = (sleepObs.component ?? []) as Array<{
      code: { coding: Array<{ code: string }> };
      valueQuantity?: { value: number; unit: string };
    }>;
    const longest = components.find(
      (c) => (c.code.coding[0] ?? {}).code === "LongestSleepDuration",
    );
    expect(longest).toBeDefined();
    expect(longest?.valueQuantity?.value).toBeCloseTo(6.5, 3);
  });

  test("uses previous 9AM UTC window for statistics batch fallback", () => {
    const out = normalizePayloadForStorage(
      "devices/acme-1/batch-statistics",
      {
        hrStats: { mean: 70, median: 71 },
      },
      "250000",
      new Date("2025-09-03T08:30:00.000Z"),
    ) as Record<string, unknown>;

    const entry = out.entry as Array<{ resource: Record<string, unknown> }>;
    const hr = entry[0]?.resource;
    expect(hr.effectivePeriod).toEqual({
      start: "2025-09-01T09:00:00.000Z",
      end: "2025-09-02T09:00:00.000Z",
    });
  });
});

describe("mqtt ingest ovok forwarding", () => {
  test("resolves observation and bundle ingest URLs", () => {
    expect(
      resolveOvokIngestUrl("https://api.dev.ovok.com", {
        resourceType: "Observation",
      }),
    ).toBe("https://api.dev.ovok.com/v1/ingest/fhir");
    expect(
      resolveOvokIngestUrl("https://api.dev.ovok.com", {
        resourceType: "Observation",
        code: {
          coding: [
            {
              system: "https://api.ovok.com/StructuredDefinition",
              code: "room_temperature",
            },
          ],
        },
      }),
    ).toBe("https://api.dev.ovok.com/v1/ingest/environment");
    expect(
      resolveOvokIngestUrl("https://api.dev.ovok.com/", {
        resourceType: "Bundle",
      }),
    ).toBe("https://api.dev.ovok.com/v1/ingest/fhir/bundle");
    expect(resolveOvokIngestUrl("https://api.dev.ovok.com", { x: 1 })).toBeNull();
  });

  test("forwards normalized observation payload to ovok ingest", async () => {
    resetOvokForwardMetricsForTests();
    const originalFetch = globalThis.fetch;
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    globalThis.fetch = (async (url: string | URL | Request, init?: RequestInit) => {
      calls.push({ url: String(url), init });
      return new Response("", { status: 202 });
    }) as typeof fetch;

    try {
      await forwardNormalizedPayloadToOvok(
        "devices/acme-1/presence",
        { resourceType: "Observation", id: "obs-1" },
        {
          ovokIngestEnabled: true,
          ovokIngestBaseUrl: "https://api.dev.ovok.com",
          ovokIngestApiKey: "secret",
          ovokIngestApiKeyHeader: "x-api-key",
          ovokIngestTimeoutMs: 10000,
        },
      );
    } finally {
      globalThis.fetch = originalFetch;
    }

    expect(calls.length).toBe(1);
    expect(calls[0]?.url).toBe("https://api.dev.ovok.com/v1/ingest/fhir");
    expect((calls[0]?.init?.headers as Record<string, string>)["x-api-key"]).toBe(
      "secret",
    );
    const sentBody = JSON.parse(String(calls[0]?.init?.body)) as Record<string, unknown>;
    expect(
      (sentBody.device as { reference?: string } | undefined)?.reference,
    ).toBeUndefined();
    const metrics = getOvokForwardMetricsSnapshot();
    expect(metrics.requests_sent_total).toBe(1);
    expect(metrics.requests_failed_total).toBe(0);
    expect(metrics.bytes_sent_total).toBeGreaterThan(10);
  });

  test("forwards environment observations to the dedicated Ovok environment ingest route", async () => {
    resetOvokForwardMetricsForTests();
    const originalFetch = globalThis.fetch;
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    globalThis.fetch = (async (url: string | URL | Request, init?: RequestInit) => {
      calls.push({ url: String(url), init });
      return new Response("", { status: 202 });
    }) as typeof fetch;

    try {
      await forwardNormalizedPayloadToOvok(
        "devices/acme-1/environment",
        {
          resourceType: "Observation",
          code: {
            coding: [
              {
                system: "https://api.ovok.com/StructuredDefinition",
                code: "ambient_light",
              },
            ],
          },
          component: [],
          device: { reference: "Device/200d3d2c0109" },
        },
        {
          ovokIngestEnabled: true,
          ovokIngestBaseUrl: "https://api.dev.ovok.com",
          ovokIngestApiKey: undefined,
          ovokIngestApiKeyHeader: "x-api-key",
          ovokIngestTimeoutMs: 10000,
        },
      );
    } finally {
      globalThis.fetch = originalFetch;
    }

    expect(calls.length).toBe(1);
    expect(calls[0]?.url).toBe("https://api.dev.ovok.com/v1/ingest/environment");
    const sentBody = JSON.parse(String(calls[0]?.init?.body)) as Record<string, unknown>;
    expect((sentBody.device as { reference: string }).reference).toBe("Device/MC01-200d3d2c0109");
  });

  test("rewrites device references to Device/MC01-deviceId before forwarding", async () => {
    resetOvokForwardMetricsForTests();
    const originalFetch = globalThis.fetch;
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    globalThis.fetch = (async (url: string | URL | Request, init?: RequestInit) => {
      calls.push({ url: String(url), init });
      return new Response("", { status: 202 });
    }) as typeof fetch;

    try {
      await forwardNormalizedPayloadToOvok(
        "devices/acme-1/presence",
        {
          resourceType: "Bundle",
          entry: [
            { resource: { resourceType: "Observation", device: { reference: "Device/200d3d2c0109" } } },
            { resource: { resourceType: "Observation", device: { reference: "200d3d2c011f" } } },
            { resource: { resourceType: "Observation", device: { reference: "Device/MC01-200d3d2c011a" } } },
          ],
        },
        {
          ovokIngestEnabled: true,
          ovokIngestBaseUrl: "https://api.dev.ovok.com",
          ovokIngestApiKey: undefined,
          ovokIngestApiKeyHeader: "x-api-key",
          ovokIngestTimeoutMs: 10000,
        },
      );
    } finally {
      globalThis.fetch = originalFetch;
    }

    const sentBody = JSON.parse(String(calls[0]?.init?.body)) as {
      entry: Array<{ resource: { device: { reference: string } } }>;
    };
    expect(sentBody.entry[0]?.resource.device.reference).toBe("Device/MC01-200d3d2c0109");
    expect(sentBody.entry[1]?.resource.device.reference).toBe("Device/MC01-200d3d2c011f");
    expect(sentBody.entry[2]?.resource.device.reference).toBe("Device/MC01-200d3d2c011a");
  });

  test("forwards to secondary Ovok ingest base URL when configured", async () => {
    resetOvokForwardMetricsForTests();
    const originalFetch = globalThis.fetch;
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    globalThis.fetch = (async (url: string | URL | Request, init?: RequestInit) => {
      calls.push({ url: String(url), init });
      return new Response("", { status: 202 });
    }) as typeof fetch;

    try {
      await forwardNormalizedPayloadToOvok(
        "devices/acme-1/presence",
        { resourceType: "Observation", id: "obs-1" },
        {
          ovokIngestEnabled: true,
          ovokIngestBaseUrl: "https://api.dev.ovok.com",
          ovokIngestSecondaryBaseUrl: "https://api.staging.ovok.com",
          ovokIngestApiKey: undefined,
          ovokIngestApiKeyHeader: "x-api-key",
          ovokIngestTimeoutMs: 10000,
        },
      );
    } finally {
      globalThis.fetch = originalFetch;
    }

    expect(calls.map((call) => call.url)).toEqual([
      "https://api.dev.ovok.com/v1/ingest/fhir",
      "https://api.staging.ovok.com/v1/ingest/fhir",
    ]);
    const metrics = getOvokForwardMetricsSnapshot();
    expect(metrics.requests_sent_total).toBe(2);
    expect(metrics.requests_failed_total).toBe(0);
  });
});
