import { describe, expect, test } from "bun:test";
import {
  buildInvalidObservationForKind,
  classifyScheduledPayloadKind,
  hasPresenceSignal,
} from "../../src/ovok-scheduler";

describe("classifyScheduledPayloadKind", () => {
  test("classifies realtime presence and batch stats", () => {
    expect(
      classifyScheduledPayloadKind({
        resourceType: "Observation",
        code: { coding: [{ code: "presence-detection" }] },
        effectiveInstant: "2026-01-01T00:00:00.000Z",
      }),
    ).toBe("presenceRealtime");

    expect(classifyScheduledPayloadKind({ resourceType: "Bundle" })).toBe(
      "statsBatch",
    );
  });

  test("classifies vitals realtime and batch by effective field", () => {
    expect(
      classifyScheduledPayloadKind({
        resourceType: "Observation",
        code: { coding: [{ code: "8867-4" }] },
        effectiveInstant: "2026-01-01T00:00:00.000Z",
      }),
    ).toBe("heartRateRealtime");
    expect(
      classifyScheduledPayloadKind({
        resourceType: "Observation",
        code: { coding: [{ code: "8867-4" }] },
        effectivePeriod: {
          start: "2026-01-01T00:00:00.000Z",
          end: "2026-01-01T01:00:00.000Z",
        },
      }),
    ).toBe("heartRateBatch");
    expect(
      classifyScheduledPayloadKind({
        resourceType: "Observation",
        code: { coding: [{ code: "107145-5" }] },
        effectiveInstant: "2026-01-01T00:00:00.000Z",
      }),
    ).toBe("sleepRealtime");
  });
});

describe("buildInvalidObservationForKind", () => {
  test("treats E-only presence sampled data as missing signal", () => {
    expect(
      hasPresenceSignal({
        resourceType: "Observation",
        valueSampledData: { data: "E E E" },
      }),
    ).toBe(false);
    expect(
      hasPresenceSignal({
        resourceType: "Observation",
        valueSampledData: { data: "0 E 1" },
      }),
    ).toBe(true);
  });

  test("uses zero for invalid presence fallback", () => {
    const payload = buildInvalidObservationForKind(
      "presenceRealtime",
      {
        resourceType: "Observation",
        valueSampledData: { data: "1" },
      },
      "dev-1",
      {
        start: new Date("2026-01-01T00:00:00.000Z"),
        end: new Date("2026-01-01T00:00:30.000Z"),
      },
    );

    expect((payload.valueSampledData as Record<string, unknown>).data).toBe("0");
  });

  test("forces presence zero even when template lacks sampled data", () => {
    const payload = buildInvalidObservationForKind(
      "presenceRealtime",
      {
        resourceType: "Observation",
        code: { coding: [{ code: "presence-detection" }] },
      },
      "dev-1",
      {
        start: new Date("2026-01-01T00:00:00.000Z"),
        end: new Date("2026-01-01T00:00:30.000Z"),
      },
    );

    expect((payload.valueSampledData as Record<string, unknown>).data).toBe("0");
  });

  test("forces invalid sleep realtime values", () => {
    const payload = buildInvalidObservationForKind(
      "sleepRealtime",
      {
        resourceType: "Observation",
        code: { coding: [{ code: "sleep-status" }] },
        valueSampledData: { data: "1" },
        valueString: "awake",
      },
      "dev-1",
      {
        start: new Date("2026-01-01T00:00:00.000Z"),
        end: new Date("2026-01-01T00:10:00.000Z"),
      },
    );

    expect(payload.valueString).toBe("invalid");
    expect((payload.valueSampledData as Record<string, unknown>).data).toBe("-1");
    expect((payload.code as { coding: Array<{ code: string }> }).coding[0]?.code).toBe(
      "107145-5",
    );
    expect(payload.effectiveInstant).toBe("2026-01-01T00:10:00.000Z");
    expect(payload.effectivePeriod).toBeUndefined();
  });

  test("drops realtime vital components for invalid fallback", () => {
    const payload = buildInvalidObservationForKind(
      "heartRateRealtime",
      {
        resourceType: "Observation",
        valueSampledData: { data: "80" },
        component: [{ valueQuantity: { value: 80 } }],
      },
      "dev-1",
      {
        start: new Date("2026-01-01T00:00:00.000Z"),
        end: new Date("2026-01-01T00:10:00.000Z"),
      },
    );

    expect((payload.valueSampledData as Record<string, unknown>).data).toBe("E");
    expect(payload.component).toBeUndefined();
  });
});
