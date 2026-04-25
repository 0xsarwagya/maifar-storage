import { describe, expect, test } from "bun:test";
import {
  buildRealtimeObservationForKind,
  buildInvalidObservationForKind,
  classifyScheduledPayloadKind,
  enrichStatsPayloadWithEnvironmentAverages,
  hasPresenceSignal,
  OVOK_FORWARD_CRON,
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

    expect(
      classifyScheduledPayloadKind({
        resourceType: "Observation",
        code: { coding: [{ code: "room_temperature" }] },
        effectiveInstant: "2026-01-01T00:00:00.000Z",
      }),
    ).toBe("roomTemperatureRealtime");
  });
});

describe("buildInvalidObservationForKind", () => {
  test("documents the ovok forwarding cron cadence", () => {
    expect(OVOK_FORWARD_CRON.presenceRealtime).toBe("*/30 * * * * *");
    expect(OVOK_FORWARD_CRON.sleepRealtime).toBe("*/10 * * * *");
    expect(OVOK_FORWARD_CRON.vitalsRealtime).toBe("*/10 * * * *");
    expect(OVOK_FORWARD_CRON.dailyBatch).toBe("0 9 * * *");
  });

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

  test("uses webhook-compatible sampled periods for batch fallback payloads", () => {
    const dayWindow = {
      start: new Date("2026-01-01T09:00:00.000Z"),
      end: new Date("2026-01-02T09:00:00.000Z"),
    };

    const presence = buildInvalidObservationForKind(
      "presenceBatch",
      { resourceType: "Observation" },
      "dev-1",
      dayWindow,
    );
    expect(
      (presence.valueSampledData as Record<string, unknown>).period,
    ).toBe(30000);
    expect(presence.effectivePeriod).toEqual({
      start: "2026-01-01T09:00:00.000Z",
      end: "2026-01-02T09:00:00.000Z",
    });

    const sleep = buildInvalidObservationForKind(
      "sleepBatch",
      { resourceType: "Observation" },
      "dev-1",
      dayWindow,
    );
    expect((sleep.valueSampledData as Record<string, unknown>).period).toBe(
      30000,
    );

    const heartRate = buildInvalidObservationForKind(
      "heartRateBatch",
      { resourceType: "Observation" },
      "dev-1",
      dayWindow,
    );
    expect(
      (heartRate.valueSampledData as Record<string, unknown>).period,
    ).toBe(5000);
  });
});

describe("buildRealtimeObservationForKind", () => {
  const window = {
    start: new Date("2026-01-01T00:00:00.000Z"),
    end: new Date("2026-01-01T00:10:00.000Z"),
  };

  test("sends presence zero when the device is active but radar has no presence or sleep", () => {
    const payload = buildRealtimeObservationForKind(
      "presenceRealtime",
      "dev-1",
      [
        {
          topic: "devices/dev-1/hr",
          receivedAt: new Date("2026-01-01T00:05:00.000Z"),
          payload: { heartRateValue: 72 },
        },
      ],
      window,
    );

    expect(payload).not.toBeNull();
    expect((payload?.valueSampledData as Record<string, unknown>).data).toBe("0");
  });

  test("forces presence true when recent sleep data exists", () => {
    const payload = buildRealtimeObservationForKind(
      "presenceRealtime",
      "dev-1",
      [
        {
          topic: "devices/dev-1/sleep",
          receivedAt: new Date("2026-01-01T00:05:00.000Z"),
          payload: { sleep: 0 },
        },
      ],
      window,
    );

    expect(payload).not.toBeNull();
    expect((payload?.valueSampledData as Record<string, unknown>).data).toBe("1");
  });

  test("suppresses realtime forwarding when the latest online state is offline", () => {
    const payload = buildRealtimeObservationForKind(
      "breathingRateRealtime",
      "dev-1",
      [
        {
          topic: "devices/dev-1/lwt",
          receivedAt: new Date("2026-01-01T00:04:00.000Z"),
          payload: { method: "lwt", online: "0" },
        },
        {
          topic: "devices/dev-1/br",
          receivedAt: new Date("2026-01-01T00:05:00.000Z"),
          payload: { breathValue: 12 },
        },
      ],
      window,
    );

    expect(payload).toBeNull();
  });

  test("only uses telemetry from the current online session after reconnect", () => {
    const payload = buildRealtimeObservationForKind(
      "heartRateRealtime",
      "dev-1",
      [
        {
          topic: "devices/dev-1/lwt",
          receivedAt: new Date("2026-01-01T00:03:00.000Z"),
          payload: { method: "lwt", online: "0" },
        },
        {
          topic: "devices/dev-1/hr",
          receivedAt: new Date("2026-01-01T00:04:00.000Z"),
          payload: { heartRateValue: 60 },
        },
        {
          topic: "devices/dev-1/lwt",
          receivedAt: new Date("2026-01-01T00:05:00.000Z"),
          payload: { method: "lwt", online: "1" },
        },
        {
          topic: "devices/dev-1/hr",
          receivedAt: new Date("2026-01-01T00:06:00.000Z"),
          payload: { heartRateValue: 72 },
        },
      ],
      window,
    );

    expect(payload).not.toBeNull();
    expect((payload?.valueSampledData as Record<string, unknown>).data).toBe("72");
  });

  test("aggregates realtime sleep samples and adds an average component", () => {
    const payload = buildRealtimeObservationForKind(
      "sleepRealtime",
      "dev-1",
      [
        {
          topic: "devices/dev-1/sleep",
          receivedAt: new Date("2026-01-01T00:00:30.000Z"),
          payload: { sleep: 0 },
        },
        {
          topic: "devices/dev-1/sleep",
          receivedAt: new Date("2026-01-01T00:01:00.000Z"),
          payload: { sleep: 1 },
        },
      ],
      window,
    );

    expect(payload).not.toBeNull();
    expect((payload?.valueSampledData as Record<string, unknown>).data).toBe("0 1");
    expect(
      (
        payload?.component as Array<{
          valueQuantity: { value: number };
        }>
      )[0]?.valueQuantity.value,
    ).toBe(0.5);
  });

  test("aggregates realtime breathing samples into 10-minute sampled data", () => {
    const payload = buildRealtimeObservationForKind(
      "breathingRateRealtime",
      "dev-1",
      [
        {
          topic: "devices/dev-1/br",
          receivedAt: new Date("2026-01-01T00:00:05.000Z"),
          payload: { breathValue: 12 },
        },
        {
          topic: "devices/dev-1/br",
          receivedAt: new Date("2026-01-01T00:00:10.000Z"),
          payload: { breathValue: 14 },
        },
      ],
      window,
    );

    expect(payload).not.toBeNull();
    expect((payload?.valueSampledData as Record<string, unknown>).data).toBe("12 14");
    expect((payload?.valueSampledData as Record<string, unknown>).period).toBe(5000);
    expect(
      (
        payload?.component as Array<{
          valueQuantity: { value: number };
        }>
      )[0]?.valueQuantity.value,
    ).toBe(13);
  });

  test("returns null when the device has no recent data", () => {
    const payload = buildRealtimeObservationForKind(
      "presenceRealtime",
      "dev-1",
      [],
      window,
    );

    expect(payload).toBeNull();
  });
});

describe("enrichStatsPayloadWithEnvironmentAverages", () => {
  test("adds room temperature and ambient light averages to sleep stats", () => {
    const payload = enrichStatsPayloadWithEnvironmentAverages(
      {
        resourceType: "Bundle",
        type: "collection",
        entry: [
          {
            resource: {
              resourceType: "Observation",
              code: { coding: [{ code: "sleep-stats" }] },
              component: [
                {
                  code: { coding: [{ code: "TST" }] },
                  valueQuantity: { value: 7.5, unit: "h" },
                },
              ],
            },
          },
        ],
      },
      "dev-1",
      [
        {
          topic: "devices/dev-1/temperature",
          receivedAt: new Date("2026-01-01T00:05:00.000Z"),
          payload: { temperature: 22 },
        },
        {
          topic: "devices/dev-1/temperature",
          receivedAt: new Date("2026-01-01T00:15:00.000Z"),
          payload: { temperature: 24 },
        },
        {
          topic: "devices/dev-1/ambient-light",
          receivedAt: new Date("2026-01-01T00:20:00.000Z"),
          payload: { illuminance: 300 },
        },
        {
          topic: "devices/dev-1/ambient-light",
          receivedAt: new Date("2026-01-01T00:30:00.000Z"),
          payload: { illuminance: 500 },
        },
      ],
    ) as {
      entry: Array<{
        resource: {
          component: Array<{
            code: { coding: Array<{ code: string }> };
            valueQuantity?: { value: number; unit: string };
          }>;
        };
      }>;
    };

    expect(payload.entry).toHaveLength(1);

    const components = payload.entry[0]?.resource.component ?? [];
    const roomTemperature = components.find((component) =>
      component.code.coding.some((coding) => coding.code === "room_temperature"),
    );
    const illuminance = components.find((component) =>
      component.code.coding.some((coding) => coding.code === "illuminance"),
    );

    expect(roomTemperature?.valueQuantity).toMatchObject({
      value: 23,
      unit: "°C",
      system: "http://unitsofmeasure.org",
      code: "Cel",
    });
    expect(illuminance?.valueQuantity).toEqual({
      value: 400,
      unit: "lux",
    });
  });
});
