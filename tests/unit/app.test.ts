import { describe, expect, test } from "bun:test";
import { filterRowsForOvokForwarding } from "../../src/app";
import type { QueuedRow } from "../../src/types";

describe("filterRowsForOvokForwarding", () => {
  test("stops forwarding after online=0 until a later online=1", () => {
    const onlineStateByDevice = new Map<string, boolean>();
    const baseTime = new Date("2026-01-01T00:00:00.000Z").getTime();

    const rows: QueuedRow[] = [
      {
        topic: "devices/dev-1/lwt",
        receivedAt: new Date(baseTime + 1_000),
        deviceId: "dev-1",
        payload: { method: "lwt", online: "0" },
      },
      {
        topic: "devices/dev-1/br",
        receivedAt: new Date(baseTime + 2_000),
        deviceId: "dev-1",
        payload: { resourceType: "Observation", id: "suppressed" },
      },
      {
        topic: "devices/dev-1/lwt",
        receivedAt: new Date(baseTime + 3_000),
        deviceId: "dev-1",
        payload: { method: "lwt", online: "1" },
      },
      {
        topic: "devices/dev-1/hr",
        receivedAt: new Date(baseTime + 4_000),
        deviceId: "dev-1",
        payload: { resourceType: "Observation", id: "forwarded" },
      },
    ];

    const forwardedRows = filterRowsForOvokForwarding(rows, onlineStateByDevice);

    expect(forwardedRows.map((row) => row.topic)).toEqual(["devices/dev-1/hr"]);
    expect((forwardedRows[0]?.payload as { id: string }).id).toBe("forwarded");
    expect(onlineStateByDevice.get("dev-1")).toBe(true);
  });

  test("keeps offline state across batches", () => {
    const onlineStateByDevice = new Map<string, boolean>();
    const offlineBatch: QueuedRow[] = [
      {
        topic: "devices/dev-2/lwt",
        receivedAt: new Date("2026-01-01T00:00:01.000Z"),
        deviceId: "dev-2",
        payload: { method: "lwt", online: "0" },
      },
    ];
    const telemetryBatch: QueuedRow[] = [
      {
        topic: "devices/dev-2/presence",
        receivedAt: new Date("2026-01-01T00:00:02.000Z"),
        deviceId: "dev-2",
        payload: { resourceType: "Observation", id: "presence" },
      },
    ];

    expect(filterRowsForOvokForwarding(offlineBatch, onlineStateByDevice)).toEqual([]);
    expect(filterRowsForOvokForwarding(telemetryBatch, onlineStateByDevice)).toEqual([]);
    expect(onlineStateByDevice.get("dev-2")).toBe(false);
  });
});
