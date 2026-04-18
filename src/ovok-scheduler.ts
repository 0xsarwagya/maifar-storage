import cron, { type ScheduledTask } from "node-cron";
import type { AppConfig } from "./config";
import type { Sql } from "./db";
import { forwardNormalizedPayloadToOvok } from "./mqtt-ingest";

const DAY_MS = 24 * 60 * 60 * 1000;

const PRESENCE_REALTIME_PERIOD_MS = 30_000;
const SLEEP_REALTIME_PERIOD_MS = 600_000;
const VITALS_REALTIME_PERIOD_MS = 600_000;
const PRESENCE_BATCH_PERIOD_MS = 3_600_000;
const SLEEP_BATCH_PERIOD_MS = 3_600_000;
const DAILY_BATCH_PERIOD_MS = DAY_MS;

type JsonRecord = Record<string, unknown>;

type ObservationKind =
  | "presenceRealtime"
  | "sleepRealtime"
  | "heartRateRealtime"
  | "breathingRateRealtime"
  | "presenceBatch"
  | "sleepBatch"
  | "heartRateBatch"
  | "breathingRateBatch";

export type ScheduledPayloadKind = ObservationKind | "statsBatch";

type TimeWindow = {
  start: Date;
  end: Date;
};

type SchedulerOptions = Pick<
  AppConfig,
  | "ovokIngestEnabled"
  | "ovokIngestBaseUrl"
  | "ovokIngestApiKey"
  | "ovokIngestApiKeyHeader"
  | "ovokIngestTimeoutMs"
>;

type SchedulerHandle = {
  stop: () => void;
};

function isObjectRecord(value: unknown): value is JsonRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function deepClone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function nowWindow(periodMs: number): TimeWindow {
  const end = new Date();
  return { start: new Date(end.getTime() - periodMs), end };
}

function dailyWindowEndingAtNow(): TimeWindow {
  return nowWindow(DAILY_BATCH_PERIOD_MS);
}

function readPrimaryCode(payload: JsonRecord): string | null {
  const code = payload.code;
  if (!isObjectRecord(code) || !Array.isArray(code.coding)) return null;
  const firstCoding = code.coding[0];
  if (!isObjectRecord(firstCoding) || typeof firstCoding.code !== "string") return null;
  return firstCoding.code;
}

export function classifyScheduledPayloadKind(payload: unknown): ScheduledPayloadKind | null {
  if (!isObjectRecord(payload)) return null;
  if (payload.resourceType === "Bundle") return "statsBatch";
  if (payload.resourceType !== "Observation") return null;

  const code = readPrimaryCode(payload);
  if (!code) return null;
  const hasInstant = "effectiveInstant" in payload;
  const hasPeriod = "effectivePeriod" in payload;

  if (code === "presence-detection") {
    if (hasInstant) return "presenceRealtime";
    if (hasPeriod) return "presenceBatch";
    return null;
  }
  if (code === "sleep-status") {
    return hasInstant ? "sleepRealtime" : null;
  }
  if (code === "107145-5") {
    if (hasInstant) return "sleepRealtime";
    if (hasPeriod) return "sleepBatch";
    return null;
  }
  if (code === "8867-4") {
    if (hasInstant) return "heartRateRealtime";
    if (hasPeriod) return "heartRateBatch";
    return null;
  }
  if (code === "9279-1") {
    if (hasInstant) return "breathingRateRealtime";
    if (hasPeriod) return "breathingRateBatch";
    return null;
  }
  return null;
}

function setEffectiveFields(
  observation: JsonRecord,
  kind: ObservationKind,
  window: TimeWindow,
): void {
  if (kind.endsWith("Realtime")) {
    observation.effectiveInstant = window.end.toISOString();
    delete observation.effectivePeriod;
    return;
  }
  observation.effectivePeriod = {
    start: window.start.toISOString(),
    end: window.end.toISOString(),
  };
  delete observation.effectiveInstant;
}

function applyInvalidValue(observation: JsonRecord, kind: ObservationKind): void {
  const sampledPeriod =
    kind === "presenceRealtime"
      ? PRESENCE_REALTIME_PERIOD_MS
      : kind === "sleepRealtime"
        ? SLEEP_REALTIME_PERIOD_MS
        : kind === "heartRateRealtime" || kind === "breathingRateRealtime"
          ? VITALS_REALTIME_PERIOD_MS
          : kind === "presenceBatch"
            ? PRESENCE_BATCH_PERIOD_MS
            : kind === "sleepBatch"
              ? SLEEP_BATCH_PERIOD_MS
              : DAILY_BATCH_PERIOD_MS;
  const sampledData = isObjectRecord(observation.valueSampledData)
    ? observation.valueSampledData
    : {
        origin: {
          value: 0,
          unit: "1",
          system: "http://unitsofmeasure.org",
          code: "1",
        },
        period: sampledPeriod,
        factor: 1,
        dimensions: 1,
      };
  sampledData.data =
    kind === "sleepRealtime" ? "-1" : kind.startsWith("presence") ? "0" : "E";
  observation.valueSampledData = sampledData;

  if (kind === "sleepRealtime") {
    observation.valueString = "invalid";
  }

  if (
    kind === "heartRateRealtime" ||
    kind === "breathingRateRealtime" ||
    kind === "heartRateBatch" ||
    kind === "breathingRateBatch"
  ) {
    delete observation.component;
  }
}

function enforceCanonicalCoding(observation: JsonRecord, kind: ObservationKind): void {
  const coding =
    kind === "presenceRealtime" || kind === "presenceBatch"
      ? {
          system: "https://sleepiz.com/fhir/CodeSystem/observation-codes",
          code: "presence-detection",
          display: "Presence",
        }
      : kind === "sleepRealtime" || kind === "sleepBatch"
        ? {
            system: "http://loinc.org",
            code: "107145-5",
            display: "Sleep status",
          }
        : kind === "heartRateRealtime" || kind === "heartRateBatch"
          ? {
              system: "http://loinc.org",
              code: "8867-4",
              display: "Heart rate",
            }
          : {
              system: "http://loinc.org",
              code: "9279-1",
              display: "Breathing rate",
            };
  observation.code = { coding: [coding] };
}

export function hasPresenceSignal(payload: unknown): boolean {
  if (!isObjectRecord(payload)) return false;
  if (payload.resourceType !== "Observation") return false;
  const sampled = payload.valueSampledData;
  if (!isObjectRecord(sampled) || typeof sampled.data !== "string") return false;
  const tokens = sampled.data
    .split(/\s+/)
    .map((token) => token.trim())
    .filter(Boolean);
  if (tokens.length === 0) return false;
  return tokens.some((token) => token === "0" || token === "1");
}

function buildFallbackObservation(kind: ObservationKind, deviceId: string): JsonRecord {
  const isHr = kind === "heartRateRealtime" || kind === "heartRateBatch";
  const isBr = kind === "breathingRateRealtime" || kind === "breathingRateBatch";
  const isSleepRealtime = kind === "sleepRealtime";
  const isSleepBatch = kind === "sleepBatch";
  const isPresence = kind === "presenceRealtime" || kind === "presenceBatch";
  const periodMs =
    kind === "presenceRealtime"
      ? PRESENCE_REALTIME_PERIOD_MS
      : kind === "sleepRealtime"
        ? SLEEP_REALTIME_PERIOD_MS
        : kind === "heartRateRealtime" || kind === "breathingRateRealtime"
          ? VITALS_REALTIME_PERIOD_MS
          : kind === "presenceBatch"
            ? PRESENCE_BATCH_PERIOD_MS
            : kind === "sleepBatch"
              ? SLEEP_BATCH_PERIOD_MS
              : DAILY_BATCH_PERIOD_MS;

  const observation: JsonRecord = {
    resourceType: "Observation",
    status: "final",
    category: [
      {
        coding: [
          {
            system: "http://terminology.hl7.org/CodeSystem/observation-category",
            code: isPresence || isSleepRealtime || isSleepBatch ? "activity" : "vital-signs",
            display: isPresence || isSleepRealtime || isSleepBatch ? "Activity" : "Vital Signs",
          },
        ],
      },
    ],
    code: {
      coding: [
        {
          system: isHr || isBr ? "http://loinc.org" : "https://sleepiz.com/fhir/CodeSystem/observation-codes",
          code: isHr
            ? "8867-4"
            : isBr
              ? "9279-1"
              : isSleepBatch || isSleepRealtime
                ? "107145-5"
                : "presence-detection",
          display: isHr
            ? "Heart rate"
            : isBr
              ? "Respiratory rate"
              : isSleepBatch || isSleepRealtime
                ? "Sleep status"
                : "Presence",
        },
      ],
    },
    subject: { reference: "Patient/not_implemented" },
    valueSampledData: {
      origin: {
        value: 0,
        unit: "1",
        system: "http://unitsofmeasure.org",
        code: "1",
      },
      period: periodMs,
      factor: 1,
      dimensions: 1,
      data: isSleepRealtime ? "-1" : isPresence ? "0" : "E",
    },
    device: { reference: `Device/${deviceId}` },
  };

  if (isSleepRealtime) {
    observation.valueString = "invalid";
  }
  return observation;
}

export function buildInvalidObservationForKind(
  kind: ObservationKind,
  template: unknown,
  deviceId: string,
  window: TimeWindow,
): JsonRecord {
  const observation =
    isObjectRecord(template) && template.resourceType === "Observation"
      ? deepClone(template)
      : buildFallbackObservation(kind, deviceId);
  setEffectiveFields(observation, kind, window);
  enforceCanonicalCoding(observation, kind);
  applyInvalidValue(observation, kind);
  return observation;
}

async function listKnownDeviceIds(sql: Sql): Promise<string[]> {
  const rows = await sql<{ deviceId: string }[]>`
    select distinct device_id as "deviceId"
    from device_messages
    where device_id is not null and btrim(device_id) <> ''
  `;
  return rows.map((row) => row.deviceId);
}

async function listRecentDevicePayloads(
  sql: Sql,
  deviceId: string,
  window: TimeWindow,
): Promise<unknown[]> {
  const rows = await sql<{ payload: unknown }[]>`
    select payload
    from device_messages
    where device_id = ${deviceId}
      and received_at >= ${window.start}
      and received_at < ${window.end}
    order by received_at desc
    limit 200
  `;
  return rows.map((row) => row.payload);
}

async function selectLatestPayloadForKind(
  sql: Sql,
  deviceId: string,
  kind: ScheduledPayloadKind,
  window: TimeWindow,
): Promise<unknown | null> {
  const payloads = await listRecentDevicePayloads(sql, deviceId, window);
  for (const payload of payloads) {
    if (classifyScheduledPayloadKind(payload) === kind) return payload;
  }
  return null;
}

async function selectLatestTemplateForKind(
  sql: Sql,
  deviceId: string,
  kind: ObservationKind,
): Promise<unknown | null> {
  const rows = await sql<{ payload: unknown }[]>`
    select payload
    from device_messages
    where device_id = ${deviceId}
    order by received_at desc
    limit 500
  `;
  for (const row of rows) {
    if (classifyScheduledPayloadKind(row.payload) === kind) return row.payload;
  }
  return null;
}

async function sendPerDeviceKind(params: {
  sql: Sql;
  config: SchedulerOptions;
  deviceIds: string[];
  kind: ObservationKind;
  window: TimeWindow;
}): Promise<void> {
  const { sql, config, deviceIds, kind, window } = params;
  for (const deviceId of deviceIds) {
    const latest = await selectLatestPayloadForKind(sql, deviceId, kind, window);
    if (latest && (!kind.startsWith("presence") || hasPresenceSignal(latest))) {
      await forwardNormalizedPayloadToOvok(
        `scheduler/${kind}/${deviceId}`,
        latest,
        config,
      );
      continue;
    }

    const template = await selectLatestTemplateForKind(sql, deviceId, kind);
    const invalid = buildInvalidObservationForKind(kind, template, deviceId, window);
    await forwardNormalizedPayloadToOvok(
      `scheduler/${kind}/${deviceId}/invalid`,
      invalid,
      config,
    );
  }
}

async function sendStatsIfPresent(params: {
  sql: Sql;
  config: SchedulerOptions;
  deviceIds: string[];
  window: TimeWindow;
}): Promise<void> {
  const { sql, config, deviceIds, window } = params;
  for (const deviceId of deviceIds) {
    const payload = await selectLatestPayloadForKind(
      sql,
      deviceId,
      "statsBatch",
      window,
    );
    if (!payload) continue;
    await forwardNormalizedPayloadToOvok(
      `scheduler/statsBatch/${deviceId}`,
      payload,
      config,
    );
  }
}

function scheduleJob(
  tasks: ScheduledTask[],
  expr: string,
  jobName: string,
  run: () => Promise<void>,
): void {
  if (!cron.validate(expr)) {
    throw new Error(`Invalid cron expression for ${jobName}: ${expr}`);
  }

  let inFlight = false;
  const task = cron.schedule(
    expr,
    async () => {
      if (inFlight) {
        console.log(`[ovok-scheduler] skip ${jobName} in-flight`);
        return;
      }
      inFlight = true;
      try {
        await run();
      } catch (error) {
        console.error(`[ovok-scheduler] job failed ${jobName}`, error);
      } finally {
        inFlight = false;
      }
    },
    { timezone: "UTC" },
  );
  tasks.push(task);
}

export function startOvokScheduledForwarding(
  sql: Sql,
  config: AppConfig,
): SchedulerHandle {
  if (!config.ovokIngestEnabled || !config.ovokScheduledEnabled) {
    return { stop: () => {} };
  }

  const tasks: ScheduledTask[] = [];
  const sharedConfig: SchedulerOptions = {
    ovokIngestEnabled: config.ovokIngestEnabled,
    ovokIngestBaseUrl: config.ovokIngestBaseUrl,
    ovokIngestApiKey: config.ovokIngestApiKey,
    ovokIngestApiKeyHeader: config.ovokIngestApiKeyHeader,
    ovokIngestTimeoutMs: config.ovokIngestTimeoutMs,
  };

  async function withDevices(
    action: (deviceIds: string[]) => Promise<void>,
  ): Promise<void> {
    const deviceIds = await listKnownDeviceIds(sql);
    if (deviceIds.length === 0) return;
    await action(deviceIds);
  }

  scheduleJob(tasks, "*/30 * * * * *", "presenceRealtime", async () => {
    await withDevices(async (deviceIds) => {
      await sendPerDeviceKind({
        sql,
        config: sharedConfig,
        deviceIds,
        kind: "presenceRealtime",
        window: nowWindow(PRESENCE_REALTIME_PERIOD_MS),
      });
    });
  });

  scheduleJob(tasks, "*/10 * * * *", "sleepRealtime", async () => {
    await withDevices(async (deviceIds) => {
      await sendPerDeviceKind({
        sql,
        config: sharedConfig,
        deviceIds,
        kind: "sleepRealtime",
        window: nowWindow(SLEEP_REALTIME_PERIOD_MS),
      });
    });
  });

  scheduleJob(tasks, "*/10 * * * *", "vitalsRealtime", async () => {
    await withDevices(async (deviceIds) => {
      const window = nowWindow(VITALS_REALTIME_PERIOD_MS);
      await sendPerDeviceKind({
        sql,
        config: sharedConfig,
        deviceIds,
        kind: "heartRateRealtime",
        window,
      });
      await sendPerDeviceKind({
        sql,
        config: sharedConfig,
        deviceIds,
        kind: "breathingRateRealtime",
        window,
      });
    });
  });

  scheduleJob(tasks, "0 * * * *", "sleepBatch", async () => {
    await withDevices(async (deviceIds) => {
      await sendPerDeviceKind({
        sql,
        config: sharedConfig,
        deviceIds,
        kind: "sleepBatch",
        window: nowWindow(SLEEP_BATCH_PERIOD_MS),
      });
    });
  });

  scheduleJob(tasks, "0 * * * *", "presenceBatch", async () => {
    await withDevices(async (deviceIds) => {
      await sendPerDeviceKind({
        sql,
        config: sharedConfig,
        deviceIds,
        kind: "presenceBatch",
        window: nowWindow(PRESENCE_BATCH_PERIOD_MS),
      });
    });
  });

  scheduleJob(tasks, "0 9 * * *", "vitalsBatch", async () => {
    await withDevices(async (deviceIds) => {
      const window = dailyWindowEndingAtNow();
      await sendPerDeviceKind({
        sql,
        config: sharedConfig,
        deviceIds,
        kind: "heartRateBatch",
        window,
      });
      await sendPerDeviceKind({
        sql,
        config: sharedConfig,
        deviceIds,
        kind: "breathingRateBatch",
        window,
      });
    });
  });

  scheduleJob(tasks, "0 9 * * *", "statsBatch", async () => {
    await withDevices(async (deviceIds) => {
      await sendStatsIfPresent({
        sql,
        config: sharedConfig,
        deviceIds,
        window: dailyWindowEndingAtNow(),
      });
    });
  });

  console.log("[ovok-scheduler] enabled with UTC cron jobs");

  return {
    stop: () => {
      for (const task of tasks) task.stop();
    },
  };
}
