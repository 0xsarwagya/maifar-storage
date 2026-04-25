import cron, { type ScheduledTask } from "node-cron";
import type { AppConfig } from "./config";
import type { Sql } from "./db";
import {
  extractDeviceOnlineState,
  forwardNormalizedPayloadToOvok,
  normalizePayloadForStorage,
} from "./mqtt-ingest";

const DAY_MS = 24 * 60 * 60 * 1000;
const TEN_MINUTES_MS = 10 * 60 * 1000;

const DEVICE_ACTIVITY_WINDOW_MS = TEN_MINUTES_MS;
const PRESENCE_REALTIME_SAMPLE_PERIOD_MS = 30_000;
const SLEEP_REALTIME_SAMPLE_PERIOD_MS = 30_000;
const VITALS_REALTIME_SAMPLE_PERIOD_MS = 5_000;
const PRESENCE_BATCH_SAMPLE_PERIOD_MS = 30_000;
const SLEEP_BATCH_SAMPLE_PERIOD_MS = 30_000;
const VITALS_BATCH_SAMPLE_PERIOD_MS = 5_000;
const DAILY_BATCH_WINDOW_MS = DAY_MS;

const VALID_PRESENCE_TOKENS = new Set(["0", "1"]);
const VALID_SLEEP_TOKENS = new Set(["0", "1"]);

export const OVOK_FORWARD_CRON = {
  presenceRealtime: "*/30 * * * * *",
  sleepRealtime: "*/10 * * * *",
  vitalsRealtime: "*/10 * * * *",
  dailyBatch: "0 9 * * *",
} as const;

type JsonRecord = Record<string, unknown>;

type ObservationKind =
  | "presenceRealtime"
  | "sleepRealtime"
  | "heartRateRealtime"
  | "breathingRateRealtime"
  | "roomTemperatureRealtime"
  | "ambientLightRealtime"
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

export type SchedulerMessageRow = {
  topic: string;
  receivedAt: Date;
  payload: unknown;
};

type NormalizedMessageRow = SchedulerMessageRow & {
  normalizedPayload: unknown;
  onlineState: boolean | null;
};

function isObjectRecord(value: unknown): value is JsonRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isObservation(payload: unknown): payload is JsonRecord {
  return isObjectRecord(payload) && payload.resourceType === "Observation";
}

function deepClone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function nowWindow(periodMs: number): TimeWindow {
  const end = new Date();
  return { start: new Date(end.getTime() - periodMs), end };
}

function dailyWindowEndingAtNow(): TimeWindow {
  return nowWindow(DAILY_BATCH_WINDOW_MS);
}

function sampledPeriodMsForKind(kind: ObservationKind): number {
  switch (kind) {
    case "presenceRealtime":
      return PRESENCE_REALTIME_SAMPLE_PERIOD_MS;
    case "sleepRealtime":
      return SLEEP_REALTIME_SAMPLE_PERIOD_MS;
    case "heartRateRealtime":
    case "breathingRateRealtime":
      return VITALS_REALTIME_SAMPLE_PERIOD_MS;
    case "presenceBatch":
      return PRESENCE_BATCH_SAMPLE_PERIOD_MS;
    case "sleepBatch":
      return SLEEP_BATCH_SAMPLE_PERIOD_MS;
    case "heartRateBatch":
    case "breathingRateBatch":
      return VITALS_BATCH_SAMPLE_PERIOD_MS;
    default:
      return PRESENCE_REALTIME_SAMPLE_PERIOD_MS;
  }
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
  if (code === "room_temperature") {
    return hasPeriod ? null : "roomTemperatureRealtime";
  }
  if (code === "ambient_light") {
    return hasPeriod ? null : "ambientLightRealtime";
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

function defaultOriginForKind(kind: ObservationKind): JsonRecord {
  if (kind === "heartRateRealtime" || kind === "heartRateBatch") {
    return {
      value: 0,
      unit: "beats/min",
      system: "http://unitsofmeasure.org",
      code: "/min",
    };
  }
  if (kind === "breathingRateRealtime" || kind === "breathingRateBatch") {
    return {
      value: 0,
      unit: "breaths/min",
      system: "http://unitsofmeasure.org",
      code: "/min",
    };
  }
  return {
    value: 0,
    unit: "1",
    system: "http://unitsofmeasure.org",
    code: "1",
  };
}

function applyInvalidValue(observation: JsonRecord, kind: ObservationKind): void {
  if (kind === "roomTemperatureRealtime") {
    observation.valueQuantity = {
      value: 0,
      unit: "°C",
      system: "http://unitsofmeasure.org",
      code: "Cel",
    };
    return;
  }
  if (kind === "ambientLightRealtime") {
    observation.component = [
      {
        code: {
          coding: [
            {
              system: "https://api.ovok.com/StructuredDefinition",
              code: "illuminance",
            },
            {
              system: "http://loinc.org",
              code: "39125-0",
              display: "Light intensity",
            },
          ],
        },
        valueQuantity: {
          value: 0,
          unit: "lux",
        },
      },
      {
        code: {
          coding: [
            {
              system: "https://api.ovok.com/StructuredDefinition",
              code: "light_color",
            },
          ],
        },
        valueString: "#000000",
      },
    ];
    return;
  }

  const sampledPeriod = sampledPeriodMsForKind(kind);
  observation.valueSampledData = {
    origin: defaultOriginForKind(kind),
    period: sampledPeriod,
    factor: 1,
    dimensions: 1,
    data: kind === "sleepRealtime" ? "-1" : kind.startsWith("presence") ? "0" : "E",
  };

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
  switch (kind) {
    case "presenceRealtime":
    case "presenceBatch":
      observation.code = {
        coding: [
          {
            system: "https://sleepiz.com/fhir/CodeSystem/observation-codes",
            code: "presence-detection",
            display: "Presence",
          },
        ],
      };
      break;
    case "sleepRealtime":
    case "sleepBatch":
      observation.code = {
        coding: [
          {
            system: "http://loinc.org",
            code: "107145-5",
            display: "Sleep status",
          },
        ],
      };
      break;
    case "heartRateRealtime":
    case "heartRateBatch":
      observation.code = {
        coding: [
          {
            system: "http://loinc.org",
            code: "8867-4",
            display: "Heart rate",
          },
        ],
      };
      break;
    case "breathingRateRealtime":
    case "breathingRateBatch":
      observation.code = {
        coding: [
          {
            system: "http://loinc.org",
            code: "9279-1",
            display: "Breathing rate",
          },
        ],
      };
      break;
    case "roomTemperatureRealtime":
      observation.code = {
        coding: [
          {
            system: "https://api.ovok.com/StructuredDefinition",
            code: "room_temperature",
          },
          {
            system: "http://loinc.org",
            code: "8310-5",
            display: "Room temperature",
          },
        ],
      };
      break;
    case "ambientLightRealtime":
      observation.code = {
        coding: [
          {
            system: "https://api.ovok.com/StructuredDefinition",
            code: "ambient_light",
          },
        ],
      };
      break;
  }
}

export function hasPresenceSignal(payload: unknown): boolean {
  if (!isObservation(payload)) return false;
  return splitSampledDataTokens(payload).some((token) => VALID_PRESENCE_TOKENS.has(token));
}

function buildFallbackObservation(kind: ObservationKind, deviceId: string): JsonRecord {
  if (kind === "roomTemperatureRealtime") {
    return {
      resourceType: "Observation",
      status: "final",
      category: [
        {
          coding: [
            {
              system: "http://terminology.hl7.org/CodeSystem/observation-category",
              code: "environment",
            },
          ],
        },
      ],
      code: {
        coding: [
          {
            system: "https://api.ovok.com/StructuredDefinition",
            code: "room_temperature",
          },
          {
            system: "http://loinc.org",
            code: "8310-5",
            display: "Room temperature",
          },
        ],
      },
      subject: { reference: "Patient/not_implemented" },
      valueQuantity: {
        value: 0,
        unit: "°C",
        system: "http://unitsofmeasure.org",
        code: "Cel",
      },
      device: { reference: `Device/${deviceId}` },
    };
  }

  if (kind === "ambientLightRealtime") {
    return {
      resourceType: "Observation",
      status: "final",
      category: [
        {
          coding: [
            {
              system: "http://terminology.hl7.org/CodeSystem/observation-category",
              code: "environment",
            },
          ],
        },
      ],
      code: {
        coding: [
          {
            system: "https://api.ovok.com/StructuredDefinition",
            code: "ambient_light",
          },
        ],
      },
      subject: { reference: "Patient/not_implemented" },
      component: [
        {
          code: {
            coding: [
              {
                system: "https://api.ovok.com/StructuredDefinition",
                code: "illuminance",
              },
              {
                system: "http://loinc.org",
                code: "39125-0",
                display: "Light intensity",
              },
            ],
          },
          valueQuantity: {
            value: 0,
            unit: "lux",
          },
        },
        {
          code: {
            coding: [
              {
                system: "https://api.ovok.com/StructuredDefinition",
                code: "light_color",
              },
            ],
          },
          valueString: "#000000",
        },
      ],
      device: { reference: `Device/${deviceId}` },
    };
  }

  const isHr = kind === "heartRateRealtime" || kind === "heartRateBatch";
  const isBr = kind === "breathingRateRealtime" || kind === "breathingRateBatch";
  const isSleep = kind === "sleepRealtime" || kind === "sleepBatch";
  const isPresence = kind === "presenceRealtime" || kind === "presenceBatch";

  const observation: JsonRecord = {
    resourceType: "Observation",
    status: "final",
    category: [
      {
        coding: [
          {
            system: "http://terminology.hl7.org/CodeSystem/observation-category",
            code: isPresence || isSleep ? "activity" : "vital-signs",
            display: isPresence || isSleep ? "Activity" : "Vital Signs",
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
              : isSleep
                ? "107145-5"
                : "presence-detection",
          display: isHr
            ? "Heart rate"
            : isBr
              ? "Breathing rate"
              : isSleep
                ? "Sleep status"
                : "Presence",
        },
      ],
    },
    subject: { reference: "Patient/not_implemented" },
    valueSampledData: {
      origin: defaultOriginForKind(kind),
      period: sampledPeriodMsForKind(kind),
      factor: 1,
      dimensions: 1,
      data: isSleep ? "-1" : isPresence ? "0" : "E",
    },
    device: { reference: `Device/${deviceId}` },
  };

  if (kind === "sleepRealtime") {
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
    isObservation(template)
      ? deepClone(template)
      : buildFallbackObservation(kind, deviceId);
  setEffectiveFields(observation, kind, window);
  enforceCanonicalCoding(observation, kind);
  applyInvalidValue(observation, kind);
  return observation;
}

function splitSampledDataTokens(payload: unknown): string[] {
  if (!isObservation(payload)) return [];
  const sampled = payload.valueSampledData;
  if (!isObjectRecord(sampled) || typeof sampled.data !== "string") return [];
  return sampled.data
    .split(/\s+/)
    .map((token) => token.trim())
    .filter(Boolean);
}

function extractLatestAllowedToken(
  payload: unknown,
  allowedTokens: ReadonlySet<string>,
): string | null {
  const tokens = splitSampledDataTokens(payload);
  for (let index = tokens.length - 1; index >= 0; index -= 1) {
    const token = tokens[index];
    if (token && allowedTokens.has(token)) return token;
  }
  return null;
}

function computeAverageFromTokens(tokens: readonly string[]): number | null {
  const numeric = tokens
    .map((token) => Number(token))
    .filter((value) => Number.isFinite(value));
  if (numeric.length === 0) return null;
  const sum = numeric.reduce((acc, value) => acc + value, 0);
  return sum / numeric.length;
}

function normalizeRows(
  deviceId: string,
  rows: readonly SchedulerMessageRow[],
): NormalizedMessageRow[] {
  return rows.map((row) => ({
    ...row,
    onlineState: extractDeviceOnlineState(row.payload),
    normalizedPayload: normalizePayloadForStorage(
      row.topic,
      row.payload,
      deviceId,
      row.receivedAt,
    ),
  }));
}

function rowsForKind(
  rows: readonly NormalizedMessageRow[],
  kind: ScheduledPayloadKind,
): NormalizedMessageRow[] {
  return rows.filter((row) => classifyScheduledPayloadKind(row.normalizedPayload) === kind);
}

function sortRowsNewestFirst(rows: readonly NormalizedMessageRow[]): NormalizedMessageRow[] {
  return [...rows].sort((left, right) => right.receivedAt.getTime() - left.receivedAt.getTime());
}

function sortRowsOldestFirst(rows: readonly NormalizedMessageRow[]): NormalizedMessageRow[] {
  return [...rows].sort((left, right) => left.receivedAt.getTime() - right.receivedAt.getTime());
}

function filterRowsForCurrentOnlineSession(
  rows: readonly NormalizedMessageRow[],
): NormalizedMessageRow[] {
  const newestRows = sortRowsNewestFirst(rows);
  const latestControlRow = newestRows.find((row) => row.onlineState !== null);
  if (!latestControlRow) return [...rows];
  if (latestControlRow.onlineState === false) return [];

  const hasOlderOffline = newestRows.some(
    (row) =>
      row.receivedAt.getTime() < latestControlRow.receivedAt.getTime() &&
      row.onlineState === false,
  );
  if (!hasOlderOffline) return [...rows];

  return rows.filter(
    (row) => row.receivedAt.getTime() > latestControlRow.receivedAt.getTime(),
  );
}

function buildSampledRealtimeObservationFromTokens(
  kind: "presenceRealtime" | "sleepRealtime" | "heartRateRealtime" | "breathingRateRealtime",
  deviceId: string,
  tokens: readonly string[],
  window: TimeWindow,
): JsonRecord | null {
  if (tokens.length === 0) return null;

  const observation = buildFallbackObservation(kind, deviceId);
  const average = computeAverageFromTokens(tokens);

  setEffectiveFields(observation, kind, window);
  enforceCanonicalCoding(observation, kind);
  observation.valueSampledData = {
    origin: defaultOriginForKind(kind),
    period: sampledPeriodMsForKind(kind),
    factor: 1,
    dimensions: 1,
    data: tokens.join(" "),
  };

  if (kind === "heartRateRealtime" || kind === "breathingRateRealtime") {
    if (average !== null) {
      observation.component = [
        {
          code: {
            coding: [
              {
                system: "http://loinc.org",
                code: kind === "heartRateRealtime" ? "8867-4" : "9279-1",
                display: "AVERAGE",
              },
            ],
          },
          valueQuantity: {
            value: average,
            unit: kind === "heartRateRealtime" ? "beats/min" : "breaths/min",
            system: "http://unitsofmeasure.org",
            code: "/min",
          },
        },
      ];
    } else {
      delete observation.component;
    }
    delete observation.valueString;
    return observation;
  }

  if (kind === "sleepRealtime") {
    if (average !== null) {
      observation.component = [
        {
          code: {
            coding: [
              {
                system: "http://loinc.org",
                code: "107145-5",
                display: "AVERAGE",
              },
            ],
          },
          valueQuantity: {
            value: average,
            unit: "1",
            system: "http://unitsofmeasure.org",
            code: "1",
          },
        },
      ];
    } else {
      delete observation.component;
    }

    const latestToken = extractLatestAllowedToken(
      { resourceType: "Observation", valueSampledData: { data: tokens.join(" ") } },
      new Set(["-1", "0", "1"]),
    );
    if (latestToken === "0") {
      observation.valueString = "asleep";
    } else if (latestToken === "1") {
      observation.valueString = "awake";
    } else if (latestToken === "-1") {
      observation.valueString = "invalid";
    } else {
      delete observation.valueString;
    }
    return observation;
  }

  delete observation.component;
  delete observation.valueString;
  return observation;
}

function buildPresenceRealtimeObservation(
  deviceId: string,
  rows: readonly NormalizedMessageRow[],
  window: TimeWindow,
): JsonRecord | null {
  if (rows.length === 0) return null;

  const newestSleepRows = sortRowsNewestFirst(rowsForKind(rows, "sleepRealtime"));
  for (const row of newestSleepRows) {
    if (extractLatestAllowedToken(row.normalizedPayload, VALID_SLEEP_TOKENS) !== null) {
      return buildSampledRealtimeObservationFromTokens(
        "presenceRealtime",
        deviceId,
        ["1"],
        window,
      );
    }
  }

  const newestPresenceRows = sortRowsNewestFirst(rowsForKind(rows, "presenceRealtime"));
  for (const row of newestPresenceRows) {
    const token = extractLatestAllowedToken(row.normalizedPayload, VALID_PRESENCE_TOKENS);
    if (token !== null) {
      return buildSampledRealtimeObservationFromTokens(
        "presenceRealtime",
        deviceId,
        [token],
        window,
      );
    }
  }

  return buildSampledRealtimeObservationFromTokens(
    "presenceRealtime",
    deviceId,
    ["0"],
    window,
  );
}

function buildAggregatedRealtimeObservation(
  kind: "sleepRealtime" | "heartRateRealtime" | "breathingRateRealtime",
  deviceId: string,
  rows: readonly NormalizedMessageRow[],
  window: TimeWindow,
): JsonRecord | null {
  const matchingRows = rowsForKind(rows, kind);
  if (matchingRows.length === 0) return null;

  const newestRows = sortRowsNewestFirst(matchingRows);
  const latestAggregatedRow = newestRows.find(
    (row) => splitSampledDataTokens(row.normalizedPayload).length > 1,
  );
  if (latestAggregatedRow) {
    const tokens = splitSampledDataTokens(latestAggregatedRow.normalizedPayload);
    return buildSampledRealtimeObservationFromTokens(kind, deviceId, tokens, window);
  }

  const tokens = sortRowsOldestFirst(matchingRows).flatMap((row) =>
    splitSampledDataTokens(row.normalizedPayload),
  );
  return buildSampledRealtimeObservationFromTokens(kind, deviceId, tokens, window);
}

function buildLatestRealtimeObservation(
  kind: "roomTemperatureRealtime" | "ambientLightRealtime",
  rows: readonly NormalizedMessageRow[],
): JsonRecord | null {
  const latestRow = sortRowsNewestFirst(rowsForKind(rows, kind))[0];
  if (!latestRow || !isObservation(latestRow.normalizedPayload)) return null;
  return deepClone(latestRow.normalizedPayload);
}

export function buildRealtimeObservationForKind(
  kind: "presenceRealtime" | "sleepRealtime" | "heartRateRealtime" | "breathingRateRealtime" | "roomTemperatureRealtime" | "ambientLightRealtime",
  deviceId: string,
  rows: readonly SchedulerMessageRow[],
  window: TimeWindow,
): JsonRecord | null {
  const normalizedRows = filterRowsForCurrentOnlineSession(
    normalizeRows(deviceId, rows),
  );
  if (normalizedRows.length === 0) return null;

  switch (kind) {
    case "presenceRealtime":
      return buildPresenceRealtimeObservation(deviceId, normalizedRows, window);
    case "sleepRealtime":
    case "heartRateRealtime":
    case "breathingRateRealtime":
      return buildAggregatedRealtimeObservation(kind, deviceId, normalizedRows, window);
    case "roomTemperatureRealtime":
    case "ambientLightRealtime":
      return buildLatestRealtimeObservation(kind, normalizedRows);
  }
}

async function listKnownDeviceIds(sql: Sql): Promise<string[]> {
  const rows = await sql<{ deviceId: string }[]>`
    select distinct device_id as "deviceId"
    from device_messages
    where device_id is not null and btrim(device_id) <> ''
  `;
  return rows.map((row) => row.deviceId);
}

async function listRecentDeviceMessages(
  sql: Sql,
  deviceId: string,
  window: TimeWindow,
  limit: number,
): Promise<SchedulerMessageRow[]> {
  const rows = await sql<{
    topic: string;
    receivedAt: Date;
    payload: unknown;
  }[]>`
    select topic, received_at as "receivedAt", payload
    from device_messages
    where device_id = ${deviceId}
      and received_at >= ${window.start}
      and received_at < ${window.end}
    order by received_at desc
    limit ${limit}
  `;
  return rows;
}

async function selectLatestPayloadForKind(
  sql: Sql,
  deviceId: string,
  kind: ScheduledPayloadKind,
  window: TimeWindow,
): Promise<unknown | null> {
  const normalizedRows = filterRowsForCurrentOnlineSession(
    normalizeRows(
      deviceId,
      await listRecentDeviceMessages(sql, deviceId, window, 1_000),
    ),
  );
  if (normalizedRows.length === 0) return null;

  for (const row of normalizedRows) {
    if (classifyScheduledPayloadKind(row.normalizedPayload) === kind) {
      return row.normalizedPayload;
    }
  }
  return null;
}

async function sendRealtimeKind(params: {
  sql: Sql;
  config: SchedulerOptions;
  deviceIds: string[];
  kind:
    | "presenceRealtime"
    | "sleepRealtime"
    | "heartRateRealtime"
    | "breathingRateRealtime"
    | "roomTemperatureRealtime"
    | "ambientLightRealtime";
  window: TimeWindow;
}): Promise<void> {
  const { sql, config, deviceIds, kind, window } = params;
  for (const deviceId of deviceIds) {
    const rows = await listRecentDeviceMessages(sql, deviceId, window, 2_000);
    const payload = buildRealtimeObservationForKind(kind, deviceId, rows, window);
    if (!payload) continue;
    await forwardNormalizedPayloadToOvok(
      `scheduler/${kind}/${deviceId}`,
      payload,
      config,
    );
  }
}

async function sendBatchKindIfPresent(params: {
  sql: Sql;
  config: SchedulerOptions;
  deviceIds: string[];
  kind: "presenceBatch" | "sleepBatch" | "heartRateBatch" | "breathingRateBatch";
  window: TimeWindow;
}): Promise<void> {
  const { sql, config, deviceIds, kind, window } = params;
  for (const deviceId of deviceIds) {
    const payload = await selectLatestPayloadForKind(sql, deviceId, kind, window);
    if (!payload) continue;
    await forwardNormalizedPayloadToOvok(
      `scheduler/${kind}/${deviceId}`,
      payload,
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

  scheduleJob(tasks, OVOK_FORWARD_CRON.presenceRealtime, "presenceRealtime", async () => {
    await withDevices(async (deviceIds) => {
      await sendRealtimeKind({
        sql,
        config: sharedConfig,
        deviceIds,
        kind: "presenceRealtime",
        window: nowWindow(DEVICE_ACTIVITY_WINDOW_MS),
      });
    });
  });

  scheduleJob(tasks, OVOK_FORWARD_CRON.sleepRealtime, "sleepRealtime", async () => {
    await withDevices(async (deviceIds) => {
      await sendRealtimeKind({
        sql,
        config: sharedConfig,
        deviceIds,
        kind: "sleepRealtime",
        window: nowWindow(TEN_MINUTES_MS),
      });
    });
  });

  scheduleJob(tasks, OVOK_FORWARD_CRON.vitalsRealtime, "vitalsRealtime", async () => {
    await withDevices(async (deviceIds) => {
      const window = nowWindow(TEN_MINUTES_MS);
      await sendRealtimeKind({
        sql,
        config: sharedConfig,
        deviceIds,
        kind: "heartRateRealtime",
        window,
      });
      await sendRealtimeKind({
        sql,
        config: sharedConfig,
        deviceIds,
        kind: "breathingRateRealtime",
        window,
      });
      await sendRealtimeKind({
        sql,
        config: sharedConfig,
        deviceIds,
        kind: "roomTemperatureRealtime",
        window,
      });
      await sendRealtimeKind({
        sql,
        config: sharedConfig,
        deviceIds,
        kind: "ambientLightRealtime",
        window,
      });
    });
  });

  scheduleJob(tasks, OVOK_FORWARD_CRON.dailyBatch, "sleepBatch", async () => {
    await withDevices(async (deviceIds) => {
      await sendBatchKindIfPresent({
        sql,
        config: sharedConfig,
        deviceIds,
        kind: "sleepBatch",
        window: dailyWindowEndingAtNow(),
      });
    });
  });

  scheduleJob(tasks, OVOK_FORWARD_CRON.dailyBatch, "presenceBatch", async () => {
    await withDevices(async (deviceIds) => {
      await sendBatchKindIfPresent({
        sql,
        config: sharedConfig,
        deviceIds,
        kind: "presenceBatch",
        window: dailyWindowEndingAtNow(),
      });
    });
  });

  scheduleJob(tasks, OVOK_FORWARD_CRON.dailyBatch, "vitalsBatch", async () => {
    await withDevices(async (deviceIds) => {
      const window = dailyWindowEndingAtNow();
      await sendBatchKindIfPresent({
        sql,
        config: sharedConfig,
        deviceIds,
        kind: "heartRateBatch",
        window,
      });
      await sendBatchKindIfPresent({
        sql,
        config: sharedConfig,
        deviceIds,
        kind: "breathingRateBatch",
        window,
      });
    });
  });

  scheduleJob(tasks, OVOK_FORWARD_CRON.dailyBatch, "statsBatch", async () => {
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
