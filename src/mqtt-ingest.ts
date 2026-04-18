import mqtt, { type IClientOptions, type MqttClient } from "mqtt";
import type { AppConfig } from "./config";
import { extractDeviceId } from "./device-id";
import {
  recordOvokForwardFailure,
  recordOvokForwardSuccess,
} from "./ovok-forward-metrics";
import * as queue from "./queue";
import type { QueuedRow } from "./types";

/** Max JSON characters logged per message payload (full row still stored). */
const LOG_PAYLOAD_MAX_CHARS = 16_384;
const NUL = "\u0000";
const PRESENCE_REALTIME_DEFAULT_PERIOD_MS = 30_000;
const SLEEP_REALTIME_DEFAULT_PERIOD_MS = 600_000;
const VITALS_REALTIME_DEFAULT_PERIOD_MS = 600_000;
const VITALS_BATCH_DEFAULT_PERIOD_MS = 5_000;
const PRESENCE_BATCH_DEFAULT_PERIOD_MS = 3_600_000;
const SLEEP_BATCH_DEFAULT_PERIOD_MS = 3_600_000;

type JsonRecord = Record<string, unknown>;

type OvokIngestForwardConfig = Pick<
  AppConfig,
  | "ovokIngestEnabled"
  | "ovokIngestBaseUrl"
  | "ovokIngestApiKey"
  | "ovokIngestApiKeyHeader"
  | "ovokIngestTimeoutMs"
>;

function truncateForLog(s: string, max: number): string {
  if (s.length <= max) return s;
  return `${s.slice(0, max)}… (+${s.length - max} more chars)`;
}

function isFhirObservationOrBundle(payload: unknown): payload is JsonRecord {
  return (
    isObjectRecord(payload) &&
    (payload.resourceType === "Observation" || payload.resourceType === "Bundle")
  );
}

export function resolveOvokIngestUrl(baseUrl: string, payload: unknown): string | null {
  if (!isFhirObservationOrBundle(payload)) return null;
  const path =
    payload.resourceType === "Bundle" ? "/v1/ingest/fhir/bundle" : "/v1/ingest/fhir";
  return `${baseUrl.replace(/\/+$/, "")}${path}`;
}

export async function forwardNormalizedPayloadToOvok(
  topic: string,
  payload: unknown,
  config: OvokIngestForwardConfig,
): Promise<void> {
  if (!config.ovokIngestEnabled) return;
  const url = resolveOvokIngestUrl(config.ovokIngestBaseUrl, payload);
  if (!url) return;

  const headers: Record<string, string> = {
    "content-type": "application/json",
  };
  if (config.ovokIngestApiKey) {
    headers[config.ovokIngestApiKeyHeader] = config.ovokIngestApiKey;
  }
  const body = JSON.stringify(payload);
  const bodyBytes = Buffer.byteLength(body, "utf8");

  let response: Response;
  try {
    response = await fetch(url, {
      method: "POST",
      headers,
      body,
      signal: AbortSignal.timeout(config.ovokIngestTimeoutMs),
    });
  } catch (error) {
    recordOvokForwardFailure(bodyBytes);
    console.error(
      `[ovok] ingest forward failed topic=${JSON.stringify(topic)} url=${url}:`,
      error,
    );
    return;
  }

  if (!response.ok) {
    recordOvokForwardFailure(bodyBytes);
    const body = await response.text().catch(() => "");
    console.error(
      `[ovok] ingest rejected topic=${JSON.stringify(topic)} url=${url} status=${response.status} body=${truncateForLog(body, 2048)}`,
    );
    return;
  }
  recordOvokForwardSuccess(bodyBytes);
}

function stripNullChars(value: string): string {
  return value.includes(NUL) ? value.replaceAll(NUL, "") : value;
}

function isObjectRecord(value: unknown): value is JsonRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readString(
  record: JsonRecord,
  keys: readonly string[],
): string | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim() !== "") {
      return value.trim();
    }
  }
  return undefined;
}

function readUnknown(record: JsonRecord, keys: readonly string[]): unknown {
  for (const key of keys) {
    if (key in record) return record[key];
  }
  return undefined;
}

function readNumber(record: JsonRecord, keys: readonly string[]): number | null {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "number" && Number.isFinite(value) && value > 0) {
      return value;
    }
    if (typeof value === "string") {
      const parsed = Number(value);
      if (Number.isFinite(parsed) && parsed > 0) {
        return parsed;
      }
    }
  }
  return null;
}

function normalizePresenceValue(value: unknown): "1" | "0" | null {
  if (typeof value === "boolean") return value ? "1" : "0";
  if (typeof value === "number") {
    if (!Number.isFinite(value)) return null;
    return value > 0 ? "1" : "0";
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (!normalized) return null;
    if (["1", "true", "present", "detected", "on", "yes"].includes(normalized)) {
      return "1";
    }
    if (["0", "false", "absent", "not_detected", "off", "no"].includes(normalized)) {
      return "0";
    }
    const numeric = Number(normalized);
    if (Number.isFinite(numeric)) {
      return numeric > 0 ? "1" : "0";
    }
  }
  return null;
}

function resolveEffectiveInstant(
  payload: JsonRecord,
  receivedAt: Date,
): string {
  const candidate = readString(payload, [
    "effectiveInstant",
    "timestamp",
    "time",
    "ts",
    "date",
    "datetime",
  ]);
  if (!candidate) return receivedAt.toISOString();
  const parsed = new Date(candidate);
  if (Number.isNaN(parsed.getTime())) {
    return receivedAt.toISOString();
  }
  return parsed.toISOString();
}

function looksLikePresencePayload(topic: string, payload: JsonRecord): boolean {
  if (/\bpresence\b/i.test(topic)) return true;
  return (
    "presence" in payload ||
    "present" in payload ||
    "occupancy" in payload ||
    "presenceDetected" in payload ||
    "someoneExists" in payload
  );
}

function extractPresenceValue(payload: JsonRecord): "1" | "0" | null {
  const keys = [
    "presence",
    "present",
    "occupancy",
    "presenceDetected",
    "someoneExists",
    "value",
  ];
  for (const key of keys) {
    const normalized = normalizePresenceValue(payload[key]);
    if (normalized !== null) return normalized;
  }
  return null;
}

type SleepStatusLabel = "awake" | "asleep" | "invalid";

type VitalSample = {
  sampledData: string;
  average: number | null;
};

type EffectivePeriod = {
  start: string;
  end: string;
};

type StatsMetric = {
  code: string;
  display: string;
  valueQuantity?: {
    value: number;
    unit: string;
    system?: string;
    code?: string;
  };
  valueDateTime?: string;
  valueInteger?: number;
};

function looksLikeSleepStatusPayload(topic: string, payload: JsonRecord): boolean {
  if (/\bsleep(?:[_-]?status)?\b/i.test(topic)) return true;
  return (
    "sleepStatus" in payload ||
    "sleep_status" in payload ||
    "sleepstatus" in payload ||
    "sleep" in payload
  );
}

function normalizeSleepStatusValue(
  value: unknown,
): { data: "1" | "0" | "-1"; status: SleepStatusLabel } | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    if (value === 1) return { data: "1", status: "awake" };
    if (value === 0) return { data: "0", status: "asleep" };
    if (value === -1) return { data: "-1", status: "invalid" };
    return null;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "1" || normalized === "awake") {
      return { data: "1", status: "awake" };
    }
    if (normalized === "0" || normalized === "asleep") {
      return { data: "0", status: "asleep" };
    }
    if (normalized === "-1" || normalized === "invalid") {
      return { data: "-1", status: "invalid" };
    }
  }
  return null;
}

function extractSleepStatusValue(
  topic: string,
  payload: JsonRecord,
): { data: "1" | "0" | "-1"; status: SleepStatusLabel } | null {
  const keys = ["sleepStatus", "sleep_status", "sleepstatus", "sleep"];
  for (const key of keys) {
    const normalized = normalizeSleepStatusValue(payload[key]);
    if (normalized !== null) return normalized;
  }
  if (/\bsleep(?:[_-]?status)?\b/i.test(topic)) {
    return normalizeSleepStatusValue(payload.value);
  }
  return null;
}

function looksLikeBatchPayload(topic: string, payload: JsonRecord): boolean {
  if (/\bbatch\b/i.test(topic)) return true;
  return (
    "effectivePeriod" in payload ||
    "start" in payload ||
    "end" in payload ||
    "from" in payload ||
    "to" in payload ||
    "batch" in payload
  );
}

function parseIsoDateValue(value: unknown): string | null {
  if (typeof value !== "string" || value.trim() === "") return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString();
}

function resolveEffectivePeriod(
  payload: JsonRecord,
  receivedAt: Date,
  sampleCount: number,
  periodMs: number,
): EffectivePeriod {
  const nested =
    isObjectRecord(payload.effectivePeriod) ? payload.effectivePeriod : null;
  const startIso =
    parseIsoDateValue(
      nested
        ? readUnknown(nested, ["start", "from"])
        : readUnknown(payload, ["start", "startTime", "from", "fromTime"]),
    ) ?? null;
  const endIso =
    parseIsoDateValue(
      nested
        ? readUnknown(nested, ["end", "to"])
        : readUnknown(payload, ["end", "endTime", "to", "toTime"]),
    ) ?? null;

  const ticks = Math.max(sampleCount - 1, 0);
  const windowMs = ticks * periodMs;

  if (startIso !== null && endIso !== null) {
    return { start: startIso, end: endIso };
  }
  if (startIso !== null) {
    const start = new Date(startIso);
    return { start: startIso, end: new Date(start.getTime() + windowMs).toISOString() };
  }
  if (endIso !== null) {
    const end = new Date(endIso);
    return { start: new Date(end.getTime() - windowMs).toISOString(), end: endIso };
  }

  const end = receivedAt;
  return {
    start: new Date(end.getTime() - windowMs).toISOString(),
    end: end.toISOString(),
  };
}

function resolveDailyNineAmUtcWindow(receivedAt: Date): EffectivePeriod {
  const end = new Date(receivedAt);
  end.setUTCHours(9, 0, 0, 0);
  if (receivedAt.getTime() < end.getTime()) {
    end.setUTCDate(end.getUTCDate() - 1);
  }
  const start = new Date(end.getTime() - 24 * 60 * 60 * 1000);
  return { start: start.toISOString(), end: end.toISOString() };
}

function resolveNineAmBatchEffectivePeriod(
  payload: JsonRecord,
  receivedAt: Date,
): EffectivePeriod {
  const nested =
    isObjectRecord(payload.effectivePeriod) ? payload.effectivePeriod : null;
  const startIso =
    parseIsoDateValue(
      nested
        ? readUnknown(nested, ["start", "from"])
        : readUnknown(payload, ["start", "startTime", "from", "fromTime"]),
    ) ?? null;
  const endIso =
    parseIsoDateValue(
      nested
        ? readUnknown(nested, ["end", "to"])
        : readUnknown(payload, ["end", "endTime", "to", "toTime"]),
    ) ?? null;
  if (startIso !== null && endIso !== null) {
    return { start: startIso, end: endIso };
  }
  if (startIso !== null) {
    const start = new Date(startIso);
    return {
      start: startIso,
      end: new Date(start.getTime() + 24 * 60 * 60 * 1000).toISOString(),
    };
  }
  if (endIso !== null) {
    const end = new Date(endIso);
    return {
      start: new Date(end.getTime() - 24 * 60 * 60 * 1000).toISOString(),
      end: endIso,
    };
  }
  return resolveDailyNineAmUtcWindow(receivedAt);
}

function toFiniteNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function toInteger(value: unknown): number | null {
  const n = toFiniteNumber(value);
  if (n === null || !Number.isInteger(n)) return null;
  return n;
}

function looksLikeBatchStatisticsPayload(topic: string, payload: JsonRecord): boolean {
  if (/\bbatch[_-]?statistics\b/i.test(topic)) return true;
  return (
    "statistics" in payload ||
    "stats" in payload ||
    "hrStats" in payload ||
    "brStats" in payload ||
    "sleepStats" in payload
  );
}

function metricAsComponent(metric: StatsMetric): JsonRecord {
  const component: JsonRecord = {
    code: {
      coding: [
        {
          code: metric.code,
          display: metric.display,
        },
      ],
    },
  };
  if (metric.valueQuantity) {
    component.valueQuantity = {
      value: metric.valueQuantity.value,
      unit: metric.valueQuantity.unit,
      ...(metric.valueQuantity.system
        ? { system: metric.valueQuantity.system }
        : {}),
      ...(metric.valueQuantity.code ? { code: metric.valueQuantity.code } : {}),
    };
  }
  if (metric.valueDateTime) component.valueDateTime = metric.valueDateTime;
  if (metric.valueInteger !== undefined) component.valueInteger = metric.valueInteger;
  return component;
}

function statsValue(
  section: JsonRecord,
  keys: readonly string[],
): number | null {
  for (const key of keys) {
    const value = toFiniteNumber(section[key]);
    if (value !== null) return value;
  }
  return null;
}

function computeDurationHours(startIso: string, endIso: string): number {
  const diffMs = new Date(endIso).getTime() - new Date(startIso).getTime();
  return Math.max(diffMs / (1000 * 60 * 60), 0);
}

function upsertLongestSleepDuration(metrics: StatsMetric[]): StatsMetric[] {
  const start = metrics.find((m) => m.code === "SleepStartTime")?.valueDateTime;
  const end = metrics.find((m) => m.code === "SleepEndTime")?.valueDateTime;
  if (!start || !end) return metrics;

  const durationMetric: StatsMetric = {
    code: "LongestSleepDuration",
    display: "Longest Sleep Duration",
    valueQuantity: {
      value: computeDurationHours(start, end),
      unit: "h",
      system: "http://unitsofmeasure.org",
      code: "h",
    },
  };

  const existingIdx = metrics.findIndex((m) => m.code === "LongestSleepDuration");
  if (existingIdx === -1) return [...metrics, durationMetric];

  const next = [...metrics];
  next[existingIdx] = durationMetric;
  return next;
}

function readSection(
  payload: JsonRecord,
  keys: readonly string[],
): JsonRecord | null {
  for (const key of keys) {
    const value = payload[key];
    if (isObjectRecord(value)) return value;
  }
  return null;
}

function buildHrStatsMetrics(section: JsonRecord): StatsMetric[] {
  const metrics: StatsMetric[] = [];
  const defs: Array<{ code: string; display: string; keys: string[] }> = [
    { code: "mean", display: "Mean", keys: ["mean", "avg", "average"] },
    { code: "median", display: "Median", keys: ["median"] },
    { code: "max", display: "Maximum", keys: ["max", "maximum"] },
    { code: "min", display: "Minimum", keys: ["min", "minimum"] },
    { code: "p90", display: "UpperPercentile", keys: ["p90", "upperPercentile"] },
    { code: "p10", display: "LowerPercentile", keys: ["p10", "lowerPercentile"] },
  ];
  for (const def of defs) {
    const value = statsValue(section, def.keys);
    if (value !== null) {
      metrics.push({
        code: def.code,
        display: def.display,
        valueQuantity: { value, unit: "beats/min" },
      });
    }
  }
  const coverage = statsValue(section, ["coverage", "coveragePercentage"]);
  if (coverage !== null) {
    metrics.push({
      code: "coverage",
      display: "Coverage Percentage",
      valueQuantity: {
        value: coverage,
        unit: "%",
        system: "http://unitsofmeasure.org",
        code: "%",
      },
    });
  }
  return metrics;
}

function buildBrStatsMetrics(section: JsonRecord): StatsMetric[] {
  const metrics: StatsMetric[] = [];
  const defs: Array<{ code: string; display: string; keys: string[] }> = [
    { code: "mean", display: "Mean", keys: ["mean", "avg", "average"] },
    { code: "median", display: "Median", keys: ["median"] },
    { code: "max", display: "Maximum", keys: ["max", "maximum"] },
    { code: "min", display: "Minimum", keys: ["min", "minimum"] },
    { code: "p90", display: "UpperPercentile", keys: ["p90", "upperPercentile"] },
    { code: "p10", display: "LowerPercentile", keys: ["p10", "lowerPercentile"] },
  ];
  for (const def of defs) {
    const value = statsValue(section, def.keys);
    if (value !== null) {
      metrics.push({
        code: def.code,
        display: def.display,
        valueQuantity: { value, unit: "breaths/min" },
      });
    }
  }
  const coverage = statsValue(section, ["coverage", "coveragePercentage"]);
  if (coverage !== null) {
    metrics.push({
      code: "coverage",
      display: "Coverage Percentage",
      valueQuantity: {
        value: coverage,
        unit: "%",
        system: "http://unitsofmeasure.org",
        code: "%",
      },
    });
  }
  return metrics;
}

function buildSleepStatsMetrics(section: JsonRecord): StatsMetric[] {
  const metrics: StatsMetric[] = [];
  const quantityDefs: Array<{
    code: string;
    display: string;
    keys: string[];
    unit: string;
    codeUnit: string;
  }> = [
    { code: "TST", display: "Total Sleep Time", keys: ["TST", "tst"], unit: "h", codeUnit: "h" },
    { code: "SE", display: "Sleep Efficiency", keys: ["SE", "se"], unit: "%", codeUnit: "%" },
    { code: "WASO", display: "Wake After Sleep Onset", keys: ["WASO", "waso"], unit: "min", codeUnit: "min" },
    { code: "TBT", display: "Total Bed Time", keys: ["TBT", "tbt"], unit: "h", codeUnit: "h" },
    { code: "AwakeningDuration", display: "Average Duration of each Awakening", keys: ["AwakeningDuration", "awakeningDuration"], unit: "min", codeUnit: "min" },
    { code: "SOL", display: "Sleep Onset Latency", keys: ["SOL", "sol"], unit: "min", codeUnit: "min" },
    { code: "WUL", display: "Wake Up Latency", keys: ["WUL", "wul"], unit: "min", codeUnit: "min" },
  ];

  for (const def of quantityDefs) {
    const value = statsValue(section, def.keys);
    if (value !== null) {
      metrics.push({
        code: def.code,
        display: def.display,
        valueQuantity: {
          value,
          unit: def.unit,
          system: "http://unitsofmeasure.org",
          code: def.codeUnit,
        },
      });
    }
  }

  const sleepStart = parseIsoDateValue(
    readUnknown(section, ["SleepStartTime", "sleepStartTime", "sleep_start_time"]),
  );
  if (sleepStart) {
    metrics.push({
      code: "SleepStartTime",
      display: "Sleep Start Time",
      valueDateTime: sleepStart,
    });
  }

  const sleepEnd = parseIsoDateValue(
    readUnknown(section, ["SleepEndTime", "sleepEndTime", "sleep_end_time"]),
  );
  if (sleepEnd) {
    metrics.push({
      code: "SleepEndTime",
      display: "Sleep End Time",
      valueDateTime: sleepEnd,
    });
  }

  const awakeningCount = toInteger(
    readUnknown(section, ["AwakeningCount", "awakeningCount"]),
  );
  if (awakeningCount !== null) {
    metrics.push({
      code: "AwakeningCount",
      display: "Awakening Count",
      valueInteger: awakeningCount,
    });
  }

  const obc = toInteger(readUnknown(section, ["OBC", "obc", "OutOfBedCount"]));
  if (obc !== null) {
    metrics.push({
      code: "OBC",
      display: "Out of Bed Count",
      valueInteger: obc,
    });
  }

  return upsertLongestSleepDuration(metrics);
}

function buildStatsObservation(params: {
  idSuffix: string;
  categoryCode: "vital-signs" | "activity";
  categoryDisplay: "Vital Signs" | "Activity";
  system: string;
  code: string;
  display: string;
  effectivePeriod: EffectivePeriod;
  deviceReferenceId: string;
  metrics: StatsMetric[];
}): JsonRecord {
  return {
    resourceType: "Observation",
    status: "final",
    category: [
      {
        coding: [
          {
            system: "http://terminology.hl7.org/CodeSystem/observation-category",
            code: params.categoryCode,
            display: params.categoryDisplay,
          },
        ],
      },
    ],
    code: {
      coding: [
        {
          system: params.system,
          code: params.code,
          display: params.display,
        },
      ],
    },
    subject: { reference: "Patient/not_implemented" },
    effectivePeriod: params.effectivePeriod,
    device: { reference: `Device/${params.deviceReferenceId}` },
    component: params.metrics.map((metric) => metricAsComponent(metric)),
    _idSuffix: params.idSuffix,
  };
}

function removeInternalFields(value: unknown): unknown {
  if (Array.isArray(value)) return value.map((v) => removeInternalFields(v));
  if (!isObjectRecord(value)) return value;
  const out: JsonRecord = {};
  for (const [key, v] of Object.entries(value)) {
    if (key === "_idSuffix") continue;
    out[key] = removeInternalFields(v);
  }
  return out;
}

function ensureLongestSleepDurationForBundle(bundle: JsonRecord): JsonRecord {
  const entry = Array.isArray(bundle.entry) ? bundle.entry : [];
  const patched = entry.map((raw) => {
    if (!isObjectRecord(raw) || !isObjectRecord(raw.resource)) return raw;
    const resource = raw.resource;
    if (resource.resourceType !== "Observation") return raw;
    const coding =
      isObjectRecord(resource.code) &&
      Array.isArray(resource.code.coding) &&
      isObjectRecord(resource.code.coding[0])
        ? resource.code.coding[0]
        : null;
    if (!coding || coding.code !== "sleep-stats") return raw;
    const existingComponents = Array.isArray(resource.component)
      ? resource.component
      : [];
    const metrics: StatsMetric[] = [];
    for (const comp of existingComponents) {
      if (!isObjectRecord(comp)) continue;
      const codeObj =
        isObjectRecord(comp.code) &&
        Array.isArray(comp.code.coding) &&
        isObjectRecord(comp.code.coding[0])
          ? comp.code.coding[0]
          : null;
      if (!codeObj || typeof codeObj.code !== "string") continue;
      if (typeof comp.valueDateTime === "string") {
        metrics.push({
          code: codeObj.code,
          display:
            typeof codeObj.display === "string" ? codeObj.display : codeObj.code,
          valueDateTime: comp.valueDateTime,
        });
      }
    }
    const merged = upsertLongestSleepDuration(metrics);
    const longest = merged.find((m) => m.code === "LongestSleepDuration");
    if (!longest || !longest.valueQuantity) return raw;
    const filtered = existingComponents.filter((comp) => {
      if (!isObjectRecord(comp)) return true;
      const codeObj =
        isObjectRecord(comp.code) &&
        Array.isArray(comp.code.coding) &&
        isObjectRecord(comp.code.coding[0])
          ? comp.code.coding[0]
          : null;
      return !(codeObj && codeObj.code === "LongestSleepDuration");
    });
    return {
      ...raw,
      resource: {
        ...resource,
        component: [...filtered, metricAsComponent(longest)],
      },
    };
  });
  return { ...bundle, entry: patched };
}

function buildBatchStatisticsBundle(
  payload: JsonRecord,
  deviceReferenceId: string,
  receivedAt: Date,
): JsonRecord | null {
  const hrSection =
    readSection(payload, ["hrStats", "hr_stats", "heartRateStats", "heart_rate_stats"]) ??
    readSection(payload, ["hr", "heartRate"]);
  const brSection =
    readSection(payload, ["brStats", "br_stats", "breathingRateStats", "breathing_rate_stats"]) ??
    readSection(payload, ["br", "breathingRate"]);
  const sleepSection =
    readSection(payload, ["sleepStats", "sleep_stats"]) ?? readSection(payload, ["sleep"]);

  const allSectionForPeriod = hrSection ?? brSection ?? sleepSection ?? payload;
  const effectivePeriod = resolveNineAmBatchEffectivePeriod(
    allSectionForPeriod,
    receivedAt,
  );

  const entries: JsonRecord[] = [];

  if (hrSection) {
    const metrics = buildHrStatsMetrics(hrSection);
    if (metrics.length > 0) {
      entries.push({
        resource: buildStatsObservation({
          idSuffix: "hr-stats",
          categoryCode: "vital-signs",
          categoryDisplay: "Vital Signs",
          system: "http://loinc.org",
          code: "8867-4",
          display: "Heart rate statistics",
          effectivePeriod,
          deviceReferenceId,
          metrics,
        }),
      });
    }
  }

  if (brSection) {
    const metrics = buildBrStatsMetrics(brSection);
    if (metrics.length > 0) {
      entries.push({
        resource: buildStatsObservation({
          idSuffix: "br-stats",
          categoryCode: "vital-signs",
          categoryDisplay: "Vital Signs",
          system: "http://loinc.org",
          code: "9279-1",
          display: "Breathing rate statistics",
          effectivePeriod,
          deviceReferenceId,
          metrics,
        }),
      });
    }
  }

  if (sleepSection) {
    const metrics = buildSleepStatsMetrics(sleepSection);
    if (metrics.length > 0) {
      entries.push({
        resource: buildStatsObservation({
          idSuffix: "sleep-stats",
          categoryCode: "activity",
          categoryDisplay: "Activity",
          system: "https://sleepiz.com/fhir/CodeSystem/sleep-metrics",
          code: "sleep-stats",
          display: "Sleep statistics",
          effectivePeriod,
          deviceReferenceId,
          metrics,
        }),
      });
    }
  }

  if (entries.length === 0) return null;

  const bundleId =
    readString(payload, ["id", "bundleId", "bundle_id"]) ??
    `${deviceReferenceId}-${receivedAt.getTime()}`;
  const bundleTimestamp =
    parseIsoDateValue(readUnknown(payload, ["timestamp", "time", "ts"])) ??
    receivedAt.toISOString();

  const withIds = entries.map((entry) => {
    if (!isObjectRecord(entry.resource)) return entry;
    const suffix =
      typeof entry.resource._idSuffix === "string" ? entry.resource._idSuffix : "stats";
    return {
      ...entry,
      resource: {
        ...entry.resource,
        id: `${bundleId}-${suffix}`,
      },
    };
  });

  return removeInternalFields({
    resourceType: "Bundle",
    id: bundleId,
    type: "collection",
    timestamp: bundleTimestamp,
    entry: withIds,
  }) as JsonRecord;
}

function looksLikeHeartRatePayload(topic: string, payload: JsonRecord): boolean {
  if (/\b(hr|heart[_-]?rate|realtime[_-]?hr)\b/i.test(topic)) return true;
  return (
    "hr" in payload ||
    "heartRate" in payload ||
    "heart_rate" in payload ||
    "heartRateValue" in payload ||
    "realtime_hr" in payload
  );
}

function looksLikeBreathingRatePayload(topic: string, payload: JsonRecord): boolean {
  if (/\b(br|rr|resp(?:iratory)?[_-]?rate|breath(?:ing)?[_-]?rate|realtime[_-]?br)\b/i.test(topic)) {
    return true;
  }
  return (
    "br" in payload ||
    "rr" in payload ||
    "breathingRate" in payload ||
    "breathing_rate" in payload ||
    "breathValue" in payload ||
    "respiratoryRate" in payload ||
    "respiratory_rate" in payload ||
    "realtime_br" in payload
  );
}

function normalizeVitalToken(value: unknown): string | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  if (typeof value === "string") {
    const token = value.trim();
    if (!token) return null;
    if (token.toUpperCase() === "E") return "E";
    const asNumber = Number(token);
    if (Number.isFinite(asNumber)) return token;
  }
  return null;
}

function computeAverageFromSampledData(sampledData: string): number | null {
  const numeric = sampledData
    .split(/\s+/)
    .map((token) => Number(token))
    .filter((n) => Number.isFinite(n));
  if (numeric.length === 0) return null;
  const sum = numeric.reduce((acc, n) => acc + n, 0);
  return sum / numeric.length;
}

function extractVitalSample(value: unknown): VitalSample | null {
  if (typeof value === "string") {
    const raw = value.trim();
    if (!raw) return null;
    const sampledData = raw
      .split(/\s+/)
      .map((token) => normalizeVitalToken(token))
      .filter((token): token is string => token !== null)
      .join(" ");
    if (!sampledData) return null;
    return { sampledData, average: computeAverageFromSampledData(sampledData) };
  }

  if (Array.isArray(value)) {
    const sampledData = value
      .map((item) => (item === null ? "E" : normalizeVitalToken(item)))
      .filter((token): token is string => token !== null)
      .join(" ");
    if (!sampledData) return null;
    return { sampledData, average: computeAverageFromSampledData(sampledData) };
  }

  const single = normalizeVitalToken(value);
  if (single === null) return null;
  return {
    sampledData: single,
    average: single === "E" ? null : Number(single),
  };
}

function extractPresenceBatchSample(value: unknown): string | null {
  const normalizeOne = (item: unknown): string | null => {
    if (item === null) return "E";
    if (typeof item === "string" && item.trim().toUpperCase() === "E") return "E";
    return normalizePresenceValue(item);
  };
  if (typeof value === "string") {
    const sampledData = value
      .trim()
      .split(/\s+/)
      .map((token) => normalizeOne(token))
      .filter((token): token is string => token !== null)
      .join(" ");
    return sampledData || null;
  }
  if (Array.isArray(value)) {
    const sampledData = value
      .map((item) => normalizeOne(item))
      .filter((token): token is string => token !== null)
      .join(" ");
    return sampledData || null;
  }
  return normalizeOne(value);
}

function extractSleepBatchSample(value: unknown): string | null {
  const normalizeOne = (item: unknown): string | null => {
    if (item === null) return "E";
    if (typeof item === "string" && item.trim().toUpperCase() === "E") return "E";
    const normalized = normalizeSleepStatusValue(item);
    return normalized?.data ?? null;
  };
  if (typeof value === "string") {
    const sampledData = value
      .trim()
      .split(/\s+/)
      .map((token) => normalizeOne(token))
      .filter((token): token is string => token !== null)
      .join(" ");
    return sampledData || null;
  }
  if (Array.isArray(value)) {
    const sampledData = value
      .map((item) => normalizeOne(item))
      .filter((token): token is string => token !== null)
      .join(" ");
    return sampledData || null;
  }
  return normalizeOne(value);
}

function buildRealtimeVitalObservation(params: {
  code: string;
  display: string;
  unit: string;
  sampledData: string;
  average: number | null;
  effectiveInstant?: string;
  effectivePeriod?: EffectivePeriod;
  period: number;
  deviceReferenceId: string;
  includeAverageComponent: boolean;
}): JsonRecord {
  const observation: JsonRecord = {
    resourceType: "Observation",
    status: "final",
    category: [
      {
        coding: [
          {
            system: "http://terminology.hl7.org/CodeSystem/observation-category",
            code: "vital-signs",
            display: "Vital Signs",
          },
        ],
      },
    ],
    code: {
      coding: [
        {
          system: "http://loinc.org",
          code: params.code,
          display: params.display,
        },
      ],
    },
    subject: { reference: "Patient/not_implemented" },
    valueSampledData: {
      origin: {
        value: 0,
        unit: params.unit,
        system: "http://unitsofmeasure.org",
        code: "/min",
      },
      period: params.period,
      factor: 1,
      dimensions: 1,
      data: params.sampledData,
    },
    device: {
      reference: `Device/${params.deviceReferenceId}`,
    },
  };

  if (params.effectivePeriod) {
    observation.effectivePeriod = params.effectivePeriod;
  } else if (params.effectiveInstant) {
    observation.effectiveInstant = params.effectiveInstant;
  }

  if (params.includeAverageComponent && params.average !== null) {
    observation.component = [
      {
        code: {
          coding: [
            {
              system: "http://loinc.org",
              code: params.code,
              display: "AVERAGE",
            },
          ],
        },
        valueQuantity: {
          value: params.average,
          unit: params.unit,
          system: "http://unitsofmeasure.org",
          code: "/min",
        },
      },
    ];
  }

  return observation;
}

export function normalizePayloadForStorage(
  topic: string,
  payload: unknown,
  deviceId: string | null,
  receivedAt: Date,
): unknown {
  if (!isObjectRecord(payload)) return payload;
  if (payload.resourceType === "Bundle" && looksLikeBatchStatisticsPayload(topic, payload)) {
    return ensureLongestSleepDurationForBundle(payload);
  }
  if (payload.resourceType === "Observation") return payload;

  const payloadPeriod = readNumber(payload, ["period", "periodMs", "samplePeriodMs"]);
  const deviceReferenceId =
    deviceId ??
    readString(payload, ["deviceId", "device_id", "device"]) ??
    "not_implemented";
  const effectiveInstant = resolveEffectiveInstant(payload, receivedAt);
  const isBatch = looksLikeBatchPayload(topic, payload);

  if (looksLikeBatchStatisticsPayload(topic, payload)) {
    const bundle = buildBatchStatisticsBundle(payload, deviceReferenceId, receivedAt);
    if (bundle !== null) return bundle;
  }

  if (looksLikeHeartRatePayload(topic, payload)) {
    const raw = readUnknown(payload, [
      "hr",
      "heartRate",
      "heart_rate",
      "heartRateValue",
      "realtime_hr",
      "batch_hr",
      "batchHr",
      "data",
      "values",
      "samples",
      "value",
    ]);
    const vital = extractVitalSample(raw);
    if (vital !== null) {
      const vitalPeriod =
        payloadPeriod ??
        (isBatch
          ? VITALS_BATCH_DEFAULT_PERIOD_MS
          : VITALS_REALTIME_DEFAULT_PERIOD_MS);
      return buildRealtimeVitalObservation({
        code: "8867-4",
        display: "Heart rate",
        unit: "beats/min",
        sampledData: vital.sampledData,
        average: vital.average,
        effectiveInstant: isBatch ? undefined : effectiveInstant,
        effectivePeriod: isBatch
          ? resolveNineAmBatchEffectivePeriod(payload, receivedAt)
          : undefined,
        period: vitalPeriod,
        deviceReferenceId,
        includeAverageComponent: !isBatch,
      });
    }
  }

  if (looksLikeBreathingRatePayload(topic, payload)) {
    const raw = readUnknown(payload, [
      "br",
      "rr",
      "breathingRate",
      "breathing_rate",
      "breathValue",
      "respiratoryRate",
      "respiratory_rate",
      "realtime_br",
      "batch_br",
      "batchBr",
      "data",
      "values",
      "samples",
      "value",
    ]);
    const vital = extractVitalSample(raw);
    if (vital !== null) {
      const vitalPeriod =
        payloadPeriod ??
        (isBatch
          ? VITALS_BATCH_DEFAULT_PERIOD_MS
          : VITALS_REALTIME_DEFAULT_PERIOD_MS);
      return buildRealtimeVitalObservation({
        code: "9279-1",
        display: "Breathing rate",
        unit: "breaths/min",
        sampledData: vital.sampledData,
        average: vital.average,
        effectiveInstant: isBatch ? undefined : effectiveInstant,
        effectivePeriod: isBatch
          ? resolveNineAmBatchEffectivePeriod(payload, receivedAt)
          : undefined,
        period: vitalPeriod,
        deviceReferenceId,
        includeAverageComponent: !isBatch,
      });
    }
  }

  if (looksLikeSleepStatusPayload(topic, payload)) {
    if (isBatch) {
      const sleepBatchPeriod = payloadPeriod ?? SLEEP_BATCH_DEFAULT_PERIOD_MS;
      const batchRaw = readUnknown(payload, [
        "sleepStatus",
        "sleep_status",
        "sleepstatus",
        "sleep",
        "batch_sleep",
        "batchSleep",
        "data",
        "values",
        "samples",
        "value",
      ]);
      const sampledData = extractSleepBatchSample(batchRaw);
      if (sampledData !== null) {
        const sampledCount = sampledData.split(/\s+/).filter(Boolean).length;
        const effectivePeriod = resolveEffectivePeriod(
          payload,
          receivedAt,
          sampledCount > 1 ? 2 : 1,
          sleepBatchPeriod,
        );
        return {
          resourceType: "Observation",
          status: "final",
          category: [
            {
              coding: [
                {
                  system: "http://terminology.hl7.org/CodeSystem/observation-category",
                  code: "activity",
                  display: "Activity",
                },
              ],
            },
          ],
          code: {
            coding: [
              {
                system: "http://loinc.org",
                code: "107145-5",
                display: "Sleep status",
              },
            ],
          },
          subject: { reference: "Patient/not_implemented" },
          effectivePeriod,
          valueSampledData: {
            origin: {
              value: 0,
              unit: "1",
              system: "http://unitsofmeasure.org",
              code: "1",
            },
            period: sleepBatchPeriod,
            factor: 1,
            dimensions: 1,
            data: sampledData,
          },
          device: {
            reference: `Device/${deviceReferenceId}`,
          },
        };
      }
    }

    const sleepStatus = extractSleepStatusValue(topic, payload);
    if (sleepStatus !== null) {
      const sleepRealtimePeriod = payloadPeriod ?? SLEEP_REALTIME_DEFAULT_PERIOD_MS;
      return {
        resourceType: "Observation",
        status: "final",
        category: [
          {
            coding: [
              {
                system: "http://terminology.hl7.org/CodeSystem/observation-category",
                code: "activity",
                display: "Activity",
              },
            ],
          },
        ],
        code: {
          coding: [
            {
              system: "https://sleepiz.com/fhir/CodeSystem/observation-codes",
              code: "sleep-status",
              display: "Sleep Status",
            },
          ],
        },
        subject: { reference: "Patient/not_implemented" },
        effectiveInstant,
        valueSampledData: {
          origin: {
            value: 0,
            unit: "1",
            system: "http://unitsofmeasure.org",
            code: "1",
          },
          period: sleepRealtimePeriod,
          factor: 1,
          dimensions: 1,
          data: sleepStatus.data,
        },
        valueString: sleepStatus.status,
        device: {
          reference: `Device/${deviceReferenceId}`,
        },
      };
    }
  }

  if (!looksLikePresencePayload(topic, payload)) return payload;

  if (isBatch) {
    const presenceBatchPeriod = payloadPeriod ?? PRESENCE_BATCH_DEFAULT_PERIOD_MS;
    const batchRaw = readUnknown(payload, [
      "presence",
      "present",
      "occupancy",
      "presenceDetected",
      "batch_presence",
      "batchPresence",
      "data",
      "values",
      "samples",
      "value",
    ]);
    const sampledData = extractPresenceBatchSample(batchRaw);
    if (sampledData !== null) {
      const sampledCount = sampledData.split(/\s+/).filter(Boolean).length;
      const effectivePeriod = resolveEffectivePeriod(
        payload,
        receivedAt,
        sampledCount > 1 ? 2 : 1,
        presenceBatchPeriod,
      );
      return {
        resourceType: "Observation",
        status: "final",
        category: [
          {
            coding: [
              {
                system: "http://terminology.hl7.org/CodeSystem/observation-category",
                code: "activity",
                display: "Activity",
              },
            ],
          },
        ],
        code: {
          coding: [
            {
              system: "https://sleepiz.com/fhir/CodeSystem/observation-codes",
              code: "presence-detection",
              display: "Presence",
            },
          ],
        },
        subject: { reference: "Patient/not_implemented" },
        effectivePeriod,
        valueSampledData: {
          origin: {
            value: 0,
            unit: "1",
            system: "http://unitsofmeasure.org",
            code: "1",
          },
          period: presenceBatchPeriod,
          factor: 1,
          dimensions: 1,
          data: sampledData,
        },
        device: {
          reference: `Device/${deviceReferenceId}`,
        },
      };
    }
    return payload;
  }

  const value = extractPresenceValue(payload);
  if (value === null) return payload;
  const presenceRealtimePeriod = payloadPeriod ?? PRESENCE_REALTIME_DEFAULT_PERIOD_MS;

  return {
    resourceType: "Observation",
    status: "final",
    category: [
      {
        coding: [
          {
            system: "http://terminology.hl7.org/CodeSystem/observation-category",
            code: "activity",
            display: "Activity",
          },
        ],
      },
    ],
    code: {
      coding: [
        {
          system: "https://sleepiz.com/fhir/CodeSystem/observation-codes",
          code: "presence-detection",
          display: "Presence",
        },
      ],
    },
    subject: { reference: "Patient/not_implemented" },
    effectiveInstant,
    valueSampledData: {
      origin: {
        value: 0,
        unit: "1",
        system: "http://unitsofmeasure.org",
        code: "1",
      },
      period: presenceRealtimePeriod,
      factor: 1,
      dimensions: 1,
      data: value,
    },
    device: {
      reference: `Device/${deviceReferenceId}`,
    },
  };
}

export function sanitizePayloadForStorage(value: unknown): unknown {
  if (typeof value === "string") {
    return stripNullChars(value);
  }
  if (Array.isArray(value)) {
    return value.map((item) => sanitizePayloadForStorage(item));
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [
        key,
        sanitizePayloadForStorage(item),
      ]),
    );
  }
  return value;
}

export function parsePayloadTextForStorage(text: string): unknown {
  const sanitizedText = stripNullChars(text);
  try {
    return sanitizePayloadForStorage(JSON.parse(sanitizedText) as unknown);
  } catch {
    // Store non-JSON bodies as JSON strings to avoid drops.
    return sanitizedText;
  }
}

function buildClientOptions(
  config: Pick<
    AppConfig,
    | "mqttUsername"
    | "mqttPassword"
    | "mqttClientId"
    | "mqttTlsRejectUnauthorized"
    | "mqttTlsCa"
  >,
): IClientOptions {
  const options: IClientOptions = {
    reconnectPeriod: 5000,
    connectTimeout: 30_000,
  };
  if (config.mqttUsername !== undefined) {
    options.username = config.mqttUsername;
    options.password = config.mqttPassword ?? "";
  }
  if (config.mqttClientId) {
    options.clientId = config.mqttClientId;
  }
  if (config.mqttTlsCa) {
    options.ca = config.mqttTlsCa;
  }
  if (!config.mqttTlsRejectUnauthorized) {
    options.rejectUnauthorized = false;
  }
  return options;
}

export function startMqttIngest(
  config: Pick<
    AppConfig,
    | "mqtt"
    | "mqttUsername"
    | "mqttPassword"
    | "mqttClientId"
    | "mqttTlsRejectUnauthorized"
    | "mqttTlsCa"
    | "mqttTopics"
    | "mqttSubscribeQos"
    | "deviceIdTopicRegex"
    | "deviceIdJsonKey"
    | "skipDeviceIdPrefixes"
    | "batchMax"
    | "ovokIngestEnabled"
    | "ovokIngestBaseUrl"
    | "ovokIngestApiKey"
    | "ovokIngestApiKeyHeader"
    | "ovokIngestTimeoutMs"
    | "storeServerTopics"
  >,
  requestFlush: () => void,
): MqttClient {
  if (!config.mqttTlsRejectUnauthorized) {
    console.warn(
      "[mqtt] TLS certificate verification disabled (MQTT_TLS_INSECURE or MQTT_TLS_REJECT_UNAUTHORIZED=false)",
    );
  }

  const base = buildClientOptions(config);

  const client =
    config.mqtt.kind === "url"
      ? mqtt.connect(config.mqtt.url, base)
      : mqtt.connect({
          ...base,
          protocol: config.mqtt.protocol,
          servers: config.mqtt.servers,
        });

  client.on("connect", () => {
    console.log(`[mqtt] connected (subscribe_qos=${config.mqttSubscribeQos})`);
    for (const t of config.mqttTopics) {
      client.subscribe(t, { qos: config.mqttSubscribeQos }, (err) => {
        if (err) console.error(`[mqtt] subscribe failed for ${t}:`, err);
        else console.log(`[mqtt] subscribed: ${t}`);
      });
    }
  });

  client.on("reconnect", () => {
    console.log("[mqtt] reconnecting...");
  });

  client.on("error", (err) => {
    console.error("[mqtt] error:", err);
  });

  client.on("message", (topic, buf) => {
    if (!config.storeServerTopics && /^MC01\/Server\//i.test(topic)) {
      return;
    }
    const parsed = parsePayloadTextForStorage(buf.toString("utf8"));
    const receivedAt = new Date();
    const rawDeviceId = extractDeviceId(
      topic,
      parsed,
      config.deviceIdTopicRegex,
      config.deviceIdJsonKey,
    );
    const deviceId =
      rawDeviceId === null ? null : stripNullChars(rawDeviceId) || null;
    const normalizedPayload = normalizePayloadForStorage(
      topic,
      parsed,
      deviceId,
      receivedAt,
    );

    const row: QueuedRow = {
      receivedAt,
      topic,
      deviceId,
      payload: normalizedPayload,
    };

    const did = row.deviceId;
    if (did && config.skipDeviceIdPrefixes.some((p) => did.startsWith(p))) {
      return;
    }

    const payloadJson = JSON.stringify(normalizedPayload);
    console.log(
      `[mqtt] message topic=${JSON.stringify(topic)} device_id=${JSON.stringify(row.deviceId)} bytes=${buf.length} payload=${truncateForLog(payloadJson, LOG_PAYLOAD_MAX_CHARS)}`,
    );

    queue.enqueue(row);
    if (queue.depth() >= config.batchMax) {
      requestFlush();
    }
  });

  return client;
}
