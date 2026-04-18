import { readFileSync } from "node:fs";

/** Opt-in TLS: `true`, `1`, `yes`, `on` (case-insensitive). */
function mqttTlsEnvTruthy(raw: string | undefined): boolean {
  const v = raw?.trim().toLowerCase();
  return v === "true" || v === "1" || v === "yes" || v === "on";
}

/** Default verify server cert for MQTTS. */
function resolveMqttTlsRejectUnauthorized(): boolean {
  if (mqttTlsEnvTruthy(process.env.MQTT_TLS_INSECURE)) {
    return false;
  }
  const rau = process.env.MQTT_TLS_REJECT_UNAUTHORIZED?.trim().toLowerCase();
  if (
    rau === "false" ||
    rau === "0" ||
    rau === "no" ||
    rau === "off"
  ) {
    return false;
  }
  return true;
}

function resolveMqttTlsCaPem(): string | undefined {
  const p = process.env.MQTT_TLS_CA_FILE?.trim();
  if (!p) return undefined;
  try {
    return readFileSync(p, "utf8");
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    throw new Error(`MQTT_TLS_CA_FILE (${p}): ${msg}`);
  }
}

/** PostgreSQL (used by migrate + pool): verify server cert unless opted out. */
function resolveDatabaseTlsRejectUnauthorized(): boolean {
  if (mqttTlsEnvTruthy(process.env.DATABASE_TLS_INSECURE)) {
    return false;
  }
  const rau = process.env.DATABASE_TLS_REJECT_UNAUTHORIZED?.trim().toLowerCase();
  if (
    rau === "false" ||
    rau === "0" ||
    rau === "no" ||
    rau === "off"
  ) {
    return false;
  }
  return true;
}

function resolveDatabaseTlsCaPem(): string | undefined {
  const p = process.env.DATABASE_TLS_CA_FILE?.trim();
  if (!p) return undefined;
  try {
    return readFileSync(p, "utf8");
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    throw new Error(`DATABASE_TLS_CA_FILE (${p}): ${msg}`);
  }
}

export type DatabaseTlsSettings = {
  databaseTlsRejectUnauthorized: boolean;
  databaseTlsCa?: string;
};

/** For `postgres` + migrate script without full app env. */
export function loadDatabaseTlsFromEnv(): DatabaseTlsSettings {
  return {
    databaseTlsRejectUnauthorized: resolveDatabaseTlsRejectUnauthorized(),
    databaseTlsCa: resolveDatabaseTlsCaPem(),
  };
}

function requireEnv(name: string): string {
  const v = process.env[name];
  if (v === undefined || v === "") {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return v;
}

export type MqttServerEntry = {
  host: string;
  port: number;
  protocol?: "mqtt" | "mqtts";
};

/** How to reach the broker: full URL, or `servers` list (iterated on reconnect). */
export type MqttConnectionConfig =
  | { kind: "url"; url: string }
  | { kind: "servers"; protocol: "mqtt" | "mqtts"; servers: MqttServerEntry[] };

function parseMqttServersJson(raw: string): MqttServerEntry[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error("MQTT_SERVERS must be valid JSON");
  }
  if (!Array.isArray(parsed) || parsed.length === 0) {
    throw new Error("MQTT_SERVERS must be a non-empty JSON array");
  }
  const out: MqttServerEntry[] = [];
  for (let i = 0; i < parsed.length; i++) {
    const item = parsed[i];
    if (!item || typeof item !== "object") {
      throw new Error(`MQTT_SERVERS[${i}] must be an object with host and port`);
    }
    const o = item as Record<string, unknown>;
    const host = String(o.host ?? "").trim();
    const port = Number(o.port);
    if (!host) {
      throw new Error(`MQTT_SERVERS[${i}].host is required`);
    }
    if (!Number.isInteger(port) || port < 1 || port > 65535) {
      throw new Error(`MQTT_SERVERS[${i}].port must be an integer from 1 to 65535`);
    }
    const pr = o.protocol;
    const protocol =
      pr === "mqtt" || pr === "mqtts" ? (pr as "mqtt" | "mqtts") : undefined;
    out.push({ host, port, protocol });
  }
  return out;
}

function resolveMqttConnection(): MqttConnectionConfig {
  const fromUrl = process.env.MQTT_URL?.trim();
  if (fromUrl) {
    return { kind: "url", url: fromUrl };
  }

  const tlsOn =
    mqttTlsEnvTruthy(process.env.MQTT_SSL) ||
    mqttTlsEnvTruthy(process.env.MQTT_TLS);
  const defaultProtocol = tlsOn ? "mqtts" : "mqtt";
  const defaultPort = tlsOn ? 8883 : 1883;

  const serversJson = process.env.MQTT_SERVERS?.trim();
  if (serversJson) {
    const servers = parseMqttServersJson(serversJson);
    return { kind: "servers", protocol: defaultProtocol, servers };
  }

  const host = requireEnv("MQTT_HOST").trim();
  if (!host) {
    throw new Error(
      "MQTT_HOST cannot be empty or whitespace when MQTT_URL is unset",
    );
  }
  const portRaw = process.env.MQTT_PORT?.trim();
  const port = portRaw ? Number(portRaw) : defaultPort;
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("MQTT_PORT must be an integer from 1 to 65535");
  }

  return {
    kind: "servers",
    protocol: defaultProtocol,
    servers: [{ host, port, protocol: defaultProtocol }],
  };
}

export type AppConfig = {
  databaseUrl: string;
  databaseTlsRejectUnauthorized: boolean;
  databaseTlsCa?: string;
  /** When true, `src/index.ts` applies `schema.sql` before serving (idempotent). */
  autoMigrate: boolean;
  mqtt: MqttConnectionConfig;
  /** When set, sent as MQTT `username` / `password` (avoids putting secrets in `MQTT_URL`). */
  mqttUsername?: string;
  mqttPassword?: string;
  /** Optional stable client id for the broker session. */
  mqttClientId?: string;
  /** When false, MQTTS accepts self-signed / unknown CAs (insecure). */
  mqttTlsRejectUnauthorized: boolean;
  /** PEM bundle read from `MQTT_TLS_CA_FILE`, if set. */
  mqttTlsCa?: string;
  mqttTopics: string[];
  /** MQTT subscribe QoS (0/1/2). Default 1 for better delivery reliability. */
  mqttSubscribeQos: 0 | 1 | 2;
  httpPort: number;
  batchMax: number;
  flushIntervalMs: number;
  deviceIdTopicRegex: RegExp;
  deviceIdJsonKey?: string;
  /** Comma-separated prefixes; matching device ids are skipped before storage. */
  skipDeviceIdPrefixes: string[];
  /** Forward normalized FHIR payloads to Ovok ingest API. */
  ovokIngestEnabled: boolean;
  /** Ovok ingest base URL (no trailing slash). */
  ovokIngestBaseUrl: string;
  /** Optional API key for Ovok ingest API. */
  ovokIngestApiKey?: string;
  /** Header name for API key when `ovokIngestApiKey` is set. */
  ovokIngestApiKeyHeader: string;
  /** HTTP timeout for Ovok ingest forwarding in ms. */
  ovokIngestTimeoutMs: number;
  /** Use cron-based DB-driven forwarding instead of per-row forwarding. */
  ovokScheduledEnabled: boolean;
};

export function loadConfig(): AppConfig {
  const topics = requireEnv("MQTT_TOPICS")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  if (topics.length === 0) {
    throw new Error("MQTT_TOPICS must list at least one topic pattern");
  }

  const regexSource = process.env.DEVICE_ID_TOPIC_REGEX ?? "^devices/([^/]+)/";
  let deviceIdTopicRegex: RegExp;
  try {
    deviceIdTopicRegex = new RegExp(regexSource);
  } catch {
    throw new Error(`Invalid DEVICE_ID_TOPIC_REGEX: ${regexSource}`);
  }

  const jsonKey = process.env.DEVICE_ID_JSON_KEY?.trim();
  const skipDeviceIdPrefixes = (process.env.SKIP_DEVICE_ID_PREFIXES ?? "Client-")
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  const mqttUsernameRaw = process.env.MQTT_USERNAME?.trim();
  const mqttUsername =
    mqttUsernameRaw && mqttUsernameRaw.length > 0 ? mqttUsernameRaw : undefined;
  const mqttPassword =
    mqttUsername !== undefined ? (process.env.MQTT_PASSWORD ?? "") : undefined;
  const mqttClientId = process.env.MQTT_CLIENT_ID?.trim() || undefined;
  const mqttSubscribeQosRaw = process.env.MQTT_SUBSCRIBE_QOS?.trim();
  const mqttSubscribeQos = (mqttSubscribeQosRaw === undefined ||
  mqttSubscribeQosRaw === "")
    ? 1
    : Number(mqttSubscribeQosRaw);
  if (
    !Number.isInteger(mqttSubscribeQos) ||
    (mqttSubscribeQos !== 0 &&
      mqttSubscribeQos !== 1 &&
      mqttSubscribeQos !== 2)
  ) {
    throw new Error("MQTT_SUBSCRIBE_QOS must be 0, 1, or 2");
  }

  const autoMigrateRaw = process.env.AUTO_MIGRATE?.trim().toLowerCase();
  const autoMigrate =
    autoMigrateRaw !== "false" &&
    autoMigrateRaw !== "0" &&
    autoMigrateRaw !== "no" &&
    autoMigrateRaw !== "off";

  const databaseTls = loadDatabaseTlsFromEnv();
  const ovokIngestEnabled = mqttTlsEnvTruthy(process.env.OVOK_INGEST_ENABLED);
  const ovokIngestBaseUrlRaw =
    process.env.OVOK_INGEST_BASE_URL?.trim() || "https://api.dev.ovok.com";
  const ovokIngestBaseUrl = ovokIngestBaseUrlRaw.replace(/\/+$/, "");
  const ovokIngestApiKey = process.env.OVOK_INGEST_API_KEY?.trim() || undefined;
  const ovokIngestApiKeyHeader =
    process.env.OVOK_INGEST_API_KEY_HEADER?.trim() || "x-api-key";
  const ovokIngestTimeoutMs = Math.max(
    1000,
    Number(process.env.OVOK_INGEST_TIMEOUT_MS ?? 10_000) || 10_000,
  );
  const ovokScheduledEnabled = mqttTlsEnvTruthy(
    process.env.OVOK_SCHEDULED_ENABLED,
  );

  return {
    databaseUrl: requireEnv("DATABASE_URL"),
    ...databaseTls,
    autoMigrate,
    mqtt: resolveMqttConnection(),
    mqttUsername,
    mqttPassword,
    mqttClientId,
    mqttTlsRejectUnauthorized: resolveMqttTlsRejectUnauthorized(),
    mqttTlsCa: resolveMqttTlsCaPem(),
    mqttTopics: topics,
    mqttSubscribeQos,
    httpPort: Number(process.env.HTTP_PORT ?? 3000) || 3000,
    batchMax: Math.max(1, Number(process.env.BATCH_MAX ?? 100)),
    flushIntervalMs: Math.max(50, Number(process.env.FLUSH_INTERVAL_MS ?? 1000)),
    deviceIdTopicRegex,
    deviceIdJsonKey: jsonKey || undefined,
    skipDeviceIdPrefixes,
    ovokIngestEnabled,
    ovokIngestBaseUrl,
    ovokIngestApiKey,
    ovokIngestApiKeyHeader,
    ovokIngestTimeoutMs,
    ovokScheduledEnabled,
  };
}
