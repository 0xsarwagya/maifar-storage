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
  httpPort: number;
  batchMax: number;
  flushIntervalMs: number;
  deviceIdTopicRegex: RegExp;
  deviceIdJsonKey?: string;
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
  const mqttUsernameRaw = process.env.MQTT_USERNAME?.trim();
  const mqttUsername =
    mqttUsernameRaw && mqttUsernameRaw.length > 0 ? mqttUsernameRaw : undefined;
  const mqttPassword =
    mqttUsername !== undefined ? (process.env.MQTT_PASSWORD ?? "") : undefined;
  const mqttClientId = process.env.MQTT_CLIENT_ID?.trim() || undefined;

  const autoMigrateRaw = process.env.AUTO_MIGRATE?.trim().toLowerCase();
  const autoMigrate =
    autoMigrateRaw !== "false" &&
    autoMigrateRaw !== "0" &&
    autoMigrateRaw !== "no" &&
    autoMigrateRaw !== "off";

  const databaseTls = loadDatabaseTlsFromEnv();

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
    httpPort: Number(process.env.HTTP_PORT ?? 3000) || 3000,
    batchMax: Math.max(1, Number(process.env.BATCH_MAX ?? 100)),
    flushIntervalMs: Math.max(50, Number(process.env.FLUSH_INTERVAL_MS ?? 1000)),
    deviceIdTopicRegex,
    deviceIdJsonKey: jsonKey || undefined,
  };
}
