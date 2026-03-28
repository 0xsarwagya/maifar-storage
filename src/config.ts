function requireEnv(name: string): string {
  const v = process.env[name];
  if (v === undefined || v === "") {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return v;
}

function resolveMqttUrl(): string {
  const fromUrl = process.env.MQTT_URL?.trim();
  if (fromUrl) return fromUrl;

  const host = requireEnv("MQTT_HOST").trim();
  if (!host) {
    throw new Error(
      "MQTT_HOST cannot be empty or whitespace when MQTT_URL is unset",
    );
  }
  const tlsOff =
    process.env.MQTT_SSL === "false" || process.env.MQTT_TLS === "false";
  const scheme = tlsOff ? "mqtt" : "mqtts";
  const defaultPort = tlsOff ? 1883 : 8883;
  const portRaw = process.env.MQTT_PORT?.trim();
  const port = portRaw ? Number(portRaw) : defaultPort;
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("MQTT_PORT must be an integer from 1 to 65535");
  }
  return `${scheme}://${host}:${port}`;
}

export type AppConfig = {
  databaseUrl: string;
  mqttUrl: string;
  /** When set, sent as MQTT `username` / `password` (avoids putting secrets in `MQTT_URL`). */
  mqttUsername?: string;
  mqttPassword?: string;
  /** Optional stable client id for the broker session. */
  mqttClientId?: string;
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

  return {
    databaseUrl: requireEnv("DATABASE_URL"),
    mqttUrl: resolveMqttUrl(),
    mqttUsername,
    mqttPassword,
    mqttClientId,
    mqttTopics: topics,
    httpPort: Number(process.env.HTTP_PORT ?? 3000) || 3000,
    batchMax: Math.max(1, Number(process.env.BATCH_MAX ?? 100)),
    flushIntervalMs: Math.max(50, Number(process.env.FLUSH_INTERVAL_MS ?? 1000)),
    deviceIdTopicRegex,
    deviceIdJsonKey: jsonKey || undefined,
  };
}
