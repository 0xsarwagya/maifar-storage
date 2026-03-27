function requireEnv(name: string): string {
  const v = process.env[name];
  if (v === undefined || v === "") {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return v;
}

export type AppConfig = {
  databaseUrl: string;
  mqttUrl: string;
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
  return {
    databaseUrl: requireEnv("DATABASE_URL"),
    mqttUrl: requireEnv("MQTT_URL"),
    mqttTopics: topics,
    httpPort: Number(process.env.HTTP_PORT ?? 3000) || 3000,
    batchMax: Math.max(1, Number(process.env.BATCH_MAX ?? 100)),
    flushIntervalMs: Math.max(50, Number(process.env.FLUSH_INTERVAL_MS ?? 1000)),
    deviceIdTopicRegex,
    deviceIdJsonKey: jsonKey || undefined,
  };
}
