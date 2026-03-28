const KEYS = [
  "DATABASE_URL",
  "MQTT_URL",
  "MQTT_HOST",
  "MQTT_PORT",
  "MQTT_SSL",
  "MQTT_TLS",
  "MQTT_SERVERS",
  "MQTT_USERNAME",
  "MQTT_PASSWORD",
  "MQTT_CLIENT_ID",
  "MQTT_TOPICS",
  "HTTP_PORT",
  "BATCH_MAX",
  "FLUSH_INTERVAL_MS",
  "DEVICE_ID_TOPIC_REGEX",
  "DEVICE_ID_JSON_KEY",
] as const;

export type EnvSnapshot = Record<string, string | undefined>;

export function snapshotEnv(): EnvSnapshot {
  const out: EnvSnapshot = {};
  for (const k of KEYS) {
    out[k] = process.env[k];
  }
  return out;
}

export function restoreEnv(prev: EnvSnapshot) {
  for (const k of KEYS) {
    const v = prev[k];
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
}
