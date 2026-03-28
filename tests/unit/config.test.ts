import { afterEach, describe, expect, test } from "bun:test";
import { loadConfig } from "../../src/config";

const keys = [
  "DATABASE_URL",
  "MQTT_URL",
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

function snapshotEnv(): Record<string, string | undefined> {
  const out: Record<string, string | undefined> = {};
  for (const k of keys) {
    out[k] = process.env[k];
  }
  return out;
}

function restoreEnv(prev: Record<string, string | undefined>) {
  for (const k of keys) {
    const v = prev[k];
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
}

describe("loadConfig", () => {
  let prev: Record<string, string | undefined>;

  afterEach(() => {
    restoreEnv(prev);
  });

  test("parses required env and defaults", () => {
    prev = snapshotEnv();
    process.env.DATABASE_URL = "postgres://x/y";
    process.env.MQTT_URL = "mqtt://localhost:1883";
    process.env.MQTT_TOPICS = "a/b, #";
    delete process.env.HTTP_PORT;
    delete process.env.BATCH_MAX;
    delete process.env.FLUSH_INTERVAL_MS;
    delete process.env.DEVICE_ID_TOPIC_REGEX;
    delete process.env.DEVICE_ID_JSON_KEY;

    const c = loadConfig();
    expect(c.databaseUrl).toBe("postgres://x/y");
    expect(c.mqttUrl).toBe("mqtt://localhost:1883");
    expect(c.mqttTopics).toEqual(["a/b", "#"]);
    expect(c.httpPort).toBe(3000);
    expect(c.batchMax).toBe(100);
    expect(c.flushIntervalMs).toBe(1000);
    expect(c.deviceIdTopicRegex.test("devices/acme/telemetry")).toBe(true);
    expect(c.deviceIdTopicRegex.exec("devices/acme/telemetry")?.[1]).toBe(
      "acme",
    );
    expect(c.deviceIdJsonKey).toBeUndefined();
  });

  test("trims topics and optional json key", () => {
    prev = snapshotEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    process.env.MQTT_TOPICS = " t1 , t2 ";
    process.env.DEVICE_ID_JSON_KEY = "  did  ";

    const c = loadConfig();
    expect(c.mqttTopics).toEqual(["t1", "t2"]);
    expect(c.deviceIdJsonKey).toBe("did");
  });

  test("throws when MQTT_TOPICS empty after split", () => {
    prev = snapshotEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    process.env.MQTT_TOPICS = "  ,  ";

    expect(() => loadConfig()).toThrow(/at least one topic pattern/i);
  });

  test("throws on invalid DEVICE_ID_TOPIC_REGEX", () => {
    prev = snapshotEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    process.env.MQTT_TOPICS = "#";
    process.env.DEVICE_ID_TOPIC_REGEX = "(";

    expect(() => loadConfig()).toThrow(/Invalid DEVICE_ID_TOPIC_REGEX/);
  });

  test("respects numeric overrides and floors", () => {
    prev = snapshotEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    process.env.MQTT_TOPICS = "#";
    process.env.HTTP_PORT = "0";
    process.env.BATCH_MAX = "0";
    process.env.FLUSH_INTERVAL_MS = "10";

    const c = loadConfig();
    expect(c.httpPort).toBe(3000);
    expect(c.batchMax).toBe(1);
    expect(c.flushIntervalMs).toBe(50);
  });

  test("MQTT_USERNAME and MQTT_PASSWORD for broker auth", () => {
    prev = snapshotEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtts://mqtt.example.com:8883";
    process.env.MQTT_TOPICS = "#";
    delete process.env.MQTT_CLIENT_ID;
    process.env.MQTT_USERNAME = "device-reader";
    process.env.MQTT_PASSWORD = "secret";

    const c = loadConfig();
    expect(c.mqttUsername).toBe("device-reader");
    expect(c.mqttPassword).toBe("secret");
    expect(c.mqttClientId).toBeUndefined();
  });

  test("MQTT auth omitted when MQTT_USERNAME empty", () => {
    prev = snapshotEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    process.env.MQTT_TOPICS = "#";
    delete process.env.MQTT_USERNAME;
    delete process.env.MQTT_PASSWORD;
    process.env.MQTT_CLIENT_ID = "maifar-storage-1";

    const c = loadConfig();
    expect(c.mqttUsername).toBeUndefined();
    expect(c.mqttPassword).toBeUndefined();
    expect(c.mqttClientId).toBe("maifar-storage-1");
  });
});
