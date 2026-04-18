import { afterEach, describe, expect, test } from "bun:test";
import { loadConfig } from "../../src/config";

const keys = [
  "DATABASE_URL",
  "AUTO_MIGRATE",
  "DATABASE_TLS_INSECURE",
  "DATABASE_TLS_REJECT_UNAUTHORIZED",
  "DATABASE_TLS_CA_FILE",
  "MQTT_URL",
  "MQTT_HOST",
  "MQTT_PORT",
  "MQTT_SSL",
  "MQTT_TLS",
  "MQTT_SERVERS",
  "MQTT_USERNAME",
  "MQTT_PASSWORD",
  "MQTT_CLIENT_ID",
  "MQTT_SUBSCRIBE_QOS",
  "MQTT_TLS_INSECURE",
  "MQTT_TLS_REJECT_UNAUTHORIZED",
  "MQTT_TLS_CA_FILE",
  "MQTT_TOPICS",
  "HTTP_PORT",
  "BATCH_MAX",
  "FLUSH_INTERVAL_MS",
  "DEVICE_ID_TOPIC_REGEX",
  "DEVICE_ID_JSON_KEY",
  "SKIP_DEVICE_ID_PREFIXES",
  "OVOK_INGEST_ENABLED",
  "OVOK_INGEST_BASE_URL",
  "OVOK_INGEST_API_KEY",
  "OVOK_INGEST_API_KEY_HEADER",
  "OVOK_INGEST_TIMEOUT_MS",
  "OVOK_SCHEDULED_ENABLED",
  "MQTT_MISSING_VALUE_QUERY_ENABLED",
  "MQTT_MISSING_VALUE_QUERY_CRON",
  "MQTT_MISSING_VALUE_QUERY_LOOKBACK_MS",
  "STORE_SERVER_TOPICS",
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

  /** Default `AUTO_MIGRATE` is on; clear so parent shell env does not affect tests. */
  function baseEnv() {
    prev = snapshotEnv();
    delete process.env.AUTO_MIGRATE;
    delete process.env.DATABASE_TLS_INSECURE;
    delete process.env.DATABASE_TLS_REJECT_UNAUTHORIZED;
    delete process.env.DATABASE_TLS_CA_FILE;
    delete process.env.MQTT_TLS_INSECURE;
    delete process.env.MQTT_TLS_REJECT_UNAUTHORIZED;
    delete process.env.MQTT_TLS_CA_FILE;
    delete process.env.OVOK_INGEST_ENABLED;
    delete process.env.OVOK_INGEST_BASE_URL;
    delete process.env.OVOK_INGEST_API_KEY;
    delete process.env.OVOK_INGEST_API_KEY_HEADER;
    delete process.env.OVOK_INGEST_TIMEOUT_MS;
    delete process.env.OVOK_SCHEDULED_ENABLED;
    delete process.env.MQTT_MISSING_VALUE_QUERY_ENABLED;
    delete process.env.MQTT_MISSING_VALUE_QUERY_CRON;
    delete process.env.MQTT_MISSING_VALUE_QUERY_LOOKBACK_MS;
    delete process.env.STORE_SERVER_TOPICS;
  }

  test("parses required env and defaults", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x/y";
    process.env.MQTT_URL = "mqtt://localhost:1883";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_PORT;
    delete process.env.MQTT_SSL;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_TOPICS = "a/b, #";
    delete process.env.HTTP_PORT;
    delete process.env.BATCH_MAX;
    delete process.env.FLUSH_INTERVAL_MS;
    delete process.env.DEVICE_ID_TOPIC_REGEX;
    delete process.env.DEVICE_ID_JSON_KEY;
    delete process.env.SKIP_DEVICE_ID_PREFIXES;

    const c = loadConfig();
    expect(c.databaseUrl).toBe("postgres://x/y");
    expect(c.autoMigrate).toBe(true);
    expect(c.mqtt).toEqual({ kind: "url", url: "mqtt://localhost:1883" });
    expect(c.mqttTopics).toEqual(["a/b", "#"]);
    expect(c.httpPort).toBe(3000);
    expect(c.batchMax).toBe(100);
    expect(c.flushIntervalMs).toBe(1000);
    expect(c.deviceIdTopicRegex.test("devices/acme/telemetry")).toBe(true);
    expect(c.deviceIdTopicRegex.exec("devices/acme/telemetry")?.[1]).toBe(
      "acme",
    );
    expect(c.deviceIdJsonKey).toBeUndefined();
    expect(c.skipDeviceIdPrefixes).toEqual(["Client-"]);
    expect(c.mqttSubscribeQos).toBe(1);
    expect(c.mqttTlsRejectUnauthorized).toBe(true);
    expect(c.mqttTlsCa).toBeUndefined();
    expect(c.databaseTlsRejectUnauthorized).toBe(true);
    expect(c.databaseTlsCa).toBeUndefined();
    expect(c.ovokIngestEnabled).toBe(false);
    expect(c.ovokIngestBaseUrl).toBe("https://api.dev.ovok.com");
    expect(c.ovokIngestApiKey).toBeUndefined();
    expect(c.ovokIngestApiKeyHeader).toBe("x-api-key");
    expect(c.ovokIngestTimeoutMs).toBe(10000);
    expect(c.ovokScheduledEnabled).toBe(false);
    expect(c.mqttMissingValueQueryEnabled).toBe(false);
    expect(c.mqttMissingValueQueryCron).toBe("*/10 * * * *");
    expect(c.mqttMissingValueQueryLookbackMs).toBe(3600000);
    expect(c.storeServerTopics).toBe(true);
  });

  test("trims topics and optional json key", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_TOPICS = " t1 , t2 ";
    process.env.DEVICE_ID_JSON_KEY = "  did  ";

    const c = loadConfig();
    expect(c.mqttTopics).toEqual(["t1", "t2"]);
    expect(c.deviceIdJsonKey).toBe("did");
  });

  test("parses SKIP_DEVICE_ID_PREFIXES csv list", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_TOPICS = "#";
    process.env.SKIP_DEVICE_ID_PREFIXES = " Client-XYZ , demo- , ";

    const c = loadConfig();
    expect(c.skipDeviceIdPrefixes).toEqual(["Client-XYZ", "demo-"]);
  });

  test("parses MQTT_SUBSCRIBE_QOS", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_TOPICS = "#";
    process.env.MQTT_SUBSCRIBE_QOS = "2";

    const c = loadConfig();
    expect(c.mqttSubscribeQos).toBe(2);
  });

  test("throws when MQTT_TOPICS empty after split", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_TOPICS = "  ,  ";

    expect(() => loadConfig()).toThrow(/at least one topic pattern/i);
  });

  test("throws on invalid DEVICE_ID_TOPIC_REGEX", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_TOPICS = "#";
    process.env.DEVICE_ID_TOPIC_REGEX = "(";

    expect(() => loadConfig()).toThrow(/Invalid DEVICE_ID_TOPIC_REGEX/);
  });

  test("respects numeric overrides and floors", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_SERVERS;
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
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtts://mqtt.example.com:8883";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_SERVERS;
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
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_TOPICS = "#";
    delete process.env.MQTT_USERNAME;
    delete process.env.MQTT_PASSWORD;
    process.env.MQTT_CLIENT_ID = "maifar-storage-1";

    const c = loadConfig();
    expect(c.mqttUsername).toBeUndefined();
    expect(c.mqttPassword).toBeUndefined();
    expect(c.mqttClientId).toBe("maifar-storage-1");
  });

  test("defaults to plain mqtt on port 1883 when TLS flags unset", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    delete process.env.MQTT_URL;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_HOST = "mqtt.example.com";
    delete process.env.MQTT_PORT;
    delete process.env.MQTT_SSL;
    delete process.env.MQTT_TLS;
    process.env.MQTT_TOPICS = "#";

    const c = loadConfig();
    expect(c.mqtt).toEqual({
      kind: "servers",
      protocol: "mqtt",
      servers: [{ host: "mqtt.example.com", port: 1883, protocol: "mqtt" }],
    });
  });

  test("MQTT_SSL=true uses mqtts and port 8883 by default", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    delete process.env.MQTT_URL;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_HOST = "localhost";
    delete process.env.MQTT_PORT;
    process.env.MQTT_SSL = "true";
    delete process.env.MQTT_TLS;
    process.env.MQTT_TOPICS = "#";

    const c = loadConfig();
    expect(c.mqtt).toEqual({
      kind: "servers",
      protocol: "mqtts",
      servers: [{ host: "localhost", port: 8883, protocol: "mqtts" }],
    });
  });

  test("MQTT_SERVERS JSON without MQTT_HOST", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    delete process.env.MQTT_URL;
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_PORT;
    delete process.env.MQTT_SSL;
    process.env.MQTT_SERVERS = JSON.stringify([
      { host: "a.example.com", port: 8883 },
      { host: "b.example.com", port: 8883, protocol: "mqtts" },
    ]);
    process.env.MQTT_TOPICS = "#";

    const c = loadConfig();
    expect(c.mqtt).toEqual({
      kind: "servers",
      protocol: "mqtt",
      servers: [
        { host: "a.example.com", port: 8883 },
        { host: "b.example.com", port: 8883, protocol: "mqtts" },
      ],
    });
  });

  test("MQTT_URL wins when MQTT_HOST and MQTT_SERVERS are also set", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://from-url:1883";
    process.env.MQTT_HOST = "ignored";
    process.env.MQTT_SERVERS = JSON.stringify([{ host: "x", port: 1 }]);
    process.env.MQTT_TOPICS = "#";

    const c = loadConfig();
    expect(c.mqtt).toEqual({ kind: "url", url: "mqtt://from-url:1883" });
  });

  test("throws when MQTT_HOST is only whitespace", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    delete process.env.MQTT_URL;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_HOST = "   ";
    process.env.MQTT_TOPICS = "#";

    expect(() => loadConfig()).toThrow(/MQTT_HOST/);
  });

  test("throws when MQTT_URL and MQTT_HOST both missing and no MQTT_SERVERS", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    delete process.env.MQTT_URL;
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_TOPICS = "#";

    expect(() => loadConfig()).toThrow(/MQTT_HOST/);
  });

  test("throws on invalid MQTT_PORT", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    delete process.env.MQTT_URL;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_HOST = "h";
    process.env.MQTT_PORT = "70000";
    process.env.MQTT_TOPICS = "#";

    expect(() => loadConfig()).toThrow(/MQTT_PORT/);
  });

  test("throws on invalid MQTT_SUBSCRIBE_QOS", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_TOPICS = "#";
    process.env.MQTT_SUBSCRIBE_QOS = "3";

    expect(() => loadConfig()).toThrow(/MQTT_SUBSCRIBE_QOS/);
  });

  test("AUTO_MIGRATE false disables auto-migrate flag", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_TOPICS = "#";
    process.env.AUTO_MIGRATE = "false";

    const c = loadConfig();
    expect(c.autoMigrate).toBe(false);
  });

  test("OVOK_SCHEDULED_ENABLED=true enables cron forwarding mode", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_TOPICS = "#";
    process.env.OVOK_SCHEDULED_ENABLED = "true";

    const c = loadConfig();
    expect(c.ovokScheduledEnabled).toBe(true);
  });

  test("parses MQTT missing-value query and store-server toggles", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_TOPICS = "#";
    process.env.MQTT_MISSING_VALUE_QUERY_ENABLED = "true";
    process.env.MQTT_MISSING_VALUE_QUERY_CRON = "*/5 * * * *";
    process.env.MQTT_MISSING_VALUE_QUERY_LOOKBACK_MS = "120000";
    process.env.STORE_SERVER_TOPICS = "false";

    const c = loadConfig();
    expect(c.mqttMissingValueQueryEnabled).toBe(true);
    expect(c.mqttMissingValueQueryCron).toBe("*/5 * * * *");
    expect(c.mqttMissingValueQueryLookbackMs).toBe(120000);
    expect(c.storeServerTopics).toBe(false);
  });

  test("MQTT_TLS_INSECURE=true disables TLS verification flag", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_TOPICS = "#";
    process.env.MQTT_TLS_INSECURE = "true";

    const c = loadConfig();
    expect(c.mqttTlsRejectUnauthorized).toBe(false);
  });

  test("MQTT_TLS_REJECT_UNAUTHORIZED=false disables verification", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_TOPICS = "#";
    process.env.MQTT_TLS_REJECT_UNAUTHORIZED = "false";

    const c = loadConfig();
    expect(c.mqttTlsRejectUnauthorized).toBe(false);
  });

  test("throws when MQTT_TLS_CA_FILE is missing", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_TOPICS = "#";
    process.env.MQTT_TLS_CA_FILE = "/nonexistent/maifar-ca-bundle.pem";

    expect(() => loadConfig()).toThrow(/MQTT_TLS_CA_FILE/);
  });

  test("DATABASE_TLS_INSECURE=true disables Postgres TLS verification flag", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_TOPICS = "#";
    process.env.DATABASE_TLS_INSECURE = "true";

    const c = loadConfig();
    expect(c.databaseTlsRejectUnauthorized).toBe(false);
  });

  test("throws when DATABASE_TLS_CA_FILE is missing", () => {
    baseEnv();
    process.env.DATABASE_URL = "postgres://x";
    process.env.MQTT_URL = "mqtt://h";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_TOPICS = "#";
    process.env.DATABASE_TLS_CA_FILE = "/nonexistent/db-ca.pem";

    expect(() => loadConfig()).toThrow(/DATABASE_TLS_CA_FILE/);
  });
});
