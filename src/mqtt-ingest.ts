import mqtt, { type IClientOptions, type MqttClient } from "mqtt";
import type { AppConfig } from "./config";
import { extractDeviceId } from "./device-id";
import * as queue from "./queue";
import type { QueuedRow } from "./types";

/** Max JSON characters logged per message payload (full row still stored). */
const LOG_PAYLOAD_MAX_CHARS = 16_384;

function truncateForLog(s: string, max: number): string {
  if (s.length <= max) return s;
  return `${s.slice(0, max)}… (+${s.length - max} more chars)`;
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
    const text = buf.toString("utf8");

    let parsed: unknown;
    try {
      parsed = JSON.parse(text) as unknown;
    } catch {
      // Store non-JSON bodies as JSON strings to avoid drops.
      parsed = text;
    }

    const row: QueuedRow = {
      receivedAt: new Date(),
      topic,
      deviceId: extractDeviceId(
        topic,
        parsed,
        config.deviceIdTopicRegex,
        config.deviceIdJsonKey,
      ),
      payload: parsed,
    };

    const did = row.deviceId;
    if (did && config.skipDeviceIdPrefixes.some((p) => did.startsWith(p))) {
      return;
    }

    const payloadJson = JSON.stringify(parsed);
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
