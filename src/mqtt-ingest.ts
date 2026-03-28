import mqtt, { type IClientOptions, type MqttClient } from "mqtt";
import type { AppConfig } from "./config";
import { extractDeviceId } from "./device-id";
import * as queue from "./queue";
import type { QueuedRow } from "./types";

export function startMqttIngest(
  config: Pick<
    AppConfig,
    | "mqttUrl"
    | "mqttUsername"
    | "mqttPassword"
    | "mqttClientId"
    | "mqttTopics"
    | "deviceIdTopicRegex"
    | "deviceIdJsonKey"
    | "batchMax"
  >,
  requestFlush: () => void,
): MqttClient {
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

  const client = mqtt.connect(config.mqttUrl, options);

  client.on("connect", () => {
    console.log("[mqtt] connected");
    for (const t of config.mqttTopics) {
      client.subscribe(t, { qos: 0 }, (err) => {
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
    let text: string;
    try {
      text = buf.toString("utf8");
    } catch {
      console.error("[mqtt] invalid utf-8, topic:", topic);
      return;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(text) as unknown;
    } catch {
      console.error("[mqtt] skip non-json payload, topic:", topic);
      return;
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

    queue.enqueue(row);
    if (queue.depth() >= config.batchMax) {
      requestFlush();
    }
  });

  return client;
}
