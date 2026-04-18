import cron, { type ScheduledTask } from "node-cron";
import type { MqttClient } from "mqtt";
import type { AppConfig } from "./config";
import type { Sql } from "./db";

type QueryTarget = {
  commandKey: string;
  method: "get" | "set";
  commandValue?: string;
};

const QUERY_TARGETS: QueryTarget[] = [
  {
    commandKey: "HeartRateGet",
    method: "get",
  },
  {
    commandKey: "SomeoneExistsGet",
    method: "get",
  },
  {
    commandKey: "HeartRateValueGet",
    method: "get",
  },
  {
    commandKey: "BreathRateGet",
    method: "get",
  },
  {
    commandKey: "HeartRateValueGet",
    method: "set",
    commandValue: "1",
  },
  {
    commandKey: "BreathValueGet",
    method: "get",
  },
  {
    commandKey: "BreathValueGet",
    method: "set",
    commandValue: "1",
  },
  {
    commandKey: "SleepStatusGet",
    method: "get",
  },
  {
    commandKey: "SleepStatusGet",
    method: "set",
    commandValue: "1",
  },
  {
    commandKey: "SleepComprehensiveStatusGet",
    method: "get",
  },
  {
    commandKey: "HumanPositionGet",
    method: "get",
  },
  {
    commandKey: "HumanPositionGet",
    method: "set",
    commandValue: "1",
  },
  {
    commandKey: "HumanDistanceGet",
    method: "get",
  },
  {
    commandKey: "HumanDistanceGet",
    method: "set",
    commandValue: "1",
  },
  {
    commandKey: "MotionStatusGet",
    method: "get",
  },
  {
    commandKey: "MotionStatusGet",
    method: "set",
    commandValue: "1",
  },
  {
    commandKey: "MovementSignsGet",
    method: "get",
  },
  {
    commandKey: "LocationOutOfBoundsGet",
    method: "get",
  },
  {
    commandKey: "GetIntoBedGet",
    method: "get",
  },
  {
    commandKey: "BreathInformGet",
    method: "get",
  },
  {
    commandKey: "LightLimitGet",
    method: "set",
    commandValue: "[60,60,60,60]",
  },
];

type MissingQueryConfig = Pick<
  AppConfig,
  | "mqttMissingValueQueryEnabled"
  | "mqttMissingValueQueryCron"
  | "mqttMissingValueQueryLookbackMs"
>;

export type MqttMissingValueQueryHandle = {
  stop: () => void;
};

function publishCommand(
  mqttClient: MqttClient,
  topic: string,
  payload: string,
): Promise<void> {
  return new Promise((resolve, reject) => {
    mqttClient.publish(topic, payload, { qos: 1 }, (error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}

async function fetchDeviceIds(sql: Sql): Promise<string[]> {
  const rows = await sql<{ deviceId: string }[]>`
    select distinct device_id as "deviceId"
    from device_messages
    where device_id is not null and btrim(device_id) <> ''
  `;
  return rows.map((row) => row.deviceId);
}

export function startMqttMissingValueQueryCron(
  sql: Sql,
  mqttClient: MqttClient,
  config: MissingQueryConfig,
): MqttMissingValueQueryHandle {
  if (!config.mqttMissingValueQueryEnabled) {
    return { stop: () => {} };
  }
  if (!cron.validate(config.mqttMissingValueQueryCron)) {
    throw new Error(
      `Invalid MQTT_MISSING_VALUE_QUERY_CRON expression: ${config.mqttMissingValueQueryCron}`,
    );
  }

  let inFlight = false;
  async function run(): Promise<void> {
    if (!mqttClient.connected) {
      console.log("[mqtt-query] skip broker disconnected");
      return;
    }
    const deviceIds = await fetchDeviceIds(sql);
    for (const deviceId of deviceIds) {
      for (const target of QUERY_TARGETS) {
        const topic = `MC01/Server/${deviceId}`;
        const payloadBody: Record<string, string> = {
          method: target.method,
          [target.commandKey]: target.commandValue ?? "",
        };
        const payload = JSON.stringify(payloadBody);
        try {
          await publishCommand(mqttClient, topic, payload);
          console.log(
            `[mqtt-query] sent device_id=${deviceId} method=${target.method} command=${target.commandKey}`,
          );
        } catch (error) {
          console.error(
            `[mqtt-query] publish failed device_id=${deviceId} method=${target.method} command=${target.commandKey}`,
            error,
          );
        }
      }
    }
  }

  const task: ScheduledTask = cron.schedule(
    config.mqttMissingValueQueryCron,
    async () => {
      if (inFlight) {
        console.log("[mqtt-query] skip in-flight");
        return;
      }
      inFlight = true;
      try {
        await run();
      } catch (error) {
        console.error("[mqtt-query] job failed", error);
      } finally {
        inFlight = false;
      }
    },
    { timezone: "UTC" },
  );

  console.log(
    `[mqtt-query] enabled cron="${config.mqttMissingValueQueryCron}" lookback_ms=${config.mqttMissingValueQueryLookbackMs} poll_mode=continuous`,
  );

  return {
    stop: () => {
      task.stop();
    },
  };
}
