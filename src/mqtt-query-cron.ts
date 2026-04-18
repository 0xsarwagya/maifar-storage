import cron, { type ScheduledTask } from "node-cron";
import type { MqttClient } from "mqtt";
import type { AppConfig } from "./config";
import type { Sql } from "./db";

type QueryTarget = {
  telemetryKey: string;
  commandKey: string;
  commandValue: string;
};

const QUERY_TARGETS: QueryTarget[] = [
  {
    telemetryKey: "someoneExists",
    commandKey: "SomeoneExistsGet",
    commandValue: "1",
  },
  {
    telemetryKey: "heartRateValue",
    commandKey: "HeartRateValueGet",
    commandValue: "1",
  },
  {
    telemetryKey: "breathValue",
    commandKey: "BreathValueGet",
    commandValue: "1",
  },
  {
    telemetryKey: "sleepStatus",
    commandKey: "SleepStatusGet",
    commandValue: "1",
  },
  {
    telemetryKey: "humanPosition",
    commandKey: "HumanPositionGet",
    commandValue: "1",
  },
  {
    telemetryKey: "humanDistance",
    commandKey: "HumanDistanceGet",
    commandValue: "1",
  },
  {
    telemetryKey: "motionStatus",
    commandKey: "MotionStatusGet",
    commandValue: "1",
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

async function hasRecentTelemetry(
  sql: Sql,
  deviceId: string,
  telemetryKey: string,
  from: Date,
): Promise<boolean> {
  const rows = await sql<{ found: boolean }[]>`
    select exists(
      select 1
      from device_messages
      where device_id = ${deviceId}
        and topic like 'MC01/Client/%'
        and received_at >= ${from}
        and payload ? ${telemetryKey}
    ) as found
  `;
  return rows[0]?.found ?? false;
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
    const from = new Date(Date.now() - config.mqttMissingValueQueryLookbackMs);
    const deviceIds = await fetchDeviceIds(sql);
    for (const deviceId of deviceIds) {
      for (const target of QUERY_TARGETS) {
        const present = await hasRecentTelemetry(
          sql,
          deviceId,
          target.telemetryKey,
          from,
        );
        if (present) continue;
        const topic = `MC01/Server/${deviceId}`;
        const payload = JSON.stringify({
          method: "set",
          [target.commandKey]: target.commandValue,
        });
        try {
          await publishCommand(mqttClient, topic, payload);
          console.log(
            `[mqtt-query] sent device_id=${deviceId} command=${target.commandKey}`,
          );
        } catch (error) {
          console.error(
            `[mqtt-query] publish failed device_id=${deviceId} command=${target.commandKey}`,
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
    `[mqtt-query] enabled cron="${config.mqttMissingValueQueryCron}" lookback_ms=${config.mqttMissingValueQueryLookbackMs}`,
  );

  return {
    stop: () => {
      task.stop();
    },
  };
}
