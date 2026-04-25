import { createFetchHandler } from "./api";
import { loadConfig, type AppConfig } from "./config";
import { createDb, type Sql } from "./db";
import { createFlushWorker } from "./flush-worker";
import {
  extractDeviceOnlineState,
  forwardNormalizedPayloadToOvok,
  startMqttIngest,
} from "./mqtt-ingest";
import { startOvokScheduledForwarding } from "./ovok-scheduler";
import * as queue from "./queue";
import type { FlushWorkerDeps } from "./flush-worker";
import type { MqttClient } from "mqtt";
import type { QueuedRow } from "./types";

export type AppInstance = {
  config: AppConfig;
  server: ReturnType<typeof Bun.serve>;
  sql: Sql;
  flush: () => Promise<void>;
  mqttClient: MqttClient;
  flushTimer: ReturnType<typeof setInterval>;
  url: string;
  stop: () => Promise<void>;
};

export type CreateAppOptions = {
  /** Overrides `HTTP_PORT` from env when set. */
  httpPort?: number;
  flushDeps?: FlushWorkerDeps;
};

export function filterRowsForOvokForwarding(
  rows: readonly QueuedRow[],
  onlineStateByDevice: Map<string, boolean>,
): QueuedRow[] {
  const allowedRows: QueuedRow[] = [];
  const orderedRows = [...rows].sort(
    (left, right) => left.receivedAt.getTime() - right.receivedAt.getTime(),
  );

  for (const row of orderedRows) {
    const onlineState = extractDeviceOnlineState(row.payload);
    if (row.deviceId && onlineState !== null) {
      onlineStateByDevice.set(row.deviceId, onlineState);
      continue;
    }
    if (row.deviceId && onlineStateByDevice.get(row.deviceId) === false) {
      continue;
    }
    allowedRows.push(row);
  }

  return allowedRows;
}

export function createApp(options: CreateAppOptions = {}): AppInstance {
  const config = loadConfig();
  const sql = createDb(config.databaseUrl, config);
  const ovokOnlineStateByDevice = new Map<string, boolean>();
  const { flush } = createFlushWorker(sql, config.batchMax, {
    ...options.flushDeps,
    afterBatchInserted: async (rows) => {
      if (options.flushDeps?.afterBatchInserted) {
        await options.flushDeps.afterBatchInserted(rows);
      }
      if (config.ovokScheduledEnabled) return;
      const rowsToForward = filterRowsForOvokForwarding(
        rows,
        ovokOnlineStateByDevice,
      );
      for (const row of rowsToForward) {
        await forwardNormalizedPayloadToOvok(row.topic, row.payload, config);
      }
    },
  });

  const mqttClient = startMqttIngest(config, () => {
    void flush();
  });
  const ovokScheduler = startOvokScheduledForwarding(sql, config);

  const flushTimer = setInterval(() => {
    void flush();
  }, config.flushIntervalMs);

  const fetch = createFetchHandler(sql, () => queue.depth());

  const port = options.httpPort ?? config.httpPort;
  const server = Bun.serve({
    port,
    fetch,
  });

  const url = `http://127.0.0.1:${server.port}`;

  async function stop(): Promise<void> {
    clearInterval(flushTimer);
    ovokScheduler.stop();
    mqttClient.end(true);
    await flush();
    await sql.end({ timeout: 5 });
    server.stop();
  }

  return {
    config,
    server,
    sql,
    flush,
    mqttClient,
    flushTimer,
    url,
    stop,
  };
}
