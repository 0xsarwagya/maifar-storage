import { createFetchHandler } from "./api";
import { loadConfig, type AppConfig } from "./config";
import { createDb, type Sql } from "./db";
import { createFlushWorker } from "./flush-worker";
import { startMqttIngest } from "./mqtt-ingest";
import * as queue from "./queue";
import type { FlushWorkerDeps } from "./flush-worker";
import type { MqttClient } from "mqtt";

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

export function createApp(options: CreateAppOptions = {}): AppInstance {
  const config = loadConfig();
  const sql = createDb(config.databaseUrl);
  const { flush } = createFlushWorker(sql, config.batchMax, options.flushDeps ?? {});

  const mqttClient = startMqttIngest(config, () => {
    void flush();
  });

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
