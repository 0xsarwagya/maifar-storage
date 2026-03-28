import { describe, expect, test } from "bun:test";
import { createApp } from "../../src/app";
import { restoreEnv, snapshotEnv } from "../helpers/test-env";
import {
  applySchema,
  createTestSql,
  hasTestDatabase,
  truncateDeviceMessages,
} from "../helpers/test-db";

/**
 * Smoke: minimal real stack comes up and answers /health.
 */
describe.skipIf(!hasTestDatabase)("smoke: server + health", () => {
  test("createApp serves /health with database up", async () => {
    const prev = snapshotEnv();
    process.env.DATABASE_URL = process.env.TEST_DATABASE_URL!;
    process.env.MQTT_URL = "mqtt://127.0.0.1:1";
    delete process.env.MQTT_HOST;
    delete process.env.MQTT_PORT;
    delete process.env.MQTT_SERVERS;
    process.env.MQTT_TOPICS = "#";
    process.env.BATCH_MAX = "10";
    process.env.FLUSH_INTERVAL_MS = "60000";

    const setupSql = createTestSql();
    await applySchema(setupSql);
    await truncateDeviceMessages(setupSql);
    await setupSql.end({ timeout: 5 });

    const app = createApp({ httpPort: 0 });
    try {
      const res = await fetch(`${app.url}/health`);
      expect(res.ok).toBe(true);
      const j = (await res.json()) as { database: string; ok: boolean };
      expect(j.ok).toBe(true);
      expect(j.database).toBe("up");
    } finally {
      await app.stop();
      restoreEnv(prev);
    }
  }, 20_000);
});

describe("smoke: modules load", () => {
  test("core exports are functions", async () => {
    const { createFetchHandler } = await import("../../src/api");
    const { createFlushWorker } = await import("../../src/flush-worker");
    const { loadConfig } = await import("../../src/config");
    const { createDb } = await import("../../src/db");
    expect(typeof createFetchHandler).toBe("function");
    expect(typeof createFlushWorker).toBe("function");
    expect(typeof loadConfig).toBe("function");
    expect(typeof createDb).toBe("function");
  });
});
