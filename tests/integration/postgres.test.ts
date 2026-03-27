import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { createFetchHandler } from "../../src/api";
import { createApp } from "../../src/app";
import { createFlushWorker } from "../../src/flush-worker";
import * as queue from "../../src/queue";
import type { QueuedRow } from "../../src/types";
import { startEmbeddedMqttBroker } from "../helpers/mqtt-broker";
import { restoreEnv, snapshotEnv } from "../helpers/test-env";
import {
  applySchema,
  createTestSql,
  hasTestDatabase,
  truncateDeviceMessages,
} from "../helpers/test-db";
import mqtt from "mqtt";

describe.skipIf(!hasTestDatabase)("integration: Postgres + HTTP + MQTT", () => {
  let sql: ReturnType<typeof createTestSql>;

  beforeAll(async () => {
    sql = createTestSql();
    await applySchema(sql);
    await truncateDeviceMessages(sql);
  });

  afterAll(async () => {
    await sql.end({ timeout: 5 });
  });

  test("GET /health reports database up", async () => {
    const fetch = createFetchHandler(sql, () => queue.depth());
    const res = await fetch(new Request("http://localhost/health"));
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      ok: boolean;
      database: string;
    };
    expect(body.ok).toBe(true);
    expect(body.database).toBe("up");
  });

  test("flush persists queued rows as jsonb", async () => {
    await truncateDeviceMessages(sql);
    const { flush } = createFlushWorker(sql, 100);
    queue.enqueue({
      receivedAt: new Date("2024-07-01T12:00:00.000Z"),
      topic: "devices/p1/telemetry",
      deviceId: "p1",
      payload: { v: 3 },
    } satisfies QueuedRow);
    await flush();

    const rows = await sql<{ topic: string; payload: { v: number } }[]>`
      select topic, payload from device_messages
    `;
    expect(rows.length).toBe(1);
    expect(rows[0]!.payload.v).toBe(3);
  });

  test("GET /messages returns stored rows", async () => {
    const fetch = createFetchHandler(sql, () => queue.depth());
    await truncateDeviceMessages(sql);
    await sql`
      insert into device_messages (received_at, topic, device_id, payload)
      values (
        ${new Date("2024-05-01T10:00:00.000Z")},
        ${"devices/x/telemetry"},
        ${"x"},
        ${sql.json({ temp: 22 })}
      )
    `;

    const res = await fetch(new Request("http://localhost/messages?limit=10"));
    expect(res.status).toBe(200);
    const data = (await res.json()) as {
      items: { topic: string }[];
      next_cursor: string | null;
    };
    expect(data.items.length).toBe(1);
    expect(data.items[0]!.topic).toBe("devices/x/telemetry");
  });

  test("GET /export.csv streams rows", async () => {
    const fetch = createFetchHandler(sql, () => queue.depth());
    await truncateDeviceMessages(sql);
    await sql`
      insert into device_messages (received_at, topic, device_id, payload)
      values (now(), 't/csv', 'd', ${sql.json({ a: 1 })})
    `;

    const res = await fetch(new Request("http://localhost/export.csv"));
    expect(res.status).toBe(200);
    const text = await res.text();
    expect(text.includes("t/csv")).toBe(true);
  });

  test("GET /export.json NDJSON", async () => {
    const fetch = createFetchHandler(sql, () => queue.depth());
    await truncateDeviceMessages(sql);
    await sql`
      insert into device_messages (received_at, topic, device_id, payload)
      values (now(), 't/nd', null, ${sql.json([1, 2])})
    `;

    const res = await fetch(new Request("http://localhost/export.json"));
    const line = (await res.text()).trim().split("\n")[0]!;
    const obj = JSON.parse(line) as { topic: string };
    expect(obj.topic).toBe("t/nd");
  });

  test("invalid limit returns 400", async () => {
    const fetch = createFetchHandler(sql, () => queue.depth());
    const res = await fetch(new Request("http://localhost/messages?limit=0"));
    expect(res.status).toBe(400);
  });

  test("MQTT publish → flush → row in database", async () => {
    const broker = await startEmbeddedMqttBroker();
    const prev = snapshotEnv();
    process.env.DATABASE_URL = process.env.TEST_DATABASE_URL!;
    process.env.MQTT_URL = broker.mqttUrl;
    process.env.MQTT_TOPICS = "devices/+/telemetry";
    process.env.BATCH_MAX = "100";
    process.env.FLUSH_INTERVAL_MS = "60000";
    process.env.HTTP_PORT = "0";

    await truncateDeviceMessages(sql);

    const app = createApp({ httpPort: 0 });
    try {
      await new Promise((r) => setTimeout(r, 400));

      const pub = mqtt.connect(broker.mqttUrl, { connectTimeout: 5000 });
      await new Promise<void>((resolve, reject) => {
        pub.on("connect", () => resolve());
        pub.on("error", reject);
      });

      pub.publish(
        "devices/z9/telemetry",
        JSON.stringify({ reading: 99 }),
        {},
      );

      await new Promise((r) => setTimeout(r, 250));
      await app.flush();

      const rows = await sql<{ device_id: string; payload: { reading: number } }[]>`
        select device_id, payload from device_messages
        where topic = ${"devices/z9/telemetry"}
      `;
      expect(rows.length).toBe(1);
      expect(rows[0]!.device_id).toBe("z9");
      expect(rows[0]!.payload.reading).toBe(99);

      pub.end(true);
      await app.stop();
    } finally {
      await broker.close();
      restoreEnv(prev);
    }
  }, 25_000);
});
