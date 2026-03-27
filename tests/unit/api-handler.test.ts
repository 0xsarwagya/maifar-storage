import { describe, expect, test } from "bun:test";
import type { Sql } from "../../src/db";
import { createFetchHandler } from "../../src/api";

describe("createFetchHandler", () => {
  test("405 for non-GET", async () => {
    const handler = createFetchHandler(null as unknown as Sql, () => 0);
    const res = await handler(
      new Request("http://localhost/health", { method: "POST" }),
    );
    expect(res.status).toBe(405);
  });

  test("404 for unknown path", async () => {
    const handler = createFetchHandler(null as unknown as Sql, () => 0);
    const res = await handler(new Request("http://localhost/nope"));
    expect(res.status).toBe(404);
  });

  test("GET /health when DB ping fails", async () => {
    const brokenSql = ((strings: TemplateStringsArray) => {
      const q = strings.join("");
      if (q.includes("select 1")) {
        return Promise.reject(new Error("unreachable"));
      }
      return Promise.resolve([]);
    }) as Sql;

    const handler = createFetchHandler(brokenSql, () => 7);
    const res = await handler(new Request("http://localhost/health"));
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      database: string;
      queue_depth: number;
    };
    expect(body.database).toBe("down");
    expect(body.queue_depth).toBe(7);
  });

  test("GET /messages invalid cursor returns 400", async () => {
    const handler = createFetchHandler(null as unknown as Sql, () => 0);
    const res = await handler(
      new Request("http://localhost/messages?cursor=not-valid-base64"),
    );
    expect(res.status).toBe(400);
  });

  test("GET /messages invalid from date returns 400", async () => {
    const handler = createFetchHandler(null as unknown as Sql, () => 0);
    const res = await handler(
      new Request("http://localhost/messages?from=not-a-date"),
    );
    expect(res.status).toBe(400);
  });

  test("GET /openapi.json returns OpenAPI document", async () => {
    const handler = createFetchHandler(null as unknown as Sql, () => 0);
    const res = await handler(
      new Request("http://localhost/openapi.json"),
    );
    expect(res.status).toBe(200);
    expect(res.headers.get("Content-Type")?.includes("openapi")).toBe(true);
    const spec = (await res.json()) as { openapi: string; paths: object };
    expect(spec.openapi).toBe("3.0.3");
    expect(spec.paths).toHaveProperty("/health");
    expect(spec.paths).toHaveProperty("/messages");
  });

  test("GET /docs serves Swagger UI shell", async () => {
    const handler = createFetchHandler(null as unknown as Sql, () => 0);
    const res = await handler(new Request("http://localhost/docs"));
    expect(res.status).toBe(200);
    const html = await res.text();
    expect(html).toContain("swagger-ui");
    expect(html).toContain("/openapi.json");
  });

  test("GET /scalar serves Scalar shell", async () => {
    const handler = createFetchHandler(null as unknown as Sql, () => 0);
    const res = await handler(new Request("http://localhost/scalar"));
    expect(res.status).toBe(200);
    const html = await res.text();
    expect(html).toContain("api-reference");
    expect(html).toContain("/openapi.json");
  });

  test("GET /redoc serves ReDoc shell", async () => {
    const handler = createFetchHandler(null as unknown as Sql, () => 0);
    const res = await handler(new Request("http://localhost/redoc"));
    expect(res.status).toBe(200);
    const html = await res.text();
    expect(html).toContain("redoc");
    expect(html).toContain("/openapi.json");
  });

  test("GET /messages returns rows from mock sql", async () => {
    const mockSql = ((strings: TemplateStringsArray) => {
      const q = strings.join("");
      if (q.includes("device_messages")) {
        return Promise.resolve([
          {
            id: 1n,
            received_at: new Date("2024-01-01T00:00:00.000Z"),
            topic: "devices/1/t",
            device_id: "1",
            payload: { n: 2 },
          },
        ]);
      }
      return Promise.resolve([]);
    }) as Sql;

    const handler = createFetchHandler(mockSql, () => 0);
    const res = await handler(
      new Request("http://localhost/messages?limit=5"),
    );
    expect(res.status).toBe(200);
    const data = (await res.json()) as {
      items: { id: string; topic: string }[];
      next_cursor: string | null;
    };
    expect(data.items.length).toBe(1);
    expect(data.items[0]!.id).toBe("1");
    expect(data.next_cursor).toBeNull();
  });
});
