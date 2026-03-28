import { tryServeApiDocs } from "./api-docs";
import type { Sql } from "./db";
import {
  csvEscapeCell,
  decodeCursor,
  encodeCursor,
  escapeLikePrefix,
  formatInstantInTimeZone,
  parseIsoDate,
  parseLimit,
  resolveDisplayTimeZone,
  rowToApi,
  type CursorPayload,
} from "./format";

const EXPORT_PAGE = 500;

type ListFilters = {
  from?: Date;
  to?: Date;
  deviceId?: string;
  topicPrefix?: string;
};

function badRequest(message: string): Response {
  return Response.json({ error: message }, { status: 400 });
}

type DbRow = {
  id: bigint;
  received_at: Date;
  topic: string;
  device_id: string | null;
  payload: unknown;
};

type DeviceSummaryRow = {
  device_id: string;
  last_received_at: Date;
  first_received_at: Date;
  message_count: bigint;
};

async function selectPage(
  sql: Sql,
  f: ListFilters,
  cursor: CursorPayload | undefined,
  limit: number,
): Promise<DbRow[]> {
  const likePattern =
    f.topicPrefix !== undefined && f.topicPrefix !== ""
      ? escapeLikePrefix(f.topicPrefix) + "%"
      : null;

  const rows = await sql`
    select id, received_at, topic, device_id, payload
    from device_messages
    where true
    ${f.from ? sql`and received_at >= ${f.from}` : sql``}
    ${f.to ? sql`and received_at <= ${f.to}` : sql``}
    ${f.deviceId ? sql`and device_id = ${f.deviceId}` : sql``}
    ${
      likePattern
        ? sql`and topic like ${likePattern} escape '\\'`
        : sql``
    }
    ${
      cursor
        ? sql`and (received_at, id) > (${cursor.receivedAt}, ${cursor.id})`
        : sql``
    }
    order by received_at asc, id asc
    limit ${limit}
  `;
  return rows as unknown as DbRow[];
}

function parseListFilters(url: URL): ListFilters | Response {
  const from = parseIsoDate("from", url.searchParams.get("from"));
  if (!from.ok) return badRequest(from.message);
  const to = parseIsoDate("to", url.searchParams.get("to"));
  if (!to.ok) return badRequest(to.message);
  const deviceId = url.searchParams.get("device_id")?.trim();
  const topicPrefix = url.searchParams.get("topic_prefix")?.trim();
  return {
    from: from.value,
    to: to.value,
    deviceId: deviceId || undefined,
    topicPrefix: topicPrefix || undefined,
  };
}

export function createFetchHandler(sql: Sql, getQueueDepth: () => number) {
  return async function fetch(req: Request): Promise<Response> {
    if (req.method === "GET") {
      const docs = tryServeApiDocs(req);
      if (docs) return docs;
    }

    const url = new URL(req.url);
    const path = url.pathname;

    if (req.method !== "GET") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    if (path === "/health") {
      let database: "up" | "down" = "down";
      try {
        await sql`select 1`;
        database = "up";
      } catch {
        /* leave down */
      }
      return Response.json({
        ok: true,
        database,
        queue_depth: getQueueDepth(),
      });
    }

    if (path === "/devices") {
      const tz = resolveDisplayTimeZone(url.searchParams.get("timezone"));
      if (!tz.ok) return badRequest(tz.message);

      type NullCountRow = { n: bigint };
      const [rows, nullRows] = await Promise.all([
        sql`
          /* maifar:devices_summary */
          select
            device_id,
            max(received_at) as last_received_at,
            min(received_at) as first_received_at,
            count(*)::bigint as message_count
          from device_messages
          where device_id is not null
          group by device_id
          order by max(received_at) desc
        `,
        sql`
          /* maifar:devices_null_device_count */
          select count(*)::bigint as n
          from device_messages
          where device_id is null
        `,
      ]);

      const summary = rows as unknown as DeviceSummaryRow[];
      const withoutId = (nullRows as unknown as NullCountRow[])[0]?.n ?? 0n;

      return Response.json({
        timezone: tz.value,
        messages_without_device_id: withoutId.toString(),
        items: summary.map((r) => ({
          device_id: r.device_id,
          message_count: r.message_count.toString(),
          first_received_at: r.first_received_at.toISOString(),
          last_received_at: r.last_received_at.toISOString(),
          first_received_at_local: formatInstantInTimeZone(
            r.first_received_at,
            tz.value,
          ),
          last_received_at_local: formatInstantInTimeZone(
            r.last_received_at,
            tz.value,
          ),
        })),
      });
    }

    if (path === "/messages") {
      const filters = parseListFilters(url);
      if (filters instanceof Response) return filters;

      const limit = parseLimit(url.searchParams.get("limit"));
      if (!limit.ok) return badRequest(limit.message);

      const cursorRaw = url.searchParams.get("cursor");
      let cursor: CursorPayload | undefined;
      if (cursorRaw) {
        const c = decodeCursor(cursorRaw);
        if (!c.ok) return badRequest(c.message);
        cursor = c.value;
      }

      const rows = await selectPage(sql, filters, cursor, limit.value + 1);
      const hasMore = rows.length > limit.value;
      const items = hasMore ? rows.slice(0, limit.value) : rows;
      const last = items[items.length - 1];
      const next_cursor =
        hasMore && last
          ? encodeCursor({
              receivedAt: last.received_at,
              id: last.id,
            })
          : null;

      return Response.json({
        items: items.map(rowToApi),
        next_cursor,
      });
    }

    if (path === "/export.csv" || path === "/export.json") {
      const filters = parseListFilters(url);
      if (filters instanceof Response) return filters;

      const ndjson = path === "/export.json";
      const encoder = new TextEncoder();

      const stream = new ReadableStream<Uint8Array>({
        async start(controller) {
          try {
            if (!ndjson) {
              controller.enqueue(
                encoder.encode(
                  [
                    "id",
                    "received_at",
                    "topic",
                    "device_id",
                    "payload",
                  ].join(",") + "\n",
                ),
              );
            }

            let cursor: CursorPayload | undefined;
            while (true) {
              const rows = await selectPage(
                sql,
                filters,
                cursor,
                EXPORT_PAGE,
              );
              if (rows.length === 0) break;

              for (const r of rows) {
                if (ndjson) {
                  const line = JSON.stringify(rowToApi(r)) + "\n";
                  controller.enqueue(encoder.encode(line));
                } else {
                  const payloadStr = JSON.stringify(r.payload);
                  const line =
                    [
                      csvEscapeCell(r.id.toString()),
                      csvEscapeCell(r.received_at.toISOString()),
                      csvEscapeCell(r.topic),
                      csvEscapeCell(r.device_id ?? ""),
                      csvEscapeCell(payloadStr),
                    ].join(",") + "\n";
                  controller.enqueue(encoder.encode(line));
                }
              }

              const last = rows[rows.length - 1];
              cursor = { receivedAt: last.received_at, id: last.id };
              if (rows.length < EXPORT_PAGE) break;
            }
          } catch (e) {
            console.error("[export] stream error:", e);
            controller.error(e);
            return;
          }
          controller.close();
        },
      });

      const headers: Record<string, string> = {
        "Cache-Control": "no-store",
      };
      if (ndjson) {
        headers["Content-Type"] = "application/x-ndjson; charset=utf-8";
        headers["Content-Disposition"] =
          'attachment; filename="messages.ndjson"';
      } else {
        headers["Content-Type"] = "text/csv; charset=utf-8";
        headers["Content-Disposition"] =
          'attachment; filename="messages.csv"';
      }

      return new Response(stream, { headers });
    }

    return new Response("Not Found", { status: 404 });
  };
}
