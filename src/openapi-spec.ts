/** OpenAPI 3.0 document; `servers` filled per-request with the caller origin. */
export function buildOpenApiDocument(origin: string): Record<string, unknown> {
  return {
    openapi: "3.0.3",
    info: {
      title: "maifar-storage API",
      description:
        "Read stored MQTT device messages (JSON payloads) from PostgreSQL. " +
        "Ingest is MQTT-only; these routes are for querying and export.",
      version: "1.0.0",
    },
    servers: [{ url: origin }],
    tags: [
      { name: "Health", description: "Service status" },
      { name: "Messages", description: "Query and export ingested messages" },
    ],
    paths: {
      "/health": {
        get: {
          tags: ["Health"],
          summary: "Health check",
          description:
            "Returns process liveness, database connectivity, and current in-memory queue depth.",
          responses: {
            "200": {
              description: "OK",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/HealthResponse" },
                },
              },
            },
          },
        },
      },
      "/messages": {
        get: {
          tags: ["Messages"],
          summary: "List messages (paginated)",
          description:
            "Keyset pagination on `(received_at, id)` ascending. Pass `next_cursor` from the previous response to continue.",
          parameters: [
            { $ref: "#/components/parameters/From" },
            { $ref: "#/components/parameters/To" },
            { $ref: "#/components/parameters/DeviceId" },
            { $ref: "#/components/parameters/TopicPrefix" },
            { $ref: "#/components/parameters/Limit" },
            { $ref: "#/components/parameters/Cursor" },
          ],
          responses: {
            "200": {
              description: "Page of messages",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/MessagesPage" },
                },
              },
            },
            "400": {
              description: "Invalid query parameters",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ErrorBody" },
                },
              },
            },
          },
        },
      },
      "/export.csv": {
        get: {
          tags: ["Messages"],
          summary: "Export as CSV (streamed)",
          description:
            "Same filters as `/messages`. Response is streamed; `payload` is a JSON string in the CSV cell.",
          parameters: [
            { $ref: "#/components/parameters/From" },
            { $ref: "#/components/parameters/To" },
            { $ref: "#/components/parameters/DeviceId" },
            { $ref: "#/components/parameters/TopicPrefix" },
          ],
          responses: {
            "200": {
              description: "CSV stream",
              headers: {
                "Content-Disposition": {
                  schema: { type: "string" },
                  example: 'attachment; filename="messages.csv"',
                },
              },
              content: {
                "text/csv": {
                  schema: { type: "string", format: "binary" },
                },
              },
            },
            "400": {
              description: "Invalid query parameters",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ErrorBody" },
                },
              },
            },
          },
        },
      },
      "/export.json": {
        get: {
          tags: ["Messages"],
          summary: "Export as NDJSON (streamed)",
          description:
            "Same filters as `/messages`. Each line is one JSON object (newline-delimited JSON).",
          parameters: [
            { $ref: "#/components/parameters/From" },
            { $ref: "#/components/parameters/To" },
            { $ref: "#/components/parameters/DeviceId" },
            { $ref: "#/components/parameters/TopicPrefix" },
          ],
          responses: {
            "200": {
              description: "NDJSON stream",
              headers: {
                "Content-Disposition": {
                  schema: { type: "string" },
                  example: 'attachment; filename="messages.ndjson"',
                },
              },
              content: {
                "application/x-ndjson": {
                  schema: { type: "string", format: "binary" },
                },
              },
            },
            "400": {
              description: "Invalid query parameters",
              content: {
                "application/json": {
                  schema: { $ref: "#/components/schemas/ErrorBody" },
                },
              },
            },
          },
        },
      },
    },
    components: {
      parameters: {
        From: {
          name: "from",
          in: "query",
          description: "Inclusive lower bound on `received_at` (ISO 8601)",
          schema: { type: "string", format: "date-time" },
        },
        To: {
          name: "to",
          in: "query",
          description: "Inclusive upper bound on `received_at` (ISO 8601)",
          schema: { type: "string", format: "date-time" },
        },
        DeviceId: {
          name: "device_id",
          in: "query",
          description: "Exact match on parsed device id",
          schema: { type: "string" },
        },
        TopicPrefix: {
          name: "topic_prefix",
          in: "query",
          description: "Topic prefix filter (SQL LIKE with escape)",
          schema: { type: "string" },
        },
        Limit: {
          name: "limit",
          in: "query",
          description: "Page size (1–1000, default 100)",
          schema: { type: "integer", minimum: 1, maximum: 1000, default: 100 },
        },
        Cursor: {
          name: "cursor",
          in: "query",
          description: "Opaque keyset cursor from `next_cursor`",
          schema: { type: "string" },
        },
      },
      schemas: {
        HealthResponse: {
          type: "object",
          required: ["ok", "database", "queue_depth"],
          properties: {
            ok: { type: "boolean", example: true },
            database: {
              type: "string",
              enum: ["up", "down"],
              description: "Result of a simple DB ping",
            },
            queue_depth: {
              type: "integer",
              minimum: 0,
              description: "Rows waiting in the in-memory flush queue",
            },
          },
        },
        MessageItem: {
          type: "object",
          required: [
            "id",
            "received_at",
            "topic",
            "device_id",
            "payload",
          ],
          properties: {
            id: { type: "string", description: "Bigserial as string" },
            received_at: { type: "string", format: "date-time" },
            topic: { type: "string" },
            device_id: { type: "string", nullable: true },
            payload: {
              description:
                "Original MQTT JSON value (object, array, string, number, boolean, or null)",
              nullable: true,
            },
          },
        },
        MessagesPage: {
          type: "object",
          required: ["items", "next_cursor"],
          properties: {
            items: {
              type: "array",
              items: { $ref: "#/components/schemas/MessageItem" },
            },
            next_cursor: { type: "string", nullable: true },
          },
        },
        ErrorBody: {
          type: "object",
          required: ["error"],
          properties: {
            error: { type: "string" },
          },
        },
      },
    },
  };
}
