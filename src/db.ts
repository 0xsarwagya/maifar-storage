import postgres from "postgres";
import type { DatabaseTlsSettings } from "./config";
import { postgresSslConnectOption } from "./pg-tls";

export function createDb(databaseUrl: string, tls: DatabaseTlsSettings) {
  const ssl = postgresSslConnectOption(tls);
  return postgres(databaseUrl, {
    max: 10,
    idle_timeout: 30,
    connect_timeout: 10,
    types: {
      bigint: postgres.BigInt,
    },
    ...(ssl !== undefined ? { ssl } : {}),
  });
}

export type Sql = ReturnType<typeof createDb>;
