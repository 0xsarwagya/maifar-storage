import type { DatabaseTlsSettings } from "./config";

/** When omitted, `postgres` uses URL / defaults only. */
export function postgresSslConnectOption(
  tls: DatabaseTlsSettings,
): Record<string, unknown> | undefined {
  if (!tls.databaseTlsCa && tls.databaseTlsRejectUnauthorized) {
    return undefined;
  }
  return {
    ...(tls.databaseTlsCa ? { ca: tls.databaseTlsCa } : {}),
    rejectUnauthorized: tls.databaseTlsRejectUnauthorized,
  };
}
