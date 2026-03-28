import type { DatabaseTlsSettings } from "./config";

/**
 * `postgres.js` forwards unknown URL query keys into the startup `connection`
 * payload; libpq-only params like `sslrootcert` then hit the server as bogus
 * GUCs (`unrecognized configuration parameter "sslrootcert"` on many PG versions).
 */
/** Do not strip `sslmode` — `postgres.js` maps it to client `ssl` correctly. */
const LIBPQ_URL_PARAMS_TO_STRIP = [
  "sslrootcert",
  "sslcert",
  "sslkey",
  "sslcrl",
  "requiressl",
];

export function sanitizePostgresConnectionUrl(urlString: string): string {
  try {
    const u = new URL(urlString);
    let changed = false;
    for (const p of LIBPQ_URL_PARAMS_TO_STRIP) {
      if (u.searchParams.has(p)) {
        u.searchParams.delete(p);
        changed = true;
      }
    }
    return changed ? u.toString() : urlString;
  } catch {
    return urlString;
  }
}

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
