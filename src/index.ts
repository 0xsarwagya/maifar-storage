import { createApp } from "./app";
import { startKeepalivePing } from "./keepalive";
import { loadConfig } from "./config";
import { migrateDatabase } from "./schema-migrate";
import { logger } from "./logger";

const log = logger.child({ module: "index" });

const config = loadConfig();
if (!config.databaseTlsRejectUnauthorized) {
  log.warn(
    "[db] PostgreSQL TLS certificate verification disabled (DATABASE_TLS_INSECURE or DATABASE_TLS_REJECT_UNAUTHORIZED=false)",
  );
}
if (config.autoMigrate) {
  await migrateDatabase(config.databaseUrl, config);
  log.info("[db] schema migration applied");
}

const app = createApp();
const keepalive = startKeepalivePing();

log.info(`[http] listening on ${app.url}`);
log.info(
  `[http] API docs: ${app.url}/docs (Swagger), ${app.url}/scalar, ${app.url}/redoc, ${app.url}/openapi.json`,
);
log.info(
  `[flush] batch_max=${app.config.batchMax} interval_ms=${app.config.flushIntervalMs}`,
);

async function shutdown(signal: string) {
  log.info({ signal }, "[shutdown] received");
  keepalive.stop();
  await app.stop();
  process.exit(0);
}

process.on("SIGINT", () => {
  void shutdown("SIGINT");
});
process.on("SIGTERM", () => {
  void shutdown("SIGTERM");
});
