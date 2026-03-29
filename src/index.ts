import { createApp } from "./app";
import { startKeepalivePing } from "./keepalive";
import { loadConfig } from "./config";
import { migrateDatabase } from "./schema-migrate";

const config = loadConfig();
if (!config.databaseTlsRejectUnauthorized) {
  console.warn(
    "[db] PostgreSQL TLS certificate verification disabled (DATABASE_TLS_INSECURE or DATABASE_TLS_REJECT_UNAUTHORIZED=false)",
  );
}
if (config.autoMigrate) {
  await migrateDatabase(config.databaseUrl, config);
  console.log("[db] schema migration applied");
}

const app = createApp();
const keepalive = startKeepalivePing();

console.log(`[http] listening on ${app.url}`);
console.log(`[http] API docs: ${app.url}/docs (Swagger), ${app.url}/scalar, ${app.url}/redoc, ${app.url}/openapi.json`);
console.log(
  `[flush] batch_max=${app.config.batchMax} interval_ms=${app.config.flushIntervalMs}`,
);

async function shutdown(signal: string) {
  console.log(`[shutdown] ${signal}`);
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
