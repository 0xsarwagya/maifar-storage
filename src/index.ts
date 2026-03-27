import { createApp } from "./app";

const app = createApp();

console.log(`[http] listening on ${app.url}`);
console.log(`[http] API docs: ${app.url}/docs (Swagger), ${app.url}/scalar, ${app.url}/redoc, ${app.url}/openapi.json`);
console.log(
  `[flush] batch_max=${app.config.batchMax} interval_ms=${app.config.flushIntervalMs}`,
);

async function shutdown(signal: string) {
  console.log(`[shutdown] ${signal}`);
  await app.stop();
  process.exit(0);
}

process.on("SIGINT", () => {
  void shutdown("SIGINT");
});
process.on("SIGTERM", () => {
  void shutdown("SIGTERM");
});
