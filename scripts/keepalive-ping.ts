import { startKeepalivePing } from "../src/keepalive";

const keepalive = startKeepalivePing();

process.on("SIGTERM", () => {
  keepalive.stop();
  process.exit(0);
});

process.on("SIGINT", () => {
  keepalive.stop();
  process.exit(0);
});
