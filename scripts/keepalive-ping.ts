/**
 * Hits an HTTP URL on a fixed interval (default every 20s). Useful to wake or
 * keep warm a Render web service.
 *
 *   bun run scripts/keepalive-ping.ts
 *   PING_URL=https://example.com/health PING_INTERVAL_MS=15000 bun run scripts/keepalive-ping.ts
 *
 * Classic crontab cannot run faster than once per minute; for sub-minute pings,
 * run this script under systemd, pm2, or another process supervisor.
 */

const url =
  process.env.PING_URL?.trim() ||
  "https://maifar-storage.onrender.com/health";
const intervalMs = Math.max(
  1000,
  Number(process.env.PING_INTERVAL_MS ?? 20_000) || 20_000,
);

async function ping(): Promise<void> {
  const t0 = Date.now();
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(25_000) });
    console.log(
      `[ping] ${new Date().toISOString()} ${res.status} ${Date.now() - t0}ms ${url}`,
    );
  } catch (e) {
    console.error(`[ping] ${new Date().toISOString()} FAIL ${url}`, e);
  }
}

console.log(`[ping] interval_ms=${intervalMs} → ${url}`);
await ping();
setInterval(() => {
  void ping();
}, intervalMs);
