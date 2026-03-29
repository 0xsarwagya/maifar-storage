import cron, { type ScheduledTask } from "node-cron";

const DEFAULT_URL = "https://maifar-storage.onrender.com/health";
const DEFAULT_CRON = "*/20 * * * * *";

function envFalse(raw: string | undefined): boolean {
  const v = raw?.trim().toLowerCase();
  return v === "false" || v === "0" || v === "no" || v === "off";
}

export type KeepaliveHandle = { stop: () => void };

/** Starts keepalive pings unless KEEPALIVE_ENABLED is explicitly false. */
export function startKeepalivePing(defaultUrl?: string): KeepaliveHandle {
  if (envFalse(process.env.KEEPALIVE_ENABLED)) {
    console.log("[ping] disabled via KEEPALIVE_ENABLED=false");
    return { stop: () => {} };
  }

  const url = process.env.PING_URL?.trim() || defaultUrl || DEFAULT_URL;
  const cronExpr = process.env.PING_CRON?.trim() || DEFAULT_CRON;
  if (!cron.validate(cronExpr)) {
    throw new Error(`Invalid PING_CRON expression: ${cronExpr}`);
  }

  let inFlight = false;
  async function ping(): Promise<void> {
    if (inFlight) return;
    inFlight = true;
    const t0 = Date.now();
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(25_000) });
      console.log(
        `[ping] ${new Date().toISOString()} ${res.status} ${Date.now() - t0}ms ${url}`,
      );
    } catch (e) {
      console.error(`[ping] ${new Date().toISOString()} FAIL ${url}`, e);
    } finally {
      inFlight = false;
    }
  }

  console.log(`[ping] cron="${cronExpr}" → ${url}`);
  void ping(); // run once immediately
  const task: ScheduledTask = cron.schedule(cronExpr, () => {
    void ping();
  });

  return {
    stop: () => {
      task.stop();
    },
  };
}

