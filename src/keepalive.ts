import cron, { type ScheduledTask } from "node-cron";
import { logger } from "./logger";

const DEFAULT_URL = "https://maifar-storage.onrender.com/health";
const DEFAULT_CRON = "*/20 * * * * *";
const log = logger.child({ module: "keepalive" });

function envFalse(raw: string | undefined): boolean {
  const v = raw?.trim().toLowerCase();
  return v === "false" || v === "0" || v === "no" || v === "off";
}

export type KeepaliveHandle = { stop: () => void };

/** Starts keepalive pings unless KEEPALIVE_ENABLED is explicitly false. */
export function startKeepalivePing(defaultUrl?: string): KeepaliveHandle {
  if (envFalse(process.env.KEEPALIVE_ENABLED)) {
    log.info("[ping] disabled via KEEPALIVE_ENABLED=false");
    return { stop: () => {} };
  }

  const url = process.env.PING_URL?.trim() || defaultUrl || DEFAULT_URL;
  const cronExpr = process.env.PING_CRON?.trim() || DEFAULT_CRON;
  if (!cron.validate(cronExpr)) {
    throw new Error(`Invalid PING_CRON expression: ${cronExpr}`);
  }

  let inFlight = false;
  let attempts = 0;
  async function ping(): Promise<void> {
    if (inFlight) {
      log.info({ url }, "[ping] skip=in_flight");
      return;
    }
    inFlight = true;
    attempts++;
    const attemptNo = attempts;
    const t0 = Date.now();
    log.info({ attempt: attemptNo, url }, "[ping] sent");
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(25_000) });
      log.info(
        {
          attempt: attemptNo,
          status: res.status,
          duration_ms: Date.now() - t0,
          url,
        },
        "[ping] ok",
      );
    } catch (e) {
      log.error({ err: e, attempt: attemptNo, url }, "[ping] fail");
    } finally {
      inFlight = false;
    }
  }

  log.info({ cron: cronExpr, url }, "[ping] enabled");
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

