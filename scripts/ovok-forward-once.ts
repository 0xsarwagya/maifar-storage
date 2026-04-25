import { loadConfig } from "../src/config";
import { createDb } from "../src/db";
import {
  DEFAULT_OVOK_DAILY_BATCH_JOBS,
  OVOK_FORWARD_JOB_NAMES,
  runOvokForwardJobs,
  type OvokForwardJobName,
} from "../src/ovok-scheduler";

const SUPPORTED_JOBS = new Set<string>(OVOK_FORWARD_JOB_NAMES);

function printUsage(): void {
  console.log(`Usage:
  bun run scripts/ovok-forward-once.ts [--jobs=<job1,job2,...>] [--devices=<device1,device2,...>] [--at=<iso8601>]

Jobs:
  ${OVOK_FORWARD_JOB_NAMES.join(", ")}

Defaults:
  --jobs=${DEFAULT_OVOK_DAILY_BATCH_JOBS.join(",")}
`);
}

function parseCsvArg(raw: string | undefined): string[] {
  if (!raw) return [];
  return raw
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
}

function parseJobs(raw: string | undefined): OvokForwardJobName[] {
  const requested = parseCsvArg(raw);
  if (requested.length === 0) {
    return [...DEFAULT_OVOK_DAILY_BATCH_JOBS];
  }

  const invalid = requested.filter((job) => !SUPPORTED_JOBS.has(job));
  if (invalid.length > 0) {
    throw new Error(
      `Unsupported job name(s): ${invalid.join(", ")}. Supported jobs: ${OVOK_FORWARD_JOB_NAMES.join(", ")}`,
    );
  }

  return requested as OvokForwardJobName[];
}

function parseAt(raw: string | undefined): Date | undefined {
  if (!raw) return undefined;
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid --at value: ${raw}`);
  }
  return parsed;
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  if (args.includes("--help") || args.includes("-h")) {
    printUsage();
    return;
  }

  const jobs = parseJobs(args.find((arg) => arg.startsWith("--jobs="))?.slice("--jobs=".length));
  const devices = parseCsvArg(
    args.find((arg) => arg.startsWith("--devices="))?.slice("--devices=".length),
  );
  const at = parseAt(args.find((arg) => arg.startsWith("--at="))?.slice("--at=".length));

  const config = loadConfig();
  const sql = createDb(config.databaseUrl, config);

  try {
    await runOvokForwardJobs({
      sql,
      config,
      jobs,
      deviceIds: devices.length > 0 ? devices : undefined,
      now: at,
    });

    console.log(
      JSON.stringify({
        ok: true,
        jobs,
        devices: devices.length > 0 ? devices : "all",
        at: (at ?? new Date()).toISOString(),
      }),
    );
  } finally {
    await sql.end({ timeout: 5 });
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
