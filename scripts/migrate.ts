import { loadDatabaseTlsFromEnv } from "../src/config";
import { logger } from "../src/logger";
import { migrateDatabase } from "../src/schema-migrate";

const log = logger.child({ module: "scripts/migrate" });

const url = process.env.DATABASE_URL;
if (!url) {
  log.error("DATABASE_URL is required");
  process.exit(1);
}

try {
  await migrateDatabase(url, loadDatabaseTlsFromEnv());
  log.info("Migration applied.");
} catch (e) {
  log.error({ err: e }, "Migration failed.");
  process.exit(1);
}
