import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import postgres from "postgres";

const __dirname = dirname(fileURLToPath(import.meta.url));

export function readSchemaSql(): string {
  return readFileSync(join(__dirname, "..", "schema.sql"), "utf8");
}

/** Apply `schema.sql` (idempotent). Uses a short-lived single connection. */
export async function migrateDatabase(databaseUrl: string): Promise<void> {
  const sql = postgres(databaseUrl, { max: 1 });
  try {
    await sql.unsafe(readSchemaSql());
  } finally {
    await sql.end();
  }
}
