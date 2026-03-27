import { readFileSync } from "node:fs";
import { join } from "node:path";
import postgres from "postgres";

export const TEST_DATABASE_URL = process.env.TEST_DATABASE_URL;

export const hasTestDatabase = Boolean(TEST_DATABASE_URL);

const root = join(import.meta.dir, "../..");

export function createTestSql() {
  if (!TEST_DATABASE_URL) {
    throw new Error("TEST_DATABASE_URL is not set");
  }
  return postgres(TEST_DATABASE_URL, {
    max: 4,
    idle_timeout: 20,
    connect_timeout: 10,
    types: {
      bigint: postgres.BigInt,
    },
  });
}

export async function applySchema(sql: ReturnType<typeof createTestSql>) {
  const schema = readFileSync(join(root, "schema.sql"), "utf8");
  await sql.unsafe(schema);
}

export async function truncateDeviceMessages(
  sql: ReturnType<typeof createTestSql>,
) {
  await sql`truncate table device_messages restart identity`;
}
