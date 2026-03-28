import { migrateDatabase } from "../src/schema-migrate";

const url = process.env.DATABASE_URL;
if (!url) {
  console.error("DATABASE_URL is required");
  process.exit(1);
}

try {
  await migrateDatabase(url);
  console.log("Migration applied.");
} catch (e) {
  console.error(e);
  process.exit(1);
}
