import postgres from "postgres";

export function createDb(databaseUrl: string) {
  return postgres(databaseUrl, {
    max: 10,
    idle_timeout: 30,
    connect_timeout: 10,
    types: {
      bigint: postgres.BigInt,
    },
  });
}

export type Sql = ReturnType<typeof createDb>;
