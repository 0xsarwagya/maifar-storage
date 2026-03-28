import { describe, expect, test } from "bun:test";
import { sanitizePostgresConnectionUrl } from "../../src/pg-tls";

describe("sanitizePostgresConnectionUrl", () => {
  test("removes sslrootcert and related libpq params", () => {
    const u =
      "postgres://u:p@h.example/db?sslmode=require&sslrootcert=/tmp/ca.pem&foo=bar";
    const out = sanitizePostgresConnectionUrl(u);
    const parsed = new URL(out);
    expect(parsed.searchParams.has("sslrootcert")).toBe(false);
    expect(parsed.searchParams.get("sslmode")).toBe("require");
    expect(parsed.searchParams.get("foo")).toBe("bar");
  });

  test("preserves sslmode only", () => {
    const u = "postgres://localhost/db?sslmode=verify-full";
    expect(sanitizePostgresConnectionUrl(u)).toBe(u);
  });

  test("no-op on invalid URL", () => {
    const bad = "not-a-url";
    expect(sanitizePostgresConnectionUrl(bad)).toBe(bad);
  });
});
