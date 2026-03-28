export function extractDeviceId(
  topic: string,
  payload: unknown,
  regex: RegExp,
  jsonKey?: string,
): string | null {
  const m = topic.match(regex);
  if (m) {
    for (let i = 1; i < m.length; i++) {
      const g = m[i];
      if (g !== undefined && g !== "") return g;
    }
  }
  if (
    jsonKey &&
    payload &&
    typeof payload === "object" &&
    !Array.isArray(payload)
  ) {
    const v = (payload as Record<string, unknown>)[jsonKey];
    if (typeof v === "string" && v.length > 0) return v;
    if (typeof v === "number" && Number.isFinite(v)) return String(v);
  }
  return null;
}
