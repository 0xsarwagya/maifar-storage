export function extractDeviceId(
  topic: string,
  payload: unknown,
  regex: RegExp,
  jsonKey?: string,
): string | null {
  const m = topic.match(regex);
  if (m?.[1]) return m[1];
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
