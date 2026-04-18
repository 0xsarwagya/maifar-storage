export type OvokForwardMetrics = {
  requests_sent_total: number;
  requests_failed_total: number;
  bytes_sent_total: number;
  bytes_failed_total: number;
  last_sent_at: string | null;
  last_failed_at: string | null;
};

const metrics: OvokForwardMetrics = {
  requests_sent_total: 0,
  requests_failed_total: 0,
  bytes_sent_total: 0,
  bytes_failed_total: 0,
  last_sent_at: null,
  last_failed_at: null,
};

export function recordOvokForwardSuccess(bytes: number): void {
  metrics.requests_sent_total += 1;
  metrics.bytes_sent_total += Math.max(0, bytes);
  metrics.last_sent_at = new Date().toISOString();
}

export function recordOvokForwardFailure(bytes: number): void {
  metrics.requests_failed_total += 1;
  metrics.bytes_failed_total += Math.max(0, bytes);
  metrics.last_failed_at = new Date().toISOString();
}

export function getOvokForwardMetricsSnapshot(): OvokForwardMetrics {
  return { ...metrics };
}

export function resetOvokForwardMetricsForTests(): void {
  metrics.requests_sent_total = 0;
  metrics.requests_failed_total = 0;
  metrics.bytes_sent_total = 0;
  metrics.bytes_failed_total = 0;
  metrics.last_sent_at = null;
  metrics.last_failed_at = null;
}
