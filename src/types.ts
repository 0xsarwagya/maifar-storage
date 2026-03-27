export type QueuedRow = {
  receivedAt: Date;
  topic: string;
  deviceId: string | null;
  payload: unknown;
};
