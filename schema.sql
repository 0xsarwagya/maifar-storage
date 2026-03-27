CREATE TABLE IF NOT EXISTS device_messages (
  id BIGSERIAL PRIMARY KEY,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  topic TEXT NOT NULL,
  device_id TEXT,
  payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_device_messages_received_at ON device_messages (received_at);
CREATE INDEX IF NOT EXISTS idx_device_messages_device_received ON device_messages (device_id, received_at);
