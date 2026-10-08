CREATE TABLE IF NOT EXISTS dlq_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  transfer_id TEXT,
  topic TEXT NOT NULL,
  payload JSONB NOT NULL,
  failure_reason TEXT NOT NULL,
  attempts INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  reprocessed_at TIMESTAMPTZ
);
