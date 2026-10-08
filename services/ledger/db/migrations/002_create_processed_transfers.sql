CREATE TABLE IF NOT EXISTS processed_transfers (
  transfer_id TEXT PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
