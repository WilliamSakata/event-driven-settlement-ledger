CREATE TABLE IF NOT EXISTS payout_attempts (
  transfer_id TEXT PRIMARY KEY,
  to_account TEXT NOT NULL,
  amount NUMERIC NOT NULL,
  status TEXT NOT NULL,
  psp_reference TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
