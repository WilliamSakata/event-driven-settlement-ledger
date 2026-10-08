CREATE TABLE IF NOT EXISTS balance_projection (
  account_id TEXT PRIMARY KEY,
  confirmed_balance NUMERIC NOT NULL
);
