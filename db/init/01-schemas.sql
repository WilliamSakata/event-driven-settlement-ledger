-- Named authz, not authorization: AUTHORIZATION is a reserved SQL keyword
-- (used in CREATE SCHEMA ... AUTHORIZATION owner), so the bare identifier
-- "authorization" is a syntax error here.
CREATE SCHEMA IF NOT EXISTS authz;
CREATE SCHEMA IF NOT EXISTS ledger;
CREATE SCHEMA IF NOT EXISTS payout;
