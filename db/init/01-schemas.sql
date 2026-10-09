-- Named authz, not authorization: AUTHORIZATION is a reserved SQL keyword
-- (used in CREATE SCHEMA ... AUTHORIZATION owner), so the bare identifier
-- "authorization" is a syntax error here.
CREATE SCHEMA IF NOT EXISTS authz;
CREATE SCHEMA IF NOT EXISTS ledger;
CREATE SCHEMA IF NOT EXISTS payout;

-- Created once, here, single-threaded, before any app/migrate container starts.
-- pgcrypto is database-scoped (not per-schema), and each service's own migration
-- also declares CREATE EXTENSION IF NOT EXISTS pgcrypto for bare-Postgres
-- compatibility -- but when docker-compose starts all three services' migrate
-- jobs concurrently, three sessions racing on the same IF NOT EXISTS check is a
-- real Postgres race (duplicate key on pg_extension_name_index). Creating it here
-- first means the per-service migrations find it already present and skip it.
CREATE EXTENSION IF NOT EXISTS pgcrypto;
