# Event-Driven Settlement Ledger — Design

## Goal

Prove the design of a financial settlement system with strong consistency guarantees: no duplicate transactions, no lost events, full traceability of every movement. Three services split by bounded context (authorization, ledger, payout), communicating only through Kafka, each backed by its own Postgres schema, using the outbox pattern for transactional publish and idempotency keys end to end.

## Non-Goals

- No automatic saga/compensation when the external payout leg fails (see "Payout failure handling"). A failed payout is a traceable terminal state requiring manual retry, not an automatically reversed ledger entry.
- No multi-currency, multi-ledger, or interest/fee calculation — a transfer moves a fixed amount between two accounts, nothing else.
- No authentication/authorization of the HTTP caller (no user accounts, JWTs, etc.) — the project's "authorization" is business-rule authorization (balance/limits), not access control.
- No Kubernetes cluster is actually stood up or tested against; the Helm chart documents deployment intent only (same posture as the other portfolio projects' optional Kubernetes section).

## 1. Architecture Overview

Three independent services, each with its own HTTP app, Kafka consumer(s), and outbox poller, communicating **only via Kafka** — never direct HTTP calls between them:

```
Client → POST /transfers → [authorization]
                                │ approves against locally projected balance
                                │ writes decision + outbox row (same Postgres tx)
                                ▼
                        outbox poller → Kafka topic "transfer-authorized"
                                ▼
                            [ledger] (consumer)
                                │ writes 2+ double-entry lines + outbox row (same tx)
                                ▼
                        outbox poller → Kafka topic "settlement-posted"
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
            [authorization] (consumer)   [payout] (consumer)
            updates confirmed balance,   calls mock external PSP,
            releases the reservation     records outcome + outbox row
                                                ▼
                                        Kafka topic "payout-result"
                                        (terminal — no consumer in this project)
```

Each service owns its own Postgres schema (`authz`, `ledger`, `payout`, one shared Postgres instance — see "Project Structure" for why one instance is acceptable here) and never queries another service's schema directly. Cross-service communication is exclusively asynchronous, through Kafka topics. The `authorization` service's Postgres schema is named `authz`, not `authorization` — `AUTHORIZATION` is a reserved SQL keyword (used in `CREATE SCHEMA ... AUTHORIZATION owner`), making the bare identifier `authorization` a syntax error as a schema name. Only the Postgres schema is renamed; the service itself, its code, and its ports keep the name `authorization` throughout.

## 2. Data Model

**`authz` schema** (the `authorization` service's schema):
- `transfers` — `id` (the client-supplied idempotency key), `from_account`, `to_account`, `amount`, `status` (`approved` | `rejected` | `confirmed` | `released`), `created_at`. `id` is the primary key, which is what makes `POST /transfers` idempotent: a retried request with the same key returns the already-decided result instead of re-evaluating.
- `reservations` — `transfer_id`, `account_id`, `amount`, `status` (`pending` | `released`). Exists to close the staleness window between "approved" and "the ledger confirmed": `available_balance = confirmed_balance − sum(reservations where status = pending for that account)`. Without this, two concurrent transfers could both be approved against the same not-yet-confirmed balance (double-spend).
- `balance_projection` — `account_id`, `confirmed_balance`. Updated **only** when consuming `settlement-posted` — never optimistically updated at approval time.
- `outbox` — `id`, `topic`, `payload` (jsonb), `created_at`, `published_at` (nullable). Standard transactional-outbox table, same shape as the webhook-ingestion-replay-dlq project.

**`ledger` schema:**
- `ledger_entries` — append-only, `id`, `transfer_id`, `account_id`, `direction` (`debit` | `credit`), `amount`, `created_at`. Every transfer writes exactly 2 rows (debit on `from_account`, credit on `to_account`); rows are never updated or deleted. Invariant: for any `transfer_id`, `sum(amount where direction = debit) = sum(amount where direction = credit)`.
- `processed_transfers` — `transfer_id` (primary key). Dedupe guard: if `transfer-authorized` is redelivered for a `transfer_id` already in this table, the consumer acks without writing new entries.
- `outbox` — same shape as above.

**`payout` schema:**
- `payout_attempts` — `transfer_id` (primary key, dedupe), `status` (`sent` | `failed`), `psp_reference` (nullable), `created_at`.
- `outbox` — same shape as above.

**Every service's own `dlq_events` table** (same shape in all three schemas): `id` (uuid), `transfer_id`, `topic`, `payload` (jsonb), `failure_reason`, `attempts`, `created_at`, `reprocessed_at` (nullable). This is where a consumer's *technical* failures land after exhausting retries (see "Event Flow and Idempotency") — distinct from `payout_attempts.status = failed`, which is a successfully-processed *business* outcome (the PSP call itself failed), not a processing error.

**Seed data:** a migration-time seed script inserts a handful of demo accounts into `authz.balance_projection` with starting `confirmed_balance` values (e.g. `acc_demo_1`, `acc_demo_2`, each funded), so the README's demo walkthrough has accounts to transfer between without needing an account-creation endpoint.

The **same `transfer_id`** — the idempotency key the client supplies on `POST /transfers` — flows through all three services and is the dedupe key at every boundary. No service mints its own derived key.

## 3. Event Flow and Idempotency

**Kafka topics** (one per state transition, matching the "one topic per logical flow" pattern from webhook-ingestion-replay-dlq):

1. **`transfer-authorized`** — published by `authorization`'s outbox poller. Payload: `{ transferId, fromAccount, toAccount, amount }`. Consumed by `ledger`.
2. **`settlement-posted`** — published by `ledger`'s outbox poller after the double-entry write commits. Payload: `{ transferId, fromAccount, toAccount, amount, postedAt }`. Consumed by **two independent consumer groups**: `authorization` (updates `balance_projection`, releases the reservation, sets `transfers.status = confirmed`) and `payout` (initiates the external send).
3. **`payout-result`** — published by `payout`'s outbox poller. Payload: `{ transferId, outcome: "succeeded" | "failed", pspReference? }`. No consumer in this project — it is a terminal, queryable state (see "Payout Failure Handling").

**Dedupe sequence in every consumer** (check after processing succeeds, never before — the same ordering used in webhook-ingestion-replay-dlq, so that a crash between the business write and the dedupe-row write is safe to retry):

```
on message(transferId, ...):
  if transferId already present in this service's dedupe table: ack, skip
  else:
    begin tx
      perform the business write (ledger entries / balance update / PSP call)
      insert the dedupe row (processed_transfers / payout_attempts)
      insert outbox row
    commit tx
    ack
```

**Technical consumer failures** (e.g. Postgres unreachable at the moment of consumption) use the same in-process retry-with-backoff-then-DLQ mechanism already validated in webhook-ingestion-replay-dlq: a bounded number of retries with configurable backoff, and if exhausted, the message is written to that service's own DLQ table (not a second Kafka topic) and is browsable/reprocessable over HTTP. This is infrastructure-level failure handling, distinct from the payout service's *business* outcome (PSP call succeeded or failed), which is modeled as data rather than a DLQ entry.

## 4. Payout Failure Handling

When `payout` exhausts its retries calling the mocked external PSP, the money has already settled internally (the ledger's double-entry write is committed), but the external leg never happened. This project takes the position that this is a **terminal state requiring manual intervention, not an automatic reversal**:

- `payout_attempts.status = failed` is recorded, and `payout-result: failed` is published.
- `GET /payout/failed` lists failed attempts (the `payout` service's equivalent of the webhook project's `/dlq` browsing endpoint).
- `POST /payout/:transferId/retry` re-attempts the same PSP call using the same `transfer_id` as the idempotency key.
- No service automatically reverses the ledger or re-credits the authorization balance. This mirrors how real payment systems actually handle PSP failures operationally (manual retry or a separately-initiated refund), and avoids building a full compensating-transaction saga (`ledger` consuming `payout-result`, emitting reversal entries with an original/reversal type discriminator, `authorization` re-crediting without double-counting) — a scope roughly equivalent to a second project, for a correctness story (idempotency + traceability) this design already delivers without it.

## 5. Testing Strategy

- **Unit tests (no infra)** — pure domain logic per service: the double-entry invariant (debits = credits per transfer), `available_balance` calculation, the approve/reject decision, and zod validation of inbound event payloads. Use cases are tested against one fake per port (`AccountRepositoryPort`, `OutboxRepositoryPort`, etc.), with no Postgres/Kafka dependency.
- **Adapter tests (real Postgres)** — each service's own repository and outbox adapters are tested against the real Postgres instance from `docker-compose`, scoped to that service's own schema.
- **Integration tests (real Kafka)** — one test per hop: `authorization` publishes `transfer-authorized` → `ledger` consumes and writes; `ledger` publishes `settlement-posted` → **both** `authorization` and `payout` consume it independently (the fan-out across two consumer groups is the integration point new to this project). Following the lesson learned in webhook-ingestion-replay-dlq (Kafka consumer group join taking longer than expected locally), tests wait ~5000ms for consumer group join before publishing, applied from the start rather than discovered via a failing test.
- **End-to-end test** — `POST /transfers` on `authorization` through to the mock PSP being called by `payout`, asserted via polling/retry (this path is eventually consistent through Kafka, not synchronous).
- **Mock PSP** (`mock-payout-psp`, same shape as the prior project's `mock-partner-api`) — succeeds by default; fails deterministically for a magic destination account (e.g. `acc_psp_fail_demo`), to exercise the terminal-failure path from section 4 on demand.
- `vitest.config.mts` with `fileParallelism: false` per service (shared infra across that service's own test file), same as the prior project.

## 6. Project Structure

```
event-driven-settlement-ledger/
├── services/
│   ├── authorization/
│   │   ├── src/
│   │   │   ├── domain/            (approval rule, available-balance calculation)
│   │   │   ├── application/       (ports + use-cases: RequestTransfer, HandleSettlementPosted)
│   │   │   ├── adapters/
│   │   │   │   ├── inbound/http/       (POST /transfers, GET /transfers/:id)
│   │   │   │   ├── inbound/kafka/      (settlement-posted consumer)
│   │   │   │   ├── outbound/postgres/  (repositories + outbox)
│   │   │   │   └── outbound/kafka/     (outbox poller → producer)
│   │   │   ├── config/
│   │   │   └── main.ts
│   │   ├── db/migrations/
│   │   ├── tests/
│   │   ├── package.json, Dockerfile, .dockerignore, vitest.config.mts, .eslintrc.cjs
│   ├── ledger/      (same shape; domain = the double-entry invariant)
│   └── payout/      (same shape; domain = the send-or-not decision)
├── mock-payout-psp/
├── helm/            (basic chart: one Deployment per service, Kafka StatefulSet, one Service per service — documents deployment intent, not applied to a real cluster)
├── docker-compose.yml
├── docs/design/{specs,plans}/
└── README.md
```

Each service is fully independent: its own `package.json`, `node_modules`, lint/test config — no shared npm workspace. The outbox poller and the retry/backoff utility are small (roughly 30-50 lines each) and are **duplicated** across the three services rather than extracted into a shared library. This is deliberate: real microservices start this way — a shared library across bounded contexts becomes a deploy-time coupling that this project is specifically trying to demonstrate the absence of. Extraction would only be worth it once duplication actually causes pain, which three small files don't.

`docker-compose.yml` brings up: one Postgres instance (three schemas created via an init script), Kafka (KRaft mode, no ZooKeeper, matching the prior project's dual `PLAINTEXT`/`PLAINTEXT_HOST` listener setup), the three services, `mock-payout-psp`, and one migration step per service.

## 7. Deployment / Kubernetes

A `helm/` directory with a basic chart: one `Deployment` + one `Service` per service (`authorization`, `ledger`, `payout`), Kafka as a `StatefulSet`. This documents deployment intent, matching the posture of the other portfolio projects — it is not applied against a real cluster or tested with `helm install`.

## 8. API Endpoints

The idempotency key is a client-supplied `transferId` field in the request body — not an HTTP header — so the same field name is used consistently across the HTTP boundary, the database primary keys, and every Kafka payload described in section 3.

Every service additionally exposes the same generic pair for its own **technical** DLQ (the webhook project's `/dlq` pattern, repeated per service): `GET /dlq` (list entries) and `POST /dlq/:id/reprocess` (replay that entry's stored payload through the same consumer logic). This is separate from `payout`'s business-outcome endpoints below.

**`authorization`:**
- `POST /transfers` — body `{ transferId, fromAccount, toAccount, amount }` → `201 { transferId, status }`. Replaying the same `transferId` returns the previously decided result instead of re-evaluating.
- `GET /transfers/:transferId` → `{ transferId, status, fromAccount, toAccount, amount }`
- `GET /dlq`, `POST /dlq/:id/reprocess` — technical failures from consuming `settlement-posted`.
- `GET /health`

**`ledger`:**
- `GET /accounts/:accountId/entries` → the append-only list of ledger entries for that account, in order — the direct demonstration of this project's "full traceability of every movement" goal.
- `GET /dlq`, `POST /dlq/:id/reprocess` — technical failures from consuming `transfer-authorized`.
- `GET /health`

**`payout`:**
- `GET /payout/failed` → list of failed payout attempts, i.e. business outcomes where the PSP call itself failed (not a processing error).
- `POST /payout/:transferId/retry` → re-attempts the PSP call for that `transferId`.
- `GET /dlq`, `POST /dlq/:id/reprocess` — technical failures from consuming `settlement-posted`, separate from the business-outcome endpoints above.
- `GET /health`

## Stack

Node.js/TypeScript (CommonJS, matching the prior two projects), Express, zod, KafkaJS, `pg` (node-postgres, no ORM), vitest, supertest, Docker Compose for the local environment.
