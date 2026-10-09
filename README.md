# Event-Driven Settlement Ledger

> Status: ✅ implemented

## Goal

Prove the design of a financial settlement system with strong consistency guarantees: no duplicate transactions, no lost events, full traceability of every movement.

## Features

- Three independent services split by bounded context — authorization, ledger, payout — communicating only through Kafka
- Append-only, double-entry ledger as the source of truth: every transfer writes exactly one debit and one credit line, never updated or deleted
- Every balance-moving operation is idempotent on a single client-supplied `transferId`, enforced independently at each service's boundary
- Transactional outbox pattern: the business write and the outbox row are committed in the same Postgres transaction; a separate poller publishes to Kafka
- A failed external payout is a traceable, retryable terminal state — not silently or automatically reversed

## Stack

Node.js/TypeScript, PostgreSQL (one instance, one schema per service), Kafka, Docker Compose.

## Kubernetes

A basic Helm chart under `helm/settlement-ledger/` documents deployment intent (one Deployment + Service per service, Kafka as a StatefulSet) — it is not applied against a real cluster.

## How to run

With Docker Compose (everything, including the three services and the mock PSP):

```bash
docker compose up --build
```

Locally, without Docker (useful for iterating on one service):

```bash
docker compose up -d postgres kafka
cd services/authorization && npm install && npm run migrate && npm run dev   # terminal 1, port 3001
cd services/ledger && npm install && npm run migrate && npm run dev          # terminal 2, port 3002
cd services/payout && npm install && npm run migrate && npm run dev          # terminal 3, port 3003
cd mock-payout-psp && npm install && npm run dev                              # terminal 4, port 4003
```

## Demo walkthrough

```bash
curl -X POST http://localhost:3001/transfers \
  -H 'Content-Type: application/json' \
  -d '{"transferId":"demo-1","fromAccount":"acc_demo_1","toAccount":"acc_demo_2","amount":100}'

curl http://localhost:3001/transfers/demo-1
curl http://localhost:3002/accounts/acc_demo_1/entries
curl http://localhost:3002/accounts/acc_demo_2/entries
```

The second call should eventually show `"status":"confirmed"` once the ledger has posted the settlement and authorization's projection consumer has caught up (this is eventually consistent through Kafka, typically well under a second locally).

To see the terminal payout-failure path, transfer to the magic failure account:

```bash
curl -X POST http://localhost:3001/transfers \
  -H 'Content-Type: application/json' \
  -d '{"transferId":"demo-2","fromAccount":"acc_demo_1","toAccount":"acc_psp_fail_demo","amount":50}'

curl http://localhost:3003/payout/failed
curl -X POST http://localhost:3003/payout/demo-2/retry
```

Retrying still fails deterministically (the magic account always declines) — to see a successful retry, send a new transfer to `acc_demo_2` instead and compare.

Each service also exposes its own technical DLQ for Kafka-consumer failures:

```bash
curl http://localhost:3001/dlq   # authorization: settlement-posted consumer failures
curl http://localhost:3002/dlq   # ledger: transfer-authorized consumer failures
curl http://localhost:3003/dlq   # payout: settlement-posted consumer failures
```

## Running the tests

Each service has its own independent test suite (requires `docker compose up -d postgres kafka` and that service's own `npm run migrate` first):

```bash
cd services/authorization && npm test && npm run typecheck && npm run lint
cd services/ledger && npm test && npm run typecheck && npm run lint
cd services/payout && npm test && npm run typecheck && npm run lint
cd mock-payout-psp && npm test
```

The end-to-end test spawns all three services and the mock PSP itself (requires the same infra up and migrated first):

```bash
cd tests-e2e && npm install && npm test
```
