# Event-Driven Settlement Ledger

> Status: 🚧 planned — not yet implemented

## Goal

Prove the design of a financial settlement system with strong consistency guarantees: no duplicate transactions, no lost events, full traceability of every movement.

## Features

- Services split by bounded context: authorization, ledger, payout
- Append-only ledger as the source of truth — nothing is overwritten, only added
- Every balance-moving operation requires an idempotency key
- Outbox pattern: domain event published to Kafka transactionally alongside the database write

## Stack

Node.js/TypeScript, PostgreSQL, Kafka, Docker Compose for the local environment.

## Kubernetes

Helm chart with one Deployment per service, Kafka as a StatefulSet, one internal Service per service.
