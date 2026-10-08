# Event-Driven Settlement Ledger Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build three independent services (authorization, ledger, payout) that settle transfers between accounts with strong consistency guarantees, communicating only through Kafka via the transactional outbox pattern.

**Architecture:** Hexagonal layering (domain/application/adapters/config) repeated independently in each of `services/authorization`, `services/ledger`, `services/payout` — no shared npm workspace, no shared library code. Each service owns one Postgres schema in a single shared Postgres instance and talks to the others only via Kafka topics `transfer-authorized`, `settlement-posted`, `payout-result`.

**Tech Stack:** Node.js/TypeScript (CommonJS), Express, zod, KafkaJS, `pg` (node-postgres, no ORM), vitest, supertest, Docker Compose.

**Spec:** `docs/design/specs/2026-10-08-event-driven-settlement-ledger-design.md`

## Global Constraints

- CommonJS TypeScript, strict mode, matching the `tsconfig.json` shape used in the prior two portfolio projects.
- `eslint-plugin-boundaries` v7 with the `boundaries/dependencies` rule + `policies` + `import/resolver` configured for `.ts` extensions, in every service, from that service's first commit — not retrofitted.
- `vitest.config.mts` (not `.ts`) in every service, with `fileParallelism: false`.
- Every external or cross-boundary payload (Kafka messages, the mock PSP's HTTP response) is validated with zod at the adapter boundary.
- Every service's `Dockerfile` uses `npm ci`, with a `.dockerignore` excluding `node_modules`.
- Every service's `app.ts` has a JSON body parser, a `GET /health` route, and a 4-argument Express error-handling middleware (JSON error responses, including malformed-JSON handling) from its first commit.
- Every Kafka consumer integration test waits ~5000ms after `consumer.run()` for the consumer group to finish joining before publishing — a real timing issue found the hard way in the webhook-ingestion-replay-dlq project.
- Every consumer's dedupe check is written to the dedupe table/column *after* the business write succeeds, never before — a crash between the two must be safe to retry.
- Postgres is one shared instance (database `settlement_ledger`), three schemas (`authz`, `ledger`, `payout`) created by a root `db/init/01-schemas.sql` mounted into the Postgres container's `/docker-entrypoint-initdb.d/`. Each service's own `pg.Pool` is constructed with `options: '-c search_path=<its own schema>'` so every bare table name in its queries resolves inside that schema. Each service's own `db/migrate.ts` additionally runs `CREATE SCHEMA IF NOT EXISTS <schema>` defensively, so `npm run migrate` works even against a bare Postgres with no init script. The `authorization` service's Postgres schema is named `authz`, not `authorization` — `AUTHORIZATION` is a reserved SQL keyword (used in `CREATE SCHEMA ... AUTHORIZATION owner`), so `CREATE SCHEMA IF NOT EXISTS authorization;` is a syntax error. This was discovered when Task 1's init script was first run against real Postgres; `authz` is used everywhere a Postgres schema name is needed, while the service's directory, package name, ports, and code identifiers remain `authorization` throughout.
- Kafka is a single KRaft-mode broker (no ZooKeeper), dual `PLAINTEXT` (internal, port 29092)/`PLAINTEXT_HOST` (external, port 9092) listeners — reusing the exact working `docker-compose` config from webhook-ingestion-replay-dlq.
- The idempotency key is a client-supplied `transferId` field in the request body (never a header), reused verbatim as the primary/dedupe key in every table and the `key` of every Kafka message it appears in.
- Ports: `authorization` = 3001, `ledger` = 3002, `payout` = 3003, `mock-payout-psp` = 4003.
- No `Co-Authored-By` or any AI-attribution trailer in any commit message.
- The outbox poller and the retry-with-backoff-then-DLQ logic are small (~30-50 lines) and are written independently in each of the three services — never extracted into a shared library.

---

## Task 1: Shared infrastructure — Postgres (3 schemas) and Kafka

**Files:**
- Create: `docker-compose.yml`
- Create: `db/init/01-schemas.sql`
- Create: `.gitignore` additions (if needed — check the existing file first)

**Interfaces:**
- Produces: a running Postgres reachable at `localhost:5432` (user/password `postgres`, database `settlement_ledger`) with empty schemas `authz`, `ledger`, `payout` already created; a running Kafka reachable at `localhost:9092` externally / `kafka:29092` from other containers.

- [ ] **Step 1: Write the schema init script**

```sql
-- db/init/01-schemas.sql
CREATE SCHEMA IF NOT EXISTS authz;
CREATE SCHEMA IF NOT EXISTS ledger;
CREATE SCHEMA IF NOT EXISTS payout;
```

- [ ] **Step 2: Write docker-compose.yml with Postgres and Kafka only (app services come in the final task)**

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: settlement_ledger
    ports:
      - "5432:5432"
    volumes:
      - ./db/init:/docker-entrypoint-initdb.d
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 3s
      retries: 20

  kafka:
    image: apache/kafka:3.8.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:29092,CONTROLLER://:9093,PLAINTEXT_HOST://:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - "9092:9092"
```

- [ ] **Step 3: Bring the infra up and verify**

Run: `docker compose up -d postgres kafka`
Expected: both containers start; `docker compose ps` shows `postgres` as `healthy`.

Run: `docker exec -it $(docker compose ps -q postgres) psql -U postgres -d settlement_ledger -c '\dn'`
Expected: lists schemas `authz`, `ledger`, `payout` (plus `public`).

- [ ] **Step 4: Commit**

```bash
git checkout -b task-1-shared-infra
git add docker-compose.yml db/init/01-schemas.sql
git commit -m "Add shared Postgres and Kafka infrastructure"
git push -u origin task-1-shared-infra
```

---

## Task 2: Scaffold the authorization service

**Files:**
- Create: `services/authorization/package.json`
- Create: `services/authorization/tsconfig.json`
- Create: `services/authorization/.eslintrc.cjs`
- Create: `services/authorization/vitest.config.mts`
- Create: `services/authorization/.dockerignore`
- Create: `services/authorization/Dockerfile`
- Create: `services/authorization/src/config/env.ts`
- Create: `services/authorization/src/adapters/inbound/http/app.ts`
- Create: `services/authorization/src/main.ts`
- Test: `services/authorization/tests/health.test.ts`

**Interfaces:**
- Produces: `loadEnv(): AppEnv` (fields: `port`, `databaseUrl`, `kafkaBrokers: string[]`), `createApp(deps: AppDependencies): Express` (empty `AppDependencies` for now), a running HTTP server with `GET /health` → `200 { status: 'ok' }`.

- [ ] **Step 1: package.json**

```json
{
  "name": "authorization",
  "version": "0.1.0",
  "private": true,
  "engines": { "node": ">=18" },
  "scripts": {
    "dev": "tsx src/main.ts",
    "migrate": "tsx db/migrate.ts",
    "test": "vitest run",
    "test:watch": "vitest",
    "typecheck": "tsc --noEmit",
    "lint": "eslint . --ext .ts"
  },
  "dependencies": {
    "express": "^4.19.2",
    "kafkajs": "^2.2.4",
    "pg": "^8.13.0",
    "zod": "^3.23.8"
  },
  "devDependencies": {
    "@types/express": "^4.17.21",
    "@types/node": "^22.5.0",
    "@types/pg": "^8.11.10",
    "@types/supertest": "^6.0.2",
    "@typescript-eslint/eslint-plugin": "^7.16.0",
    "@typescript-eslint/parser": "^7.16.0",
    "eslint": "^8.57.0",
    "eslint-import-resolver-node": "^0.3.9",
    "eslint-plugin-boundaries": "^7.2.0",
    "supertest": "^7.0.0",
    "tsx": "^4.16.2",
    "typescript": "^5.5.4",
    "vitest": "^5.0.0"
  }
}
```

- [ ] **Step 2: tsconfig.json**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "CommonJS",
    "moduleResolution": "Node",
    "lib": ["ES2022"],
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "resolveJsonModule": true,
    "noEmit": true
  },
  "include": ["src", "db", "tests"]
}
```

- [ ] **Step 3: .eslintrc.cjs**

```js
module.exports = {
  root: true,
  parser: '@typescript-eslint/parser',
  parserOptions: {
    ecmaVersion: 2022,
    sourceType: 'module',
  },
  plugins: ['@typescript-eslint', 'boundaries'],
  extends: ['eslint:recommended', 'plugin:@typescript-eslint/recommended'],
  env: {
    node: true,
    es2022: true,
  },
  settings: {
    'import/resolver': {
      node: {
        extensions: ['.ts', '.js'],
      },
    },
    'boundaries/elements': [
      { type: 'domain', pattern: 'src/domain/**' },
      { type: 'application', pattern: 'src/application/**' },
      { type: 'adapters', pattern: 'src/adapters/**' },
      { type: 'config', pattern: 'src/config/**' },
      { type: 'main', pattern: 'src/main.ts' },
    ],
  },
  rules: {
    '@typescript-eslint/no-unused-vars': [
      'error',
      { argsIgnorePattern: '^_' },
    ],
    'boundaries/dependencies': [
      2,
      {
        default: 'disallow',
        policies: [
          {
            from: { element: { type: 'domain' } },
            allow: { to: { element: { type: 'domain' } } },
          },
          {
            from: { element: { type: 'application' } },
            allow: { to: { element: { types: { anyOf: ['domain', 'application'] } } } },
          },
          {
            from: { element: { type: 'adapters' } },
            allow: {
              to: { element: { types: { anyOf: ['domain', 'application', 'adapters', 'config'] } } },
            },
          },
          {
            from: { element: { type: 'config' } },
            allow: { to: { element: { type: 'config' } } },
          },
          {
            from: { element: { type: 'main' } },
            allow: {
              to: { element: { types: { anyOf: ['domain', 'application', 'adapters', 'config'] } } },
            },
          },
        ],
      },
    ],
  },
};
```

- [ ] **Step 4: vitest.config.mts**

```ts
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    include: ['tests/**/*.test.ts'],
    testTimeout: 15000,
    fileParallelism: false,
  },
});
```

- [ ] **Step 5: .dockerignore and Dockerfile**

```
node_modules
.git
.worktrees
.superpowers
coverage
```

```dockerfile
FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

EXPOSE 3001

CMD ["npx", "tsx", "src/main.ts"]
```

- [ ] **Step 6: config/env.ts**

```ts
export interface AppEnv {
  port: number;
  databaseUrl: string;
  kafkaBrokers: string[];
}

export function loadEnv(): AppEnv {
  const rawPort = process.env.PORT;
  const port = rawPort === undefined ? 3001 : Number(rawPort);
  if (Number.isNaN(port)) {
    throw new Error(`Invalid PORT environment variable: "${rawPort}" is not a number`);
  }

  return {
    port,
    databaseUrl: process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger',
    kafkaBrokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  };
}
```

- [ ] **Step 7: Write the failing health-check test**

```ts
// tests/health.test.ts
import { describe, it, expect } from 'vitest';
import request from 'supertest';
import { createApp } from '../src/adapters/inbound/http/app';

describe('GET /health', () => {
  it('returns 200 ok', async () => {
    const app = createApp({});
    const response = await request(app).get('/health');
    expect(response.status).toBe(200);
    expect(response.body).toEqual({ status: 'ok' });
  });
});
```

- [ ] **Step 8: Run test to verify it fails**

Run: `cd services/authorization && npm install && npx vitest run tests/health.test.ts`
Expected: FAIL — `app.ts` does not exist yet.

- [ ] **Step 9: Implement app.ts and main.ts**

```ts
// src/adapters/inbound/http/app.ts
import express, { Express, Request, Response, NextFunction } from 'express';

export interface AppDependencies {
  [key: string]: unknown;
}

export function createApp(_deps: AppDependencies): Express {
  const app = express();
  app.use(express.json());

  app.get('/health', (_req: Request, res: Response) => {
    res.status(200).json({ status: 'ok' });
  });

  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof SyntaxError && 'status' in err && (err as { status?: number }).status === 400) {
      res.status(400).json({ error: 'invalid JSON body' });
      return;
    }
    res.status(500).json({ error: 'internal server error' });
  });

  return app;
}
```

```ts
// src/main.ts
import { loadEnv } from './config/env';
import { createApp } from './adapters/inbound/http/app';

async function main(): Promise<void> {
  const env = loadEnv();
  const app = createApp({});
  app.listen(env.port, () => {
    console.log(`authorization listening on port ${env.port}`);
  });
}

main().catch((error) => {
  console.error('fatal startup error', error);
  process.exit(1);
});
```

- [ ] **Step 10: Run test to verify it passes**

Run: `npx vitest run tests/health.test.ts`
Expected: PASS

- [ ] **Step 11: Typecheck and lint**

Run: `npm run typecheck && npm run lint`
Expected: both clean.

- [ ] **Step 12: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-2-authorization-scaffold
git add services/authorization
git commit -m "Scaffold the authorization service"
git push -u origin task-2-authorization-scaffold
```

---

## Task 3: Authorization domain — approval decision and available balance

**Files:**
- Create: `services/authorization/src/domain/services/AvailableBalance.ts`
- Create: `services/authorization/src/domain/services/ApprovalDecision.ts`
- Test: `services/authorization/tests/domain/ApprovalDecision.test.ts`
- Test: `services/authorization/tests/domain/AvailableBalance.test.ts`

**Interfaces:**
- Produces: `calculateAvailableBalance(confirmedBalance: number, pendingReservationsTotal: number): number`; `decideApproval(input: { availableBalance: number; amount: number }): { approved: true } | { approved: false; reason: string }`.

- [ ] **Step 1: Write the failing tests**

```ts
// tests/domain/AvailableBalance.test.ts
import { describe, it, expect } from 'vitest';
import { calculateAvailableBalance } from '../../src/domain/services/AvailableBalance';

describe('calculateAvailableBalance', () => {
  it('subtracts the pending reservations total from the confirmed balance', () => {
    expect(calculateAvailableBalance(1000, 300)).toBe(700);
  });

  it('returns the full confirmed balance when there are no pending reservations', () => {
    expect(calculateAvailableBalance(1000, 0)).toBe(1000);
  });
});
```

```ts
// tests/domain/ApprovalDecision.test.ts
import { describe, it, expect } from 'vitest';
import { decideApproval } from '../../src/domain/services/ApprovalDecision';

describe('decideApproval', () => {
  it('approves when the amount is within the available balance', () => {
    expect(decideApproval({ availableBalance: 500, amount: 500 })).toEqual({ approved: true });
  });

  it('rejects when the amount exceeds the available balance', () => {
    const result = decideApproval({ availableBalance: 500, amount: 501 });
    expect(result).toEqual({ approved: false, reason: 'insufficient available balance' });
  });

  it('rejects a zero or negative amount', () => {
    const result = decideApproval({ availableBalance: 500, amount: 0 });
    expect(result).toEqual({ approved: false, reason: 'amount must be greater than zero' });
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run tests/domain`
Expected: FAIL — modules don't exist yet.

- [ ] **Step 3: Implement**

```ts
// src/domain/services/AvailableBalance.ts
export function calculateAvailableBalance(confirmedBalance: number, pendingReservationsTotal: number): number {
  return confirmedBalance - pendingReservationsTotal;
}
```

```ts
// src/domain/services/ApprovalDecision.ts
export interface ApprovalInput {
  availableBalance: number;
  amount: number;
}

export type ApprovalResult = { approved: true } | { approved: false; reason: string };

export function decideApproval(input: ApprovalInput): ApprovalResult {
  if (input.amount <= 0) {
    return { approved: false, reason: 'amount must be greater than zero' };
  }
  if (input.amount > input.availableBalance) {
    return { approved: false, reason: 'insufficient available balance' };
  }
  return { approved: true };
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/domain`
Expected: PASS

- [ ] **Step 5: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-3-authorization-domain
git add services/authorization/src/domain services/authorization/tests/domain
git commit -m "Implement the approval decision and available balance calculation"
git push -u origin task-3-authorization-domain
```

---

## Task 4: Authorization database migrations and seed data

**Files:**
- Create: `services/authorization/db/migrate.ts`
- Create: `services/authorization/db/migrations/001_create_transfers.sql`
- Create: `services/authorization/db/migrations/002_create_reservations.sql`
- Create: `services/authorization/db/migrations/003_create_balance_projection.sql`
- Create: `services/authorization/db/migrations/004_create_outbox.sql`
- Create: `services/authorization/db/migrations/005_create_dlq_events.sql`
- Create: `services/authorization/db/migrations/006_seed_demo_accounts.sql`

**Interfaces:**
- Produces: tables `transfers`, `reservations`, `balance_projection`, `outbox`, `dlq_events` inside the `authz` Postgres schema; seeded rows in `balance_projection` for `acc_demo_1` (100000), `acc_demo_2` (5000), `acc_psp_fail_demo` (5000).

- [ ] **Step 1: Write the migration runner**

```ts
// db/migrate.ts
import { readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { Pool } from 'pg';

const SCHEMA = 'authz';

async function migrate(): Promise<void> {
  const databaseUrl = process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';
  const pool = new Pool({ connectionString: databaseUrl, options: `-c search_path=${SCHEMA}` });

  await pool.query(`CREATE SCHEMA IF NOT EXISTS ${SCHEMA}`);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      filename TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  const migrationsDir = path.join(__dirname, 'migrations');
  const files = readdirSync(migrationsDir)
    .filter((file) => file.endsWith('.sql'))
    .sort();

  for (const file of files) {
    const alreadyApplied = await pool.query('SELECT 1 FROM schema_migrations WHERE filename = $1', [file]);
    if (alreadyApplied.rows.length > 0) {
      continue;
    }

    const sql = readFileSync(path.join(migrationsDir, file), 'utf-8');
    await pool.query(sql);
    await pool.query('INSERT INTO schema_migrations (filename) VALUES ($1)', [file]);
    console.log(`applied migration ${file}`);
  }

  await pool.end();
}

migrate().catch((error) => {
  console.error('migration failed', error);
  process.exit(1);
});
```

- [ ] **Step 2: Write the migration SQL files**

```sql
-- db/migrations/001_create_transfers.sql
CREATE TABLE IF NOT EXISTS transfers (
  id TEXT PRIMARY KEY,
  from_account TEXT NOT NULL,
  to_account TEXT NOT NULL,
  amount NUMERIC NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

```sql
-- db/migrations/002_create_reservations.sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS reservations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  transfer_id TEXT NOT NULL REFERENCES transfers(id),
  account_id TEXT NOT NULL,
  amount NUMERIC NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

```sql
-- db/migrations/003_create_balance_projection.sql
CREATE TABLE IF NOT EXISTS balance_projection (
  account_id TEXT PRIMARY KEY,
  confirmed_balance NUMERIC NOT NULL
);
```

```sql
-- db/migrations/004_create_outbox.sql
CREATE TABLE IF NOT EXISTS outbox (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  topic TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);
```

```sql
-- db/migrations/005_create_dlq_events.sql
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
```

```sql
-- db/migrations/006_seed_demo_accounts.sql
-- acc_demo_1 is seeded generously (not just enough for one demo transfer) because
-- Task 24's end-to-end test and every manual README walkthrough draw from it
-- repeatedly over the life of this project, and confirmed_balance only ever
-- decreases for it (nothing credits it back).
INSERT INTO balance_projection (account_id, confirmed_balance) VALUES
  ('acc_demo_1', 100000),
  ('acc_demo_2', 5000),
  ('acc_psp_fail_demo', 5000)
ON CONFLICT (account_id) DO NOTHING;
```

- [ ] **Step 3: Run the migration against the real Postgres from Task 1**

Run: `cd services/authorization && DATABASE_URL=postgres://postgres:postgres@localhost:5432/settlement_ledger npm run migrate`
Expected: prints `applied migration 001_create_transfers.sql` through `006_seed_demo_accounts.sql`.

Run: `docker exec -it $(docker compose ps -q postgres) psql -U postgres -d settlement_ledger -c 'SELECT account_id, confirmed_balance FROM authorization.balance_projection'`
Expected: shows the three seeded accounts with their balances.

- [ ] **Step 4: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-4-authorization-migrations
git add services/authorization/db
git commit -m "Add authorization database migrations and seed demo accounts"
git push -u origin task-4-authorization-migrations
```

---

## Task 5: Authorization application layer — RequestTransfer use case

**Files:**
- Create: `services/authorization/src/application/ports/TransferRepositoryPort.ts`
- Create: `services/authorization/src/application/ports/BalanceProjectionPort.ts`
- Create: `services/authorization/src/application/use-cases/RequestTransfer.ts`
- Create: `services/authorization/tests/fakes/FakeTransferRepository.ts`
- Create: `services/authorization/tests/fakes/FakeBalanceProjection.ts`
- Test: `services/authorization/tests/application/RequestTransfer.test.ts`

**Interfaces:**
- Consumes: `calculateAvailableBalance`, `decideApproval` from Task 3.
- Produces: `TransferRepositoryPort` (`findById`, `saveApproved`, `saveRejected`), `BalanceProjectionPort` (`getConfirmedBalance`, `getPendingReservationsTotal`), `RequestTransfer.execute(input): Promise<{ transferId: string; status: TransferStatus }>`. `TransferStatus = 'approved' | 'rejected' | 'confirmed' | 'released'`. The outbox payload produced on approval has shape `{ transferId, fromAccount, toAccount, amount }` — this is the exact shape Task 12 (ledger) must parse from `transfer-authorized`.

- [ ] **Step 1: Write the ports**

```ts
// src/application/ports/TransferRepositoryPort.ts
export type TransferStatus = 'approved' | 'rejected' | 'confirmed' | 'released';

export interface TransferRecord {
  id: string;
  fromAccount: string;
  toAccount: string;
  amount: number;
  status: TransferStatus;
}

export interface ReservationInput {
  accountId: string;
  amount: number;
}

export interface TransferRepositoryPort {
  findById(id: string): Promise<TransferRecord | null>;
  saveApproved(transfer: TransferRecord, reservation: ReservationInput, outboxPayload: unknown): Promise<void>;
  saveRejected(transfer: TransferRecord): Promise<void>;
}
```

```ts
// src/application/ports/BalanceProjectionPort.ts
export interface BalanceProjectionPort {
  getConfirmedBalance(accountId: string): Promise<number>;
  getPendingReservationsTotal(accountId: string): Promise<number>;
}
```

- [ ] **Step 2: Write the fakes**

```ts
// tests/fakes/FakeTransferRepository.ts
import {
  TransferRepositoryPort,
  TransferRecord,
  ReservationInput,
} from '../../src/application/ports/TransferRepositoryPort';

export class FakeTransferRepository implements TransferRepositoryPort {
  private readonly transfers = new Map<string, TransferRecord>();
  public savedApprovedCalls = 0;
  public savedRejectedCalls = 0;

  async findById(id: string): Promise<TransferRecord | null> {
    return this.transfers.get(id) ?? null;
  }

  async saveApproved(transfer: TransferRecord, _reservation: ReservationInput, _outboxPayload: unknown): Promise<void> {
    this.transfers.set(transfer.id, transfer);
    this.savedApprovedCalls += 1;
  }

  async saveRejected(transfer: TransferRecord): Promise<void> {
    this.transfers.set(transfer.id, transfer);
    this.savedRejectedCalls += 1;
  }
}
```

```ts
// tests/fakes/FakeBalanceProjection.ts
import { BalanceProjectionPort } from '../../src/application/ports/BalanceProjectionPort';

export class FakeBalanceProjection implements BalanceProjectionPort {
  constructor(
    private readonly confirmedBalances: Record<string, number> = {},
    private readonly pendingReservationsTotals: Record<string, number> = {},
  ) {}

  async getConfirmedBalance(accountId: string): Promise<number> {
    return this.confirmedBalances[accountId] ?? 0;
  }

  async getPendingReservationsTotal(accountId: string): Promise<number> {
    return this.pendingReservationsTotals[accountId] ?? 0;
  }
}
```

- [ ] **Step 3: Write the failing test**

```ts
// tests/application/RequestTransfer.test.ts
import { describe, it, expect } from 'vitest';
import { RequestTransfer } from '../../src/application/use-cases/RequestTransfer';
import { FakeTransferRepository } from '../fakes/FakeTransferRepository';
import { FakeBalanceProjection } from '../fakes/FakeBalanceProjection';

describe('RequestTransfer', () => {
  it('approves a transfer within the available balance', async () => {
    const transferRepository = new FakeTransferRepository();
    const balanceProjection = new FakeBalanceProjection({ acc_1: 1000 });
    const useCase = new RequestTransfer(transferRepository, balanceProjection);

    const result = await useCase.execute({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });

    expect(result).toEqual({ transferId: 't1', status: 'approved' });
    expect(transferRepository.savedApprovedCalls).toBe(1);
  });

  it('rejects a transfer that exceeds the available balance', async () => {
    const transferRepository = new FakeTransferRepository();
    const balanceProjection = new FakeBalanceProjection({ acc_1: 100 });
    const useCase = new RequestTransfer(transferRepository, balanceProjection);

    const result = await useCase.execute({ transferId: 't2', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });

    expect(result).toEqual({ transferId: 't2', status: 'rejected' });
    expect(transferRepository.savedRejectedCalls).toBe(1);
  });

  it('accounts for pending reservations when computing the available balance', async () => {
    const transferRepository = new FakeTransferRepository();
    const balanceProjection = new FakeBalanceProjection({ acc_1: 1000 }, { acc_1: 600 });
    const useCase = new RequestTransfer(transferRepository, balanceProjection);

    const result = await useCase.execute({ transferId: 't3', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });

    expect(result).toEqual({ transferId: 't3', status: 'rejected' });
  });

  it('is idempotent: replaying the same transferId returns the existing result without re-deciding', async () => {
    const transferRepository = new FakeTransferRepository();
    const balanceProjection = new FakeBalanceProjection({ acc_1: 1000 });
    const useCase = new RequestTransfer(transferRepository, balanceProjection);

    await useCase.execute({ transferId: 't4', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });
    const second = await useCase.execute({ transferId: 't4', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });

    expect(second).toEqual({ transferId: 't4', status: 'approved' });
    expect(transferRepository.savedApprovedCalls).toBe(1);
  });
});
```

- [ ] **Step 4: Run test to verify it fails**

Run: `npx vitest run tests/application/RequestTransfer.test.ts`
Expected: FAIL — `RequestTransfer` module does not exist.

- [ ] **Step 5: Implement RequestTransfer**

```ts
// src/application/use-cases/RequestTransfer.ts
import { calculateAvailableBalance } from '../../domain/services/AvailableBalance';
import { decideApproval } from '../../domain/services/ApprovalDecision';
import { TransferRepositoryPort, TransferRecord, TransferStatus } from '../ports/TransferRepositoryPort';
import { BalanceProjectionPort } from '../ports/BalanceProjectionPort';

export interface RequestTransferInput {
  transferId: string;
  fromAccount: string;
  toAccount: string;
  amount: number;
}

export interface RequestTransferResult {
  transferId: string;
  status: TransferStatus;
}

export class RequestTransfer {
  constructor(
    private readonly transferRepository: TransferRepositoryPort,
    private readonly balanceProjection: BalanceProjectionPort,
  ) {}

  async execute(input: RequestTransferInput): Promise<RequestTransferResult> {
    const existing = await this.transferRepository.findById(input.transferId);
    if (existing !== null) {
      return { transferId: existing.id, status: existing.status };
    }

    const confirmedBalance = await this.balanceProjection.getConfirmedBalance(input.fromAccount);
    const pendingReservationsTotal = await this.balanceProjection.getPendingReservationsTotal(input.fromAccount);
    const availableBalance = calculateAvailableBalance(confirmedBalance, pendingReservationsTotal);
    const decision = decideApproval({ availableBalance, amount: input.amount });

    if (!decision.approved) {
      const rejected: TransferRecord = {
        id: input.transferId,
        fromAccount: input.fromAccount,
        toAccount: input.toAccount,
        amount: input.amount,
        status: 'rejected',
      };
      await this.transferRepository.saveRejected(rejected);
      return { transferId: rejected.id, status: 'rejected' };
    }

    const approved: TransferRecord = {
      id: input.transferId,
      fromAccount: input.fromAccount,
      toAccount: input.toAccount,
      amount: input.amount,
      status: 'approved',
    };
    const outboxPayload = {
      transferId: input.transferId,
      fromAccount: input.fromAccount,
      toAccount: input.toAccount,
      amount: input.amount,
    };
    await this.transferRepository.saveApproved(
      approved,
      { accountId: input.fromAccount, amount: input.amount },
      outboxPayload,
    );
    return { transferId: approved.id, status: 'approved' };
  }
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `npx vitest run tests/application/RequestTransfer.test.ts`
Expected: PASS (4 tests)

- [ ] **Step 7: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-5-authorization-application
git add services/authorization/src/application services/authorization/tests/application services/authorization/tests/fakes
git commit -m "Implement the RequestTransfer use case"
git push -u origin task-5-authorization-application
```

---

## Task 6: Authorization Postgres adapters

**Files:**
- Create: `services/authorization/src/adapters/outbound/postgres/PostgresTransferRepository.ts`
- Create: `services/authorization/src/adapters/outbound/postgres/PostgresBalanceProjection.ts`
- Test: `services/authorization/tests/adapters/PostgresTransferRepository.test.ts`
- Test: `services/authorization/tests/adapters/PostgresBalanceProjection.test.ts`

**Interfaces:**
- Consumes: `TransferRepositoryPort`, `BalanceProjectionPort` from Task 5.
- Produces: `PostgresTransferRepository`, `PostgresBalanceProjection`, both constructed with `(pool: Pool)` where `pool` already has `search_path=authz` set.

- [ ] **Step 1: Write the failing adapter tests**

```ts
// tests/adapters/PostgresTransferRepository.test.ts
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgresTransferRepository } from '../../src/adapters/outbound/postgres/PostgresTransferRepository';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';

let pool: Pool;
let repository: PostgresTransferRepository;

beforeAll(() => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=authz' });
  repository = new PostgresTransferRepository(pool);
});

beforeEach(async () => {
  await pool.query('TRUNCATE transfers, reservations, outbox CASCADE');
});

afterAll(async () => {
  await pool.end();
});

describe('PostgresTransferRepository', () => {
  it('returns null when the transfer does not exist', async () => {
    expect(await repository.findById('missing')).toBeNull();
  });

  it('saves an approved transfer, its reservation, and the outbox row atomically', async () => {
    await repository.saveApproved(
      { id: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500, status: 'approved' },
      { accountId: 'acc_1', amount: 500 },
      { transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 },
    );

    const transfer = await repository.findById('t1');
    expect(transfer).toEqual({ id: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500, status: 'approved' });

    const reservation = await pool.query("SELECT * FROM reservations WHERE transfer_id = 't1'");
    expect(reservation.rows).toHaveLength(1);
    expect(reservation.rows[0].status).toBe('pending');

    const outbox = await pool.query("SELECT * FROM outbox WHERE topic = 'transfer-authorized'");
    expect(outbox.rows).toHaveLength(1);
    expect(outbox.rows[0].published_at).toBeNull();
  });

  it('saves a rejected transfer without a reservation or outbox row', async () => {
    await repository.saveRejected({ id: 't2', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 99999, status: 'rejected' });

    const transfer = await repository.findById('t2');
    expect(transfer?.status).toBe('rejected');

    const reservation = await pool.query("SELECT * FROM reservations WHERE transfer_id = 't2'");
    expect(reservation.rows).toHaveLength(0);
  });
});
```

```ts
// tests/adapters/PostgresBalanceProjection.test.ts
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgresBalanceProjection } from '../../src/adapters/outbound/postgres/PostgresBalanceProjection';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';

let pool: Pool;
let balanceProjection: PostgresBalanceProjection;

beforeAll(() => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=authz' });
  balanceProjection = new PostgresBalanceProjection(pool);
});

beforeEach(async () => {
  await pool.query('TRUNCATE balance_projection, reservations, transfers CASCADE');
  await pool.query("INSERT INTO balance_projection (account_id, confirmed_balance) VALUES ('acc_1', 1000)");
});

afterAll(async () => {
  await pool.end();
});

describe('PostgresBalanceProjection', () => {
  it('returns the confirmed balance for a known account', async () => {
    expect(await balanceProjection.getConfirmedBalance('acc_1')).toBe(1000);
  });

  it('returns 0 for an unknown account', async () => {
    expect(await balanceProjection.getConfirmedBalance('acc_unknown')).toBe(0);
  });

  it('sums only pending reservations for the account', async () => {
    await pool.query("INSERT INTO transfers (id, from_account, to_account, amount, status) VALUES ('t1', 'acc_1', 'acc_2', 100, 'approved')");
    await pool.query("INSERT INTO transfers (id, from_account, to_account, amount, status) VALUES ('t2', 'acc_1', 'acc_2', 50, 'confirmed')");
    await pool.query("INSERT INTO reservations (transfer_id, account_id, amount, status) VALUES ('t1', 'acc_1', 100, 'pending')");
    await pool.query("INSERT INTO reservations (transfer_id, account_id, amount, status) VALUES ('t2', 'acc_1', 50, 'released')");

    expect(await balanceProjection.getPendingReservationsTotal('acc_1')).toBe(100);
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `DATABASE_URL=postgres://postgres:postgres@localhost:5432/settlement_ledger npm run migrate && npx vitest run tests/adapters`
Expected: FAIL — adapter modules don't exist.

- [ ] **Step 3: Implement the adapters**

```ts
// src/adapters/outbound/postgres/PostgresTransferRepository.ts
import { Pool } from 'pg';
import { TransferRepositoryPort, TransferRecord, ReservationInput } from '../../../application/ports/TransferRepositoryPort';

export class PostgresTransferRepository implements TransferRepositoryPort {
  constructor(private readonly pool: Pool) {}

  async findById(id: string): Promise<TransferRecord | null> {
    const result = await this.pool.query(
      'SELECT id, from_account, to_account, amount, status FROM transfers WHERE id = $1',
      [id],
    );
    if (result.rows.length === 0) {
      return null;
    }
    const row = result.rows[0];
    return {
      id: row.id,
      fromAccount: row.from_account,
      toAccount: row.to_account,
      amount: Number(row.amount),
      status: row.status,
    };
  }

  async saveApproved(transfer: TransferRecord, reservation: ReservationInput, outboxPayload: unknown): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        'INSERT INTO transfers (id, from_account, to_account, amount, status) VALUES ($1, $2, $3, $4, $5)',
        [transfer.id, transfer.fromAccount, transfer.toAccount, transfer.amount, transfer.status],
      );
      await client.query(
        "INSERT INTO reservations (transfer_id, account_id, amount, status) VALUES ($1, $2, $3, 'pending')",
        [transfer.id, reservation.accountId, reservation.amount],
      );
      await client.query(
        "INSERT INTO outbox (topic, payload) VALUES ('transfer-authorized', $1)",
        [JSON.stringify(outboxPayload)],
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async saveRejected(transfer: TransferRecord): Promise<void> {
    await this.pool.query(
      'INSERT INTO transfers (id, from_account, to_account, amount, status) VALUES ($1, $2, $3, $4, $5)',
      [transfer.id, transfer.fromAccount, transfer.toAccount, transfer.amount, transfer.status],
    );
  }
}
```

```ts
// src/adapters/outbound/postgres/PostgresBalanceProjection.ts
import { Pool } from 'pg';
import { BalanceProjectionPort } from '../../../application/ports/BalanceProjectionPort';

export class PostgresBalanceProjection implements BalanceProjectionPort {
  constructor(private readonly pool: Pool) {}

  async getConfirmedBalance(accountId: string): Promise<number> {
    const result = await this.pool.query('SELECT confirmed_balance FROM balance_projection WHERE account_id = $1', [accountId]);
    if (result.rows.length === 0) {
      return 0;
    }
    return Number(result.rows[0].confirmed_balance);
  }

  async getPendingReservationsTotal(accountId: string): Promise<number> {
    const result = await this.pool.query(
      "SELECT COALESCE(SUM(amount), 0) AS total FROM reservations WHERE account_id = $1 AND status = 'pending'",
      [accountId],
    );
    return Number(result.rows[0].total);
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/adapters`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-6-authorization-postgres-adapters
git add services/authorization/src/adapters/outbound/postgres services/authorization/tests/adapters
git commit -m "Implement authorization Postgres adapters"
git push -u origin task-6-authorization-postgres-adapters
```

---

## Task 7: Authorization HTTP routes — POST /transfers, GET /transfers/:transferId

**Files:**
- Create: `services/authorization/src/adapters/inbound/http/transfersRouter.ts`
- Modify: `services/authorization/src/adapters/inbound/http/app.ts`
- Test: `services/authorization/tests/http/transfersRouter.test.ts`

**Interfaces:**
- Consumes: `RequestTransfer` (Task 5), `TransferRepositoryPort` (Task 5).
- Produces: `createTransfersRouter(requestTransfer, transferRepository): Router`. `AppDependencies` gains `requestTransfer: RequestTransfer` and `transferRepository: TransferRepositoryPort`.

- [ ] **Step 1: Write the failing test**

```ts
// tests/http/transfersRouter.test.ts
import { describe, it, expect } from 'vitest';
import request from 'supertest';
import { createApp } from '../../src/adapters/inbound/http/app';
import { RequestTransfer } from '../../src/application/use-cases/RequestTransfer';
import { FakeTransferRepository } from '../fakes/FakeTransferRepository';
import { FakeBalanceProjection } from '../fakes/FakeBalanceProjection';

function buildApp() {
  const transferRepository = new FakeTransferRepository();
  const balanceProjection = new FakeBalanceProjection({ acc_1: 1000 });
  const requestTransfer = new RequestTransfer(transferRepository, balanceProjection);
  const app = createApp({ requestTransfer, transferRepository });
  return app;
}

describe('transfers HTTP routes', () => {
  it('creates an approved transfer', async () => {
    const app = buildApp();
    const response = await request(app)
      .post('/transfers')
      .send({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });

    expect(response.status).toBe(201);
    expect(response.body).toEqual({ transferId: 't1', status: 'approved' });
  });

  it('rejects an invalid request body', async () => {
    const app = buildApp();
    const response = await request(app).post('/transfers').send({ transferId: 't1' });
    expect(response.status).toBe(400);
  });

  it('returns 404 for an unknown transferId', async () => {
    const app = buildApp();
    const response = await request(app).get('/transfers/missing');
    expect(response.status).toBe(404);
  });

  it('returns the transfer after creation', async () => {
    const app = buildApp();
    await request(app).post('/transfers').send({ transferId: 't2', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 200 });
    const response = await request(app).get('/transfers/t2');
    expect(response.status).toBe(200);
    expect(response.body).toEqual({ id: 't2', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 200, status: 'approved' });
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/http/transfersRouter.test.ts`
Expected: FAIL — `transfersRouter` does not exist and `AppDependencies` doesn't accept these fields.

- [ ] **Step 3: Implement the router**

```ts
// src/adapters/inbound/http/transfersRouter.ts
import { Router, Request, Response, NextFunction } from 'express';
import { z } from 'zod';
import { RequestTransfer } from '../../../application/use-cases/RequestTransfer';
import { TransferRepositoryPort } from '../../../application/ports/TransferRepositoryPort';

const requestTransferSchema = z.object({
  transferId: z.string().min(1),
  fromAccount: z.string().min(1),
  toAccount: z.string().min(1),
  amount: z.number().positive(),
});

export function createTransfersRouter(
  requestTransfer: RequestTransfer,
  transferRepository: TransferRepositoryPort,
): Router {
  const router = Router();

  router.post('/transfers', async (req: Request, res: Response, next: NextFunction) => {
    const parseResult = requestTransferSchema.safeParse(req.body);
    if (!parseResult.success) {
      res.status(400).json({ error: 'invalid request body', details: parseResult.error.issues });
      return;
    }
    try {
      const result = await requestTransfer.execute(parseResult.data);
      res.status(201).json(result);
    } catch (error) {
      next(error);
    }
  });

  router.get('/transfers/:transferId', async (req: Request, res: Response, next: NextFunction) => {
    try {
      const transfer = await transferRepository.findById(req.params.transferId);
      if (transfer === null) {
        res.status(404).json({ error: 'transfer not found' });
        return;
      }
      res.status(200).json(transfer);
    } catch (error) {
      next(error);
    }
  });

  return router;
}
```

- [ ] **Step 4: Wire the router into app.ts**

```ts
// src/adapters/inbound/http/app.ts
import express, { Express, Request, Response, NextFunction } from 'express';
import { RequestTransfer } from '../../../application/use-cases/RequestTransfer';
import { TransferRepositoryPort } from '../../../application/ports/TransferRepositoryPort';
import { createTransfersRouter } from './transfersRouter';

export interface AppDependencies {
  requestTransfer?: RequestTransfer;
  transferRepository?: TransferRepositoryPort;
}

export function createApp(deps: AppDependencies): Express {
  const app = express();
  app.use(express.json());

  app.get('/health', (_req: Request, res: Response) => {
    res.status(200).json({ status: 'ok' });
  });

  if (deps.requestTransfer && deps.transferRepository) {
    app.use(createTransfersRouter(deps.requestTransfer, deps.transferRepository));
  }

  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof SyntaxError && 'status' in err && (err as { status?: number }).status === 400) {
      res.status(400).json({ error: 'invalid JSON body' });
      return;
    }
    res.status(500).json({ error: 'internal server error' });
  });

  return app;
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `npx vitest run tests/http/transfersRouter.test.ts tests/health.test.ts`
Expected: PASS

- [ ] **Step 6: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-7-authorization-http-routes
git add services/authorization/src/adapters/inbound/http services/authorization/tests/http
git commit -m "Add POST /transfers and GET /transfers/:transferId routes"
git push -u origin task-7-authorization-http-routes
```

---

## Task 8: Authorization outbox poller and Kafka producer

**Files:**
- Create: `services/authorization/src/application/ports/OutboxRepositoryPort.ts`
- Create: `services/authorization/src/application/ports/KafkaProducerPort.ts`
- Create: `services/authorization/src/application/services/OutboxPoller.ts`
- Create: `services/authorization/src/adapters/outbound/postgres/PostgresOutboxRepository.ts`
- Create: `services/authorization/src/adapters/outbound/kafka/KafkaProducerAdapter.ts`
- Modify: `services/authorization/src/main.ts`
- Test: `services/authorization/tests/adapters/outboxPoller.integration.test.ts`

**Interfaces:**
- Produces: `OutboxRepositoryPort` (`findUnpublished(limit)`, `markPublished(id)`), `KafkaProducerPort` (`publish(topic, key, payload)`), `OutboxPoller` (`start()`, `stop()`, `pollOnce()`), `KafkaProducerAdapter`, `createKafka(brokers, clientId)`.

- [ ] **Step 1: Write the ports**

```ts
// src/application/ports/OutboxRepositoryPort.ts
export interface OutboxRow {
  id: string;
  topic: string;
  payload: unknown;
}

export interface OutboxRepositoryPort {
  findUnpublished(limit: number): Promise<OutboxRow[]>;
  markPublished(id: string): Promise<void>;
}
```

```ts
// src/application/ports/KafkaProducerPort.ts
export interface KafkaProducerPort {
  publish(topic: string, key: string, payload: unknown): Promise<void>;
}
```

- [ ] **Step 2: Write the OutboxPoller**

```ts
// src/application/services/OutboxPoller.ts
import { OutboxRepositoryPort } from '../ports/OutboxRepositoryPort';
import { KafkaProducerPort } from '../ports/KafkaProducerPort';

export class OutboxPoller {
  private timer: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly outboxRepository: OutboxRepositoryPort,
    private readonly producer: KafkaProducerPort,
    private readonly intervalMs: number = 500,
  ) {}

  start(): void {
    this.timer = setInterval(() => {
      this.pollOnce().catch((error) => console.error('outbox poll failed', error));
    }, this.intervalMs);
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async pollOnce(): Promise<void> {
    const rows = await this.outboxRepository.findUnpublished(20);
    for (const row of rows) {
      const key = this.extractKey(row);
      await this.producer.publish(row.topic, key, row.payload);
      await this.outboxRepository.markPublished(row.id);
    }
  }

  private extractKey(row: { id: string; payload: unknown }): string {
    if (typeof row.payload === 'object' && row.payload !== null && 'transferId' in row.payload) {
      const value = (row.payload as { transferId: unknown }).transferId;
      if (typeof value === 'string') {
        return value;
      }
    }
    return row.id;
  }
}
```

- [ ] **Step 3: Write the failing integration test**

```ts
// tests/adapters/outboxPoller.integration.test.ts
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { Pool } from 'pg';
import { Kafka } from 'kafkajs';
import { OutboxPoller } from '../../src/application/services/OutboxPoller';
import { PostgresOutboxRepository } from '../../src/adapters/outbound/postgres/PostgresOutboxRepository';
import { KafkaProducerAdapter, createKafka } from '../../src/adapters/outbound/kafka/KafkaProducerAdapter';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';
const KAFKA_BROKERS = (process.env.TEST_KAFKA_BROKERS ?? 'localhost:9092').split(',');
const TOPIC = 'transfer-authorized';

let pool: Pool;
let kafka: Kafka;
let poller: OutboxPoller;

beforeAll(async () => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=authz' });
  kafka = createKafka(KAFKA_BROKERS, 'authorization-test');
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: 1 }] });
  await admin.disconnect();

  const producer = kafka.producer();
  await producer.connect();
  poller = new OutboxPoller(new PostgresOutboxRepository(pool), new KafkaProducerAdapter(producer));
}, 30000);

beforeEach(async () => {
  await pool.query('TRUNCATE outbox CASCADE');
});

afterAll(async () => {
  await pool.end();
});

describe('OutboxPoller (integration)', () => {
  it('publishes an unpublished outbox row to Kafka and marks it published', async () => {
    await pool.query(
      "INSERT INTO outbox (topic, payload) VALUES ($1, $2)",
      [TOPIC, JSON.stringify({ transferId: 'ob1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 100 })],
    );

    const consumer = kafka.consumer({ groupId: `outbox-test-${Date.now()}` });
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

    const received: unknown[] = [];
    await consumer.run({
      eachMessage: async ({ message }) => {
        received.push(JSON.parse(message.value?.toString() ?? '{}'));
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 5000));

    await poller.pollOnce();

    const start = Date.now();
    while (received.length === 0 && Date.now() - start < 10000) {
      await new Promise((resolve) => setTimeout(resolve, 200));
    }

    expect(received).toEqual([{ transferId: 'ob1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 100 }]);

    const row = await pool.query("SELECT published_at FROM outbox WHERE topic = $1", [TOPIC]);
    expect(row.rows[0].published_at).not.toBeNull();

    await consumer.disconnect();
  }, 30000);
});
```

- [ ] **Step 4: Run test to verify it fails**

Run: `npx vitest run tests/adapters/outboxPoller.integration.test.ts`
Expected: FAIL — `PostgresOutboxRepository` and `KafkaProducerAdapter` don't exist.

- [ ] **Step 5: Implement the adapters**

```ts
// src/adapters/outbound/postgres/PostgresOutboxRepository.ts
import { Pool } from 'pg';
import { OutboxRepositoryPort, OutboxRow } from '../../../application/ports/OutboxRepositoryPort';

export class PostgresOutboxRepository implements OutboxRepositoryPort {
  constructor(private readonly pool: Pool) {}

  async findUnpublished(limit: number): Promise<OutboxRow[]> {
    const result = await this.pool.query(
      'SELECT id, topic, payload FROM outbox WHERE published_at IS NULL ORDER BY created_at ASC LIMIT $1',
      [limit],
    );
    return result.rows.map((row) => ({ id: row.id, topic: row.topic, payload: row.payload }));
  }

  async markPublished(id: string): Promise<void> {
    await this.pool.query('UPDATE outbox SET published_at = now() WHERE id = $1', [id]);
  }
}
```

```ts
// src/adapters/outbound/kafka/KafkaProducerAdapter.ts
import { Kafka, Producer } from 'kafkajs';
import { KafkaProducerPort } from '../../../application/ports/KafkaProducerPort';

export function createKafka(brokers: string[], clientId: string): Kafka {
  return new Kafka({ clientId, brokers });
}

export class KafkaProducerAdapter implements KafkaProducerPort {
  constructor(private readonly producer: Producer) {}

  async publish(topic: string, key: string, payload: unknown): Promise<void> {
    await this.producer.send({ topic, messages: [{ key, value: JSON.stringify(payload) }] });
  }
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `npx vitest run tests/adapters/outboxPoller.integration.test.ts`
Expected: PASS

- [ ] **Step 7: Wire the poller into main.ts**

```ts
// src/main.ts
import { Pool } from 'pg';
import { loadEnv } from './config/env';
import { createApp } from './adapters/inbound/http/app';
import { createKafka, KafkaProducerAdapter } from './adapters/outbound/kafka/KafkaProducerAdapter';
import { PostgresTransferRepository } from './adapters/outbound/postgres/PostgresTransferRepository';
import { PostgresBalanceProjection } from './adapters/outbound/postgres/PostgresBalanceProjection';
import { PostgresOutboxRepository } from './adapters/outbound/postgres/PostgresOutboxRepository';
import { RequestTransfer } from './application/use-cases/RequestTransfer';
import { OutboxPoller } from './application/services/OutboxPoller';

async function main(): Promise<void> {
  const env = loadEnv();
  const pool = new Pool({ connectionString: env.databaseUrl, options: '-c search_path=authz' });

  const kafka = createKafka(env.kafkaBrokers, 'authorization');
  const producer = kafka.producer();
  await producer.connect();

  const transferRepository = new PostgresTransferRepository(pool);
  const balanceProjection = new PostgresBalanceProjection(pool);
  const outboxRepository = new PostgresOutboxRepository(pool);
  const requestTransfer = new RequestTransfer(transferRepository, balanceProjection);

  const outboxPoller = new OutboxPoller(outboxRepository, new KafkaProducerAdapter(producer));
  outboxPoller.start();

  const app = createApp({ requestTransfer, transferRepository });
  app.listen(env.port, () => {
    console.log(`authorization listening on port ${env.port}`);
  });
}

main().catch((error) => {
  console.error('fatal startup error', error);
  process.exit(1);
});
```

- [ ] **Step 8: Typecheck and lint**

Run: `npm run typecheck && npm run lint`
Expected: both clean.

- [ ] **Step 9: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-8-authorization-outbox-poller
git add services/authorization/src
git commit -m "Add the authorization outbox poller and Kafka producer"
git push -u origin task-8-authorization-outbox-poller
```

---

## Task 9: Scaffold the ledger service

**Files:**
- Create: `services/ledger/package.json`, `tsconfig.json`, `.eslintrc.cjs`, `vitest.config.mts`, `.dockerignore`, `Dockerfile`
- Create: `services/ledger/src/config/env.ts`
- Create: `services/ledger/src/adapters/inbound/http/app.ts`
- Create: `services/ledger/src/main.ts`
- Test: `services/ledger/tests/health.test.ts`

**Interfaces:**
- Produces: identical shapes to Task 2, renamed for `ledger` (`name: "ledger"` in package.json, `PORT` default `3002`, `EXPOSE 3002` in the Dockerfile).

- [ ] **Step 1: package.json** — identical to Task 2's, with `"name": "ledger"`.

- [ ] **Step 2: tsconfig.json** — identical to Task 2's (`include: ["src", "db", "tests"]`).

- [ ] **Step 3: .eslintrc.cjs** — identical to Task 2's.

- [ ] **Step 4: vitest.config.mts** — identical to Task 2's.

- [ ] **Step 5: .dockerignore** — identical to Task 2's. **Dockerfile** — identical except `EXPOSE 3002`.

- [ ] **Step 6: config/env.ts**

```ts
// src/config/env.ts
export interface AppEnv {
  port: number;
  databaseUrl: string;
  kafkaBrokers: string[];
}

export function loadEnv(): AppEnv {
  const rawPort = process.env.PORT;
  const port = rawPort === undefined ? 3002 : Number(rawPort);
  if (Number.isNaN(port)) {
    throw new Error(`Invalid PORT environment variable: "${rawPort}" is not a number`);
  }

  return {
    port,
    databaseUrl: process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger',
    kafkaBrokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  };
}
```

- [ ] **Step 7: Write the failing health-check test** — identical in shape to Task 2's `tests/health.test.ts`, importing from `../src/adapters/inbound/http/app`.

- [ ] **Step 8: Run test to verify it fails**

Run: `cd services/ledger && npm install && npx vitest run tests/health.test.ts`
Expected: FAIL.

- [ ] **Step 9: Implement app.ts and main.ts** — identical in shape to Task 2's, with the log line `ledger listening on port ${env.port}`.

- [ ] **Step 10: Run test to verify it passes**

Run: `npx vitest run tests/health.test.ts`
Expected: PASS

- [ ] **Step 11: Typecheck and lint**

Run: `npm run typecheck && npm run lint`
Expected: both clean.

- [ ] **Step 12: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-9-ledger-scaffold
git add services/ledger
git commit -m "Scaffold the ledger service"
git push -u origin task-9-ledger-scaffold
```

---

## Task 10: Ledger domain — double-entry invariant

**Files:**
- Create: `services/ledger/src/domain/services/DoubleEntry.ts`
- Test: `services/ledger/tests/domain/DoubleEntry.test.ts`

**Interfaces:**
- Produces: `LedgerEntryInput { accountId: string; direction: 'debit' | 'credit'; amount: number }`; `buildDoubleEntryLines(input: { transferId: string; fromAccount: string; toAccount: string; amount: number }): LedgerEntryInput[]`; `isBalanced(entries: LedgerEntryInput[]): boolean`.

- [ ] **Step 1: Write the failing test**

```ts
// tests/domain/DoubleEntry.test.ts
import { describe, it, expect } from 'vitest';
import { buildDoubleEntryLines, isBalanced } from '../../src/domain/services/DoubleEntry';

describe('buildDoubleEntryLines', () => {
  it('produces exactly one debit line on the source account and one credit line on the destination account', () => {
    const lines = buildDoubleEntryLines({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });
    expect(lines).toEqual([
      { accountId: 'acc_1', direction: 'debit', amount: 500 },
      { accountId: 'acc_2', direction: 'credit', amount: 500 },
    ]);
  });
});

describe('isBalanced', () => {
  it('returns true when total debits equal total credits', () => {
    const lines = buildDoubleEntryLines({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });
    expect(isBalanced(lines)).toBe(true);
  });

  it('returns false when debits and credits do not match', () => {
    const lines = [
      { accountId: 'acc_1', direction: 'debit' as const, amount: 500 },
      { accountId: 'acc_2', direction: 'credit' as const, amount: 400 },
    ];
    expect(isBalanced(lines)).toBe(false);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/domain/DoubleEntry.test.ts`
Expected: FAIL.

- [ ] **Step 3: Implement**

```ts
// src/domain/services/DoubleEntry.ts
export interface LedgerEntryInput {
  accountId: string;
  direction: 'debit' | 'credit';
  amount: number;
}

export function buildDoubleEntryLines(input: {
  transferId: string;
  fromAccount: string;
  toAccount: string;
  amount: number;
}): LedgerEntryInput[] {
  return [
    { accountId: input.fromAccount, direction: 'debit', amount: input.amount },
    { accountId: input.toAccount, direction: 'credit', amount: input.amount },
  ];
}

export function isBalanced(entries: LedgerEntryInput[]): boolean {
  const totalDebit = entries.filter((entry) => entry.direction === 'debit').reduce((sum, entry) => sum + entry.amount, 0);
  const totalCredit = entries.filter((entry) => entry.direction === 'credit').reduce((sum, entry) => sum + entry.amount, 0);
  return totalDebit === totalCredit;
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/domain/DoubleEntry.test.ts`
Expected: PASS

- [ ] **Step 5: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-10-ledger-domain
git add services/ledger/src/domain services/ledger/tests/domain
git commit -m "Implement the double-entry invariant"
git push -u origin task-10-ledger-domain
```

---

## Task 11: Ledger database migrations

**Files:**
- Create: `services/ledger/db/migrate.ts` (identical to Task 4's, with `SCHEMA = 'ledger'`)
- Create: `services/ledger/db/migrations/001_create_ledger_entries.sql`
- Create: `services/ledger/db/migrations/002_create_processed_transfers.sql`
- Create: `services/ledger/db/migrations/003_create_outbox.sql`
- Create: `services/ledger/db/migrations/004_create_dlq_events.sql`

**Interfaces:**
- Produces: tables `ledger_entries`, `processed_transfers`, `outbox`, `dlq_events` inside the `ledger` schema.

- [ ] **Step 1: Write db/migrate.ts** — identical to Task 4's `db/migrate.ts`, with `const SCHEMA = 'ledger';`.

- [ ] **Step 2: Write the migrations**

```sql
-- db/migrations/001_create_ledger_entries.sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS ledger_entries (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  transfer_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  direction TEXT NOT NULL,
  amount NUMERIC NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ledger_entries_account_id ON ledger_entries(account_id);
```

```sql
-- db/migrations/002_create_processed_transfers.sql
CREATE TABLE IF NOT EXISTS processed_transfers (
  transfer_id TEXT PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

```sql
-- db/migrations/003_create_outbox.sql
CREATE TABLE IF NOT EXISTS outbox (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  topic TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);
```

```sql
-- db/migrations/004_create_dlq_events.sql
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
```

- [ ] **Step 3: Run the migration and verify**

Run: `cd services/ledger && DATABASE_URL=postgres://postgres:postgres@localhost:5432/settlement_ledger npm run migrate`
Expected: prints each applied migration filename.

- [ ] **Step 4: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-11-ledger-migrations
git add services/ledger/db
git commit -m "Add ledger database migrations"
git push -u origin task-11-ledger-migrations
```

---

## Task 12: Ledger application layer — PostSettlement use case

**Files:**
- Create: `services/ledger/src/domain/services/TransferAuthorizedValidator.ts`
- Create: `services/ledger/src/application/ports/LedgerRepositoryPort.ts`
- Create: `services/ledger/src/application/ports/DlqRepositoryPort.ts`
- Create: `services/ledger/src/application/use-cases/PostSettlement.ts`
- Create: `services/ledger/tests/fakes/FakeLedgerRepository.ts`
- Create: `services/ledger/tests/fakes/FakeDlqRepository.ts`
- Test: `services/ledger/tests/application/PostSettlement.test.ts`

**Interfaces:**
- Consumes: `buildDoubleEntryLines`, `isBalanced` from Task 10. Parses the `transfer-authorized` payload shape produced in Task 5: `{ transferId, fromAccount, toAccount, amount }`.
- Produces: `validateTransferAuthorized(raw: unknown): TransferAuthorizedEvent`; `LedgerRepositoryPort` (`wasProcessed`, `postSettlement`, `listEntriesForAccount`); `DlqRepositoryPort` (`add`, `list`, `get`, `markReprocessed` — the shared shape used identically in all three services, see Task 14); `PostSettlement.execute(rawPayload: unknown): Promise<void>`, constructor options `{ maxAttempts?, backoffMs? }`. The outbox payload `postSettlement` writes has shape `{ transferId, fromAccount, toAccount, amount, postedAt }` — this is the exact shape Task 16 (authorization) and Task 22 (payout) must parse from `settlement-posted`.

- [ ] **Step 1: Write the zod validator**

```ts
// src/domain/services/TransferAuthorizedValidator.ts
import { z } from 'zod';

const transferAuthorizedSchema = z.object({
  transferId: z.string().min(1),
  fromAccount: z.string().min(1),
  toAccount: z.string().min(1),
  amount: z.number().positive(),
});

export type TransferAuthorizedEvent = z.infer<typeof transferAuthorizedSchema>;

export function validateTransferAuthorized(raw: unknown): TransferAuthorizedEvent {
  return transferAuthorizedSchema.parse(raw);
}
```

- [ ] **Step 2: Write the ports**

```ts
// src/application/ports/LedgerRepositoryPort.ts
export interface LedgerEntryRecord {
  id: string;
  transferId: string;
  accountId: string;
  direction: 'debit' | 'credit';
  amount: number;
  createdAt: Date;
}

export interface LedgerRepositoryPort {
  wasProcessed(transferId: string): Promise<boolean>;
  postSettlement(input: { transferId: string; fromAccount: string; toAccount: string; amount: number }): Promise<void>;
  listEntriesForAccount(accountId: string): Promise<LedgerEntryRecord[]>;
}
```

```ts
// src/application/ports/DlqRepositoryPort.ts
export interface DlqEntry {
  id: string;
  transferId: string;
  topic: string;
  payload: unknown;
  failureReason: string;
  attempts: number;
  createdAt: Date;
  reprocessedAt: Date | null;
}

export interface DlqRepositoryPort {
  add(entry: { transferId: string; topic: string; payload: unknown; failureReason: string; attempts: number }): Promise<string>;
  list(options: { limit: number; offset: number }): Promise<DlqEntry[]>;
  get(id: string): Promise<DlqEntry | null>;
  markReprocessed(id: string): Promise<void>;
}
```

- [ ] **Step 3: Write the fakes**

```ts
// tests/fakes/FakeLedgerRepository.ts
import { LedgerRepositoryPort, LedgerEntryRecord } from '../../src/application/ports/LedgerRepositoryPort';

export class FakeLedgerRepository implements LedgerRepositoryPort {
  public readonly processed = new Set<string>();
  public postSettlementCalls = 0;

  async wasProcessed(transferId: string): Promise<boolean> {
    return this.processed.has(transferId);
  }

  async postSettlement(input: { transferId: string; fromAccount: string; toAccount: string; amount: number }): Promise<void> {
    this.postSettlementCalls += 1;
    this.processed.add(input.transferId);
  }

  async listEntriesForAccount(_accountId: string): Promise<LedgerEntryRecord[]> {
    return [];
  }
}
```

```ts
// tests/fakes/FakeDlqRepository.ts
import { DlqRepositoryPort, DlqEntry } from '../../src/application/ports/DlqRepositoryPort';

export class FakeDlqRepository implements DlqRepositoryPort {
  public readonly entries: DlqEntry[] = [];

  async add(entry: { transferId: string; topic: string; payload: unknown; failureReason: string; attempts: number }): Promise<string> {
    const id = `dlq-${this.entries.length + 1}`;
    this.entries.push({ id, ...entry, createdAt: new Date(), reprocessedAt: null });
    return id;
  }

  async list(): Promise<DlqEntry[]> {
    return this.entries;
  }

  async get(id: string): Promise<DlqEntry | null> {
    return this.entries.find((entry) => entry.id === id) ?? null;
  }

  async markReprocessed(id: string): Promise<void> {
    const entry = this.entries.find((e) => e.id === id);
    if (entry) {
      entry.reprocessedAt = new Date();
    }
  }
}
```

- [ ] **Step 4: Write the failing test**

```ts
// tests/application/PostSettlement.test.ts
import { describe, it, expect } from 'vitest';
import { PostSettlement } from '../../src/application/use-cases/PostSettlement';
import { FakeLedgerRepository } from '../fakes/FakeLedgerRepository';
import { FakeDlqRepository } from '../fakes/FakeDlqRepository';

describe('PostSettlement', () => {
  it('posts the settlement for a valid transfer-authorized payload', async () => {
    const ledgerRepository = new FakeLedgerRepository();
    const dlqRepository = new FakeDlqRepository();
    const useCase = new PostSettlement(ledgerRepository, dlqRepository);

    await useCase.execute({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });

    expect(ledgerRepository.postSettlementCalls).toBe(1);
    expect(dlqRepository.entries).toHaveLength(0);
  });

  it('skips reprocessing a transferId that was already processed', async () => {
    const ledgerRepository = new FakeLedgerRepository();
    ledgerRepository.processed.add('t1');
    const dlqRepository = new FakeDlqRepository();
    const useCase = new PostSettlement(ledgerRepository, dlqRepository);

    await useCase.execute({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });

    expect(ledgerRepository.postSettlementCalls).toBe(0);
  });

  it('sends a malformed payload to the DLQ after exhausting retries', async () => {
    const ledgerRepository = new FakeLedgerRepository();
    const dlqRepository = new FakeDlqRepository();
    const useCase = new PostSettlement(ledgerRepository, dlqRepository, { maxAttempts: 2, backoffMs: () => 1 });

    await useCase.execute({ transferId: 't2', fromAccount: 'acc_1' });

    expect(dlqRepository.entries).toHaveLength(1);
    expect(dlqRepository.entries[0].transferId).toBe('t2');
    expect(dlqRepository.entries[0].topic).toBe('transfer-authorized');
  });
});
```

- [ ] **Step 5: Run test to verify it fails**

Run: `npx vitest run tests/application/PostSettlement.test.ts`
Expected: FAIL.

- [ ] **Step 6: Implement PostSettlement**

```ts
// src/application/use-cases/PostSettlement.ts
import { validateTransferAuthorized } from '../../domain/services/TransferAuthorizedValidator';
import { buildDoubleEntryLines, isBalanced } from '../../domain/services/DoubleEntry';
import { LedgerRepositoryPort } from '../ports/LedgerRepositoryPort';
import { DlqRepositoryPort } from '../ports/DlqRepositoryPort';

export interface PostSettlementOptions {
  maxAttempts?: number;
  backoffMs?: (attempt: number) => number;
}

const DEFAULT_BACKOFF_MS = (attempt: number): number => 100 * 2 ** (attempt - 1);

export class PostSettlement {
  private readonly maxAttempts: number;
  private readonly backoffMs: (attempt: number) => number;

  constructor(
    private readonly ledgerRepository: LedgerRepositoryPort,
    private readonly dlqRepository: DlqRepositoryPort,
    options: PostSettlementOptions = {},
  ) {
    this.maxAttempts = options.maxAttempts ?? 3;
    this.backoffMs = options.backoffMs ?? DEFAULT_BACKOFF_MS;
  }

  async execute(rawPayload: unknown): Promise<void> {
    const transferId = this.extractTransferId(rawPayload);
    if (transferId !== null && (await this.ledgerRepository.wasProcessed(transferId))) {
      return;
    }

    let lastError: Error = new Error('unknown processing error');

    for (let attempt = 1; attempt <= this.maxAttempts; attempt += 1) {
      try {
        const event = validateTransferAuthorized(rawPayload);
        const lines = buildDoubleEntryLines(event);
        if (!isBalanced(lines)) {
          throw new Error('double-entry lines are not balanced');
        }
        await this.ledgerRepository.postSettlement(event);
        return;
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        if (attempt < this.maxAttempts) {
          await this.delay(this.backoffMs(attempt));
        }
      }
    }

    await this.dlqRepository.add({
      transferId: transferId ?? 'unknown',
      topic: 'transfer-authorized',
      payload: rawPayload,
      failureReason: lastError.message,
      attempts: this.maxAttempts,
    });
  }

  private extractTransferId(rawPayload: unknown): string | null {
    if (typeof rawPayload === 'object' && rawPayload !== null && 'transferId' in rawPayload) {
      const value = (rawPayload as { transferId: unknown }).transferId;
      return typeof value === 'string' ? value : null;
    }
    return null;
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
```

- [ ] **Step 7: Run test to verify it passes**

Run: `npx vitest run tests/application/PostSettlement.test.ts`
Expected: PASS (3 tests)

- [ ] **Step 8: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-12-ledger-application
git add services/ledger/src/domain/services/TransferAuthorizedValidator.ts services/ledger/src/application services/ledger/tests/application services/ledger/tests/fakes
git commit -m "Implement the PostSettlement use case"
git push -u origin task-12-ledger-application
```

---

## Task 13: Ledger Postgres adapters

**Files:**
- Create: `services/ledger/src/adapters/outbound/postgres/PostgresLedgerRepository.ts`
- Create: `services/ledger/src/adapters/outbound/postgres/PostgresDlqRepository.ts`
- Test: `services/ledger/tests/adapters/PostgresLedgerRepository.test.ts`
- Test: `services/ledger/tests/adapters/PostgresDlqRepository.test.ts`

**Interfaces:**
- Consumes: `LedgerRepositoryPort`, `DlqRepositoryPort` from Task 12.
- Produces: `PostgresLedgerRepository`, `PostgresDlqRepository`, both constructed with `(pool: Pool)`.

- [ ] **Step 1: Write the failing adapter tests**

```ts
// tests/adapters/PostgresLedgerRepository.test.ts
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgresLedgerRepository } from '../../src/adapters/outbound/postgres/PostgresLedgerRepository';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';

let pool: Pool;
let repository: PostgresLedgerRepository;

beforeAll(() => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=ledger' });
  repository = new PostgresLedgerRepository(pool);
});

beforeEach(async () => {
  await pool.query('TRUNCATE ledger_entries, processed_transfers, outbox CASCADE');
});

afterAll(async () => {
  await pool.end();
});

describe('PostgresLedgerRepository', () => {
  it('reports a transferId as not processed before it is posted', async () => {
    expect(await repository.wasProcessed('t1')).toBe(false);
  });

  it('posts a settlement atomically: two ledger entries, a processed_transfers row, and an outbox row', async () => {
    await repository.postSettlement({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });

    expect(await repository.wasProcessed('t1')).toBe(true);

    const entries = await pool.query("SELECT * FROM ledger_entries WHERE transfer_id = 't1' ORDER BY direction");
    expect(entries.rows).toHaveLength(2);
    expect(entries.rows[0]).toMatchObject({ account_id: 'acc_2', direction: 'credit', amount: '500' });
    expect(entries.rows[1]).toMatchObject({ account_id: 'acc_1', direction: 'debit', amount: '500' });

    const outbox = await pool.query("SELECT * FROM outbox WHERE topic = 'settlement-posted'");
    expect(outbox.rows).toHaveLength(1);
    expect(outbox.rows[0].payload).toMatchObject({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });
  });

  it('lists entries for an account in chronological order', async () => {
    await repository.postSettlement({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });
    await repository.postSettlement({ transferId: 't2', fromAccount: 'acc_2', toAccount: 'acc_1', amount: 100 });

    const entries = await repository.listEntriesForAccount('acc_1');
    expect(entries).toHaveLength(2);
    expect(entries[0]).toMatchObject({ transferId: 't1', direction: 'debit', amount: 500 });
    expect(entries[1]).toMatchObject({ transferId: 't2', direction: 'credit', amount: 100 });
  });
});
```

```ts
// tests/adapters/PostgresDlqRepository.test.ts
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgresDlqRepository } from '../../src/adapters/outbound/postgres/PostgresDlqRepository';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';

let pool: Pool;
let repository: PostgresDlqRepository;

beforeAll(() => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=ledger' });
  repository = new PostgresDlqRepository(pool);
});

beforeEach(async () => {
  await pool.query('TRUNCATE dlq_events CASCADE');
});

afterAll(async () => {
  await pool.end();
});

describe('PostgresDlqRepository', () => {
  it('adds and lists an entry', async () => {
    const id = await repository.add({
      transferId: 't1',
      topic: 'transfer-authorized',
      payload: { transferId: 't1' },
      failureReason: 'boom',
      attempts: 3,
    });

    const entries = await repository.list({ limit: 20, offset: 0 });
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({ id, transferId: 't1', topic: 'transfer-authorized', failureReason: 'boom', attempts: 3 });
  });

  it('returns null for a non-UUID id instead of throwing', async () => {
    expect(await repository.get('not-a-uuid')).toBeNull();
  });

  it('marks an entry as reprocessed and excludes it from the default list', async () => {
    const id = await repository.add({
      transferId: 't2',
      topic: 'transfer-authorized',
      payload: {},
      failureReason: 'boom',
      attempts: 3,
    });

    await repository.markReprocessed(id);

    const entry = await repository.get(id);
    expect(entry?.reprocessedAt).not.toBeNull();

    const entries = await repository.list({ limit: 20, offset: 0 });
    expect(entries).toHaveLength(0);
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `DATABASE_URL=postgres://postgres:postgres@localhost:5432/settlement_ledger npm run migrate && npx vitest run tests/adapters`
Expected: FAIL.

- [ ] **Step 3: Implement the adapters**

```ts
// src/adapters/outbound/postgres/PostgresLedgerRepository.ts
import { Pool } from 'pg';
import { LedgerRepositoryPort, LedgerEntryRecord } from '../../../application/ports/LedgerRepositoryPort';

export class PostgresLedgerRepository implements LedgerRepositoryPort {
  constructor(private readonly pool: Pool) {}

  async wasProcessed(transferId: string): Promise<boolean> {
    const result = await this.pool.query('SELECT 1 FROM processed_transfers WHERE transfer_id = $1', [transferId]);
    return result.rows.length > 0;
  }

  async postSettlement(input: { transferId: string; fromAccount: string; toAccount: string; amount: number }): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        "INSERT INTO ledger_entries (transfer_id, account_id, direction, amount) VALUES ($1, $2, 'debit', $3)",
        [input.transferId, input.fromAccount, input.amount],
      );
      await client.query(
        "INSERT INTO ledger_entries (transfer_id, account_id, direction, amount) VALUES ($1, $2, 'credit', $3)",
        [input.transferId, input.toAccount, input.amount],
      );
      await client.query('INSERT INTO processed_transfers (transfer_id) VALUES ($1)', [input.transferId]);
      const outboxPayload = {
        transferId: input.transferId,
        fromAccount: input.fromAccount,
        toAccount: input.toAccount,
        amount: input.amount,
        postedAt: new Date().toISOString(),
      };
      await client.query("INSERT INTO outbox (topic, payload) VALUES ('settlement-posted', $1)", [JSON.stringify(outboxPayload)]);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async listEntriesForAccount(accountId: string): Promise<LedgerEntryRecord[]> {
    const result = await this.pool.query(
      'SELECT id, transfer_id, account_id, direction, amount, created_at FROM ledger_entries WHERE account_id = $1 ORDER BY created_at ASC',
      [accountId],
    );
    return result.rows.map((row) => ({
      id: row.id,
      transferId: row.transfer_id,
      accountId: row.account_id,
      direction: row.direction,
      amount: Number(row.amount),
      createdAt: row.created_at,
    }));
  }
}
```

```ts
// src/adapters/outbound/postgres/PostgresDlqRepository.ts
import { Pool } from 'pg';
import { DlqEntry, DlqRepositoryPort } from '../../../application/ports/DlqRepositoryPort';

interface DlqRow {
  id: string;
  transfer_id: string;
  topic: string;
  payload: unknown;
  failure_reason: string;
  attempts: number;
  created_at: Date;
  reprocessed_at: Date | null;
}

function toDlqEntry(row: DlqRow): DlqEntry {
  return {
    id: row.id,
    transferId: row.transfer_id,
    topic: row.topic,
    payload: row.payload,
    failureReason: row.failure_reason,
    attempts: row.attempts,
    createdAt: row.created_at,
    reprocessedAt: row.reprocessed_at,
  };
}

export class PostgresDlqRepository implements DlqRepositoryPort {
  constructor(private readonly pool: Pool) {}

  async add(entry: { transferId: string; topic: string; payload: unknown; failureReason: string; attempts: number }): Promise<string> {
    const result = await this.pool.query<{ id: string }>(
      `INSERT INTO dlq_events (transfer_id, topic, payload, failure_reason, attempts)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id`,
      [entry.transferId, entry.topic, JSON.stringify(entry.payload), entry.failureReason, entry.attempts],
    );
    return result.rows[0].id;
  }

  async list(options: { limit: number; offset: number }): Promise<DlqEntry[]> {
    const result = await this.pool.query<DlqRow>(
      'SELECT * FROM dlq_events WHERE reprocessed_at IS NULL ORDER BY created_at ASC LIMIT $1 OFFSET $2',
      [options.limit, options.offset],
    );
    return result.rows.map(toDlqEntry);
  }

  async get(id: string): Promise<DlqEntry | null> {
    try {
      const result = await this.pool.query<DlqRow>('SELECT * FROM dlq_events WHERE id = $1', [id]);
      return result.rows[0] ? toDlqEntry(result.rows[0]) : null;
    } catch (error) {
      if (error instanceof Error && 'code' in error && (error as { code: string }).code === '22P02') {
        return null;
      }
      throw error;
    }
  }

  async markReprocessed(id: string): Promise<void> {
    await this.pool.query('UPDATE dlq_events SET reprocessed_at = now() WHERE id = $1', [id]);
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/adapters`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-13-ledger-postgres-adapters
git add services/ledger/src/adapters/outbound/postgres services/ledger/tests/adapters
git commit -m "Implement ledger Postgres adapters"
git push -u origin task-13-ledger-postgres-adapters
```

---

## Task 14: Ledger Kafka consumer, HTTP routes, and DLQ reprocessing

**Files:**
- Create: `services/ledger/src/application/ports/KafkaProducerPort.ts`
- Create: `services/ledger/src/application/use-cases/ReprocessDlqEvent.ts`
- Create: `services/ledger/src/adapters/inbound/kafka/transferAuthorizedConsumer.ts`
- Create: `services/ledger/src/adapters/inbound/http/accountsRouter.ts`
- Create: `services/ledger/src/adapters/inbound/http/dlqRouter.ts`
- Create: `services/ledger/src/adapters/outbound/kafka/KafkaProducerAdapter.ts`
- Modify: `services/ledger/src/adapters/inbound/http/app.ts`
- Modify: `services/ledger/src/main.ts`
- Test: `services/ledger/tests/integration/transferAuthorizedFlow.test.ts`

**Interfaces:**
- Consumes: `PostSettlement` (Task 12), `LedgerRepositoryPort`, `DlqRepositoryPort` (Task 12/13).
- Produces: `GET /accounts/:accountId/entries`, `GET /dlq`, `POST /dlq/:id/reprocess`, `startTransferAuthorizedConsumer(consumer, postSettlement): Promise<void>`.

- [ ] **Step 1: Write the Kafka producer port and adapter (needed by DLQ reprocessing)**

```ts
// src/application/ports/KafkaProducerPort.ts
export interface KafkaProducerPort {
  publish(topic: string, key: string, payload: unknown): Promise<void>;
}
```

```ts
// src/adapters/outbound/kafka/KafkaProducerAdapter.ts
import { Kafka, Producer } from 'kafkajs';
import { KafkaProducerPort } from '../../../application/ports/KafkaProducerPort';

export function createKafka(brokers: string[], clientId: string): Kafka {
  return new Kafka({ clientId, brokers });
}

export class KafkaProducerAdapter implements KafkaProducerPort {
  constructor(private readonly producer: Producer) {}

  async publish(topic: string, key: string, payload: unknown): Promise<void> {
    await this.producer.send({ topic, messages: [{ key, value: JSON.stringify(payload) }] });
  }
}
```

- [ ] **Step 2: Write ReprocessDlqEvent**

```ts
// src/application/use-cases/ReprocessDlqEvent.ts
import { DlqRepositoryPort } from '../ports/DlqRepositoryPort';
import { KafkaProducerPort } from '../ports/KafkaProducerPort';

export class DlqEntryNotFoundError extends Error {}

export class ReprocessDlqEvent {
  constructor(
    private readonly dlqRepository: DlqRepositoryPort,
    private readonly producer: KafkaProducerPort,
  ) {}

  async execute(id: string): Promise<void> {
    const entry = await this.dlqRepository.get(id);
    if (!entry) {
      throw new DlqEntryNotFoundError(`no DLQ entry found with id "${id}"`);
    }
    await this.producer.publish(entry.topic, entry.transferId, entry.payload);
    await this.dlqRepository.markReprocessed(id);
  }
}
```

- [ ] **Step 3: Write the Kafka consumer**

```ts
// src/adapters/inbound/kafka/transferAuthorizedConsumer.ts
import { Consumer } from 'kafkajs';
import { PostSettlement } from '../../../application/use-cases/PostSettlement';

export const TRANSFER_AUTHORIZED_TOPIC = 'transfer-authorized';

export async function startTransferAuthorizedConsumer(consumer: Consumer, postSettlement: PostSettlement): Promise<void> {
  await consumer.subscribe({ topic: TRANSFER_AUTHORIZED_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      let rawPayload: unknown;
      try {
        rawPayload = JSON.parse(message.value?.toString() ?? '{}');
      } catch {
        rawPayload = {};
      }
      await postSettlement.execute(rawPayload);
    },
  });
}
```

- [ ] **Step 4: Write the HTTP routes**

```ts
// src/adapters/inbound/http/accountsRouter.ts
import { Router, Request, Response, NextFunction } from 'express';
import { LedgerRepositoryPort } from '../../../application/ports/LedgerRepositoryPort';

export function createAccountsRouter(ledgerRepository: LedgerRepositoryPort): Router {
  const router = Router();

  router.get('/accounts/:accountId/entries', async (req: Request, res: Response, next: NextFunction) => {
    try {
      const entries = await ledgerRepository.listEntriesForAccount(req.params.accountId);
      res.status(200).json({ entries });
    } catch (error) {
      next(error);
    }
  });

  return router;
}
```

```ts
// src/adapters/inbound/http/dlqRouter.ts
import { Router, Request, Response, NextFunction } from 'express';
import { DlqRepositoryPort } from '../../../application/ports/DlqRepositoryPort';
import { ReprocessDlqEvent, DlqEntryNotFoundError } from '../../../application/use-cases/ReprocessDlqEvent';

export function createDlqRouter(dlqRepository: DlqRepositoryPort, reprocessDlqEvent: ReprocessDlqEvent): Router {
  const router = Router();

  router.get('/dlq', async (req: Request, res: Response, next: NextFunction) => {
    try {
      const limit = typeof req.query.limit === 'string' ? Number(req.query.limit) : 20;
      const offset = typeof req.query.offset === 'string' ? Number(req.query.offset) : 0;
      if (!Number.isFinite(limit) || limit < 0 || !Number.isFinite(offset) || offset < 0) {
        res.status(400).json({ error: 'invalid limit or offset' });
        return;
      }
      const entries = await dlqRepository.list({ limit, offset });
      res.status(200).json({ entries });
    } catch (error) {
      next(error);
    }
  });

  router.post('/dlq/:id/reprocess', async (req: Request, res: Response, next: NextFunction) => {
    try {
      await reprocessDlqEvent.execute(req.params.id);
      res.status(200).json({ status: 'reprocessed' });
    } catch (error) {
      if (error instanceof DlqEntryNotFoundError) {
        res.status(404).json({ error: 'DLQ entry not found' });
        return;
      }
      next(error);
    }
  });

  return router;
}
```

- [ ] **Step 5: Wire the routes into app.ts**

```ts
// src/adapters/inbound/http/app.ts
import express, { Express, Request, Response, NextFunction } from 'express';
import { LedgerRepositoryPort } from '../../../application/ports/LedgerRepositoryPort';
import { DlqRepositoryPort } from '../../../application/ports/DlqRepositoryPort';
import { ReprocessDlqEvent } from '../../../application/use-cases/ReprocessDlqEvent';
import { createAccountsRouter } from './accountsRouter';
import { createDlqRouter } from './dlqRouter';

export interface AppDependencies {
  ledgerRepository?: LedgerRepositoryPort;
  dlqRepository?: DlqRepositoryPort;
  reprocessDlqEvent?: ReprocessDlqEvent;
}

export function createApp(deps: AppDependencies): Express {
  const app = express();
  app.use(express.json());

  app.get('/health', (_req: Request, res: Response) => {
    res.status(200).json({ status: 'ok' });
  });

  if (deps.ledgerRepository) {
    app.use(createAccountsRouter(deps.ledgerRepository));
  }
  if (deps.dlqRepository && deps.reprocessDlqEvent) {
    app.use(createDlqRouter(deps.dlqRepository, deps.reprocessDlqEvent));
  }

  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof SyntaxError && 'status' in err && (err as { status?: number }).status === 400) {
      res.status(400).json({ error: 'invalid JSON body' });
      return;
    }
    res.status(500).json({ error: 'internal server error' });
  });

  return app;
}
```

- [ ] **Step 6: Write the failing integration test**

```ts
// tests/integration/transferAuthorizedFlow.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgresLedgerRepository } from '../../src/adapters/outbound/postgres/PostgresLedgerRepository';
import { PostgresDlqRepository } from '../../src/adapters/outbound/postgres/PostgresDlqRepository';
import { PostSettlement } from '../../src/application/use-cases/PostSettlement';
import { startTransferAuthorizedConsumer, TRANSFER_AUTHORIZED_TOPIC } from '../../src/adapters/inbound/kafka/transferAuthorizedConsumer';
import { createKafka } from '../../src/adapters/outbound/kafka/KafkaProducerAdapter';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';
const KAFKA_BROKERS = (process.env.TEST_KAFKA_BROKERS ?? 'localhost:9092').split(',');

let pool: Pool;

async function waitFor<T>(check: () => Promise<T | null>, timeoutMs = 10000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const result = await check();
    if (result !== null) {
      return result;
    }
    if (Date.now() - start > timeoutMs) {
      throw new Error('waitFor timed out');
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
}

beforeAll(async () => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=ledger' });
  await pool.query('TRUNCATE ledger_entries, processed_transfers, outbox, dlq_events CASCADE');

  const kafka = createKafka(KAFKA_BROKERS, 'ledger-test');
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: TRANSFER_AUTHORIZED_TOPIC, numPartitions: 1 }] });
  await admin.disconnect();

  const producer = kafka.producer();
  await producer.connect();

  const ledgerRepository = new PostgresLedgerRepository(pool);
  const dlqRepository = new PostgresDlqRepository(pool);
  const postSettlement = new PostSettlement(ledgerRepository, dlqRepository);

  const consumer = kafka.consumer({ groupId: `ledger-flow-test-${Date.now()}` });
  await consumer.connect();
  await startTransferAuthorizedConsumer(consumer, postSettlement);

  // give the consumer group time to finish joining before any test publishes
  await new Promise((resolve) => setTimeout(resolve, 5000));

  (global as Record<string, unknown>).__producer = producer;
}, 30000);

afterAll(async () => {
  await pool.end();
});

describe('transfer-authorized consumption (integration)', () => {
  it('writes double-entry ledger entries when a transfer-authorized message is consumed', async () => {
    const producer = (global as Record<string, unknown>).__producer as import('kafkajs').Producer;
    await producer.send({
      topic: TRANSFER_AUTHORIZED_TOPIC,
      messages: [{ key: 'lt1', value: JSON.stringify({ transferId: 'lt1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 250 }) }],
    });

    const rows = await waitFor(async () => {
      const result = await pool.query("SELECT * FROM ledger_entries WHERE transfer_id = 'lt1'");
      return result.rows.length === 2 ? result.rows : null;
    });

    expect(rows).toHaveLength(2);

    const outbox = await pool.query("SELECT * FROM outbox WHERE topic = 'settlement-posted'");
    expect(outbox.rows).toHaveLength(1);
  }, 30000);
});
```

- [ ] **Step 7: Run test to verify it fails, then implement and verify it passes**

Run: `DATABASE_URL=postgres://postgres:postgres@localhost:5432/settlement_ledger npm run migrate && npx vitest run tests/integration/transferAuthorizedFlow.test.ts`
Expected: FAIL first (modules missing), then PASS once Steps 1-5 above are in place.

- [ ] **Step 8: Wire everything into main.ts**

```ts
// src/main.ts
import { Pool } from 'pg';
import { loadEnv } from './config/env';
import { createApp } from './adapters/inbound/http/app';
import { createKafka, KafkaProducerAdapter } from './adapters/outbound/kafka/KafkaProducerAdapter';
import { PostgresLedgerRepository } from './adapters/outbound/postgres/PostgresLedgerRepository';
import { PostgresDlqRepository } from './adapters/outbound/postgres/PostgresDlqRepository';
import { PostSettlement } from './application/use-cases/PostSettlement';
import { ReprocessDlqEvent } from './application/use-cases/ReprocessDlqEvent';
import { startTransferAuthorizedConsumer } from './adapters/inbound/kafka/transferAuthorizedConsumer';

async function main(): Promise<void> {
  const env = loadEnv();
  const pool = new Pool({ connectionString: env.databaseUrl, options: '-c search_path=ledger' });

  const kafka = createKafka(env.kafkaBrokers, 'ledger');
  const producer = kafka.producer();
  await producer.connect();
  const producerAdapter = new KafkaProducerAdapter(producer);

  const ledgerRepository = new PostgresLedgerRepository(pool);
  const dlqRepository = new PostgresDlqRepository(pool);
  const postSettlement = new PostSettlement(ledgerRepository, dlqRepository);
  const reprocessDlqEvent = new ReprocessDlqEvent(dlqRepository, producerAdapter);

  const consumer = kafka.consumer({ groupId: 'ledger' });
  await consumer.connect();
  await startTransferAuthorizedConsumer(consumer, postSettlement);

  const app = createApp({ ledgerRepository, dlqRepository, reprocessDlqEvent });
  app.listen(env.port, () => {
    console.log(`ledger listening on port ${env.port}`);
  });
}

main().catch((error) => {
  console.error('fatal startup error', error);
  process.exit(1);
});
```

- [ ] **Step 9: Typecheck and lint**

Run: `npm run typecheck && npm run lint`
Expected: both clean.

- [ ] **Step 10: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-14-ledger-consumer-http
git add services/ledger/src services/ledger/tests/integration
git commit -m "Add the transfer-authorized consumer, account entries endpoint, and DLQ routes"
git push -u origin task-14-ledger-consumer-http
```

---

## Task 15: Ledger outbox poller and Kafka producer wiring

**Files:**
- Create: `services/ledger/src/application/ports/OutboxRepositoryPort.ts`
- Create: `services/ledger/src/application/services/OutboxPoller.ts`
- Create: `services/ledger/src/adapters/outbound/postgres/PostgresOutboxRepository.ts`
- Modify: `services/ledger/src/main.ts`
- Test: `services/ledger/tests/adapters/outboxPoller.integration.test.ts`

**Interfaces:**
- Identical shapes to Task 8's `OutboxRepositoryPort`, `OutboxPoller`, `PostgresOutboxRepository` — repeated independently for `ledger`.

- [ ] **Step 1: Write the port, poller, and adapter** — identical in content to Task 8's `OutboxRepositoryPort.ts`, `OutboxPoller.ts`, and `PostgresOutboxRepository.ts`, copied verbatim into `services/ledger/src/...` (no import paths change; only the schema the `Pool` is scoped to differs, which is set at construction time in `main.ts`, not in this code).

- [ ] **Step 2: Write the failing integration test**

Identical in shape to Task 8's `tests/adapters/outboxPoller.integration.test.ts`, with these differences: `options: '-c search_path=ledger'` on the `Pool`, topic `'settlement-posted'`, test payload `{ transferId: 'ob1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 100, postedAt: new Date().toISOString() }`, client id `'ledger-test'`.

- [ ] **Step 3: Run test to verify it fails, then it passes**

Run: `npx vitest run tests/adapters/outboxPoller.integration.test.ts`
Expected: FAIL, then PASS once Step 1 is in place.

- [ ] **Step 4: Wire the poller into main.ts**

```ts
// src/main.ts (add alongside the existing imports and wiring from Task 14)
import { PostgresOutboxRepository } from './adapters/outbound/postgres/PostgresOutboxRepository';
import { OutboxPoller } from './application/services/OutboxPoller';

// inside main(), after producerAdapter is created:
const outboxRepository = new PostgresOutboxRepository(pool);
const outboxPoller = new OutboxPoller(outboxRepository, producerAdapter);
outboxPoller.start();
```

- [ ] **Step 5: Typecheck, lint, and run the full ledger test suite**

Run: `npm run typecheck && npm run lint && npm test`
Expected: all clean, all tests passing.

- [ ] **Step 6: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-15-ledger-outbox-poller
git add services/ledger/src services/ledger/tests/adapters/outboxPoller.integration.test.ts
git commit -m "Add the ledger outbox poller and Kafka producer"
git push -u origin task-15-ledger-outbox-poller
```

---

## Task 16: Authorization settlement-posted consumer — close the balance loop

**Files:**
- Create: `services/authorization/src/domain/services/SettlementPostedValidator.ts`
- Create: `services/authorization/src/application/ports/SettlementRepositoryPort.ts`
- Create: `services/authorization/src/application/ports/DlqRepositoryPort.ts`
- Create: `services/authorization/src/application/ports/KafkaProducerPort.ts`
- Create: `services/authorization/src/application/use-cases/ApplySettlementToProjection.ts`
- Create: `services/authorization/src/application/use-cases/ReprocessDlqEvent.ts`
- Create: `services/authorization/src/adapters/inbound/kafka/settlementPostedConsumer.ts`
- Create: `services/authorization/src/adapters/inbound/http/dlqRouter.ts`
- Create: `services/authorization/src/adapters/outbound/postgres/PostgresSettlementRepository.ts`
- Create: `services/authorization/src/adapters/outbound/postgres/PostgresDlqRepository.ts`
- Create: `services/authorization/src/adapters/outbound/kafka/KafkaProducerAdapter.ts`
- Create: `services/authorization/tests/fakes/FakeSettlementRepository.ts`
- Create: `services/authorization/tests/fakes/FakeDlqRepository.ts`
- Modify: `services/authorization/src/adapters/inbound/http/app.ts`
- Modify: `services/authorization/src/main.ts`
- Test: `services/authorization/tests/application/ApplySettlementToProjection.test.ts`
- Test: `services/authorization/tests/adapters/PostgresSettlementRepository.test.ts`
- Test: `services/authorization/tests/integration/settlementPostedFlow.test.ts`

No new migration is needed: the `dlq_events` table this task's DLQ repository uses was already created by Task 4's `005_create_dlq_events.sql`.

**Interfaces:**
- Consumes: the `settlement-posted` payload shape from Task 13: `{ transferId, fromAccount, toAccount, amount, postedAt }`.
- Produces: `SettlementRepositoryPort` (`wasConfirmed`, `confirmSettlement`), `ApplySettlementToProjection.execute(rawPayload: unknown): Promise<void>`, `GET /dlq`, `POST /dlq/:id/reprocess` on the authorization app.

- [ ] **Step 1: Write the zod validator**

```ts
// src/domain/services/SettlementPostedValidator.ts
import { z } from 'zod';

const settlementPostedSchema = z.object({
  transferId: z.string().min(1),
  fromAccount: z.string().min(1),
  toAccount: z.string().min(1),
  amount: z.number().positive(),
  postedAt: z.string().min(1),
});

export type SettlementPostedEvent = z.infer<typeof settlementPostedSchema>;

export function validateSettlementPosted(raw: unknown): SettlementPostedEvent {
  return settlementPostedSchema.parse(raw);
}
```

- [ ] **Step 2: Write the ports**

```ts
// src/application/ports/SettlementRepositoryPort.ts
export interface SettlementEvent {
  transferId: string;
  fromAccount: string;
  toAccount: string;
  amount: number;
}

export interface SettlementRepositoryPort {
  wasConfirmed(transferId: string): Promise<boolean>;
  confirmSettlement(event: SettlementEvent): Promise<void>;
}
```

```ts
// src/application/ports/DlqRepositoryPort.ts
export interface DlqEntry {
  id: string;
  transferId: string;
  topic: string;
  payload: unknown;
  failureReason: string;
  attempts: number;
  createdAt: Date;
  reprocessedAt: Date | null;
}

export interface DlqRepositoryPort {
  add(entry: { transferId: string; topic: string; payload: unknown; failureReason: string; attempts: number }): Promise<string>;
  list(options: { limit: number; offset: number }): Promise<DlqEntry[]>;
  get(id: string): Promise<DlqEntry | null>;
  markReprocessed(id: string): Promise<void>;
}
```

```ts
// src/application/ports/KafkaProducerPort.ts
export interface KafkaProducerPort {
  publish(topic: string, key: string, payload: unknown): Promise<void>;
}
```

- [ ] **Step 3: Write the fakes**

```ts
// tests/fakes/FakeSettlementRepository.ts
import { SettlementRepositoryPort, SettlementEvent } from '../../src/application/ports/SettlementRepositoryPort';

export class FakeSettlementRepository implements SettlementRepositoryPort {
  public readonly confirmed = new Set<string>();
  public confirmSettlementCalls = 0;

  async wasConfirmed(transferId: string): Promise<boolean> {
    return this.confirmed.has(transferId);
  }

  async confirmSettlement(event: SettlementEvent): Promise<void> {
    this.confirmSettlementCalls += 1;
    this.confirmed.add(event.transferId);
  }
}
```

```ts
// tests/fakes/FakeDlqRepository.ts
import { DlqRepositoryPort, DlqEntry } from '../../src/application/ports/DlqRepositoryPort';

export class FakeDlqRepository implements DlqRepositoryPort {
  public readonly entries: DlqEntry[] = [];

  async add(entry: { transferId: string; topic: string; payload: unknown; failureReason: string; attempts: number }): Promise<string> {
    const id = `dlq-${this.entries.length + 1}`;
    this.entries.push({ id, ...entry, createdAt: new Date(), reprocessedAt: null });
    return id;
  }

  async list(): Promise<DlqEntry[]> {
    return this.entries;
  }

  async get(id: string): Promise<DlqEntry | null> {
    return this.entries.find((entry) => entry.id === id) ?? null;
  }

  async markReprocessed(id: string): Promise<void> {
    const entry = this.entries.find((e) => e.id === id);
    if (entry) {
      entry.reprocessedAt = new Date();
    }
  }
}
```

- [ ] **Step 4: Write the failing use-case test**

```ts
// tests/application/ApplySettlementToProjection.test.ts
import { describe, it, expect } from 'vitest';
import { ApplySettlementToProjection } from '../../src/application/use-cases/ApplySettlementToProjection';
import { FakeSettlementRepository } from '../fakes/FakeSettlementRepository';
import { FakeDlqRepository } from '../fakes/FakeDlqRepository';

describe('ApplySettlementToProjection', () => {
  it('confirms a valid settlement-posted payload', async () => {
    const settlementRepository = new FakeSettlementRepository();
    const dlqRepository = new FakeDlqRepository();
    const useCase = new ApplySettlementToProjection(settlementRepository, dlqRepository);

    await useCase.execute({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500, postedAt: '2026-01-01T00:00:00.000Z' });

    expect(settlementRepository.confirmSettlementCalls).toBe(1);
  });

  it('skips a transferId that was already confirmed', async () => {
    const settlementRepository = new FakeSettlementRepository();
    settlementRepository.confirmed.add('t1');
    const dlqRepository = new FakeDlqRepository();
    const useCase = new ApplySettlementToProjection(settlementRepository, dlqRepository);

    await useCase.execute({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500, postedAt: '2026-01-01T00:00:00.000Z' });

    expect(settlementRepository.confirmSettlementCalls).toBe(0);
  });

  it('sends a malformed payload to the DLQ after exhausting retries', async () => {
    const settlementRepository = new FakeSettlementRepository();
    const dlqRepository = new FakeDlqRepository();
    const useCase = new ApplySettlementToProjection(settlementRepository, dlqRepository, { maxAttempts: 2, backoffMs: () => 1 });

    await useCase.execute({ transferId: 't2' });

    expect(dlqRepository.entries).toHaveLength(1);
    expect(dlqRepository.entries[0].topic).toBe('settlement-posted');
  });
});
```

- [ ] **Step 5: Run test to verify it fails**

Run: `npx vitest run tests/application/ApplySettlementToProjection.test.ts`
Expected: FAIL.

- [ ] **Step 6: Implement ApplySettlementToProjection**

```ts
// src/application/use-cases/ApplySettlementToProjection.ts
import { validateSettlementPosted } from '../../domain/services/SettlementPostedValidator';
import { SettlementRepositoryPort } from '../ports/SettlementRepositoryPort';
import { DlqRepositoryPort } from '../ports/DlqRepositoryPort';

export interface ApplySettlementToProjectionOptions {
  maxAttempts?: number;
  backoffMs?: (attempt: number) => number;
}

const DEFAULT_BACKOFF_MS = (attempt: number): number => 100 * 2 ** (attempt - 1);

export class ApplySettlementToProjection {
  private readonly maxAttempts: number;
  private readonly backoffMs: (attempt: number) => number;

  constructor(
    private readonly settlementRepository: SettlementRepositoryPort,
    private readonly dlqRepository: DlqRepositoryPort,
    options: ApplySettlementToProjectionOptions = {},
  ) {
    this.maxAttempts = options.maxAttempts ?? 3;
    this.backoffMs = options.backoffMs ?? DEFAULT_BACKOFF_MS;
  }

  async execute(rawPayload: unknown): Promise<void> {
    const transferId = this.extractTransferId(rawPayload);
    if (transferId !== null && (await this.settlementRepository.wasConfirmed(transferId))) {
      return;
    }

    let lastError: Error = new Error('unknown processing error');

    for (let attempt = 1; attempt <= this.maxAttempts; attempt += 1) {
      try {
        const event = validateSettlementPosted(rawPayload);
        await this.settlementRepository.confirmSettlement(event);
        return;
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        if (attempt < this.maxAttempts) {
          await this.delay(this.backoffMs(attempt));
        }
      }
    }

    await this.dlqRepository.add({
      transferId: transferId ?? 'unknown',
      topic: 'settlement-posted',
      payload: rawPayload,
      failureReason: lastError.message,
      attempts: this.maxAttempts,
    });
  }

  private extractTransferId(rawPayload: unknown): string | null {
    if (typeof rawPayload === 'object' && rawPayload !== null && 'transferId' in rawPayload) {
      const value = (rawPayload as { transferId: unknown }).transferId;
      return typeof value === 'string' ? value : null;
    }
    return null;
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
```

- [ ] **Step 7: Run test to verify it passes**

Run: `npx vitest run tests/application/ApplySettlementToProjection.test.ts`
Expected: PASS (3 tests)

- [ ] **Step 8: Write the failing Postgres adapter test**

```ts
// tests/adapters/PostgresSettlementRepository.test.ts
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgresSettlementRepository } from '../../src/adapters/outbound/postgres/PostgresSettlementRepository';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';

let pool: Pool;
let repository: PostgresSettlementRepository;

beforeAll(() => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=authz' });
  repository = new PostgresSettlementRepository(pool);
});

beforeEach(async () => {
  await pool.query('TRUNCATE transfers, reservations, balance_projection CASCADE');
  await pool.query("INSERT INTO transfers (id, from_account, to_account, amount, status) VALUES ('t1', 'acc_1', 'acc_2', 500, 'approved')");
  await pool.query("INSERT INTO reservations (transfer_id, account_id, amount, status) VALUES ('t1', 'acc_1', 500, 'pending')");
  await pool.query("INSERT INTO balance_projection (account_id, confirmed_balance) VALUES ('acc_1', 1000)");
});

afterAll(async () => {
  await pool.end();
});

describe('PostgresSettlementRepository', () => {
  it('reports a transfer as not confirmed before confirmSettlement runs', async () => {
    expect(await repository.wasConfirmed('t1')).toBe(false);
  });

  it('confirms the settlement: transfer status, reservation release, and both balances', async () => {
    await repository.confirmSettlement({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });

    expect(await repository.wasConfirmed('t1')).toBe(true);

    const transfer = await pool.query("SELECT status FROM transfers WHERE id = 't1'");
    expect(transfer.rows[0].status).toBe('confirmed');

    const reservation = await pool.query("SELECT status FROM reservations WHERE transfer_id = 't1'");
    expect(reservation.rows[0].status).toBe('released');

    const fromBalance = await pool.query("SELECT confirmed_balance FROM balance_projection WHERE account_id = 'acc_1'");
    expect(Number(fromBalance.rows[0].confirmed_balance)).toBe(500);

    const toBalance = await pool.query("SELECT confirmed_balance FROM balance_projection WHERE account_id = 'acc_2'");
    expect(Number(toBalance.rows[0].confirmed_balance)).toBe(500);
  });
});
```

- [ ] **Step 9: Run test to verify it fails**

Run: `npx vitest run tests/adapters/PostgresSettlementRepository.test.ts`
Expected: FAIL.

- [ ] **Step 10: Implement the Postgres adapters**

```ts
// src/adapters/outbound/postgres/PostgresSettlementRepository.ts
import { Pool } from 'pg';
import { SettlementRepositoryPort, SettlementEvent } from '../../../application/ports/SettlementRepositoryPort';

export class PostgresSettlementRepository implements SettlementRepositoryPort {
  constructor(private readonly pool: Pool) {}

  async wasConfirmed(transferId: string): Promise<boolean> {
    const result = await this.pool.query("SELECT 1 FROM transfers WHERE id = $1 AND status = 'confirmed'", [transferId]);
    return result.rows.length > 0;
  }

  async confirmSettlement(event: SettlementEvent): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query("UPDATE transfers SET status = 'confirmed' WHERE id = $1 AND status = 'approved'", [event.transferId]);
      await client.query("UPDATE reservations SET status = 'released' WHERE transfer_id = $1 AND status = 'pending'", [event.transferId]);
      await client.query(
        `INSERT INTO balance_projection (account_id, confirmed_balance) VALUES ($1, $2)
         ON CONFLICT (account_id) DO UPDATE SET confirmed_balance = balance_projection.confirmed_balance - $2`,
        [event.fromAccount, event.amount],
      );
      await client.query(
        `INSERT INTO balance_projection (account_id, confirmed_balance) VALUES ($1, $2)
         ON CONFLICT (account_id) DO UPDATE SET confirmed_balance = balance_projection.confirmed_balance + $2`,
        [event.toAccount, event.amount],
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
}
```

```ts
// src/adapters/outbound/postgres/PostgresDlqRepository.ts
import { Pool } from 'pg';
import { DlqEntry, DlqRepositoryPort } from '../../../application/ports/DlqRepositoryPort';

interface DlqRow {
  id: string;
  transfer_id: string;
  topic: string;
  payload: unknown;
  failure_reason: string;
  attempts: number;
  created_at: Date;
  reprocessed_at: Date | null;
}

function toDlqEntry(row: DlqRow): DlqEntry {
  return {
    id: row.id,
    transferId: row.transfer_id,
    topic: row.topic,
    payload: row.payload,
    failureReason: row.failure_reason,
    attempts: row.attempts,
    createdAt: row.created_at,
    reprocessedAt: row.reprocessed_at,
  };
}

export class PostgresDlqRepository implements DlqRepositoryPort {
  constructor(private readonly pool: Pool) {}

  async add(entry: { transferId: string; topic: string; payload: unknown; failureReason: string; attempts: number }): Promise<string> {
    const result = await this.pool.query<{ id: string }>(
      `INSERT INTO dlq_events (transfer_id, topic, payload, failure_reason, attempts)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id`,
      [entry.transferId, entry.topic, JSON.stringify(entry.payload), entry.failureReason, entry.attempts],
    );
    return result.rows[0].id;
  }

  async list(options: { limit: number; offset: number }): Promise<DlqEntry[]> {
    const result = await this.pool.query<DlqRow>(
      'SELECT * FROM dlq_events WHERE reprocessed_at IS NULL ORDER BY created_at ASC LIMIT $1 OFFSET $2',
      [options.limit, options.offset],
    );
    return result.rows.map(toDlqEntry);
  }

  async get(id: string): Promise<DlqEntry | null> {
    try {
      const result = await this.pool.query<DlqRow>('SELECT * FROM dlq_events WHERE id = $1', [id]);
      return result.rows[0] ? toDlqEntry(result.rows[0]) : null;
    } catch (error) {
      if (error instanceof Error && 'code' in error && (error as { code: string }).code === '22P02') {
        return null;
      }
      throw error;
    }
  }

  async markReprocessed(id: string): Promise<void> {
    await this.pool.query('UPDATE dlq_events SET reprocessed_at = now() WHERE id = $1', [id]);
  }
}
```

- [ ] **Step 11: Run test to verify it passes**

Run: `npx vitest run tests/adapters/PostgresSettlementRepository.test.ts`
Expected: PASS (2 tests)

- [ ] **Step 12: Write ReprocessDlqEvent, the Kafka consumer, producer adapter, and dlqRouter**

```ts
// src/application/use-cases/ReprocessDlqEvent.ts
import { DlqRepositoryPort } from '../ports/DlqRepositoryPort';
import { KafkaProducerPort } from '../ports/KafkaProducerPort';

export class DlqEntryNotFoundError extends Error {}

export class ReprocessDlqEvent {
  constructor(
    private readonly dlqRepository: DlqRepositoryPort,
    private readonly producer: KafkaProducerPort,
  ) {}

  async execute(id: string): Promise<void> {
    const entry = await this.dlqRepository.get(id);
    if (!entry) {
      throw new DlqEntryNotFoundError(`no DLQ entry found with id "${id}"`);
    }
    await this.producer.publish(entry.topic, entry.transferId, entry.payload);
    await this.dlqRepository.markReprocessed(id);
  }
}
```

```ts
// src/adapters/inbound/kafka/settlementPostedConsumer.ts
import { Consumer } from 'kafkajs';
import { ApplySettlementToProjection } from '../../../application/use-cases/ApplySettlementToProjection';

export const SETTLEMENT_POSTED_TOPIC = 'settlement-posted';

export async function startSettlementPostedConsumer(consumer: Consumer, applySettlementToProjection: ApplySettlementToProjection): Promise<void> {
  await consumer.subscribe({ topic: SETTLEMENT_POSTED_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      let rawPayload: unknown;
      try {
        rawPayload = JSON.parse(message.value?.toString() ?? '{}');
      } catch {
        rawPayload = {};
      }
      await applySettlementToProjection.execute(rawPayload);
    },
  });
}
```

```ts
// src/adapters/outbound/kafka/KafkaProducerAdapter.ts
import { Kafka, Producer } from 'kafkajs';
import { KafkaProducerPort } from '../../../application/ports/KafkaProducerPort';

export function createKafka(brokers: string[], clientId: string): Kafka {
  return new Kafka({ clientId, brokers });
}

export class KafkaProducerAdapter implements KafkaProducerPort {
  constructor(private readonly producer: Producer) {}

  async publish(topic: string, key: string, payload: unknown): Promise<void> {
    await this.producer.send({ topic, messages: [{ key, value: JSON.stringify(payload) }] });
  }
}
```

```ts
// src/adapters/inbound/http/dlqRouter.ts
import { Router, Request, Response, NextFunction } from 'express';
import { DlqRepositoryPort } from '../../../application/ports/DlqRepositoryPort';
import { ReprocessDlqEvent, DlqEntryNotFoundError } from '../../../application/use-cases/ReprocessDlqEvent';

export function createDlqRouter(dlqRepository: DlqRepositoryPort, reprocessDlqEvent: ReprocessDlqEvent): Router {
  const router = Router();

  router.get('/dlq', async (req: Request, res: Response, next: NextFunction) => {
    try {
      const limit = typeof req.query.limit === 'string' ? Number(req.query.limit) : 20;
      const offset = typeof req.query.offset === 'string' ? Number(req.query.offset) : 0;
      if (!Number.isFinite(limit) || limit < 0 || !Number.isFinite(offset) || offset < 0) {
        res.status(400).json({ error: 'invalid limit or offset' });
        return;
      }
      const entries = await dlqRepository.list({ limit, offset });
      res.status(200).json({ entries });
    } catch (error) {
      next(error);
    }
  });

  router.post('/dlq/:id/reprocess', async (req: Request, res: Response, next: NextFunction) => {
    try {
      await reprocessDlqEvent.execute(req.params.id);
      res.status(200).json({ status: 'reprocessed' });
    } catch (error) {
      if (error instanceof DlqEntryNotFoundError) {
        res.status(404).json({ error: 'DLQ entry not found' });
        return;
      }
      next(error);
    }
  });

  return router;
}
```

- [ ] **Step 13: Wire dlqRouter into app.ts** (add to the existing `AppDependencies` interface and conditional `app.use` block from Task 7, alongside `requestTransfer`/`transferRepository`)

```ts
// src/adapters/inbound/http/app.ts — add these fields and this block
// AppDependencies gains: dlqRepository?: DlqRepositoryPort; reprocessDlqEvent?: ReprocessDlqEvent;
// (import DlqRepositoryPort, ReprocessDlqEvent, createDlqRouter at the top)

if (deps.dlqRepository && deps.reprocessDlqEvent) {
  app.use(createDlqRouter(deps.dlqRepository, deps.reprocessDlqEvent));
}
```

- [ ] **Step 14: Write the failing end-to-end integration test for this consumer**

```ts
// tests/integration/settlementPostedFlow.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgresSettlementRepository } from '../../src/adapters/outbound/postgres/PostgresSettlementRepository';
import { PostgresDlqRepository } from '../../src/adapters/outbound/postgres/PostgresDlqRepository';
import { ApplySettlementToProjection } from '../../src/application/use-cases/ApplySettlementToProjection';
import { startSettlementPostedConsumer, SETTLEMENT_POSTED_TOPIC } from '../../src/adapters/inbound/kafka/settlementPostedConsumer';
import { createKafka } from '../../src/adapters/outbound/kafka/KafkaProducerAdapter';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';
const KAFKA_BROKERS = (process.env.TEST_KAFKA_BROKERS ?? 'localhost:9092').split(',');

let pool: Pool;

async function waitFor<T>(check: () => Promise<T | null>, timeoutMs = 10000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const result = await check();
    if (result !== null) {
      return result;
    }
    if (Date.now() - start > timeoutMs) {
      throw new Error('waitFor timed out');
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
}

beforeAll(async () => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=authz' });
  await pool.query('TRUNCATE transfers, reservations, balance_projection, outbox, dlq_events CASCADE');
  await pool.query("INSERT INTO transfers (id, from_account, to_account, amount, status) VALUES ('st1', 'acc_1', 'acc_2', 300, 'approved')");
  await pool.query("INSERT INTO reservations (transfer_id, account_id, amount, status) VALUES ('st1', 'acc_1', 300, 'pending')");
  await pool.query("INSERT INTO balance_projection (account_id, confirmed_balance) VALUES ('acc_1', 1000)");

  const kafka = createKafka(KAFKA_BROKERS, 'authorization-test');
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: SETTLEMENT_POSTED_TOPIC, numPartitions: 1 }] });
  await admin.disconnect();

  const producer = kafka.producer();
  await producer.connect();

  const settlementRepository = new PostgresSettlementRepository(pool);
  const dlqRepository = new PostgresDlqRepository(pool);
  const applySettlementToProjection = new ApplySettlementToProjection(settlementRepository, dlqRepository);

  const consumer = kafka.consumer({ groupId: `authorization-flow-test-${Date.now()}` });
  await consumer.connect();
  await startSettlementPostedConsumer(consumer, applySettlementToProjection);

  await new Promise((resolve) => setTimeout(resolve, 5000));

  (global as Record<string, unknown>).__producer = producer;
}, 30000);

afterAll(async () => {
  await pool.end();
});

describe('settlement-posted consumption (integration)', () => {
  it('confirms the transfer and updates both balances when the message is consumed', async () => {
    const producer = (global as Record<string, unknown>).__producer as import('kafkajs').Producer;
    await producer.send({
      topic: SETTLEMENT_POSTED_TOPIC,
      messages: [{
        key: 'st1',
        value: JSON.stringify({ transferId: 'st1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 300, postedAt: new Date().toISOString() }),
      }],
    });

    const transfer = await waitFor(async () => {
      const result = await pool.query("SELECT status FROM transfers WHERE id = 'st1' AND status = 'confirmed'");
      return result.rows[0] ?? null;
    });

    expect(transfer.status).toBe('confirmed');

    const balance = await pool.query("SELECT confirmed_balance FROM balance_projection WHERE account_id = 'acc_2'");
    expect(Number(balance.rows[0].confirmed_balance)).toBe(300);
  }, 30000);
});
```

- [ ] **Step 15: Run test to verify it passes**

Run: `DATABASE_URL=postgres://postgres:postgres@localhost:5432/settlement_ledger npm run migrate && npx vitest run tests/integration/settlementPostedFlow.test.ts`
Expected: PASS

- [ ] **Step 16: Wire the consumer and outbox-reuse into main.ts**

```ts
// src/main.ts — extends Task 8's main.ts with the settlement-posted consumer and DLQ wiring
import { Pool } from 'pg';
import { loadEnv } from './config/env';
import { createApp } from './adapters/inbound/http/app';
import { createKafka, KafkaProducerAdapter } from './adapters/outbound/kafka/KafkaProducerAdapter';
import { PostgresTransferRepository } from './adapters/outbound/postgres/PostgresTransferRepository';
import { PostgresBalanceProjection } from './adapters/outbound/postgres/PostgresBalanceProjection';
import { PostgresOutboxRepository } from './adapters/outbound/postgres/PostgresOutboxRepository';
import { PostgresSettlementRepository } from './adapters/outbound/postgres/PostgresSettlementRepository';
import { PostgresDlqRepository } from './adapters/outbound/postgres/PostgresDlqRepository';
import { RequestTransfer } from './application/use-cases/RequestTransfer';
import { ApplySettlementToProjection } from './application/use-cases/ApplySettlementToProjection';
import { ReprocessDlqEvent } from './application/use-cases/ReprocessDlqEvent';
import { OutboxPoller } from './application/services/OutboxPoller';
import { startSettlementPostedConsumer } from './adapters/inbound/kafka/settlementPostedConsumer';

async function main(): Promise<void> {
  const env = loadEnv();
  const pool = new Pool({ connectionString: env.databaseUrl, options: '-c search_path=authz' });

  const kafka = createKafka(env.kafkaBrokers, 'authorization');
  const producer = kafka.producer();
  await producer.connect();
  const producerAdapter = new KafkaProducerAdapter(producer);

  const transferRepository = new PostgresTransferRepository(pool);
  const balanceProjection = new PostgresBalanceProjection(pool);
  const outboxRepository = new PostgresOutboxRepository(pool);
  const settlementRepository = new PostgresSettlementRepository(pool);
  const dlqRepository = new PostgresDlqRepository(pool);

  const requestTransfer = new RequestTransfer(transferRepository, balanceProjection);
  const applySettlementToProjection = new ApplySettlementToProjection(settlementRepository, dlqRepository);
  const reprocessDlqEvent = new ReprocessDlqEvent(dlqRepository, producerAdapter);

  const outboxPoller = new OutboxPoller(outboxRepository, producerAdapter);
  outboxPoller.start();

  const consumer = kafka.consumer({ groupId: 'authorization' });
  await consumer.connect();
  await startSettlementPostedConsumer(consumer, applySettlementToProjection);

  const app = createApp({ requestTransfer, transferRepository, dlqRepository, reprocessDlqEvent });
  app.listen(env.port, () => {
    console.log(`authorization listening on port ${env.port}`);
  });
}

main().catch((error) => {
  console.error('fatal startup error', error);
  process.exit(1);
});
```

- [ ] **Step 17: Typecheck, lint, and run the full authorization test suite**

Run: `npm run typecheck && npm run lint && npm test`
Expected: all clean, all tests passing.

- [ ] **Step 18: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-16-authorization-settlement-consumer
git add services/authorization/src services/authorization/tests
git commit -m "Close the balance projection loop by consuming settlement-posted"
git push -u origin task-16-authorization-settlement-consumer
```

---

## Task 17: Scaffold the payout service

**Files:**
- Create: `services/payout/package.json`, `tsconfig.json`, `.eslintrc.cjs`, `vitest.config.mts`, `.dockerignore`, `Dockerfile`
- Create: `services/payout/src/config/env.ts`
- Create: `services/payout/src/adapters/inbound/http/app.ts`
- Create: `services/payout/src/main.ts`
- Test: `services/payout/tests/health.test.ts`

**Interfaces:**
- Produces: identical shapes to Task 2/9, renamed for `payout` (`name: "payout"`, `PORT` default `3003`, `EXPOSE 3003`). `env.ts` additionally has `pspBaseUrl: string` (default `http://localhost:4003`).

- [ ] **Step 1-5:** package.json, tsconfig.json, .eslintrc.cjs, vitest.config.mts, .dockerignore/Dockerfile — identical to Task 2's, with `"name": "payout"` and `EXPOSE 3003`.

- [ ] **Step 6: config/env.ts**

```ts
// src/config/env.ts
export interface AppEnv {
  port: number;
  databaseUrl: string;
  kafkaBrokers: string[];
  pspBaseUrl: string;
}

export function loadEnv(): AppEnv {
  const rawPort = process.env.PORT;
  const port = rawPort === undefined ? 3003 : Number(rawPort);
  if (Number.isNaN(port)) {
    throw new Error(`Invalid PORT environment variable: "${rawPort}" is not a number`);
  }

  return {
    port,
    databaseUrl: process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger',
    kafkaBrokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
    pspBaseUrl: process.env.PSP_BASE_URL ?? 'http://localhost:4003',
  };
}
```

- [ ] **Step 7-10:** health-check test, app.ts, main.ts — identical in shape to Task 2's, with `payout listening on port ${env.port}`.

- [ ] **Step 11: Typecheck and lint**

Run: `cd services/payout && npm install && npm run typecheck && npm run lint && npm test`
Expected: all clean.

- [ ] **Step 12: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-17-payout-scaffold
git add services/payout
git commit -m "Scaffold the payout service"
git push -u origin task-17-payout-scaffold
```

---

## Task 18: Mock payout PSP

**Files:**
- Create: `mock-payout-psp/package.json`, `tsconfig.json`, `Dockerfile`, `.dockerignore`
- Create: `mock-payout-psp/server.ts`
- Test: `mock-payout-psp/tests/server.test.ts`

**Interfaces:**
- Produces: `createMockPayoutPspApp(): Express` — `GET /health` → `200 { status: 'ok' }`; `POST /psp/send` with body `{ transferId, toAccount, amount }` → `200 { accepted: true, pspReference }` normally, or `200 { accepted: false, reason }` when `toAccount === 'acc_psp_fail_demo'`. The health route exists because Task 24's end-to-end test polls it before starting, the same way it polls every other service's `GET /health`.

- [ ] **Step 1: package.json**

```json
{
  "name": "mock-payout-psp",
  "version": "0.1.0",
  "private": true,
  "engines": { "node": ">=18" },
  "scripts": {
    "dev": "tsx server.ts",
    "test": "vitest run",
    "typecheck": "tsc --noEmit"
  },
  "dependencies": {
    "express": "^4.19.2",
    "zod": "^3.23.8"
  },
  "devDependencies": {
    "@types/express": "^4.17.21",
    "@types/node": "^22.5.0",
    "@types/supertest": "^6.0.2",
    "supertest": "^7.0.0",
    "tsx": "^4.16.2",
    "typescript": "^5.5.4",
    "vitest": "^5.0.0"
  }
}
```

- [ ] **Step 2: tsconfig.json**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "CommonJS",
    "moduleResolution": "Node",
    "lib": ["ES2022"],
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "resolveJsonModule": true,
    "noEmit": true
  },
  "include": ["server.ts", "tests"]
}
```

- [ ] **Step 3: .dockerignore and Dockerfile**

```
node_modules
.git
coverage
```

```dockerfile
FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

EXPOSE 4003

CMD ["npx", "tsx", "server.ts"]
```

- [ ] **Step 4: Write the failing test**

```ts
// tests/server.test.ts
import { describe, it, expect } from 'vitest';
import request from 'supertest';
import { createMockPayoutPspApp } from '../server';

describe('mock-payout-psp', () => {
  it('reports healthy', async () => {
    const app = createMockPayoutPspApp();
    const response = await request(app).get('/health');
    expect(response.status).toBe(200);
    expect(response.body).toEqual({ status: 'ok' });
  });

  it('accepts a normal payout', async () => {
    const app = createMockPayoutPspApp();
    const response = await request(app).post('/psp/send').send({ transferId: 't1', toAccount: 'acc_2', amount: 500 });
    expect(response.status).toBe(200);
    expect(response.body.accepted).toBe(true);
    expect(typeof response.body.pspReference).toBe('string');
  });

  it('declines a payout to the magic failure account', async () => {
    const app = createMockPayoutPspApp();
    const response = await request(app).post('/psp/send').send({ transferId: 't2', toAccount: 'acc_psp_fail_demo', amount: 500 });
    expect(response.status).toBe(200);
    expect(response.body).toEqual({ accepted: false, reason: 'destination account rejected the payment' });
  });

  it('rejects an invalid request body', async () => {
    const app = createMockPayoutPspApp();
    const response = await request(app).post('/psp/send').send({ transferId: 't3' });
    expect(response.status).toBe(400);
  });
});
```

- [ ] **Step 5: Run test to verify it fails**

Run: `cd mock-payout-psp && npm install && npx vitest run`
Expected: FAIL — `server.ts` has no export yet.

- [ ] **Step 6: Implement server.ts**

```ts
// server.ts
import express, { Express, Request, Response } from 'express';
import { randomUUID } from 'node:crypto';
import { z } from 'zod';

const sendSchema = z.object({
  transferId: z.string().min(1),
  toAccount: z.string().min(1),
  amount: z.number().positive(),
});

const FAILING_ACCOUNT = 'acc_psp_fail_demo';

export function createMockPayoutPspApp(): Express {
  const app = express();
  app.use(express.json());

  app.get('/health', (_req: Request, res: Response) => {
    res.status(200).json({ status: 'ok' });
  });

  app.post('/psp/send', (req: Request, res: Response) => {
    const parseResult = sendSchema.safeParse(req.body);
    if (!parseResult.success) {
      res.status(400).json({ error: 'invalid request body' });
      return;
    }

    if (parseResult.data.toAccount === FAILING_ACCOUNT) {
      res.status(200).json({ accepted: false, reason: 'destination account rejected the payment' });
      return;
    }

    res.status(200).json({ accepted: true, pspReference: randomUUID() });
  });

  return app;
}

if (require.main === module) {
  const app = createMockPayoutPspApp();
  const port = process.env.PORT === undefined ? 4003 : Number(process.env.PORT);
  app.listen(port, () => {
    console.log(`mock-payout-psp listening on port ${port}`);
  });
}
```

- [ ] **Step 7: Run test to verify it passes**

Run: `npx vitest run`
Expected: PASS (4 tests)

- [ ] **Step 8: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-18-mock-payout-psp
git add mock-payout-psp
git commit -m "Add the mock payout PSP server"
git push -u origin task-18-mock-payout-psp
```

---

## Task 19: Payout application layer — SendPayout use case

**Files:**
- Create: `services/payout/src/domain/services/SettlementPostedValidator.ts`
- Create: `services/payout/src/application/ports/PspClientPort.ts`
- Create: `services/payout/src/application/ports/PayoutRepositoryPort.ts`
- Create: `services/payout/src/application/ports/DlqRepositoryPort.ts`
- Create: `services/payout/src/application/use-cases/SendPayout.ts`
- Create: `services/payout/tests/fakes/FakePspClient.ts`
- Create: `services/payout/tests/fakes/FakePayoutRepository.ts`
- Create: `services/payout/tests/fakes/FakeDlqRepository.ts`
- Test: `services/payout/tests/application/SendPayout.test.ts`

**Interfaces:**
- Consumes: the `settlement-posted` payload shape from Task 13: `{ transferId, fromAccount, toAccount, amount, postedAt }`.
- Produces: `PspClientPort.send(input): Promise<PspSendResult>`; `PayoutRepositoryPort` (`wasProcessed`, `savePayoutAttempt`, `get`, `listFailed`); `SendPayout.execute(rawPayload: unknown): Promise<void>`. `PayoutAttemptInput { transferId, toAccount, amount, status: 'sent' | 'failed', pspReference: string | null }` — this is the exact shape Task 22's HTTP routes and Task 20's adapter rely on.

- [ ] **Step 1: Write the zod validator** — identical content to Task 16's `SettlementPostedValidator.ts` (same payload shape, same schema), copied into `services/payout/src/domain/services/SettlementPostedValidator.ts`.

- [ ] **Step 2: Write the ports**

```ts
// src/application/ports/PspClientPort.ts
export interface PspSendInput {
  transferId: string;
  toAccount: string;
  amount: number;
}

export interface PspSendResult {
  succeeded: boolean;
  pspReference: string | null;
  reason: string | null;
}

export interface PspClientPort {
  send(input: PspSendInput): Promise<PspSendResult>;
}
```

```ts
// src/application/ports/PayoutRepositoryPort.ts
export type PayoutStatus = 'sent' | 'failed';

export interface PayoutAttemptInput {
  transferId: string;
  toAccount: string;
  amount: number;
  status: PayoutStatus;
  pspReference: string | null;
}

export interface PayoutAttemptRecord extends PayoutAttemptInput {
  createdAt: Date;
}

export interface PayoutRepositoryPort {
  wasProcessed(transferId: string): Promise<boolean>;
  savePayoutAttempt(input: PayoutAttemptInput): Promise<void>;
  get(transferId: string): Promise<PayoutAttemptRecord | null>;
  listFailed(options: { limit: number; offset: number }): Promise<PayoutAttemptRecord[]>;
}
```

```ts
// src/application/ports/DlqRepositoryPort.ts
export interface DlqEntry {
  id: string;
  transferId: string;
  topic: string;
  payload: unknown;
  failureReason: string;
  attempts: number;
  createdAt: Date;
  reprocessedAt: Date | null;
}

export interface DlqRepositoryPort {
  add(entry: { transferId: string; topic: string; payload: unknown; failureReason: string; attempts: number }): Promise<string>;
  list(options: { limit: number; offset: number }): Promise<DlqEntry[]>;
  get(id: string): Promise<DlqEntry | null>;
  markReprocessed(id: string): Promise<void>;
}
```

- [ ] **Step 3: Write the fakes**

```ts
// tests/fakes/FakePspClient.ts
import { PspClientPort, PspSendInput, PspSendResult } from '../../src/application/ports/PspClientPort';

export class FakePspClient implements PspClientPort {
  public lastInput: PspSendInput | null = null;

  constructor(private readonly result: PspSendResult = { succeeded: true, pspReference: 'fake-ref', reason: null }) {}

  async send(input: PspSendInput): Promise<PspSendResult> {
    this.lastInput = input;
    return this.result;
  }
}
```

```ts
// tests/fakes/FakePayoutRepository.ts
import { PayoutRepositoryPort, PayoutAttemptInput, PayoutAttemptRecord } from '../../src/application/ports/PayoutRepositoryPort';

export class FakePayoutRepository implements PayoutRepositoryPort {
  private readonly attempts = new Map<string, PayoutAttemptRecord>();
  public saveCalls = 0;

  async wasProcessed(transferId: string): Promise<boolean> {
    return this.attempts.has(transferId);
  }

  async savePayoutAttempt(input: PayoutAttemptInput): Promise<void> {
    this.saveCalls += 1;
    this.attempts.set(input.transferId, { ...input, createdAt: new Date() });
  }

  async get(transferId: string): Promise<PayoutAttemptRecord | null> {
    return this.attempts.get(transferId) ?? null;
  }

  async listFailed(): Promise<PayoutAttemptRecord[]> {
    return [...this.attempts.values()].filter((attempt) => attempt.status === 'failed');
  }
}
```

```ts
// tests/fakes/FakeDlqRepository.ts
import { DlqRepositoryPort, DlqEntry } from '../../src/application/ports/DlqRepositoryPort';

export class FakeDlqRepository implements DlqRepositoryPort {
  public readonly entries: DlqEntry[] = [];

  async add(entry: { transferId: string; topic: string; payload: unknown; failureReason: string; attempts: number }): Promise<string> {
    const id = `dlq-${this.entries.length + 1}`;
    this.entries.push({ id, ...entry, createdAt: new Date(), reprocessedAt: null });
    return id;
  }

  async list(): Promise<DlqEntry[]> {
    return this.entries;
  }

  async get(id: string): Promise<DlqEntry | null> {
    return this.entries.find((entry) => entry.id === id) ?? null;
  }

  async markReprocessed(id: string): Promise<void> {
    const entry = this.entries.find((e) => e.id === id);
    if (entry) {
      entry.reprocessedAt = new Date();
    }
  }
}
```

- [ ] **Step 4: Write the failing test**

```ts
// tests/application/SendPayout.test.ts
import { describe, it, expect } from 'vitest';
import { SendPayout } from '../../src/application/use-cases/SendPayout';
import { FakePspClient } from '../fakes/FakePspClient';
import { FakePayoutRepository } from '../fakes/FakePayoutRepository';
import { FakeDlqRepository } from '../fakes/FakeDlqRepository';

describe('SendPayout', () => {
  it('records a successful payout', async () => {
    const pspClient = new FakePspClient({ succeeded: true, pspReference: 'ref-1', reason: null });
    const payoutRepository = new FakePayoutRepository();
    const dlqRepository = new FakeDlqRepository();
    const useCase = new SendPayout(payoutRepository, pspClient, dlqRepository);

    await useCase.execute({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500, postedAt: '2026-01-01T00:00:00.000Z' });

    const record = await payoutRepository.get('t1');
    expect(record).toMatchObject({ transferId: 't1', status: 'sent', pspReference: 'ref-1' });
  });

  it('records a declined payout as failed, not as a DLQ entry', async () => {
    const pspClient = new FakePspClient({ succeeded: false, pspReference: null, reason: 'destination account rejected the payment' });
    const payoutRepository = new FakePayoutRepository();
    const dlqRepository = new FakeDlqRepository();
    const useCase = new SendPayout(payoutRepository, pspClient, dlqRepository);

    await useCase.execute({ transferId: 't2', fromAccount: 'acc_1', toAccount: 'acc_psp_fail_demo', amount: 500, postedAt: '2026-01-01T00:00:00.000Z' });

    const record = await payoutRepository.get('t2');
    expect(record?.status).toBe('failed');
    expect(dlqRepository.entries).toHaveLength(0);
  });

  it('skips a transferId that was already processed', async () => {
    const pspClient = new FakePspClient();
    const payoutRepository = new FakePayoutRepository();
    const dlqRepository = new FakeDlqRepository();
    const useCase = new SendPayout(payoutRepository, pspClient, dlqRepository);

    await useCase.execute({ transferId: 't3', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500, postedAt: '2026-01-01T00:00:00.000Z' });
    await useCase.execute({ transferId: 't3', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500, postedAt: '2026-01-01T00:00:00.000Z' });

    expect(payoutRepository.saveCalls).toBe(1);
  });

  it('sends a malformed payload to the DLQ after exhausting retries', async () => {
    const pspClient = new FakePspClient();
    const payoutRepository = new FakePayoutRepository();
    const dlqRepository = new FakeDlqRepository();
    const useCase = new SendPayout(payoutRepository, pspClient, dlqRepository, { maxAttempts: 2, backoffMs: () => 1 });

    await useCase.execute({ transferId: 't4' });

    expect(dlqRepository.entries).toHaveLength(1);
    expect(dlqRepository.entries[0].topic).toBe('settlement-posted');
  });
});
```

- [ ] **Step 5: Run test to verify it fails**

Run: `npx vitest run tests/application/SendPayout.test.ts`
Expected: FAIL.

- [ ] **Step 6: Implement SendPayout**

```ts
// src/application/use-cases/SendPayout.ts
import { validateSettlementPosted } from '../../domain/services/SettlementPostedValidator';
import { PspClientPort } from '../ports/PspClientPort';
import { PayoutRepositoryPort } from '../ports/PayoutRepositoryPort';
import { DlqRepositoryPort } from '../ports/DlqRepositoryPort';

export interface SendPayoutOptions {
  maxAttempts?: number;
  backoffMs?: (attempt: number) => number;
}

const DEFAULT_BACKOFF_MS = (attempt: number): number => 100 * 2 ** (attempt - 1);

export class SendPayout {
  private readonly maxAttempts: number;
  private readonly backoffMs: (attempt: number) => number;

  constructor(
    private readonly payoutRepository: PayoutRepositoryPort,
    private readonly pspClient: PspClientPort,
    private readonly dlqRepository: DlqRepositoryPort,
    options: SendPayoutOptions = {},
  ) {
    this.maxAttempts = options.maxAttempts ?? 3;
    this.backoffMs = options.backoffMs ?? DEFAULT_BACKOFF_MS;
  }

  async execute(rawPayload: unknown): Promise<void> {
    const transferId = this.extractTransferId(rawPayload);
    if (transferId !== null && (await this.payoutRepository.wasProcessed(transferId))) {
      return;
    }

    let lastError: Error = new Error('unknown processing error');

    for (let attempt = 1; attempt <= this.maxAttempts; attempt += 1) {
      try {
        const event = validateSettlementPosted(rawPayload);
        const result = await this.pspClient.send({ transferId: event.transferId, toAccount: event.toAccount, amount: event.amount });
        await this.payoutRepository.savePayoutAttempt({
          transferId: event.transferId,
          toAccount: event.toAccount,
          amount: event.amount,
          status: result.succeeded ? 'sent' : 'failed',
          pspReference: result.pspReference,
        });
        return;
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        if (attempt < this.maxAttempts) {
          await this.delay(this.backoffMs(attempt));
        }
      }
    }

    await this.dlqRepository.add({
      transferId: transferId ?? 'unknown',
      topic: 'settlement-posted',
      payload: rawPayload,
      failureReason: lastError.message,
      attempts: this.maxAttempts,
    });
  }

  private extractTransferId(rawPayload: unknown): string | null {
    if (typeof rawPayload === 'object' && rawPayload !== null && 'transferId' in rawPayload) {
      const value = (rawPayload as { transferId: unknown }).transferId;
      return typeof value === 'string' ? value : null;
    }
    return null;
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
```

- [ ] **Step 7: Run test to verify it passes**

Run: `npx vitest run tests/application/SendPayout.test.ts`
Expected: PASS (4 tests)

- [ ] **Step 8: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-19-payout-application
git add services/payout/src/domain services/payout/src/application services/payout/tests/application services/payout/tests/fakes
git commit -m "Implement the SendPayout use case"
git push -u origin task-19-payout-application
```

---

## Task 20: Payout Postgres adapters

**Files:**
- Create: `services/payout/db/migrate.ts` (identical to Task 4's, with `SCHEMA = 'payout'`)
- Create: `services/payout/db/migrations/001_create_payout_attempts.sql`
- Create: `services/payout/db/migrations/002_create_outbox.sql`
- Create: `services/payout/db/migrations/003_create_dlq_events.sql`
- Create: `services/payout/src/adapters/outbound/postgres/PostgresPayoutRepository.ts`
- Create: `services/payout/src/adapters/outbound/postgres/PostgresDlqRepository.ts`
- Test: `services/payout/tests/adapters/PostgresPayoutRepository.test.ts`
- Test: `services/payout/tests/adapters/PostgresDlqRepository.test.ts`

**Interfaces:**
- Consumes: `PayoutRepositoryPort`, `DlqRepositoryPort` from Task 19.
- Produces: `PostgresPayoutRepository`, `PostgresDlqRepository`.

- [ ] **Step 1: Write db/migrate.ts** — identical to Task 4's, with `const SCHEMA = 'payout';`.

- [ ] **Step 2: Write the migrations**

```sql
-- db/migrations/001_create_payout_attempts.sql
CREATE TABLE IF NOT EXISTS payout_attempts (
  transfer_id TEXT PRIMARY KEY,
  to_account TEXT NOT NULL,
  amount NUMERIC NOT NULL,
  status TEXT NOT NULL,
  psp_reference TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

```sql
-- db/migrations/002_create_outbox.sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS outbox (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  topic TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);
```

```sql
-- db/migrations/003_create_dlq_events.sql
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
```

- [ ] **Step 3: Run the migration**

Run: `cd services/payout && DATABASE_URL=postgres://postgres:postgres@localhost:5432/settlement_ledger npm run migrate`
Expected: prints each applied migration filename.

- [ ] **Step 4: Write the failing adapter tests**

```ts
// tests/adapters/PostgresPayoutRepository.test.ts
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgresPayoutRepository } from '../../src/adapters/outbound/postgres/PostgresPayoutRepository';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';

let pool: Pool;
let repository: PostgresPayoutRepository;

beforeAll(() => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=payout' });
  repository = new PostgresPayoutRepository(pool);
});

beforeEach(async () => {
  await pool.query('TRUNCATE payout_attempts, outbox CASCADE');
});

afterAll(async () => {
  await pool.end();
});

describe('PostgresPayoutRepository', () => {
  it('reports a transferId as unprocessed before any attempt is saved', async () => {
    expect(await repository.wasProcessed('t1')).toBe(false);
  });

  it('saves a successful attempt and publishes a payout-result outbox row', async () => {
    await repository.savePayoutAttempt({ transferId: 't1', toAccount: 'acc_2', amount: 500, status: 'sent', pspReference: 'ref-1' });

    expect(await repository.wasProcessed('t1')).toBe(true);
    const record = await repository.get('t1');
    expect(record).toMatchObject({ transferId: 't1', toAccount: 'acc_2', amount: 500, status: 'sent', pspReference: 'ref-1' });

    const outbox = await pool.query("SELECT * FROM outbox WHERE topic = 'payout-result'");
    expect(outbox.rows).toHaveLength(1);
    expect(outbox.rows[0].payload).toMatchObject({ transferId: 't1', outcome: 'succeeded', pspReference: 'ref-1' });
  });

  it('upserts on retry: a second save for the same transferId updates the row instead of duplicating it', async () => {
    await repository.savePayoutAttempt({ transferId: 't2', toAccount: 'acc_2', amount: 500, status: 'failed', pspReference: null });
    await repository.savePayoutAttempt({ transferId: 't2', toAccount: 'acc_2', amount: 500, status: 'sent', pspReference: 'ref-2' });

    const record = await repository.get('t2');
    expect(record?.status).toBe('sent');

    const all = await pool.query("SELECT * FROM payout_attempts WHERE transfer_id = 't2'");
    expect(all.rows).toHaveLength(1);
  });

  it('lists only failed attempts', async () => {
    await repository.savePayoutAttempt({ transferId: 't3', toAccount: 'acc_2', amount: 100, status: 'sent', pspReference: 'ref-3' });
    await repository.savePayoutAttempt({ transferId: 't4', toAccount: 'acc_psp_fail_demo', amount: 100, status: 'failed', pspReference: null });

    const failed = await repository.listFailed({ limit: 20, offset: 0 });
    expect(failed).toHaveLength(1);
    expect(failed[0].transferId).toBe('t4');
  });
});
```

```ts
// tests/adapters/PostgresDlqRepository.test.ts
import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { Pool } from 'pg';
import { PostgresDlqRepository } from '../../src/adapters/outbound/postgres/PostgresDlqRepository';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';

let pool: Pool;
let repository: PostgresDlqRepository;

beforeAll(() => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=payout' });
  repository = new PostgresDlqRepository(pool);
});

beforeEach(async () => {
  await pool.query('TRUNCATE dlq_events CASCADE');
});

afterAll(async () => {
  await pool.end();
});

describe('PostgresDlqRepository', () => {
  it('adds, lists, and marks an entry as reprocessed', async () => {
    const id = await repository.add({ transferId: 't1', topic: 'settlement-posted', payload: {}, failureReason: 'boom', attempts: 3 });
    expect(await repository.list({ limit: 20, offset: 0 })).toHaveLength(1);
    await repository.markReprocessed(id);
    expect(await repository.list({ limit: 20, offset: 0 })).toHaveLength(0);
  });

  it('returns null for a non-UUID id', async () => {
    expect(await repository.get('not-a-uuid')).toBeNull();
  });
});
```

- [ ] **Step 5: Run tests to verify they fail**

Run: `npx vitest run tests/adapters`
Expected: FAIL.

- [ ] **Step 6: Implement the adapters**

```ts
// src/adapters/outbound/postgres/PostgresPayoutRepository.ts
import { Pool } from 'pg';
import { PayoutRepositoryPort, PayoutAttemptInput, PayoutAttemptRecord } from '../../../application/ports/PayoutRepositoryPort';

export class PostgresPayoutRepository implements PayoutRepositoryPort {
  constructor(private readonly pool: Pool) {}

  async wasProcessed(transferId: string): Promise<boolean> {
    const result = await this.pool.query('SELECT 1 FROM payout_attempts WHERE transfer_id = $1', [transferId]);
    return result.rows.length > 0;
  }

  async savePayoutAttempt(input: PayoutAttemptInput): Promise<void> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO payout_attempts (transfer_id, to_account, amount, status, psp_reference)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (transfer_id) DO UPDATE SET status = excluded.status, psp_reference = excluded.psp_reference`,
        [input.transferId, input.toAccount, input.amount, input.status, input.pspReference],
      );
      const outboxPayload = {
        transferId: input.transferId,
        outcome: input.status === 'sent' ? 'succeeded' : 'failed',
        pspReference: input.pspReference,
      };
      await client.query("INSERT INTO outbox (topic, payload) VALUES ('payout-result', $1)", [JSON.stringify(outboxPayload)]);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async get(transferId: string): Promise<PayoutAttemptRecord | null> {
    const result = await this.pool.query('SELECT * FROM payout_attempts WHERE transfer_id = $1', [transferId]);
    if (result.rows.length === 0) {
      return null;
    }
    return this.toRecord(result.rows[0]);
  }

  async listFailed(options: { limit: number; offset: number }): Promise<PayoutAttemptRecord[]> {
    const result = await this.pool.query(
      "SELECT * FROM payout_attempts WHERE status = 'failed' ORDER BY created_at ASC LIMIT $1 OFFSET $2",
      [options.limit, options.offset],
    );
    return result.rows.map((row) => this.toRecord(row));
  }

  private toRecord(row: Record<string, unknown>): PayoutAttemptRecord {
    return {
      transferId: row.transfer_id as string,
      toAccount: row.to_account as string,
      amount: Number(row.amount),
      status: row.status as 'sent' | 'failed',
      pspReference: row.psp_reference as string | null,
      createdAt: row.created_at as Date,
    };
  }
}
```

```ts
// src/adapters/outbound/postgres/PostgresDlqRepository.ts
import { Pool } from 'pg';
import { DlqEntry, DlqRepositoryPort } from '../../../application/ports/DlqRepositoryPort';

interface DlqRow {
  id: string;
  transfer_id: string;
  topic: string;
  payload: unknown;
  failure_reason: string;
  attempts: number;
  created_at: Date;
  reprocessed_at: Date | null;
}

function toDlqEntry(row: DlqRow): DlqEntry {
  return {
    id: row.id,
    transferId: row.transfer_id,
    topic: row.topic,
    payload: row.payload,
    failureReason: row.failure_reason,
    attempts: row.attempts,
    createdAt: row.created_at,
    reprocessedAt: row.reprocessed_at,
  };
}

export class PostgresDlqRepository implements DlqRepositoryPort {
  constructor(private readonly pool: Pool) {}

  async add(entry: { transferId: string; topic: string; payload: unknown; failureReason: string; attempts: number }): Promise<string> {
    const result = await this.pool.query<{ id: string }>(
      `INSERT INTO dlq_events (transfer_id, topic, payload, failure_reason, attempts)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id`,
      [entry.transferId, entry.topic, JSON.stringify(entry.payload), entry.failureReason, entry.attempts],
    );
    return result.rows[0].id;
  }

  async list(options: { limit: number; offset: number }): Promise<DlqEntry[]> {
    const result = await this.pool.query<DlqRow>(
      'SELECT * FROM dlq_events WHERE reprocessed_at IS NULL ORDER BY created_at ASC LIMIT $1 OFFSET $2',
      [options.limit, options.offset],
    );
    return result.rows.map(toDlqEntry);
  }

  async get(id: string): Promise<DlqEntry | null> {
    try {
      const result = await this.pool.query<DlqRow>('SELECT * FROM dlq_events WHERE id = $1', [id]);
      return result.rows[0] ? toDlqEntry(result.rows[0]) : null;
    } catch (error) {
      if (error instanceof Error && 'code' in error && (error as { code: string }).code === '22P02') {
        return null;
      }
      throw error;
    }
  }

  async markReprocessed(id: string): Promise<void> {
    await this.pool.query('UPDATE dlq_events SET reprocessed_at = now() WHERE id = $1', [id]);
  }
}
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `npx vitest run tests/adapters`
Expected: PASS (6 tests)

- [ ] **Step 8: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-20-payout-postgres-adapters
git add services/payout/db services/payout/src/adapters/outbound/postgres services/payout/tests/adapters
git commit -m "Implement payout Postgres adapters"
git push -u origin task-20-payout-postgres-adapters
```

---

## Task 21: Payout HTTP PSP client adapter

**Files:**
- Create: `services/payout/src/adapters/outbound/psp/HttpPspClient.ts`
- Test: `services/payout/tests/adapters/HttpPspClient.test.ts`

**Interfaces:**
- Consumes: `PspClientPort` from Task 19; `mock-payout-psp`'s `POST /psp/send` contract from Task 18.
- Produces: `HttpPspClient` constructed with `(baseUrl: string)`, validating the PSP's HTTP response with zod before mapping it to `PspSendResult`.

- [ ] **Step 1: Write the failing adapter test (against the real mock-payout-psp)**

```ts
// tests/adapters/HttpPspClient.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Server } from 'node:http';
import { createMockPayoutPspApp } from '../../../../mock-payout-psp/server';
import { HttpPspClient } from '../../src/adapters/outbound/psp/HttpPspClient';

let server: Server;
let baseUrl: string;

beforeAll(async () => {
  const app = createMockPayoutPspApp();
  await new Promise<void>((resolve) => {
    server = app.listen(0, () => resolve());
  });
  const address = server.address();
  const port = typeof address === 'object' && address !== null ? address.port : 0;
  baseUrl = `http://localhost:${port}`;
});

afterAll(() => {
  server.close();
});

describe('HttpPspClient', () => {
  it('maps a successful PSP response to a succeeded result', async () => {
    const client = new HttpPspClient(baseUrl);
    const result = await client.send({ transferId: 't1', toAccount: 'acc_2', amount: 500 });
    expect(result.succeeded).toBe(true);
    expect(typeof result.pspReference).toBe('string');
  });

  it('maps a declined PSP response to a failed result', async () => {
    const client = new HttpPspClient(baseUrl);
    const result = await client.send({ transferId: 't2', toAccount: 'acc_psp_fail_demo', amount: 500 });
    expect(result).toEqual({ succeeded: false, pspReference: null, reason: 'destination account rejected the payment' });
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/adapters/HttpPspClient.test.ts`
Expected: FAIL — `HttpPspClient` does not exist. (Note: this test imports the mock PSP's `server.ts` directly from its sibling top-level package for test convenience only; production code never does this — `HttpPspClient` only ever talks to it over HTTP, via `pspBaseUrl`.)

- [ ] **Step 3: Implement HttpPspClient**

```ts
// src/adapters/outbound/psp/HttpPspClient.ts
import { z } from 'zod';
import { PspClientPort, PspSendInput, PspSendResult } from '../../../application/ports/PspClientPort';

const pspResponseSchema = z.union([
  z.object({ accepted: z.literal(true), pspReference: z.string() }),
  z.object({ accepted: z.literal(false), reason: z.string() }),
]);

export class HttpPspClient implements PspClientPort {
  constructor(private readonly baseUrl: string) {}

  async send(input: PspSendInput): Promise<PspSendResult> {
    const response = await fetch(`${this.baseUrl}/psp/send`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input),
    });

    if (!response.ok) {
      throw new Error(`PSP request failed with status ${response.status}`);
    }

    const parsed = pspResponseSchema.parse(await response.json());

    if (parsed.accepted) {
      return { succeeded: true, pspReference: parsed.pspReference, reason: null };
    }
    return { succeeded: false, pspReference: null, reason: parsed.reason };
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/adapters/HttpPspClient.test.ts`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-21-payout-psp-client
git add services/payout/src/adapters/outbound/psp services/payout/tests/adapters/HttpPspClient.test.ts
git commit -m "Implement the HTTP PSP client adapter"
git push -u origin task-21-payout-psp-client
```

---

## Task 22: Payout Kafka consumer, HTTP routes, and retry endpoint

**Files:**
- Create: `services/payout/src/application/ports/KafkaProducerPort.ts`
- Create: `services/payout/src/application/use-cases/RetryPayout.ts`
- Create: `services/payout/src/application/use-cases/ReprocessDlqEvent.ts`
- Create: `services/payout/src/adapters/inbound/kafka/settlementPostedConsumer.ts`
- Create: `services/payout/src/adapters/inbound/http/payoutRouter.ts`
- Create: `services/payout/src/adapters/inbound/http/dlqRouter.ts`
- Create: `services/payout/src/adapters/outbound/kafka/KafkaProducerAdapter.ts`
- Modify: `services/payout/src/adapters/inbound/http/app.ts`
- Modify: `services/payout/src/main.ts`
- Test: `services/payout/tests/application/RetryPayout.test.ts`
- Test: `services/payout/tests/integration/settlementPostedFlow.test.ts`

**Interfaces:**
- Consumes: `SendPayout` (Task 19), `PayoutRepositoryPort`, `PspClientPort`, `DlqRepositoryPort` (Tasks 19-21).
- Produces: `RetryPayout.execute(transferId): Promise<PayoutAttemptRecord>`, `GET /payout/failed`, `POST /payout/:transferId/retry`, `GET /dlq`, `POST /dlq/:id/reprocess`.

- [ ] **Step 1: Write the Kafka producer port/adapter** — identical in content to Task 14's `KafkaProducerPort.ts`/`KafkaProducerAdapter.ts`.

- [ ] **Step 2: Write RetryPayout and its failing test**

```ts
// tests/application/RetryPayout.test.ts
import { describe, it, expect } from 'vitest';
import { RetryPayout, PayoutAttemptNotFoundError } from '../../src/application/use-cases/RetryPayout';
import { FakePayoutRepository } from '../fakes/FakePayoutRepository';
import { FakePspClient } from '../fakes/FakePspClient';

describe('RetryPayout', () => {
  it('re-attempts the PSP call using the stored transfer data', async () => {
    const payoutRepository = new FakePayoutRepository();
    await payoutRepository.savePayoutAttempt({ transferId: 't1', toAccount: 'acc_2', amount: 500, status: 'failed', pspReference: null });
    const pspClient = new FakePspClient({ succeeded: true, pspReference: 'retry-ref', reason: null });
    const useCase = new RetryPayout(payoutRepository, pspClient);

    const result = await useCase.execute('t1');

    expect(result).toMatchObject({ transferId: 't1', status: 'sent', pspReference: 'retry-ref' });
    expect(pspClient.lastInput).toEqual({ transferId: 't1', toAccount: 'acc_2', amount: 500 });
  });

  it('throws when the transferId has no prior payout attempt', async () => {
    const payoutRepository = new FakePayoutRepository();
    const pspClient = new FakePspClient();
    const useCase = new RetryPayout(payoutRepository, pspClient);

    await expect(useCase.execute('missing')).rejects.toThrow(PayoutAttemptNotFoundError);
  });
});
```

Run: `npx vitest run tests/application/RetryPayout.test.ts`
Expected: FAIL, then implement:

```ts
// src/application/use-cases/RetryPayout.ts
import { PayoutRepositoryPort, PayoutAttemptRecord } from '../ports/PayoutRepositoryPort';
import { PspClientPort } from '../ports/PspClientPort';

export class PayoutAttemptNotFoundError extends Error {}

export class RetryPayout {
  constructor(
    private readonly payoutRepository: PayoutRepositoryPort,
    private readonly pspClient: PspClientPort,
  ) {}

  async execute(transferId: string): Promise<PayoutAttemptRecord> {
    const existing = await this.payoutRepository.get(transferId);
    if (existing === null) {
      throw new PayoutAttemptNotFoundError(`no payout attempt found for transferId "${transferId}"`);
    }

    const result = await this.pspClient.send({ transferId, toAccount: existing.toAccount, amount: existing.amount });
    await this.payoutRepository.savePayoutAttempt({
      transferId,
      toAccount: existing.toAccount,
      amount: existing.amount,
      status: result.succeeded ? 'sent' : 'failed',
      pspReference: result.pspReference,
    });

    const updated = await this.payoutRepository.get(transferId);
    return updated as PayoutAttemptRecord;
  }
}
```

Run: `npx vitest run tests/application/RetryPayout.test.ts`
Expected: PASS (2 tests)

- [ ] **Step 3: Write ReprocessDlqEvent** — identical in content to Task 14's `ReprocessDlqEvent.ts`.

- [ ] **Step 4: Write the Kafka consumer**

```ts
// src/adapters/inbound/kafka/settlementPostedConsumer.ts
import { Consumer } from 'kafkajs';
import { SendPayout } from '../../../application/use-cases/SendPayout';

export const SETTLEMENT_POSTED_TOPIC = 'settlement-posted';

export async function startSettlementPostedConsumer(consumer: Consumer, sendPayout: SendPayout): Promise<void> {
  await consumer.subscribe({ topic: SETTLEMENT_POSTED_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      let rawPayload: unknown;
      try {
        rawPayload = JSON.parse(message.value?.toString() ?? '{}');
      } catch {
        rawPayload = {};
      }
      await sendPayout.execute(rawPayload);
    },
  });
}
```

- [ ] **Step 5: Write the HTTP routes**

```ts
// src/adapters/inbound/http/payoutRouter.ts
import { Router, Request, Response, NextFunction } from 'express';
import { PayoutRepositoryPort } from '../../../application/ports/PayoutRepositoryPort';
import { RetryPayout, PayoutAttemptNotFoundError } from '../../../application/use-cases/RetryPayout';

export function createPayoutRouter(payoutRepository: PayoutRepositoryPort, retryPayout: RetryPayout): Router {
  const router = Router();

  router.get('/payout/failed', async (req: Request, res: Response, next: NextFunction) => {
    try {
      const limit = typeof req.query.limit === 'string' ? Number(req.query.limit) : 20;
      const offset = typeof req.query.offset === 'string' ? Number(req.query.offset) : 0;
      if (!Number.isFinite(limit) || limit < 0 || !Number.isFinite(offset) || offset < 0) {
        res.status(400).json({ error: 'invalid limit or offset' });
        return;
      }
      const attempts = await payoutRepository.listFailed({ limit, offset });
      res.status(200).json({ attempts });
    } catch (error) {
      next(error);
    }
  });

  router.post('/payout/:transferId/retry', async (req: Request, res: Response, next: NextFunction) => {
    try {
      const result = await retryPayout.execute(req.params.transferId);
      res.status(200).json(result);
    } catch (error) {
      if (error instanceof PayoutAttemptNotFoundError) {
        res.status(404).json({ error: 'payout attempt not found' });
        return;
      }
      next(error);
    }
  });

  return router;
}
```

```ts
// src/adapters/inbound/http/dlqRouter.ts
```
(Identical in content to Task 14's `dlqRouter.ts`.)

- [ ] **Step 6: Wire the routes into app.ts**

```ts
// src/adapters/inbound/http/app.ts
import express, { Express, Request, Response, NextFunction } from 'express';
import { PayoutRepositoryPort } from '../../../application/ports/PayoutRepositoryPort';
import { DlqRepositoryPort } from '../../../application/ports/DlqRepositoryPort';
import { RetryPayout } from '../../../application/use-cases/RetryPayout';
import { ReprocessDlqEvent } from '../../../application/use-cases/ReprocessDlqEvent';
import { createPayoutRouter } from './payoutRouter';
import { createDlqRouter } from './dlqRouter';

export interface AppDependencies {
  payoutRepository?: PayoutRepositoryPort;
  retryPayout?: RetryPayout;
  dlqRepository?: DlqRepositoryPort;
  reprocessDlqEvent?: ReprocessDlqEvent;
}

export function createApp(deps: AppDependencies): Express {
  const app = express();
  app.use(express.json());

  app.get('/health', (_req: Request, res: Response) => {
    res.status(200).json({ status: 'ok' });
  });

  if (deps.payoutRepository && deps.retryPayout) {
    app.use(createPayoutRouter(deps.payoutRepository, deps.retryPayout));
  }
  if (deps.dlqRepository && deps.reprocessDlqEvent) {
    app.use(createDlqRouter(deps.dlqRepository, deps.reprocessDlqEvent));
  }

  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof SyntaxError && 'status' in err && (err as { status?: number }).status === 400) {
      res.status(400).json({ error: 'invalid JSON body' });
      return;
    }
    res.status(500).json({ error: 'internal server error' });
  });

  return app;
}
```

- [ ] **Step 7: Write the failing integration test**

```ts
// tests/integration/settlementPostedFlow.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Pool } from 'pg';
import { Server } from 'node:http';
import { createMockPayoutPspApp } from '../../../../mock-payout-psp/server';
import { PostgresPayoutRepository } from '../../src/adapters/outbound/postgres/PostgresPayoutRepository';
import { PostgresDlqRepository } from '../../src/adapters/outbound/postgres/PostgresDlqRepository';
import { HttpPspClient } from '../../src/adapters/outbound/psp/HttpPspClient';
import { SendPayout } from '../../src/application/use-cases/SendPayout';
import { startSettlementPostedConsumer, SETTLEMENT_POSTED_TOPIC } from '../../src/adapters/inbound/kafka/settlementPostedConsumer';
import { createKafka } from '../../src/adapters/outbound/kafka/KafkaProducerAdapter';

const DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/settlement_ledger';
const KAFKA_BROKERS = (process.env.TEST_KAFKA_BROKERS ?? 'localhost:9092').split(',');

let pool: Pool;
let pspServer: Server;

async function waitFor<T>(check: () => Promise<T | null>, timeoutMs = 10000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const result = await check();
    if (result !== null) {
      return result;
    }
    if (Date.now() - start > timeoutMs) {
      throw new Error('waitFor timed out');
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
}

beforeAll(async () => {
  pool = new Pool({ connectionString: DATABASE_URL, options: '-c search_path=payout' });
  await pool.query('TRUNCATE payout_attempts, outbox, dlq_events CASCADE');

  const pspApp = createMockPayoutPspApp();
  await new Promise<void>((resolve) => {
    pspServer = pspApp.listen(0, () => resolve());
  });
  const address = pspServer.address();
  const pspPort = typeof address === 'object' && address !== null ? address.port : 0;

  const kafka = createKafka(KAFKA_BROKERS, 'payout-test');
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: SETTLEMENT_POSTED_TOPIC, numPartitions: 1 }] });
  await admin.disconnect();

  const producer = kafka.producer();
  await producer.connect();

  const payoutRepository = new PostgresPayoutRepository(pool);
  const dlqRepository = new PostgresDlqRepository(pool);
  const pspClient = new HttpPspClient(`http://localhost:${pspPort}`);
  const sendPayout = new SendPayout(payoutRepository, pspClient, dlqRepository);

  const consumer = kafka.consumer({ groupId: `payout-flow-test-${Date.now()}` });
  await consumer.connect();
  await startSettlementPostedConsumer(consumer, sendPayout);

  await new Promise((resolve) => setTimeout(resolve, 5000));

  (global as Record<string, unknown>).__producer = producer;
}, 30000);

afterAll(async () => {
  await pool.end();
  pspServer.close();
});

describe('settlement-posted consumption in payout (integration)', () => {
  it('sends the payout and records a sent attempt for a normal account', async () => {
    const producer = (global as Record<string, unknown>).__producer as import('kafkajs').Producer;
    await producer.send({
      topic: SETTLEMENT_POSTED_TOPIC,
      messages: [{
        key: 'pt1',
        value: JSON.stringify({ transferId: 'pt1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 200, postedAt: new Date().toISOString() }),
      }],
    });

    const row = await waitFor(async () => {
      const result = await pool.query("SELECT * FROM payout_attempts WHERE transfer_id = 'pt1'");
      return result.rows[0] ?? null;
    });

    expect(row.status).toBe('sent');
  }, 30000);

  it('records a failed attempt for the magic failure account, without a DLQ entry', async () => {
    const producer = (global as Record<string, unknown>).__producer as import('kafkajs').Producer;
    await producer.send({
      topic: SETTLEMENT_POSTED_TOPIC,
      messages: [{
        key: 'pt2',
        value: JSON.stringify({ transferId: 'pt2', fromAccount: 'acc_1', toAccount: 'acc_psp_fail_demo', amount: 200, postedAt: new Date().toISOString() }),
      }],
    });

    const row = await waitFor(async () => {
      const result = await pool.query("SELECT * FROM payout_attempts WHERE transfer_id = 'pt2'");
      return result.rows[0] ?? null;
    });

    expect(row.status).toBe('failed');

    const dlq = await pool.query("SELECT * FROM dlq_events WHERE transfer_id = 'pt2'");
    expect(dlq.rows).toHaveLength(0);
  }, 30000);
});
```

- [ ] **Step 8: Run test to verify it passes**

Run: `DATABASE_URL=postgres://postgres:postgres@localhost:5432/settlement_ledger npm run migrate && npx vitest run tests/integration/settlementPostedFlow.test.ts`
Expected: PASS (2 tests)

- [ ] **Step 9: Wire everything into main.ts**

```ts
// src/main.ts
import { Pool } from 'pg';
import { loadEnv } from './config/env';
import { createApp } from './adapters/inbound/http/app';
import { createKafka, KafkaProducerAdapter } from './adapters/outbound/kafka/KafkaProducerAdapter';
import { PostgresPayoutRepository } from './adapters/outbound/postgres/PostgresPayoutRepository';
import { PostgresDlqRepository } from './adapters/outbound/postgres/PostgresDlqRepository';
import { HttpPspClient } from './adapters/outbound/psp/HttpPspClient';
import { SendPayout } from './application/use-cases/SendPayout';
import { RetryPayout } from './application/use-cases/RetryPayout';
import { ReprocessDlqEvent } from './application/use-cases/ReprocessDlqEvent';
import { startSettlementPostedConsumer } from './adapters/inbound/kafka/settlementPostedConsumer';

async function main(): Promise<void> {
  const env = loadEnv();
  const pool = new Pool({ connectionString: env.databaseUrl, options: '-c search_path=payout' });

  const kafka = createKafka(env.kafkaBrokers, 'payout');
  const producer = kafka.producer();
  await producer.connect();
  const producerAdapter = new KafkaProducerAdapter(producer);

  const payoutRepository = new PostgresPayoutRepository(pool);
  const dlqRepository = new PostgresDlqRepository(pool);
  const pspClient = new HttpPspClient(env.pspBaseUrl);

  const sendPayout = new SendPayout(payoutRepository, pspClient, dlqRepository);
  const retryPayout = new RetryPayout(payoutRepository, pspClient);
  const reprocessDlqEvent = new ReprocessDlqEvent(dlqRepository, producerAdapter);

  const consumer = kafka.consumer({ groupId: 'payout' });
  await consumer.connect();
  await startSettlementPostedConsumer(consumer, sendPayout);

  const app = createApp({ payoutRepository, retryPayout, dlqRepository, reprocessDlqEvent });
  app.listen(env.port, () => {
    console.log(`payout listening on port ${env.port}`);
  });
}

main().catch((error) => {
  console.error('fatal startup error', error);
  process.exit(1);
});
```

- [ ] **Step 10: Typecheck, lint, and run the full payout test suite**

Run: `npm run typecheck && npm run lint && npm test`
Expected: all clean.

- [ ] **Step 11: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-22-payout-consumer-http
git add services/payout/src services/payout/tests/application/RetryPayout.test.ts services/payout/tests/integration
git commit -m "Add the settlement-posted consumer, payout retry endpoint, and DLQ routes"
git push -u origin task-22-payout-consumer-http
```

---

## Task 23: Payout outbox poller and Kafka producer wiring

**Files:**
- Create: `services/payout/src/application/ports/OutboxRepositoryPort.ts`
- Create: `services/payout/src/application/services/OutboxPoller.ts`
- Create: `services/payout/src/adapters/outbound/postgres/PostgresOutboxRepository.ts`
- Modify: `services/payout/src/main.ts`
- Test: `services/payout/tests/adapters/outboxPoller.integration.test.ts`

**Interfaces:**
- Identical shapes to Task 8's `OutboxRepositoryPort`, `OutboxPoller`, `PostgresOutboxRepository` — repeated independently for `payout`. Note: `payout`'s outbox rows are already written by Task 20's `PostgresPayoutRepository.savePayoutAttempt`, so this task only adds the poller that drains them — no new business write.

- [ ] **Step 1: Write the port, poller, and adapter** — identical in content to Task 8's `OutboxRepositoryPort.ts`, `OutboxPoller.ts`, and `PostgresOutboxRepository.ts`.

- [ ] **Step 2: Write the failing integration test** — identical in shape to Task 8's, with `options: '-c search_path=payout'`, topic `'payout-result'`, test payload `{ transferId: 'ob1', outcome: 'succeeded', pspReference: 'ref-1' }`, client id `'payout-test'`.

- [ ] **Step 3: Run test to verify it fails, then it passes**

Run: `npx vitest run tests/adapters/outboxPoller.integration.test.ts`
Expected: FAIL, then PASS once Step 1 is in place.

- [ ] **Step 4: Wire the poller into main.ts**

```ts
// src/main.ts (add alongside the existing imports and wiring from Task 22)
import { PostgresOutboxRepository } from './adapters/outbound/postgres/PostgresOutboxRepository';
import { OutboxPoller } from './application/services/OutboxPoller';

// inside main(), after producerAdapter is created:
const outboxRepository = new PostgresOutboxRepository(pool);
const outboxPoller = new OutboxPoller(outboxRepository, producerAdapter);
outboxPoller.start();
```

- [ ] **Step 5: Typecheck, lint, and run the full payout test suite**

Run: `npm run typecheck && npm run lint && npm test`
Expected: all clean, all tests passing.

- [ ] **Step 6: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-23-payout-outbox-poller
git add services/payout/src services/payout/tests/adapters/outboxPoller.integration.test.ts
git commit -m "Add the payout outbox poller and Kafka producer"
git push -u origin task-23-payout-outbox-poller
```

---

## Task 24: End-to-end test across all three services

**Files:**
- Create: `tests-e2e/package.json`, `tsconfig.json`, `vitest.config.mts`
- Create: `tests-e2e/tests/settlementFlow.test.ts`

**Interfaces:**
- Consumes: `GET /health`, `POST /transfers`, `GET /transfers/:transferId` (authorization); `GET /accounts/:accountId/entries` (ledger); `GET /payout/failed` (payout). Treats all three services as black boxes over HTTP — this package never imports their source code.

- [ ] **Step 1: package.json**

```json
{
  "name": "tests-e2e",
  "version": "0.1.0",
  "private": true,
  "engines": { "node": ">=18" },
  "scripts": {
    "test": "vitest run"
  },
  "devDependencies": {
    "@types/node": "^22.5.0",
    "typescript": "^5.5.4",
    "vitest": "^5.0.0"
  }
}
```

- [ ] **Step 2: tsconfig.json**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "CommonJS",
    "moduleResolution": "Node",
    "lib": ["ES2022"],
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "resolveJsonModule": true,
    "noEmit": true,
    "types": ["node"]
  },
  "include": ["tests"]
}
```

- [ ] **Step 3: vitest.config.mts**

```ts
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    include: ['tests/**/*.test.ts'],
    testTimeout: 60000,
    fileParallelism: false,
  },
});
```

- [ ] **Step 4: Write the failing end-to-end test**

```ts
// tests/settlementFlow.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { ChildProcess, spawn } from 'node:child_process';
import path from 'node:path';

const AUTHORIZATION_URL = 'http://localhost:3001';
const LEDGER_URL = 'http://localhost:3002';
const PAYOUT_URL = 'http://localhost:3003';
const PSP_URL = 'http://localhost:4003';

let authorizationProcess: ChildProcess;
let ledgerProcess: ChildProcess;
let payoutProcess: ChildProcess;
let pspProcess: ChildProcess;

function spawnService(name: string, port: number): ChildProcess {
  const cwd = path.join(__dirname, '..', '..', 'services', name);
  return spawn('npx', ['tsx', 'src/main.ts'], {
    cwd,
    env: {
      ...process.env,
      PORT: String(port),
      DATABASE_URL: 'postgres://postgres:postgres@localhost:5432/settlement_ledger',
      KAFKA_BROKERS: 'localhost:9092',
      PSP_BASE_URL: PSP_URL,
    },
    stdio: 'pipe',
  });
}

async function waitForHealth(url: string, timeoutMs = 30000): Promise<void> {
  const start = Date.now();
  for (;;) {
    try {
      const response = await fetch(`${url}/health`);
      if (response.ok) {
        return;
      }
    } catch {
      // not up yet
    }
    if (Date.now() - start > timeoutMs) {
      throw new Error(`service at ${url} did not become healthy in time`);
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
}

async function waitFor<T>(check: () => Promise<T | null>, timeoutMs = 20000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const result = await check();
    if (result !== null) {
      return result;
    }
    if (Date.now() - start > timeoutMs) {
      throw new Error('waitFor timed out');
    }
    await new Promise((resolve) => setTimeout(resolve, 300));
  }
}

beforeAll(async () => {
  pspProcess = spawn('npx', ['tsx', 'server.ts'], {
    cwd: path.join(__dirname, '..', '..', 'mock-payout-psp'),
    env: { ...process.env, PORT: '4003' },
    stdio: 'pipe',
  });

  authorizationProcess = spawnService('authorization', 3001);
  ledgerProcess = spawnService('ledger', 3002);
  payoutProcess = spawnService('payout', 3003);

  await waitForHealth(PSP_URL);
  await Promise.all([waitForHealth(AUTHORIZATION_URL), waitForHealth(LEDGER_URL), waitForHealth(PAYOUT_URL)]);

  // give every service's Kafka consumer group time to finish joining before the test publishes
  await new Promise((resolve) => setTimeout(resolve, 5000));
}, 60000);

afterAll(() => {
  authorizationProcess.kill();
  ledgerProcess.kill();
  payoutProcess.kill();
  pspProcess.kill();
});

describe('end-to-end settlement flow', () => {
  it('settles a transfer between two demo accounts and posts it to the ledger', async () => {
    const transferId = `e2e-happy-${Date.now()}`;

    const createResponse = await fetch(`${AUTHORIZATION_URL}/transfers`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ transferId, fromAccount: 'acc_demo_1', toAccount: 'acc_demo_2', amount: 100 }),
    });
    expect(createResponse.status).toBe(201);
    const created = await createResponse.json();
    expect(created.status).toBe('approved');

    const confirmed = await waitFor(async () => {
      const response = await fetch(`${AUTHORIZATION_URL}/transfers/${transferId}`);
      const body = await response.json();
      return body.status === 'confirmed' ? body : null;
    });
    expect(confirmed.status).toBe('confirmed');

    const entry = await waitFor(async () => {
      const response = await fetch(`${LEDGER_URL}/accounts/acc_demo_1/entries`);
      const body = await response.json();
      const match = body.entries.find((e: { transferId: string }) => e.transferId === transferId);
      return match ?? null;
    });
    expect(entry.direction).toBe('debit');
    expect(entry.amount).toBe(100);

    const failedResponse = await fetch(`${PAYOUT_URL}/payout/failed`);
    const failedBody = await failedResponse.json();
    expect(failedBody.attempts.find((attempt: { transferId: string }) => attempt.transferId === transferId)).toBeUndefined();
  }, 30000);

  it('settles internally but records a failed payout for the magic failure account', async () => {
    const transferId = `e2e-fail-${Date.now()}`;

    const createResponse = await fetch(`${AUTHORIZATION_URL}/transfers`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ transferId, fromAccount: 'acc_demo_1', toAccount: 'acc_psp_fail_demo', amount: 50 }),
    });
    expect(createResponse.status).toBe(201);

    await waitFor(async () => {
      const response = await fetch(`${AUTHORIZATION_URL}/transfers/${transferId}`);
      const body = await response.json();
      return body.status === 'confirmed' ? body : null;
    });

    const failedAttempt = await waitFor(async () => {
      const response = await fetch(`${PAYOUT_URL}/payout/failed`);
      const body = await response.json();
      const match = body.attempts.find((attempt: { transferId: string }) => attempt.transferId === transferId);
      return match ?? null;
    });

    expect(failedAttempt.status).toBe('failed');
  }, 30000);
});
```

- [ ] **Step 5: Run test to verify it fails, then passes**

Run (ensure `docker compose up -d postgres kafka`, every service's own `npm run migrate` from its own directory, and every service's `npm install` have already been run by earlier tasks):

```bash
cd tests-e2e && npm install && npx vitest run
```

Expected: FAIL the first time only if any earlier task's service isn't independently runnable (fix that service first); once all three services and the mock PSP start cleanly, PASS (2 tests).

- [ ] **Step 6: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-24-end-to-end-test
git add tests-e2e
git commit -m "Add the end-to-end settlement flow test"
git push -u origin task-24-end-to-end-test
```

---

## Task 25: Docker Compose app services, basic Helm chart, and README

**Files:**
- Modify: `docker-compose.yml`
- Create: `helm/settlement-ledger/Chart.yaml`
- Create: `helm/settlement-ledger/values.yaml`
- Create: `helm/settlement-ledger/templates/authorization-deployment.yaml`
- Create: `helm/settlement-ledger/templates/authorization-service.yaml`
- Create: `helm/settlement-ledger/templates/ledger-deployment.yaml`
- Create: `helm/settlement-ledger/templates/ledger-service.yaml`
- Create: `helm/settlement-ledger/templates/payout-deployment.yaml`
- Create: `helm/settlement-ledger/templates/payout-service.yaml`
- Create: `helm/settlement-ledger/templates/kafka-statefulset.yaml`
- Create: `helm/settlement-ledger/templates/kafka-service.yaml`
- Modify: `README.md`

**Interfaces:**
- Produces: a full `docker compose up --build` bringing up Postgres, Kafka, all three services, their migrations, and `mock-payout-psp`.

- [ ] **Step 1: Add the migration, app, and mock-PSP services to docker-compose.yml**

Append these service entries under the existing `services:` key from Task 1 (alongside `postgres` and `kafka`):

```yaml
  authorization-migrate:
    build:
      context: ./services/authorization
    command: ["npx", "tsx", "db/migrate.ts"]
    environment:
      DATABASE_URL: "postgres://postgres:postgres@postgres:5432/settlement_ledger"
    depends_on:
      postgres:
        condition: service_healthy

  ledger-migrate:
    build:
      context: ./services/ledger
    command: ["npx", "tsx", "db/migrate.ts"]
    environment:
      DATABASE_URL: "postgres://postgres:postgres@postgres:5432/settlement_ledger"
    depends_on:
      postgres:
        condition: service_healthy

  payout-migrate:
    build:
      context: ./services/payout
    command: ["npx", "tsx", "db/migrate.ts"]
    environment:
      DATABASE_URL: "postgres://postgres:postgres@postgres:5432/settlement_ledger"
    depends_on:
      postgres:
        condition: service_healthy

  mock-payout-psp:
    build:
      context: ./mock-payout-psp
    environment:
      PORT: "4003"
    ports:
      - "4003:4003"

  authorization:
    build:
      context: ./services/authorization
    environment:
      PORT: "3001"
      DATABASE_URL: "postgres://postgres:postgres@postgres:5432/settlement_ledger"
      KAFKA_BROKERS: "kafka:29092"
    ports:
      - "3001:3001"
    depends_on:
      postgres:
        condition: service_healthy
      authorization-migrate:
        condition: service_completed_successfully
    restart: on-failure

  ledger:
    build:
      context: ./services/ledger
    environment:
      PORT: "3002"
      DATABASE_URL: "postgres://postgres:postgres@postgres:5432/settlement_ledger"
      KAFKA_BROKERS: "kafka:29092"
    ports:
      - "3002:3002"
    depends_on:
      postgres:
        condition: service_healthy
      ledger-migrate:
        condition: service_completed_successfully
    restart: on-failure

  payout:
    build:
      context: ./services/payout
    environment:
      PORT: "3003"
      DATABASE_URL: "postgres://postgres:postgres@postgres:5432/settlement_ledger"
      KAFKA_BROKERS: "kafka:29092"
      PSP_BASE_URL: "http://mock-payout-psp:4003"
    ports:
      - "3003:3003"
    depends_on:
      postgres:
        condition: service_healthy
      payout-migrate:
        condition: service_completed_successfully
      mock-payout-psp:
        condition: service_started
    restart: on-failure
```

- [ ] **Step 2: Write the basic Helm chart**

```yaml
# helm/settlement-ledger/Chart.yaml
apiVersion: v2
name: settlement-ledger
description: Deployment intent for the event-driven settlement ledger (documents intent only — not applied against a real cluster)
version: 0.1.0
```

```yaml
# helm/settlement-ledger/values.yaml
image:
  repository: settlement-ledger
  tag: latest

services:
  authorization:
    port: 3001
  ledger:
    port: 3002
  payout:
    port: 3003
```

```yaml
# helm/settlement-ledger/templates/authorization-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: authorization
spec:
  replicas: 1
  selector:
    matchLabels:
      app: authorization
  template:
    metadata:
      labels:
        app: authorization
    spec:
      containers:
        - name: authorization
          image: "{{ .Values.image.repository }}-authorization:{{ .Values.image.tag }}"
          ports:
            - containerPort: {{ .Values.services.authorization.port }}
          env:
            - name: PORT
              value: "{{ .Values.services.authorization.port }}"
```

```yaml
# helm/settlement-ledger/templates/authorization-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: authorization
spec:
  selector:
    app: authorization
  ports:
    - port: {{ .Values.services.authorization.port }}
      targetPort: {{ .Values.services.authorization.port }}
```

```yaml
# helm/settlement-ledger/templates/ledger-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ledger
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ledger
  template:
    metadata:
      labels:
        app: ledger
    spec:
      containers:
        - name: ledger
          image: "{{ .Values.image.repository }}-ledger:{{ .Values.image.tag }}"
          ports:
            - containerPort: {{ .Values.services.ledger.port }}
          env:
            - name: PORT
              value: "{{ .Values.services.ledger.port }}"
```

```yaml
# helm/settlement-ledger/templates/ledger-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: ledger
spec:
  selector:
    app: ledger
  ports:
    - port: {{ .Values.services.ledger.port }}
      targetPort: {{ .Values.services.ledger.port }}
```

```yaml
# helm/settlement-ledger/templates/payout-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: payout
spec:
  replicas: 1
  selector:
    matchLabels:
      app: payout
  template:
    metadata:
      labels:
        app: payout
    spec:
      containers:
        - name: payout
          image: "{{ .Values.image.repository }}-payout:{{ .Values.image.tag }}"
          ports:
            - containerPort: {{ .Values.services.payout.port }}
          env:
            - name: PORT
              value: "{{ .Values.services.payout.port }}"
```

```yaml
# helm/settlement-ledger/templates/payout-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: payout
spec:
  selector:
    app: payout
  ports:
    - port: {{ .Values.services.payout.port }}
      targetPort: {{ .Values.services.payout.port }}
```

```yaml
# helm/settlement-ledger/templates/kafka-statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
spec:
  serviceName: kafka
  replicas: 1
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      containers:
        - name: kafka
          image: apache/kafka:3.8.0
          ports:
            - containerPort: 9092
```

```yaml
# helm/settlement-ledger/templates/kafka-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: kafka
spec:
  clusterIP: None
  selector:
    app: kafka
  ports:
    - port: 9092
```

- [ ] **Step 3: Rewrite README.md**

```markdown
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
```

- [ ] **Step 4: Bring up the full stack and smoke-test it**

Run: `docker compose up --build -d`
Expected: all containers start; `docker compose ps` shows `authorization`, `ledger`, `payout`, `mock-payout-psp` running (migrations show `Exit 0`).

Run the demo walkthrough curl commands from the README.
Expected: matches the README's described behavior.

Run: `docker compose down`

- [ ] **Step 5: Commit and push**

```bash
git checkout main && git pull
git checkout -b task-25-docker-helm-readme
git add docker-compose.yml helm README.md
git commit -m "Add docker-compose app services, a basic Helm chart, and update the README"
git push -u origin task-25-docker-helm-readme
```

---
