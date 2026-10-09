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
