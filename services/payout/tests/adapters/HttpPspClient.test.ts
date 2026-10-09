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
