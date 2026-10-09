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
