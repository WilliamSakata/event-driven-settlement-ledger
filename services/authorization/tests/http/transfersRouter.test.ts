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
