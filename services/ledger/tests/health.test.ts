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
