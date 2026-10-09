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
