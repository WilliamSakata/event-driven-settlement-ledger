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
