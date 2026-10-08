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
