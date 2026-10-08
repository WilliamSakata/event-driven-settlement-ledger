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
