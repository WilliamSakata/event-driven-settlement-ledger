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
