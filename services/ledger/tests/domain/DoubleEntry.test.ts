import { describe, it, expect } from 'vitest';
import { buildDoubleEntryLines, isBalanced } from '../../src/domain/services/DoubleEntry';

describe('buildDoubleEntryLines', () => {
  it('produces exactly one debit line on the source account and one credit line on the destination account', () => {
    const lines = buildDoubleEntryLines({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });
    expect(lines).toEqual([
      { accountId: 'acc_1', direction: 'debit', amount: 500 },
      { accountId: 'acc_2', direction: 'credit', amount: 500 },
    ]);
  });
});

describe('isBalanced', () => {
  it('returns true when total debits equal total credits', () => {
    const lines = buildDoubleEntryLines({ transferId: 't1', fromAccount: 'acc_1', toAccount: 'acc_2', amount: 500 });
    expect(isBalanced(lines)).toBe(true);
  });

  it('returns false when debits and credits do not match', () => {
    const lines = [
      { accountId: 'acc_1', direction: 'debit' as const, amount: 500 },
      { accountId: 'acc_2', direction: 'credit' as const, amount: 400 },
    ];
    expect(isBalanced(lines)).toBe(false);
  });
});
