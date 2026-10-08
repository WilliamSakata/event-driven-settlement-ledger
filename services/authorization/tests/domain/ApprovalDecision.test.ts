import { describe, it, expect } from 'vitest';
import { decideApproval } from '../../src/domain/services/ApprovalDecision';

describe('decideApproval', () => {
  it('approves when the amount is within the available balance', () => {
    expect(decideApproval({ availableBalance: 500, amount: 500 })).toEqual({ approved: true });
  });

  it('rejects when the amount exceeds the available balance', () => {
    const result = decideApproval({ availableBalance: 500, amount: 501 });
    expect(result).toEqual({ approved: false, reason: 'insufficient available balance' });
  });

  it('rejects a zero or negative amount', () => {
    const result = decideApproval({ availableBalance: 500, amount: 0 });
    expect(result).toEqual({ approved: false, reason: 'amount must be greater than zero' });
  });
});
