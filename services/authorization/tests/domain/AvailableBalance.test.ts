import { describe, it, expect } from 'vitest';
import { calculateAvailableBalance } from '../../src/domain/services/AvailableBalance';

describe('calculateAvailableBalance', () => {
  it('subtracts the pending reservations total from the confirmed balance', () => {
    expect(calculateAvailableBalance(1000, 300)).toBe(700);
  });

  it('returns the full confirmed balance when there are no pending reservations', () => {
    expect(calculateAvailableBalance(1000, 0)).toBe(1000);
  });
});
