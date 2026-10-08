import { BalanceProjectionPort } from '../../src/application/ports/BalanceProjectionPort';

export class FakeBalanceProjection implements BalanceProjectionPort {
  constructor(
    private readonly confirmedBalances: Record<string, number> = {},
    private readonly pendingReservationsTotals: Record<string, number> = {},
  ) {}

  async getConfirmedBalance(accountId: string): Promise<number> {
    return this.confirmedBalances[accountId] ?? 0;
  }

  async getPendingReservationsTotal(accountId: string): Promise<number> {
    return this.pendingReservationsTotals[accountId] ?? 0;
  }
}
