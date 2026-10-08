export interface BalanceProjectionPort {
  getConfirmedBalance(accountId: string): Promise<number>;
  getPendingReservationsTotal(accountId: string): Promise<number>;
}
