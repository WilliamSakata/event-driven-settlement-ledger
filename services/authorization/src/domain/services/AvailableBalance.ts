export function calculateAvailableBalance(confirmedBalance: number, pendingReservationsTotal: number): number {
  return confirmedBalance - pendingReservationsTotal;
}
