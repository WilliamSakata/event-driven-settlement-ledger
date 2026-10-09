export interface SettlementEvent {
  transferId: string;
  fromAccount: string;
  toAccount: string;
  amount: number;
}

export interface SettlementRepositoryPort {
  wasConfirmed(transferId: string): Promise<boolean>;
  confirmSettlement(event: SettlementEvent): Promise<void>;
}
