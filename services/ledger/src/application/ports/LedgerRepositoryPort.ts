export interface LedgerEntryRecord {
  id: string;
  transferId: string;
  accountId: string;
  direction: 'debit' | 'credit';
  amount: number;
  createdAt: Date;
}

export interface LedgerRepositoryPort {
  wasProcessed(transferId: string): Promise<boolean>;
  postSettlement(input: { transferId: string; fromAccount: string; toAccount: string; amount: number }): Promise<void>;
  listEntriesForAccount(accountId: string): Promise<LedgerEntryRecord[]>;
}
