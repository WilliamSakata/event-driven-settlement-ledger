export interface LedgerEntryInput {
  accountId: string;
  direction: 'debit' | 'credit';
  amount: number;
}

export function buildDoubleEntryLines(input: {
  transferId: string;
  fromAccount: string;
  toAccount: string;
  amount: number;
}): LedgerEntryInput[] {
  return [
    { accountId: input.fromAccount, direction: 'debit', amount: input.amount },
    { accountId: input.toAccount, direction: 'credit', amount: input.amount },
  ];
}

export function isBalanced(entries: LedgerEntryInput[]): boolean {
  const totalDebit = entries.filter((entry) => entry.direction === 'debit').reduce((sum, entry) => sum + entry.amount, 0);
  const totalCredit = entries.filter((entry) => entry.direction === 'credit').reduce((sum, entry) => sum + entry.amount, 0);
  return totalDebit === totalCredit;
}
