export interface ApprovalInput {
  availableBalance: number;
  amount: number;
}

export type ApprovalResult = { approved: true } | { approved: false; reason: string };

export function decideApproval(input: ApprovalInput): ApprovalResult {
  if (input.amount <= 0) {
    return { approved: false, reason: 'amount must be greater than zero' };
  }
  if (input.amount > input.availableBalance) {
    return { approved: false, reason: 'insufficient available balance' };
  }
  return { approved: true };
}
