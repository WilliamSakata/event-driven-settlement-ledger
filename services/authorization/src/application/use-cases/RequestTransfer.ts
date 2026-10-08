import { calculateAvailableBalance } from '../../domain/services/AvailableBalance';
import { decideApproval } from '../../domain/services/ApprovalDecision';
import { TransferRepositoryPort, TransferRecord, TransferStatus } from '../ports/TransferRepositoryPort';
import { BalanceProjectionPort } from '../ports/BalanceProjectionPort';

export interface RequestTransferInput {
  transferId: string;
  fromAccount: string;
  toAccount: string;
  amount: number;
}

export interface RequestTransferResult {
  transferId: string;
  status: TransferStatus;
}

export class RequestTransfer {
  constructor(
    private readonly transferRepository: TransferRepositoryPort,
    private readonly balanceProjection: BalanceProjectionPort,
  ) {}

  async execute(input: RequestTransferInput): Promise<RequestTransferResult> {
    const existing = await this.transferRepository.findById(input.transferId);
    if (existing !== null) {
      return { transferId: existing.id, status: existing.status };
    }

    const confirmedBalance = await this.balanceProjection.getConfirmedBalance(input.fromAccount);
    const pendingReservationsTotal = await this.balanceProjection.getPendingReservationsTotal(input.fromAccount);
    const availableBalance = calculateAvailableBalance(confirmedBalance, pendingReservationsTotal);
    const decision = decideApproval({ availableBalance, amount: input.amount });

    if (!decision.approved) {
      const rejected: TransferRecord = {
        id: input.transferId,
        fromAccount: input.fromAccount,
        toAccount: input.toAccount,
        amount: input.amount,
        status: 'rejected',
      };
      await this.transferRepository.saveRejected(rejected);
      return { transferId: rejected.id, status: 'rejected' };
    }

    const approved: TransferRecord = {
      id: input.transferId,
      fromAccount: input.fromAccount,
      toAccount: input.toAccount,
      amount: input.amount,
      status: 'approved',
    };
    const outboxPayload = {
      transferId: input.transferId,
      fromAccount: input.fromAccount,
      toAccount: input.toAccount,
      amount: input.amount,
    };
    await this.transferRepository.saveApproved(
      approved,
      { accountId: input.fromAccount, amount: input.amount },
      outboxPayload,
    );
    return { transferId: approved.id, status: 'approved' };
  }
}
