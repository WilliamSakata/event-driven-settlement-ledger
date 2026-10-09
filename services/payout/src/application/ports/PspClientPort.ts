export interface PspSendInput {
  transferId: string;
  toAccount: string;
  amount: number;
}

export interface PspSendResult {
  succeeded: boolean;
  pspReference: string | null;
  reason: string | null;
}

export interface PspClientPort {
  send(input: PspSendInput): Promise<PspSendResult>;
}
