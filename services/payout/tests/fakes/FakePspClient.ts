import { PspClientPort, PspSendInput, PspSendResult } from '../../src/application/ports/PspClientPort';

export class FakePspClient implements PspClientPort {
  public lastInput: PspSendInput | null = null;

  constructor(private readonly result: PspSendResult = { succeeded: true, pspReference: 'fake-ref', reason: null }) {}

  async send(input: PspSendInput): Promise<PspSendResult> {
    this.lastInput = input;
    return this.result;
  }
}
