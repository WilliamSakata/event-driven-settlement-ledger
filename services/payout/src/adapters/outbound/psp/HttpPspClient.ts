import { z } from 'zod';
import { PspClientPort, PspSendInput, PspSendResult } from '../../../application/ports/PspClientPort';

const pspResponseSchema = z.union([
  z.object({ accepted: z.literal(true), pspReference: z.string() }),
  z.object({ accepted: z.literal(false), reason: z.string() }),
]);

export class HttpPspClient implements PspClientPort {
  constructor(private readonly baseUrl: string) {}

  async send(input: PspSendInput): Promise<PspSendResult> {
    const response = await fetch(`${this.baseUrl}/psp/send`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input),
    });

    if (!response.ok) {
      throw new Error(`PSP request failed with status ${response.status}`);
    }

    const parsed = pspResponseSchema.parse(await response.json());

    if (parsed.accepted) {
      return { succeeded: true, pspReference: parsed.pspReference, reason: null };
    }
    return { succeeded: false, pspReference: null, reason: parsed.reason };
  }
}
