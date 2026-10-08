import { Consumer } from 'kafkajs';
import { PostSettlement } from '../../../application/use-cases/PostSettlement';

export const TRANSFER_AUTHORIZED_TOPIC = 'transfer-authorized';

export async function startTransferAuthorizedConsumer(consumer: Consumer, postSettlement: PostSettlement): Promise<void> {
  await consumer.subscribe({ topic: TRANSFER_AUTHORIZED_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      let rawPayload: unknown;
      try {
        rawPayload = JSON.parse(message.value?.toString() ?? '{}');
      } catch {
        rawPayload = {};
      }
      await postSettlement.execute(rawPayload);
    },
  });
}
