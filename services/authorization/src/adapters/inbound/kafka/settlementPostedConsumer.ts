import { Consumer } from 'kafkajs';
import { ApplySettlementToProjection } from '../../../application/use-cases/ApplySettlementToProjection';

export const SETTLEMENT_POSTED_TOPIC = 'settlement-posted';

export async function startSettlementPostedConsumer(consumer: Consumer, applySettlementToProjection: ApplySettlementToProjection): Promise<void> {
  await consumer.subscribe({ topic: SETTLEMENT_POSTED_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      let rawPayload: unknown;
      try {
        rawPayload = JSON.parse(message.value?.toString() ?? '{}');
      } catch {
        rawPayload = {};
      }
      await applySettlementToProjection.execute(rawPayload);
    },
  });
}
