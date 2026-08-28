import { z } from "zod";
import { ProtocolError } from "./errors";
import { SnapshotResourceIdSchema } from "./snapshot";

const connectionId = z.string().min(1).max(128);

export const DeliveryBarrierPayloadSchema = z.discriminatedUnion("mode", [
  z.strictObject({
    mode: z.literal("warm"),
    connectionId,
  }),
  z.strictObject({
    mode: z.literal("snapshot"),
    connectionId,
    snapshotId: SnapshotResourceIdSchema,
  }),
]);

export type DeliveryBarrierPayload = z.infer<typeof DeliveryBarrierPayloadSchema>;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });
const MAX_DELIVERY_BARRIER_PAYLOAD_BYTES = 512;

export function encodeDeliveryBarrierPayload(payload: DeliveryBarrierPayload): Uint8Array {
  const encoded = textEncoder.encode(JSON.stringify(DeliveryBarrierPayloadSchema.parse(payload)));
  if (encoded.byteLength > MAX_DELIVERY_BARRIER_PAYLOAD_BYTES) {
    throw new ProtocolError("BAD_LENGTH", "delivery barrier payload is too large");
  }
  return encoded;
}

export function decodeDeliveryBarrierPayload(payload: Uint8Array): DeliveryBarrierPayload {
  if (payload.byteLength === 0 || payload.byteLength > MAX_DELIVERY_BARRIER_PAYLOAD_BYTES) {
    throw new ProtocolError("BAD_LENGTH", "invalid delivery barrier payload length");
  }
  try {
    return DeliveryBarrierPayloadSchema.parse(JSON.parse(textDecoder.decode(payload)) as unknown);
  } catch (error) {
    if (error instanceof ProtocolError) throw error;
    throw new ProtocolError("BAD_PAYLOAD", "invalid delivery barrier payload");
  }
}
