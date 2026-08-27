export const DELIVERY_EVENT_OVERHEAD_BYTES = 64n;
export const MAX_DELIVERY_OUTSTANDING_BYTES = 512 * 1024;

export interface DeliveryCreditCursor {
  eventSeq: bigint;
  nextPtyOffset: bigint;
}

export function deliveryOutstandingBytes(
  acknowledged: DeliveryCreditCursor,
  sent: DeliveryCreditCursor,
): bigint {
  if (
    acknowledged.eventSeq < 0n ||
    acknowledged.nextPtyOffset < 0n ||
    sent.eventSeq < acknowledged.eventSeq ||
    sent.nextPtyOffset < acknowledged.nextPtyOffset
  ) {
    throw new RangeError("delivery credit cursors must be monotonic and non-negative");
  }
  return (
    sent.nextPtyOffset -
    acknowledged.nextPtyOffset +
    (sent.eventSeq - acknowledged.eventSeq) * DELIVERY_EVENT_OVERHEAD_BYTES
  );
}
