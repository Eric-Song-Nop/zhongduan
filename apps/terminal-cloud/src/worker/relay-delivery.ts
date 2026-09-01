import {
  DataFrameKind,
  MAX_DELIVERY_OUTSTANDING_BYTES,
  MAX_U64,
  deliveryOutstandingBytes,
  type DataFrame,
} from "@zhongduan/protocol";

export interface DeliveryCursorState {
  ackedEventSeq: string | null;
  ackedPtyOffset: string | null;
  dataState: "awaiting-attach" | "catching-up" | "synced" | null;
  firstEventSeq: string | null;
  firstPtyOffset: string | null;
  sentEventSeq: string | null;
  sentPtyOffset: string | null;
}

export type DeliveryCursorResult =
  | { kind: "credit-exceeded" }
  | { kind: "sequence-error" }
  | { kind: "ok"; nextState: DeliveryCursorState; outstandingBytes: bigint };

export function advanceDeliveryCursor(
  state: DeliveryCursorState,
  frame: DataFrame,
): DeliveryCursorResult {
  return advanceDeliveryCursorBatch(state, [frame]);
}

export function advanceDeliveryCursorBatch(
  state: DeliveryCursorState,
  frames: readonly DataFrame[],
): DeliveryCursorResult {
  if (frames.length === 0) return { kind: "sequence-error" };
  if (state.dataState === null || state.dataState === "awaiting-attach") {
    return { kind: "sequence-error" };
  }
  if (
    state.sentEventSeq === null ||
    state.sentPtyOffset === null ||
    state.ackedEventSeq === null ||
    state.ackedPtyOffset === null
  ) {
    return { kind: "sequence-error" };
  }

  let previousSentEvent = BigInt(state.sentEventSeq);
  let previousSentPty = BigInt(state.sentPtyOffset);
  const ackedEvent = BigInt(state.ackedEventSeq);
  const ackedPty = BigInt(state.ackedPtyOffset);
  let dataState = state.dataState;
  let outstandingBytes = 0n;
  for (const frame of frames) {
    if (
      frame.kind !== DataFrameKind.PtyOutput &&
      frame.kind !== DataFrameKind.ResizeApplied &&
      frame.kind !== DataFrameKind.ReplayCommit
    ) {
      return { kind: "sequence-error" };
    }
    if (frame.kind === DataFrameKind.ReplayCommit && frame.payload.byteLength !== 0) {
      return { kind: "sequence-error" };
    }
    if (frame.kind === DataFrameKind.ResizeApplied && frame.payload.byteLength !== 16) {
      return { kind: "sequence-error" };
    }
    if (
      frame.kind === DataFrameKind.PtyOutput &&
      frame.ptyOffset > MAX_U64 - BigInt(frame.payload.byteLength)
    ) {
      return { kind: "sequence-error" };
    }
    const expectedEvent =
      frame.kind === DataFrameKind.ReplayCommit ? previousSentEvent : previousSentEvent + 1n;
    if (frame.eventSeq !== expectedEvent || frame.ptyOffset !== previousSentPty) {
      return { kind: "sequence-error" };
    }
    const nextPtyOffset =
      frame.kind === DataFrameKind.PtyOutput
        ? frame.ptyOffset + BigInt(frame.payload.byteLength)
        : frame.ptyOffset;
    if (frame.eventSeq < ackedEvent || nextPtyOffset < ackedPty) {
      return { kind: "sequence-error" };
    }
    outstandingBytes = deliveryOutstandingBytes(
      { eventSeq: ackedEvent, nextPtyOffset: ackedPty },
      { eventSeq: frame.eventSeq, nextPtyOffset },
    );
    if (outstandingBytes > BigInt(MAX_DELIVERY_OUTSTANDING_BYTES)) {
      return { kind: "credit-exceeded" };
    }
    previousSentEvent = frame.eventSeq;
    previousSentPty = nextPtyOffset;
    if (frame.kind === DataFrameKind.ReplayCommit) dataState = "synced";
  }

  return {
    kind: "ok",
    outstandingBytes,
    nextState: {
      ...state,
      dataState,
      sentEventSeq: previousSentEvent.toString(),
      sentPtyOffset: previousSentPty.toString(),
    },
  };
}
