import { DataFrameKind, type DataFrame, decodeResizePayload } from "./data-frame";
import { ProtocolError } from "./errors";

export interface ReplicaCursor {
  sessionEpoch: bigint;
  deliveryGeneration: bigint;
  lastEventSeq: bigint;
  nextPtyOffset: bigint;
}

export interface AppliedMutation {
  cursor: ReplicaCursor;
  resize?: ReturnType<typeof decodeResizePayload>;
}

export function applyMutationCursor(cursor: ReplicaCursor, frame: DataFrame): AppliedMutation {
  if (frame.sessionEpoch !== cursor.sessionEpoch) {
    throw new ProtocolError("BAD_EPOCH", "session epoch changed");
  }
  if (frame.deliveryGeneration !== cursor.deliveryGeneration) {
    throw new ProtocolError("BAD_GENERATION", "delivery generation changed");
  }
  if (frame.eventSeq !== cursor.lastEventSeq + 1n) {
    throw new ProtocolError("EVENT_GAP", "terminal mutation sequence has a gap");
  }
  if (frame.ptyOffset !== cursor.nextPtyOffset) {
    throw new ProtocolError("PTY_OFFSET_GAP", "PTY stream offset is missing or duplicated");
  }

  if (frame.kind === DataFrameKind.PtyOutput) {
    return {
      cursor: {
        ...cursor,
        lastEventSeq: frame.eventSeq,
        nextPtyOffset: cursor.nextPtyOffset + BigInt(frame.payload.byteLength),
      },
    };
  }

  if (frame.kind === DataFrameKind.ResizeApplied) {
    return {
      cursor: {
        ...cursor,
        lastEventSeq: frame.eventSeq,
      },
      resize: decodeResizePayload(frame.payload),
    };
  }

  throw new ProtocolError("BAD_KIND", "only terminal mutations can advance a replica cursor");
}
