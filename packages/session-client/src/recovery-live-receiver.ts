import {
  DATA_HEADER_BYTES,
  DELIVERY_ENVELOPE_V3_HEADER_BYTES,
  DataFrameKind,
  MAX_U64,
  RecoveryAdoptedSchema,
  ReplicaAppliedSchema,
  advanceDeliveryLaneCursor,
  applyMutationCursor,
  decodeDataFrame,
  decodeDeliveryEnvelopeV3,
  initialDeliveryLaneCursor,
  type AuthorityCursor,
  type DeliveryLaneCursor,
  type DeliveryReceived,
  type ReplicaApplied,
  type ReplicaCursor,
} from "@zhongduan/protocol";

import type { RecoveryAssemblerCompletion } from "./recovery-assembler";
import type { ReplicaSink } from "./types";

export type { RecoveryAssemblerCompletion } from "./recovery-assembler";

export type RecoveryLiveReceiverState = "live" | "failed" | "closed";

export type RecoveryLiveReceiverFailure = "protocol-conflict" | "apply-outcome-uncertain";

/** Caller-owned binding for the exact target atomically adopted for one completion. */
export interface RecoveryLiveReceiverAdoptedTarget {
  readonly engineId: string;
  readonly recoveryId: string;
  readonly replica: ReplicaSink;
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  for (let index = 0; index < left.byteLength; index += 1) {
    if (left[index] !== right[index]) return false;
  }
  return true;
}

function cloneLaneCursor(cursor: DeliveryLaneCursor): DeliveryLaneCursor {
  return { ...cursor };
}

function cloneAuthorityCursor(cursor: AuthorityCursor): AuthorityCursor {
  return { ...cursor };
}

function cloneReplicaApplied(applied: ReplicaApplied): ReplicaApplied {
  return { ...applied, authorityCursor: cloneAuthorityCursor(applied.authorityCursor) };
}

function cloneReceipt(receipt: DeliveryReceived | null): DeliveryReceived | null {
  return receipt === null ? null : { ...receipt };
}

function deliveryReceived(cursor: DeliveryLaneCursor): DeliveryReceived | null {
  if (cursor.deliveryOrdinal === 0n) return null;
  return {
    type: "delivery-received",
    deliveryGeneration: cursor.deliveryGeneration.toString(),
    lane: "live",
    contiguousDeliveryOrdinal: cursor.deliveryOrdinal.toString(),
    cumulativeEncodedBytes: cursor.cumulativeEncodedBytes.toString(),
  };
}

function authorityCursor(cursor: ReplicaCursor): AuthorityCursor {
  return {
    sessionEpoch: cursor.sessionEpoch.toString(),
    eventSeq: cursor.lastEventSeq.toString(),
    nextPtyOffset: cursor.nextPtyOffset.toString(),
  };
}

function assertSeedLaneCursor(
  cursor: DeliveryLaneCursor,
  lane: "live" | "recovery",
  deliveryGeneration: bigint,
  streamId: number,
): DeliveryLaneCursor {
  // Reuse the protocol constructor for generation and stream bounds.
  initialDeliveryLaneCursor(deliveryGeneration, lane, streamId);
  if (
    cursor.deliveryGeneration !== deliveryGeneration ||
    cursor.streamId !== streamId ||
    cursor.lane !== lane
  ) {
    throw new Error(`${lane} lane cursor does not match the receiver identity`);
  }
  if (
    typeof cursor.deliveryOrdinal !== "bigint" ||
    typeof cursor.cumulativeEncodedBytes !== "bigint" ||
    cursor.deliveryOrdinal < 0n ||
    cursor.deliveryOrdinal > MAX_U64 ||
    cursor.cumulativeEncodedBytes < 0n ||
    cursor.cumulativeEncodedBytes > MAX_U64 ||
    (cursor.deliveryOrdinal === 0n) !== (cursor.cumulativeEncodedBytes === 0n)
  ) {
    throw new Error(`${lane} lane cursor is not valid scalar progress`);
  }
  const minimumCumulativeEncodedBytes =
    cursor.deliveryOrdinal * BigInt(DATA_HEADER_BYTES + DELIVERY_ENVELOPE_V3_HEADER_BYTES);
  if (cursor.cumulativeEncodedBytes < minimumCumulativeEncodedBytes) {
    throw new Error(`${lane} lane cursor is not valid scalar progress`);
  }
  return cloneLaneCursor(cursor);
}

function cursorAtOrAfter(candidate: AuthorityCursor, floor: AuthorityCursor): boolean {
  if (candidate.sessionEpoch !== floor.sessionEpoch) return false;
  const candidateEvent = BigInt(candidate.eventSeq);
  const floorEvent = BigInt(floor.eventSeq);
  const candidateOffset = BigInt(candidate.nextPtyOffset);
  const floorOffset = BigInt(floor.nextPtyOffset);
  return (
    candidateEvent >= floorEvent &&
    candidateOffset >= floorOffset &&
    (candidateEvent !== floorEvent || candidateOffset === floorOffset)
  );
}

/**
 * Long-lived Recovery v3 live-lane mutation owner, seeded after RecoveryAssembler completion.
 *
 * The constructor consumes one immutable assembler completion plus the exact target/engine binding
 * the caller already adopted. The completion contains scalar lane progress but not the raw envelope
 * that produced its final ordinal. Consequently, a same-ordinal retry at that exact handoff
 * boundary cannot be authenticated and fails closed. After this receiver accepts its first new
 * ordinal, it retains that raw envelope and accepts only a byte-identical retry without applying it
 * twice.
 *
 * The receiver borrows `replica`; it never adopts or disposes it.
 */
export class RecoveryLiveReceiver {
  readonly #deliveryGeneration: bigint;
  readonly #replica: ReplicaSink;
  readonly #streamId: number;

  #applyingToken: symbol | null = null;
  #failure: RecoveryLiveReceiverFailure | null = null;
  #lastAcceptedRaw: Uint8Array | null = null;
  #latestReceipt: DeliveryReceived | null;
  #liveLaneCursor: DeliveryLaneCursor;
  #replicaApplied: ReplicaApplied;
  #replicaCursor: ReplicaCursor;
  #state: RecoveryLiveReceiverState = "live";
  #targetTainted = false;

  constructor(
    completion: RecoveryAssemblerCompletion,
    adoptedTarget: RecoveryLiveReceiverAdoptedTarget,
  ) {
    this.#deliveryGeneration = completion.laneCursors.live.deliveryGeneration;
    this.#streamId = completion.laneCursors.live.streamId;
    this.#liveLaneCursor = assertSeedLaneCursor(
      completion.laneCursors.live,
      "live",
      this.#deliveryGeneration,
      this.#streamId,
    );
    assertSeedLaneCursor(
      completion.laneCursors.recovery,
      "recovery",
      this.#deliveryGeneration,
      this.#streamId,
    );

    const adopted = RecoveryAdoptedSchema.parse(completion.recoveryAdopted);
    const applied = ReplicaAppliedSchema.parse(completion.replicaApplied);
    if (BigInt(adopted.deliveryGeneration) !== this.#deliveryGeneration) {
      throw new Error("recovery adopted progress does not match the receiver generation");
    }
    if (BigInt(applied.deliveryGeneration) !== this.#deliveryGeneration) {
      throw new Error("replica applied progress does not match the receiver generation");
    }
    if (!cursorAtOrAfter(applied.authorityCursor, adopted.replicaApplied)) {
      throw new Error("replica applied progress precedes the adopted target");
    }
    if (adoptedTarget.recoveryId !== adopted.recoveryId) {
      throw new Error("adopted target does not match the assembler completion");
    }
    if (
      adoptedTarget.engineId.length === 0 ||
      adoptedTarget.replica.engineId !== adoptedTarget.engineId
    ) {
      throw new Error("adopted target engine does not match its replica");
    }
    this.#replicaApplied = cloneReplicaApplied(applied);
    this.#replicaCursor = {
      sessionEpoch: BigInt(applied.authorityCursor.sessionEpoch),
      deliveryGeneration: this.#deliveryGeneration,
      lastEventSeq: BigInt(applied.authorityCursor.eventSeq),
      nextPtyOffset: BigInt(applied.authorityCursor.nextPtyOffset),
    };
    this.#latestReceipt = deliveryReceived(this.#liveLaneCursor);
    this.#replica = adoptedTarget.replica;
  }

  get state(): RecoveryLiveReceiverState {
    return this.#state;
  }

  get failure(): RecoveryLiveReceiverFailure | null {
    return this.#failure;
  }

  /** True when the borrowed adopted target may be ahead of the last published safe scalar. */
  get targetTainted(): boolean {
    return this.#targetTainted;
  }

  get liveLaneCursor(): DeliveryLaneCursor {
    return cloneLaneCursor(this.#liveLaneCursor);
  }

  get latestReceipt(): DeliveryReceived | null {
    return cloneReceipt(this.#latestReceipt);
  }

  get replicaApplied(): ReplicaApplied {
    return cloneReplicaApplied(this.#replicaApplied);
  }

  /** Returns true for one newly applied envelope or its exact latest-ordinal retry. */
  acceptEnvelope(input: ArrayBuffer | Uint8Array): boolean {
    if (this.#state !== "live" || this.#applyingToken !== null) return false;

    const inputBytes = input instanceof Uint8Array ? input : new Uint8Array(input);
    const raw = Uint8Array.from(inputBytes);
    if (this.#lastAcceptedRaw !== null && sameBytes(this.#lastAcceptedRaw, raw)) return true;

    let envelope: ReturnType<typeof decodeDeliveryEnvelopeV3>;
    let frame: ReturnType<typeof decodeDataFrame>;
    let nextLaneCursor: DeliveryLaneCursor;
    let applied: ReturnType<typeof applyMutationCursor>;
    try {
      envelope = decodeDeliveryEnvelopeV3(raw);
      frame = decodeDataFrame(envelope.payload);
      nextLaneCursor = advanceDeliveryLaneCursor(this.#liveLaneCursor, envelope);
      applied = applyMutationCursor(this.#replicaCursor, {
        ...frame,
        deliveryGeneration: this.#deliveryGeneration,
      });
    } catch {
      this.#fail("protocol-conflict");
      return false;
    }

    const resize = frame.kind === DataFrameKind.ResizeApplied ? applied.resize : undefined;
    if (frame.kind !== DataFrameKind.PtyOutput && resize === undefined) {
      this.#fail("protocol-conflict");
      return false;
    }

    const applyingToken = Symbol("recovery-live-apply");
    this.#applyingToken = applyingToken;
    // A void sink API cannot prove whether an effect happened if it throws or
    // synchronously closes the receiver. Clear this only after safe scalar
    // publication succeeds.
    this.#targetTainted = true;
    try {
      if (frame.kind === DataFrameKind.PtyOutput) {
        this.#replica.writePty(frame.payload.slice());
      } else {
        this.#replica.resize(resize!);
      }
    } catch {
      if (this.#applyingToken === applyingToken) this.#applyingToken = null;
      this.#failApplyOutcomeUncertain();
      return false;
    }

    if (this.#applyingToken !== applyingToken || this.#state !== "live") {
      if (this.#applyingToken === applyingToken) this.#applyingToken = null;
      this.#lastAcceptedRaw = null;
      return false;
    }

    this.#lastAcceptedRaw = raw;
    this.#liveLaneCursor = nextLaneCursor;
    this.#replicaCursor = applied.cursor;
    this.#latestReceipt = deliveryReceived(nextLaneCursor);
    this.#replicaApplied = {
      type: "replica-applied",
      deliveryGeneration: this.#deliveryGeneration.toString(),
      authorityCursor: authorityCursor(applied.cursor),
    };
    this.#targetTainted = false;
    this.#applyingToken = null;
    return true;
  }

  close(): void {
    if (this.#state === "closed") return;
    this.#state = "closed";
    this.#lastAcceptedRaw = null;
  }

  #fail(reason: RecoveryLiveReceiverFailure): void {
    if (this.#state !== "live") return;
    this.#failure = reason;
    this.#state = "failed";
    this.#lastAcceptedRaw = null;
  }

  #failApplyOutcomeUncertain(): void {
    this.#failure = "apply-outcome-uncertain";
    if (this.#state === "live") this.#state = "failed";
    this.#lastAcceptedRaw = null;
    this.#targetTainted = true;
  }
}
