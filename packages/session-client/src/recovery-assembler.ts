import {
  AuthorityCursorSchema,
  DataFrameKind,
  MAX_U64,
  RecoverySourceClosedSchema,
  RecoveryStartSchema,
  advanceDeliveryLaneCursor,
  applyMutationCursor,
  decodeDataFrame,
  decodeDeliveryEnvelopeV3,
  initialDeliveryLaneCursor,
  type AuthorityCursor,
  type DataFrame,
  type DeliveryLane,
  type DeliveryLaneCursor,
  type DeliveryReceived,
  type RecoveryAdopted,
  type RecoveryDoneRecord,
  type RecoverySourceClosed,
  type RecoveryStart,
  type ReplicaApplied,
  type ReplicaCursor,
} from "@zhongduan/protocol";

import type { ReplicaSink } from "./types";

export type RecoveryAssemblerState =
  | "awaiting-start"
  | "restoring"
  | "assembling"
  | "handoff-eligible"
  | "adopted"
  | "complete"
  | "reset"
  | "closed";

export type RecoveryAssemblerResetReason =
  | "protocol-conflict"
  | "capacity-exceeded"
  | "gap-span-exceeded"
  | "generation-deadline"
  | "no-progress-deadline"
  | "apply-failed"
  | "candidate-conflict"
  | "handoff-conflict"
  | "ownership-uncertain";

export interface RecoveryAssemblerLimits {
  readonly maxApplyFramesPerCall: number;
  readonly maxGapSpan: bigint;
  readonly maxOwnedBytes: number;
  readonly maxOwnedFrames: number;
  readonly noProgressDeadlineMs: number;
  readonly recoveryDeadlineMs: number;
}

export interface RecoveryAssemblerWarmTarget {
  readonly cursor: AuthorityCursor;
  readonly replica: ReplicaSink;
}

export interface RecoveryAssemblerOptions {
  readonly deliveryGeneration: bigint;
  readonly engineId: string;
  readonly limits: RecoveryAssemblerLimits;
  readonly startedAtMs: number;
  readonly streamId: number;
  readonly warm?: RecoveryAssemblerWarmTarget;
}

export interface RecoverySnapshotCandidateIdentity {
  readonly base: AuthorityCursor;
  readonly deliveryGeneration: string;
  readonly recoveryId: string;
}

export interface RecoveryAssemblerHandoff {
  readonly candidate: ReplicaSink;
  readonly cursor: ReplicaCursor;
  readonly mode: "cold" | "warm";
}

export interface RecoveryAssemblerLaneCursors {
  readonly live: DeliveryLaneCursor;
  readonly recovery: DeliveryLaneCursor;
}

export interface RecoveryAssemblerReceipts {
  readonly live: DeliveryReceived | null;
  readonly recovery: DeliveryReceived | null;
}

export interface RecoveryAssemblerReset {
  readonly reason: RecoveryAssemblerResetReason;
  readonly reusableWarmCursor: AuthorityCursor | null;
  readonly warmTargetTainted: boolean;
}

export interface RecoveryAssemblerCompletion {
  readonly laneCursors: RecoveryAssemblerLaneCursors;
  readonly recoveryAdopted: RecoveryAdopted;
  readonly replicaApplied: ReplicaApplied;
}

export interface RecoveryAssemblerSnapshot {
  readonly completion: RecoveryAssemblerCompletion | null;
  readonly handoff: RecoveryAssemblerHandoff | null;
  readonly laneCursors: RecoveryAssemblerLaneCursors;
  readonly latestReceipts: RecoveryAssemblerReceipts;
  readonly recoveryAdopted: RecoveryAdopted | null;
  readonly recoveryDone: RecoveryDoneRecord | null;
  readonly replicaApplied: ReplicaApplied | null;
  readonly reset: RecoveryAssemblerReset | null;
  readonly state: RecoveryAssemblerState;
}

interface OwnedEnvelope {
  readonly envelope: ReturnType<typeof decodeDeliveryEnvelopeV3>;
  readonly frame: DataFrame;
  readonly raw: Uint8Array;
  pending: boolean;
  retryRetained: boolean;
}

interface ValidatedStartState {
  readonly done: RecoveryDoneRecord | null;
  readonly liveCursor: ReplicaCursor;
  readonly recoveryCursor: ReplicaCursor;
}

const LANES = ["live", "recovery"] as const satisfies readonly DeliveryLane[];

function positiveSafeInteger(value: number, field: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${field} must be a positive safe integer`);
  }
  return value;
}

function positiveDuration(value: number, field: string): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new RangeError(`${field} must be a positive finite duration`);
  }
  return value;
}

function assertNow(value: number, field = "nowMs"): number {
  if (!Number.isFinite(value)) throw new RangeError(`${field} must be finite`);
  return value;
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  for (let index = 0; index < left.byteLength; index += 1) {
    if (left[index] !== right[index]) return false;
  }
  return true;
}

function sameAuthorityCursor(left: AuthorityCursor, right: AuthorityCursor): boolean {
  return (
    left.sessionEpoch === right.sessionEpoch &&
    left.eventSeq === right.eventSeq &&
    left.nextPtyOffset === right.nextPtyOffset
  );
}

function sameReplicaCursor(left: ReplicaCursor, right: ReplicaCursor): boolean {
  return (
    left.sessionEpoch === right.sessionEpoch &&
    left.deliveryGeneration === right.deliveryGeneration &&
    left.lastEventSeq === right.lastEventSeq &&
    left.nextPtyOffset === right.nextPtyOffset
  );
}

function cloneAuthorityCursor(cursor: AuthorityCursor): AuthorityCursor {
  return { ...cursor };
}

function cloneReplicaCursor(cursor: ReplicaCursor): ReplicaCursor {
  return { ...cursor };
}

function replicaCursor(cursor: AuthorityCursor, deliveryGeneration: bigint): ReplicaCursor {
  return {
    sessionEpoch: BigInt(cursor.sessionEpoch),
    deliveryGeneration,
    lastEventSeq: BigInt(cursor.eventSeq),
    nextPtyOffset: BigInt(cursor.nextPtyOffset),
  };
}

function authorityCursor(cursor: ReplicaCursor): AuthorityCursor {
  return {
    sessionEpoch: cursor.sessionEpoch.toString(),
    eventSeq: cursor.lastEventSeq.toString(),
    nextPtyOffset: cursor.nextPtyOffset.toString(),
  };
}

function reboundFrame(frame: DataFrame, deliveryGeneration: bigint): DataFrame {
  return { ...frame, deliveryGeneration };
}

function cloneLaneCursor(cursor: DeliveryLaneCursor): DeliveryLaneCursor {
  return { ...cursor };
}

function cloneReceipt(receipt: DeliveryReceived | null): DeliveryReceived | null {
  return receipt === null ? null : { ...receipt };
}

function cloneReplicaApplied(applied: ReplicaApplied | null): ReplicaApplied | null {
  return applied === null
    ? null
    : { ...applied, authorityCursor: cloneAuthorityCursor(applied.authorityCursor) };
}

function cloneRecoveryAdopted(adopted: RecoveryAdopted | null): RecoveryAdopted | null {
  return adopted === null
    ? null
    : { ...adopted, replicaApplied: cloneAuthorityCursor(adopted.replicaApplied) };
}

function cloneRecoveryDone(done: RecoveryDoneRecord | null): RecoveryDoneRecord | null {
  return done === null
    ? null
    : { ...done, replayedThrough: cloneAuthorityCursor(done.replayedThrough) };
}

function sameRecoveryStart(left: RecoveryStart, right: RecoveryStart): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function isMutation(frame: DataFrame): boolean {
  return frame.kind === DataFrameKind.PtyOutput || frame.kind === DataFrameKind.ResizeApplied;
}

function deliveryReceived(cursor: DeliveryLaneCursor): DeliveryReceived | null {
  if (cursor.deliveryOrdinal === 0n) return null;
  return {
    type: "delivery-received",
    deliveryGeneration: cursor.deliveryGeneration.toString(),
    lane: cursor.lane,
    contiguousDeliveryOrdinal: cursor.deliveryOrdinal.toString(),
    cumulativeEncodedBytes: cursor.cumulativeEncodedBytes.toString(),
  };
}

/**
 * Generation-scoped Recovery v3 assembly core.
 *
 * This class deliberately does not own transport, snapshot loading, timers, or visible-core
 * adoption. A caller may send the latest receipt after every successful `acceptEnvelope` call,
 * installs an already-restored detached candidate, performs the real atomic handoff, and then
 * confirms that exact cursor.
 */
export class RecoveryAssembler {
  readonly #deliveryGeneration: bigint;
  readonly #engineId: string;
  readonly #limits: RecoveryAssemblerLimits;
  readonly #startedAtMs: number;
  readonly #streamId: number;
  readonly #warm: RecoveryAssemblerWarmTarget | null;

  #state: RecoveryAssemblerState = "awaiting-start";
  #lastNowMs: number;
  #lastProgressAtMs: number;
  #start: RecoveryStart | null = null;
  #mode: "cold" | "warm" | null = null;

  #laneCursors: Record<DeliveryLane, DeliveryLaneCursor>;
  #latestReceipts: Record<DeliveryLane, DeliveryReceived | null> = {
    live: null,
    recovery: null,
  };
  readonly #byLaneOrdinal: Record<DeliveryLane, Map<bigint, OwnedEnvelope>> = {
    live: new Map(),
    recovery: new Map(),
  };
  readonly #mutationByEventSeq = new Map<bigint, OwnedEnvelope>();
  readonly #pendingByEventSeq = new Map<bigint, OwnedEnvelope>();
  readonly #owned = new Set<OwnedEnvelope>();
  #ownedBytes = 0;
  #ownedFrames = 0;

  #recoveryMutationCursor: ReplicaCursor | null = null;
  #liveMutationCursor: ReplicaCursor | null = null;
  #availableCursor: ReplicaCursor | null = null;
  #appliedCursor: ReplicaCursor | null = null;
  #target: ReplicaSink | null = null;
  #installedCandidate: ReplicaSink | null = null;
  #coldCandidateOwned = false;
  #targetMutationAttempted = false;

  #done: RecoveryDoneRecord | null = null;
  #sourceClosed: RecoverySourceClosed | null = null;
  #latestReplicaApplied: ReplicaApplied | null = null;
  #recoveryAdopted: RecoveryAdopted | null = null;
  #adoptedCursor: ReplicaCursor | null = null;
  #resetResult: RecoveryAssemblerReset | null = null;
  #completion: RecoveryAssemblerCompletion | null = null;

  constructor(options: RecoveryAssemblerOptions) {
    if (options.deliveryGeneration <= 0n || options.deliveryGeneration > MAX_U64) {
      throw new RangeError("deliveryGeneration must be a positive uint64");
    }
    if (
      !Number.isInteger(options.streamId) ||
      options.streamId <= 0 ||
      options.streamId > 0xffff_ffff
    ) {
      throw new RangeError("streamId must be a positive uint32");
    }
    if (options.engineId.length === 0) throw new Error("engineId must not be empty");

    const limits: RecoveryAssemblerLimits = {
      maxApplyFramesPerCall: positiveSafeInteger(
        options.limits.maxApplyFramesPerCall,
        "maxApplyFramesPerCall",
      ),
      maxGapSpan: options.limits.maxGapSpan,
      maxOwnedBytes: positiveSafeInteger(options.limits.maxOwnedBytes, "maxOwnedBytes"),
      maxOwnedFrames: positiveSafeInteger(options.limits.maxOwnedFrames, "maxOwnedFrames"),
      noProgressDeadlineMs: positiveDuration(
        options.limits.noProgressDeadlineMs,
        "noProgressDeadlineMs",
      ),
      recoveryDeadlineMs: positiveDuration(options.limits.recoveryDeadlineMs, "recoveryDeadlineMs"),
    };
    if (limits.maxGapSpan < 0n || limits.maxGapSpan > MAX_U64) {
      throw new RangeError("maxGapSpan must be a uint64");
    }

    const startedAtMs = assertNow(options.startedAtMs, "startedAtMs");
    let warm: RecoveryAssemblerWarmTarget | null = null;
    if (options.warm !== undefined) {
      const cursor = AuthorityCursorSchema.parse(options.warm.cursor);
      if (options.warm.replica.engineId !== options.engineId) {
        throw new Error("warm replica engine does not match the assembler engine");
      }
      warm = { replica: options.warm.replica, cursor: cloneAuthorityCursor(cursor) };
    }

    this.#deliveryGeneration = options.deliveryGeneration;
    this.#engineId = options.engineId;
    this.#limits = limits;
    this.#startedAtMs = startedAtMs;
    this.#lastNowMs = startedAtMs;
    this.#lastProgressAtMs = startedAtMs;
    this.#streamId = options.streamId;
    this.#warm = warm;
    this.#laneCursors = {
      live: initialDeliveryLaneCursor(options.deliveryGeneration, "live", options.streamId),
      recovery: initialDeliveryLaneCursor(options.deliveryGeneration, "recovery", options.streamId),
    };
  }

  get state(): RecoveryAssemblerState {
    return this.#state;
  }

  /** Exact next caller-driven deadline, or null after this generation becomes terminal. */
  get nextDeadlineAtMs(): number | null {
    if (this.#state === "reset" || this.#state === "closed" || this.#state === "complete") {
      return null;
    }
    return Math.min(
      this.#startedAtMs + this.#limits.recoveryDeadlineMs,
      this.#lastProgressAtMs + this.#limits.noProgressDeadlineMs,
    );
  }

  get laneCursors(): RecoveryAssemblerLaneCursors {
    return {
      live: cloneLaneCursor(this.#laneCursors.live),
      recovery: cloneLaneCursor(this.#laneCursors.recovery),
    };
  }

  get latestReceipts(): RecoveryAssemblerReceipts {
    return {
      live: cloneReceipt(this.#latestReceipts.live),
      recovery: cloneReceipt(this.#latestReceipts.recovery),
    };
  }

  get replicaApplied(): ReplicaApplied | null {
    return cloneReplicaApplied(this.#latestReplicaApplied);
  }

  get recoveryDone(): RecoveryDoneRecord | null {
    return cloneRecoveryDone(this.#done);
  }

  get handoff(): RecoveryAssemblerHandoff | null {
    if (this.#state !== "handoff-eligible" || this.#mode === null) return null;
    if (this.#target === null || this.#appliedCursor === null) return null;
    return {
      candidate: this.#target,
      cursor: cloneReplicaCursor(this.#appliedCursor),
      mode: this.#mode,
    };
  }

  get recoveryAdopted(): RecoveryAdopted | null {
    return cloneRecoveryAdopted(this.#recoveryAdopted);
  }

  get resetResult(): RecoveryAssemblerReset | null {
    return this.#resetResult === null
      ? null
      : {
          ...this.#resetResult,
          reusableWarmCursor:
            this.#resetResult.reusableWarmCursor === null
              ? null
              : cloneAuthorityCursor(this.#resetResult.reusableWarmCursor),
        };
  }

  get completion(): RecoveryAssemblerCompletion | null {
    if (this.#completion === null) return null;
    return {
      laneCursors: {
        live: cloneLaneCursor(this.#completion.laneCursors.live),
        recovery: cloneLaneCursor(this.#completion.laneCursors.recovery),
      },
      recoveryAdopted: cloneRecoveryAdopted(this.#completion.recoveryAdopted)!,
      replicaApplied: cloneReplicaApplied(this.#completion.replicaApplied)!,
    };
  }

  get snapshot(): RecoveryAssemblerSnapshot {
    return {
      completion: this.completion,
      handoff: this.handoff,
      laneCursors: this.laneCursors,
      latestReceipts: this.latestReceipts,
      recoveryAdopted: this.recoveryAdopted,
      recoveryDone: this.recoveryDone,
      replicaApplied: this.replicaApplied,
      reset: this.resetResult,
      state: this.#state,
    };
  }

  acceptStart(input: RecoveryStart, nowMs: number): boolean {
    let start: RecoveryStart;
    try {
      start = RecoveryStartSchema.parse(input);
    } catch {
      if (!this.#enter(nowMs)) return false;
      this.reset("protocol-conflict");
      return false;
    }

    if (this.#state === "complete") {
      this.#recordNow(nowMs);
      return this.#start !== null && sameRecoveryStart(this.#start, start);
    }
    if (!this.#enter(nowMs)) return false;

    if (this.#start !== null) {
      if (sameRecoveryStart(this.#start, start)) return true;
      this.reset("protocol-conflict");
      return false;
    }
    if (
      BigInt(start.deliveryGeneration) !== this.#deliveryGeneration ||
      start.streamId !== this.#streamId ||
      start.engineId !== this.#engineId
    ) {
      this.reset("protocol-conflict");
      return false;
    }
    if (start.source.kind === "warm") {
      if (
        this.#warm === null ||
        !sameAuthorityCursor(this.#warm.cursor, start.base) ||
        this.#warm.replica.engineId !== this.#engineId
      ) {
        this.reset("protocol-conflict");
        return false;
      }
    }

    let validated: ValidatedStartState;
    try {
      validated = this.#validateBufferedStart(start);
    } catch {
      this.reset("protocol-conflict");
      return false;
    }
    this.#start = start;
    this.#mode = start.source.kind === "warm" ? "warm" : "cold";
    this.#recoveryMutationCursor = validated.recoveryCursor;
    this.#liveMutationCursor = validated.liveCursor;
    this.#done = validated.done;
    this.#availableCursor = replicaCursor(start.base, this.#deliveryGeneration);
    try {
      this.#advanceAvailablePrefix(nowMs);
    } catch {
      this.reset("protocol-conflict");
      return false;
    }
    if (this.#pendingGapExceeds()) {
      this.reset("gap-span-exceeded");
      return false;
    }

    for (const lane of LANES) {
      this.#latestReceipts[lane] = deliveryReceived(this.#laneCursors[lane]);
    }

    if (this.#mode === "warm") {
      this.#target = this.#warm!.replica;
      this.#appliedCursor = replicaCursor(start.base, this.#deliveryGeneration);
      this.#latestReplicaApplied = this.#makeReplicaApplied(this.#appliedCursor);
      this.#state = "assembling";
    } else {
      this.#state = "restoring";
    }
    this.#noteProgress(nowMs);
    this.#refreshHandoffEligibility(nowMs);
    return true;
  }

  /** Returns true for a newly admitted envelope or an exact same-ordinal retry. */
  acceptEnvelope(input: ArrayBuffer | Uint8Array, nowMs: number): boolean {
    if (!this.#enter(nowMs)) return false;

    const inputBytes = input instanceof Uint8Array ? input : new Uint8Array(input);
    if (inputBytes.byteLength > this.#limits.maxOwnedBytes) {
      this.reset("capacity-exceeded");
      return false;
    }

    let envelope: ReturnType<typeof decodeDeliveryEnvelopeV3>;
    let frame: DataFrame;
    try {
      envelope = decodeDeliveryEnvelopeV3(inputBytes);
      frame = decodeDataFrame(envelope.payload);
    } catch {
      this.reset("protocol-conflict");
      return false;
    }
    if (
      envelope.deliveryGeneration !== this.#deliveryGeneration ||
      envelope.streamId !== this.#streamId
    ) {
      this.reset("protocol-conflict");
      return false;
    }

    const laneRecords = this.#byLaneOrdinal[envelope.lane];
    const retry = laneRecords.get(envelope.deliveryOrdinal);
    if (retry !== undefined) {
      if (!sameBytes(retry.raw, inputBytes)) {
        this.reset("protocol-conflict");
        return false;
      }
      return true;
    }

    if (
      this.#ownedFrames >= this.#limits.maxOwnedFrames ||
      inputBytes.byteLength > this.#limits.maxOwnedBytes - this.#ownedBytes
    ) {
      this.reset("capacity-exceeded");
      return false;
    }

    const raw = inputBytes.slice();
    try {
      envelope = decodeDeliveryEnvelopeV3(raw);
      frame = decodeDataFrame(envelope.payload);
    } catch {
      this.reset("protocol-conflict");
      return false;
    }

    let nextLaneCursor: DeliveryLaneCursor;
    try {
      nextLaneCursor = advanceDeliveryLaneCursor(this.#laneCursors[envelope.lane], envelope);
    } catch {
      this.reset("protocol-conflict");
      return false;
    }

    if (envelope.lane === "recovery" && this.#done !== null) {
      this.reset("protocol-conflict");
      return false;
    }
    if (isMutation(frame) && this.#mutationByEventSeq.has(frame.eventSeq)) {
      this.reset("protocol-conflict");
      return false;
    }

    let postStartCursor: ReplicaCursor | null = null;
    let postStartDone: RecoveryDoneRecord | null = null;
    if (this.#start !== null) {
      try {
        if (isMutation(frame)) {
          postStartCursor = this.#validateNewMutation(envelope.lane, frame);
        } else {
          postStartDone = this.#validateDone(envelope, frame, this.#recoveryMutationCursor!);
        }
      } catch {
        this.reset("protocol-conflict");
        return false;
      }
    }

    const owned: OwnedEnvelope = {
      envelope,
      frame,
      raw,
      pending: isMutation(frame),
      retryRetained: true,
    };
    laneRecords.set(envelope.deliveryOrdinal, owned);
    this.#laneCursors[envelope.lane] = nextLaneCursor;
    this.#owned.add(owned);
    this.#ownedBytes += raw.byteLength;
    this.#ownedFrames += 1;
    if (isMutation(frame)) {
      this.#mutationByEventSeq.set(frame.eventSeq, owned);
      this.#pendingByEventSeq.set(frame.eventSeq, owned);
      if (postStartCursor !== null) {
        if (envelope.lane === "live") this.#liveMutationCursor = postStartCursor;
        else this.#recoveryMutationCursor = postStartCursor;
      }
    } else if (postStartDone !== null) {
      this.#done = postStartDone;
    }

    if (this.#start !== null) {
      let availableAdvanced: boolean;
      try {
        availableAdvanced = this.#advanceAvailablePrefix(nowMs);
      } catch {
        this.reset("protocol-conflict");
        return false;
      }
      if (this.#pendingGapExceeds()) {
        this.reset("gap-span-exceeded");
        return false;
      }
      this.#latestReceipts[envelope.lane] = deliveryReceived(nextLaneCursor);
      if (postStartDone !== null && !availableAdvanced) this.#noteProgress(nowMs);
      this.#refreshHandoffEligibility(nowMs);
    }
    return true;
  }

  installSnapshotCandidate(
    identity: RecoverySnapshotCandidateIdentity,
    replica: ReplicaSink,
    nowMs: number,
  ): boolean {
    const start = this.#start;
    let base: AuthorityCursor;
    try {
      base = AuthorityCursorSchema.parse(identity.base);
    } catch {
      if (!this.#enter(nowMs)) return false;
      this.reset("candidate-conflict");
      return false;
    }
    if (
      start === null ||
      start.source.kind !== "snapshot" ||
      identity.recoveryId !== start.recoveryId ||
      identity.deliveryGeneration !== start.deliveryGeneration ||
      !sameAuthorityCursor(base, start.base) ||
      replica.engineId !== this.#engineId
    ) {
      if (this.#state === "complete") {
        this.#recordNow(nowMs);
        return false;
      }
      if (!this.#enter(nowMs)) return false;
      this.reset("candidate-conflict");
      return false;
    }
    if (this.#state === "complete") {
      this.#recordNow(nowMs);
      return this.#installedCandidate === replica;
    }
    if (!this.#enter(nowMs)) return false;
    if (this.#installedCandidate !== null) {
      if (this.#installedCandidate === replica) return true;
      this.reset("candidate-conflict");
      return false;
    }
    if (this.#state !== "restoring" && this.#state !== "assembling") return false;

    this.#target = replica;
    this.#installedCandidate = replica;
    this.#coldCandidateOwned = true;
    this.#appliedCursor = replicaCursor(start.base, this.#deliveryGeneration);
    this.#latestReplicaApplied = this.#makeReplicaApplied(this.#appliedCursor);
    this.#state = "assembling";
    this.#noteProgress(nowMs);
    this.#refreshHandoffEligibility(nowMs);
    return true;
  }

  acceptSourceClosed(input: RecoverySourceClosed, nowMs: number): boolean {
    let closed: RecoverySourceClosed;
    try {
      closed = RecoverySourceClosedSchema.parse(input);
    } catch {
      if (!this.#enter(nowMs)) return false;
      this.reset("protocol-conflict");
      return false;
    }
    if (this.#state === "complete") {
      this.#recordNow(nowMs);
      return (
        this.#sourceClosed !== null && JSON.stringify(this.#sourceClosed) === JSON.stringify(closed)
      );
    }
    if (!this.#enter(nowMs)) return false;
    const start = this.#start;
    const done = this.#done;
    if (
      start === null ||
      done === null ||
      closed.recoveryId !== start.recoveryId ||
      closed.deliveryGeneration !== start.deliveryGeneration ||
      closed.throughRecoveryOrdinal !== done.recoveryOrdinal ||
      closed.throughRecoveryCumulativeEncodedBytes !== done.cumulativeEncodedBytes ||
      BigInt(closed.throughRecoveryOrdinal) !== this.#laneCursors.recovery.deliveryOrdinal ||
      BigInt(closed.throughRecoveryCumulativeEncodedBytes) !==
        this.#laneCursors.recovery.cumulativeEncodedBytes
    ) {
      this.reset("protocol-conflict");
      return false;
    }
    if (this.#sourceClosed !== null) {
      if (JSON.stringify(this.#sourceClosed) === JSON.stringify(closed)) return true;
      this.reset("protocol-conflict");
      return false;
    }

    this.#sourceClosed = closed;
    this.#releaseRecoveryRetryCache();
    this.#noteProgress(nowMs);
    this.#completeIfReady();
    return true;
  }

  /** Applies at most `maxApplyFramesPerCall` mutations from the global continuous prefix. */
  continueApply(nowMs: number): number {
    if (!this.#enter(nowMs)) return 0;
    if (
      this.#target === null ||
      this.#appliedCursor === null ||
      this.#start === null ||
      (this.#state !== "assembling" &&
        this.#state !== "handoff-eligible" &&
        this.#state !== "adopted")
    ) {
      return 0;
    }

    let appliedCount = 0;
    while (appliedCount < this.#limits.maxApplyFramesPerCall) {
      const nextEventSeq = this.#appliedCursor.lastEventSeq + 1n;
      const owned = this.#pendingByEventSeq.get(nextEventSeq);
      if (owned === undefined) break;

      let applied: ReturnType<typeof applyMutationCursor>;
      try {
        applied = applyMutationCursor(
          this.#appliedCursor,
          reboundFrame(owned.frame, this.#deliveryGeneration),
        );
      } catch {
        this.reset("protocol-conflict");
        break;
      }

      this.#targetMutationAttempted = true;
      try {
        if (owned.frame.kind === DataFrameKind.PtyOutput) {
          this.#target.writePty(owned.frame.payload);
        } else if (applied.resize !== undefined) {
          this.#target.resize(applied.resize);
        } else {
          this.reset("protocol-conflict");
          break;
        }
      } catch {
        this.reset("apply-failed");
        break;
      }

      this.#appliedCursor = applied.cursor;
      this.#pendingByEventSeq.delete(nextEventSeq);
      owned.pending = false;
      this.#releaseIfUnused(owned);
      appliedCount += 1;
    }

    if (!this.#canPublishApplyProgress()) return appliedCount;
    if (appliedCount > 0 && this.#appliedCursor !== null) {
      this.#latestReplicaApplied = this.#makeReplicaApplied(this.#appliedCursor);
      this.#noteProgress(nowMs);
      this.#refreshHandoffEligibility(nowMs);
      this.#completeIfReady();
    }
    return appliedCount;
  }

  /**
   * Confirms a handoff the caller has already performed. For cold recovery, calling this method
   * transfers candidate ownership before cursor validation so a failure cannot dispose a core that
   * may already be visible.
   */
  confirmHandoff(expectedCursor: ReplicaCursor, nowMs: number): boolean {
    // A cold candidate may already be visible before this call. Relinquish disposal ownership
    // before any deadline or cursor validation can reset the assembler.
    if (this.#state === "handoff-eligible" && this.#mode === "cold" && this.#target !== null) {
      this.#coldCandidateOwned = false;
    }
    if (!this.#enter(nowMs)) {
      return (
        (this.#state === "adopted" || this.#state === "complete") &&
        this.#adoptedCursor !== null &&
        sameReplicaCursor(this.#adoptedCursor, expectedCursor)
      );
    }
    if ((this.#state === "adopted" || this.#state === "complete") && this.#adoptedCursor !== null) {
      if (sameReplicaCursor(this.#adoptedCursor, expectedCursor)) return true;
      this.reset("handoff-conflict");
      return false;
    }
    if (
      this.#state !== "handoff-eligible" ||
      this.#start === null ||
      this.#target === null ||
      this.#appliedCursor === null ||
      this.#mode === null
    ) {
      this.reset("handoff-conflict");
      return false;
    }

    if (!sameReplicaCursor(this.#appliedCursor, expectedCursor)) {
      this.reset("ownership-uncertain");
      return false;
    }

    this.#adoptedCursor = cloneReplicaCursor(expectedCursor);
    this.#recoveryAdopted = {
      type: "recovery-adopted",
      recoveryId: this.#start.recoveryId,
      deliveryGeneration: this.#start.deliveryGeneration,
      replicaApplied: authorityCursor(expectedCursor),
    };
    this.#state = "adopted";
    this.#noteProgress(nowMs);
    this.#completeIfReady();
    return true;
  }

  /**
   * Relinquishes a cold candidate after the host handoff API returned an uncertain outcome.
   * The candidate may already be visible, so this transition must never dispose it or publish
   * RecoveryAdopted.
   */
  abandonHandoffOutcomeUncertain(nowMs: number): boolean {
    if (this.#state !== "handoff-eligible" || this.#mode !== "cold" || this.#target === null) {
      return false;
    }
    this.#coldCandidateOwned = false;
    this.#recordNow(nowMs);
    this.reset("ownership-uncertain");
    return true;
  }

  checkDeadlines(nowMs: number): boolean {
    if (!this.#recordNow(nowMs)) return false;
    return this.#checkDeadlinesAt(nowMs);
  }

  reset(reason: RecoveryAssemblerResetReason): void {
    if (this.#state === "reset" || this.#state === "closed" || this.#state === "complete") return;

    const warmTargetTainted = this.#mode === "warm" && this.#targetMutationAttempted;
    let reusableWarmCursor: AuthorityCursor | null = null;
    if (this.#warm !== null && !warmTargetTainted) {
      reusableWarmCursor =
        this.#mode === "warm" && this.#appliedCursor !== null
          ? authorityCursor(this.#appliedCursor)
          : cloneAuthorityCursor(this.#warm.cursor);
    }

    this.#disposeOwnedColdCandidate();
    this.#clearOwnedRecords();
    this.#target = null;
    this.#installedCandidate = null;
    this.#resetResult = { reason, reusableWarmCursor, warmTargetTainted };
    this.#state = "reset";
  }

  close(): void {
    if (this.#state === "closed") return;
    this.#disposeOwnedColdCandidate();
    this.#clearOwnedRecords();
    this.#target = null;
    this.#installedCandidate = null;
    this.#state = "closed";
  }

  #enter(nowMs: number): boolean {
    if (!this.#recordNow(nowMs)) return false;
    if (this.#state === "reset" || this.#state === "closed" || this.#state === "complete") {
      return false;
    }
    return this.#checkDeadlinesAt(nowMs);
  }

  #recordNow(nowMs: number): boolean {
    assertNow(nowMs);
    if (nowMs < this.#lastNowMs) {
      throw new RangeError("nowMs must be monotonic");
    }
    this.#lastNowMs = nowMs;
    return !(this.#state === "reset" || this.#state === "closed" || this.#state === "complete");
  }

  #checkDeadlinesAt(nowMs: number): boolean {
    if (nowMs - this.#startedAtMs >= this.#limits.recoveryDeadlineMs) {
      this.reset("generation-deadline");
      return false;
    }
    if (nowMs - this.#lastProgressAtMs >= this.#limits.noProgressDeadlineMs) {
      this.reset("no-progress-deadline");
      return false;
    }
    return true;
  }

  #noteProgress(nowMs: number): void {
    this.#lastProgressAtMs = nowMs;
  }

  #validateBufferedStart(start: RecoveryStart): ValidatedStartState {
    let recoveryCursor = replicaCursor(start.base, this.#deliveryGeneration);
    let liveCursor = replicaCursor(start.committedThrough, this.#deliveryGeneration);
    let done: RecoveryDoneRecord | null = null;

    for (const owned of this.#byLaneOrdinal.recovery.values()) {
      if (isMutation(owned.frame)) {
        if (done !== null) throw new Error("recovery mutation follows RecoveryDone");
        if (owned.frame.eventSeq > BigInt(start.committedThrough.eventSeq)) {
          throw new Error("recovery mutation exceeds committed-through");
        }
        recoveryCursor = applyMutationCursor(
          recoveryCursor,
          reboundFrame(owned.frame, this.#deliveryGeneration),
        ).cursor;
      } else {
        if (done !== null) throw new Error("RecoveryDone is duplicated at a new ordinal");
        done = this.#validateDone(owned.envelope, owned.frame, recoveryCursor, start);
      }
    }

    for (const owned of this.#byLaneOrdinal.live.values()) {
      if (!isMutation(owned.frame)) throw new Error("live lane contains a non-mutation");
      liveCursor = applyMutationCursor(
        liveCursor,
        reboundFrame(owned.frame, this.#deliveryGeneration),
      ).cursor;
    }
    return { done, liveCursor, recoveryCursor };
  }

  #validateNewMutation(lane: DeliveryLane, frame: DataFrame): ReplicaCursor {
    const start = this.#start!;
    if (frame.sessionEpoch !== BigInt(start.base.sessionEpoch)) {
      throw new Error("mutation epoch does not match RecoveryStart");
    }
    if (lane === "recovery") {
      if (this.#done !== null) throw new Error("recovery mutation follows RecoveryDone");
      if (frame.eventSeq > BigInt(start.committedThrough.eventSeq)) {
        throw new Error("recovery mutation exceeds committed-through");
      }
      return applyMutationCursor(
        this.#recoveryMutationCursor!,
        reboundFrame(frame, this.#deliveryGeneration),
      ).cursor;
    }
    return applyMutationCursor(
      this.#liveMutationCursor!,
      reboundFrame(frame, this.#deliveryGeneration),
    ).cursor;
  }

  #validateDone(
    envelope: OwnedEnvelope["envelope"],
    frame: DataFrame,
    recoveryCursor: ReplicaCursor,
    explicitStart: RecoveryStart | null = this.#start,
  ): RecoveryDoneRecord {
    const start = explicitStart;
    if (
      start === null ||
      envelope.lane !== "recovery" ||
      frame.kind !== DataFrameKind.ReplayCommit ||
      frame.payload.byteLength !== 0
    ) {
      throw new Error("invalid RecoveryDone record");
    }
    const through = replicaCursor(start.committedThrough, this.#deliveryGeneration);
    if (
      !sameReplicaCursor(recoveryCursor, through) ||
      frame.sessionEpoch !== through.sessionEpoch ||
      frame.eventSeq !== through.lastEventSeq ||
      frame.ptyOffset !== through.nextPtyOffset
    ) {
      throw new Error("RecoveryDone does not exactly close committed-through");
    }
    return {
      type: "recovery-done",
      recoveryId: start.recoveryId,
      deliveryGeneration: start.deliveryGeneration,
      replayedThrough: cloneAuthorityCursor(start.committedThrough),
      recoveryOrdinal: envelope.deliveryOrdinal.toString(),
      cumulativeEncodedBytes: envelope.cumulativeEncodedBytes.toString(),
    };
  }

  #advanceAvailablePrefix(nowMs: number): boolean {
    let cursor = this.#availableCursor;
    if (cursor === null) return false;
    const before = cursor.lastEventSeq;
    while (cursor.lastEventSeq < MAX_U64) {
      const owned = this.#pendingByEventSeq.get(cursor.lastEventSeq + 1n);
      if (owned === undefined) break;
      cursor = applyMutationCursor(
        cursor,
        reboundFrame(owned.frame, this.#deliveryGeneration),
      ).cursor;
    }
    this.#availableCursor = cursor;
    if (cursor.lastEventSeq === before) return false;
    this.#noteProgress(nowMs);
    return true;
  }

  #pendingGapExceeds(): boolean {
    const start = this.#start;
    const base =
      this.#availableCursor?.lastEventSeq ??
      this.#appliedCursor?.lastEventSeq ??
      (start === null ? null : BigInt(start.base.eventSeq));
    if (base === null) return false;
    let highest = base;
    for (const eventSeq of this.#pendingByEventSeq.keys()) {
      if (eventSeq > highest) highest = eventSeq;
    }
    return highest - base > this.#limits.maxGapSpan;
  }

  #makeReplicaApplied(cursor: ReplicaCursor): ReplicaApplied {
    return {
      type: "replica-applied",
      deliveryGeneration: this.#deliveryGeneration.toString(),
      authorityCursor: authorityCursor(cursor),
    };
  }

  #canPublishApplyProgress(): boolean {
    return (
      this.#state === "assembling" ||
      this.#state === "handoff-eligible" ||
      this.#state === "adopted"
    );
  }

  #refreshHandoffEligibility(nowMs: number): void {
    if (
      this.#start === null ||
      this.#done === null ||
      this.#target === null ||
      this.#appliedCursor === null ||
      this.#state === "adopted" ||
      this.#state === "complete" ||
      this.#state === "reset" ||
      this.#state === "closed"
    ) {
      return;
    }
    const committedEventSeq = BigInt(this.#start.committedThrough.eventSeq);
    if (this.#appliedCursor.lastEventSeq < committedEventSeq) return;
    if (this.#state !== "handoff-eligible") this.#noteProgress(nowMs);
    this.#state = "handoff-eligible";
  }

  #completeIfReady(): void {
    if (
      this.#state !== "adopted" ||
      this.#sourceClosed === null ||
      this.#recoveryAdopted === null ||
      this.#latestReplicaApplied === null ||
      this.#pendingByEventSeq.size !== 0
    ) {
      return;
    }
    this.#completion = {
      laneCursors: this.laneCursors,
      recoveryAdopted: cloneRecoveryAdopted(this.#recoveryAdopted)!,
      replicaApplied: cloneReplicaApplied(this.#latestReplicaApplied)!,
    };
    this.#clearOwnedRecords();
    this.#target = null;
    this.#state = "complete";
  }

  #releaseRecoveryRetryCache(): void {
    for (const owned of this.#byLaneOrdinal.recovery.values()) {
      owned.retryRetained = false;
      if (isMutation(owned.frame)) this.#mutationByEventSeq.delete(owned.frame.eventSeq);
      this.#releaseIfUnused(owned);
    }
    this.#byLaneOrdinal.recovery.clear();
  }

  #releaseIfUnused(owned: OwnedEnvelope): void {
    if (owned.pending || owned.retryRetained || !this.#owned.delete(owned)) return;
    this.#ownedBytes -= owned.raw.byteLength;
    this.#ownedFrames -= 1;
  }

  #disposeOwnedColdCandidate(): void {
    if (!this.#coldCandidateOwned || this.#target === null) return;
    this.#coldCandidateOwned = false;
    try {
      this.#target.dispose();
    } catch {
      // Ownership has still been relinquished; cleanup errors cannot justify a second dispose.
    }
  }

  #clearOwnedRecords(): void {
    this.#byLaneOrdinal.live.clear();
    this.#byLaneOrdinal.recovery.clear();
    this.#mutationByEventSeq.clear();
    this.#pendingByEventSeq.clear();
    this.#owned.clear();
    this.#ownedBytes = 0;
    this.#ownedFrames = 0;
  }
}
