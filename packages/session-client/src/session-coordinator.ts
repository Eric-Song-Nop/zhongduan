import {
  DATA_HEADER_BYTES,
  DataFrameKind,
  ProtocolError,
  applyMutationCursor,
  decodeDataFrame,
  type DataFrame,
  type ReplicaCursor,
} from "@zhongduan/protocol";

import type {
  DeliveryState,
  ReplicaHost,
  ReplicaSink,
  ResyncReason,
  SessionCoordinatorOptions,
  SnapshotManifest,
  SnapshotTransport,
  WarmReplayStart,
} from "./types";

const DEFAULT_MAX_BUFFERED_TAIL_BYTES = 2 * 1024 * 1024;
const DEFAULT_MAX_BUFFERED_TAIL_FRAMES = 1_024;
const DEFAULT_RESTORE_DEADLINE_MS = 5_000;
const MAX_U64 = 0xffff_ffff_ffff_ffffn;

interface Delivery {
  start: DeliveryStart;
  streamId: number;
  received: ReplicaCursor;
  applied: ReplicaCursor;
  buffered: DataFrame[];
  bufferedBytes: number;
  commit: ReplicaCursor | null;
  expectedCommit: ReplicaCursor;
  candidate: ReplicaSink | null;
}

type DeliveryStart =
  | {
      mode: "warm";
      streamId: number;
      cursor: ReplicaCursor;
      expectedCommit: ReplicaCursor;
    }
  | { mode: "snapshot"; streamId: number; manifest: SnapshotManifest };

function warmReplayCursor(start: WarmReplayStart): ReplicaCursor {
  return {
    sessionEpoch: BigInt(start.sessionEpoch),
    deliveryGeneration: BigInt(start.deliveryGeneration),
    lastEventSeq: BigInt(start.baseEventSeq),
    nextPtyOffset: BigInt(start.basePtyOffset),
  };
}

function warmReplayCommit(start: WarmReplayStart): ReplicaCursor {
  return {
    sessionEpoch: BigInt(start.sessionEpoch),
    deliveryGeneration: BigInt(start.deliveryGeneration),
    lastEventSeq: BigInt(start.commitEventSeq),
    nextPtyOffset: BigInt(start.commitPtyOffset),
  };
}

function manifestCursor(manifest: SnapshotManifest): ReplicaCursor {
  return {
    sessionEpoch: BigInt(manifest.sessionEpoch),
    deliveryGeneration: BigInt(manifest.deliveryGeneration),
    lastEventSeq: BigInt(manifest.cutEventSeq),
    nextPtyOffset: BigInt(manifest.nextPtyOffset),
  };
}

function representsSameTerminalState(left: ReplicaCursor, right: ReplicaCursor): boolean {
  return (
    left.sessionEpoch === right.sessionEpoch &&
    left.lastEventSeq === right.lastEventSeq &&
    left.nextPtyOffset === right.nextPtyOffset
  );
}

function sameCursor(left: ReplicaCursor, right: ReplicaCursor): boolean {
  return (
    left.sessionEpoch === right.sessionEpoch &&
    left.deliveryGeneration === right.deliveryGeneration &&
    left.lastEventSeq === right.lastEventSeq &&
    left.nextPtyOffset === right.nextPtyOffset
  );
}

function sameSnapshotManifest(left: SnapshotManifest, right: SnapshotManifest): boolean {
  return (
    left.snapshotId === right.snapshotId &&
    left.engineId === right.engineId &&
    left.sessionEpoch === right.sessionEpoch &&
    left.deliveryGeneration === right.deliveryGeneration &&
    left.cutEventSeq === right.cutEventSeq &&
    left.nextPtyOffset === right.nextPtyOffset &&
    left.commitEventSeq === right.commitEventSeq &&
    left.commitPtyOffset === right.commitPtyOffset &&
    left.compression === right.compression &&
    left.compressedLength === right.compressedLength &&
    left.uncompressedLength === right.uncompressedLength &&
    left.sha256 === right.sha256 &&
    left.downloadPath === right.downloadPath &&
    left.restoreThrough === right.restoreThrough
  );
}

function sameDeliveryStart(left: DeliveryStart, right: DeliveryStart): boolean {
  if (left.mode !== right.mode || left.streamId !== right.streamId) return false;
  if (left.mode === "warm" && right.mode === "warm") {
    return (
      sameCursor(left.cursor, right.cursor) && sameCursor(left.expectedCommit, right.expectedCommit)
    );
  }
  return (
    left.mode === "snapshot" &&
    right.mode === "snapshot" &&
    sameSnapshotManifest(left.manifest, right.manifest)
  );
}

function deliveryStartGeneration(start: DeliveryStart): bigint {
  return start.mode === "warm"
    ? start.cursor.deliveryGeneration
    : BigInt(start.manifest.deliveryGeneration);
}

function applyFrame(replica: ReplicaSink, cursor: ReplicaCursor, frame: DataFrame): ReplicaCursor {
  const applied = applyMutationCursor(cursor, frame);
  if (frame.kind === DataFrameKind.PtyOutput) {
    replica.writePty(frame.payload);
  } else if (applied.resize) {
    replica.resize(applied.resize);
  }
  return applied.cursor;
}

export class SessionCoordinator {
  readonly #host: ReplicaHost;
  readonly #snapshots: SnapshotTransport;
  readonly #onAcknowledge: (cursor: ReplicaCursor) => void;
  readonly #onReplicaProgress: (cursor: ReplicaCursor) => void;
  readonly #onResync: (reason: ResyncReason) => void;
  readonly #maxBufferedTailBytes: number;
  readonly #maxBufferedTailFrames: number;
  readonly #restoreDeadlineMs: number;
  readonly #setTimer: NonNullable<SessionCoordinatorOptions["setTimer"]>;
  readonly #clearTimer: NonNullable<SessionCoordinatorOptions["clearTimer"]>;

  #state: DeliveryState = "idle";
  #delivery: Delivery | null = null;
  #pendingFrames: DataFrame[] = [];
  #pendingBytes = 0;
  #liveReplica: ReplicaSink | null;
  #activeCursor: ReplicaCursor | null = null;
  #generationFence: bigint | null;
  #restoreAbort: AbortController | null = null;
  #restoreTimer: ReturnType<typeof setTimeout> | null = null;
  #restoreAttempt = 0;

  constructor(options: SessionCoordinatorOptions) {
    this.#host = options.host;
    this.#liveReplica = options.host.active;
    if (options.initialCursor !== undefined && this.#liveReplica === null) {
      throw new Error("initialCursor requires an active replica");
    }
    this.#activeCursor = options.initialCursor ? { ...options.initialCursor } : null;
    this.#generationFence = options.initialCursor?.deliveryGeneration ?? null;
    this.#snapshots = options.snapshots;
    this.#onAcknowledge = options.onAcknowledge;
    this.#onReplicaProgress = options.onReplicaProgress;
    this.#onResync = options.onResync;
    this.#maxBufferedTailBytes = options.maxBufferedTailBytes ?? DEFAULT_MAX_BUFFERED_TAIL_BYTES;
    this.#maxBufferedTailFrames = options.maxBufferedTailFrames ?? DEFAULT_MAX_BUFFERED_TAIL_FRAMES;
    this.#restoreDeadlineMs = options.restoreDeadlineMs ?? DEFAULT_RESTORE_DEADLINE_MS;
    this.#setTimer =
      options.setTimer ?? ((callback, delayMs) => globalThis.setTimeout(callback, delayMs));
    this.#clearTimer = options.clearTimer ?? ((timer) => globalThis.clearTimeout(timer));
  }

  get state(): DeliveryState {
    return this.#state;
  }

  get activeCursor(): ReplicaCursor | null {
    return this.#activeCursor === null ? null : { ...this.#activeCursor };
  }

  /** Freeze the previous delivery before installing a replacement data socket or sampling attach. */
  fenceDeliveryGeneration(generation: bigint): void {
    this.#assertOpen();
    if (generation < 0n || generation > MAX_U64) {
      throw new Error("delivery generation must be a uint64");
    }
    this.#advanceGenerationFence(generation, true);
  }

  startWarmReplay(start: WarmReplayStart): void {
    this.#assertOpen();
    const cursor = warmReplayCursor(start);
    const expectedCommit = warmReplayCommit(start);
    const identity: DeliveryStart = {
      mode: "warm",
      streamId: start.streamId,
      cursor,
      expectedCommit,
    };
    if (!this.#acceptDeliveryStart(identity)) return;
    this.#advanceGenerationFence(cursor.deliveryGeneration);
    if (!this.#liveReplica || this.#liveReplica.engineId !== this.#host.engineId) {
      this.#requestResync("engine-mismatch");
      return;
    }
    if (
      expectedCommit.lastEventSeq < cursor.lastEventSeq ||
      expectedCommit.nextPtyOffset < cursor.nextPtyOffset ||
      !this.#activeCursor ||
      !representsSameTerminalState(this.#activeCursor, cursor)
    ) {
      this.#requestResync("journal-gap");
      return;
    }
    this.#discardPendingDelivery();
    this.#delivery = {
      start: identity,
      streamId: start.streamId,
      received: cursor,
      applied: cursor,
      buffered: [],
      bufferedBytes: 0,
      commit: null,
      expectedCommit,
      candidate: this.#liveReplica,
    };
    this.#activeCursor = { ...cursor };
    this.#state = "replaying";
    this.#drainPendingFrames(this.#delivery);
  }

  async startSnapshot(manifest: SnapshotManifest): Promise<void> {
    this.#assertOpen();
    const generation = BigInt(manifest.deliveryGeneration);
    const identity: DeliveryStart = {
      mode: "snapshot",
      streamId: manifest.streamId,
      manifest: { ...manifest },
    };
    if (!this.#acceptDeliveryStart(identity)) return;
    this.#advanceGenerationFence(generation);
    if (manifest.engineId !== this.#host.engineId) {
      this.#requestResync("engine-mismatch");
      return;
    }

    this.#discardPendingDelivery();
    const attempt = ++this.#restoreAttempt;
    const cursor = manifestCursor(manifest);
    const expectedCommit = {
      sessionEpoch: cursor.sessionEpoch,
      deliveryGeneration: cursor.deliveryGeneration,
      lastEventSeq: BigInt(manifest.commitEventSeq),
      nextPtyOffset: BigInt(manifest.commitPtyOffset),
    };
    if (
      expectedCommit.lastEventSeq < cursor.lastEventSeq ||
      expectedCommit.nextPtyOffset < cursor.nextPtyOffset
    ) {
      this.#requestResync("journal-gap");
      return;
    }
    this.#delivery = {
      start: identity,
      streamId: manifest.streamId,
      received: cursor,
      applied: cursor,
      buffered: [],
      bufferedBytes: 0,
      commit: null,
      expectedCommit,
      candidate: null,
    };
    this.#state = "restoring";

    this.#drainPendingFrames(this.#delivery);
    if (this.#delivery === null || this.#state !== "restoring") return;

    const abort = new AbortController();
    this.#restoreAbort = abort;
    this.#restoreTimer = this.#setTimer(() => {
      if (this.#restoreAttempt === attempt && this.#state === "restoring") {
        this.#requestResync("slow-client");
      }
    }, this.#restoreDeadlineMs);

    try {
      const snapshot = await this.#snapshots.load(manifest, abort.signal);
      if (attempt !== this.#restoreAttempt || abort.signal.aborted) return;
      const candidate = await this.#host.restore(snapshot, manifest, abort.signal);
      if (attempt !== this.#restoreAttempt || abort.signal.aborted) {
        candidate.dispose();
        return;
      }
      if (candidate.engineId !== this.#host.engineId) {
        candidate.dispose();
        this.#requestResync("engine-mismatch");
        return;
      }

      const delivery = this.#delivery;
      if (!delivery) {
        candidate.dispose();
        return;
      }
      delivery.candidate = candidate;
      this.#flushBuffered(delivery);
      this.#tryCommit(delivery);
    } catch (error) {
      if (attempt !== this.#restoreAttempt || abort.signal.aborted) return;
      this.#requestResync(error instanceof ProtocolError ? "journal-gap" : "restore-failed");
    }
  }

  acceptData(encoded: ArrayBuffer | Uint8Array): void {
    if (this.#state === "closed" || this.#state === "resyncing") return;
    try {
      this.#acceptFrame(decodeDataFrame(encoded), false);
    } catch (error) {
      this.#requestResync(
        error instanceof ProtocolError
          ? error.code === "BAD_EPOCH"
            ? "epoch-changed"
            : "journal-gap"
          : "restore-failed",
      );
    }
  }

  close(): void {
    if (this.#state === "closed") return;
    this.#cancelRestore();
    const candidate = this.#delivery?.candidate;
    if (candidate && candidate !== this.#liveReplica) candidate.dispose();
    this.#delivery = null;
    this.#clearPendingFrames();
    this.#state = "closed";
  }

  #acceptFrame(frame: DataFrame, ownsPayload: boolean): void {
    if (this.#generationFence !== null && frame.deliveryGeneration < this.#generationFence) return;
    this.#advanceGenerationFence(frame.deliveryGeneration);

    if (frame.kind === DataFrameKind.DeliveryBarrier) {
      throw new ProtocolError("BAD_KIND", "delivery barriers are host-to-relay frames");
    }
    if (frame.kind === DataFrameKind.Reset) {
      this.#requestResync("slow-client");
      return;
    }

    const delivery = this.#delivery;
    if (!delivery) {
      this.#bufferPendingFrame(frame, ownsPayload);
      return;
    }
    if (
      frame.deliveryGeneration < delivery.received.deliveryGeneration ||
      frame.streamId !== delivery.streamId
    ) {
      return;
    }

    let applyingLiveReplica = false;
    try {
      if (frame.kind === DataFrameKind.ReplayCommit) {
        this.#acceptCommit(delivery, frame);
        this.#acknowledge(delivery.received);
        this.#tryCommit(delivery);
        return;
      }

      delivery.received = applyMutationCursor(delivery.received, frame).cursor;
      let activeReplicaAdvanced = false;
      if (delivery.candidate) {
        applyingLiveReplica = delivery.candidate === this.#liveReplica;
        delivery.applied = applyFrame(delivery.candidate, delivery.applied, frame);
        activeReplicaAdvanced = applyingLiveReplica;
        applyingLiveReplica = false;
      } else {
        delivery.buffered.push(ownsPayload ? frame : { ...frame, payload: frame.payload.slice() });
        delivery.bufferedBytes += DATA_HEADER_BYTES + frame.payload.byteLength;
        if (this.#bufferBudgetExceeded()) {
          this.#requestResync("slow-client");
          return;
        }
      }
      if (activeReplicaAdvanced) {
        this.#activeCursor = { ...delivery.applied };
        this.#reportReplicaProgress(delivery.applied);
      }
      this.#acknowledge(delivery.received);
    } catch (error) {
      if (applyingLiveReplica && !(error instanceof ProtocolError)) {
        this.#activeCursor = null;
      }
      throw error;
    }
  }

  #bufferPendingFrame(frame: DataFrame, ownsPayload: boolean): void {
    this.#pendingFrames.push(ownsPayload ? frame : { ...frame, payload: frame.payload.slice() });
    this.#pendingBytes += DATA_HEADER_BYTES + frame.payload.byteLength;
    if (this.#bufferBudgetExceeded()) this.#requestResync("slow-client");
  }

  #drainPendingFrames(delivery: Delivery): void {
    const ready: DataFrame[] = [];
    const retained: DataFrame[] = [];
    let retainedBytes = 0;
    for (const frame of this.#pendingFrames) {
      if (
        frame.deliveryGeneration === delivery.received.deliveryGeneration &&
        frame.streamId === delivery.streamId
      ) {
        ready.push(frame);
      } else if (frame.deliveryGeneration > delivery.received.deliveryGeneration) {
        retained.push(frame);
        retainedBytes += DATA_HEADER_BYTES + frame.payload.byteLength;
      }
    }
    this.#pendingFrames = retained;
    this.#pendingBytes = retainedBytes;

    for (const frame of ready) {
      if (this.#delivery !== delivery || this.#state === "resyncing") return;
      try {
        this.#acceptFrame(frame, true);
      } catch (error) {
        this.#requestResync(
          error instanceof ProtocolError
            ? error.code === "BAD_EPOCH"
              ? "epoch-changed"
              : "journal-gap"
            : "restore-failed",
        );
        return;
      }
    }
  }

  #bufferBudgetExceeded(): boolean {
    return (
      this.#pendingBytes + (this.#delivery?.bufferedBytes ?? 0) > this.#maxBufferedTailBytes ||
      this.#pendingFrames.length + (this.#delivery?.buffered.length ?? 0) >
        this.#maxBufferedTailFrames
    );
  }

  #clearPendingFrames(): void {
    this.#pendingFrames = [];
    this.#pendingBytes = 0;
  }

  #advanceGenerationFence(generation: bigint, resumeFromResync = false): void {
    if (this.#generationFence !== null && generation <= this.#generationFence) return;
    this.#generationFence = generation;

    if (
      this.#delivery !== null &&
      this.#delivery.received.deliveryGeneration < this.#generationFence
    ) {
      this.#discardPendingDelivery();
    }
    this.#discardPendingFramesBefore(generation);
    if (this.#state !== "closed" && (this.#state !== "resyncing" || resumeFromResync)) {
      this.#state = "awaiting-control";
    }
  }

  #discardPendingFramesBefore(generation: bigint): void {
    this.#pendingFrames = this.#pendingFrames.filter(
      (frame) => frame.deliveryGeneration >= generation,
    );
    this.#pendingBytes = this.#pendingFrames.reduce(
      (total, frame) => total + DATA_HEADER_BYTES + frame.payload.byteLength,
      0,
    );
  }

  #acceptCommit(delivery: Delivery, frame: DataFrame): void {
    if (frame.payload.byteLength !== 0) {
      throw new ProtocolError("BAD_PAYLOAD", "replay commit payload must be empty");
    }
    if (
      frame.sessionEpoch !== delivery.received.sessionEpoch ||
      frame.eventSeq !== delivery.received.lastEventSeq ||
      frame.ptyOffset !== delivery.received.nextPtyOffset
    ) {
      throw new ProtocolError("EVENT_GAP", "replay commit does not match received tail");
    }
    if (
      frame.eventSeq !== delivery.expectedCommit.lastEventSeq ||
      frame.ptyOffset !== delivery.expectedCommit.nextPtyOffset
    ) {
      throw new ProtocolError(
        "EVENT_GAP",
        "replay commit does not match pinned delivery watermark",
      );
    }
    delivery.commit = delivery.received;
  }

  #flushBuffered(delivery: Delivery): void {
    const candidate = delivery.candidate;
    if (!candidate) return;
    for (const frame of delivery.buffered) {
      delivery.applied = applyFrame(candidate, delivery.applied, frame);
    }
    delivery.buffered = [];
    delivery.bufferedBytes = 0;
  }

  #tryCommit(delivery: Delivery): void {
    if (!delivery.candidate || !delivery.commit) return;
    if (
      delivery.applied.lastEventSeq !== delivery.received.lastEventSeq ||
      delivery.applied.nextPtyOffset !== delivery.received.nextPtyOffset
    ) {
      this.#requestResync("journal-gap");
      return;
    }

    if (this.#state === "restoring") {
      this.#host.adopt(delivery.candidate, delivery.applied);
      this.#liveReplica = delivery.candidate;
      this.#activeCursor = { ...delivery.applied };
      this.#reportReplicaProgress(delivery.applied);
      this.#restoreAbort = null;
    }
    this.#finishRestoreTimer();
    this.#state = "live";
  }

  #requestResync(reason: ResyncReason): void {
    if (this.#state === "closed" || this.#state === "resyncing") return;
    this.#state = "resyncing";
    this.#cancelRestore();
    const candidate = this.#delivery?.candidate;
    if (candidate && candidate !== this.#liveReplica) candidate.dispose();
    this.#delivery = null;
    this.#clearPendingFrames();
    this.#onResync(reason);
  }

  #cancelRestore(): void {
    ++this.#restoreAttempt;
    this.#restoreAbort?.abort();
    this.#restoreAbort = null;
    this.#finishRestoreTimer();
  }

  #discardPendingDelivery(): void {
    this.#cancelRestore();
    const candidate = this.#delivery?.candidate;
    if (candidate && candidate !== this.#liveReplica) candidate.dispose();
    this.#delivery = null;
  }

  #finishRestoreTimer(): void {
    if (this.#restoreTimer !== null) this.#clearTimer(this.#restoreTimer);
    this.#restoreTimer = null;
  }

  #acknowledge(cursor: ReplicaCursor): void {
    this.#onAcknowledge({ ...cursor });
  }

  #reportReplicaProgress(cursor: ReplicaCursor): void {
    this.#onReplicaProgress({ ...cursor });
  }

  #acceptDeliveryStart(start: DeliveryStart): boolean {
    if (this.#state === "resyncing") return false;
    const generation = deliveryStartGeneration(start);
    if (this.#generationFence !== null && generation < this.#generationFence) return false;
    const currentStart = this.#delivery?.start;
    if (currentStart !== undefined) {
      const currentGeneration = deliveryStartGeneration(currentStart);
      if (generation < currentGeneration) return false;
      if (generation === currentGeneration) {
        if (sameDeliveryStart(currentStart, start)) return false;
        this.#requestResync("journal-gap");
        return false;
      }
    }
    return !(this.#activeCursor !== null && generation < this.#activeCursor.deliveryGeneration);
  }

  #assertOpen(): void {
    if (this.#state === "closed") throw new Error("session coordinator is closed");
  }
}
