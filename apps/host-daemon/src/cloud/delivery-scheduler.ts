import {
  DataFrameFlag,
  DataFrameKind,
  MAX_DELIVERY_OUTSTANDING_BYTES,
  decodeDataFrame,
  deliveryOutstandingBytes,
  encodeDataFrame,
  rewriteDelivery,
  type HostControlFrame,
  type RelayToHostControlFrame,
} from "@zhongduan/protocol";
import {
  createBufferedTelemetrySink,
  elapsedMs,
  type BufferedTelemetrySink,
  type TelemetrySink,
  type TerminalTelemetryEvent,
} from "@zhongduan/telemetry";

import type {
  ReplayCursor,
  ReplayRangeMeasurement,
  SnapshotCapture,
  TerminalSession,
} from "../session";
import { CanonicalPublisher, HOST_CANONICAL_QUEUE_LIMITS } from "./canonical-publisher";
import {
  DeliveryBarrierWaiter,
  type BarrierIdentity,
  type DeliveryBarrierResult,
} from "./delivery-barrier-waiter";
import { DeliveryRecoveryQueue, recoveryKey, type AttachRequest } from "./delivery-recovery-queue";
import { SnapshotCheckpointCache, type SnapshotCheckpoint } from "./snapshot-checkpoint-cache";
import {
  RetryableSnapshotPublishError,
  SnapshotCleanupConfirmedError,
  SnapshotUnavailableError,
  type PublishedSnapshot,
} from "./snapshot-publisher";

export { HOST_CANONICAL_QUEUE_LIMITS } from "./canonical-publisher";
export const WARM_REPLAY_MAX_OUTSTANDING_BYTES = 256 * 1024;
export const WARM_REPLAY_MAX_FRAMES = 512;
export const SNAPSHOT_ENCODE_BUDGET_MS = 5_000;
export const SNAPSHOT_PUBLISH_TIMEOUT_MS = 120_000;
export const DELIVERY_BARRIER_TIMEOUT_MS = 5_000;
export const SNAPSHOT_RECOVERY_QUIET_MS = 250;
export const SNAPSHOT_RECOVERY_MAX_RETRY_MS = 5_000;
export const SNAPSHOT_RECOVERY_MAX_QUIET_WAIT_MS = 5_000;

const PUMP_YIELD_BYTES = 256 * 1024;
const PUMP_YIELD_FRAMES = 64;
const EMPTY_PAYLOAD = new Uint8Array();

type DeliveryReset = Extract<RelayToHostControlFrame, { type: "delivery-reset" }>;

interface ActiveRecovery {
  controller: AbortController;
  mode: "cold" | "warm" | undefined;
  phase: "pre-marker" | "marker-uncertain" | "pinned" | "committed";
  request: AttachRequest;
}

interface ColdPreparation {
  checkpoint?: SnapshotCheckpoint;
  controller: AbortController;
  request: AttachRequest;
  status: "publishing" | "ready";
}

export interface HostDeliverySchedulerOptions {
  bufferedAmount?: () => number;
  closePair: (reason: string) => void;
  monotonicNow?: () => number;
  sendControl: (frame: HostControlFrame) => void;
  sendData: (frame: Uint8Array) => void;
  session: TerminalSession;
  snapshotCheckpointCache?: SnapshotCheckpointCache;
  snapshotPublisher: SnapshotPublisherLike;
  telemetryBuffer?: BufferedTelemetrySink;
  telemetry?: TelemetrySink;
  yieldIo?: () => Promise<void>;
}

export interface SnapshotPublisherLike {
  publish(snapshot: SnapshotCapture, signal?: AbortSignal): Promise<PublishedSnapshot>;
  resumePending?(signal?: AbortSignal): Promise<PublishedSnapshot> | undefined;
}

export class HostDeliveryScheduler {
  readonly #bufferedAmount: () => number;
  readonly #barriers: DeliveryBarrierWaiter;
  readonly #canonicalPublisher: CanonicalPublisher;
  readonly #closePair: (reason: string) => void;
  readonly #connectionAbort = new AbortController();
  readonly #monotonicNow: () => number;
  readonly #sendControl: (frame: HostControlFrame) => void;
  readonly #sendDataFrame: (frame: Uint8Array) => void;
  readonly #session: TerminalSession;
  readonly #snapshotCheckpointCache: SnapshotCheckpointCache;
  readonly #snapshotPublisher: SnapshotPublisherLike;
  readonly #telemetry: TelemetrySink | undefined;
  readonly #telemetryBuffer: BufferedTelemetrySink | undefined;
  readonly #yieldIo: () => Promise<void>;
  readonly #coldPreparations = new Map<string, ColdPreparation>();
  readonly #recoveryQueue: DeliveryRecoveryQueue;

  #activeRecovery: ActiveRecovery | undefined;
  #coldBuildInFlight: ColdPreparation | undefined;
  #coldCaptureNotBefore: number | undefined;
  #coldCaptureQuietDeadline: number | undefined;
  #failed = false;
  #lastCanonicalAt = Number.NEGATIVE_INFINITY;
  #ready = false;
  #recoveriesRunning = false;
  #recoveryTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(options: HostDeliverySchedulerOptions) {
    this.#bufferedAmount = options.bufferedAmount ?? (() => 0);
    this.#closePair = options.closePair;
    this.#monotonicNow = options.monotonicNow ?? performance.now.bind(performance);
    this.#sendControl = options.sendControl;
    this.#sendDataFrame = options.sendData;
    this.#session = options.session;
    this.#snapshotCheckpointCache =
      options.snapshotCheckpointCache ?? new SnapshotCheckpointCache();
    this.#snapshotPublisher = options.snapshotPublisher;
    this.#telemetryBuffer =
      options.telemetryBuffer ??
      (options.telemetry === undefined
        ? undefined
        : createBufferedTelemetrySink(options.telemetry));
    this.#telemetry = this.#telemetryBuffer?.sink;
    this.#yieldIo = options.yieldIo ?? (() => new Promise((resolve) => setImmediate(resolve)));
    this.#recoveryQueue = new DeliveryRecoveryQueue({
      maxQuietWaitMs: SNAPSHOT_RECOVERY_MAX_QUIET_WAIT_MS,
      maxRetryMs: SNAPSHOT_RECOVERY_MAX_RETRY_MS,
      monotonicNow: this.#monotonicNow,
      quietMs: SNAPSHOT_RECOVERY_QUIET_MS,
    });
    this.#canonicalPublisher = new CanonicalPublisher({
      canInterruptRecovery: () => this.#activeRecovery?.phase === "pre-marker",
      onFailure: (reason) => this.#failPair(reason),
      onIngress: () => {
        this.#lastCanonicalAt = this.#monotonicNow();
      },
      onRecoveryPressure: () => this.#abortRecoveryForPressure(),
      sendData: (frame) => this.#sendData(frame),
      session: this.#session,
      yieldIo: this.#yieldIo,
    });
    this.#barriers = new DeliveryBarrierWaiter({
      sendData: (frame) => this.#sendData(frame),
      sessionEpoch: () => this.#session.sessionEpoch,
    });
  }

  prepareHostReady(): ReplayCursor {
    return this.#canonicalPublisher.prepare();
  }

  activateHostReady(acknowledged: ReplayCursor): void {
    this.#canonicalPublisher.activate(acknowledged);
    this.#ready = true;
    this.#scheduleRecoveries();
  }

  enqueueAttach(request: AttachRequest): void {
    if (this.#failed) return;
    const superseded = this.#recoveryQueue.enqueue(request);
    this.#abortColdPreparations(superseded, "delivery generation was superseded");
    if (
      this.#activeRecovery?.request.streamId === request.streamId &&
      BigInt(this.#activeRecovery.request.deliveryGeneration) < BigInt(request.deliveryGeneration)
    ) {
      this.#activeRecovery.controller.abort(new Error("delivery generation was superseded"));
    }
    this.#scheduleRecoveries();
  }

  handleDeliveryReset(reset: DeliveryReset): void {
    const removed = this.#recoveryQueue.reset(reset.streamId, reset.deliveryGeneration);
    this.#abortColdPreparations(removed, "delivery was reset");
    if (
      this.#activeRecovery?.request.streamId === reset.streamId &&
      this.#activeRecovery.request.deliveryGeneration === reset.deliveryGeneration
    ) {
      this.#activeRecovery.controller.abort(new Error("delivery was reset"));
    }
    this.#schedulePendingRecoveries();
  }

  handleBarrierResult(result: DeliveryBarrierResult): void {
    this.#barriers.handle(result);
  }

  dispose(reason = "delivery scheduler stopped"): void {
    if (this.#failed) return;
    this.#failed = true;
    this.#ready = false;
    this.#canonicalPublisher.dispose();
    this.#telemetryBuffer?.resume();
    this.#connectionAbort.abort(new Error(reason));
    this.#activeRecovery?.controller.abort(new Error(reason));
    this.#coldBuildInFlight?.controller.abort(new Error(reason));
    for (const preparation of this.#coldPreparations.values()) {
      preparation.controller.abort(new Error(reason));
    }
    this.#coldPreparations.clear();
    this.#recoveryQueue.clear();
    if (this.#recoveryTimer !== undefined) clearTimeout(this.#recoveryTimer);
    this.#recoveryTimer = undefined;
    this.#barriers.dispose(new Error(reason));
  }

  #abortRecoveryForPressure(): void {
    const active = this.#activeRecovery;
    if (active === undefined || active.phase !== "pre-marker") {
      this.#failPair("canonical publisher queue exceeded");
      return;
    }
    this.#recoveryQueue.requeue(active.request);
    this.#discardColdPreparation(active.request);
    this.#recoveryQueue.defer(active.request, SNAPSHOT_RECOVERY_QUIET_MS);
    active.controller.abort(new Error("canonical pressure interrupted recovery"));
    this.#resumeCanonical();
  }

  #abortColdPreparations(keys: string[], reason: string): void {
    for (const key of keys) {
      const preparation = this.#coldPreparations.get(key);
      preparation?.controller.abort(new Error(reason));
      this.#coldPreparations.delete(key);
    }
  }

  #discardColdPreparation(request: AttachRequest): void {
    const key = recoveryKey(request);
    const preparation = this.#coldPreparations.get(key);
    preparation?.controller.abort(new Error("cold preparation was discarded"));
    this.#coldPreparations.delete(key);
  }

  #scheduleRecoveries(): void {
    if (this.#recoveriesRunning || this.#failed || !this.#ready) return;
    if (!this.#hasRunnableRecovery()) {
      this.#scheduleRecoveryTimer();
      return;
    }
    if (this.#recoveryTimer !== undefined) clearTimeout(this.#recoveryTimer);
    this.#recoveryTimer = undefined;
    this.#recoveriesRunning = true;
    void this.#drainRecoveries().then(
      () => {
        this.#recoveriesRunning = false;
        this.#schedulePendingRecoveries();
      },
      (error: unknown) => {
        this.#recoveriesRunning = false;
        this.#failPair(error instanceof Error ? error.message : "delivery recovery failed");
      },
    );
  }

  async #drainRecoveries(): Promise<void> {
    const requests = this.#recoveryQueue.takeRunnablePass((request) =>
      this.#isRunnableRecovery(request),
    );
    for (const request of requests) {
      if (this.#failed) return;
      const active: ActiveRecovery = {
        controller: new AbortController(),
        mode: undefined,
        phase: "pre-marker",
        request,
      };
      this.#activeRecovery = active;
      await this.#recover(request, active);
      if (this.#activeRecovery === active) this.#activeRecovery = undefined;
    }
  }

  #schedulePendingRecoveries(): void {
    if (this.#failed || !this.#ready || this.#recoveryQueue.pendingSize === 0) return;
    if (this.#hasRunnableRecovery()) {
      this.#scheduleRecoveries();
      return;
    }
    this.#scheduleRecoveryTimer();
  }

  #hasRunnableRecovery(): boolean {
    return this.#recoveryQueue.some((request) => this.#isRunnableRecovery(request));
  }

  #isRunnableRecovery(request: AttachRequest): boolean {
    const key = recoveryKey(request);
    if (!this.#recoveryQueue.isCold(request)) return true;
    if (this.#recoveryQueue.readyAt(request, this.#lastCanonicalAt) > this.#monotonicNow()) {
      return false;
    }
    const preparation = this.#coldPreparations.get(key);
    if (preparation?.status === "ready") return true;
    if (this.#snapshotCheckpointCache.current() !== undefined) return true;
    if (preparation?.status === "publishing" || this.#coldBuildInFlight !== undefined) return false;
    return this.#coldCaptureReadyAt(request) <= this.#monotonicNow();
  }

  #scheduleRecoveryTimer(): void {
    if (this.#failed || !this.#ready || this.#recoveryQueue.pendingSize === 0) return;
    let earliest = Number.POSITIVE_INFINITY;
    this.#recoveryQueue.forEach((request) => {
      const key = recoveryKey(request);
      if (!this.#recoveryQueue.isCold(request)) {
        earliest = 0;
        return;
      }
      const retryReadyAt = this.#recoveryQueue.readyAt(request, this.#lastCanonicalAt);
      if (retryReadyAt > this.#monotonicNow()) {
        earliest = Math.min(earliest, retryReadyAt);
        return;
      }
      const preparation = this.#coldPreparations.get(key);
      if (preparation?.status === "ready") earliest = 0;
      if (this.#snapshotCheckpointCache.current() !== undefined) earliest = 0;
      if (preparation?.status === "publishing" || this.#coldBuildInFlight !== undefined) return;
      earliest = Math.min(earliest, this.#coldCaptureReadyAt(request));
    });
    if (!Number.isFinite(earliest)) return;
    const remaining = Math.max(0, Math.ceil(earliest - this.#monotonicNow()));
    if (remaining === 0) {
      this.#scheduleRecoveries();
      return;
    }
    if (this.#recoveryTimer !== undefined) clearTimeout(this.#recoveryTimer);
    this.#recoveryTimer = setTimeout(() => {
      this.#recoveryTimer = undefined;
      this.#scheduleRecoveries();
    }, remaining);
  }

  async #recover(request: AttachRequest, active: ActiveRecovery): Promise<void> {
    if (request.engineId !== this.#session.engineId) {
      this.#sendReplayUnavailable(request.connectionId, "engine-mismatch");
      return;
    }
    if (request.hasLiveReplica && BigInt(request.lastSessionEpoch) !== this.#session.sessionEpoch) {
      this.#sendReplayUnavailable(request.connectionId, "epoch-changed");
      return;
    }

    try {
      const key = recoveryKey(request);
      if (request.hasLiveReplica && !this.#recoveryQueue.isCold(request)) {
        await this.#pauseCanonical();
        if (this.#failed) return;
        const commit = this.#session.cursor;
        const base = cursorFromAttach(request);
        const measuredReplay = this.#session.replayAndMeasureThrough(base, commit);
        this.#emitRange("warm", measuredReplay.measurement);
        const replay = measuredReplay.replay;
        if (
          replay.status === "ok" &&
          deliveryOutstandingBytes(
            { eventSeq: base.lastEventSeq, nextPtyOffset: base.nextPtyOffset },
            { eventSeq: commit.lastEventSeq, nextPtyOffset: commit.nextPtyOffset },
          ) <= BigInt(WARM_REPLAY_MAX_OUTSTANDING_BYTES) &&
          replay.frames.length <= WARM_REPLAY_MAX_FRAMES &&
          WARM_REPLAY_MAX_OUTSTANDING_BYTES <= MAX_DELIVERY_OUTSTANDING_BYTES
        ) {
          active.mode = "warm";
          const result = await this.#deliverWarm(request, base, commit, replay.frames, active);
          if (result.status === "ready" || result.status === "stale") {
            this.#recoveryQueue.complete(request);
            return;
          }
          const retryScope = deliveryBarrierRetryScope(result);
          if (retryScope === "drop-client" || retryScope === "reset-generation") {
            this.#recoveryQueue.complete(request);
            return;
          }
          this.#recoveryQueue.markCold(request);
          this.#recoveryQueue.requeue(request);
          return;
        }
        this.#resumeCanonical();
      }
      active.mode = "cold";
      this.#recoveryQueue.markCold(request);
      let preparation = this.#coldPreparations.get(key);
      const recentCheckpoint = this.#snapshotCheckpointCache.current();
      if (preparation === undefined && recentCheckpoint !== undefined) {
        preparation = {
          checkpoint: recentCheckpoint,
          controller: new AbortController(),
          request,
          status: "ready",
        };
        this.#coldPreparations.set(key, preparation);
      }
      if (preparation === undefined) {
        if (
          this.#coldBuildInFlight === undefined &&
          this.#coldCaptureReadyAt(request) <= this.#monotonicNow()
        ) {
          this.#startColdPreparation(request);
        } else {
          this.#recoveryQueue.requeue(request);
        }
        return;
      }
      if (preparation.status !== "ready") return;
      const result = await this.#deliverPreparedCold(request, preparation, active);
      if (result.status === "ready" || result.status === "stale") {
        this.#recoveryQueue.complete(request);
        this.#discardColdPreparation(request);
        return;
      }
      const retryScope = deliveryBarrierRetryScope(result);
      if (retryScope === "drop-client" || retryScope === "reset-generation") {
        this.#recoveryQueue.complete(request);
        this.#discardColdPreparation(request);
        return;
      }
      if (retryScope === "refresh-checkpoint") {
        if (preparation.checkpoint !== undefined) {
          this.#invalidateCheckpoint(preparation.checkpoint);
        }
        this.#discardColdPreparation(request);
      }
      this.#recoveryQueue.requeue(request);
      this.#recoveryQueue.defer(request, SNAPSHOT_RECOVERY_QUIET_MS);
    } catch (error) {
      if (active.phase === "marker-uncertain" || active.phase === "pinned") {
        this.#failPair("delivery marker outcome is uncertain");
        return;
      }
      if (active.phase === "committed" || this.#connectionAbort.signal.aborted) return;
      if (active.controller.signal.aborted) {
        this.#resumeCanonical();
        return;
      }
      if (active.mode === "cold" && isRetryableColdRecoveryError(error)) {
        this.#discardColdPreparation(request);
        this.#recoveryQueue.requeue(request);
        const publisherDelay = snapshotRetryAfter(error);
        const retryDelay = this.#recoveryQueue.deferAfterFailure(request, publisherDelay);
        this.#deferColdCaptures(retryDelay);
        this.#resumeCanonical();
        return;
      }
      this.#failPair(error instanceof Error ? error.message : "delivery recovery failed");
    }
  }

  #startColdPreparation(request: AttachRequest): void {
    const key = recoveryKey(request);
    if (this.#coldBuildInFlight !== undefined || this.#coldPreparations.has(key)) {
      this.#recoveryQueue.requeue(request);
      return;
    }
    const preparation: ColdPreparation = {
      controller: new AbortController(),
      request,
      status: "publishing",
    };
    this.#coldBuildInFlight = preparation;
    this.#coldPreparations.set(key, preparation);
    void this.#prepareCold(preparation).then(
      (checkpoint) => {
        if (this.#coldBuildInFlight === preparation) this.#coldBuildInFlight = undefined;
        if (
          this.#failed ||
          preparation.controller.signal.aborted ||
          this.#coldPreparations.get(key) !== preparation
        ) {
          this.#schedulePendingRecoveries();
          return;
        }
        preparation.checkpoint = this.#snapshotCheckpointCache.install(checkpoint);
        preparation.status = "ready";
        this.#coldCaptureNotBefore = undefined;
        this.#coldCaptureQuietDeadline = undefined;
        this.#recoveryQueue.requeue(request);
        this.#schedulePendingRecoveries();
      },
      (error: unknown) => {
        if (this.#coldBuildInFlight === preparation) this.#coldBuildInFlight = undefined;
        const isCurrent = this.#coldPreparations.get(key) === preparation;
        if (isCurrent) this.#coldPreparations.delete(key);
        if (this.#failed || preparation.controller.signal.aborted || !isCurrent) {
          this.#schedulePendingRecoveries();
          return;
        }
        if (isRetryableColdRecoveryError(error)) {
          this.#recoveryQueue.requeue(request);
          const retryDelay = this.#recoveryQueue.deferAfterFailure(
            request,
            snapshotRetryAfter(error),
          );
          this.#deferColdCaptures(retryDelay);
          this.#schedulePendingRecoveries();
          return;
        }
        this.#failPair(error instanceof Error ? error.message : "snapshot preparation failed");
      },
    );
  }

  async #prepareCold(preparation: ColdPreparation): Promise<SnapshotCheckpoint> {
    const resumed = this.#resumePendingSnapshot(preparation);
    if (resumed !== undefined) {
      return checkpointFromPublished(await resumed, this.#session);
    }
    const snapshot = await this.#captureSnapshot(preparation);
    preparation.controller.signal.throwIfAborted();
    if (snapshot.timing.actorPauseMs > SNAPSHOT_ENCODE_BUDGET_MS) {
      throw new SnapshotEncodeBudgetError();
    }
    const published = await this.#publishSnapshot(snapshot, preparation);
    preparation.controller.signal.throwIfAborted();
    assertPublishedSnapshotMatchesCapture(published, snapshot);
    return checkpointFromPublished(published, this.#session);
  }

  #resumePendingSnapshot(preparation: ColdPreparation): Promise<PublishedSnapshot> | undefined {
    const startedAt = this.#monotonicNow();
    const deadline = new AbortController();
    const timeout = setTimeout(
      () => deadline.abort(new DOMException("snapshot publish timed out", "TimeoutError")),
      SNAPSHOT_PUBLISH_TIMEOUT_MS,
    );
    const signal = AbortSignal.any([
      preparation.controller.signal,
      deadline.signal,
      this.#connectionAbort.signal,
    ]);
    let operation: Promise<PublishedSnapshot> | undefined;
    try {
      operation = this.#snapshotPublisher.resumePending?.(signal);
    } catch (error) {
      clearTimeout(timeout);
      this.#emitPublishTotal(
        "pending",
        startedAt,
        this.#publishOutcome(error, deadline, preparation),
      );
      throw error;
    }
    if (operation === undefined) {
      clearTimeout(timeout);
      return undefined;
    }
    return raceAbort(operation, signal)
      .then(
        (published) => {
          this.#emitPublishTotal("pending", startedAt, "ready");
          return published;
        },
        (error: unknown) => {
          this.#emitPublishTotal(
            "pending",
            startedAt,
            this.#publishOutcome(error, deadline, preparation),
          );
          throw error;
        },
      )
      .finally(() => clearTimeout(timeout));
  }

  async #captureSnapshot(preparation: ColdPreparation): Promise<SnapshotCapture> {
    const startedAt = this.#monotonicNow();
    const deadline = new AbortController();
    const timeout = setTimeout(
      () => deadline.abort(new SnapshotEncodeBudgetError()),
      SNAPSHOT_ENCODE_BUDGET_MS,
    );
    const signal = AbortSignal.any([
      preparation.controller.signal,
      deadline.signal,
      this.#connectionAbort.signal,
    ]);
    try {
      const snapshot = await raceAbort(this.#session.captureSnapshot(), signal);
      this.#enqueueTelemetry({
        schemaVersion: 1,
        monotonicAtMs: this.#monotonicNow(),
        name: "host.snapshot.capture",
        outcome: "ready",
        ...snapshot.timing,
        snapshotBytes: snapshot.bytes.byteLength,
      });
      return snapshot;
    } catch (error) {
      const finishedAt = this.#monotonicNow();
      this.#enqueueTelemetry({
        schemaVersion: 1,
        monotonicAtMs: finishedAt,
        name: "host.snapshot.capture",
        outcome: deadline.signal.aborted
          ? "timeout"
          : preparation.controller.signal.aborted || this.#connectionAbort.signal.aborted
            ? "cancelled"
            : "failed",
        totalDurationMs: elapsedMs(startedAt, finishedAt),
      });
      throw error;
    } finally {
      clearTimeout(timeout);
    }
  }

  async #publishSnapshot(
    snapshot: SnapshotCapture,
    preparation: ColdPreparation,
  ): Promise<PublishedSnapshot> {
    const startedAt = this.#monotonicNow();
    const deadline = new AbortController();
    const timeout = setTimeout(
      () => deadline.abort(new DOMException("snapshot publish timed out", "TimeoutError")),
      SNAPSHOT_PUBLISH_TIMEOUT_MS,
    );
    const signal = AbortSignal.any([
      preparation.controller.signal,
      deadline.signal,
      this.#connectionAbort.signal,
    ]);
    try {
      const published = await raceAbort(this.#snapshotPublisher.publish(snapshot, signal), signal);
      this.#emitPublishTotal("fresh", startedAt, "ready", snapshot.bytes.byteLength);
      return published;
    } catch (error) {
      this.#emitPublishTotal(
        "fresh",
        startedAt,
        this.#publishOutcome(error, deadline, preparation),
        snapshot.bytes.byteLength,
      );
      throw error;
    } finally {
      clearTimeout(timeout);
    }
  }

  async #deliverWarm(
    request: AttachRequest,
    base: ReplayCursor,
    commit: ReplayCursor,
    replay: Uint8Array[],
    active: ActiveRecovery,
  ): Promise<DeliveryBarrierResult> {
    await this.#flushCanonicalThrough(commit);
    active.controller.signal.throwIfAborted();
    const identity: BarrierIdentity = {
      mode: "warm",
      connectionId: request.connectionId,
      streamId: request.streamId,
      deliveryGeneration: request.deliveryGeneration,
      commitEventSeq: commit.lastEventSeq.toString(),
      commitPtyOffset: commit.nextPtyOffset.toString(),
    };
    const deadline = new AbortController();
    const timeout = setTimeout(
      () => deadline.abort(new DOMException("delivery barrier timed out", "TimeoutError")),
      DELIVERY_BARRIER_TIMEOUT_MS,
    );
    let result: DeliveryBarrierResult;
    try {
      result = await this.#barriers.wait(
        identity,
        AbortSignal.any([active.controller.signal, deadline.signal, this.#connectionAbort.signal]),
        () => {
          active.phase = "marker-uncertain";
        },
      );
    } finally {
      clearTimeout(timeout);
    }
    if (result.status !== "ready") {
      active.phase = "pre-marker";
      this.#resumeCanonical();
      return result;
    }
    active.phase = "pinned";
    await this.#sendDirectedReplay(request, base, commit, replay, active);
    this.#sendReplayCommit(request, commit);
    active.phase = "committed";
    this.#resumeCanonical();
    return result;
  }

  async #deliverPreparedCold(
    request: AttachRequest,
    preparation: ColdPreparation,
    active: ActiveRecovery,
  ): Promise<DeliveryBarrierResult> {
    const checkpoint = preparation.checkpoint;
    if (checkpoint === undefined) {
      throw new Error("prepared snapshot is incomplete");
    }
    if (
      checkpoint.engineId !== this.#session.engineId ||
      checkpoint.base.sessionEpoch !== this.#session.sessionEpoch
    ) {
      throw new Error("published checkpoint does not match the terminal authority");
    }
    await this.#pauseCanonical();
    if (this.#failed) {
      throw this.#connectionAbort.signal.reason ?? new Error("delivery scheduler stopped");
    }
    active.controller.signal.throwIfAborted();
    const base = checkpoint.base;
    const commit = this.#session.cursor;
    const measuredReplay = this.#session.replayAndMeasureThrough(base, commit);
    this.#emitRange("snapshot", measuredReplay.measurement);
    const replay = measuredReplay.replay;
    if (
      replay.status !== "ok" ||
      deliveryOutstandingBytes(
        { eventSeq: base.lastEventSeq, nextPtyOffset: base.nextPtyOffset },
        { eventSeq: commit.lastEventSeq, nextPtyOffset: commit.nextPtyOffset },
      ) > BigInt(WARM_REPLAY_MAX_OUTSTANDING_BYTES) ||
      replay.frames.length > WARM_REPLAY_MAX_FRAMES
    ) {
      this.#invalidateCheckpoint(checkpoint);
      throw new ColdTailUnavailableError();
    }
    await this.#flushCanonicalThrough(commit);
    active.controller.signal.throwIfAborted();
    const deadline = new AbortController();
    const timeout = setTimeout(
      () => deadline.abort(new DOMException("delivery barrier timed out", "TimeoutError")),
      DELIVERY_BARRIER_TIMEOUT_MS,
    );
    let result: DeliveryBarrierResult;
    try {
      const identity: BarrierIdentity = {
        mode: "snapshot",
        connectionId: request.connectionId,
        snapshotId: checkpoint.published.metadata.snapshotId,
        streamId: request.streamId,
        deliveryGeneration: request.deliveryGeneration,
        commitEventSeq: commit.lastEventSeq.toString(),
        commitPtyOffset: commit.nextPtyOffset.toString(),
      };
      result = await this.#barriers.wait(
        identity,
        AbortSignal.any([active.controller.signal, deadline.signal, this.#connectionAbort.signal]),
        () => {
          active.phase = "marker-uncertain";
        },
      );
    } finally {
      clearTimeout(timeout);
    }
    if (result.status !== "ready") {
      active.phase = "pre-marker";
      this.#resumeCanonical();
      return result;
    }
    active.phase = "pinned";
    await this.#sendDirectedReplay(request, base, commit, replay.frames, active);
    this.#sendReplayCommit(request, commit);
    active.phase = "committed";
    this.#resumeCanonical();
    return result;
  }

  async #sendDirectedReplay(
    request: AttachRequest,
    base: ReplayCursor,
    commit: ReplayCursor,
    replay: Uint8Array[],
    active: ActiveRecovery,
  ): Promise<void> {
    let cursor = base;
    let bytesSinceYield = 0;
    let framesSinceYield = 0;
    for (const encoded of replay) {
      active.controller.signal.throwIfAborted();
      const directed = rewriteDelivery(
        encoded,
        BigInt(request.deliveryGeneration),
        request.streamId,
      );
      this.#sendData(directed);
      cursor = cursorAfter(decodeDataFrame(encoded));
      bytesSinceYield += directed.byteLength;
      framesSinceYield += 1;
      if (bytesSinceYield >= PUMP_YIELD_BYTES || framesSinceYield >= PUMP_YIELD_FRAMES) {
        bytesSinceYield = 0;
        framesSinceYield = 0;
        await this.#yieldIo();
      }
    }
    if (!sameCursor(cursor, commit)) {
      throw new Error("directed replay did not reach its pinned commit");
    }
  }

  async #flushCanonicalThrough(commit: ReplayCursor): Promise<void> {
    await this.#canonicalPublisher.flushThrough(commit);
  }

  #sendReplayCommit(request: AttachRequest, commit: ReplayCursor): void {
    this.#sendData(
      encodeDataFrame({
        kind: DataFrameKind.ReplayCommit,
        flags: DataFrameFlag.None,
        sessionEpoch: commit.sessionEpoch,
        deliveryGeneration: BigInt(request.deliveryGeneration),
        eventSeq: commit.lastEventSeq,
        ptyOffset: commit.nextPtyOffset,
        streamId: request.streamId,
        payload: EMPTY_PAYLOAD,
      }),
    );
  }

  #sendReplayUnavailable(
    connectionId: string,
    reason: "journal-gap" | "engine-mismatch" | "epoch-changed",
  ): void {
    this.#sendControl({ type: "replay-unavailable", connectionId, reason });
  }

  #resumeCanonical(): void {
    this.#canonicalPublisher.resume();
    this.#telemetryBuffer?.resume();
  }

  async #pauseCanonical(): Promise<void> {
    this.#telemetryBuffer?.pause();
    try {
      await this.#canonicalPublisher.pause();
    } catch (error) {
      this.#telemetryBuffer?.resume();
      throw error;
    }
  }

  #coldCaptureReadyAt(request: AttachRequest): number {
    const requestReadyAt = this.#recoveryQueue.readyAt(request, this.#lastCanonicalAt);
    if (this.#coldCaptureNotBefore === undefined) return requestReadyAt;
    const quietDeadline = this.#coldCaptureQuietDeadline ?? this.#coldCaptureNotBefore;
    return Math.max(
      requestReadyAt,
      this.#coldCaptureNotBefore,
      Math.min(this.#lastCanonicalAt + SNAPSHOT_RECOVERY_QUIET_MS, quietDeadline),
    );
  }

  #deferColdCaptures(delayMs: number): void {
    const now = this.#monotonicNow();
    const notBefore = Math.max(this.#coldCaptureNotBefore ?? 0, now + delayMs);
    this.#coldCaptureNotBefore = notBefore;
    this.#coldCaptureQuietDeadline = Math.max(
      this.#coldCaptureQuietDeadline ?? 0,
      notBefore,
      now + SNAPSHOT_RECOVERY_MAX_QUIET_WAIT_MS,
    );
  }

  #invalidateCheckpoint(checkpoint: SnapshotCheckpoint): void {
    this.#snapshotCheckpointCache.invalidate(checkpoint);
    this.#deferColdCaptures(SNAPSHOT_RECOVERY_QUIET_MS);
  }

  #emitRange(mode: "warm" | "snapshot", measurement: ReplayRangeMeasurement): void {
    const monotonicAtMs = this.#monotonicNow();
    this.#enqueueTelemetry(
      measurement.status === "exact"
        ? {
            schemaVersion: 1,
            monotonicAtMs,
            name: "host.journal.range",
            mode,
            status: "exact",
            deliveryCreditBytes: measurement.deliveryCreditBytes,
            encodedBytes: measurement.encodedBytes,
            frames: measurement.frames,
            oldestMutationAgeMs: measurement.oldestMutationAgeMs,
          }
        : {
            schemaVersion: 1,
            monotonicAtMs,
            name: "host.journal.range",
            mode,
            status: "gap",
            reason: measurement.reason,
          },
    );
  }

  #emitPublishTotal(
    source: "fresh" | "pending",
    startedAt: number,
    outcome: "ready" | "retry" | "unavailable" | "timeout" | "cancelled" | "failed",
    snapshotBytes?: number,
  ): void {
    const finishedAt = this.#monotonicNow();
    this.#enqueueTelemetry(
      source === "fresh"
        ? {
            schemaVersion: 1,
            monotonicAtMs: finishedAt,
            name: "host.snapshot.publish-total",
            source,
            outcome,
            totalDurationMs: elapsedMs(startedAt, finishedAt),
            snapshotBytes: snapshotBytes!,
          }
        : {
            schemaVersion: 1,
            monotonicAtMs: finishedAt,
            name: "host.snapshot.publish-total",
            source,
            outcome,
            totalDurationMs: elapsedMs(startedAt, finishedAt),
          },
    );
  }

  #enqueueTelemetry(event: TerminalTelemetryEvent): void {
    try {
      this.#telemetry?.(event);
    } catch {
      // The bounded diagnostics queue is observational and must not affect recovery.
    }
  }

  #publishOutcome(
    error: unknown,
    deadline: AbortController,
    preparation: ColdPreparation,
  ): "retry" | "unavailable" | "timeout" | "cancelled" | "failed" {
    if (deadline.signal.aborted) return "timeout";
    if (preparation.controller.signal.aborted || this.#connectionAbort.signal.aborted) {
      return "cancelled";
    }
    if (
      error instanceof RetryableSnapshotPublishError ||
      error instanceof SnapshotCleanupConfirmedError
    ) {
      return "retry";
    }
    if (error instanceof SnapshotUnavailableError) return "unavailable";
    return "failed";
  }

  #sendData(frame: Uint8Array): void {
    if (this.#failed) throw new Error("delivery scheduler is stopped");
    if (this.#bufferedAmount() > HOST_CANONICAL_QUEUE_LIMITS.maxBytes) {
      throw new Error("Host data WebSocket buffer exceeded");
    }
    this.#sendDataFrame(frame);
    if (this.#bufferedAmount() > HOST_CANONICAL_QUEUE_LIMITS.maxBytes) {
      throw new Error("Host data WebSocket buffer exceeded");
    }
  }

  #failPair(reason: string): void {
    if (this.#failed) return;
    this.dispose(reason);
    this.#closePair(reason);
  }
}

function deliveryBarrierRetryScope(
  result: DeliveryBarrierResult,
): "same-generation" | "refresh-checkpoint" | "reset-generation" | "drop-client" {
  if (result.status !== "rejected") {
    throw new Error("delivery barrier retry scope requires a rejected result");
  }
  if ("retryScope" in result) return result.retryScope;
  return result.mode === "warm" ? "same-generation" : "refresh-checkpoint";
}

function cursorAfter(frame: ReturnType<typeof decodeDataFrame>): ReplayCursor {
  return {
    sessionEpoch: frame.sessionEpoch,
    lastEventSeq: frame.eventSeq,
    nextPtyOffset:
      frame.kind === DataFrameKind.PtyOutput
        ? frame.ptyOffset + BigInt(frame.payload.byteLength)
        : frame.ptyOffset,
  };
}

function cursorFromAttach(request: Extract<AttachRequest, { hasLiveReplica: true }>): ReplayCursor {
  return {
    sessionEpoch: BigInt(request.lastSessionEpoch),
    lastEventSeq: BigInt(request.lastEventSeq),
    nextPtyOffset: BigInt(request.nextPtyOffset),
  };
}

function sameCursor(left: ReplayCursor, right: ReplayCursor): boolean {
  return (
    left.sessionEpoch === right.sessionEpoch &&
    left.lastEventSeq === right.lastEventSeq &&
    left.nextPtyOffset === right.nextPtyOffset
  );
}

function assertPublishedSnapshotMatchesCapture(
  published: PublishedSnapshot,
  snapshot: SnapshotCapture,
): void {
  if (
    published.metadata.engineId !== snapshot.engineId ||
    BigInt(published.metadata.sessionEpoch) !== snapshot.sessionEpoch ||
    BigInt(published.metadata.cutEventSeq) !== snapshot.cutEventSeq ||
    BigInt(published.metadata.nextPtyOffset) !== snapshot.nextPtyOffset
  ) {
    throw new Error("published snapshot metadata does not match its authority cut");
  }
}

function checkpointFromPublished(
  published: PublishedSnapshot,
  session: TerminalSession,
): SnapshotCheckpoint {
  if (
    published.metadata.engineId !== session.engineId ||
    BigInt(published.metadata.sessionEpoch) !== session.sessionEpoch
  ) {
    throw new Error("pending snapshot metadata does not match the terminal authority");
  }
  return {
    base: {
      sessionEpoch: BigInt(published.metadata.sessionEpoch),
      lastEventSeq: BigInt(published.metadata.cutEventSeq),
      nextPtyOffset: BigInt(published.metadata.nextPtyOffset),
    },
    engineId: published.metadata.engineId,
    published: { metadata: { ...published.metadata } },
  };
}

function raceAbort<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise((resolve, reject) => {
    const onAbort = () => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    void operation.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}

class ColdTailUnavailableError extends Error {
  constructor() {
    super("snapshot tail is outside the bounded recovery window");
    this.name = "ColdTailUnavailableError";
  }
}

class SnapshotEncodeBudgetError extends Error {
  constructor() {
    super("snapshot encoding exceeded its authority budget");
    this.name = "SnapshotEncodeBudgetError";
  }
}

function isRetryableColdRecoveryError(error: unknown): boolean {
  return (
    error instanceof ColdTailUnavailableError ||
    error instanceof SnapshotEncodeBudgetError ||
    error instanceof RetryableSnapshotPublishError ||
    error instanceof SnapshotCleanupConfirmedError ||
    error instanceof SnapshotUnavailableError ||
    (error instanceof DOMException && error.name === "TimeoutError")
  );
}

function snapshotRetryAfter(error: unknown): number {
  return error instanceof RetryableSnapshotPublishError || error instanceof SnapshotUnavailableError
    ? error.retryAfterMs
    : 0;
}
