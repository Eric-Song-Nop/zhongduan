import { SnapshotMetadataSchema, type SnapshotMetadata } from "@zhongduan/protocol";

import type { ReplayCursor, SnapshotCapture, TerminalSession } from "../session";
import {
  SnapshotPendingSupersededError,
  type PublishedSnapshot,
  type SnapshotPendingAdmission as PublisherPendingAdmission,
} from "./snapshot-publisher";

export const SNAPSHOT_CHECKPOINT_FRESHNESS_MS = 30_000;
export const SNAPSHOT_ENCODE_BUDGET_MS = 5_000;
export const SNAPSHOT_PUBLISH_TIMEOUT_MS = 120_000;

export class SnapshotEncodeBudgetError extends Error {
  constructor() {
    super("snapshot encoding exceeded its authority budget");
    this.name = "SnapshotEncodeBudgetError";
  }
}

export interface SnapshotCheckpoint {
  readonly base: ReplayCursor;
  readonly engineId: string;
  readonly published: PublishedSnapshot;
}

export interface SnapshotCheckpointState {
  readonly ageFresh: boolean;
  readonly checkpoint: SnapshotCheckpoint;
}

export type SnapshotCheckpointAdmission = (base: Readonly<ReplayCursor>) => boolean;

export interface SnapshotRefreshRequest {
  readonly admitPending: SnapshotCheckpointAdmission;
  readonly minimumCut?: Readonly<ReplayCursor>;
  readonly signal?: AbortSignal;
}

export interface SnapshotPublisherLike {
  dispose?(reason?: unknown): void;
  publish(
    snapshot: SnapshotCapture,
    signal?: AbortSignal,
    admit?: PublisherPendingAdmission,
  ): Promise<PublishedSnapshot>;
  resumePending?(
    signal?: AbortSignal,
    admit?: PublisherPendingAdmission,
  ): Promise<PublishedSnapshot> | undefined;
}

type SnapshotManagerSession = Pick<
  TerminalSession,
  "captureSnapshot" | "cursor" | "engineId" | "sessionEpoch"
>;

export interface SnapshotCheckpointManagerOptions {
  freshnessMs?: number;
  monotonicNow?: () => number;
  publisher: SnapshotPublisherLike;
  session: SnapshotManagerSession;
  sessionId: string;
}

interface InstalledCheckpoint {
  readonly checkpoint: SnapshotCheckpoint;
  readonly installedAt: number;
}

interface RefreshWaiter {
  readonly admitPending: SnapshotCheckpointAdmission;
  readonly minimumCut: Readonly<ReplayCursor> | undefined;
  readonly onAbort: (() => void) | undefined;
  readonly reject: (reason: unknown) => void;
  readonly resolve: (checkpoint: SnapshotCheckpoint) => void;
  readonly signal: AbortSignal | undefined;
}

interface RefreshFlight {
  readonly admitPending: SnapshotCheckpointAdmission;
  readonly controller: AbortController;
}

export class SnapshotCheckpointManager {
  readonly #engineId: string;
  readonly #freshnessMs: number;
  readonly #monotonicNow: () => number;
  readonly #publisher: SnapshotPublisherLike;
  readonly #session: SnapshotManagerSession;
  readonly #sessionEpoch: bigint;
  readonly #sessionId: string;
  readonly #waiters = new Set<RefreshWaiter>();

  #disposedReason: Error | undefined;
  #highWater: SnapshotCheckpoint | undefined;
  #latestValid: InstalledCheckpoint | undefined;
  #needsNewerCut: Readonly<ReplayCursor> | undefined;
  #refreshInFlight: RefreshFlight | undefined;
  #replacementMinimumCut: Readonly<ReplayCursor> | undefined;

  constructor(options: SnapshotCheckpointManagerOptions) {
    this.#engineId = options.session.engineId;
    this.#freshnessMs = options.freshnessMs ?? SNAPSHOT_CHECKPOINT_FRESHNESS_MS;
    if (!Number.isInteger(this.#freshnessMs) || this.#freshnessMs <= 0) {
      throw new RangeError("snapshot checkpoint freshness must be a positive integer");
    }
    this.#monotonicNow = options.monotonicNow ?? performance.now.bind(performance);
    this.#publisher = options.publisher;
    this.#session = options.session;
    this.#sessionEpoch = options.session.sessionEpoch;
    this.#sessionId = options.sessionId;
  }

  latestValid(): SnapshotCheckpointState | undefined {
    if (this.#disposedReason !== undefined) return undefined;
    const latest = this.#latestValid;
    if (latest === undefined) return undefined;
    return Object.freeze({
      ageFresh: this.#monotonicNow() - latest.installedAt < this.#freshnessMs,
      checkpoint: latest.checkpoint,
    });
  }

  refresh(request: SnapshotRefreshRequest): Promise<SnapshotCheckpoint> {
    this.#throwIfDisposed();
    request.signal?.throwIfAborted();
    const minimumCut =
      request.minimumCut === undefined
        ? undefined
        : this.#validatedRegisteredMinimum(request.minimumCut);
    const latest = this.#latestValid?.checkpoint;
    if (
      latest !== undefined &&
      this.#cursorSatisfies(latest.base, minimumCut) &&
      this.#cursorSatisfies(latest.base, this.#replacementMinimumCut)
    ) {
      return Promise.resolve(latest);
    }

    return new Promise((resolve, reject) => {
      let waiter!: RefreshWaiter;
      const onAbort =
        request.signal === undefined
          ? undefined
          : () => this.#rejectWaiter(waiter, request.signal!.reason);
      waiter = {
        admitPending: request.admitPending,
        minimumCut,
        onAbort,
        reject,
        resolve,
        signal: request.signal,
      };
      this.#waiters.add(waiter);
      request.signal?.addEventListener("abort", onAbort!, { once: true });
      if (request.signal?.aborted === true) {
        onAbort!();
        return;
      }
      this.#recomputeNeedsNewerCut();
      this.#startRefreshIfNeeded();
    });
  }

  invalidate(checkpoint: SnapshotCheckpoint, replacementMinimumCut?: Readonly<ReplayCursor>): void {
    if (this.#latestValid?.checkpoint !== checkpoint) return;
    const validatedMinimum =
      replacementMinimumCut === undefined
        ? undefined
        : this.#validatedRegisteredMinimum(replacementMinimumCut);
    this.#latestValid = undefined;
    if (validatedMinimum !== undefined) this.#mergeReplacementMinimum(validatedMinimum);
    this.#recomputeNeedsNewerCut();
  }

  dispose(reason: unknown = new Error("snapshot checkpoint manager disposed")): void {
    if (this.#disposedReason !== undefined) return;
    this.#disposedReason =
      reason instanceof Error
        ? reason
        : new Error("snapshot checkpoint manager disposed", { cause: reason });
    this.#latestValid = undefined;
    this.#highWater = undefined;
    this.#needsNewerCut = undefined;
    this.#replacementMinimumCut = undefined;
    const flight = this.#refreshInFlight;
    this.#refreshInFlight = undefined;
    flight?.controller.abort(this.#disposedReason);
    this.#rejectAllWaiters(this.#disposedReason);
    this.#publisher.dispose?.(this.#disposedReason);
  }

  #startRefreshIfNeeded(): void {
    if (
      this.#disposedReason !== undefined ||
      this.#refreshInFlight !== undefined ||
      this.#waiters.size === 0
    ) {
      return;
    }
    const firstWaiter = this.#waiters.values().next().value;
    if (firstWaiter === undefined) return;
    const flight: RefreshFlight = {
      admitPending: firstWaiter.admitPending,
      controller: new AbortController(),
    };
    this.#refreshInFlight = flight;
    void this.#performRefresh(flight).then(
      (checkpoint) => this.#completeRefresh(flight, checkpoint),
      (error: unknown) => this.#failRefresh(flight, error),
    );
  }

  async #performRefresh(flight: RefreshFlight): Promise<SnapshotCheckpoint | undefined> {
    try {
      const resumed = await this.#resumePending(flight);
      if (resumed !== undefined) {
        flight.controller.signal.throwIfAborted();
        this.#throwIfDisposed();
        return this.#install(resumed);
      }

      flight.controller.signal.throwIfAborted();
      this.#throwIfDisposed();
      const snapshot = await this.#captureSnapshot(flight.controller.signal);
      flight.controller.signal.throwIfAborted();
      this.#throwIfDisposed();
      if (snapshot.encodeMs > SNAPSHOT_ENCODE_BUDGET_MS) {
        throw new SnapshotEncodeBudgetError();
      }
      const published = await this.#publishSnapshot(snapshot, flight);
      flight.controller.signal.throwIfAborted();
      this.#throwIfDisposed();
      return this.#install(published, snapshot);
    } catch (error) {
      if (error instanceof SnapshotPendingSupersededError) {
        this.#mergeReplacementMinimum(this.#validatedRegisteredMinimum(this.#session.cursor));
        this.#recomputeNeedsNewerCut();
      }
      throw error;
    }
  }

  async #captureSnapshot(signal: AbortSignal): Promise<SnapshotCapture> {
    signal.throwIfAborted();
    const deadline = new AbortController();
    const timeout = setTimeout(
      () => deadline.abort(new SnapshotEncodeBudgetError()),
      SNAPSHOT_ENCODE_BUDGET_MS,
    );
    const operationSignal = AbortSignal.any([signal, deadline.signal]);
    try {
      operationSignal.throwIfAborted();
      return await raceAbort(this.#session.captureSnapshot(), operationSignal);
    } finally {
      clearTimeout(timeout);
    }
  }

  async #resumePending(flight: RefreshFlight): Promise<PublishedSnapshot | undefined> {
    if (this.#publisher.resumePending === undefined) return undefined;
    const deadline = new AbortController();
    const timeout = setTimeout(
      () => deadline.abort(new DOMException("snapshot publish timed out", "TimeoutError")),
      SNAPSHOT_PUBLISH_TIMEOUT_MS,
    );
    const signal = AbortSignal.any([flight.controller.signal, deadline.signal]);
    try {
      const operation = this.#publisher.resumePending(signal, (metadata) =>
        this.#admitPending(flight, metadata),
      );
      return operation === undefined ? undefined : await raceAbort(operation, signal);
    } finally {
      clearTimeout(timeout);
    }
  }

  async #publishSnapshot(
    snapshot: SnapshotCapture,
    flight: RefreshFlight,
  ): Promise<PublishedSnapshot> {
    const deadline = new AbortController();
    const timeout = setTimeout(
      () => deadline.abort(new DOMException("snapshot publish timed out", "TimeoutError")),
      SNAPSHOT_PUBLISH_TIMEOUT_MS,
    );
    const signal = AbortSignal.any([flight.controller.signal, deadline.signal]);
    try {
      return await raceAbort(
        this.#publisher.publish(snapshot, signal, (metadata) =>
          this.#admitPending(flight, metadata),
        ),
        signal,
      );
    } finally {
      clearTimeout(timeout);
    }
  }

  #admitPending(flight: RefreshFlight, metadata: Readonly<SnapshotMetadata>): boolean {
    this.#throwIfDisposed();
    const base = this.#validatedBase(SnapshotMetadataSchema.parse(metadata));
    if (!this.#cursorSatisfies(base, this.#replacementMinimumCut)) return false;
    if (this.#waiters.size === 0) return flight.admitPending(base);
    for (const waiter of this.#waiters) {
      if (waiter.admitPending(base)) return true;
    }
    return false;
  }

  #completeRefresh(flight: RefreshFlight, checkpoint: SnapshotCheckpoint | undefined): void {
    if (this.#refreshInFlight !== flight) return;
    this.#refreshInFlight = undefined;
    if (this.#disposedReason !== undefined) return;
    if (
      checkpoint !== undefined &&
      this.#cursorSatisfies(checkpoint.base, this.#replacementMinimumCut)
    ) {
      this.#replacementMinimumCut = undefined;
    }
    this.#recomputeNeedsNewerCut();
    const needsFollowUp =
      checkpoint === undefined || !this.#cursorSatisfies(checkpoint.base, this.#needsNewerCut);
    if (checkpoint !== undefined) {
      for (const waiter of this.#waiters) {
        if (this.#cursorSatisfies(checkpoint.base, waiter.minimumCut)) {
          this.#resolveWaiter(waiter, checkpoint);
        }
      }
    }
    this.#recomputeNeedsNewerCut();
    if (needsFollowUp) this.#startRefreshIfNeeded();
  }

  #failRefresh(flight: RefreshFlight, error: unknown): void {
    if (this.#refreshInFlight !== flight) return;
    this.#refreshInFlight = undefined;
    if (this.#disposedReason !== undefined) return;
    this.#rejectAllWaiters(error);
    this.#recomputeNeedsNewerCut();
  }

  #resolveWaiter(waiter: RefreshWaiter, checkpoint: SnapshotCheckpoint): void {
    if (!this.#waiters.delete(waiter)) return;
    if (waiter.onAbort !== undefined) {
      waiter.signal?.removeEventListener("abort", waiter.onAbort);
    }
    waiter.resolve(checkpoint);
  }

  #rejectWaiter(waiter: RefreshWaiter, error: unknown): void {
    if (!this.#waiters.delete(waiter)) return;
    if (waiter.onAbort !== undefined) {
      waiter.signal?.removeEventListener("abort", waiter.onAbort);
    }
    waiter.reject(error);
    this.#recomputeNeedsNewerCut();
  }

  #rejectAllWaiters(error: unknown): void {
    for (const waiter of this.#waiters) this.#rejectWaiter(waiter, error);
  }

  #recomputeNeedsNewerCut(): void {
    let maximum = this.#replacementMinimumCut;
    for (const waiter of this.#waiters) {
      if (waiter.minimumCut !== undefined) maximum = maxCursor(maximum, waiter.minimumCut);
    }
    this.#needsNewerCut = maximum;
  }

  #mergeReplacementMinimum(minimumCut: Readonly<ReplayCursor>): void {
    this.#replacementMinimumCut = maxCursor(this.#replacementMinimumCut, minimumCut);
  }

  #install(
    published: PublishedSnapshot,
    capture?: SnapshotCapture,
  ): SnapshotCheckpoint | undefined {
    const metadata = SnapshotMetadataSchema.parse(published.metadata);
    const base = this.#validatedBase(metadata, capture);
    if (!this.#cursorSatisfies(base, this.#replacementMinimumCut)) return undefined;
    const latest = this.#latestValid?.checkpoint;
    const highWater = this.#highWater;
    if (highWater !== undefined) {
      if (metadata.snapshotId === highWater.published.metadata.snapshotId) {
        if (!sameSnapshotMetadata(metadata, highWater.published.metadata)) {
          throw new Error("snapshot checkpoint immutable identity changed");
        }
        if (latest === highWater) return latest;
        throw new Error("invalidated snapshot checkpoint identity cannot be reinstalled");
      }
      const comparison = compareCursor(base, highWater.base);
      if (comparison < 0) throw new Error("snapshot checkpoint cut regressed");
      if (comparison === 0 && latest !== undefined) {
        throw new Error("snapshot checkpoint identity changed without advancing its cut");
      }
    }
    const ownedMetadata = Object.freeze({ ...metadata });
    const checkpoint = Object.freeze({
      base,
      engineId: metadata.engineId,
      published: Object.freeze({ metadata: ownedMetadata }),
    });
    const installedAt = this.#monotonicNow();
    this.#highWater = checkpoint;
    this.#latestValid = Object.freeze({ checkpoint, installedAt });
    return checkpoint;
  }

  #assertAuthorityLineage(metadata: SnapshotMetadata, capture?: SnapshotCapture): void {
    if (
      metadata.sessionId !== this.#sessionId ||
      metadata.engineId !== this.#engineId ||
      BigInt(metadata.sessionEpoch) !== this.#sessionEpoch
    ) {
      throw new Error("published snapshot metadata does not match its terminal session");
    }
    if (
      capture !== undefined &&
      (metadata.engineId !== capture.engineId ||
        BigInt(metadata.sessionEpoch) !== capture.sessionEpoch ||
        BigInt(metadata.cutEventSeq) !== capture.cutEventSeq ||
        BigInt(metadata.nextPtyOffset) !== capture.nextPtyOffset)
    ) {
      throw new Error("published snapshot metadata does not match its authority cut");
    }
  }

  #validatedBase(metadata: SnapshotMetadata, capture?: SnapshotCapture): Readonly<ReplayCursor> {
    this.#assertAuthorityLineage(metadata, capture);
    return Object.freeze({
      sessionEpoch: BigInt(metadata.sessionEpoch),
      lastEventSeq: BigInt(metadata.cutEventSeq),
      nextPtyOffset: BigInt(metadata.nextPtyOffset),
    });
  }

  #validatedRegisteredMinimum(minimumCut: Readonly<ReplayCursor>): Readonly<ReplayCursor> {
    const owned = Object.freeze({ ...minimumCut });
    if (owned.sessionEpoch !== this.#sessionEpoch) {
      throw new Error("snapshot refresh minimum cut does not match its terminal session");
    }
    if (compareCursor(this.#session.cursor, owned) < 0) {
      throw new Error("snapshot refresh minimum cut is ahead of its terminal session");
    }
    return owned;
  }

  #cursorSatisfies(
    cursor: Readonly<ReplayCursor>,
    minimum: Readonly<ReplayCursor> | undefined,
  ): boolean {
    return minimum === undefined || compareCursor(cursor, minimum) >= 0;
  }

  #throwIfDisposed(): void {
    if (this.#disposedReason !== undefined) throw this.#disposedReason;
  }
}

function compareCursor(left: Readonly<ReplayCursor>, right: Readonly<ReplayCursor>): number {
  if (left.sessionEpoch !== right.sessionEpoch) {
    throw new Error("snapshot checkpoint epochs cannot be compared");
  }
  if (left.lastEventSeq < right.lastEventSeq) return -1;
  if (left.lastEventSeq > right.lastEventSeq) {
    if (left.nextPtyOffset < right.nextPtyOffset) {
      throw new Error("snapshot checkpoint PTY offset regressed");
    }
    return 1;
  }
  if (left.nextPtyOffset !== right.nextPtyOffset) {
    throw new Error("snapshot checkpoint cursor is inconsistent at the same event sequence");
  }
  return 0;
}

function maxCursor(
  left: Readonly<ReplayCursor> | undefined,
  right: Readonly<ReplayCursor>,
): Readonly<ReplayCursor> {
  if (left === undefined || compareCursor(right, left) > 0) return right;
  return left;
}

function sameSnapshotMetadata(left: SnapshotMetadata, right: SnapshotMetadata): boolean {
  return (
    left.sessionId === right.sessionId &&
    left.snapshotId === right.snapshotId &&
    left.engineId === right.engineId &&
    left.sessionEpoch === right.sessionEpoch &&
    left.cutEventSeq === right.cutEventSeq &&
    left.nextPtyOffset === right.nextPtyOffset &&
    left.compression === right.compression &&
    left.compressedLength === right.compressedLength &&
    left.uncompressedLength === right.uncompressedLength &&
    left.sha256 === right.sha256
  );
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
