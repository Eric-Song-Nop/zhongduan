import { SnapshotMetadataSchema, type SnapshotMetadata } from "@zhongduan/protocol";

import type { ReplayCursor, SnapshotCapture, TerminalSession } from "../session";
import type {
  PublishedSnapshot,
  SnapshotPendingAdmission as PublisherPendingAdmission,
} from "./snapshot-publisher";

export const SNAPSHOT_CHECKPOINT_FRESHNESS_MS = 30_000;

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

export interface SnapshotCheckpointManagerOptions {
  freshnessMs?: number;
  monotonicNow?: () => number;
  publisher: SnapshotPublisherLike;
  session: Pick<TerminalSession, "engineId" | "sessionEpoch">;
  sessionId: string;
}

interface InstalledCheckpoint {
  readonly checkpoint: SnapshotCheckpoint;
  readonly installedAt: number;
}

export class SnapshotCheckpointManager {
  readonly #engineId: string;
  readonly #freshnessMs: number;
  readonly #monotonicNow: () => number;
  readonly #publisher: SnapshotPublisherLike;
  readonly #sessionEpoch: bigint;
  readonly #sessionId: string;

  #disposedReason: Error | undefined;
  #highWater: SnapshotCheckpoint | undefined;
  #latestValid: InstalledCheckpoint | undefined;

  constructor(options: SnapshotCheckpointManagerOptions) {
    this.#engineId = options.session.engineId;
    this.#freshnessMs = options.freshnessMs ?? SNAPSHOT_CHECKPOINT_FRESHNESS_MS;
    if (!Number.isInteger(this.#freshnessMs) || this.#freshnessMs <= 0) {
      throw new RangeError("snapshot checkpoint freshness must be a positive integer");
    }
    this.#monotonicNow = options.monotonicNow ?? performance.now.bind(performance);
    this.#publisher = options.publisher;
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

  publish(
    snapshot: SnapshotCapture,
    signal?: AbortSignal,
    admit?: SnapshotCheckpointAdmission,
  ): Promise<PublishedSnapshot> {
    this.#throwIfDisposed();
    return this.#publisher.publish(snapshot, signal, this.#adaptAdmission(admit));
  }

  resumePending(
    signal?: AbortSignal,
    admit?: SnapshotCheckpointAdmission,
  ): Promise<PublishedSnapshot> | undefined {
    this.#throwIfDisposed();
    return this.#publisher.resumePending?.(signal, this.#adaptAdmission(admit));
  }

  dispose(reason: unknown = new Error("snapshot checkpoint manager disposed")): void {
    if (this.#disposedReason !== undefined) return;
    this.#disposedReason =
      reason instanceof Error
        ? reason
        : new Error("snapshot checkpoint manager disposed", { cause: reason });
    this.#latestValid = undefined;
    this.#highWater = undefined;
    this.#publisher.dispose?.(this.#disposedReason);
  }

  install(published: PublishedSnapshot, capture?: SnapshotCapture): SnapshotCheckpoint {
    this.#throwIfDisposed();
    return this.#install(published, capture);
  }

  invalidate(checkpoint: SnapshotCheckpoint): void {
    if (this.#latestValid?.checkpoint === checkpoint) this.#latestValid = undefined;
  }

  #install(published: PublishedSnapshot, capture?: SnapshotCapture): SnapshotCheckpoint {
    const metadata = SnapshotMetadataSchema.parse(published.metadata);
    const base = this.#validatedBase(metadata, capture);
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
    this.#latestValid = Object.freeze({
      checkpoint,
      installedAt,
    });
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

  #adaptAdmission(
    admit: SnapshotCheckpointAdmission | undefined,
  ): PublisherPendingAdmission | undefined {
    if (admit === undefined) return undefined;
    return (metadata) => {
      this.#throwIfDisposed();
      const parsed = SnapshotMetadataSchema.parse(metadata);
      return admit(this.#validatedBase(parsed));
    };
  }

  #validatedBase(metadata: SnapshotMetadata, capture?: SnapshotCapture): Readonly<ReplayCursor> {
    this.#assertAuthorityLineage(metadata, capture);
    return Object.freeze({
      sessionEpoch: BigInt(metadata.sessionEpoch),
      lastEventSeq: BigInt(metadata.cutEventSeq),
      nextPtyOffset: BigInt(metadata.nextPtyOffset),
    });
  }

  #throwIfDisposed(): void {
    if (this.#disposedReason !== undefined) throw this.#disposedReason;
  }
}

function compareCursor(left: ReplayCursor, right: ReplayCursor): number {
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
