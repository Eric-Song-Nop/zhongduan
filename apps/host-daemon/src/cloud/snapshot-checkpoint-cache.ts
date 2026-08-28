import type { ReplayCursor } from "../session";
import type { PublishedSnapshot } from "./snapshot-publisher";

export const SNAPSHOT_CHECKPOINT_TTL_MS = 30_000;

export interface SnapshotCheckpoint {
  readonly base: ReplayCursor;
  readonly engineId: string;
  readonly published: PublishedSnapshot;
}

export interface SnapshotCheckpointCacheOptions {
  monotonicNow?: () => number;
  ttlMs?: number;
}

interface CachedCheckpoint {
  checkpoint: SnapshotCheckpoint;
  expiresAt: number;
}

export class SnapshotCheckpointCache {
  readonly #monotonicNow: () => number;
  readonly #ttlMs: number;
  #cached: CachedCheckpoint | undefined;

  constructor(options: SnapshotCheckpointCacheOptions = {}) {
    this.#monotonicNow = options.monotonicNow ?? performance.now.bind(performance);
    this.#ttlMs = options.ttlMs ?? SNAPSHOT_CHECKPOINT_TTL_MS;
    if (!Number.isInteger(this.#ttlMs) || this.#ttlMs <= 0) {
      throw new RangeError("snapshot checkpoint TTL must be a positive integer");
    }
  }

  current(): SnapshotCheckpoint | undefined {
    const cached = this.#cached;
    if (cached === undefined) return undefined;
    if (this.#monotonicNow() >= cached.expiresAt) {
      this.#cached = undefined;
      return undefined;
    }
    return cached.checkpoint;
  }

  install(checkpoint: SnapshotCheckpoint): SnapshotCheckpoint {
    const stored: SnapshotCheckpoint = {
      base: { ...checkpoint.base },
      engineId: checkpoint.engineId,
      published: { metadata: { ...checkpoint.published.metadata } },
    };
    this.#cached = {
      checkpoint: stored,
      expiresAt: this.#monotonicNow() + this.#ttlMs,
    };
    return stored;
  }

  invalidate(checkpoint: SnapshotCheckpoint): void {
    if (this.#cached?.checkpoint === checkpoint) this.#cached = undefined;
  }
}
