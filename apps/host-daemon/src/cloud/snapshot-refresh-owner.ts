import type { TerminalSession } from "../session";
import type { SnapshotCheckpointManager } from "./snapshot-checkpoint-manager";

export const HOST_SNAPSHOT_REFRESH_INTERVAL_MS = 30_000;
export const HOST_SNAPSHOT_RETRY_INTERVAL_MS = 30_000;

const ADMIT_SNAPSHOT = () => true;

export interface SnapshotRefreshOwnerOptions {
  checkpointManager: SnapshotCheckpointManager;
  refreshIntervalMs?: number;
  retryIntervalMs?: number;
  session: Pick<TerminalSession, "cursor">;
}

/**
 * Session-scoped owner for snapshot priming and refresh.
 *
 * Snapshot production is independent from relay connections and recovery requests. A refresh
 * samples the current authority cursor, then asks the checkpoint manager for a snapshot at or
 * beyond that cut. An unchanged authority reuses the installed checkpoint; an advanced authority
 * publishes a new immutable snapshot.
 */
export class SnapshotRefreshOwner {
  readonly #checkpointManager: SnapshotCheckpointManager;
  readonly #controller = new AbortController();
  readonly #refreshIntervalMs: number;
  readonly #retryIntervalMs: number;
  readonly #session: Pick<TerminalSession, "cursor">;

  #disposed = false;
  #refreshing = false;
  #started = false;
  #timer: ReturnType<typeof setTimeout> | undefined;

  constructor(options: SnapshotRefreshOwnerOptions) {
    this.#checkpointManager = options.checkpointManager;
    this.#refreshIntervalMs = positiveInteger(
      options.refreshIntervalMs ?? HOST_SNAPSHOT_REFRESH_INTERVAL_MS,
      "refreshIntervalMs",
    );
    this.#retryIntervalMs = positiveInteger(
      options.retryIntervalMs ?? HOST_SNAPSHOT_RETRY_INTERVAL_MS,
      "retryIntervalMs",
    );
    this.#session = options.session;
  }

  start(): void {
    if (this.#disposed) throw new Error("snapshot refresh owner is disposed");
    if (this.#started) return;
    this.#started = true;
    this.#refresh();
  }

  dispose(reason: unknown = new Error("snapshot refresh owner disposed")): void {
    if (this.#disposed) return;
    this.#disposed = true;
    if (this.#timer !== undefined) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#controller.abort(reason);
    this.#checkpointManager.dispose(reason);
  }

  #refresh(): void {
    if (this.#disposed || this.#refreshing) return;
    this.#refreshing = true;
    const minimumCut = this.#session.cursor;
    void this.#checkpointManager
      .refresh({
        admitPending: ADMIT_SNAPSHOT,
        minimumCut,
        signal: this.#controller.signal,
      })
      .then(
        () => this.#finishRefresh(this.#refreshIntervalMs),
        () => this.#finishRefresh(this.#retryIntervalMs),
      );
  }

  #finishRefresh(delayMs: number): void {
    this.#refreshing = false;
    if (this.#disposed) return;
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      this.#refresh();
    }, delayMs);
  }
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive integer`);
  }
  return value;
}
