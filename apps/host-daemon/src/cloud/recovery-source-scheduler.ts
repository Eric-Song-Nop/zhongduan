import type { RecoveryHostRoutingIdentity } from "@zhongduan/protocol";

import type {
  RecoverySourceDrainResult,
  RecoverySourceManager,
  RecoverySourceOwnerToken,
} from "./recovery-source-manager";

export const HOST_RECOVERY_SOURCE_QUANTUM_BYTES = 64 * 1024;
export const HOST_RECOVERY_SOURCE_MAX_DEFICIT_BYTES = 512 * 1024;
export const HOST_RECOVERY_DATA_HIGH_WATER_BYTES = 512 * 1024;
export const HOST_RECOVERY_BACKPRESSURE_RETRY_MS = 10;

interface RecoverySourceDrain {
  drainGranted: RecoverySourceManager["drainGranted"];
}

interface RecoverySourceSchedulerOptions {
  readonly bufferedAmount: () => number;
  readonly dataHighWaterBytes: number;
  readonly manager: RecoverySourceDrain;
  readonly onFailure: (reason: string) => void;
  readonly ownerToken: RecoverySourceOwnerToken;
  readonly sendData: (encoded: Uint8Array) => void;
  readonly yieldDataTurn?: (delayMs: number) => Promise<void>;
}

interface ScheduledSource {
  deficitBytes: number;
  identity: RecoveryHostRoutingIdentity;
  needsQuantum: boolean;
  queued: boolean;
}

/**
 * Connection-local, deterministic DRR for recovery source records.
 *
 * The manager owns bytes and source truth. This scheduler only owns fair data turns. Every turn
 * crosses an asynchronous I/O yield and sends at most one record so control, input ACKs, and the
 * canonical publisher can run before another recovery record is attempted.
 */
export class RecoverySourceScheduler {
  readonly #bufferedAmount: () => number;
  readonly #dataHighWaterBytes: number;
  readonly #manager: RecoverySourceDrain;
  readonly #onFailure: (reason: string) => void;
  readonly #ownerToken: RecoverySourceOwnerToken;
  readonly #sendData: (encoded: Uint8Array) => void;
  readonly #sources = new Map<number, ScheduledSource>();
  readonly #yieldDataTurn: (delayMs: number) => Promise<void>;
  readonly #runnable: ScheduledSource[] = [];

  #disposed = false;
  #retryDelayMs = 0;
  #scheduled = false;

  constructor(options: RecoverySourceSchedulerOptions) {
    if (!Number.isSafeInteger(options.dataHighWaterBytes) || options.dataHighWaterBytes <= 0) {
      throw new RangeError("Recovery data high-water mark must be a positive safe integer");
    }
    this.#bufferedAmount = options.bufferedAmount;
    this.#dataHighWaterBytes = options.dataHighWaterBytes;
    this.#manager = options.manager;
    this.#onFailure = options.onFailure;
    this.#ownerToken = options.ownerToken;
    this.#sendData = options.sendData;
    this.#yieldDataTurn =
      options.yieldDataTurn ??
      ((delayMs) => new Promise((resolve) => setTimeout(resolve, delayMs)));
  }

  notify(identity: RecoveryHostRoutingIdentity): void {
    if (this.#disposed) return;
    const current = this.#sources.get(identity.streamId);
    if (current === undefined || !sameRouting(current.identity, identity)) {
      if (current !== undefined) current.queued = false;
      const source: ScheduledSource = {
        deficitBytes: 0,
        identity: frozenIdentity(identity),
        needsQuantum: true,
        queued: false,
      };
      this.#sources.set(identity.streamId, source);
      this.#enqueue(source);
    } else {
      this.#enqueue(current);
    }
    this.#schedule();
  }

  forget(identity: RecoveryHostRoutingIdentity): void {
    const source = this.#sources.get(identity.streamId);
    if (source === undefined || !sameRouting(source.identity, identity)) return;
    source.queued = false;
    this.#sources.delete(identity.streamId);
  }

  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.#runnable.length = 0;
    this.#sources.clear();
  }

  #enqueue(source: ScheduledSource, position: "front" | "back" = "back"): void {
    if (source.queued || this.#sources.get(source.identity.streamId) !== source) return;
    source.queued = true;
    if (position === "front") this.#runnable.unshift(source);
    else this.#runnable.push(source);
  }

  #schedule(): void {
    if (this.#disposed || this.#scheduled || this.#runnable.length === 0) return;
    this.#scheduled = true;
    const delayMs = this.#retryDelayMs;
    this.#retryDelayMs = 0;
    void this.#yieldDataTurn(delayMs).then(
      () => {
        this.#scheduled = false;
        if (this.#disposed) return;
        try {
          this.#runTurn();
        } catch (error) {
          this.dispose();
          this.#onFailure(
            error instanceof Error ? error.message : "Recovery source data send failed",
          );
          return;
        }
        this.#schedule();
      },
      (error: unknown) => {
        this.#scheduled = false;
        if (this.#disposed) return;
        this.dispose();
        this.#onFailure(
          error instanceof Error ? error.message : "Recovery source data turn failed",
        );
      },
    );
  }

  #runTurn(): void {
    let source: ScheduledSource | undefined;
    while (source === undefined && this.#runnable.length > 0) {
      const candidate = this.#runnable.shift();
      if (
        candidate === undefined ||
        !candidate.queued ||
        this.#sources.get(candidate.identity.streamId) !== candidate
      ) {
        continue;
      }
      candidate.queued = false;
      source = candidate;
    }
    if (source === undefined) return;

    if (source.needsQuantum) {
      source.deficitBytes = Math.min(
        HOST_RECOVERY_SOURCE_MAX_DEFICIT_BYTES,
        source.deficitBytes + HOST_RECOVERY_SOURCE_QUANTUM_BYTES,
      );
      source.needsQuantum = false;
    }
    const buffered = nonNegativeBufferedAmount(this.#bufferedAmount());
    const availableBytes = Math.max(0, this.#dataHighWaterBytes - buffered);
    if (availableBytes === 0) {
      this.#retryDelayMs = HOST_RECOVERY_BACKPRESSURE_RETRY_MS;
      this.#enqueue(source);
      return;
    }

    const maxWireBytes = Math.min(source.deficitBytes, availableBytes);
    const result = this.#manager.drainGranted(
      this.#ownerToken,
      source.identity,
      {
        maxRecords: 1,
        maxWireBytes,
      },
      this.#sendData,
    );
    this.#handleResult(source, result, availableBytes < source.deficitBytes);
  }

  #handleResult(
    source: ScheduledSource,
    result: RecoverySourceDrainResult,
    socketLimited: boolean,
  ): void {
    if (result.records > 1 || result.wireBytes > source.deficitBytes) {
      throw new Error("Recovery source manager exceeded its scheduler budget");
    }
    source.deficitBytes -= result.wireBytes;
    if (result.status === "runnable") {
      if (result.records === 0) {
        if (socketLimited) {
          this.#retryDelayMs = HOST_RECOVERY_BACKPRESSURE_RETRY_MS;
          this.#enqueue(source);
        } else {
          source.needsQuantum = true;
          this.#enqueue(source);
        }
        return;
      }
      if (source.deficitBytes === 0) {
        source.needsQuantum = true;
        this.#enqueue(source);
      } else {
        this.#enqueue(source, "front");
      }
      return;
    }
    if (result.status === "credit-blocked") {
      source.deficitBytes = 0;
      source.needsQuantum = true;
      return;
    }
    this.forget(source.identity);
  }
}

function frozenIdentity(identity: RecoveryHostRoutingIdentity): RecoveryHostRoutingIdentity {
  return Object.freeze({
    recoveryId: identity.recoveryId,
    connectionId: identity.connectionId,
    streamId: identity.streamId,
    deliveryGeneration: identity.deliveryGeneration,
  });
}

function sameRouting(
  left: RecoveryHostRoutingIdentity,
  right: RecoveryHostRoutingIdentity,
): boolean {
  return (
    left.recoveryId === right.recoveryId &&
    left.connectionId === right.connectionId &&
    left.streamId === right.streamId &&
    left.deliveryGeneration === right.deliveryGeneration
  );
}

function nonNegativeBufferedAmount(value: number): number {
  if (!Number.isFinite(value) || value < 0) {
    throw new RangeError("Host data WebSocket bufferedAmount must be finite and non-negative");
  }
  return value;
}
