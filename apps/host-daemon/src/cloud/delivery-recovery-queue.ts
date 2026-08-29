import type { RelayToHostControlFrame } from "@zhongduan/protocol";

export type AttachRequest = Extract<RelayToHostControlFrame, { type: "attach-request" }>;

interface DeliveryRecoveryQueueOptions {
  maxQuietWaitMs: number;
  maxRetryMs: number;
  monotonicNow: () => number;
  quietMs: number;
}

export class DeliveryRecoveryQueue {
  readonly #attempts = new Map<string, number>();
  readonly #cold = new Set<string>();
  readonly #maxQuietWaitMs: number;
  readonly #maxRetryMs: number;
  readonly #monotonicNow: () => number;
  readonly #notBefore = new Map<string, number>();
  readonly #order: number[] = [];
  readonly #pending = new Map<number, AttachRequest>();
  readonly #queuedStreams = new Set<number>();
  readonly #quietDeadline = new Map<string, number>();
  readonly #quietMs: number;

  constructor(options: DeliveryRecoveryQueueOptions) {
    this.#maxQuietWaitMs = options.maxQuietWaitMs;
    this.#maxRetryMs = options.maxRetryMs;
    this.#monotonicNow = options.monotonicNow;
    this.#quietMs = options.quietMs;
  }

  get pendingSize(): number {
    return this.#pending.size;
  }

  enqueue(request: AttachRequest): string[] {
    const existing = this.#pending.get(request.streamId);
    if (
      existing !== undefined &&
      BigInt(existing.deliveryGeneration) >= BigInt(request.deliveryGeneration)
    ) {
      return [];
    }
    const superseded = this.#clearMatching(
      request.streamId,
      (generation) => generation < BigInt(request.deliveryGeneration),
    );
    if (
      existing !== undefined &&
      BigInt(existing.deliveryGeneration) < BigInt(request.deliveryGeneration)
    ) {
      superseded.add(recoveryKey(existing));
    }
    this.#pending.set(request.streamId, request);
    this.#queueStream(request.streamId);
    return [...superseded];
  }

  requeue(request: AttachRequest): void {
    const existing = this.#pending.get(request.streamId);
    if (
      existing !== undefined &&
      BigInt(existing.deliveryGeneration) >= BigInt(request.deliveryGeneration)
    ) {
      return;
    }
    this.#pending.set(request.streamId, request);
    this.#queueStream(request.streamId);
  }

  takeRunnablePass(isRunnable: (request: AttachRequest) => boolean): AttachRequest[] {
    const runnable: AttachRequest[] = [];
    const passSize = this.#order.length;
    for (let index = 0; index < passSize; index += 1) {
      const streamId = this.#order.shift();
      if (streamId === undefined) break;
      this.#queuedStreams.delete(streamId);
      const request = this.#pending.get(streamId);
      if (request === undefined) continue;
      if (!isRunnable(request)) {
        this.#queueStream(streamId);
        continue;
      }
      this.#pending.delete(streamId);
      runnable.push(request);
    }
    return runnable;
  }

  some(predicate: (request: AttachRequest) => boolean): boolean {
    for (const request of this.#pending.values()) {
      if (predicate(request)) return true;
    }
    return false;
  }

  forEach(visitor: (request: AttachRequest) => void): void {
    this.#pending.forEach(visitor);
  }

  isCold(request: AttachRequest): boolean {
    return this.#cold.has(recoveryKey(request));
  }

  markCold(request: AttachRequest): void {
    this.#cold.add(recoveryKey(request));
  }

  readyAt(request: AttachRequest, lastCanonicalAt: number): number {
    const key = recoveryKey(request);
    const notBefore = this.#notBefore.get(key);
    if (notBefore === undefined) return 0;
    const quietDeadline = this.#quietDeadline.get(key) ?? notBefore;
    return Math.max(notBefore, Math.min(lastCanonicalAt + this.#quietMs, quietDeadline));
  }

  defer(request: AttachRequest, delayMs: number): void {
    const key = recoveryKey(request);
    const now = this.#monotonicNow();
    const notBefore = Math.max(this.#notBefore.get(key) ?? 0, now + delayMs);
    this.#cold.add(key);
    this.#notBefore.set(key, notBefore);
    this.#quietDeadline.set(
      key,
      Math.max(this.#quietDeadline.get(key) ?? 0, notBefore, now + this.#maxQuietWaitMs),
    );
  }

  deferAfterFailure(request: AttachRequest, minimumDelayMs: number): number {
    const key = recoveryKey(request);
    const attempt = (this.#attempts.get(key) ?? 0) + 1;
    this.#attempts.set(key, attempt);
    const exponential = Math.min(this.#maxRetryMs, this.#quietMs * 2 ** Math.min(20, attempt - 1));
    const delayMs = Math.max(minimumDelayMs, exponential);
    this.defer(request, delayMs);
    return delayMs;
  }

  complete(request: AttachRequest): void {
    const key = recoveryKey(request);
    this.#attempts.delete(key);
    this.#cold.delete(key);
    this.#notBefore.delete(key);
    this.#quietDeadline.delete(key);
  }

  reset(streamId: number, throughGeneration: string): string[] {
    const maximum = BigInt(throughGeneration);
    const removed = this.#clearMatching(streamId, (generation) => generation <= maximum);
    const pending = this.#pending.get(streamId);
    if (pending !== undefined && BigInt(pending.deliveryGeneration) <= maximum) {
      this.#pending.delete(streamId);
      removed.add(recoveryKey(pending));
    }
    return [...removed];
  }

  clear(): void {
    this.#attempts.clear();
    this.#cold.clear();
    this.#notBefore.clear();
    this.#quietDeadline.clear();
    this.#order.length = 0;
    this.#pending.clear();
    this.#queuedStreams.clear();
  }

  #queueStream(streamId: number): void {
    if (this.#queuedStreams.has(streamId)) return;
    this.#queuedStreams.add(streamId);
    this.#order.push(streamId);
  }

  #clearMatching(
    streamId: number,
    matchesGeneration: (generation: bigint) => boolean,
  ): Set<string> {
    const removed = new Set<string>();
    const keys = new Set([
      ...this.#attempts.keys(),
      ...this.#cold,
      ...this.#notBefore.keys(),
      ...this.#quietDeadline.keys(),
    ]);
    for (const key of keys) {
      const separator = key.indexOf(":");
      if (
        Number(key.slice(0, separator)) === streamId &&
        matchesGeneration(BigInt(key.slice(separator + 1)))
      ) {
        this.#attempts.delete(key);
        this.#cold.delete(key);
        this.#notBefore.delete(key);
        this.#quietDeadline.delete(key);
        removed.add(key);
      }
    }
    return removed;
  }
}

export function recoveryKey(request: AttachRequest): string {
  return `${request.streamId}:${request.deliveryGeneration}`;
}
