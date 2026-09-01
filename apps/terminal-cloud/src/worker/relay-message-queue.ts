export interface QueueLimits {
  globalBytes: number;
  globalCount: number;
  socketBytes: number;
  socketCount: number;
}

export type SocketQueueLimits = Pick<QueueLimits, "socketBytes" | "socketCount">;

export interface KeyedQueueLimits extends QueueLimits {
  maxAgeMs: number;
  maxConcurrency: number;
}

export interface QueueTaskTiming {
  enqueuedAtMs: number;
  startedAtMs: number;
  waitMs: number;
}

export type QueueExpirationReason = "age" | "global-overload";

interface QueueUsage {
  bytes: number;
  count: number;
}

interface QueueReservation<Key extends object> {
  bytes: number;
  key: Key;
}

interface KeyedQueueEntry<Key extends object> extends QueueReservation<Key> {
  deadline: ReturnType<typeof setTimeout> | undefined;
  enqueuedAtMs: number;
  expire: (timing: QueueTaskTiming, reason: QueueExpirationReason) => Promise<void> | void;
  reject: (reason: unknown) => void;
  resolve: () => void;
  run: (timing: QueueTaskTiming) => Promise<void> | void;
  startedAtMs: number | undefined;
  state: "active" | "queued" | "settled";
}

export const RELAY_MESSAGE_QUEUE_LIMITS: QueueLimits = {
  globalBytes: 32 * 1024 * 1024,
  globalCount: 2_048,
  socketBytes: 16 * 1024 * 1024,
  socketCount: 8,
};

export const RELAY_MESSAGE_QUEUE_PROFILES = {
  browserControl: { socketBytes: 16 * 1024 * 1024, socketCount: 64 },
  hostControl: { socketBytes: 1024 * 1024, socketCount: 64 },
  hostData: { socketBytes: 16 * 1024 * 1024, socketCount: 1_024 },
} satisfies Record<string, SocketQueueLimits>;

/**
 * E2 Browser control/input lane contract. A connection preserves FIFO while independent Browser
 * connections may make progress concurrently. Reservations include active work until it settles, so
 * these limits cover both queued and executing tasks.
 */
export const BROWSER_CONTROL_LANE_LIMITS: KeyedQueueLimits = Object.freeze({
  globalBytes: 32 * 1024 * 1024,
  globalCount: 512,
  socketBytes: 16 * 1024 * 1024,
  socketCount: 64,
  maxAgeMs: 250,
  maxConcurrency: 4,
});

export class BoundedSerialQueue<Key extends object> {
  readonly #limits: QueueLimits;
  readonly #socketUsage = new Map<Key, QueueUsage>();
  #globalUsage: QueueUsage = { bytes: 0, count: 0 };
  #tail: Promise<void> = Promise.resolve();

  constructor(limits: QueueLimits = RELAY_MESSAGE_QUEUE_LIMITS) {
    this.#limits = limits;
  }

  get queuedBytes(): number {
    return this.#globalUsage.bytes;
  }

  get queuedCount(): number {
    return this.#globalUsage.count;
  }

  enqueue(
    key: Key,
    bytes: number,
    task: () => Promise<void>,
    socketLimits: SocketQueueLimits = this.#limits,
  ): Promise<void> | undefined {
    const reservation = this.#reserve(key, bytes, socketLimits);
    if (reservation === undefined) return undefined;

    const processing = this.#tail.then(task);
    const settled = processing.finally(() => this.#release(reservation));
    this.#tail = settled.catch(() => undefined);
    return settled;
  }

  #reserve(
    key: Key,
    bytes: number,
    socketLimits: SocketQueueLimits,
  ): QueueReservation<Key> | undefined {
    if (!Number.isSafeInteger(bytes) || bytes < 0) return undefined;
    const socket = this.#socketUsage.get(key) ?? { bytes: 0, count: 0 };
    if (
      this.#globalUsage.count + 1 > this.#limits.globalCount ||
      this.#globalUsage.bytes + bytes > this.#limits.globalBytes ||
      socket.count + 1 > socketLimits.socketCount ||
      socket.bytes + bytes > socketLimits.socketBytes
    ) {
      return undefined;
    }

    this.#globalUsage = {
      bytes: this.#globalUsage.bytes + bytes,
      count: this.#globalUsage.count + 1,
    };
    this.#socketUsage.set(key, { bytes: socket.bytes + bytes, count: socket.count + 1 });
    return { bytes, key };
  }

  #release(reservation: QueueReservation<Key>): void {
    const socket = this.#socketUsage.get(reservation.key);
    if (socket === undefined) throw new Error("queue reservation is missing");

    this.#globalUsage = {
      bytes: this.#globalUsage.bytes - reservation.bytes,
      count: this.#globalUsage.count - 1,
    };
    const next = { bytes: socket.bytes - reservation.bytes, count: socket.count - 1 };
    if (next.count === 0) this.#socketUsage.delete(reservation.key);
    else this.#socketUsage.set(reservation.key, next);
  }
}

/**
 * A bounded, keyed execution lane. Work for one key is strictly FIFO; ready keys rotate after each
 * task so one connection cannot monopolize the lane. Every entry has an enqueue-relative hard
 * deadline; expiry releases the entire key even if its active head never settles. Independent keys
 * can execute up to the declared concurrency without sharing the bulk relay's serial tail. At global
 * pressure, a lower-occupancy key evicts the largest actual occupant rather than inheriting its
 * overload.
 */
export class BoundedKeyedQueue<Key extends object> {
  readonly #limits: KeyedQueueLimits;
  readonly #now: () => number;
  readonly #socketUsage = new Map<Key, QueueUsage>();
  readonly #pending = new Map<Key, Array<KeyedQueueEntry<Key>>>();
  readonly #activeKeys = new Set<Key>();
  readonly #readyKeys = new Set<Key>();
  readonly #readyOrder: Key[] = [];
  #activeCount = 0;
  #globalUsage: QueueUsage = { bytes: 0, count: 0 };

  constructor(limits: KeyedQueueLimits, now: () => number = () => performance.now()) {
    if (!Number.isSafeInteger(limits.maxConcurrency) || limits.maxConcurrency < 1) {
      throw new Error("keyed queue maxConcurrency must be a positive safe integer");
    }
    if (!Number.isFinite(limits.maxAgeMs) || limits.maxAgeMs < 0) {
      throw new Error("keyed queue maxAgeMs must be finite and non-negative");
    }
    this.#limits = limits;
    this.#now = now;
  }

  get activeCount(): number {
    return this.#activeCount;
  }

  get queuedBytes(): number {
    return this.#globalUsage.bytes;
  }

  get queuedCount(): number {
    return this.#globalUsage.count;
  }

  enqueue(
    key: Key,
    bytes: number,
    run: (timing: QueueTaskTiming) => Promise<void> | void,
    expire: (timing: QueueTaskTiming, reason: QueueExpirationReason) => Promise<void> | void,
    socketLimits: SocketQueueLimits = this.#limits,
  ): Promise<void> | undefined {
    if (!this.#reserve(key, bytes, socketLimits)) return undefined;

    const enqueuedAtMs = this.#now();
    let resolve!: () => void;
    let reject!: (reason: unknown) => void;
    const settled = new Promise<void>((resolvePromise, rejectPromise) => {
      resolve = resolvePromise;
      reject = rejectPromise;
    });
    const entry: KeyedQueueEntry<Key> = {
      bytes,
      deadline: undefined,
      enqueuedAtMs,
      expire,
      key,
      reject,
      resolve,
      run,
      startedAtMs: undefined,
      state: "queued",
    };
    entry.deadline = setTimeout(() => this.#expireKey(key, "age", entry), this.#limits.maxAgeMs);
    const pending = this.#pending.get(key);
    if (pending === undefined) this.#pending.set(key, [entry]);
    else pending.push(entry);
    this.#markReady(key);
    this.#pump();
    return settled;
  }

  #reserve(key: Key, bytes: number, socketLimits: SocketQueueLimits): boolean {
    if (!Number.isSafeInteger(bytes) || bytes < 0) return false;
    let socket = this.#socketUsage.get(key) ?? { bytes: 0, count: 0 };
    if (
      socket.count + 1 > socketLimits.socketCount ||
      socket.bytes + bytes > socketLimits.socketBytes
    ) {
      return false;
    }
    if (!this.#makeGlobalRoom(key, bytes)) {
      return false;
    }
    socket = this.#socketUsage.get(key) ?? { bytes: 0, count: 0 };

    this.#globalUsage = {
      bytes: this.#globalUsage.bytes + bytes,
      count: this.#globalUsage.count + 1,
    };
    this.#socketUsage.set(key, { bytes: socket.bytes + bytes, count: socket.count + 1 });
    return true;
  }

  #makeGlobalRoom(key: Key, bytes: number): boolean {
    while (
      this.#globalUsage.count + 1 > this.#limits.globalCount ||
      this.#globalUsage.bytes + bytes > this.#limits.globalBytes
    ) {
      const current = this.#socketUsage.get(key) ?? { bytes: 0, count: 0 };
      const currentScore = this.#usageScore(current);
      let largest: Key | undefined;
      let largestScore = currentScore;
      for (const [candidate, usage] of this.#socketUsage) {
        if (candidate === key) continue;
        const score = this.#usageScore(usage);
        if (score > largestScore) {
          largest = candidate;
          largestScore = score;
        }
      }
      if (largest === undefined) return false;
      this.#expireKey(largest, "global-overload");
    }
    return true;
  }

  #usageScore(usage: QueueUsage): number {
    const byteShare =
      this.#limits.globalBytes === 0
        ? usage.bytes === 0
          ? 0
          : Number.POSITIVE_INFINITY
        : usage.bytes / this.#limits.globalBytes;
    const countShare =
      this.#limits.globalCount === 0
        ? usage.count === 0
          ? 0
          : Number.POSITIVE_INFINITY
        : usage.count / this.#limits.globalCount;
    return Math.max(byteShare, countShare);
  }

  #markReady(key: Key): void {
    if (this.#activeKeys.has(key) || this.#readyKeys.has(key)) return;
    this.#readyKeys.add(key);
    this.#readyOrder.push(key);
  }

  #pump(): void {
    while (this.#activeCount < this.#limits.maxConcurrency) {
      const key = this.#readyOrder.shift();
      if (key === undefined) return;
      this.#readyKeys.delete(key);
      const entry = this.#pending.get(key)?.[0];
      if (entry === undefined || this.#activeKeys.has(key)) continue;
      this.#activeKeys.add(key);
      this.#activeCount += 1;
      entry.state = "active";
      this.#run(entry);
    }
  }

  #run(entry: KeyedQueueEntry<Key>): void {
    const startedAtMs = this.#now();
    entry.startedAtMs = startedAtMs;
    const timing = {
      enqueuedAtMs: entry.enqueuedAtMs,
      startedAtMs,
      waitMs: Math.max(0, startedAtMs - entry.enqueuedAtMs),
    } satisfies QueueTaskTiming;
    let processing: Promise<void> | void;
    try {
      if (timing.waitMs >= this.#limits.maxAgeMs) {
        this.#expireKey(entry.key, "age", entry);
        return;
      }
      processing = entry.run(timing);
    } catch (error) {
      this.#reject(entry, error);
      return;
    }
    if (processing === undefined) {
      this.#resolve(entry);
      return;
    }
    void processing.then(
      () => this.#resolve(entry),
      (error: unknown) => this.#reject(entry, error),
    );
  }

  #resolve(entry: KeyedQueueEntry<Key>): void {
    if (entry.state === "settled") return;
    entry.resolve();
    this.#finish(entry);
  }

  #reject(entry: KeyedQueueEntry<Key>, error: unknown): void {
    if (entry.state === "settled") return;
    entry.reject(error);
    this.#finish(entry);
  }

  #finish(entry: KeyedQueueEntry<Key>): void {
    const pending = this.#pending.get(entry.key);
    if (pending?.[0] !== entry) {
      throw new Error("keyed queue head is inconsistent");
    }
    pending.shift();
    if (pending.length === 0) this.#pending.delete(entry.key);

    entry.state = "settled";
    if (entry.deadline !== undefined) clearTimeout(entry.deadline);
    entry.deadline = undefined;
    this.#releaseReservation(entry);

    this.#activeKeys.delete(entry.key);
    this.#activeCount -= 1;
    if (pending.length > 0) this.#markReady(entry.key);
    this.#pump();
  }

  #expireKey(key: Key, reason: QueueExpirationReason, trigger?: KeyedQueueEntry<Key>): void {
    const entries = this.#pending.get(key);
    if (entries === undefined || (trigger !== undefined && !entries.includes(trigger))) return;

    this.#pending.delete(key);
    this.#readyKeys.delete(key);
    for (let index = this.#readyOrder.length - 1; index >= 0; index -= 1) {
      if (this.#readyOrder[index] === key) this.#readyOrder.splice(index, 1);
    }
    if (this.#activeKeys.delete(key)) this.#activeCount -= 1;

    const expiredAtMs = this.#now();
    for (const entry of entries) {
      if (entry.state === "settled") continue;
      entry.state = "settled";
      if (entry.deadline !== undefined) clearTimeout(entry.deadline);
      entry.deadline = undefined;
      this.#releaseReservation(entry);
      const startedAtMs = entry.startedAtMs ?? expiredAtMs;
      const timing = {
        enqueuedAtMs: entry.enqueuedAtMs,
        startedAtMs,
        waitMs: Math.max(0, startedAtMs - entry.enqueuedAtMs),
      } satisfies QueueTaskTiming;
      let expiration: Promise<void> | void;
      try {
        expiration = entry.expire(timing, reason);
      } catch (error) {
        entry.reject(error);
        continue;
      }
      if (expiration === undefined) entry.resolve();
      else void expiration.then(entry.resolve, entry.reject);
    }
    this.#pump();
  }

  #releaseReservation(entry: QueueReservation<Key>): void {
    const socket = this.#socketUsage.get(entry.key);
    if (socket === undefined) throw new Error("keyed queue reservation is missing");
    this.#globalUsage = {
      bytes: this.#globalUsage.bytes - entry.bytes,
      count: this.#globalUsage.count - 1,
    };
    const next = { bytes: socket.bytes - entry.bytes, count: socket.count - 1 };
    if (next.count === 0) this.#socketUsage.delete(entry.key);
    else this.#socketUsage.set(entry.key, next);
  }
}
