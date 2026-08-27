export interface QueueLimits {
  globalBytes: number;
  globalCount: number;
  socketBytes: number;
  socketCount: number;
}

export type SocketQueueLimits = Pick<QueueLimits, "socketBytes" | "socketCount">;

interface QueueUsage {
  bytes: number;
  count: number;
}

interface QueueReservation<Key extends object> {
  bytes: number;
  key: Key;
}

export const RELAY_MESSAGE_QUEUE_LIMITS: QueueLimits = {
  globalBytes: 32 * 1024 * 1024,
  globalCount: 2_048,
  socketBytes: 16 * 1024 * 1024,
  socketCount: 8,
};

export const RELAY_MESSAGE_QUEUE_PROFILES = {
  browserControl: { socketBytes: 16 * 1024 * 1024, socketCount: 8 },
  hostControl: { socketBytes: 1024 * 1024, socketCount: 64 },
  hostData: { socketBytes: 16 * 1024 * 1024, socketCount: 1_024 },
} satisfies Record<string, SocketQueueLimits>;

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
