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

interface QueueOutstanding {
  globalOutstandingBytes: number;
  globalOutstandingCount: number;
  socketOutstandingBytes: number;
  socketOutstandingCount: number;
}

type QueueReservationResult<Key extends object> =
  | ({
      outcome: "reserved";
      reservation: QueueReservation<Key>;
    } & QueueOutstanding)
  | ({
      outcome: "capacity";
      reason: RelayQueueCapacityReason;
    } & QueueOutstanding);

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

export type RelayQueueProfile = "browser-control" | "host-control" | "host-data";

export type RelayQueueCapacityReason =
  | "global-count"
  | "global-bytes"
  | "socket-count"
  | "socket-bytes"
  | "invalid-size";

export interface RelayQueueObservationContext {
  readonly queueProfile?: RelayQueueProfile;
}

export interface RelayQueueTaskContext extends RelayQueueObservationContext {
  readonly reservationBytes: number;
  readonly globalOutstandingBytes: number;
  readonly globalOutstandingCount: number;
  readonly socketOutstandingBytes: number;
  readonly socketOutstandingCount: number;
  readonly admissionStartedAt?: number;
  readonly admittedAt?: number;
  readonly observedAdmissionMs?: number;
  readonly startedAt?: number;
  readonly observedQueueWaitMs?: number;
}

export type RelayQueueObservation =
  | (RelayQueueObservationContext &
      QueueOutstanding & {
        readonly outcome: "capacity";
        readonly capacityReason: RelayQueueCapacityReason;
        readonly reservationBytes: number;
        readonly admissionStartedAt: number;
        readonly admittedAt: number;
      })
  | (RelayQueueObservationContext &
      QueueOutstanding & {
        readonly outcome: "completed" | "failed";
        readonly reservationBytes: number;
        readonly admissionStartedAt: number;
        readonly admittedAt: number;
        readonly startedAt: number;
        readonly finishedAt: number;
      });

export type RelayQueueClock = () => number;
export type RelayQueueObserver = (
  observation: Readonly<RelayQueueObservation>,
) => void | PromiseLike<void>;

export interface BoundedSerialQueueOptions {
  readonly clock?: RelayQueueClock;
  readonly observer?: RelayQueueObserver;
}

function defaultRelayQueueClock(): number {
  return performance.now();
}

export class BoundedSerialQueue<Key extends object> {
  readonly #limits: QueueLimits;
  readonly #clock: RelayQueueClock;
  readonly #observer: RelayQueueObserver | undefined;
  readonly #socketUsage = new Map<Key, QueueUsage>();
  #globalUsage: QueueUsage = { bytes: 0, count: 0 };
  #tail: Promise<void> = Promise.resolve();

  constructor(
    limits: QueueLimits = RELAY_MESSAGE_QUEUE_LIMITS,
    options: BoundedSerialQueueOptions = {},
  ) {
    this.#limits = limits;
    this.#clock = options.clock ?? defaultRelayQueueClock;
    this.#observer = options.observer;
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
    task: (context: Readonly<RelayQueueTaskContext>) => Promise<void>,
    socketLimits: SocketQueueLimits = this.#limits,
    observationContext: RelayQueueObservationContext = {},
  ): Promise<void> | undefined {
    const queueProfile = observationContext.queueProfile;
    const admissionStartedAt = this.#observer === undefined ? undefined : this.#readClock();
    const result = this.#reserve(key, bytes, socketLimits);
    const admittedAt = this.#observer === undefined ? undefined : this.#readClock();
    const reservationBytes = Number.isSafeInteger(bytes) && bytes >= 0 ? bytes : 0;
    if (result.outcome === "capacity") {
      if (admissionStartedAt !== undefined && admittedAt !== undefined) {
        this.#dispatchObservation(
          Object.freeze({
            outcome: "capacity",
            capacityReason: result.reason,
            ...(queueProfile === undefined ? {} : { queueProfile }),
            reservationBytes,
            admissionStartedAt,
            admittedAt,
            globalOutstandingBytes: result.globalOutstandingBytes,
            globalOutstandingCount: result.globalOutstandingCount,
            socketOutstandingBytes: result.socketOutstandingBytes,
            socketOutstandingCount: result.socketOutstandingCount,
          }),
        );
      }
      return undefined;
    }

    let taskContext: Readonly<RelayQueueTaskContext> | undefined;
    const processing = this.#tail.then(() => {
      const startedAt = this.#observer === undefined ? undefined : this.#readClock();
      const context = Object.freeze({
        ...(queueProfile === undefined ? {} : { queueProfile }),
        reservationBytes,
        globalOutstandingBytes: result.globalOutstandingBytes,
        globalOutstandingCount: result.globalOutstandingCount,
        socketOutstandingBytes: result.socketOutstandingBytes,
        socketOutstandingCount: result.socketOutstandingCount,
        ...(admissionStartedAt === undefined ? {} : { admissionStartedAt }),
        ...(admittedAt === undefined ? {} : { admittedAt }),
        ...(admissionStartedAt === undefined || admittedAt === undefined
          ? {}
          : { observedAdmissionMs: Math.max(0, admittedAt - admissionStartedAt) }),
        ...(startedAt === undefined ? {} : { startedAt }),
        ...(admittedAt === undefined || startedAt === undefined
          ? {}
          : { observedQueueWaitMs: Math.max(0, startedAt - admittedAt) }),
      });
      taskContext = context;
      return task(context);
    });
    const settled = processing.finally(() => this.#release(result.reservation));
    this.#tail = settled.catch(() => undefined);
    this.#observeSettlement(settled, () => taskContext);
    return settled;
  }

  #reserve(key: Key, bytes: number, socketLimits: SocketQueueLimits): QueueReservationResult<Key> {
    const socket = this.#socketUsage.get(key) ?? { bytes: 0, count: 0 };
    const capacity = (reason: RelayQueueCapacityReason): QueueReservationResult<Key> => ({
      outcome: "capacity",
      reason,
      globalOutstandingBytes: this.#globalUsage.bytes,
      globalOutstandingCount: this.#globalUsage.count,
      socketOutstandingBytes: socket.bytes,
      socketOutstandingCount: socket.count,
    });
    if (!Number.isSafeInteger(bytes) || bytes < 0) return capacity("invalid-size");
    if (this.#globalUsage.count + 1 > this.#limits.globalCount) {
      return capacity("global-count");
    }
    if (this.#globalUsage.bytes + bytes > this.#limits.globalBytes) {
      return capacity("global-bytes");
    }
    if (socket.count + 1 > socketLimits.socketCount) return capacity("socket-count");
    if (socket.bytes + bytes > socketLimits.socketBytes) return capacity("socket-bytes");

    this.#globalUsage = {
      bytes: this.#globalUsage.bytes + bytes,
      count: this.#globalUsage.count + 1,
    };
    const nextSocket = { bytes: socket.bytes + bytes, count: socket.count + 1 };
    this.#socketUsage.set(key, nextSocket);
    return {
      outcome: "reserved",
      reservation: { bytes, key },
      globalOutstandingBytes: this.#globalUsage.bytes,
      globalOutstandingCount: this.#globalUsage.count,
      socketOutstandingBytes: nextSocket.bytes,
      socketOutstandingCount: nextSocket.count,
    };
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

  #readClock(): number | undefined {
    try {
      const value = this.#clock();
      return Number.isFinite(value) && value >= 0 ? value : undefined;
    } catch {
      return undefined;
    }
  }

  #observeSettlement(
    settled: Promise<void>,
    getTaskContext: () => Readonly<RelayQueueTaskContext> | undefined,
  ): void {
    if (this.#observer === undefined) return;
    void settled
      .then(
        () => this.#dispatchCompletedObservation("completed", getTaskContext()),
        () => this.#dispatchCompletedObservation("failed", getTaskContext()),
      )
      .catch(() => undefined);
  }

  #dispatchCompletedObservation(
    outcome: "completed" | "failed",
    context: Readonly<RelayQueueTaskContext> | undefined,
  ): void {
    const finishedAt = this.#readClock();
    if (
      context === undefined ||
      context.admissionStartedAt === undefined ||
      context.admittedAt === undefined ||
      context.startedAt === undefined ||
      finishedAt === undefined
    ) {
      return;
    }
    this.#dispatchObservation(
      Object.freeze({
        outcome,
        ...(context.queueProfile === undefined ? {} : { queueProfile: context.queueProfile }),
        reservationBytes: context.reservationBytes,
        admissionStartedAt: context.admissionStartedAt,
        admittedAt: context.admittedAt,
        startedAt: context.startedAt,
        finishedAt,
        globalOutstandingBytes: context.globalOutstandingBytes,
        globalOutstandingCount: context.globalOutstandingCount,
        socketOutstandingBytes: context.socketOutstandingBytes,
        socketOutstandingCount: context.socketOutstandingCount,
      }),
    );
  }

  #dispatchObservation(observation: Readonly<RelayQueueObservation>): void {
    const observer = this.#observer;
    if (observer === undefined) return;
    void Promise.resolve()
      .then(() => observer(observation))
      .catch(() => undefined);
  }
}
