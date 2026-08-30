import {
  RecoveryProgressFrameSchema,
  type AuthorityCursor,
  type RecoverySourceClosed,
  type RecoveryStart,
  type RecoveryProgressFrame,
  type ReplicaCursor,
} from "@zhongduan/protocol";

import {
  RecoveryAssembler,
  type RecoveryAssemblerLimits,
  type RecoveryAssemblerResetReason,
} from "./recovery-assembler";
import { RecoveryLiveReceiver, type RecoveryLiveReceiverFailure } from "./recovery-live-receiver";
import type { ReplicaHost, ReplicaSink, SnapshotRestoreSource, SnapshotTransport } from "./types";

export type RecoveryRuntimeState =
  | "awaiting-start"
  | "restoring"
  | "assembling"
  | "live"
  | "failed"
  | "closed";

export type RecoveryRuntimeFailure =
  | RecoveryAssemblerResetReason
  | RecoveryLiveReceiverFailure
  | "restore-failed"
  | "progress-unavailable";

export interface RecoveryRuntimeOptions {
  readonly deliveryGeneration: bigint;
  readonly engineId: string;
  readonly host: ReplicaHost;
  readonly initialCursor?: ReplicaCursor;
  readonly limits: RecoveryAssemblerLimits;
  readonly snapshots: SnapshotTransport;
  readonly streamId: number;
  readonly now?: () => number;
  readonly onFailure: (reason: RecoveryRuntimeFailure) => void;
  readonly onProgress: (frame: RecoveryProgressFrame) => boolean | void;
  readonly onStateChange?: () => void;
  readonly progressRetryMs?: number;
  readonly setTimer?: (callback: () => void, delayMs: number) => ReturnType<typeof setTimeout>;
  readonly clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
  readonly scheduleWork?: (callback: () => void) => void;
}

const DEFAULT_PROGRESS_RETRY_MS = 1_000;

function authorityCursor(cursor: ReplicaCursor): AuthorityCursor {
  return {
    sessionEpoch: cursor.sessionEpoch.toString(),
    eventSeq: cursor.lastEventSeq.toString(),
    nextPtyOffset: cursor.nextPtyOffset.toString(),
  };
}

function replicaCursor(cursor: AuthorityCursor, deliveryGeneration: bigint): ReplicaCursor {
  return {
    sessionEpoch: BigInt(cursor.sessionEpoch),
    deliveryGeneration,
    lastEventSeq: BigInt(cursor.eventSeq),
    nextPtyOffset: BigInt(cursor.nextPtyOffset),
  };
}

function frameKey(frame: RecoveryProgressFrame): string {
  return frame.type === "delivery-received" ? `${frame.type}:${frame.lane}` : frame.type;
}

function sameProgress(
  left: RecoveryProgressFrame | undefined,
  right: RecoveryProgressFrame,
): boolean {
  return left !== undefined && JSON.stringify(left) === JSON.stringify(right);
}

function disposeCallerOwned(replica: ReplicaSink): void {
  try {
    replica.dispose();
  } catch {
    // Ownership was relinquished; a cleanup throw cannot justify a second dispose.
  }
}

/**
 * One Recovery delivery-generation owner.
 *
 * It binds the pure assembler to snapshot restore, the exact ReplicaHost handoff, stable progress
 * retries, and the long-lived live-lane receiver. The caller still owns the WebSocket and replaces
 * this object wholesale when the delivery generation changes.
 */
export class RecoveryRuntime {
  readonly #deliveryGeneration: bigint;
  readonly #engineId: string;
  readonly #host: ReplicaHost;
  readonly #initialCursor: ReplicaCursor | null;
  readonly #initialReplica: ReplicaSink | null;
  readonly #now: () => number;
  readonly #onFailure: RecoveryRuntimeOptions["onFailure"];
  readonly #onProgress: RecoveryRuntimeOptions["onProgress"];
  readonly #onStateChange: NonNullable<RecoveryRuntimeOptions["onStateChange"]>;
  readonly #progressRetryMs: number;
  readonly #setTimer: NonNullable<RecoveryRuntimeOptions["setTimer"]>;
  readonly #clearTimer: NonNullable<RecoveryRuntimeOptions["clearTimer"]>;
  readonly #scheduleWork: NonNullable<RecoveryRuntimeOptions["scheduleWork"]>;
  readonly #snapshots: SnapshotTransport;
  readonly #assembler: RecoveryAssembler;
  readonly #pendingProgress = new Map<string, RecoveryProgressFrame>();

  #adopted = false;
  #adoptedReplica: ReplicaSink | null = null;
  #applyScheduled = false;
  #deadlineTimer: ReturnType<typeof setTimeout> | null = null;
  #failure: RecoveryRuntimeFailure | null = null;
  #live: RecoveryLiveReceiver | null = null;
  #progressRetryTimer: ReturnType<typeof setTimeout> | null = null;
  #restoreAbort: AbortController | null = null;
  #restoreStartedFor: string | null = null;
  #sourceClosedAccepted = false;
  #sourceKind: RecoveryStart["source"]["kind"] | null = null;
  #state: RecoveryRuntimeState = "awaiting-start";
  #targetTainted = false;
  #workEpoch = 0;

  constructor(options: RecoveryRuntimeOptions) {
    const initialReplica = options.host.active;
    if (options.host.engineId !== options.engineId) {
      throw new Error("recovery host engine does not match the runtime engine");
    }
    if (options.initialCursor !== undefined && initialReplica === null) {
      throw new Error("initialCursor requires an active recovery replica");
    }
    if (
      options.progressRetryMs !== undefined &&
      (!Number.isFinite(options.progressRetryMs) || options.progressRetryMs <= 0)
    ) {
      throw new RangeError("progressRetryMs must be a positive finite duration");
    }

    this.#deliveryGeneration = options.deliveryGeneration;
    this.#engineId = options.engineId;
    this.#host = options.host;
    this.#initialCursor = options.initialCursor ? { ...options.initialCursor } : null;
    this.#initialReplica = this.#initialCursor === null ? null : initialReplica;
    const readNow = options.now ?? (() => globalThis.performance.now());
    let lastNow = Number.NEGATIVE_INFINITY;
    this.#now = () => {
      const next = readNow();
      if (!Number.isFinite(next)) throw new RangeError("recovery clock must be finite");
      lastNow = Math.max(lastNow, next);
      return lastNow;
    };
    this.#onFailure = options.onFailure;
    this.#onProgress = options.onProgress;
    this.#onStateChange = options.onStateChange ?? (() => undefined);
    this.#progressRetryMs = options.progressRetryMs ?? DEFAULT_PROGRESS_RETRY_MS;
    this.#setTimer =
      options.setTimer ?? ((callback, delayMs) => globalThis.setTimeout(callback, delayMs));
    this.#clearTimer = options.clearTimer ?? ((timer) => globalThis.clearTimeout(timer));
    this.#scheduleWork = options.scheduleWork ?? queueMicrotask;
    this.#snapshots = options.snapshots;

    const initialCursor = this.#initialCursor;
    this.#assembler = new RecoveryAssembler({
      deliveryGeneration: options.deliveryGeneration,
      engineId: options.engineId,
      limits: options.limits,
      startedAtMs: this.#now(),
      streamId: options.streamId,
      ...(initialCursor === null
        ? {}
        : {
            warm: {
              cursor: authorityCursor(initialCursor),
              replica: this.#initialReplica!,
            },
          }),
    });
    this.#scheduleDeadline();
  }

  get state(): RecoveryRuntimeState {
    return this.#state;
  }

  get failure(): RecoveryRuntimeFailure | null {
    return this.#failure;
  }

  /** Last cursor known to be represented by the visible host replica. */
  get activeCursor(): ReplicaCursor | null {
    if (this.#targetTainted) return null;
    if (this.#assembler.resetResult?.warmTargetTainted) return null;

    let cursor: ReplicaCursor | null;
    let expectedReplica: ReplicaSink | null;
    if (this.#live !== null) {
      if (this.#live.targetTainted) return null;
      cursor = replicaCursor(this.#live.replicaApplied.authorityCursor, this.#deliveryGeneration);
      expectedReplica = this.#adoptedReplica;
    } else if (this.#sourceKind === "warm" || this.#adopted) {
      const applied = this.#assembler.replicaApplied;
      cursor =
        applied === null ? null : replicaCursor(applied.authorityCursor, this.#deliveryGeneration);
      expectedReplica = this.#adopted ? this.#adoptedReplica : this.#initialReplica;
    } else {
      cursor = this.#initialCursor === null ? null : { ...this.#initialCursor };
      expectedReplica = this.#initialReplica;
    }

    if (cursor === null) return null;
    if (expectedReplica === null || this.#host.active !== expectedReplica) {
      this.#targetTainted = true;
      return null;
    }
    return cursor;
  }

  acceptStart(start: RecoveryStart): boolean {
    if (!this.#isOpen()) return false;
    const now = this.#now();
    const accepted = this.#assembler.acceptStart(start, now);
    if (!accepted) return this.#failFromAssembler();
    this.#sourceKind = start.source.kind;
    if (start.source.kind === "snapshot") this.#startRestore(start, start.source);
    this.#drive(true);
    return this.#isOpen();
  }

  acceptSourceClosed(closed: RecoverySourceClosed): boolean {
    if (!this.#isOpen()) return false;
    const accepted = this.#assembler.acceptSourceClosed(closed, this.#now());
    if (!accepted) return this.#failFromAssembler();
    this.#sourceClosedAccepted = true;
    this.#pendingProgress.delete("delivery-received:recovery");
    this.#drive(true);
    return this.#isOpen();
  }

  acceptEnvelope(encoded: ArrayBuffer | Uint8Array): boolean {
    if (!this.#isOpen()) return false;
    if (this.#live !== null) {
      if (!this.#live.acceptEnvelope(encoded)) {
        this.#fail(this.#live.failure ?? "protocol-conflict", false);
        return false;
      }
      this.#publish(this.#live.latestReceipt, true);
      if (!this.#isOpen()) return false;
      this.#publish(this.#live.replicaApplied, true);
      if (!this.#isOpen()) return false;
      this.#notifyStateChange();
      return this.#isOpen();
    }

    const accepted = this.#assembler.acceptEnvelope(encoded, this.#now());
    if (!accepted) return this.#failFromAssembler();
    this.#publishReceipts(true);
    this.#drive(false);
    return this.#isOpen();
  }

  checkDeadlines(): boolean {
    if (!this.#isOpen() || this.#live !== null) return this.#state === "live";
    if (!this.#assembler.checkDeadlines(this.#now())) return this.#failFromAssembler();
    this.#scheduleDeadline();
    return true;
  }

  close(): void {
    if (this.#state === "closed") return;
    ++this.#workEpoch;
    this.#abortRestore();
    this.#cancelDeadline();
    this.#cancelProgressRetry();
    this.#live?.close();
    this.#assembler.close();
    this.#pendingProgress.clear();
    this.#state = "closed";
    this.#notifyStateChange();
  }

  #drive(forceProgress: boolean): void {
    if (!this.#isOpen() || this.#live !== null) return;
    this.#publishReceipts(forceProgress);
    if (!this.#isOpen()) return;
    this.#publish(this.#assembler.replicaApplied, forceProgress);
    if (!this.#isOpen()) return;

    const applied = this.#assembler.continueApply(this.#now());
    if (this.#assembler.state === "reset") {
      this.#failFromAssembler();
      return;
    }
    if (applied > 0) {
      this.#publish(this.#assembler.replicaApplied, true);
      if (!this.#isOpen()) return;
    }

    const handoff = this.#assembler.handoff;
    if (handoff !== null) {
      if (handoff.mode === "warm") {
        if (this.#host.active !== handoff.candidate) {
          this.#assembler.reset("handoff-conflict");
          this.#failFromAssembler();
          return;
        }
      } else {
        try {
          this.#host.adopt(handoff.candidate, handoff.cursor);
          if (this.#host.active !== handoff.candidate) {
            throw new Error("host did not expose the adopted recovery candidate");
          }
        } catch {
          this.#assembler.abandonHandoffOutcomeUncertain(this.#now());
          this.#failFromAssembler(true);
          return;
        }
      }
      if (!this.#assembler.confirmHandoff(handoff.cursor, this.#now())) {
        this.#failFromAssembler(handoff.mode === "cold");
        return;
      }
      this.#adopted = true;
      this.#adoptedReplica = handoff.candidate;
      this.#publish(this.#assembler.replicaApplied, true);
      if (!this.#isOpen()) return;
      this.#publish(this.#assembler.recoveryAdopted, true);
      if (!this.#isOpen()) return;
    }

    const completion = this.#assembler.completion;
    if (completion !== null) {
      const adopted = this.#adoptedReplica;
      if (adopted === null || this.#host.active !== adopted) {
        this.#fail("handoff-conflict", true, true);
        return;
      }
      try {
        this.#live = new RecoveryLiveReceiver(completion, {
          engineId: this.#engineId,
          recoveryId: completion.recoveryAdopted.recoveryId,
          replica: adopted,
        });
      } catch {
        this.#fail("handoff-conflict", true, true);
        return;
      }
      this.#cancelDeadline();
      this.#setState("live");
      this.#notifyStateChange();
      return;
    }

    this.#syncState();
    this.#scheduleDeadline();
    if (applied > 0) this.#scheduleApply();
    this.#notifyStateChange();
  }

  #scheduleApply(): void {
    if (this.#applyScheduled || !this.#isOpen() || this.#live !== null) return;
    this.#applyScheduled = true;
    const epoch = this.#workEpoch;
    this.#scheduleWork(() => {
      this.#applyScheduled = false;
      if (epoch !== this.#workEpoch || !this.#isOpen() || this.#live !== null) return;
      this.#drive(false);
    });
  }

  #startRestore(start: RecoveryStart, source: SnapshotRestoreSource): void {
    if (this.#restoreStartedFor !== null) return;
    this.#restoreStartedFor = start.recoveryId;
    const abort = new AbortController();
    this.#restoreAbort = abort;
    const epoch = this.#workEpoch;
    void (async () => {
      let candidate: ReplicaSink | null = null;
      try {
        const snapshot = await this.#snapshots.load(source, abort.signal);
        abort.signal.throwIfAborted();
        candidate = await this.#host.restore(snapshot, source, abort.signal);
        abort.signal.throwIfAborted();
        if (epoch !== this.#workEpoch || !this.#isOpen()) {
          const staleCandidate = candidate;
          candidate = null;
          disposeCallerOwned(staleCandidate);
          return;
        }
        const accepted = this.#assembler.installSnapshotCandidate(
          {
            base: start.base,
            deliveryGeneration: start.deliveryGeneration,
            recoveryId: start.recoveryId,
          },
          candidate,
          this.#now(),
        );
        if (!accepted) {
          const rejectedCandidate = candidate;
          candidate = null;
          disposeCallerOwned(rejectedCandidate);
          this.#failFromAssembler();
          return;
        }
        candidate = null;
        this.#drive(true);
      } catch {
        if (candidate !== null) {
          const failedCandidate = candidate;
          candidate = null;
          disposeCallerOwned(failedCandidate);
        }
        if (abort.signal.aborted || epoch !== this.#workEpoch || !this.#isOpen()) return;
        this.#fail("restore-failed", true);
      } finally {
        if (this.#restoreAbort === abort) this.#restoreAbort = null;
      }
    })();
  }

  #publishReceipts(force: boolean): void {
    const receipts = this.#assembler.latestReceipts;
    if (!this.#sourceClosedAccepted) this.#publish(receipts.recovery, force);
    if (!this.#isOpen()) return;
    this.#publish(receipts.live, force);
  }

  #publish(frame: RecoveryProgressFrame | null, force: boolean): void {
    if (frame === null || !this.#isOpen()) return;
    const parsed = RecoveryProgressFrameSchema.parse(frame);
    const key = frameKey(parsed);
    const previous = this.#pendingProgress.get(key);
    this.#pendingProgress.set(key, parsed);
    if (!force && sameProgress(previous, parsed)) {
      this.#scheduleProgressRetry();
      return;
    }
    if (!this.#sendProgress(parsed)) return;
    this.#scheduleProgressRetry();
  }

  #sendProgress(frame: RecoveryProgressFrame): boolean {
    try {
      const sent = this.#onProgress(frame);
      if (!this.#isOpen()) return false;
      if (sent === false) {
        this.#fail("progress-unavailable", true);
        return false;
      }
      return true;
    } catch {
      this.#fail("progress-unavailable", true);
      return false;
    }
  }

  #scheduleProgressRetry(): void {
    if (this.#progressRetryTimer !== null || this.#pendingProgress.size === 0 || !this.#isOpen()) {
      return;
    }
    this.#progressRetryTimer = this.#setTimer(() => {
      this.#progressRetryTimer = null;
      if (!this.#isOpen()) return;
      for (const frame of this.#pendingProgress.values()) {
        if (!this.#sendProgress(frame)) return;
      }
      this.#scheduleProgressRetry();
    }, this.#progressRetryMs);
  }

  #scheduleDeadline(): void {
    this.#cancelDeadline();
    const deadline = this.#assembler.nextDeadlineAtMs;
    if (deadline === null || !this.#isOpen() || this.#live !== null) return;
    this.#deadlineTimer = this.#setTimer(
      () => {
        this.#deadlineTimer = null;
        if (!this.#isOpen() || this.#live !== null) return;
        // A wall-clock source may move backwards. Timer delivery still proves the previously
        // scheduled monotonic boundary was reached, so never postpone that boundary.
        if (!this.#assembler.checkDeadlines(Math.max(this.#now(), deadline))) {
          this.#failFromAssembler();
          return;
        }
        this.#scheduleDeadline();
      },
      Math.max(0, deadline - this.#now()),
    );
  }

  #cancelDeadline(): void {
    if (this.#deadlineTimer !== null) this.#clearTimer(this.#deadlineTimer);
    this.#deadlineTimer = null;
  }

  #cancelProgressRetry(): void {
    if (this.#progressRetryTimer !== null) this.#clearTimer(this.#progressRetryTimer);
    this.#progressRetryTimer = null;
  }

  #abortRestore(): void {
    this.#restoreAbort?.abort(new DOMException("recovery generation replaced", "AbortError"));
    this.#restoreAbort = null;
  }

  #syncState(): void {
    if (!this.#isOpen()) return;
    const assemblerState = this.#assembler.state;
    if (assemblerState === "awaiting-start") this.#setState("awaiting-start");
    else if (assemblerState === "restoring") this.#setState("restoring");
    else this.#setState("assembling");
  }

  #setState(state: RecoveryRuntimeState): void {
    this.#state = state;
  }

  #failFromAssembler(targetTainted = false): false {
    const reason = this.#assembler.resetResult?.reason ?? "protocol-conflict";
    this.#fail(reason, false, targetTainted || reason === "handoff-conflict");
    return false;
  }

  #fail(reason: RecoveryRuntimeFailure, closeAssembler: boolean, targetTainted = false): void {
    if (!this.#isOpen()) return;
    if (targetTainted) this.#targetTainted = true;
    ++this.#workEpoch;
    this.#abortRestore();
    this.#cancelDeadline();
    this.#cancelProgressRetry();
    this.#live?.close();
    if (closeAssembler) this.#assembler.close();
    this.#pendingProgress.clear();
    this.#failure = reason;
    this.#state = "failed";
    this.#notifyStateChange();
    this.#onFailure(reason);
  }

  #notifyStateChange(): void {
    this.#onStateChange();
  }

  #isOpen(): boolean {
    return this.#state !== "failed" && this.#state !== "closed";
  }
}
