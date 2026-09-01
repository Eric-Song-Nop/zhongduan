import type { InputSink, TerminalInputEvent } from "@wterm/core";
import type { ResizePayload, ServerControlFrame } from "@zhongduan/protocol";

import {
  allocateInputIdentity,
  assertAuthorityInvariants,
  attachAuthority,
  authorityEpoch,
  authorityTransport,
  controlReplacementRequired,
  detachAuthority,
  identityBelongsToAuthority,
  initialAuthorityState,
  isWritableAuthority,
  revokeAuthority,
  sealAuthority,
  type AuthorityState,
  type InputTransport,
} from "./input-authority";
import {
  encodeInputFrame,
  encodedJsonBytes,
  isCoalesciblePayloadPair,
  normalizeInputEvent,
  validateInputPayload,
  type InputPayload,
} from "./input-codec";
import {
  INPUT_QUEUE_CONTRACT,
  IntentLedger,
  type ActiveIntentRecord,
  type InputIdentity,
  type InputIntentResult,
  type InputIntentSnapshot,
  type InputNotSentReason,
  type InputUncertainReason,
  type IntentRecord,
  type IntentTerminationReason,
  type LocalIntentId,
} from "./input-intent-ledger";

export { INPUT_QUEUE_CONTRACT } from "./input-intent-ledger";
export type {
  InputIdentity,
  InputIntentResult,
  InputIntentSnapshot,
  InputNotSentReason,
  InputUncertainReason,
  LocalIntentId,
} from "./input-intent-ledger";
export type { InputTransport, InputTransportSendResult } from "./input-authority";

type InputAck = Extract<ServerControlFrame, { type: "input-ack" }>;

export interface InputDispatcherStatus {
  readonly connected: boolean;
  readonly controlReplacementRequired: boolean;
  readonly lastStatus: InputAck["status"] | "idle" | "not-sent";
  readonly pending: number;
  readonly preAdmission: number;
  readonly preAdmissionBytes: number;
  readonly queued: number;
  readonly queuedBytes: number;
  readonly replicaCurrent: boolean;
  readonly resizeConfirmed: boolean;
  readonly writable: boolean;
}

export interface InputDispatcherOptions {
  getObservedEventSeq: () => bigint | null;
  inputEpoch?: string;
  createInputEpoch?: () => string;
  createLocalIntentId?: () => string;
  onIntentResult?: (result: InputIntentResult) => void;
  policy?: (event: TerminalInputEvent) => boolean;
  now?: () => number;
  queueMicrotask?: (callback: () => void) => void;
  scheduleSendDecision?: (callback: () => void) => void;
  setTimer?: (callback: () => void, delayMs: number) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
  assertInvariants?: boolean;
}

export interface InputTombstoneProof extends InputIdentity {
  readonly authorityEventSeq: string;
}

function sameDimensions(
  left: { cols: number; rows: number; widthPx: number; heightPx: number },
  right: ResizePayload,
): boolean {
  return (
    left.cols === right.cols &&
    left.rows === right.rows &&
    left.widthPx === right.widthPx &&
    left.heightPx === right.heightPx
  );
}

/** Owns every UI-consumed semantic intent until one immutable local terminal result exists. */
export class InputDispatcher implements InputSink {
  readonly #getObservedEventSeq: () => bigint | null;
  readonly #createInputEpoch: () => string;
  readonly #createLocalIntentId: (() => string) | undefined;
  readonly #onIntentResult: ((result: InputIntentResult) => void) | undefined;
  readonly #policy: ((event: TerminalInputEvent) => boolean) | undefined;
  readonly #now: () => number;
  readonly #queueMicrotask: (callback: () => void) => void;
  readonly #scheduleSendDecision: (callback: () => void) => void;
  readonly #setTimer: NonNullable<InputDispatcherOptions["setTimer"]>;
  readonly #clearTimer: NonNullable<InputDispatcherOptions["clearTimer"]>;
  readonly #assertInvariantTurns: boolean;
  readonly #listeners = new Set<() => void>();
  readonly #ledger = new IntentLedger();
  readonly #resultNotifications: InputIntentResult[] = [];
  #authority: AuthorityState = initialAuthorityState();
  #unusedInitialEpoch: string;
  #admissionScheduled = false;
  #drainingQueue = false;
  #sendScheduled = false;
  #deadlineTimer: ReturnType<typeof setTimeout> | null = null;
  #deadlineAtMs: number | null = null;
  #localIntentCounter = 0n;
  #latestResize: { cols: number; rows: number; widthPx: number; heightPx: number } | null = null;
  #replicaCurrent = false;
  #resizeConfirmed = false;
  #lastStatus: InputDispatcherStatus["lastStatus"] = "idle";
  #mutationDepth = 0;
  #dirty = true;
  #statusSnapshot: InputDispatcherStatus = {
    connected: false,
    controlReplacementRequired: false,
    lastStatus: "idle",
    pending: 0,
    preAdmission: 0,
    preAdmissionBytes: 0,
    queued: 0,
    queuedBytes: 0,
    replicaCurrent: false,
    resizeConfirmed: false,
    writable: false,
  };

  constructor(options: InputDispatcherOptions) {
    this.#getObservedEventSeq = options.getObservedEventSeq;
    this.#createInputEpoch = options.createInputEpoch ?? (() => crypto.randomUUID());
    this.#createLocalIntentId = options.createLocalIntentId;
    this.#onIntentResult = options.onIntentResult;
    this.#policy = options.policy;
    this.#now = options.now ?? Date.now;
    this.#queueMicrotask =
      options.queueMicrotask ?? ((callback) => globalThis.queueMicrotask(callback));
    // Admission keeps one microtask-sized coalescing window. Once identities are allocated, the
    // healthy Browser path drains inline; tests can inject a scheduler to hold the queued state at
    // an exact fault boundary without imposing a second production microtask on every key.
    this.#scheduleSendDecision = options.scheduleSendDecision ?? ((callback) => callback());
    this.#setTimer =
      options.setTimer ?? ((callback, delayMs) => globalThis.setTimeout(callback, delayMs));
    this.#clearTimer = options.clearTimer ?? ((timer) => globalThis.clearTimeout(timer));
    this.#assertInvariantTurns = options.assertInvariants ?? import.meta.env.MODE === "test";
    this.#unusedInitialEpoch = this.#safeGeneratedId(options.inputEpoch, "input-epoch");
  }

  get inputEpoch(): string {
    return authorityEpoch(this.#authority)?.inputEpoch ?? this.#unusedInitialEpoch;
  }

  get status(): InputDispatcherStatus {
    return this.#statusSnapshot;
  }

  subscribe(listener: () => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  getIntent(localIntentId: LocalIntentId): InputIntentSnapshot | undefined {
    return this.#ledger.getSnapshot(localIntentId);
  }

  getResult(localIntentId: LocalIntentId): InputIntentResult | undefined {
    return this.#ledger.getResult(localIntentId);
  }

  /**
   * A full/control attachment may write only when it carries a strictly newer Cloud writer fence.
   * Same-fence epoch rollover is intentionally deferred to E3.
   */
  attachTransport(transport: InputTransport): boolean {
    return this.#transaction(() => {
      const previousTransport = authorityTransport(this.#authority);
      if (previousTransport !== null && previousTransport !== transport) {
        this.#settleControlTransport("control-replaced");
      }

      const attachment = attachAuthority(this.#authority, transport, this.#unusedInitialEpoch);
      this.#authority = attachment.authority;
      if (attachment.accepted) {
        this.#unusedInitialEpoch = this.#safeGeneratedId(undefined, "input-epoch");
      }
      this.#markDirty();
      return attachment.accepted;
    });
  }

  /** Full/control replacement. Data-only replacement must not call this method. */
  detachTransport(reason: "closed" | "control-replaced" = "control-replaced"): void {
    this.#transaction(() => this.#detachTransport(reason));
  }

  /** Explicit marker for tests/callers: replacing only the data socket leaves input untouched. */
  noteDataTransportReplacement(): void {
    // Input authority, FIFO state, and epoch intentionally remain unchanged.
  }

  /** A lease revocation on the current control socket fences writes without replacing that socket. */
  revokeWriterAuthority(): void {
    this.#transaction(() => {
      this.#authority = revokeAuthority(this.#authority);
      this.#settlePreAdmission("not-writable");
      this.#settleQueued("epoch-sealed");
      this.#terminateSent("control-replaced");
      this.#sealActiveEpoch(false);
      this.#markDirty();
    });
  }

  send(event: TerminalInputEvent): LocalIntentId {
    return this.#transaction(() => this.#consume(event));
  }

  /** Reconciliation is a new consumption and therefore receives a new LocalIntentId and identity. */
  reconcileLatestResize(): LocalIntentId | null {
    if (this.#latestResize === null) return null;
    return this.send({ type: "resize", ...this.#latestResize });
  }

  setReplicaCurrent(current: boolean): void {
    this.#transaction(() => {
      if (this.#replicaCurrent === current) return;
      this.#replicaCurrent = current;
      this.#markDirty();
    });
  }

  acceptAcknowledgement(frame: InputAck): boolean {
    return this.#transaction(() => {
      if (frame.writerFence === undefined) return false;
      const identity = {
        writerFence: frame.writerFence,
        inputEpoch: frame.inputEpoch,
        clientInputSeq: frame.clientInputSeq,
      };
      const record = this.#ledger.getByIdentity(identity);
      if (record === undefined || (record.phase !== "sent" && record.phase !== "terminating")) {
        return false;
      }
      this.#lastStatus = frame.status;
      if (frame.status === "uncertain") {
        this.#finishUncertain(record, "input-ack-uncertain");
        this.#sealIdentityEpoch(identity, true);
      } else {
        this.#finishDeterministic(record, "input-ack", frame.status, frame.authorityEventSeq);
      }
      return true;
    });
  }

  /** E1 consumes a proof API in tests; the tombstone wire protocol remains deferred to E3. */
  acceptTombstoneProof(proof: InputTombstoneProof): boolean {
    return this.#transaction(() => {
      const record = this.#ledger.getByIdentity(proof);
      if (record === undefined || record.phase !== "terminating") return false;
      this.#finishDeterministic(record, "tombstone-proof", "rejected", proof.authorityEventSeq);
      return true;
    });
  }

  confirmAuthoritativeResize(dimensions: ResizePayload): void {
    this.#transaction(() => {
      if (
        this.#resizeConfirmed ||
        this.#latestResize === null ||
        !sameDimensions(this.#latestResize, dimensions)
      ) {
        return;
      }
      this.#resizeConfirmed = true;
      this.#markDirty();
    });
  }

  #consume(event: TerminalInputEvent): LocalIntentId {
    const localIntentId = this.#nextLocalIntentId();
    this.#ledger.createConsumed(localIntentId, this.#now());

    let normalized: InputPayload;
    try {
      const observedEventSeq = event.type === "key" ? (this.#getObservedEventSeq() ?? 0n) : 0n;
      const result = normalizeInputEvent(event, observedEventSeq);
      normalized = Object.freeze(result.payload);
      this.#ledger.identify(localIntentId, result.kind);
    } catch {
      this.#finishNotSent(localIntentId, "malformed");
      return localIntentId;
    }

    const validated = validateInputPayload(normalized);
    if (validated === null) {
      this.#finishNotSent(localIntentId, "validation");
      return localIntentId;
    }

    let normalizedBytes: number;
    try {
      normalizedBytes = encodedJsonBytes(validated);
    } catch {
      this.#finishNotSent(localIntentId, "malformed");
      return localIntentId;
    }
    if (normalizedBytes > INPUT_QUEUE_CONTRACT.maxBytes) {
      this.#finishNotSent(localIntentId, "overload");
      return localIntentId;
    }

    try {
      if (this.#policy?.(event) === false) {
        this.#finishNotSent(localIntentId, "policy");
        return localIntentId;
      }
    } catch {
      this.#finishNotSent(localIntentId, "policy");
      return localIntentId;
    }

    if (validated.type === "resize-request") {
      const nextResize = {
        cols: validated["cols"] as number,
        rows: validated["rows"] as number,
        widthPx: validated["widthPx"] as number,
        heightPx: validated["heightPx"] as number,
      };
      if (this.#latestResize !== null && !sameDimensions(this.#latestResize, nextResize)) {
        this.#resizeConfirmed = false;
      }
      this.#latestResize = nextResize;
    }

    const validationReason = this.#authorityValidationReason(validated);
    if (validationReason !== null) {
      this.#finishNotSent(localIntentId, validationReason);
      return localIntentId;
    }

    const previous = this.#ledger.lastPreAdmission();
    if (isCoalesciblePayloadPair(previous?.normalized, validated)) {
      this.#finishNotSent(previous!, "superseded");
    }
    if (!this.#ledger.canReservePreAdmission(normalizedBytes)) {
      this.#finishNotSent(localIntentId, "overload");
      return localIntentId;
    }

    this.#ledger.enqueuePreAdmission(localIntentId, validated, normalizedBytes);
    // Key events cannot coalesce. Admit them in the originating event turn so the safety ledger
    // does not add a whole microtask to the latency-sensitive keydown path. A key also acts as an
    // ordering barrier and admits any earlier state intent before itself.
    if (validated.type === "key") this.#admitPreAdmission();
    else this.#scheduleAdmission();
    this.#markDirty();
    return localIntentId;
  }

  #authorityValidationReason(normalized: InputPayload): InputNotSentReason | null {
    if (!isWritableAuthority(this.#authority)) return "not-writable";
    if (normalized.type !== "mouse") return null;
    if (!this.#replicaCurrent) return "replica-not-current";
    if (!this.#resizeConfirmed) return "mouse-gate";
    return null;
  }

  #scheduleAdmission(): void {
    if (this.#admissionScheduled) return;
    this.#admissionScheduled = true;
    this.#queueMicrotask(() => {
      this.#transaction(() => this.#admitPreAdmission());
    });
  }

  #admitPreAdmission(): void {
    this.#admissionScheduled = false;
    const records = this.#ledger.takePreAdmission();
    const now = this.#now();
    for (const candidate of records) {
      const record = this.#ledger.get(candidate.localIntentId);
      if (record?.phase !== "pre-admission") continue;
      if (now - record.consumedAtMs >= INPUT_QUEUE_CONTRACT.maxPreAdmissionAgeMs) {
        this.#finishNotSent(record, "pre-admission-expired");
        continue;
      }

      const authority = this.#authority;
      const validationReason = this.#authorityValidationReason(record.normalized);
      if (!isWritableAuthority(authority) || validationReason !== null) {
        this.#finishNotSent(record, validationReason ?? "not-writable");
        continue;
      }
      if (this.#ledger.pendingCount >= INPUT_QUEUE_CONTRACT.maxPending) {
        this.#finishNotSent(record, "overload");
        continue;
      }

      const identity = Object.freeze({
        writerFence: authority.epoch.writerFence,
        inputEpoch: authority.epoch.inputEpoch,
        clientInputSeq: authority.epoch.nextSequence.toString(),
      });
      let encoded: ReturnType<typeof encodeInputFrame>;
      try {
        encoded = encodeInputFrame(record.normalized, authority.transport.writerLease, identity);
      } catch {
        this.#finishNotSent(record, "validation");
        continue;
      }

      // Pure codec work should not change authority, but retaining this identity check makes the
      // critical section explicit if encoding later acquires a callback or effect.
      if (this.#authority !== authority || !this.#ledger.canAdmit(encoded.encodedBytes)) {
        this.#finishNotSent(
          record,
          this.#authority !== authority ? "control-replaced" : "overload",
        );
        continue;
      }

      const allocated = allocateInputIdentity(authority);
      this.#authority = allocated.authority;
      this.#ledger.admit(record.localIntentId, {
        identity: allocated.identity,
        frame: encoded.frame,
        encodedBytes: encoded.encodedBytes,
        admittedAtMs: now,
        deadlineAtMs: now + INPUT_QUEUE_CONTRACT.maxQueuedAgeMs,
        transportGeneration: authority.transport.generation,
      });
      this.#markDirty();
    }
    if (this.#ledger.queuedCount > 0) this.#scheduleSend();
  }

  #scheduleSend(): void {
    if (this.#sendScheduled || this.#drainingQueue) return;
    this.#sendScheduled = true;
    this.#scheduleSendDecision(() => {
      this.#transaction(() => this.#drainQueue());
    });
  }

  #drainQueue(): void {
    this.#sendScheduled = false;
    if (this.#drainingQueue) return;
    this.#drainingQueue = true;
    try {
      for (;;) {
        const queued = this.#ledger.queueHead();
        if (queued === undefined) return;
        const now = this.#now();
        if (now >= queued.deadlineAtMs) {
          this.#finishNotSent(queued, "queue-expired");
          this.#sealIdentityEpoch(queued.identity, true);
          continue;
        }

        const authority = this.#authority;
        if (
          !isWritableAuthority(authority) ||
          queued.transportGeneration !== authority.transport.generation ||
          queued.identity.writerFence !== authority.epoch.writerFence ||
          queued.identity.inputEpoch !== authority.epoch.inputEpoch
        ) {
          this.#finishNotSent(queued, "control-replaced");
          this.#sealIdentityEpoch(queued.identity, false);
          continue;
        }

        const sending = this.#ledger.startSending(queued.localIntentId, now);
        this.#markDirty();
        let decision: ReturnType<InputTransport["sender"]>;
        try {
          decision = authority.transport.sender(sending.frame);
        } catch {
          decision = "uncertain";
        }

        // Replacement/result callbacks may reenter while sender is on the stack. The canonical
        // ledger record wins; a stale return value cannot rewrite a transition that already occurred.
        const current = this.#ledger.get(sending.localIntentId);
        if (current?.phase !== "sending") continue;
        const decidedAt = this.#now();
        if (decision === "accepted") {
          this.#ledger.markSent(
            current.localIntentId,
            decidedAt + INPUT_QUEUE_CONTRACT.maxSentAgeMs,
          );
          this.#markDirty();
          continue;
        }
        if (decision === "proven-not-accepted") {
          this.#finishNotSent(current, "transport-rejected");
          this.#sealIdentityEpoch(current.identity, true);
          continue;
        }
        this.#beginTermination(current, "transport-uncertain", decidedAt);
        this.#sealIdentityEpoch(current.identity, true);
      }
    } finally {
      this.#drainingQueue = false;
    }
  }

  #detachTransport(reason: "closed" | "control-replaced"): void {
    this.#settleControlTransport(reason);
    this.#authority = detachAuthority(this.#authority);
    this.#markDirty();
  }

  #settleControlTransport(reason: "closed" | "control-replaced"): void {
    this.#settlePreAdmission(reason);
    this.#settleQueued(reason);
    if (reason === "closed") {
      for (const record of this.#ledger.activeRecords()) {
        if (
          record.phase === "sending" ||
          record.phase === "sent" ||
          record.phase === "terminating"
        ) {
          this.#finishUncertain(record, "closed");
        }
      }
    } else {
      this.#terminateSent("control-replaced");
    }
    this.#sealActiveEpoch(false);
    this.#replicaCurrent = false;
    this.#resizeConfirmed = false;
    this.#markDirty();
  }

  #settlePreAdmission(reason: InputNotSentReason): void {
    for (const record of this.#ledger.takePreAdmission()) this.#finishNotSent(record, reason);
  }

  #settleQueued(reason: InputNotSentReason): void {
    for (const record of this.#ledger.queuedRecords()) this.#finishNotSent(record, reason);
  }

  #terminateSent(reason: "control-replaced"): void {
    const now = this.#now();
    for (const record of this.#ledger.activeRecords()) {
      if (record.phase === "sending" || record.phase === "sent") {
        this.#beginTermination(record, reason, now);
      }
    }
  }

  #beginTermination(
    record: ActiveIntentRecord,
    reason: IntentTerminationReason,
    now: number,
  ): void {
    if (
      this.#ledger.beginTermination(
        record.localIntentId,
        reason,
        now + INPUT_QUEUE_CONTRACT.maxTerminationAgeMs,
      ) !== null
    ) {
      this.#markDirty();
    }
  }

  #sealIdentityEpoch(identity: InputIdentity, requireReplacement: boolean): void {
    if (!identityBelongsToAuthority(this.#authority, identity)) return;
    this.#sealActiveEpoch(requireReplacement);
  }

  #sealActiveEpoch(requireReplacement: boolean): void {
    const epoch = authorityEpoch(this.#authority);
    if (epoch === null) return;
    this.#authority = sealAuthority(
      this.#authority,
      requireReplacement ? "replacement-required" : "transport-replaced",
    );
    for (const record of this.#ledger.queuedRecords()) {
      if (
        record.identity.writerFence === epoch.writerFence &&
        record.identity.inputEpoch === epoch.inputEpoch
      ) {
        this.#finishNotSent(record, "epoch-sealed");
      }
    }
    this.#markDirty();
  }

  #expireDeadlines(): void {
    const now = this.#now();
    for (const record of this.#ledger.preAdmissionRecords()) {
      if (now - record.consumedAtMs >= INPUT_QUEUE_CONTRACT.maxPreAdmissionAgeMs) {
        this.#finishNotSent(record, "pre-admission-expired");
      }
    }
    for (const record of this.#ledger.queuedRecords()) {
      if (now >= record.deadlineAtMs) {
        this.#finishNotSent(record, "queue-expired");
        this.#sealIdentityEpoch(record.identity, true);
      }
    }
    for (const candidate of this.#ledger.activeRecords()) {
      if (candidate.phase === "sent" && now >= candidate.deadlineAtMs) {
        // A throttled timer does not move the product boundary. The proof interval starts at the
        // original ACK deadline, not when JavaScript eventually gets CPU time again.
        this.#beginTermination(candidate, "acknowledgement-timeout", candidate.deadlineAtMs);
        this.#sealIdentityEpoch(candidate.identity, true);
      }
      const current = this.#ledger.get(candidate.localIntentId);
      if (current?.phase === "terminating" && now >= current.deadlineAtMs) {
        this.#finishUncertain(
          current,
          current.terminationReason === "control-replaced"
            ? "control-replaced"
            : current.terminationReason === "transport-uncertain"
              ? "transport-uncertain"
              : "termination-timeout",
        );
      }
    }
  }

  #finishNotSent(recordOrId: IntentRecord | LocalIntentId, reason: InputNotSentReason): void {
    const record = this.#canonicalRecord(recordOrId);
    if (record === undefined || record.phase === "terminal") return;
    this.#lastStatus = "not-sent";
    this.#finish(
      record,
      Object.freeze({
        localIntentId: record.localIntentId,
        kind: record.kind,
        identity: record.identity,
        consumedAtMs: record.consumedAtMs,
        terminalAtMs: this.#now(),
        outcome: "not-sent",
        reason,
      }),
    );
  }

  #finishDeterministic(
    recordOrId: IntentRecord | LocalIntentId,
    reason: "input-ack" | "tombstone-proof",
    result: "duplicate" | "rejected" | "written",
    authorityEventSeq: string,
  ): void {
    const record = this.#canonicalRecord(recordOrId);
    if (record === undefined || record.phase === "terminal") return;
    this.#lastStatus = result;
    this.#finish(
      record,
      Object.freeze({
        localIntentId: record.localIntentId,
        kind: record.kind,
        identity: record.identity,
        consumedAtMs: record.consumedAtMs,
        terminalAtMs: this.#now(),
        outcome: "deterministic",
        reason,
        result,
        authorityEventSeq,
      }),
    );
  }

  #finishUncertain(recordOrId: IntentRecord | LocalIntentId, reason: InputUncertainReason): void {
    const record = this.#canonicalRecord(recordOrId);
    if (record === undefined || record.phase === "terminal") return;
    this.#lastStatus = "uncertain";
    this.#finish(
      record,
      Object.freeze({
        localIntentId: record.localIntentId,
        kind: record.kind,
        identity: record.identity,
        consumedAtMs: record.consumedAtMs,
        terminalAtMs: this.#now(),
        outcome: "uncertain",
        reason,
      }),
    );
  }

  #finish(record: Exclude<IntentRecord, { phase: "terminal" }>, result: InputIntentResult): void {
    if (this.#ledger.finish(record.localIntentId, result) === null) return;
    this.#resultNotifications.push(result);
    this.#markDirty();
  }

  #canonicalRecord(recordOrId: IntentRecord | LocalIntentId): IntentRecord | undefined {
    return this.#ledger.get(typeof recordOrId === "string" ? recordOrId : recordOrId.localIntentId);
  }

  #flushResultNotifications(): void {
    // This is a transaction-local worklist, not a deferred queue. Every outer owner transaction
    // drains it synchronously after status publication and before returning to its caller.
    while (this.#resultNotifications.length > 0) {
      const result = this.#resultNotifications.shift()!;
      try {
        this.#onIntentResult?.(result);
      } catch {
        // Result observation is isolated from the state machine owner.
      }
    }
  }

  #nextLocalIntentId(): LocalIntentId {
    this.#localIntentCounter += 1n;
    let generated: unknown;
    try {
      generated = this.#createLocalIntentId?.();
    } catch {
      generated = undefined;
    }
    const candidate =
      typeof generated === "string" && generated.length > 0 && generated.length <= 220
        ? generated
        : crypto.randomUUID();
    return `local-intent-${this.#localIntentCounter}-${candidate}`;
  }

  #safeGeneratedId(provided: string | undefined, prefix: string): string {
    let candidate: unknown = provided;
    if (candidate === undefined) {
      try {
        candidate = this.#createInputEpoch();
      } catch {
        candidate = undefined;
      }
    }
    return typeof candidate === "string" && candidate.length > 0 && candidate.length <= 128
      ? candidate
      : `${prefix}-${crypto.randomUUID()}`;
  }

  #transaction<T>(operation: () => T): T {
    const outermost = this.#mutationDepth === 0;
    this.#mutationDepth += 1;
    try {
      // Timers are only wake-up hints. Every outer entry first catches up semantic time.
      if (outermost) this.#expireDeadlines();
      return operation();
    } finally {
      this.#mutationDepth -= 1;
      if (this.#mutationDepth === 0) {
        this.#rescheduleDeadline();
        if (this.#assertInvariantTurns) this.#assertInvariants(false);
        this.#publishStatus();
        this.#flushResultNotifications();
        if (this.#assertInvariantTurns) this.#assertInvariants(true);
      }
    }
  }

  #rescheduleDeadline(): void {
    const earliest = this.#ledger.earliestDeadline();
    if (earliest === this.#deadlineAtMs) return;
    if (this.#deadlineTimer !== null) this.#clearTimer(this.#deadlineTimer);
    this.#deadlineTimer = null;
    this.#deadlineAtMs = earliest;
    if (earliest === null) return;
    this.#deadlineTimer = this.#setTimer(
      () => {
        this.#deadlineTimer = null;
        this.#deadlineAtMs = null;
        this.#transaction(() => this.#expireDeadlines());
      },
      Math.max(0, earliest - this.#now()),
    );
  }

  #assertInvariants(notificationsDrained: boolean): void {
    this.#ledger.assertInvariants();
    assertAuthorityInvariants(this.#authority);
    if (notificationsDrained && this.#resultNotifications.length !== 0) {
      throw new Error("result notification worklist escaped its owner turn");
    }
    const queued = this.#ledger.queuedRecords();
    if (!isWritableAuthority(this.#authority) && queued.length > 0) {
      throw new Error("a non-open authority retains admitted queue work");
    }
    if (isWritableAuthority(this.#authority)) {
      for (const record of queued) {
        if (
          record.identity.writerFence !== this.#authority.epoch.writerFence ||
          record.identity.inputEpoch !== this.#authority.epoch.inputEpoch ||
          record.transportGeneration !== this.#authority.transport.generation
        ) {
          throw new Error("queued intent does not belong to the current open authority");
        }
      }
    }
  }

  #markDirty(): void {
    this.#dirty = true;
  }

  #publishStatus(): void {
    if (!this.#dirty) return;
    this.#dirty = false;
    this.#statusSnapshot = Object.freeze({
      connected: authorityTransport(this.#authority) !== null,
      controlReplacementRequired: controlReplacementRequired(this.#authority),
      lastStatus: this.#lastStatus,
      pending: this.#ledger.pendingCount,
      preAdmission: this.#ledger.preAdmissionCount,
      preAdmissionBytes: this.#ledger.preAdmissionBytes,
      queued: this.#ledger.queuedCount,
      queuedBytes: this.#ledger.queuedBytes,
      replicaCurrent: this.#replicaCurrent,
      resizeConfirmed: this.#resizeConfirmed,
      writable: isWritableAuthority(this.#authority),
    });
    for (const listener of this.#listeners) {
      try {
        listener();
      } catch {
        // A view subscriber cannot roll back or hide an already-committed owner transition.
      }
    }
  }
}
