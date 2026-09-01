import type { InputSink, TerminalInputEvent, TerminalMouseInputEvent } from "@wterm/core";
import {
  ClientControlFrameSchema,
  type ClientControlFrame,
  type ResizePayload,
  type ServerControlFrame,
} from "@zhongduan/protocol";

/**
 * E1 Browser-local input queue contract. These are product limits, not tuning defaults:
 * overload is a terminal not-sent result and never consumes a clientInputSeq.
 */
export const INPUT_QUEUE_CONTRACT = Object.freeze({
  maxBytes: 6 * 1024 * 1024 + 4_096,
  maxCount: 256,
  maxPending: 1_024,
  maxPreAdmissionAgeMs: 250,
  maxQueuedAgeMs: 250,
  maxSentAgeMs: 30_000,
  maxTerminationAgeMs: 5_000,
  maxRetainedResults: 4_096,
});

export type LocalIntentId = string;

export interface InputIdentity {
  readonly writerFence: string;
  readonly inputEpoch: string;
  readonly clientInputSeq: string;
}

export type InputTransportSendResult = "accepted" | "proven-not-accepted" | "uncertain";

type InputAck = Extract<ServerControlFrame, { type: "input-ack" }>;
type InputFrame = Exclude<ClientControlFrame, { type: "ack" | "attach" | "writer-lease-renew" }>;
type InputKind = TerminalInputEvent["type"] | "unknown";
type InputPayload = { type: InputFrame["type"] } & Record<string, unknown>;

export interface InputTransport {
  readonly generation: number;
  readonly sender: (frame: InputFrame) => InputTransportSendResult;
  readonly writerFence?: string;
  readonly writerLease?: string;
}

export type InputNotSentReason =
  | "closed"
  | "control-replaced"
  | "epoch-sealed"
  | "malformed"
  | "mouse-gate"
  | "not-writable"
  | "overload"
  | "policy"
  | "pre-admission-expired"
  | "queue-expired"
  | "replica-not-current"
  | "superseded"
  | "transport-rejected"
  | "validation";

export type InputUncertainReason =
  | "acknowledgement-timeout"
  | "closed"
  | "control-replaced"
  | "input-ack-uncertain"
  | "termination-timeout"
  | "transport-uncertain";

interface InputIntentResultBase {
  readonly localIntentId: LocalIntentId;
  readonly kind: InputKind;
  readonly identity: InputIdentity | null;
  readonly consumedAtMs: number;
  readonly terminalAtMs: number;
}

export type InputIntentResult =
  | (InputIntentResultBase & {
      readonly outcome: "not-sent";
      readonly reason: InputNotSentReason;
    })
  | (InputIntentResultBase & {
      readonly outcome: "deterministic";
      readonly reason: "input-ack" | "tombstone-proof";
      readonly result: "duplicate" | "rejected" | "written";
      readonly authorityEventSeq: string;
    })
  | (InputIntentResultBase & {
      readonly outcome: "uncertain";
      readonly reason: InputUncertainReason;
    });

export interface InputIntentSnapshot {
  readonly localIntentId: LocalIntentId;
  readonly kind: InputKind;
  readonly identity: InputIdentity | null;
  readonly state: "consumed" | "queued" | "sending" | "sent" | "terminating" | "terminal";
  readonly consumedAtMs: number;
  readonly admittedAtMs?: number;
  readonly sendDecisionAtMs?: number;
  readonly encodedBytes?: number;
  readonly transportGeneration?: number;
  readonly deadlineAtMs?: number;
  readonly result?: InputIntentResult;
}

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
  setTimer?: (callback: () => void, delayMs: number) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
}

export interface InputTombstoneProof extends InputIdentity {
  readonly authorityEventSeq: string;
}

interface ActiveEpoch {
  readonly writerFence: string;
  readonly inputEpoch: string;
  nextSequence: bigint;
  sealed: boolean;
}

interface IntentRecord {
  readonly localIntentId: LocalIntentId;
  kind: InputKind;
  readonly consumedAtMs: number;
  state: InputIntentSnapshot["state"];
  identity: InputIdentity | null;
  normalized?: InputPayload;
  frame?: InputFrame;
  preAdmissionBytes: number;
  encodedBytes: number;
  admittedAtMs?: number;
  sendDecisionAtMs?: number;
  deadlineAtMs?: number;
  transportGeneration?: number;
  terminationReason?: "acknowledgement-timeout" | "control-replaced" | "transport-uncertain";
  result?: InputIntentResult;
}

function boundedInteger(value: number, maximum = 1_000_000): number {
  if (!Number.isFinite(value)) return 0;
  return Math.min(maximum, Math.max(0, Math.round(value)));
}

function deltaModeName(value: number | undefined): "pixel" | "line" | "page" {
  if (value === 1) return "line";
  if (value === 2) return "page";
  return "pixel";
}

function identityKey(identity: InputIdentity): string {
  return `${identity.writerFence}\u0000${identity.inputEpoch}\u0000${identity.clientInputSeq}`;
}

function positiveFence(value: string | undefined): bigint | null {
  if (value === undefined) return null;
  try {
    const parsed = BigInt(value);
    return parsed > 0n && parsed.toString() === value ? parsed : null;
  } catch {
    return null;
  }
}

function isCoalesciblePair(left: IntentRecord | undefined, right: IntentRecord): boolean {
  if (left?.normalized === undefined || right.normalized === undefined) return false;
  if (left.normalized.type === "resize-request" && right.normalized.type === "resize-request") {
    return true;
  }
  return (
    left.normalized.type === "mouse" &&
    left.normalized["action"] === "move" &&
    right.normalized.type === "mouse" &&
    right.normalized["action"] === "move"
  );
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
  readonly #setTimer: NonNullable<InputDispatcherOptions["setTimer"]>;
  readonly #clearTimer: NonNullable<InputDispatcherOptions["clearTimer"]>;
  readonly #listeners = new Set<() => void>();
  readonly #records = new Map<LocalIntentId, IntentRecord>();
  readonly #activeByIdentity = new Map<string, IntentRecord>();
  readonly #terminalOrder: LocalIntentId[] = [];
  readonly #resultNotifications: InputIntentResult[] = [];
  #unusedInitialEpoch: string;
  #transport: InputTransport | null = null;
  #activeEpoch: ActiveEpoch | null = null;
  #highestWriterFence = 0n;
  #preAdmission: IntentRecord[] = [];
  #preAdmissionBytes = 0;
  #queue: IntentRecord[] = [];
  #queueBytes = 0;
  #admissionScheduled = false;
  #sendScheduled = false;
  #deadlineTimer: ReturnType<typeof setTimeout> | null = null;
  #deadlineAtMs: number | null = null;
  #localIntentCounter = 0n;
  #latestResize: { cols: number; rows: number; widthPx: number; heightPx: number } | null = null;
  #replicaCurrent = false;
  #resizeConfirmed = false;
  #controlReplacementRequired = false;
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
    this.#setTimer =
      options.setTimer ?? ((callback, delayMs) => globalThis.setTimeout(callback, delayMs));
    this.#clearTimer = options.clearTimer ?? ((timer) => globalThis.clearTimeout(timer));
    this.#unusedInitialEpoch = this.#safeGeneratedId(options.inputEpoch, "input-epoch");
  }

  get inputEpoch(): string {
    return this.#activeEpoch?.inputEpoch ?? this.#unusedInitialEpoch;
  }

  get status(): InputDispatcherStatus {
    return this.#statusSnapshot;
  }

  subscribe(listener: () => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  getIntent(localIntentId: LocalIntentId): InputIntentSnapshot | undefined {
    const record = this.#records.get(localIntentId);
    if (record === undefined) return undefined;
    return Object.freeze({
      localIntentId: record.localIntentId,
      kind: record.kind,
      identity: record.identity,
      state: record.state,
      consumedAtMs: record.consumedAtMs,
      ...(record.admittedAtMs === undefined ? {} : { admittedAtMs: record.admittedAtMs }),
      ...(record.sendDecisionAtMs === undefined
        ? {}
        : { sendDecisionAtMs: record.sendDecisionAtMs }),
      ...(record.encodedBytes === 0 ? {} : { encodedBytes: record.encodedBytes }),
      ...(record.transportGeneration === undefined
        ? {}
        : { transportGeneration: record.transportGeneration }),
      ...(record.deadlineAtMs === undefined ? {} : { deadlineAtMs: record.deadlineAtMs }),
      ...(record.result === undefined ? {} : { result: record.result }),
    });
  }

  getResult(localIntentId: LocalIntentId): InputIntentResult | undefined {
    return this.#records.get(localIntentId)?.result;
  }

  /**
   * A full/control attachment may write only when it carries a strictly newer Cloud writer fence.
   * Same-fence epoch rollover is intentionally deferred to E3.
   */
  attachTransport(transport: InputTransport): boolean {
    return this.#transaction(() => {
      const replacementWasRequired = this.#controlReplacementRequired;
      if (this.#transport !== null && this.#transport !== transport) {
        this.#detachTransport("control-replaced");
      }
      this.#transport = transport;
      const fence = positiveFence(transport.writerFence);
      const hasAuthorityPair = fence !== null && transport.writerLease !== undefined;
      if (!hasAuthorityPair || fence <= this.#highestWriterFence) {
        // Replacing the transport as part of this attempt must not erase a requirement that the
        // old sealed epoch could satisfy only with a strictly newer writer fence.
        if (replacementWasRequired) this.#controlReplacementRequired = true;
        this.#markDirty();
        return false;
      }

      this.#highestWriterFence = fence;
      // A stale, same-fence, or incomplete attachment cannot satisfy a previously required
      // control replacement. Clear the observable requirement only after accepting the strictly
      // newer authority that can create a writable epoch.
      this.#controlReplacementRequired = false;
      const inputEpoch = this.#unusedInitialEpoch;
      this.#unusedInitialEpoch = this.#safeGeneratedId(undefined, "input-epoch");
      this.#activeEpoch = {
        writerFence: transport.writerFence!,
        inputEpoch,
        nextSequence: 1n,
        sealed: false,
      };
      this.#markDirty();
      return true;
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
      const transport = this.#transport;
      if (transport !== null) {
        this.#transport = { generation: transport.generation, sender: transport.sender };
      }
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
      const record = this.#activeByIdentity.get(identityKey(identity));
      if (record === undefined || (record.state !== "sent" && record.state !== "terminating")) {
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
      const record = this.#activeByIdentity.get(identityKey(proof));
      if (record === undefined || record.state !== "terminating") return false;
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
    const record: IntentRecord = {
      localIntentId: this.#nextLocalIntentId(),
      kind: "unknown",
      consumedAtMs: this.#now(),
      state: "consumed",
      identity: null,
      preAdmissionBytes: 0,
      encodedBytes: 0,
    };
    this.#records.set(record.localIntentId, record);

    let normalized: InputPayload;
    try {
      normalized = this.#normalize(event);
      record.kind = normalized.type === "resize-request" ? "resize" : normalized.type;
    } catch {
      this.#finishNotSent(record, "malformed");
      return record.localIntentId;
    }

    const validation = ClientControlFrameSchema.safeParse({
      ...normalized,
      writerLease: "validation",
      inputEpoch: "validation",
      clientInputSeq: "1",
    });
    if (!validation.success) {
      this.#finishNotSent(record, "validation");
      return record.localIntentId;
    }

    let normalizedBytes: number;
    try {
      normalizedBytes = new TextEncoder().encode(JSON.stringify(normalized)).byteLength;
    } catch {
      this.#finishNotSent(record, "malformed");
      return record.localIntentId;
    }
    if (normalizedBytes > INPUT_QUEUE_CONTRACT.maxBytes) {
      this.#finishNotSent(record, "overload");
      return record.localIntentId;
    }

    try {
      if (this.#policy?.(event) === false) {
        this.#finishNotSent(record, "policy");
        return record.localIntentId;
      }
    } catch {
      this.#finishNotSent(record, "policy");
      return record.localIntentId;
    }

    if (normalized.type === "resize-request") {
      const nextResize = {
        cols: normalized["cols"] as number,
        rows: normalized["rows"] as number,
        widthPx: normalized["widthPx"] as number,
        heightPx: normalized["heightPx"] as number,
      };
      if (this.#latestResize !== null && !sameDimensions(this.#latestResize, nextResize)) {
        this.#resizeConfirmed = false;
      }
      this.#latestResize = nextResize;
    }

    const validationReason = this.#authorityValidationReason(normalized);
    if (validationReason !== null) {
      this.#finishNotSent(record, validationReason);
      return record.localIntentId;
    }

    record.normalized = Object.freeze(normalized);
    record.preAdmissionBytes = normalizedBytes;
    const previous = this.#preAdmission.at(-1);
    if (isCoalesciblePair(previous, record)) {
      this.#preAdmission.pop();
      this.#preAdmissionBytes -= previous!.preAdmissionBytes;
      this.#finishNotSent(previous!, "superseded");
    }
    if (
      this.#preAdmission.length + this.#queue.length >= INPUT_QUEUE_CONTRACT.maxCount ||
      this.#preAdmissionBytes + this.#queueBytes + normalizedBytes > INPUT_QUEUE_CONTRACT.maxBytes
    ) {
      this.#finishNotSent(record, "overload");
      return record.localIntentId;
    }

    this.#preAdmission.push(record);
    this.#preAdmissionBytes += normalizedBytes;
    this.#scheduleAdmission();
    this.#markDirty();
    return record.localIntentId;
  }

  #normalize(event: TerminalInputEvent): InputPayload {
    switch (event.type) {
      case "key": {
        const observedEventSeq = this.#getObservedEventSeq() ?? 0n;
        return {
          type: "key",
          observedEventSeq: observedEventSeq.toString(),
          code: event.code,
          key: event.key,
          ...(event.text === undefined ? {} : { text: event.text }),
          modifiers: event.modifiers,
          action: event.action,
          altGraph: event.altGraph,
          composing: event.composing,
          consumedModifiers: event.consumedModifiers,
          ...(event.unshiftedCodepoint === 0
            ? {}
            : { unshiftedCodepoint: event.unshiftedCodepoint }),
        };
      }
      case "text":
        return { type: "text", data: event.text };
      case "paste":
        return { type: "paste", data: event.text };
      case "focus":
        return { type: "focus", focused: event.focused };
      case "resize":
        return {
          type: "resize-request",
          cols: event.cols,
          rows: event.rows,
          widthPx: event.widthPx,
          heightPx: event.heightPx,
        };
      case "mouse":
        return this.#mousePayload(event);
      default:
        throw new Error("unknown input event");
    }
  }

  #mousePayload(event: TerminalMouseInputEvent): InputPayload {
    const common = {
      type: "mouse" as const,
      action: event.action,
      button: event.action === "press" || event.action === "release" ? event.button : null,
      buttons: event.buttons,
      modifiers: event.modifiers,
      altGraph: event.altGraph,
      surface: {
        x: boundedInteger(event.surface.x),
        y: boundedInteger(event.surface.y),
      },
    };
    return event.action === "wheel"
      ? {
          ...common,
          deltaX: event.deltaX,
          deltaY: event.deltaY,
          deltaMode: deltaModeName(event.deltaMode),
        }
      : common;
  }

  #authorityValidationReason(normalized: InputPayload): InputNotSentReason | null {
    if (!this.#isWritable()) return "not-writable";
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
    const records = this.#preAdmission;
    this.#preAdmission = [];
    this.#preAdmissionBytes = 0;
    const now = this.#now();
    for (const record of records) {
      if (record.result !== undefined || record.normalized === undefined) continue;
      if (now - record.consumedAtMs >= INPUT_QUEUE_CONTRACT.maxPreAdmissionAgeMs) {
        this.#finishNotSent(record, "pre-admission-expired");
        continue;
      }

      const transport = this.#transport;
      const epoch = this.#activeEpoch;
      const validationReason = this.#authorityValidationReason(record.normalized);
      if (transport === null || epoch === null || validationReason !== null) {
        this.#finishNotSent(record, validationReason ?? "not-writable");
        continue;
      }
      if (this.#activeByIdentity.size >= INPUT_QUEUE_CONTRACT.maxPending) {
        this.#finishNotSent(record, "overload");
        continue;
      }

      const identity = Object.freeze({
        writerFence: epoch.writerFence,
        inputEpoch: epoch.inputEpoch,
        clientInputSeq: epoch.nextSequence.toString(),
      });
      let frame: InputFrame;
      let encodedBytes: number;
      try {
        frame = ClientControlFrameSchema.parse({
          ...record.normalized,
          writerLease: transport.writerLease,
          inputEpoch: identity.inputEpoch,
          clientInputSeq: identity.clientInputSeq,
        }) as InputFrame;
        encodedBytes = new TextEncoder().encode(JSON.stringify(frame)).byteLength;
      } catch {
        this.#finishNotSent(record, "validation");
        continue;
      }

      // No callback above is trusted to preserve authority. Recheck immediately before the
      // reservation/identity critical section so reentrant replacement cannot capture old state.
      if (
        this.#transport !== transport ||
        this.#activeEpoch !== epoch ||
        epoch.sealed ||
        transport.writerLease === undefined ||
        transport.writerFence !== identity.writerFence
      ) {
        this.#finishNotSent(record, "control-replaced");
        continue;
      }
      if (
        this.#queue.length >= INPUT_QUEUE_CONTRACT.maxCount ||
        this.#queueBytes + encodedBytes > INPUT_QUEUE_CONTRACT.maxBytes
      ) {
        this.#finishNotSent(record, "overload");
        continue;
      }

      // Capacity reservation and sequence allocation are one synchronous critical section.
      epoch.nextSequence += 1n;
      record.identity = identity;
      record.frame = Object.freeze(frame);
      delete record.normalized;
      record.preAdmissionBytes = 0;
      record.encodedBytes = encodedBytes;
      record.admittedAtMs = now;
      record.deadlineAtMs = now + INPUT_QUEUE_CONTRACT.maxQueuedAgeMs;
      record.transportGeneration = transport.generation;
      record.state = "queued";
      this.#queue.push(record);
      this.#queueBytes += encodedBytes;
      this.#activeByIdentity.set(identityKey(identity), record);
      this.#markDirty();
    }
    if (this.#queue.length > 0) this.#scheduleSend();
  }

  #scheduleSend(): void {
    if (this.#sendScheduled) return;
    this.#sendScheduled = true;
    this.#queueMicrotask(() => {
      this.#transaction(() => this.#drainQueue());
    });
  }

  #drainQueue(): void {
    this.#sendScheduled = false;
    while (this.#queue.length > 0) {
      const record = this.#queue[0]!;
      const identity = record.identity;
      const now = this.#now();
      if (identity === null) {
        this.#finishNotSent(record, "validation");
        continue;
      }
      if (record.deadlineAtMs !== undefined && now >= record.deadlineAtMs) {
        this.#finishNotSent(record, "queue-expired");
        this.#sealIdentityEpoch(identity, true);
        continue;
      }
      const transport = this.#transport;
      const epoch = this.#activeEpoch;
      if (
        transport === null ||
        epoch === null ||
        epoch.sealed ||
        record.transportGeneration !== transport.generation ||
        identity.writerFence !== epoch.writerFence ||
        identity.inputEpoch !== epoch.inputEpoch
      ) {
        this.#finishNotSent(record, "control-replaced");
        this.#sealIdentityEpoch(identity, false);
        continue;
      }

      this.#removeQueued(record);
      record.state = "sending";
      record.sendDecisionAtMs = now;
      delete record.deadlineAtMs;
      this.#markDirty();
      let decision: InputTransportSendResult;
      try {
        decision = transport.sender(record.frame!);
      } catch {
        decision = "uncertain";
      }

      // Replacement/result callbacks may reenter while sender is on the stack. The owner that
      // already changed the record wins; a stale return value cannot rewrite that transition.
      if (record.result !== undefined || record.state !== "sending") continue;
      const decidedAt = this.#now();
      if (decision === "accepted") {
        delete record.frame;
        record.state = "sent";
        record.deadlineAtMs = decidedAt + INPUT_QUEUE_CONTRACT.maxSentAgeMs;
        this.#markDirty();
        continue;
      }
      if (decision === "proven-not-accepted") {
        this.#finishNotSent(record, "transport-rejected");
        this.#sealIdentityEpoch(identity, true);
        continue;
      }
      delete record.frame;
      this.#beginTermination(record, "transport-uncertain", decidedAt);
      this.#sealIdentityEpoch(identity, true);
    }
  }

  #detachTransport(reason: "closed" | "control-replaced"): void {
    this.#transport = null;
    this.#controlReplacementRequired = false;
    this.#settlePreAdmission(reason);
    this.#settleQueued(reason);
    if (reason === "closed") {
      for (const record of this.#activeByIdentity.values()) {
        if (
          record.state === "sending" ||
          record.state === "sent" ||
          record.state === "terminating"
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
    const records = this.#preAdmission;
    this.#preAdmission = [];
    this.#preAdmissionBytes = 0;
    for (const record of records) this.#finishNotSent(record, reason);
  }

  #settleQueued(reason: InputNotSentReason): void {
    for (const record of this.#queue.slice()) this.#finishNotSent(record, reason);
  }

  #terminateSent(reason: "control-replaced"): void {
    const now = this.#now();
    for (const record of this.#activeByIdentity.values()) {
      if (record.state === "sending" || record.state === "sent") {
        this.#beginTermination(record, reason, now);
      }
    }
  }

  #beginTermination(
    record: IntentRecord,
    reason: "acknowledgement-timeout" | "control-replaced" | "transport-uncertain",
    now: number,
  ): void {
    if (record.result !== undefined) return;
    delete record.frame;
    record.state = "terminating";
    record.deadlineAtMs = now + INPUT_QUEUE_CONTRACT.maxTerminationAgeMs;
    // The initiating reason is retained as the eventual uncertainty reason unless this was the
    // sent-age transition, whose bounded proof wait ends as termination-timeout.
    record.terminationReason = reason;
    this.#markDirty();
  }

  #sealIdentityEpoch(identity: InputIdentity, requireReplacement: boolean): void {
    const epoch = this.#activeEpoch;
    if (
      epoch === null ||
      epoch.writerFence !== identity.writerFence ||
      epoch.inputEpoch !== identity.inputEpoch
    ) {
      return;
    }
    this.#sealActiveEpoch(requireReplacement);
  }

  #sealActiveEpoch(requireReplacement: boolean): void {
    const epoch = this.#activeEpoch;
    if (epoch === null) return;
    epoch.sealed = true;
    if (requireReplacement) this.#controlReplacementRequired = true;
    for (const record of this.#queue.slice()) {
      if (
        record.identity?.writerFence === epoch.writerFence &&
        record.identity.inputEpoch === epoch.inputEpoch
      ) {
        this.#finishNotSent(record, "epoch-sealed");
      }
    }
    this.#markDirty();
  }

  #expireDeadlines(): void {
    const now = this.#now();
    for (const record of this.#preAdmission.slice()) {
      if (now - record.consumedAtMs >= INPUT_QUEUE_CONTRACT.maxPreAdmissionAgeMs) {
        this.#finishNotSent(record, "pre-admission-expired");
      }
    }
    for (const record of this.#queue.slice()) {
      if (record.deadlineAtMs !== undefined && now >= record.deadlineAtMs) {
        const identity = record.identity;
        this.#finishNotSent(record, "queue-expired");
        if (identity !== null) this.#sealIdentityEpoch(identity, true);
      }
    }
    for (const record of this.#activeByIdentity.values()) {
      if (record.deadlineAtMs === undefined || now < record.deadlineAtMs) continue;
      if (record.state === "sent") {
        const identity = record.identity;
        // A throttled timer does not move the product boundary. The proof interval starts at the
        // original ACK deadline, not when JavaScript eventually gets CPU time again.
        this.#beginTermination(record, "acknowledgement-timeout", record.deadlineAtMs);
        if (identity !== null) this.#sealIdentityEpoch(identity, true);
      }
      if (
        record.state === "terminating" &&
        record.deadlineAtMs !== undefined &&
        now >= record.deadlineAtMs
      ) {
        const { terminationReason } = record;
        this.#finishUncertain(
          record,
          terminationReason === "control-replaced"
            ? "control-replaced"
            : terminationReason === "transport-uncertain"
              ? "transport-uncertain"
              : "termination-timeout",
        );
      }
    }
  }

  #finishNotSent(record: IntentRecord, reason: InputNotSentReason): void {
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
    record: IntentRecord,
    reason: "input-ack" | "tombstone-proof",
    result: "duplicate" | "rejected" | "written",
    authorityEventSeq: string,
  ): void {
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

  #finishUncertain(record: IntentRecord, reason: InputUncertainReason): void {
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

  #finish(record: IntentRecord, result: InputIntentResult): void {
    if (record.result !== undefined) return;
    this.#removePreAdmission(record);
    this.#removeQueued(record);
    if (record.identity !== null) this.#activeByIdentity.delete(identityKey(record.identity));
    delete record.normalized;
    delete record.frame;
    delete record.deadlineAtMs;
    delete record.terminationReason;
    record.state = "terminal";
    record.result = result;
    this.#terminalOrder.push(record.localIntentId);
    while (this.#terminalOrder.length > INPUT_QUEUE_CONTRACT.maxRetainedResults) {
      const evicted = this.#terminalOrder.shift();
      if (evicted !== undefined && this.#records.get(evicted)?.state === "terminal") {
        this.#records.delete(evicted);
      }
    }
    this.#resultNotifications.push(result);
    this.#markDirty();
  }

  #removePreAdmission(record: IntentRecord): void {
    const index = this.#preAdmission.indexOf(record);
    if (index < 0) return;
    this.#preAdmission.splice(index, 1);
    this.#preAdmissionBytes -= record.preAdmissionBytes;
  }

  #removeQueued(record: IntentRecord): void {
    const index = this.#queue.indexOf(record);
    if (index < 0) return;
    this.#queue.splice(index, 1);
    this.#queueBytes -= record.encodedBytes;
  }

  #flushResultNotifications(): void {
    // This is a transaction-local worklist, not a deferred queue. Every outer owner transaction
    // drains it synchronously after status publication and before returning to its caller. If an
    // observer reenters the dispatcher, that nested transaction drains the same worklist in commit
    // order, so synchronous local rejection cannot accumulate unbounded microtask-held results.
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
    // The monotonic owner counter prevents reuse even after bounded result retention evicts the
    // old record; a caller-supplied generator is only a diagnostic suffix.
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

  #isWritable(): boolean {
    const transport = this.#transport;
    const epoch = this.#activeEpoch;
    return (
      transport !== null &&
      epoch !== null &&
      !epoch.sealed &&
      transport.writerLease !== undefined &&
      transport.writerFence === epoch.writerFence
    );
  }

  #transaction<T>(operation: () => T): T {
    const outermost = this.#mutationDepth === 0;
    this.#mutationDepth += 1;
    try {
      // Timers are only wake-up hints. Any owner entry first catches up semantic time so browser
      // suspension/throttling cannot admit input or accept proof beyond a product deadline.
      if (outermost) this.#expireDeadlines();
      return operation();
    } finally {
      this.#mutationDepth -= 1;
      if (this.#mutationDepth === 0) {
        this.#rescheduleDeadline();
        this.#publishStatus();
        this.#flushResultNotifications();
      }
    }
  }

  #rescheduleDeadline(): void {
    let earliest: number | null = null;
    for (const record of this.#preAdmission) {
      const deadline = record.consumedAtMs + INPUT_QUEUE_CONTRACT.maxPreAdmissionAgeMs;
      earliest = earliest === null ? deadline : Math.min(earliest, deadline);
    }
    for (const record of this.#queue) {
      if (record.deadlineAtMs !== undefined) {
        earliest =
          earliest === null ? record.deadlineAtMs : Math.min(earliest, record.deadlineAtMs);
      }
    }
    for (const record of this.#activeByIdentity.values()) {
      if (record.deadlineAtMs !== undefined) {
        earliest =
          earliest === null ? record.deadlineAtMs : Math.min(earliest, record.deadlineAtMs);
      }
    }
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

  #markDirty(): void {
    this.#dirty = true;
  }

  #publishStatus(): void {
    if (!this.#dirty) return;
    this.#dirty = false;
    this.#statusSnapshot = Object.freeze({
      connected: this.#transport !== null,
      controlReplacementRequired: this.#controlReplacementRequired,
      lastStatus: this.#lastStatus,
      pending: this.#activeByIdentity.size,
      preAdmission: this.#preAdmission.length,
      preAdmissionBytes: this.#preAdmissionBytes,
      queued: this.#queue.length,
      queuedBytes: this.#queueBytes,
      replicaCurrent: this.#replicaCurrent,
      resizeConfirmed: this.#resizeConfirmed,
      writable: this.#isWritable(),
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
