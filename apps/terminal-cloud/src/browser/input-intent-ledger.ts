import type { InputFrame, InputKind, InputPayload } from "./input-codec";

/** Product limits for the Browser-local E1 owner, not tuning defaults. */
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

export type IntentTerminationReason =
  | "acknowledgement-timeout"
  | "control-replaced"
  | "transport-uncertain";

interface IntentRecordBase {
  readonly localIntentId: LocalIntentId;
  readonly kind: InputKind;
  readonly consumedAtMs: number;
}

export interface ConsumedIntentRecord extends IntentRecordBase {
  readonly phase: "consumed";
  readonly identity: null;
  readonly encodedBytes: 0;
}

export interface PreAdmissionIntentRecord extends IntentRecordBase {
  readonly phase: "pre-admission";
  readonly identity: null;
  readonly normalized: InputPayload;
  readonly preAdmissionBytes: number;
  readonly encodedBytes: 0;
}

interface AdmittedIntentRecordBase extends IntentRecordBase {
  readonly identity: InputIdentity;
  readonly encodedBytes: number;
  readonly admittedAtMs: number;
  readonly transportGeneration: number;
}

export interface QueuedIntentRecord extends AdmittedIntentRecordBase {
  readonly phase: "queued";
  readonly frame: InputFrame;
  readonly deadlineAtMs: number;
}

export interface SendingIntentRecord extends AdmittedIntentRecordBase {
  readonly phase: "sending";
  readonly frame: InputFrame;
  readonly sendDecisionAtMs: number;
}

export interface SentIntentRecord extends AdmittedIntentRecordBase {
  readonly phase: "sent";
  readonly sendDecisionAtMs: number;
  readonly deadlineAtMs: number;
}

export interface TerminatingIntentRecord extends AdmittedIntentRecordBase {
  readonly phase: "terminating";
  readonly sendDecisionAtMs: number;
  readonly deadlineAtMs: number;
  readonly terminationReason: IntentTerminationReason;
}

interface TerminalIntentRecordBase extends IntentRecordBase {
  readonly phase: "terminal";
  readonly result: InputIntentResult;
}

export interface UnadmittedTerminalIntentRecord extends TerminalIntentRecordBase {
  readonly identity: null;
  readonly encodedBytes: 0;
}

export interface AdmittedTerminalIntentRecord
  extends TerminalIntentRecordBase, AdmittedIntentRecordBase {
  readonly sendDecisionAtMs?: number;
}

export type TerminalIntentRecord = UnadmittedTerminalIntentRecord | AdmittedTerminalIntentRecord;

export type ActiveIntentRecord =
  | QueuedIntentRecord
  | SendingIntentRecord
  | SentIntentRecord
  | TerminatingIntentRecord;

export type IntentRecord =
  | ConsumedIntentRecord
  | PreAdmissionIntentRecord
  | ActiveIntentRecord
  | TerminalIntentRecord;

function identityKey(identity: InputIdentity): string {
  return `${identity.writerFence}\u0000${identity.inputEpoch}\u0000${identity.clientInputSeq}`;
}

function sameIdentity(left: InputIdentity | null, right: InputIdentity | null): boolean {
  if (left === null || right === null) return left === right;
  return identityKey(left) === identityKey(right);
}

function snapshotState(record: IntentRecord): InputIntentSnapshot["state"] {
  return record.phase === "pre-admission" ? "consumed" : record.phase;
}

function terminalRecord(
  record: Exclude<IntentRecord, TerminalIntentRecord>,
  result: InputIntentResult,
): TerminalIntentRecord {
  if (record.identity === null) {
    return {
      phase: "terminal",
      localIntentId: record.localIntentId,
      kind: record.kind,
      consumedAtMs: record.consumedAtMs,
      identity: null,
      encodedBytes: 0,
      result,
    };
  }
  return {
    phase: "terminal",
    localIntentId: record.localIntentId,
    kind: record.kind,
    consumedAtMs: record.consumedAtMs,
    identity: record.identity,
    encodedBytes: record.encodedBytes,
    admittedAtMs: record.admittedAtMs,
    transportGeneration: record.transportGeneration,
    ...(record.phase === "queued" ? {} : { sendDecisionAtMs: record.sendDecisionAtMs }),
    result,
  };
}

export class IntentLedger {
  readonly #records = new Map<LocalIntentId, IntentRecord>();
  readonly #activeByIdentity = new Map<string, LocalIntentId>();
  readonly #terminalOrder: LocalIntentId[] = [];
  #preAdmissionIds: LocalIntentId[] = [];
  #preAdmissionBytes = 0;
  #queueIds: LocalIntentId[] = [];
  #queueBytes = 0;

  createConsumed(localIntentId: LocalIntentId, consumedAtMs: number): ConsumedIntentRecord {
    if (this.#records.has(localIntentId)) throw new Error("LocalIntentId must be unique");
    const record: ConsumedIntentRecord = {
      phase: "consumed",
      localIntentId,
      kind: "unknown",
      consumedAtMs,
      identity: null,
      encodedBytes: 0,
    };
    this.#records.set(localIntentId, record);
    return record;
  }

  identify(localIntentId: LocalIntentId, kind: InputKind): ConsumedIntentRecord {
    const record = this.require(localIntentId, "consumed");
    const next: ConsumedIntentRecord = { ...record, kind };
    this.#records.set(localIntentId, next);
    return next;
  }

  get(localIntentId: LocalIntentId): IntentRecord | undefined {
    return this.#records.get(localIntentId);
  }

  getResult(localIntentId: LocalIntentId): InputIntentResult | undefined {
    const record = this.#records.get(localIntentId);
    return record?.phase === "terminal" ? record.result : undefined;
  }

  getSnapshot(localIntentId: LocalIntentId): InputIntentSnapshot | undefined {
    const record = this.#records.get(localIntentId);
    if (record === undefined) return undefined;
    return Object.freeze({
      localIntentId: record.localIntentId,
      kind: record.kind,
      identity: record.identity,
      state: snapshotState(record),
      consumedAtMs: record.consumedAtMs,
      ...(record.identity === null
        ? {}
        : {
            admittedAtMs: record.admittedAtMs,
            encodedBytes: record.encodedBytes,
            transportGeneration: record.transportGeneration,
          }),
      ...(record.phase === "sending" ||
      record.phase === "sent" ||
      record.phase === "terminating" ||
      (record.phase === "terminal" &&
        record.identity !== null &&
        record.sendDecisionAtMs !== undefined)
        ? { sendDecisionAtMs: record.sendDecisionAtMs }
        : {}),
      ...(record.phase === "queued" || record.phase === "sent" || record.phase === "terminating"
        ? { deadlineAtMs: record.deadlineAtMs }
        : {}),
      ...(record.phase === "terminal" ? { result: record.result } : {}),
    });
  }

  canReservePreAdmission(encodedBytes: number): boolean {
    return (
      this.#preAdmissionIds.length + this.#queueIds.length < INPUT_QUEUE_CONTRACT.maxCount &&
      this.#preAdmissionBytes + this.#queueBytes + encodedBytes <= INPUT_QUEUE_CONTRACT.maxBytes
    );
  }

  enqueuePreAdmission(
    localIntentId: LocalIntentId,
    normalized: InputPayload,
    preAdmissionBytes: number,
  ): PreAdmissionIntentRecord {
    const record = this.require(localIntentId, "consumed");
    if (!this.canReservePreAdmission(preAdmissionBytes)) {
      throw new Error("pre-admission capacity must be reserved before enqueue");
    }
    const next: PreAdmissionIntentRecord = {
      ...record,
      phase: "pre-admission",
      normalized,
      preAdmissionBytes,
    };
    this.#records.set(localIntentId, next);
    this.#preAdmissionIds.push(localIntentId);
    this.#preAdmissionBytes += preAdmissionBytes;
    return next;
  }

  lastPreAdmission(): PreAdmissionIntentRecord | undefined {
    const localIntentId = this.#preAdmissionIds.at(-1);
    if (localIntentId === undefined) return undefined;
    return this.require(localIntentId, "pre-admission");
  }

  preAdmissionRecords(): PreAdmissionIntentRecord[] {
    return this.#preAdmissionIds.map((localIntentId) =>
      this.require(localIntentId, "pre-admission"),
    );
  }

  takePreAdmission(): PreAdmissionIntentRecord[] {
    const records = this.preAdmissionRecords();
    this.#preAdmissionIds = [];
    this.#preAdmissionBytes = 0;
    return records;
  }

  canAdmit(encodedBytes: number): boolean {
    return (
      this.#activeByIdentity.size < INPUT_QUEUE_CONTRACT.maxPending &&
      this.#queueIds.length < INPUT_QUEUE_CONTRACT.maxCount &&
      this.#queueBytes + encodedBytes <= INPUT_QUEUE_CONTRACT.maxBytes
    );
  }

  admit(
    localIntentId: LocalIntentId,
    admission: {
      readonly identity: InputIdentity;
      readonly frame: InputFrame;
      readonly encodedBytes: number;
      readonly admittedAtMs: number;
      readonly deadlineAtMs: number;
      readonly transportGeneration: number;
    },
  ): QueuedIntentRecord {
    const record = this.require(localIntentId, "pre-admission");
    if (!this.canAdmit(admission.encodedBytes)) {
      throw new Error("queue and identity capacity must be reserved before admission");
    }
    const key = identityKey(admission.identity);
    if (this.#activeByIdentity.has(key)) throw new Error("InputIdentity must be unique");
    const next: QueuedIntentRecord = {
      phase: "queued",
      localIntentId: record.localIntentId,
      kind: record.kind,
      consumedAtMs: record.consumedAtMs,
      ...admission,
    };
    this.#records.set(localIntentId, next);
    this.#queueIds.push(localIntentId);
    this.#queueBytes += admission.encodedBytes;
    this.#activeByIdentity.set(key, localIntentId);
    return next;
  }

  queueHead(): QueuedIntentRecord | undefined {
    const localIntentId = this.#queueIds[0];
    return localIntentId === undefined ? undefined : this.require(localIntentId, "queued");
  }

  queuedRecords(): QueuedIntentRecord[] {
    return this.#queueIds.map((localIntentId) => this.require(localIntentId, "queued"));
  }

  startSending(localIntentId: LocalIntentId, sendDecisionAtMs: number): SendingIntentRecord {
    const record = this.require(localIntentId, "queued");
    this.#removeQueued(localIntentId, record.encodedBytes);
    const next: SendingIntentRecord = {
      phase: "sending",
      localIntentId: record.localIntentId,
      kind: record.kind,
      consumedAtMs: record.consumedAtMs,
      identity: record.identity,
      frame: record.frame,
      encodedBytes: record.encodedBytes,
      admittedAtMs: record.admittedAtMs,
      transportGeneration: record.transportGeneration,
      sendDecisionAtMs,
    };
    this.#records.set(localIntentId, next);
    return next;
  }

  markSent(localIntentId: LocalIntentId, deadlineAtMs: number): SentIntentRecord {
    const record = this.require(localIntentId, "sending");
    const next: SentIntentRecord = {
      phase: "sent",
      localIntentId: record.localIntentId,
      kind: record.kind,
      consumedAtMs: record.consumedAtMs,
      identity: record.identity,
      encodedBytes: record.encodedBytes,
      admittedAtMs: record.admittedAtMs,
      transportGeneration: record.transportGeneration,
      sendDecisionAtMs: record.sendDecisionAtMs,
      deadlineAtMs,
    };
    this.#records.set(localIntentId, next);
    return next;
  }

  beginTermination(
    localIntentId: LocalIntentId,
    terminationReason: IntentTerminationReason,
    deadlineAtMs: number,
  ): TerminatingIntentRecord | null {
    const record = this.#records.get(localIntentId);
    if (record === undefined || record.phase === "terminal" || record.identity === null)
      return null;
    if (record.phase !== "sending" && record.phase !== "sent") return null;
    const next: TerminatingIntentRecord = {
      phase: "terminating",
      localIntentId: record.localIntentId,
      kind: record.kind,
      consumedAtMs: record.consumedAtMs,
      identity: record.identity,
      encodedBytes: record.encodedBytes,
      admittedAtMs: record.admittedAtMs,
      transportGeneration: record.transportGeneration,
      sendDecisionAtMs: record.sendDecisionAtMs,
      deadlineAtMs,
      terminationReason,
    };
    this.#records.set(localIntentId, next);
    return next;
  }

  activeRecords(): ActiveIntentRecord[] {
    const records: ActiveIntentRecord[] = [];
    for (const localIntentId of this.#activeByIdentity.values()) {
      const record = this.#records.get(localIntentId);
      if (
        record?.phase === "queued" ||
        record?.phase === "sending" ||
        record?.phase === "sent" ||
        record?.phase === "terminating"
      ) {
        records.push(record);
      } else {
        throw new Error("active identity index references a non-active intent");
      }
    }
    return records;
  }

  getByIdentity(identity: InputIdentity): ActiveIntentRecord | undefined {
    const localIntentId = this.#activeByIdentity.get(identityKey(identity));
    if (localIntentId === undefined) return undefined;
    const record = this.#records.get(localIntentId);
    return record?.phase === "queued" ||
      record?.phase === "sending" ||
      record?.phase === "sent" ||
      record?.phase === "terminating"
      ? record
      : undefined;
  }

  finish(localIntentId: LocalIntentId, result: InputIntentResult): TerminalIntentRecord | null {
    const record = this.#records.get(localIntentId);
    if (record === undefined || record.phase === "terminal") return null;
    if (
      result.localIntentId !== record.localIntentId ||
      result.kind !== record.kind ||
      result.consumedAtMs !== record.consumedAtMs ||
      !sameIdentity(result.identity, record.identity)
    ) {
      throw new Error("terminal result does not match its canonical intent record");
    }
    if (record.phase === "pre-admission") {
      this.#removePreAdmission(localIntentId, record.preAdmissionBytes);
    }
    if (record.phase === "queued") this.#removeQueued(localIntentId, record.encodedBytes);
    if (record.identity !== null) this.#activeByIdentity.delete(identityKey(record.identity));

    const next = terminalRecord(record, result);
    this.#records.set(localIntentId, next);
    this.#terminalOrder.push(localIntentId);
    while (this.#terminalOrder.length > INPUT_QUEUE_CONTRACT.maxRetainedResults) {
      const evicted = this.#terminalOrder.shift();
      if (evicted !== undefined && this.#records.get(evicted)?.phase === "terminal") {
        this.#records.delete(evicted);
      }
    }
    return next;
  }

  earliestDeadline(): number | null {
    let earliest: number | null = null;
    for (const record of this.preAdmissionRecords()) {
      const deadline = record.consumedAtMs + INPUT_QUEUE_CONTRACT.maxPreAdmissionAgeMs;
      earliest = earliest === null ? deadline : Math.min(earliest, deadline);
    }
    for (const record of this.queuedRecords()) {
      earliest = earliest === null ? record.deadlineAtMs : Math.min(earliest, record.deadlineAtMs);
    }
    for (const record of this.activeRecords()) {
      if (record.phase !== "sent" && record.phase !== "terminating") continue;
      earliest = earliest === null ? record.deadlineAtMs : Math.min(earliest, record.deadlineAtMs);
    }
    return earliest;
  }

  get pendingCount(): number {
    return this.#activeByIdentity.size;
  }

  get preAdmissionCount(): number {
    return this.#preAdmissionIds.length;
  }

  get preAdmissionBytes(): number {
    return this.#preAdmissionBytes;
  }

  get queuedCount(): number {
    return this.#queueIds.length;
  }

  get queuedBytes(): number {
    return this.#queueBytes;
  }

  assertInvariants(): void {
    const preAdmissionSet = new Set(this.#preAdmissionIds);
    const queueSet = new Set(this.#queueIds);
    const terminalSet = new Set(this.#terminalOrder);
    if (preAdmissionSet.size !== this.#preAdmissionIds.length) {
      throw new Error("pre-admission index contains duplicate LocalIntentIds");
    }
    if (queueSet.size !== this.#queueIds.length) {
      throw new Error("queue index contains duplicate LocalIntentIds");
    }
    if (terminalSet.size !== this.#terminalOrder.length) {
      throw new Error("terminal retention index contains duplicate LocalIntentIds");
    }

    let preAdmissionBytes = 0;
    let queueBytes = 0;
    let terminalRecords = 0;
    for (const [localIntentId, record] of this.#records) {
      if (record.phase === "consumed") {
        throw new Error(`consumed intent escaped its owner turn: ${localIntentId}`);
      }
      if (record.phase === "pre-admission") {
        if (!preAdmissionSet.has(localIntentId)) {
          throw new Error("pre-admission record is missing from its index");
        }
        preAdmissionBytes += record.preAdmissionBytes;
        continue;
      }
      if (record.phase === "queued") {
        if (!queueSet.has(localIntentId))
          throw new Error("queued record is missing from its index");
        if (this.#activeByIdentity.get(identityKey(record.identity)) !== localIntentId) {
          throw new Error("queued record is missing from the identity index");
        }
        queueBytes += record.encodedBytes;
        continue;
      }
      if (record.phase === "sending" || record.phase === "sent" || record.phase === "terminating") {
        if (record.phase === "sending") {
          throw new Error(`sending intent escaped its sender call: ${localIntentId}`);
        }
        if (queueSet.has(localIntentId) || preAdmissionSet.has(localIntentId)) {
          throw new Error("active non-queued record remains in a queue index");
        }
        if (this.#activeByIdentity.get(identityKey(record.identity)) !== localIntentId) {
          throw new Error("active record is missing from the identity index");
        }
        continue;
      }
      terminalRecords += 1;
      if (!terminalSet.has(localIntentId)) {
        throw new Error("retained terminal record is missing from retention order");
      }
      if (!sameIdentity(record.identity, record.result.identity)) {
        throw new Error("terminal record and result identities disagree");
      }
    }

    for (const [key, localIntentId] of this.#activeByIdentity) {
      const record = this.#records.get(localIntentId);
      if (
        record === undefined ||
        record.identity === null ||
        record.phase === "terminal" ||
        identityKey(record.identity) !== key
      ) {
        throw new Error("identity index contains a stale or inconsistent entry");
      }
    }

    if (terminalRecords !== this.#terminalOrder.length) {
      throw new Error("terminal record count and retention order disagree");
    }
    if (preAdmissionBytes !== this.#preAdmissionBytes || queueBytes !== this.#queueBytes) {
      throw new Error("intent byte ledger does not match canonical records");
    }
    if (this.#preAdmissionIds.length + this.#queueIds.length > INPUT_QUEUE_CONTRACT.maxCount) {
      throw new Error("combined local queue count exceeds its product limit");
    }
    if (this.#preAdmissionBytes + this.#queueBytes > INPUT_QUEUE_CONTRACT.maxBytes) {
      throw new Error("combined local queue bytes exceed their product limit");
    }
    if (this.#activeByIdentity.size > INPUT_QUEUE_CONTRACT.maxPending) {
      throw new Error("pending identity count exceeds its product limit");
    }
    if (this.#terminalOrder.length > INPUT_QUEUE_CONTRACT.maxRetainedResults) {
      throw new Error("terminal result retention exceeds its product limit");
    }
  }

  #removePreAdmission(localIntentId: LocalIntentId, bytes: number): void {
    const index = this.#preAdmissionIds.indexOf(localIntentId);
    if (index < 0) return;
    this.#preAdmissionIds.splice(index, 1);
    this.#preAdmissionBytes -= bytes;
  }

  #removeQueued(localIntentId: LocalIntentId, bytes: number): void {
    const index = this.#queueIds.indexOf(localIntentId);
    if (index < 0) return;
    this.#queueIds.splice(index, 1);
    this.#queueBytes -= bytes;
  }

  #requireRecord(localIntentId: LocalIntentId): IntentRecord {
    const record = this.#records.get(localIntentId);
    if (record === undefined) throw new Error(`unknown LocalIntentId: ${localIntentId}`);
    return record;
  }

  private require<TPhase extends IntentRecord["phase"]>(
    localIntentId: LocalIntentId,
    phase: TPhase,
  ): Extract<IntentRecord, { phase: TPhase }> {
    const record = this.#requireRecord(localIntentId);
    if (record.phase !== phase) {
      throw new Error(`intent ${localIntentId} is ${record.phase}, expected ${phase}`);
    }
    return record as Extract<IntentRecord, { phase: TPhase }>;
  }
}
