import {
  DataFrameFlag,
  DataFrameKind,
  encodeDataFrame,
  encodeResizePayload,
  type ResizePayload,
} from "@zhongduan/protocol";
import { elapsedMs } from "@zhongduan/telemetry";

import type {
  EventJournal,
  JournalCursor,
  JournalMeasuredReplay,
  JournalRangeMeasurement,
  JournalReplay,
} from "./journal";
import type { PtyProcess } from "./pty-process";
import type { SemanticKey, SemanticMouse, TerminalAuthority } from "./terminal-authority";

const MAX_PTY_FRAME_BYTES = 16 * 1024;
const MAX_TEXT_BYTES = 1024 * 1024;
const MAX_U64 = (1n << 64n) - 1n;
const textEncoder = new TextEncoder();
export type SubmittedInputKind = "key" | "text" | "paste" | "focus" | "mouse" | "resize";
export type SubmittedInputEncodeKind = "ghostty" | "utf8" | "resize" | "none";

interface SubmittedInputProbe {
  effectStage: "not-attempted" | "completed" | "threw";
  encodeKind: SubmittedInputEncodeKind;
  inputEncodeMs: number;
  ptyBytes: number;
  ptyResizeAttempted: boolean;
  ptyResizeMs: number;
  ptyWriteAttempted: boolean;
  ptyWriteMs: number;
}

type SessionMessage =
  | { type: "pty-output"; data: Uint8Array }
  | { type: "resize"; dimensions: ResizePayload }
  | { type: "raw-input"; data: Uint8Array }
  | { type: "key"; input: SemanticKey }
  | { type: "paste"; data: string }
  | {
      type: "submitted-input";
      identity: SemanticInputIdentity;
      inputKind: SubmittedInputKind;
      queuedAt: number;
      validate?: () => void;
      commit: (probe: SubmittedInputProbe) => void;
      initialProbe?: Partial<SubmittedInputProbe>;
      resolve: (result: SubmittedInputResult) => void;
    }
  | {
      type: "snapshot";
      queuedAt: number;
      fence?: (snapshot: SnapshotCapture) => void;
      resolve: (snapshot: SnapshotCapture) => void;
      reject: (error: unknown) => void;
    };

export interface TerminalSessionOptions {
  authority: TerminalAuthority;
  journal: EventJournal;
  pty: PtyProcess;
  sessionEpoch: bigint;
  monotonicNow?: () => number;
  inputDedupEntries?: number;
}

export interface SnapshotCaptureTiming {
  actorPauseMs: number;
  authorityEncodeExportMs: number;
  ownershipCopyMs: number;
  queueWaitMs: number;
}

export interface SnapshotCapture {
  bytes: Uint8Array;
  cutEventSeq: bigint;
  /** Legacy alias for timing.actorPauseMs until the final interface simplification. */
  encodeMs: number;
  engineId: string;
  nextPtyOffset: bigint;
  sessionEpoch: bigint;
  timing: SnapshotCaptureTiming;
}

export type PtyExit =
  | { status: "exited"; exitCode: number; signal: number }
  | { status: "failed"; message: string }
  | { status: "disposed" };

export interface SemanticInputIdentity {
  clientId: string;
  clientInputSeq: bigint;
  inputEpoch: string;
  observedEventSeq?: bigint;
  writerFence: bigint;
}

export interface ReplayCursor extends JournalCursor {
  sessionEpoch: bigint;
}

export type ReplayRangeMeasurement =
  | JournalRangeMeasurement
  | { status: "gap"; reason: "epoch-mismatch" };

export interface MeasuredReplay {
  measurement: ReplayRangeMeasurement;
  replay: JournalReplay;
}

export type InputAckStatus = "written" | "duplicate" | "rejected" | "uncertain";

export interface InputAck {
  authorityEventSeq: bigint;
  clientInputSeq: bigint;
  inputEpoch: string;
  status: InputAckStatus;
}

export interface SubmittedInputTiming {
  actorProcessingMs: number;
  actorQueueWaitMs: number;
  effectStage: "not-attempted" | "completed" | "threw";
  encodeKind: SubmittedInputEncodeKind;
  inputEncodeMs: number;
  inputKind: SubmittedInputKind;
  ptyBytes: number;
  ptyResizeAttempted: boolean;
  ptyResizeMs: number;
  ptyWriteAttempted: boolean;
  ptyWriteMs: number;
}

export interface SubmittedInputResult extends InputAck {
  timing: SubmittedInputTiming;
}

export type SubscriptionResult =
  | { status: "attached"; unsubscribe: () => void }
  | { status: "gap" };

interface InputWriterState {
  clientId: string;
  highWater: bigint;
  inputEpoch: string;
}

export class TerminalSession {
  readonly #authority: TerminalAuthority;
  readonly #journal: EventJournal;
  readonly #pty: PtyProcess;
  readonly #sessionEpoch: bigint;
  readonly #monotonicNow: () => number;
  readonly #queue: SessionMessage[] = [];
  readonly #subscribers = new Set<(frame: Uint8Array) => void>();
  readonly #disposePtyData: () => void;
  readonly #disposePtyExit: () => void;
  readonly #exitPromise: Promise<PtyExit>;
  readonly #resolveExit: (exit: PtyExit) => void;
  readonly #inputDedupEntries: number;
  readonly #inputResults = new Map<string, "written" | "uncertain">();

  #draining = false;
  #disposed = false;
  #authorityDisposed = false;
  #exitSettled = false;
  #failure: Error | null = null;
  #ptyExited = false;
  #eventSeq = 0n;
  #inputFence = 0n;
  #inputWriter: InputWriterState | undefined;
  #nextPtyOffset = 0n;

  constructor(options: TerminalSessionOptions) {
    this.#authority = options.authority;
    this.#journal = options.journal;
    this.#pty = options.pty;
    this.#sessionEpoch = options.sessionEpoch;
    if (this.#sessionEpoch <= 0n || this.#sessionEpoch > MAX_U64) {
      throw new RangeError("sessionEpoch must be a non-zero u64");
    }
    this.#inputDedupEntries = options.inputDedupEntries ?? 4_096;
    if (!Number.isInteger(this.#inputDedupEntries) || this.#inputDedupEntries <= 0) {
      throw new RangeError("inputDedupEntries must be a positive integer");
    }
    this.#monotonicNow = options.monotonicNow ?? performance.now.bind(performance);
    let resolveExitPromise!: (exit: PtyExit) => void;
    this.#exitPromise = new Promise((resolve) => {
      resolveExitPromise = resolve;
    });
    this.#resolveExit = (exit) => {
      if (this.#exitSettled) return;
      this.#exitSettled = true;
      resolveExitPromise(exit);
    };
    this.#disposePtyExit = this.#pty.onExit((exitCode, signal) => {
      this.#ptyExited = true;
      this.#resolveExit({ status: "exited", exitCode, signal });
    });
    this.#disposePtyData = this.#pty.onData((data) => {
      try {
        for (let start = 0; start < data.byteLength; start += MAX_PTY_FRAME_BYTES) {
          this.#enqueue({
            type: "pty-output",
            data: data.slice(start, start + MAX_PTY_FRAME_BYTES),
          });
        }
      } catch {
        // The actor has already failed closed; the failure is observable via waitForExit().
      }
    });
  }

  get eventSeq(): bigint {
    return this.#eventSeq;
  }

  get nextPtyOffset(): bigint {
    return this.#nextPtyOffset;
  }

  get engineId(): string {
    return this.#authority.engineId;
  }

  get sessionEpoch(): bigint {
    return this.#sessionEpoch;
  }

  get cursor(): ReplayCursor {
    return {
      sessionEpoch: this.#sessionEpoch,
      lastEventSeq: this.#eventSeq,
      nextPtyOffset: this.#nextPtyOffset,
    };
  }

  replayThrough(base: ReplayCursor, commit: ReplayCursor): JournalReplay {
    if (base.sessionEpoch !== this.#sessionEpoch || commit.sessionEpoch !== this.#sessionEpoch) {
      return { status: "gap" };
    }
    return this.#journal.replayThrough(base, commit);
  }

  measureReplayThrough(base: ReplayCursor, commit: ReplayCursor): ReplayRangeMeasurement {
    if (base.sessionEpoch !== this.#sessionEpoch || commit.sessionEpoch !== this.#sessionEpoch) {
      return { status: "gap", reason: "epoch-mismatch" };
    }
    return this.#journal.measureRange(base, commit);
  }

  replayAndMeasureThrough(base: ReplayCursor, commit: ReplayCursor): MeasuredReplay {
    if (base.sessionEpoch !== this.#sessionEpoch || commit.sessionEpoch !== this.#sessionEpoch) {
      return {
        measurement: { status: "gap", reason: "epoch-mismatch" },
        replay: { status: "gap" },
      };
    }
    const result: JournalMeasuredReplay = this.#journal.replayAndMeasureThrough(base, commit);
    return result;
  }

  subscribe(subscriber: (frame: Uint8Array) => void, cursor?: ReplayCursor): SubscriptionResult {
    if (cursor !== undefined && cursor.sessionEpoch !== this.#sessionEpoch) {
      return { status: "gap" };
    }
    const replay = cursor === undefined ? undefined : this.#journal.replayFrom(cursor);
    if (replay?.status === "gap") return { status: "gap" };

    let replaying = replay !== undefined;
    let active = true;
    const pendingLive: Uint8Array[] = [];
    const wrapped = (frame: Uint8Array) => {
      if (!active) return;
      if (replaying) {
        pendingLive.push(frame.slice());
      } else {
        subscriber(frame);
      }
    };
    this.#subscribers.add(wrapped);

    try {
      for (const frame of replay?.frames ?? []) subscriber(frame);
      for (let index = 0; index < pendingLive.length; index += 1) {
        subscriber(pendingLive[index]!);
      }
      pendingLive.length = 0;
      replaying = false;
    } catch (error) {
      active = false;
      this.#subscribers.delete(wrapped);
      throw error;
    }

    return {
      status: "attached",
      unsubscribe: () => {
        active = false;
        this.#subscribers.delete(wrapped);
      },
    };
  }

  resize(dimensions: ResizePayload): void {
    this.#assertPtyWritable();
    this.#enqueue({ type: "resize", dimensions: { ...dimensions } });
  }

  writeKey(input: SemanticKey): void {
    this.#assertPtyWritable();
    this.#enqueue({ type: "key", input: { ...input } });
  }

  writePaste(data: string): void {
    this.#assertPtyWritable();
    this.#enqueue({ type: "paste", data });
  }

  writeRawInput(data: Uint8Array): void {
    this.#assertPtyWritable();
    for (let start = 0; start < data.byteLength; start += MAX_PTY_FRAME_BYTES) {
      this.#enqueue({ type: "raw-input", data: data.slice(start, start + MAX_PTY_FRAME_BYTES) });
    }
  }

  submitKey(identity: SemanticInputIdentity, input: SemanticKey): Promise<SubmittedInputResult> {
    const captured = { ...input };
    return this.#submitInput(identity, "key", (probe) =>
      this.#encodeAndWriteSubmittedInput(probe, "ghostty", () =>
        this.#authority.encodeKey(captured),
      ),
    );
  }

  submitPaste(identity: SemanticInputIdentity, data: string): Promise<SubmittedInputResult> {
    return this.#submitInput(identity, "paste", (probe) =>
      this.#encodeAndWriteSubmittedInput(probe, "ghostty", () => this.#authority.encodePaste(data)),
    );
  }

  submitText(identity: SemanticInputIdentity, data: string): Promise<SubmittedInputResult> {
    const encodingStartedAt = this.#monotonicNow();
    const encoded = textEncoder.encode(data);
    const inputEncodeMs = elapsedMs(encodingStartedAt, this.#monotonicNow());
    return this.#submitInput(
      identity,
      "text",
      (probe) => this.#writeSubmittedInput(probe, encoded),
      () => {
        if (encoded.byteLength > MAX_TEXT_BYTES) {
          throw new RangeError(`text exceeds ${MAX_TEXT_BYTES} bytes`);
        }
      },
      { encodeKind: "utf8", inputEncodeMs },
    );
  }

  submitFocus(identity: SemanticInputIdentity, focused: boolean): Promise<SubmittedInputResult> {
    return this.#submitInput(identity, "focus", (probe) =>
      this.#encodeAndWriteSubmittedInput(probe, "ghostty", () =>
        this.#authority.encodeFocus(focused),
      ),
    );
  }

  submitMouse(
    identity: SemanticInputIdentity,
    mouse: SemanticMouse,
  ): Promise<SubmittedInputResult> {
    const captured = cloneMouse(mouse);
    return this.#submitInput(
      identity,
      "mouse",
      (probe) =>
        this.#encodeAndWriteSubmittedInput(probe, "ghostty", () =>
          this.#authority.encodeMouse(captured),
        ),
      () => this.#authority.validateMouse(captured),
    );
  }

  submitResize(
    identity: SemanticInputIdentity,
    dimensions: ResizePayload,
  ): Promise<SubmittedInputResult> {
    const captured = { ...dimensions };
    return this.#submitInput(identity, "resize", (probe) =>
      this.#commitSubmittedResize(captured, probe),
    );
  }

  captureSnapshot(): Promise<SnapshotCapture> {
    return new Promise((resolve, reject) => {
      this.#enqueue({ type: "snapshot", queuedAt: this.#monotonicNow(), resolve, reject });
    });
  }

  captureSnapshotWithFence(fence: (snapshot: SnapshotCapture) => void): Promise<SnapshotCapture> {
    return new Promise((resolve, reject) => {
      this.#enqueue({
        type: "snapshot",
        queuedAt: this.#monotonicNow(),
        fence,
        resolve,
        reject,
      });
    });
  }

  waitForExit(): Promise<PtyExit> {
    return this.#exitPromise;
  }

  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.#rejectQueued(new Error("terminal session is disposed"));
    this.#resolveExit({ status: "disposed" });
    this.#bestEffort(this.#disposePtyData);
    this.#bestEffort(this.#disposePtyExit);
    this.#subscribers.clear();
    this.#disposeAuthority();
    this.#bestEffort(() => this.#pty.kill());
  }

  #enqueue(message: SessionMessage): void {
    if (this.#disposed) throw new Error("terminal session is disposed");
    if (this.#failure !== null)
      throw new Error("terminal session failed", { cause: this.#failure });
    this.#queue.push(message);
    this.#drainQueue();
  }

  #drainQueue(): void {
    if (this.#draining || this.#disposed || this.#failure !== null) return;

    this.#draining = true;
    try {
      let next: SessionMessage | undefined;
      while ((next = this.#queue.shift()) !== undefined) {
        switch (next.type) {
          case "pty-output":
            this.#commitPtyOutput(next.data);
            break;
          case "resize":
            this.#commitResize(next.dimensions);
            break;
          case "raw-input":
            this.#writePty(next.data);
            break;
          case "key":
            this.#writePty(this.#authority.encodeKey(next.input));
            break;
          case "paste":
            this.#writePty(this.#authority.encodePaste(next.data));
            break;
          case "submitted-input":
            next.resolve(this.#measureSubmittedInput(next));
            break;
          case "snapshot":
            try {
              const snapshot = this.#captureSnapshot(next.queuedAt);
              next.fence?.(snapshot);
              next.resolve(snapshot);
            } catch (error) {
              next.reject(error);
              throw error;
            }
            break;
        }
      }
    } catch (error) {
      this.#fail(error);
      throw error;
    } finally {
      this.#draining = false;
    }
  }

  #commitPtyOutput(data: Uint8Array): void {
    for (let start = 0; start < data.byteLength; start += MAX_PTY_FRAME_BYTES) {
      const chunk = data.subarray(start, start + MAX_PTY_FRAME_BYTES);
      const replies = this.#authority.applyOutput(chunk);
      const frame = encodeDataFrame({
        kind: DataFrameKind.PtyOutput,
        flags: DataFrameFlag.None,
        sessionEpoch: this.#sessionEpoch,
        deliveryGeneration: 0n,
        eventSeq: ++this.#eventSeq,
        ptyOffset: this.#nextPtyOffset,
        streamId: 0,
        payload: chunk,
      });
      this.#nextPtyOffset += BigInt(chunk.byteLength);
      this.#publish(frame);
      for (const reply of replies) this.#writePty(reply);
    }
  }

  #submitInput(
    identity: SemanticInputIdentity,
    inputKind: SubmittedInputKind,
    commit: (probe: SubmittedInputProbe) => void,
    validate?: () => void,
    initialProbe?: Partial<SubmittedInputProbe>,
  ): Promise<SubmittedInputResult> {
    if (this.#disposed || this.#failure !== null || this.#ptyExited) {
      return Promise.resolve({
        ...this.#inputAck(identity, "rejected"),
        timing: this.#submittedInputTiming(inputKind, this.#newSubmittedInputProbe(initialProbe)),
      });
    }
    return new Promise((resolve) => {
      this.#enqueue({
        type: "submitted-input",
        identity: { ...identity },
        inputKind,
        queuedAt: this.#monotonicNow(),
        commit,
        ...(initialProbe === undefined ? {} : { initialProbe }),
        ...(validate === undefined ? {} : { validate }),
        resolve,
      });
    });
  }

  #measureSubmittedInput(
    message: Extract<SessionMessage, { type: "submitted-input" }>,
  ): SubmittedInputResult {
    const actorStartedAt = this.#monotonicNow();
    const probe = this.#newSubmittedInputProbe(message.initialProbe);
    const ack = this.#commitSubmittedInput(
      message.identity,
      () => message.commit(probe),
      probe,
      message.validate,
    );
    const actorFinishedAt = this.#monotonicNow();
    return {
      ...ack,
      timing: {
        ...this.#submittedInputTiming(message.inputKind, probe),
        actorQueueWaitMs: elapsedMs(message.queuedAt, actorStartedAt),
        actorProcessingMs: elapsedMs(actorStartedAt, actorFinishedAt),
      },
    };
  }

  #commitSubmittedInput(
    identity: SemanticInputIdentity,
    commit: () => void,
    probe: SubmittedInputProbe,
    validate?: () => void,
  ): InputAck {
    const ack = (status: InputAckStatus): InputAck => this.#inputAck(identity, status);
    if (this.#ptyExited || this.#failure !== null) return ack("rejected");
    if (identity.writerFence <= 0n || identity.writerFence > MAX_U64) {
      return ack("rejected");
    }
    if (identity.writerFence < this.#inputFence) return ack("rejected");
    if (identity.writerFence > this.#inputFence) {
      this.#inputFence = identity.writerFence;
      this.#inputWriter = undefined;
      this.#inputResults.clear();
    }

    if (
      identity.clientId.length === 0 ||
      identity.clientId.length > 128 ||
      identity.inputEpoch.length === 0 ||
      identity.inputEpoch.length > 128 ||
      identity.clientInputSeq <= 0n
    ) {
      return ack("rejected");
    }

    const currentWriter = this.#inputWriter;
    if (currentWriter !== undefined) {
      if (
        currentWriter.clientId !== identity.clientId ||
        currentWriter.inputEpoch !== identity.inputEpoch
      ) {
        return ack("rejected");
      }
    } else if (identity.clientInputSeq !== 1n) {
      return ack("rejected");
    }

    const key = identity.clientInputSeq.toString();
    if (currentWriter !== undefined) {
      const previous = this.#inputResults.get(key);
      if (previous !== undefined) {
        return ack(previous === "written" ? "duplicate" : "uncertain");
      }
      if (identity.clientInputSeq <= currentWriter.highWater) return ack("uncertain");
    }
    if (identity.observedEventSeq !== undefined && identity.observedEventSeq > this.#eventSeq) {
      return ack("rejected");
    }

    try {
      validate?.();
    } catch {
      return ack("rejected");
    }

    this.#inputWriter ??= {
      clientId: identity.clientId,
      highWater: 0n,
      inputEpoch: identity.inputEpoch,
    };

    try {
      probe.effectStage = "threw";
      commit();
      probe.effectStage = "completed";
      this.#rememberInput(key, identity.clientInputSeq, "written");
      return ack("written");
    } catch (error) {
      this.#rememberInput(key, identity.clientInputSeq, "uncertain");
      this.#fail(error);
      return ack("uncertain");
    }
  }

  #newSubmittedInputProbe(initial: Partial<SubmittedInputProbe> | undefined): SubmittedInputProbe {
    return {
      effectStage: initial?.effectStage ?? "not-attempted",
      encodeKind: initial?.encodeKind ?? "none",
      inputEncodeMs: initial?.inputEncodeMs ?? 0,
      ptyBytes: initial?.ptyBytes ?? 0,
      ptyResizeAttempted: initial?.ptyResizeAttempted ?? false,
      ptyResizeMs: initial?.ptyResizeMs ?? 0,
      ptyWriteAttempted: initial?.ptyWriteAttempted ?? false,
      ptyWriteMs: initial?.ptyWriteMs ?? 0,
    };
  }

  #submittedInputTiming(
    inputKind: SubmittedInputKind,
    probe: SubmittedInputProbe,
  ): SubmittedInputTiming {
    return {
      actorProcessingMs: 0,
      actorQueueWaitMs: 0,
      effectStage: probe.effectStage,
      encodeKind: probe.encodeKind,
      inputEncodeMs: probe.inputEncodeMs,
      inputKind,
      ptyBytes: probe.ptyBytes,
      ptyResizeAttempted: probe.ptyResizeAttempted,
      ptyResizeMs: probe.ptyResizeMs,
      ptyWriteAttempted: probe.ptyWriteAttempted,
      ptyWriteMs: probe.ptyWriteMs,
    };
  }

  #encodeAndWriteSubmittedInput(
    probe: SubmittedInputProbe,
    encodeKind: Exclude<SubmittedInputEncodeKind, "none" | "resize">,
    encode: () => Uint8Array,
  ): void {
    probe.encodeKind = encodeKind;
    const encodeStartedAt = this.#monotonicNow();
    let bytes: Uint8Array;
    try {
      bytes = encode();
    } finally {
      probe.inputEncodeMs += elapsedMs(encodeStartedAt, this.#monotonicNow());
    }
    this.#writeSubmittedInput(probe, bytes);
  }

  #writeSubmittedInput(probe: SubmittedInputProbe, data: Uint8Array): void {
    probe.ptyBytes += data.byteLength;
    if (data.byteLength === 0) return;
    probe.ptyWriteAttempted = true;
    const writeStartedAt = this.#monotonicNow();
    try {
      this.#pty.write(data);
    } finally {
      probe.ptyWriteMs += elapsedMs(writeStartedAt, this.#monotonicNow());
    }
  }

  #commitSubmittedResize(dimensions: ResizePayload, probe: SubmittedInputProbe): void {
    probe.encodeKind = "resize";
    const authorityStartedAt = this.#monotonicNow();
    let payload: Uint8Array;
    let replies: readonly Uint8Array[];
    try {
      payload = encodeResizePayload(dimensions);
      replies = this.#authority.resize(dimensions);
    } finally {
      probe.inputEncodeMs += elapsedMs(authorityStartedAt, this.#monotonicNow());
    }

    probe.ptyResizeAttempted = true;
    const resizeStartedAt = this.#monotonicNow();
    try {
      this.#pty.resize(dimensions);
    } finally {
      probe.ptyResizeMs += elapsedMs(resizeStartedAt, this.#monotonicNow());
    }
    const frame = encodeDataFrame({
      kind: DataFrameKind.ResizeApplied,
      flags: DataFrameFlag.None,
      sessionEpoch: this.#sessionEpoch,
      deliveryGeneration: 0n,
      eventSeq: ++this.#eventSeq,
      ptyOffset: this.#nextPtyOffset,
      streamId: 0,
      payload,
    });
    this.#publish(frame);
    for (const reply of replies) this.#writeSubmittedInput(probe, reply);
  }

  #rememberInput(key: string, clientInputSeq: bigint, result: "written" | "uncertain"): void {
    this.#inputResults.set(key, result);
    if (clientInputSeq > this.#inputWriter!.highWater) {
      this.#inputWriter!.highWater = clientInputSeq;
    }
    while (this.#inputResults.size > this.#inputDedupEntries) {
      const oldest = this.#inputResults.keys().next().value;
      if (oldest === undefined) break;
      this.#inputResults.delete(oldest);
    }
  }

  #commitResize(dimensions: ResizePayload): void {
    const payload = encodeResizePayload(dimensions);
    const replies = this.#authority.resize(dimensions);
    this.#pty.resize(dimensions);
    const frame = encodeDataFrame({
      kind: DataFrameKind.ResizeApplied,
      flags: DataFrameFlag.None,
      sessionEpoch: this.#sessionEpoch,
      deliveryGeneration: 0n,
      eventSeq: ++this.#eventSeq,
      ptyOffset: this.#nextPtyOffset,
      streamId: 0,
      payload,
    });
    this.#publish(frame);
    for (const reply of replies) this.#writePty(reply);
  }

  #captureSnapshot(queuedAt: number): SnapshotCapture {
    const cutEventSeq = this.#eventSeq;
    const nextPtyOffset = this.#nextPtyOffset;
    const actorStartedAt = this.#monotonicNow();
    const authorityStartedAt = this.#monotonicNow();
    const encoded = this.#authority.encodeSnapshot();
    const authorityFinishedAt = this.#monotonicNow();
    const bytes = encoded.slice();
    const actorFinishedAt = this.#monotonicNow();
    if (bytes.byteLength === 0) throw new Error("terminal authority returned an empty snapshot");
    const timing = {
      actorPauseMs: elapsedMs(actorStartedAt, actorFinishedAt),
      authorityEncodeExportMs: elapsedMs(authorityStartedAt, authorityFinishedAt),
      ownershipCopyMs: elapsedMs(authorityFinishedAt, actorFinishedAt),
      queueWaitMs: elapsedMs(queuedAt, actorStartedAt),
    };
    return {
      bytes,
      cutEventSeq,
      encodeMs: timing.actorPauseMs,
      engineId: this.#authority.engineId,
      nextPtyOffset,
      sessionEpoch: this.#sessionEpoch,
      timing,
    };
  }

  #publish(frame: Uint8Array): void {
    this.#journal.append(frame);
    const subscribers = Array.from(this.#subscribers);
    for (const subscriber of subscribers) {
      try {
        subscriber(frame.slice());
      } catch {
        this.#subscribers.delete(subscriber);
      }
    }
  }

  #inputAck(identity: SemanticInputIdentity, status: InputAckStatus): InputAck {
    return {
      authorityEventSeq: this.#eventSeq,
      clientInputSeq: identity.clientInputSeq,
      inputEpoch: identity.inputEpoch,
      status,
    };
  }

  #writePty(data: Uint8Array): void {
    if (data.byteLength !== 0) this.#pty.write(data);
  }

  #assertPtyWritable(): void {
    if (this.#ptyExited) throw new Error("terminal PTY has exited");
    if (this.#failure !== null)
      throw new Error("terminal session failed", { cause: this.#failure });
    if (this.#disposed) throw new Error("terminal session is disposed");
  }

  #fail(cause: unknown): void {
    if (this.#failure !== null || this.#disposed) return;
    this.#failure = cause instanceof Error ? cause : new Error(String(cause));
    this.#rejectQueued(this.#failure);
    this.#resolveExit({ status: "failed", message: this.#failure.message });
    this.#bestEffort(this.#disposePtyData);
    this.#bestEffort(this.#disposePtyExit);
    this.#subscribers.clear();
    this.#disposeAuthority();
    this.#bestEffort(() => this.#pty.kill());
  }

  #rejectQueued(error: Error): void {
    let queued: SessionMessage | undefined;
    while ((queued = this.#queue.shift()) !== undefined) {
      if (queued.type === "snapshot") queued.reject(error);
      if (queued.type === "submitted-input") {
        queued.resolve({
          ...this.#inputAck(queued.identity, "rejected"),
          timing: {
            ...this.#submittedInputTiming(
              queued.inputKind,
              this.#newSubmittedInputProbe(queued.initialProbe),
            ),
            actorQueueWaitMs: elapsedMs(queued.queuedAt, this.#monotonicNow()),
          },
        });
      }
    }
  }

  #disposeAuthority(): void {
    if (this.#authorityDisposed) return;
    this.#authorityDisposed = true;
    this.#bestEffort(() => this.#authority.dispose());
  }

  #bestEffort(action: () => void): void {
    try {
      action();
    } catch {
      // Preserve the first actor failure while still releasing independent resources.
    }
  }
}

function cloneMouse(mouse: SemanticMouse): SemanticMouse {
  return {
    ...mouse,
    surface: { ...mouse.surface },
  };
}
