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

type SessionMessage =
  | { type: "pty-output"; data: Uint8Array }
  | { type: "resize"; dimensions: ResizePayload }
  | { type: "raw-input"; data: Uint8Array }
  | { type: "key"; input: SemanticKey }
  | { type: "paste"; data: string }
  | {
      type: "submitted-input";
      identity: SemanticInputIdentity;
      validate?: () => void;
      commit: () => void;
      resolve: (ack: InputAck) => void;
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

  submitKey(identity: SemanticInputIdentity, input: SemanticKey): Promise<InputAck> {
    const captured = { ...input };
    return this.#submitInput(identity, () => {
      const bytes = this.#authority.encodeKey(captured);
      this.#writePty(bytes);
    });
  }

  submitPaste(identity: SemanticInputIdentity, data: string): Promise<InputAck> {
    return this.#submitInput(identity, () => {
      const bytes = this.#authority.encodePaste(data);
      this.#writePty(bytes);
    });
  }

  submitText(identity: SemanticInputIdentity, data: string): Promise<InputAck> {
    const encoded = textEncoder.encode(data);
    return this.#submitInput(
      identity,
      () => this.#writePty(encoded),
      () => {
        if (encoded.byteLength > MAX_TEXT_BYTES) {
          throw new RangeError(`text exceeds ${MAX_TEXT_BYTES} bytes`);
        }
      },
    );
  }

  submitFocus(identity: SemanticInputIdentity, focused: boolean): Promise<InputAck> {
    return this.#submitInput(identity, () => {
      const bytes = this.#authority.encodeFocus(focused);
      this.#writePty(bytes);
    });
  }

  submitMouse(identity: SemanticInputIdentity, mouse: SemanticMouse): Promise<InputAck> {
    const captured = cloneMouse(mouse);
    return this.#submitInput(
      identity,
      () => {
        const bytes = this.#authority.encodeMouse(captured);
        this.#writePty(bytes);
      },
      () => this.#authority.validateMouse(captured),
    );
  }

  submitResize(identity: SemanticInputIdentity, dimensions: ResizePayload): Promise<InputAck> {
    const captured = { ...dimensions };
    return this.#submitInput(identity, () => this.#commitResize(captured));
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
            next.resolve(this.#commitSubmittedInput(next.identity, next.commit, next.validate));
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
    commit: () => void,
    validate?: () => void,
  ): Promise<InputAck> {
    if (this.#disposed || this.#failure !== null || this.#ptyExited) {
      return Promise.resolve(this.#inputAck(identity, "rejected"));
    }
    return new Promise((resolve) => {
      this.#enqueue({
        type: "submitted-input",
        identity: { ...identity },
        commit,
        ...(validate === undefined ? {} : { validate }),
        resolve,
      });
    });
  }

  #commitSubmittedInput(
    identity: SemanticInputIdentity,
    commit: () => void,
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
      commit();
      this.#rememberInput(key, identity.clientInputSeq, "written");
      return ack("written");
    } catch (error) {
      this.#rememberInput(key, identity.clientInputSeq, "uncertain");
      this.#fail(error);
      return ack("uncertain");
    }
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
        queued.resolve(this.#inputAck(queued.identity, "rejected"));
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
