import { DATA_HEADER_BYTES, DataFrameKind, decodeDataFrame } from "@zhongduan/protocol";

import type { ReplayCursor, TerminalSession } from "../session";

export const HOST_CANONICAL_QUEUE_LIMITS = {
  maxBytes: 8 * 1024 * 1024,
  maxFrames: 1024,
} as const;

const PUMP_YIELD_BYTES = 256 * 1024;
const PUMP_YIELD_FRAMES = 64;
const MAX_CANONICAL_FRAME_BYTES = DATA_HEADER_BYTES + 16 * 1024;

interface QueuedCanonicalFrame {
  bytes: Uint8Array;
  cursor: ReplayCursor;
}

export interface CanonicalPublisherOptions {
  canInterruptRecovery: () => boolean;
  onFailure: (reason: string) => void;
  onIngress: () => void;
  onRecoveryPressure: () => void;
  sendData: (frame: Uint8Array) => void;
  session: TerminalSession;
  yieldIo: () => Promise<void>;
}

export class CanonicalPublisher {
  readonly #canInterruptRecovery: () => boolean;
  readonly #onFailure: (reason: string) => void;
  readonly #onIngress: () => void;
  readonly #onRecoveryPressure: () => void;
  readonly #queue: QueuedCanonicalFrame[] = [];
  readonly #sendData: (frame: Uint8Array) => void;
  readonly #session: TerminalSession;
  readonly #yieldIo: () => Promise<void>;

  #lastQueuedCursor: ReplayCursor | undefined;
  #lastSentCursor: ReplayCursor | undefined;
  #paused = false;
  #pumpPromise: Promise<void> | undefined;
  #queueBytes = 0;
  #ready = false;
  #stopped = false;
  #unsubscribe: (() => void) | undefined;

  constructor(options: CanonicalPublisherOptions) {
    this.#canInterruptRecovery = options.canInterruptRecovery;
    this.#onFailure = options.onFailure;
    this.#onIngress = options.onIngress;
    this.#onRecoveryPressure = options.onRecoveryPressure;
    this.#sendData = options.sendData;
    this.#session = options.session;
    this.#yieldIo = options.yieldIo;
  }

  prepare(): ReplayCursor {
    if (this.#unsubscribe !== undefined) throw new Error("canonical publisher is already prepared");
    const baseline = this.#session.cursor;
    this.#lastSentCursor = baseline;
    this.#lastQueuedCursor = baseline;
    const subscription = this.#session.subscribe((frame) => this.#enqueue(frame), baseline);
    if (subscription.status !== "attached") {
      throw new Error("current terminal cursor is not replayable");
    }
    this.#unsubscribe = subscription.unsubscribe;
    return baseline;
  }

  activate(acknowledged: ReplayCursor): void {
    const baseline = this.#lastSentCursor;
    if (baseline === undefined || !sameCursor(baseline, acknowledged)) {
      throw new Error("host-ready ACK does not match the advertised cursor");
    }
    this.#ready = true;
    this.#schedulePump();
  }

  async pause(): Promise<void> {
    this.#paused = true;
    await this.#pumpPromise;
  }

  resume(): void {
    if (this.#stopped) return;
    this.#paused = false;
    this.#schedulePump();
  }

  async flushThrough(commit: ReplayCursor): Promise<void> {
    let bytesSinceYield = 0;
    let framesSinceYield = 0;
    while (!this.#stopped) {
      const queued = this.#queue[0];
      if (queued === undefined || queued.cursor.lastEventSeq > commit.lastEventSeq) break;
      this.#queue.shift();
      this.#queueBytes -= queued.bytes.byteLength;
      this.#sendData(queued.bytes);
      this.#lastSentCursor = queued.cursor;
      bytesSinceYield += queued.bytes.byteLength;
      framesSinceYield += 1;
      if (bytesSinceYield >= PUMP_YIELD_BYTES || framesSinceYield >= PUMP_YIELD_FRAMES) {
        bytesSinceYield = 0;
        framesSinceYield = 0;
        await this.#yieldIo();
      }
    }
    if (this.#queue.length === 0 && this.#lastSentCursor !== undefined) {
      this.#lastQueuedCursor = this.#lastSentCursor;
    }
    if (this.#lastSentCursor === undefined || !sameCursor(this.#lastSentCursor, commit)) {
      throw new Error("canonical publisher cannot reach the requested commit");
    }
  }

  dispose(): void {
    if (this.#stopped) return;
    this.#stopped = true;
    this.#ready = false;
    this.#unsubscribe?.();
    this.#unsubscribe = undefined;
    this.#queue.length = 0;
    this.#queueBytes = 0;
  }

  #enqueue(encoded: Uint8Array): void {
    if (this.#stopped) return;
    let frame;
    try {
      frame = decodeDataFrame(encoded);
      if (
        (frame.kind !== DataFrameKind.PtyOutput && frame.kind !== DataFrameKind.ResizeApplied) ||
        frame.deliveryGeneration !== 0n ||
        frame.streamId !== 0 ||
        this.#lastQueuedCursor === undefined ||
        frame.eventSeq !== this.#lastQueuedCursor.lastEventSeq + 1n ||
        frame.ptyOffset !== this.#lastQueuedCursor.nextPtyOffset
      ) {
        throw new Error("canonical publisher received a discontinuous frame");
      }
    } catch (error) {
      this.#fail(error instanceof Error ? error.message : "invalid canonical frame");
      return;
    }
    const exceedsSoftLimit =
      this.#queue.length + 1 > HOST_CANONICAL_QUEUE_LIMITS.maxFrames ||
      this.#queueBytes + encoded.byteLength > HOST_CANONICAL_QUEUE_LIMITS.maxBytes;
    const withinPressureFrame =
      this.#queue.length + 1 <= HOST_CANONICAL_QUEUE_LIMITS.maxFrames + 1 &&
      this.#queueBytes + encoded.byteLength <=
        HOST_CANONICAL_QUEUE_LIMITS.maxBytes + MAX_CANONICAL_FRAME_BYTES;
    if (exceedsSoftLimit && (!withinPressureFrame || !this.#canInterruptRecovery())) {
      this.#fail("canonical publisher queue exceeded");
      return;
    }
    const cursor = cursorAfter(frame);
    const bytes = encoded.slice();
    this.#queue.push({ bytes, cursor });
    this.#queueBytes += bytes.byteLength;
    this.#lastQueuedCursor = cursor;
    this.#onIngress();
    if (exceedsSoftLimit) this.#onRecoveryPressure();
    this.#schedulePump();
  }

  #schedulePump(): void {
    if (
      this.#stopped ||
      !this.#ready ||
      this.#paused ||
      this.#pumpPromise !== undefined ||
      this.#queue.length === 0
    ) {
      return;
    }
    const running = this.#pump();
    this.#pumpPromise = running;
    void running.then(
      () => {
        if (this.#pumpPromise === running) this.#pumpPromise = undefined;
        this.#schedulePump();
      },
      (error: unknown) => {
        if (this.#pumpPromise === running) this.#pumpPromise = undefined;
        this.#fail(error instanceof Error ? error.message : "canonical publisher failed");
      },
    );
  }

  async #pump(): Promise<void> {
    let bytesSinceYield = 0;
    let framesSinceYield = 0;
    while (!this.#stopped && this.#ready && !this.#paused) {
      const queued = this.#queue.shift();
      if (queued === undefined) return;
      this.#queueBytes -= queued.bytes.byteLength;
      this.#sendData(queued.bytes);
      this.#lastSentCursor = queued.cursor;
      if (this.#queue.length === 0) this.#lastQueuedCursor = queued.cursor;
      bytesSinceYield += queued.bytes.byteLength;
      framesSinceYield += 1;
      if (bytesSinceYield >= PUMP_YIELD_BYTES || framesSinceYield >= PUMP_YIELD_FRAMES) {
        bytesSinceYield = 0;
        framesSinceYield = 0;
        await this.#yieldIo();
      }
    }
  }

  #fail(reason: string): void {
    if (this.#stopped) return;
    this.dispose();
    this.#onFailure(reason);
  }
}

function cursorAfter(frame: ReturnType<typeof decodeDataFrame>): ReplayCursor {
  return {
    sessionEpoch: frame.sessionEpoch,
    lastEventSeq: frame.eventSeq,
    nextPtyOffset:
      frame.kind === DataFrameKind.PtyOutput
        ? frame.ptyOffset + BigInt(frame.payload.byteLength)
        : frame.ptyOffset,
  };
}

function sameCursor(left: ReplayCursor, right: ReplayCursor): boolean {
  return (
    left.sessionEpoch === right.sessionEpoch &&
    left.lastEventSeq === right.lastEventSeq &&
    left.nextPtyOffset === right.nextPtyOffset
  );
}
