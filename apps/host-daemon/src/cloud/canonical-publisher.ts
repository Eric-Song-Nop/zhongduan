import { DataFrameKind, decodeDataFrame, decodeRecoveryStartFence } from "@zhongduan/protocol";

import type { ReplayCursor, TerminalSession } from "../session";

export const HOST_CANONICAL_QUEUE_LIMITS = {
  maxBytes: 8 * 1024 * 1024,
  maxFrames: 1024,
} as const;

const PUMP_YIELD_BYTES = 256 * 1024;
const PUMP_YIELD_FRAMES = 64;

interface QueuedCanonicalMutation {
  type: "mutation";
  bytes: Uint8Array;
  cursor: ReplayCursor;
}

interface QueuedOrderedMarker {
  type: "ordered-marker";
  bytes: Uint8Array;
  cursor: ReplayCursor;
}

type QueuedCanonicalItem = QueuedCanonicalMutation | QueuedOrderedMarker;

export interface CanonicalPublisherOptions {
  onFailure: (reason: string) => void;
  sendData: (frame: Uint8Array) => void;
  session: TerminalSession;
  yieldIo?: () => Promise<void>;
}

export class CanonicalPublisher {
  readonly #onFailure: (reason: string) => void;
  readonly #queue: QueuedCanonicalItem[] = [];
  readonly #sendData: (frame: Uint8Array) => void;
  readonly #session: TerminalSession;
  readonly #yieldIo: () => Promise<void>;

  #lastQueuedCursor: ReplayCursor | undefined;
  #lastSentCursor: ReplayCursor | undefined;
  #pumpPromise: Promise<void> | undefined;
  #queueBytes = 0;
  #ready = false;
  #stopped = false;
  #unsubscribe: (() => void) | undefined;

  constructor(options: CanonicalPublisherOptions) {
    this.#onFailure = options.onFailure;
    this.#sendData = options.sendData;
    this.#session = options.session;
    this.#yieldIo = options.yieldIo ?? (() => new Promise((resolve) => setImmediate(resolve)));
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

  tryEnqueueRecoveryStartFence(encoded: Uint8Array): boolean {
    if (this.#stopped || this.#lastQueuedCursor === undefined) return false;

    let cursor: ReplayCursor;
    try {
      const fence = decodeRecoveryStartFence(encoded);
      cursor = {
        sessionEpoch: BigInt(fence.committedThrough.sessionEpoch),
        lastEventSeq: BigInt(fence.committedThrough.eventSeq),
        nextPtyOffset: BigInt(fence.committedThrough.nextPtyOffset),
      };
    } catch {
      return false;
    }
    if (!sameCursor(cursor, this.#lastQueuedCursor)) return false;
    if (
      this.#queue.length + 1 > HOST_CANONICAL_QUEUE_LIMITS.maxFrames ||
      this.#queueBytes + encoded.byteLength > HOST_CANONICAL_QUEUE_LIMITS.maxBytes
    ) {
      return false;
    }

    const bytes = encoded.slice();
    this.#queue.push({ type: "ordered-marker", bytes, cursor });
    this.#queueBytes += bytes.byteLength;
    this.#schedulePump();
    return true;
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
    const exceedsLimit =
      this.#queue.length + 1 > HOST_CANONICAL_QUEUE_LIMITS.maxFrames ||
      this.#queueBytes + encoded.byteLength > HOST_CANONICAL_QUEUE_LIMITS.maxBytes;
    if (exceedsLimit) {
      this.#fail("canonical publisher queue exceeded");
      return;
    }
    const cursor = cursorAfter(frame);
    const bytes = encoded.slice();
    this.#queue.push({ type: "mutation", bytes, cursor });
    this.#queueBytes += bytes.byteLength;
    this.#lastQueuedCursor = cursor;
    this.#schedulePump();
  }

  #schedulePump(): void {
    if (
      this.#stopped ||
      !this.#ready ||
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
    while (!this.#stopped && this.#ready) {
      const queued = this.#queue.shift();
      if (queued === undefined) return;
      this.#queueBytes -= queued.bytes.byteLength;
      this.#sendData(queued.bytes);
      if (queued.type === "mutation") this.#lastSentCursor = queued.cursor;
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
