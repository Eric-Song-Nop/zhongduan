import {
  DATA_HEADER_BYTES,
  MAX_DATA_BATCH_BYTES,
  MAX_DATA_BATCH_FRAMES,
  RelayCapability,
  RelayToHostControlFrameSchema,
  decodeControlFrame,
  encodeDataFrameBatch,
  encodeControlFrame,
  type HostControlFrame,
  type RelayToHostControlFrame,
} from "@zhongduan/protocol";

import type { ReplayCursor, TerminalSession } from "../session";
import {
  HOST_CANONICAL_QUEUE_LIMITS,
  HostDeliveryScheduler,
  type SnapshotPublisherLike,
} from "./delivery-scheduler";
import { dispatchForwardedInput, type ForwardedInput } from "./input-dispatcher";
import type { HostSocketPair } from "./paired-websocket";
import type { SnapshotCheckpointCache } from "./snapshot-checkpoint-cache";

export const HOST_READY_TIMEOUT_MS = 10_000;
export const HOST_HEARTBEAT_INTERVAL_MS = 15_000;
export const HOST_HEARTBEAT_TIMEOUT_MS = 45_000;
export const HOST_CONTROL_QUEUE_LIMITS = {
  maxBytes: 8 * 1024 * 1024,
  maxCount: 64,
} as const;
const MAX_CONTROL_MESSAGE_CHARS = 6 * 1024 * 1024 + 4_096;
const MAX_CONTROL_BUFFERED_BYTES = 1024 * 1024;
const HOST_DATA_BATCH_ACK = "data-ack";
const MAX_PENDING_DATA_FRAMES = 8_192;
const MIN_LATENCY_SENSITIVE_DATA_FRAME_BYTES = DATA_HEADER_BYTES + 3;
const MAX_LATENCY_SENSITIVE_DATA_FRAME_BYTES = DATA_HEADER_BYTES + 256;
const textEncoder = new TextEncoder();

export interface HostRelayConnectionOptions {
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  monotonicNow?: () => number;
  pair: HostSocketPair;
  readyTimeoutMs?: number;
  session: TerminalSession;
  snapshotCheckpointCache?: SnapshotCheckpointCache;
  snapshotPublisher: SnapshotPublisherLike;
}

export class HostRelayConnection {
  readonly #pair: HostSocketPair;
  readonly #heartbeatIntervalMs: number;
  readonly #heartbeatTimeoutMs: number;
  readonly #monotonicNow: () => number;
  readonly #session: TerminalSession;
  readonly #scheduler: HostDeliveryScheduler;
  readonly #dataBatchEnabled: boolean;
  readonly #readyTimeoutMs: number;
  readonly #readyPromise: Promise<void>;
  readonly #resolveReady: () => void;
  readonly #rejectReady: (error: unknown) => void;
  readonly #closedPromise: Promise<void>;
  readonly #resolveClosed: () => void;

  #advertisedCursor: ReplayCursor | undefined;
  #pairFence: object | null = {};
  #closed = false;
  #controlChain = Promise.resolve();
  #heartbeatLastControlAckAt = 0;
  #heartbeatLastDataAckAt = 0;
  #heartbeatTimer: ReturnType<typeof setTimeout> | undefined;
  #dataBatchScheduled = false;
  #inFlightDataBytes = 0;
  #pendingDataBytes = 0;
  readonly #pendingDataFrames: Uint8Array[] = [];
  #queuedControlBytes = 0;
  #queuedControlCount = 0;
  #ready = false;

  constructor(options: HostRelayConnectionOptions) {
    this.#pair = options.pair;
    this.#session = options.session;
    this.#heartbeatIntervalMs = options.heartbeatIntervalMs ?? HOST_HEARTBEAT_INTERVAL_MS;
    this.#heartbeatTimeoutMs = options.heartbeatTimeoutMs ?? HOST_HEARTBEAT_TIMEOUT_MS;
    if (
      !Number.isInteger(this.#heartbeatIntervalMs) ||
      this.#heartbeatIntervalMs <= 0 ||
      !Number.isInteger(this.#heartbeatTimeoutMs) ||
      this.#heartbeatTimeoutMs <= this.#heartbeatIntervalMs
    ) {
      throw new RangeError("Host heartbeat timeout must exceed its positive interval");
    }
    this.#monotonicNow = options.monotonicNow ?? performance.now.bind(performance);
    this.#dataBatchEnabled =
      options.pair.connection.selectedCapabilities?.includes(RelayCapability.hostDataBatchV1) ??
      false;
    this.#readyTimeoutMs = options.readyTimeoutMs ?? HOST_READY_TIMEOUT_MS;
    if (!Number.isInteger(this.#readyTimeoutMs) || this.#readyTimeoutMs <= 0) {
      throw new RangeError("host-ready timeout must be a positive integer");
    }
    let resolveReady!: () => void;
    let rejectReady!: (error: unknown) => void;
    this.#readyPromise = new Promise((resolve, reject) => {
      resolveReady = resolve;
      rejectReady = reject;
    });
    this.#resolveReady = resolveReady;
    this.#rejectReady = rejectReady;
    let resolveClosed!: () => void;
    this.#closedPromise = new Promise((resolve) => {
      resolveClosed = resolve;
    });
    this.#resolveClosed = resolveClosed;
    this.#scheduler = new HostDeliveryScheduler({
      bufferedAmount: () => this.#pair.data.bufferedAmount,
      closePair: (reason) => this.#close(1011, reason),
      sendControl: (frame) => this.#sendControl(frame),
      sendData: (frame) => this.#sendData(frame),
      session: this.#session,
      ...(options.snapshotCheckpointCache === undefined
        ? {}
        : { snapshotCheckpointCache: options.snapshotCheckpointCache }),
      snapshotPublisher: options.snapshotPublisher,
    });
  }

  async start(): Promise<void> {
    if (this.#advertisedCursor !== undefined)
      throw new Error("Host relay connection already started");
    this.#listen();
    try {
      const cursor = this.#scheduler.prepareHostReady();
      this.#advertisedCursor = cursor;
      this.#sendControl({
        type: "host-ready",
        engineId: this.#session.engineId,
        sessionEpoch: cursor.sessionEpoch.toString(),
        headEventSeq: cursor.lastEventSeq.toString(),
        nextPtyOffset: cursor.nextPtyOffset.toString(),
      });
      const timeout = setTimeout(
        () => this.#rejectReady(new DOMException("host-ready ACK timed out", "TimeoutError")),
        this.#readyTimeoutMs,
      );
      try {
        await this.#readyPromise;
      } finally {
        clearTimeout(timeout);
      }
    } catch (error) {
      this.#close(1011, "host relay handshake failed");
      throw error;
    }
  }

  waitClosed(): Promise<void> {
    return this.#closedPromise;
  }

  close(): void {
    this.#close(1000, "host relay stopped");
  }

  #listen(): void {
    this.#pair.control.addEventListener("message", this.#onControlMessage);
    this.#pair.control.addEventListener("close", this.#onSocketClose);
    this.#pair.control.addEventListener("error", this.#onSocketError);
    this.#pair.data.addEventListener("message", this.#onDataMessage);
    this.#pair.data.addEventListener("close", this.#onSocketClose);
    this.#pair.data.addEventListener("error", this.#onSocketError);
  }

  #unlisten(): void {
    this.#pair.control.removeEventListener("message", this.#onControlMessage);
    this.#pair.control.removeEventListener("close", this.#onSocketClose);
    this.#pair.control.removeEventListener("error", this.#onSocketError);
    this.#pair.data.removeEventListener("message", this.#onDataMessage);
    this.#pair.data.removeEventListener("close", this.#onSocketClose);
    this.#pair.data.removeEventListener("error", this.#onSocketError);
  }

  readonly #onControlMessage = (event: MessageEvent) => {
    if (typeof event.data !== "string" || event.data.length > MAX_CONTROL_MESSAGE_CHARS) {
      this.#close(1002, "invalid relay control frame");
      return;
    }
    if (event.data === "pong") {
      if (!this.#ready) {
        this.#close(1002, "relay heartbeat arrived before host-ready ACK");
        return;
      }
      this.#heartbeatLastControlAckAt = this.#monotonicNow();
      return;
    }
    const encoded = event.data;
    const encodedBytes = textEncoder.encode(encoded).byteLength;
    if (
      this.#queuedControlCount + 1 > HOST_CONTROL_QUEUE_LIMITS.maxCount ||
      this.#queuedControlBytes + encodedBytes > HOST_CONTROL_QUEUE_LIMITS.maxBytes
    ) {
      this.#close(1009, "relay control queue exceeded");
      return;
    }
    this.#queuedControlCount += 1;
    this.#queuedControlBytes += encodedBytes;
    const next = this.#controlChain.then(() => this.#handleControl(encoded));
    this.#controlChain = next.then(
      () => this.#releaseQueuedControl(encodedBytes),
      (error: unknown) => {
        this.#releaseQueuedControl(encodedBytes);
        this.#close(1011, error instanceof Error ? error.message : "relay control failed");
      },
    );
  };

  readonly #onDataMessage = (event: MessageEvent) => {
    if (event.data === "pong") {
      if (!this.#ready) {
        this.#close(1002, "relay data heartbeat arrived before host-ready ACK");
        return;
      }
      this.#heartbeatLastDataAckAt = this.#monotonicNow();
      return;
    }
    if (event.data === HOST_DATA_BATCH_ACK) {
      if (!this.#ready || !this.#dataBatchEnabled || this.#inFlightDataBytes === 0) {
        this.#close(1002, "unexpected Host data acknowledgement");
        return;
      }
      this.#heartbeatLastDataAckAt = this.#monotonicNow();
      this.#inFlightDataBytes = 0;
      try {
        this.#flushDataBatch();
      } catch (error) {
        this.#close(1011, error instanceof Error ? error.message : "Host data batch failed");
      }
      return;
    }
    this.#close(1002, "Host data channel is send-only");
  };

  readonly #onSocketClose = () => {
    this.#close(1012, "relay socket disconnected");
  };

  readonly #onSocketError = () => {
    this.#close(1011, "relay socket failed");
  };

  async #handleControl(encoded: string): Promise<void> {
    if (this.#closed) return;
    let frame: RelayToHostControlFrame;
    try {
      frame = decodeControlFrame(encoded, RelayToHostControlFrameSchema);
    } catch {
      this.#close(1002, "invalid relay control frame");
      return;
    }
    this.#heartbeatLastControlAckAt = this.#monotonicNow();

    if (frame.type === "host-ready-ack") {
      this.#handleReadyAck(frame);
      return;
    }
    if (!this.#ready) throw new Error("relay sent Host traffic before host-ready ACK");
    if (frame.type === "delivery-barrier-result") {
      this.#scheduler.handleBarrierResult(frame);
      return;
    }
    if (frame.type === "attach-request") {
      this.#scheduler.enqueueAttach(frame);
      return;
    }
    if (frame.type === "delivery-reset") {
      this.#scheduler.handleDeliveryReset(frame);
      return;
    }
    await this.#handleInput(frame);
  }

  #handleReadyAck(frame: Extract<RelayToHostControlFrame, { type: "host-ready-ack" }>): void {
    const advertised = this.#advertisedCursor;
    if (this.#ready || advertised === undefined) {
      throw new Error("unexpected host-ready ACK");
    }
    const acknowledged: ReplayCursor = {
      sessionEpoch: BigInt(frame.sessionEpoch),
      lastEventSeq: BigInt(frame.headEventSeq),
      nextPtyOffset: BigInt(frame.nextPtyOffset),
    };
    this.#scheduler.activateHostReady(acknowledged);
    this.#ready = true;
    const now = this.#monotonicNow();
    this.#heartbeatLastControlAckAt = now;
    this.#heartbeatLastDataAckAt = now;
    this.#scheduleHeartbeat();
    this.#resolveReady();
  }

  #scheduleHeartbeat(): void {
    if (this.#closed) return;
    if (this.#heartbeatTimer !== undefined) clearTimeout(this.#heartbeatTimer);
    this.#heartbeatTimer = setTimeout(() => {
      this.#heartbeatTimer = undefined;
      if (this.#closed) return;
      const now = this.#monotonicNow();
      if (now - this.#heartbeatLastControlAckAt >= this.#heartbeatTimeoutMs) {
        this.#close(1012, "Host control relay heartbeat timed out");
        return;
      }
      if (now - this.#heartbeatLastDataAckAt >= this.#heartbeatTimeoutMs) {
        this.#close(1012, "Host data relay heartbeat timed out");
        return;
      }
      try {
        if (now - this.#heartbeatLastControlAckAt >= this.#heartbeatIntervalMs) {
          this.#sendRawControl("ping");
        }
        if (now - this.#heartbeatLastDataAckAt >= this.#heartbeatIntervalMs) {
          this.#sendRawDataHeartbeat();
        }
      } catch (error) {
        this.#close(1011, error instanceof Error ? error.message : "Host heartbeat failed");
        return;
      }
      this.#scheduleHeartbeat();
    }, this.#heartbeatIntervalMs);
  }

  async #handleInput(frame: ForwardedInput): Promise<void> {
    const fence = this.#pairFence;
    const ack = await dispatchForwardedInput(this.#session, frame);
    if (
      this.#closed ||
      fence === null ||
      fence !== this.#pairFence ||
      this.#pair.control.readyState !== WebSocket.OPEN
    ) {
      return;
    }
    this.#sendControl(ack);
  }

  #sendControl(frame: HostControlFrame): void {
    this.#sendRawControl(encodeControlFrame(frame));
  }

  #sendRawControl(encoded: string): void {
    if (this.#closed || this.#pair.control.readyState !== WebSocket.OPEN) {
      throw new Error("Host control WebSocket is not open");
    }
    if (this.#pair.control.bufferedAmount > MAX_CONTROL_BUFFERED_BYTES) {
      throw new Error("Host control WebSocket buffer exceeded");
    }
    this.#pair.control.send(encoded);
    if (this.#pair.control.bufferedAmount > MAX_CONTROL_BUFFERED_BYTES) {
      throw new Error("Host control WebSocket buffer exceeded");
    }
  }

  #sendData(frame: Uint8Array): void {
    if (this.#closed || this.#pair.data.readyState !== WebSocket.OPEN) {
      throw new Error("Host data WebSocket is not open");
    }
    if (!(frame.buffer instanceof ArrayBuffer)) {
      throw new Error("Host data frame must use an owned ArrayBuffer");
    }
    if (!this.#dataBatchEnabled) {
      this.#sendRawData(frame);
      return;
    }
    if (frame.byteLength > MAX_DATA_BATCH_BYTES) {
      throw new Error("Host data frame exceeds the negotiated batch bound");
    }
    if (
      this.#pendingDataFrames.length + 1 > MAX_PENDING_DATA_FRAMES ||
      this.#pair.data.bufferedAmount +
        this.#inFlightDataBytes +
        this.#pendingDataBytes +
        frame.byteLength >
        HOST_CANONICAL_QUEUE_LIMITS.maxBytes
    ) {
      throw new Error("Host data WebSocket buffer exceeded");
    }
    this.#pendingDataFrames.push(frame);
    this.#pendingDataBytes += frame.byteLength;
    if (
      this.#pendingDataFrames.length >= MAX_DATA_BATCH_FRAMES ||
      this.#pendingDataBytes >= MAX_DATA_BATCH_BYTES
    ) {
      this.#flushDataBatch();
      return;
    }
    if (this.#dataBatchScheduled) return;
    this.#dataBatchScheduled = true;
    queueMicrotask(() => {
      this.#dataBatchScheduled = false;
      if (this.#closed) return;
      try {
        this.#flushDataBatch();
      } catch (error) {
        this.#close(1011, error instanceof Error ? error.message : "Host data batch failed");
      }
    });
  }

  #flushDataBatch(): void {
    if (this.#pendingDataFrames.length === 0 || this.#inFlightDataBytes > 0) return;
    let count = 0;
    let bytes = 0;
    while (count < this.#pendingDataFrames.length && count < MAX_DATA_BATCH_FRAMES) {
      const next = this.#pendingDataFrames[count]!;
      const latencySensitive =
        next.byteLength >= MIN_LATENCY_SENSITIVE_DATA_FRAME_BYTES &&
        next.byteLength <= MAX_LATENCY_SENSITIVE_DATA_FRAME_BYTES;
      if (count > 0 && latencySensitive) break;
      if (count > 0 && bytes + next.byteLength > MAX_DATA_BATCH_BYTES) break;
      bytes += next.byteLength;
      count += 1;
      if (latencySensitive) break;
    }
    const frames = this.#pendingDataFrames.splice(0, count);
    this.#pendingDataBytes -= bytes;
    const encoded = encodeDataFrameBatch(frames);
    this.#sendRawData(encoded);
    this.#inFlightDataBytes = encoded.byteLength;
  }

  #sendRawData(frame: Uint8Array): void {
    if (this.#closed || this.#pair.data.readyState !== WebSocket.OPEN) {
      throw new Error("Host data WebSocket is not open");
    }
    if (!(frame.buffer instanceof ArrayBuffer)) {
      throw new Error("Host data frame must use an owned ArrayBuffer");
    }
    if (this.#pair.data.bufferedAmount + frame.byteLength > HOST_CANONICAL_QUEUE_LIMITS.maxBytes) {
      throw new Error("Host data WebSocket buffer exceeded");
    }
    this.#pair.data.send(frame as Uint8Array<ArrayBuffer>);
    if (this.#pair.data.bufferedAmount > HOST_CANONICAL_QUEUE_LIMITS.maxBytes) {
      throw new Error("Host data WebSocket buffer exceeded");
    }
  }

  #sendRawDataHeartbeat(): void {
    if (this.#closed || this.#pair.data.readyState !== WebSocket.OPEN) {
      throw new Error("Host data WebSocket is not open");
    }
    this.#flushDataBatch();
    if (this.#pair.data.bufferedAmount > HOST_CANONICAL_QUEUE_LIMITS.maxBytes) {
      throw new Error("Host data WebSocket buffer exceeded");
    }
    this.#pair.data.send("ping");
    if (this.#pair.data.bufferedAmount > HOST_CANONICAL_QUEUE_LIMITS.maxBytes) {
      throw new Error("Host data WebSocket buffer exceeded");
    }
  }

  #releaseQueuedControl(encodedBytes: number): void {
    this.#queuedControlCount -= 1;
    this.#queuedControlBytes -= encodedBytes;
  }

  #close(code: number, reason: string): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#pairFence = null;
    this.#ready = false;
    this.#pendingDataFrames.length = 0;
    this.#inFlightDataBytes = 0;
    this.#pendingDataBytes = 0;
    if (this.#heartbeatTimer !== undefined) clearTimeout(this.#heartbeatTimer);
    this.#heartbeatTimer = undefined;
    this.#unlisten();
    this.#scheduler.dispose(reason);
    this.#rejectReady(new Error(reason));
    this.#pair.close(code, reason.slice(0, 120));
    this.#resolveClosed();
  }
}
