import {
  RelayToHostControlFrameSchema,
  decodeControlFrame,
  encodeControlFrame,
  type HostControlFrame,
  type RelayToHostControlFrame,
} from "@zhongduan/protocol";
import {
  createBufferedTelemetrySink,
  elapsedMs,
  telemetryByteSizeBucket,
  type BufferedTelemetrySink,
  type TelemetrySink,
  type TerminalTelemetryEvent,
} from "@zhongduan/telemetry";

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
const textEncoder = new TextEncoder();
type ControlMessageClass = "host-ready" | "recovery" | "input" | "unknown";
interface ControlHandlingResult {
  messageClass: ControlMessageClass;
  outcome: "handled" | "rejected";
}

interface ControlQueueContext {
  handlingStartedAt: number;
  queuedAt: number;
  queuedCount: number;
}

export interface HostRelayConnectionOptions {
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  monotonicNow?: () => number;
  pair: HostSocketPair;
  readyTimeoutMs?: number;
  session: TerminalSession;
  snapshotCheckpointCache?: SnapshotCheckpointCache;
  snapshotPublisher: SnapshotPublisherLike;
  telemetryBuffer?: BufferedTelemetrySink;
  telemetry?: TelemetrySink;
}

export class HostRelayConnection {
  readonly #pair: HostSocketPair;
  readonly #heartbeatIntervalMs: number;
  readonly #heartbeatMaxOutstanding: number;
  readonly #heartbeatTimeoutMs: number;
  readonly #monotonicNow: () => number;
  readonly #session: TerminalSession;
  readonly #scheduler: HostDeliveryScheduler;
  readonly #telemetry: TelemetrySink | undefined;
  readonly #telemetryBuffer: BufferedTelemetrySink | undefined;
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
  #heartbeatControlPings: number[] = [];
  #heartbeatControlRttEnabled = true;
  #heartbeatDataPings: number[] = [];
  #heartbeatDataRttEnabled = true;
  #heartbeatTimer: ReturnType<typeof setTimeout> | undefined;
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
    this.#heartbeatMaxOutstanding = Math.ceil(this.#heartbeatTimeoutMs / this.#heartbeatIntervalMs);
    this.#monotonicNow = options.monotonicNow ?? performance.now.bind(performance);
    this.#telemetryBuffer =
      options.telemetryBuffer ??
      (options.telemetry === undefined
        ? undefined
        : createBufferedTelemetrySink(options.telemetry));
    this.#telemetry = this.#telemetryBuffer?.sink;
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
      monotonicNow: this.#monotonicNow,
      sendControl: (frame) => this.#sendControl(frame),
      sendData: (frame) => this.#sendData(frame),
      session: this.#session,
      ...(options.snapshotCheckpointCache === undefined
        ? {}
        : { snapshotCheckpointCache: options.snapshotCheckpointCache }),
      snapshotPublisher: options.snapshotPublisher,
      ...(this.#telemetryBuffer === undefined ? {} : { telemetryBuffer: this.#telemetryBuffer }),
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
      const now = this.#monotonicNow();
      this.#heartbeatLastControlAckAt = now;
      this.#recordHeartbeatRtt("control", this.#heartbeatControlPings, now);
      return;
    }
    const encoded = event.data;
    const encodedBytes = textEncoder.encode(encoded).byteLength;
    if (
      this.#queuedControlCount + 1 > HOST_CONTROL_QUEUE_LIMITS.maxCount ||
      this.#queuedControlBytes + encodedBytes > HOST_CONTROL_QUEUE_LIMITS.maxBytes
    ) {
      this.#emitControlQueue({
        messageClass: "unknown",
        outcome: "capacity",
        queueWaitMs: 0,
        handlingMs: 0,
        queuedBytes: this.#queuedControlBytes,
        queuedCount: this.#queuedControlCount,
      });
      this.#close(1009, "relay control queue exceeded");
      return;
    }
    const queuedAt = this.#monotonicNow();
    this.#queuedControlCount += 1;
    this.#queuedControlBytes += encodedBytes;
    const queuedCount = this.#queuedControlCount;
    const queuedBytes = this.#queuedControlBytes;
    let handlingStartedAt = queuedAt;
    const next = this.#controlChain.then(() => {
      handlingStartedAt = this.#monotonicNow();
      return this.#handleControl(encoded, { handlingStartedAt, queuedAt, queuedCount });
    });
    this.#controlChain = next.then(
      (result) => {
        const finishedAt = this.#monotonicNow();
        this.#releaseQueuedControl(encodedBytes);
        this.#emitControlQueue({
          ...result,
          queueWaitMs: elapsedMs(queuedAt, handlingStartedAt),
          handlingMs: elapsedMs(handlingStartedAt, finishedAt),
          queuedBytes,
          queuedCount,
        });
      },
      (error: unknown) => {
        const finishedAt = this.#monotonicNow();
        this.#releaseQueuedControl(encodedBytes);
        this.#emitControlQueue({
          messageClass: "unknown",
          outcome: "failed",
          queueWaitMs: elapsedMs(queuedAt, handlingStartedAt),
          handlingMs: elapsedMs(handlingStartedAt, finishedAt),
          queuedBytes,
          queuedCount,
        });
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
      const now = this.#monotonicNow();
      this.#heartbeatLastDataAckAt = now;
      this.#recordHeartbeatRtt("data", this.#heartbeatDataPings, now);
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

  async #handleControl(
    encoded: string,
    context: ControlQueueContext,
  ): Promise<ControlHandlingResult> {
    if (this.#closed) return { messageClass: "unknown", outcome: "rejected" };
    let frame: RelayToHostControlFrame;
    try {
      frame = decodeControlFrame(encoded, RelayToHostControlFrameSchema);
    } catch {
      this.#close(1002, "invalid relay control frame");
      return { messageClass: "unknown", outcome: "rejected" };
    }

    if (frame.type === "host-ready-ack") {
      this.#handleReadyAck(frame);
      return { messageClass: "host-ready", outcome: "handled" };
    }
    if (!this.#ready) throw new Error("relay sent Host traffic before host-ready ACK");
    if (frame.type === "delivery-barrier-result") {
      this.#scheduler.handleBarrierResult(frame);
      return { messageClass: "recovery", outcome: "handled" };
    }
    if (frame.type === "attach-request") {
      this.#scheduler.enqueueAttach(frame);
      return { messageClass: "recovery", outcome: "handled" };
    }
    if (frame.type === "delivery-reset") {
      this.#scheduler.handleDeliveryReset(frame);
      return { messageClass: "recovery", outcome: "handled" };
    }
    await this.#handleInput(frame, context);
    return { messageClass: "input", outcome: "handled" };
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
        this.#recordHeartbeatTimeout(
          "control",
          now,
          this.#heartbeatLastControlAckAt,
          this.#heartbeatControlPings.length,
        );
        this.#close(1012, "Host control relay heartbeat timed out");
        return;
      }
      if (now - this.#heartbeatLastDataAckAt >= this.#heartbeatTimeoutMs) {
        this.#recordHeartbeatTimeout(
          "data",
          now,
          this.#heartbeatLastDataAckAt,
          this.#heartbeatDataPings.length,
        );
        this.#close(1012, "Host data relay heartbeat timed out");
        return;
      }
      try {
        this.#sendRawControl("ping");
        this.#rememberHeartbeat("control", this.#heartbeatControlPings, now);
        this.#sendRawDataHeartbeat();
        this.#rememberHeartbeat("data", this.#heartbeatDataPings, now);
      } catch (error) {
        this.#close(1011, error instanceof Error ? error.message : "Host heartbeat failed");
        return;
      }
      this.#scheduleHeartbeat();
    }, this.#heartbeatIntervalMs);
  }

  async #handleInput(frame: ForwardedInput, context: ControlQueueContext): Promise<void> {
    const fence = this.#pairFence;
    const result = await dispatchForwardedInput(this.#session, frame);
    let ackSendOutcome: "send-returned" | "not-attempted" | "uncertain" = "not-attempted";
    try {
      if (
        !this.#closed &&
        fence !== null &&
        fence === this.#pairFence &&
        this.#pair.control.readyState === WebSocket.OPEN
      ) {
        this.#sendControl(result.ack);
        ackSendOutcome = "send-returned";
      }
    } catch (error) {
      ackSendOutcome = "uncertain";
      throw error;
    } finally {
      const finishedAt = this.#monotonicNow();
      const common = {
        schemaVersion: 1,
        monotonicAtMs: finishedAt,
        name: "host.input.apply",
        outcome: result.ack.status,
        effectStage: result.timing.effectStage,
        ackSendOutcome,
        controlQueueWaitMs: elapsedMs(context.queuedAt, context.handlingStartedAt),
        controlQueueDepth: context.queuedCount,
        actorQueueWaitMs: result.timing.actorQueueWaitMs,
        actorProcessingMs: result.timing.actorProcessingMs,
        hostIngressToAckDecisionMs: elapsedMs(context.queuedAt, finishedAt),
      } as const;
      if (result.timing.inputKind === "resize") {
        this.#enqueueTelemetry({
          ...common,
          inputKind: "resize",
          authorityResizeMs: result.timing.inputEncodeMs,
          ptyResizeAttempted: result.timing.ptyResizeAttempted,
          ptyResizeMs: result.timing.ptyResizeMs,
          effectWriteAttempted: result.timing.ptyWriteAttempted,
          effectWriteMs: result.timing.ptyWriteMs,
          effectBytesBucket: telemetryByteSizeBucket(result.timing.ptyBytes),
        });
      } else {
        this.#enqueueTelemetry({
          ...common,
          inputKind: result.timing.inputKind,
          encodeKind: result.timing.encodeKind === "resize" ? "none" : result.timing.encodeKind,
          inputEncodeMs: result.timing.inputEncodeMs,
          ptyWriteAttempted: result.timing.ptyWriteAttempted,
          ptyWriteMs: result.timing.ptyWriteMs,
          ptyBytesBucket: telemetryByteSizeBucket(result.timing.ptyBytes),
        });
      }
    }
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
    this.#pair.data.send(frame as Uint8Array<ArrayBuffer>);
  }

  #sendRawDataHeartbeat(): void {
    if (this.#closed || this.#pair.data.readyState !== WebSocket.OPEN) {
      throw new Error("Host data WebSocket is not open");
    }
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

  #emitControlQueue(event: {
    messageClass: ControlMessageClass;
    outcome: "handled" | "rejected" | "failed" | "capacity";
    queueWaitMs: number;
    handlingMs: number;
    queuedBytes: number;
    queuedCount: number;
  }): void {
    this.#enqueueTelemetry({
      schemaVersion: 1,
      monotonicAtMs: this.#monotonicNow(),
      name: "host.control.queue",
      messageClass: event.messageClass,
      outcome: event.outcome,
      queueWaitMs: event.queueWaitMs,
      handlingMs: event.handlingMs,
      queuedBytesBucket: telemetryByteSizeBucket(event.queuedBytes),
      queuedCount: event.queuedCount,
    });
  }

  #rememberHeartbeat(channel: "control" | "data", pings: number[], sentAt: number): void {
    const enabled =
      channel === "control" ? this.#heartbeatControlRttEnabled : this.#heartbeatDataRttEnabled;
    if (!enabled) return;
    if (pings.length >= this.#heartbeatMaxOutstanding) {
      pings.length = 0;
      if (channel === "control") this.#heartbeatControlRttEnabled = false;
      else this.#heartbeatDataRttEnabled = false;
      return;
    }
    pings.push(sentAt);
  }

  #recordHeartbeatRtt(channel: "control" | "data", pings: number[], receivedAt: number): void {
    if (
      (channel === "control" && !this.#heartbeatControlRttEnabled) ||
      (channel === "data" && !this.#heartbeatDataRttEnabled)
    ) {
      return;
    }
    const sentAt = pings.shift();
    if (sentAt === undefined) return;
    this.#enqueueTelemetry({
      schemaVersion: 1,
      monotonicAtMs: receivedAt,
      name: "host.relay.rtt",
      channel,
      outcome: "ok",
      durationMs: elapsedMs(sentAt, receivedAt),
      outstandingPings: pings.length,
    });
  }

  #recordHeartbeatTimeout(
    channel: "control" | "data",
    observedAt: number,
    lastAckAt: number,
    outstandingPings: number,
  ): void {
    this.#enqueueTelemetry({
      schemaVersion: 1,
      monotonicAtMs: observedAt,
      name: "host.relay.rtt",
      channel,
      outcome: "timeout",
      silenceMs: elapsedMs(lastAckAt, observedAt),
      outstandingPings,
    });
  }

  #enqueueTelemetry(event: TerminalTelemetryEvent): void {
    try {
      this.#telemetry?.(event);
    } catch {
      // The bounded diagnostics queue is observational and must not affect relay handling.
    }
  }

  #close(code: number, reason: string): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#pairFence = null;
    this.#ready = false;
    if (this.#heartbeatTimer !== undefined) clearTimeout(this.#heartbeatTimer);
    this.#heartbeatTimer = undefined;
    this.#heartbeatControlPings = [];
    this.#heartbeatDataPings = [];
    this.#unlisten();
    this.#scheduler.dispose(reason);
    this.#rejectReady(new Error(reason));
    this.#pair.close(code, reason.slice(0, 120));
    this.#resolveClosed();
  }
}
