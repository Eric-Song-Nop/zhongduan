import {
  HostControlFrameSchema as HostToCloudControlFrameSchema,
  RelayToHostControlFrameSchema as CloudToHostControlFrameSchema,
  decodeControlFrame,
  encodeControlFrame,
  type HostControlFrame as HostToCloudControlFrame,
  type RelayToHostControlFrame as CloudToHostControlFrame,
} from "@zhongduan/protocol";

import type { ReplayCursor, TerminalSession } from "../session";
import { CanonicalPublisher, HOST_CANONICAL_QUEUE_LIMITS } from "./canonical-publisher";
import { dispatchForwardedInput, type ForwardedInput } from "./input-dispatcher";
import type { HostSocketPair } from "./paired-websocket";
import {
  type RecoverySourceManager,
  type RecoverySourceOwnerToken,
} from "./recovery-source-manager";
import {
  HOST_RECOVERY_DATA_HIGH_WATER_BYTES,
  RecoverySourceScheduler,
} from "./recovery-source-scheduler";

export const HOST_READY_TIMEOUT_MS = 10_000;
export const HOST_HEARTBEAT_INTERVAL_MS = 15_000;
export const HOST_HEARTBEAT_TIMEOUT_MS = 45_000;
export const HOST_RECOVERY_DEADLINE_TICK_MS = 1_000;
export const HOST_CONTROL_QUEUE_LIMITS = {
  maxBytes: 8 * 1024 * 1024,
  maxCount: 64,
} as const;
const MAX_CONTROL_MESSAGE_CHARS = 6 * 1024 * 1024 + 4_096;
const MAX_CONTROL_BUFFERED_BYTES = 1024 * 1024;
const textEncoder = new TextEncoder();

type RecoveryControlFrame = Extract<
  CloudToHostControlFrame,
  {
    type:
      | "recovery-prepare"
      | "recovery-start-ready"
      | "recovery-source-grant"
      | "recovery-source-received"
      | "recovery-source-reset";
  }
>;

type RecoveryRoutingIdentity = Pick<
  RecoveryControlFrame,
  "recoveryId" | "connectionId" | "streamId" | "deliveryGeneration"
>;

function isRecoveryControlFrame(frame: CloudToHostControlFrame): frame is RecoveryControlFrame {
  return (
    frame.type === "recovery-prepare" ||
    frame.type === "recovery-start-ready" ||
    frame.type === "recovery-source-grant" ||
    frame.type === "recovery-source-received" ||
    frame.type === "recovery-source-reset"
  );
}

export interface HostRelayConnectionOptions {
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  monotonicNow?: () => number;
  pair: HostSocketPair;
  readyTimeoutMs?: number;
  recoveryDeadlineTickMs?: number;
  recoverySourceManager: RecoverySourceManager;
  session: TerminalSession;
}

export class HostRelayConnection {
  readonly #pair: HostSocketPair;
  readonly #heartbeatIntervalMs: number;
  readonly #heartbeatTimeoutMs: number;
  readonly #monotonicNow: () => number;
  readonly #session: TerminalSession;
  readonly #canonicalPublisher: CanonicalPublisher;
  readonly #readyTimeoutMs: number;
  readonly #recoveryDeadlineTickMs: number;
  readonly #recoveryOwnerToken: RecoverySourceOwnerToken = {};
  readonly #recoveryScheduler: RecoverySourceScheduler;
  readonly #recoverySourceManager: RecoverySourceManager;
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
  #queuedControlBytes = 0;
  #queuedControlCount = 0;
  #ready = false;
  #recoveryDeadlineTimer: ReturnType<typeof setTimeout> | undefined;

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
    this.#readyTimeoutMs = options.readyTimeoutMs ?? HOST_READY_TIMEOUT_MS;
    if (!Number.isInteger(this.#readyTimeoutMs) || this.#readyTimeoutMs <= 0) {
      throw new RangeError("host-ready timeout must be a positive integer");
    }
    this.#recoveryDeadlineTickMs = options.recoveryDeadlineTickMs ?? HOST_RECOVERY_DEADLINE_TICK_MS;
    if (!Number.isInteger(this.#recoveryDeadlineTickMs) || this.#recoveryDeadlineTickMs <= 0) {
      throw new RangeError("recovery deadline tick must be a positive integer");
    }
    this.#recoverySourceManager = options.recoverySourceManager;
    this.#recoveryScheduler = new RecoverySourceScheduler({
      bufferedAmount: () => this.#pair.data.bufferedAmount,
      dataHighWaterBytes: HOST_RECOVERY_DATA_HIGH_WATER_BYTES,
      manager: this.#recoverySourceManager,
      onFailure: (reason) => this.#close(1011, reason),
      ownerToken: this.#recoveryOwnerToken,
      sendData: (encoded) => this.#sendRecoveryData(encoded),
    });
    this.#canonicalPublisher = new CanonicalPublisher({
      onFailure: (reason) => this.#close(1011, reason),
      sendData: (encoded) => this.#sendCanonicalData(encoded),
      session: this.#session,
    });
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
  }

  async start(): Promise<void> {
    if (this.#advertisedCursor !== undefined)
      throw new Error("Host relay connection already started");
    this.#listen();
    try {
      const cursor = this.#canonicalPublisher.prepare();
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
    let frame: CloudToHostControlFrame;
    try {
      frame = decodeControlFrame(encoded, CloudToHostControlFrameSchema);
    } catch {
      this.#close(1002, "invalid relay control frame");
      return;
    }

    if (frame.type === "host-ready-ack") {
      this.#handleReadyAck(frame);
      return;
    }
    if (!this.#ready) throw new Error("relay sent Host traffic before host-ready ACK");
    if (isRecoveryControlFrame(frame)) {
      await this.#handleRecoveryControl(frame);
      return;
    }
    await this.#handleInput(frame);
  }

  #handleReadyAck(frame: Extract<CloudToHostControlFrame, { type: "host-ready-ack" }>): void {
    const advertised = this.#advertisedCursor;
    if (this.#ready || advertised === undefined) {
      throw new Error("unexpected host-ready ACK");
    }
    const acknowledged: ReplayCursor = {
      sessionEpoch: BigInt(frame.sessionEpoch),
      lastEventSeq: BigInt(frame.headEventSeq),
      nextPtyOffset: BigInt(frame.nextPtyOffset),
    };
    this.#canonicalPublisher.activate(acknowledged);
    this.#ready = true;
    const now = this.#monotonicNow();
    this.#heartbeatLastControlAckAt = now;
    this.#heartbeatLastDataAckAt = now;
    this.#scheduleHeartbeat();
    this.#scheduleRecoveryDeadlineTick();
    this.#resolveReady();
  }

  async #handleRecoveryControl(frame: RecoveryControlFrame): Promise<void> {
    if (frame.type === "recovery-prepare") {
      const pairFence = this.#pairFence;
      const result = await this.#recoverySourceManager.prepare(
        this.#recoveryOwnerToken,
        frame,
        (encoded) =>
          !this.#closed &&
          pairFence !== null &&
          pairFence === this.#pairFence &&
          this.#canonicalPublisher.tryEnqueueRecoveryStartFence(encoded),
      );
      if (this.#closed || pairFence === null || pairFence !== this.#pairFence) return;
      if (result.status === "prepared") return;
      if (result.status === "rejected") {
        this.#sendControl(result.rejection);
        return;
      }
      if (result.status === "unavailable" && result.reason === "deadline") return;
      throw new Error(`Recovery source prepare ${result.status}: ${result.reason}`);
    }
    if (frame.type === "recovery-start-ready") {
      if (!this.#recoverySourceManager.startReady(this.#recoveryOwnerToken, frame)) {
        if (this.#isRetiredRecoveryIdentity(frame)) return;
        throw new Error("invalid Recovery source start-ready identity");
      }
      this.#recoveryScheduler.notify(frame);
      return;
    }
    if (frame.type === "recovery-source-grant") {
      if (!this.#recoverySourceManager.grant(this.#recoveryOwnerToken, frame)) {
        if (this.#isRetiredRecoveryIdentity(frame)) return;
        throw new Error("invalid Recovery source grant identity");
      }
      this.#recoveryScheduler.notify(frame);
      return;
    }
    if (frame.type === "recovery-source-received") {
      const result = this.#recoverySourceManager.received(this.#recoveryOwnerToken, frame);
      if (result.status === "invalid") {
        if (this.#isRetiredRecoveryIdentity(frame)) return;
        throw new Error(`invalid Recovery source receipt: ${result.reason}`);
      }
      if (result.status === "closed") {
        this.#recoveryScheduler.forget(frame);
        this.#sendControl(result.closed);
      }
      if (result.status === "partial" && result.advanced) {
        this.#recoveryScheduler.notify(frame);
      }
      return;
    }
    if (!this.#recoverySourceManager.reset(this.#recoveryOwnerToken, frame)) {
      if (this.#isRetiredRecoveryIdentity(frame)) return;
      throw new Error("invalid Recovery source reset identity");
    }
    this.#recoveryScheduler.forget(frame);
  }

  #isRetiredRecoveryIdentity(identity: RecoveryRoutingIdentity): boolean {
    return this.#recoverySourceManager.isRetiredIdentity(this.#recoveryOwnerToken, {
      recoveryId: identity.recoveryId,
      connectionId: identity.connectionId,
      streamId: identity.streamId,
      deliveryGeneration: identity.deliveryGeneration,
    });
  }

  #scheduleRecoveryDeadlineTick(): void {
    if (this.#closed) return;
    if (this.#recoveryDeadlineTimer !== undefined) clearTimeout(this.#recoveryDeadlineTimer);
    this.#recoveryDeadlineTimer = setTimeout(() => {
      this.#recoveryDeadlineTimer = undefined;
      if (this.#closed) return;
      try {
        const expirations = this.#recoverySourceManager.checkDeadlines(this.#recoveryOwnerToken);
        for (const expiration of expirations) {
          this.#recoveryScheduler.forget(expiration.identity);
        }
      } catch (error) {
        this.#close(
          1011,
          error instanceof Error ? error.message : "Recovery source deadline check failed",
        );
        return;
      }
      this.#scheduleRecoveryDeadlineTick();
    }, this.#recoveryDeadlineTickMs);
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
        this.#sendRawControl("ping");
        this.#sendRawDataHeartbeat();
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

  #sendControl(frame: HostToCloudControlFrame): void {
    this.#sendRawControl(encodeControlFrame(HostToCloudControlFrameSchema.parse(frame)));
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

  #sendCanonicalData(frame: Uint8Array): void {
    if (this.#pair.data.bufferedAmount > HOST_CANONICAL_QUEUE_LIMITS.maxBytes) {
      throw new Error("Host data WebSocket buffer exceeded");
    }
    this.#sendData(frame);
    if (this.#pair.data.bufferedAmount > HOST_CANONICAL_QUEUE_LIMITS.maxBytes) {
      throw new Error("Host data WebSocket buffer exceeded");
    }
  }

  #sendRecoveryData(frame: Uint8Array): void {
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

  #close(code: number, reason: string): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#pairFence = null;
    this.#ready = false;
    if (this.#heartbeatTimer !== undefined) clearTimeout(this.#heartbeatTimer);
    this.#heartbeatTimer = undefined;
    if (this.#recoveryDeadlineTimer !== undefined) clearTimeout(this.#recoveryDeadlineTimer);
    this.#recoveryDeadlineTimer = undefined;
    this.#unlisten();
    this.#recoveryScheduler.dispose();
    this.#recoverySourceManager.resetOwner(this.#recoveryOwnerToken);
    this.#canonicalPublisher.dispose();
    this.#rejectReady(new Error(reason));
    this.#pair.close(code, reason.slice(0, 120));
    this.#resolveClosed();
  }
}
