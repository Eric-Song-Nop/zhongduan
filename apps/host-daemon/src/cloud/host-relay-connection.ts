import {
  RecoveryV3CloudToHostControlFrameSchema,
  RecoveryV3HostToCloudControlFrameSchema,
  RelayCapability,
  RelayToHostControlFrameSchema,
  decodeControlFrame,
  encodeControlFrame,
  type HostControlFrame,
  type RecoveryV3CloudToHostControlFrame,
  type RecoveryV3HostRoutingIdentity,
  type RecoveryV3HostToCloudControlFrame,
  type RelayToHostControlFrame,
} from "@zhongduan/protocol";

import type { ReplayCursor, TerminalSession } from "../session";
import { HOST_CANONICAL_QUEUE_LIMITS } from "./canonical-publisher";
import { HostDeliveryScheduler } from "./delivery-scheduler";
import { dispatchForwardedInput, type ForwardedInput } from "./input-dispatcher";
import type { HostSocketPair } from "./paired-websocket";
import {
  type RecoverySourceManager,
  type RecoverySourceOwnerToken,
} from "./recovery-source-manager";
import type { SnapshotCheckpointManager } from "./snapshot-checkpoint-manager";

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
const HOST_RECOVERY_DRAIN_MAX_WIRE_BYTES = 512 * 1024;
const textEncoder = new TextEncoder();
const HOST_RECOVERY_V3_CAPABILITIES = [
  RelayCapability.capabilityNegotiationV1,
  RelayCapability.authorityDataV2,
  RelayCapability.wireEndpointV3,
  RelayCapability.recoveryV3GapFillV1,
] as const;

export interface HostRelayConnectionOptions {
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  monotonicNow?: () => number;
  pair: HostSocketPair;
  readyTimeoutMs?: number;
  recoveryDeadlineTickMs?: number;
  recoverySourceManager: RecoverySourceManager;
  session: TerminalSession;
  snapshotCheckpointManager: SnapshotCheckpointManager;
}

export class HostRelayConnection {
  readonly #pair: HostSocketPair;
  readonly #heartbeatIntervalMs: number;
  readonly #heartbeatTimeoutMs: number;
  readonly #monotonicNow: () => number;
  readonly #session: TerminalSession;
  readonly #scheduler: HostDeliveryScheduler;
  readonly #readyTimeoutMs: number;
  readonly #recoveryDeadlineTickMs: number;
  readonly #recoveryOwnerToken: RecoverySourceOwnerToken = {};
  readonly #recoverySourceManager: RecoverySourceManager;
  readonly #recoveryV3Enabled: boolean;
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
    const negotiated = new Set(options.pair.connection.negotiatedCapabilities ?? []);
    this.#recoveryV3Enabled = HOST_RECOVERY_V3_CAPABILITIES.every((capability) =>
      negotiated.has(capability),
    );
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
      snapshotCheckpointManager: options.snapshotCheckpointManager,
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
      if (!this.#recoveryV3Enabled) {
        this.#close(1002, "invalid relay control frame");
        return;
      }
      let recoveryFrame: RecoveryV3CloudToHostControlFrame;
      try {
        recoveryFrame = decodeControlFrame(encoded, RecoveryV3CloudToHostControlFrameSchema);
      } catch {
        this.#close(1002, "invalid relay control frame");
        return;
      }
      if (!this.#ready) throw new Error("relay sent Recovery v3 traffic before host-ready ACK");
      await this.#handleRecoveryControl(recoveryFrame);
      return;
    }

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
    if (this.#recoveryV3Enabled) this.#scheduleRecoveryDeadlineTick();
    this.#resolveReady();
  }

  async #handleRecoveryControl(frame: RecoveryV3CloudToHostControlFrame): Promise<void> {
    if (frame.type === "recovery-prepare") {
      const pairFence = this.#pairFence;
      const result = await this.#recoverySourceManager.prepare(
        this.#recoveryOwnerToken,
        frame,
        (encoded) =>
          !this.#closed &&
          pairFence !== null &&
          pairFence === this.#pairFence &&
          this.#scheduler.tryEnqueueRecoveryStartFence(encoded),
      );
      if (this.#closed || pairFence === null || pairFence !== this.#pairFence) return;
      if (result.status === "prepared") return;
      if (result.status === "rejected") {
        this.#sendRecoveryControl(result.rejection);
        return;
      }
      throw new Error(`Recovery source prepare ${result.status}: ${result.reason}`);
    }
    if (frame.type === "recovery-start-ready") {
      if (!this.#recoverySourceManager.startReady(this.#recoveryOwnerToken, frame)) {
        throw new Error("invalid Recovery source start-ready identity");
      }
      this.#drainRecovery(frame);
      return;
    }
    if (frame.type === "recovery-source-grant") {
      if (!this.#recoverySourceManager.grant(this.#recoveryOwnerToken, frame)) {
        throw new Error("invalid Recovery source grant identity");
      }
      this.#drainRecovery(frame);
      return;
    }
    if (frame.type === "recovery-source-received") {
      const result = this.#recoverySourceManager.received(this.#recoveryOwnerToken, frame);
      if (result.status === "invalid") {
        throw new Error(`invalid Recovery source receipt: ${result.reason}`);
      }
      if (result.status === "closed") this.#sendRecoveryControl(result.closed);
      if (result.status === "partial" && result.advanced) this.#drainRecovery(frame);
      return;
    }
    if (!this.#recoverySourceManager.reset(this.#recoveryOwnerToken, frame)) {
      throw new Error("invalid Recovery source reset identity");
    }
  }

  #drainRecovery(identity: RecoveryV3HostRoutingIdentity): void {
    this.#recoverySourceManager.drainGranted(
      this.#recoveryOwnerToken,
      {
        recoveryId: identity.recoveryId,
        connectionId: identity.connectionId,
        streamId: identity.streamId,
        deliveryGeneration: identity.deliveryGeneration,
      },
      { maxRecords: 1, maxWireBytes: HOST_RECOVERY_DRAIN_MAX_WIRE_BYTES },
      (encoded) => this.#sendRecoveryData(encoded),
    );
  }

  #sendRecoveryControl(frame: RecoveryV3HostToCloudControlFrame): void {
    this.#sendRawControl(encodeControlFrame(RecoveryV3HostToCloudControlFrameSchema.parse(frame)));
  }

  #scheduleRecoveryDeadlineTick(): void {
    if (this.#closed || !this.#recoveryV3Enabled) return;
    if (this.#recoveryDeadlineTimer !== undefined) clearTimeout(this.#recoveryDeadlineTimer);
    this.#recoveryDeadlineTimer = setTimeout(() => {
      this.#recoveryDeadlineTimer = undefined;
      if (this.#closed) return;
      try {
        const expiration = this.#recoverySourceManager.checkDeadlines(this.#recoveryOwnerToken)[0];
        if (expiration !== undefined) {
          this.#close(
            1012,
            `Recovery source ${expiration.identity.recoveryId} ${expiration.reason}`,
          );
          return;
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

  #sendRecoveryData(frame: Uint8Array): void {
    if (this.#closed || this.#pair.data.readyState !== WebSocket.OPEN) {
      throw new Error("Host data WebSocket is not open");
    }
    if (frame.byteLength > HOST_CANONICAL_QUEUE_LIMITS.maxBytes - this.#pair.data.bufferedAmount) {
      throw new Error("Host data WebSocket buffer exceeded");
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
    this.#recoverySourceManager.resetOwner(this.#recoveryOwnerToken);
    this.#scheduler.dispose(reason);
    this.#rejectReady(new Error(reason));
    this.#pair.close(code, reason.slice(0, 120));
    this.#resolveClosed();
  }
}
