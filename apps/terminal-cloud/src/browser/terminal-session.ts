import {
  ClientControlFrameSchema,
  CloudResourceIdSchema,
  ServerControlFrameSchema,
  type BrowserCapabilityRole,
  type ClientControlFrame,
  type ReplicaCursor,
  type ServerControlFrame,
} from "@zhongduan/protocol";
import {
  SessionCoordinator,
  type DeliveryState,
  type ReplicaHost,
  type ResyncReason,
  type SnapshotTransport,
} from "@zhongduan/session-client";

import {
  BrowserRelayAuthenticationError,
  BrowserRelayConnection,
  BrowserRelayConnectionFactory,
  type BrowserRelayConnectionEvents,
} from "./browser-relay-connection";
import { BrowserWriterSession } from "./browser-writer-session";
import type { CapabilityManager } from "./capability";
import { DeliveryAckCoalescer } from "./delivery-ack-coalescer";
import type { InputDispatcher } from "./input-dispatcher";

// CURRENT recovery serializes delivery barriers for at most 16 Browsers. A cold build may spend 5s
// encoding and 120s publishing before each Browser then consumes up to a 5s barrier window. Keep a
// 10s scheduling margin beyond that 205s service envelope. A later capability replaces this
// duplicated cross-app budget with generation-scoped progress.
export const ATTACH_START_TIMEOUT_MS = 5_000 + 120_000 + 16 * 5_000 + 10_000;
const DATA_REPLACEMENT_GRACE_MS = 350;
const MAX_RECONNECT_DELAY_MS = 10_000;
const SNAPSHOT_RETRY_BASE_MS = 2_000;
const SNAPSHOT_RETRY_MAX_MS = 30_000;

type SessionPhase =
  | "idle"
  | "connecting"
  | "attaching"
  | "restoring"
  | "live"
  | "offline"
  | "displaced"
  | "reconnecting"
  | "failed"
  | "closed";

export interface TerminalSessionSnapshot {
  attempt: number;
  controlConnected: boolean;
  controlOwnership: "observer" | "waiting" | "writer";
  dataConnected: boolean;
  deliveryState: DeliveryState;
  hostOnline: boolean;
  lastError: "authentication" | "connection" | "engine" | "protocol" | null;
  phase: SessionPhase;
  role: BrowserCapabilityRole;
}

export interface TerminalSessionOptions {
  capabilities: CapabilityManager;
  engineId: string;
  host: ReplicaHost;
  input: InputDispatcher;
  initialCursor?: ReplicaCursor;
  sessionId: string;
  snapshots: SnapshotTransport;
  fetch?: typeof fetch;
  createWebSocket?: (url: string) => WebSocket;
  makeWebSocketUrl?: (path: string) => string;
  now?: () => number;
  random?: () => number;
  setTimer?: (callback: () => void, delayMs: number) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
  storage?: Pick<Storage, "getItem" | "removeItem" | "setItem">;
}

function defaultWebSocketUrl(path: string): string {
  const url = new URL(path, globalThis.location.href);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.href;
}

function frameMatchesDelivery(
  frame: Extract<ServerControlFrame, { type: "replay-start" | "snapshot-manifest" }>,
  generation: bigint,
  streamId: number,
): boolean {
  return BigInt(frame.deliveryGeneration) === generation && frame.streamId === streamId;
}

/** Owns user-visible session/recovery state and composes transport, writer, and replica owners. */
export class TerminalSession {
  readonly #sessionId: string;
  readonly #engineId: string;
  readonly #capabilities: CapabilityManager;
  readonly #connectionFactory: BrowserRelayConnectionFactory;
  readonly #now: () => number;
  readonly #random: () => number;
  readonly #setTimer: NonNullable<TerminalSessionOptions["setTimer"]>;
  readonly #clearTimer: NonNullable<TerminalSessionOptions["clearTimer"]>;
  readonly #listeners = new Set<() => void>();
  readonly #deliveryAcks: DeliveryAckCoalescer;
  readonly #writer: BrowserWriterSession;
  readonly coordinator: SessionCoordinator;
  #snapshot: TerminalSessionSnapshot;
  #connection: BrowserRelayConnection | null = null;
  #connectionEpoch = 0;
  #connectPromise: Promise<void> | null = null;
  #connectAbort: AbortController | null = null;
  #needsReconnect = false;
  #reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  #dataFailureTimer: ReturnType<typeof setTimeout> | null = null;
  #attachStartTimer: ReturnType<typeof setTimeout> | null = null;
  #snapshotRetryTimer: ReturnType<typeof setTimeout> | null = null;
  #snapshotFailureCount = 0;
  #automaticReconnectBlocked = false;
  #closed = false;

  constructor(options: TerminalSessionOptions) {
    this.#sessionId = CloudResourceIdSchema.parse(options.sessionId);
    this.#engineId = options.engineId;
    this.#capabilities = options.capabilities;
    this.#now = options.now ?? Date.now;
    this.#random = options.random ?? Math.random;
    this.#setTimer =
      options.setTimer ?? ((callback, delayMs) => globalThis.setTimeout(callback, delayMs));
    this.#clearTimer = options.clearTimer ?? ((timer) => globalThis.clearTimeout(timer));
    this.#snapshot = {
      attempt: 0,
      controlConnected: false,
      controlOwnership: options.capabilities.role === "observer" ? "observer" : "waiting",
      dataConnected: false,
      deliveryState: "idle",
      hostOnline: true,
      lastError: null,
      phase: "idle",
      role: options.capabilities.role,
    };
    this.#connectionFactory = new BrowserRelayConnectionFactory({
      capabilities: this.#capabilities,
      clearTimer: this.#clearTimer,
      createWebSocket: options.createWebSocket ?? ((url) => new WebSocket(url)),
      fetch: options.fetch ?? ((input, init) => globalThis.fetch(input, init)),
      makeWebSocketUrl: options.makeWebSocketUrl ?? defaultWebSocketUrl,
      now: this.#now,
      sessionId: this.#sessionId,
      setTimer: this.#setTimer,
      ...(options.storage === undefined ? {} : { storage: options.storage }),
    });
    this.#deliveryAcks = new DeliveryAckCoalescer({
      clearTimer: this.#clearTimer,
      connectionEpoch: () => this.#connectionEpoch,
      deliveryGeneration: () => this.#connection?.deliveryGeneration ?? null,
      isConnectionCurrent: (connectionEpoch) => this.#isCurrentConnection(connectionEpoch),
      isDeliveryLive: () => this.coordinator.state === "live",
      protocolFailure: () => this.#protocolFailure(),
      send: (cursor) =>
        this.#sendControl(
          ClientControlFrameSchema.parse({
            type: "ack",
            sessionEpoch: cursor.sessionEpoch.toString(),
            deliveryGeneration: cursor.deliveryGeneration.toString(),
            eventSeq: cursor.lastEventSeq.toString(),
            nextPtyOffset: cursor.nextPtyOffset.toString(),
          }),
          true,
        ),
      setTimer: this.#setTimer,
    });
    this.coordinator = new SessionCoordinator({
      host: options.host,
      ...(options.initialCursor === undefined ? {} : { initialCursor: options.initialCursor }),
      snapshots: options.snapshots,
      onAcknowledge: (cursor) => this.#deliveryAcks.acknowledge(cursor),
      onReplicaProgress: () => this.#syncDeliveryState(),
      onResync: (reason) => this.#handleCoordinatorResync(reason),
    });
    this.#writer = new BrowserWriterSession({
      clearTimer: this.#clearTimer,
      connectionEpoch: () => this.#connectionEpoch,
      input: options.input,
      isConnectionCurrent: (connectionEpoch) => this.#isCurrentConnection(connectionEpoch),
      onControlReplacementRequired: () => this.#failFullConnection(),
      onProtocolFailure: () => this.#protocolFailure(),
      role: options.capabilities.role,
      sendControl: (frame, critical) => this.#sendControl(frame, critical),
      sendInput: (frame) => this.#connection?.sendInput(frame) ?? "proven-not-accepted",
      setTimer: this.#setTimer,
    });
  }

  get snapshot(): TerminalSessionSnapshot {
    return this.#snapshot;
  }

  subscribe(listener: () => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  start(): void {
    if (this.#closed || this.#automaticReconnectBlocked || this.#connectPromise !== null) return;
    this.#capabilities.start();
    this.#startFullConnection(false);
  }

  reconnectNow(): void {
    if (this.#closed) return;
    this.#automaticReconnectBlocked = false;
    this.#cancelSnapshotRetry(true);
    this.#cancelReconnectTimer();
    this.#invalidateFullConnection();
    if (this.#connectPromise === null) this.#startFullConnection(true);
    else this.#scheduleReconnect();
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#needsReconnect = false;
    this.#cancelReconnectTimer();
    this.#cancelSnapshotRetry(true);
    this.#invalidateFullConnection();
    this.#writer.close();
    this.#capabilities.stop();
    this.coordinator.close();
    this.#update({
      controlConnected: false,
      dataConnected: false,
      deliveryState: "closed",
      phase: "closed",
    });
  }

  #startFullConnection(manual: boolean): void {
    if (this.#closed || this.#automaticReconnectBlocked || this.#connectPromise !== null) return;
    this.#needsReconnect = false;
    const attempt = this.#snapshot.attempt + 1;
    this.#update({
      attempt,
      lastError: null,
      phase: manual || attempt > 1 ? "reconnecting" : "connecting",
    });
    const abort = new AbortController();
    this.#connectAbort = abort;
    this.#connectPromise = this.#connectFull(abort.signal)
      .catch((error: unknown) => {
        if (this.#closed || this.#automaticReconnectBlocked) return;
        this.#invalidateFullConnection();
        this.#update({
          lastError:
            error instanceof BrowserRelayAuthenticationError ? "authentication" : "connection",
          phase: error instanceof BrowserRelayAuthenticationError ? "failed" : "reconnecting",
        });
        if (!(error instanceof BrowserRelayAuthenticationError)) this.#scheduleReconnect();
      })
      .finally(() => {
        this.#connectPromise = null;
        if (this.#connectAbort === abort) this.#connectAbort = null;
        if (this.#needsReconnect && !this.#closed && !this.#automaticReconnectBlocked) {
          this.#scheduleReconnect();
        }
      });
  }

  async #connectFull(signal: AbortSignal): Promise<void> {
    const connectionEpoch = ++this.#connectionEpoch;
    this.#writer.beginConnection();
    this.#cancelDataFailureTimer();

    const connection = await this.#connectionFactory.create(
      connectionEpoch,
      this.#connectionEvents(),
      signal,
    );
    if (!this.#isCurrentConnection(connectionEpoch)) {
      connection.close();
      return;
    }
    this.#writer.setInputAdmissionEnabled(connection.inputAdmissionEnabled);

    // Activation is one synchronous fence before any replacement callback can run.
    this.coordinator.fenceDeliveryGeneration(connection.deliveryGeneration);
    this.#connection?.close();
    this.#connection = connection;
    this.#update({ controlConnected: false, dataConnected: false });

    await connection.openControl(signal);
    if (!this.#isCurrentRelayConnection(connection)) {
      connection.close();
      return;
    }
    this.#update({ controlConnected: true });

    await connection.openInitialData(signal);
    if (!this.#isCurrentRelayConnection(connection)) {
      connection.close();
      return;
    }
    this.#update({ dataConnected: true, phase: "attaching" });
    this.#sendAttach();
  }

  #connectionEvents(): BrowserRelayConnectionEvents {
    return {
      controlClosed: (connection, externallyReplaced) =>
        this.#handleControlClosed(connection, externallyReplaced),
      controlMessage: (connection, data) => {
        if (this.#isCurrentRelayConnection(connection)) this.#handleControlMessage(data);
      },
      dataClosed: (connection) => this.#handleDataClosed(connection),
      dataFrames: (connection, encodedFrames) => {
        if (!this.#isCurrentRelayConnection(connection)) return;
        for (const encoded of encodedFrames) this.coordinator.acceptData(encoded);
        this.#syncDeliveryState();
      },
      protocolFailure: (connection) => {
        if (this.#isCurrentRelayConnection(connection)) this.#protocolFailure();
      },
      transportFailure: (connection) => {
        if (this.#isCurrentRelayConnection(connection)) this.#failFullConnection();
      },
    };
  }

  #handleControlClosed(connection: BrowserRelayConnection, externallyReplaced: boolean): void {
    if (!this.#isCurrentRelayConnection(connection)) return;
    this.#update({ controlConnected: false });
    this.#invalidateFullConnection();
    if (externallyReplaced) {
      // Another control connection for this stable Browser client is now authoritative. Automatic
      // reconnect would let two tabs repeatedly displace each other; reconnectNow() explicitly
      // reclaims control.
      this.#automaticReconnectBlocked = true;
      this.#needsReconnect = false;
      this.#update({
        controlOwnership: this.#capabilities.role === "observer" ? "observer" : "waiting",
        lastError: null,
        phase: "displaced",
      });
      return;
    }
    this.#scheduleReconnect();
  }

  #handleDataClosed(connection: BrowserRelayConnection): void {
    if (!this.#isCurrentRelayConnection(connection)) return;
    this.#writer.setReplicaCurrent(false);
    this.#update({ dataConnected: false, phase: "reconnecting" });
    this.#cancelDataFailureTimer();
    this.#dataFailureTimer = this.#setTimer(() => {
      this.#dataFailureTimer = null;
      if (this.#snapshotRetryTimer !== null) return;
      this.#invalidateFullConnection();
      this.#scheduleReconnect();
    }, DATA_REPLACEMENT_GRACE_MS);
  }

  #sendAttach(): void {
    const connection = this.#connection;
    if (connection === null) {
      this.#protocolFailure();
      return;
    }
    const generation = connection.deliveryGeneration;
    const cursor = this.coordinator.activeCursor;
    const frame =
      cursor === null
        ? {
            type: "attach" as const,
            engineId: this.#engineId,
            deliveryGeneration: generation.toString(),
            hasLiveReplica: false as const,
          }
        : {
            type: "attach" as const,
            engineId: this.#engineId,
            deliveryGeneration: generation.toString(),
            hasLiveReplica: true as const,
            lastSessionEpoch: cursor.sessionEpoch.toString(),
            lastEventSeq: cursor.lastEventSeq.toString(),
            nextPtyOffset: cursor.nextPtyOffset.toString(),
          };
    if (this.#sendControl(ClientControlFrameSchema.parse(frame), true)) {
      this.#deliveryAcks.reset(
        cursor === null ? null : { ...cursor, deliveryGeneration: generation },
      );
      this.#startAttachStartWatchdog(connection, generation);
    }
  }

  #handleControlMessage(data: unknown): void {
    if (typeof data !== "string") {
      this.#protocolFailure();
      return;
    }
    let frame: ServerControlFrame;
    try {
      frame = ServerControlFrameSchema.parse(JSON.parse(data));
    } catch {
      this.#protocolFailure();
      return;
    }
    switch (frame.type) {
      case "welcome":
        this.#acceptWelcome(frame);
        return;
      case "input-ack":
        this.#writer.acceptAcknowledgement(frame);
        return;
      case "writer-lease-status":
        {
          const controlOwnership = this.#writer.acceptLeaseStatus(frame);
          if (controlOwnership !== undefined) this.#update({ controlOwnership });
        }
        return;
      case "host-offline":
        this.#update({ hostOnline: false, phase: "offline" });
        return;
      case "resync-required":
        this.#handleResyncRequired(frame);
        return;
      case "replay-start":
        if (!this.#matchesCurrentDelivery(frame)) return;
        this.#update({ hostOnline: true });
        this.coordinator.startWarmReplay(frame);
        this.#syncDeliveryState();
        return;
      case "snapshot-manifest":
        if (!this.#matchesCurrentDelivery(frame)) return;
        this.#cancelAttachStartWatchdog();
        this.#update({ hostOnline: true, phase: "restoring" });
        void this.coordinator
          .startSnapshot(frame)
          .then(() => this.#syncDeliveryState())
          .catch(() => this.#protocolFailure());
        return;
    }
  }

  #acceptWelcome(frame: Extract<ServerControlFrame, { type: "welcome" }>): void {
    const connection = this.#connection;
    if (
      connection === null ||
      frame.connectionId !== connection.connectionId ||
      frame.streamId !== connection.streamId ||
      BigInt(frame.deliveryGeneration) !== connection.deliveryGeneration ||
      frame.engineId !== this.#engineId
    ) {
      this.#protocolFailure(frame.engineId !== this.#engineId ? "engine" : "protocol");
      return;
    }
    const controlOwnership = this.#writer.acceptWelcome(frame);
    if (controlOwnership === undefined) return;
    this.#update({ controlOwnership, hostOnline: true, phase: "attaching" });
  }

  #handleResyncRequired(frame: Extract<ServerControlFrame, { type: "resync-required" }>): void {
    if (frame.reason === "engine-mismatch") {
      this.#terminalFailure("engine");
      return;
    }
    const generation = BigInt(frame.deliveryGeneration);
    const connection = this.#connection;
    if (
      frame.dataTicket !== undefined &&
      (connection === null ||
        generation <= connection.deliveryGeneration ||
        frame.expiresAt! <= this.#now())
    ) {
      this.#protocolFailure();
      return;
    }
    if (frame.dataTicket !== undefined) this.#cancelSnapshotRetry(false);
    this.#cancelAttachStartWatchdog();
    this.#writer.setReplicaCurrent(false);
    this.coordinator.fenceDeliveryGeneration(generation);
    this.#syncDeliveryState();
    if (frame.dataTicket === undefined) {
      this.#invalidateFullConnection();
      this.#scheduleReconnect();
      return;
    }
    this.#cancelDataFailureTimer();
    void this.#replaceData(frame.dataTicket, generation);
  }

  async #replaceData(ticket: string, generation: bigint): Promise<void> {
    const connection = this.#connection;
    if (
      connection === null ||
      !this.#isCurrentRelayConnection(connection) ||
      !connection.controlConnected
    ) {
      this.#invalidateFullConnection();
      this.#scheduleReconnect();
      return;
    }
    // Fence first, then invalidate the old data incarnation inside the connection owner.
    this.coordinator.fenceDeliveryGeneration(generation);
    this.#deliveryAcks.reset();
    this.#writer.noteDataTransportReplacement();
    this.#writer.setReplicaCurrent(false);
    this.#update({ dataConnected: false, phase: "reconnecting" });
    try {
      const replaced = await connection.replaceData(ticket, generation);
      if (!replaced || !this.#isCurrentRelayConnection(connection)) return;
      this.#update({ dataConnected: true, phase: "attaching" });
      this.#sendAttach();
    } catch {
      if (
        !this.#isCurrentRelayConnection(connection) ||
        !connection.isCurrentGeneration(generation)
      ) {
        return;
      }
      if (this.#snapshotRetryTimer !== null) return;
      this.#invalidateFullConnection();
      this.#scheduleReconnect();
    }
  }

  #matchesCurrentDelivery(
    frame: Extract<ServerControlFrame, { type: "replay-start" | "snapshot-manifest" }>,
  ): boolean {
    const connection = this.#connection;
    if (connection === null) return false;
    const frameGeneration = BigInt(frame.deliveryGeneration);
    if (frameGeneration < connection.deliveryGeneration) return false;
    if (!frameMatchesDelivery(frame, connection.deliveryGeneration, connection.streamId)) {
      this.#protocolFailure();
      return false;
    }
    return true;
  }

  #sendControl(frame: ClientControlFrame, critical = false): boolean {
    const connection = this.#connection;
    if (connection !== null) return connection.sendControl(frame, critical);
    if (critical) this.#failFullConnection();
    return false;
  }

  #syncDeliveryState(): void {
    const deliveryState = this.coordinator.state;
    let phase = this.#snapshot.phase;
    if (deliveryState === "live") {
      this.#cancelAttachStartWatchdog();
      phase = "live";
      this.#cancelSnapshotRetry(true);
      this.#writer.setReplicaCurrent(true);
    } else {
      this.#writer.setReplicaCurrent(false);
      if (deliveryState === "restoring" || deliveryState === "replaying") phase = "restoring";
      else if (deliveryState === "resyncing") phase = "reconnecting";
    }
    this.#update({ deliveryState, phase });
  }

  #handleCoordinatorResync(reason: ResyncReason): void {
    if (reason === "engine-mismatch") {
      this.#terminalFailure("engine");
      return;
    }
    if (
      reason === "restore-failed" ||
      (this.#snapshot.phase === "restoring" &&
        (reason === "journal-gap" || reason === "slow-client"))
    ) {
      this.#scheduleSnapshotRetry();
      return;
    }
    this.#update({ lastError: "protocol", phase: "reconnecting" });
    this.#invalidateFullConnection();
    this.#scheduleReconnect();
  }

  #protocolFailure(kind: "engine" | "protocol" = "protocol"): void {
    if (kind === "engine") {
      this.#terminalFailure("engine");
      return;
    }
    this.#update({ lastError: kind, phase: "reconnecting" });
    this.#invalidateFullConnection();
    this.#scheduleReconnect();
  }

  #scheduleReconnect(): void {
    if (this.#closed || this.#automaticReconnectBlocked) return;
    this.#needsReconnect = true;
    if (this.#snapshotRetryTimer !== null) return;
    if (this.#reconnectTimer !== null || this.#connectPromise !== null) return;
    this.#writer.detachInputTransport();
    const exponent = Math.min(6, Math.max(0, this.#snapshot.attempt - 1));
    const base = Math.min(MAX_RECONNECT_DELAY_MS, 250 * 2 ** exponent);
    const delay = Math.max(0, Math.round(base * (0.8 + this.#random() * 0.4)));
    this.#update({ phase: "reconnecting" });
    this.#reconnectTimer = this.#setTimer(() => {
      this.#reconnectTimer = null;
      if (this.#connectPromise !== null) return;
      this.#needsReconnect = false;
      this.#startFullConnection(false);
    }, delay);
  }

  #startAttachStartWatchdog(connection: BrowserRelayConnection, generation: bigint): void {
    this.#cancelAttachStartWatchdog();
    this.#attachStartTimer = this.#setTimer(() => {
      this.#attachStartTimer = null;
      if (
        !this.#isCurrentRelayConnection(connection) ||
        !connection.isCurrentGeneration(generation) ||
        this.coordinator.state === "live"
      ) {
        return;
      }
      this.#update({ lastError: "connection", phase: "reconnecting" });
      this.#failFullConnection();
    }, ATTACH_START_TIMEOUT_MS);
  }

  #cancelAttachStartWatchdog(): void {
    if (this.#attachStartTimer !== null) this.#clearTimer(this.#attachStartTimer);
    this.#attachStartTimer = null;
  }

  #cancelReconnectTimer(): void {
    if (this.#reconnectTimer !== null) this.#clearTimer(this.#reconnectTimer);
    this.#reconnectTimer = null;
  }

  #failFullConnection(): void {
    this.#invalidateFullConnection();
    this.#scheduleReconnect();
  }

  #terminalFailure(kind: "engine"): void {
    this.#automaticReconnectBlocked = true;
    this.#needsReconnect = false;
    this.#cancelReconnectTimer();
    this.#cancelSnapshotRetry(false);
    this.#invalidateFullConnection();
    this.#update({ lastError: kind, phase: "failed" });
  }

  #invalidateFullConnection(): void {
    ++this.#connectionEpoch;
    this.#connectAbort?.abort(new DOMException("connection replaced", "AbortError"));
    this.#connectAbort = null;
    this.#cancelDataFailureTimer();
    this.#cancelAttachStartWatchdog();
    this.#deliveryAcks.reset();
    this.#writer.disconnect();
    this.#connection?.close();
    this.#connection = null;
    this.#update({ controlConnected: false, dataConnected: false });
  }

  #scheduleSnapshotRetry(): void {
    if (this.#closed || this.#automaticReconnectBlocked || this.#snapshotRetryTimer !== null) {
      return;
    }
    const exponent = Math.min(4, this.#snapshotFailureCount++);
    const base = Math.min(SNAPSHOT_RETRY_MAX_MS, SNAPSHOT_RETRY_BASE_MS * 2 ** exponent);
    const delay = Math.max(0, Math.round(base * (0.8 + this.#random() * 0.4)));
    this.#writer.setReplicaCurrent(false);
    this.#update({ lastError: "connection", phase: "reconnecting" });
    this.#snapshotRetryTimer = this.#setTimer(() => {
      this.#snapshotRetryTimer = null;
      if (this.#closed || this.#automaticReconnectBlocked) return;
      this.#invalidateFullConnection();
      if (this.#connectPromise === null) this.#startFullConnection(false);
      else this.#needsReconnect = true;
    }, delay);
  }

  #cancelSnapshotRetry(resetFailures: boolean): void {
    if (this.#snapshotRetryTimer !== null) this.#clearTimer(this.#snapshotRetryTimer);
    this.#snapshotRetryTimer = null;
    if (resetFailures) this.#snapshotFailureCount = 0;
  }

  #cancelDataFailureTimer(): void {
    if (this.#dataFailureTimer !== null) this.#clearTimer(this.#dataFailureTimer);
    this.#dataFailureTimer = null;
  }

  #isCurrentConnection(connectionEpoch: number): boolean {
    return !this.#closed && connectionEpoch === this.#connectionEpoch;
  }

  #isCurrentRelayConnection(connection: BrowserRelayConnection): boolean {
    return this.#connection === connection && this.#isCurrentConnection(connection.connectionEpoch);
  }

  #update(patch: Partial<TerminalSessionSnapshot>): void {
    this.#snapshot = { ...this.#snapshot, ...patch };
    for (const listener of this.#listeners) listener();
  }
}
