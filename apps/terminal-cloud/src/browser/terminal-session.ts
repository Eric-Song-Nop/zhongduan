import {
  ClientControlFrameSchema,
  CloudResourceIdSchema,
  ConnectionSetRequestSchema,
  ConnectionSetResponseSchema,
  ServerControlFrameSchema,
  decodeDataFrame,
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

import type { CapabilityManager } from "./capability";
import type { InputDispatcher } from "./input-dispatcher";

const CLIENT_ID_PREFIX = "zhongduan:browser-client:";
const SOCKET_OPEN_TIMEOUT_MS = 10_000;
const DATA_REPLACEMENT_GRACE_MS = 350;
const HEARTBEAT_INTERVAL_MS = 15_000;
const HEARTBEAT_TIMEOUT_MS = 45_000;
const MAX_RECONNECT_DELAY_MS = 10_000;
const SNAPSHOT_RETRY_BASE_MS = 2_000;
const SNAPSHOT_RETRY_MAX_MS = 30_000;
// A legal 1 MiB input can expand to six bytes per byte when JSON escapes control characters.
const MAX_CONTROL_FRAME_BYTES = 6 * 1024 * 1024 + 4_096;
const MAX_CONTROL_QUEUE_BYTES = MAX_CONTROL_FRAME_BYTES;
const textEncoder = new TextEncoder();

type SessionPhase =
  | "idle"
  | "connecting"
  | "attaching"
  | "restoring"
  | "live"
  | "offline"
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

function clientStorageKey(sessionId: string): string {
  return `${CLIENT_ID_PREFIX}${sessionId}`;
}

function readClientId(
  storage: Pick<Storage, "getItem" | "removeItem"> | undefined,
  sessionId: string,
): string | undefined {
  if (storage === undefined) return undefined;
  let value: string | null;
  try {
    value = storage.getItem(clientStorageKey(sessionId));
  } catch {
    return undefined;
  }
  if (value === null) return undefined;
  if (!CloudResourceIdSchema.safeParse(value).success) {
    try {
      storage.removeItem(clientStorageKey(sessionId));
    } catch {
      // Browser storage is an optional reconnect optimization.
    }
    return undefined;
  }
  return value;
}

function frameMatchesDelivery(
  frame: Extract<ServerControlFrame, { type: "replay-start" | "snapshot-manifest" }>,
  generation: bigint,
  streamId: number,
): boolean {
  return BigInt(frame.deliveryGeneration) === generation && frame.streamId === streamId;
}

export class TerminalSession {
  readonly #sessionId: string;
  readonly #engineId: string;
  readonly #capabilities: CapabilityManager;
  readonly #input: InputDispatcher;
  readonly #fetch: typeof fetch;
  readonly #createWebSocket: NonNullable<TerminalSessionOptions["createWebSocket"]>;
  readonly #makeWebSocketUrl: NonNullable<TerminalSessionOptions["makeWebSocketUrl"]>;
  readonly #now: () => number;
  readonly #random: () => number;
  readonly #setTimer: NonNullable<TerminalSessionOptions["setTimer"]>;
  readonly #clearTimer: NonNullable<TerminalSessionOptions["clearTimer"]>;
  readonly #storage?: TerminalSessionOptions["storage"];
  readonly #listeners = new Set<() => void>();
  readonly #intentionalSockets = new WeakSet<WebSocket>();
  readonly coordinator: SessionCoordinator;
  #snapshot: TerminalSessionSnapshot;
  #control: WebSocket | null = null;
  #data: WebSocket | null = null;
  #connectionEpoch = 0;
  #dataEpoch = 0;
  #generation: bigint | null = null;
  #streamId = 0;
  #connectionId: string | null = null;
  #writerLease: string | null = null;
  #clientId: string | undefined;
  #connectPromise: Promise<void> | null = null;
  #connectAbort: AbortController | null = null;
  #replacementAbort: AbortController | null = null;
  #needsReconnect = false;
  #reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  #dataFailureTimer: ReturnType<typeof setTimeout> | null = null;
  #heartbeatTimer: ReturnType<typeof setTimeout> | null = null;
  #snapshotRetryTimer: ReturnType<typeof setTimeout> | null = null;
  #snapshotFailureCount = 0;
  #lastPongAt = 0;
  #automaticReconnectBlocked = false;
  #closed = false;

  constructor(options: TerminalSessionOptions) {
    this.#sessionId = CloudResourceIdSchema.parse(options.sessionId);
    this.#engineId = options.engineId;
    this.#capabilities = options.capabilities;
    this.#input = options.input;
    this.#fetch = options.fetch ?? fetch;
    this.#createWebSocket = options.createWebSocket ?? ((url) => new WebSocket(url));
    this.#makeWebSocketUrl = options.makeWebSocketUrl ?? defaultWebSocketUrl;
    this.#now = options.now ?? Date.now;
    this.#random = options.random ?? Math.random;
    this.#setTimer = options.setTimer ?? setTimeout;
    this.#clearTimer = options.clearTimer ?? clearTimeout;
    this.#storage = options.storage;
    this.#clientId = readClientId(this.#storage, this.#sessionId);
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
    this.coordinator = new SessionCoordinator({
      host: options.host,
      ...(options.initialCursor === undefined ? {} : { initialCursor: options.initialCursor }),
      snapshots: options.snapshots,
      onAcknowledge: (cursor) => this.#acknowledge(cursor),
      onReplicaProgress: () => this.#syncDeliveryState(),
      onResync: (reason) => this.#handleCoordinatorResync(reason),
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
          lastError: error instanceof AuthenticationError ? "authentication" : "connection",
          phase: error instanceof AuthenticationError ? "failed" : "reconnecting",
        });
        if (!(error instanceof AuthenticationError)) this.#scheduleReconnect();
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
    ++this.#dataEpoch;
    this.#input.detachTransport();
    this.#writerLease = null;
    this.#stopHeartbeat();
    this.#cancelDataFailureTimer();

    const response = await this.#createConnectionSet(signal);
    if (!this.#isCurrentConnection(connectionEpoch)) return;
    const generation = BigInt(response.deliveryGeneration);

    // Activation is one synchronous fence before any replacement callback can run.
    this.coordinator.fenceDeliveryGeneration(generation);
    ++this.#dataEpoch;
    this.#generation = generation;
    this.#streamId = response.streamId;
    this.#connectionId = response.connectionId;
    this.#closeSocket(this.#control);
    this.#closeSocket(this.#data);
    this.#control = null;
    this.#data = null;
    this.#update({ controlConnected: false, dataConnected: false });

    const control = await this.#openControl(response.controlTicket, connectionEpoch, signal);
    if (!this.#isCurrentConnection(connectionEpoch)) {
      this.#closeSocket(control);
      return;
    }
    this.#control = control;
    this.#lastPongAt = this.#now();
    this.#startHeartbeat(connectionEpoch);
    this.#update({ controlConnected: true });

    const dataEpoch = this.#dataEpoch;
    const data = await this.#openData(
      response.dataTicket,
      connectionEpoch,
      dataEpoch,
      generation,
      response.streamId,
      signal,
    );
    if (!this.#isCurrentData(connectionEpoch, dataEpoch, generation)) {
      this.#closeSocket(data);
      return;
    }
    this.#data = data;
    this.#update({ dataConnected: true, phase: "attaching" });
    this.#sendAttach();
  }

  async #createConnectionSet(signal: AbortSignal) {
    const request = ConnectionSetRequestSchema.parse(
      this.#clientId === undefined ? {} : { clientId: this.#clientId },
    );
    const response = await this.#fetch(
      `/api/v1/sessions/${encodeURIComponent(this.#sessionId)}/connection-sets`,
      {
        method: "POST",
        credentials: "same-origin",
        redirect: "error",
        headers: {
          ...this.#capabilities.authorizationHeaders(),
          "Content-Type": "application/json",
        },
        body: JSON.stringify(request),
        signal,
      },
    );
    if (response.status === 401 || response.status === 403) throw new AuthenticationError();
    if (!response.ok) throw new Error("connection set request failed");
    const connectionSet = ConnectionSetResponseSchema.parse(await response.json());
    if (connectionSet.clientId === null || connectionSet.streamId === 0) {
      throw new Error("browser connection set has no delivery identity");
    }
    this.#clientId = connectionSet.clientId;
    try {
      this.#storage?.setItem(clientStorageKey(this.#sessionId), connectionSet.clientId);
    } catch {
      // The in-memory identity remains valid when storage is unavailable.
    }
    return connectionSet;
  }

  async #openControl(
    ticket: string,
    connectionEpoch: number,
    signal: AbortSignal,
  ): Promise<WebSocket> {
    const socket = this.#createWebSocket(
      this.#makeWebSocketUrl(
        `/api/v1/sessions/${encodeURIComponent(this.#sessionId)}/ws/control?ticket=${encodeURIComponent(ticket)}`,
      ),
    );
    socket.addEventListener("message", (event) => {
      if (!this.#isCurrentConnection(connectionEpoch) || socket !== this.#control) return;
      this.#handleControlMessage(event.data);
    });
    socket.addEventListener("close", () => {
      if (this.#intentionalSockets.has(socket) || !this.#isCurrentConnection(connectionEpoch))
        return;
      this.#update({ controlConnected: false });
      this.#invalidateFullConnection();
      this.#scheduleReconnect();
    });
    return this.#awaitOpen(socket, signal);
  }

  async #openData(
    ticket: string,
    connectionEpoch: number,
    dataEpoch: number,
    generation: bigint,
    streamId: number,
    signal?: AbortSignal,
  ): Promise<WebSocket> {
    const socket = this.#createWebSocket(
      this.#makeWebSocketUrl(
        `/api/v1/sessions/${encodeURIComponent(this.#sessionId)}/ws/data?ticket=${encodeURIComponent(ticket)}`,
      ),
    );
    socket.binaryType = "arraybuffer";
    socket.addEventListener("message", (event) => {
      if (!this.#isCurrentData(connectionEpoch, dataEpoch, generation) || socket !== this.#data) {
        return;
      }
      if (!(event.data instanceof ArrayBuffer)) {
        this.#protocolFailure();
        return;
      }
      try {
        const frame = decodeDataFrame(event.data);
        if (frame.deliveryGeneration !== generation || frame.streamId !== streamId) {
          this.#protocolFailure();
          return;
        }
        this.coordinator.acceptData(event.data);
        this.#syncDeliveryState();
      } catch {
        this.#protocolFailure();
      }
    });
    socket.addEventListener("close", () => {
      if (
        this.#intentionalSockets.has(socket) ||
        !this.#isCurrentData(connectionEpoch, dataEpoch, generation)
      ) {
        return;
      }
      this.#data = null;
      this.#input.setReplicaCurrent(false);
      this.#update({ dataConnected: false, phase: "reconnecting" });
      this.#cancelDataFailureTimer();
      this.#dataFailureTimer = this.#setTimer(() => {
        this.#dataFailureTimer = null;
        if (this.#snapshotRetryTimer !== null) return;
        this.#invalidateFullConnection();
        this.#scheduleReconnect();
      }, DATA_REPLACEMENT_GRACE_MS);
    });
    return this.#awaitOpen(socket, signal);
  }

  #awaitOpen(socket: WebSocket, signal?: AbortSignal): Promise<WebSocket> {
    return new Promise((resolve, reject) => {
      const timeout = this.#setTimer(() => {
        cleanup();
        this.#closeSocket(socket);
        reject(new Error("websocket open timed out"));
      }, SOCKET_OPEN_TIMEOUT_MS);
      const cleanup = () => {
        this.#clearTimer(timeout);
        socket.removeEventListener("open", onOpen);
        socket.removeEventListener("close", onClose);
        socket.removeEventListener("error", onError);
        signal?.removeEventListener("abort", onAbort);
      };
      const onOpen = () => {
        cleanup();
        resolve(socket);
      };
      const onClose = () => {
        cleanup();
        reject(new Error("websocket closed before opening"));
      };
      const onError = () => {
        cleanup();
        reject(new Error("websocket failed before opening"));
      };
      const onAbort = () => {
        cleanup();
        this.#closeSocket(socket);
        reject(signal?.reason ?? new DOMException("connection attempt aborted", "AbortError"));
      };
      socket.addEventListener("open", onOpen);
      socket.addEventListener("close", onClose);
      socket.addEventListener("error", onError);
      signal?.addEventListener("abort", onAbort, { once: true });
      if (signal?.aborted) onAbort();
    });
  }

  #sendAttach(): void {
    const cursor = this.coordinator.activeCursor;
    const frame =
      cursor === null
        ? { type: "attach" as const, engineId: this.#engineId, hasLiveReplica: false as const }
        : {
            type: "attach" as const,
            engineId: this.#engineId,
            hasLiveReplica: true as const,
            lastSessionEpoch: cursor.sessionEpoch.toString(),
            lastEventSeq: cursor.lastEventSeq.toString(),
            nextPtyOffset: cursor.nextPtyOffset.toString(),
          };
    this.#sendControl(ClientControlFrameSchema.parse(frame), true);
  }

  #handleControlMessage(data: unknown): void {
    if (data === "pong") {
      this.#lastPongAt = this.#now();
      return;
    }
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
        this.#input.acceptAcknowledgement(frame);
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
        this.#update({ hostOnline: true, phase: "restoring" });
        void this.coordinator
          .startSnapshot(frame)
          .then(() => this.#syncDeliveryState())
          .catch(() => this.#protocolFailure());
        return;
    }
  }

  #acceptWelcome(frame: Extract<ServerControlFrame, { type: "welcome" }>): void {
    const generation = this.#generation;
    if (
      generation === null ||
      frame.connectionId !== this.#connectionId ||
      frame.streamId !== this.#streamId ||
      BigInt(frame.deliveryGeneration) !== generation ||
      frame.engineId !== this.#engineId
    ) {
      this.#protocolFailure(frame.engineId !== this.#engineId ? "engine" : "protocol");
      return;
    }
    const previousLease = this.#writerLease;
    this.#writerLease = frame.writerLease ?? null;
    if (!this.#input.status.connected || previousLease !== this.#writerLease) {
      this.#input.attachTransport((inputFrame) => {
        const sent = this.#sendControl(inputFrame);
        if (!sent) this.#failFullConnection();
        return sent;
      }, this.#writerLease ?? undefined);
    }
    this.#update({
      controlOwnership:
        this.#capabilities.role === "observer"
          ? "observer"
          : this.#writerLease === null
            ? "waiting"
            : "writer",
      hostOnline: true,
      phase: "attaching",
    });
  }

  #handleResyncRequired(frame: Extract<ServerControlFrame, { type: "resync-required" }>): void {
    if (frame.reason === "engine-mismatch") {
      this.#terminalFailure("engine");
      return;
    }
    const generation = BigInt(frame.deliveryGeneration);
    if (
      frame.dataTicket !== undefined &&
      (this.#generation === null ||
        generation <= this.#generation ||
        frame.expiresAt! <= this.#now())
    ) {
      this.#protocolFailure();
      return;
    }
    if (frame.dataTicket !== undefined) this.#cancelSnapshotRetry(false);
    this.#input.setReplicaCurrent(false);
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
    const connectionEpoch = this.#connectionEpoch;
    if (!this.#isCurrentConnection(connectionEpoch) || this.#control?.readyState !== 1) {
      this.#invalidateFullConnection();
      this.#scheduleReconnect();
      return;
    }
    this.#abortDataReplacement();
    // Fence first, then invalidate the old transport callback before opening replacement data.
    this.coordinator.fenceDeliveryGeneration(generation);
    const dataEpoch = ++this.#dataEpoch;
    const abort = new AbortController();
    this.#replacementAbort = abort;
    this.#generation = generation;
    this.#input.setReplicaCurrent(false);
    this.#closeSocket(this.#data);
    this.#data = null;
    this.#update({ dataConnected: false, phase: "reconnecting" });
    try {
      const data = await this.#openData(
        ticket,
        connectionEpoch,
        dataEpoch,
        generation,
        this.#streamId,
        abort.signal,
      );
      if (!this.#isCurrentData(connectionEpoch, dataEpoch, generation)) {
        this.#closeSocket(data);
        return;
      }
      this.#data = data;
      this.#update({ dataConnected: true, phase: "attaching" });
      this.#sendAttach();
    } catch {
      if (abort.signal.aborted || !this.#isCurrentData(connectionEpoch, dataEpoch, generation)) {
        return;
      }
      if (this.#snapshotRetryTimer !== null) return;
      this.#invalidateFullConnection();
      this.#scheduleReconnect();
    } finally {
      if (this.#replacementAbort === abort) this.#replacementAbort = null;
    }
  }

  #matchesCurrentDelivery(
    frame: Extract<ServerControlFrame, { type: "replay-start" | "snapshot-manifest" }>,
  ): boolean {
    const generation = this.#generation;
    if (generation === null) return false;
    const frameGeneration = BigInt(frame.deliveryGeneration);
    if (frameGeneration < generation) return false;
    if (!frameMatchesDelivery(frame, generation, this.#streamId)) {
      this.#protocolFailure();
      return false;
    }
    return true;
  }

  #acknowledge(cursor: ReplicaCursor): void {
    if (cursor.deliveryGeneration !== this.#generation) return;
    this.#sendControl(
      ClientControlFrameSchema.parse({
        type: "ack",
        sessionEpoch: cursor.sessionEpoch.toString(),
        deliveryGeneration: cursor.deliveryGeneration.toString(),
        eventSeq: cursor.lastEventSeq.toString(),
        nextPtyOffset: cursor.nextPtyOffset.toString(),
      }),
      true,
    );
  }

  #sendControl(frame: ClientControlFrame, critical = false): boolean {
    const control = this.#control;
    try {
      const payload = JSON.stringify(frame);
      const payloadBytes = textEncoder.encode(payload).byteLength;
      if (
        control === null ||
        control.readyState !== 1 ||
        payloadBytes > MAX_CONTROL_FRAME_BYTES ||
        control.bufferedAmount + payloadBytes > MAX_CONTROL_QUEUE_BYTES
      ) {
        if (critical) this.#failFullConnection();
        return false;
      }
      control.send(payload);
      if (control.bufferedAmount > MAX_CONTROL_QUEUE_BYTES) {
        if (critical) this.#failFullConnection();
        return false;
      }
      return true;
    } catch {
      if (critical) this.#failFullConnection();
      return false;
    }
  }

  #syncDeliveryState(): void {
    const deliveryState = this.coordinator.state;
    let phase = this.#snapshot.phase;
    if (deliveryState === "live") {
      phase = "live";
      this.#cancelSnapshotRetry(true);
      this.#input.setReplicaCurrent(true);
    } else {
      this.#input.setReplicaCurrent(false);
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
    this.#update({
      lastError: "protocol",
      phase: "reconnecting",
    });
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
    this.#input.detachTransport();
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

  #startHeartbeat(connectionEpoch: number): void {
    this.#stopHeartbeat();
    const tick = () => {
      this.#heartbeatTimer = null;
      if (!this.#isCurrentConnection(connectionEpoch)) return;
      const control = this.#control;
      if (control === null || control.readyState !== 1) {
        this.#invalidateFullConnection();
        this.#scheduleReconnect();
        return;
      }
      if (this.#now() - this.#lastPongAt > HEARTBEAT_TIMEOUT_MS) {
        this.#invalidateFullConnection();
        this.#scheduleReconnect();
        return;
      }
      if (
        control.bufferedAmount + textEncoder.encode("ping").byteLength >
        MAX_CONTROL_QUEUE_BYTES
      ) {
        this.#invalidateFullConnection();
        this.#scheduleReconnect();
        return;
      }
      try {
        control.send("ping");
      } catch {
        this.#invalidateFullConnection();
        this.#scheduleReconnect();
        return;
      }
      this.#heartbeatTimer = this.#setTimer(tick, HEARTBEAT_INTERVAL_MS);
    };
    this.#heartbeatTimer = this.#setTimer(tick, HEARTBEAT_INTERVAL_MS);
  }

  #stopHeartbeat(): void {
    if (this.#heartbeatTimer !== null) this.#clearTimer(this.#heartbeatTimer);
    this.#heartbeatTimer = null;
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
    ++this.#dataEpoch;
    this.#connectAbort?.abort(new DOMException("connection replaced", "AbortError"));
    this.#connectAbort = null;
    this.#abortDataReplacement();
    this.#cancelDataFailureTimer();
    this.#stopHeartbeat();
    this.#input.detachTransport();
    this.#closeSocket(this.#control);
    this.#closeSocket(this.#data);
    this.#control = null;
    this.#data = null;
    this.#update({ controlConnected: false, dataConnected: false });
  }

  #scheduleSnapshotRetry(): void {
    if (this.#closed || this.#automaticReconnectBlocked || this.#snapshotRetryTimer !== null)
      return;
    const exponent = Math.min(4, this.#snapshotFailureCount++);
    const base = Math.min(SNAPSHOT_RETRY_MAX_MS, SNAPSHOT_RETRY_BASE_MS * 2 ** exponent);
    const delay = Math.max(0, Math.round(base * (0.8 + this.#random() * 0.4)));
    this.#input.setReplicaCurrent(false);
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

  #abortDataReplacement(): void {
    this.#replacementAbort?.abort(new DOMException("data replacement superseded", "AbortError"));
    this.#replacementAbort = null;
  }

  #closeSocket(socket: WebSocket | null): void {
    if (socket === null) return;
    this.#intentionalSockets.add(socket);
    if (socket.readyState < 2) socket.close(1000, "replaced");
  }

  #isCurrentConnection(connectionEpoch: number): boolean {
    return !this.#closed && connectionEpoch === this.#connectionEpoch;
  }

  #isCurrentData(connectionEpoch: number, dataEpoch: number, generation: bigint): boolean {
    return (
      this.#isCurrentConnection(connectionEpoch) &&
      dataEpoch === this.#dataEpoch &&
      generation === this.#generation
    );
  }

  #update(patch: Partial<TerminalSessionSnapshot>): void {
    this.#snapshot = { ...this.#snapshot, ...patch };
    for (const listener of this.#listeners) listener();
  }
}

class AuthenticationError extends Error {}
