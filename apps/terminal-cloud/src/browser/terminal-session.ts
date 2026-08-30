import {
  BrowserConnectionSetResponseSchema,
  ClientControlFrameSchema,
  CloudResourceIdSchema,
  ConnectionSetRequestSchema,
  decodeServerControlFrame,
  type BrowserCapabilityRole,
  type ClientControlFrame,
  type ReplicaCursor,
  type RecoveryProgressFrame,
  type ServerControlFrame,
} from "@zhongduan/protocol";
import {
  RecoveryRuntime,
  type DeliveryState,
  type ReplicaHost,
  type SnapshotTransport,
} from "@zhongduan/session-client";

import type { CapabilityManager } from "./capability";
import type { InputDispatcher } from "./input-dispatcher";

const CLIENT_ID_PREFIX = "zhongduan:browser-client:";
const SOCKET_OPEN_TIMEOUT_MS = 10_000;
const HEARTBEAT_INTERVAL_MS = 15_000;
const HEARTBEAT_TIMEOUT_MS = 45_000;
const WRITER_LEASE_RENEW_INTERVAL_MS = 10_000;
const HTTP_REQUEST_TIMEOUT_MS = 10_000;
const MAX_RECONNECT_DELAY_MS = 10_000;
// A legal 1 MiB input can expand to six bytes per byte when JSON escapes control characters.
const MAX_CONTROL_FRAME_BYTES = 6 * 1024 * 1024 + 4_096;
const MAX_CONTROL_QUEUE_BYTES = MAX_CONTROL_FRAME_BYTES;
const BROWSER_RECOVERY_LIMITS = {
  maxApplyFramesPerCall: 32,
  maxGapSpan: 1_024n,
  maxOwnedBytes: 2 * 1024 * 1024,
  maxOwnedFrames: 1_024,
  noProgressDeadlineMs: 15_000,
  recoveryDeadlineMs: 60_000,
} as const;
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

export class TerminalSession {
  readonly #sessionId: string;
  readonly #engineId: string;
  readonly #host: ReplicaHost;
  readonly #snapshots: SnapshotTransport;
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
  #snapshot: TerminalSessionSnapshot;
  #control: WebSocket | null = null;
  #data: WebSocket | null = null;
  #connectionEpoch = 0;
  #dataEpoch = 0;
  #generation: bigint | null = null;
  #recoveryRuntime: RecoveryRuntime | null = null;
  #activeCursor: ReplicaCursor | null;
  #streamId = 0;
  #connectionId: string | null = null;
  #writerLease: string | null = null;
  #writerIdentityConnectionId: string | null = null;
  #clientId: string | undefined;
  #connectPromise: Promise<void> | null = null;
  #connectAbort: AbortController | null = null;
  #needsReconnect = false;
  #reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  #heartbeatTimer: ReturnType<typeof setTimeout> | null = null;
  #writerLeaseTimer: ReturnType<typeof setTimeout> | null = null;
  #lastControlPongAt = 0;
  #lastDataPongAt = 0;
  #automaticReconnectBlocked = false;
  #closed = false;

  constructor(options: TerminalSessionOptions) {
    this.#sessionId = CloudResourceIdSchema.parse(options.sessionId);
    this.#engineId = options.engineId;
    this.#host = options.host;
    this.#snapshots = options.snapshots;
    this.#capabilities = options.capabilities;
    this.#input = options.input;
    this.#fetch = options.fetch ?? ((input, init) => globalThis.fetch(input, init));
    this.#createWebSocket = options.createWebSocket ?? ((url) => new WebSocket(url));
    this.#makeWebSocketUrl = options.makeWebSocketUrl ?? defaultWebSocketUrl;
    this.#now = options.now ?? Date.now;
    this.#random = options.random ?? Math.random;
    this.#setTimer =
      options.setTimer ?? ((callback, delayMs) => globalThis.setTimeout(callback, delayMs));
    this.#clearTimer = options.clearTimer ?? ((timer) => globalThis.clearTimeout(timer));
    this.#storage = options.storage;
    this.#activeCursor = options.initialCursor ? { ...options.initialCursor } : null;
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
  }

  get snapshot(): TerminalSessionSnapshot {
    return this.#snapshot;
  }

  get activeCursor(): ReplicaCursor | null {
    return this.#activeCursor === null ? null : { ...this.#activeCursor };
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
    this.#invalidateFullConnection();
    this.#capabilities.stop();
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
    this.#stopWriterLeaseRenewal();

    const response = await this.#createConnectionSet(signal);
    if (!this.#isCurrentConnection(connectionEpoch)) return;
    const generation = BigInt(response.deliveryGeneration);

    // Activation is one synchronous fence before any replacement callback can run.
    this.#activateDeliveryGeneration();
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
    this.#lastControlPongAt = this.#now();
    this.#update({ controlConnected: true });

    const dataEpoch = this.#dataEpoch;
    const data = await this.#openData(
      response.dataTicket,
      connectionEpoch,
      dataEpoch,
      generation,
      signal,
    );
    if (!this.#isCurrentData(connectionEpoch, dataEpoch, generation)) {
      this.#closeSocket(data);
      return;
    }
    this.#data = data;
    this.#lastDataPongAt = this.#now();
    this.#startHeartbeat(connectionEpoch, dataEpoch, generation);
    this.#update({ dataConnected: true, phase: "attaching" });
    this.#sendAttach();
  }

  async #createConnectionSet(signal: AbortSignal) {
    const request = ConnectionSetRequestSchema.parse(
      this.#clientId === undefined ? {} : { clientId: this.#clientId },
    );
    return this.#withRequestDeadline(signal, async (requestSignal) => {
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
          signal: requestSignal,
        },
      );
      this.#throwIfAborted(requestSignal);
      if (response.status === 401 || response.status === 403) throw new AuthenticationError();
      if (!response.ok) throw new Error("connection set request failed");
      const connectionSet = BrowserConnectionSetResponseSchema.parse(await response.json());
      this.#throwIfAborted(requestSignal);
      this.#clientId = connectionSet.clientId;
      try {
        this.#storage?.setItem(clientStorageKey(this.#sessionId), connectionSet.clientId);
      } catch {
        // The in-memory identity remains valid when storage is unavailable.
      }
      return connectionSet;
    });
  }

  #activateDeliveryGeneration(): void {
    this.#closeRecoveryRuntime();
    this.#input.setReplicaCurrent(false);
    this.#update({ deliveryState: "awaiting-control" });
  }

  #startRecoveryRuntime(generation: bigint, streamId: number): void {
    if (this.#recoveryRuntime !== null) return;
    let runtime!: RecoveryRuntime;
    runtime = new RecoveryRuntime({
      deliveryGeneration: generation,
      engineId: this.#engineId,
      host: this.#host,
      ...(this.#activeCursor === null ? {} : { initialCursor: this.#activeCursor }),
      limits: BROWSER_RECOVERY_LIMITS,
      snapshots: this.#snapshots,
      streamId,
      now: this.#now,
      setTimer: this.#setTimer,
      clearTimer: this.#clearTimer,
      onProgress: (frame) => this.#recoveryRuntime === runtime && this.#sendProgressControl(frame),
      onFailure: () => {
        if (this.#recoveryRuntime === runtime) this.#protocolFailure();
      },
      onStateChange: () => {
        if (this.#recoveryRuntime === runtime) this.#syncDeliveryState();
      },
    });
    this.#recoveryRuntime = runtime;
    this.#syncDeliveryState();
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
      if (event.data === "pong") {
        this.#lastDataPongAt = this.#now();
        return;
      }
      if (!(event.data instanceof ArrayBuffer)) {
        this.#protocolFailure();
        return;
      }
      try {
        const runtime = this.#recoveryRuntime;
        if (runtime === null || !runtime.acceptEnvelope(event.data)) {
          if (this.#isCurrentData(connectionEpoch, dataEpoch, generation)) {
            this.#protocolFailure();
          }
          return;
        }
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
      this.#stopHeartbeat();
      this.#input.setReplicaCurrent(false);
      this.#update({ dataConnected: false, phase: "reconnecting" });
      this.#invalidateFullConnection();
      this.#scheduleReconnect();
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
    const generation = this.#generation;
    if (generation === null) {
      this.#protocolFailure();
      return;
    }
    // Recovery time belongs to the active delivery attempt, not to the two
    // independently bounded WebSocket handshakes. Start its monotonic budget
    // only after both sockets are ready and immediately before Attach.
    this.#startRecoveryRuntime(generation, this.#streamId);
    const cursor = this.#activeCursor;
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
    this.#sendControl(ClientControlFrameSchema.parse(frame), true);
  }

  #handleControlMessage(data: unknown): void {
    if (data === "pong") {
      this.#lastControlPongAt = this.#now();
      return;
    }
    if (typeof data !== "string") {
      this.#protocolFailure();
      return;
    }
    let frame: ServerControlFrame;
    try {
      frame = decodeServerControlFrame(data);
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
      case "writer-lease-status":
        this.#acceptWriterLeaseStatus(frame);
        return;
      case "host-offline":
        this.#update({ hostOnline: false, phase: "offline" });
        return;
      case "recovery-start": {
        const runtime = this.#recoveryRuntime;
        if (runtime === null || !runtime.acceptStart(frame)) {
          if (this.#recoveryRuntime === runtime) this.#protocolFailure();
          return;
        }
        this.#syncDeliveryState();
        return;
      }
      case "recovery-source-closed": {
        const runtime = this.#recoveryRuntime;
        if (runtime === null || !runtime.acceptSourceClosed(frame)) {
          if (this.#recoveryRuntime === runtime) this.#protocolFailure();
          return;
        }
        this.#syncDeliveryState();
        return;
      }
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
    if (
      this.#writerLease !== null &&
      (this.#writerIdentityConnectionId !== frame.connectionId ||
        previousLease !== this.#writerLease)
    ) {
      if (this.#writerIdentityConnectionId !== null) this.#input.startNewInputEpoch();
      this.#writerIdentityConnectionId = frame.connectionId;
    }
    if (!this.#input.status.connected || previousLease !== this.#writerLease) {
      this.#attachInputTransport();
    }
    if (this.#writerLease === null) this.#stopWriterLeaseRenewal();
    else this.#startWriterLeaseRenewal();
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

  #acceptWriterLeaseStatus(
    frame: Extract<ServerControlFrame, { type: "writer-lease-status" }>,
  ): void {
    if (frame.active || this.#capabilities.role === "observer") return;
    this.#writerLease = null;
    this.#stopWriterLeaseRenewal();
    this.#attachInputTransport();
    this.#update({ controlOwnership: "waiting" });
  }

  #sendProgressControl(frame: RecoveryProgressFrame): boolean {
    return this.#sendControl(ClientControlFrameSchema.parse(frame), true);
  }

  #sendControl(frame: ClientControlFrame, critical = false): boolean {
    const control = this.#control;
    try {
      const validated = ClientControlFrameSchema.parse(frame);
      const payload = JSON.stringify(validated);
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
    const runtime = this.#recoveryRuntime;
    if (runtime === null) return;
    this.#activeCursor = runtime.activeCursor;
    let deliveryState: DeliveryState;
    let phase: SessionPhase;
    if (runtime.state === "live") {
      deliveryState = "live";
      phase = "live";
      this.#input.setReplicaCurrent(true);
    } else if (runtime.state === "failed") {
      deliveryState = "resyncing";
      phase = "reconnecting";
      this.#input.setReplicaCurrent(false);
    } else if (runtime.state === "restoring") {
      deliveryState = "restoring";
      phase = "restoring";
      this.#input.setReplicaCurrent(false);
    } else {
      deliveryState = runtime.state === "awaiting-start" ? "awaiting-control" : "replaying";
      phase = runtime.state === "awaiting-start" ? "attaching" : "restoring";
      this.#input.setReplicaCurrent(false);
    }
    this.#update({ deliveryState, phase });
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

  #startHeartbeat(connectionEpoch: number, dataEpoch: number, generation: bigint): void {
    this.#stopHeartbeat();
    const tick = () => {
      this.#heartbeatTimer = null;
      if (!this.#isCurrentData(connectionEpoch, dataEpoch, generation)) return;
      const control = this.#control;
      const data = this.#data;
      if (control === null || data === null || control.readyState !== 1 || data.readyState !== 1) {
        this.#invalidateFullConnection();
        this.#scheduleReconnect();
        return;
      }
      const now = this.#now();
      if (
        now - this.#lastControlPongAt >= HEARTBEAT_TIMEOUT_MS ||
        now - this.#lastDataPongAt >= HEARTBEAT_TIMEOUT_MS
      ) {
        this.#invalidateFullConnection();
        this.#scheduleReconnect();
        return;
      }
      const heartbeatBytes = textEncoder.encode("ping").byteLength;
      if (
        control.bufferedAmount + heartbeatBytes > MAX_CONTROL_QUEUE_BYTES ||
        data.bufferedAmount + heartbeatBytes > MAX_CONTROL_QUEUE_BYTES
      ) {
        this.#invalidateFullConnection();
        this.#scheduleReconnect();
        return;
      }
      try {
        control.send("ping");
        data.send("ping");
        if (
          control.bufferedAmount > MAX_CONTROL_QUEUE_BYTES ||
          data.bufferedAmount > MAX_CONTROL_QUEUE_BYTES
        ) {
          throw new Error("heartbeat backpressure exceeded");
        }
      } catch {
        this.#invalidateFullConnection();
        this.#scheduleReconnect();
        return;
      }
      this.#heartbeatTimer = this.#setTimer(tick, HEARTBEAT_INTERVAL_MS);
    };
    this.#heartbeatTimer = this.#setTimer(tick, HEARTBEAT_INTERVAL_MS);
  }

  #startWriterLeaseRenewal(): void {
    this.#stopWriterLeaseRenewal();
    const connectionEpoch = this.#connectionEpoch;
    const tick = () => {
      this.#writerLeaseTimer = null;
      if (!this.#isCurrentConnection(connectionEpoch)) return;
      const writerLease = this.#writerLease;
      if (writerLease === null) return;
      const sent = this.#sendControl(
        ClientControlFrameSchema.parse({ type: "writer-lease-renew", writerLease }),
        true,
      );
      if (!sent || !this.#isCurrentConnection(connectionEpoch)) return;
      this.#writerLeaseTimer = this.#setTimer(tick, WRITER_LEASE_RENEW_INTERVAL_MS);
    };
    this.#writerLeaseTimer = this.#setTimer(tick, WRITER_LEASE_RENEW_INTERVAL_MS);
  }

  #stopWriterLeaseRenewal(): void {
    if (this.#writerLeaseTimer !== null) this.#clearTimer(this.#writerLeaseTimer);
    this.#writerLeaseTimer = null;
  }

  #attachInputTransport(): void {
    this.#input.attachTransport((inputFrame) => {
      const sent = this.#sendControl(inputFrame);
      if (!sent) this.#failFullConnection();
      return sent;
    }, this.#writerLease ?? undefined);
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
    this.#invalidateFullConnection();
    this.#update({ lastError: kind, phase: "failed" });
  }

  #invalidateFullConnection(): void {
    ++this.#connectionEpoch;
    ++this.#dataEpoch;
    this.#connectAbort?.abort(new DOMException("connection replaced", "AbortError"));
    this.#connectAbort = null;
    this.#stopHeartbeat();
    this.#stopWriterLeaseRenewal();
    this.#input.detachTransport();
    this.#closeRecoveryRuntime();
    this.#closeSocket(this.#control);
    this.#closeSocket(this.#data);
    this.#control = null;
    this.#data = null;
    this.#update({ controlConnected: false, dataConnected: false });
  }

  #closeRecoveryRuntime(): void {
    const runtime = this.#recoveryRuntime;
    if (runtime === null) return;
    this.#activeCursor = runtime.activeCursor;
    this.#recoveryRuntime = null;
    runtime.close();
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

  async #withRequestDeadline<T>(
    parentSignal: AbortSignal,
    operation: (signal: AbortSignal) => Promise<T>,
  ): Promise<T> {
    const abort = new AbortController();
    let rejectBoundary!: (reason?: unknown) => void;
    const boundary = new Promise<never>((_resolve, reject) => {
      rejectBoundary = reject;
    });
    const fail = (reason: unknown) => {
      if (!abort.signal.aborted) abort.abort(reason);
      rejectBoundary(reason);
    };
    const onAbort = () =>
      fail(parentSignal.reason ?? new DOMException("connection attempt aborted", "AbortError"));
    const timeout = this.#setTimer(
      () => fail(new DOMException("connection set request timed out", "TimeoutError")),
      HTTP_REQUEST_TIMEOUT_MS,
    );
    parentSignal.addEventListener("abort", onAbort, { once: true });
    if (parentSignal.aborted) onAbort();
    try {
      return await Promise.race([operation(abort.signal), boundary]);
    } finally {
      this.#clearTimer(timeout);
      parentSignal.removeEventListener("abort", onAbort);
    }
  }

  #throwIfAborted(signal: AbortSignal): void {
    if (signal.aborted) {
      throw signal.reason ?? new DOMException("request aborted", "AbortError");
    }
  }

  #update(patch: Partial<TerminalSessionSnapshot>): void {
    this.#snapshot = { ...this.#snapshot, ...patch };
    for (const listener of this.#listeners) listener();
  }
}

class AuthenticationError extends Error {}
