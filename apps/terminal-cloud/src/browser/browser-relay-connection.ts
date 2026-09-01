import {
  CloudResourceIdSchema,
  ConnectionSetRequestSchema,
  ConnectionSetResponseSchema,
  RELAY_CAPABILITIES_HEADER,
  RelayCapability,
  decodeDataFrame,
  decodeDataFrameBatchEntries,
  type ClientControlFrame,
} from "@zhongduan/protocol";

import type { CapabilityManager } from "./capability";
import type { InputTransportSendResult } from "./input-dispatcher";

const CLIENT_ID_PREFIX = "zhongduan:browser-client:";
const SOCKET_OPEN_TIMEOUT_MS = 10_000;
const HEARTBEAT_INTERVAL_MS = 15_000;
const HEARTBEAT_TIMEOUT_MS = 45_000;
const HTTP_REQUEST_TIMEOUT_MS = 10_000;
const SOCKET_REPLACED_CODE = 4001;
const SOCKET_REPLACED_REASON = "connection replaced";
const MAX_CONTROL_FRAME_BYTES = 6 * 1024 * 1024 + 4_096;
const MAX_CONTROL_QUEUE_BYTES = MAX_CONTROL_FRAME_BYTES;
const textEncoder = new TextEncoder();

type Timer = ReturnType<typeof setTimeout>;

export interface BrowserRelayConnectionEvents {
  controlClosed: (connection: BrowserRelayConnection, externallyReplaced: boolean) => void;
  controlMessage: (connection: BrowserRelayConnection, data: unknown) => void;
  dataClosed: (connection: BrowserRelayConnection) => void;
  dataFrames: (connection: BrowserRelayConnection, encodedFrames: readonly Uint8Array[]) => void;
  protocolFailure: (connection: BrowserRelayConnection) => void;
  transportFailure: (connection: BrowserRelayConnection) => void;
}

export interface BrowserRelayConnectionFactoryOptions {
  capabilities: CapabilityManager;
  sessionId: string;
  clearTimer: (timer: Timer) => void;
  createWebSocket: (url: string) => WebSocket;
  fetch: typeof fetch;
  makeWebSocketUrl: (path: string) => string;
  now: () => number;
  setTimer: (callback: () => void, delayMs: number) => Timer;
  storage?: Pick<Storage, "getItem" | "removeItem" | "setItem">;
}

interface BrowserRelayConnectionOptions {
  connectionEpoch: number;
  connectionId: string;
  controlTicket: string;
  createWebSocket: (url: string) => WebSocket;
  dataBatchEnabled: boolean;
  dataTicket: string;
  deliveryGeneration: bigint;
  events: BrowserRelayConnectionEvents;
  inputAdmissionEnabled: boolean;
  makeWebSocketUrl: (path: string) => string;
  now: () => number;
  sessionId: string;
  setTimer: (callback: () => void, delayMs: number) => Timer;
  clearTimer: (timer: Timer) => void;
  streamId: number;
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

export class BrowserRelayAuthenticationError extends Error {}

/** Owns the stable Browser client identity and bounded HTTP connection-set reservation. */
export class BrowserRelayConnectionFactory {
  readonly #capabilities: CapabilityManager;
  readonly #clearTimer: (timer: Timer) => void;
  readonly #createWebSocket: (url: string) => WebSocket;
  readonly #fetch: typeof fetch;
  readonly #makeWebSocketUrl: (path: string) => string;
  readonly #now: () => number;
  readonly #sessionId: string;
  readonly #setTimer: (callback: () => void, delayMs: number) => Timer;
  readonly #storage?: BrowserRelayConnectionFactoryOptions["storage"];
  #clientId: string | undefined;

  constructor(options: BrowserRelayConnectionFactoryOptions) {
    this.#capabilities = options.capabilities;
    this.#clearTimer = options.clearTimer;
    this.#createWebSocket = options.createWebSocket;
    this.#fetch = options.fetch;
    this.#makeWebSocketUrl = options.makeWebSocketUrl;
    this.#now = options.now;
    this.#sessionId = CloudResourceIdSchema.parse(options.sessionId);
    this.#setTimer = options.setTimer;
    this.#storage = options.storage;
    this.#clientId = readClientId(this.#storage, this.#sessionId);
  }

  async create(
    connectionEpoch: number,
    events: BrowserRelayConnectionEvents,
    signal: AbortSignal,
  ): Promise<BrowserRelayConnection> {
    const request = ConnectionSetRequestSchema.parse(
      this.#clientId === undefined ? {} : { clientId: this.#clientId },
    );
    const connectionSet = await this.#withRequestDeadline(signal, async (requestSignal) => {
      const response = await this.#fetch(
        `/api/v1/sessions/${encodeURIComponent(this.#sessionId)}/connection-sets`,
        {
          method: "POST",
          credentials: "same-origin",
          redirect: "error",
          headers: {
            ...this.#capabilities.authorizationHeaders(),
            "Content-Type": "application/json",
            [RELAY_CAPABILITIES_HEADER]: [
              RelayCapability.browserDataBatchV1,
              RelayCapability.browserInputAdmissionV1,
            ].join(","),
          },
          body: JSON.stringify(request),
          signal: requestSignal,
        },
      );
      this.#throwIfAborted(requestSignal);
      if (response.status === 401 || response.status === 403) {
        throw new BrowserRelayAuthenticationError();
      }
      if (!response.ok) throw new Error("connection set request failed");
      const parsed = ConnectionSetResponseSchema.parse(await response.json());
      this.#throwIfAborted(requestSignal);
      if (parsed.clientId === null || parsed.streamId === 0) {
        throw new Error("browser connection set has no delivery identity");
      }
      return parsed;
    });

    this.#clientId = connectionSet.clientId!;
    try {
      this.#storage?.setItem(clientStorageKey(this.#sessionId), this.#clientId);
    } catch {
      // The in-memory identity remains valid when storage is unavailable.
    }
    return new BrowserRelayConnection({
      connectionEpoch,
      connectionId: connectionSet.connectionId,
      controlTicket: connectionSet.controlTicket,
      createWebSocket: this.#createWebSocket,
      dataBatchEnabled:
        connectionSet.selectedCapabilities?.includes(RelayCapability.browserDataBatchV1) ?? false,
      dataTicket: connectionSet.dataTicket,
      deliveryGeneration: BigInt(connectionSet.deliveryGeneration),
      events,
      inputAdmissionEnabled:
        connectionSet.selectedCapabilities?.includes(RelayCapability.browserInputAdmissionV1) ??
        false,
      makeWebSocketUrl: this.#makeWebSocketUrl,
      now: this.#now,
      sessionId: this.#sessionId,
      setTimer: this.#setTimer,
      clearTimer: this.#clearTimer,
      streamId: connectionSet.streamId,
    });
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
}

/** Owns one full relay connection and every data-channel incarnation within that connection. */
export class BrowserRelayConnection {
  readonly connectionEpoch: number;
  readonly connectionId: string;
  readonly inputAdmissionEnabled: boolean;
  readonly streamId: number;
  readonly #clearTimer: (timer: Timer) => void;
  readonly #controlTicket: string;
  readonly #createWebSocket: (url: string) => WebSocket;
  readonly #dataBatchEnabled: boolean;
  readonly #dataTicket: string;
  readonly #events: BrowserRelayConnectionEvents;
  readonly #intentionalSockets = new WeakSet<WebSocket>();
  readonly #makeWebSocketUrl: (path: string) => string;
  readonly #now: () => number;
  readonly #sessionId: string;
  readonly #setTimer: (callback: () => void, delayMs: number) => Timer;
  #closed = false;
  #control: WebSocket | null = null;
  #data: WebSocket | null = null;
  #dataIncarnation = 0;
  #deliveryGeneration: bigint;
  #heartbeatTimer: Timer | null = null;
  #lastControlPongAt = 0;
  #lastDataPongAt = 0;
  #replacementAbort: AbortController | null = null;

  constructor(options: BrowserRelayConnectionOptions) {
    this.connectionEpoch = options.connectionEpoch;
    this.connectionId = options.connectionId;
    this.inputAdmissionEnabled = options.inputAdmissionEnabled;
    this.streamId = options.streamId;
    this.#clearTimer = options.clearTimer;
    this.#controlTicket = options.controlTicket;
    this.#createWebSocket = options.createWebSocket;
    this.#dataBatchEnabled = options.dataBatchEnabled;
    this.#dataTicket = options.dataTicket;
    this.#deliveryGeneration = options.deliveryGeneration;
    this.#events = options.events;
    this.#makeWebSocketUrl = options.makeWebSocketUrl;
    this.#now = options.now;
    this.#sessionId = options.sessionId;
    this.#setTimer = options.setTimer;
  }

  get deliveryGeneration(): bigint {
    return this.#deliveryGeneration;
  }

  get controlConnected(): boolean {
    return this.#control?.readyState === WebSocket.OPEN;
  }

  get dataConnected(): boolean {
    return this.#data?.readyState === WebSocket.OPEN;
  }

  async openControl(signal: AbortSignal): Promise<void> {
    const socket = this.#createWebSocket(this.#socketUrl("control", this.#controlTicket));
    socket.addEventListener("message", (event) => {
      if (this.#closed || socket !== this.#control) return;
      if (event.data === "pong") {
        this.#lastControlPongAt = this.#now();
        return;
      }
      this.#events.controlMessage(this, event.data);
    });
    socket.addEventListener("close", (event) => {
      if (this.#closed || this.#intentionalSockets.has(socket) || socket !== this.#control) {
        return;
      }
      this.#control = null;
      const externallyReplaced =
        event.code === SOCKET_REPLACED_CODE && event.reason === SOCKET_REPLACED_REASON;
      this.#events.controlClosed(this, externallyReplaced);
    });
    await this.#awaitOpen(socket, signal);
    if (this.#closed) {
      this.#closeSocket(socket);
      return;
    }
    this.#control = socket;
    this.#lastControlPongAt = this.#now();
  }

  async openInitialData(signal: AbortSignal): Promise<void> {
    const incarnation = ++this.#dataIncarnation;
    const socket = await this.#openDataSocket(
      this.#dataTicket,
      incarnation,
      this.#deliveryGeneration,
      signal,
    );
    if (!this.#isCurrentData(incarnation, this.#deliveryGeneration)) {
      this.#closeSocket(socket);
      return;
    }
    this.#data = socket;
    this.#lastDataPongAt = this.#now();
    this.#startHeartbeat(incarnation, this.#deliveryGeneration);
  }

  replaceData(ticket: string, generation: bigint): Promise<boolean> {
    this.#abortDataReplacement();
    this.#deliveryGeneration = generation;
    const incarnation = ++this.#dataIncarnation;
    const abort = new AbortController();
    this.#replacementAbort = abort;
    this.#stopHeartbeat();
    this.#closeSocket(this.#data);
    this.#data = null;
    return this.#openDataSocket(ticket, incarnation, generation, abort.signal)
      .then((socket) => {
        if (!this.#isCurrentData(incarnation, generation)) {
          this.#closeSocket(socket);
          return false;
        }
        this.#data = socket;
        this.#lastDataPongAt = this.#now();
        this.#startHeartbeat(incarnation, generation);
        return true;
      })
      .finally(() => {
        if (this.#replacementAbort === abort) this.#replacementAbort = null;
      });
  }

  sendControl(frame: ClientControlFrame, critical = false): boolean {
    const control = this.#control;
    try {
      const payload = JSON.stringify(frame);
      const payloadBytes = textEncoder.encode(payload).byteLength;
      if (
        control === null ||
        control.readyState !== WebSocket.OPEN ||
        payloadBytes > MAX_CONTROL_FRAME_BYTES ||
        control.bufferedAmount + payloadBytes > MAX_CONTROL_QUEUE_BYTES
      ) {
        if (critical) this.#events.transportFailure(this);
        return false;
      }
      control.send(payload);
      if (control.bufferedAmount > MAX_CONTROL_QUEUE_BYTES) {
        if (critical) this.#events.transportFailure(this);
        return false;
      }
      return true;
    } catch {
      if (critical) this.#events.transportFailure(this);
      return false;
    }
  }

  sendInput(frame: ClientControlFrame): InputTransportSendResult {
    const control = this.#control;
    let payload: string;
    let payloadBytes: number;
    try {
      payload = JSON.stringify(frame);
      payloadBytes = textEncoder.encode(payload).byteLength;
    } catch {
      return "proven-not-accepted";
    }
    if (
      control === null ||
      control.readyState !== WebSocket.OPEN ||
      payloadBytes > MAX_CONTROL_FRAME_BYTES ||
      control.bufferedAmount + payloadBytes > MAX_CONTROL_QUEUE_BYTES
    ) {
      return "proven-not-accepted";
    }
    try {
      control.send(payload);
    } catch {
      return "uncertain";
    }
    if (control.bufferedAmount > MAX_CONTROL_QUEUE_BYTES) {
      globalThis.queueMicrotask(() => this.#events.transportFailure(this));
    }
    return "accepted";
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    ++this.#dataIncarnation;
    this.#abortDataReplacement();
    this.#stopHeartbeat();
    this.#closeSocket(this.#control);
    this.#closeSocket(this.#data);
    this.#control = null;
    this.#data = null;
  }

  isCurrentGeneration(generation: bigint): boolean {
    return !this.#closed && generation === this.#deliveryGeneration;
  }

  async #openDataSocket(
    ticket: string,
    incarnation: number,
    generation: bigint,
    signal: AbortSignal,
  ): Promise<WebSocket> {
    const socket = this.#createWebSocket(this.#socketUrl("data", ticket));
    socket.binaryType = "arraybuffer";
    socket.addEventListener("message", (event) => {
      if (!this.#isCurrentData(incarnation, generation) || socket !== this.#data) return;
      if (event.data === "pong") {
        this.#lastDataPongAt = this.#now();
        return;
      }
      if (!(event.data instanceof ArrayBuffer)) {
        this.#events.protocolFailure(this);
        return;
      }
      try {
        const entries = this.#dataBatchEnabled
          ? decodeDataFrameBatchEntries(event.data)
          : [{ encoded: new Uint8Array(event.data), frame: decodeDataFrame(event.data) }];
        for (const entry of entries) {
          if (
            entry.frame.deliveryGeneration !== generation ||
            entry.frame.streamId !== this.streamId
          ) {
            this.#events.protocolFailure(this);
            return;
          }
        }
        this.#events.dataFrames(
          this,
          entries.map((entry) => entry.encoded),
        );
      } catch {
        this.#events.protocolFailure(this);
      }
    });
    socket.addEventListener("close", () => {
      if (
        this.#closed ||
        this.#intentionalSockets.has(socket) ||
        !this.#isCurrentData(incarnation, generation) ||
        socket !== this.#data
      ) {
        return;
      }
      this.#data = null;
      this.#stopHeartbeat();
      this.#events.dataClosed(this);
    });
    return this.#awaitOpen(socket, signal);
  }

  #awaitOpen(socket: WebSocket, signal: AbortSignal): Promise<WebSocket> {
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
        signal.removeEventListener("abort", onAbort);
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
        reject(signal.reason ?? new DOMException("connection attempt aborted", "AbortError"));
      };
      socket.addEventListener("open", onOpen);
      socket.addEventListener("close", onClose);
      socket.addEventListener("error", onError);
      signal.addEventListener("abort", onAbort, { once: true });
      if (signal.aborted) onAbort();
    });
  }

  #startHeartbeat(incarnation: number, generation: bigint): void {
    this.#stopHeartbeat();
    const tick = () => {
      this.#heartbeatTimer = null;
      if (!this.#isCurrentData(incarnation, generation)) return;
      const control = this.#control;
      const data = this.#data;
      if (
        control === null ||
        data === null ||
        control.readyState !== WebSocket.OPEN ||
        data.readyState !== WebSocket.OPEN
      ) {
        this.#events.transportFailure(this);
        return;
      }
      const now = this.#now();
      if (
        now - this.#lastControlPongAt >= HEARTBEAT_TIMEOUT_MS ||
        now - this.#lastDataPongAt >= HEARTBEAT_TIMEOUT_MS
      ) {
        this.#events.transportFailure(this);
        return;
      }
      const heartbeatBytes = textEncoder.encode("ping").byteLength;
      if (
        control.bufferedAmount + heartbeatBytes > MAX_CONTROL_QUEUE_BYTES ||
        data.bufferedAmount + heartbeatBytes > MAX_CONTROL_QUEUE_BYTES
      ) {
        this.#events.transportFailure(this);
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
        this.#events.transportFailure(this);
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

  #abortDataReplacement(): void {
    this.#replacementAbort?.abort(new DOMException("data replacement superseded", "AbortError"));
    this.#replacementAbort = null;
  }

  #closeSocket(socket: WebSocket | null): void {
    if (socket === null) return;
    this.#intentionalSockets.add(socket);
    if (socket.readyState < WebSocket.CLOSING) socket.close(1000, "replaced");
  }

  #isCurrentData(incarnation: number, generation: bigint): boolean {
    return (
      !this.#closed &&
      incarnation === this.#dataIncarnation &&
      generation === this.#deliveryGeneration
    );
  }

  #socketUrl(channel: "control" | "data", ticket: string): string {
    return this.#makeWebSocketUrl(
      `/api/v1/sessions/${encodeURIComponent(this.#sessionId)}/ws/${channel}?ticket=${encodeURIComponent(ticket)}`,
    );
  }
}
