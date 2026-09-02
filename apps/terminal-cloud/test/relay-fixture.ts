import {
  DATA_HEADER_BYTES,
  DataFrameKind,
  SNAPSHOT_MEDIA_TYPE,
  SnapshotHeader,
  RELAY_CAPABILITIES_HEADER,
  RelayCapability,
  decodeDataFrame,
  decodeDataFrameBatch,
  encodeDeliveryBarrierPayload,
  encodeDataFrame,
  encodeDataFrameBatch,
  type DataFrame,
} from "@zhongduan/protocol";
import { env, exports as workerExports } from "cloudflare:workers";
import { evictDurableObject, runInDurableObject } from "cloudflare:test";
import { afterEach, expect } from "vitest";
import { installMiniflareMultipartEtagShim, storedSnapshotKey } from "./snapshot-test-helpers";

export {
  DATA_HEADER_BYTES,
  DataFrameKind,
  SNAPSHOT_MEDIA_TYPE,
  SnapshotHeader,
  RELAY_CAPABILITIES_HEADER,
  RelayCapability,
  decodeDataFrame,
  decodeDataFrameBatch,
  encodeDeliveryBarrierPayload,
  encodeDataFrame,
  encodeDataFrameBatch,
  type DataFrame,
};
export { env, workerExports };
export { evictDurableObject, runInDurableObject };
export { storedSnapshotKey };

export interface CreatedSession {
  hostCapability: string;
  observerCapability: string;
  sessionId: string;
  writerCapability: string;
}

export interface ConnectionSet {
  clientId: string | null;
  connectionId: string;
  connectionSetId: string;
  controlTicket: string;
  dataTicket: string;
  deliveryGeneration: string;
  streamId: number;
  selectedCapabilities?: string[];
}

export interface SocketEndpoint {
  inbox: SocketInbox;
  socket: WebSocket;
}

export interface HostEndpoint {
  connection: ConnectionSet;
  control: SocketEndpoint;
  data: SocketEndpoint;
}

export interface BrowserEndpoint {
  connection: ConnectionSet;
  control: SocketEndpoint;
  data: SocketEndpoint;
}

export interface ReplicaCursor {
  eventSeq: string;
  nextPtyOffset: string;
  sessionEpoch: string;
}

export const origin = "https://terminal.example.test";
export const engineId = "ghostty:test+snapshot-v1+wterm:test";
let sessionCounter = 0;
export const textEncoder = new TextEncoder();
const testSockets = new Set<WebSocket>();
const testSocketSessions = new Set<string>();

export class SocketInbox {
  readonly #messages: unknown[] = [];
  readonly #messageWaiters: Array<(message: unknown) => void> = [];
  readonly #closes: CloseEvent[] = [];
  readonly #closeWaiters: Array<(event: CloseEvent) => void> = [];

  constructor(socket: WebSocket) {
    socket.addEventListener("message", (event) => {
      const waiter = this.#messageWaiters.shift();
      if (waiter === undefined) this.#messages.push(event.data);
      else waiter(event.data);
    });
    socket.addEventListener("close", (event) => {
      const waiter = this.#closeWaiters.shift();
      if (waiter === undefined) this.#closes.push(event);
      else waiter(event);
    });
  }

  get pendingMessageCount(): number {
    return this.#messages.length;
  }

  discardAll(expected: unknown): number {
    let discarded = 0;
    for (let index = this.#messages.length - 1; index >= 0; index -= 1) {
      if (this.#messages[index] !== expected) continue;
      this.#messages.splice(index, 1);
      discarded += 1;
    }
    return discarded;
  }

  nextMessage(): Promise<unknown> {
    const message = this.#messages.shift();
    if (message !== undefined) return Promise.resolve(message);
    return new Promise((resolve) => this.#messageWaiters.push(resolve));
  }

  async nextJson<T>(): Promise<T> {
    const message = await this.nextMessage();
    if (typeof message !== "string") throw new Error("expected a text WebSocket message");
    return JSON.parse(message) as T;
  }

  async nextDataFrame(): Promise<DataFrame> {
    return decodeDataFrame(await bytesFromMessage(await this.nextMessage()));
  }

  nextClose(): Promise<CloseEvent> {
    const event = this.#closes.shift();
    if (event !== undefined) return Promise.resolve(event);
    return new Promise((resolve) => this.#closeWaiters.push(resolve));
  }
}

export async function bytesFromMessage(message: unknown): Promise<Uint8Array> {
  if (message instanceof ArrayBuffer) return new Uint8Array(message);
  if (ArrayBuffer.isView(message)) {
    return new Uint8Array(message.buffer, message.byteOffset, message.byteLength).slice();
  }
  if (message instanceof Blob) return new Uint8Array(await message.arrayBuffer());
  throw new Error("expected a binary WebSocket message");
}

export function sessionStub(sessionId: string) {
  return env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${sessionId}`));
}

export async function drainSession(sessionId: string): Promise<void> {
  await runInDurableObject(sessionStub(sessionId), () => undefined);
}

afterEach(async () => {
  const sockets = [...testSockets];
  const sessionIds = [...testSocketSessions];
  try {
    for (const socket of sockets) {
      if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
        socket.close(1000, "test cleanup");
      }
    }
    await Promise.all(
      sessionIds.map(async (sessionId) => {
        await drainSession(sessionId);
        await evictDurableObject(sessionStub(sessionId));
      }),
    );
  } finally {
    testSockets.clear();
    testSocketSessions.clear();
  }
});

export async function injectServerSendFailure(
  session: CreatedSession,
  browser: BrowserEndpoint,
  channel: "control" | "data",
): Promise<void> {
  await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
    const socket = state
      .getWebSockets(`client:${browser.connection.clientId}`)
      .find((candidate) => {
        return (candidate.deserializeAttachment() as { channel?: string }).channel === channel;
      });
    if (socket === undefined) throw new Error(`browser ${channel} socket missing`);
    expect(socket.readyState).toBe(WebSocket.OPEN);
    Object.defineProperty(socket, "send", {
      configurable: true,
      value() {
        throw new Error(`injected browser ${channel} send failure`);
      },
    });
  });
}

export async function injectHostControlSendFailure(session: CreatedSession): Promise<void> {
  await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
    const socket = state.getWebSockets("peer:host").find((candidate) => {
      return (candidate.deserializeAttachment() as { channel?: string }).channel === "control";
    });
    if (socket === undefined) throw new Error("current Host control socket missing");
    expect(socket.readyState).toBe(WebSocket.OPEN);
    Object.defineProperty(socket, "send", {
      configurable: true,
      value() {
        throw new Error("injected Host control send failure");
      },
    });
  });
}

export async function createSession(): Promise<CreatedSession> {
  const sessionId = `session_relay_${(++sessionCounter).toString().padStart(16, "0")}`;
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions`, {
      method: "POST",
      headers: {
        authorization: "Bearer test-bootstrap-token-with-at-least-32-bytes",
        "content-type": "application/json",
      },
      body: JSON.stringify({ sessionId, engineId, sessionEpoch: "7" }),
    }),
  );
  expect(response.status).toBe(201);
  const session = await response.json<CreatedSession>();
  await installMiniflareMultipartEtagShim(session.sessionId);
  return session;
}

export async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const digest = new Uint8Array(
    await crypto.subtle.digest("SHA-256", Uint8Array.from(bytes).buffer),
  );
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

export async function publishSnapshot(
  session: CreatedSession,
  snapshotId: string,
  cutEventSeq = "0",
  nextPtyOffset = "0",
): Promise<void> {
  const body = textEncoder.encode(`snapshot:${snapshotId}`);
  const length = body.byteLength.toString();
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions/${session.sessionId}/snapshots/${snapshotId}`, {
      method: "PUT",
      headers: {
        authorization: `Bearer ${session.hostCapability}`,
        "content-type": SNAPSHOT_MEDIA_TYPE,
        [SnapshotHeader.compression]: "none",
        [SnapshotHeader.compressedLength]: length,
        [SnapshotHeader.cutEventSeq]: cutEventSeq,
        [SnapshotHeader.engineId]: engineId,
        [SnapshotHeader.nextPtyOffset]: nextPtyOffset,
        [SnapshotHeader.sessionEpoch]: "7",
        [SnapshotHeader.sha256]: await sha256Hex(body),
        [SnapshotHeader.uncompressedLength]: length,
      },
      body: body.buffer,
    }),
  );
  expect(response.status).toBe(201);
  await response.body?.cancel();
}

export function deliveryBarrier(
  browser: BrowserEndpoint,
  mode: "warm" | "snapshot",
  commitEventSeq: bigint,
  commitPtyOffset: bigint,
  snapshotId?: string,
): Uint8Array {
  return encodeDataFrame({
    kind: DataFrameKind.DeliveryBarrier,
    flags: 0,
    sessionEpoch: 7n,
    deliveryGeneration: BigInt(browser.connection.deliveryGeneration),
    eventSeq: commitEventSeq,
    ptyOffset: commitPtyOffset,
    streamId: browser.connection.streamId,
    payload: encodeDeliveryBarrierPayload(
      mode === "warm"
        ? { mode, connectionId: browser.connection.connectionId }
        : {
            mode,
            connectionId: browser.connection.connectionId,
            snapshotId: snapshotId!,
          },
    ),
  });
}

export async function beginWarmDelivery(
  session: CreatedSession,
  host: HostEndpoint,
  browser: BrowserEndpoint,
  commitEventSeq: bigint,
  commitPtyOffset: bigint,
): Promise<void> {
  host.data.socket.send(deliveryBarrier(browser, "warm", commitEventSeq, commitPtyOffset));
  await drainSession(session.sessionId);
  expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
  expect(browser.control.socket.readyState).toBe(WebSocket.OPEN);
  const [start, result] = await Promise.all([
    browser.control.inbox.nextJson<Record<string, unknown>>(),
    host.control.inbox.nextJson<Record<string, unknown>>(),
  ]);
  expect(start).toMatchObject({
    type: "replay-start",
    streamId: browser.connection.streamId,
    deliveryGeneration: browser.connection.deliveryGeneration,
    commitEventSeq: commitEventSeq.toString(),
    commitPtyOffset: commitPtyOffset.toString(),
  });
  expect(result).toMatchObject({
    type: "delivery-barrier-result",
    status: "ready",
    mode: "warm",
    connectionId: browser.connection.connectionId,
    streamId: browser.connection.streamId,
    deliveryGeneration: browser.connection.deliveryGeneration,
  });
  expect(host.data.inbox.discardAll("data-ack")).toBeGreaterThan(0);
  expect(host.data.inbox.pendingMessageCount).toBe(0);
}

export async function beginSnapshotDelivery(
  session: CreatedSession,
  host: HostEndpoint,
  browser: BrowserEndpoint,
  snapshotId: string,
  commitEventSeq: bigint,
  commitPtyOffset: bigint,
): Promise<Record<string, unknown>> {
  host.data.socket.send(
    deliveryBarrier(browser, "snapshot", commitEventSeq, commitPtyOffset, snapshotId),
  );
  await drainSession(session.sessionId);
  expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
  expect(browser.control.socket.readyState).toBe(WebSocket.OPEN);
  const [manifest, result] = await Promise.all([
    browser.control.inbox.nextJson<Record<string, unknown>>(),
    host.control.inbox.nextJson<Record<string, unknown>>(),
  ]);
  expect(manifest).toMatchObject({
    type: "snapshot-manifest",
    snapshotId,
    streamId: browser.connection.streamId,
    deliveryGeneration: browser.connection.deliveryGeneration,
    commitEventSeq: commitEventSeq.toString(),
    commitPtyOffset: commitPtyOffset.toString(),
  });
  expect(result).toMatchObject({
    type: "delivery-barrier-result",
    status: "ready",
    mode: "snapshot",
    connectionId: browser.connection.connectionId,
    snapshotId,
  });
  expect(host.data.inbox.discardAll("data-ack")).toBeGreaterThan(0);
  expect(host.data.inbox.pendingMessageCount).toBe(0);
  return manifest;
}

export async function createConnectionSet(
  sessionId: string,
  capability: string,
  clientId?: string,
  relayCapabilities: string[] = [],
): Promise<ConnectionSet> {
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions/${sessionId}/connection-sets`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${capability}`,
        "content-type": "application/json",
        ...(relayCapabilities.length === 0
          ? {}
          : { [RELAY_CAPABILITIES_HEADER]: relayCapabilities.join(",") }),
      },
      body: JSON.stringify(clientId === undefined ? {} : { clientId }),
    }),
  );
  expect(response.status).toBe(200);
  return response.json<ConnectionSet>();
}

export async function upgradeResponse(
  sessionId: string,
  channel: "control" | "data",
  ticket: string,
): Promise<Response> {
  return workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions/${sessionId}/ws/${channel}?ticket=${ticket}`, {
      headers: { upgrade: "websocket" },
    }),
  );
}

export async function upgrade(
  sessionId: string,
  channel: "control" | "data",
  ticket: string,
): Promise<SocketEndpoint> {
  const response = await upgradeResponse(sessionId, channel, ticket);
  expect(response.status).toBe(101);
  const socket = response.webSocket;
  if (socket === null) throw new Error("upgrade response did not include a WebSocket");
  socket.accept();
  testSockets.add(socket);
  testSocketSessions.add(sessionId);
  socket.addEventListener("close", () => testSockets.delete(socket), { once: true });
  return { socket, inbox: new SocketInbox(socket) };
}

export async function openHost(
  session: CreatedSession,
  headEventSeq = "0",
  nextPtyOffset = "0",
  negotiateOutcomeDetails = true,
): Promise<HostEndpoint> {
  const connection = await createConnectionSet(
    session.sessionId,
    session.hostCapability,
    undefined,
    negotiateOutcomeDetails
      ? [RelayCapability.deliveryBarrierOutcomeV1, RelayCapability.hostDataBatchV1]
      : [],
  );
  expect(connection.selectedCapabilities).toEqual(
    negotiateOutcomeDetails
      ? [RelayCapability.deliveryBarrierOutcomeV1, RelayCapability.hostDataBatchV1]
      : undefined,
  );
  const control = await upgrade(session.sessionId, "control", connection.controlTicket);
  const data = await upgrade(session.sessionId, "data", connection.dataTicket);
  control.socket.send(
    JSON.stringify({
      type: "host-ready",
      engineId,
      sessionEpoch: "7",
      headEventSeq,
      nextPtyOffset,
    }),
  );
  expect(await control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
    type: "host-ready-ack",
    sessionEpoch: "7",
    headEventSeq,
    nextPtyOffset,
  });
  return { connection, control, data };
}

export async function openBrowser(
  session: CreatedSession,
  capability: string,
  clientId?: string,
  relayCapabilities: string[] = [],
): Promise<BrowserEndpoint> {
  const connection = await createConnectionSet(
    session.sessionId,
    capability,
    clientId,
    relayCapabilities,
  );
  const control = await upgrade(session.sessionId, "control", connection.controlTicket);
  const data = await upgrade(session.sessionId, "data", connection.dataTicket);
  return { connection, control, data };
}

export async function attachBrowser(
  session: CreatedSession,
  host: HostEndpoint,
  browser: BrowserEndpoint,
  cursor: ReplicaCursor | null = {
    sessionEpoch: "7",
    eventSeq: "0",
    nextPtyOffset: "0",
  },
): Promise<{ request: Record<string, unknown>; welcome: Record<string, unknown> }> {
  sendBrowserAttach(browser, cursor);
  await drainSession(session.sessionId);
  const [welcome, request] = await Promise.all([
    browser.control.inbox.nextJson<Record<string, unknown>>(),
    host.control.inbox.nextJson<Record<string, unknown>>(),
  ]);
  expect(welcome.type).toBe("welcome");
  expect(request.type).toBe("attach-request");
  if (typeof welcome.deliveryGeneration === "string") {
    browser.connection.deliveryGeneration = welcome.deliveryGeneration;
  }
  return { welcome, request };
}

export async function reattachBrowser(
  session: CreatedSession,
  host: HostEndpoint,
  browser: BrowserEndpoint,
  deliveryGeneration: string,
  cursor: ReplicaCursor | null = {
    sessionEpoch: "7",
    eventSeq: "0",
    nextPtyOffset: "0",
  },
): Promise<Record<string, unknown>> {
  browser.connection.deliveryGeneration = deliveryGeneration;
  sendBrowserAttach(browser, cursor);
  await drainSession(session.sessionId);
  expect(browser.control.inbox.pendingMessageCount).toBe(0);
  const request = await host.control.inbox.nextJson<Record<string, unknown>>();
  expect(request).toMatchObject({ type: "attach-request", deliveryGeneration });
  return request;
}

export function sendBrowserAttach(browser: BrowserEndpoint, cursor: ReplicaCursor | null): void {
  browser.control.socket.send(
    JSON.stringify(
      cursor === null
        ? {
            type: "attach",
            engineId,
            deliveryGeneration: browser.connection.deliveryGeneration,
            hasLiveReplica: false,
          }
        : {
            type: "attach",
            engineId,
            deliveryGeneration: browser.connection.deliveryGeneration,
            hasLiveReplica: true,
            lastSessionEpoch: cursor.sessionEpoch,
            lastEventSeq: cursor.eventSeq,
            nextPtyOffset: cursor.nextPtyOffset,
          },
    ),
  );
}

export async function renewWriterLease(
  session: CreatedSession,
  browser: BrowserEndpoint,
  writerLease: string,
): Promise<{ active: boolean; expiresAt?: number; type: string; writerFence?: string }> {
  browser.control.socket.send(JSON.stringify({ type: "writer-lease-renew", writerLease }));
  await drainSession(session.sessionId);
  return browser.control.inbox.nextJson<{
    active: boolean;
    expiresAt?: number;
    type: string;
    writerFence?: string;
  }>();
}

export function ptyFrame(
  eventSeq: bigint,
  ptyOffset: bigint,
  payload: Uint8Array,
  deliveryGeneration = 0n,
  streamId = 0,
): Uint8Array {
  return encodeDataFrame({
    kind: DataFrameKind.PtyOutput,
    flags: 0,
    sessionEpoch: 7n,
    deliveryGeneration,
    eventSeq,
    ptyOffset,
    streamId,
    payload,
  });
}

export function replayCommit(
  eventSeq: bigint,
  ptyOffset: bigint,
  deliveryGeneration: bigint,
  streamId: number,
): Uint8Array {
  return encodeDataFrame({
    kind: DataFrameKind.ReplayCommit,
    flags: 0,
    sessionEpoch: 7n,
    deliveryGeneration,
    eventSeq,
    ptyOffset,
    streamId,
    payload: new Uint8Array(),
  });
}

export function resetFrame(deliveryGeneration: bigint, streamId: number): Uint8Array {
  return encodeDataFrame({
    kind: DataFrameKind.Reset,
    flags: 0,
    sessionEpoch: 7n,
    deliveryGeneration,
    eventSeq: 0n,
    ptyOffset: 0n,
    streamId,
    payload: new Uint8Array(),
  });
}

export function keyFrame(writerLease: string, inputEpoch: string, clientInputSeq: string) {
  return {
    type: "key",
    writerLease,
    inputEpoch,
    clientInputSeq,
    observedEventSeq: "0",
    code: "KeyE",
    key: "€",
    text: "€",
    modifiers: 0,
    action: "press",
    altGraph: true,
    composing: false,
    consumedModifiers: 0,
    unshiftedCodepoint: 0x65,
  };
}

export function recoveryInputFrames(writerLease: string) {
  const identity = { writerLease, inputEpoch: "delivery_recovery" };
  return [
    {
      type: "key",
      ...identity,
      clientInputSeq: "1",
      observedEventSeq: "32",
      code: "KeyC",
      key: "c",
      text: "c",
      modifiers: 2,
      action: "press",
      altGraph: false,
      composing: false,
      consumedModifiers: 0,
      unshiftedCodepoint: 0x63,
    },
    { type: "text", ...identity, clientInputSeq: "2", data: "input during recovery" },
    { type: "paste", ...identity, clientInputSeq: "3", data: "pasted during recovery" },
    { type: "focus", ...identity, clientInputSeq: "4", focused: false },
    {
      type: "resize-request",
      ...identity,
      clientInputSeq: "5",
      cols: 120,
      rows: 40,
      widthPx: 1_200,
      heightPx: 800,
    },
  ] as const;
}
