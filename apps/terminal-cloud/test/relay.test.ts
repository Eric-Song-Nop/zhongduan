import {
  DATA_HEADER_BYTES,
  DataFrameKind,
  SNAPSHOT_MEDIA_TYPE,
  SnapshotHeader,
  RELAY_CAPABILITIES_HEADER,
  RelayCapability,
  decodeDataFrame,
  encodeDeliveryBarrierPayload,
  encodeDataFrame,
  type DataFrame,
} from "@zhongduan/protocol";
import { env, exports as workerExports } from "cloudflare:workers";
import { evictDurableObject, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import { installMiniflareMultipartEtagShim, storedSnapshotKey } from "./snapshot-test-helpers";

interface CreatedSession {
  hostCapability: string;
  observerCapability: string;
  sessionId: string;
  writerCapability: string;
}

interface ConnectionSet {
  clientId: string | null;
  connectionId: string;
  connectionSetId: string;
  controlTicket: string;
  dataTicket: string;
  deliveryGeneration: string;
  streamId: number;
}

interface SocketEndpoint {
  inbox: SocketInbox;
  socket: WebSocket;
}

interface HostEndpoint {
  connection: ConnectionSet;
  control: SocketEndpoint;
  data: SocketEndpoint;
}

interface BrowserEndpoint {
  connection: ConnectionSet;
  control: SocketEndpoint;
  data: SocketEndpoint;
}

interface ReplicaCursor {
  eventSeq: string;
  nextPtyOffset: string;
  sessionEpoch: string;
}

const origin = "https://terminal.example.test";
const engineId = "ghostty:test+snapshot-v1+wterm:test";
let sessionCounter = 0;
const textEncoder = new TextEncoder();
const testSockets = new Set<WebSocket>();
const testSocketSessions = new Set<string>();

class SocketInbox {
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

async function bytesFromMessage(message: unknown): Promise<Uint8Array> {
  if (message instanceof ArrayBuffer) return new Uint8Array(message);
  if (ArrayBuffer.isView(message)) {
    return new Uint8Array(message.buffer, message.byteOffset, message.byteLength).slice();
  }
  if (message instanceof Blob) return new Uint8Array(await message.arrayBuffer());
  throw new Error("expected a binary WebSocket message");
}

function sessionStub(sessionId: string) {
  return env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${sessionId}`));
}

async function drainSession(sessionId: string): Promise<void> {
  await runInDurableObject(sessionStub(sessionId), () => undefined);
}

afterEach(async () => {
  for (const socket of testSockets) {
    if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
      socket.close(1000, "test cleanup");
    }
  }
  await Promise.all([...testSocketSessions].map((sessionId) => drainSession(sessionId)));
  testSockets.clear();
  testSocketSessions.clear();
});

async function injectServerSendFailure(
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

async function injectHostControlSendFailure(session: CreatedSession): Promise<void> {
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

async function createSession(): Promise<CreatedSession> {
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

async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const digest = new Uint8Array(
    await crypto.subtle.digest("SHA-256", Uint8Array.from(bytes).buffer),
  );
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function publishSnapshot(
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

function deliveryBarrier(
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

async function beginWarmDelivery(
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
  expect(host.data.inbox.pendingMessageCount).toBe(0);
}

async function beginSnapshotDelivery(
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
  expect(host.data.inbox.pendingMessageCount).toBe(0);
  return manifest;
}

async function createConnectionSet(
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

async function upgradeResponse(
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

async function upgrade(
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

async function openHost(
  session: CreatedSession,
  headEventSeq = "0",
  nextPtyOffset = "0",
  negotiateOutcomeDetails = true,
): Promise<HostEndpoint> {
  const connection = await createConnectionSet(
    session.sessionId,
    session.hostCapability,
    undefined,
    negotiateOutcomeDetails ? [RelayCapability.deliveryBarrierOutcomeV1] : [],
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

async function openBrowser(
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

async function attachBrowser(
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

async function reattachBrowser(
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

function sendBrowserAttach(browser: BrowserEndpoint, cursor: ReplicaCursor | null): void {
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

async function renewWriterLease(
  session: CreatedSession,
  browser: BrowserEndpoint,
  writerLease: string,
): Promise<{ active: boolean; expiresAt?: number; type: string }> {
  browser.control.socket.send(JSON.stringify({ type: "writer-lease-renew", writerLease }));
  await drainSession(session.sessionId);
  return browser.control.inbox.nextJson<{
    active: boolean;
    expiresAt?: number;
    type: string;
  }>();
}

function ptyFrame(
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

function replayCommit(
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

function resetFrame(deliveryGeneration: bigint, streamId: number): Uint8Array {
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

function keyFrame(writerLease: string, inputEpoch: string, clientInputSeq: string) {
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

function recoveryInputFrames(writerLease: string) {
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

describe("live Durable Object relay", () => {
  it("fails the current host pair before queueing an oversized data frame", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser);

    host.data.socket.send(new Uint8Array(DATA_HEADER_BYTES + 16 * 1024 + 1));
    expect((await host.control.inbox.nextClose()).code).toBe(4400);
    expect((await host.data.inbox.nextClose()).code).toBe(4400);
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "host-offline",
    });
  });

  it("isolates an oversized browser control frame without failing the host", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);

    browser.control.socket.send("x".repeat(6 * 1024 * 1024 + 4_097));
    expect((await browser.control.inbox.nextClose()).code).toBe(4400);
    expect((await browser.data.inbox.nextClose()).code).toBe(4001);
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
  });

  it("isolates an input acknowledgement control sink failure without failing the Host", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const failing = await openBrowser(session, session.observerCapability);
    const healthy = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, failing);
    await attachBrowser(session, host, healthy);
    await injectServerSendFailure(session, failing, "control");

    host.control.socket.send(
      JSON.stringify({
        type: "input-ack",
        connectionId: failing.connection.connectionId,
        inputEpoch: "input_ack_failure",
        clientInputSeq: "1",
        status: "written",
        authorityEventSeq: "0",
      }),
    );
    const [controlClose, dataClose] = await Promise.all([
      failing.control.inbox.nextClose(),
      failing.data.inbox.nextClose(),
    ]);
    expect(controlClose.code).toBe(4400);
    expect(dataClose.code).toBe(4400);
    await drainSession(session.sessionId);
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
    expect(healthy.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(healthy.data.socket.readyState).toBe(WebSocket.OPEN);
  });

  it("fences a throwing Host sink and returns uncertain without dropping recovery input control", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.writerCapability);
    const attached = await attachBrowser(session, host, browser);
    const writerLease = String(attached.welcome.writerLease);

    browser.data.socket.close(1000, "recover without data");
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "resync-required",
      deliveryGeneration: "2",
    });
    await injectHostControlSendFailure(session);

    const interrupt = {
      ...keyFrame(writerLease, "throwing_host_input", "1"),
      code: "KeyC",
      key: "c",
      text: "c",
      modifiers: 2,
      altGraph: false,
      unshiftedCodepoint: 0x63,
    };
    browser.control.socket.send(JSON.stringify(interrupt));
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toEqual({
      type: "host-offline",
    });
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "input-ack",
      inputEpoch: "throwing_host_input",
      clientInputSeq: "1",
      status: "uncertain",
    });
    expect((await host.control.inbox.nextClose()).code).toBe(4400);
    expect((await host.data.inbox.nextClose()).code).toBe(4400);
    expect(browser.control.socket.readyState).toBe(WebSocket.OPEN);
    const lease = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      return state.storage.sql.exec("SELECT client_id, fence, expires_at FROM writer_lease").one();
    });
    expect(lease).toMatchObject({ client_id: browser.connection.clientId, fence: "1" });
    expect(Number(lease.expires_at)).toBeGreaterThan(Date.now());
  });

  it("fences a throwing Host attach-request sink while preserving browser recovery state", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.writerCapability);
    const attached = await attachBrowser(session, host, browser);
    const writerLease = String(attached.welcome.writerLease);

    browser.data.socket.close(1000, "replace browser data");
    const resync = await browser.control.inbox.nextJson<{
      dataTicket: string;
      deliveryGeneration: string;
      type: string;
    }>();
    browser.data = await upgrade(session.sessionId, "data", resync.dataTicket);
    browser.connection.deliveryGeneration = resync.deliveryGeneration;
    await injectHostControlSendFailure(session);

    sendBrowserAttach(browser, { sessionEpoch: "7", eventSeq: "0", nextPtyOffset: "0" });
    await drainSession(session.sessionId);
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toEqual({
      type: "host-offline",
    });
    expect(browser.control.inbox.pendingMessageCount).toBe(0);
    expect((await host.control.inbox.nextClose()).code).toBe(4400);
    expect((await host.data.inbox.nextClose()).code).toBe(4400);
    expect(browser.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(browser.data.socket.readyState).toBe(WebSocket.OPEN);

    const recovery = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const sockets = state.getWebSockets(`client:${browser.connection.clientId}`);
        const control = sockets.find((socket) => {
          return (socket.deserializeAttachment() as { channel?: string }).channel === "control";
        });
        const data = sockets.find((socket) => {
          return (socket.deserializeAttachment() as { channel?: string }).channel === "data";
        });
        return {
          control: control?.deserializeAttachment(),
          data: data?.deserializeAttachment(),
        } as {
          control: { controlState: string; deliveryGeneration: string; leaseFence: string };
          data: { dataState: string; deliveryGeneration: string };
        };
      },
    );
    expect(recovery.control).toMatchObject({
      controlState: "active",
      deliveryGeneration: "2",
      leaseFence: "1",
    });
    expect(recovery.data).toMatchObject({
      dataState: "catching-up",
      deliveryGeneration: "2",
    });
    expect(await renewWriterLease(session, browser, writerLease)).toMatchObject({
      type: "writer-lease-status",
      active: true,
    });
  });

  it("continues host-offline broadcast after one browser control sink fails", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const failing = await openBrowser(session, session.observerCapability);
    const healthy = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, failing);
    await attachBrowser(session, host, healthy);
    await injectServerSendFailure(session, failing, "control");

    host.data.socket.send(ptyFrame(2n, 0n, textEncoder.encode("gap")));
    expect((await host.control.inbox.nextClose()).code).toBe(4400);
    expect((await host.data.inbox.nextClose()).code).toBe(4400);
    const [controlClose, dataClose] = await Promise.all([
      failing.control.inbox.nextClose(),
      failing.data.inbox.nextClose(),
    ]);
    expect(controlClose.code).toBe(4400);
    expect(dataClose.code).toBe(4400);
    expect(await healthy.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "host-offline",
    });
    expect(healthy.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(healthy.data.socket.readyState).toBe(WebSocket.OPEN);
    const healthyData = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const socket = state
          .getWebSockets(`client:${healthy.connection.clientId}`)
          .find((candidate) => {
            return (candidate.deserializeAttachment() as { channel?: string }).channel === "data";
          });
        return socket?.deserializeAttachment() as { dataState: string };
      },
    );
    expect(healthyData.dataState).toBe("catching-up");
  });

  it("rejects host-ready until the same fenced connection has an open data channel", async () => {
    const session = await createSession();
    const connection = await createConnectionSet(session.sessionId, session.hostCapability);
    const control = await upgrade(session.sessionId, "control", connection.controlTicket);

    control.socket.send(
      JSON.stringify({
        type: "host-ready",
        engineId,
        sessionEpoch: "7",
        headEventSeq: "0",
        nextPtyOffset: "0",
      }),
    );
    expect((await control.inbox.nextClose()).code).toBe(4400);
  });

  it("fails a current Host that sends data before the ready barrier", async () => {
    const session = await createSession();
    const connection = await createConnectionSet(session.sessionId, session.hostCapability);
    const control = await upgrade(session.sessionId, "control", connection.controlTicket);
    const data = await upgrade(session.sessionId, "data", connection.dataTicket);

    data.socket.send(ptyFrame(1n, 0n, textEncoder.encode("early")));
    expect((await control.inbox.nextClose()).code).toBe(4400);
    expect((await data.inbox.nextClose()).code).toBe(4400);
  });

  it("accepts consecutive Host data only after host-ready-ack", async () => {
    const session = await createSession();
    const host = await openHost(session);

    host.data.socket.send(ptyFrame(1n, 0n, textEncoder.encode("a")));
    host.data.socket.send(ptyFrame(2n, 1n, textEncoder.encode("b")));
    await drainSession(session.sessionId);

    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
    const head = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      return state.storage.sql
        .exec("SELECT head_event_seq, next_pty_offset FROM session_state")
        .one();
    });
    expect(head).toMatchObject({ head_event_seq: "2", next_pty_offset: "2" });
  });

  it("invalidates a pre-ready host connection when its data channel closes", async () => {
    const session = await createSession();
    const connection = await createConnectionSet(session.sessionId, session.hostCapability);
    const control = await upgrade(session.sessionId, "control", connection.controlTicket);
    const data = await upgrade(session.sessionId, "data", connection.dataTicket);

    data.socket.close(4001, "forged replacement before ready");
    expect((await control.inbox.nextClose()).code).toBe(4400);
  });

  it("closes same-set host data when its pre-ready control channel closes", async () => {
    const session = await createSession();
    const connection = await createConnectionSet(session.sessionId, session.hostCapability);
    const control = await upgrade(session.sessionId, "control", connection.controlTicket);
    const data = await upgrade(session.sessionId, "data", connection.dataTicket);

    control.socket.close(1000, "control closed before ready");
    expect((await data.inbox.nextClose()).code).toBe(4400);
  });

  it("orders directed replay before broadcast and fences host reconnect with a fresh cursor", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser);

    const first = textEncoder.encode("one");
    host.data.socket.send(ptyFrame(1n, 0n, first));
    await drainSession(session.sessionId);
    await beginWarmDelivery(session, host, browser, 1n, 3n);
    host.data.socket.send(
      ptyFrame(
        1n,
        0n,
        first,
        BigInt(browser.connection.deliveryGeneration),
        browser.connection.streamId,
      ),
    );
    host.data.socket.send(
      replayCommit(
        1n,
        3n,
        BigInt(browser.connection.deliveryGeneration),
        browser.connection.streamId,
      ),
    );
    const replayedFirst = await browser.data.inbox.nextDataFrame();
    expect(replayedFirst).toMatchObject({ kind: DataFrameKind.PtyOutput, eventSeq: 1n });
    expect(new TextDecoder().decode(replayedFirst.payload)).toBe("one");
    expect((await browser.data.inbox.nextDataFrame()).kind).toBe(DataFrameKind.ReplayCommit);

    const second = textEncoder.encode("two");
    const third = textEncoder.encode("three");
    host.data.socket.send(ptyFrame(2n, 3n, second));
    host.data.socket.send(ptyFrame(3n, 6n, third));
    const deliveredSecond = await browser.data.inbox.nextDataFrame();
    const deliveredThird = await browser.data.inbox.nextDataFrame();
    expect(deliveredSecond).toMatchObject({
      deliveryGeneration: 1n,
      eventSeq: 2n,
      ptyOffset: 3n,
      streamId: browser.connection.streamId,
    });
    expect(deliveredThird.eventSeq).toBe(3n);

    browser.control.socket.send(
      JSON.stringify({
        type: "ack",
        sessionEpoch: "7",
        deliveryGeneration: "1",
        eventSeq: "2",
        nextPtyOffset: "6",
      }),
    );
    await drainSession(session.sessionId);
    const credit = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      const dataSocket = state
        .getWebSockets(`client:${browser.connection.clientId}`)
        .find((socket) => {
          return (socket.deserializeAttachment() as { channel?: string }).channel === "data";
        });
      return dataSocket?.deserializeAttachment() as {
        ackedPtyOffset: string;
        sentPtyOffset: string;
      };
    });
    expect(credit).toMatchObject({ ackedPtyOffset: "6", sentPtyOffset: "11" });

    const hostTwoConnection = await createConnectionSet(session.sessionId, session.hostCapability);
    const hostTwoControl = await upgrade(
      session.sessionId,
      "control",
      hostTwoConnection.controlTicket,
    );
    expect((await browser.control.inbox.nextJson<Record<string, unknown>>()).type).toBe(
      "host-offline",
    );
    browser.control.socket.send(
      JSON.stringify({
        type: "ack",
        sessionEpoch: "7",
        deliveryGeneration: "1",
        eventSeq: "3",
        nextPtyOffset: "11",
      }),
    );
    await drainSession(session.sessionId);
    expect(browser.control.socket.readyState).toBe(WebSocket.OPEN);

    await upgrade(session.sessionId, "data", hostTwoConnection.dataTicket);
    hostTwoControl.socket.send(
      JSON.stringify({
        type: "host-ready",
        engineId,
        sessionEpoch: "7",
        headEventSeq: "3",
        nextPtyOffset: "11",
      }),
    );
    expect(await hostTwoControl.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "host-ready-ack",
      sessionEpoch: "7",
      headEventSeq: "3",
      nextPtyOffset: "11",
    });
    const reconnect = await browser.control.inbox.nextJson<{
      dataTicket: string;
      deliveryGeneration: string;
      reason: string;
      type: string;
    }>();
    expect(reconnect).toMatchObject({
      type: "resync-required",
      reason: "host-reconnect",
      deliveryGeneration: "2",
    });
    expect(hostTwoControl.inbox.pendingMessageCount).toBe(0);
    expect((await browser.data.inbox.nextClose()).code).toBe(4001);
    browser.control.socket.send(
      JSON.stringify({
        type: "ack",
        sessionEpoch: "7",
        deliveryGeneration: "1",
        eventSeq: "3",
        nextPtyOffset: "11",
      }),
    );
    browser.control.socket.send(JSON.stringify(keyFrame("stale", "awaiting_attach", "1")));
    await drainSession(session.sessionId);
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "input-ack",
      inputEpoch: "awaiting_attach",
      clientInputSeq: "1",
      status: "rejected",
    });
    expect(browser.control.socket.readyState).toBe(WebSocket.OPEN);
    await upgrade(session.sessionId, "data", reconnect.dataTicket);
    browser.control.socket.send(
      JSON.stringify({
        type: "attach",
        engineId,
        deliveryGeneration: "2",
        hasLiveReplica: true,
        lastSessionEpoch: "7",
        lastEventSeq: "3",
        nextPtyOffset: "11",
      }),
    );
    await drainSession(session.sessionId);
    expect(browser.control.inbox.pendingMessageCount).toBe(0);
    expect(await hostTwoControl.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "attach-request",
      hasLiveReplica: true,
      deliveryGeneration: "2",
      lastEventSeq: "3",
      nextPtyOffset: "11",
    });
  });

  it("drops a delayed old-generation attach after a cross-channel Host reset", async () => {
    const session = await createSession();
    const firstHost = await openHost(session);
    const browser = await openBrowser(session, session.writerCapability);
    const attached = await attachBrowser(session, firstHost, browser);
    const writerLease = String(attached.welcome.writerLease);

    browser.data.socket.close(1000, "begin generation two recovery");
    const generationTwo = await browser.control.inbox.nextJson<{
      dataTicket: string;
      deliveryGeneration: string;
      type: string;
    }>();
    expect(generationTwo).toMatchObject({
      type: "resync-required",
      deliveryGeneration: "2",
    });
    browser.connection.deliveryGeneration = "2";

    sendBrowserAttach(browser, { sessionEpoch: "7", eventSeq: "0", nextPtyOffset: "0" });
    await drainSession(session.sessionId);
    expect(browser.control.inbox.pendingMessageCount).toBe(0);
    expect(firstHost.control.inbox.pendingMessageCount).toBe(0);

    browser.data = await upgrade(session.sessionId, "data", generationTwo.dataTicket);
    const secondHost = await openHost(session);
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toEqual({
      type: "host-offline",
    });
    const generationThree = await browser.control.inbox.nextJson<{
      dataTicket: string;
      deliveryGeneration: string;
      type: string;
    }>();
    expect(generationThree).toMatchObject({
      type: "resync-required",
      deliveryGeneration: "3",
    });
    expect((await browser.data.inbox.nextClose()).code).toBe(4001);

    sendBrowserAttach(browser, { sessionEpoch: "7", eventSeq: "0", nextPtyOffset: "0" });
    await drainSession(session.sessionId);
    expect(browser.control.inbox.pendingMessageCount).toBe(0);
    expect(secondHost.control.inbox.pendingMessageCount).toBe(0);

    const recovering = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const control = state
          .getWebSockets(`client:${browser.connection.clientId}`)
          .find((socket) => {
            return (socket.deserializeAttachment() as { channel?: string }).channel === "control";
          });
        return control?.deserializeAttachment() as {
          controlState: string;
          deliveryGeneration: string;
          leaseFence: string | null;
        };
      },
    );
    expect(recovering).toMatchObject({
      controlState: "active",
      deliveryGeneration: "3",
      leaseFence: "1",
    });

    const interrupt = {
      ...keyFrame(writerLease, "reordered_attach", "1"),
      code: "KeyC",
      key: "c",
      text: "c",
      modifiers: 2,
      altGraph: false,
      unshiftedCodepoint: 0x63,
    };
    browser.control.socket.send(JSON.stringify(interrupt));
    expect(await secondHost.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "key",
      inputEpoch: "reordered_attach",
      clientInputSeq: "1",
    });

    browser.data = await upgrade(session.sessionId, "data", generationThree.dataTicket);
    const request = await reattachBrowser(session, secondHost, browser, "3");
    expect(request).toMatchObject({
      connectionId: browser.connection.connectionId,
      deliveryGeneration: "3",
    });
  });

  it("scopes acknowledgement monotonicity to the current data connection", async () => {
    const session = await createSession();
    const host = await openHost(session, "100", "100");
    const first = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, first, {
      sessionEpoch: "7",
      eventSeq: "100",
      nextPtyOffset: "100",
    });
    first.control.socket.send(
      JSON.stringify({
        type: "ack",
        sessionEpoch: "7",
        deliveryGeneration: "1",
        eventSeq: "100",
        nextPtyOffset: "100",
      }),
    );
    await drainSession(session.sessionId);

    const replacement = await openBrowser(
      session,
      session.observerCapability,
      first.connection.clientId ?? undefined,
    );
    await attachBrowser(session, host, replacement, {
      sessionEpoch: "7",
      eventSeq: "90",
      nextPtyOffset: "90",
    });
    await beginWarmDelivery(session, host, replacement, 100n, 100n);
    host.data.socket.send(
      ptyFrame(
        91n,
        90n,
        textEncoder.encode("x"),
        BigInt(replacement.connection.deliveryGeneration),
        replacement.connection.streamId,
      ),
    );
    expect(await replacement.data.inbox.nextDataFrame()).toMatchObject({
      eventSeq: 91n,
      ptyOffset: 90n,
    });
    replacement.control.socket.send(
      JSON.stringify({
        type: "ack",
        sessionEpoch: "7",
        deliveryGeneration: replacement.connection.deliveryGeneration,
        eventSeq: "91",
        nextPtyOffset: "91",
      }),
    );
    await drainSession(session.sessionId);
    expect(replacement.control.socket.readyState).toBe(WebSocket.OPEN);
    const acknowledged = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const dataSocket = state
          .getWebSockets(`client:${replacement.connection.clientId}`)
          .find((socket) => {
            return (socket.deserializeAttachment() as { channel?: string }).channel === "data";
          });
        return dataSocket?.deserializeAttachment() as { ackedEventSeq: string };
      },
    );
    expect(acknowledged.ackedEventSeq).toBe("91");
  });

  it("bumps generation on full reconnect before old directed replay can reach new data", async () => {
    const session = await createSession();
    const host = await openHost(session, "100", "100");
    const first = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, first);
    await beginWarmDelivery(session, host, first, 100n, 100n);
    host.data.socket.send(ptyFrame(1n, 0n, textEncoder.encode("a"), 1n, first.connection.streamId));
    expect((await first.data.inbox.nextDataFrame()).eventSeq).toBe(1n);

    first.control.socket.close(1000, "full reconnect");
    await drainSession(session.sessionId);
    const connection = await createConnectionSet(
      session.sessionId,
      session.observerCapability,
      first.connection.clientId ?? undefined,
    );
    expect(connection.deliveryGeneration).toBe("2");
    const control = await upgrade(session.sessionId, "control", connection.controlTicket);
    host.data.socket.send(ptyFrame(50n, 50n, textEncoder.encode("stale"), 1n, connection.streamId));
    await drainSession(session.sessionId);
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);

    const data = await upgrade(session.sessionId, "data", connection.dataTicket);
    const replacement = { connection, control, data };
    const attached = await attachBrowser(session, host, replacement, {
      sessionEpoch: "7",
      eventSeq: "1",
      nextPtyOffset: "1",
    });
    expect(attached.welcome).toMatchObject({ deliveryGeneration: "2" });
    expect(connection.deliveryGeneration).toBe("2");
    await beginWarmDelivery(session, host, replacement, 100n, 100n);

    host.data.socket.send(ptyFrame(50n, 50n, textEncoder.encode("stale"), 1n, connection.streamId));
    await drainSession(session.sessionId);
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(data.inbox.pendingMessageCount).toBe(0);

    host.data.socket.send(ptyFrame(2n, 1n, textEncoder.encode("b"), 2n, connection.streamId));
    expect(await data.inbox.nextDataFrame()).toMatchObject({
      deliveryGeneration: 2n,
      eventSeq: 2n,
      ptyOffset: 1n,
    });
  });

  it("replaces each pending full set with one reset data ticket without blocking Host", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browsers: BrowserEndpoint[] = [];
    for (let index = 0; index < 16; index += 1) {
      const browser = await openBrowser(session, session.observerCapability);
      await attachBrowser(session, host, browser);
      browsers.push(browser);
    }
    for (const browser of browsers) {
      await createConnectionSet(
        session.sessionId,
        session.observerCapability,
        browser.connection.clientId ?? undefined,
      );
    }

    const replacementHost = await createConnectionSet(session.sessionId, session.hostCapability);
    const replacementControl = await upgrade(
      session.sessionId,
      "control",
      replacementHost.controlTicket,
    );
    await upgrade(session.sessionId, "data", replacementHost.dataTicket);
    replacementControl.socket.send(
      JSON.stringify({
        type: "host-ready",
        engineId,
        sessionEpoch: "7",
        headEventSeq: "0",
        nextPtyOffset: "0",
      }),
    );
    expect(await replacementControl.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "host-ready-ack",
    });

    const pending = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) =>
        state.storage.sql
          .exec(
            `SELECT COUNT(*) AS tickets,
                    COUNT(DISTINCT connection_set_id) AS sets
             FROM connection_ticket WHERE peer = 'browser'`,
          )
          .one() as { sets: number; tickets: number },
    );
    expect(pending).toEqual({ sets: 16, tickets: 16 });
    await createConnectionSet(session.sessionId, session.hostCapability);
  });

  it("forwards strictly increasing writer fences in Host control order", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const first = await openBrowser(session, session.writerCapability);
    const firstAttach = await attachBrowser(session, host, first);
    const firstLease = firstAttach.welcome.writerLease;
    expect(typeof firstLease).toBe("string");

    const second = await openBrowser(session, session.writerCapability);
    const secondAttach = await attachBrowser(session, host, second);
    expect(secondAttach.welcome.writerLease).toBeUndefined();

    second.control.socket.send(JSON.stringify(keyFrame("lease_not_owned", "input_second", "1")));
    expect(await second.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "input-ack",
      status: "rejected",
    });

    first.control.socket.send(JSON.stringify(keyFrame(String(firstLease), "input_first", "1")));
    const forwarded = await host.control.inbox.nextJson<Record<string, unknown>>();
    expect(forwarded).toMatchObject({
      type: "key",
      connectionId: first.connection.connectionId,
      clientId: first.connection.clientId,
      writerFence: "1",
      inputEpoch: "input_first",
      clientInputSeq: "1",
      action: "press",
      altGraph: true,
      composing: false,
      consumedModifiers: 0,
      unshiftedCodepoint: 0x65,
    });

    await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      state.storage.sql.exec("UPDATE writer_lease SET expires_at = 0 WHERE singleton = 1");
    });
    const secondReplacement = await openBrowser(
      session,
      session.writerCapability,
      second.connection.clientId ?? undefined,
    );
    const { welcome: secondWelcome } = await attachBrowser(session, host, secondReplacement);
    const secondLease = secondWelcome.writerLease;
    expect(typeof secondLease).toBe("string");

    first.control.socket.send(JSON.stringify(keyFrame(String(firstLease), "input_first", "2")));
    expect(await first.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "input-ack",
      status: "rejected",
    });
    first.control.socket.close(1000, "old writer closed late");
    await drainSession(session.sessionId);
    const lease = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      return state.storage.sql.exec("SELECT client_id, fence, expires_at FROM writer_lease").one();
    });
    expect(lease).toMatchObject({ client_id: second.connection.clientId, fence: "2" });
    expect(Number(lease.expires_at)).toBeGreaterThan(Date.now());

    secondReplacement.control.socket.send(
      JSON.stringify(keyFrame(String(secondLease), "input_second", "1")),
    );
    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "key",
      clientId: second.connection.clientId,
      writerFence: "2",
      inputEpoch: "input_second",
      clientInputSeq: "1",
    });

    secondReplacement.control.socket.send(
      JSON.stringify({
        ...keyFrame(String(secondLease), "input_second", "3"),
        clientId: first.connection.clientId,
      }),
    );
    expect((await secondReplacement.control.inbox.nextClose()).code).toBe(4400);
  });

  it("keeps the writer fence high-water when inactive client LRU reclaims its row", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const first = await openBrowser(session, session.writerCapability);
    const firstAttach = await attachBrowser(session, host, first);
    expect(typeof firstAttach.welcome.writerLease).toBe("string");

    await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      state.storage.sql.exec("UPDATE writer_lease SET expires_at = 0 WHERE singleton = 1");
      state.storage.sql.exec(
        "UPDATE client_delivery SET updated_at = 0 WHERE client_id = ?",
        first.connection.clientId,
      );
    });
    first.control.socket.close(1000, "expire first writer");
    first.data.socket.close(1000, "expire first writer");
    await drainSession(session.sessionId);

    for (let index = 0; index < 15; index += 1) {
      const inactive = await openBrowser(session, session.observerCapability);
      inactive.control.socket.close(1000, "inactive observer");
      inactive.data.socket.close(1000, "inactive observer");
      await drainSession(session.sessionId);
    }

    const next = await openBrowser(session, session.writerCapability);
    const nextAttach = await attachBrowser(session, host, next);
    expect(typeof nextAttach.welcome.writerLease).toBe("string");
    const state = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durableState) => ({
        firstClient: durableState.storage.sql
          .exec(
            "SELECT client_id FROM client_delivery WHERE client_id = ?",
            first.connection.clientId,
          )
          .toArray()[0],
        lease: durableState.storage.sql
          .exec("SELECT client_id, fence, expires_at FROM writer_lease WHERE singleton = 1")
          .one(),
      }),
    );
    expect(state.firstClient).toBeUndefined();
    expect(state.lease).toMatchObject({
      client_id: next.connection.clientId,
      fence: "2",
    });
    expect(Number(state.lease.expires_at)).toBeGreaterThan(Date.now());
  });

  it("fails closed instead of wrapping an exhausted writer fence", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const first = await openBrowser(session, session.writerCapability);
    await attachBrowser(session, host, first);
    first.control.socket.close(1000, "expire first writer");
    first.data.socket.close(1000, "expire first writer");
    await drainSession(session.sessionId);
    await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE writer_lease SET fence = '18446744073709551615', expires_at = 0 WHERE singleton = 1",
      );
    });

    const next = await openBrowser(session, session.writerCapability);
    sendBrowserAttach(next, { sessionEpoch: "7", eventSeq: "0", nextPtyOffset: "0" });
    expect((await next.control.inbox.nextClose()).code).toBe(4400);
    await drainSession(session.sessionId);
    const lease = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) =>
      state.storage.sql
        .exec("SELECT fence, expires_at FROM writer_lease WHERE singleton = 1")
        .one(),
    );
    expect(lease).toEqual({ fence: "18446744073709551615", expires_at: 0 });
    expect(host.control.inbox.pendingMessageCount).toBe(0);
  });

  it("keeps a writer lease alive across semantic idle with explicit ten-second renewals", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const startedAt = Date.now();
    try {
      const session = await createSession();
      const host = await openHost(session);
      const writer = await openBrowser(session, session.writerCapability);
      const attached = await attachBrowser(session, host, writer);
      const writerLease = String(attached.welcome.writerLease);

      for (const elapsed of [10_000, 20_000, 30_000, 40_000]) {
        vi.setSystemTime(startedAt + elapsed);
        expect(await renewWriterLease(session, writer, writerLease)).toEqual({
          type: "writer-lease-status",
          active: true,
          expiresAt: startedAt + elapsed + 30_000,
        });
      }

      vi.setSystemTime(startedAt + 40_001);
      writer.control.socket.send(JSON.stringify(keyFrame(writerLease, "idle_writer", "1")));
      expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
        type: "key",
        inputEpoch: "idle_writer",
        clientInputSeq: "1",
      });

      vi.setSystemTime(startedAt + 70_002);
      expect(await renewWriterLease(session, writer, writerLease)).toEqual({
        type: "writer-lease-status",
        active: false,
      });
      writer.control.socket.send(JSON.stringify(keyFrame(writerLease, "idle_writer", "2")));
      expect(await writer.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
        type: "input-ack",
        inputEpoch: "idle_writer",
        clientInputSeq: "2",
        status: "rejected",
      });
      expect(host.control.inbox.pendingMessageCount).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it("reports a fenced writer lease as lost and releases the current fence on control close", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const first = await openBrowser(session, session.writerCapability);
    const firstAttach = await attachBrowser(session, host, first);
    const firstLease = String(firstAttach.welcome.writerLease);

    await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      state.storage.sql.exec("UPDATE writer_lease SET expires_at = 0 WHERE singleton = 1");
    });
    const second = await openBrowser(session, session.writerCapability);
    const secondAttach = await attachBrowser(session, host, second);
    const secondLease = String(secondAttach.welcome.writerLease);
    expect(secondLease).not.toBe(firstLease);

    expect(await renewWriterLease(session, first, firstLease)).toEqual({
      type: "writer-lease-status",
      active: false,
    });
    const secondStatus = await renewWriterLease(session, second, secondLease);
    expect(secondStatus).toMatchObject({ type: "writer-lease-status", active: true });
    expect(Number(secondStatus.expiresAt)).toBeGreaterThan(Date.now());

    second.control.socket.close(1000, "writer left");
    await drainSession(session.sessionId);
    const lease = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      return state.storage.sql.exec("SELECT client_id, fence, expires_at FROM writer_lease").one();
    });
    expect(lease).toMatchObject({
      client_id: second.connection.clientId,
      fence: "2",
      expires_at: 0,
    });
  });

  it("forwards structured text, focus, and mouse input without terminal encoding", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const writer = await openBrowser(session, session.writerCapability);
    const observer = await openBrowser(session, session.observerCapability);
    const writerAttach = await attachBrowser(session, host, writer);
    await attachBrowser(session, host, observer);
    const writerLease = String(writerAttach.welcome.writerLease);

    const inputs = [
      {
        type: "text",
        writerLease,
        inputEpoch: "structured_input",
        clientInputSeq: "1",
        data: "你好, terminal",
      },
      {
        type: "focus",
        writerLease,
        inputEpoch: "structured_input",
        clientInputSeq: "2",
        focused: false,
      },
      {
        type: "mouse",
        writerLease,
        inputEpoch: "structured_input",
        clientInputSeq: "3",
        action: "wheel",
        button: null,
        buttons: 0,
        modifiers: 3,
        altGraph: true,
        surface: { x: 120, y: 48 },
        deltaX: 0,
        deltaY: -12.5,
        deltaMode: "pixel",
      },
    ] as const;

    for (const input of inputs) {
      writer.control.socket.send(JSON.stringify(input));
      expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toEqual({
        ...input,
        connectionId: writer.connection.connectionId,
        clientId: writer.connection.clientId,
        writerFence: "1",
      });
    }

    host.control.socket.send(
      JSON.stringify({
        type: "input-ack",
        connectionId: writer.connection.connectionId,
        inputEpoch: "structured_input",
        clientInputSeq: "3",
        status: "written",
        authorityEventSeq: "0",
      }),
    );
    expect(await writer.control.inbox.nextJson<Record<string, unknown>>()).toEqual({
      type: "input-ack",
      inputEpoch: "structured_input",
      clientInputSeq: "3",
      status: "written",
      authorityEventSeq: "0",
    });

    observer.control.socket.send(
      JSON.stringify({
        ...inputs[0],
        writerLease: "observer_cannot_write",
        inputEpoch: "observer_input",
      }),
    );
    expect(await observer.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "input-ack",
      inputEpoch: "observer_input",
      clientInputSeq: "1",
      status: "rejected",
    });
    expect(host.control.inbox.pendingMessageCount).toBe(0);
  });

  it("isolates malformed mouse input without affecting Host authority", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);

    browser.control.socket.send(
      JSON.stringify({
        type: "mouse",
        writerLease: "malformed_mouse",
        inputEpoch: "malformed_mouse",
        clientInputSeq: "1",
        action: "move",
        button: null,
        buttons: 0,
        modifiers: 0,
        altGraph: false,
        surface: { x: 1, y: 2 },
        deltaY: 1,
      }),
    );
    expect((await browser.control.inbox.nextClose()).code).toBe(4400);
    expect((await browser.data.inbox.nextClose()).code).toBe(4001);
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
  });

  it("isolates a pre-attach data disconnect instead of leaving an unrecoverable control", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.writerCapability);

    browser.data.socket.close(1000, "data closed before attach");
    expect((await browser.control.inbox.nextClose()).code).toBe(4400);
    await drainSession(session.sessionId);
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
    const state = await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      return {
        leases: durable.storage.sql.exec("SELECT fence FROM writer_lease").toArray().length,
        tickets: durable.storage.sql
          .exec("SELECT ticket_digest FROM connection_ticket WHERE peer = 'browser'")
          .toArray().length,
      };
    });
    expect(state).toEqual({ leases: 0, tickets: 0 });
  });

  it("fails closed when a current-generation initial attach has no data channel", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const connection = await createConnectionSet(session.sessionId, session.writerCapability);
    const control = await upgrade(session.sessionId, "control", connection.controlTicket);

    control.socket.send(
      JSON.stringify({
        type: "attach",
        engineId,
        deliveryGeneration: connection.deliveryGeneration,
        hasLiveReplica: true,
        lastSessionEpoch: "7",
        lastEventSeq: "0",
        nextPtyOffset: "0",
      }),
    );
    expect((await control.inbox.nextClose()).code).toBe(4400);
    await drainSession(session.sessionId);
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.control.inbox.pendingMessageCount).toBe(0);
    const leases = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      return state.storage.sql.exec("SELECT fence FROM writer_lease").toArray();
    });
    expect(leases).toHaveLength(0);
  });

  it("isolates a repeated attach without replacing the generation baseline", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);

    const awaiting = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const dataSocket = state
          .getWebSockets(`client:${browser.connection.clientId}`)
          .find((socket) => {
            return (socket.deserializeAttachment() as { channel?: string }).channel === "data";
          });
        return dataSocket?.deserializeAttachment() as {
          dataState: string;
          sentEventSeq: string | null;
        };
      },
    );
    expect(awaiting).toMatchObject({ dataState: "awaiting-attach", sentEventSeq: null });

    await attachBrowser(session, host, browser);
    const attached = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const dataSocket = state
          .getWebSockets(`client:${browser.connection.clientId}`)
          .find((socket) => {
            return (socket.deserializeAttachment() as { channel?: string }).channel === "data";
          });
        return dataSocket?.deserializeAttachment() as {
          dataState: string;
          firstEventSeq: string | null;
          firstPtyOffset: string | null;
        };
      },
    );
    expect(attached).toMatchObject({
      dataState: "catching-up",
      firstEventSeq: "0",
      firstPtyOffset: "0",
    });

    browser.control.socket.send(
      JSON.stringify({
        type: "attach",
        engineId,
        deliveryGeneration: browser.connection.deliveryGeneration,
        hasLiveReplica: true,
        lastSessionEpoch: "7",
        lastEventSeq: "0",
        nextPtyOffset: "0",
      }),
    );
    const [controlClose, dataClose] = await Promise.all([
      browser.control.inbox.nextClose(),
      browser.data.inbox.nextClose(),
    ]);
    expect(controlClose.code).toBe(4400);
    expect(dataClose.code).toBe(4400);
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.control.inbox.pendingMessageCount).toBe(0);
  });

  it("does not acquire a writer lease for a stale replica epoch", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const writer = await openBrowser(session, session.writerCapability);

    writer.control.socket.send(
      JSON.stringify({
        type: "attach",
        engineId,
        deliveryGeneration: writer.connection.deliveryGeneration,
        hasLiveReplica: true,
        lastSessionEpoch: "6",
        lastEventSeq: "0",
        nextPtyOffset: "0",
      }),
    );
    await drainSession(session.sessionId);
    expect(await writer.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "resync-required",
      reason: "epoch-changed",
    });
    expect(await renewWriterLease(session, writer, "lease_not_issued")).toEqual({
      type: "writer-lease-status",
      active: false,
    });
    expect(host.control.inbox.pendingMessageCount).toBe(0);
    const leases = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) =>
      state.storage.sql.exec("SELECT fence FROM writer_lease").toArray(),
    );
    expect(leases).toHaveLength(0);
  });

  it("seeds a cold replica before manifest acknowledgement and relays the directed tail", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const snapshotId = "snapshot_relay_seed_0001";
    await publishSnapshot(session, snapshotId);
    const browser = await openBrowser(session, session.writerCapability);
    const initialAttach = await attachBrowser(session, host, browser);
    const writerLease = String(initialAttach.welcome.writerLease);

    browser.data.socket.close(1000, "switch to cold recovery");
    const resync = await browser.control.inbox.nextJson<{
      dataTicket: string;
      deliveryGeneration: string;
      type: string;
    }>();
    expect(resync).toMatchObject({ type: "resync-required", deliveryGeneration: "2" });

    const interrupt = {
      ...recoveryInputFrames(writerLease)[0],
      inputEpoch: "cold_recovery",
      observedEventSeq: "0",
    };
    browser.control.socket.send(JSON.stringify(interrupt));
    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toEqual({
      ...interrupt,
      connectionId: browser.connection.connectionId,
      clientId: browser.connection.clientId,
      writerFence: "1",
    });

    browser.data = await upgrade(session.sessionId, "data", resync.dataTicket);
    const coldRequest = await reattachBrowser(session, host, browser, "2", null);
    expect(coldRequest).toMatchObject({
      connectionId: browser.connection.connectionId,
      hasLiveReplica: false,
    });

    host.data.socket.send(ptyFrame(1n, 0n, textEncoder.encode("a")));
    host.data.socket.send(ptyFrame(2n, 1n, textEncoder.encode("b")));
    await drainSession(session.sessionId);
    expect(browser.data.inbox.pendingMessageCount).toBe(0);

    host.data.socket.send(deliveryBarrier(browser, "snapshot", 2n, 2n, snapshotId));
    const acknowledgement = await host.control.inbox.nextJson<Record<string, unknown>>();
    expect(acknowledgement).toMatchObject({
      type: "delivery-barrier-result",
      status: "ready",
      mode: "snapshot",
      snapshotId,
      connectionId: browser.connection.connectionId,
      streamId: browser.connection.streamId,
      deliveryGeneration: browser.connection.deliveryGeneration,
      commitEventSeq: "2",
      commitPtyOffset: "2",
    });
    expect(browser.control.inbox.pendingMessageCount).toBe(1);

    const seeded = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      const dataSocket = state
        .getWebSockets(`client:${browser.connection.clientId}`)
        .find((socket) => {
          return (socket.deserializeAttachment() as { channel?: string }).channel === "data";
        });
      return dataSocket?.deserializeAttachment() as {
        ackedEventSeq: string;
        ackedPtyOffset: string;
        sentEventSeq: string;
        sentPtyOffset: string;
        replayCommitEventSeq: string;
        replayCommitPtyOffset: string;
        replayMode: string;
        snapshotId: string;
      };
    });
    expect(seeded).toMatchObject({
      ackedEventSeq: "0",
      ackedPtyOffset: "0",
      sentEventSeq: "0",
      sentPtyOffset: "0",
      replayCommitEventSeq: "2",
      replayCommitPtyOffset: "2",
      replayMode: "snapshot",
      snapshotId,
    });
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "snapshot-manifest",
      snapshotId,
      engineId,
      sessionEpoch: "7",
      streamId: browser.connection.streamId,
      deliveryGeneration: browser.connection.deliveryGeneration,
      cutEventSeq: "0",
      nextPtyOffset: "0",
      commitEventSeq: "2",
      commitPtyOffset: "2",
      compression: "none",
      downloadPath: `/api/v1/sessions/${session.sessionId}/snapshots/${snapshotId}`,
      restoreThrough: "finish",
    });

    const generation = BigInt(browser.connection.deliveryGeneration);
    host.data.socket.send(
      ptyFrame(1n, 0n, textEncoder.encode("a"), generation, browser.connection.streamId),
    );
    host.data.socket.send(
      ptyFrame(2n, 1n, textEncoder.encode("b"), generation, browser.connection.streamId),
    );
    host.data.socket.send(replayCommit(2n, 2n, generation, browser.connection.streamId));
    expect(await browser.data.inbox.nextDataFrame()).toMatchObject({
      kind: DataFrameKind.PtyOutput,
      eventSeq: 1n,
      ptyOffset: 0n,
    });
    expect(await browser.data.inbox.nextDataFrame()).toMatchObject({
      kind: DataFrameKind.PtyOutput,
      eventSeq: 2n,
      ptyOffset: 1n,
    });
    expect(await browser.data.inbox.nextDataFrame()).toMatchObject({
      kind: DataFrameKind.ReplayCommit,
      eventSeq: 2n,
      ptyOffset: 2n,
    });
    host.data.socket.send(ptyFrame(3n, 2n, textEncoder.encode("c")));
    expect(await browser.data.inbox.nextDataFrame()).toMatchObject({
      kind: DataFrameKind.PtyOutput,
      eventSeq: 3n,
      ptyOffset: 2n,
    });

    const committedPin = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const dataSocket = state
          .getWebSockets(`client:${browser.connection.clientId}`)
          .find((socket) => {
            return (socket.deserializeAttachment() as { channel?: string }).channel === "data";
          });
        return dataSocket?.deserializeAttachment() as {
          dataState: string;
          snapshotId: string;
        };
      },
    );
    expect(committedPin).toMatchObject({ dataState: "synced", snapshotId });

    await evictDurableObject(sessionStub(session.sessionId));
    for (const nextSnapshotId of [
      "snapshot_retention_new_01",
      "snapshot_retention_new_02",
      "snapshot_retention_new_03",
    ]) {
      await publishSnapshot(session, nextSnapshotId);
    }
    const hibernatedPinState = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) =>
        state.storage.sql.exec("SELECT state FROM snapshot WHERE snapshot_id = ?", snapshotId).one()
          .state,
    );
    expect(hibernatedPinState).toBe("servable");
    const pinnedObjectKey = await storedSnapshotKey(session.sessionId, snapshotId);

    browser.data.socket.close(1000, "release snapshot pin");
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "resync-required",
      reason: "data-disconnected",
    });
    await runInDurableObject(sessionStub(session.sessionId), async (instance) => {
      await instance.alarm();
    });
    const released = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) =>
        state.storage.sql
          .exec("SELECT state FROM snapshot WHERE snapshot_id = ?", snapshotId)
          .toArray()[0],
    );
    expect(released).toBeUndefined();
    expect(await env.SNAPSHOTS.head(pinnedObjectKey)).toBeNull();
  });

  it("allows an unstarted live attach to fall back to a snapshot seed", async () => {
    const session = await createSession();
    const host = await openHost(session, "2", "2");
    const snapshotId = "snapshot_warm_fallback_01";
    await publishSnapshot(session, snapshotId, "0", "0");
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser, {
      sessionEpoch: "7",
      eventSeq: "1",
      nextPtyOffset: "1",
    });
    expect(browser.control.inbox.pendingMessageCount).toBe(0);

    await beginSnapshotDelivery(session, host, browser, snapshotId, 2n, 2n);
    const seeded = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      const dataSocket = state
        .getWebSockets(`client:${browser.connection.clientId}`)
        .find((socket) => {
          return (socket.deserializeAttachment() as { channel?: string }).channel === "data";
        });
      return dataSocket?.deserializeAttachment() as {
        firstEventSeq: string;
        firstPtyOffset: string;
        replayMode: string;
      };
    });
    expect(seeded).toMatchObject({
      firstEventSeq: "0",
      firstPtyOffset: "0",
      replayMode: "snapshot",
    });
  });

  it("isolates a browser manifest send failure without acknowledging or failing the Host", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const snapshotId = "snapshot_manifest_failure1";
    await publishSnapshot(session, snapshotId);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser, null);

    await injectServerSendFailure(session, browser, "control");

    host.data.socket.send(deliveryBarrier(browser, "snapshot", 0n, 0n, snapshotId));
    const [controlClose, dataClose, result] = await Promise.all([
      browser.control.inbox.nextClose(),
      browser.data.inbox.nextClose(),
      host.control.inbox.nextJson<Record<string, unknown>>(),
    ]);
    expect(controlClose.code).toBe(4400);
    expect(dataClose.code).toBe(4400);
    expect(result).toMatchObject({
      type: "delivery-barrier-result",
      status: "rejected",
      snapshotId,
      reason: "browser-control-send-failed",
      retryScope: "drop-client",
    });
    await drainSession(session.sessionId);
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
  });

  it("resets only the target when a directed browser data send fails", async () => {
    const session = await createSession();
    const host = await openHost(session, "1", "1");
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser);
    await beginWarmDelivery(session, host, browser, 1n, 1n);
    await injectServerSendFailure(session, browser, "data");

    host.data.socket.send(
      ptyFrame(
        1n,
        0n,
        textEncoder.encode("x"),
        BigInt(browser.connection.deliveryGeneration),
        browser.connection.streamId,
      ),
    );
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "resync-required",
      reason: "data-disconnected",
      deliveryGeneration: "2",
    });
    expect((await browser.data.inbox.nextClose()).code).toBe(4001);
    host.data.socket.send(ptyFrame(2n, 1n, textEncoder.encode("y")));
    await drainSession(session.sessionId);
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
  });

  it("continues canonical broadcast when one browser data send fails", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const failing = await openBrowser(session, session.observerCapability);
    const healthy = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, failing);
    await attachBrowser(session, host, healthy);
    await beginWarmDelivery(session, host, failing, 0n, 0n);
    host.data.socket.send(
      replayCommit(
        0n,
        0n,
        BigInt(failing.connection.deliveryGeneration),
        failing.connection.streamId,
      ),
    );
    expect((await failing.data.inbox.nextDataFrame()).kind).toBe(DataFrameKind.ReplayCommit);
    await beginWarmDelivery(session, host, healthy, 0n, 0n);
    host.data.socket.send(
      replayCommit(
        0n,
        0n,
        BigInt(healthy.connection.deliveryGeneration),
        healthy.connection.streamId,
      ),
    );
    expect((await healthy.data.inbox.nextDataFrame()).kind).toBe(DataFrameKind.ReplayCommit);
    await injectServerSendFailure(session, failing, "data");

    host.data.socket.send(ptyFrame(1n, 0n, textEncoder.encode("x")));
    expect(await healthy.data.inbox.nextDataFrame()).toMatchObject({
      kind: DataFrameKind.PtyOutput,
      eventSeq: 1n,
    });
    expect(await failing.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "resync-required",
      deliveryGeneration: "2",
    });
    await drainSession(session.sessionId);
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
    const head = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) =>
      state.storage.sql.exec("SELECT head_event_seq FROM session_state").one(),
    );
    expect(head).toMatchObject({ head_event_seq: "1" });
  });

  it("replays an exact snapshot barrier only while its seed has not advanced", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const snapshotId = "snapshot_offer_retry_0001";
    await publishSnapshot(session, snapshotId);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser, null);
    host.data.socket.send(ptyFrame(1n, 0n, textEncoder.encode("x")));
    await drainSession(session.sessionId);

    const barrier = deliveryBarrier(browser, "snapshot", 1n, 1n, snapshotId);
    host.data.socket.send(barrier);
    await Promise.all([
      host.control.inbox.nextJson<Record<string, unknown>>(),
      browser.control.inbox.nextJson<Record<string, unknown>>(),
    ]);
    host.data.socket.send(barrier);
    const [retryAck, retryManifest] = await Promise.all([
      host.control.inbox.nextJson<Record<string, unknown>>(),
      browser.control.inbox.nextJson<Record<string, unknown>>(),
    ]);
    expect(retryAck).toMatchObject({
      type: "delivery-barrier-result",
      status: "ready",
      snapshotId,
    });
    expect(retryManifest).toMatchObject({ type: "snapshot-manifest", snapshotId });

    host.data.socket.send(
      ptyFrame(
        1n,
        0n,
        textEncoder.encode("x"),
        BigInt(browser.connection.deliveryGeneration),
        browser.connection.streamId,
      ),
    );
    expect(await browser.data.inbox.nextDataFrame()).toMatchObject({ eventSeq: 1n });

    host.data.socket.send(barrier);
    expect((await host.control.inbox.nextClose()).code).toBe(4400);
    expect((await host.data.inbox.nextClose()).code).toBe(4400);
    const delivery = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const dataSocket = state
          .getWebSockets(`client:${browser.connection.clientId}`)
          .find((socket) => {
            return (socket.deserializeAttachment() as { channel?: string }).channel === "data";
          });
        return dataSocket?.deserializeAttachment() as {
          sentEventSeq: string;
          sentPtyOffset: string;
        };
      },
    );
    expect(delivery).toMatchObject({ sentEventSeq: "1", sentPtyOffset: "1" });
  });

  it("fails the Host when directed snapshot tail arrives before the ready acknowledgement", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const snapshotId = "snapshot_early_tail_00001";
    await publishSnapshot(session, snapshotId);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser, null);
    host.data.socket.send(ptyFrame(1n, 0n, textEncoder.encode("x")));
    await drainSession(session.sessionId);

    host.data.socket.send(
      ptyFrame(
        1n,
        0n,
        textEncoder.encode("x"),
        BigInt(browser.connection.deliveryGeneration),
        browser.connection.streamId,
      ),
    );
    expect((await host.control.inbox.nextClose()).code).toBe(4400);
    expect((await host.data.inbox.nextClose()).code).toBe(4400);
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "host-offline",
    });
  });

  it("fails the Host when a delivery barrier does not equal the canonical head", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const snapshotId = "snapshot_bad_barrier_0001";
    await publishSnapshot(session, snapshotId);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser, null);
    host.data.socket.send(ptyFrame(1n, 0n, textEncoder.encode("x")));
    await drainSession(session.sessionId);

    host.data.socket.send(deliveryBarrier(browser, "snapshot", 0n, 0n, snapshotId));
    expect((await host.control.inbox.nextClose()).code).toBe(4400);
    expect((await host.data.inbox.nextClose()).code).toBe(4400);
  });

  it("rejects a missing finalized snapshot without failing the Host", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const snapshotId = "snapshot_missing_barrier1";
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser, null);

    host.data.socket.send(deliveryBarrier(browser, "snapshot", 0n, 0n, snapshotId));
    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "delivery-barrier-result",
      status: "rejected",
      snapshotId,
      reason: "snapshot-missing",
      retryScope: "refresh-checkpoint",
    });
    expect(browser.control.inbox.pendingMessageCount).toBe(0);
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
  });

  it("rejects mismatched finalized snapshot metadata without failing the Host", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const snapshotId = "snapshot_bad_metadata_001";
    await publishSnapshot(session, snapshotId);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser, null);
    await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      state.storage.sql.exec(
        "UPDATE snapshot SET engine_id = 'ghostty:other' WHERE snapshot_id = ?",
        snapshotId,
      );
    });

    host.data.socket.send(deliveryBarrier(browser, "snapshot", 0n, 0n, snapshotId));
    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "delivery-barrier-result",
      status: "rejected",
      snapshotId,
      reason: "snapshot-metadata-mismatch",
      retryScope: "refresh-checkpoint",
    });
    expect(browser.control.inbox.pendingMessageCount).toBe(0);
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
  });

  it("returns stale when a barrier target closed its control connection", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser);
    const barrier = deliveryBarrier(browser, "warm", 0n, 0n);

    browser.control.socket.close(1000, "browser left");
    await drainSession(session.sessionId);
    host.data.socket.send(barrier);
    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "delivery-barrier-result",
      status: "stale",
      connectionId: browser.connection.connectionId,
      reason: "client-gone",
    });
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
  });

  it("keeps barrier outcomes legacy-shaped when the Host did not negotiate details", async () => {
    const session = await createSession();
    const host = await openHost(session, "0", "0", false);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser);
    const barrier = deliveryBarrier(browser, "warm", 0n, 0n);

    browser.control.socket.close(1000, "browser left");
    await drainSession(session.sessionId);
    host.data.socket.send(barrier);
    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toEqual({
      type: "delivery-barrier-result",
      status: "stale",
      mode: "warm",
      connectionId: browser.connection.connectionId,
      streamId: browser.connection.streamId,
      deliveryGeneration: browser.connection.deliveryGeneration,
      commitEventSeq: "0",
      commitPtyOffset: "0",
    });
  });

  it("returns stale when a browser data reset fenced the barrier generation", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser);
    const barrier = deliveryBarrier(browser, "warm", 0n, 0n);

    browser.data.socket.close(1000, "data disconnected");
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "resync-required",
      deliveryGeneration: "2",
    });
    host.data.socket.send(barrier);
    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "delivery-barrier-result",
      status: "stale",
      deliveryGeneration: "1",
      reason: "generation-fenced",
    });
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
  });

  it("returns stale after a full browser connection replacement", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);
    const other = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser);
    await attachBrowser(session, host, other);
    const barrier = deliveryBarrier(browser, "warm", 0n, 0n);

    const replacement = await openBrowser(
      session,
      session.observerCapability,
      browser.connection.clientId ?? undefined,
    );
    expect(replacement.control.socket.readyState).toBe(WebSocket.OPEN);
    host.data.socket.send(barrier);
    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "delivery-barrier-result",
      status: "stale",
      deliveryGeneration: "1",
      reason: "generation-fenced",
    });
    expect(other.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(other.data.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
  });

  it("restores finalized snapshot metadata and delivery seeding after hibernation", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const snapshotId = "snapshot_offer_hibernate1";
    await publishSnapshot(session, snapshotId);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser, null);

    await evictDurableObject(sessionStub(session.sessionId), { webSockets: "hibernate" });
    host.data.socket.send(deliveryBarrier(browser, "snapshot", 0n, 0n, snapshotId));
    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "delivery-barrier-result",
      status: "ready",
      snapshotId,
    });
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "snapshot-manifest",
      snapshotId,
      cutEventSeq: "0",
      nextPtyOffset: "0",
    });

    const restored = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const snapshot = state.storage.sql
          .exec("SELECT state, object_key FROM snapshot WHERE snapshot_id = ?", snapshotId)
          .one();
        const data = state
          .getWebSockets(`client:${browser.connection.clientId}`)
          .find((socket) => {
            return (socket.deserializeAttachment() as { channel?: string }).channel === "data";
          })
          ?.deserializeAttachment();
        return { snapshot, data };
      },
    );
    expect(restored.snapshot).toMatchObject({ state: "servable" });
    expect(restored.data).toMatchObject({
      snapshotId,
      firstEventSeq: "0",
      firstPtyOffset: "0",
    });
  });

  it("retains negotiated barrier outcome details across hibernation", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser, null);

    await evictDurableObject(sessionStub(session.sessionId), { webSockets: "hibernate" });
    host.data.socket.send(deliveryBarrier(browser, "warm", 0n, 0n));

    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "delivery-barrier-result",
      status: "rejected",
      mode: "warm",
      reason: "missing-live-seed",
      retryScope: "same-generation",
    });
  });

  it("refuses a directed commit until an explicit live-replica baseline exists", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser, null);

    host.data.socket.send(
      replayCommit(
        0n,
        0n,
        BigInt(browser.connection.deliveryGeneration),
        browser.connection.streamId,
      ),
    );
    expect((await host.control.inbox.nextClose()).code).toBe(4400);
    expect((await host.data.inbox.nextClose()).code).toBe(4400);
    expect((await browser.control.inbox.nextJson<Record<string, unknown>>()).type).toBe(
      "host-offline",
    );
    expect(browser.data.inbox.pendingMessageCount).toBe(0);
  });

  it("fails the host when ReplayCommit is behind the canonical head", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser);

    host.data.socket.send(ptyFrame(1n, 0n, textEncoder.encode("one")));
    await drainSession(session.sessionId);
    await beginWarmDelivery(session, host, browser, 1n, 3n);
    host.data.socket.send(
      replayCommit(
        0n,
        0n,
        BigInt(browser.connection.deliveryGeneration),
        browser.connection.streamId,
      ),
    );
    expect((await host.control.inbox.nextClose()).code).toBe(4400);
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "host-offline",
    });
    expect(browser.data.inbox.pendingMessageCount).toBe(0);
  });

  it("fails the Host when canonical output overtakes a pinned replay commit", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser);
    host.data.socket.send(ptyFrame(1n, 0n, textEncoder.encode("a")));
    await drainSession(session.sessionId);
    await beginWarmDelivery(session, host, browser, 1n, 1n);

    host.data.socket.send(ptyFrame(2n, 1n, textEncoder.encode("b")));
    expect((await host.control.inbox.nextClose()).code).toBe(4400);
    expect((await host.data.inbox.nextClose()).code).toBe(4400);
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "host-offline",
    });
    expect(browser.data.inbox.pendingMessageCount).toBe(0);
  });

  it("rejects directed Reset instead of silently desynchronizing a synced client", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser);
    await beginWarmDelivery(session, host, browser, 0n, 0n);
    host.data.socket.send(
      replayCommit(
        0n,
        0n,
        BigInt(browser.connection.deliveryGeneration),
        browser.connection.streamId,
      ),
    );
    expect((await browser.data.inbox.nextDataFrame()).kind).toBe(DataFrameKind.ReplayCommit);

    host.data.socket.send(
      resetFrame(BigInt(browser.connection.deliveryGeneration), browser.connection.streamId),
    );
    expect((await host.control.inbox.nextClose()).code).toBe(4400);
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "host-offline",
    });
    expect(browser.data.inbox.pendingMessageCount).toBe(0);
  });

  it("does not trust a forged slow-client close code from browser data", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.writerCapability);
    const attached = await attachBrowser(session, host, browser);
    const writerLease = String(attached.welcome.writerLease);

    browser.data.socket.close(4008, "forged slow-client reset");
    const resync = await browser.control.inbox.nextJson<{
      dataTicket: string;
      deliveryGeneration: string;
      reason: string;
      type: string;
    }>();
    expect(resync).toMatchObject({
      type: "resync-required",
      reason: "data-disconnected",
      deliveryGeneration: "2",
    });
    expect(browser.control.socket.readyState).toBe(WebSocket.OPEN);
    const replacementData = await upgrade(session.sessionId, "data", resync.dataTicket);
    browser.control.socket.send(
      JSON.stringify({
        type: "attach",
        engineId,
        deliveryGeneration: "2",
        hasLiveReplica: true,
        lastSessionEpoch: "7",
        lastEventSeq: "0",
        nextPtyOffset: "0",
      }),
    );
    await drainSession(session.sessionId);
    expect(browser.control.inbox.pendingMessageCount).toBe(0);
    browser.connection.deliveryGeneration = "2";
    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "attach-request",
      deliveryGeneration: "2",
    });

    browser.control.socket.send(JSON.stringify(keyFrame(writerLease, "input_recovered", "1")));
    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "key",
      clientId: browser.connection.clientId,
      inputEpoch: "input_recovered",
    });
    expect(replacementData.socket.readyState).toBe(WebSocket.OPEN);
    const lease = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) =>
      state.storage.sql.exec("SELECT client_id, fence FROM writer_lease").one(),
    );
    expect(lease).toMatchObject({ client_id: browser.connection.clientId, fence: "1" });
  });

  it("does not trust a forged replacement close code from host data", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser);

    host.data.socket.close(4001, "forged replacement");
    expect((await host.control.inbox.nextClose()).code).toBe(4400);
    expect((await browser.control.inbox.nextJson<Record<string, unknown>>()).type).toBe(
      "host-offline",
    );
    const dataAttachment = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const socket = state
          .getWebSockets(`client:${browser.connection.clientId}`)
          .find((candidate) => {
            return (candidate.deserializeAttachment() as { channel?: string }).channel === "data";
          });
        return socket?.deserializeAttachment() as {
          dataState: string;
          sentEventSeq: string | null;
        };
      },
    );
    expect(dataAttachment).toMatchObject({ dataState: "catching-up", sentEventSeq: "0" });
  });

  it("fails the current Host pair when an open data socket receives an error event", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser);

    await runInDurableObject(sessionStub(session.sessionId), async (instance, state) => {
      const hostData = state.getWebSockets("peer:host").find((socket) => {
        return (socket.deserializeAttachment() as { channel?: string }).channel === "data";
      });
      if (hostData === undefined) throw new Error("current Host data socket missing");
      expect(hostData.readyState).toBe(WebSocket.OPEN);
      await instance.webSocketError(hostData, new Error("injected Host data error"));
    });

    expect((await host.control.inbox.nextClose()).code).toBe(4400);
    expect((await host.data.inbox.nextClose()).code).toBe(4400);
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "host-offline",
    });
  });

  it("resets browser delivery when an open data socket receives an error event", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability, undefined, [
      RelayCapability.capabilityNegotiationV1,
      RelayCapability.authorityDataV2,
    ]);
    await attachBrowser(session, host, browser);

    await runInDurableObject(sessionStub(session.sessionId), async (instance, state) => {
      const browserData = state
        .getWebSockets(`client:${browser.connection.clientId}`)
        .find((socket) => {
          return (socket.deserializeAttachment() as { channel?: string }).channel === "data";
        });
      if (browserData === undefined) throw new Error("current browser data socket missing");
      expect(browserData.readyState).toBe(WebSocket.OPEN);
      await instance.webSocketError(browserData, new Error("injected browser data error"));
    });

    const resync = await browser.control.inbox.nextJson<{
      dataTicket: string;
      deliveryGeneration: string;
      reason: string;
      type: string;
    }>();
    expect(resync).toMatchObject({
      type: "resync-required",
      reason: "data-disconnected",
      deliveryGeneration: "2",
    });
    expect(browser.control.socket.readyState).toBe(WebSocket.OPEN);

    const replacementState = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const ticket = state.storage.sql
          .exec(
            `SELECT relay_capabilities_json FROM connection_ticket
             WHERE peer = 'browser' AND channel = 'data' AND client_id = ?`,
            browser.connection.clientId,
          )
          .one() as { relay_capabilities_json: string };
        return {
          capabilities: ticket.relay_capabilities_json,
          client: state.storage.sql
            .exec(
              `SELECT delivery_generation, recovery_strategy FROM client_delivery
               WHERE client_id = ?`,
              browser.connection.clientId,
            )
            .one(),
        };
      },
    );
    expect(JSON.parse(replacementState.capabilities)).toEqual([
      RelayCapability.capabilityNegotiationV1,
      RelayCapability.authorityDataV2,
    ]);
    expect(replacementState.client).toEqual({
      delivery_generation: "2",
      recovery_strategy: "v2",
    });

    const replacementData = await upgrade(session.sessionId, "data", resync.dataTicket);
    const replacementAttachment = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const dataSocket = state
          .getWebSockets(`client:${browser.connection.clientId}`)
          .find((socket) => {
            return (socket.deserializeAttachment() as { channel?: string }).channel === "data";
          });
        return dataSocket?.deserializeAttachment() as { relayCapabilities?: string[] };
      },
    );
    expect(replacementAttachment.relayCapabilities).toEqual([
      RelayCapability.capabilityNegotiationV1,
      RelayCapability.authorityDataV2,
    ]);
    const replacement = { ...browser, data: replacementData };
    const request = await reattachBrowser(session, host, replacement, "2");
    expect(request).toMatchObject({
      connectionId: browser.connection.connectionId,
      deliveryGeneration: "2",
    });
  });

  it("keeps writer control usable after a replacement ticket expires and permits full replacement", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.writerCapability);
    const attached = await attachBrowser(session, host, browser);
    const writerLease = String(attached.welcome.writerLease);

    browser.data.socket.close(1000, "expire replacement ticket");
    const resync = await browser.control.inbox.nextJson<{
      dataTicket: string;
      deliveryGeneration: string;
      type: string;
    }>();
    expect(resync).toMatchObject({ type: "resync-required", deliveryGeneration: "2" });
    await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      state.storage.sql.exec(
        `UPDATE connection_ticket SET expires_at = 0
         WHERE peer = 'browser' AND channel = 'data' AND client_id = ?`,
        browser.connection.clientId,
      );
    });

    const expired = await upgradeResponse(session.sessionId, "data", resync.dataTicket);
    expect(expired.status).toBe(401);
    await expired.text();

    const input = {
      ...keyFrame(writerLease, "expired_ticket", "1"),
      code: "KeyC",
      key: "c",
      text: "c",
      modifiers: 2,
      altGraph: false,
      unshiftedCodepoint: 0x63,
    };
    browser.control.socket.send(JSON.stringify(input));
    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toEqual({
      ...input,
      connectionId: browser.connection.connectionId,
      clientId: browser.connection.clientId,
      writerFence: "1",
    });

    const connection = await createConnectionSet(
      session.sessionId,
      session.writerCapability,
      browser.connection.clientId ?? undefined,
    );
    expect(connection.deliveryGeneration).toBe("3");
    const control = await upgrade(session.sessionId, "control", connection.controlTicket);
    expect((await browser.control.inbox.nextClose()).code).toBe(4001);
    const data = await upgrade(session.sessionId, "data", connection.dataTicket);
    const replacement = { connection, control, data };
    const replacementAttach = await attachBrowser(session, host, replacement);
    const replacementLease = String(replacementAttach.welcome.writerLease);
    expect(replacementAttach.welcome).toMatchObject({ deliveryGeneration: "3" });
    expect(replacementLease).not.toBe(writerLease);

    replacement.control.socket.send(JSON.stringify(keyFrame(writerLease, "full_replacement", "1")));
    expect(await replacement.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "input-ack",
      inputEpoch: "full_replacement",
      clientInputSeq: "1",
      status: "rejected",
    });
    replacement.control.socket.send(
      JSON.stringify(keyFrame(replacementLease, "full_replacement", "1")),
    );
    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "key",
      clientId: browser.connection.clientId,
      writerFence: "2",
      inputEpoch: "full_replacement",
      clientInputSeq: "1",
    });
    const lease = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      return state.storage.sql.exec("SELECT client_id, fence FROM writer_lease").one();
    });
    expect(lease).toMatchObject({ client_id: browser.connection.clientId, fence: "2" });
  });

  it("drops stale host fences and fails closed on a current host sequence gap", async () => {
    const session = await createSession();
    const oldHost = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, oldHost, browser);

    await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      state.storage.sql.exec("UPDATE session_state SET host_fence = '2' WHERE singleton = 1");
    });
    oldHost.data.socket.send(ptyFrame(1n, 0n, textEncoder.encode("stale")));
    await drainSession(session.sessionId);
    const head = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      return state.storage.sql.exec("SELECT head_event_seq FROM session_state").one();
    });
    expect(head.head_event_seq).toBe("0");

    const currentHost = await openHost(session);
    expect((await browser.control.inbox.nextJson<Record<string, unknown>>()).type).toBe(
      "host-offline",
    );
    const reconnect = await browser.control.inbox.nextJson<{
      dataTicket: string;
      deliveryGeneration: string;
      type: string;
    }>();
    expect(reconnect).toMatchObject({
      type: "resync-required",
      deliveryGeneration: "2",
    });
    await upgrade(session.sessionId, "data", reconnect.dataTicket);
    browser.control.socket.send(
      JSON.stringify({
        type: "attach",
        engineId,
        deliveryGeneration: "2",
        hasLiveReplica: true,
        lastSessionEpoch: "7",
        lastEventSeq: "0",
        nextPtyOffset: "0",
      }),
    );
    await drainSession(session.sessionId);
    expect(browser.control.inbox.pendingMessageCount).toBe(0);
    expect(await currentHost.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "attach-request",
    });
    currentHost.data.socket.send(ptyFrame(2n, 0n, textEncoder.encode("gap")));
    expect((await currentHost.control.inbox.nextClose()).code).toBe(4400);
    expect((await currentHost.data.inbox.nextClose()).code).toBe(4400);
    expect((await browser.control.inbox.nextJson<Record<string, unknown>>()).type).toBe(
      "host-offline",
    );
  });

  it("resets only slow data with a same-set single-use ticket and survives hibernation", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.writerCapability);
    const initialAttach = await attachBrowser(session, host, browser);
    const writerLease = String(initialAttach.welcome.writerLease);

    await beginWarmDelivery(session, host, browser, 0n, 0n);
    host.data.socket.send(
      replayCommit(
        0n,
        0n,
        BigInt(browser.connection.deliveryGeneration),
        browser.connection.streamId,
      ),
    );
    expect((await browser.data.inbox.nextDataFrame()).kind).toBe(DataFrameKind.ReplayCommit);

    const chunk = new Uint8Array(16 * 1024);
    for (let index = 0; index < 32; index += 1) {
      host.data.socket.send(ptyFrame(BigInt(index + 1), BigInt(index * chunk.byteLength), chunk));
    }
    await drainSession(session.sessionId);
    const resync = await browser.control.inbox.nextJson<{
      dataTicket: string;
      deliveryGeneration: string;
      expiresAt: number;
      reason: string;
      type: string;
    }>();
    expect(resync).toMatchObject({
      type: "resync-required",
      reason: "slow-client",
      deliveryGeneration: "2",
    });
    expect(typeof resync.dataTicket).toBe("string");
    expect(resync.expiresAt - Date.now()).toBeGreaterThan(25_000);
    let lastDelivered: DataFrame | undefined;
    for (let eventSeq = 1; eventSeq <= 31; eventSeq += 1) {
      lastDelivered = await browser.data.inbox.nextDataFrame();
    }
    expect(lastDelivered).toMatchObject({ kind: DataFrameKind.PtyOutput, eventSeq: 31n });
    expect((await browser.data.inbox.nextClose()).code).toBe(4008);
    expect(browser.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "delivery-reset",
      connectionId: browser.connection.connectionId,
      streamId: browser.connection.streamId,
      deliveryGeneration: "2",
      reason: "slow-client",
    });

    const recoveringControl = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const controlSocket = state
          .getWebSockets(`client:${browser.connection.clientId}`)
          .find((socket) => {
            return (socket.deserializeAttachment() as { channel?: string }).channel === "control";
          });
        return controlSocket?.deserializeAttachment() as {
          controlState: string;
          deliveryGeneration: string;
          leaseFence: string | null;
        };
      },
    );
    expect(recoveringControl).toMatchObject({
      controlState: "active",
      deliveryGeneration: "2",
      leaseFence: "1",
    });

    for (const input of recoveryInputFrames(writerLease)) {
      browser.control.socket.send(JSON.stringify(input));
      expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toEqual({
        ...input,
        connectionId: browser.connection.connectionId,
        clientId: browser.connection.clientId,
        writerFence: "1",
      });
    }
    host.control.socket.send(
      JSON.stringify({
        type: "input-ack",
        connectionId: browser.connection.connectionId,
        inputEpoch: "delivery_recovery",
        clientInputSeq: "1",
        status: "written",
        authorityEventSeq: "32",
      }),
    );
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toEqual({
      type: "input-ack",
      inputEpoch: "delivery_recovery",
      clientInputSeq: "1",
      status: "written",
      authorityEventSeq: "32",
    });

    const consumedInitialTicket = await upgradeResponse(
      session.sessionId,
      "data",
      browser.connection.dataTicket,
    );
    expect(consumedInitialTicket.status).toBe(401);
    await consumedInitialTicket.text();
    const replacementData = await upgrade(session.sessionId, "data", resync.dataTicket);
    const consumedReplacementTicket = await upgradeResponse(
      session.sessionId,
      "data",
      resync.dataTicket,
    );
    expect(consumedReplacementTicket.status).toBe(401);
    await consumedReplacementTicket.text();
    const replacementAttachment = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const socket = state
          .getWebSockets(`set:${browser.connection.connectionSetId}`)
          .find((candidate) => {
            const value = candidate.deserializeAttachment() as { channel?: string };
            return value.channel === "data";
          });
        return socket?.deserializeAttachment() as {
          connectionSetId: string;
          dataState: string;
          deliveryGeneration: string;
          sentEventSeq: string | null;
        };
      },
    );
    expect(replacementAttachment).toMatchObject({
      connectionSetId: browser.connection.connectionSetId,
      dataState: "awaiting-attach",
      deliveryGeneration: "2",
      sentEventSeq: null,
    });

    await evictDurableObject(sessionStub(session.sessionId), { webSockets: "hibernate" });
    const postHibernateInput = {
      ...recoveryInputFrames(writerLease)[1],
      clientInputSeq: "6",
    };
    browser.control.socket.send(JSON.stringify(postHibernateInput));
    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toEqual({
      ...postHibernateInput,
      connectionId: browser.connection.connectionId,
      clientId: browser.connection.clientId,
      writerFence: "1",
    });
    const restoredAwaiting = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const sockets = state.getWebSockets(`client:${browser.connection.clientId}`);
        const control = sockets.find((socket) => {
          return (socket.deserializeAttachment() as { channel?: string }).channel === "control";
        });
        const data = sockets.find((socket) => {
          return (socket.deserializeAttachment() as { channel?: string }).channel === "data";
        });
        return {
          control: control?.deserializeAttachment(),
          data: data?.deserializeAttachment(),
        } as {
          control: { controlState: string; deliveryGeneration: string; leaseFence: string };
          data: { dataState: string; deliveryGeneration: string; sentEventSeq: string | null };
        };
      },
    );
    expect(restoredAwaiting.control).toMatchObject({
      controlState: "active",
      deliveryGeneration: "2",
      leaseFence: "1",
    });
    expect(restoredAwaiting.data).toMatchObject({
      dataState: "awaiting-attach",
      deliveryGeneration: "2",
      sentEventSeq: null,
    });
    const recoveryLeaseStatus = await renewWriterLease(session, browser, writerLease);
    expect(recoveryLeaseStatus).toMatchObject({
      type: "writer-lease-status",
      active: true,
    });
    expect(Number(recoveryLeaseStatus.expiresAt)).toBeGreaterThan(Date.now());

    browser.control.socket.send(
      JSON.stringify({
        type: "attach",
        engineId: `${engineId}:mismatch`,
        deliveryGeneration: "2",
        hasLiveReplica: true,
        lastSessionEpoch: "7",
        lastEventSeq: "31",
        nextPtyOffset: String(31 * chunk.byteLength),
      }),
    );
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "resync-required",
      reason: "engine-mismatch",
      deliveryGeneration: "2",
    });
    expect(host.control.inbox.pendingMessageCount).toBe(0);

    browser.control.socket.send(
      JSON.stringify({
        type: "attach",
        engineId,
        deliveryGeneration: "2",
        hasLiveReplica: true,
        lastSessionEpoch: "7",
        lastEventSeq: "31",
        nextPtyOffset: String(31 * chunk.byteLength),
      }),
    );
    await drainSession(session.sessionId);
    expect(browser.control.inbox.pendingMessageCount).toBe(0);
    browser.connection.deliveryGeneration = "2";
    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "attach-request",
      deliveryGeneration: "2",
    });
    const committedRecovery = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const sockets = state.getWebSockets(`client:${browser.connection.clientId}`);
        const control = sockets.find((socket) => {
          return (socket.deserializeAttachment() as { channel?: string }).channel === "control";
        });
        const data = sockets.find((socket) => {
          return (socket.deserializeAttachment() as { channel?: string }).channel === "data";
        });
        return {
          control: control?.deserializeAttachment(),
          data: data?.deserializeAttachment(),
        } as {
          control: { controlState: string; leaseFence: string };
          data: { dataState: string; firstEventSeq: string; firstPtyOffset: string };
        };
      },
    );
    expect(committedRecovery.control).toMatchObject({ controlState: "active", leaseFence: "1" });
    expect(committedRecovery.data).toMatchObject({
      dataState: "catching-up",
      firstEventSeq: "31",
      firstPtyOffset: String(31 * chunk.byteLength),
    });
    await beginWarmDelivery(session, host, browser, 32n, BigInt(32 * chunk.byteLength));

    const oldGeneration = ptyFrame(
      32n,
      BigInt(31 * chunk.byteLength),
      chunk,
      1n,
      browser.connection.streamId,
    );
    host.data.socket.send(oldGeneration);
    host.data.socket.send(
      ptyFrame(32n, BigInt(31 * chunk.byteLength), chunk, 2n, browser.connection.streamId),
    );
    host.data.socket.send(
      replayCommit(32n, BigInt(32 * chunk.byteLength), 2n, browser.connection.streamId),
    );
    const replayedTail = await replacementData.inbox.nextDataFrame();
    expect(replayedTail).toMatchObject({ kind: DataFrameKind.PtyOutput, eventSeq: 32n });
    expect((await replacementData.inbox.nextDataFrame()).kind).toBe(DataFrameKind.ReplayCommit);

    browser.control.socket.send(
      JSON.stringify({
        type: "ack",
        sessionEpoch: "7",
        deliveryGeneration: "2",
        eventSeq: "32",
        nextPtyOffset: String(32 * chunk.byteLength),
      }),
    );
    await drainSession(session.sessionId);

    await evictDurableObject(sessionStub(session.sessionId), { webSockets: "hibernate" });
    const restoredCredit = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const socket = state
          .getWebSockets(`client:${browser.connection.clientId}`)
          .find((candidate) => {
            return (candidate.deserializeAttachment() as { channel?: string }).channel === "data";
          });
        return socket?.deserializeAttachment() as {
          ackedEventSeq: string;
          ackedPtyOffset: string;
          sentEventSeq: string;
          sentPtyOffset: string;
        };
      },
    );
    expect(restoredCredit).toMatchObject({
      ackedEventSeq: "32",
      ackedPtyOffset: String(32 * chunk.byteLength),
      sentEventSeq: "32",
      sentPtyOffset: String(32 * chunk.byteLength),
    });

    const finalPayload = textEncoder.encode("live");
    host.data.socket.send(ptyFrame(33n, BigInt(32 * chunk.byteLength), finalPayload));
    await drainSession(session.sessionId);
    const afterHibernate = await replacementData.inbox.nextDataFrame();
    expect(afterHibernate).toMatchObject({
      eventSeq: 33n,
      deliveryGeneration: 2n,
      streamId: browser.connection.streamId,
    });

    const finalOffset = String(32 * chunk.byteLength + finalPayload.byteLength);
    browser.control.socket.send(
      JSON.stringify({
        type: "ack",
        sessionEpoch: "7",
        deliveryGeneration: "2",
        eventSeq: "33",
        nextPtyOffset: finalOffset,
      }),
    );
    await drainSession(session.sessionId);
    const delivery = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const socket = state
          .getWebSockets(`client:${browser.connection.clientId}`)
          .find((candidate) => {
            return (candidate.deserializeAttachment() as { channel?: string }).channel === "data";
          });
        return socket?.deserializeAttachment() as {
          ackedEventSeq: string;
          ackedPtyOffset: string;
          sentEventSeq: string;
          sentPtyOffset: string;
        };
      },
    );
    expect(delivery).toMatchObject({
      ackedEventSeq: "33",
      ackedPtyOffset: finalOffset,
      sentEventSeq: "33",
      sentPtyOffset: finalOffset,
    });
  });
});
