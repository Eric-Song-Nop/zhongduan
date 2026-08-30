import {
  DATA_HEADER_BYTES,
  DELIVERY_ENVELOPE_V3_HEADER_BYTES,
  DataFrameFlag,
  DataFrameKind,
  RELAY_CAPABILITIES_HEADER,
  RelayCapability,
  decodeDataFrame,
  decodeDeliveryEnvelopeV3,
  decodeRelayToHostControlFrame,
  decodeServerControlFrame,
  encodeDataFrame,
  encodeDeliveryEnvelopeV3,
  encodeRecoveryStartFence,
  type RecoveryStart,
  type RecoveryV3HostPrepare,
  type RecoveryV3HostSourceGrant,
  type RecoveryV3HostStartReady,
} from "@zhongduan/protocol";
import { env, exports as workerExports } from "cloudflare:workers";
import { evictDurableObject, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it } from "vitest";
import { RelayRecoveryStore } from "../src/worker/relay-recovery-store";
import { RelayStore } from "../src/worker/relay-store";
import { SocketAttachmentV2Schema, SocketAttachmentV3Schema } from "../src/worker/relay-socket";
import { RelayV3DeliveryRing } from "../src/worker/relay-v3-delivery-ring";
import { RelayV3DeliveryScheduler } from "../src/worker/relay-v3-delivery-scheduler";

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
  recoveryStrategy?: "v3";
  streamId: number;
}

interface ReadyHost {
  connection: ConnectionSet;
  control: WebSocket;
  data: WebSocket;
}

interface RecoveryBrowser {
  connection: ConnectionSet & { clientId: string };
  control: WebSocket;
  data: WebSocket;
  prepare: RecoveryV3HostPrepare;
  sessionId: string;
  welcome: Extract<ReturnType<typeof decodeServerControlFrame<"v3">>, { type: "welcome" }>;
}

interface DeliveryYieldGate {
  calls: number;
  releases: Array<() => void>;
}

type RecoveryHostControlFrame = ReturnType<typeof decodeRelayToHostControlFrame<"v3">>;

const origin = "https://terminal.example.test";
const engineId = "ghostty:test+snapshot-v1+wterm:test";
const recoveryV3HostCapabilities = [
  RelayCapability.capabilityNegotiationV1,
  RelayCapability.authorityDataV2,
  RelayCapability.wireEndpointV3,
  RelayCapability.recoveryV3GapFillV1,
] as const;
const recoveryV3BrowserCapabilities = [
  RelayCapability.capabilityNegotiationV1,
  RelayCapability.authorityDataV2,
  RelayCapability.wireEndpointV3,
  RelayCapability.deliveryEnvelopeV3,
  RelayCapability.deliveryReceiveCreditV1,
  RelayCapability.authorityApplyProgressV1,
  RelayCapability.recoveryV3GapFillV1,
] as const;
const sockets = new Set<WebSocket>();
const sessions = new Set<string>();
let sessionCounter = 0;

function nextSessionId(): string {
  sessionCounter += 1;
  return `session_recovery_runtime_${sessionCounter.toString().padStart(16, "0")}`;
}

function sessionStub(sessionId: string) {
  return env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${sessionId}`));
}

async function createSession(): Promise<CreatedSession> {
  const sessionId = nextSessionId();
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
  sessions.add(sessionId);
  return response.json<CreatedSession>();
}

async function createConnectionSet(
  sessionId: string,
  capability: string,
  relayCapabilities: readonly string[],
  clientId?: string,
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

async function upgrade(
  sessionId: string,
  channel: "control" | "data",
  ticket: string,
): Promise<WebSocket> {
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions/${sessionId}/ws/${channel}?ticket=${ticket}`, {
      headers: { upgrade: "websocket" },
    }),
  );
  expect(response.status).toBe(101);
  if (response.webSocket === null) throw new Error("upgrade did not return a WebSocket");
  response.webSocket.accept();
  sockets.add(response.webSocket);
  response.webSocket.addEventListener("close", () => sockets.delete(response.webSocket!), {
    once: true,
  });
  return response.webSocket;
}

function nextText(socket: WebSocket): Promise<string> {
  return new Promise((resolve, reject) => {
    socket.addEventListener(
      "message",
      (event) => {
        if (typeof event.data === "string") resolve(event.data);
        else reject(new Error("expected a text WebSocket message"));
      },
      { once: true },
    );
    socket.addEventListener("error", () => reject(new Error("WebSocket failed")), { once: true });
  });
}

function nextTexts(socket: WebSocket, count: number): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const messages: string[] = [];
    const onMessage = (event: MessageEvent) => {
      if (typeof event.data !== "string") {
        socket.removeEventListener("message", onMessage);
        reject(new Error("expected text WebSocket messages"));
        return;
      }
      messages.push(event.data);
      if (messages.length === count) {
        socket.removeEventListener("message", onMessage);
        resolve(messages);
      }
    };
    socket.addEventListener("message", onMessage);
    socket.addEventListener("error", () => reject(new Error("WebSocket failed")), { once: true });
  });
}

function nextBinary(socket: WebSocket): Promise<Uint8Array> {
  return new Promise((resolve, reject) => {
    socket.addEventListener(
      "message",
      (event) => {
        if (event.data instanceof ArrayBuffer) {
          resolve(new Uint8Array(event.data));
        } else if (ArrayBuffer.isView(event.data)) {
          resolve(
            new Uint8Array(event.data.buffer, event.data.byteOffset, event.data.byteLength).slice(),
          );
        } else if (event.data instanceof Blob) {
          void event.data.arrayBuffer().then(
            (buffer) => resolve(new Uint8Array(buffer)),
            () => reject(new Error("failed to read binary WebSocket Blob")),
          );
        } else {
          reject(new Error("expected a binary WebSocket message"));
        }
      },
      { once: true },
    );
    socket.addEventListener("error", () => reject(new Error("WebSocket failed")), { once: true });
  });
}

function nextBinaries(socket: WebSocket, count: number): Promise<Uint8Array[]> {
  return new Promise((resolve, reject) => {
    const messages: Uint8Array[] = [];
    let conversions = Promise.resolve();
    const onMessage = (event: MessageEvent) => {
      conversions = conversions.then(async () => {
        let message: Uint8Array;
        if (event.data instanceof ArrayBuffer) {
          message = new Uint8Array(event.data);
        } else if (ArrayBuffer.isView(event.data)) {
          message = new Uint8Array(
            event.data.buffer,
            event.data.byteOffset,
            event.data.byteLength,
          ).slice();
        } else if (event.data instanceof Blob) {
          message = new Uint8Array(await event.data.arrayBuffer());
        } else {
          throw new Error("expected binary WebSocket messages");
        }
        messages.push(message);
        if (messages.length !== count) return;
        socket.removeEventListener("message", onMessage);
        resolve(messages);
      });
      void conversions.catch((error: unknown) => {
        socket.removeEventListener("message", onMessage);
        reject(error instanceof Error ? error : new Error("binary WebSocket conversion failed"));
      });
    };
    socket.addEventListener("message", onMessage);
    socket.addEventListener("error", () => reject(new Error("WebSocket failed")), { once: true });
  });
}

function nextClose(socket: WebSocket): Promise<CloseEvent> {
  return new Promise((resolve) => socket.addEventListener("close", resolve, { once: true }));
}

function nextHostControlFrame(
  socket: WebSocket,
  matches: (frame: RecoveryHostControlFrame) => boolean,
): Promise<RecoveryHostControlFrame> {
  return new Promise((resolve, reject) => {
    const onMessage = (event: MessageEvent) => {
      if (typeof event.data !== "string") return;
      let frame: RecoveryHostControlFrame;
      try {
        frame = decodeRelayToHostControlFrame(event.data, "v3");
      } catch {
        return;
      }
      if (!matches(frame)) return;
      socket.removeEventListener("message", onMessage);
      resolve(frame);
    };
    socket.addEventListener("message", onMessage);
    socket.addEventListener("error", () => reject(new Error("WebSocket failed")), { once: true });
  });
}

async function waitForCondition(
  condition: () => boolean | Promise<boolean>,
  description: string,
): Promise<void> {
  for (let turn = 0; turn < 200; turn += 1) {
    if (await condition()) return;
    await scheduler.wait(0);
  }
  throw new Error(`timed out waiting for ${description}`);
}

async function openReadyHost(
  session: CreatedSession,
  authority: { headEventSeq: string; nextPtyOffset: string } = {
    headEventSeq: "0",
    nextPtyOffset: "0",
  },
): Promise<ReadyHost> {
  const connection = await createConnectionSet(
    session.sessionId,
    session.hostCapability,
    recoveryV3HostCapabilities,
  );
  const control = await upgrade(session.sessionId, "control", connection.controlTicket);
  const data = await upgrade(session.sessionId, "data", connection.dataTicket);
  const acknowledgement = nextText(control);
  control.send(
    JSON.stringify({
      type: "host-ready",
      engineId,
      sessionEpoch: "7",
      headEventSeq: authority.headEventSeq,
      nextPtyOffset: authority.nextPtyOffset,
    }),
  );
  expect(decodeRelayToHostControlFrame(await acknowledgement, "v3")).toMatchObject({
    type: "host-ready-ack",
  });
  return { connection, control, data };
}

async function publishSnapshot(session: CreatedSession, snapshotId: string): Promise<void> {
  await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
    state.storage.sql.exec(
      `INSERT INTO snapshot
        (snapshot_id, session_epoch, cut_event_seq, next_pty_offset, engine_id,
         object_key, r2_version, etag, sha256, compressed_length,
         uncompressed_length, compression, state, created_at)
       VALUES (?, '7', '0', '0', ?, ?, 'r2-version', 'etag', ?, 1, '1', 'none', 'servable', ?)`,
      snapshotId,
      engineId,
      `sessions/${session.sessionId}/snapshots/${snapshotId}`,
      "0".repeat(64),
      Date.now(),
    );
    state.storage.sql.exec(
      "UPDATE session_state SET latest_snapshot_id = ? WHERE singleton = 1",
      snapshotId,
    );
  });
}

async function openRecoveryBrowser(
  session: CreatedSession,
  host: ReadyHost,
  role: "observer" | "writer" = "observer",
  clientId?: string,
): Promise<RecoveryBrowser> {
  const connection = await createConnectionSet(
    session.sessionId,
    role === "writer" ? session.writerCapability : session.observerCapability,
    recoveryV3BrowserCapabilities,
    clientId,
  );
  expect(connection.recoveryStrategy).toBe("v3");
  if (connection.clientId === null) throw new Error("browser connection lacks client identity");
  const browserConnection = { ...connection, clientId: connection.clientId };
  const control = await upgrade(session.sessionId, "control", connection.controlTicket);
  const data = await upgrade(session.sessionId, "data", connection.dataTicket);
  const welcomeMessage = nextText(control);
  const prepareFrame = nextHostControlFrame(
    host.control,
    (frame) => frame.type === "recovery-prepare" && frame.connectionId === connection.connectionId,
  );
  control.send(
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
  const welcome = decodeServerControlFrame(await welcomeMessage, "v3");
  const prepare = await prepareFrame;
  if (welcome.type !== "welcome") throw new Error("expected Recovery v3 Welcome");
  if (prepare.type !== "recovery-prepare") throw new Error("expected recovery-prepare");
  expect(prepare).toMatchObject({
    connectionId: connection.connectionId,
    deliveryGeneration: connection.deliveryGeneration,
    streamId: connection.streamId,
    source: { kind: "warm" },
  });
  return {
    connection: browserConnection,
    control,
    data,
    prepare,
    sessionId: session.sessionId,
    welcome,
  };
}

async function installRecovery(
  host: ReadyHost,
  browser: RecoveryBrowser,
  cut: {
    committedThrough?: { eventSeq: string; nextPtyOffset: string; sessionEpoch: string };
    liveFloor?: { nextEventSeq: string; nextPtyOffset: string; sessionEpoch: string };
  } = {},
): Promise<{
  grant: RecoveryV3HostSourceGrant;
  ready: RecoveryV3HostStartReady;
  start: RecoveryStart;
}> {
  const startMessage = nextText(browser.control);
  const hostFrames: RecoveryHostControlFrame[] = [];
  const onHostMessage = (event: MessageEvent) => {
    if (typeof event.data !== "string") return;
    try {
      hostFrames.push(decodeRelayToHostControlFrame(event.data, "v3"));
    } catch {
      // The exact runtime gate below only consumes valid Recovery v3 Host frames.
    }
  };
  host.control.addEventListener("message", onHostMessage);
  host.data.send(
    encodeRecoveryStartFence({
      type: "recovery-start-fence",
      recoveryId: browser.prepare.recoveryId,
      connectionId: browser.prepare.connectionId,
      deliveryGeneration: browser.prepare.deliveryGeneration,
      streamId: browser.prepare.streamId,
      engineId,
      base: browser.prepare.base,
      source: browser.prepare.source,
      committedThrough: cut.committedThrough ?? {
        sessionEpoch: "7",
        eventSeq: "0",
        nextPtyOffset: "0",
      },
      liveFloor: cut.liveFloor ?? {
        sessionEpoch: "7",
        nextEventSeq: "1",
        nextPtyOffset: "0",
      },
    }),
  );
  const start = decodeServerControlFrame(await startMessage, "v3");
  if (start.type !== "recovery-start") throw new Error("expected recovery-start");
  await waitForCondition(async () => {
    const outboxCount = await runInDurableObject(
      sessionStub(browser.sessionId),
      (_instance, state) =>
        state.storage.sql
          .exec<{ value: number }>(
            "SELECT COUNT(*) AS value FROM recovery_control_outbox WHERE recovery_id = ?",
            browser.prepare.recoveryId,
          )
          .one().value,
    );
    const hasReady = hostFrames.some(
      (frame) =>
        frame.type === "recovery-start-ready" && frame.recoveryId === browser.prepare.recoveryId,
    );
    const hasGrant = hostFrames.some(
      (frame) =>
        frame.type === "recovery-source-grant" && frame.recoveryId === browser.prepare.recoveryId,
    );
    return outboxCount === 0 && hasReady && hasGrant;
  }, "Recovery v3 start-ready ACK and first grant drain");
  await scheduler.wait(0);
  host.control.removeEventListener("message", onHostMessage);
  const ready = hostFrames.find(
    (frame): frame is RecoveryV3HostStartReady =>
      frame.type === "recovery-start-ready" && frame.recoveryId === browser.prepare.recoveryId,
  );
  const grant = hostFrames.find(
    (frame): frame is RecoveryV3HostSourceGrant =>
      frame.type === "recovery-source-grant" && frame.recoveryId === browser.prepare.recoveryId,
  );
  if (ready === undefined) throw new Error("expected recovery-start-ready");
  if (grant === undefined) throw new Error("expected recovery-source-grant");
  if (ready.type !== "recovery-start-ready") throw new Error("expected recovery-start-ready");
  if (grant.type !== "recovery-source-grant") throw new Error("expected recovery-source-grant");
  expect(ready.cumulativeGrantedEncodedBytes).toBe("0");
  expect(BigInt(grant.cumulativeGrantedEncodedBytes)).toBeGreaterThan(0n);
  expect(hostFrames.indexOf(ready)).toBeLessThan(hostFrames.indexOf(grant));
  return { grant, ready, start };
}

async function completeRecoveryAdoptedBeforeClose(
  host: ReadyHost,
  browser: RecoveryBrowser,
): Promise<void> {
  await installRecovery(host, browser);
  const recoveryMessage = nextBinary(browser.data);
  host.data.send(recoveryDone(browser));
  const recovery = decodeDeliveryEnvelopeV3(await recoveryMessage);
  const sourceReceivedMessage = nextText(host.control);
  browser.control.send(
    JSON.stringify({
      type: "delivery-received",
      deliveryGeneration: browser.connection.deliveryGeneration,
      lane: "recovery",
      contiguousDeliveryOrdinal: "1",
      cumulativeEncodedBytes: recovery.cumulativeEncodedBytes.toString(),
    }),
  );
  expect(decodeRelayToHostControlFrame(await sourceReceivedMessage, "v3")).toMatchObject({
    type: "recovery-source-received",
    recoveryId: browser.prepare.recoveryId,
  });
  browser.control.send(
    JSON.stringify({
      type: "replica-applied",
      deliveryGeneration: browser.connection.deliveryGeneration,
      authorityCursor: { sessionEpoch: "7", eventSeq: "0", nextPtyOffset: "0" },
    }),
  );
  browser.control.send(
    JSON.stringify({
      type: "recovery-adopted",
      recoveryId: browser.prepare.recoveryId,
      deliveryGeneration: browser.connection.deliveryGeneration,
      replicaApplied: { sessionEpoch: "7", eventSeq: "0", nextPtyOffset: "0" },
    }),
  );
  await runInDurableObject(sessionStub(browser.sessionId), () => undefined);
  const sourceClosedMessage = nextText(browser.control);
  host.control.send(
    JSON.stringify({
      type: "recovery-source-closed",
      recoveryId: browser.prepare.recoveryId,
      connectionId: browser.prepare.connectionId,
      streamId: browser.prepare.streamId,
      deliveryGeneration: browser.prepare.deliveryGeneration,
      throughRecoveryOrdinal: "1",
      throughRecoveryCumulativeEncodedBytes: recovery.cumulativeEncodedBytes.toString(),
    }),
  );
  expect(decodeServerControlFrame(await sourceClosedMessage, "v3")).toMatchObject({
    type: "recovery-source-closed",
    recoveryId: browser.prepare.recoveryId,
  });
}

async function openSyncedV2Browser(session: CreatedSession): Promise<{
  connection: ConnectionSet & { clientId: string };
  control: WebSocket;
  data: WebSocket;
}> {
  const connection = await createConnectionSet(session.sessionId, session.observerCapability, []);
  if (connection.clientId === null) throw new Error("v2 browser lacks client identity");
  const control = await upgrade(session.sessionId, "control", connection.controlTicket);
  const data = await upgrade(session.sessionId, "data", connection.dataTicket);
  await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
    for (const server of state.getWebSockets(`client:${connection.clientId}`)) {
      const attachment = SocketAttachmentV2Schema.parse(server.deserializeAttachment());
      server.serializeAttachment(
        SocketAttachmentV2Schema.parse(
          attachment.channel === "control"
            ? { ...attachment, controlState: "active" }
            : {
                ...attachment,
                dataState: "synced",
                firstEventSeq: "0",
                ackedEventSeq: "0",
                sentEventSeq: "0",
                firstPtyOffset: "0",
                ackedPtyOffset: "0",
                sentPtyOffset: "0",
              },
        ),
      );
    }
  });
  return { connection: { ...connection, clientId: connection.clientId }, control, data };
}

function canonicalPty(eventSeq: bigint, ptyOffset: bigint, payload: Uint8Array): Uint8Array {
  return encodeDataFrame({
    kind: DataFrameKind.PtyOutput,
    flags: DataFrameFlag.None,
    sessionEpoch: 7n,
    deliveryGeneration: 0n,
    eventSeq,
    ptyOffset,
    streamId: 0,
    payload,
  });
}

function recoveryDone(browser: RecoveryBrowser): Uint8Array {
  const payload = encodeDataFrame({
    kind: DataFrameKind.ReplayCommit,
    flags: DataFrameFlag.None,
    sessionEpoch: 7n,
    deliveryGeneration: 0n,
    eventSeq: 0n,
    ptyOffset: 0n,
    streamId: 0,
    payload: new Uint8Array(),
  });
  return encodeDeliveryEnvelopeV3({
    lane: "recovery",
    deliveryGeneration: BigInt(browser.connection.deliveryGeneration),
    deliveryOrdinal: 1n,
    cumulativeEncodedBytes: BigInt(DELIVERY_ENVELOPE_V3_HEADER_BYTES + payload.byteLength),
    streamId: browser.connection.streamId,
    payload,
  });
}

function recoveryDelivery(
  browser: RecoveryBrowser,
  deliveryOrdinal: bigint,
  previousCumulativeEncodedBytes: bigint,
  payload: Uint8Array,
): { cumulativeEncodedBytes: bigint; encoded: Uint8Array } {
  const cumulativeEncodedBytes =
    previousCumulativeEncodedBytes + BigInt(DELIVERY_ENVELOPE_V3_HEADER_BYTES + payload.byteLength);
  return {
    cumulativeEncodedBytes,
    encoded: encodeDeliveryEnvelopeV3({
      lane: "recovery",
      deliveryGeneration: BigInt(browser.connection.deliveryGeneration),
      deliveryOrdinal,
      cumulativeEncodedBytes,
      streamId: browser.connection.streamId,
      payload,
    }),
  };
}

function runtimeRecoveryStore(state: DurableObjectState): RelayRecoveryStore {
  return new RelayRecoveryStore(state.storage.sql, new RelayStore(state.storage.sql), {
    maxAttempts: 16,
    maxAttemptLiveEncodedBytes: 512 * 1024,
    maxAttemptLiveRecords: 64,
    maxDeliveryEncodedBytes: 2 * 1024 * 1024,
    maxDeliveryRecords: 1_024,
    maxDeliveryEnvelopeEncodedBytes:
      DELIVERY_ENVELOPE_V3_HEADER_BYTES + DATA_HEADER_BYTES + 16 * 1024,
    maxOutboxEntries: 112,
    maxRecoveryGrantWindowEncodedBytes: 96 * 1024,
    maxSessionDeliveryEncodedBytes: 2 * 1024 * 1024,
    maxSessionDeliveryRecords: 1_024,
    maxSessionRecoveryEncodedBytes: 1_536 * 1024,
    maxSessionRecoveryRecords: 1_008,
    minDeliveryEnvelopeEncodedBytes: DELIVERY_ENVELOPE_V3_HEADER_BYTES + DATA_HEADER_BYTES,
    writerDeliveryReserveEncodedBytes:
      DELIVERY_ENVELOPE_V3_HEADER_BYTES + DATA_HEADER_BYTES + 16 * 1024,
    writerDeliveryReserveRecords: 1,
  });
}

async function recoveryDeliveryMemoryUsage(sessionId: string): Promise<{
  queuedRecords: number;
  refRecords: number;
  ring: { physicalBytes: number; physicalEntries: number; references: number };
}> {
  return runInDurableObject(sessionStub(sessionId), (instance) => {
    const ring = Reflect.get(instance, "recoveryDeliveryRing");
    const deliveryScheduler = Reflect.get(instance, "recoveryDeliveryScheduler");
    const refRecords = Reflect.get(instance, "recoveryDeliveryRefRecords");
    if (!(ring instanceof RelayV3DeliveryRing)) throw new Error("runtime delivery ring is missing");
    if (!(deliveryScheduler instanceof RelayV3DeliveryScheduler)) {
      throw new Error("runtime delivery scheduler is missing");
    }
    if (!(refRecords instanceof Map)) throw new Error("runtime delivery ref owner map is missing");
    return {
      queuedRecords: deliveryScheduler.queuedRecords,
      refRecords: refRecords.size,
      ring: ring.usage,
    };
  });
}

afterEach(async () => {
  for (const socket of sockets) {
    if (socket.readyState < WebSocket.CLOSING) socket.close(1000, "test cleanup");
  }
  await Promise.all(
    [...sessions].map((sessionId) =>
      runInDurableObject(sessionStub(sessionId), async (instance) => {
        const deliveryScheduler = Reflect.get(instance, "recoveryDeliveryScheduler");
        if (deliveryScheduler instanceof RelayV3DeliveryScheduler) {
          await deliveryScheduler.whenIdle();
        }
      }),
    ),
  );
  sockets.clear();
  sessions.clear();
});

describe("Recovery v3 Durable Object runtime", () => {
  it("commits H/fence/H+1, rehydrates exact outbox routing, and advances both lanes", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0001");
    const browser = await openRecoveryBrowser(session, host);

    const durableBeforeHibernate = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => ({
        attempt: state.storage.sql
          .exec(
            "SELECT state FROM recovery_attempt WHERE recovery_id = ?",
            browser.prepare.recoveryId,
          )
          .one(),
        attachments: state
          .getWebSockets(`client:${browser.connection.clientId}`)
          .map((socket) => socket.deserializeAttachment() as { recoveryLookupKey: string | null }),
        outboxCount: state.storage.sql
          .exec<{ value: number }>("SELECT COUNT(*) AS value FROM recovery_control_outbox")
          .one().value,
      }),
    );
    expect(durableBeforeHibernate).toMatchObject({
      attempt: { state: "preparing" },
      outboxCount: 0,
    });
    expect(
      durableBeforeHibernate.attachments.every(
        ({ recoveryLookupKey }) => recoveryLookupKey === browser.prepare.recoveryId,
      ),
    ).toBe(true);

    await evictDurableObject(sessionStub(session.sessionId), { webSockets: "hibernate" });
    const startMessage = nextText(browser.control);
    const installHostMessages = nextTexts(host.control, 2);
    const liveMessage = nextBinary(browser.data);
    host.data.send(
      encodeRecoveryStartFence({
        type: "recovery-start-fence",
        recoveryId: browser.prepare.recoveryId,
        connectionId: browser.prepare.connectionId,
        deliveryGeneration: browser.prepare.deliveryGeneration,
        streamId: browser.prepare.streamId,
        engineId,
        base: browser.prepare.base,
        source: browser.prepare.source,
        committedThrough: { sessionEpoch: "7", eventSeq: "0", nextPtyOffset: "0" },
        liveFloor: { sessionEpoch: "7", nextEventSeq: "1", nextPtyOffset: "0" },
      }),
    );
    const canonical = canonicalPty(1n, 0n, new Uint8Array([65, 66]));
    host.data.send(canonical);

    const start = decodeServerControlFrame(await startMessage, "v3");
    const [readyMessage, grantMessage] = await installHostMessages;
    if (readyMessage === undefined || grantMessage === undefined) {
      throw new Error("expected start-ready and first grant");
    }
    const ready = decodeRelayToHostControlFrame(readyMessage, "v3");
    const grant = decodeRelayToHostControlFrame(grantMessage, "v3");
    expect(start).toMatchObject({
      type: "recovery-start",
      recoveryId: browser.prepare.recoveryId,
      committedThrough: { eventSeq: "0", nextPtyOffset: "0" },
    });
    expect(ready).toMatchObject({
      type: "recovery-start-ready",
      cumulativeGrantedEncodedBytes: "0",
    });
    expect(grant).toMatchObject({
      type: "recovery-source-grant",
      cumulativeGrantedEncodedBytes: (
        DELIVERY_ENVELOPE_V3_HEADER_BYTES +
        DATA_HEADER_BYTES +
        16 * 1024
      ).toString(),
    });
    const live = decodeDeliveryEnvelopeV3(await liveMessage);
    expect(live).toMatchObject({
      lane: "live",
      deliveryGeneration: BigInt(browser.connection.deliveryGeneration),
      deliveryOrdinal: 1n,
      streamId: browser.connection.streamId,
    });
    expect(decodeDataFrame(live.payload)).toMatchObject({
      eventSeq: 1n,
      ptyOffset: 0n,
      payload: new Uint8Array([65, 66]),
    });

    browser.control.send(
      JSON.stringify({
        type: "delivery-received",
        deliveryGeneration: browser.connection.deliveryGeneration,
        lane: "live",
        contiguousDeliveryOrdinal: "1",
        cumulativeEncodedBytes: live.cumulativeEncodedBytes.toString(),
      }),
    );
    await runInDurableObject(sessionStub(session.sessionId), () => undefined);

    const recoveryMessage = nextBinary(browser.data);
    host.data.send(recoveryDone(browser));
    const recovery = decodeDeliveryEnvelopeV3(await recoveryMessage);
    expect(recovery.lane).toBe("recovery");
    const sourceReceivedMessage = nextText(host.control);
    browser.control.send(
      JSON.stringify({
        type: "delivery-received",
        deliveryGeneration: browser.connection.deliveryGeneration,
        lane: "recovery",
        contiguousDeliveryOrdinal: "1",
        cumulativeEncodedBytes: recovery.cumulativeEncodedBytes.toString(),
      }),
    );
    expect(decodeRelayToHostControlFrame(await sourceReceivedMessage, "v3")).toMatchObject({
      type: "recovery-source-received",
      recoveryId: browser.prepare.recoveryId,
      contiguousDeliveryOrdinal: "1",
    });

    const sourceClosedMessage = nextText(browser.control);
    host.control.send(
      JSON.stringify({
        type: "recovery-source-closed",
        recoveryId: browser.prepare.recoveryId,
        connectionId: browser.prepare.connectionId,
        streamId: browser.prepare.streamId,
        deliveryGeneration: browser.prepare.deliveryGeneration,
        throughRecoveryOrdinal: "1",
        throughRecoveryCumulativeEncodedBytes: recovery.cumulativeEncodedBytes.toString(),
      }),
    );
    expect(decodeServerControlFrame(await sourceClosedMessage, "v3")).toMatchObject({
      type: "recovery-source-closed",
      recoveryId: browser.prepare.recoveryId,
    });
    browser.control.send(
      JSON.stringify({
        type: "replica-applied",
        deliveryGeneration: browser.connection.deliveryGeneration,
        authorityCursor: { sessionEpoch: "7", eventSeq: "1", nextPtyOffset: "2" },
      }),
    );
    browser.control.send(
      JSON.stringify({
        type: "recovery-adopted",
        recoveryId: browser.prepare.recoveryId,
        deliveryGeneration: browser.connection.deliveryGeneration,
        replicaApplied: { sessionEpoch: "7", eventSeq: "1", nextPtyOffset: "2" },
      }),
    );
    await runInDurableObject(sessionStub(session.sessionId), () => undefined);

    const durable = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => ({
        attempt: state.storage.sql
          .exec<{ granted_cumulative_encoded_bytes: string; state: string }>(
            `SELECT state, granted_cumulative_encoded_bytes
             FROM recovery_attempt WHERE recovery_id = ?`,
            browser.prepare.recoveryId,
          )
          .one(),
        head: state.storage.sql
          .exec("SELECT head_event_seq, next_pty_offset FROM session_state WHERE singleton = 1")
          .one(),
        lanes: state.storage.sql
          .exec(
            `SELECT lane, sent_delivery_ordinal, received_delivery_ordinal,
                    sent_cumulative_encoded_bytes, received_cumulative_encoded_bytes
             FROM recovery_delivery_lane WHERE recovery_id = ? ORDER BY lane`,
            browser.prepare.recoveryId,
          )
          .toArray(),
        payloadColumns: state.storage.sql
          .exec<{ name: string }>("PRAGMA table_info(recovery_attempt)")
          .toArray()
          .map(({ name }) => name)
          .filter((name) => /(?:bytes|hash|payload)/u.test(name)),
      }),
    );
    expect(durable).toMatchObject({
      attempt: { state: "complete" },
      head: { head_event_seq: "1", next_pty_offset: "2" },
      lanes: [
        {
          lane: "live",
          sent_delivery_ordinal: "1",
          received_delivery_ordinal: "1",
        },
        {
          lane: "recovery",
          sent_delivery_ordinal: "1",
          received_delivery_ordinal: "1",
        },
      ],
      payloadColumns: [
        "granted_cumulative_encoded_bytes",
        "recovery_done_cumulative_encoded_bytes",
      ],
    });
    expect(BigInt(durable.attempt.granted_cumulative_encoded_bytes)).toBeGreaterThanOrEqual(
      recovery.cumulativeEncodedBytes,
    );
  });

  it("delivers three live records without a receipt and releases the prefix with one receipt", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0002");
    const browser = await openRecoveryBrowser(session, host);
    const legacy = await openSyncedV2Browser(session);
    await installRecovery(host, browser);

    const v3Messages = nextBinaries(browser.data, 3);
    const legacyMessages = nextBinaries(legacy.data, 3);
    host.data.send(canonicalPty(1n, 0n, new Uint8Array([1])));
    host.data.send(canonicalPty(2n, 1n, new Uint8Array([2])));
    host.data.send(canonicalPty(3n, 2n, new Uint8Array([3])));
    const envelopes = (await v3Messages).map((message) => decodeDeliveryEnvelopeV3(message));
    expect(envelopes.map(({ deliveryOrdinal }) => deliveryOrdinal)).toEqual([1n, 2n, 3n]);
    expect(envelopes.map(({ payload }) => decodeDataFrame(payload).eventSeq)).toEqual([1n, 2n, 3n]);
    expect((await legacyMessages).map((message) => decodeDataFrame(message).eventSeq)).toEqual([
      1n,
      2n,
      3n,
    ]);

    const beforeReceipt = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => ({
        records: state.storage.sql
          .exec<{ state: string }>(
            `SELECT state FROM recovery_delivery_record
             WHERE recovery_id = ? AND lane = 'live' ORDER BY delivery_ordinal`,
            browser.prepare.recoveryId,
          )
          .toArray(),
        lane: state.storage.sql
          .exec(
            `SELECT sent_delivery_ordinal, received_delivery_ordinal
             FROM recovery_delivery_lane WHERE recovery_id = ? AND lane = 'live'`,
            browser.prepare.recoveryId,
          )
          .one(),
      }),
    );
    expect(beforeReceipt).toEqual({
      lane: { received_delivery_ordinal: "0", sent_delivery_ordinal: "3" },
      records: [{ state: "sent" }, { state: "sent" }, { state: "sent" }],
    });

    browser.control.send(
      JSON.stringify({
        type: "delivery-received",
        deliveryGeneration: browser.connection.deliveryGeneration,
        lane: "live",
        contiguousDeliveryOrdinal: "3",
        cumulativeEncodedBytes: envelopes[2]!.cumulativeEncodedBytes.toString(),
      }),
    );
    const afterReceipt = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => ({
        lane: state.storage.sql
          .exec(
            `SELECT sent_delivery_ordinal, received_delivery_ordinal
             FROM recovery_delivery_lane WHERE recovery_id = ? AND lane = 'live'`,
            browser.prepare.recoveryId,
          )
          .one(),
        records: state.storage.sql
          .exec<{ value: number }>(
            `SELECT COUNT(*) AS value FROM recovery_delivery_record
             WHERE recovery_id = ? AND lane = 'live'`,
            browser.prepare.recoveryId,
          )
          .one().value,
      }),
    );
    expect(afterReceipt).toEqual({
      lane: { received_delivery_ordinal: "3", sent_delivery_ordinal: "3" },
      records: 0,
    });
    expect(host.control.readyState).toBe(WebSocket.OPEN);
    expect(browser.control.readyState).toBe(WebSocket.OPEN);
    expect(legacy.control.readyState).toBe(WebSocket.OPEN);
  });

  it("isolates one failing observer while writer, healthy observer, V2, and Host continue", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0017");
    const writer = await openRecoveryBrowser(session, host, "writer");
    await installRecovery(host, writer);
    const slowObserver = await openRecoveryBrowser(session, host);
    await installRecovery(host, slowObserver);
    const healthyObserver = await openRecoveryBrowser(session, host);
    await installRecovery(host, healthyObserver);
    const legacy = await openSyncedV2Browser(session);

    await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      const serverData = state
        .getWebSockets(`client:${slowObserver.connection.clientId}`)
        .find(
          (socket) =>
            SocketAttachmentV3Schema.parse(socket.deserializeAttachment()).channel === "data",
        );
      if (serverData === undefined) throw new Error("slow observer data socket is missing");
      Object.defineProperty(serverData, "send", {
        configurable: true,
        value: () => {
          throw new Error("injected slow observer send failure");
        },
      });
    });

    const writerAtOne = nextBinary(writer.data);
    const healthyAtOne = nextBinary(healthyObserver.data);
    const legacyAtOne = nextBinary(legacy.data);
    const slowControlClosed = nextClose(slowObserver.control);
    const slowDataClosed = nextClose(slowObserver.data);
    const slowReset = nextHostControlFrame(
      host.control,
      (frame) =>
        frame.type === "recovery-source-reset" &&
        frame.recoveryId === slowObserver.prepare.recoveryId,
    );
    host.data.send(canonicalPty(1n, 0n, new Uint8Array([1])));

    expect(decodeDeliveryEnvelopeV3(await writerAtOne)).toMatchObject({
      deliveryOrdinal: 1n,
      lane: "live",
    });
    expect(decodeDeliveryEnvelopeV3(await healthyAtOne)).toMatchObject({
      deliveryOrdinal: 1n,
      lane: "live",
    });
    expect(decodeDataFrame(await legacyAtOne).eventSeq).toBe(1n);
    await expect(slowControlClosed).resolves.toMatchObject({ code: 4400 });
    await expect(slowDataClosed).resolves.toMatchObject({ code: 4400 });
    await expect(slowReset).resolves.toMatchObject({
      reason: "ack-outcome-uncertain",
      recoveryId: slowObserver.prepare.recoveryId,
      type: "recovery-source-reset",
    });
    expect(await recoveryDeliveryMemoryUsage(session.sessionId)).toEqual({
      queuedRecords: 0,
      refRecords: 0,
      ring: { physicalBytes: 0, physicalEntries: 0, references: 0 },
    });

    const writerAtTwo = nextBinary(writer.data);
    const healthyAtTwo = nextBinary(healthyObserver.data);
    const legacyAtTwo = nextBinary(legacy.data);
    host.data.send(canonicalPty(2n, 1n, new Uint8Array([2])));
    expect(decodeDeliveryEnvelopeV3(await writerAtTwo).deliveryOrdinal).toBe(2n);
    expect(decodeDeliveryEnvelopeV3(await healthyAtTwo).deliveryOrdinal).toBe(2n);
    expect(decodeDataFrame(await legacyAtTwo).eventSeq).toBe(2n);
    const durableIsolation = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => ({
        attempts: state.storage.sql
          .exec<{ recovery_id: string; state: string }>(
            `SELECT recovery_id, state FROM recovery_attempt
             WHERE recovery_id IN (?, ?, ?) ORDER BY recovery_id`,
            writer.prepare.recoveryId,
            slowObserver.prepare.recoveryId,
            healthyObserver.prepare.recoveryId,
          )
          .toArray(),
        authority: state.storage.sql
          .exec<{ head_event_seq: string; next_pty_offset: string }>(
            "SELECT head_event_seq, next_pty_offset FROM session_state WHERE singleton = 1",
          )
          .one(),
      }),
    );
    expect(durableIsolation.authority).toEqual({ head_event_seq: "2", next_pty_offset: "2" });
    expect(
      durableIsolation.attempts.find(
        ({ recovery_id: recoveryId }) => recoveryId === slowObserver.prepare.recoveryId,
      )?.state,
    ).toBe("resetting");
    expect(
      durableIsolation.attempts
        .filter(({ recovery_id: recoveryId }) => recoveryId !== slowObserver.prepare.recoveryId)
        .map(({ state: attemptState }) => attemptState),
    ).toEqual(["assembling", "assembling"]);
    expect(host.control.readyState).toBe(WebSocket.OPEN);
    expect(writer.control.readyState).toBe(WebSocket.OPEN);
    expect(healthyObserver.control.readyState).toBe(WebSocket.OPEN);
    expect(legacy.control.readyState).toBe(WebSocket.OPEN);
  });

  it("yields once before every queued send and confirms only after that yield resolves", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0018");
    const browser = await openRecoveryBrowser(session, host);
    await installRecovery(host, browser);

    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const gate: DeliveryYieldGate = { calls: 0, releases: [] };
      Reflect.set(instance, "testRecoveryDeliveryYieldGate", gate);
      Reflect.set(
        instance,
        "yieldRecoveryDeliveryTurn",
        (delayMs: number) =>
          new Promise<void>((resolve) => {
            expect(delayMs).toBe(0);
            gate.calls += 1;
            gate.releases.push(resolve);
          }),
      );
    });

    let binaryMessages = 0;
    browser.data.addEventListener("message", (event) => {
      if (typeof event.data !== "string") binaryMessages += 1;
    });
    for (let ordinal = 1; ordinal <= 2; ordinal += 1) {
      const delivered = nextBinary(browser.data);
      host.data.send(canonicalPty(BigInt(ordinal), BigInt(ordinal - 1), new Uint8Array([ordinal])));
      await waitForCondition(
        () =>
          runInDurableObject(sessionStub(session.sessionId), (instance) => {
            const gate = Reflect.get(instance, "testRecoveryDeliveryYieldGate") as
              | DeliveryYieldGate
              | undefined;
            return gate?.calls === ordinal && gate.releases.length === 1;
          }),
        `delivery yield ${ordinal}`,
      );
      const beforeRelease = await runInDurableObject(
        sessionStub(session.sessionId),
        (_instance, state) =>
          state.storage.sql
            .exec<{ state: string }>(
              `SELECT state FROM recovery_delivery_record
               WHERE recovery_id = ? AND lane = 'live' AND delivery_ordinal = ?`,
              browser.prepare.recoveryId,
              ordinal.toString(),
            )
            .one().state,
      );
      expect(beforeRelease).toBe("queued");
      expect(binaryMessages).toBe(ordinal - 1);
      expect(await recoveryDeliveryMemoryUsage(session.sessionId)).toEqual({
        queuedRecords: 1,
        refRecords: 1,
        ring: {
          physicalBytes: DATA_HEADER_BYTES + 1,
          physicalEntries: 1,
          references: 1,
        },
      });

      await runInDurableObject(sessionStub(session.sessionId), (instance) => {
        const gate = Reflect.get(instance, "testRecoveryDeliveryYieldGate") as
          | DeliveryYieldGate
          | undefined;
        const release = gate?.releases.shift();
        if (release === undefined) throw new Error("delivery yield release is missing");
        release();
      });
      expect(decodeDeliveryEnvelopeV3(await delivered).deliveryOrdinal).toBe(BigInt(ordinal));
      await waitForCondition(
        async () => (await recoveryDeliveryMemoryUsage(session.sessionId)).queuedRecords === 0,
        `delivery confirmation ${ordinal}`,
      );
      expect(binaryMessages).toBe(ordinal);
    }
    const final = await runInDurableObject(sessionStub(session.sessionId), (instance, state) => ({
      calls: (Reflect.get(instance, "testRecoveryDeliveryYieldGate") as DeliveryYieldGate).calls,
      states: state.storage.sql
        .exec<{ state: string }>(
          `SELECT state FROM recovery_delivery_record
             WHERE recovery_id = ? AND lane = 'live' ORDER BY delivery_ordinal`,
          browser.prepare.recoveryId,
        )
        .toArray()
        .map(({ state: deliveryState }) => deliveryState),
    }));
    expect(final).toEqual({ calls: 2, states: ["sent", "sent"] });
  });

  it("keeps recovery and live delivery interleaved in bounded scheduler visits", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    for (let eventSeq = 1; eventSeq <= 5; eventSeq += 1) {
      host.data.send(
        canonicalPty(BigInt(eventSeq), BigInt(eventSeq - 1), new Uint8Array([eventSeq])),
      );
    }
    await waitForCondition(
      () =>
        runInDurableObject(
          sessionStub(session.sessionId),
          (_instance, state) =>
            state.storage.sql
              .exec<{ head_event_seq: string }>(
                "SELECT head_event_seq FROM session_state WHERE singleton = 1",
              )
              .one().head_event_seq === "5",
        ),
      "canonical head 5",
    );
    await publishSnapshot(session, "snapshot_recovery_runtime_0019");
    const browser = await openRecoveryBrowser(session, host);
    await installRecovery(host, browser, {
      committedThrough: { sessionEpoch: "7", eventSeq: "5", nextPtyOffset: "5" },
      liveFloor: { sessionEpoch: "7", nextEventSeq: "6", nextPtyOffset: "5" },
    });
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const gate: DeliveryYieldGate = { calls: 0, releases: [] };
      Reflect.set(instance, "testRecoveryDeliveryYieldGate", gate);
      Reflect.set(
        instance,
        "yieldRecoveryDeliveryTurn",
        (_delayMs: number) =>
          new Promise<void>((resolve) => {
            gate.calls += 1;
            gate.releases.push(resolve);
          }),
      );
    });

    const delivered = nextBinaries(browser.data, 12);
    let recoveryCumulative = 0n;
    for (let ordinal = 1; ordinal <= 5; ordinal += 1) {
      const delivery = recoveryDelivery(
        browser,
        BigInt(ordinal),
        recoveryCumulative,
        canonicalPty(BigInt(ordinal), BigInt(ordinal - 1), new Uint8Array([ordinal])),
      );
      recoveryCumulative = delivery.cumulativeEncodedBytes;
      host.data.send(delivery.encoded);
    }
    const recoveryCommit = recoveryDelivery(
      browser,
      6n,
      recoveryCumulative,
      encodeDataFrame({
        kind: DataFrameKind.ReplayCommit,
        flags: DataFrameFlag.None,
        sessionEpoch: 7n,
        deliveryGeneration: 0n,
        eventSeq: 5n,
        ptyOffset: 5n,
        streamId: 0,
        payload: new Uint8Array(),
      }),
    );
    host.data.send(recoveryCommit.encoded);
    for (let eventSeq = 6; eventSeq <= 11; eventSeq += 1) {
      host.data.send(
        canonicalPty(BigInt(eventSeq), BigInt(eventSeq - 1), new Uint8Array([eventSeq])),
      );
    }
    await waitForCondition(
      () =>
        runInDurableObject(
          sessionStub(session.sessionId),
          (_instance, state) =>
            state.storage.sql
              .exec<{ value: number }>(
                "SELECT COUNT(*) AS value FROM recovery_delivery_record WHERE recovery_id = ?",
                browser.prepare.recoveryId,
              )
              .one().value === 12,
        ),
      "twelve durable recovery/live obligations",
    );

    for (let turn = 1; turn <= 12; turn += 1) {
      await waitForCondition(
        () =>
          runInDurableObject(sessionStub(session.sessionId), (instance) => {
            const gate = Reflect.get(instance, "testRecoveryDeliveryYieldGate") as
              | DeliveryYieldGate
              | undefined;
            return gate?.calls === turn && gate.releases.length === 1;
          }),
        `interleaved delivery yield ${turn}`,
      );
      await runInDurableObject(sessionStub(session.sessionId), (instance) => {
        const gate = Reflect.get(instance, "testRecoveryDeliveryYieldGate") as
          | DeliveryYieldGate
          | undefined;
        const release = gate?.releases.shift();
        if (release === undefined) throw new Error("interleaved delivery release is missing");
        release();
      });
    }
    const lanes = (await delivered).map((message) => decodeDeliveryEnvelopeV3(message).lane);
    expect(lanes.filter((lane) => lane === "live")).toHaveLength(6);
    expect(lanes.filter((lane) => lane === "recovery")).toHaveLength(6);
    let longestRun = 0;
    let currentRun = 0;
    let previousLane: (typeof lanes)[number] | undefined;
    for (const lane of lanes) {
      currentRun = lane === previousLane ? currentRun + 1 : 1;
      previousLane = lane;
      longestRun = Math.max(longestRun, currentRun);
    }
    // The weighted round has two consecutive observer-live class slots and
    // each DRR visit is capped at four records, so one flow's exact class-run
    // bound is eight while every individual record still yields.
    expect(longestRun).toBeLessThanOrEqual(8);
    await waitForCondition(
      async () => (await recoveryDeliveryMemoryUsage(session.sessionId)).queuedRecords === 0,
      "interleaved scheduler drain",
    );
    expect(await recoveryDeliveryMemoryUsage(session.sessionId)).toEqual({
      queuedRecords: 0,
      refRecords: 0,
      ring: { physicalBytes: 0, physicalEntries: 0, references: 0 },
    });
  });

  it("reconciles full observers before activating a writer and preserves its live reserve", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0020");
    const observers: RecoveryBrowser[] = [];
    for (let index = 0; index < 5; index += 1) {
      const observer = await openRecoveryBrowser(session, host);
      await completeRecoveryAdoptedBeforeClose(host, observer);
      observers.push(observer);
    }

    const payloadBytes = 16 * 1024;
    const encodedBytes = DELIVERY_ENVELOPE_V3_HEADER_BYTES + DATA_HEADER_BYTES + payloadBytes;
    const seededUsage = await runInDurableObject(
      sessionStub(session.sessionId),
      (instance, state) => {
        const store = runtimeRecoveryStore(state);
        const counts = [31, 31, 31, 31, 3] as const;
        const now = Date.now();
        state.storage.transactionSync(() => {
          state.storage.sql.exec(
            `UPDATE session_state SET head_event_seq = '31', next_pty_offset = ?, updated_at = ?
             WHERE singleton = 1`,
            (31 * payloadBytes).toString(),
            now,
          );
          for (let browserIndex = 0; browserIndex < observers.length; browserIndex += 1) {
            const browser = observers[browserIndex]!;
            let cumulativeEncodedBytes = 0n;
            for (let ordinal = 1; ordinal <= counts[browserIndex]!; ordinal += 1) {
              cumulativeEncodedBytes += BigInt(encodedBytes);
              const enqueued = store.enqueueValidatedLaneDelivery(
                browser.prepare.recoveryId,
                encodeDeliveryEnvelopeV3({
                  lane: "live",
                  deliveryGeneration: BigInt(browser.connection.deliveryGeneration),
                  deliveryOrdinal: BigInt(ordinal),
                  cumulativeEncodedBytes,
                  streamId: browser.connection.streamId,
                  payload: canonicalPty(
                    BigInt(ordinal),
                    BigInt((ordinal - 1) * payloadBytes),
                    new Uint8Array(payloadBytes),
                  ),
                }),
                now,
              );
              if (!enqueued.ok) throw new Error(`observer seed rejected: ${enqueued.reason}`);
              const begun = store.beginLaneDeliverySend(enqueued.record, now);
              if (!begun.ok || !begun.changed) throw new Error("observer seed did not begin");
              const confirmed = store.confirmLaneDeliverySend(enqueued.record, now);
              if (!confirmed.ok || !confirmed.changed) {
                throw new Error("observer seed did not confirm");
              }
            }
          }
        });
        const runtimeStore = Reflect.get(instance, "recoveries");
        if (!(runtimeStore instanceof RelayRecoveryStore)) {
          throw new Error("runtime recovery store is missing");
        }
        return runtimeStore.sessionDeliveryUsage();
      },
    );
    expect(seededUsage).toMatchObject({
      encodedBytes: 127 * encodedBytes,
      observerEncodedBytes: 127 * encodedBytes,
      observerRecords: 127,
      records: 127,
      writerEncodedBytes: 0,
      writerRecords: 0,
    });
    expect(seededUsage.encodedBytes).toBeLessThanOrEqual(2 * 1024 * 1024);
    expect(seededUsage.observerEncodedBytes).toBeGreaterThan(2 * 1024 * 1024 - encodedBytes);

    let hostResetCount = 0;
    host.control.addEventListener("message", (event) => {
      if (typeof event.data !== "string") return;
      try {
        if (decodeRelayToHostControlFrame(event.data, "v3").type === "recovery-source-reset") {
          hostResetCount += 1;
        }
      } catch {
        // Ignore unrelated Host control variants.
      }
    });
    const writer = await openRecoveryBrowser(session, host, "writer");
    await waitForCondition(
      () => observers.filter(({ control }) => control.readyState !== WebSocket.OPEN).length === 1,
      "one observer capacity victim",
    );
    const activeObservers = observers.filter(
      ({ control, data }) =>
        control.readyState === WebSocket.OPEN && data.readyState === WebSocket.OPEN,
    );
    expect(activeObservers).toHaveLength(4);
    const reconciled = await runInDurableObject(
      sessionStub(session.sessionId),
      (instance, state) => {
        const store = Reflect.get(instance, "recoveries");
        if (!(store instanceof RelayRecoveryStore)) {
          throw new Error("runtime recovery store is missing");
        }
        return {
          observerSockets: observers.map(
            (observer) => state.getWebSockets(`client:${observer.connection.clientId}`).length,
          ),
          records: state.storage.sql
            .exec<{ value: number }>("SELECT COUNT(*) AS value FROM recovery_delivery_record")
            .one().value,
          usage: store.sessionDeliveryUsage(),
          writerState: state.storage.sql
            .exec<{ state: string }>(
              "SELECT state FROM recovery_attempt WHERE recovery_id = ?",
              writer.prepare.recoveryId,
            )
            .one().state,
        };
      },
    );
    expect(reconciled.observerSockets.filter((count) => count === 0)).toHaveLength(1);
    expect(reconciled.records).toBe(96);
    expect(reconciled.writerState).toBe("preparing");
    expect(reconciled.usage.observerEncodedBytes).toBeLessThanOrEqual(
      2 * 1024 * 1024 - encodedBytes,
    );
    expect(hostResetCount).toBe(0);

    await installRecovery(host, writer, {
      committedThrough: {
        sessionEpoch: "7",
        eventSeq: "31",
        nextPtyOffset: (31 * payloadBytes).toString(),
      },
      liveFloor: {
        sessionEpoch: "7",
        nextEventSeq: "32",
        nextPtyOffset: (31 * payloadBytes).toString(),
      },
    });
    const writerDelivery = nextBinary(writer.data);
    host.data.send(canonicalPty(32n, BigInt(31 * payloadBytes), new Uint8Array([32])));
    expect(decodeDeliveryEnvelopeV3(await writerDelivery)).toMatchObject({
      deliveryOrdinal: 1n,
      lane: "live",
    });
    expect(writer.control.readyState).toBe(WebSocket.OPEN);
    expect(host.control.readyState).toBe(WebSocket.OPEN);
    expect(hostResetCount).toBe(0);
  }, 20_000);

  it("fences a Host that tries to inject a live envelope", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0003");
    const browser = await openRecoveryBrowser(session, host);
    await installRecovery(host, browser);

    const hostControlClosed = nextClose(host.control);
    const hostDataClosed = nextClose(host.data);
    const browserControlClosed = nextClose(browser.control);
    const browserDataClosed = nextClose(browser.data);
    const canonical = canonicalPty(1n, 0n, new Uint8Array([1]));
    host.data.send(
      encodeDeliveryEnvelopeV3({
        lane: "live",
        deliveryGeneration: BigInt(browser.connection.deliveryGeneration),
        deliveryOrdinal: 1n,
        cumulativeEncodedBytes: BigInt(DELIVERY_ENVELOPE_V3_HEADER_BYTES + canonical.byteLength),
        streamId: browser.connection.streamId,
        payload: canonical,
      }),
    );
    await expect(hostControlClosed).resolves.toMatchObject({ code: 4400 });
    await expect(hostDataClosed).resolves.toMatchObject({ code: 4400 });
    await expect(browserControlClosed).resolves.toMatchObject({ code: 4400 });
    await expect(browserDataClosed).resolves.toMatchObject({ code: 4400 });

    const durable = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => ({
        activeAttempts: state.storage.sql
          .exec<{ value: number }>(
            `SELECT COUNT(*) AS value FROM recovery_attempt
             WHERE state IN ('preparing', 'installed', 'assembling', 'complete')`,
          )
          .one().value,
        hostFence: state.storage.sql
          .exec<{ host_fence: string }>("SELECT host_fence FROM session_state WHERE singleton = 1")
          .one().host_fence,
      }),
    );
    expect(durable).toEqual({ activeAttempts: 0, hostFence: "2" });
  });

  it("scopes client activation fences but closes pre-Attach v3 pairs on a Host fence", async () => {
    const session = await createSession();
    const firstHost = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0005");
    const paired = await createConnectionSet(
      session.sessionId,
      session.observerCapability,
      recoveryV3BrowserCapabilities,
    );
    if (paired.clientId === null) throw new Error("paired browser lacks client identity");
    const pairedControl = await upgrade(session.sessionId, "control", paired.controlTicket);
    const pairedData = await upgrade(session.sessionId, "data", paired.dataTicket);
    const pairedControlClosed = nextClose(pairedControl);
    const pairedDataClosed = nextClose(pairedData);

    const unrelated = await openRecoveryBrowser(session, firstHost);
    await installRecovery(firstHost, unrelated);
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const gate: DeliveryYieldGate = { calls: 0, releases: [] };
      Reflect.set(instance, "testRecoveryDeliveryYieldGate", gate);
      Reflect.set(
        instance,
        "yieldRecoveryDeliveryTurn",
        (_delayMs: number) =>
          new Promise<void>((resolve) => {
            gate.calls += 1;
            gate.releases.push(resolve);
          }),
      );
    });
    firstHost.data.send(canonicalPty(1n, 0n, new Uint8Array([1])));
    await waitForCondition(
      async () => (await recoveryDeliveryMemoryUsage(session.sessionId)).queuedRecords === 1,
      "queued delivery before Host fence",
    );
    expect(pairedControl.readyState).toBe(WebSocket.OPEN);
    expect(pairedData.readyState).toBe(WebSocket.OPEN);
    const pairCount = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => state.getWebSockets(`client:${paired.clientId}`).length,
    );
    expect(pairCount).toBe(2);

    const unrelatedClosed = nextClose(unrelated.control);
    const unrelatedDataClosed = nextClose(unrelated.data);
    const secondHost = await openReadyHost(session, { headEventSeq: "1", nextPtyOffset: "1" });
    await expect(pairedControlClosed).resolves.toMatchObject({ code: 4400 });
    await expect(pairedDataClosed).resolves.toMatchObject({ code: 4400 });
    await expect(unrelatedClosed).resolves.toMatchObject({ code: 4400 });
    await expect(unrelatedDataClosed).resolves.toMatchObject({ code: 4400 });
    expect(await recoveryDeliveryMemoryUsage(session.sessionId)).toEqual({
      queuedRecords: 0,
      refRecords: 0,
      ring: { physicalBytes: 0, physicalEntries: 0, references: 0 },
    });
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const gate = Reflect.get(instance, "testRecoveryDeliveryYieldGate") as
        | DeliveryYieldGate
        | undefined;
      gate?.releases.shift()?.();
    });
    await scheduler.wait(0);
    expect(secondHost.control.readyState).toBe(WebSocket.OPEN);
  });

  it("cancels an exact yielded payload when its Browser data pair closes", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0021");
    const browser = await openRecoveryBrowser(session, host);
    await installRecovery(host, browser);
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const gate: DeliveryYieldGate = { calls: 0, releases: [] };
      Reflect.set(instance, "testRecoveryDeliveryYieldGate", gate);
      Reflect.set(
        instance,
        "yieldRecoveryDeliveryTurn",
        (_delayMs: number) =>
          new Promise<void>((resolve) => {
            gate.calls += 1;
            gate.releases.push(resolve);
          }),
      );
    });
    host.data.send(canonicalPty(1n, 0n, new Uint8Array([1])));
    await waitForCondition(
      async () => (await recoveryDeliveryMemoryUsage(session.sessionId)).queuedRecords === 1,
      "yielded delivery before Browser close",
    );

    const controlClosed = nextClose(browser.control);
    const resetFrame = nextHostControlFrame(
      host.control,
      (frame) =>
        frame.type === "recovery-source-reset" && frame.recoveryId === browser.prepare.recoveryId,
    );
    browser.data.close(1000, "injected data disconnect");
    await expect(controlClosed).resolves.toMatchObject({ code: 4400 });
    await expect(resetFrame).resolves.toMatchObject({
      reason: "generation-reset",
      type: "recovery-source-reset",
    });
    await waitForCondition(
      async () => (await recoveryDeliveryMemoryUsage(session.sessionId)).queuedRecords === 0,
      "Browser-close delivery cancellation",
    );
    expect(await recoveryDeliveryMemoryUsage(session.sessionId)).toEqual({
      queuedRecords: 0,
      refRecords: 0,
      ring: { physicalBytes: 0, physicalEntries: 0, references: 0 },
    });
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const gate = Reflect.get(instance, "testRecoveryDeliveryYieldGate") as
        | DeliveryYieldGate
        | undefined;
      gate?.releases.shift()?.();
    });
    await scheduler.wait(0);
    const authority = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) =>
      state.storage.sql
        .exec<{ head_event_seq: string; next_pty_offset: string }>(
          "SELECT head_event_seq, next_pty_offset FROM session_state WHERE singleton = 1",
        )
        .one(),
    );
    expect(authority).toEqual({ head_event_seq: "1", next_pty_offset: "1" });
    expect(host.control.readyState).toBe(WebSocket.OPEN);
  });

  it("uses a v3 writer token for renew, semantic input, and Host input acknowledgement", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0004");
    const browser = await openRecoveryBrowser(session, host, "writer");
    if (browser.welcome.writerLease === undefined) throw new Error("writer Welcome lacks lease");

    const leaseStatusMessage = nextText(browser.control);
    browser.control.send(
      JSON.stringify({ type: "writer-lease-renew", writerLease: browser.welcome.writerLease }),
    );
    expect(decodeServerControlFrame(await leaseStatusMessage, "v3")).toMatchObject({
      type: "writer-lease-status",
      active: true,
    });

    const forwardedMessage = nextText(host.control);
    browser.control.send(
      JSON.stringify({
        type: "text",
        inputEpoch: "1",
        clientInputSeq: "1",
        writerLease: browser.welcome.writerLease,
        data: "x",
      }),
    );
    const forwarded = decodeRelayToHostControlFrame(await forwardedMessage, "v3");
    expect(forwarded).toMatchObject({
      type: "text",
      connectionId: browser.connection.connectionId,
      clientId: browser.connection.clientId,
      data: "x",
    });
    if (forwarded.type !== "text") throw new Error("expected forwarded text");
    const durableFence = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) =>
        state.storage.sql
          .exec<{ fence: string }>("SELECT fence FROM writer_lease WHERE singleton = 1")
          .one().fence,
    );
    expect(forwarded.writerFence).toBe(durableFence);

    const inputAckMessage = nextText(browser.control);
    host.control.send(
      JSON.stringify({
        type: "input-ack",
        connectionId: browser.connection.connectionId,
        inputEpoch: "1",
        clientInputSeq: "1",
        status: "written",
        authorityEventSeq: "0",
      }),
    );
    expect(decodeServerControlFrame(await inputAckMessage, "v3")).toMatchObject({
      type: "input-ack",
      inputEpoch: "1",
      clientInputSeq: "1",
      status: "written",
    });
  });

  it("rotates a same-client writer lease across both activation crash cuts", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0006");
    const connection = await createConnectionSet(
      session.sessionId,
      session.writerCapability,
      recoveryV3BrowserCapabilities,
    );
    if (connection.clientId === null) throw new Error("writer connection lacks client identity");
    const clientId = connection.clientId;
    const control = await upgrade(session.sessionId, "control", connection.controlTicket);
    await upgrade(session.sessionId, "data", connection.dataTicket);
    const recoveryId = "recovery_writer_crash_0001";
    const prepare = {
      type: "recovery-prepare",
      recoveryId,
      connectionId: connection.connectionId,
      streamId: connection.streamId,
      deliveryGeneration: connection.deliveryGeneration,
      engineId,
      base: { sessionEpoch: "7", eventSeq: "0", nextPtyOffset: "0" },
      source: { kind: "warm" },
    } satisfies RecoveryV3HostPrepare;
    const seeded = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      const now = Date.now();
      const store = runtimeRecoveryStore(state);
      let result: ReturnType<RelayRecoveryStore["beginPreparing"]> | undefined;
      state.storage.transactionSync(() => {
        state.storage.sql.exec(
          `INSERT INTO writer_lease
              (singleton, client_id, lease_digest, fence, expires_at)
             VALUES (1, ?, ?, '1', ?)`,
          clientId,
          "0".repeat(64),
          now + 30_000,
        );
        result = store.beginPreparing({
          clientId,
          hardDeadlineAt: now + 60_000,
          hostFence: "1",
          noProgressTimeoutMs: 15_000,
          now,
          prepare,
        });
      });
      return result;
    });
    expect(seeded).toEqual({ changed: true, ok: true });

    const firstWelcomeMessage = nextText(control);
    const prepareMessage = nextText(host.control);
    const attach = {
      type: "attach",
      engineId,
      deliveryGeneration: connection.deliveryGeneration,
      hasLiveReplica: true,
      lastSessionEpoch: "7",
      lastEventSeq: "0",
      nextPtyOffset: "0",
    } as const;
    control.send(JSON.stringify(attach));
    const firstWelcome = decodeServerControlFrame(await firstWelcomeMessage, "v3");
    expect(decodeRelayToHostControlFrame(await prepareMessage, "v3")).toMatchObject({
      type: "recovery-prepare",
      recoveryId,
    });
    if (firstWelcome.type !== "welcome" || firstWelcome.writerLease === undefined) {
      throw new Error("first retry did not mint a writer token");
    }
    const firstActivation = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => ({
        fence: state.storage.sql
          .exec<{ fence: string }>("SELECT fence FROM writer_lease WHERE singleton = 1")
          .one().fence,
        keys: state
          .getWebSockets(`client:${clientId}`)
          .map((socket) => SocketAttachmentV3Schema.parse(socket.deserializeAttachment()))
          .map(({ recoveryLookupKey }) => recoveryLookupKey),
      }),
    );
    expect(firstActivation).toEqual({ fence: "2", keys: [recoveryId, recoveryId] });

    // Model a crash after Welcome.send() returned but before either active key
    // write became the reconnect witness.
    await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      for (const socket of state.getWebSockets(`client:${clientId}`)) {
        const attachment = SocketAttachmentV3Schema.parse(socket.deserializeAttachment());
        socket.serializeAttachment(
          SocketAttachmentV3Schema.parse({ ...attachment, recoveryLookupKey: null }),
        );
      }
    });
    const secondWelcomeMessage = nextText(control);
    control.send(JSON.stringify(attach));
    const secondWelcome = decodeServerControlFrame(await secondWelcomeMessage, "v3");
    if (secondWelcome.type !== "welcome" || secondWelcome.writerLease === undefined) {
      throw new Error("second retry did not mint a writer token");
    }
    expect(secondWelcome.writerLease).not.toBe(firstWelcome.writerLease);
    const secondActivation = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => ({
        fence: state.storage.sql
          .exec<{ fence: string }>("SELECT fence FROM writer_lease WHERE singleton = 1")
          .one().fence,
        keys: state
          .getWebSockets(`client:${clientId}`)
          .map((socket) => SocketAttachmentV3Schema.parse(socket.deserializeAttachment()))
          .map(({ recoveryLookupKey }) => recoveryLookupKey),
      }),
    );
    expect(secondActivation).toEqual({ fence: "3", keys: [recoveryId, recoveryId] });

    const staleStatusMessage = nextText(control);
    control.send(
      JSON.stringify({ type: "writer-lease-renew", writerLease: firstWelcome.writerLease }),
    );
    expect(decodeServerControlFrame(await staleStatusMessage, "v3")).toMatchObject({
      type: "writer-lease-status",
      active: false,
    });
    const currentStatusMessage = nextText(control);
    control.send(
      JSON.stringify({ type: "writer-lease-renew", writerLease: secondWelcome.writerLease }),
    );
    expect(decodeServerControlFrame(await currentStatusMessage, "v3")).toMatchObject({
      type: "writer-lease-status",
      active: true,
    });
  });

  it("drains and ACKs outbox rows only for exact Host, Browser, and payload identities", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0007");
    const browser = await openRecoveryBrowser(session, host);
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      // Hold the automatic continuation so this test can exercise the exact
      // destination and CAS seams through the production drainer.
      Reflect.set(instance, "recoveryOutboxDrainScheduled", true);
    });
    host.data.send(
      encodeRecoveryStartFence({
        type: "recovery-start-fence",
        recoveryId: browser.prepare.recoveryId,
        connectionId: browser.prepare.connectionId,
        deliveryGeneration: browser.prepare.deliveryGeneration,
        streamId: browser.prepare.streamId,
        engineId,
        base: browser.prepare.base,
        source: browser.prepare.source,
        committedThrough: { sessionEpoch: "7", eventSeq: "0", nextPtyOffset: "0" },
        liveFloor: { sessionEpoch: "7", nextEventSeq: "1", nextPtyOffset: "0" },
      }),
    );
    await runInDurableObject(sessionStub(session.sessionId), () => undefined);

    await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      for (const socket of state.getWebSockets(`client:${browser.connection.clientId}`)) {
        const attachment = SocketAttachmentV3Schema.parse(socket.deserializeAttachment());
        if (attachment.channel === "data") {
          socket.serializeAttachment(
            SocketAttachmentV3Schema.parse({ ...attachment, recoveryLookupKey: null }),
          );
        }
      }
      for (const socket of state.getWebSockets("peer:host")) {
        const attachment = SocketAttachmentV2Schema.parse(socket.deserializeAttachment());
        if (attachment.channel === "control") {
          socket.serializeAttachment(
            SocketAttachmentV2Schema.parse({ ...attachment, hostFence: "0" }),
          );
        }
      }
    });
    const blocked = await runInDurableObject(sessionStub(session.sessionId), (instance, state) => {
      const drain = Reflect.get(instance, "drainRecoveryOutboxBatch");
      if (typeof drain !== "function") throw new Error("runtime outbox drainer is missing");
      const progressed = Reflect.apply(drain, instance, []) as boolean;
      return {
        kinds: state.storage.sql
          .exec<{ kind: string }>("SELECT kind FROM recovery_control_outbox ORDER BY kind")
          .toArray()
          .map(({ kind }) => kind),
        progressed,
      };
    });
    expect(blocked).toEqual({
      kinds: ["recovery-start", "recovery-start-ready"],
      progressed: false,
    });

    await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      for (const socket of state.getWebSockets("peer:host")) {
        const attachment = SocketAttachmentV2Schema.parse(socket.deserializeAttachment());
        if (attachment.channel === "control") {
          socket.serializeAttachment(
            SocketAttachmentV2Schema.parse({ ...attachment, hostFence: "1" }),
          );
        }
      }
    });
    const readyMessage = nextText(host.control);
    const hostOnly = await runInDurableObject(sessionStub(session.sessionId), (instance, state) => {
      const drain = Reflect.get(instance, "drainRecoveryOutboxBatch");
      if (typeof drain !== "function") throw new Error("runtime outbox drainer is missing");
      const progressed = Reflect.apply(drain, instance, []) as boolean;
      return {
        kinds: state.storage.sql
          .exec<{ kind: string }>("SELECT kind FROM recovery_control_outbox ORDER BY kind")
          .toArray()
          .map(({ kind }) => kind),
        progressed,
      };
    });
    expect(decodeRelayToHostControlFrame(await readyMessage, "v3")).toMatchObject({
      type: "recovery-start-ready",
    });
    expect(hostOnly).toEqual({
      kinds: ["recovery-source-grant", "recovery-start"],
      progressed: true,
    });

    const cas = await runInDurableObject(sessionStub(session.sessionId), (instance, state) => {
      const entry = state.storage.sql
        .exec<{
          created_at: number;
          destination: "browser";
          kind: "recovery-start";
          payload_json: string;
          recovery_id: string;
          updated_at: number;
        }>(
          "SELECT * FROM recovery_control_outbox WHERE recovery_id = ? AND kind = 'recovery-start'",
          browser.prepare.recoveryId,
        )
        .one();
      state.storage.sql.exec(
        `UPDATE recovery_control_outbox SET payload_json = ?
           WHERE recovery_id = ? AND kind = 'recovery-start'`,
        `${entry.payload_json} `,
        entry.recovery_id,
      );
      const acknowledge = Reflect.get(instance, "acknowledgeRecoveryOutbox");
      if (typeof acknowledge !== "function") throw new Error("runtime outbox ACK is missing");
      return {
        acknowledged: Reflect.apply(acknowledge, instance, [entry]) as boolean,
        payload: state.storage.sql
          .exec<{ payload_json: string }>(
            "SELECT payload_json FROM recovery_control_outbox WHERE recovery_id = ? AND kind = 'recovery-start'",
            entry.recovery_id,
          )
          .one().payload_json,
      };
    });
    expect(cas).toEqual({ acknowledged: false, payload: expect.stringMatching(/\s$/u) });

    await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      for (const socket of state.getWebSockets(`client:${browser.connection.clientId}`)) {
        const attachment = SocketAttachmentV3Schema.parse(socket.deserializeAttachment());
        if (attachment.channel === "data") {
          socket.serializeAttachment(
            SocketAttachmentV3Schema.parse({
              ...attachment,
              recoveryLookupKey: browser.prepare.recoveryId,
            }),
          );
        }
      }
    });
    const startMessage = nextText(browser.control);
    const completed = await runInDurableObject(
      sessionStub(session.sessionId),
      (instance, state) => {
        const drain = Reflect.get(instance, "drainRecoveryOutboxBatch");
        if (typeof drain !== "function") throw new Error("runtime outbox drainer is missing");
        const progressed = Reflect.apply(drain, instance, []) as boolean;
        return {
          outboxCount: state.storage.sql
            .exec<{ value: number }>("SELECT COUNT(*) AS value FROM recovery_control_outbox")
            .one().value,
          progressed,
        };
      },
    );
    expect(decodeServerControlFrame(await startMessage, "v3")).toMatchObject({
      type: "recovery-start",
      recoveryId: browser.prepare.recoveryId,
    });
    expect(completed).toEqual({ outboxCount: 0, progressed: true });
  });

  it("scans past an unreachable outbox prefix without exceeding the drain batch", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0012");
    const browser = await openRecoveryBrowser(session, host);
    await runInDurableObject(sessionStub(session.sessionId), (instance, state) => {
      Reflect.set(instance, "recoveryOutboxDrainScheduled", true);
      const createdAt = Date.now();
      for (let index = 0; index < 15; index += 1) {
        const suffix = index.toString().padStart(4, "0");
        const recoveryId = `recovery_outbox_blocked_${suffix}`;
        const clientId = `client_outbox_blocked_${suffix}`;
        const connectionId = `connection_outbox_blocked_${suffix}`;
        const streamId = 1_000 + index;
        const deliveryGeneration = (1_000 + index).toString();
        const prepare = {
          type: "recovery-prepare",
          recoveryId,
          connectionId,
          streamId,
          deliveryGeneration,
          engineId,
          base: { sessionEpoch: "7", eventSeq: "0", nextPtyOffset: "0" },
          source: { kind: "warm" },
        } satisfies RecoveryV3HostPrepare;
        state.storage.sql.exec(
          `INSERT INTO recovery_attempt
            (recovery_id, client_id, connection_id, host_fence, stream_id,
             delivery_generation, engine_id, state, prepare_json, start_json,
             base_cursor_json, committed_through_json, live_floor_json,
             granted_cumulative_encoded_bytes, recovery_done_through_json,
             recovery_done_ordinal, recovery_done_cumulative_encoded_bytes,
             replica_applied_json, adopted_json, source_closed_json,
             hard_deadline_at, no_progress_timeout_ms, no_progress_deadline_at,
             reset_reason, created_at, updated_at)
           SELECT ?, ?, ?, '0', ?, ?, engine_id, 'preparing', ?, NULL,
                  base_cursor_json, NULL, NULL, '0', NULL, NULL, NULL,
                  replica_applied_json, NULL, NULL, hard_deadline_at,
                  no_progress_timeout_ms, no_progress_deadline_at, NULL, ?, ?
             FROM recovery_attempt WHERE recovery_id = ?`,
          recoveryId,
          clientId,
          connectionId,
          streamId,
          deliveryGeneration,
          JSON.stringify(prepare),
          createdAt,
          createdAt,
          browser.prepare.recoveryId,
        );
        state.storage.sql.exec(
          `INSERT INTO recovery_control_outbox
            (recovery_id, kind, destination, payload_json, created_at, updated_at)
           VALUES (?, 'recovery-prepare', 'host', ?, ?, ?)`,
          recoveryId,
          JSON.stringify(prepare),
          createdAt,
          createdAt,
        );
        if (index === 0) {
          state.storage.sql.exec(
            `INSERT INTO recovery_control_outbox
              (recovery_id, kind, destination, payload_json, created_at, updated_at)
             VALUES (?, 'recovery-start-ready', 'host', ?, ?, ?)`,
            recoveryId,
            JSON.stringify({
              type: "recovery-start-ready",
              recoveryId,
              connectionId,
              streamId,
              deliveryGeneration,
              committedThrough: { sessionEpoch: "7", eventSeq: "0", nextPtyOffset: "0" },
              cumulativeGrantedEncodedBytes: "0",
            }),
            createdAt,
            createdAt,
          );
        }
      }
      state.storage.sql.exec(
        `INSERT INTO recovery_control_outbox
          (recovery_id, kind, destination, payload_json, created_at, updated_at)
         VALUES (?, 'recovery-prepare', 'host', ?, ?, ?)`,
        browser.prepare.recoveryId,
        JSON.stringify(browser.prepare),
        createdAt + 1,
        createdAt + 1,
      );
    });

    const prepareMessage = nextText(host.control);
    const drained = await runInDurableObject(sessionStub(session.sessionId), (instance, state) => {
      const drain = Reflect.get(instance, "drainRecoveryOutboxBatch");
      if (typeof drain !== "function") throw new Error("runtime outbox drainer is missing");
      const progressed = Reflect.apply(drain, instance, []) as boolean;
      return {
        outboxCount: state.storage.sql
          .exec<{ value: number }>("SELECT COUNT(*) AS value FROM recovery_control_outbox")
          .one().value,
        progressed,
        reachableRows: state.storage.sql
          .exec<{ value: number }>(
            `SELECT COUNT(*) AS value FROM recovery_control_outbox
               WHERE recovery_id = ? AND kind = 'recovery-prepare'`,
            browser.prepare.recoveryId,
          )
          .one().value,
      };
    });
    expect(drained).toEqual({ outboxCount: 16, progressed: true, reachableRows: 0 });
    expect(decodeRelayToHostControlFrame(await prepareMessage, "v3")).toMatchObject({
      type: "recovery-prepare",
      recoveryId: browser.prepare.recoveryId,
    });

    const capped = await runInDurableObject(sessionStub(session.sessionId), (instance, state) => {
      const createdAt = Date.now();
      state.storage.sql.exec(
        "UPDATE recovery_attempt SET host_fence = '1' WHERE client_id LIKE 'client_outbox_blocked_%'",
      );
      state.storage.sql.exec(
        `INSERT INTO recovery_control_outbox
            (recovery_id, kind, destination, payload_json, created_at, updated_at)
           VALUES (?, 'recovery-prepare', 'host', ?, ?, ?)`,
        browser.prepare.recoveryId,
        JSON.stringify(browser.prepare),
        createdAt + 1,
        createdAt + 1,
      );
      const drain = Reflect.get(instance, "drainRecoveryOutboxBatch");
      if (typeof drain !== "function") throw new Error("runtime outbox drainer is missing");
      const progressed = Reflect.apply(drain, instance, []) as boolean;
      return {
        outboxCount: state.storage.sql
          .exec<{ value: number }>("SELECT COUNT(*) AS value FROM recovery_control_outbox")
          .one().value,
        progressed,
        reachableRows: state.storage.sql
          .exec<{ value: number }>(
            `SELECT COUNT(*) AS value FROM recovery_control_outbox
               WHERE recovery_id = ? AND kind = 'recovery-prepare'`,
            browser.prepare.recoveryId,
          )
          .one().value,
      };
    });
    expect(capped).toEqual({ outboxCount: 1, progressed: true, reachableRows: 1 });
  });

  it("fences queued and sending delivery owners when hibernation loses their payloads", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0013");
    const queued = await openRecoveryBrowser(session, host);
    const sending = await openRecoveryBrowser(session, host);
    await installRecovery(host, queued);
    await installRecovery(host, sending);

    const seeded = await runInDurableObject(sessionStub(session.sessionId), (instance, state) => {
      const recoveries = Reflect.get(instance, "recoveries");
      if (!(recoveries instanceof RelayRecoveryStore)) {
        throw new Error("runtime recovery store is missing");
      }
      const states: string[] = [];
      state.storage.transactionSync(() => {
        for (const [browser, begin] of [
          [queued, false],
          [sending, true],
        ] as const) {
          const enqueued = recoveries.enqueueValidatedLaneDelivery(
            browser.prepare.recoveryId,
            recoveryDone(browser),
            Date.now(),
          );
          if (!enqueued.ok) throw new Error(`failed to seed delivery: ${enqueued.reason}`);
          if (begin) {
            const begun = recoveries.beginLaneDeliverySend(enqueued.record, Date.now());
            if (!begun.ok) throw new Error(`failed to begin delivery: ${begun.reason}`);
          }
          states.push(
            state.storage.sql
              .exec<{ state: string }>(
                "SELECT state FROM recovery_delivery_record WHERE recovery_id = ?",
                browser.prepare.recoveryId,
              )
              .one().state,
          );
        }
      });
      return states;
    });
    expect(seeded).toEqual(["queued", "sending"]);

    const queuedControlClosed = nextClose(queued.control);
    const queuedDataClosed = nextClose(queued.data);
    const sendingControlClosed = nextClose(sending.control);
    const sendingDataClosed = nextClose(sending.data);
    const resetMessages = nextTexts(host.control, 2);
    await evictDurableObject(sessionStub(session.sessionId), { webSockets: "hibernate" });
    await runInDurableObject(sessionStub(session.sessionId), () => undefined);

    await Promise.all([
      expect(queuedControlClosed).resolves.toMatchObject({ code: 4400 }),
      expect(queuedDataClosed).resolves.toMatchObject({ code: 4400 }),
      expect(sendingControlClosed).resolves.toMatchObject({ code: 4400 }),
      expect(sendingDataClosed).resolves.toMatchObject({ code: 4400 }),
    ]);
    const resets = (await resetMessages)
      .map((message) => decodeRelayToHostControlFrame(message, "v3"))
      .map((frame) => {
        if (frame.type !== "recovery-source-reset") throw new Error("expected source reset");
        return { reason: frame.reason, recoveryId: frame.recoveryId };
      })
      .sort((left, right) => left.recoveryId.localeCompare(right.recoveryId));
    expect(resets).toEqual(
      [queued.prepare.recoveryId, sending.prepare.recoveryId]
        .sort((left, right) => left.localeCompare(right))
        .map((recoveryId) => ({ reason: "ack-outcome-uncertain", recoveryId })),
    );
    const durable = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => ({
        records: state.storage.sql
          .exec<{ value: number }>("SELECT COUNT(*) AS value FROM recovery_delivery_record")
          .one().value,
        states: state.storage.sql
          .exec<{ reset_reason: string; state: string }>(
            `SELECT state, reset_reason FROM recovery_attempt
             WHERE recovery_id IN (?, ?) ORDER BY recovery_id`,
            queued.prepare.recoveryId,
            sending.prepare.recoveryId,
          )
          .toArray(),
      }),
    );
    expect(durable).toEqual({
      records: 0,
      states: [
        { reset_reason: "ack-outcome-uncertain", state: "resetting" },
        { reset_reason: "ack-outcome-uncertain", state: "resetting" },
      ],
    });
    expect(host.control.readyState).toBe(WebSocket.OPEN);
  });

  it("keeps a sent delivery across hibernation only for the exact Browser pair", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0014");
    const browser = await openRecoveryBrowser(session, host);
    await installRecovery(host, browser);

    let binaryMessages = 0;
    browser.data.addEventListener("message", (event) => {
      if (typeof event.data !== "string") binaryMessages += 1;
    });
    const recoveryMessage = nextBinary(browser.data);
    host.data.send(recoveryDone(browser));
    const delivered = decodeDeliveryEnvelopeV3(await recoveryMessage);
    expect(binaryMessages).toBe(1);
    const beforeWake = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) =>
        state.storage.sql
          .exec<{ state: string }>(
            "SELECT state FROM recovery_delivery_record WHERE recovery_id = ?",
            browser.prepare.recoveryId,
          )
          .one().state,
    );
    expect(beforeWake).toBe("sent");

    await evictDurableObject(sessionStub(session.sessionId), { webSockets: "hibernate" });
    await runInDurableObject(sessionStub(session.sessionId), () => undefined);
    expect(binaryMessages).toBe(1);
    expect(await recoveryDeliveryMemoryUsage(session.sessionId)).toEqual({
      queuedRecords: 0,
      refRecords: 0,
      ring: { physicalBytes: 0, physicalEntries: 0, references: 0 },
    });
    const sourceReceivedFrame = nextHostControlFrame(
      host.control,
      (frame) =>
        frame.type === "recovery-source-received" &&
        frame.recoveryId === browser.prepare.recoveryId,
    );
    browser.control.send(
      JSON.stringify({
        type: "delivery-received",
        deliveryGeneration: browser.connection.deliveryGeneration,
        lane: "recovery",
        contiguousDeliveryOrdinal: "1",
        cumulativeEncodedBytes: delivered.cumulativeEncodedBytes.toString(),
      }),
    );
    expect(await sourceReceivedFrame).toMatchObject({
      type: "recovery-source-received",
      recoveryId: browser.prepare.recoveryId,
    });
    const durable = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => ({
        records: state.storage.sql
          .exec<{ value: number }>(
            "SELECT COUNT(*) AS value FROM recovery_delivery_record WHERE recovery_id = ?",
            browser.prepare.recoveryId,
          )
          .one().value,
        received: state.storage.sql
          .exec<{ received_delivery_ordinal: string }>(
            `SELECT received_delivery_ordinal FROM recovery_delivery_lane
             WHERE recovery_id = ? AND lane = 'recovery'`,
            browser.prepare.recoveryId,
          )
          .one().received_delivery_ordinal,
      }),
    );
    expect(durable).toEqual({ received: "1", records: 0 });
    expect(binaryMessages).toBe(1);
    expect(browser.control.readyState).toBe(WebSocket.OPEN);
    expect(browser.data.readyState).toBe(WebSocket.OPEN);
  });

  it("fences a sent owner when its exact Browser pair is missing after hibernation", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0016");
    const browser = await openRecoveryBrowser(session, host);
    await installRecovery(host, browser);

    const recoveryMessage = nextBinary(browser.data);
    host.data.send(recoveryDone(browser));
    await recoveryMessage;
    await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      for (const socket of state.getWebSockets(`client:${browser.connection.clientId}`)) {
        const attachment = SocketAttachmentV3Schema.parse(socket.deserializeAttachment());
        if (attachment.channel === "data") {
          socket.serializeAttachment(
            SocketAttachmentV3Schema.parse({ ...attachment, recoveryLookupKey: null }),
          );
        }
      }
    });

    const controlClosed = nextClose(browser.control);
    const dataClosed = nextClose(browser.data);
    const resetMessage = nextText(host.control);
    await evictDurableObject(sessionStub(session.sessionId), { webSockets: "hibernate" });
    await runInDurableObject(sessionStub(session.sessionId), () => undefined);
    await expect(controlClosed).resolves.toMatchObject({ code: 4400 });
    await expect(dataClosed).resolves.toMatchObject({ code: 4400 });
    expect(decodeRelayToHostControlFrame(await resetMessage, "v3")).toMatchObject({
      type: "recovery-source-reset",
      recoveryId: browser.prepare.recoveryId,
      reason: "generation-reset",
    });
    const durable = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => ({
        attempt: state.storage.sql
          .exec<{ reset_reason: string; state: string }>(
            "SELECT state, reset_reason FROM recovery_attempt WHERE recovery_id = ?",
            browser.prepare.recoveryId,
          )
          .one(),
        records: state.storage.sql
          .exec<{ value: number }>(
            "SELECT COUNT(*) AS value FROM recovery_delivery_record WHERE recovery_id = ?",
            browser.prepare.recoveryId,
          )
          .one().value,
      }),
    );
    expect(durable).toEqual({
      attempt: { reset_reason: "generation-reset", state: "resetting" },
      records: 0,
    });
    expect(host.control.readyState).toBe(WebSocket.OPEN);
  });

  it("locally fences a complete owner with a missing cold Browser pair and sends no Host reset", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0015");
    const browser = await openRecoveryBrowser(session, host, "writer");
    await completeRecoveryAdoptedBeforeClose(host, browser);
    await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      for (const socket of state.getWebSockets(`client:${browser.connection.clientId}`)) {
        const attachment = SocketAttachmentV3Schema.parse(socket.deserializeAttachment());
        if (attachment.channel === "control") {
          socket.serializeAttachment(
            SocketAttachmentV3Schema.parse({ ...attachment, channel: "data" }),
          );
        }
      }
    });

    let hostResetCount = 0;
    host.control.addEventListener("message", (event) => {
      if (
        typeof event.data === "string" &&
        decodeRelayToHostControlFrame(event.data, "v3").type === "recovery-source-reset"
      ) {
        hostResetCount += 1;
      }
    });
    const controlClosed = nextClose(browser.control);
    const dataClosed = nextClose(browser.data);
    await evictDurableObject(sessionStub(session.sessionId), { webSockets: "hibernate" });
    const durable = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => ({
        attempt: state.storage.sql
          .exec<{ reset_reason: string; state: string }>(
            "SELECT state, reset_reason FROM recovery_attempt WHERE recovery_id = ?",
            browser.prepare.recoveryId,
          )
          .one(),
        leaseExpiresAt: state.storage.sql
          .exec<{ expires_at: number }>("SELECT expires_at FROM writer_lease WHERE singleton = 1")
          .one().expires_at,
        outbox: state.storage.sql
          .exec<{ value: number }>(
            "SELECT COUNT(*) AS value FROM recovery_control_outbox WHERE recovery_id = ?",
            browser.prepare.recoveryId,
          )
          .one().value,
      }),
    );
    await expect(controlClosed).resolves.toMatchObject({ code: 4400 });
    await expect(dataClosed).resolves.toMatchObject({ code: 4400 });
    expect(durable).toEqual({
      attempt: { reset_reason: "generation-reset", state: "resetting" },
      leaseExpiresAt: 0,
      outbox: 0,
    });
    expect(hostResetCount).toBe(0);
    expect(host.control.readyState).toBe(WebSocket.OPEN);
  });

  it("isolates only the affected generation on ordinal replay and uncertain data send", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0008");
    const replayed = await openRecoveryBrowser(session, host);
    const uncertain = await openRecoveryBrowser(session, host);
    const survivor = await openRecoveryBrowser(session, host);
    await installRecovery(host, replayed);
    await installRecovery(host, uncertain);
    await installRecovery(host, survivor);

    const firstRecoveryMessage = nextBinary(replayed.data);
    const replayedEnvelope = recoveryDone(replayed);
    host.data.send(replayedEnvelope);
    await expect(firstRecoveryMessage).resolves.toEqual(replayedEnvelope);
    const replayedControlClosed = nextClose(replayed.control);
    const replayedDataClosed = nextClose(replayed.data);
    const replayResetFrame = nextHostControlFrame(
      host.control,
      (frame) =>
        frame.type === "recovery-source-reset" && frame.recoveryId === replayed.prepare.recoveryId,
    );
    host.data.send(replayedEnvelope);
    await expect(replayedControlClosed).resolves.toMatchObject({ code: 4400 });
    await expect(replayedDataClosed).resolves.toMatchObject({ code: 4400 });
    expect(await replayResetFrame).toMatchObject({
      type: "recovery-source-reset",
      recoveryId: replayed.prepare.recoveryId,
      reason: "generation-reset",
    });

    await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      const serverData = state
        .getWebSockets(`client:${uncertain.connection.clientId}`)
        .find(
          (socket) =>
            SocketAttachmentV3Schema.parse(socket.deserializeAttachment()).channel === "data",
        );
      if (serverData === undefined) throw new Error("uncertain Browser data socket is missing");
      Object.defineProperty(serverData, "send", {
        configurable: true,
        value: () => {
          throw new Error("injected uncertain send");
        },
      });
    });
    const uncertainControlClosed = nextClose(uncertain.control);
    const uncertainDataClosed = nextClose(uncertain.data);
    const uncertainResetFrame = nextHostControlFrame(
      host.control,
      (frame) =>
        frame.type === "recovery-source-reset" && frame.recoveryId === uncertain.prepare.recoveryId,
    );
    host.data.send(recoveryDone(uncertain));
    await expect(uncertainControlClosed).resolves.toMatchObject({ code: 4400 });
    await expect(uncertainDataClosed).resolves.toMatchObject({ code: 4400 });
    expect(await uncertainResetFrame).toMatchObject({
      type: "recovery-source-reset",
      recoveryId: uncertain.prepare.recoveryId,
      reason: "ack-outcome-uncertain",
    });

    const survivorMessage = nextBinary(survivor.data);
    host.data.send(canonicalPty(1n, 0n, new Uint8Array([9])));
    expect(decodeDeliveryEnvelopeV3(await survivorMessage)).toMatchObject({
      lane: "live",
      deliveryOrdinal: 1n,
    });
    expect(host.control.readyState).toBe(WebSocket.OPEN);
    expect(survivor.control.readyState).toBe(WebSocket.OPEN);
  });

  it("fences a complete live owner on Host replacement and permits its next generation", async () => {
    const session = await createSession();
    const firstHost = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0009");
    const completed = await openRecoveryBrowser(session, firstHost, "writer");
    await completeRecoveryAdoptedBeforeClose(firstHost, completed);
    await runInDurableObject(sessionStub(session.sessionId), () => undefined);
    const beforeFence = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => ({
        attemptState: state.storage.sql
          .exec<{ state: string }>(
            "SELECT state FROM recovery_attempt WHERE recovery_id = ?",
            completed.prepare.recoveryId,
          )
          .one().state,
        leaseExpiresAt: state.storage.sql
          .exec<{ expires_at: number }>("SELECT expires_at FROM writer_lease WHERE singleton = 1")
          .one().expires_at,
      }),
    );
    expect(beforeFence.attemptState).toBe("complete");
    expect(beforeFence.leaseExpiresAt).toBeGreaterThan(Date.now());

    const controlClosed = nextClose(completed.control);
    const dataClosed = nextClose(completed.data);
    const secondHost = await openReadyHost(session);
    await expect(controlClosed).resolves.toMatchObject({ code: 4400 });
    await expect(dataClosed).resolves.toMatchObject({ code: 4400 });
    await runInDurableObject(sessionStub(session.sessionId), () => undefined);
    const fenced = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => ({
      attemptCount: state.storage.sql
        .exec<{ value: number }>(
          "SELECT COUNT(*) AS value FROM recovery_attempt WHERE recovery_id = ?",
          completed.prepare.recoveryId,
        )
        .one().value,
      leaseExpiresAt: state.storage.sql
        .exec<{ expires_at: number }>("SELECT expires_at FROM writer_lease WHERE singleton = 1")
        .one().expires_at,
    }));
    expect(fenced).toEqual({ attemptCount: 0, leaseExpiresAt: 0 });

    const next = await openRecoveryBrowser(
      session,
      secondHost,
      "writer",
      completed.connection.clientId,
    );
    expect(next.connection.deliveryGeneration).toBe(
      (BigInt(completed.connection.deliveryGeneration) + 1n).toString(),
    );
    expect(next.prepare.connectionId).toBe(next.connection.connectionId);
  }, 10_000);

  it("sends zero pending v3 outbox frames once the kill switch is closed", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0010");
    const connection = await createConnectionSet(
      session.sessionId,
      session.observerCapability,
      recoveryV3BrowserCapabilities,
    );
    if (connection.clientId === null) throw new Error("kill-switch Browser lacks client identity");
    const control = await upgrade(session.sessionId, "control", connection.controlTicket);
    const data = await upgrade(session.sessionId, "data", connection.dataTicket);
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      Reflect.set(instance, "recoveryOutboxDrainScheduled", true);
    });
    let hostRecoveryMessages = 0;
    host.control.addEventListener("message", () => {
      hostRecoveryMessages += 1;
    });
    const welcomeMessage = nextText(control);
    control.send(
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
    expect(decodeServerControlFrame(await welcomeMessage, "v3")).toMatchObject({
      type: "welcome",
    });
    const pending = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) =>
        state.storage.sql.exec<{ kind: string }>("SELECT kind FROM recovery_control_outbox").one()
          .kind,
    );
    expect(pending).toBe("recovery-prepare");

    const controlClosed = nextClose(control);
    const dataClosed = nextClose(data);
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      Reflect.set(instance, "recoveryV3Enabled", false);
      Reflect.set(instance, "recoveryOutboxDrainScheduled", false);
      const schedule = Reflect.get(instance, "scheduleRecoveryOutboxDrain");
      const fence = Reflect.get(instance, "fenceDisabledRecoveryV3");
      if (typeof schedule !== "function" || typeof fence !== "function") {
        throw new Error("kill-switch recovery gates are missing");
      }
      Reflect.apply(schedule, instance, []);
      Reflect.apply(fence, instance, []);
    });
    await expect(controlClosed).resolves.toMatchObject({ code: 4400 });
    await expect(dataClosed).resolves.toMatchObject({ code: 4400 });
    await runInDurableObject(sessionStub(session.sessionId), () => undefined);
    expect(hostRecoveryMessages).toBe(0);
    const durable = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => ({
        kind: state.storage.sql
          .exec<{ kind: string }>("SELECT kind FROM recovery_control_outbox")
          .one().kind,
        state: state.storage.sql.exec<{ state: string }>("SELECT state FROM recovery_attempt").one()
          .state,
      }),
    );
    expect(durable).toEqual({ kind: "recovery-source-reset", state: "resetting" });
  });

  it("replaces an undrained prepare with an exact reset when the Browser disappears", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "snapshot_recovery_runtime_0011");
    const connection = await createConnectionSet(
      session.sessionId,
      session.observerCapability,
      recoveryV3BrowserCapabilities,
    );
    if (connection.clientId === null) throw new Error("reset Browser lacks client identity");
    const control = await upgrade(session.sessionId, "control", connection.controlTicket);
    const data = await upgrade(session.sessionId, "data", connection.dataTicket);
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      Reflect.set(instance, "recoveryOutboxDrainScheduled", true);
    });
    const welcomeMessage = nextText(control);
    control.send(
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
    expect(decodeServerControlFrame(await welcomeMessage, "v3")).toMatchObject({ type: "welcome" });
    const prepare = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) =>
        JSON.parse(
          state.storage.sql
            .exec<{ payload_json: string }>(
              "SELECT payload_json FROM recovery_control_outbox WHERE kind = 'recovery-prepare'",
            )
            .one().payload_json,
        ) as RecoveryV3HostPrepare,
    );
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      Reflect.set(instance, "recoveryOutboxDrainScheduled", false);
    });
    const resetMessage = nextText(host.control);
    data.close(1000, "simulate Browser disappearance");
    expect(decodeRelayToHostControlFrame(await resetMessage, "v3")).toMatchObject({
      type: "recovery-source-reset",
      recoveryId: prepare.recoveryId,
      connectionId: connection.connectionId,
      streamId: connection.streamId,
      deliveryGeneration: connection.deliveryGeneration,
      reason: "generation-reset",
    });
    const durable = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => ({
        prepareCount: state.storage.sql
          .exec<{ value: number }>(
            "SELECT COUNT(*) AS value FROM recovery_control_outbox WHERE kind = 'recovery-prepare'",
          )
          .one().value,
        state: state.storage.sql
          .exec<{ state: string }>("SELECT state FROM recovery_attempt")
          .toArray()[0]?.state,
      }),
    );
    expect(durable).toEqual({ prepareCount: 0, state: "resetting" });
  });
});
