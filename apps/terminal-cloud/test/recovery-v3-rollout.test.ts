import {
  DELIVERY_ENVELOPE_V3_HEADER_BYTES,
  DataFrameFlag,
  DataFrameKind,
  RELAY_CAPABILITIES_HEADER,
  RelayCapability,
  decodeDeliveryEnvelopeV3,
  decodeRelayToHostControlFrame,
  decodeServerControlFrame,
  encodeDataFrame,
  encodeDeliveryEnvelopeV3,
  encodeRecoveryStartFence,
  type RecoveryV3HostPrepare,
} from "@zhongduan/protocol";
import { env, exports as workerExports } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it } from "vitest";
import productionWranglerConfiguration from "../wrangler.jsonc?raw";
import { RelayV3DeliveryScheduler } from "../src/worker/relay-v3-delivery-scheduler";

interface CreatedSession {
  hostCapability: string;
  observerCapability: string;
  sessionId: string;
}

interface ConnectionSet {
  clientId: string | null;
  connectionId: string;
  connectionSetId: string;
  controlTicket: string;
  dataTicket: string;
  deliveryGeneration: string;
  negotiatedCapabilities?: string[];
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
}

type HostControlFrame = ReturnType<typeof decodeRelayToHostControlFrame<"v3">>;

const origin = "https://terminal.example.test";
const engineId = "ghostty:test+snapshot-v1+wterm:test";
const hostRecoveryV3Capabilities = [
  RelayCapability.capabilityNegotiationV1,
  RelayCapability.authorityDataV2,
  RelayCapability.wireEndpointV3,
  RelayCapability.recoveryV3GapFillV1,
] as const;
const browserRecoveryV3Capabilities = [
  RelayCapability.capabilityNegotiationV1,
  RelayCapability.authorityDataV2,
  RelayCapability.wireEndpointV3,
  RelayCapability.deliveryEnvelopeV3,
  RelayCapability.deliveryReceiveCreditV1,
  RelayCapability.authorityApplyProgressV1,
  RelayCapability.recoveryV3GapFillV1,
] as const;
const sockets = new Set<WebSocket>();
const sessionIds = new Set<string>();
let sessionOrdinal = 0;

function nextSessionId(): string {
  sessionOrdinal += 1;
  return `session_recovery_rollout_${sessionOrdinal.toString().padStart(16, "0")}`;
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
      body: JSON.stringify({ engineId, sessionEpoch: "7", sessionId }),
    }),
  );
  expect(response.status).toBe(201);
  sessionIds.add(sessionId);
  return response.json<CreatedSession>();
}

async function createConnectionSet(
  session: CreatedSession,
  capability: string,
  relayCapabilities: readonly string[],
  clientId?: string,
): Promise<ConnectionSet> {
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions/${session.sessionId}/connection-sets`, {
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

function nextClose(socket: WebSocket): Promise<CloseEvent> {
  return new Promise((resolve) => socket.addEventListener("close", resolve, { once: true }));
}

function nextHostControlFrame(
  socket: WebSocket,
  matches: (frame: HostControlFrame) => boolean,
): Promise<HostControlFrame> {
  return new Promise((resolve, reject) => {
    const onMessage = (event: MessageEvent) => {
      if (typeof event.data !== "string") return;
      let frame: HostControlFrame;
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
  for (let turn = 0; turn < 300; turn += 1) {
    if (await condition()) return;
    await scheduler.wait(0);
  }
  throw new Error(`timed out waiting for ${description}`);
}

async function openReadyHost(
  session: CreatedSession,
  relayCapabilities: readonly string[] = hostRecoveryV3Capabilities,
): Promise<ReadyHost> {
  const connection = await createConnectionSet(session, session.hostCapability, relayCapabilities);
  const control = await upgrade(session.sessionId, "control", connection.controlTicket);
  const data = await upgrade(session.sessionId, "data", connection.dataTicket);
  const acknowledgement = nextText(control);
  control.send(
    JSON.stringify({
      type: "host-ready",
      engineId,
      sessionEpoch: "7",
      headEventSeq: "0",
      nextPtyOffset: "0",
    }),
  );
  expect(
    decodeRelayToHostControlFrame(
      await acknowledgement,
      relayCapabilities.length === 0 ? "v2" : "v3",
    ),
  ).toMatchObject({ type: "host-ready-ack" });
  return { connection, control, data };
}

async function publishSnapshot(session: CreatedSession, suffix: string): Promise<void> {
  const snapshotId = `snapshot_recovery_rollout_${suffix.padStart(16, "0")}`;
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

async function openConnectionPair(
  session: CreatedSession,
  connection: ConnectionSet,
): Promise<{ control: WebSocket; data: WebSocket }> {
  const control = await upgrade(session.sessionId, "control", connection.controlTicket);
  const data = await upgrade(session.sessionId, "data", connection.dataTicket);
  return { control, data };
}

async function attachRecoveryBrowser(
  session: CreatedSession,
  host: ReadyHost,
  connection: ConnectionSet,
): Promise<RecoveryBrowser> {
  if (connection.clientId === null) throw new Error("Browser connection lacks client identity");
  const { control, data } = await openConnectionPair(session, connection);
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
  expect(decodeServerControlFrame(await welcomeMessage, "v3")).toMatchObject({
    type: "welcome",
    deliveryGeneration: connection.deliveryGeneration,
  });
  const prepare = await prepareFrame;
  if (prepare.type !== "recovery-prepare") throw new Error("expected recovery-prepare");
  return {
    connection: { ...connection, clientId: connection.clientId },
    control,
    data,
    prepare,
  };
}

async function installRecovery(host: ReadyHost, browser: RecoveryBrowser): Promise<void> {
  const startMessage = nextText(browser.control);
  const readyFrame = nextHostControlFrame(
    host.control,
    (frame) =>
      frame.type === "recovery-start-ready" && frame.recoveryId === browser.prepare.recoveryId,
  );
  const grantFrame = nextHostControlFrame(
    host.control,
    (frame) =>
      frame.type === "recovery-source-grant" && frame.recoveryId === browser.prepare.recoveryId,
  );
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
  expect(decodeServerControlFrame(await startMessage, "v3")).toMatchObject({
    type: "recovery-start",
    recoveryId: browser.prepare.recoveryId,
  });
  await expect(readyFrame).resolves.toMatchObject({
    type: "recovery-start-ready",
    cumulativeGrantedEncodedBytes: "0",
  });
  await expect(grantFrame).resolves.toMatchObject({ type: "recovery-source-grant" });
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

afterEach(async () => {
  for (const socket of sockets) {
    if (socket.readyState < WebSocket.CLOSING) socket.close(1000, "test cleanup");
  }
  await Promise.all(
    [...sessionIds].map((sessionId) =>
      runInDurableObject(sessionStub(sessionId), async (instance) => {
        const deliveryScheduler = Reflect.get(instance, "recoveryDeliveryScheduler");
        if (deliveryScheduler instanceof RelayV3DeliveryScheduler) {
          await deliveryScheduler.whenIdle();
        }
      }),
    ),
  );
  sockets.clear();
  sessionIds.clear();
});

describe("Recovery v3 rollout and control fault boundaries", () => {
  it("keeps the production kill switch closed by default", () => {
    expect(productionWranglerConfiguration).toMatch(/"RECOVERY_V3_ENABLED"\s*:\s*"false"/u);
  });

  it("activates and rolls back only on a newly claimed Browser generation", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "1");

    const baseline = await createConnectionSet(session, session.observerCapability, []);
    expect(baseline.recoveryStrategy).toBeUndefined();
    const baselinePair = await openConnectionPair(session, baseline);

    if (baseline.clientId === null) throw new Error("baseline Browser lacks client identity");
    const baselineControlClosed = nextClose(baselinePair.control);
    const baselineDataClosed = nextClose(baselinePair.data);
    const enabledConnection = await createConnectionSet(
      session,
      session.observerCapability,
      browserRecoveryV3Capabilities,
      baseline.clientId,
    );
    expect(enabledConnection).toMatchObject({
      deliveryGeneration: (BigInt(baseline.deliveryGeneration) + 1n).toString(),
      recoveryStrategy: "v3",
    });
    const enabled = await attachRecoveryBrowser(session, host, enabledConnection);
    await Promise.all([
      expect(baselineControlClosed).resolves.toMatchObject({ code: 4001 }),
      expect(baselineDataClosed).resolves.toMatchObject({ code: 4001 }),
    ]);

    const enabledControlClosed = nextClose(enabled.control);
    const enabledDataClosed = nextClose(enabled.data);
    const replacementConnection = await createConnectionSet(
      session,
      session.observerCapability,
      browserRecoveryV3Capabilities,
      baseline.clientId,
    );
    expect(replacementConnection).toMatchObject({
      deliveryGeneration: (BigInt(enabledConnection.deliveryGeneration) + 1n).toString(),
      recoveryStrategy: "v3",
    });
    const replacement = await attachRecoveryBrowser(session, host, replacementConnection);
    await Promise.all([
      expect(enabledControlClosed).resolves.toMatchObject({ code: 4400 }),
      expect(enabledDataClosed).resolves.toMatchObject({ code: 4400 }),
    ]);
    const afterReplacement = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => ({
        active: state.storage.sql
          .exec<{ delivery_generation: string; recovery_strategy: string }>(
            "SELECT delivery_generation, recovery_strategy FROM client_delivery WHERE client_id = ?",
            baseline.clientId,
          )
          .one(),
        old: state.storage.sql
          .exec<{ reset_reason: string; state: string }>(
            "SELECT state, reset_reason FROM recovery_attempt WHERE recovery_id = ?",
            enabled.prepare.recoveryId,
          )
          .toArray(),
      }),
    );
    expect(afterReplacement.active).toEqual({
      delivery_generation: replacementConnection.deliveryGeneration,
      recovery_strategy: "v3",
    });
    expect(afterReplacement.old).toEqual([]);

    const replacementControlClosed = nextClose(replacement.control);
    const replacementDataClosed = nextClose(replacement.data);
    const rolledBackConnection = await createConnectionSet(
      session,
      session.observerCapability,
      [],
      baseline.clientId,
    );
    expect(rolledBackConnection).toMatchObject({
      deliveryGeneration: (BigInt(replacementConnection.deliveryGeneration) + 1n).toString(),
    });
    expect(rolledBackConnection.recoveryStrategy).toBeUndefined();
    const rolledBackPair = await openConnectionPair(session, rolledBackConnection);
    await Promise.all([
      expect(replacementControlClosed).resolves.toMatchObject({ code: 4400 }),
      expect(replacementDataClosed).resolves.toMatchObject({ code: 4400 }),
    ]);
    const durable = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) =>
      state.storage.sql
        .exec<{ delivery_generation: string; recovery_strategy: string }>(
          "SELECT delivery_generation, recovery_strategy FROM client_delivery WHERE client_id = ?",
          baseline.clientId,
        )
        .one(),
    );
    expect(durable).toEqual({
      delivery_generation: rolledBackConnection.deliveryGeneration,
      recovery_strategy: "v2",
    });
    expect(host.control.readyState).toBe(WebSocket.OPEN);
    expect(rolledBackPair.control.readyState).toBe(WebSocket.OPEN);
    expect(rolledBackPair.data.readyState).toBe(WebSocket.OPEN);
  }, 10_000);

  it("makes exact receipt, adoption, and source-closure retries idempotent", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "2");
    const connection = await createConnectionSet(
      session,
      session.observerCapability,
      browserRecoveryV3Capabilities,
    );
    const browser = await attachRecoveryBrowser(session, host, connection);
    await installRecovery(host, browser);

    let sourceReceivedCount = 0;
    let sourceClosedCount = 0;
    host.control.addEventListener("message", (event) => {
      if (typeof event.data !== "string") return;
      try {
        const frame = decodeRelayToHostControlFrame(event.data, "v3");
        if (
          frame.type === "recovery-source-received" &&
          frame.recoveryId === browser.prepare.recoveryId
        ) {
          sourceReceivedCount += 1;
        }
      } catch {
        // This counter intentionally ignores non-Recovery-v3 traffic.
      }
    });
    browser.control.addEventListener("message", (event) => {
      if (typeof event.data !== "string") return;
      try {
        const frame = decodeServerControlFrame(event.data, "v3");
        if (
          frame.type === "recovery-source-closed" &&
          frame.recoveryId === browser.prepare.recoveryId
        ) {
          sourceClosedCount += 1;
        }
      } catch {
        // This counter intentionally ignores non-Recovery-v3 traffic.
      }
    });

    const deliveredMessage = nextBinary(browser.data);
    host.data.send(recoveryDone(browser));
    const delivered = decodeDeliveryEnvelopeV3(await deliveredMessage);
    const received = JSON.stringify({
      type: "delivery-received",
      deliveryGeneration: browser.connection.deliveryGeneration,
      lane: "recovery",
      contiguousDeliveryOrdinal: "1",
      cumulativeEncodedBytes: delivered.cumulativeEncodedBytes.toString(),
    });
    browser.control.send(received);
    browser.control.send(received);

    const replicaApplied = JSON.stringify({
      type: "replica-applied",
      deliveryGeneration: browser.connection.deliveryGeneration,
      authorityCursor: { sessionEpoch: "7", eventSeq: "0", nextPtyOffset: "0" },
    });
    const adopted = JSON.stringify({
      type: "recovery-adopted",
      recoveryId: browser.prepare.recoveryId,
      deliveryGeneration: browser.connection.deliveryGeneration,
      replicaApplied: { sessionEpoch: "7", eventSeq: "0", nextPtyOffset: "0" },
    });
    browser.control.send(replicaApplied);
    browser.control.send(replicaApplied);
    browser.control.send(adopted);
    browser.control.send(adopted);

    const sourceClosed = JSON.stringify({
      type: "recovery-source-closed",
      recoveryId: browser.prepare.recoveryId,
      connectionId: browser.prepare.connectionId,
      streamId: browser.prepare.streamId,
      deliveryGeneration: browser.prepare.deliveryGeneration,
      throughRecoveryOrdinal: "1",
      throughRecoveryCumulativeEncodedBytes: delivered.cumulativeEncodedBytes.toString(),
    });
    host.control.send(sourceClosed);
    host.control.send(sourceClosed);

    await waitForCondition(async () => {
      const durable = await runInDurableObject(
        sessionStub(session.sessionId),
        (_instance, state) => ({
          outbox: state.storage.sql
            .exec<{ value: number }>(
              "SELECT COUNT(*) AS value FROM recovery_control_outbox WHERE recovery_id = ?",
              browser.prepare.recoveryId,
            )
            .one().value,
          state: state.storage.sql
            .exec<{ state: string }>(
              "SELECT state FROM recovery_attempt WHERE recovery_id = ?",
              browser.prepare.recoveryId,
            )
            .one().state,
        }),
      );
      return durable.state === "complete" && durable.outbox === 0;
    }, "idempotent control retries to complete");
    await scheduler.wait(0);

    expect(sourceReceivedCount).toBe(1);
    expect(sourceClosedCount).toBe(1);
    expect(host.control.readyState).toBe(WebSocket.OPEN);
    expect(browser.control.readyState).toBe(WebSocket.OPEN);
    expect(browser.data.readyState).toBe(WebSocket.OPEN);
  });

  it("fails only the reordered Browser generation before Done", async () => {
    const session = await createSession();
    const host = await openReadyHost(session);
    await publishSnapshot(session, "3");
    const badConnection = await createConnectionSet(
      session,
      session.observerCapability,
      browserRecoveryV3Capabilities,
    );
    const bad = await attachRecoveryBrowser(session, host, badConnection);
    await installRecovery(host, bad);
    const survivorConnection = await createConnectionSet(
      session,
      session.observerCapability,
      browserRecoveryV3Capabilities,
    );
    const survivor = await attachRecoveryBrowser(session, host, survivorConnection);
    await installRecovery(host, survivor);

    const badControlClosed = nextClose(bad.control);
    const badDataClosed = nextClose(bad.data);
    bad.control.send(
      JSON.stringify({
        type: "recovery-adopted",
        recoveryId: bad.prepare.recoveryId,
        deliveryGeneration: bad.connection.deliveryGeneration,
        replicaApplied: { sessionEpoch: "7", eventSeq: "0", nextPtyOffset: "0" },
      }),
    );
    await Promise.all([
      expect(badControlClosed).resolves.toMatchObject({ code: 4400 }),
      expect(badDataClosed).resolves.toMatchObject({ code: 4400 }),
    ]);

    const survivorDelivery = nextBinary(survivor.data);
    host.data.send(
      encodeDataFrame({
        kind: DataFrameKind.PtyOutput,
        flags: DataFrameFlag.None,
        sessionEpoch: 7n,
        deliveryGeneration: 0n,
        eventSeq: 1n,
        ptyOffset: 0n,
        streamId: 0,
        payload: new Uint8Array([65]),
      }),
    );
    expect(decodeDeliveryEnvelopeV3(await survivorDelivery)).toMatchObject({
      lane: "live",
      deliveryOrdinal: 1n,
    });
    const durable = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => ({
        bad: state.storage.sql
          .exec<{ reset_reason: string; state: string }>(
            "SELECT state, reset_reason FROM recovery_attempt WHERE recovery_id = ?",
            bad.prepare.recoveryId,
          )
          .one(),
        survivor: state.storage.sql
          .exec<{ state: string }>(
            "SELECT state FROM recovery_attempt WHERE recovery_id = ?",
            survivor.prepare.recoveryId,
          )
          .one(),
      }),
    );
    expect(durable).toEqual({
      bad: { reset_reason: "generation-reset", state: "resetting" },
      survivor: { state: "assembling" },
    });
    expect(host.control.readyState).toBe(WebSocket.OPEN);
    expect(survivor.control.readyState).toBe(WebSocket.OPEN);
  });
});
