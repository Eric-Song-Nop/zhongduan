import { env, exports as workerExports } from "cloudflare:workers";
import { evictDurableObject, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it } from "vitest";
import { snapshotAttemptObjectKey } from "../src/worker/snapshot-contract";
import { within } from "./snapshot-test-helpers";

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
  expiresAt: number;
  streamId: number;
}

interface UpgradeResult {
  response: Response;
  socket: WebSocket | undefined;
}

const origin = "https://terminal.example.test";
const engineId = "ghostty:test+snapshot-v1+wterm:test";
const sockets = new Set<WebSocket>();
const sessions = new Set<string>();
let sessionCounter = 0;

function nextSessionId(): string {
  sessionCounter += 1;
  return `session_runtime_${sessionCounter.toString().padStart(16, "0")}`;
}

function sessionStub(sessionId: string) {
  return env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${sessionId}`));
}

async function createSession(overrides: { sessionEpoch?: string } = {}): Promise<CreatedSession> {
  const sessionId = nextSessionId();
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions`, {
      method: "POST",
      headers: {
        authorization: "Bearer test-bootstrap-token-with-at-least-32-bytes",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        sessionId,
        engineId,
        sessionEpoch: overrides.sessionEpoch ?? "7",
      }),
    }),
  );
  expect(response.status).toBe(201);
  sessions.add(sessionId);
  return response.json<CreatedSession>();
}

function requestConnectionSet(
  sessionId: string,
  capability: string,
  clientId?: string,
): Promise<Response> {
  return workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions/${sessionId}/connection-sets`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${capability}`,
        "content-type": "application/json",
      },
      body: JSON.stringify(clientId === undefined ? {} : { clientId }),
    }),
  );
}

async function createConnectionSet(
  sessionId: string,
  capability: string,
  clientId?: string,
): Promise<ConnectionSet> {
  const response = await requestConnectionSet(sessionId, capability, clientId);
  expect(response.status).toBe(200);
  return response.json<ConnectionSet>();
}

async function upgrade(
  sessionId: string,
  channel: "control" | "data",
  ticket: string,
): Promise<UpgradeResult> {
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions/${sessionId}/ws/${channel}?ticket=${ticket}`, {
      headers: { upgrade: "websocket" },
    }),
  );
  const socket = response.webSocket ?? undefined;
  socket?.accept();
  if (socket !== undefined) {
    sockets.add(socket);
    socket.addEventListener("close", () => sockets.delete(socket), { once: true });
  }
  return { response, socket };
}

function nextMessage(socket: WebSocket): Promise<string> {
  return new Promise((resolve, reject) => {
    socket.addEventListener("message", (event) => resolve(String(event.data)), { once: true });
    socket.addEventListener("error", () => reject(new Error("websocket failed")), { once: true });
  });
}

function nextClose(socket: WebSocket): Promise<CloseEvent> {
  return new Promise((resolve) => socket.addEventListener("close", resolve, { once: true }));
}

async function drainSession(sessionId: string): Promise<void> {
  await runInDurableObject(sessionStub(sessionId), () => undefined);
}

async function publishRuntimeSnapshot(session: CreatedSession): Promise<void> {
  const snapshotId = "snapshot_runtime_000001";
  await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
    state.storage.sql.exec(
      `INSERT INTO snapshot
        (snapshot_id, session_epoch, cut_event_seq, next_pty_offset, engine_id,
         object_key, r2_version, etag, sha256, compressed_length,
         uncompressed_length, compression, state, created_at)
       VALUES (?, '7', '0', '0', ?, ?, 'runtime-version', 'runtime-etag', ?, 1, '1',
               'none', 'servable', ?)`,
      snapshotId,
      engineId,
      snapshotAttemptObjectKey(session.sessionId, snapshotId, "runtime_attempt_0001"),
      "0".repeat(64),
      Date.now(),
    );
    state.storage.sql.exec(
      "UPDATE session_state SET latest_snapshot_id = ? WHERE singleton = 1",
      snapshotId,
    );
  });
}

async function openReadyHost(
  session: CreatedSession,
): Promise<{ control: WebSocket; data: WebSocket }> {
  const connection = await createConnectionSet(session.sessionId, session.hostCapability);
  const control = await upgrade(session.sessionId, "control", connection.controlTicket);
  const data = await upgrade(session.sessionId, "data", connection.dataTicket);
  if (control.socket === undefined || data.socket === undefined) {
    throw new Error("host upgrade did not return both sockets");
  }
  const acknowledgement = nextMessage(control.socket);
  control.socket.send(
    JSON.stringify({
      type: "host-ready",
      engineId,
      sessionEpoch: "7",
      headEventSeq: "0",
      nextPtyOffset: "0",
    }),
  );
  await expect(acknowledgement).resolves.toContain('"type":"host-ready-ack"');
  return { control: control.socket, data: data.socket };
}

async function readyForBrowsers(
  session: CreatedSession,
): Promise<{ control: WebSocket; data: WebSocket }> {
  await publishRuntimeSnapshot(session);
  return openReadyHost(session);
}

afterEach(async () => {
  for (const socket of sockets) {
    if (socket.readyState < WebSocket.CLOSING) socket.close(1000, "test cleanup");
  }
  await Promise.all([...sessions].map((sessionId) => drainSession(sessionId)));
  sockets.clear();
  sessions.clear();
});

describe("cloud relay runtime", () => {
  it("authenticates session and connection-set creation", async () => {
    const unauthorized = await workerExports.default.fetch(
      new Request(`${origin}/api/v1/sessions`, {
        method: "POST",
        headers: {
          authorization: "Bearer wrong-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({ sessionId: nextSessionId(), engineId, sessionEpoch: "7" }),
      }),
    );
    expect(unauthorized.status).toBe(401);
    await unauthorized.body?.cancel();

    const session = await createSession();
    await readyForBrowsers(session);
    const writer = await createConnectionSet(session.sessionId, session.writerCapability);
    expect(writer.clientId).toMatch(/^[A-Za-z0-9_-]{16,128}$/u);
    expect(writer.streamId).toBeGreaterThan(0);
    expect(writer.deliveryGeneration).toBe("1");

    const wrongSession = await workerExports.default.fetch(
      new Request(`${origin}/api/v1/sessions/AAAAAAAAAAAAAAAA/connection-sets`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${session.writerCapability}`,
          "content-type": "application/json",
        },
        body: "{}",
      }),
    );
    expect(wrongSession.status).toBe(401);
    await wrongSession.body?.cancel();
  });

  it("rejects zero and overflowing session epochs", async () => {
    for (const sessionEpoch of ["0", "18446744073709551616"]) {
      const response = await workerExports.default.fetch(
        new Request(`${origin}/api/v1/sessions`, {
          method: "POST",
          headers: {
            authorization: "Bearer test-bootstrap-token-with-at-least-32-bytes",
            "content-type": "application/json",
          },
          body: JSON.stringify({ sessionId: nextSessionId(), engineId, sessionEpoch }),
        }),
      );
      expect(response.status).toBe(400);
      await response.body?.cancel();
    }
  });

  it("requires an explicit fresh epoch for every created PTY lifecycle", async () => {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const response = await workerExports.default.fetch(
        new Request(`${origin}/api/v1/sessions`, {
          method: "POST",
          headers: {
            authorization: "Bearer test-bootstrap-token-with-at-least-32-bytes",
            "content-type": "application/json",
          },
          body: JSON.stringify({ sessionId: nextSessionId(), engineId }),
        }),
      );
      expect(response.status).toBe(400);
      await response.body?.cancel();
    }

    const created = await Promise.all(
      ["11", "12"].map((sessionEpoch) => createSession({ sessionEpoch })),
    );
    expect(created.map(({ sessionId }) => sessionId)).toEqual([
      expect.stringMatching(/^session_runtime_/u),
      expect.stringMatching(/^session_runtime_/u),
    ]);
    expect(created[0]!.sessionId).not.toBe(created[1]!.sessionId);
  });

  it("retries a lost create response against one stable session identity", async () => {
    const sessionId = nextSessionId();
    sessions.add(sessionId);
    const identity = { sessionId, engineId, sessionEpoch: "21" };
    const create = (body: object) =>
      workerExports.default.fetch(
        new Request(`${origin}/api/v1/sessions`, {
          method: "POST",
          headers: {
            authorization: "Bearer test-bootstrap-token-with-at-least-32-bytes",
            "content-type": "application/json",
          },
          body: JSON.stringify(body),
        }),
      );

    const lost = await create(identity);
    expect(lost.status).toBe(201);
    const firstIssue = await lost.clone().json<{ hostCapability: string }>();
    await lost.body?.cancel();
    const retried = await create(identity);
    expect(retried.status).toBe(200);
    const recreated = await retried.json<{ hostCapability: string; sessionId: string }>();
    expect(recreated.sessionId).toBe(sessionId);
    expect(recreated.hostCapability).not.toBe(firstIssue.hostCapability);

    const row = await runInDurableObject(sessionStub(sessionId), (_instance, state) =>
      state.storage.sql.exec("SELECT COUNT(*) AS count FROM session_state").one(),
    );
    expect(row).toEqual({ count: 1 });

    const engineConflict = await create({ ...identity, engineId: `${engineId}+other` });
    expect(engineConflict.status).toBe(409);
    await engineConflict.body?.cancel();
    const epochConflict = await create({ ...identity, sessionEpoch: "22" });
    expect(epochConflict.status).toBe(409);
    await epochConflict.body?.cancel();
  });

  it("requires both a published snapshot and a ready Host before Browser reservation", async () => {
    const missingHost = await createSession();
    await publishRuntimeSnapshot(missingHost);
    const withoutHost = await requestConnectionSet(
      missingHost.sessionId,
      missingHost.observerCapability,
    );
    expect(withoutHost.status).toBe(409);
    expect(await withoutHost.json()).toEqual({ error: "recovery-unavailable" });
    await openReadyHost(missingHost);
    const ready = await requestConnectionSet(missingHost.sessionId, missingHost.observerCapability);
    expect(ready.status).toBe(200);
    await ready.body?.cancel();

    const missingSnapshot = await createSession();
    await openReadyHost(missingSnapshot);
    const withoutSnapshot = await requestConnectionSet(
      missingSnapshot.sessionId,
      missingSnapshot.observerCapability,
    );
    expect(withoutSnapshot.status).toBe(409);
    expect(await withoutSnapshot.json()).toEqual({ error: "recovery-unavailable" });
  });

  it("consumes each channel ticket once and requires control first", async () => {
    const session = await createSession();
    await readyForBrowsers(session);
    const connection = await createConnectionSet(session.sessionId, session.writerCapability);

    const earlyData = await upgrade(session.sessionId, "data", connection.dataTicket);
    expect(earlyData.response.status).toBe(409);
    await earlyData.response.body?.cancel();

    const control = await upgrade(session.sessionId, "control", connection.controlTicket);
    expect(control.response.status).toBe(101);
    expect(control.socket).toBeDefined();

    const replayedControl = await upgrade(session.sessionId, "control", connection.controlTicket);
    expect(replayedControl.response.status).toBe(401);
    await replayedControl.response.body?.cancel();

    const data = await upgrade(session.sessionId, "data", connection.dataTicket);
    expect(data.response.status).toBe(101);
    expect(data.socket).toBeDefined();

    const replayedData = await upgrade(session.sessionId, "data", connection.dataTicket);
    expect(replayedData.response.status).toBe(401);
    await replayedData.response.body?.cancel();
  });

  it("does not pair data to a closed control socket", async () => {
    const session = await createSession();
    await readyForBrowsers(session);
    const connection = await createConnectionSet(session.sessionId, session.writerCapability);
    const control = await upgrade(session.sessionId, "control", connection.controlTicket);
    expect(control.response.status).toBe(101);
    expect(control.socket).toBeDefined();

    control.socket?.close(1000, "control ended");
    await drainSession(session.sessionId);
    const data = await upgrade(session.sessionId, "data", connection.dataTicket);
    expect(data.response.status).toBe(409);
    await data.response.body?.cancel();
  });

  it("binds a data ticket to the control socket from the same connection set", async () => {
    const session = await createSession();
    await readyForBrowsers(session);
    const first = await createConnectionSet(session.sessionId, session.writerCapability);
    const firstControl = await upgrade(session.sessionId, "control", first.controlTicket);
    expect(firstControl.response.status).toBe(101);

    const second = await createConnectionSet(
      session.sessionId,
      session.writerCapability,
      first.clientId ?? undefined,
    );
    expect(second.deliveryGeneration).toBe("2");
    const secondControl = await upgrade(session.sessionId, "control", second.controlTicket);
    expect(secondControl.response.status).toBe(101);

    const staleData = await upgrade(session.sessionId, "data", first.dataTicket);
    expect(staleData.response.status).toBe(401);
    await staleData.response.body?.cancel();
    const currentData = await upgrade(session.sessionId, "data", second.dataTicket);
    expect(currentData.response.status).toBe(101);
  });

  it("freezes one activation across each reserved browser connection set", async () => {
    const session = await createSession();
    await readyForBrowsers(session);
    const first = await createConnectionSet(session.sessionId, session.writerCapability);
    expect(first.deliveryGeneration).toBe("1");

    const replacedPending = await createConnectionSet(
      session.sessionId,
      session.writerCapability,
      first.clientId ?? undefined,
    );
    expect(replacedPending.deliveryGeneration).toBe("1");
    const revokedFirstControl = await upgrade(session.sessionId, "control", first.controlTicket);
    expect(revokedFirstControl.response.status).toBe(401);
    await revokedFirstControl.response.body?.cancel();

    const firstControl = await upgrade(session.sessionId, "control", replacedPending.controlTicket);
    const firstData = await upgrade(session.sessionId, "data", replacedPending.dataTicket);
    expect(firstControl.response.status).toBe(101);
    expect(firstData.response.status).toBe(101);

    const staleReplacement = await createConnectionSet(
      session.sessionId,
      session.writerCapability,
      first.clientId ?? undefined,
    );
    const currentReplacement = await createConnectionSet(
      session.sessionId,
      session.writerCapability,
      first.clientId ?? undefined,
    );
    expect(staleReplacement.deliveryGeneration).toBe("2");
    expect(currentReplacement.deliveryGeneration).toBe("2");

    const staleControl = await upgrade(
      session.sessionId,
      "control",
      staleReplacement.controlTicket,
    );
    expect(staleControl.response.status).toBe(401);
    await staleControl.response.body?.cancel();
    const staleData = await upgrade(session.sessionId, "data", staleReplacement.dataTicket);
    expect(staleData.response.status).toBe(401);
    await staleData.response.body?.cancel();
    const earlyReplacementData = await upgrade(
      session.sessionId,
      "data",
      currentReplacement.dataTicket,
    );
    expect(earlyReplacementData.response.status).toBe(409);
    await earlyReplacementData.response.body?.cancel();

    const replacementControl = await upgrade(
      session.sessionId,
      "control",
      currentReplacement.controlTicket,
    );
    const replacementData = await upgrade(session.sessionId, "data", currentReplacement.dataTicket);
    expect(replacementControl.response.status).toBe(101);
    expect(replacementData.response.status).toBe(101);
    await drainSession(session.sessionId);
    expect(firstControl.socket?.readyState).not.toBe(WebSocket.OPEN);
    expect(firstData.socket?.readyState).not.toBe(WebSocket.OPEN);

    const activated = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) =>
      state.storage.sql
        .exec(
          "SELECT delivery_generation, registered_at FROM client_delivery WHERE client_id = ?",
          first.clientId,
        )
        .one(),
    );
    expect(activated).toMatchObject({ delivery_generation: "2" });
    expect(activated.registered_at).not.toBeNull();
  });

  it("shares one atomic quota between pending reservations and registered clients", async () => {
    const session = await createSession();
    await readyForBrowsers(session);
    const pendingResponses = await Promise.all(
      Array.from({ length: 16 }, () =>
        requestConnectionSet(session.sessionId, session.observerCapability),
      ),
    );
    expect(pendingResponses.every((response) => response.status === 200)).toBe(true);
    const pending = await Promise.all(
      pendingResponses.map((response) => response.json<ConnectionSet>()),
    );

    const overflow = await requestConnectionSet(session.sessionId, session.observerCapability);
    expect(overflow.status).toBe(429);
    await overflow.body?.cancel();

    const reconnect = await createConnectionSet(
      session.sessionId,
      session.observerCapability,
      pending[0]!.clientId ?? undefined,
    );
    const controls = await Promise.all(
      [...pending.slice(1), reconnect].map((connection) =>
        upgrade(session.sessionId, "control", connection.controlTicket),
      ),
    );
    expect(controls.every(({ response }) => response.status === 101)).toBe(true);

    await evictDurableObject(sessionStub(session.sessionId), { webSockets: "hibernate" });
    const activeOverflow = await requestConnectionSet(
      session.sessionId,
      session.observerCapability,
    );
    expect(activeOverflow.status).toBe(429);
    await activeOverflow.body?.cancel();
  }, 15_000);

  it("reclaims fully disconnected clients instead of imposing a lifetime quota", async () => {
    const session = await createSession();
    await readyForBrowsers(session);
    const connections = await Promise.all(
      Array.from({ length: 16 }, () =>
        createConnectionSet(session.sessionId, session.observerCapability),
      ),
    );
    const controls = await Promise.all(
      connections.map((connection) =>
        upgrade(session.sessionId, "control", connection.controlTicket),
      ),
    );
    const data = await Promise.all(
      connections.map((connection) => upgrade(session.sessionId, "data", connection.dataTicket)),
    );
    expect(controls.every(({ response }) => response.status === 101)).toBe(true);
    expect(data.every(({ response }) => response.status === 101)).toBe(true);
    for (const { socket } of [...controls, ...data]) socket?.close(1000, "fully disconnected");
    await drainSession(session.sessionId);

    await expect(
      createConnectionSet(session.sessionId, session.observerCapability),
    ).resolves.toMatchObject({ clientId: expect.any(String) });
    const clientCount = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) =>
        state.storage.sql.exec("SELECT COUNT(*) AS value FROM client_delivery").one() as {
          value: number;
        },
    );
    expect(clientCount.value).toBeLessThanOrEqual(16);
  }, 30_000);

  it("keeps only one pending connection set for the same browser identity", async () => {
    const session = await createSession();
    await readyForBrowsers(session);
    const first = await createConnectionSet(session.sessionId, session.observerCapability);
    let latest = first;
    for (let index = 0; index < 4; index += 1) {
      latest = await createConnectionSet(
        session.sessionId,
        session.observerCapability,
        first.clientId ?? undefined,
      );
    }

    const pending = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) =>
        state.storage.sql
          .exec("SELECT COUNT(DISTINCT connection_set_id) AS sets FROM connection_ticket")
          .one() as { sets: number },
    );
    expect(pending.sets).toBe(1);
    const revoked = await upgrade(session.sessionId, "control", first.controlTicket);
    expect(revoked.response.status).toBe(401);
    await revoked.response.body?.cancel();
    const current = await upgrade(session.sessionId, "control", latest.controlTicket);
    expect(current.response.status).toBe(101);
  });

  it("keeps only one pending connection set for the host identity", async () => {
    const session = await createSession();
    const first = await createConnectionSet(session.sessionId, session.hostCapability);
    let latest = first;
    for (let index = 0; index < 4; index += 1) {
      latest = await createConnectionSet(session.sessionId, session.hostCapability);
    }

    const revoked = await upgrade(session.sessionId, "control", first.controlTicket);
    expect(revoked.response.status).toBe(401);
    await revoked.response.body?.cancel();
    const current = await upgrade(session.sessionId, "control", latest.controlTicket);
    expect(current.response.status).toBe(101);
  });

  it("keeps strict host and browser channel attachments across hibernation", async () => {
    const session = await createSession();
    const host = await readyForBrowsers(session);
    const browserSet = await createConnectionSet(session.sessionId, session.observerCapability);

    const browserControl = await upgrade(session.sessionId, "control", browserSet.controlTicket);
    const browserData = await upgrade(session.sessionId, "data", browserSet.dataTicket);
    const opened = [browserControl.socket, browserData.socket, host.control, host.data];
    expect(opened.every((socket) => socket !== undefined)).toBe(true);

    const stub = sessionStub(session.sessionId);
    await evictDurableObject(stub, { webSockets: "hibernate" });
    const attachments = await runInDurableObject(stub, (_instance, state) =>
      state.getWebSockets().map((socket) => socket.deserializeAttachment()),
    );
    expect(attachments).toHaveLength(4);
    expect(attachments).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ peer: "browser", channel: "control", ready: false }),
        expect.objectContaining({ peer: "browser", channel: "data", ready: false }),
        expect.objectContaining({ peer: "host", channel: "control", hostFence: "1" }),
        expect.objectContaining({ peer: "host", channel: "data", hostFence: "1" }),
      ]),
    );

    for (const socket of opened) {
      if (socket === undefined) continue;
      const pong = nextMessage(socket);
      socket.send("ping");
      await expect(pong).resolves.toBe("pong");
    }
  });

  it("rebuilds a non-current store and rejects every dormant socket", async () => {
    const session = await createSession();
    const host = await readyForBrowsers(session);
    const connection = await createConnectionSet(session.sessionId, session.observerCapability);
    const control = await upgrade(session.sessionId, "control", connection.controlTicket);
    const data = await upgrade(session.sessionId, "data", connection.dataTicket);
    expect(control.response.status).toBe(101);
    expect(data.response.status).toBe(101);
    if (control.socket === undefined || data.socket === undefined) {
      throw new Error("browser upgrade did not return both sockets");
    }

    const controlClosed = nextClose(control.socket);
    const dataClosed = nextClose(data.socket);
    const hostControlClosed = nextClose(host.control);
    const hostDataClosed = nextClose(host.data);
    const stub = sessionStub(session.sessionId);
    await runInDurableObject(stub, async (_instance, state) => {
      state.storage.sql.exec("CREATE TABLE obsolete_runtime_state (value TEXT) STRICT");
      state.storage.sql.exec("INSERT INTO obsolete_runtime_state VALUES ('discard')");
      state.storage.kv.put("obsolete-fact", { active: true });
      state.storage.kv.put("terminal-session:relay-schema", "obsolete-marker");
      await state.storage.setAlarm(Date.now() + 60_000);
    });
    await evictDurableObject(stub, { webSockets: "hibernate" });

    const rebuilt = await runInDurableObject(stub, async (_instance, state) => {
      const productTables = state.storage.sql
        .exec<{ name: string }>(
          `SELECT name FROM sqlite_master
           WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%'
           ORDER BY name`,
        )
        .toArray()
        .map(({ name }) => name);
      return {
        alarm: await state.storage.getAlarm(),
        clients: state.storage.sql.exec("SELECT COUNT(*) AS value FROM client_delivery").one(),
        kv: [...state.storage.kv.list()],
        productTables,
        sessions: state.storage.sql.exec("SELECT COUNT(*) AS value FROM session_state").one(),
      };
    });

    const closed = await within(
      Promise.all([controlClosed, dataClosed, hostControlClosed, hostDataClosed]),
      "store reset did not close dormant sockets",
    );
    expect(closed.map(({ code, reason }) => ({ code, reason }))).toEqual([
      { code: 4400, reason: "relay storage reset" },
      { code: 4400, reason: "relay storage reset" },
      { code: 4400, reason: "relay storage reset" },
      { code: 4400, reason: "relay storage reset" },
    ]);
    expect(rebuilt.alarm).toBeNull();
    expect(rebuilt.kv).toEqual([["terminal-session:relay-schema", "single-recovery"]]);
    expect(rebuilt.sessions).toEqual({ value: 0 });
    expect(rebuilt.clients).toEqual({ value: 0 });
    expect(rebuilt.productTables).toEqual([
      "client_delivery",
      "connection_ticket",
      "recovery_attempt",
      "recovery_control_outbox",
      "recovery_delivery_lane",
      "recovery_delivery_record",
      "session_state",
      "snapshot",
      "snapshot_upload",
      "writer_lease",
    ]);
  });
});
