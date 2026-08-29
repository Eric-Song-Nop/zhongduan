import {
  ClientControlFrameSchema,
  HostControlFrameSchema,
  RELAY_CAPABILITIES_HEADER,
  RecoveryV3ClientControlFrameSchema,
  RecoveryV3HostToCloudControlFrameSchema,
  RelayCapability,
} from "@zhongduan/protocol";
import { env, exports as workerExports } from "cloudflare:workers";
import { evictDurableObject, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it } from "vitest";

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
  negotiatedCapabilities?: string[];
  streamId: number;
}

const origin = "https://terminal.example.test";
let sessionCounter = 0;
const testSockets = new Set<WebSocket>();
const testSocketSessions = new Set<string>();

function nextSessionId(): string {
  sessionCounter += 1;
  return `session_runtime_${sessionCounter.toString().padStart(16, "0")}`;
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
      body: JSON.stringify({
        sessionId,
        engineId: "ghostty:test+snapshot-v1+wterm:test",
        sessionEpoch: "7",
      }),
    }),
  );
  expect(response.status).toBe(201);
  return response.json<CreatedSession>();
}

async function createConnectionSet(
  sessionId: string,
  capability: string,
  clientId?: string,
  relayCapabilities: string[] = [],
): Promise<ConnectionSet> {
  const response = await requestConnectionSet(sessionId, capability, clientId, relayCapabilities);
  expect(response.status).toBe(200);
  return response.json<ConnectionSet>();
}

async function requestConnectionSet(
  sessionId: string,
  capability: string,
  clientId?: string,
  relayCapabilities: string[] = [],
): Promise<Response> {
  return workerExports.default.fetch(
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
}

async function upgrade(
  sessionId: string,
  channel: "control" | "data",
  ticket: string,
): Promise<{ response: Response; socket: WebSocket | undefined }> {
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions/${sessionId}/ws/${channel}?ticket=${ticket}`, {
      headers: { upgrade: "websocket" },
    }),
  );
  const socket = response.webSocket ?? undefined;
  socket?.accept();
  if (socket !== undefined) {
    testSockets.add(socket);
    testSocketSessions.add(sessionId);
    socket.addEventListener("close", () => testSockets.delete(socket), { once: true });
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
  return new Promise((resolve) => {
    socket.addEventListener("close", resolve, { once: true });
  });
}

async function drainSession(sessionId: string): Promise<void> {
  const stub = env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${sessionId}`));
  await runInDurableObject(stub, () => undefined);
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

describe("cloud relay runtime", () => {
  it("accepts the legacy internal connection-set body and rejects a capability body field", async () => {
    const session = await createSession();
    const stub = env.TERMINAL_SESSIONS.get(
      env.TERMINAL_SESSIONS.idFromName(`v1:${session.sessionId}`),
    );
    const legacyBody = {
      sessionId: session.sessionId,
      subject: "subject_runtime_0000000000000001",
      role: "host",
    } as const;
    const legacyResponse = await stub.fetch(
      new Request("https://do.internal/internal/connection-sets", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(legacyBody),
      }),
    );
    expect(legacyResponse.status).toBe(200);
    await expect(legacyResponse.json()).resolves.not.toHaveProperty("selectedCapabilities");

    const capabilityBodyResponse = await stub.fetch(
      new Request("https://do.internal/internal/connection-sets", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          ...legacyBody,
          selectedCapabilities: ["delivery-barrier-outcome-v1"],
        }),
      }),
    );
    expect(capabilityBodyResponse.status).toBe(400);
    await capabilityBodyResponse.body?.cancel();
  });

  it("ignores valid unknown capabilities at the edge-to-DO negotiation hop", async () => {
    const session = await createSession();
    const stub = env.TERMINAL_SESSIONS.get(
      env.TERMINAL_SESSIONS.idFromName(`v1:${session.sessionId}`),
    );
    const response = await stub.fetch(
      new Request("https://do.internal/internal/connection-sets", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-zhongduan-relay-capabilities":
            "future-relay-capability-v2,delivery-barrier-outcome-v1",
        },
        body: JSON.stringify({
          sessionId: session.sessionId,
          subject: "subject_runtime_0000000000000001",
          role: "host",
        }),
      }),
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.not.toHaveProperty("selectedCapabilities");
  });

  it("confirms and persists only the supported Browser negotiation without enabling v3", async () => {
    const session = await createSession();
    const connection = await createConnectionSet(
      session.sessionId,
      session.writerCapability,
      undefined,
      [
        RelayCapability.capabilityNegotiationV1,
        RelayCapability.authorityDataV2,
        RelayCapability.deliveryBarrierOutcomeV1,
        RelayCapability.recoveryV3GapFillV1,
      ],
    );
    expect(connection.negotiatedCapabilities).toEqual([
      RelayCapability.capabilityNegotiationV1,
      RelayCapability.authorityDataV2,
    ]);

    const stub = env.TERMINAL_SESSIONS.get(
      env.TERMINAL_SESSIONS.idFromName(`v1:${session.sessionId}`),
    );
    const durable = await runInDurableObject(stub, (_instance, state) => ({
      client: state.storage.sql
        .exec(
          `SELECT recovery_strategy FROM client_delivery
           WHERE client_id = ?`,
          connection.clientId,
        )
        .one(),
      session: state.storage.sql
        .exec("SELECT authority_data_version FROM session_state WHERE singleton = 1")
        .one(),
      tickets: state.storage.sql
        .exec(
          `SELECT channel, relay_capabilities_json FROM connection_ticket
           WHERE connection_set_id = ? ORDER BY channel`,
          connection.connectionSetId,
        )
        .toArray(),
    }));
    expect(durable).toEqual({
      client: { recovery_strategy: "v2" },
      session: { authority_data_version: 2 },
      tickets: [
        {
          channel: "control",
          relay_capabilities_json: JSON.stringify(connection.negotiatedCapabilities),
        },
        {
          channel: "data",
          relay_capabilities_json: JSON.stringify(connection.negotiatedCapabilities),
        },
      ],
    });

    const control = await upgrade(session.sessionId, "control", connection.controlTicket);
    const data = await upgrade(session.sessionId, "data", connection.dataTicket);
    expect(control.response.status).toBe(101);
    expect(data.response.status).toBe(101);
    await evictDurableObject(stub, { webSockets: "hibernate" });
    const attachments = await runInDurableObject(stub, (_instance, state) =>
      state
        .getWebSockets(`client:${connection.clientId}`)
        .map((socket) => socket.deserializeAttachment() as { relayCapabilities?: string[] }),
    );
    expect(attachments).toHaveLength(2);
    expect(
      attachments.every(
        ({ relayCapabilities }) =>
          JSON.stringify(relayCapabilities) === JSON.stringify(connection.negotiatedCapabilities),
      ),
    ).toBe(true);
    control.socket?.close(1000, "test complete");
    data.socket?.close(1000, "test complete");
  });

  // This is a production-v2 rejection gate, not Recovery v3 runtime coverage.
  it("keeps Recovery v3 control candidates unreachable through production v2 sockets", async () => {
    const session = await createSession();
    const host = await createConnectionSet(session.sessionId, session.hostCapability);
    const browser = await createConnectionSet(session.sessionId, session.observerCapability);
    expect(host.negotiatedCapabilities).toBeUndefined();
    expect(browser.negotiatedCapabilities).toBeUndefined();

    const hostCandidate = {
      type: "recovery-source-closed",
      recoveryId: "recovery_runtime_gate_0001",
      connectionId: browser.connectionId,
      streamId: browser.streamId,
      deliveryGeneration: browser.deliveryGeneration,
      throughRecoveryOrdinal: "1",
      throughRecoveryCumulativeEncodedBytes: "1",
    } as const;
    const browserCandidate = {
      type: "delivery-received",
      deliveryGeneration: browser.deliveryGeneration,
      lane: "recovery",
      contiguousDeliveryOrdinal: "1",
      cumulativeEncodedBytes: "1",
    } as const;
    expect(RecoveryV3HostToCloudControlFrameSchema.safeParse(hostCandidate).success).toBe(true);
    expect(RecoveryV3ClientControlFrameSchema.safeParse(browserCandidate).success).toBe(true);
    expect(HostControlFrameSchema.safeParse(hostCandidate).success).toBe(false);
    expect(ClientControlFrameSchema.safeParse(browserCandidate).success).toBe(false);

    const hostControl = await upgrade(session.sessionId, "control", host.controlTicket);
    const browserControl = await upgrade(session.sessionId, "control", browser.controlTicket);
    expect(hostControl.response.status).toBe(101);
    expect(browserControl.response.status).toBe(101);
    if (hostControl.socket === undefined || browserControl.socket === undefined) {
      throw new Error("control upgrade did not return both WebSockets");
    }
    const hostClosed = nextClose(hostControl.socket);
    const browserClosed = nextClose(browserControl.socket);
    hostControl.socket.send(JSON.stringify(hostCandidate));
    browserControl.socket.send(JSON.stringify(browserCandidate));
    await expect(hostClosed).resolves.toMatchObject({
      code: 4400,
      reason: "invalid host control frame",
    });
    await expect(browserClosed).resolves.toMatchObject({
      code: 4400,
      reason: "invalid browser control frame",
    });

    const stub = env.TERMINAL_SESSIONS.get(
      env.TERMINAL_SESSIONS.idFromName(`v1:${session.sessionId}`),
    );
    const durable = await runInDurableObject(stub, (_instance, state) => ({
      attemptCount: state.storage.sql
        .exec<{ value: number }>("SELECT COUNT(*) AS value FROM recovery_attempt")
        .one().value,
      browser: state.storage.sql
        .exec<{ recovery_strategy: string }>(
          "SELECT recovery_strategy FROM client_delivery WHERE client_id = ?",
          browser.clientId,
        )
        .one(),
      outboxCount: state.storage.sql
        .exec<{ value: number }>("SELECT COUNT(*) AS value FROM recovery_control_outbox")
        .one().value,
    }));
    expect(durable).toEqual({
      attemptCount: 0,
      browser: { recovery_strategy: "v2" },
      outboxCount: 0,
    });
  });

  it("keeps the exact legacy response keys when the negotiation bootstrap is absent", async () => {
    const session = await createSession();
    const response = await requestConnectionSet(
      session.sessionId,
      session.observerCapability,
      undefined,
      [RelayCapability.authorityDataV2],
    );
    expect(response.status).toBe(200);
    const body = await response.json<Record<string, unknown>>();
    expect(Object.keys(body).sort()).toEqual([
      "clientId",
      "connectionId",
      "connectionSetId",
      "controlTicket",
      "dataTicket",
      "deliveryGeneration",
      "expiresAt",
      "streamId",
    ]);
  });

  it("authenticates session and connection-set creation", async () => {
    const unauthorized = await workerExports.default.fetch(
      new Request(`${origin}/api/v1/sessions`, {
        method: "POST",
        headers: {
          authorization: "Bearer wrong-token",
          "content-type": "application/json",
        },
        body: JSON.stringify({ engineId: "ghostty:test" }),
      }),
    );
    expect(unauthorized.status).toBe(401);
    await unauthorized.text();

    const session = await createSession();
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
    await wrongSession.text();
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
          body: JSON.stringify({
            sessionId: nextSessionId(),
            engineId: "ghostty:test+snapshot-v1+wterm:test",
            sessionEpoch,
          }),
        }),
      );
      expect(response.status).toBe(400);
      await response.text();
    }
  });

  it("requires an explicit fresh epoch for every created PTY lifecycle", async () => {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const missingEpoch = await workerExports.default.fetch(
        new Request(`${origin}/api/v1/sessions`, {
          method: "POST",
          headers: {
            authorization: "Bearer test-bootstrap-token-with-at-least-32-bytes",
            "content-type": "application/json",
          },
          body: JSON.stringify({
            sessionId: nextSessionId(),
            engineId: "ghostty:test+snapshot-v1+wterm:test",
          }),
        }),
      );
      expect(missingEpoch.status).toBe(400);
      await missingEpoch.text();
    }

    const created = await Promise.all(
      ["11", "12"].map(async (sessionEpoch) => {
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
              engineId: "ghostty:test+snapshot-v1+wterm:test",
              sessionEpoch,
            }),
          }),
        );
        expect(response.status).toBe(201);
        return response.json<{ sessionEpoch: string; sessionId: string }>();
      }),
    );
    expect(created.map(({ sessionEpoch }) => sessionEpoch)).toEqual(["11", "12"]);
    expect(created[0]!.sessionId).not.toBe(created[1]!.sessionId);
  });

  it("retries a lost create response against one stable session identity", async () => {
    const sessionId = nextSessionId();
    const identity = {
      sessionId,
      engineId: "ghostty:test+snapshot-v1+wterm:test",
      sessionEpoch: "21",
    };
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

    const row = await runInDurableObject(
      env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${sessionId}`)),
      (_instance, state) =>
        state.storage.sql.exec("SELECT COUNT(*) AS count FROM session_state").one(),
    );
    expect(row).toEqual({ count: 1 });
    const engineConflict = await create({
      ...identity,
      engineId: `${identity.engineId}+other`,
    });
    expect(engineConflict.status).toBe(409);
    await engineConflict.text();
    const epochConflict = await create({ ...identity, sessionEpoch: "22" });
    expect(epochConflict.status).toBe(409);
    await epochConflict.text();
  });

  it("consumes each channel ticket once and requires control first", async () => {
    const session = await createSession();
    const connection = await createConnectionSet(session.sessionId, session.writerCapability);

    const earlyData = await upgrade(session.sessionId, "data", connection.dataTicket);
    expect(earlyData.response.status).toBe(409);
    await earlyData.response.text();

    const control = await upgrade(session.sessionId, "control", connection.controlTicket);
    expect(control.response.status).toBe(101);
    expect(control.socket).toBeDefined();

    const replayedControl = await upgrade(session.sessionId, "control", connection.controlTicket);
    expect(replayedControl.response.status).toBe(401);
    await replayedControl.response.text();

    const data = await upgrade(session.sessionId, "data", connection.dataTicket);
    expect(data.response.status).toBe(101);
    expect(data.socket).toBeDefined();

    const replayedData = await upgrade(session.sessionId, "data", connection.dataTicket);
    expect(replayedData.response.status).toBe(401);
    await replayedData.response.text();

    control.socket?.close(1000, "test complete");
    data.socket?.close(1000, "test complete");
  });

  it("does not treat a closed control socket as an active connection set", async () => {
    const session = await createSession();
    const connection = await createConnectionSet(session.sessionId, session.writerCapability);
    const control = await upgrade(session.sessionId, "control", connection.controlTicket);
    expect(control.response.status).toBe(101);
    expect(control.socket).toBeDefined();

    control.socket?.close(1000, "control ended");

    const data = await upgrade(session.sessionId, "data", connection.dataTicket);
    expect(data.response.status).toBe(401);
    await data.response.text();
  });

  it("binds a data ticket to the control socket from the same connection set", async () => {
    const session = await createSession();
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
    await staleData.response.text();
    const currentData = await upgrade(session.sessionId, "data", second.dataTicket);
    expect(currentData.response.status).toBe(101);

    secondControl.socket?.close(1000, "test complete");
    currentData.socket?.close(1000, "test complete");
    firstControl.socket?.close(1000, "test complete");
  });

  it("freezes one activation across each reserved browser connection set", async () => {
    const session = await createSession();
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
    await revokedFirstControl.response.text();

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

    const ticketGenerations = await runInDurableObject(
      env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${session.sessionId}`)),
      (_instance, state) =>
        state.storage.sql
          .exec(
            `SELECT channel, delivery_generation
             FROM connection_ticket
             WHERE connection_set_id = ?
             ORDER BY channel`,
            currentReplacement.connectionSetId,
          )
          .toArray() as unknown as Array<{ channel: string; delivery_generation: string }>,
    );
    expect(ticketGenerations).toEqual([
      { channel: "control", delivery_generation: "2" },
      { channel: "data", delivery_generation: "2" },
    ]);

    const staleControl = await upgrade(
      session.sessionId,
      "control",
      staleReplacement.controlTicket,
    );
    expect(staleControl.response.status).toBe(401);
    await staleControl.response.text();
    const staleData = await upgrade(session.sessionId, "data", staleReplacement.dataTicket);
    expect(staleData.response.status).toBe(401);
    await staleData.response.text();
    const earlyReplacementData = await upgrade(
      session.sessionId,
      "data",
      currentReplacement.dataTicket,
    );
    expect(earlyReplacementData.response.status).toBe(409);
    await earlyReplacementData.response.text();

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

    const activated = await runInDurableObject(
      env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${session.sessionId}`)),
      (_instance, state) =>
        state.storage.sql
          .exec(
            "SELECT delivery_generation, registered_at FROM client_delivery WHERE client_id = ?",
            first.clientId,
          )
          .one(),
    );
    expect(activated).toMatchObject({ delivery_generation: "2" });
    expect(activated.registered_at).not.toBeNull();

    replacementControl.socket?.close(1000, "test complete");
    replacementData.socket?.close(1000, "test complete");
  });

  it("shares one atomic quota between pending reservations and registered clients", async () => {
    const session = await createSession();
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
    await overflow.text();

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

    await evictDurableObject(
      env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${session.sessionId}`)),
      { webSockets: "hibernate" },
    );
    const activeOverflow = await requestConnectionSet(
      session.sessionId,
      session.observerCapability,
    );
    expect(activeOverflow.status).toBe(429);
    await activeOverflow.text();
    for (const { socket } of controls) socket?.close(1000, "test complete");
    await drainSession(session.sessionId);
  }, 15_000);

  it("reclaims fully disconnected clients instead of imposing a lifetime quota", async () => {
    const session = await createSession();
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
    for (const { socket } of [...controls, ...data]) {
      socket?.close(1000, "client fully disconnected");
    }
    await drainSession(session.sessionId);

    await expect(
      createConnectionSet(session.sessionId, session.observerCapability),
    ).resolves.toMatchObject({ clientId: expect.any(String) });

    const clientCount = await runInDurableObject(
      env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${session.sessionId}`)),
      (_instance, state) =>
        state.storage.sql.exec("SELECT COUNT(*) AS value FROM client_delivery").one() as {
          value: number;
        },
    );
    expect(clientCount.value).toBeLessThanOrEqual(16);
  }, 15_000);

  it("keeps only one pending connection set for the same browser identity", async () => {
    const session = await createSession();
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
      env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${session.sessionId}`)),
      (_instance, state) =>
        state.storage.sql
          .exec("SELECT COUNT(DISTINCT connection_set_id) AS sets FROM connection_ticket")
          .one() as { sets: number },
    );
    expect(pending.sets).toBe(1);
    const revoked = await upgrade(session.sessionId, "control", first.controlTicket);
    expect(revoked.response.status).toBe(401);
    await revoked.response.text();
    const current = await upgrade(session.sessionId, "control", latest.controlTicket);
    expect(current.response.status).toBe(101);
    current.socket?.close(1000, "test complete");
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
    await revoked.response.text();
    const current = await upgrade(session.sessionId, "control", latest.controlTicket);
    expect(current.response.status).toBe(101);
    current.socket?.close(1000, "test complete");
  });

  it("keeps host and browser channel attachments across hibernation", async () => {
    const session = await createSession();
    const browserSet = await createConnectionSet(session.sessionId, session.observerCapability);
    const hostSet = await createConnectionSet(session.sessionId, session.hostCapability);

    const browserControl = await upgrade(session.sessionId, "control", browserSet.controlTicket);
    const browserData = await upgrade(session.sessionId, "data", browserSet.dataTicket);
    const hostControl = await upgrade(session.sessionId, "control", hostSet.controlTicket);
    const hostData = await upgrade(session.sessionId, "data", hostSet.dataTicket);
    const sockets = [
      browserControl.socket,
      browserData.socket,
      hostControl.socket,
      hostData.socket,
    ];
    expect(sockets.every((socket) => socket !== undefined)).toBe(true);

    const stub = env.TERMINAL_SESSIONS.get(
      env.TERMINAL_SESSIONS.idFromName(`v1:${session.sessionId}`),
    );
    await evictDurableObject(stub, { webSockets: "hibernate" });

    const attachments = await runInDurableObject(stub, (_instance, state) => {
      return state.getWebSockets().map((socket) => socket.deserializeAttachment());
    });
    expect(attachments).toHaveLength(4);
    expect(attachments).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ peer: "browser", channel: "control" }),
        expect.objectContaining({ peer: "browser", channel: "data" }),
        expect.objectContaining({ peer: "host", channel: "control", hostFence: "1" }),
        expect.objectContaining({ peer: "host", channel: "data", hostFence: "1" }),
      ]),
    );

    for (const socket of sockets) {
      if (socket !== undefined) {
        const pong = nextMessage(socket);
        socket.send("ping");
        await expect(pong).resolves.toBe("pong");
        socket.close(1000, "test complete");
      }
    }
  });
});
