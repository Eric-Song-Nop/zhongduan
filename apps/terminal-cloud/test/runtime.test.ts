import { env, exports as workerExports } from "cloudflare:workers";
import { evictDurableObject, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

interface CreatedSession {
  hostCapability: string;
  observerCapability: string;
  sessionId: string;
  writerCapability: string;
}

interface ConnectionSet {
  clientId: string | null;
  connectionSetId: string;
  controlTicket: string;
  dataTicket: string;
  deliveryGeneration: string;
  streamId: number;
}

const origin = "https://terminal.example.test";

async function createSession(): Promise<CreatedSession> {
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions`, {
      method: "POST",
      headers: {
        authorization: "Bearer test-bootstrap-token-with-at-least-32-bytes",
        "content-type": "application/json",
      },
      body: JSON.stringify({
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
): Promise<ConnectionSet> {
  const response = await requestConnectionSet(sessionId, capability, clientId);
  expect(response.status).toBe(200);
  return response.json<ConnectionSet>();
}

async function requestConnectionSet(
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
  return { response, socket };
}

function nextMessage(socket: WebSocket): Promise<string> {
  return new Promise((resolve, reject) => {
    socket.addEventListener("message", (event) => resolve(String(event.data)), { once: true });
    socket.addEventListener("error", () => reject(new Error("websocket failed")), { once: true });
  });
}

async function drainSession(sessionId: string): Promise<void> {
  const stub = env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${sessionId}`));
  await runInDurableObject(stub, () => undefined);
}

describe("cloud relay runtime", () => {
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
          body: JSON.stringify({ engineId: "ghostty:test+snapshot-v1+wterm:test" }),
        }),
      );
      expect(missingEpoch.status).toBe(400);
      await missingEpoch.text();
    }

    const created = await Promise.all(
      ["11", "12"].map(async (sessionEpoch) => {
        const response = await workerExports.default.fetch(
          new Request(`${origin}/api/v1/sessions`, {
            method: "POST",
            headers: {
              authorization: "Bearer test-bootstrap-token-with-at-least-32-bytes",
              "content-type": "application/json",
            },
            body: JSON.stringify({
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

  it("consumes each channel ticket once and requires control first", async () => {
    const session = await createSession();
    const connection = await createConnectionSet(session.sessionId, session.writerCapability);

    const earlyData = await upgrade(session.sessionId, "data", connection.dataTicket);
    expect(earlyData.response.status).toBe(409);

    const control = await upgrade(session.sessionId, "control", connection.controlTicket);
    expect(control.response.status).toBe(101);
    expect(control.socket).toBeDefined();

    const replayedControl = await upgrade(session.sessionId, "control", connection.controlTicket);
    expect(replayedControl.response.status).toBe(401);

    const data = await upgrade(session.sessionId, "data", connection.dataTicket);
    expect(data.response.status).toBe(101);
    expect(data.socket).toBeDefined();

    const replayedData = await upgrade(session.sessionId, "data", connection.dataTicket);
    expect(replayedData.response.status).toBe(401);

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
    const secondControl = await upgrade(session.sessionId, "control", second.controlTicket);
    expect(secondControl.response.status).toBe(101);

    const staleData = await upgrade(session.sessionId, "data", first.dataTicket);
    expect(staleData.response.status).toBe(401);
    await staleData.response.text();
    const currentData = await upgrade(session.sessionId, "data", second.dataTicket);
    expect(currentData.response.status).toBe(101);

    secondControl.socket?.close(1000, "test complete");
    currentData.socket?.close(1000, "test complete");
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
  });

  it("reclaims fully disconnected clients instead of imposing a lifetime quota", async () => {
    const session = await createSession();
    for (let index = 0; index < 17; index += 1) {
      const connection = await createConnectionSet(session.sessionId, session.observerCapability);
      const control = await upgrade(session.sessionId, "control", connection.controlTicket);
      const data = await upgrade(session.sessionId, "data", connection.dataTicket);
      expect(control.response.status).toBe(101);
      expect(data.response.status).toBe(101);
      control.socket?.close(1000, "client fully disconnected");
      data.socket?.close(1000, "client fully disconnected");
      await drainSession(session.sessionId);
    }

    const clientCount = await runInDurableObject(
      env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${session.sessionId}`)),
      (_instance, state) =>
        state.storage.sql.exec("SELECT COUNT(*) AS value FROM client_delivery").one() as {
          value: number;
        },
    );
    expect(clientCount.value).toBeLessThanOrEqual(16);
  });

  it("keeps only one pending connection set for the same browser identity", async () => {
    const session = await createSession();
    const first = await createConnectionSet(session.sessionId, session.observerCapability);
    let latest = first;
    for (let index = 0; index < 20; index += 1) {
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
    for (let index = 0; index < 20; index += 1) {
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
