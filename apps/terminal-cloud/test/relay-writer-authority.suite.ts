import { describe, expect, it, vi } from "vitest";

import {
  RelayCapability,
  evictDurableObject,
  runInDurableObject,
  engineId,
  sessionStub,
  drainSession,
  createSession,
  createConnectionSet,
  upgrade,
  openHost,
  openBrowser,
  attachBrowser,
  sendBrowserAttach,
  renewWriterLease,
  keyFrame,
} from "./relay-fixture";

describe("live Durable Object relay: writer authority", () => {
  it("drops an old input frame that was already queued when a higher writer fence wins", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const first = await openBrowser(session, session.writerCapability, undefined, [
      RelayCapability.browserInputAdmissionV1,
    ]);
    const firstAttach = await attachBrowser(session, host, first);
    const firstLease = String(firstAttach.welcome.writerLease);
    const delayedMessage = JSON.stringify(keyFrame(firstLease, "delayed_old_writer", "1"));

    await runInDurableObject(sessionStub(session.sessionId), async (instance, state) => {
      const oldControl = state
        .getWebSockets(`client:${first.connection.clientId}`)
        .find(
          (socket) =>
            (socket.deserializeAttachment() as { channel?: string }).channel === "control",
        );
      if (oldControl === undefined) throw new Error("old writer control socket is missing");
      const lane = Reflect.get(instance, "browserControlLane") as {
        enqueue(
          key: WebSocket,
          bytes: number,
          run: (timing: {
            enqueuedAtMs: number;
            startedAtMs: number;
            waitMs: number;
          }) => Promise<void>,
          expire: () => void,
        ): Promise<void> | undefined;
        queuedCount: number;
      };
      const processMessage = Reflect.get(instance, "processWebSocketMessage") as (
        socket: WebSocket,
        message: ArrayBuffer | string,
        timing?: {
          enqueuedAtMs: number;
          receivedAtMs: number;
          startedAtMs: number;
          waitMs: number;
        },
      ) => Promise<void>;
      let releaseOldLane!: () => void;
      const gate = new Promise<void>((resolve) => {
        releaseOldLane = resolve;
      });
      const blocker = lane.enqueue(
        oldControl,
        1,
        () => gate,
        () => undefined,
      );
      const receivedAtMs = performance.now();
      const delayed = lane.enqueue(
        oldControl,
        delayedMessage.length * 2,
        (timing) =>
          processMessage.call(instance, oldControl, delayedMessage, { ...timing, receivedAtMs }),
        () => undefined,
      );
      expect(blocker).toBeDefined();
      expect(delayed).toBeDefined();
      expect(lane.queuedCount).toBe(2);
      state.storage.sql.exec(
        `UPDATE writer_lease
         SET connection_id = 'replacement_connection', fence = '2', expires_at = ?
         WHERE singleton = 1`,
        Date.now() + 30_000,
      );
      oldControl.close(4001, "connection replaced");
      releaseOldLane();
      await Promise.all([blocker, delayed]);
    });
    expect(await first.control.inbox.nextClose()).toMatchObject({
      code: 4001,
      reason: "connection replaced",
    });
    await drainSession(session.sessionId);
    expect(host.control.inbox.pendingMessageCount).toBe(0);
    const lease = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) =>
      state.storage.sql
        .exec("SELECT connection_id, fence, expires_at FROM writer_lease WHERE singleton = 1")
        .one(),
    );
    expect(lease).toMatchObject({
      connection_id: "replacement_connection",
      fence: "2",
    });
    expect(Number(lease.expires_at)).toBeGreaterThan(Date.now());
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

    const displaced = await first.control.inbox.nextClose();
    expect(displaced).toMatchObject({ code: 4001, reason: "connection replaced" });
    await drainSession(session.sessionId);
    const lease = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      return state.storage.sql
        .exec("SELECT client_id, connection_id, fence, expires_at FROM writer_lease")
        .one();
    });
    expect(lease).toMatchObject({
      client_id: second.connection.clientId,
      connection_id: secondReplacement.connection.connectionId,
      fence: "2",
    });
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
  }, 15_000);

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
        status: "uncertain",
      });
      expect((await writer.control.inbox.nextClose()).code).toBe(4400);
      expect((await writer.data.inbox.nextClose()).code).toBe(4400);
      expect(host.control.inbox.pendingMessageCount).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it("terminates an E1 writer epoch after heartbeat expiry", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const startedAt = Date.now();
    try {
      const session = await createSession();
      const host = await openHost(session);
      const writer = await openBrowser(session, session.writerCapability, undefined, [
        RelayCapability.browserInputAdmissionV1,
      ]);
      const { welcome } = await attachBrowser(session, host, writer);
      const writerLease = String(welcome.writerLease);

      vi.setSystemTime(startedAt + 30_001);
      writer.control.socket.send(JSON.stringify(keyFrame(writerLease, "expired_e1", "1")));
      expect(await writer.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
        type: "input-ack",
        writerFence: "1",
        inputEpoch: "expired_e1",
        clientInputSeq: "1",
        status: "uncertain",
      });
      expect((await writer.control.inbox.nextClose()).code).toBe(4400);
      expect((await writer.data.inbox.nextClose()).code).toBe(4400);
      expect(host.control.inbox.pendingMessageCount).toBe(0);

      const lease = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) =>
        state.storage.sql
          .exec("SELECT connection_id, expires_at, fence FROM writer_lease WHERE singleton = 1")
          .one(),
      );
      expect(lease).toEqual({
        connection_id: writer.connection.connectionId,
        expires_at: 0,
        fence: "1",
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it("proves the same connection-scoped writer attachment after hibernation", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const writer = await openBrowser(session, session.writerCapability);
    const { welcome } = await attachBrowser(session, host, writer);
    const writerLease = String(welcome.writerLease);
    const before = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => ({
      attachment: state
        .getWebSockets(`client:${writer.connection.clientId}`)
        .find(
          (socket) =>
            (socket.deserializeAttachment() as { channel?: string }).channel === "control",
        )
        ?.deserializeAttachment(),
      lease: state.storage.sql
        .exec(
          "SELECT client_id, connection_id, expires_at, fence FROM writer_lease WHERE singleton = 1",
        )
        .one(),
    }));
    expect(before.attachment).toMatchObject({
      clientId: writer.connection.clientId,
      connectionId: writer.connection.connectionId,
      leaseExpiresAt: before.lease.expires_at,
      leaseFence: "1",
    });

    await evictDurableObject(sessionStub(session.sessionId), { webSockets: "hibernate" });
    writer.control.socket.send(JSON.stringify(keyFrame(writerLease, "hibernated_writer", "1")));
    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "key",
      clientId: writer.connection.clientId,
      connectionId: writer.connection.connectionId,
      inputEpoch: "hibernated_writer",
      clientInputSeq: "1",
      writerFence: "1",
    });
    await drainSession(session.sessionId);

    const after = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) =>
      state.storage.sql
        .exec(
          "SELECT client_id, connection_id, expires_at, fence FROM writer_lease WHERE singleton = 1",
        )
        .one(),
    );
    expect(after).toEqual(before.lease);
  });

  it("closes a displaced writer and releases only the current connection fence on close", async () => {
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

    expect(await first.control.inbox.nextClose()).toMatchObject({
      code: 4001,
      reason: "connection replaced",
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
});
