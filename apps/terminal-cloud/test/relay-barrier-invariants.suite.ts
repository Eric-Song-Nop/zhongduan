import { describe, expect, it } from "vitest";

import {
  DataFrameKind,
  evictDurableObject,
  runInDurableObject,
  engineId,
  textEncoder,
  sessionStub,
  drainSession,
  createSession,
  publishSnapshot,
  deliveryBarrier,
  beginWarmDelivery,
  upgrade,
  openHost,
  openBrowser,
  attachBrowser,
  reattachBrowser,
  ptyFrame,
  replayCommit,
  resetFrame,
  keyFrame,
} from "./relay-fixture";

describe("live Durable Object relay: delivery barrier invariants", () => {
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
    const browser = await openBrowser(session, session.observerCapability);
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

    const replacementData = await upgrade(session.sessionId, "data", resync.dataTicket);
    const replacement = { ...browser, data: replacementData };
    const request = await reattachBrowser(session, host, replacement, "2");
    expect(request).toMatchObject({
      connectionId: browser.connection.connectionId,
      deliveryGeneration: "2",
    });
  });
});
