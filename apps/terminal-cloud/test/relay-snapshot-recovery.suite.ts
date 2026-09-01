import { describe, expect, it } from "vitest";

import {
  DataFrameKind,
  env,
  evictDurableObject,
  runInDurableObject,
  storedSnapshotKey,
  engineId,
  textEncoder,
  sessionStub,
  drainSession,
  injectServerSendFailure,
  createSession,
  publishSnapshot,
  deliveryBarrier,
  beginWarmDelivery,
  beginSnapshotDelivery,
  upgrade,
  openHost,
  openBrowser,
  attachBrowser,
  reattachBrowser,
  ptyFrame,
  replayCommit,
  recoveryInputFrames,
} from "./relay-fixture";

describe("live Durable Object relay: snapshot recovery", () => {
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
});
