import { describe, expect, it } from "vitest";

import {
  DataFrameKind,
  type DataFrame,
  evictDurableObject,
  runInDurableObject,
  engineId,
  textEncoder,
  sessionStub,
  drainSession,
  createSession,
  beginWarmDelivery,
  createConnectionSet,
  upgradeResponse,
  upgrade,
  openHost,
  openBrowser,
  attachBrowser,
  renewWriterLease,
  ptyFrame,
  replayCommit,
  keyFrame,
  recoveryInputFrames,
} from "./relay-fixture";

describe("live Durable Object relay: connection lifecycle", () => {
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
