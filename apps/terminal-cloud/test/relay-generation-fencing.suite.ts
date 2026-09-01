import { describe, expect, it } from "vitest";

import {
  DataFrameKind,
  runInDurableObject,
  type BrowserEndpoint,
  engineId,
  textEncoder,
  sessionStub,
  drainSession,
  createSession,
  beginWarmDelivery,
  createConnectionSet,
  upgrade,
  openHost,
  openBrowser,
  attachBrowser,
  reattachBrowser,
  sendBrowserAttach,
  ptyFrame,
  replayCommit,
  keyFrame,
} from "./relay-fixture";

describe("live Durable Object relay: delivery generation fencing", () => {
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
  }, 15_000);
});
