import { describe, expect, it } from "vitest";

import {
  DATA_HEADER_BYTES,
  RelayCapability,
  decodeDataFrameBatch,
  encodeDataFrameBatch,
  runInDurableObject,
  textEncoder,
  bytesFromMessage,
  sessionStub,
  drainSession,
  createSession,
  beginWarmDelivery,
  openHost,
  openBrowser,
  attachBrowser,
  ptyFrame,
  replayCommit,
} from "./relay-fixture";

describe("live Durable Object relay: Host data batches", () => {
  it("commits a negotiated Host data batch as its original ordered v2 frames", async () => {
    const session = await createSession();
    const host = await openHost(session);
    host.data.socket.send(
      encodeDataFrameBatch([
        ptyFrame(1n, 0n, textEncoder.encode("a")),
        ptyFrame(2n, 1n, textEncoder.encode("b")),
      ]),
    );
    await drainSession(session.sessionId);

    const cursor = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) =>
        state.storage.sql
          .exec("SELECT head_event_seq, next_pty_offset FROM session_state WHERE singleton = 1")
          .one() as { head_event_seq: string; next_pty_offset: string },
    );
    expect(cursor).toEqual({ head_event_seq: "2", next_pty_offset: "2" });
    expect(await host.data.inbox.nextMessage()).toBe("data-ack");
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
  });

  it("forwards a negotiated canonical batch as one ordered Browser data message", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability, undefined, [
      RelayCapability.browserDataBatchV1,
    ]);
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
    await drainSession(session.sessionId);
    await browser.data.inbox.nextDataFrame();
    expect(await host.data.inbox.nextMessage()).toBe("data-ack");

    host.data.socket.send(
      encodeDataFrameBatch([
        ptyFrame(1n, 0n, textEncoder.encode("a")),
        ptyFrame(2n, 1n, textEncoder.encode("b")),
      ]),
    );
    await drainSession(session.sessionId);

    const forwarded = decodeDataFrameBatch(
      await bytesFromMessage(await browser.data.inbox.nextMessage()),
    );
    expect(forwarded).toMatchObject([
      {
        deliveryGeneration: BigInt(browser.connection.deliveryGeneration),
        eventSeq: 1n,
        ptyOffset: 0n,
        streamId: browser.connection.streamId,
      },
      {
        deliveryGeneration: BigInt(browser.connection.deliveryGeneration),
        eventSeq: 2n,
        ptyOffset: 1n,
        streamId: browser.connection.streamId,
      },
    ]);
    expect(browser.data.inbox.pendingMessageCount).toBe(0);
    expect(await host.data.inbox.nextMessage()).toBe("data-ack");
  });

  it("retains one-frame-per-message delivery for an unnegotiated Browser", async () => {
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
    await drainSession(session.sessionId);
    await browser.data.inbox.nextDataFrame();
    expect(await host.data.inbox.nextMessage()).toBe("data-ack");

    host.data.socket.send(
      encodeDataFrameBatch([
        ptyFrame(1n, 0n, textEncoder.encode("a")),
        ptyFrame(2n, 1n, textEncoder.encode("b")),
      ]),
    );
    await drainSession(session.sessionId);

    expect(await browser.data.inbox.nextDataFrame()).toMatchObject({
      eventSeq: 1n,
      payload: textEncoder.encode("a"),
    });
    expect(await browser.data.inbox.nextDataFrame()).toMatchObject({
      eventSeq: 2n,
      payload: textEncoder.encode("b"),
    });
    expect(browser.data.inbox.pendingMessageCount).toBe(0);
    expect(await host.data.inbox.nextMessage()).toBe("data-ack");
  });

  it("fails the current host pair before queueing an oversized data frame", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, browser);

    host.data.socket.send(new Uint8Array(DATA_HEADER_BYTES + 16 * 1024 + 1));
    expect((await host.control.inbox.nextClose()).code).toBe(4400);
    expect((await host.data.inbox.nextClose()).code).toBe(4400);
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "host-offline",
    });
  });

  it("fails a negotiated Host that spends a second data credit before the first settles", async () => {
    const session = await createSession();
    const host = await openHost(session);

    await runInDurableObject(sessionStub(session.sessionId), async (instance, state) => {
      const hostData = state.getWebSockets("peer:host").find((socket) => {
        return (socket.deserializeAttachment() as { channel?: string }).channel === "data";
      });
      if (hostData === undefined) throw new Error("Host data socket is missing");
      const original = Reflect.get(instance, "messageQueue");
      let release!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      let admissions = 0;
      Reflect.set(instance, "messageQueue", {
        enqueue() {
          admissions += 1;
          return gate;
        },
      });
      try {
        await instance.webSocketMessage(hostData, new ArrayBuffer(1));
        await instance.webSocketMessage(hostData, new ArrayBuffer(1));
        expect(admissions).toBe(1);
      } finally {
        release();
        await gate;
        Reflect.set(instance, "messageQueue", original);
      }
    });

    expect((await host.control.inbox.nextClose()).code).toBe(4400);
    expect((await host.data.inbox.nextClose()).code).toBe(4400);
  });
});
