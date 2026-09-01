import { describe, expect, it } from "vitest";

import {
  runInDurableObject,
  engineId,
  textEncoder,
  sessionStub,
  drainSession,
  injectServerSendFailure,
  injectHostControlSendFailure,
  createSession,
  createConnectionSet,
  upgrade,
  openHost,
  openBrowser,
  attachBrowser,
  sendBrowserAttach,
  renewWriterLease,
  ptyFrame,
} from "./relay-fixture";

describe("live Durable Object relay: Host control lifecycle", () => {
  it("fences a throwing Host attach-request sink while preserving browser recovery state", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.writerCapability);
    const attached = await attachBrowser(session, host, browser);
    const writerLease = String(attached.welcome.writerLease);

    browser.data.socket.close(1000, "replace browser data");
    const resync = await browser.control.inbox.nextJson<{
      dataTicket: string;
      deliveryGeneration: string;
      type: string;
    }>();
    browser.data = await upgrade(session.sessionId, "data", resync.dataTicket);
    browser.connection.deliveryGeneration = resync.deliveryGeneration;
    await injectHostControlSendFailure(session);

    sendBrowserAttach(browser, { sessionEpoch: "7", eventSeq: "0", nextPtyOffset: "0" });
    await drainSession(session.sessionId);
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toEqual({
      type: "host-offline",
    });
    expect(browser.control.inbox.pendingMessageCount).toBe(0);
    expect((await host.control.inbox.nextClose()).code).toBe(4400);
    expect((await host.data.inbox.nextClose()).code).toBe(4400);
    expect(browser.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(browser.data.socket.readyState).toBe(WebSocket.OPEN);

    const recovery = await runInDurableObject(
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
          data: { dataState: string; deliveryGeneration: string };
        };
      },
    );
    expect(recovery.control).toMatchObject({
      controlState: "active",
      deliveryGeneration: "2",
      leaseFence: "1",
    });
    expect(recovery.data).toMatchObject({
      dataState: "catching-up",
      deliveryGeneration: "2",
    });
    expect(await renewWriterLease(session, browser, writerLease)).toMatchObject({
      type: "writer-lease-status",
      active: true,
    });
  });

  it("continues host-offline broadcast after one browser control sink fails", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const failing = await openBrowser(session, session.observerCapability);
    const healthy = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, failing);
    await attachBrowser(session, host, healthy);
    await injectServerSendFailure(session, failing, "control");

    host.data.socket.send(ptyFrame(2n, 0n, textEncoder.encode("gap")));
    expect((await host.control.inbox.nextClose()).code).toBe(4400);
    expect((await host.data.inbox.nextClose()).code).toBe(4400);
    const [controlClose, dataClose] = await Promise.all([
      failing.control.inbox.nextClose(),
      failing.data.inbox.nextClose(),
    ]);
    expect(controlClose.code).toBe(4400);
    expect(dataClose.code).toBe(4400);
    expect(await healthy.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "host-offline",
    });
    expect(healthy.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(healthy.data.socket.readyState).toBe(WebSocket.OPEN);
    const healthyData = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) => {
        const socket = state
          .getWebSockets(`client:${healthy.connection.clientId}`)
          .find((candidate) => {
            return (candidate.deserializeAttachment() as { channel?: string }).channel === "data";
          });
        return socket?.deserializeAttachment() as { dataState: string };
      },
    );
    expect(healthyData.dataState).toBe("catching-up");
  });

  it("rejects host-ready until the same fenced connection has an open data channel", async () => {
    const session = await createSession();
    const connection = await createConnectionSet(session.sessionId, session.hostCapability);
    const control = await upgrade(session.sessionId, "control", connection.controlTicket);

    control.socket.send(
      JSON.stringify({
        type: "host-ready",
        engineId,
        sessionEpoch: "7",
        headEventSeq: "0",
        nextPtyOffset: "0",
      }),
    );
    expect((await control.inbox.nextClose()).code).toBe(4400);
  });

  it("fails a current Host that sends data before the ready barrier", async () => {
    const session = await createSession();
    const connection = await createConnectionSet(session.sessionId, session.hostCapability);
    const control = await upgrade(session.sessionId, "control", connection.controlTicket);
    const data = await upgrade(session.sessionId, "data", connection.dataTicket);

    data.socket.send(ptyFrame(1n, 0n, textEncoder.encode("early")));
    expect((await control.inbox.nextClose()).code).toBe(4400);
    expect((await data.inbox.nextClose()).code).toBe(4400);
  });

  it("accepts consecutive Host data only after host-ready-ack", async () => {
    const session = await createSession();
    const host = await openHost(session);

    host.data.socket.send(ptyFrame(1n, 0n, textEncoder.encode("a")));
    host.data.socket.send(ptyFrame(2n, 1n, textEncoder.encode("b")));
    await drainSession(session.sessionId);

    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
    const head = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      return state.storage.sql
        .exec("SELECT head_event_seq, next_pty_offset FROM session_state")
        .one();
    });
    expect(head).toMatchObject({ head_event_seq: "2", next_pty_offset: "2" });
  });

  it("invalidates a pre-ready host connection when its data channel closes", async () => {
    const session = await createSession();
    const connection = await createConnectionSet(session.sessionId, session.hostCapability);
    const control = await upgrade(session.sessionId, "control", connection.controlTicket);
    const data = await upgrade(session.sessionId, "data", connection.dataTicket);

    data.socket.close(4001, "forged replacement before ready");
    expect((await control.inbox.nextClose()).code).toBe(4400);
  });

  it("closes same-set host data when its pre-ready control channel closes", async () => {
    const session = await createSession();
    const connection = await createConnectionSet(session.sessionId, session.hostCapability);
    const control = await upgrade(session.sessionId, "control", connection.controlTicket);
    const data = await upgrade(session.sessionId, "data", connection.dataTicket);

    control.socket.close(1000, "control closed before ready");
    expect((await data.inbox.nextClose()).code).toBe(4400);
  });
});
