import { describe, expect, it } from "vitest";

import {
  RelayCapability,
  runInDurableObject,
  sessionStub,
  drainSession,
  injectServerSendFailure,
  injectHostControlSendFailure,
  createSession,
  openHost,
  openBrowser,
  attachBrowser,
  renewWriterLease,
  keyFrame,
} from "./relay-fixture";

describe("live Durable Object relay: Browser input lane", () => {
  it("isolates an oversized browser control frame without failing the host", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);

    browser.control.socket.send("x".repeat(6 * 1024 * 1024 + 4_097));
    expect((await browser.control.inbox.nextClose()).code).toBe(4400);
    expect((await browser.data.inbox.nextClose()).code).toBe(4001);
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
  });

  it("releases WebSocket events after bounded admission instead of awaiting queue tails", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);

    await runInDurableObject(sessionStub(session.sessionId), async (instance, state) => {
      const hostData = state.getWebSockets("peer:host").find((socket) => {
        return (socket.deserializeAttachment() as { channel?: string }).channel === "data";
      });
      const browserControl = state
        .getWebSockets(`client:${browser.connection.clientId}`)
        .find((socket) => {
          return (socket.deserializeAttachment() as { channel?: string }).channel === "control";
        });
      if (hostData === undefined) throw new Error("Host data socket is missing");
      if (browserControl === undefined) throw new Error("Browser control socket is missing");

      const originalBulk = Reflect.get(instance, "messageQueue");
      const originalInput = Reflect.get(instance, "browserControlLane");
      let releaseBulk!: () => void;
      let releaseInput!: () => void;
      const bulkGate = new Promise<void>((resolve) => {
        releaseBulk = resolve;
      });
      const inputGate = new Promise<void>((resolve) => {
        releaseInput = resolve;
      });
      let bulkAdmissions = 0;
      let inputAdmissions = 0;

      Reflect.set(instance, "messageQueue", {
        enqueue() {
          bulkAdmissions += 1;
          return bulkGate;
        },
      });
      Reflect.set(instance, "browserControlLane", {
        dispatch() {
          inputAdmissions += 1;
          void inputGate;
        },
      });

      let handlersReleased = false;
      try {
        const bulkDispatch = instance.webSocketMessage(hostData, new ArrayBuffer(1));
        const inputDispatch = instance.webSocketMessage(
          browserControl,
          JSON.stringify(keyFrame("not_owned", "event_release", "1")),
        );
        handlersReleased = await Promise.race([
          Promise.all([bulkDispatch, inputDispatch]).then(() => true),
          new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 100)),
        ]);
      } finally {
        releaseBulk();
        releaseInput();
        await Promise.all([bulkGate, inputGate]);
        Reflect.set(instance, "messageQueue", originalBulk);
        Reflect.set(instance, "browserControlLane", originalInput);
      }

      expect(bulkAdmissions).toBe(1);
      expect(inputAdmissions).toBe(1);
      expect(handlersReleased).toBe(true);
    });

    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(browser.control.socket.readyState).toBe(WebSocket.OPEN);
  });

  it("keeps a cumulative delivery ACK burst from becoming an input-lane tail", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.writerCapability, undefined, [
      RelayCapability.browserInputAdmissionV1,
    ]);
    const { welcome } = await attachBrowser(session, host, browser);
    const writerLease = String(welcome.writerLease);

    await runInDurableObject(sessionStub(session.sessionId), async (instance, state) => {
      const browserControl = state
        .getWebSockets(`client:${browser.connection.clientId}`)
        .find((socket) => {
          return (socket.deserializeAttachment() as { channel?: string }).channel === "control";
        });
      if (browserControl === undefined) throw new Error("Browser control socket is missing");
      const acknowledgement = JSON.stringify({
        type: "ack",
        sessionEpoch: "7",
        deliveryGeneration: browser.connection.deliveryGeneration,
        eventSeq: "0",
        nextPtyOffset: "0",
      });
      const dispatches = Array.from({ length: 64 }, () =>
        instance.webSocketMessage(browserControl, acknowledgement),
      );
      dispatches.push(
        instance.webSocketMessage(
          browserControl,
          JSON.stringify(keyFrame(writerLease, "ack_burst", "1")),
        ),
      );
      await Promise.all(dispatches);
    });

    expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "key",
      connectionId: browser.connection.connectionId,
      writerFence: "1",
      inputEpoch: "ack_burst",
      clientInputSeq: "1",
    });
    expect(browser.control.socket.readyState).toBe(WebSocket.OPEN);
  });

  it("drains the E0 supported input load outside a blocked 4 MiB Host bulk tail", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.writerCapability, undefined, [
      RelayCapability.browserInputAdmissionV1,
    ]);
    const { welcome } = await attachBrowser(session, host, browser);
    const writerLease = String(welcome.writerLease);

    let releaseBulk!: () => void;
    let markBulkStarted!: () => void;
    const bulkStarted = new Promise<void>((resolve) => {
      markBulkStarted = resolve;
    });
    await runInDurableObject(sessionStub(session.sessionId), async (instance) => {
      const queue = Reflect.get(instance, "messageQueue") as {
        enqueue(key: object, bytes: number, task: () => Promise<void>): Promise<void> | undefined;
      };
      const gate = new Promise<void>((resolve) => {
        releaseBulk = resolve;
      });
      const blocked = queue.enqueue({}, 4 * 1024 * 1024, async () => {
        markBulkStarted();
        await gate;
      });
      expect(blocked).toBeDefined();
      await bulkStarted;
    });

    const frames = Array.from({ length: 32 }, (_, index) => ({
      type: "paste",
      writerLease,
      inputEpoch: "bulk_isolation",
      clientInputSeq: String(index + 1),
      data: index === 0 ? "\u0003" : "x".repeat(1_800),
    }));
    const encoded = frames.map((frame) => JSON.stringify(frame));
    expect(
      encoded.reduce((bytes, frame) => bytes + Buffer.byteLength(frame), 0),
    ).toBeLessThanOrEqual(64 * 1024);
    for (const frame of encoded) browser.control.socket.send(frame);

    const forwarded = await Promise.all(
      frames.map(() => host.control.inbox.nextJson<Record<string, unknown>>()),
    );
    expect(forwarded).toHaveLength(32);
    expect(forwarded.map((frame) => frame.clientInputSeq)).toEqual(
      Array.from({ length: 32 }, (_, index) => String(index + 1)),
    );
    expect(forwarded.filter((frame) => frame.data === "\u0003")).toHaveLength(1);
    expect(
      forwarded.every(
        (frame) =>
          frame.type === "paste" &&
          frame.connectionId === browser.connection.connectionId &&
          frame.inputEpoch === "bulk_isolation" &&
          frame.writerFence === "1",
      ),
    ).toBe(true);

    const whileBulkBlocked = await runInDurableObject(
      sessionStub(session.sessionId),
      (instance) => {
        const bulk = Reflect.get(instance, "messageQueue") as {
          queuedBytes: number;
          queuedCount: number;
        };
        const input = Reflect.get(instance, "browserControlLane") as {
          queuedBytes: number;
          queuedCount: number;
          telemetry: {
            snapshot(): {
              dispositionCounts: Record<string, number>;
              maxQueueWaitMs: number;
            };
          };
        };
        return {
          bulk: { bytes: bulk.queuedBytes, count: bulk.queuedCount },
          input: { bytes: input.queuedBytes, count: input.queuedCount },
          telemetry: input.telemetry.snapshot(),
        };
      },
    );
    expect(whileBulkBlocked).toMatchObject({
      bulk: { bytes: 4 * 1024 * 1024, count: 1 },
      input: { bytes: 0, count: 0 },
      telemetry: { dispositionCounts: { "host-sent": 32 } },
    });
    expect(whileBulkBlocked.telemetry.maxQueueWaitMs).toBeLessThanOrEqual(250);

    releaseBulk();
    await drainSession(session.sessionId);
    const queues = await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const bulk = Reflect.get(instance, "messageQueue") as { queuedCount: number };
      const input = Reflect.get(instance, "browserControlLane") as { queuedCount: number };
      return { bulk: bulk.queuedCount, input: input.queuedCount };
    });
    expect(queues).toEqual({ bulk: 0, input: 0 });
  });

  it("isolates an input acknowledgement control sink failure without failing the Host", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const failing = await openBrowser(session, session.observerCapability);
    const healthy = await openBrowser(session, session.observerCapability);
    await attachBrowser(session, host, failing);
    await attachBrowser(session, host, healthy);
    await injectServerSendFailure(session, failing, "control");

    host.control.socket.send(
      JSON.stringify({
        type: "input-ack",
        connectionId: failing.connection.connectionId,
        inputEpoch: "input_ack_failure",
        clientInputSeq: "1",
        status: "written",
        authorityEventSeq: "0",
      }),
    );
    const [controlClose, dataClose] = await Promise.all([
      failing.control.inbox.nextClose(),
      failing.data.inbox.nextClose(),
    ]);
    expect(controlClose.code).toBe(4400);
    expect(dataClose.code).toBe(4400);
    await drainSession(session.sessionId);
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
    expect(healthy.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(healthy.data.socket.readyState).toBe(WebSocket.OPEN);
  });

  it("fences both Host and writer epochs when an input send becomes uncertain", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.writerCapability, undefined, [
      RelayCapability.browserInputAdmissionV1,
    ]);
    const attached = await attachBrowser(session, host, browser);
    const writerLease = String(attached.welcome.writerLease);

    browser.data.socket.close(1000, "recover without data");
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "resync-required",
      deliveryGeneration: "2",
    });
    await injectHostControlSendFailure(session);

    const interrupt = {
      ...keyFrame(writerLease, "throwing_host_input", "1"),
      code: "KeyC",
      key: "c",
      text: "c",
      modifiers: 2,
      altGraph: false,
      unshiftedCodepoint: 0x63,
    };
    browser.control.socket.send(JSON.stringify(interrupt));
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toEqual({
      type: "host-offline",
    });
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "input-ack",
      inputEpoch: "throwing_host_input",
      clientInputSeq: "1",
      status: "uncertain",
    });
    expect((await host.control.inbox.nextClose()).code).toBe(4400);
    expect((await host.data.inbox.nextClose()).code).toBe(4400);
    expect((await browser.control.inbox.nextClose()).code).toBe(4400);
    const lease = await runInDurableObject(sessionStub(session.sessionId), (_instance, state) => {
      return state.storage.sql.exec("SELECT client_id, fence, expires_at FROM writer_lease").one();
    });
    expect(lease).toMatchObject({ client_id: browser.connection.clientId, fence: "1" });
    expect(lease.expires_at).toBe(0);
  });

  it("forwards structured text, focus, and mouse input without terminal encoding", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const writer = await openBrowser(session, session.writerCapability);
    const observer = await openBrowser(session, session.observerCapability);
    const writerAttach = await attachBrowser(session, host, writer);
    await attachBrowser(session, host, observer);
    const writerLease = String(writerAttach.welcome.writerLease);

    const inputs = [
      {
        type: "text",
        writerLease,
        inputEpoch: "structured_input",
        clientInputSeq: "1",
        data: "你好, terminal",
      },
      {
        type: "focus",
        writerLease,
        inputEpoch: "structured_input",
        clientInputSeq: "2",
        focused: false,
      },
      {
        type: "mouse",
        writerLease,
        inputEpoch: "structured_input",
        clientInputSeq: "3",
        action: "wheel",
        button: null,
        buttons: 0,
        modifiers: 3,
        altGraph: true,
        surface: { x: 120, y: 48 },
        deltaX: 0,
        deltaY: -12.5,
        deltaMode: "pixel",
      },
    ] as const;

    for (const input of inputs) {
      writer.control.socket.send(JSON.stringify(input));
      expect(await host.control.inbox.nextJson<Record<string, unknown>>()).toEqual({
        ...input,
        connectionId: writer.connection.connectionId,
        clientId: writer.connection.clientId,
        writerFence: "1",
      });
    }

    host.control.socket.send(
      JSON.stringify({
        type: "input-ack",
        connectionId: writer.connection.connectionId,
        inputEpoch: "structured_input",
        clientInputSeq: "3",
        status: "written",
        authorityEventSeq: "0",
      }),
    );
    expect(await writer.control.inbox.nextJson<Record<string, unknown>>()).toEqual({
      type: "input-ack",
      inputEpoch: "structured_input",
      clientInputSeq: "3",
      status: "written",
      authorityEventSeq: "0",
    });

    observer.control.socket.send(
      JSON.stringify({
        ...inputs[0],
        writerLease: "observer_cannot_write",
        inputEpoch: "observer_input",
      }),
    );
    expect(await observer.control.inbox.nextJson<Record<string, unknown>>()).toMatchObject({
      type: "input-ack",
      inputEpoch: "observer_input",
      clientInputSeq: "1",
      status: "rejected",
    });
    expect(host.control.inbox.pendingMessageCount).toBe(0);
  });

  it("isolates malformed mouse input without affecting Host authority", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.observerCapability);

    browser.control.socket.send(
      JSON.stringify({
        type: "mouse",
        writerLease: "malformed_mouse",
        inputEpoch: "malformed_mouse",
        clientInputSeq: "1",
        action: "move",
        button: null,
        buttons: 0,
        modifiers: 0,
        altGraph: false,
        surface: { x: 1, y: 2 },
        deltaY: 1,
      }),
    );
    expect((await browser.control.inbox.nextClose()).code).toBe(4400);
    expect((await browser.data.inbox.nextClose()).code).toBe(4001);
    expect(host.control.socket.readyState).toBe(WebSocket.OPEN);
    expect(host.data.socket.readyState).toBe(WebSocket.OPEN);
  });

  it("negotiates Browser input and data lanes while filtering Host-only capabilities", async () => {
    const session = await createSession();
    const host = await openHost(session);
    const browser = await openBrowser(session, session.writerCapability, undefined, [
      RelayCapability.deliveryBarrierOutcomeV1,
      RelayCapability.browserDataBatchV1,
      RelayCapability.browserInputAdmissionV1,
    ]);
    expect(browser.connection.selectedCapabilities).toEqual([
      RelayCapability.browserDataBatchV1,
      RelayCapability.browserInputAdmissionV1,
    ]);
    const { welcome } = await attachBrowser(session, host, browser);
    expect(welcome).toMatchObject({
      type: "welcome",
      writerFence: "1",
      writerLease: expect.any(String),
    });
    const writerLease = String(welcome.writerLease);

    expect(await renewWriterLease(session, browser, writerLease)).toMatchObject({
      type: "writer-lease-status",
      active: true,
      writerFence: "1",
    });
    const leaseBeforeInput = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, state) =>
        state.storage.sql
          .exec("SELECT connection_id, expires_at, fence FROM writer_lease WHERE singleton = 1")
          .one(),
    );
    expect(leaseBeforeInput).toMatchObject({
      connection_id: browser.connection.connectionId,
      fence: "1",
    });
    browser.control.socket.send(JSON.stringify(keyFrame(writerLease, "e1_browser_epoch", "1")));
    const forwarded = await host.control.inbox.nextJson<Record<string, unknown>>();
    expect(forwarded).toMatchObject({
      type: "key",
      connectionId: browser.connection.connectionId,
      writerFence: "1",
      inputEpoch: "e1_browser_epoch",
      clientInputSeq: "1",
    });
    host.control.socket.send(
      JSON.stringify({
        type: "input-ack",
        connectionId: browser.connection.connectionId,
        inputEpoch: "e1_browser_epoch",
        clientInputSeq: "1",
        status: "written",
        authorityEventSeq: "1",
      }),
    );
    expect(await browser.control.inbox.nextJson<Record<string, unknown>>()).toEqual({
      type: "input-ack",
      writerFence: "1",
      inputEpoch: "e1_browser_epoch",
      clientInputSeq: "1",
      status: "written",
      authorityEventSeq: "1",
    });

    const evidence = await runInDurableObject(sessionStub(session.sessionId), (instance, state) => {
      const lane = Reflect.get(instance, "browserControlLane") as {
        queuedBytes: number;
        queuedCount: number;
        telemetry: {
          snapshot(): {
            records: Array<Record<string, unknown>>;
            retainedCount: number;
          };
        };
      };
      return {
        lane: { bytes: lane.queuedBytes, count: lane.queuedCount },
        lease: state.storage.sql
          .exec("SELECT connection_id, expires_at, fence FROM writer_lease WHERE singleton = 1")
          .one(),
        telemetry: lane.telemetry.snapshot(),
      };
    });
    expect(evidence.lease).toEqual(leaseBeforeInput);
    expect(evidence.lane).toEqual({ bytes: 0, count: 0 });
    expect(evidence.telemetry.retainedCount).toBe(1);
    expect(evidence.telemetry.records[0]).toMatchObject({
      clientInputSeq: "1",
      connectionId: browser.connection.connectionId,
      disposition: "host-sent",
      inputEpoch: "e1_browser_epoch",
      writerFence: "1",
    });
    expect(evidence.telemetry.records[0]).not.toHaveProperty("writerLease");
  });
});
