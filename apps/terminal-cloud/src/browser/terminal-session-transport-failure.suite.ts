import type { ReplicaHost } from "@zhongduan/session-client";
import { describe, expect, it, vi } from "vitest";

import { CapabilityManager } from "./capability";
import { InputDispatcher } from "./input-dispatcher";
import {
  ENGINE_ID,
  FakeSocket,
  ManualTimers,
  SESSION_ID,
  VisibleSink,
  controlFrames,
  waitForSockets,
} from "./terminal-session.fixture";
import { TerminalSession } from "./terminal-session";

describe("TerminalSession transport failure boundaries", () => {
  it("bounds a connection-set request even when fetch ignores abort", async () => {
    const timers = new ManualTimers();
    timers.now = 2_000_000_000_000;
    let requestSignal: AbortSignal | undefined;
    const fetch = vi.fn((_input: RequestInfo | URL, init?: RequestInit) => {
      requestSignal = init?.signal ?? undefined;
      return new Promise<Response>(() => undefined);
    });
    const capabilities = new CapabilityManager({
      bootstrap: {
        capability: "opaque",
        expiresAt: Math.floor(timers.now / 1_000) + 1_000,
        issuedAt: Math.floor(timers.now / 1_000),
        role: "observer",
        sessionId: SESSION_ID,
      },
      fetch,
      setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
      clearTimer: vi.fn(),
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host: {
        engineId: ENGINE_ID,
        active: new VisibleSink(),
        restore: vi.fn<ReplicaHost["restore"]>(),
        adopt: vi.fn(),
      },
      input: new InputDispatcher({
        getObservedEventSeq: () => null,
        inputEpoch: "input_epoch_timeout1",
      }),
      sessionId: SESSION_ID,
      snapshots: { load: vi.fn() },
      fetch,
      createWebSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
      makeWebSocketUrl: (path) => path,
      now: () => timers.now,
      random: () => 0,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });

    session.start();
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledOnce());
    await timers.advanceBy(10_000);
    await vi.waitFor(() => expect(session.snapshot.phase).toBe("reconnecting"));

    expect(requestSignal?.aborted).toBe(true);
    expect(sockets).toHaveLength(0);
    session.close();
  });

  it("returns not-sent and replaces both sockets before invoking a backpressured sender", async () => {
    const sink = new VisibleSink();
    const host: ReplicaHost = {
      engineId: ENGINE_ID,
      active: sink,
      restore: vi.fn<ReplicaHost["restore"]>(),
      adopt: vi.fn(),
    };
    const response = {
      connectionSetId: "connection_set_queue1",
      connectionId: "connection_id_queue01",
      clientId: "browser_client_queue1",
      streamId: 7,
      deliveryGeneration: "2",
      expiresAt: Date.now() + 30_000,
      controlTicket: "control_ticket_queue1",
      dataTicket: "data_ticket_queue001",
      selectedCapabilities: ["browser-input-admission-v1"],
    };
    const fetch = vi.fn(
      async () =>
        new Response(JSON.stringify(response), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
    );
    const capabilities = new CapabilityManager({
      bootstrap: {
        capability: "opaque",
        expiresAt: Math.floor(Date.now() / 1_000) + 1_000,
        issuedAt: Math.floor(Date.now() / 1_000),
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch,
      setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
      clearTimer: vi.fn(),
    });
    const input = new InputDispatcher({
      getObservedEventSeq: () => 10n,
      inputEpoch: "input_epoch_queue1",
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host,
      input,
      initialCursor: {
        sessionEpoch: 1n,
        deliveryGeneration: 1n,
        lastEventSeq: 10n,
        nextPtyOffset: 20n,
      },
      sessionId: SESSION_ID,
      snapshots: { load: vi.fn() },
      fetch,
      createWebSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
      makeWebSocketUrl: (path) => path,
    });

    session.start();
    await waitForSockets(sockets, 1);
    const control = sockets[0]!;
    control.open();
    await waitForSockets(sockets, 2);
    const data = sockets[1]!;
    data.open();
    control.message(
      JSON.stringify({
        type: "welcome",
        connectionId: response.connectionId,
        streamId: 7,
        writerLease: "writer_lease_queue1",
        writerFence: "1",
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "2",
        headEventSeq: "10",
        nextPtyOffset: "20",
      }),
    );

    control.bufferedAmount = Number.MAX_SAFE_INTEGER;
    const sentBeforeBackpressure = control.sent.length;
    input.send({ type: "text", text: "bounded", source: "input" });
    await vi.waitFor(() => expect(input.status.lastStatus).toBe("not-sent"));
    expect(control.sent).toHaveLength(sentBeforeBackpressure);
    expect(control.readyState).toBe(3);
    expect(data.readyState).toBe(3);
    session.close();
  });

  it("aborts superseded and closed data replacement attempts", async () => {
    const sink = new VisibleSink();
    const host: ReplicaHost = {
      engineId: ENGINE_ID,
      active: sink,
      restore: vi.fn<ReplicaHost["restore"]>(),
      adopt: vi.fn(),
    };
    const response = {
      connectionSetId: "connection_set_replace1",
      connectionId: "connection_id_replace1",
      clientId: "browser_client_replace1",
      streamId: 7,
      deliveryGeneration: "2",
      expiresAt: Date.now() + 30_000,
      controlTicket: "control_ticket_replace1",
      dataTicket: "data_ticket_replace01",
      selectedCapabilities: ["browser-input-admission-v1"],
    };
    const fetch = vi.fn(
      async () =>
        new Response(JSON.stringify(response), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
    );
    const capabilities = new CapabilityManager({
      bootstrap: {
        capability: "opaque",
        expiresAt: Math.floor(Date.now() / 1_000) + 1_000,
        issuedAt: Math.floor(Date.now() / 1_000),
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch,
      setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
      clearTimer: vi.fn(),
    });
    const input = new InputDispatcher({
      getObservedEventSeq: () => 10n,
      inputEpoch: "input_epoch_replace1",
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host,
      input,
      initialCursor: {
        sessionEpoch: 1n,
        deliveryGeneration: 1n,
        lastEventSeq: 10n,
        nextPtyOffset: 20n,
      },
      sessionId: SESSION_ID,
      snapshots: { load: vi.fn() },
      fetch,
      createWebSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
      makeWebSocketUrl: (path) => path,
    });

    session.start();
    await waitForSockets(sockets, 1);
    const control = sockets[0]!;
    control.open();
    await waitForSockets(sockets, 2);
    sockets[1]!.open();
    await vi.waitFor(() => {
      expect(controlFrames(control).filter((frame) => frame.type === "attach")).toHaveLength(1);
    });

    control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "3",
        reason: "data-disconnected",
        dataTicket: "replacement_ticket_3",
        expiresAt: Date.now() + 30_000,
      }),
    );
    await waitForSockets(sockets, 3);
    const superseded = sockets[2]!;
    control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "4",
        reason: "data-disconnected",
        dataTicket: "replacement_ticket_4",
        expiresAt: Date.now() + 30_000,
      }),
    );
    await waitForSockets(sockets, 4);
    const current = sockets[3]!;
    expect(superseded.readyState).toBe(3);

    current.open();
    await vi.waitFor(() => {
      expect(controlFrames(control).filter((frame) => frame.type === "attach")).toHaveLength(2);
    });
    superseded.open();
    expect(controlFrames(control).filter((frame) => frame.type === "attach")).toHaveLength(2);

    control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "5",
        reason: "data-disconnected",
        dataTicket: "replacement_ticket_5",
        expiresAt: Date.now() + 30_000,
      }),
    );
    await waitForSockets(sockets, 5);
    const pendingAtClose = sockets[4]!;
    session.close();
    expect(pendingAtClose.readyState).toBe(3);
  });

  it("retries after a protocol failure aborts a pending data open", async () => {
    const sink = new VisibleSink();
    const host: ReplicaHost = {
      engineId: ENGINE_ID,
      active: sink,
      restore: vi.fn<ReplicaHost["restore"]>(),
      adopt: vi.fn(),
    };
    const responses = [2, 3].map((generation) => ({
      connectionSetId: `connection_set_000${generation}`,
      connectionId: `connection_id_0000${generation}`,
      clientId: "browser_client_0001",
      streamId: 7,
      deliveryGeneration: generation.toString(),
      expiresAt: Date.now() + 30_000,
      controlTicket: `control_ticket_000${generation}`,
      dataTicket: `data_ticket_00000${generation}`,
      selectedCapabilities: ["browser-input-admission-v1"],
    }));
    const fetch = vi.fn(
      async () =>
        new Response(JSON.stringify(responses.shift()), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
    );
    const capabilities = new CapabilityManager({
      bootstrap: {
        capability: "opaque",
        expiresAt: Math.floor(Date.now() / 1_000) + 1_000,
        issuedAt: Math.floor(Date.now() / 1_000),
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch,
      setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
      clearTimer: vi.fn(),
    });
    const input = new InputDispatcher({
      getObservedEventSeq: () => 10n,
      inputEpoch: "input_epoch_00001",
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host,
      input,
      initialCursor: {
        sessionEpoch: 1n,
        deliveryGeneration: 1n,
        lastEventSeq: 10n,
        nextPtyOffset: 20n,
      },
      sessionId: SESSION_ID,
      snapshots: { load: vi.fn() },
      fetch,
      createWebSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
      makeWebSocketUrl: (path) => path,
      random: () => 0,
    });

    session.start();
    await waitForSockets(sockets, 1);
    sockets[0]!.open();
    await waitForSockets(sockets, 2);
    sockets[0]!.message("{}");

    await vi.waitFor(() => expect(fetch).toHaveBeenCalledTimes(2), { timeout: 2_000 });
    await waitForSockets(sockets, 3);
    expect(session.snapshot.attempt).toBe(2);
    session.close();
  });

  it("fails closed without reconnecting on a same-generation engine mismatch", async () => {
    const sink = new VisibleSink();
    const host: ReplicaHost = {
      engineId: ENGINE_ID,
      active: sink,
      restore: vi.fn<ReplicaHost["restore"]>(),
      adopt: vi.fn(),
    };
    const response = {
      connectionSetId: "connection_set_engine1",
      connectionId: "connection_id_engine01",
      clientId: "browser_client_engine1",
      streamId: 7,
      deliveryGeneration: "2",
      expiresAt: Date.now() + 30_000,
      controlTicket: "control_ticket_engine1",
      dataTicket: "data_ticket_engine001",
      selectedCapabilities: ["browser-input-admission-v1"],
    };
    const fetch = vi.fn(
      async () =>
        new Response(JSON.stringify(response), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
    );
    const capabilities = new CapabilityManager({
      bootstrap: {
        capability: "opaque",
        expiresAt: Math.floor(Date.now() / 1_000) + 1_000,
        issuedAt: Math.floor(Date.now() / 1_000),
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch,
      setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
      clearTimer: vi.fn(),
    });
    const input = new InputDispatcher({
      getObservedEventSeq: () => 10n,
      inputEpoch: "input_epoch_engine1",
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host,
      input,
      initialCursor: {
        sessionEpoch: 1n,
        deliveryGeneration: 1n,
        lastEventSeq: 10n,
        nextPtyOffset: 20n,
      },
      sessionId: SESSION_ID,
      snapshots: { load: vi.fn() },
      fetch,
      createWebSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
      makeWebSocketUrl: (path) => path,
      random: () => 0,
    });

    session.start();
    await waitForSockets(sockets, 1);
    const control = sockets[0]!;
    control.open();
    await waitForSockets(sockets, 2);
    const pendingData = sockets[1]!;
    control.message(
      JSON.stringify({
        type: "welcome",
        connectionId: response.connectionId,
        streamId: 7,
        writerLease: "writer_lease_engine1",
        writerFence: "1",
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "2",
        headEventSeq: "10",
        nextPtyOffset: "20",
      }),
    );
    expect(input.status.writable).toBe(true);
    control.message(
      JSON.stringify({
        type: "welcome",
        connectionId: response.connectionId,
        streamId: 7,
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "2",
        headEventSeq: "10",
        nextPtyOffset: "20",
      }),
    );
    expect(input.status.writable).toBe(false);
    expect(session.snapshot.controlOwnership).toBe("waiting");
    control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "2",
        reason: "engine-mismatch",
      }),
    );

    expect(session.snapshot).toMatchObject({ lastError: "engine", phase: "failed" });
    expect(control.readyState).toBe(3);
    expect(pendingData.readyState).toBe(3);
    await new Promise((resolve) => setTimeout(resolve, 400));
    expect(fetch).toHaveBeenCalledOnce();
    expect(sockets).toHaveLength(2);
    session.close();
  });
});
