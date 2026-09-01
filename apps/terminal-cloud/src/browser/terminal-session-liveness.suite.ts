import { DataFrameKind } from "@zhongduan/protocol";
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
  dataFrame,
  interruptKey,
  openRecoveryUnderWatchdog,
  snapshotManifest,
  waitForSockets,
} from "./terminal-session.fixture";
import { ATTACH_START_TIMEOUT_MS, TerminalSession } from "./terminal-session";

describe("TerminalSession writer and recovery liveness", () => {
  it("renews writer ownership across data replacement and stops after lease loss", async () => {
    const timers = new ManualTimers();
    timers.now = 2_000_000_000_000;
    const sink = new VisibleSink();
    const response = {
      connectionSetId: "connection_set_lease01",
      connectionId: "connection_id_lease001",
      clientId: "browser_client_lease01",
      streamId: 7,
      deliveryGeneration: "2",
      expiresAt: timers.now + 30_000,
      controlTicket: "control_ticket_lease01",
      dataTicket: "data_ticket_lease001",
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
        expiresAt: Math.floor(timers.now / 1_000) + 1_000,
        issuedAt: Math.floor(timers.now / 1_000),
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch,
      setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
      clearTimer: vi.fn(),
    });
    const input = new InputDispatcher({
      getObservedEventSeq: () => 10n,
      inputEpoch: "input_epoch_lease01",
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host: {
        engineId: ENGINE_ID,
        active: sink,
        restore: vi.fn<ReplicaHost["restore"]>(),
        adopt: vi.fn(),
      },
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
      now: () => timers.now,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });

    session.start();
    await waitForSockets(sockets, 1);
    const control = sockets[0]!;
    control.open();
    await waitForSockets(sockets, 2);
    sockets[1]!.open();
    control.message(
      JSON.stringify({
        type: "welcome",
        connectionId: response.connectionId,
        streamId: 7,
        writerLease: "writer_lease_lease01",
        writerFence: "1",
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "2",
        headEventSeq: "10",
        nextPtyOffset: "20",
      }),
    );

    await timers.advanceBy(10_000);
    expect(controlFrames(control).filter((frame) => frame.type === "writer-lease-renew")).toEqual([
      { type: "writer-lease-renew", writerLease: "writer_lease_lease01" },
    ]);

    control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "3",
        reason: "data-disconnected",
        dataTicket: "replacement_ticket_lease3",
        expiresAt: timers.now + 30_000,
      }),
    );
    await waitForSockets(sockets, 3);
    sockets[2]!.open();
    await timers.advanceBy(10_000);
    expect(
      controlFrames(control).filter((frame) => frame.type === "writer-lease-renew"),
    ).toHaveLength(2);
    expect(input.status.writable).toBe(true);

    control.message(JSON.stringify({ type: "writer-lease-status", active: false }));
    const renewalsAtLoss = controlFrames(control).filter(
      (frame) => frame.type === "writer-lease-renew",
    ).length;
    input.send(interruptKey());
    await timers.advanceBy(30_000);

    expect(session.snapshot.controlOwnership).toBe("waiting");
    expect(input.status.writable).toBe(false);
    expect(controlFrames(control).filter((frame) => frame.type === "key")).toHaveLength(0);
    expect(
      controlFrames(control).filter((frame) => frame.type === "writer-lease-renew"),
    ).toHaveLength(renewalsAtLoss);
    session.close();
  });

  it.each(["control", "data"] as const)(
    "fences and reconnects when the %s channel becomes a silent half-open socket",
    async (silentChannel) => {
      const timers = new ManualTimers();
      timers.now = 2_000_000_000_000;
      const response = {
        connectionSetId: `connection_set_${silentChannel}`,
        connectionId: `connection_id_${silentChannel}`,
        clientId: `browser_client_${silentChannel}`,
        streamId: 7,
        deliveryGeneration: "1",
        expiresAt: timers.now + 30_000,
        controlTicket: `control_ticket_${silentChannel}`,
        dataTicket: `data_ticket_${silentChannel}`,
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
          inputEpoch: `input_epoch_${silentChannel}`,
        }),
        sessionId: SESSION_ID,
        snapshots: { load: vi.fn(() => new Promise<Uint8Array>(() => undefined)) },
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
      await waitForSockets(sockets, 1);
      const control = sockets[0]!;
      control.open();
      await waitForSockets(sockets, 2);
      const data = sockets[1]!;
      data.open();
      await vi.waitFor(() => {
        expect(controlFrames(control).some((frame) => frame.type === "attach")).toBe(true);
      });
      control.message(JSON.stringify(snapshotManifest(1n)));
      (silentChannel === "control" ? control : data).autoPong = false;

      await timers.advanceBy(44_999);
      expect(control.readyState).toBe(1);
      expect(data.readyState).toBe(1);
      await timers.advanceBy(1);

      expect(control.readyState).toBe(3);
      expect(data.readyState).toBe(3);
      expect(session.snapshot.phase).toBe("reconnecting");
      session.close();
    },
  );

  it("fences an attach that never receives a recovery start", async () => {
    const timers = new ManualTimers();
    timers.now = 2_000_000_000_000;
    const { control, data, session } = await openRecoveryUnderWatchdog(timers, "cold-pending");

    await timers.advanceBy(ATTACH_START_TIMEOUT_MS - 1);
    expect(control.readyState).toBe(1);
    expect(data.readyState).toBe(1);
    await timers.advanceBy(1);

    expect(control.readyState).toBe(3);
    expect(data.readyState).toBe(3);
    expect(session.snapshot).toMatchObject({ lastError: "connection", phase: "reconnecting" });
    session.close();
  });

  it("keeps the attach watchdog through replay-start and fences a missing ReplayCommit", async () => {
    const timers = new ManualTimers();
    timers.now = 2_000_000_000_000;
    const { control, data, session } = await openRecoveryUnderWatchdog(timers, "warm-start");

    await timers.advanceBy(ATTACH_START_TIMEOUT_MS - 1);
    expect(control.readyState).toBe(1);
    expect(data.readyState).toBe(1);
    await timers.advanceBy(1);

    expect(control.readyState).toBe(3);
    expect(data.readyState).toBe(3);
    expect(session.snapshot).toMatchObject({ lastError: "connection", phase: "reconnecting" });
    session.close();
  });

  it("cancels the attach watchdog only after the matching ReplayCommit makes warm replay live", async () => {
    const timers = new ManualTimers();
    timers.now = 2_000_000_000_000;
    const { control, data, session } = await openRecoveryUnderWatchdog(timers, "warm-start");

    await timers.advanceBy(ATTACH_START_TIMEOUT_MS - 1);
    data.message(dataFrame(2n, 10n, 20n, [], DataFrameKind.ReplayCommit));
    expect(session.snapshot).toMatchObject({ deliveryState: "live", phase: "live" });

    await timers.advanceBy(ATTACH_START_TIMEOUT_MS + 1);
    expect(control.readyState).toBe(1);
    expect(data.readyState).toBe(1);
    expect(session.snapshot).toMatchObject({ lastError: null, phase: "live" });
    session.close();
  });
});
