import type { InputSink } from "@wterm/core";
import {
  DataFrameKind,
  RELAY_CAPABILITIES_HEADER,
  RelayCapability,
  encodeDataFrameBatch,
  type ReplicaCursor,
} from "@zhongduan/protocol";
import type { ReplicaHost, SnapshotManifest, SnapshotTransport } from "@zhongduan/session-client";
import { describe, expect, it, vi } from "vitest";

import { CapabilityManager } from "./capability";
import { InputDispatcher } from "./input-dispatcher";
import {
  ENGINE_ID,
  FakeSocket,
  ManualTimers,
  RESIZE,
  SESSION_ID,
  VisibleSink,
  controlFrames,
  dataFrame,
  interruptKey,
  mousePress,
  openRecoveryUnderWatchdog,
  waitForSockets,
} from "./terminal-session.fixture";
import { TerminalSession } from "./terminal-session";

describe("TerminalSession activation and negotiated delivery", () => {
  it("coalesces live delivery acknowledgements while retaining the latest cursor", async () => {
    const timers = new ManualTimers();
    timers.now = 2_000_000_000_000;
    const { control, data, session } = await openRecoveryUnderWatchdog(timers, "warm-start");
    data.message(dataFrame(2n, 10n, 20n, [], DataFrameKind.ReplayCommit));
    expect(session.snapshot.phase).toBe("live");

    const acknowledgementsBefore = controlFrames(control).filter(
      (frame) => frame.type === "ack",
    ).length;
    for (let index = 0; index < 64; index += 1) {
      data.message(dataFrame(2n, 11n + BigInt(index), 20n + BigInt(index), [65]));
    }
    expect(controlFrames(control).filter((frame) => frame.type === "ack")).toHaveLength(
      acknowledgementsBefore,
    );

    await timers.advanceBy(10);
    const acknowledgements = controlFrames(control).filter((frame) => frame.type === "ack");
    expect(acknowledgements).toHaveLength(acknowledgementsBefore + 1);
    expect(acknowledgements.at(-1)).toMatchObject({
      deliveryGeneration: "2",
      eventSeq: "74",
      nextPtyOffset: "84",
    });
    session.close();
  });

  it("applies every logical frame in a negotiated Browser data batch", async () => {
    const timers = new ManualTimers();
    timers.now = 2_000_000_000_000;
    const { control, data, session } = await openRecoveryUnderWatchdog(timers, "warm-start");
    data.message(dataFrame(2n, 10n, 20n, [], DataFrameKind.ReplayCommit));

    const batch = encodeDataFrameBatch([
      new Uint8Array(dataFrame(2n, 11n, 20n, [65])),
      new Uint8Array(dataFrame(2n, 12n, 21n, [66])),
    ]);
    data.message(batch.buffer as ArrayBuffer);
    await timers.advanceBy(10);

    expect(
      controlFrames(control)
        .filter((frame) => frame.type === "ack")
        .at(-1),
    ).toMatchObject({
      deliveryGeneration: "2",
      eventSeq: "12",
      nextPtyOffset: "22",
    });
    expect(session.snapshot.phase).toBe("live");
    session.close();
  });

  it("waits for explicit reclaim after another control connection replaces this page", async () => {
    const timers = new ManualTimers();
    timers.now = 2_000_000_000_000;
    let connection = 0;
    const fetch = vi.fn(async () => {
      connection += 1;
      return new Response(
        JSON.stringify({
          connectionSetId: `connection_set_displaced_${connection}`,
          connectionId: `connection_displaced_${connection}`,
          clientId: "browser_client_displaced",
          streamId: 7,
          deliveryGeneration: String(connection),
          expiresAt: timers.now + 30_000,
          controlTicket: `control_ticket_displaced_${connection}`,
          dataTicket: `data_ticket_displaced_${connection}`,
          selectedCapabilities: ["browser-input-admission-v1"],
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    });
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
    const input = new InputDispatcher({ getObservedEventSeq: () => null });
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
      input,
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
    await waitForSockets(sockets, 1);
    sockets[0]!.open();
    await waitForSockets(sockets, 2);
    sockets[1]!.open();
    await vi.waitFor(() =>
      expect(session.snapshot).toMatchObject({ controlConnected: true, dataConnected: true }),
    );

    sockets[0]!.remoteClose(4001, "connection replaced");
    expect(session.snapshot).toMatchObject({
      controlConnected: false,
      controlOwnership: "waiting",
      dataConnected: false,
      phase: "displaced",
    });
    expect(input.status.writable).toBe(false);
    await timers.advanceBy(60_000);
    expect(sockets).toHaveLength(2);

    session.reconnectNow();
    await waitForSockets(sockets, 3);
    expect(session.snapshot.phase).toBe("reconnecting");
    session.close();
  });

  it("fails closed to read-only when an older Cloud does not select E1 admission", async () => {
    const response = {
      connectionSetId: "connection_set_legacy1",
      connectionId: "connection_id_legacy01",
      clientId: "browser_client_legacy1",
      streamId: 7,
      deliveryGeneration: "1",
      expiresAt: Date.now() + 30_000,
      controlTicket: "control_ticket_legacy1",
      dataTicket: "data_ticket_legacy001",
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
      getObservedEventSeq: () => null,
      inputEpoch: "input_epoch_legacy1",
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
      input,
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
    control.message(
      JSON.stringify({
        type: "welcome",
        connectionId: response.connectionId,
        streamId: 7,
        writerLease: "legacy_writer_lease",
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "1",
        headEventSeq: "0",
        nextPtyOffset: "0",
      }),
    );

    expect(input.status).toMatchObject({ connected: true, writable: false });
    expect(session.snapshot.controlOwnership).toBe("waiting");
    const localIntentId = input.send(interruptKey());
    expect(input.getResult(localIntentId)).toMatchObject({
      outcome: "not-sent",
      reason: "not-writable",
      identity: null,
    });
    expect(controlFrames(control).filter((frame) => frame.type === "key")).toEqual([]);
    session.close();
  });

  it("buffers data before replay-start and invalidates the old data callback before replacement", async () => {
    const sink = new VisibleSink();
    const host: ReplicaHost = {
      engineId: ENGINE_ID,
      active: sink,
      restore: vi.fn<ReplicaHost["restore"]>(),
      adopt: vi.fn(),
    };
    const snapshots: SnapshotTransport = {
      load: vi.fn<(manifest: SnapshotManifest, signal: AbortSignal) => Promise<Uint8Array>>(),
    };
    const sockets: FakeSocket[] = [];
    const connectionSets = [
      {
        connectionSetId: "connection_set_0001",
        connectionId: "connection_id_00001",
        clientId: "browser_client_0001",
        streamId: 7,
        deliveryGeneration: "2",
        expiresAt: Date.now() + 30_000,
        controlTicket: "control_ticket_0001",
        dataTicket: "data_ticket_000001",
        selectedCapabilities: ["browser-input-admission-v1"],
      },
    ];
    const fetch = vi.fn(
      async () =>
        new Response(JSON.stringify(connectionSets.shift()), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
    );
    const capabilities = new CapabilityManager({
      bootstrap: {
        capability: "not-logged-or-decoded-here",
        expiresAt: Math.floor(Date.now() / 1_000) + 1_000,
        issuedAt: Math.floor(Date.now() / 1_000),
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch,
      setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
      clearTimer: vi.fn(),
    });
    let session!: TerminalSession;
    const input: InputSink & InputDispatcher = new InputDispatcher({
      getObservedEventSeq: () => session.coordinator.activeCursor?.lastEventSeq ?? null,
      inputEpoch: "input_epoch_00001",
    });
    const initialCursor: ReplicaCursor = {
      sessionEpoch: 1n,
      deliveryGeneration: 1n,
      lastEventSeq: 10n,
      nextPtyOffset: 20n,
    };
    session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host,
      input,
      initialCursor,
      sessionId: SESSION_ID,
      snapshots,
      fetch,
      createWebSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
      makeWebSocketUrl: (path) => path,
    });

    session.start();
    expect(fetch).toHaveBeenCalledWith(
      expect.stringContaining("/connection-sets"),
      expect.objectContaining({
        headers: expect.objectContaining({
          [RELAY_CAPABILITIES_HEADER]: [
            RelayCapability.browserDataBatchV1,
            RelayCapability.browserInputAdmissionV1,
          ].join(","),
        }),
      }),
    );
    await waitForSockets(sockets, 1);
    const control = sockets[0]!;
    control.open();
    await waitForSockets(sockets, 2);
    const firstData = sockets[1]!;
    firstData.open();
    await vi.waitFor(() => {
      expect(controlFrames(control).some((frame) => frame.type === "attach")).toBe(true);
    });
    expect(controlFrames(control).find((frame) => frame.type === "attach")).toMatchObject({
      deliveryGeneration: "2",
    });

    firstData.message(dataFrame(2n, 11n, 20n, [65]));
    control.message(
      JSON.stringify({
        type: "welcome",
        connectionId: "connection_id_00001",
        streamId: 7,
        writerLease: "writer_lease_0001",
        writerFence: "1",
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "2",
        headEventSeq: "11",
        nextPtyOffset: "21",
      }),
    );
    expect(controlFrames(control).filter((frame) => frame.type === "ack")).toHaveLength(0);

    control.message(
      JSON.stringify({
        type: "replay-start",
        sessionEpoch: "1",
        streamId: 7,
        deliveryGeneration: "2",
        baseEventSeq: "10",
        basePtyOffset: "20",
        commitEventSeq: "11",
        commitPtyOffset: "21",
      }),
    );
    expect(sink.writes).toEqual([[65]]);
    expect(controlFrames(control).filter((frame) => frame.type === "ack")).toHaveLength(1);
    firstData.message(dataFrame(2n, 11n, 21n, [], DataFrameKind.ReplayCommit));
    expect(session.snapshot.phase).toBe("live");
    input.send(RESIZE);
    await vi.waitFor(() => {
      expect(
        controlFrames(control).filter((frame) => frame.type === "resize-request"),
      ).toHaveLength(1);
    });
    expect(controlFrames(control).find((frame) => frame.type === "resize-request")).toMatchObject({
      inputEpoch: "input_epoch_00001",
      clientInputSeq: "1",
    });
    expect(controlFrames(control).some((frame) => frame.type === "begin-input-epoch")).toBe(false);
    control.message(
      JSON.stringify({
        type: "input-ack",
        writerFence: "1",
        inputEpoch: "input_epoch_00001",
        clientInputSeq: "1",
        status: "written",
        authorityEventSeq: "11",
      }),
    );
    expect(input.status.pending).toBe(0);
    input.confirmAuthoritativeResize(RESIZE);
    control.message(JSON.stringify({ type: "host-offline" }));
    expect(session.snapshot.hostOnline).toBe(false);

    control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "3",
        reason: "data-disconnected",
        dataTicket: "replacement_ticket_1",
        expiresAt: Date.now() + 30_000,
      }),
    );
    expect(input.status.writable).toBe(true);
    expect(input.status.replicaCurrent).toBe(false);
    input.send(interruptKey());
    input.send(mousePress());
    await vi.waitFor(() => {
      expect(controlFrames(control).filter((frame) => frame.type === "key")).toHaveLength(1);
    });
    expect(controlFrames(control).filter((frame) => frame.type === "mouse")).toHaveLength(0);
    control.message(
      JSON.stringify({
        type: "replay-start",
        sessionEpoch: "1",
        streamId: 7,
        deliveryGeneration: "2",
        baseEventSeq: "10",
        basePtyOffset: "20",
        commitEventSeq: "11",
        commitPtyOffset: "21",
      }),
    );
    expect(session.snapshot.lastError).toBeNull();
    expect(session.snapshot.hostOnline).toBe(false);
    firstData.message(dataFrame(3n, 12n, 21n, [88]));
    await waitForSockets(sockets, 3);
    const replacementData = sockets[2]!;
    expect(control.readyState).toBe(1);
    replacementData.open();
    await vi.waitFor(() => {
      expect(controlFrames(control).filter((frame) => frame.type === "attach")).toHaveLength(2);
    });
    expect(
      controlFrames(control)
        .filter((frame) => frame.type === "attach")
        .at(-1),
    ).toMatchObject({
      deliveryGeneration: "3",
    });
    replacementData.message(dataFrame(3n, 12n, 21n, [66]));
    expect(
      controlFrames(control).filter(
        (frame) => frame.type === "ack" && frame.deliveryGeneration === "3",
      ),
    ).toHaveLength(0);
    control.message(
      JSON.stringify({
        type: "replay-start",
        sessionEpoch: "1",
        streamId: 7,
        deliveryGeneration: "3",
        baseEventSeq: "11",
        basePtyOffset: "21",
        commitEventSeq: "12",
        commitPtyOffset: "22",
      }),
    );
    expect(session.snapshot.hostOnline).toBe(true);
    expect(input.status.writable).toBe(true);
    expect(session.snapshot.controlOwnership).toBe("writer");
    replacementData.message(dataFrame(3n, 12n, 22n, [], DataFrameKind.ReplayCommit));
    input.send(mousePress());
    await vi.waitFor(() => {
      expect(controlFrames(control).filter((frame) => frame.type === "mouse")).toHaveLength(1);
    });

    expect(sink.writes).toEqual([[65], [66]]);
    expect(session.snapshot.phase).toBe("live");
    expect(input.status.replicaCurrent).toBe(true);
    expect(firstData.readyState).toBe(3);

    const acknowledgementsBeforeFailure = controlFrames(control).filter(
      (frame) => frame.type === "ack",
    ).length;
    control.message(
      JSON.stringify({
        type: "replay-start",
        sessionEpoch: "1",
        streamId: 8,
        deliveryGeneration: "3",
        baseEventSeq: "11",
        basePtyOffset: "21",
        commitEventSeq: "12",
        commitPtyOffset: "22",
      }),
    );
    expect(session.snapshot.lastError).toBe("protocol");
    expect(control.readyState).toBe(3);
    expect(replacementData.readyState).toBe(3);
    replacementData.message(dataFrame(3n, 13n, 22n, [67]));
    expect(sink.writes).toEqual([[65], [66]]);
    expect(controlFrames(control).filter((frame) => frame.type === "ack")).toHaveLength(
      acknowledgementsBeforeFailure,
    );
    session.close();
  });
});
