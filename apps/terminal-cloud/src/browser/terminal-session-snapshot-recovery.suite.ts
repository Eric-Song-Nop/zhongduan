import { DataFrameKind } from "@zhongduan/protocol";
import type { ReplicaHost, ReplicaSink, SnapshotTransport } from "@zhongduan/session-client";
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
  snapshotManifest,
  waitForSockets,
} from "./terminal-session.fixture";
import { TerminalSession } from "./terminal-session";

describe("TerminalSession snapshot recovery ownership", () => {
  it("bounds permanent snapshot failures to six connection sets in the first minute", async () => {
    const timers = new ManualTimers();
    timers.now = 2_000_000_000_000;
    const visible = new VisibleSink();
    const host: ReplicaHost = {
      engineId: ENGINE_ID,
      active: visible,
      restore: vi.fn<ReplicaHost["restore"]>(),
      adopt: vi.fn(),
    };
    const snapshots: SnapshotTransport = {
      load: vi.fn(async () => {
        throw new Error("snapshot download failed with 503");
      }),
    };
    let nextGeneration = 1;
    const fetch = vi.fn(async () => {
      const generation = nextGeneration++;
      return new Response(
        JSON.stringify({
          connectionSetId: `connection_set_retry${generation}`,
          connectionId: `connection_id_retry${generation}`,
          clientId: "browser_client_retry1",
          streamId: 7,
          deliveryGeneration: generation.toString(),
          expiresAt: timers.now + 30_000,
          controlTicket: `control_ticket_retry${generation}`,
          dataTicket: `data_ticket_retry${generation}`,
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
    const input = new InputDispatcher({
      getObservedEventSeq: () => null,
      inputEpoch: "input_epoch_retry01",
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host,
      input,
      sessionId: SESSION_ID,
      snapshots,
      fetch,
      createWebSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
      makeWebSocketUrl: (path) => path,
      now: () => timers.now,
      random: () => 0.5,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });
    const startedAt = timers.now;

    session.start();
    for (let attempt = 0; attempt < 5; attempt += 1) {
      const controlIndex = attempt * 2;
      await waitForSockets(sockets, controlIndex + 1);
      const control = sockets[controlIndex]!;
      control.open();
      await waitForSockets(sockets, controlIndex + 2);
      const data = sockets[controlIndex + 1]!;
      data.open();
      await vi.waitFor(() => {
        expect(controlFrames(control).some((frame) => frame.type === "attach")).toBe(true);
      });
      const generation = BigInt(attempt + 1);
      control.message(
        JSON.stringify({
          type: "welcome",
          connectionId: `connection_id_retry${generation}`,
          streamId: 7,
          writerLease: "writer_lease_retry1",
          writerFence: generation.toString(),
          engineId: ENGINE_ID,
          sessionEpoch: "1",
          deliveryGeneration: generation.toString(),
          headEventSeq: "6",
          nextPtyOffset: "11",
        }),
      );
      control.message(JSON.stringify(snapshotManifest(generation)));
      await vi.waitFor(() => {
        expect(session.snapshot).toMatchObject({ lastError: "connection", phase: "reconnecting" });
      });
      expect(control.readyState).toBe(1);
      expect(data.readyState).toBe(1);
      expect(input.status.writable).toBe(true);
      expect(visible.disposed).toBe(false);

      const nextControlCount = controlIndex + 3;
      await timers.runUntil(() => sockets.length >= nextControlCount);
    }

    expect(timers.now - startedAt).toBe(60_000);
    expect(fetch).toHaveBeenCalledTimes(6);
    expect(sockets).toHaveLength(11);
    session.close();
  });

  it("cancels an old snapshot retry when a higher generation starts and resets backoff at live", async () => {
    const timers = new ManualTimers();
    timers.now = 2_000_000_000_000;
    const visible = new VisibleSink();
    const candidate = new VisibleSink();
    const adopted: ReplicaSink[] = [];
    const host: ReplicaHost = {
      engineId: ENGINE_ID,
      active: visible,
      restore: vi.fn(async () => candidate),
      adopt: vi.fn((replica) => adopted.push(replica)),
    };
    let resolveSecond!: (bytes: Uint8Array) => void;
    const loadSnapshot = vi
      .fn<SnapshotTransport["load"]>()
      .mockRejectedValueOnce(new Error("snapshot SHA-256 mismatch"))
      .mockImplementationOnce(
        () =>
          new Promise<Uint8Array>((resolve) => {
            resolveSecond = resolve;
          }),
      )
      .mockRejectedValueOnce(new Error("snapshot download failed with 503"));
    const snapshots: SnapshotTransport = { load: loadSnapshot };
    const response = {
      connectionSetId: "connection_set_snapshot1",
      connectionId: "connection_id_snapshot1",
      clientId: "browser_client_snapshot1",
      streamId: 7,
      deliveryGeneration: "1",
      expiresAt: timers.now + 30_000,
      controlTicket: "control_ticket_snapshot1",
      dataTicket: "data_ticket_snapshot01",
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
      getObservedEventSeq: () => null,
      inputEpoch: "input_epoch_snapshot1",
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host,
      input,
      sessionId: SESSION_ID,
      snapshots,
      fetch,
      createWebSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
      makeWebSocketUrl: (path) => path,
      now: () => timers.now,
      random: () => 0.5,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });

    session.start();
    await waitForSockets(sockets, 1);
    const control = sockets[0]!;
    control.open();
    await waitForSockets(sockets, 2);
    sockets[1]!.open();
    await vi.waitFor(() => {
      expect(controlFrames(control).some((frame) => frame.type === "attach")).toBe(true);
    });
    control.message(
      JSON.stringify({
        type: "welcome",
        connectionId: response.connectionId,
        streamId: 7,
        writerLease: "writer_lease_snapshot1",
        writerFence: "1",
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "1",
        headEventSeq: "6",
        nextPtyOffset: "11",
      }),
    );
    control.message(JSON.stringify(snapshotManifest(1n)));
    await vi.waitFor(() => expect(session.snapshot.lastError).toBe("connection"));

    control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "2",
        reason: "data-disconnected",
        dataTicket: "replacement_ticket_snapshot2",
        expiresAt: timers.now + 30_000,
      }),
    );
    await waitForSockets(sockets, 3);
    const generationTwoData = sockets[2]!;
    generationTwoData.open();
    await vi.waitFor(() => {
      expect(controlFrames(control).filter((frame) => frame.type === "attach")).toHaveLength(2);
    });
    control.message(
      JSON.stringify({
        type: "welcome",
        connectionId: response.connectionId,
        streamId: 7,
        writerLease: "writer_lease_snapshot1",
        writerFence: "1",
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "2",
        headEventSeq: "6",
        nextPtyOffset: "11",
      }),
    );
    control.message(JSON.stringify(snapshotManifest(2n)));
    await vi.waitFor(() => expect(loadSnapshot).toHaveBeenCalledTimes(2));

    await timers.advanceBy(3_000);
    expect(control.readyState).toBe(1);
    expect(generationTwoData.readyState).toBe(1);
    expect(fetch).toHaveBeenCalledOnce();

    generationTwoData.message(dataFrame(2n, 6n, 10n, [65]));
    generationTwoData.message(dataFrame(2n, 6n, 11n, [], DataFrameKind.ReplayCommit));
    resolveSecond(Uint8Array.of(1));
    await vi.waitFor(() => expect(session.snapshot.phase).toBe("live"));
    expect(adopted).toEqual([candidate]);

    control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "3",
        reason: "data-disconnected",
        dataTicket: "replacement_ticket_snapshot3",
        expiresAt: timers.now + 30_000,
      }),
    );
    await waitForSockets(sockets, 4);
    sockets[3]!.open();
    control.message(JSON.stringify(snapshotManifest(3n)));
    await vi.waitFor(() => expect(loadSnapshot).toHaveBeenCalledTimes(3));
    expect(timers.nextDueIn()).toBe(2_000);
    session.close();
  });

  it("disposes a detached candidate after a snapshot-tail failure without dropping control input", async () => {
    const visible = new VisibleSink();
    const candidate = new VisibleSink();
    const restore = vi.fn(async () => candidate);
    const adopt = vi.fn();
    const host: ReplicaHost = {
      engineId: ENGINE_ID,
      active: visible,
      restore,
      adopt,
    };
    const response = {
      connectionSetId: "connection_set_candidate1",
      connectionId: "connection_id_candidate1",
      clientId: "browser_client_candidate1",
      streamId: 7,
      deliveryGeneration: "1",
      expiresAt: Date.now() + 30_000,
      controlTicket: "control_ticket_candidate1",
      dataTicket: "data_ticket_candidate01",
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
      getObservedEventSeq: () => null,
      inputEpoch: "input_epoch_candidate1",
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host,
      input,
      sessionId: SESSION_ID,
      snapshots: { load: vi.fn(async () => Uint8Array.of(1)) },
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
        writerLease: "writer_lease_candidate1",
        writerFence: "1",
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "1",
        headEventSeq: "6",
        nextPtyOffset: "11",
      }),
    );
    control.message(JSON.stringify(snapshotManifest(1n)));
    await vi.waitFor(() => expect(restore).toHaveBeenCalledOnce());
    for (let turn = 0; turn < 4; turn += 1) await Promise.resolve();

    data.message(dataFrame(1n, 5n, 10n, [], DataFrameKind.ReplayCommit));
    await vi.waitFor(() => expect(candidate.disposed).toBe(true));
    expect(visible.disposed).toBe(false);
    expect(adopt).not.toHaveBeenCalled();
    expect(control.readyState).toBe(1);
    expect(input.status.writable).toBe(true);
    expect(session.snapshot).toMatchObject({ lastError: "connection", phase: "reconnecting" });
    session.close();
  });

  it("keeps the old replica visible until a cold snapshot and its early tail commit", async () => {
    const visible = new VisibleSink();
    const candidate = new VisibleSink();
    const adopted: ReplicaSink[] = [];
    const host: ReplicaHost = {
      engineId: ENGINE_ID,
      active: visible,
      restore: vi.fn(async () => candidate),
      adopt: vi.fn((replica) => adopted.push(replica)),
    };
    const snapshots: SnapshotTransport = { load: vi.fn(async () => Uint8Array.of(1)) };
    const response = {
      connectionSetId: "connection_set_cold1",
      connectionId: "connection_id_cold01",
      clientId: "browser_client_cold1",
      streamId: 7,
      deliveryGeneration: "1",
      expiresAt: Date.now() + 30_000,
      controlTicket: "control_ticket_cold1",
      dataTicket: "data_ticket_cold001",
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
      getObservedEventSeq: () => null,
      inputEpoch: "input_epoch_cold01",
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host,
      input,
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
    await waitForSockets(sockets, 1);
    const control = sockets[0]!;
    control.open();
    await waitForSockets(sockets, 2);
    const data = sockets[1]!;
    data.open();
    await vi.waitFor(() => {
      expect(controlFrames(control).find((frame) => frame.type === "attach")).toMatchObject({
        hasLiveReplica: false,
      });
    });
    data.message(dataFrame(1n, 6n, 10n, [67]));
    data.message(dataFrame(1n, 6n, 11n, [], DataFrameKind.ReplayCommit));
    expect(visible.writes).toEqual([]);
    expect(adopted).toEqual([]);
    control.message(
      JSON.stringify({
        type: "welcome",
        connectionId: "connection_id_cold01",
        streamId: 7,
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "1",
        headEventSeq: "6",
        nextPtyOffset: "11",
      }),
    );
    control.message(
      JSON.stringify({
        type: "snapshot-manifest",
        snapshotId: "snapshot_cold_0001",
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        streamId: 7,
        deliveryGeneration: "1",
        cutEventSeq: "5",
        nextPtyOffset: "10",
        commitEventSeq: "6",
        commitPtyOffset: "11",
        compression: "none",
        compressedLength: "1",
        uncompressedLength: "1",
        sha256: "a".repeat(64),
        downloadPath: "/api/v1/sessions/session_123456789/snapshots/snapshot_cold_0001",
        restoreThrough: "finish",
      }),
    );

    await vi.waitFor(() => expect(adopted).toEqual([candidate]));
    expect(visible.writes).toEqual([]);
    expect(candidate.writes).toEqual([[67]]);
    expect(session.snapshot.phase).toBe("live");
    session.close();
  });
});
