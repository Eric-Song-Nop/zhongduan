import {
  DATA_HEADER_BYTES,
  DataFrameKind,
  encodeDataFrame,
  encodeResizePayload,
  type DataFrame,
  type ReplicaCursor,
} from "@zhongduan/protocol";
import type { BrowserTelemetryEvent, BrowserTelemetrySink } from "@zhongduan/telemetry";
import { describe, expect, it, vi } from "vitest";

import { SessionCoordinator } from "./session-coordinator";
import type {
  DeliveryState,
  ReplicaHost,
  ReplicaSink,
  SnapshotManifest,
  SnapshotTransport,
  WarmReplayStart,
} from "./types";

class FakeReplica implements ReplicaSink {
  readonly engineId = "eng1-test";
  readonly writes: Uint8Array[] = [];
  readonly resizes: unknown[] = [];
  disposed = false;

  writePty(data: Uint8Array): void {
    this.writes.push(data.slice());
  }

  resize(dimensions: unknown): void {
    this.resizes.push(dimensions);
  }

  dispose(): void {
    this.disposed = true;
  }
}

function createHost(active: FakeReplica | null = new FakeReplica()): ReplicaHost & {
  adopted: FakeReplica[];
  restored: FakeReplica[];
} {
  const restored: FakeReplica[] = [];
  const adopted: FakeReplica[] = [];
  return {
    engineId: "eng1-test",
    active,
    restored,
    adopted,
    async restore() {
      const replica = new FakeReplica();
      restored.push(replica);
      return replica;
    },
    adopt(replica) {
      adopted.push(replica as FakeReplica);
    },
  };
}

function cursor(overrides: Partial<ReplicaCursor> = {}): ReplicaCursor {
  return {
    sessionEpoch: 7n,
    deliveryGeneration: 3n,
    lastEventSeq: 10n,
    nextPtyOffset: 100n,
    ...overrides,
  };
}

function frame(overrides: Partial<DataFrame> = {}): Uint8Array {
  return encodeDataFrame({
    kind: DataFrameKind.PtyOutput,
    flags: 0,
    sessionEpoch: 7n,
    deliveryGeneration: 3n,
    eventSeq: 11n,
    ptyOffset: 100n,
    streamId: 41,
    payload: new Uint8Array([0x6d]),
    ...overrides,
  });
}

function commit(eventSeq = 11n, ptyOffset = 101n, overrides: Partial<DataFrame> = {}): Uint8Array {
  return frame({
    kind: DataFrameKind.ReplayCommit,
    eventSeq,
    ptyOffset,
    payload: new Uint8Array(),
    ...overrides,
  });
}

function replayStart(overrides: Partial<WarmReplayStart> = {}): WarmReplayStart {
  return {
    type: "replay-start",
    sessionEpoch: "7",
    streamId: 41,
    deliveryGeneration: "3",
    baseEventSeq: "10",
    basePtyOffset: "100",
    commitEventSeq: "11",
    commitPtyOffset: "101",
    ...overrides,
  };
}

function manifest(overrides: Partial<SnapshotManifest> = {}): SnapshotManifest {
  return {
    type: "snapshot-manifest",
    snapshotId: "snapshot_0000000001",
    engineId: "eng1-test",
    sessionEpoch: "7",
    streamId: 41,
    deliveryGeneration: "3",
    cutEventSeq: "10",
    nextPtyOffset: "100",
    commitEventSeq: "11",
    commitPtyOffset: "101",
    compression: "none",
    compressedLength: "4",
    uncompressedLength: "4",
    sha256: "0".repeat(64),
    downloadPath: "/api/v1/sessions/session_000000000001/snapshots/snapshot_0000000001",
    restoreThrough: "finish",
    ...overrides,
  };
}

function createTelemetryRecorder(): {
  events: BrowserTelemetryEvent[];
  monotonicNow: ReturnType<typeof vi.fn<() => number>>;
  telemetry: BrowserTelemetrySink;
} {
  const events: BrowserTelemetryEvent[] = [];
  let now = 0;
  return {
    events,
    monotonicNow: vi.fn(() => ++now),
    telemetry: (event) => events.push(event),
  };
}

describe("SessionCoordinator", () => {
  it("warm replays ordered output and resize into the existing replica", () => {
    const host = createHost();
    const onAcknowledge = vi.fn();
    const onReplicaProgress = vi.fn();
    const coordinator = new SessionCoordinator({
      host,
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge,
      onReplicaProgress,
      onResync: vi.fn(),
    });
    coordinator.startWarmReplay(replayStart({ commitEventSeq: "12" }));

    coordinator.acceptData(frame());
    coordinator.acceptData(
      frame({
        kind: DataFrameKind.ResizeApplied,
        eventSeq: 12n,
        ptyOffset: 101n,
        payload: encodeResizePayload({ cols: 120, rows: 40, widthPx: 960, heightPx: 800 }),
      }),
    );
    coordinator.acceptData(commit(12n, 101n));

    expect(coordinator.state).toBe("live");
    expect((host.active as FakeReplica).writes).toEqual([new Uint8Array([0x6d])]);
    expect((host.active as FakeReplica).resizes).toHaveLength(1);
    expect(host.adopted).toHaveLength(0);
    expect(coordinator.activeCursor).toEqual({
      sessionEpoch: 7n,
      deliveryGeneration: 3n,
      lastEventSeq: 12n,
      nextPtyOffset: 101n,
    });
    expect(onAcknowledge).toHaveBeenLastCalledWith({
      sessionEpoch: 7n,
      deliveryGeneration: 3n,
      lastEventSeq: 12n,
      nextPtyOffset: 101n,
    });
    expect(onReplicaProgress).toHaveBeenLastCalledWith({
      sessionEpoch: 7n,
      deliveryGeneration: 3n,
      lastEventSeq: 12n,
      nextPtyOffset: 101n,
    });
  });

  it("requires the replay commit to equal the barrier-pinned warm watermark", () => {
    const host = createHost();
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host,
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
    });
    coordinator.startWarmReplay(replayStart({ commitEventSeq: "12" }));

    coordinator.acceptData(frame());
    coordinator.acceptData(commit());

    expect(coordinator.state).toBe("resyncing");
    expect(resync).toHaveBeenCalledWith("journal-gap");
  });

  it("buffers directed data until its control delivery starts", () => {
    const host = createHost();
    const onAcknowledge = vi.fn();
    const coordinator = new SessionCoordinator({
      host,
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge,
      onReplicaProgress: vi.fn(),
      onResync: vi.fn(),
    });

    coordinator.acceptData(frame());
    coordinator.acceptData(commit());

    expect((host.active as FakeReplica).writes).toHaveLength(0);
    expect(onAcknowledge).not.toHaveBeenCalled();

    coordinator.startWarmReplay(replayStart());

    expect((host.active as FakeReplica).writes).toEqual([new Uint8Array([0x6d])]);
    expect(onAcknowledge).toHaveBeenLastCalledWith({
      sessionEpoch: 7n,
      deliveryGeneration: 3n,
      lastEventSeq: 11n,
      nextPtyOffset: 101n,
    });
    expect(coordinator.state).toBe("live");
  });

  it("freezes the old delivery when a newer generation arrives first", () => {
    const host = createHost();
    const onAcknowledge = vi.fn();
    const onResync = vi.fn();
    const coordinator = new SessionCoordinator({
      host,
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge,
      onReplicaProgress: vi.fn(),
      onResync,
    });
    coordinator.startWarmReplay(replayStart());

    coordinator.acceptData(frame({ deliveryGeneration: 4n, streamId: 42 }));
    coordinator.acceptData(frame());
    coordinator.acceptData(commit(11n, 101n, { deliveryGeneration: 4n, streamId: 42 }));

    expect((host.active as FakeReplica).writes).toHaveLength(0);
    expect(onAcknowledge).not.toHaveBeenCalled();
    expect(onResync).not.toHaveBeenCalled();
    expect(coordinator.state).toBe("awaiting-control");

    coordinator.startWarmReplay(replayStart({ streamId: 42, deliveryGeneration: "4" }));

    expect((host.active as FakeReplica).writes).toEqual([new Uint8Array([0x6d])]);
    expect(coordinator.state).toBe("live");
    expect(onResync).not.toHaveBeenCalled();
  });

  it("explicitly fences the old delivery as soon as a replacement data channel opens", () => {
    const host = createHost();
    const acknowledge = vi.fn();
    const coordinator = new SessionCoordinator({
      host,
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge: acknowledge,
      onReplicaProgress: vi.fn(),
      onResync: vi.fn(),
    });
    coordinator.startWarmReplay(replayStart());

    coordinator.fenceDeliveryGeneration(4n);
    coordinator.acceptData(frame());

    expect((host.active as FakeReplica).writes).toHaveLength(0);
    expect(acknowledge).not.toHaveBeenCalled();
    expect(coordinator.state).toBe("awaiting-control");

    coordinator.startWarmReplay(replayStart({ streamId: 42, deliveryGeneration: "4" }));
    coordinator.acceptData(frame({ deliveryGeneration: 4n, streamId: 42 }));
    coordinator.acceptData(commit(11n, 101n, { deliveryGeneration: 4n, streamId: 42 }));

    expect((host.active as FakeReplica).writes).toEqual([new Uint8Array([0x6d])]);
    expect(coordinator.state).toBe("live");
  });

  it("ignores a control start older than the highest observed data generation", () => {
    const host = createHost();
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host,
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
    });
    coordinator.startWarmReplay(replayStart());
    coordinator.acceptData(frame({ deliveryGeneration: 5n, streamId: 43 }));

    coordinator.startWarmReplay(replayStart({ streamId: 42, deliveryGeneration: "4" }));

    expect(coordinator.state).toBe("awaiting-control");
    expect((host.active as FakeReplica).writes).toHaveLength(0);
    expect(resync).not.toHaveBeenCalled();

    coordinator.startWarmReplay(replayStart({ streamId: 43, deliveryGeneration: "5" }));
    coordinator.acceptData(commit(11n, 101n, { deliveryGeneration: 5n, streamId: 43 }));

    expect((host.active as FakeReplica).writes).toEqual([new Uint8Array([0x6d])]);
    expect(coordinator.state).toBe("live");
  });

  it("bounds data that arrives before its control delivery", () => {
    const resync = vi.fn();
    const acknowledge = vi.fn();
    const coordinator = new SessionCoordinator({
      host: createHost(),
      snapshots: { load: vi.fn() },
      onAcknowledge: acknowledge,
      onReplicaProgress: vi.fn(),
      onResync: resync,
      maxBufferedTailBytes: DATA_HEADER_BYTES,
    });

    coordinator.acceptData(frame());

    expect(coordinator.state).toBe("resyncing");
    expect(resync).toHaveBeenCalledWith("slow-client");
    expect(acknowledge).not.toHaveBeenCalled();
  });

  it.each([
    [DataFrameKind.DeliveryBarrier, "journal-gap"],
    [DataFrameKind.Reset, "slow-client"],
  ] as const)("never applies host-only data frame kind %s", (kind, reason) => {
    const host = createHost();
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host,
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
    });
    coordinator.startWarmReplay(replayStart());

    coordinator.acceptData(frame({ kind, payload: new Uint8Array() }));

    expect((host.active as FakeReplica).writes).toHaveLength(0);
    expect(coordinator.state).toBe("resyncing");
    expect(resync).toHaveBeenCalledWith(reason);
  });

  it("rejects non-zero v2 data flags before applying a frame", () => {
    const host = createHost();
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host,
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
    });
    coordinator.startWarmReplay(replayStart());
    const encoded = frame();
    encoded[6] = 1;

    coordinator.acceptData(encoded);

    expect((host.active as FakeReplica).writes).toHaveLength(0);
    expect(coordinator.state).toBe("resyncing");
    expect(resync).toHaveBeenCalledWith("journal-gap");
  });

  it("requires an explicitly newer fence to recover from a failed generation", () => {
    const host = createHost();
    const acknowledge = vi.fn();
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host,
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge: acknowledge,
      onReplicaProgress: vi.fn(),
      onResync: resync,
      maxBufferedTailBytes: DATA_HEADER_BYTES + 1,
    });

    coordinator.acceptData(
      frame({
        deliveryGeneration: 4n,
        streamId: 42,
        payload: new Uint8Array([0x61, 0x62]),
      }),
    );
    expect(coordinator.state).toBe("resyncing");

    coordinator.fenceDeliveryGeneration(4n);
    coordinator.startWarmReplay(replayStart({ streamId: 42, deliveryGeneration: "4" }));
    expect(coordinator.state).toBe("resyncing");

    coordinator.fenceDeliveryGeneration(5n);
    expect(coordinator.state).toBe("awaiting-control");
    coordinator.acceptData(frame({ deliveryGeneration: 5n, streamId: 43 }));
    expect(acknowledge).not.toHaveBeenCalled();

    coordinator.startWarmReplay(replayStart({ streamId: 43, deliveryGeneration: "5" }));
    coordinator.acceptData(commit(11n, 101n, { deliveryGeneration: 5n, streamId: 43 }));

    expect(coordinator.state).toBe("live");
    expect(acknowledge).toHaveBeenLastCalledWith({
      sessionEpoch: 7n,
      deliveryGeneration: 5n,
      lastEventSeq: 11n,
      nextPtyOffset: 101n,
    });
  });

  it("keeps the old replica visible until snapshot and tail commit atomically", async () => {
    let resolveSnapshot: ((value: Uint8Array) => void) | undefined;
    const snapshots: SnapshotTransport = {
      load: () =>
        new Promise((resolve) => {
          resolveSnapshot = resolve;
        }),
    };
    const host = createHost();
    const onReplicaProgress = vi.fn();
    const coordinator = new SessionCoordinator({
      host,
      snapshots,
      onAcknowledge: vi.fn(),
      onReplicaProgress,
      onResync: vi.fn(),
    });

    const restoring = coordinator.startSnapshot(manifest());
    coordinator.acceptData(frame());
    coordinator.acceptData(commit());

    expect(host.adopted).toHaveLength(0);
    expect((host.active as FakeReplica).writes).toHaveLength(0);
    expect(onReplicaProgress).not.toHaveBeenCalled();

    resolveSnapshot?.(new Uint8Array([1, 2, 3, 4]));
    await restoring;

    expect(host.restored[0]?.writes).toEqual([new Uint8Array([0x6d])]);
    expect(host.adopted).toEqual(host.restored);
    expect(coordinator.state).toBe("live");
    expect(coordinator.activeCursor).toEqual({
      sessionEpoch: 7n,
      deliveryGeneration: 3n,
      lastEventSeq: 11n,
      nextPtyOffset: 101n,
    });
    expect(onReplicaProgress).toHaveBeenLastCalledWith({
      sessionEpoch: 7n,
      deliveryGeneration: 3n,
      lastEventSeq: 11n,
      nextPtyOffset: 101n,
    });
  });

  it("accepts snapshot tail data that arrives before its manifest", async () => {
    const host = createHost();
    const acknowledge = vi.fn();
    const coordinator = new SessionCoordinator({
      host,
      snapshots: { load: async () => new Uint8Array([1, 2, 3, 4]) },
      onAcknowledge: acknowledge,
      onReplicaProgress: vi.fn(),
      onResync: vi.fn(),
    });

    coordinator.acceptData(frame());
    coordinator.acceptData(commit());

    expect(acknowledge).not.toHaveBeenCalled();
    expect(host.restored).toHaveLength(0);

    await coordinator.startSnapshot(manifest());

    expect(host.restored[0]?.writes).toEqual([new Uint8Array([0x6d])]);
    expect(host.adopted).toEqual(host.restored);
    expect(coordinator.state).toBe("live");
  });

  it("buffers live frames that follow an early replay commit until snapshot adoption", async () => {
    let resolveSnapshot: ((value: Uint8Array) => void) | undefined;
    const host = createHost();
    const coordinator = new SessionCoordinator({
      host,
      snapshots: {
        load: () =>
          new Promise((resolve) => {
            resolveSnapshot = resolve;
          }),
      },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: vi.fn(),
    });

    const restoring = coordinator.startSnapshot(manifest());
    coordinator.acceptData(frame());
    coordinator.acceptData(commit());
    coordinator.acceptData(
      frame({
        eventSeq: 12n,
        ptyOffset: 101n,
        payload: new Uint8Array([0x6e]),
      }),
    );

    resolveSnapshot?.(new Uint8Array([1, 2, 3, 4]));
    await restoring;

    expect(host.restored[0]?.writes).toEqual([new Uint8Array([0x6d]), new Uint8Array([0x6e])]);
    expect(host.adopted).toEqual(host.restored);
    expect(coordinator.activeCursor).toEqual({
      sessionEpoch: 7n,
      deliveryGeneration: 3n,
      lastEventSeq: 12n,
      nextPtyOffset: 102n,
    });
  });

  it("does not decode a stale snapshot whose transport ignores cancellation", async () => {
    let resolveSnapshot: ((value: Uint8Array) => void) | undefined;
    const host = createHost();
    const restore = vi.spyOn(host, "restore");
    const coordinator = new SessionCoordinator({
      host,
      initialCursor: cursor(),
      snapshots: {
        load: () =>
          new Promise((resolve) => {
            resolveSnapshot = resolve;
          }),
      },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: vi.fn(),
    });

    const staleRestore = coordinator.startSnapshot(manifest());
    coordinator.startWarmReplay(replayStart({ streamId: 42, deliveryGeneration: "4" }));
    resolveSnapshot?.(new Uint8Array([1, 2, 3, 4]));
    await staleRestore;

    expect(restore).not.toHaveBeenCalled();
    expect(coordinator.state).toBe("replaying");
  });

  it("cancels a detached restore when newer generation data wins the race", async () => {
    let resolveSnapshot: ((value: Uint8Array) => void) | undefined;
    let restoreSignal: AbortSignal | undefined;
    const host = createHost();
    const restore = vi.spyOn(host, "restore");
    const coordinator = new SessionCoordinator({
      host,
      snapshots: {
        load: (_manifest, signal) => {
          restoreSignal = signal;
          return new Promise((resolve) => {
            resolveSnapshot = resolve;
          });
        },
      },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: vi.fn(),
    });

    const staleRestore = coordinator.startSnapshot(manifest());
    coordinator.acceptData(frame({ deliveryGeneration: 4n, streamId: 42 }));

    expect(restoreSignal?.aborted).toBe(true);
    expect(coordinator.state).toBe("awaiting-control");

    resolveSnapshot?.(new Uint8Array([1, 2, 3, 4]));
    await staleRestore;

    expect(restore).not.toHaveBeenCalled();
  });

  it("disposes a restored candidate that loses to a future generation fence", async () => {
    let resolveReplica: ((replica: ReplicaSink) => void) | undefined;
    const candidate = new FakeReplica();
    const dispose = vi.spyOn(candidate, "dispose");
    const host = createHost();
    const restore = vi.fn<ReplicaHost["restore"]>(
      () =>
        new Promise<ReplicaSink>((resolve) => {
          resolveReplica = resolve;
        }),
    );
    host.restore = restore;
    const coordinator = new SessionCoordinator({
      host,
      snapshots: { load: async () => new Uint8Array([1, 2, 3, 4]) },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: vi.fn(),
    });

    const staleRestore = coordinator.startSnapshot(manifest());
    await vi.waitFor(() => expect(restore).toHaveBeenCalledTimes(1));
    coordinator.fenceDeliveryGeneration(4n);
    resolveReplica?.(candidate);
    await staleRestore;

    expect(dispose).toHaveBeenCalledTimes(1);
    expect(coordinator.state).toBe("awaiting-control");
  });

  it("ignores a stale start and an exact duplicate without replacing the current stream", async () => {
    const host = createHost();
    const snapshots = { load: vi.fn() };
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host,
      initialCursor: cursor(),
      snapshots,
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
    });
    const current = replayStart({ deliveryGeneration: "4" });
    coordinator.startWarmReplay(current);

    await coordinator.startSnapshot(manifest({ deliveryGeneration: "3", streamId: 42 }));
    coordinator.startWarmReplay({ ...current });
    coordinator.acceptData(frame({ deliveryGeneration: 4n }));

    expect(snapshots.load).not.toHaveBeenCalled();
    expect((host.active as FakeReplica).writes).toEqual([new Uint8Array([0x6d])]);
    expect(coordinator.state).toBe("replaying");
    expect(resync).not.toHaveBeenCalled();
  });

  it("rejects a same-generation warm start with a different stream", () => {
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host: createHost(),
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
    });
    const current = replayStart({ deliveryGeneration: "4" });
    coordinator.startWarmReplay(current);

    coordinator.startWarmReplay({ ...current, streamId: 99 });

    expect(coordinator.state).toBe("resyncing");
    expect(resync).toHaveBeenCalledWith("journal-gap");
  });

  it("rejects a same-generation warm start with a different pinned commit", () => {
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host: createHost(),
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
    });
    coordinator.startWarmReplay(replayStart({ deliveryGeneration: "4" }));

    coordinator.startWarmReplay(replayStart({ deliveryGeneration: "4", commitEventSeq: "12" }));

    expect(coordinator.state).toBe("resyncing");
    expect(resync).toHaveBeenCalledWith("journal-gap");
  });

  it("rejects a warm-to-snapshot mode conflict in the same generation", async () => {
    const snapshots = { load: vi.fn() };
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host: createHost(),
      initialCursor: cursor(),
      snapshots,
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
    });
    coordinator.startWarmReplay(replayStart({ deliveryGeneration: "4" }));

    await coordinator.startSnapshot(manifest({ deliveryGeneration: "4" }));

    expect(snapshots.load).not.toHaveBeenCalled();
    expect(coordinator.state).toBe("resyncing");
    expect(resync).toHaveBeenCalledWith("journal-gap");
  });

  it("treats only an identical in-progress snapshot start as idempotent", async () => {
    let resolveSnapshot: ((value: Uint8Array) => void) | undefined;
    let restoreSignal: AbortSignal | undefined;
    const load = vi.fn<SnapshotTransport["load"]>((_manifest, signal) => {
      restoreSignal = signal;
      return new Promise<Uint8Array>((resolve) => {
        resolveSnapshot = resolve;
      });
    });
    const snapshots: SnapshotTransport = {
      load,
    };
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host: createHost(),
      snapshots,
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
    });
    const currentManifest = manifest();

    const restoring = coordinator.startSnapshot(currentManifest);
    await coordinator.startSnapshot({ ...currentManifest });

    expect(load).toHaveBeenCalledTimes(1);
    expect(restoreSignal?.aborted).toBe(false);
    expect(resync).not.toHaveBeenCalled();

    await coordinator.startSnapshot({
      ...currentManifest,
      snapshotId: "snapshot_0000000002",
      downloadPath: "/api/v1/sessions/session_000000000001/snapshots/snapshot_0000000002",
    });

    expect(restoreSignal?.aborted).toBe(true);
    expect(coordinator.state).toBe("resyncing");
    expect(resync).toHaveBeenCalledWith("journal-gap");

    resolveSnapshot?.(new Uint8Array([1, 2, 3, 4]));
    await restoring;
  });

  it("owns buffered tail bytes instead of borrowing the transport buffer", async () => {
    let resolveSnapshot: ((value: Uint8Array) => void) | undefined;
    const host = createHost();
    const coordinator = new SessionCoordinator({
      host,
      snapshots: {
        load: () =>
          new Promise((resolve) => {
            resolveSnapshot = resolve;
          }),
      },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: vi.fn(),
    });

    const restoring = coordinator.startSnapshot(manifest());
    const encoded = frame();
    coordinator.acceptData(encoded);
    coordinator.acceptData(commit());
    encoded[DATA_HEADER_BYTES] = 0x78;

    resolveSnapshot?.(new Uint8Array([1, 2, 3, 4]));
    await restoring;

    expect(host.restored[0]?.writes).toEqual([new Uint8Array([0x6d])]);
  });

  it("aborts a restore when buffered tail exceeds its budget", async () => {
    let signal: AbortSignal | undefined;
    const snapshots: SnapshotTransport = {
      load: (_manifest, nextSignal) => {
        signal = nextSignal;
        return new Promise(() => undefined);
      },
    };
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host: createHost(),
      snapshots,
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
      maxBufferedTailBytes: 48,
    });

    void coordinator.startSnapshot(manifest());
    coordinator.acceptData(frame());

    expect(signal?.aborted).toBe(true);
    expect(resync).toHaveBeenCalledWith("slow-client");
    expect(coordinator.state).toBe("resyncing");
  });

  it("rejects a snapshot from a different engine before downloading", async () => {
    const snapshots = { load: vi.fn() };
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host: createHost(),
      snapshots,
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
    });

    await coordinator.startSnapshot(manifest({ engineId: "eng1-other" }));

    expect(snapshots.load).not.toHaveBeenCalled();
    expect(resync).toHaveBeenCalledWith("engine-mismatch");
  });

  it("rejects a replay commit that differs from the snapshot watermark", async () => {
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host: createHost(),
      snapshots: { load: async () => new Uint8Array([1, 2, 3, 4]) },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
    });

    await coordinator.startSnapshot(manifest({ commitEventSeq: "12", commitPtyOffset: "101" }));
    coordinator.acceptData(frame());
    coordinator.acceptData(commit());

    expect(resync).toHaveBeenCalledWith("journal-gap");
    expect(coordinator.state).toBe("resyncing");
  });

  it("rejects a snapshot manifest whose commit precedes its cut", async () => {
    const snapshots = { load: vi.fn() };
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host: createHost(),
      snapshots,
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
    });

    await coordinator.startSnapshot(
      manifest({
        cutEventSeq: "11",
        nextPtyOffset: "101",
        commitEventSeq: "10",
        commitPtyOffset: "100",
      }),
    );

    expect(snapshots.load).not.toHaveBeenCalled();
    expect(resync).toHaveBeenCalledWith("journal-gap");
    expect(coordinator.state).toBe("resyncing");
  });

  it("ignores stale frames from a previous delivery generation", () => {
    const host = createHost();
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host,
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
    });
    coordinator.startWarmReplay(replayStart());

    coordinator.acceptData(frame({ deliveryGeneration: 2n }));

    expect((host.active as FakeReplica).writes).toHaveLength(0);
    expect(resync).not.toHaveBeenCalled();
  });

  it("reports an epoch transition separately from a journal gap", () => {
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host: createHost(),
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
    });
    coordinator.startWarmReplay(replayStart());

    coordinator.acceptData(frame({ sessionEpoch: 8n }));

    expect(resync).toHaveBeenCalledWith("epoch-changed");
    expect(coordinator.state).toBe("resyncing");
    expect(coordinator.activeCursor).toEqual(cursor());
  });

  it("invalidates a warm cursor when applying live output has an unknown commit state", () => {
    const active = new FakeReplica();
    active.writePty = () => {
      throw new Error("replica mutation failed");
    };
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host: createHost(active),
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
    });
    coordinator.startWarmReplay(replayStart());

    coordinator.acceptData(frame());

    expect(coordinator.activeCursor).toBeNull();
    expect(resync).toHaveBeenCalledWith("restore-failed");
  });

  it("commits the active cursor before an acknowledgement callback can fail", () => {
    const active = new FakeReplica();
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host: createHost(active),
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge: () => {
        throw new Error("control socket closed");
      },
      onReplicaProgress: vi.fn(),
      onResync: resync,
    });
    coordinator.startWarmReplay(replayStart());

    coordinator.acceptData(frame());

    expect(active.writes).toEqual([new Uint8Array([0x6d])]);
    expect(coordinator.activeCursor).toEqual({
      sessionEpoch: 7n,
      deliveryGeneration: 3n,
      lastEventSeq: 11n,
      nextPtyOffset: 101n,
    });
    expect(resync).toHaveBeenCalledWith("restore-failed");
  });

  it("refuses warm replay when its base does not match the active replica", () => {
    const active = new FakeReplica();
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host: createHost(active),
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
    });

    coordinator.startWarmReplay(
      replayStart({
        deliveryGeneration: "4",
        baseEventSeq: "11",
        commitEventSeq: "12",
      }),
    );

    expect(coordinator.state).toBe("resyncing");
    expect(active.writes).toHaveLength(0);
    expect(resync).toHaveBeenCalledWith("journal-gap");
  });

  it("bounds a restoring tail by frame count as well as encoded bytes", () => {
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host: createHost(),
      snapshots: { load: () => new Promise(() => undefined) },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
      maxBufferedTailBytes: 1_024,
      maxBufferedTailFrames: 1,
    });

    void coordinator.startSnapshot(manifest({ commitEventSeq: "12", commitPtyOffset: "102" }));
    coordinator.acceptData(frame());
    coordinator.acceptData(frame({ eventSeq: 12n, ptyOffset: 101n }));

    expect(coordinator.state).toBe("resyncing");
    expect(resync).toHaveBeenCalledWith("slow-client");
  });

  it("requests a new baseline when the restore deadline expires", async () => {
    vi.useFakeTimers();
    const resync = vi.fn();
    const coordinator = new SessionCoordinator({
      host: createHost(),
      snapshots: { load: () => new Promise(() => undefined) },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
      restoreDeadlineMs: 5_000,
    });

    void coordinator.startSnapshot(manifest());
    await vi.advanceTimersByTimeAsync(5_000);

    expect(resync).toHaveBeenCalledWith("slow-client");
    vi.useRealTimers();
  });

  it("records one ordered cold-recovery attempt after early tail adoption becomes live", async () => {
    let resolveSnapshot: ((value: Uint8Array) => void) | undefined;
    const host = createHost();
    const observed = createTelemetryRecorder();
    const coordinator = new SessionCoordinator({
      host,
      snapshots: {
        load: () =>
          new Promise((resolve) => {
            resolveSnapshot = resolve;
          }),
      },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: vi.fn(),
      telemetry: observed.telemetry,
      monotonicNow: observed.monotonicNow,
    });

    const restoring = coordinator.startSnapshot(manifest());
    coordinator.acceptData(frame());
    coordinator.acceptData(commit());
    expect(observed.events).toHaveLength(0);

    resolveSnapshot?.(new Uint8Array([1, 2, 3, 4]));
    await restoring;

    expect(coordinator.state).toBe("live");
    expect(host.adopted).toEqual(host.restored);
    expect(observed.events.map((event) => event.name)).toEqual([
      "browser.snapshot.load-total",
      "browser.snapshot.restore",
      "browser.snapshot.buffer-flush",
      "browser.snapshot.adopt",
      "browser.recovery.outcome",
    ]);
    expect(observed.events).toEqual([
      {
        schemaVersion: 1,
        monotonicAtMs: 3,
        clockKind: "browser-performance",
        name: "browser.snapshot.load-total",
        outcome: "ready",
        durationMs: 1,
        snapshotBytesBucket: "1-8",
      },
      {
        schemaVersion: 1,
        monotonicAtMs: 5,
        clockKind: "browser-performance",
        name: "browser.snapshot.restore",
        outcome: "ready",
        durationMs: 1,
        snapshotBytesBucket: "1-8",
      },
      {
        schemaVersion: 1,
        monotonicAtMs: 7,
        clockKind: "browser-performance",
        name: "browser.snapshot.buffer-flush",
        outcome: "applied",
        durationMs: 1,
        bufferedFrames: 1,
        bufferedBytesBucket: "9-64",
      },
      {
        schemaVersion: 1,
        monotonicAtMs: 9,
        clockKind: "browser-performance",
        name: "browser.snapshot.adopt",
        outcome: "call-returned",
        durationMs: 1,
      },
      {
        schemaVersion: 1,
        monotonicAtMs: 10,
        clockKind: "browser-performance",
        name: "browser.recovery.outcome",
        mode: "snapshot",
        startingReplica: "empty",
        outcome: "live",
        reason: "none",
        totalDurationMs: 9,
      },
    ]);
  });

  it("finalizes a superseded load once even when the transport resolves late", async () => {
    let resolveSnapshot: ((value: Uint8Array) => void) | undefined;
    let loadSignal: AbortSignal | undefined;
    const restore = vi.spyOn(createHost(), "restore");
    const host = createHost();
    host.restore = restore;
    const observed = createTelemetryRecorder();
    const coordinator = new SessionCoordinator({
      host,
      snapshots: {
        load: (_manifest, signal) => {
          loadSignal = signal;
          return new Promise((resolve) => {
            resolveSnapshot = resolve;
          });
        },
      },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: vi.fn(),
      telemetry: observed.telemetry,
      monotonicNow: observed.monotonicNow,
    });

    const stale = coordinator.startSnapshot(manifest());
    coordinator.fenceDeliveryGeneration(4n);

    expect(loadSignal?.aborted).toBe(true);
    expect(observed.events.map((event) => event.name)).toEqual([
      "browser.snapshot.load-total",
      "browser.recovery.outcome",
    ]);
    expect(observed.events[0]).toMatchObject({ outcome: "cancelled" });
    expect(observed.events[1]).toMatchObject({
      outcome: "superseded",
      reason: "generation-fenced",
    });

    resolveSnapshot?.(new Uint8Array([1, 2, 3, 4]));
    await stale;

    expect(restore).not.toHaveBeenCalled();
    expect(observed.events).toHaveLength(2);
  });

  it("finalizes a superseded restore once and disposes its late candidate", async () => {
    let resolveReplica: ((replica: ReplicaSink) => void) | undefined;
    const candidate = new FakeReplica();
    const dispose = vi.spyOn(candidate, "dispose");
    const host = createHost();
    host.restore = () =>
      new Promise((resolve) => {
        resolveReplica = resolve;
      });
    const observed = createTelemetryRecorder();
    const coordinator = new SessionCoordinator({
      host,
      snapshots: { load: async () => new Uint8Array([1, 2, 3, 4]) },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: vi.fn(),
      telemetry: observed.telemetry,
      monotonicNow: observed.monotonicNow,
    });

    const stale = coordinator.startSnapshot(manifest());
    await vi.waitFor(() => expect(resolveReplica).toBeTypeOf("function"));
    coordinator.fenceDeliveryGeneration(4n);

    expect(observed.events.map((event) => event.name)).toEqual([
      "browser.snapshot.load-total",
      "browser.snapshot.restore",
      "browser.recovery.outcome",
    ]);
    expect(observed.events[1]).toMatchObject({ outcome: "cancelled" });

    resolveReplica?.(candidate);
    await stale;

    expect(dispose).toHaveBeenCalledTimes(1);
    expect(observed.events).toHaveLength(3);
  });

  it("records a failed buffered-tail flush before resync and never adopts", async () => {
    const candidate = new FakeReplica();
    candidate.writePty = () => {
      throw new Error("secret tail must not enter telemetry");
    };
    const host = createHost();
    host.restore = async () => candidate;
    const resync = vi.fn();
    const observed = createTelemetryRecorder();
    const coordinator = new SessionCoordinator({
      host,
      snapshots: { load: async () => new Uint8Array([1, 2, 3, 4]) },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: resync,
      telemetry: observed.telemetry,
      monotonicNow: observed.monotonicNow,
    });

    const restoring = coordinator.startSnapshot(manifest({ commitPtyOffset: "111" }));
    coordinator.acceptData(frame({ payload: new TextEncoder().encode("secret-tail") }));
    coordinator.acceptData(commit(11n, 111n));
    await restoring;

    expect(coordinator.state).toBe("resyncing");
    expect(resync).toHaveBeenCalledWith("restore-failed");
    expect(host.adopted).toHaveLength(0);
    expect(candidate.disposed).toBe(true);
    expect(observed.events.at(-2)).toMatchObject({
      name: "browser.snapshot.buffer-flush",
      outcome: "failed",
    });
    expect(observed.events.at(-1)).toMatchObject({
      name: "browser.recovery.outcome",
      outcome: "resync",
      reason: "restore-failed",
    });
    expect(JSON.stringify(observed.events)).not.toContain("secret");
  });

  it("distinguishes an adopt throw from a completed adoption", async () => {
    const host = createHost();
    host.adopt = () => {
      throw new Error("adopt failed");
    };
    const observed = createTelemetryRecorder();
    const coordinator = new SessionCoordinator({
      host,
      snapshots: { load: async () => new Uint8Array([1, 2, 3, 4]) },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: vi.fn(),
      telemetry: observed.telemetry,
      monotonicNow: observed.monotonicNow,
    });

    const restoring = coordinator.startSnapshot(manifest());
    coordinator.acceptData(frame());
    coordinator.acceptData(commit());
    await restoring;

    expect(coordinator.state).toBe("resyncing");
    expect(observed.events.at(-2)).toMatchObject({
      name: "browser.snapshot.adopt",
      outcome: "threw",
    });
    expect(observed.events.at(-1)).toMatchObject({
      name: "browser.recovery.outcome",
      outcome: "resync",
      reason: "restore-failed",
    });
  });

  it("does not report live when post-adoption replica progress fails", async () => {
    const host = createHost();
    const observed = createTelemetryRecorder();
    const coordinator = new SessionCoordinator({
      host,
      snapshots: { load: async () => new Uint8Array([1, 2, 3, 4]) },
      onAcknowledge: vi.fn(),
      onReplicaProgress: () => {
        throw new Error("progress consumer failed");
      },
      onResync: vi.fn(),
      telemetry: observed.telemetry,
      monotonicNow: observed.monotonicNow,
    });

    const restoring = coordinator.startSnapshot(manifest());
    coordinator.acceptData(frame());
    coordinator.acceptData(commit());
    await restoring;

    expect(host.adopted).toEqual(host.restored);
    expect(coordinator.state).toBe("resyncing");
    expect(observed.events.at(-2)).toMatchObject({
      name: "browser.snapshot.adopt",
      outcome: "call-returned",
    });
    expect(observed.events.at(-1)).toMatchObject({
      name: "browser.recovery.outcome",
      outcome: "resync",
      reason: "restore-failed",
    });
  });

  it("classifies restore deadline, tail capacity, and close terminal paths once", async () => {
    vi.useFakeTimers();
    try {
      const deadlineObserved = createTelemetryRecorder();
      const deadline = new SessionCoordinator({
        host: createHost(),
        snapshots: { load: () => new Promise(() => undefined) },
        onAcknowledge: vi.fn(),
        onReplicaProgress: vi.fn(),
        onResync: vi.fn(),
        telemetry: deadlineObserved.telemetry,
        monotonicNow: deadlineObserved.monotonicNow,
        restoreDeadlineMs: 5_000,
      });
      void deadline.startSnapshot(manifest());
      await vi.advanceTimersByTimeAsync(5_000);
      expect(deadlineObserved.events).toHaveLength(2);
      expect(deadlineObserved.events[0]).toMatchObject({ outcome: "timeout" });
      expect(deadlineObserved.events[1]).toMatchObject({
        outcome: "resync",
        reason: "slow-client",
      });

      const capacityObserved = createTelemetryRecorder();
      const capacity = new SessionCoordinator({
        host: createHost(),
        snapshots: { load: () => new Promise(() => undefined) },
        onAcknowledge: vi.fn(),
        onReplicaProgress: vi.fn(),
        onResync: vi.fn(),
        telemetry: capacityObserved.telemetry,
        monotonicNow: capacityObserved.monotonicNow,
        maxBufferedTailBytes: DATA_HEADER_BYTES,
      });
      void capacity.startSnapshot(manifest());
      capacity.acceptData(frame());
      expect(capacityObserved.events).toHaveLength(2);
      expect(capacityObserved.events[0]).toMatchObject({ outcome: "cancelled" });
      expect(capacityObserved.events[1]).toMatchObject({
        outcome: "resync",
        reason: "slow-client",
      });

      const closeObserved = createTelemetryRecorder();
      const closed = new SessionCoordinator({
        host: createHost(),
        snapshots: { load: () => new Promise(() => undefined) },
        onAcknowledge: vi.fn(),
        onReplicaProgress: vi.fn(),
        onResync: vi.fn(),
        telemetry: closeObserved.telemetry,
        monotonicNow: closeObserved.monotonicNow,
      });
      void closed.startSnapshot(manifest());
      closed.close();
      closed.close();
      expect(closeObserved.events).toHaveLength(2);
      expect(closeObserved.events[0]).toMatchObject({ outcome: "cancelled" });
      expect(closeObserved.events[1]).toMatchObject({
        outcome: "closed",
        reason: "session-closed",
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it("does not create an attempt for a duplicate start and conflicts the existing attempt once", async () => {
    let resolveSnapshot: ((value: Uint8Array) => void) | undefined;
    const load = vi.fn(
      () =>
        new Promise<Uint8Array>((resolve) => {
          resolveSnapshot = resolve;
        }),
    );
    const observed = createTelemetryRecorder();
    const coordinator = new SessionCoordinator({
      host: createHost(),
      snapshots: { load },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: vi.fn(),
      telemetry: observed.telemetry,
      monotonicNow: observed.monotonicNow,
    });
    const currentManifest = manifest();

    const restoring = coordinator.startSnapshot(currentManifest);
    await coordinator.startSnapshot({ ...currentManifest });
    expect(load).toHaveBeenCalledOnce();
    expect(observed.events).toHaveLength(0);

    await coordinator.startSnapshot({
      ...currentManifest,
      snapshotId: "snapshot_0000000002",
      downloadPath: "/api/v1/sessions/session_000000000001/snapshots/snapshot_0000000002",
    });
    expect(observed.events).toHaveLength(2);
    expect(observed.events[1]).toMatchObject({
      outcome: "resync",
      reason: "journal-gap",
    });

    resolveSnapshot?.(new Uint8Array([1, 2, 3, 4]));
    await restoring;
    expect(observed.events).toHaveLength(2);
  });

  it("records warm recovery only as an overall outcome", () => {
    const liveObserved = createTelemetryRecorder();
    const live = new SessionCoordinator({
      host: createHost(),
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: vi.fn(),
      telemetry: liveObserved.telemetry,
      monotonicNow: liveObserved.monotonicNow,
    });
    live.startWarmReplay(replayStart());
    live.acceptData(frame());
    live.acceptData(commit());

    expect(liveObserved.events).toEqual([
      {
        schemaVersion: 1,
        monotonicAtMs: 2,
        clockKind: "browser-performance",
        name: "browser.recovery.outcome",
        mode: "warm",
        startingReplica: "live",
        outcome: "live",
        reason: "none",
        totalDurationMs: 1,
      },
    ]);

    const gapObserved = createTelemetryRecorder();
    const gap = new SessionCoordinator({
      host: createHost(),
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: vi.fn(),
      telemetry: gapObserved.telemetry,
      monotonicNow: gapObserved.monotonicNow,
    });
    gap.startWarmReplay(replayStart({ baseEventSeq: "11", commitEventSeq: "12" }));
    expect(gapObserved.events).toHaveLength(1);
    expect(gapObserved.events[0]).toMatchObject({
      name: "browser.recovery.outcome",
      mode: "warm",
      outcome: "resync",
      reason: "journal-gap",
    });
  });

  it("keeps telemetry clocks and sink failures observational", () => {
    const disabledClock = vi.fn(() => 1);
    const disabledHost = createHost();
    const disabled = new SessionCoordinator({
      host: disabledHost,
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: vi.fn(),
      monotonicNow: disabledClock,
    });
    disabled.startWarmReplay(replayStart());
    disabled.acceptData(frame());
    disabled.acceptData(commit());
    expect(disabledClock).not.toHaveBeenCalled();

    let throwingSinkState: DeliveryState | undefined;
    let throwingSinkCoordinator!: SessionCoordinator;
    const throwingSinkHost = createHost();
    throwingSinkCoordinator = new SessionCoordinator({
      host: throwingSinkHost,
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: vi.fn(),
      monotonicNow: () => 1,
      telemetry: () => {
        throwingSinkState = throwingSinkCoordinator.state;
        throw new Error("collector unavailable");
      },
    });
    throwingSinkCoordinator.startWarmReplay(replayStart());
    throwingSinkCoordinator.acceptData(frame());
    throwingSinkCoordinator.acceptData(commit());

    const clockFailureSink = vi.fn<BrowserTelemetrySink>();
    const clockFailureHost = createHost();
    const clockFailure = new SessionCoordinator({
      host: clockFailureHost,
      initialCursor: cursor(),
      snapshots: { load: vi.fn() },
      onAcknowledge: vi.fn(),
      onReplicaProgress: vi.fn(),
      onResync: vi.fn(),
      telemetry: clockFailureSink,
      monotonicNow: () => {
        throw new Error("clock unavailable");
      },
    });
    clockFailure.startWarmReplay(replayStart());
    clockFailure.acceptData(frame());
    clockFailure.acceptData(commit());

    for (const coordinator of [disabled, throwingSinkCoordinator, clockFailure]) {
      expect(coordinator.state).toBe("live");
      expect(coordinator.activeCursor).toEqual(cursor({ lastEventSeq: 11n, nextPtyOffset: 101n }));
    }
    expect((disabledHost.active as FakeReplica).writes).toEqual([new Uint8Array([0x6d])]);
    expect((throwingSinkHost.active as FakeReplica).writes).toEqual([new Uint8Array([0x6d])]);
    expect((clockFailureHost.active as FakeReplica).writes).toEqual([new Uint8Array([0x6d])]);
    expect(throwingSinkState).toBe("live");
    expect(clockFailureSink).not.toHaveBeenCalled();
  });
});
