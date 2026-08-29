import {
  DataFrameKind,
  encodeDataFrame,
  encodeDeliveryEnvelopeV3,
  type AuthorityCursor,
  type DeliveryLane,
  type RecoverySourceClosed,
  type RecoveryStart,
  type RecoveryV3ClientControlFrame,
  type ReplicaCursor,
} from "@zhongduan/protocol";
import { describe, expect, it, vi } from "vitest";

import { RecoveryRuntime, type RecoveryRuntimeOptions } from "./recovery-runtime";
import type { ReplicaHost, ReplicaSink, SnapshotManifest, SnapshotTransport } from "./types";

const ENGINE_ID = "ghostty:recovery-runtime";
const RECOVERY_ID = "recovery_runtime_0001";
const GENERATION = 3n;
const STREAM_ID = 7;
const BASE: AuthorityCursor = { sessionEpoch: "1", eventSeq: "10", nextPtyOffset: "20" };
const COMMITTED: AuthorityCursor = {
  sessionEpoch: "1",
  eventSeq: "11",
  nextPtyOffset: "21",
};
const INITIAL_CURSOR: ReplicaCursor = {
  sessionEpoch: 1n,
  deliveryGeneration: 2n,
  lastEventSeq: 10n,
  nextPtyOffset: 20n,
};

class FakeReplica implements ReplicaSink {
  readonly writes: number[][] = [];
  disposeCalls = 0;
  throwAfterByte: number | null = null;

  constructor(readonly engineId = ENGINE_ID) {}

  writePty(data: Uint8Array): void {
    this.writes.push([...data]);
    if (data[0] === this.throwAfterByte) throw new Error("effect happened before sink failure");
  }

  resize(): void {}

  dispose(): void {
    this.disposeCalls += 1;
  }
}

class FakeHost implements ReplicaHost {
  readonly engineId = ENGINE_ID;
  readonly restore = vi.fn<ReplicaHost["restore"]>();
  readonly adopt = vi.fn<ReplicaHost["adopt"]>((replica) => {
    this.current = replica;
  });

  constructor(public current: ReplicaSink | null) {}

  get active(): ReplicaSink | null {
    return this.current;
  }
}

function start(source: "warm" | "snapshot" = "warm"): RecoveryStart {
  return {
    type: "recovery-start",
    recoveryId: RECOVERY_ID,
    deliveryGeneration: GENERATION.toString(),
    streamId: STREAM_ID,
    engineId: ENGINE_ID,
    authorityDataVersion: 2,
    base: { ...BASE },
    source:
      source === "warm"
        ? { kind: "warm" }
        : {
            kind: "snapshot",
            sessionId: "session_runtime_0001",
            snapshotId: "snapshot_runtime_001",
            engineId: ENGINE_ID,
            sessionEpoch: BASE.sessionEpoch,
            cutEventSeq: BASE.eventSeq,
            nextPtyOffset: BASE.nextPtyOffset,
            compression: "none",
            compressedLength: "1",
            uncompressedLength: "1",
            sha256: "a".repeat(64),
            downloadPath: "/api/v1/sessions/session_runtime_0001/snapshots/snapshot_runtime_001",
            restoreThrough: "finish",
          },
    committedThrough: { ...COMMITTED },
    liveFloor: { sessionEpoch: "1", nextEventSeq: "12", nextPtyOffset: "21" },
  };
}

function canonicalMutation(eventSeq: bigint, ptyOffset: bigint, payload: number[]): Uint8Array {
  return encodeDataFrame({
    kind: DataFrameKind.PtyOutput,
    flags: 0,
    sessionEpoch: 1n,
    deliveryGeneration: 0n,
    eventSeq,
    ptyOffset,
    streamId: 0,
    payload: Uint8Array.from(payload),
  });
}

function canonicalDone(eventSeq = 11n, ptyOffset = 21n): Uint8Array {
  return encodeDataFrame({
    kind: DataFrameKind.ReplayCommit,
    flags: 0,
    sessionEpoch: 1n,
    deliveryGeneration: 0n,
    eventSeq,
    ptyOffset,
    streamId: 0,
    payload: new Uint8Array(),
  });
}

interface RecordFixture {
  readonly bytes: bigint;
  readonly ordinal: bigint;
  readonly raw: Uint8Array;
}

function record(
  lane: DeliveryLane,
  ordinal: bigint,
  previousBytes: bigint,
  payload: Uint8Array,
  generation = GENERATION,
): RecordFixture {
  const bytes = previousBytes + 40n + BigInt(payload.byteLength);
  return {
    bytes,
    ordinal,
    raw: encodeDeliveryEnvelopeV3({
      lane,
      deliveryGeneration: generation,
      deliveryOrdinal: ordinal,
      cumulativeEncodedBytes: bytes,
      streamId: STREAM_ID,
      payload,
    }),
  };
}

function fixture() {
  const recovery = record("recovery", 1n, 0n, canonicalMutation(11n, 20n, [65]));
  const done = record("recovery", 2n, recovery.bytes, canonicalDone());
  const live = record("live", 1n, 0n, canonicalMutation(12n, 21n, [66]));
  const nextLive = record("live", 2n, live.bytes, canonicalMutation(13n, 22n, [67]));
  return { recovery, done, live, nextLive };
}

function sourceClosed(done: RecordFixture): RecoverySourceClosed {
  return {
    type: "recovery-source-closed",
    recoveryId: RECOVERY_ID,
    deliveryGeneration: GENERATION.toString(),
    throughRecoveryOrdinal: done.ordinal.toString(),
    throughRecoveryCumulativeEncodedBytes: done.bytes.toString(),
  };
}

function options(
  host: ReplicaHost,
  progress: RecoveryV3ClientControlFrame[],
  overrides: Partial<RecoveryRuntimeOptions> & { cold?: boolean } = {},
): RecoveryRuntimeOptions {
  const { cold = false, ...runtimeOverrides } = overrides;
  return {
    deliveryGeneration: GENERATION,
    engineId: ENGINE_ID,
    host,
    ...(cold ? {} : { initialCursor: INITIAL_CURSOR }),
    limits: {
      maxApplyFramesPerCall: 8,
      maxGapSpan: 16n,
      maxOwnedBytes: 64 * 1024,
      maxOwnedFrames: 32,
      noProgressDeadlineMs: 1_000,
      recoveryDeadlineMs: 10_000,
    },
    snapshots: { load: vi.fn() },
    streamId: STREAM_ID,
    now: () => 0,
    onFailure: vi.fn(),
    onProgress: (frame) => {
      progress.push(frame);
      return true;
    },
    setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
    clearTimer: vi.fn(),
    ...runtimeOverrides,
  };
}

describe("RecoveryRuntime", () => {
  it("bounds pre-start data, publishes distinct progress, and atomically hands warm ownership to live", () => {
    const active = new FakeReplica();
    const host = new FakeHost(active);
    const progress: RecoveryV3ClientControlFrame[] = [];
    const runtime = new RecoveryRuntime(options(host, progress));
    const records = fixture();

    expect(runtime.acceptEnvelope(records.live.raw)).toBe(true);
    expect(progress).toEqual([]);
    expect(runtime.acceptEnvelope(records.recovery.raw)).toBe(true);
    expect(runtime.acceptEnvelope(records.done.raw)).toBe(true);
    expect(runtime.acceptStart(start())).toBe(true);

    expect(active.writes).toEqual([[65], [66]]);
    expect(host.adopt).not.toHaveBeenCalled();
    expect(runtime.state).toBe("assembling");
    expect(
      progress.some((frame) => frame.type === "delivery-received" && frame.lane === "live"),
    ).toBe(true);
    expect(
      progress.some((frame) => frame.type === "delivery-received" && frame.lane === "recovery"),
    ).toBe(true);
    expect(progress.some((frame) => frame.type === "replica-applied")).toBe(true);
    expect(progress.some((frame) => frame.type === "recovery-adopted")).toBe(true);

    expect(runtime.acceptSourceClosed(sourceClosed(records.done))).toBe(true);
    expect(runtime.state).toBe("live");
    expect(runtime.acceptEnvelope(records.nextLive.raw)).toBe(true);
    expect(active.writes).toEqual([[65], [66], [67]]);
    expect(runtime.activeCursor).toMatchObject({
      deliveryGeneration: GENERATION,
      lastEventSeq: 13n,
      nextPtyOffset: 23n,
    });
  });

  it("restores the exact RecoveryStart snapshot and adopts the detached cold target once", async () => {
    const oldVisible = new FakeReplica();
    const candidate = new FakeReplica();
    const host = new FakeHost(oldVisible);
    host.restore.mockResolvedValue(candidate);
    const load = vi.fn<SnapshotTransport["load"]>().mockResolvedValue(Uint8Array.of(9));
    const progress: RecoveryV3ClientControlFrame[] = [];
    const runtime = new RecoveryRuntime(
      options(host, progress, { cold: true, snapshots: { load } }),
    );
    const records = fixture();

    expect(runtime.acceptStart(start("snapshot"))).toBe(true);
    expect(runtime.acceptEnvelope(records.live.raw)).toBe(true);
    expect(runtime.acceptEnvelope(records.recovery.raw)).toBe(true);
    expect(runtime.acceptEnvelope(records.done.raw)).toBe(true);
    expect(runtime.acceptSourceClosed(sourceClosed(records.done))).toBe(true);

    await vi.waitFor(() => expect(runtime.state).toBe("live"));
    const manifest = load.mock.calls[0]![0] as SnapshotManifest;
    expect(manifest).toMatchObject({
      snapshotId: "snapshot_runtime_001",
      deliveryGeneration: "3",
      cutEventSeq: "10",
      commitEventSeq: "11",
      restoreThrough: "finish",
    });
    expect(host.restore).toHaveBeenCalledWith(Uint8Array.of(9), manifest, expect.any(AbortSignal));
    expect(host.adopt).toHaveBeenCalledOnce();
    expect(host.active).toBe(candidate);
    expect(candidate.writes).toEqual([[65], [66]]);
    expect(oldVisible.disposeCalls).toBe(0);
    expect(candidate.disposeCalls).toBe(0);
  });

  it("fails closed on a late-generation envelope without mutating the warm target", () => {
    const active = new FakeReplica();
    const onFailure = vi.fn();
    const runtime = new RecoveryRuntime(options(new FakeHost(active), [], { onFailure }));
    const late = record("live", 1n, 0n, canonicalMutation(12n, 21n, [88]), GENERATION - 1n);

    expect(runtime.acceptEnvelope(late.raw)).toBe(false);
    expect(runtime.state).toBe("failed");
    expect(runtime.failure).toBe("protocol-conflict");
    expect(onFailure).toHaveBeenCalledWith("protocol-conflict");
    expect(active.writes).toEqual([]);
    expect(active.disposeCalls).toBe(0);
    expect(runtime.activeCursor).toEqual(INITIAL_CURSOR);
  });

  it("keeps the exact warm cursor reusable after a post-start protocol conflict before effect", () => {
    const active = new FakeReplica();
    const host = new FakeHost(active);
    const runtime = new RecoveryRuntime(options(host, []));
    const late = record("live", 1n, 0n, canonicalMutation(12n, 21n, [88]), GENERATION - 1n);

    expect(runtime.acceptStart(start())).toBe(true);
    expect(runtime.acceptEnvelope(late.raw)).toBe(false);
    expect(runtime.state).toBe("failed");
    expect(runtime.failure).toBe("protocol-conflict");
    expect(active.writes).toEqual([]);
    expect(runtime.activeCursor).toEqual({
      ...INITIAL_CURSOR,
      deliveryGeneration: GENERATION,
    });
  });

  it("withholds the cursor when a no-op warm handoff finds a different visible owner", () => {
    const original = new FakeReplica();
    const host = new FakeHost(original);
    const runtime = new RecoveryRuntime(options(host, []));
    const noOpStart: RecoveryStart = {
      ...start(),
      committedThrough: { ...BASE },
      liveFloor: { sessionEpoch: "1", nextEventSeq: "11", nextPtyOffset: "20" },
    };
    const done = record("recovery", 1n, 0n, canonicalDone(10n, 20n));

    expect(runtime.acceptStart(noOpStart)).toBe(true);
    host.current = new FakeReplica();

    expect(runtime.acceptEnvelope(done.raw)).toBe(false);
    expect(runtime.state).toBe("failed");
    expect(runtime.failure).toBe("handoff-conflict");
    expect(original.writes).toEqual([]);
    expect(runtime.activeCursor).toBeNull();
  });

  it("permanently withholds a warm assembling cursor after the visible owner changes", () => {
    const original = new FakeReplica();
    const host = new FakeHost(original);
    const runtime = new RecoveryRuntime(options(host, []));

    expect(runtime.acceptStart(start())).toBe(true);
    expect(runtime.state).toBe("assembling");
    expect(runtime.activeCursor).toMatchObject({ lastEventSeq: 10n, nextPtyOffset: 20n });

    host.current = new FakeReplica();
    expect(runtime.activeCursor).toBeNull();
    host.current = original;
    expect(runtime.activeCursor).toBeNull();
    runtime.close();
    expect(runtime.activeCursor).toBeNull();
  });

  it("does not dispose a cold candidate when host adoption has an uncertain throw", async () => {
    const candidate = new FakeReplica();
    const host = new FakeHost(new FakeReplica());
    host.restore.mockResolvedValue(candidate);
    host.adopt.mockImplementation(() => {
      host.current = candidate;
      throw new Error("throw after visible replacement");
    });
    const onFailure = vi.fn();
    const runtime = new RecoveryRuntime(
      options(host, [], {
        snapshots: { load: vi.fn(async () => Uint8Array.of(1)) },
        onFailure,
      }),
    );
    const records = fixture();

    runtime.acceptStart(start("snapshot"));
    runtime.acceptEnvelope(records.recovery.raw);
    runtime.acceptEnvelope(records.done.raw);

    await vi.waitFor(() => expect(runtime.state).toBe("failed"));
    expect(runtime.failure).toBe("ownership-uncertain");
    expect(onFailure).toHaveBeenCalledWith("ownership-uncertain");
    expect(host.active).toBe(candidate);
    expect(candidate.disposeCalls).toBe(0);
    expect(runtime.activeCursor).toBeNull();
  });

  it.each(["warm", "snapshot"] as const)(
    "permanently withholds an adopted %s cursor when completion finds a different visible owner",
    async (source) => {
      const original = new FakeReplica();
      const candidate = source === "warm" ? original : new FakeReplica();
      const host = new FakeHost(original);
      host.restore.mockResolvedValue(candidate);
      const runtime = new RecoveryRuntime(
        options(host, [], {
          ...(source === "snapshot" ? { cold: true } : {}),
          snapshots: { load: vi.fn(async () => Uint8Array.of(1)) },
        }),
      );
      const records = fixture();

      expect(runtime.acceptStart(start(source))).toBe(true);
      expect(runtime.acceptEnvelope(records.recovery.raw)).toBe(true);
      expect(runtime.acceptEnvelope(records.done.raw)).toBe(true);
      if (source === "snapshot") {
        await vi.waitFor(() => expect(host.adopt).toHaveBeenCalledOnce());
      }
      expect(runtime.state).toBe("assembling");
      expect(runtime.activeCursor).toMatchObject({ lastEventSeq: 11n, nextPtyOffset: 21n });

      host.current = new FakeReplica();
      expect(runtime.acceptSourceClosed(sourceClosed(records.done))).toBe(false);
      expect(runtime.state).toBe("failed");
      expect(runtime.failure).toBe("handoff-conflict");
      expect(runtime.activeCursor).toBeNull();
    },
  );

  it("withholds the adopted cursor when live receiver construction rejects its visible target", () => {
    const active = new FakeReplica();
    const host = new FakeHost(active);
    const runtime = new RecoveryRuntime(options(host, []));
    const records = fixture();
    runtime.acceptStart(start());
    runtime.acceptEnvelope(records.recovery.raw);
    runtime.acceptEnvelope(records.done.raw);
    expect(runtime.state).toBe("assembling");

    (active as { engineId: string }).engineId = "ghostty:replaced";

    expect(runtime.acceptSourceClosed(sourceClosed(records.done))).toBe(false);
    expect(runtime.state).toBe("failed");
    expect(runtime.failure).toBe("handoff-conflict");
    expect(runtime.activeCursor).toBeNull();
  });

  it("permanently withholds a live cursor after the visible owner changes", () => {
    const active = new FakeReplica();
    const host = new FakeHost(active);
    const runtime = new RecoveryRuntime(options(host, []));
    const records = fixture();
    runtime.acceptStart(start());
    runtime.acceptEnvelope(records.recovery.raw);
    runtime.acceptEnvelope(records.done.raw);
    runtime.acceptSourceClosed(sourceClosed(records.done));
    expect(runtime.state).toBe("live");
    expect(runtime.activeCursor).toMatchObject({ lastEventSeq: 11n, nextPtyOffset: 21n });

    host.current = new FakeReplica();
    expect(runtime.activeCursor).toBeNull();
    host.current = active;
    expect(runtime.activeCursor).toBeNull();
  });

  it.each(["false", "throw"] as const)(
    "keeps a progress callback %s terminal instead of resuming assembly",
    (outcome) => {
      const active = new FakeReplica();
      const onFailure = vi.fn();
      const records = fixture();
      const runtime = new RecoveryRuntime(
        options(new FakeHost(active), [], {
          onFailure,
          onProgress: () => {
            if (outcome === "throw") throw new Error("transport failed");
            return false;
          },
        }),
      );
      runtime.acceptEnvelope(records.recovery.raw);

      expect(runtime.acceptStart(start())).toBe(false);
      expect(runtime.state).toBe("failed");
      expect(runtime.failure).toBe("progress-unavailable");
      expect(onFailure).toHaveBeenCalledOnce();
      expect(active.writes).toEqual([]);
    },
  );

  it("does not overwrite a synchronous close performed by the progress callback", () => {
    const active = new FakeReplica();
    const records = fixture();
    let runtime!: RecoveryRuntime;
    runtime = new RecoveryRuntime(
      options(new FakeHost(active), [], {
        onProgress: () => {
          runtime.close();
        },
      }),
    );
    runtime.acceptEnvelope(records.recovery.raw);

    expect(runtime.acceptStart(start())).toBe(false);
    expect(runtime.state).toBe("closed");
    expect(runtime.failure).toBeNull();
    expect(active.writes).toEqual([]);
  });

  it("withholds the visible cursor after a live sink mutates and then throws", () => {
    const active = new FakeReplica();
    const runtime = new RecoveryRuntime(options(new FakeHost(active), []));
    const records = fixture();
    runtime.acceptStart(start());
    runtime.acceptEnvelope(records.recovery.raw);
    runtime.acceptEnvelope(records.done.raw);
    runtime.acceptEnvelope(records.live.raw);
    runtime.acceptSourceClosed(sourceClosed(records.done));
    expect(runtime.state).toBe("live");
    active.throwAfterByte = 67;

    expect(runtime.acceptEnvelope(records.nextLive.raw)).toBe(false);
    expect(runtime.failure).toBe("apply-outcome-uncertain");
    expect(runtime.activeCursor).toBeNull();
    expect(active.writes.at(-1)).toEqual([67]);
  });

  it("does not dispose an adopted cold target when confirmation reaches its deadline", async () => {
    let now = 0;
    const candidate = new FakeReplica();
    const host = new FakeHost(new FakeReplica());
    host.restore.mockResolvedValue(candidate);
    host.adopt.mockImplementation((replica) => {
      host.current = replica;
      now = 10;
    });
    const runtime = new RecoveryRuntime(
      options(host, [], {
        limits: {
          ...options(host, []).limits,
          noProgressDeadlineMs: 10,
          recoveryDeadlineMs: 10,
        },
        now: () => now,
        snapshots: { load: vi.fn(async () => Uint8Array.of(1)) },
      }),
    );
    const records = fixture();
    runtime.acceptStart(start("snapshot"));
    runtime.acceptEnvelope(records.recovery.raw);
    runtime.acceptEnvelope(records.done.raw);

    await vi.waitFor(() => expect(runtime.state).toBe("failed"));
    expect(runtime.failure).toBe("generation-deadline");
    expect(host.active).toBe(candidate);
    expect(candidate.disposeCalls).toBe(0);
    expect(runtime.activeCursor).toBeNull();
  });

  it("does not extend an already scheduled deadline when the injected clock moves backwards", () => {
    let now = 100;
    let deadlineCallback: (() => void) | undefined;
    const onFailure = vi.fn();
    const runtime = new RecoveryRuntime(
      options(new FakeHost(new FakeReplica()), [], {
        limits: {
          ...options(new FakeHost(new FakeReplica()), []).limits,
          noProgressDeadlineMs: 10,
          recoveryDeadlineMs: 100,
        },
        now: () => now,
        onFailure,
        setTimer: (callback) => {
          deadlineCallback ??= callback;
          return 1 as unknown as ReturnType<typeof setTimeout>;
        },
      }),
    );
    expect(deadlineCallback).toBeDefined();
    now = 90;

    deadlineCallback!();
    expect(runtime.state).toBe("failed");
    expect(runtime.failure).toBe("no-progress-deadline");
    expect(onFailure).toHaveBeenCalledWith("no-progress-deadline");
  });
});
