import {
  DataFrameKind,
  decodeDataFrame,
  decodeDeliveryBarrierPayload,
  type HostControlFrame,
  type RelayToHostControlFrame,
  type ResizePayload,
} from "@zhongduan/protocol";
import { afterEach, describe, expect, it, vi } from "vitest";

import { FakeTerminalAuthority } from "../fake-terminal-authority";
import { EventJournal } from "../journal";
import type { PtyProcess } from "../pty-process";
import { TerminalSession, type SnapshotCapture } from "../session";
import {
  DELIVERY_BARRIER_TIMEOUT_MS,
  HOST_CANONICAL_QUEUE_LIMITS,
  HostDeliveryScheduler,
  SNAPSHOT_ENCODE_BUDGET_MS,
  SNAPSHOT_PUBLISH_TIMEOUT_MS,
  SNAPSHOT_RECOVERY_MAX_QUIET_WAIT_MS,
  SNAPSHOT_RECOVERY_QUIET_MS,
  WARM_REPLAY_MAX_FRAMES,
  type SnapshotPublisherLike,
} from "./delivery-scheduler";
import {
  SnapshotCleanupConfirmedError,
  SnapshotUnavailableError,
  type PublishedSnapshot,
} from "./snapshot-publisher";

const encoder = new TextEncoder();

class ManualPty implements PtyProcess {
  readonly pid = 42;
  readonly writes: Uint8Array[] = [];
  #dataListener: ((data: Uint8Array) => void) | undefined;
  #exitListener: ((exitCode: number, signal: number) => void) | undefined;

  onData(listener: (data: Uint8Array) => void): () => void {
    this.#dataListener = listener;
    return () => {
      if (this.#dataListener === listener) this.#dataListener = undefined;
    };
  }

  onExit(listener: (exitCode: number, signal: number) => void): () => void {
    this.#exitListener = listener;
    return () => {
      if (this.#exitListener === listener) this.#exitListener = undefined;
    };
  }

  write(data: Uint8Array): void {
    this.writes.push(data.slice());
  }

  resize(_dimensions: ResizePayload): void {}

  kill(): void {
    this.#exitListener?.(0, 0);
  }

  emit(data: Uint8Array): void {
    this.#dataListener?.(data);
  }
}

interface Harness {
  closeReasons: string[];
  controls: HostControlFrame[];
  data: Uint8Array[];
  pty: ManualPty;
  scheduler: HostDeliveryScheduler;
  session: TerminalSession;
  setBarrierResponder(responder: (encoded: Uint8Array) => void): void;
}

function createHarness(
  snapshotPublisher: SnapshotPublisherLike = immediatePublisher(),
  authority: FakeTerminalAuthority = new FakeTerminalAuthority(),
  sessionMonotonicNow?: () => number,
  yieldIo: () => Promise<void> = () => Promise.resolve(),
): Harness {
  const pty = new ManualPty();
  const session = new TerminalSession({
    authority,
    journal: new EventJournal({ maxBytes: 32 * 1024 * 1024 }),
    pty,
    sessionEpoch: 1n,
    ...(sessionMonotonicNow === undefined ? {} : { monotonicNow: sessionMonotonicNow }),
  });
  const controls: HostControlFrame[] = [];
  const data: Uint8Array[] = [];
  const closeReasons: string[] = [];
  let barrierResponder: ((encoded: Uint8Array) => void) | undefined;
  const scheduler = new HostDeliveryScheduler({
    closePair: (reason) => closeReasons.push(reason),
    sendControl: (frame) => controls.push(frame),
    sendData: (encoded) => {
      const copy = encoded.slice();
      data.push(copy);
      if (decodeDataFrame(copy).kind === DataFrameKind.DeliveryBarrier) {
        barrierResponder?.(copy);
      }
    },
    session,
    snapshotPublisher,
    yieldIo,
  });
  scheduler.prepareHostReady();
  return {
    closeReasons,
    controls,
    data,
    pty,
    scheduler,
    session,
    setBarrierResponder(responder) {
      barrierResponder = responder;
    },
  };
}

function activate(harness: Harness): void {
  harness.scheduler.activateHostReady({
    sessionEpoch: 1n,
    lastEventSeq: 0n,
    nextPtyOffset: 0n,
  });
}

function attach(
  harness: Harness,
  overrides: Partial<Extract<RelayToHostControlFrame, { type: "attach-request" }>> = {},
): void {
  harness.scheduler.enqueueAttach({
    type: "attach-request",
    connectionId: "connection_AAAAAAAAA",
    streamId: 1,
    deliveryGeneration: "1",
    engineId: harness.session.engineId,
    hasLiveReplica: false,
    ...overrides,
  } as Extract<RelayToHostControlFrame, { type: "attach-request" }>);
}

function liveAttach(
  harness: Harness,
  base: { lastEventSeq: bigint; nextPtyOffset: bigint },
  overrides: Partial<Extract<RelayToHostControlFrame, { type: "attach-request" }>> = {},
): void {
  attach(harness, {
    hasLiveReplica: true,
    lastSessionEpoch: "1",
    lastEventSeq: base.lastEventSeq.toString(),
    nextPtyOffset: base.nextPtyOffset.toString(),
    ...overrides,
  });
}

function answerBarrier(
  harness: Harness,
  encoded: Uint8Array,
  outcome:
    | { status: "ready" }
    | { status: "stale"; reason?: "generation-fenced" | "client-gone" }
    | {
        status: "rejected";
        reason?:
          | "missing-live-seed"
          | "snapshot-missing"
          | "snapshot-metadata-mismatch"
          | "browser-control-send-failed"
          | "cloud-head-behind-cut";
        retryScope?: "same-generation" | "refresh-checkpoint" | "reset-generation" | "drop-client";
      } = { status: "ready" },
): void {
  const frame = decodeDataFrame(encoded);
  const payload = decodeDeliveryBarrierPayload(frame.payload);
  const common = {
    type: "delivery-barrier-result" as const,
    ...outcome,
    connectionId: payload.connectionId,
    streamId: frame.streamId,
    deliveryGeneration: frame.deliveryGeneration.toString(),
    commitEventSeq: frame.eventSeq.toString(),
    commitPtyOffset: frame.ptyOffset.toString(),
  };
  harness.scheduler.handleBarrierResult(
    payload.mode === "warm"
      ? { ...common, mode: "warm" }
      : { ...common, mode: "snapshot", snapshotId: payload.snapshotId },
  );
}

function immediatePublisher(): SnapshotPublisherLike {
  return { publish: async (snapshot) => publishedSnapshot(snapshot) };
}

function publishedSnapshot(snapshot: SnapshotCapture, suffix = "A"): PublishedSnapshot {
  const body = Uint8Array.of(0x28, 0xb5, 0x2f, 0xfd);
  return {
    metadata: {
      sessionId: "session_AAAAAAAAAAAA",
      snapshotId: `snapshot_${suffix.repeat(16)}`,
      engineId: snapshot.engineId,
      sessionEpoch: snapshot.sessionEpoch.toString(),
      cutEventSeq: snapshot.cutEventSeq.toString(),
      nextPtyOffset: snapshot.nextPtyOffset.toString(),
      compression: "zstd",
      compressedLength: body.byteLength.toString(),
      uncompressedLength: snapshot.bytes.byteLength.toString(),
      sha256: "0".repeat(64),
    },
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

async function settle(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise<void>((resolve) => setImmediate(resolve));
  await Promise.resolve();
}

function describeData(encoded: Uint8Array): string {
  const frame = decodeDataFrame(encoded);
  if (frame.kind === DataFrameKind.DeliveryBarrier) return `barrier:${frame.eventSeq}`;
  if (frame.kind === DataFrameKind.ReplayCommit) return `commit:${frame.eventSeq}`;
  const lane = frame.deliveryGeneration === 0n ? "canonical" : "directed";
  return `${lane}:${frame.eventSeq}:${frame.payload[0] ?? "resize"}`;
}

afterEach(() => vi.useRealTimers());

describe("HostDeliveryScheduler", () => {
  it("holds canonical data until the exact host-ready ACK is activated", () => {
    const harness = createHarness();
    harness.pty.emit(Uint8Array.of(0x41));

    expect(harness.data).toEqual([]);
    expect(() =>
      harness.scheduler.activateHostReady({
        sessionEpoch: 1n,
        lastEventSeq: 1n,
        nextPtyOffset: 1n,
      }),
    ).toThrow(/does not match/);
    activate(harness);

    expect(harness.data.map(describeData)).toEqual(["canonical:1:65"]);
  });

  it("fails closed when the canonical ingress queue exceeds 1024 frames", () => {
    const harness = createHarness();
    activate(harness);

    for (let index = 0; index < HOST_CANONICAL_QUEUE_LIMITS.maxFrames + 2; index += 1) {
      harness.pty.emit(Uint8Array.of(index & 0xff));
    }

    expect(harness.closeReasons).toEqual(["canonical publisher queue exceeded"]);
    expect(harness.data.map(describeData)).toEqual(["canonical:1:0"]);
  });

  it("orders canonical C, warm marker, tail, commit, then canonical C+1", async () => {
    const harness = createHarness();
    activate(harness);
    harness.pty.emit(Uint8Array.of(0x41));
    const base = harness.session.cursor;
    harness.pty.emit(Uint8Array.of(0x42));
    harness.setBarrierResponder((encoded) => {
      harness.pty.emit(Uint8Array.of(0x43));
      answerBarrier(harness, encoded);
    });

    liveAttach(harness, base);
    await settle();

    expect(harness.data.map(describeData)).toEqual([
      "canonical:1:65",
      "canonical:2:66",
      "barrier:2",
      "directed:2:66",
      "commit:2",
      "canonical:3:67",
    ]);
    expect(harness.closeReasons).toEqual([]);
  });

  it("aborts a yielded warm flush under pressure without crossing the marker", async () => {
    const flushStarted = deferred<void>();
    const releaseFlush = deferred<void>();
    let yieldCount = 0;
    const harness = createHarness(
      immediatePublisher(),
      new FakeTerminalAuthority(),
      undefined,
      () => {
        yieldCount += 1;
        if (yieldCount !== 1) return Promise.resolve();
        flushStarted.resolve();
        return releaseFlush.promise;
      },
    );
    activate(harness);

    for (let index = 0; index < 65; index += 1) {
      harness.pty.emit(Uint8Array.of(index));
    }
    liveAttach(harness, { lastEventSeq: 0n, nextPtyOffset: 0n });
    await flushStarted.promise;

    for (let index = 0; index < HOST_CANONICAL_QUEUE_LIMITS.maxFrames + 1; index += 1) {
      harness.pty.emit(Uint8Array.of(index & 0xff));
    }
    releaseFlush.resolve();
    await settle();

    const frames = harness.data.map(decodeDataFrame);
    expect(frames).toHaveLength(65 + HOST_CANONICAL_QUEUE_LIMITS.maxFrames + 1);
    expect(
      frames.every(
        (frame) =>
          frame.kind === DataFrameKind.PtyOutput &&
          frame.deliveryGeneration === 0n &&
          frame.streamId === 0,
      ),
    ).toBe(true);
    expect(frames.map((frame) => frame.eventSeq)).toEqual(
      Array.from({ length: frames.length }, (_, index) => BigInt(index + 1)),
    );
    expect(frames.at(-1)?.payload).toEqual(
      Uint8Array.of(HOST_CANONICAL_QUEUE_LIMITS.maxFrames & 0xff),
    );
    expect(harness.closeReasons).toEqual([]);
    expect(harness.controls).toEqual([]);
    harness.scheduler.dispose();
  });

  it.each([
    [15, "warm"],
    [16, "snapshot"],
  ] as const)("selects %s x 16KiB replay as %s at the 256KiB boundary", async (count, mode) => {
    const publisher = {
      publish: vi.fn(async (snapshot: SnapshotCapture) => publishedSnapshot(snapshot)),
    };
    const harness = createHarness(publisher);
    activate(harness);
    harness.pty.emit(new Uint8Array(count * 16 * 1024));
    let barrierMode: "warm" | "snapshot" | undefined;
    harness.setBarrierResponder((encoded) => {
      barrierMode = decodeDeliveryBarrierPayload(decodeDataFrame(encoded).payload).mode;
      answerBarrier(harness, encoded);
    });

    liveAttach(harness, { lastEventSeq: 0n, nextPtyOffset: 0n });
    await settle();

    expect(barrierMode).toBe(mode);
    expect(publisher.publish).toHaveBeenCalledTimes(mode === "snapshot" ? 1 : 0);
  });

  it("routes resize-heavy replay over the conservative warm frame cap to snapshot", async () => {
    const publisher = {
      publish: vi.fn(async (snapshot: SnapshotCapture) => publishedSnapshot(snapshot)),
    };
    const harness = createHarness(publisher);
    activate(harness);
    for (let index = 0; index < WARM_REPLAY_MAX_FRAMES + 1; index += 1) {
      harness.session.resize({ cols: 80, rows: 24, widthPx: 800, heightPx: 480 });
    }
    harness.setBarrierResponder((encoded) => answerBarrier(harness, encoded));

    liveAttach(harness, { lastEventSeq: 0n, nextPtyOffset: 0n });
    await settle();

    expect(publisher.publish).toHaveBeenCalledOnce();
    const barrier = harness.data
      .map(decodeDataFrame)
      .find((frame) => frame.kind === DataFrameKind.DeliveryBarrier);
    expect(decodeDeliveryBarrierPayload(barrier!.payload)).toMatchObject({ mode: "snapshot" });
  });

  it.each([
    [
      "enriched",
      {
        status: "rejected",
        reason: "missing-live-seed",
        retryScope: "same-generation",
      } as const,
    ],
    ["legacy", { status: "rejected" } as const],
  ])(
    "retries a %s rejected warm barrier as cold in the same generation",
    async (_name, outcome) => {
      const publish = vi.fn(async (snapshot: SnapshotCapture) => publishedSnapshot(snapshot));
      const harness = createHarness({ publish });
      activate(harness);
      harness.pty.emit(Uint8Array.of(0x41));
      let barriers = 0;
      harness.setBarrierResponder((encoded) => {
        barriers += 1;
        answerBarrier(harness, encoded, barriers === 1 ? outcome : { status: "ready" });
      });

      liveAttach(harness, { lastEventSeq: 0n, nextPtyOffset: 0n });
      await settle();

      const frames = harness.data.map(decodeDataFrame);
      expect(
        frames
          .filter((frame) => frame.kind === DataFrameKind.DeliveryBarrier)
          .map((frame) => decodeDeliveryBarrierPayload(frame.payload).mode),
      ).toEqual(["warm", "snapshot"]);
      expect(frames.filter((frame) => frame.kind === DataFrameKind.ReplayCommit)).toHaveLength(1);
      expect(publish).toHaveBeenCalledOnce();
      expect(harness.closeReasons).toEqual([]);
    },
  );

  it.each([
    [
      "enriched",
      {
        status: "rejected",
        reason: "snapshot-missing",
        retryScope: "refresh-checkpoint",
      } as const,
    ],
    ["legacy", { status: "rejected" } as const],
  ])("refreshes a %s rejected cold checkpoint after bounded backoff", async (_name, outcome) => {
    vi.useFakeTimers();
    let publishCalls = 0;
    const publish = vi.fn(async (snapshot: SnapshotCapture) => {
      publishCalls += 1;
      return publishedSnapshot(snapshot, publishCalls === 1 ? "A" : "B");
    });
    const harness = createHarness({ publish });
    activate(harness);
    let barriers = 0;
    harness.setBarrierResponder((encoded) => {
      barriers += 1;
      answerBarrier(harness, encoded, barriers === 1 ? outcome : { status: "ready" });
    });

    attach(harness);
    for (let turn = 0; turn < 16; turn += 1) await Promise.resolve();
    expect(publish).toHaveBeenCalledOnce();
    expect(barriers).toBe(1);

    await vi.advanceTimersByTimeAsync(SNAPSHOT_RECOVERY_QUIET_MS - 1);
    expect(publish).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(1);
    for (let turn = 0; turn < 32; turn += 1) await Promise.resolve();

    expect(publish).toHaveBeenCalledTimes(2);
    expect(barriers).toBe(2);
    expect(
      harness.data
        .map(decodeDataFrame)
        .filter((frame) => frame.kind === DataFrameKind.ReplayCommit),
    ).toHaveLength(1);
    expect(harness.closeReasons).toEqual([]);
  });

  it("refreshes a rejected cold checkpoint by a hard deadline during continuous output", async () => {
    vi.useFakeTimers();
    let publishCalls = 0;
    const publish = vi.fn(async (snapshot: SnapshotCapture) => {
      publishCalls += 1;
      return publishedSnapshot(snapshot, publishCalls === 1 ? "A" : "B");
    });
    const harness = createHarness({ publish });
    activate(harness);
    let barriers = 0;
    harness.setBarrierResponder((encoded) => {
      barriers += 1;
      answerBarrier(
        harness,
        encoded,
        barriers === 1
          ? {
              status: "rejected",
              reason: "snapshot-missing",
              retryScope: "refresh-checkpoint",
            }
          : { status: "ready" },
      );
    });

    attach(harness);
    for (let turn = 0; turn < 16; turn += 1) await Promise.resolve();
    expect(publish).toHaveBeenCalledOnce();
    expect(barriers).toBe(1);

    for (let elapsed = 0; elapsed < SNAPSHOT_RECOVERY_MAX_QUIET_WAIT_MS; elapsed += 100) {
      harness.pty.emit(Uint8Array.of(elapsed & 0xff));
      await vi.advanceTimersByTimeAsync(100);
    }
    for (let turn = 0; turn < 32; turn += 1) await Promise.resolve();

    expect(publish).toHaveBeenCalledTimes(2);
    expect(barriers).toBe(2);
    expect(
      harness.data
        .map(decodeDataFrame)
        .filter((frame) => frame.kind === DataFrameKind.ReplayCommit),
    ).toHaveLength(1);
    expect(harness.closeReasons).toEqual([]);
  });

  it("drops a rejected delivery whose browser connection was already isolated", async () => {
    vi.useFakeTimers();
    const publish = vi.fn(async (snapshot: SnapshotCapture) => publishedSnapshot(snapshot));
    const harness = createHarness({ publish });
    activate(harness);
    harness.setBarrierResponder((encoded) =>
      answerBarrier(harness, encoded, {
        status: "rejected",
        reason: "browser-control-send-failed",
        retryScope: "drop-client",
      }),
    );

    attach(harness);
    for (let turn = 0; turn < 16; turn += 1) await Promise.resolve();
    await vi.advanceTimersByTimeAsync(SNAPSHOT_RECOVERY_QUIET_MS * 4);

    expect(publish).toHaveBeenCalledOnce();
    expect(
      harness.data
        .map(decodeDataFrame)
        .filter((frame) => frame.kind === DataFrameKind.DeliveryBarrier),
    ).toHaveLength(1);
    expect(
      harness.data
        .map(decodeDataFrame)
        .filter((frame) => frame.kind === DataFrameKind.ReplayCommit),
    ).toHaveLength(0);
    expect(harness.closeReasons).toEqual([]);
  });

  it("keeps canonical live during snapshot@R upload, then sends the R-to-C tail before C+1", async () => {
    const upload = deferred<PublishedSnapshot>();
    const started = deferred<SnapshotCapture>();
    const publisher: SnapshotPublisherLike = {
      publish(snapshot) {
        started.resolve(snapshot);
        return upload.promise;
      },
    };
    const harness = createHarness(publisher);
    activate(harness);
    harness.pty.emit(Uint8Array.of(0x41));
    attach(harness);
    const snapshot = await started.promise;

    await harness.session.submitText(
      { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1", writerFence: 1n },
      "\u0003",
    );
    harness.pty.emit(encoder.encode("\u001b[6n"));
    harness.pty.emit(Uint8Array.of(0x42));
    await settle();
    expect(harness.data.map(describeData)).toEqual([
      "canonical:1:65",
      "canonical:2:27",
      "canonical:3:66",
    ]);
    expect(harness.pty.writes).toEqual([encoder.encode("\u0003"), encoder.encode("\u001b[1;1R")]);

    harness.setBarrierResponder((encoded) => {
      harness.pty.emit(Uint8Array.of(0x43));
      answerBarrier(harness, encoded);
    });
    upload.resolve(publishedSnapshot(snapshot));
    await settle();

    expect(harness.data.map(describeData)).toEqual([
      "canonical:1:65",
      "canonical:2:27",
      "canonical:3:66",
      "barrier:3",
      "directed:2:27",
      "directed:3:66",
      "commit:3",
      "canonical:4:67",
    ]);
    const commit = harness.data
      .map(decodeDataFrame)
      .find((frame) => frame.kind === DataFrameKind.ReplayCommit);
    expect(commit?.payload).toEqual(new Uint8Array());
  });

  it("resumes a publisher-owned pending snapshot before capturing authority state", async () => {
    let pending!: PublishedSnapshot;
    const publish = vi.fn(async (snapshot: SnapshotCapture) => publishedSnapshot(snapshot, "B"));
    const resumePending = vi.fn(async () => pending);
    const publisher: SnapshotPublisherLike = {
      publish,
      resumePending,
    };
    const harness = createHarness(publisher);
    const captureSnapshot = vi.spyOn(harness.session, "captureSnapshot");
    pending = publishedSnapshot({
      bytes: Uint8Array.of(1),
      cutEventSeq: 0n,
      encodeMs: 1,
      engineId: harness.session.engineId,
      nextPtyOffset: 0n,
      sessionEpoch: harness.session.sessionEpoch,
    });
    harness.setBarrierResponder((encoded) => answerBarrier(harness, encoded));
    activate(harness);

    attach(harness);
    await settle();

    expect(resumePending).toHaveBeenCalledOnce();
    expect(captureSnapshot).not.toHaveBeenCalled();
    expect(publish).not.toHaveBeenCalled();
    expect(harness.data.map(describeData)).toEqual(["barrier:0", "commit:0"]);
    expect(harness.closeReasons).toEqual([]);
  });

  it("recaptures without closing after Cloud confirms pending snapshot cleanup", async () => {
    vi.useFakeTimers();
    let pending = true;
    const resumePending = vi.fn(() => {
      if (!pending) return undefined;
      pending = false;
      return Promise.reject(
        new SnapshotCleanupConfirmedError(
          new Error("checksum mismatch object was confirmed absent"),
        ),
      );
    });
    const publish = vi.fn(async (snapshot: SnapshotCapture) => publishedSnapshot(snapshot, "C"));
    const harness = createHarness({ publish, resumePending });
    harness.setBarrierResponder((encoded) => answerBarrier(harness, encoded));
    activate(harness);

    attach(harness);
    for (let index = 0; index < 8; index += 1) await Promise.resolve();
    expect(resumePending).toHaveBeenCalledOnce();
    expect(publish).not.toHaveBeenCalled();
    expect(harness.closeReasons).toEqual([]);

    await vi.advanceTimersByTimeAsync(SNAPSHOT_RECOVERY_QUIET_MS);
    for (let index = 0; index < 32; index += 1) await Promise.resolve();
    expect(resumePending).toHaveBeenCalledTimes(2);
    expect(publish).toHaveBeenCalledOnce();
    expect(harness.data.map(describeData)).toEqual(["barrier:0", "commit:0"]);
    expect(harness.closeReasons).toEqual([]);
  });

  it("lets a warm client recover while another delivery is publishing a cold snapshot", async () => {
    const upload = deferred<PublishedSnapshot>();
    const started = deferred<SnapshotCapture>();
    const publisher: SnapshotPublisherLike = {
      publish(snapshot) {
        started.resolve(snapshot);
        return upload.promise;
      },
    };
    const harness = createHarness(publisher);
    activate(harness);
    harness.pty.emit(Uint8Array.of(0x41));
    const warmBase = harness.session.cursor;
    attach(harness, { connectionId: "connection_AAAAAAAAA", streamId: 1 });
    const coldSnapshot = await started.promise;
    harness.pty.emit(Uint8Array.of(0x42));
    harness.setBarrierResponder((encoded) => answerBarrier(harness, encoded));

    liveAttach(harness, warmBase, {
      connectionId: "connection_BBBBBBBBB",
      streamId: 2,
    });
    await settle();

    expect(harness.data.map(describeData)).toEqual([
      "canonical:1:65",
      "canonical:2:66",
      "barrier:2",
      "directed:2:66",
      "commit:2",
    ]);
    expect(harness.closeReasons).toEqual([]);

    upload.resolve(publishedSnapshot(coldSnapshot));
    await settle();
  });

  it("reuses one published checkpoint across sixteen idle cold deliveries", async () => {
    const authority = new FakeTerminalAuthority();
    const encodeSnapshot = vi.spyOn(authority, "encodeSnapshot");
    const publish = vi.fn(async (snapshot: SnapshotCapture) => publishedSnapshot(snapshot));
    const harness = createHarness({ publish }, authority);
    activate(harness);
    harness.setBarrierResponder((encoded) => answerBarrier(harness, encoded));

    for (let index = 1; index <= 16; index += 1) {
      attach(harness, {
        connectionId: `connection_${index.toString().padStart(16, "0")}`,
        streamId: index,
      });
    }
    await settle();

    const frames = harness.data.map(decodeDataFrame);
    const barriers = frames.filter((frame) => frame.kind === DataFrameKind.DeliveryBarrier);
    const barrierPayloads = barriers.map((frame) => decodeDeliveryBarrierPayload(frame.payload));
    expect(encodeSnapshot).toHaveBeenCalledOnce();
    expect(publish).toHaveBeenCalledOnce();
    expect(barriers).toHaveLength(16);
    expect(barrierPayloads.every((payload) => payload.mode === "snapshot")).toBe(true);
    expect(
      new Set(
        barrierPayloads.flatMap((payload) =>
          payload.mode === "snapshot" ? [payload.snapshotId] : [],
        ),
      ).size,
    ).toBe(1);
    expect(frames.filter((frame) => frame.kind === DataFrameKind.ReplayCommit)).toHaveLength(16);
    expect(harness.closeReasons).toEqual([]);
  });

  it("invalidates an overlong cached tail and recaptures once after canonical quiet", async () => {
    vi.useFakeTimers();
    const firstStarted = deferred<SnapshotCapture>();
    const firstUpload = deferred<PublishedSnapshot>();
    let publishCalls = 0;
    const publisher: SnapshotPublisherLike = {
      publish(snapshot) {
        publishCalls += 1;
        if (publishCalls === 1) {
          firstStarted.resolve(snapshot);
          return firstUpload.promise;
        }
        return Promise.resolve(publishedSnapshot(snapshot, "Q"));
      },
    };
    const authority = new FakeTerminalAuthority();
    const encodeSnapshot = vi.spyOn(authority, "encodeSnapshot");
    const harness = createHarness(publisher, authority);
    activate(harness);
    harness.setBarrierResponder((encoded) => answerBarrier(harness, encoded));

    for (let index = 1; index <= 16; index += 1) {
      attach(harness, {
        connectionId: `connection_${index.toString().padStart(16, "0")}`,
        streamId: index,
      });
    }
    const firstSnapshot = await firstStarted.promise;
    for (let index = 0; index < WARM_REPLAY_MAX_FRAMES + 1; index += 1) {
      harness.pty.emit(Uint8Array.of(index & 0xff));
    }
    await expect(
      harness.session.submitText(
        { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1", writerFence: 1n },
        "\u0003",
      ),
    ).resolves.toMatchObject({ status: "written" });
    firstUpload.resolve(publishedSnapshot(firstSnapshot, "P"));
    for (let index = 0; index < 32; index += 1) await Promise.resolve();

    expect(publishCalls).toBe(1);
    expect(
      harness.data
        .map(decodeDataFrame)
        .filter((frame) => frame.kind === DataFrameKind.DeliveryBarrier),
    ).toEqual([]);
    expect(harness.closeReasons).toEqual([]);

    await vi.advanceTimersByTimeAsync(SNAPSHOT_RECOVERY_QUIET_MS - 1);
    expect(publishCalls).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    for (let index = 0; index < 64; index += 1) await Promise.resolve();

    const frames = harness.data.map(decodeDataFrame);
    const barriers = frames.filter((frame) => frame.kind === DataFrameKind.DeliveryBarrier);
    const barrierPayloads = barriers.map((frame) => decodeDeliveryBarrierPayload(frame.payload));
    expect(encodeSnapshot).toHaveBeenCalledTimes(2);
    expect(publishCalls).toBe(2);
    expect(barriers).toHaveLength(16);
    expect(barrierPayloads.every((payload) => payload.mode === "snapshot")).toBe(true);
    expect(
      new Set(
        barrierPayloads.flatMap((payload) =>
          payload.mode === "snapshot" ? [payload.snapshotId] : [],
        ),
      ),
    ).toEqual(new Set([`snapshot_${"Q".repeat(16)}`]));
    expect(
      frames.filter(
        (frame) => frame.kind === DataFrameKind.PtyOutput && frame.deliveryGeneration === 0n,
      ),
    ).toHaveLength(WARM_REPLAY_MAX_FRAMES + 1);
    expect(harness.pty.writes).toContainEqual(Uint8Array.of(0x03));
    expect(harness.closeReasons).toEqual([]);
  });

  it("keeps the same delivery and retries after the independent publish timeout", async () => {
    vi.useFakeTimers();
    let publishCalls = 0;
    const publisher: SnapshotPublisherLike = {
      publish(_snapshot, signal) {
        publishCalls += 1;
        return new Promise((_resolve, reject) => {
          signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
        });
      },
    };
    const harness = createHarness(publisher);
    activate(harness);
    harness.pty.emit(Uint8Array.of(0x41));
    attach(harness);
    await Promise.resolve();
    harness.pty.emit(Uint8Array.of(0x42));
    await expect(
      harness.session.submitText(
        { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1", writerFence: 1n },
        "\u0003",
      ),
    ).resolves.toMatchObject({ status: "written" });

    await vi.advanceTimersByTimeAsync(SNAPSHOT_PUBLISH_TIMEOUT_MS);
    await Promise.resolve();

    expect(publishCalls).toBe(1);
    expect(harness.controls).toEqual([]);
    expect(harness.data.map(describeData)).toEqual(["canonical:1:65", "canonical:2:66"]);
    expect(harness.pty.writes).toContainEqual(Uint8Array.of(0x03));
    expect(harness.closeReasons).toEqual([]);

    await vi.advanceTimersByTimeAsync(SNAPSHOT_RECOVERY_QUIET_MS - 1);
    expect(publishCalls).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    await Promise.resolve();
    expect(publishCalls).toBe(2);
  });

  it("rejects an over-budget authority encode before publishing and retries without closing", async () => {
    vi.useFakeTimers();
    let authorityNow = 0;
    const authority = new FakeTerminalAuthority();
    const originalEncodeSnapshot = authority.encodeSnapshot.bind(authority);
    const encodeSnapshot = vi.spyOn(authority, "encodeSnapshot");
    encodeSnapshot.mockImplementation(() => {
      const snapshot = originalEncodeSnapshot();
      authorityNow += SNAPSHOT_ENCODE_BUDGET_MS + 1;
      return snapshot;
    });
    const publish = vi.fn(async (snapshot: SnapshotCapture) => publishedSnapshot(snapshot));
    const harness = createHarness({ publish }, authority, () => authorityNow);
    activate(harness);
    attach(harness);
    for (let index = 0; index < 8; index += 1) await Promise.resolve();

    expect(encodeSnapshot).toHaveBeenCalledOnce();
    expect(publish).not.toHaveBeenCalled();
    expect(harness.closeReasons).toEqual([]);
    await expect(
      harness.session.submitText(
        { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1", writerFence: 1n },
        "\u0003",
      ),
    ).resolves.toMatchObject({ status: "written" });

    await vi.advanceTimersByTimeAsync(SNAPSHOT_RECOVERY_QUIET_MS);
    for (let index = 0; index < 8; index += 1) await Promise.resolve();
    expect(encodeSnapshot).toHaveBeenCalledTimes(2);
    expect(publish).not.toHaveBeenCalled();
    expect(harness.closeReasons).toEqual([]);
  });

  it("keeps control and canonical live while an oversized snapshot uses a long retry gate", async () => {
    vi.useFakeTimers();
    const publish = vi.fn(async () => {
      throw new SnapshotUnavailableError(new RangeError("snapshot too large"), 30_000);
    });
    const publisher: SnapshotPublisherLike = {
      publish,
    };
    const authority = new FakeTerminalAuthority();
    const encodeSnapshot = vi.spyOn(authority, "encodeSnapshot");
    const harness = createHarness(publisher, authority);
    activate(harness);
    attach(harness);
    await Promise.resolve();
    await Promise.resolve();

    harness.pty.emit(Uint8Array.of(0x41));
    await expect(
      harness.session.submitText(
        { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1", writerFence: 1n },
        "\u0003",
      ),
    ).resolves.toMatchObject({ status: "written" });
    await vi.advanceTimersByTimeAsync(29_999);
    expect(publish).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(1);
    await Promise.resolve();
    expect(publish).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(29_999);

    expect(encodeSnapshot).toHaveBeenCalledTimes(2);
    expect(harness.closeReasons).toEqual([]);
    expect(harness.controls).toEqual([]);
    expect(harness.data.map(describeData)).toEqual(["canonical:1:65"]);
    expect(harness.pty.writes).toContainEqual(Uint8Array.of(0x03));
  });

  it("closes the pair when a warm marker result times out", async () => {
    vi.useFakeTimers();
    const harness = createHarness();
    activate(harness);
    harness.pty.emit(Uint8Array.of(0x41));
    harness.setBarrierResponder(() => harness.pty.emit(Uint8Array.of(0x42)));
    liveAttach(harness, { lastEventSeq: 0n, nextPtyOffset: 0n });
    await Promise.resolve();

    await vi.advanceTimersByTimeAsync(DELIVERY_BARRIER_TIMEOUT_MS);
    await Promise.resolve();

    expect(harness.closeReasons).toEqual(["delivery marker outcome is uncertain"]);
    expect(harness.data.map(describeData)).toEqual(["canonical:1:65", "barrier:1"]);
    expect(harness.controls).toEqual([]);
  });

  it("discards an over-budget R-to-C tail and recaptures once after trailing quiet", async () => {
    vi.useFakeTimers();
    let publishCalls = 0;
    const firstStarted = deferred<SnapshotCapture>();
    const firstUpload = deferred<PublishedSnapshot>();
    const publisher: SnapshotPublisherLike = {
      publish(snapshot) {
        publishCalls += 1;
        if (publishCalls !== 1) return Promise.resolve(publishedSnapshot(snapshot, "Q"));
        firstStarted.resolve(snapshot);
        return firstUpload.promise;
      },
    };
    const harness = createHarness(publisher);
    activate(harness);
    harness.setBarrierResponder((encoded) => answerBarrier(harness, encoded));
    attach(harness);
    const firstSnapshot = await firstStarted.promise;

    for (let index = 0; index < WARM_REPLAY_MAX_FRAMES + 1; index += 1) {
      harness.pty.emit(Uint8Array.of(index & 0xff));
      await Promise.resolve();
    }
    await expect(
      harness.session.submitText(
        { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1", writerFence: 1n },
        "\u0003",
      ),
    ).resolves.toMatchObject({ status: "written" });
    firstUpload.resolve(publishedSnapshot(firstSnapshot, "P"));
    for (let index = 0; index < 8; index += 1) await Promise.resolve();

    expect(publishCalls).toBe(1);
    expect(harness.closeReasons).toEqual([]);
    expect(harness.controls).toEqual([]);
    expect(harness.pty.writes).toContainEqual(Uint8Array.of(0x03));

    await vi.advanceTimersByTimeAsync(SNAPSHOT_RECOVERY_QUIET_MS - 1);
    expect(publishCalls).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    await Promise.resolve();
    await Promise.resolve();

    expect(publishCalls).toBe(2);
    expect(harness.closeReasons).toEqual([]);
    expect(
      harness.data
        .map(decodeDataFrame)
        .filter(
          (frame) => frame.kind === DataFrameKind.PtyOutput && frame.deliveryGeneration === 0n,
        ),
    ).toHaveLength(WARM_REPLAY_MAX_FRAMES + 1);
    expect(harness.session.eventSeq).toBe(BigInt(WARM_REPLAY_MAX_FRAMES + 1));
  });

  it("serializes recovery barriers across clients", async () => {
    const harness = createHarness();
    activate(harness);
    const barriers: Uint8Array[] = [];
    harness.setBarrierResponder((encoded) => barriers.push(encoded));

    attach(harness, { connectionId: "connection_AAAAAAAAA", streamId: 1 });
    attach(harness, { connectionId: "connection_BBBBBBBBB", streamId: 2 });
    await settle();
    expect(barriers).toHaveLength(1);

    answerBarrier(harness, barriers[0]!);
    await settle();
    expect(barriers).toHaveLength(2);
    answerBarrier(harness, barriers[1]!, {
      status: "stale",
      reason: "generation-fenced",
    });
    await settle();

    expect(
      harness.data
        .map(decodeDataFrame)
        .filter((frame) => frame.kind === DataFrameKind.DeliveryBarrier),
    ).toHaveLength(2);
    expect(harness.closeReasons).toEqual([]);
  });

  it("silently supersedes pre-marker work and resumes canonical before the new generation", async () => {
    const firstStarted = deferred<void>();
    let publishCalls = 0;
    const publisher: SnapshotPublisherLike = {
      publish(snapshot, signal) {
        publishCalls += 1;
        if (publishCalls !== 1) return Promise.resolve(publishedSnapshot(snapshot, "B"));
        firstStarted.resolve();
        return new Promise((_resolve, reject) => {
          signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
        });
      },
    };
    const harness = createHarness(publisher);
    activate(harness);
    harness.pty.emit(Uint8Array.of(0x41));
    attach(harness, { deliveryGeneration: "1" });
    await firstStarted.promise;
    harness.pty.emit(Uint8Array.of(0x42));
    harness.setBarrierResponder((encoded) => answerBarrier(harness, encoded));

    attach(harness, { deliveryGeneration: "2" });
    await settle();

    expect(publishCalls).toBe(2);
    expect(harness.controls).toEqual([]);
    expect(harness.data.map(describeData)).toEqual([
      "canonical:1:65",
      "canonical:2:66",
      "barrier:2",
      "commit:2",
    ]);
    expect(harness.closeReasons).toEqual([]);
  });
});
