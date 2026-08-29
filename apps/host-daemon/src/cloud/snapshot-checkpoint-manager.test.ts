import { describe, expect, it, vi } from "vitest";

import type { ReplayCursor, SnapshotCapture } from "../session";
import {
  SNAPSHOT_CHECKPOINT_FRESHNESS_MS,
  SNAPSHOT_ENCODE_BUDGET_MS,
  SNAPSHOT_PUBLISH_TIMEOUT_MS,
  SnapshotCheckpointManager,
  SnapshotEncodeBudgetError,
  type SnapshotCheckpointManagerOptions,
  type SnapshotPublisherLike,
} from "./snapshot-checkpoint-manager";
import { SnapshotPendingSupersededError, type PublishedSnapshot } from "./snapshot-publisher";

const ENGINE_ID = "ghostty:test+snapshot-v1+wterm:test";
const SESSION_ID = "session_AAAAAAAAAAAA";
const SNAPSHOT_IDS = [
  "snapshot_AAAAAAAAAAAAAAAA",
  "snapshot_BBBBBBBBBBBBBBBB",
  "snapshot_CCCCCCCCCCCCCCCC",
  "snapshot_DDDDDDDDDDDDDDDD",
  "snapshot_EEEEEEEEEEEEEEEE",
] as const;
const ADMIT_ALL = () => true;

function capture(cutEventSeq = 2n, nextPtyOffset = 3n): SnapshotCapture {
  return {
    bytes: Uint8Array.of(1, 2, 3),
    cutEventSeq,
    encodeMs: 1,
    engineId: ENGINE_ID,
    nextPtyOffset,
    sessionEpoch: 1n,
  };
}

function cursor(snapshot: SnapshotCapture): ReplayCursor {
  return {
    sessionEpoch: snapshot.sessionEpoch,
    lastEventSeq: snapshot.cutEventSeq,
    nextPtyOffset: snapshot.nextPtyOffset,
  };
}

function published(
  snapshot: SnapshotCapture,
  snapshotId: string = SNAPSHOT_IDS[0],
  overrides: Partial<PublishedSnapshot["metadata"]> = {},
): PublishedSnapshot {
  return {
    metadata: {
      sessionId: SESSION_ID,
      snapshotId,
      engineId: snapshot.engineId,
      sessionEpoch: snapshot.sessionEpoch.toString(),
      cutEventSeq: snapshot.cutEventSeq.toString(),
      nextPtyOffset: snapshot.nextPtyOffset.toString(),
      compression: "zstd",
      compressedLength: "4",
      uncompressedLength: snapshot.bytes.byteLength.toString(),
      sha256: "0".repeat(64),
      ...overrides,
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

function controlledSession(
  initial: SnapshotCapture,
  implementation?: () => Promise<SnapshotCapture>,
) {
  let current = initial;
  const captureSnapshot = vi.fn(implementation ?? (async () => current));
  const session = {
    captureSnapshot,
    get cursor() {
      return cursor(current);
    },
    engineId: ENGINE_ID,
    sessionEpoch: 1n,
  } satisfies SnapshotCheckpointManagerOptions["session"];
  return {
    captureSnapshot,
    session,
    setCurrent(snapshot: SnapshotCapture) {
      current = snapshot;
    },
  };
}

function manager(
  publisher: SnapshotPublisherLike,
  options: {
    monotonicNow?: () => number;
    session?: SnapshotCheckpointManagerOptions["session"];
  } = {},
): SnapshotCheckpointManager {
  const session = options.session ?? controlledSession(capture()).session;
  return new SnapshotCheckpointManager({
    ...(options.monotonicNow === undefined ? {} : { monotonicNow: options.monotonicNow }),
    publisher,
    session,
    sessionId: SESSION_ID,
  });
}

function refresh(
  checkpointManager: SnapshotCheckpointManager,
  options: { minimumCut?: ReplayCursor; signal?: AbortSignal } = {},
) {
  return checkpointManager.refresh({
    admitPending: ADMIT_ALL,
    ...(options.minimumCut === undefined ? {} : { minimumCut: options.minimumCut }),
    ...(options.signal === undefined ? {} : { signal: options.signal }),
  });
}

describe("SnapshotCheckpointManager", () => {
  it("keeps latestValid after its age threshold and reuses it without renewing freshness", async () => {
    let now = 100;
    const snapshot = capture();
    const publish = vi.fn(async () => published(snapshot));
    const checkpointManager = manager({ publish }, { monotonicNow: () => now });

    const checkpoint = await refresh(checkpointManager);
    expect(checkpointManager.latestValid()).toEqual({ ageFresh: true, checkpoint });

    now += SNAPSHOT_CHECKPOINT_FRESHNESS_MS;
    expect(checkpointManager.latestValid()).toEqual({ ageFresh: false, checkpoint });
    await expect(refresh(checkpointManager)).resolves.toBe(checkpoint);
    expect(checkpointManager.latestValid()).toEqual({ ageFresh: false, checkpoint });
    expect(publish).toHaveBeenCalledOnce();
  });

  it("keeps the cut high-water while allowing an exact invalidation replacement", async () => {
    const firstSnapshot = capture(2n, 3n);
    const secondSnapshot = capture(3n, 4n);
    const controlled = controlledSession(firstSnapshot);
    let publishIndex = 0;
    const publish = vi.fn(async (snapshot: SnapshotCapture) =>
      published(snapshot, SNAPSHOT_IDS[publishIndex++]!),
    );
    const checkpointManager = manager({ publish }, { session: controlled.session });

    const first = await refresh(checkpointManager);
    controlled.setCurrent(secondSnapshot);
    const second = await refresh(checkpointManager, { minimumCut: cursor(secondSnapshot) });
    expect(Object.isFrozen(second)).toBe(true);
    expect(Object.isFrozen(second.base)).toBe(true);
    expect(Object.isFrozen(second.published.metadata)).toBe(true);

    checkpointManager.invalidate(first, cursor(secondSnapshot));
    expect(checkpointManager.latestValid()?.checkpoint).toBe(second);
    checkpointManager.invalidate(second, cursor(secondSnapshot));
    expect(checkpointManager.latestValid()).toBeUndefined();

    const replacement = await refresh(checkpointManager);
    expect(replacement).not.toBe(second);
    expect(replacement.base).toEqual(cursor(secondSnapshot));
    expect(checkpointManager.latestValid()?.checkpoint).toBe(replacement);
  });

  it("fences immutable identity changes returned by a resumed pending upload", async () => {
    const firstSnapshot = capture(2n, 3n);
    const laterSnapshot = capture(3n, 4n);
    const controlled = controlledSession(firstSnapshot);
    const changedIdentity = published(firstSnapshot, SNAPSHOT_IDS[0], {
      sha256: "1".repeat(64),
    });
    const resumePending = vi
      .fn<NonNullable<SnapshotPublisherLike["resumePending"]>>()
      .mockReturnValueOnce(undefined)
      .mockReturnValueOnce(Promise.resolve(changedIdentity));
    const publish = vi.fn(async () => published(firstSnapshot, SNAPSHOT_IDS[0]));
    const checkpointManager = manager({ publish, resumePending }, { session: controlled.session });

    const first = await refresh(checkpointManager);
    controlled.setCurrent(laterSnapshot);
    await expect(refresh(checkpointManager, { minimumCut: cursor(laterSnapshot) })).rejects.toThrow(
      "snapshot checkpoint immutable identity changed",
    );
    expect(checkpointManager.latestValid()?.checkpoint).toBe(first);
  });

  it("rejects a late regressed cut and a different identity at the same high-water", async () => {
    const installedSnapshot = capture(5n, 8n);
    const currentSnapshot = capture(6n, 9n);
    const controlled = controlledSession(installedSnapshot);
    const resumePending = vi
      .fn<NonNullable<SnapshotPublisherLike["resumePending"]>>()
      .mockReturnValueOnce(undefined)
      .mockReturnValueOnce(Promise.resolve(published(capture(4n, 7n), SNAPSHOT_IDS[1])))
      .mockReturnValueOnce(Promise.resolve(published(installedSnapshot, SNAPSHOT_IDS[2])));
    const publisher = {
      publish: async () => published(installedSnapshot, SNAPSHOT_IDS[0]),
      resumePending,
    };
    const checkpointManager = manager(publisher, { session: controlled.session });
    const installed = await refresh(checkpointManager);
    controlled.setCurrent(currentSnapshot);

    await expect(
      refresh(checkpointManager, { minimumCut: cursor(currentSnapshot) }),
    ).rejects.toThrow("snapshot checkpoint cut regressed");
    await expect(
      refresh(checkpointManager, { minimumCut: cursor(currentSnapshot) }),
    ).rejects.toThrow("snapshot checkpoint identity changed without advancing its cut");
    expect(checkpointManager.latestValid()?.checkpoint).toBe(installed);
  });

  it("fences an invalidated identity but permits a new identity at the same cut", async () => {
    const snapshot = capture();
    const controlled = controlledSession(snapshot);
    const resumePending = vi
      .fn<NonNullable<SnapshotPublisherLike["resumePending"]>>()
      .mockReturnValueOnce(undefined)
      .mockReturnValueOnce(Promise.resolve(published(snapshot, SNAPSHOT_IDS[0])))
      .mockReturnValueOnce(Promise.resolve(published(snapshot, SNAPSHOT_IDS[1])));
    const checkpointManager = manager(
      {
        publish: async () => published(snapshot, SNAPSHOT_IDS[0]),
        resumePending,
      },
      { session: controlled.session },
    );
    const first = await refresh(checkpointManager);
    checkpointManager.invalidate(first, first.base);

    await expect(refresh(checkpointManager)).rejects.toThrow(
      "invalidated snapshot checkpoint identity cannot be reinstalled",
    );
    await expect(refresh(checkpointManager)).resolves.toMatchObject({
      base: cursor(snapshot),
      published: { metadata: { snapshotId: SNAPSHOT_IDS[1] } },
    });
  });

  it("validates resumed lineage before exposing a frozen replay base to admission", async () => {
    const snapshot = capture(7n, 11n);
    const controlled = controlledSession(snapshot);
    const uploaded = published(snapshot);
    const bases: object[] = [];
    const policy = vi.fn((base: object) => {
      bases.push(base);
      return true;
    });
    const resumePending = vi.fn<NonNullable<SnapshotPublisherLike["resumePending"]>>(
      async (_signal, admit) => {
        expect(admit?.(uploaded.metadata)).toBe(true);
        return uploaded;
      },
    );
    const checkpointManager = manager(
      { publish: vi.fn<SnapshotPublisherLike["publish"]>(), resumePending },
      { session: controlled.session },
    );

    const checkpoint = await checkpointManager.refresh({ admitPending: policy });

    expect(checkpoint.base).toEqual(cursor(snapshot));
    expect(bases).toEqual([cursor(snapshot)]);
    expect(Object.isFrozen(bases[0])).toBe(true);
    expect(controlled.captureSnapshot).not.toHaveBeenCalled();
  });

  it("rejects mismatched pending lineage before calling admission policy", async () => {
    const snapshot = capture();
    const controlled = controlledSession(snapshot);
    const mismatched = published(snapshot, SNAPSHOT_IDS[0], { sessionEpoch: "2" });
    const resumePending = vi.fn<NonNullable<SnapshotPublisherLike["resumePending"]>>(
      async (_signal, admit) => {
        admit?.(mismatched.metadata);
        return mismatched;
      },
    );
    const policy = vi.fn(() => true);
    const checkpointManager = manager(
      { publish: vi.fn<SnapshotPublisherLike["publish"]>(), resumePending },
      { session: controlled.session },
    );

    await expect(checkpointManager.refresh({ admitPending: policy })).rejects.toThrow(
      "published snapshot metadata does not match its terminal session",
    );
    expect(policy).not.toHaveBeenCalled();
    expect(checkpointManager.latestValid()).toBeUndefined();
  });

  it("shares one common flight across 16 waiters when 15 abort", async () => {
    const snapshot = capture();
    const encoded = deferred<SnapshotCapture>();
    const controlled = controlledSession(snapshot, () => encoded.promise);
    const controllers = Array.from({ length: 16 }, () => new AbortController());
    const publish = vi.fn<SnapshotPublisherLike["publish"]>(async (captured, signal, admit) => {
      expect(controllers.every((controller) => signal !== controller.signal)).toBe(true);
      expect(signal?.aborted).toBe(false);
      const uploaded = published(captured);
      expect(admit?.(uploaded.metadata)).toBe(true);
      return uploaded;
    });
    const checkpointManager = manager({ publish }, { session: controlled.session });

    const waiting = controllers.map((controller) =>
      refresh(checkpointManager, { signal: controller.signal }),
    );
    await vi.waitFor(() => expect(controlled.captureSnapshot).toHaveBeenCalledOnce());
    const aborted = waiting.slice(0, 15).map((promise, index) => {
      const reason = new DOMException(`superseded ${index}`, "AbortError");
      const assertion = expect(promise).rejects.toBe(reason);
      controllers[index]!.abort(reason);
      return assertion;
    });
    await Promise.all(aborted);
    expect(publish).not.toHaveBeenCalled();

    encoded.resolve(snapshot);
    const checkpoint = await waiting[15]!;

    expect(checkpoint.base).toEqual(cursor(snapshot));
    expect(controlled.captureSnapshot).toHaveBeenCalledOnce();
    expect(publish).toHaveBeenCalledOnce();
    expect(checkpointManager.latestValid()?.checkpoint).toBe(checkpoint);
  });

  it("installs the first cut for an older waiter and performs exactly one newer-cut follow-up", async () => {
    const firstSnapshot = capture(2n, 3n);
    const newerSnapshot = capture(5n, 8n);
    const firstCapture = deferred<SnapshotCapture>();
    let captureCount = 0;
    const controlled = controlledSession(firstSnapshot, () => {
      captureCount += 1;
      return captureCount === 1 ? firstCapture.promise : Promise.resolve(newerSnapshot);
    });
    let publishCount = 0;
    const publish = vi.fn(async (snapshot: SnapshotCapture) =>
      published(snapshot, SNAPSHOT_IDS[publishCount++]!),
    );
    const checkpointManager = manager({ publish }, { session: controlled.session });

    const older = refresh(checkpointManager);
    controlled.setCurrent(newerSnapshot);
    const newer = refresh(checkpointManager, { minimumCut: cursor(newerSnapshot) });
    firstCapture.resolve(firstSnapshot);

    await expect(older).resolves.toMatchObject({ base: cursor(firstSnapshot) });
    await expect(newer).resolves.toMatchObject({ base: cursor(newerSnapshot) });
    expect(controlled.captureSnapshot).toHaveBeenCalledTimes(2);
    expect(publish).toHaveBeenCalledTimes(2);
    expect(checkpointManager.latestValid()?.checkpoint.base).toEqual(cursor(newerSnapshot));
  });

  it("does not start a newer-cut follow-up after the last live waiter aborts", async () => {
    const firstSnapshot = capture(2n, 3n);
    const newerSnapshot = capture(5n, 8n);
    const firstCapture = deferred<SnapshotCapture>();
    const controlled = controlledSession(firstSnapshot, () => firstCapture.promise);
    const controller = new AbortController();
    const publish = vi.fn(async (snapshot: SnapshotCapture) => published(snapshot));
    const checkpointManager = manager({ publish }, { session: controlled.session });

    controlled.setCurrent(newerSnapshot);
    const waiting = refresh(checkpointManager, {
      minimumCut: cursor(newerSnapshot),
      signal: controller.signal,
    });
    const reason = new DOMException("request superseded", "AbortError");
    const rejected = expect(waiting).rejects.toBe(reason);
    controller.abort(reason);
    await rejected;
    firstCapture.resolve(firstSnapshot);
    await vi.waitFor(() => expect(publish).toHaveBeenCalledOnce());

    expect(controlled.captureSnapshot).toHaveBeenCalledOnce();
    expect(checkpointManager.latestValid()?.checkpoint.base).toEqual(cursor(firstSnapshot));
  });

  it("keeps a global invalidation floor hidden until a follow-up reaches it", async () => {
    const firstSnapshot = capture(2n, 3n);
    const newerSnapshot = capture(5n, 8n);
    const controlled = controlledSession(firstSnapshot);
    let publishCount = 0;
    const admissions: boolean[] = [];
    const publish = vi.fn<SnapshotPublisherLike["publish"]>(async (snapshot, _signal, admit) => {
      const uploaded = published(snapshot, SNAPSHOT_IDS[publishCount++]!);
      admissions.push(admit?.(uploaded.metadata) ?? true);
      return uploaded;
    });
    const checkpointManager = manager({ publish }, { session: controlled.session });
    const first = await refresh(checkpointManager);

    controlled.setCurrent(newerSnapshot);
    checkpointManager.invalidate(first, cursor(newerSnapshot));
    expect(checkpointManager.latestValid()).toBeUndefined();
    const followUp = deferred<SnapshotCapture>();
    controlled.captureSnapshot.mockResolvedValueOnce(firstSnapshot);
    controlled.captureSnapshot.mockReturnValueOnce(followUp.promise);

    const replacement = refresh(checkpointManager);
    await vi.waitFor(() => expect(controlled.captureSnapshot).toHaveBeenCalledTimes(3));
    expect(checkpointManager.latestValid()).toBeUndefined();
    followUp.resolve(newerSnapshot);
    await expect(replacement).resolves.toMatchObject({ base: cursor(newerSnapshot) });
    expect(publish).toHaveBeenCalledTimes(3);
    expect(admissions).toEqual([true, false, true]);
    expect(checkpointManager.latestValid()?.checkpoint.base).toEqual(cursor(newerSnapshot));
  });

  it("ignores a stale checkpoint invalidation and its replacement floor", async () => {
    const firstSnapshot = capture(2n, 3n);
    const secondSnapshot = capture(3n, 4n);
    const futureSnapshot = capture(5n, 8n);
    const controlled = controlledSession(firstSnapshot);
    let publishCount = 0;
    const publish = vi.fn(async (snapshot: SnapshotCapture) =>
      published(snapshot, SNAPSHOT_IDS[publishCount++]!),
    );
    const checkpointManager = manager({ publish }, { session: controlled.session });
    const first = await refresh(checkpointManager);
    controlled.setCurrent(secondSnapshot);
    const second = await refresh(checkpointManager, { minimumCut: cursor(secondSnapshot) });
    controlled.setCurrent(futureSnapshot);

    checkpointManager.invalidate(first, cursor(futureSnapshot));

    expect(checkpointManager.latestValid()?.checkpoint).toBe(second);
    await expect(refresh(checkpointManager)).resolves.toBe(second);
    expect(publish).toHaveBeenCalledTimes(2);
  });

  it("validates a registered minimum cut against the fixed epoch and current cursor", async () => {
    const snapshot = capture(5n, 8n);
    const controlled = controlledSession(snapshot);
    const checkpointManager = manager(
      { publish: async () => published(snapshot) },
      { session: controlled.session },
    );

    expect(() =>
      checkpointManager.refresh({
        admitPending: ADMIT_ALL,
        minimumCut: { ...cursor(snapshot), sessionEpoch: 2n },
      }),
    ).toThrow("snapshot refresh minimum cut does not match its terminal session");
    expect(() =>
      checkpointManager.refresh({
        admitPending: ADMIT_ALL,
        minimumCut: { sessionEpoch: 1n, lastEventSeq: 6n, nextPtyOffset: 9n },
      }),
    ).toThrow("snapshot refresh minimum cut is ahead of its terminal session");
  });

  it("merges the current cursor after an exact pending supersede", async () => {
    const firstSnapshot = capture(2n, 3n);
    const newerSnapshot = capture(5n, 8n);
    const controlled = controlledSession(firstSnapshot);
    const resumePending = vi
      .fn<NonNullable<SnapshotPublisherLike["resumePending"]>>()
      .mockRejectedValueOnce(new SnapshotPendingSupersededError())
      .mockReturnValue(undefined);
    const publish = vi.fn(async (snapshot: SnapshotCapture) => published(snapshot));
    const checkpointManager = manager({ publish, resumePending }, { session: controlled.session });
    controlled.setCurrent(newerSnapshot);

    await expect(refresh(checkpointManager)).rejects.toBeInstanceOf(SnapshotPendingSupersededError);
    controlled.captureSnapshot.mockResolvedValueOnce(firstSnapshot);
    controlled.captureSnapshot.mockResolvedValueOnce(newerSnapshot);

    await expect(refresh(checkpointManager)).resolves.toMatchObject({
      base: cursor(newerSnapshot),
    });
    expect(checkpointManager.latestValid()?.checkpoint.base).toEqual(cursor(newerSnapshot));
  });

  it("enforces both the measured encode budget and the manager-owned encode deadline", async () => {
    const tooSlow = { ...capture(), encodeMs: SNAPSHOT_ENCODE_BUDGET_MS + 1 };
    const publish = vi.fn(async () => published(tooSlow));
    const measuredManager = manager({ publish }, { session: controlledSession(tooSlow).session });

    await expect(refresh(measuredManager)).rejects.toBeInstanceOf(SnapshotEncodeBudgetError);
    expect(publish).not.toHaveBeenCalled();

    vi.useFakeTimers();
    try {
      const encoded = deferred<SnapshotCapture>();
      const controlled = controlledSession(capture(), () => encoded.promise);
      const deadlineManager = manager(
        { publish: vi.fn<SnapshotPublisherLike["publish"]>() },
        { session: controlled.session },
      );
      const waiting = refresh(deadlineManager);
      const rejected = expect(waiting).rejects.toBeInstanceOf(SnapshotEncodeBudgetError);

      await vi.advanceTimersByTimeAsync(SNAPSHOT_ENCODE_BUDGET_MS);
      await rejected;
      encoded.resolve(capture());
      expect(deadlineManager.latestValid()).toBeUndefined();
    } finally {
      vi.useRealTimers();
    }
  });

  it("bounds publish independently from waiter signals", async () => {
    vi.useFakeTimers();
    try {
      const uploaded = deferred<PublishedSnapshot>();
      const snapshot = capture();
      const publish = vi.fn(() => uploaded.promise);
      const checkpointManager = manager(
        { publish },
        { session: controlledSession(snapshot).session },
      );
      const waiting = refresh(checkpointManager);
      await vi.waitFor(() => expect(publish).toHaveBeenCalledOnce());
      const rejected = expect(waiting).rejects.toMatchObject({ name: "TimeoutError" });

      await vi.advanceTimersByTimeAsync(SNAPSHOT_PUBLISH_TIMEOUT_MS);
      await rejected;
      uploaded.resolve(published(snapshot));
      expect(checkpointManager.latestValid()).toBeUndefined();
    } finally {
      vi.useRealTimers();
    }
  });

  it("does not start authority capture when disposed after an empty pending lookup", async () => {
    const snapshot = capture();
    const controlled = controlledSession(snapshot);
    const resumePending = vi.fn<NonNullable<SnapshotPublisherLike["resumePending"]>>(
      () => undefined,
    );
    const publish = vi.fn<SnapshotPublisherLike["publish"]>();
    const checkpointManager = manager({ publish, resumePending }, { session: controlled.session });
    const waiting = refresh(checkpointManager);
    const reason = new Error("relay stopped before capture");
    const rejected = expect(waiting).rejects.toBe(reason);

    checkpointManager.dispose(reason);

    await rejected;
    await Promise.resolve();
    expect(resumePending).toHaveBeenCalledOnce();
    expect(controlled.captureSnapshot).not.toHaveBeenCalled();
    expect(publish).not.toHaveBeenCalled();
    expect(checkpointManager.latestValid()).toBeUndefined();
  });

  it("dispose aborts only the common flight and a late publisher result cannot install", async () => {
    const snapshot = capture();
    const uploaded = deferred<PublishedSnapshot>();
    let commonSignal: AbortSignal | undefined;
    const dispose = vi.fn();
    const publish = vi.fn<SnapshotPublisherLike["publish"]>((_snapshot, signal) => {
      commonSignal = signal;
      return uploaded.promise;
    });
    const checkpointManager = manager(
      { dispose, publish },
      { session: controlledSession(snapshot).session },
    );
    const caller = new AbortController();
    const waiting = refresh(checkpointManager, { signal: caller.signal });
    await vi.waitFor(() => expect(publish).toHaveBeenCalledOnce());
    expect(commonSignal).not.toBe(caller.signal);
    const reason = new Error("relay stopped");
    const rejected = expect(waiting).rejects.toBe(reason);

    checkpointManager.dispose(reason);
    checkpointManager.dispose(new Error("late stop"));
    await rejected;
    expect(caller.signal.aborted).toBe(false);
    expect(commonSignal?.aborted).toBe(true);
    expect(dispose).toHaveBeenCalledOnce();
    expect(dispose).toHaveBeenCalledWith(reason);

    uploaded.resolve(published(snapshot));
    await Promise.resolve();
    expect(checkpointManager.latestValid()).toBeUndefined();
    expect(() => refresh(checkpointManager)).toThrow(reason);
  });
});
