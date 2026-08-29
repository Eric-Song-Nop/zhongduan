import { describe, expect, it, vi } from "vitest";

import type { SnapshotCapture } from "../session";
import {
  SNAPSHOT_CHECKPOINT_FRESHNESS_MS,
  SnapshotCheckpointManager,
  type SnapshotPublisherLike,
} from "./snapshot-checkpoint-manager";
import type { PublishedSnapshot } from "./snapshot-publisher";

const ENGINE_ID = "ghostty:test+snapshot-v1+wterm:test";
const SESSION_ID = "session_AAAAAAAAAAAA";

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

function published(
  snapshot: SnapshotCapture,
  snapshotId = `snapshot_${snapshot.cutEventSeq.toString().padStart(16, "A")}`,
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

function manager(
  publisher: SnapshotPublisherLike,
  options: { monotonicNow?: () => number } = {},
): SnapshotCheckpointManager {
  return new SnapshotCheckpointManager({
    ...(options.monotonicNow === undefined ? {} : { monotonicNow: options.monotonicNow }),
    publisher,
    session: { engineId: ENGINE_ID, sessionEpoch: 1n },
    sessionId: SESSION_ID,
  });
}

async function publishAndInstall(
  checkpointManager: SnapshotCheckpointManager,
  snapshot: SnapshotCapture,
) {
  const uploaded = await checkpointManager.publish(snapshot);
  return checkpointManager.install(uploaded, snapshot);
}

describe("SnapshotCheckpointManager", () => {
  it("keeps latestValid after its age freshness threshold without renewing it on read", async () => {
    let now = 100;
    const snapshot = capture();
    const checkpointManager = manager(
      { publish: async () => published(snapshot, "snapshot_AAAAAAAAAAAAAAAA") },
      { monotonicNow: () => now },
    );

    const checkpoint = await publishAndInstall(checkpointManager, snapshot);
    expect(checkpointManager.latestValid()).toEqual({ ageFresh: true, checkpoint });

    now += SNAPSHOT_CHECKPOINT_FRESHNESS_MS - 1;
    expect(checkpointManager.latestValid()).toEqual({ ageFresh: true, checkpoint });
    now += 1;
    expect(checkpointManager.latestValid()).toEqual({ ageFresh: false, checkpoint });
    now += SNAPSHOT_CHECKPOINT_FRESHNESS_MS;
    expect(checkpointManager.latestValid()).toEqual({ ageFresh: false, checkpoint });
  });

  it("keeps a cut high-water and fences the invalidated identity", async () => {
    const publish = vi.fn(async (snapshot: SnapshotCapture) =>
      published(
        snapshot,
        snapshot.cutEventSeq === 2n ? "snapshot_AAAAAAAAAAAAAAAA" : "snapshot_BBBBBBBBBBBBBBBB",
      ),
    );
    const checkpointManager = manager({ publish });
    const firstSnapshot = capture(2n, 3n);
    const secondSnapshot = capture(3n, 4n);
    const first = await publishAndInstall(checkpointManager, firstSnapshot);
    const second = await publishAndInstall(checkpointManager, secondSnapshot);

    expect(Object.isFrozen(second)).toBe(true);
    expect(Object.isFrozen(second.base)).toBe(true);
    expect(Object.isFrozen(second.published.metadata)).toBe(true);
    checkpointManager.invalidate(first);
    expect(checkpointManager.latestValid()?.checkpoint).toBe(second);
    checkpointManager.invalidate(second);
    expect(checkpointManager.latestValid()).toBeUndefined();

    expect(() =>
      checkpointManager.install(
        published(firstSnapshot, "snapshot_AAAAAAAAAAAAAAAA"),
        firstSnapshot,
      ),
    ).toThrow("snapshot checkpoint cut regressed");
    expect(() =>
      checkpointManager.install(
        published(secondSnapshot, "snapshot_BBBBBBBBBBBBBBBB"),
        secondSnapshot,
      ),
    ).toThrow("invalidated snapshot checkpoint identity cannot be reinstalled");

    const replacement = checkpointManager.install(
      published(secondSnapshot, "snapshot_CCCCCCCCCCCCCCCC"),
      secondSnapshot,
    );
    expect(replacement).not.toBe(second);
    expect(checkpointManager.latestValid()).toMatchObject({
      ageFresh: true,
      checkpoint: replacement,
    });
  });

  it("treats an idempotent install as the same checkpoint without renewing age freshness", async () => {
    let now = 0;
    const snapshot = capture();
    const checkpointManager = manager(
      { publish: async () => published(snapshot, "snapshot_AAAAAAAAAAAAAAAA") },
      { monotonicNow: () => now },
    );
    const first = await publishAndInstall(checkpointManager, snapshot);
    now = SNAPSHOT_CHECKPOINT_FRESHNESS_MS;

    const repeated = await publishAndInstall(checkpointManager, snapshot);

    expect(repeated).toBe(first);
    expect(checkpointManager.latestValid()).toEqual({ ageFresh: false, checkpoint: first });
  });

  it("rejects reuse of an immutable snapshot identity with changed metadata", () => {
    const firstSnapshot = capture(2n, 3n);
    const checkpointManager = manager({ publish: vi.fn<SnapshotPublisherLike["publish"]>() });
    const first = checkpointManager.install(
      published(firstSnapshot, "snapshot_AAAAAAAAAAAAAAAA"),
      firstSnapshot,
    );

    expect(() =>
      checkpointManager.install(
        published(firstSnapshot, "snapshot_AAAAAAAAAAAAAAAA", { sha256: "1".repeat(64) }),
        firstSnapshot,
      ),
    ).toThrow("snapshot checkpoint immutable identity changed");
    expect(checkpointManager.latestValid()?.checkpoint).toBe(first);
  });

  it("installs a resumed immutable pending upload before a new capture", async () => {
    const snapshot = capture();
    const resumePending = vi.fn(async () => published(snapshot, "snapshot_AAAAAAAAAAAAAAAA"));
    const publish = vi.fn<SnapshotPublisherLike["publish"]>();
    const checkpointManager = manager({ publish, resumePending });

    const resumed = await checkpointManager.resumePending();
    const checkpoint = checkpointManager.install(resumed!);

    expect(checkpoint).toMatchObject({
      base: { lastEventSeq: 2n, nextPtyOffset: 3n, sessionEpoch: 1n },
    });
    expect(resumePending).toHaveBeenCalledOnce();
    expect(publish).not.toHaveBeenCalled();
  });

  it("validates pending lineage before exposing a frozen replay base to admission policy", async () => {
    const snapshot = capture(7n, 11n);
    const uploaded = published(snapshot, "snapshot_AAAAAAAAAAAAAAAA");
    const bases: object[] = [];
    const policy = vi.fn((base: object) => {
      bases.push(base);
      return true;
    });
    const publish = vi.fn<SnapshotPublisherLike["publish"]>(async (_capture, _signal, admit) => {
      expect(admit?.(uploaded.metadata)).toBe(true);
      return uploaded;
    });
    const resumePending = vi.fn<NonNullable<SnapshotPublisherLike["resumePending"]>>(
      async (_signal, admit) => {
        expect(admit?.(uploaded.metadata)).toBe(true);
        return uploaded;
      },
    );
    const checkpointManager = manager({ publish, resumePending });

    await checkpointManager.publish(snapshot, undefined, policy);
    await checkpointManager.resumePending(undefined, policy);

    expect(policy).toHaveBeenCalledTimes(2);
    expect(bases).toEqual([
      { lastEventSeq: 7n, nextPtyOffset: 11n, sessionEpoch: 1n },
      { lastEventSeq: 7n, nextPtyOffset: 11n, sessionEpoch: 1n },
    ]);
    expect(bases.every((base) => Object.isFrozen(base))).toBe(true);
  });

  it("rejects mismatched pending lineage before calling admission policy", async () => {
    const snapshot = capture();
    const mismatched = published(snapshot, "snapshot_AAAAAAAAAAAAAAAA", { sessionEpoch: "2" });
    const resumePending = vi.fn<NonNullable<SnapshotPublisherLike["resumePending"]>>(
      async (_signal, admit) => {
        admit?.(mismatched.metadata);
        return mismatched;
      },
    );
    const policy = vi.fn(() => true);
    const checkpointManager = manager({
      publish: vi.fn<SnapshotPublisherLike["publish"]>(),
      resumePending,
    });

    await expect(checkpointManager.resumePending(undefined, policy)).rejects.toThrow(
      "published snapshot metadata does not match its terminal session",
    );
    expect(policy).not.toHaveBeenCalled();
  });

  it("disposes publisher ownership once and fences later manager use", async () => {
    const snapshot = capture();
    const dispose = vi.fn();
    const checkpointManager = manager({
      dispose,
      publish: async () => published(snapshot, "snapshot_AAAAAAAAAAAAAAAA"),
    });
    const checkpoint = await publishAndInstall(checkpointManager, snapshot);
    const reason = new Error("relay stopped");

    checkpointManager.dispose(reason);
    checkpointManager.dispose(new Error("late stop"));

    expect(dispose).toHaveBeenCalledOnce();
    expect(dispose).toHaveBeenCalledWith(reason);
    expect(checkpointManager.latestValid()).toBeUndefined();
    expect(() => checkpointManager.install(checkpoint.published, snapshot)).toThrow(reason);
    expect(() => checkpointManager.publish(snapshot)).toThrow(reason);
  });

  it("rejects session, engine, and epoch identity mismatches", () => {
    const snapshot = capture();
    const checkpointManager = manager({ publish: vi.fn<SnapshotPublisherLike["publish"]>() });

    for (const overrides of [
      { sessionId: "session_BBBBBBBBBBBB" },
      { engineId: "ghostty:other+snapshot-v1+wterm:test" },
      { sessionEpoch: "2" },
    ]) {
      expect(() =>
        checkpointManager.install(
          published(snapshot, "snapshot_AAAAAAAAAAAAAAAA", overrides),
          snapshot,
        ),
      ).toThrow("published snapshot metadata does not match its terminal session");
    }
    expect(checkpointManager.latestValid()).toBeUndefined();
  });

  it("rejects wrong authority lineage and late checkpoint regression without replacing latestValid", async () => {
    const responses = [
      published(capture(5n, 6n), "snapshot_AAAAAAAAAAAAAAAA"),
      published(capture(4n, 5n), "snapshot_BBBBBBBBBBBBBBBB"),
      published(capture(5n, 6n), "snapshot_CCCCCCCCCCCCCCCC"),
      published(capture(6n, 7n), "snapshot_DDDDDDDDDDDDDDDD", {
        nextPtyOffset: "8",
      }),
      published(capture(5n, 7n), "snapshot_EEEEEEEEEEEEEEEE"),
      published(capture(6n, 5n), "snapshot_FFFFFFFFFFFFFFFF"),
    ];
    const checkpointManager = manager({ publish: async () => responses.shift()! });
    const latestSnapshot = capture(5n, 6n);
    const latest = await publishAndInstall(checkpointManager, latestSnapshot);

    const regressed = capture(4n, 5n);
    expect(() => checkpointManager.install(responses.shift()!, regressed)).toThrow(
      "snapshot checkpoint cut regressed",
    );
    const equalCut = capture(5n, 6n);
    expect(() => checkpointManager.install(responses.shift()!, equalCut)).toThrow(
      "snapshot checkpoint identity changed without advancing its cut",
    );
    const mismatchedCut = capture(6n, 7n);
    expect(() => checkpointManager.install(responses.shift()!, mismatchedCut)).toThrow(
      "published snapshot metadata does not match its authority cut",
    );
    const inconsistentOffset = capture(5n, 7n);
    expect(() => checkpointManager.install(responses.shift()!, inconsistentOffset)).toThrow(
      "snapshot checkpoint cursor is inconsistent at the same event sequence",
    );
    const regressedOffset = capture(6n, 5n);
    expect(() => checkpointManager.install(responses.shift()!, regressedOffset)).toThrow(
      "snapshot checkpoint PTY offset regressed",
    );
    expect(checkpointManager.latestValid()?.checkpoint).toBe(latest);
  });
});
