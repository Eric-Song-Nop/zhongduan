import { describe, expect, it } from "vitest";

import {
  SNAPSHOT_CHECKPOINT_TTL_MS,
  SnapshotCheckpointCache,
  type SnapshotCheckpoint,
} from "./snapshot-checkpoint-cache";

function checkpoint(snapshotId: string): SnapshotCheckpoint {
  return {
    base: { sessionEpoch: 1n, lastEventSeq: 2n, nextPtyOffset: 3n },
    engineId: "ghostty:test+snapshot-v1+wterm:test",
    published: {
      metadata: {
        sessionId: "session_AAAAAAAAAAAA",
        snapshotId,
        engineId: "ghostty:test+snapshot-v1+wterm:test",
        sessionEpoch: "1",
        cutEventSeq: "2",
        nextPtyOffset: "3",
        compression: "zstd",
        compressedLength: "4",
        uncompressedLength: "5",
        sha256: "0".repeat(64),
      },
    },
  };
}

describe("SnapshotCheckpointCache", () => {
  it("keeps a metadata-only checkpoint through its short session TTL", () => {
    let now = 100;
    const cache = new SnapshotCheckpointCache({ monotonicNow: () => now });
    const stored = cache.install(checkpoint("snapshot_AAAAAAAAAAAAAAAA"));

    expect(cache.current()).toBe(stored);
    now += SNAPSHOT_CHECKPOINT_TTL_MS - 1;
    expect(cache.current()).toBe(stored);
    now += 1;
    expect(cache.current()).toBeUndefined();
  });

  it("invalidates only the installed checkpoint identity", () => {
    const cache = new SnapshotCheckpointCache();
    const stale = checkpoint("snapshot_AAAAAAAAAAAAAAAA");
    const installed = cache.install(checkpoint("snapshot_BBBBBBBBBBBBBBBB"));

    cache.invalidate(stale);
    expect(cache.current()).toBe(installed);
    cache.invalidate(installed);
    expect(cache.current()).toBeUndefined();
  });
});
