import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { ReplayCursor, SnapshotCapture, TerminalSession } from "../session";
import {
  SnapshotCheckpointManager,
  type SnapshotPublisherLike,
} from "./snapshot-checkpoint-manager";
import { SnapshotRefreshOwner } from "./snapshot-refresh-owner";

const SESSION_ID = "session_AAAAAAAAA";
const ENGINE_ID = "ghostty:test+snapshot";

function harness(publish?: SnapshotPublisherLike["publish"]) {
  let cursor: ReplayCursor = {
    sessionEpoch: 7n,
    lastEventSeq: 0n,
    nextPtyOffset: 0n,
  };
  const captureSnapshot = vi.fn(async (): Promise<SnapshotCapture> => ({
    bytes: Uint8Array.of(0x41),
    cutEventSeq: cursor.lastEventSeq,
    encodeMs: 0,
    engineId: ENGINE_ID,
    nextPtyOffset: cursor.nextPtyOffset,
    sessionEpoch: cursor.sessionEpoch,
  }));
  const session = {
    captureSnapshot,
    get cursor() {
      return { ...cursor };
    },
    engineId: ENGINE_ID,
    sessionEpoch: 7n,
  } as Pick<TerminalSession, "captureSnapshot" | "cursor" | "engineId" | "sessionEpoch">;
  const publishSnapshot =
    publish ??
    vi.fn<SnapshotPublisherLike["publish"]>(async (snapshot) => ({
      metadata: metadata(snapshot, captureSnapshot.mock.calls.length),
    }));
  const checkpointManager = new SnapshotCheckpointManager({
    publisher: { publish: publishSnapshot },
    session,
    sessionId: SESSION_ID,
  });
  const owner = new SnapshotRefreshOwner({
    checkpointManager,
    refreshIntervalMs: 100,
    retryIntervalMs: 10,
    session,
  });
  return {
    captureSnapshot,
    owner,
    publishSnapshot,
    setCursor(next: ReplayCursor) {
      cursor = { ...next };
    },
  };
}

function metadata(snapshot: SnapshotCapture, index: number) {
  return {
    sessionId: SESSION_ID,
    snapshotId: `snapshot_${index.toString().padStart(8, "A")}`,
    engineId: snapshot.engineId,
    sessionEpoch: snapshot.sessionEpoch.toString(),
    cutEventSeq: snapshot.cutEventSeq.toString(),
    nextPtyOffset: snapshot.nextPtyOffset.toString(),
    compression: "zstd" as const,
    compressedLength: "1",
    uncompressedLength: snapshot.bytes.byteLength.toString(),
    sha256: "0".repeat(64),
  };
}

async function settle(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

describe("SnapshotRefreshOwner", () => {
  it("primes immediately and publishes a later authority cut exactly once", async () => {
    const target = harness();

    target.owner.start();
    await settle();
    expect(target.captureSnapshot).toHaveBeenCalledOnce();
    expect(target.publishSnapshot).toHaveBeenCalledOnce();

    target.setCursor({ sessionEpoch: 7n, lastEventSeq: 1n, nextPtyOffset: 1n });
    await vi.advanceTimersByTimeAsync(100);
    await settle();
    expect(target.captureSnapshot).toHaveBeenCalledTimes(2);
    expect(target.publishSnapshot).toHaveBeenCalledTimes(2);

    await vi.advanceTimersByTimeAsync(100);
    await settle();
    expect(target.captureSnapshot).toHaveBeenCalledTimes(2);
    expect(target.publishSnapshot).toHaveBeenCalledTimes(2);

    target.owner.dispose();
  });

  it("retries a failed prime and stops all future refreshes when disposed", async () => {
    let attempts = 0;
    const publish = vi.fn<SnapshotPublisherLike["publish"]>(async (snapshot) => {
      attempts += 1;
      if (attempts === 1) throw new Error("injected upload failure");
      return { metadata: metadata(snapshot, attempts) };
    });
    const target = harness(publish);

    target.owner.start();
    await settle();
    expect(publish).toHaveBeenCalledOnce();

    await vi.advanceTimersByTimeAsync(10);
    await settle();
    expect(publish).toHaveBeenCalledTimes(2);

    target.owner.dispose();
    target.setCursor({ sessionEpoch: 7n, lastEventSeq: 2n, nextPtyOffset: 2n });
    await vi.advanceTimersByTimeAsync(1_000);
    expect(publish).toHaveBeenCalledTimes(2);
  });
});
