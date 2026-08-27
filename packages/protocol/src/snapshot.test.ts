import { describe, expect, it } from "vitest";
import {
  MAX_SNAPSHOT_COMPRESSED_BYTES,
  MAX_SNAPSHOT_UNCOMPRESSED_BYTES,
  SNAPSHOT_MEDIA_TYPE,
  SnapshotHeader,
  SnapshotMetadataSchema,
} from "./snapshot";

const baseMetadata = {
  sessionId: "session_000000000001",
  snapshotId: "snapshot_0000000001",
  engineId: "ghostty:test+snapshot-v1+wterm:test",
  sessionEpoch: "7",
  cutEventSeq: "10",
  nextPtyOffset: "100",
  compression: "zstd" as const,
  compressedLength: "1024",
  uncompressedLength: "4096",
  sha256: "a".repeat(64),
};

describe("snapshot wire contract", () => {
  it("fixes the public media type, budget, and header names", () => {
    expect(SNAPSHOT_MEDIA_TYPE).toBe("application/vnd.ghostty.snapshot");
    expect(MAX_SNAPSHOT_COMPRESSED_BYTES).toBe(32 * 1024 * 1024);
    expect(MAX_SNAPSHOT_UNCOMPRESSED_BYTES).toBe(128 * 1024 * 1024);
    expect(SnapshotHeader).toEqual({
      compression: "x-zhongduan-compression",
      cutEventSeq: "x-zhongduan-cut-event-seq",
      engineId: "x-zhongduan-engine-id",
      nextPtyOffset: "x-zhongduan-next-pty-offset",
      sessionEpoch: "x-zhongduan-session-epoch",
      sha256: "x-zhongduan-sha256",
      compressedLength: "x-zhongduan-compressed-length",
      uncompressedLength: "x-zhongduan-uncompressed-length",
    });
  });

  it("accepts the exact compressed and decompressed limits", () => {
    expect(
      SnapshotMetadataSchema.parse({
        ...baseMetadata,
        compressedLength: MAX_SNAPSHOT_COMPRESSED_BYTES.toString(),
        uncompressedLength: MAX_SNAPSHOT_UNCOMPRESSED_BYTES.toString(),
      }),
    ).toMatchObject({ compression: "zstd" });
  });

  it("rejects overflow, noncanonical decimals, and unknown fields", () => {
    expect(
      SnapshotMetadataSchema.safeParse({
        ...baseMetadata,
        compressedLength: (MAX_SNAPSHOT_COMPRESSED_BYTES + 1).toString(),
      }).success,
    ).toBe(false);
    expect(
      SnapshotMetadataSchema.safeParse({
        ...baseMetadata,
        uncompressedLength: (MAX_SNAPSHOT_UNCOMPRESSED_BYTES + 1).toString(),
      }).success,
    ).toBe(false);
    expect(SnapshotMetadataSchema.safeParse({ ...baseMetadata, cutEventSeq: "01" }).success).toBe(
      false,
    );
    expect(
      SnapshotMetadataSchema.safeParse({ ...baseMetadata, objectKey: "private" }).success,
    ).toBe(false);
  });

  it("requires uncompressed snapshots to declare the same wire and restored lengths", () => {
    expect(
      SnapshotMetadataSchema.safeParse({
        ...baseMetadata,
        compression: "none",
        compressedLength: "10",
        uncompressedLength: "11",
      }).success,
    ).toBe(false);
    expect(
      SnapshotMetadataSchema.parse({
        ...baseMetadata,
        compression: "none",
        compressedLength: "10",
        uncompressedLength: "10",
      }),
    ).toMatchObject({ compression: "none" });
  });
});
