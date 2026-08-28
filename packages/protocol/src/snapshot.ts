import { z } from "zod";
import { DecimalU64Schema, PositiveDecimalU64Schema } from "./scalars";

export const MAX_SNAPSHOT_COMPRESSED_BYTES = 32 * 1024 * 1024;
export const MAX_SNAPSHOT_UNCOMPRESSED_BYTES = 128 * 1024 * 1024;
export const SNAPSHOT_MEDIA_TYPE = "application/vnd.ghostty.snapshot";

export const SnapshotHeader = {
  compression: "x-zhongduan-compression",
  cutEventSeq: "x-zhongduan-cut-event-seq",
  engineId: "x-zhongduan-engine-id",
  nextPtyOffset: "x-zhongduan-next-pty-offset",
  sessionEpoch: "x-zhongduan-session-epoch",
  sha256: "x-zhongduan-sha256",
  compressedLength: "x-zhongduan-compressed-length",
  uncompressedLength: "x-zhongduan-uncompressed-length",
} as const;

export const SnapshotResourceIdSchema = z.string().regex(/^[A-Za-z0-9_-]{16,128}$/);
const boundedLength = (maximum: number) =>
  PositiveDecimalU64Schema.refine((value) => BigInt(value) <= BigInt(maximum), {
    message: `must not exceed ${maximum} bytes`,
  });

export const SnapshotContentMetadataSchema = z
  .strictObject({
    engineId: z.string().min(1).max(512),
    sessionEpoch: PositiveDecimalU64Schema,
    cutEventSeq: DecimalU64Schema,
    nextPtyOffset: DecimalU64Schema,
    compression: z.enum(["none", "zstd"]),
    compressedLength: boundedLength(MAX_SNAPSHOT_COMPRESSED_BYTES),
    uncompressedLength: boundedLength(MAX_SNAPSHOT_UNCOMPRESSED_BYTES),
    sha256: z.string().regex(/^[a-f0-9]{64}$/),
  })
  .refine(
    (metadata) =>
      metadata.compression !== "none" || metadata.compressedLength === metadata.uncompressedLength,
    {
      message: "uncompressed snapshots must have equal lengths",
      path: ["uncompressedLength"],
    },
  );

export const SnapshotMetadataSchema = SnapshotContentMetadataSchema.safeExtend({
  sessionId: SnapshotResourceIdSchema,
  snapshotId: SnapshotResourceIdSchema,
});

export type SnapshotContentMetadata = z.infer<typeof SnapshotContentMetadataSchema>;
export type SnapshotMetadata = z.infer<typeof SnapshotMetadataSchema>;
