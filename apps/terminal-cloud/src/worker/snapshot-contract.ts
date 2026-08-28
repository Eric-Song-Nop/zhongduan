import {
  SNAPSHOT_MEDIA_TYPE,
  SnapshotMetadataSchema,
  type SnapshotMetadata,
} from "@zhongduan/protocol";
import { z } from "zod";

export const FinalizedSnapshotSchema = SnapshotMetadataSchema.safeExtend({
  objectKey: z.string().min(1).max(1024),
  r2Version: z.string().min(1).max(256),
  etag: z.string().min(1).max(256),
});

export type FinalizedSnapshot = z.infer<typeof FinalizedSnapshotSchema>;

export function snapshotObjectKey(sessionId: string, snapshotId: string): string {
  return `v1/sessions/${sessionId}/snapshots/${snapshotId}.bin`;
}

export function snapshotCustomMetadata(metadata: SnapshotMetadata): Record<string, string> {
  return {
    compression: metadata.compression,
    cutEventSeq: metadata.cutEventSeq,
    engineId: metadata.engineId,
    nextPtyOffset: metadata.nextPtyOffset,
    sessionEpoch: metadata.sessionEpoch,
    sessionId: metadata.sessionId,
    sha256: metadata.sha256,
    snapshotId: metadata.snapshotId,
    compressedLength: metadata.compressedLength,
    uncompressedLength: metadata.uncompressedLength,
  };
}

export function hexToBytes(hex: string): Uint8Array {
  if (!/^[a-f0-9]{64}$/u.test(hex)) throw new RangeError("invalid SHA-256 digest");
  return Uint8Array.from({ length: 32 }, (_, index) =>
    Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16),
  );
}

export function bytesToHex(bytes: ArrayBuffer | ArrayBufferView): string {
  const view =
    bytes instanceof ArrayBuffer
      ? new Uint8Array(bytes)
      : new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return Array.from(view, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

export function matchesSnapshotObject(object: R2Object, metadata: SnapshotMetadata): boolean {
  const expectedCustomMetadata = snapshotCustomMetadata(metadata);
  const actualCustomMetadata = object.customMetadata ?? {};
  const checksum = object.checksums.sha256;
  return (
    object.size === Number(metadata.compressedLength) &&
    object.httpMetadata?.contentType === SNAPSHOT_MEDIA_TYPE &&
    object.httpMetadata.cacheControl === "private, no-store" &&
    checksum !== undefined &&
    bytesToHex(checksum) === metadata.sha256 &&
    Object.keys(actualCustomMetadata).length === Object.keys(expectedCustomMetadata).length &&
    Object.entries(expectedCustomMetadata).every(([key, value]) => {
      return actualCustomMetadata[key] === value;
    })
  );
}
