import {
  SNAPSHOT_MEDIA_TYPE,
  SnapshotHeader,
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

export function parseSnapshotUploadMetadata(
  request: Request,
  sessionId: string,
  snapshotId: string,
): SnapshotMetadata | undefined {
  if (
    request.headers.get("content-type") !== SNAPSHOT_MEDIA_TYPE ||
    request.headers.has("content-encoding")
  ) {
    return undefined;
  }
  const parsed = SnapshotMetadataSchema.safeParse({
    sessionId,
    snapshotId,
    engineId: request.headers.get(SnapshotHeader.engineId),
    sessionEpoch: request.headers.get(SnapshotHeader.sessionEpoch),
    cutEventSeq: request.headers.get(SnapshotHeader.cutEventSeq),
    nextPtyOffset: request.headers.get(SnapshotHeader.nextPtyOffset),
    compression: request.headers.get(SnapshotHeader.compression),
    compressedLength: request.headers.get(SnapshotHeader.compressedLength),
    uncompressedLength: request.headers.get(SnapshotHeader.uncompressedLength),
    sha256: request.headers.get(SnapshotHeader.sha256),
  });
  if (!parsed.success || request.headers.get("content-length") !== parsed.data.compressedLength) {
    return undefined;
  }
  return parsed.data;
}

export function snapshotUploadHeaders(metadata: SnapshotMetadata): Headers {
  return new Headers({
    "content-length": metadata.compressedLength,
    "content-type": SNAPSHOT_MEDIA_TYPE,
    [SnapshotHeader.compression]: metadata.compression,
    [SnapshotHeader.compressedLength]: metadata.compressedLength,
    [SnapshotHeader.cutEventSeq]: metadata.cutEventSeq,
    [SnapshotHeader.engineId]: metadata.engineId,
    [SnapshotHeader.nextPtyOffset]: metadata.nextPtyOffset,
    [SnapshotHeader.sessionEpoch]: metadata.sessionEpoch,
    [SnapshotHeader.sha256]: metadata.sha256,
    [SnapshotHeader.uncompressedLength]: metadata.uncompressedLength,
  });
}

export function snapshotAttemptObjectKey(
  sessionId: string,
  snapshotId: string,
  attemptId: string,
): string {
  return `v1/sessions/${sessionId}/snapshots/${snapshotId}/attempts/${attemptId}.bin`;
}

export function isSnapshotAttemptObjectKey(
  objectKey: string,
  sessionId: string,
  snapshotId: string,
): boolean {
  const prefix = `v1/sessions/${sessionId}/snapshots/${snapshotId}/attempts/`;
  if (!objectKey.startsWith(prefix) || !objectKey.endsWith(".bin")) return false;
  return /^[A-Za-z0-9_-]{16,128}$/u.test(objectKey.slice(prefix.length, -4));
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

export function matchesMultipartPartEtag(etag: string, expectedMd5: string): boolean {
  return etag.replaceAll('"', "").toLowerCase() === expectedMd5;
}

export function matchesSnapshotObject(object: R2Object, metadata: SnapshotMetadata): boolean {
  const expectedCustomMetadata = snapshotCustomMetadata(metadata);
  const actualCustomMetadata = object.customMetadata ?? {};
  const checksum = object.checksums.sha256;
  const checksumMatches =
    object.etag.endsWith("-1") &&
    (checksum === undefined || bytesToHex(checksum) === metadata.sha256);
  return (
    object.size === Number(metadata.compressedLength) &&
    object.httpMetadata?.contentType === SNAPSHOT_MEDIA_TYPE &&
    object.httpMetadata.cacheControl === "private, no-store" &&
    checksumMatches &&
    Object.keys(actualCustomMetadata).length === Object.keys(expectedCustomMetadata).length &&
    Object.entries(expectedCustomMetadata).every(([key, value]) => {
      return actualCustomMetadata[key] === value;
    })
  );
}
