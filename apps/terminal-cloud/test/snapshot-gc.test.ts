import { SNAPSHOT_MEDIA_TYPE } from "@zhongduan/protocol";
import { describe, expect, it } from "vitest";
import {
  hexToBytes,
  matchesMultipartPartEtag,
  matchesSnapshotObject,
  snapshotCustomMetadata,
} from "../src/worker/snapshot-contract";
import { deleteRetiredSnapshotObjects } from "../src/worker/snapshot-gc";
import type { RetiredSnapshotObject } from "../src/worker/snapshot-store";
import { metadataFor } from "./snapshot-test-helpers";

describe("snapshot object garbage collection", () => {
  it("accepts only the exact R2 part MD5 ETag", () => {
    const md5 = "6299cb25ddbb459b292f07a154fef4ed";
    expect(matchesMultipartPartEtag(md5, md5)).toBe(true);
    expect(matchesMultipartPartEtag(`"${md5.toUpperCase()}"`, md5)).toBe(true);
    expect(matchesMultipartPartEtag("opaque-r2-part-etag", md5)).toBe(false);
  });

  it("requires a multipart ETag even when R2 exposes a SHA-256 checksum", async () => {
    const metadata = await metadataFor(
      {
        hostCapability: "host-capability",
        observerCapability: "observer-capability",
        sessionId: "session_000000000001",
        writerCapability: "writer-capability",
      },
      "snapshot_000000000001",
    );
    const object = {
      checksums: { sha256: hexToBytes(metadata.sha256).buffer },
      customMetadata: snapshotCustomMetadata(metadata),
      etag: "single-part-etag",
      httpMetadata: {
        cacheControl: "private, no-store",
        contentType: SNAPSHOT_MEDIA_TYPE,
      },
      size: Number(metadata.compressedLength),
    };

    expect(matchesSnapshotObject(object as R2Object, metadata)).toBe(false);
    expect(
      matchesSnapshotObject({ ...object, etag: "multipart-etag-1" } as R2Object, metadata),
    ).toBe(true);
  });

  it("isolates failed deletes so a later alarm can retry the exact tombstone", async () => {
    const candidates: RetiredSnapshotObject[] = [
      {
        source: "snapshot",
        snapshotId: "snapshot_retired_000001",
        objectKey: "snapshots/retired.bin",
        r2Version: "version-retired",
      },
      {
        source: "upload",
        snapshotId: "snapshot_upload_0000001",
        objectKey: "snapshots/upload.bin",
      },
    ];
    const attempts = new Map<string, number>();
    const objects = new Set(candidates.map((candidate) => candidate.objectKey));
    const bucket = {
      async head(key: string): Promise<object | null> {
        return objects.has(key) ? {} : null;
      },
      async delete(key: string | string[]): Promise<void> {
        const objectKey = String(key);
        const count = (attempts.get(objectKey) ?? 0) + 1;
        attempts.set(objectKey, count);
        if (objectKey.endsWith("upload.bin") && count === 1) {
          throw new Error("injected R2 outage");
        }
        objects.delete(objectKey);
      },
    };

    const first = await deleteRetiredSnapshotObjects(bucket, candidates);
    expect(first.deleted.map((snapshot) => snapshot.snapshotId)).toEqual([
      "snapshot_retired_000001",
    ]);
    expect(first.failed.map((snapshot) => snapshot.snapshotId)).toEqual([
      "snapshot_upload_0000001",
    ]);

    const retry = await deleteRetiredSnapshotObjects(bucket, first.failed);
    expect(retry.failed).toEqual([]);
    expect(retry.deleted.map((snapshot) => snapshot.snapshotId)).toEqual([
      "snapshot_upload_0000001",
    ]);
    expect(attempts.get("snapshots/retired.bin")).toBe(1);
    expect(attempts.get("snapshots/upload.bin")).toBe(2);
  });
});
