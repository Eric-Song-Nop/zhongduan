import type { RetiredSnapshotObject } from "./snapshot-store";

interface SnapshotObjectDeleter {
  delete(object: string | string[]): Promise<void>;
  head(object: string): Promise<object | null>;
}

export interface SnapshotDeletionResult {
  deleted: readonly RetiredSnapshotObject[];
  failed: readonly RetiredSnapshotObject[];
}

export async function deleteRetiredSnapshotObjects(
  bucket: SnapshotObjectDeleter,
  snapshots: readonly RetiredSnapshotObject[],
): Promise<SnapshotDeletionResult> {
  const outcomes = await Promise.all(
    snapshots.map(async (snapshot) => {
      try {
        if (snapshot.source !== "snapshot") {
          if ((await bucket.head(snapshot.objectKey)) !== null) {
            await bucket.delete(snapshot.objectKey);
          }
          if ((await bucket.head(snapshot.objectKey)) !== null) {
            throw new Error("retired snapshot upload object remained after delete");
          }
        } else {
          await bucket.delete(snapshot.objectKey);
        }
        return { deleted: true as const, snapshot };
      } catch {
        return { deleted: false as const, snapshot };
      }
    }),
  );
  return {
    deleted: outcomes.filter((outcome) => outcome.deleted).map((outcome) => outcome.snapshot),
    failed: outcomes.filter((outcome) => !outcome.deleted).map((outcome) => outcome.snapshot),
  };
}
