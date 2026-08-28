import { SnapshotMetadataSchema, type SnapshotMetadata } from "@zhongduan/protocol";
import { randomId } from "./auth";
import {
  FinalizedSnapshotSchema,
  isSnapshotObjectKey,
  snapshotAttemptObjectKey,
  type FinalizedSnapshot,
} from "./snapshot-contract";
import type { RelayStore, SessionRow } from "./relay-store";

interface SnapshotRow {
  compressed_length: number;
  compression: "none" | "zstd";
  created_at: number;
  cut_event_seq: string;
  engine_id: string;
  etag: string;
  next_pty_offset: string;
  object_key: string;
  r2_version: string;
  session_epoch: string;
  sha256: string;
  snapshot_id: string;
  state: "servable" | "retired";
  uncompressed_length: string;
  upload_kind: FinalizedSnapshot["uploadKind"];
}

interface SnapshotScanRow {
  scan_rowid: number;
  snapshot_id: string;
  state: SnapshotRow["state"];
}

interface SnapshotRecentGraceState {
  invalidStoredId: boolean;
  recentIds: string[];
}

export interface SnapshotUploadRecord {
  created_at: number;
  etag: string | null;
  expires_at: number;
  metadata_json: string;
  object_key: string;
  part_etag: string | null;
  r2_version: string | null;
  snapshot_id: string;
  state:
    | "preparing"
    | "uploading"
    | "completing"
    | "uncertain"
    | "aborted"
    | "completed"
    | "retired";
  upload_id: string | null;
}

export type RetiredSnapshotObject =
  | {
      objectKey: string;
      r2Version: string;
      snapshotId: string;
      source: "snapshot";
    }
  | {
      objectKey: string;
      snapshotId: string;
      source: "upload";
    };

export const SNAPSHOT_RECENT_GRACE_LIMIT = 2;
export const SNAPSHOT_OBJECT_RECORD_LIMIT = 32;
export const SNAPSHOT_UPLOAD_RESERVATION_LIMIT = 4;
export const SNAPSHOT_UPLOAD_RESERVATION_MS = 10 * 60_000;
export const SNAPSHOT_MAINTENANCE_BATCH_LIMIT = 32;

export type ReserveSnapshotUploadResult =
  | { ok: true; created: false; snapshot: FinalizedSnapshot; state: "published" }
  | {
      ok: true;
      created: boolean;
      expiresAt: number;
      state: "started";
      upload: SnapshotUploadRecord;
    }
  | { ok: false; reason: "conflict" | "retention-backlog" | "session-mismatch" };

export interface RecoverableMultipartUpload {
  metadata: SnapshotMetadata;
  objectKey: string;
  partEtag: string | null;
  snapshotId: string;
  state: "uploading" | "completing" | "uncertain" | "aborted";
  uploadId: string;
}

export type FinalizeSnapshotResult =
  | {
      ok: true;
      created: boolean;
      retired: readonly RetiredSnapshotObject[];
      snapshot: FinalizedSnapshot;
    }
  | {
      ok: false;
      reason:
        | "attempt-changed"
        | "conflict"
        | "cursor-ahead"
        | "retention-backlog"
        | "session-mismatch";
    };

export interface ReconcileSnapshotRetentionResult {
  retired: readonly RetiredSnapshotObject[];
}

function descriptor(row: SnapshotRow, sessionId: string): FinalizedSnapshot {
  return FinalizedSnapshotSchema.parse({
    sessionId,
    snapshotId: row.snapshot_id,
    engineId: row.engine_id,
    sessionEpoch: row.session_epoch,
    cutEventSeq: row.cut_event_seq,
    nextPtyOffset: row.next_pty_offset,
    compression: row.compression,
    compressedLength: row.compressed_length.toString(),
    uncompressedLength: row.uncompressed_length,
    sha256: row.sha256,
    objectKey: row.object_key,
    r2Version: row.r2_version,
    etag: row.etag,
    uploadKind: row.upload_kind,
  });
}

function metadataOf(snapshot: FinalizedSnapshot): SnapshotMetadata {
  return SnapshotMetadataSchema.parse({
    sessionId: snapshot.sessionId,
    snapshotId: snapshot.snapshotId,
    engineId: snapshot.engineId,
    sessionEpoch: snapshot.sessionEpoch,
    cutEventSeq: snapshot.cutEventSeq,
    nextPtyOffset: snapshot.nextPtyOffset,
    compression: snapshot.compression,
    compressedLength: snapshot.compressedLength,
    uncompressedLength: snapshot.uncompressedLength,
    sha256: snapshot.sha256,
  });
}

function metadataJson(metadata: SnapshotMetadata): string {
  return JSON.stringify(SnapshotMetadataSchema.parse(metadata));
}

function uploadMetadata(row: SnapshotUploadRecord): SnapshotMetadata {
  return SnapshotMetadataSchema.parse(JSON.parse(row.metadata_json));
}

function recoverableUpload(row: SnapshotUploadRecord): RecoverableMultipartUpload | undefined {
  if (
    row.upload_id === null ||
    (row.state !== "uploading" &&
      row.state !== "completing" &&
      row.state !== "uncertain" &&
      row.state !== "aborted")
  ) {
    return undefined;
  }
  return {
    metadata: uploadMetadata(row),
    objectKey: row.object_key,
    partEtag: row.part_etag,
    snapshotId: row.snapshot_id,
    state: row.state,
    uploadId: row.upload_id,
  };
}

function exactSnapshotMetadata(row: SnapshotRow, snapshot: SnapshotMetadata): boolean {
  return (
    row.snapshot_id === snapshot.snapshotId &&
    row.session_epoch === snapshot.sessionEpoch &&
    row.cut_event_seq === snapshot.cutEventSeq &&
    row.next_pty_offset === snapshot.nextPtyOffset &&
    row.engine_id === snapshot.engineId &&
    row.sha256 === snapshot.sha256 &&
    row.compressed_length === Number(snapshot.compressedLength) &&
    row.uncompressed_length === snapshot.uncompressedLength &&
    row.compression === snapshot.compression
  );
}

function exactSnapshot(row: SnapshotRow, snapshot: FinalizedSnapshot): boolean {
  return (
    exactSnapshotMetadata(row, metadataOf(snapshot)) &&
    row.object_key === snapshot.objectKey &&
    row.r2_version === snapshot.r2Version &&
    row.etag === snapshot.etag &&
    row.upload_kind === snapshot.uploadKind
  );
}

function sameSnapshot(row: SnapshotRow, snapshot: FinalizedSnapshot): boolean {
  return row.state === "servable" && exactSnapshot(row, snapshot);
}

function retiredSnapshot(row: SnapshotRow): RetiredSnapshotObject {
  return {
    source: "snapshot",
    snapshotId: row.snapshot_id,
    objectKey: row.object_key,
    r2Version: row.r2_version,
  };
}

export class SnapshotStore {
  readonly rowLimit: number;
  readonly #recentCandidateLimit: number;

  constructor(
    private readonly state: DurableObjectState,
    private readonly sql: SqlStorage,
    private readonly relayStore: RelayStore,
    maxPinnedSnapshots: number,
  ) {
    const protectedRows = maxPinnedSnapshots + 1 + SNAPSHOT_RECENT_GRACE_LIMIT;
    if (protectedRows + SNAPSHOT_UPLOAD_RESERVATION_LIMIT > SNAPSHOT_OBJECT_RECORD_LIMIT) {
      throw new RangeError("snapshot record limit cannot preserve all browser pins");
    }
    this.rowLimit = SNAPSHOT_OBJECT_RECORD_LIMIT;
    this.#recentCandidateLimit = protectedRows + SNAPSHOT_UPLOAD_RESERVATION_LIMIT;
  }

  published(snapshotId: string): FinalizedSnapshot | undefined {
    const session = this.relayStore.session();
    if (session === undefined) return undefined;
    const row = this.#row(snapshotId);
    return row?.state === "servable" ? descriptor(row, session.session_id) : undefined;
  }

  refreshPublishedExact(
    snapshot: FinalizedSnapshot,
    pinnedSnapshotIds: ReadonlySet<string>,
  ): FinalizedSnapshot | undefined {
    let refreshed: FinalizedSnapshot | undefined;
    this.state.storage.transactionSync(() => {
      const session = this.relayStore.session();
      const row = this.#row(snapshot.snapshotId);
      if (session === undefined || row === undefined || !sameSnapshot(row, snapshot)) return;
      this.#touchRecentSnapshot(session, snapshot.snapshotId, pinnedSnapshotIds);
      const current = this.#row(snapshot.snapshotId);
      if (current !== undefined && sameSnapshot(current, snapshot)) {
        refreshed = descriptor(current, session.session_id);
      }
    });
    return refreshed;
  }

  beginUpload(input: SnapshotMetadata, now: number): ReserveSnapshotUploadResult {
    const snapshot = SnapshotMetadataSchema.parse(input);
    let result: ReserveSnapshotUploadResult | undefined;
    this.state.storage.transactionSync(() => {
      const session = this.relayStore.session();
      if (
        session === undefined ||
        session.session_id !== snapshot.sessionId ||
        session.session_epoch !== snapshot.sessionEpoch ||
        session.engine_id !== snapshot.engineId
      ) {
        result = { ok: false, reason: "session-mismatch" };
        return;
      }

      const existing = this.#row(snapshot.snapshotId);
      if (existing !== undefined) {
        result = !exactSnapshotMetadata(existing, snapshot)
          ? { ok: false, reason: "conflict" }
          : existing.state === "servable"
            ? {
                ok: true,
                created: false,
                state: "published",
                snapshot: descriptor(existing, session.session_id),
              }
            : { ok: false, reason: "retention-backlog" };
        return;
      }

      const existingUpload = this.#upload(snapshot.snapshotId);
      if (existingUpload !== undefined) {
        const exactUpload =
          existingUpload.metadata_json === metadataJson(snapshot) &&
          isSnapshotObjectKey(existingUpload.object_key, snapshot.sessionId, snapshot.snapshotId);
        if (
          (existingUpload.state === "preparing" || existingUpload.state === "retired") &&
          exactUpload
        ) {
          result = { ok: false, reason: "retention-backlog" };
          return;
        }
        if (!exactUpload) {
          result = { ok: false, reason: "conflict" };
          return;
        }
        const expiresAt = now + SNAPSHOT_UPLOAD_RESERVATION_MS;
        this.sql.exec(
          `UPDATE snapshot_upload
           SET expires_at = ?
          WHERE snapshot_id = ?
            AND state IN ('uploading', 'completing', 'uncertain', 'aborted', 'completed')`,
          expiresAt,
          snapshot.snapshotId,
        );
        result = {
          ok: true,
          created: false,
          state: "started",
          expiresAt,
          upload: { ...existingUpload, expires_at: expiresAt },
        };
        return;
      }

      if (
        session.snapshot_retention_backlog !== 0 ||
        this.#boundedRowCount(["snapshot_upload"], SNAPSHOT_UPLOAD_RESERVATION_LIMIT) >=
          SNAPSHOT_UPLOAD_RESERVATION_LIMIT ||
        this.#boundedRowCount(["snapshot", "snapshot_upload"], this.rowLimit) >= this.rowLimit
      ) {
        result = { ok: false, reason: "retention-backlog" };
        return;
      }

      const objectKey = snapshotAttemptObjectKey(
        snapshot.sessionId,
        snapshot.snapshotId,
        randomId(),
      );
      const expiresAt = now + SNAPSHOT_UPLOAD_RESERVATION_MS;
      this.sql.exec(
        `INSERT INTO snapshot_upload
          (snapshot_id, object_key, metadata_json, state, upload_id, part_etag,
           r2_version, etag, created_at, expires_at)
         VALUES (?, ?, ?, 'preparing', NULL, NULL, NULL, NULL, ?, ?)`,
        snapshot.snapshotId,
        objectKey,
        metadataJson(snapshot),
        now,
        expiresAt,
      );
      result = {
        ok: true,
        created: true,
        state: "started",
        expiresAt,
        upload: this.#upload(snapshot.snapshotId)!,
      };
    });
    if (result === undefined) throw new Error("snapshot upload transaction did not resolve");
    return result;
  }

  recordMultipartUpload(snapshotId: string, objectKey: string, uploadId: string): boolean {
    this.sql.exec(
      `UPDATE snapshot_upload
       SET state = 'uploading', upload_id = ?
       WHERE snapshot_id = ? AND object_key = ? AND state = 'preparing'`,
      uploadId,
      snapshotId,
      objectKey,
    );
    const upload = this.#upload(snapshotId);
    return (
      upload?.state === "uploading" &&
      upload.object_key === objectKey &&
      upload.upload_id === uploadId
    );
  }

  restartAfterMultipartAbort(
    snapshotId: string,
    objectKey: string,
    uploadId: string,
  ): SnapshotUploadRecord | undefined {
    const upload = this.#upload(snapshotId);
    if (
      upload?.state !== "aborted" ||
      upload.object_key !== objectKey ||
      upload.upload_id !== uploadId
    ) {
      return undefined;
    }
    const metadata = uploadMetadata(upload);
    const nextObjectKey = snapshotAttemptObjectKey(
      metadata.sessionId,
      metadata.snapshotId,
      randomId(),
    );
    this.sql.exec(
      `UPDATE snapshot_upload
       SET state = 'preparing', object_key = ?, upload_id = NULL, part_etag = NULL,
           r2_version = NULL, etag = NULL
       WHERE snapshot_id = ? AND object_key = ? AND upload_id = ?
         AND state = 'aborted'`,
      nextObjectKey,
      snapshotId,
      objectKey,
      uploadId,
    );
    const restarted = this.#upload(snapshotId);
    return restarted?.state === "preparing" && restarted.object_key === nextObjectKey
      ? restarted
      : undefined;
  }

  recordMultipartAborted(snapshotId: string, objectKey: string, uploadId: string): boolean {
    this.sql.exec(
      `UPDATE snapshot_upload
       SET state = 'aborted'
       WHERE snapshot_id = ? AND object_key = ? AND upload_id = ?
         AND state IN ('uploading', 'completing', 'uncertain')`,
      snapshotId,
      objectKey,
      uploadId,
    );
    const upload = this.#upload(snapshotId);
    return (
      upload?.state === "aborted" &&
      upload.object_key === objectKey &&
      upload.upload_id === uploadId
    );
  }

  recordMultipartUncertain(snapshotId: string, objectKey: string, uploadId: string): boolean {
    this.sql.exec(
      `UPDATE snapshot_upload
       SET state = 'uncertain'
       WHERE snapshot_id = ? AND object_key = ? AND upload_id = ?
         AND state IN ('uploading', 'completing', 'uncertain')`,
      snapshotId,
      objectKey,
      uploadId,
    );
    const upload = this.#upload(snapshotId);
    return (
      upload?.state === "uncertain" &&
      upload.object_key === objectKey &&
      upload.upload_id === uploadId
    );
  }

  recordMultipartCompleting(
    snapshotId: string,
    objectKey: string,
    uploadId: string,
    partEtag: string,
  ): boolean {
    this.sql.exec(
      `UPDATE snapshot_upload
       SET state = 'completing', part_etag = ?
       WHERE snapshot_id = ? AND object_key = ? AND upload_id = ? AND state = 'uploading'`,
      partEtag,
      snapshotId,
      objectKey,
      uploadId,
    );
    const upload = this.#upload(snapshotId);
    return (
      upload?.state === "completing" &&
      upload.object_key === objectKey &&
      upload.upload_id === uploadId &&
      upload.part_etag === partEtag
    );
  }

  recordMultipartCompleted(
    snapshotId: string,
    objectKey: string,
    uploadId: string,
    r2Version: string,
    etag: string,
  ): FinalizedSnapshot | undefined {
    this.sql.exec(
      `UPDATE snapshot_upload
       SET state = 'completed', r2_version = ?, etag = ?
       WHERE snapshot_id = ? AND object_key = ? AND upload_id = ?
         AND state IN ('completing', 'uncertain', 'completed')`,
      r2Version,
      etag,
      snapshotId,
      objectKey,
      uploadId,
    );
    const completed = this.#upload(snapshotId);
    if (
      completed?.state !== "completed" ||
      completed.object_key !== objectKey ||
      completed.upload_id !== uploadId ||
      completed.r2_version !== r2Version ||
      completed.etag !== etag
    ) {
      return undefined;
    }
    return this.completedUpload(snapshotId);
  }

  completedUpload(snapshotId: string): FinalizedSnapshot | undefined {
    const upload = this.#upload(snapshotId);
    if (upload?.state !== "completed" || upload.r2_version === null || upload.etag === null) {
      return undefined;
    }
    const metadata = uploadMetadata(upload);
    return FinalizedSnapshotSchema.parse({
      ...metadata,
      objectKey: upload.object_key,
      r2Version: upload.r2_version,
      etag: upload.etag,
      uploadKind: "multipart-verified",
    });
  }

  retirePreparedUpload(snapshotId: string, objectKey: string): void {
    this.sql.exec(
      `UPDATE snapshot_upload
       SET state = 'retired'
       WHERE snapshot_id = ? AND object_key = ? AND state = 'preparing'`,
      snapshotId,
      objectKey,
    );
  }

  deletePreparedUpload(snapshotId: string, objectKey: string): void {
    this.sql.exec(
      `DELETE FROM snapshot_upload
       WHERE snapshot_id = ? AND object_key = ? AND state = 'preparing'`,
      snapshotId,
      objectKey,
    );
  }

  deleteAbortedMultipart(snapshotId: string, objectKey: string, uploadId: string): boolean {
    let deleted = false;
    this.state.storage.transactionSync(() => {
      const upload = this.#upload(snapshotId);
      if (
        upload?.state !== "aborted" ||
        upload.object_key !== objectKey ||
        upload.upload_id !== uploadId ||
        this.#row(snapshotId) !== undefined
      ) {
        return;
      }
      this.sql.exec(
        `DELETE FROM snapshot_upload
         WHERE snapshot_id = ? AND object_key = ? AND upload_id = ?
           AND state = 'aborted'`,
        snapshotId,
        objectKey,
        uploadId,
      );
      deleted = this.#upload(snapshotId) === undefined && this.#row(snapshotId) === undefined;
    });
    return deleted;
  }

  recoverableMultipartUploads(
    activeUploadIds: ReadonlySet<string>,
    limit = SNAPSHOT_MAINTENANCE_BATCH_LIMIT,
  ): readonly RecoverableMultipartUpload[] {
    const activeIds = [...activeUploadIds];
    const rows = (activeIds.length === 0
      ? this.sql.exec(
          `SELECT * FROM snapshot_upload
             WHERE state IN ('uploading', 'completing', 'aborted')
             ORDER BY rowid ASC
             LIMIT ?`,
          limit,
        )
      : this.sql.exec(
          `SELECT * FROM snapshot_upload
             WHERE state IN ('uploading', 'completing', 'aborted')
               AND snapshot_id NOT IN (${activeIds.map(() => "?").join(", ")})
             ORDER BY rowid ASC
             LIMIT ?`,
          ...activeIds,
          limit,
        )
    ).toArray() as unknown as SnapshotUploadRecord[];
    return rows.flatMap((row) => {
      const upload = recoverableUpload(row);
      return upload === undefined ? [] : [upload];
    });
  }

  recoverableMultipartUpload(snapshotId: string): RecoverableMultipartUpload | undefined {
    const row = this.#upload(snapshotId);
    return row === undefined ? undefined : recoverableUpload(row);
  }

  retired(
    now: number,
    activeUploadIds: ReadonlySet<string> = new Set(),
    limit = SNAPSHOT_MAINTENANCE_BATCH_LIMIT,
  ): readonly RetiredSnapshotObject[] {
    if (limit <= 0) return [];
    const existing = this.#retiredObjects(activeUploadIds, limit);
    if (existing.length >= limit) return existing;

    const transitionLimit = limit - existing.length;
    this.state.storage.transactionSync(() => {
      const activeIds = [...activeUploadIds];
      const inactiveClause =
        activeIds.length === 0
          ? ""
          : ` AND snapshot_id NOT IN (${activeIds.map(() => "?").join(", ")})`;
      const expired = this.sql
        .exec(
          `SELECT snapshot_id, object_key FROM snapshot_upload
           WHERE state IN ('preparing', 'completed') AND expires_at <= ?${inactiveClause}
           ORDER BY rowid ASC
           LIMIT ?`,
          now,
          ...activeIds,
          transitionLimit,
        )
        .toArray() as unknown as Pick<SnapshotUploadRecord, "object_key" | "snapshot_id">[];
      for (const upload of expired) {
        this.sql.exec(
          `UPDATE snapshot_upload SET state = 'retired'
           WHERE snapshot_id = ? AND object_key = ?
             AND state IN ('preparing', 'completed')`,
          upload.snapshot_id,
          upload.object_key,
        );
      }

      const remaining = transitionLimit - expired.length;
      if (remaining <= 0) return;
      const uploadCount = this.#boundedRowCount(
        ["snapshot_upload"],
        SNAPSHOT_UPLOAD_RESERVATION_LIMIT + remaining,
      );
      const overflow = Math.min(
        remaining,
        Math.max(0, uploadCount - SNAPSHOT_UPLOAD_RESERVATION_LIMIT),
      );
      if (overflow === 0) return;
      const legacyOverflow = this.sql
        .exec(
          `SELECT snapshot_id, object_key FROM snapshot_upload
           WHERE state = 'preparing'${inactiveClause}
           ORDER BY rowid ASC
           LIMIT ?`,
          ...activeIds,
          overflow,
        )
        .toArray() as unknown as Pick<SnapshotUploadRecord, "object_key" | "snapshot_id">[];
      for (const upload of legacyOverflow) {
        this.sql.exec(
          `UPDATE snapshot_upload SET state = 'retired'
           WHERE snapshot_id = ? AND object_key = ? AND state = 'preparing'`,
          upload.snapshot_id,
          upload.object_key,
        );
      }
    });
    return this.#retiredObjects(activeUploadIds, limit);
  }

  nextUploadExpiry(activeUploadIds: ReadonlySet<string> = new Set()): number | undefined {
    const activeIds = [...activeUploadIds];
    const row = (
      activeIds.length === 0
        ? this.sql.exec(
            `SELECT MIN(expires_at) AS value
             FROM snapshot_upload
             WHERE state IN ('preparing', 'uploading', 'completing', 'aborted', 'completed')`,
          )
        : this.sql.exec(
            `SELECT MIN(expires_at) AS value
             FROM snapshot_upload
             WHERE state IN ('preparing', 'uploading', 'completing', 'aborted', 'completed')
               AND snapshot_id NOT IN (${activeIds.map(() => "?").join(", ")})`,
            ...activeIds,
          )
    ).one();
    return row.value === null ? undefined : Number(row.value);
  }

  deleteRetired(snapshot: RetiredSnapshotObject): void {
    if (snapshot.source === "upload") {
      this.sql.exec(
        `DELETE FROM snapshot_upload
         WHERE snapshot_id = ? AND object_key = ? AND state = 'retired'`,
        snapshot.snapshotId,
        snapshot.objectKey,
      );
      return;
    }
    this.sql.exec(
      `DELETE FROM snapshot
       WHERE snapshot_id = ? AND object_key = ? AND r2_version = ? AND state = 'retired'`,
      snapshot.snapshotId,
      snapshot.objectKey,
      snapshot.r2Version,
    );
  }

  needsMaintenance(
    now: number,
    pinnedSnapshotIds: ReadonlySet<string>,
    activeSnapshotIds: ReadonlySet<string>,
  ): boolean {
    const session = this.relayStore.session();
    if (session?.snapshot_retention_backlog !== 0) return session !== undefined;
    const activeIds = [...activeSnapshotIds];
    const inactiveClause =
      activeIds.length === 0
        ? ""
        : ` AND snapshot_id NOT IN (${activeIds.map(() => "?").join(", ")})`;
    const exists = (table: "snapshot_upload", predicate: string, ...values: SqlStorageValue[]) =>
      this.sql
        .exec(
          `SELECT 1 AS value FROM ${table} WHERE ${predicate}${inactiveClause} LIMIT 1`,
          ...values,
          ...activeIds,
        )
        .toArray().length > 0;
    if (
      exists("snapshot_upload", "state = 'retired'") ||
      exists("snapshot_upload", "state IN ('preparing', 'completed') AND expires_at <= ?", now) ||
      exists("snapshot_upload", "state IN ('uploading', 'completing', 'aborted')")
    ) {
      return true;
    }
    if (
      this.#boundedRowCount(["snapshot_upload"], SNAPSHOT_UPLOAD_RESERVATION_LIMIT + 1) >
      SNAPSHOT_UPLOAD_RESERVATION_LIMIT
    ) {
      return true;
    }
    return (
      session !== undefined &&
      (this.#recentGraceNeedsRefresh(session, pinnedSnapshotIds) ||
        this.#retentionCandidates(session, pinnedSnapshotIds, 1).length > 0)
    );
  }

  reconcile(
    pinnedSnapshotIds: ReadonlySet<string>,
    limit = SNAPSHOT_MAINTENANCE_BATCH_LIMIT,
  ): ReconcileSnapshotRetentionResult {
    let result: ReconcileSnapshotRetentionResult | undefined;
    this.state.storage.transactionSync(() => {
      const session = this.relayStore.session();
      if (session === undefined) {
        result = { retired: [] };
        return;
      }
      result = this.#retireEligible(session, pinnedSnapshotIds, limit);
      if (result.retired.length === 0) {
        const retainedSession = this.relayStore.session();
        if (retainedSession !== undefined) this.#clearRetentionBacklogIfBounded(retainedSession);
      }
    });
    if (result === undefined) throw new Error("snapshot retention transaction did not resolve");
    return result;
  }

  finalize(
    input: FinalizedSnapshot,
    pinnedSnapshotIds: ReadonlySet<string>,
  ): FinalizeSnapshotResult {
    const snapshot = FinalizedSnapshotSchema.parse(input);
    let result: FinalizeSnapshotResult | undefined;
    this.state.storage.transactionSync(() => {
      const session = this.relayStore.session();
      if (
        session === undefined ||
        session.session_id !== snapshot.sessionId ||
        session.session_epoch !== snapshot.sessionEpoch ||
        session.engine_id !== snapshot.engineId ||
        !isSnapshotObjectKey(snapshot.objectKey, snapshot.sessionId, snapshot.snapshotId)
      ) {
        result = { ok: false, reason: "session-mismatch" };
        return;
      }
      if (
        BigInt(snapshot.cutEventSeq) > BigInt(session.head_event_seq) ||
        BigInt(snapshot.nextPtyOffset) > BigInt(session.next_pty_offset)
      ) {
        result = { ok: false, reason: "cursor-ahead" };
        return;
      }

      const existing = this.#row(snapshot.snapshotId);
      if (existing !== undefined) {
        result = !exactSnapshotMetadata(existing, metadataOf(snapshot))
          ? { ok: false, reason: "conflict" }
          : !exactSnapshot(existing, snapshot)
            ? { ok: false, reason: "attempt-changed" }
            : existing.state === "servable"
              ? {
                  ok: true,
                  created: false,
                  retired: [],
                  snapshot: descriptor(existing, session.session_id),
                }
              : { ok: false, reason: "retention-backlog" };
        return;
      }
      const upload = this.#upload(snapshot.snapshotId);
      if (upload === undefined) {
        result = { ok: false, reason: "attempt-changed" };
        return;
      }
      if (upload.metadata_json !== metadataJson(metadataOf(snapshot))) {
        result = { ok: false, reason: "conflict" };
        return;
      }
      const exactUpload =
        upload.object_key === snapshot.objectKey &&
        upload.r2_version === snapshot.r2Version &&
        upload.etag === snapshot.etag;
      if (
        upload.state !== "completed" ||
        snapshot.uploadKind !== "multipart-verified" ||
        !exactUpload
      ) {
        result = { ok: false, reason: "attempt-changed" };
        return;
      }

      const createdAt = this.#nextSnapshotCreatedAt();
      this.sql.exec(
        `INSERT INTO snapshot
            (snapshot_id, session_epoch, cut_event_seq, next_pty_offset, engine_id,
             object_key, r2_version, etag, sha256, compressed_length,
             uncompressed_length, compression, state, created_at, upload_kind)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'servable', ?, ?)`,
        snapshot.snapshotId,
        snapshot.sessionEpoch,
        snapshot.cutEventSeq,
        snapshot.nextPtyOffset,
        snapshot.engineId,
        snapshot.objectKey,
        snapshot.r2Version,
        snapshot.etag,
        snapshot.sha256,
        Number(snapshot.compressedLength),
        snapshot.uncompressedLength,
        snapshot.compression,
        createdAt,
        snapshot.uploadKind,
      );
      this.sql.exec(
        "DELETE FROM snapshot_upload WHERE snapshot_id = ? AND state = 'completed'",
        snapshot.snapshotId,
      );
      const previousLatest = session.latest_snapshot_id;
      const advancesLatest = this.#shouldAdvanceLatest(session, snapshot, createdAt);
      if (advancesLatest) {
        this.sql.exec(
          "UPDATE session_state SET latest_snapshot_id = ?, updated_at = ? WHERE singleton = 1",
          snapshot.snapshotId,
          Date.now(),
        );
      }
      const currentSession = this.relayStore.session();
      if (currentSession === undefined) throw new Error("snapshot session disappeared");
      this.#touchRecentSnapshot(
        currentSession,
        advancesLatest && previousLatest !== null ? previousLatest : snapshot.snapshotId,
        pinnedSnapshotIds,
      );
      const retainedSession = this.relayStore.session();
      if (retainedSession === undefined) throw new Error("snapshot session disappeared");
      const retention = this.#retireEligible(retainedSession, pinnedSnapshotIds);
      result = { ok: true, created: true, retired: retention.retired, snapshot };
    });
    if (result === undefined) throw new Error("snapshot finalize transaction did not resolve");
    return result;
  }

  #retireEligible(
    session: SessionRow,
    pinnedSnapshotIds: ReadonlySet<string>,
    limit = SNAPSHOT_MAINTENANCE_BATCH_LIMIT,
  ): ReconcileSnapshotRetentionResult {
    if (!this.#ensureRecentGrace(session, pinnedSnapshotIds, limit)) return { retired: [] };
    const currentSession = this.relayStore.session();
    if (currentSession === undefined) return { retired: [] };
    const candidates = this.#retentionCandidates(currentSession, pinnedSnapshotIds, limit);
    if (candidates.length === 0) return { retired: [] };

    for (const candidate of candidates) {
      this.sql.exec(
        "UPDATE snapshot SET state = 'retired' WHERE snapshot_id = ? AND state = 'servable'",
        candidate.snapshot_id,
      );
    }
    return { retired: candidates.map(retiredSnapshot) };
  }

  #retentionCandidates(
    session: SessionRow,
    pinnedSnapshotIds: ReadonlySet<string>,
    limit = SNAPSHOT_MAINTENANCE_BATCH_LIMIT,
  ): readonly SnapshotRow[] {
    const protectedIds = new Set(pinnedSnapshotIds);
    if (session.latest_snapshot_id !== null) protectedIds.add(session.latest_snapshot_id);
    for (const snapshotId of [session.recent_snapshot_id_1, session.recent_snapshot_id_2]) {
      if (snapshotId === null || protectedIds.has(snapshotId)) continue;
      const row = this.#row(snapshotId);
      if (row?.state !== "servable") continue;
      protectedIds.add(snapshotId);
    }

    const exclusions = [...protectedIds];
    const exclusionClause =
      exclusions.length === 0
        ? ""
        : ` AND snapshot_id NOT IN (${exclusions.map(() => "?").join(", ")})`;
    return this.sql
      .exec(
        `SELECT * FROM snapshot
         WHERE 1 = 1${exclusionClause}
         ORDER BY rowid ASC
         LIMIT ?`,
        ...exclusions,
        limit,
      )
      .toArray() as unknown as SnapshotRow[];
  }

  #retiredObjects(
    activeSnapshotIds: ReadonlySet<string>,
    limit: number,
  ): readonly RetiredSnapshotObject[] {
    const activeIds = [...activeSnapshotIds];
    const inactiveClause =
      activeIds.length === 0
        ? ""
        : ` AND snapshot_id NOT IN (${activeIds.map(() => "?").join(", ")})`;
    const uploads = this.sql
      .exec(
        `SELECT snapshot_id, object_key
         FROM snapshot_upload
         WHERE state = 'retired'${inactiveClause}
         ORDER BY rowid ASC
         LIMIT ?`,
        ...activeIds,
        limit,
      )
      .toArray() as unknown as Pick<SnapshotUploadRecord, "object_key" | "snapshot_id">[];
    return uploads.map((row) => ({
      source: "upload" as const,
      snapshotId: row.snapshot_id,
      objectKey: row.object_key,
    }));
  }

  #ensureRecentGrace(
    session: SessionRow,
    pinnedSnapshotIds: ReadonlySet<string>,
    limit: number,
  ): boolean {
    if (session.snapshot_retention_backlog !== 0) {
      return this.#ensureMigrationRecentGrace(session, pinnedSnapshotIds, limit);
    }
    if (this.#boundedRowCount(["snapshot", "snapshot_upload"], this.rowLimit + 1) > this.rowLimit) {
      this.sql.exec(
        `UPDATE session_state
         SET snapshot_retention_backlog = 1, snapshot_recent_scan_before = NULL,
             snapshot_recent_scan_done = 0
         WHERE singleton = 1`,
      );
      return false;
    }

    const candidates = this.#recentCandidates(session).filter(
      (snapshotId) => this.#row(snapshotId)?.state === "servable",
    );
    const protectedIds = new Set(pinnedSnapshotIds);
    if (session.latest_snapshot_id !== null) protectedIds.add(session.latest_snapshot_id);
    for (const snapshotId of protectedIds) {
      if (!candidates.includes(snapshotId) && this.#row(snapshotId)?.state === "servable") {
        candidates.push(snapshotId);
      }
    }
    const rows = this.sql
      .exec(
        `SELECT rowid AS scan_rowid, snapshot_id, state
         FROM snapshot
         ORDER BY rowid DESC
         LIMIT ?`,
        this.rowLimit + 1,
      )
      .toArray() as unknown as SnapshotScanRow[];
    for (const row of rows) {
      if (row.state === "servable" && !candidates.includes(row.snapshot_id)) {
        candidates.push(row.snapshot_id);
      }
    }

    const recentIds = candidates
      .filter((snapshotId) => !protectedIds.has(snapshotId))
      .slice(0, SNAPSHOT_RECENT_GRACE_LIMIT);
    const retainedIds = new Set([...protectedIds, ...recentIds]);
    const retainedCandidates = candidates
      .filter((snapshotId) => retainedIds.has(snapshotId))
      .slice(0, this.#recentCandidateLimit);
    this.sql.exec(
      `UPDATE session_state
       SET recent_snapshot_id_1 = ?, recent_snapshot_id_2 = ?,
           snapshot_recent_candidates_json = ?, snapshot_recent_scan_before = NULL,
           snapshot_recent_scan_done = 1
       WHERE singleton = 1`,
      recentIds[0] ?? null,
      recentIds[1] ?? null,
      JSON.stringify(retainedCandidates),
    );
    return true;
  }

  #ensureMigrationRecentGrace(
    session: SessionRow,
    pinnedSnapshotIds: ReadonlySet<string>,
    limit: number,
  ): boolean {
    const candidateIds = this.#recentCandidates(session).filter(
      (snapshotId) => this.#row(snapshotId)?.state === "servable",
    );
    const protectedIds = new Set(pinnedSnapshotIds);
    if (session.latest_snapshot_id !== null) protectedIds.add(session.latest_snapshot_id);
    for (const snapshotId of protectedIds) {
      if (!candidateIds.includes(snapshotId) && this.#row(snapshotId)?.state === "servable") {
        candidateIds.push(snapshotId);
      }
    }

    let recentIds = candidateIds
      .filter((snapshotId) => !protectedIds.has(snapshotId))
      .slice(0, SNAPSHOT_RECENT_GRACE_LIMIT);
    let nextCursor = session.snapshot_recent_scan_before;
    let done = recentIds.length >= SNAPSHOT_RECENT_GRACE_LIMIT || nextCursor === 0;
    if (!done && nextCursor !== 0) {
      const rows = (nextCursor === null
        ? this.sql.exec(
            `SELECT rowid AS scan_rowid, snapshot_id, state
             FROM snapshot
             ORDER BY rowid DESC
             LIMIT ?`,
            limit,
          )
        : this.sql.exec(
            `SELECT rowid AS scan_rowid, snapshot_id, state
             FROM snapshot
             WHERE rowid < ?
             ORDER BY rowid DESC
             LIMIT ?`,
            nextCursor,
            limit,
          )
      ).toArray() as unknown as SnapshotScanRow[];
      for (const row of rows) {
        if (row.state !== "servable" || candidateIds.includes(row.snapshot_id)) {
          continue;
        }
        candidateIds.push(row.snapshot_id);
      }
      recentIds = candidateIds
        .filter((snapshotId) => !protectedIds.has(snapshotId))
        .slice(0, SNAPSHOT_RECENT_GRACE_LIMIT);
      const exhausted = rows.length < limit;
      done = recentIds.length >= SNAPSHOT_RECENT_GRACE_LIMIT || exhausted;
      nextCursor = exhausted ? 0 : (rows.at(-1)?.scan_rowid ?? 0);
    }

    const retainedIds = new Set([...protectedIds, ...recentIds]);
    const retainedCandidates = candidateIds
      .filter((snapshotId) => retainedIds.has(snapshotId))
      .slice(0, this.#recentCandidateLimit);
    this.sql.exec(
      `UPDATE session_state
       SET recent_snapshot_id_1 = ?, recent_snapshot_id_2 = ?,
           snapshot_recent_candidates_json = ?, snapshot_recent_scan_before = ?,
           snapshot_recent_scan_done = ?
       WHERE singleton = 1`,
      recentIds[0] ?? null,
      recentIds[1] ?? null,
      JSON.stringify(retainedCandidates),
      nextCursor,
      done ? 1 : 0,
    );
    return done;
  }

  #recentGraceNeedsRefresh(session: SessionRow, pinnedSnapshotIds: ReadonlySet<string>): boolean {
    const grace = this.#recentGraceState(session, pinnedSnapshotIds);
    return session.snapshot_recent_scan_done === 0 || grace.invalidStoredId;
  }

  #recentGraceState(
    session: SessionRow,
    pinnedSnapshotIds: ReadonlySet<string>,
  ): SnapshotRecentGraceState {
    const protectedIds = new Set(pinnedSnapshotIds);
    if (session.latest_snapshot_id !== null) protectedIds.add(session.latest_snapshot_id);
    const recentIds: string[] = [];
    let invalidStoredId = false;
    for (const snapshotId of [session.recent_snapshot_id_1, session.recent_snapshot_id_2]) {
      if (snapshotId === null) continue;
      if (protectedIds.has(snapshotId) || recentIds.includes(snapshotId)) {
        invalidStoredId = true;
        continue;
      }
      if (this.#row(snapshotId)?.state !== "servable") {
        invalidStoredId = true;
        continue;
      }
      recentIds.push(snapshotId);
    }
    return { invalidStoredId, recentIds };
  }

  #recentCandidates(session: SessionRow): string[] {
    const parsed: unknown = JSON.parse(session.snapshot_recent_candidates_json);
    if (!Array.isArray(parsed) || !parsed.every((value) => typeof value === "string")) {
      throw new Error("invalid snapshot recent candidate ledger");
    }
    return [...new Set(parsed)].slice(0, this.#recentCandidateLimit);
  }

  #clearRetentionBacklogIfBounded(session: SessionRow): void {
    if (
      session.snapshot_retention_backlog === 0 ||
      session.snapshot_recent_scan_done === 0 ||
      this.#boundedRowCount(["snapshot_upload"], SNAPSHOT_UPLOAD_RESERVATION_LIMIT + 1) >
        SNAPSHOT_UPLOAD_RESERVATION_LIMIT ||
      this.#boundedRowCount(["snapshot", "snapshot_upload"], SNAPSHOT_OBJECT_RECORD_LIMIT + 1) >
        SNAPSHOT_OBJECT_RECORD_LIMIT
    ) {
      return;
    }
    this.sql.exec(
      `UPDATE session_state
       SET snapshot_retention_backlog = 0, snapshot_recent_scan_before = NULL,
           snapshot_recent_scan_done = 1
       WHERE singleton = 1`,
    );
  }

  #boundedRowCount(tables: readonly ("snapshot" | "snapshot_upload")[], limit: number): number {
    let count = 0;
    for (const table of tables) {
      if (count >= limit) break;
      count += this.sql
        .exec(`SELECT 1 AS value FROM ${table} LIMIT ?`, limit - count)
        .toArray().length;
    }
    return count;
  }

  #shouldAdvanceLatest(
    session: SessionRow,
    snapshot: FinalizedSnapshot,
    createdAt: number,
  ): boolean {
    if (session.latest_snapshot_id === null) return true;
    const latest = this.#row(session.latest_snapshot_id);
    if (latest === undefined || latest.state !== "servable") return true;
    const cutComparison = BigInt(snapshot.cutEventSeq) - BigInt(latest.cut_event_seq);
    if (cutComparison !== 0n) return cutComparison > 0n;
    const offsetComparison = BigInt(snapshot.nextPtyOffset) - BigInt(latest.next_pty_offset);
    if (offsetComparison !== 0n) return offsetComparison > 0n;
    if (createdAt !== latest.created_at) return createdAt > latest.created_at;
    return snapshot.snapshotId > latest.snapshot_id;
  }

  #nextSnapshotCreatedAt(): number {
    const session = this.relayStore.session();
    if (session === undefined) throw new Error("snapshot session disappeared");
    const next = Math.max(Date.now(), session.snapshot_created_clock + 1);
    this.sql.exec("UPDATE session_state SET snapshot_created_clock = ? WHERE singleton = 1", next);
    return next;
  }

  #touchRecentSnapshot(
    session: SessionRow,
    snapshotId: string,
    pinnedSnapshotIds: ReadonlySet<string>,
  ): void {
    {
      const candidates = [
        snapshotId,
        ...this.#recentCandidates(session).filter((candidate) => candidate !== snapshotId),
      ];
      const protectedIds = new Set(pinnedSnapshotIds);
      if (session.latest_snapshot_id !== null) protectedIds.add(session.latest_snapshot_id);
      for (const protectedId of protectedIds) {
        if (!candidates.includes(protectedId) && this.#row(protectedId)?.state === "servable") {
          candidates.push(protectedId);
        }
      }
      const recentIds = candidates
        .filter((candidate) => !protectedIds.has(candidate))
        .slice(0, SNAPSHOT_RECENT_GRACE_LIMIT);
      const retainedIds = new Set([...protectedIds, ...recentIds]);
      this.sql.exec(
        `UPDATE session_state
         SET snapshot_recent_candidates_json = ?
         WHERE singleton = 1`,
        JSON.stringify(
          candidates
            .filter((candidate) => retainedIds.has(candidate))
            .slice(0, this.#recentCandidateLimit),
        ),
      );
    }
    if (snapshotId === session.latest_snapshot_id || snapshotId === session.recent_snapshot_id_1) {
      return;
    }
    this.sql.exec(
      `UPDATE session_state
       SET recent_snapshot_id_1 = ?, recent_snapshot_id_2 = ?
       WHERE singleton = 1`,
      snapshotId,
      session.recent_snapshot_id_1,
    );
  }

  #row(snapshotId: string): SnapshotRow | undefined {
    return this.sql
      .exec("SELECT * FROM snapshot WHERE snapshot_id = ?", snapshotId)
      .toArray()[0] as SnapshotRow | undefined;
  }

  #upload(snapshotId: string): SnapshotUploadRecord | undefined {
    return this.sql
      .exec("SELECT * FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
      .toArray()[0] as SnapshotUploadRecord | undefined;
  }
}
