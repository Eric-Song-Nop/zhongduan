import {
  FinalizedSnapshotSchema,
  snapshotObjectKey,
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
}

export type FinalizeSnapshotResult =
  | { ok: true; created: boolean; snapshot: FinalizedSnapshot }
  | { ok: false; reason: "conflict" | "cursor-ahead" | "session-mismatch" };

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
  });
}

function sameSnapshot(row: SnapshotRow, snapshot: FinalizedSnapshot): boolean {
  return (
    row.state === "servable" &&
    row.snapshot_id === snapshot.snapshotId &&
    row.session_epoch === snapshot.sessionEpoch &&
    row.cut_event_seq === snapshot.cutEventSeq &&
    row.next_pty_offset === snapshot.nextPtyOffset &&
    row.engine_id === snapshot.engineId &&
    row.object_key === snapshot.objectKey &&
    row.r2_version === snapshot.r2Version &&
    row.etag === snapshot.etag &&
    row.sha256 === snapshot.sha256 &&
    row.compressed_length === Number(snapshot.compressedLength) &&
    row.uncompressed_length === snapshot.uncompressedLength &&
    row.compression === snapshot.compression
  );
}

export class SnapshotStore {
  constructor(
    private readonly state: DurableObjectState,
    private readonly sql: SqlStorage,
    private readonly relayStore: RelayStore,
  ) {}

  published(snapshotId: string): FinalizedSnapshot | undefined {
    const session = this.relayStore.session();
    if (session === undefined) return undefined;
    const row = this.#row(snapshotId);
    return row?.state === "servable" ? descriptor(row, session.session_id) : undefined;
  }

  finalize(input: FinalizedSnapshot): FinalizeSnapshotResult {
    const snapshot = FinalizedSnapshotSchema.parse(input);
    let result: FinalizeSnapshotResult | undefined;
    this.state.storage.transactionSync(() => {
      const session = this.relayStore.session();
      if (
        session === undefined ||
        session.session_id !== snapshot.sessionId ||
        session.session_epoch !== snapshot.sessionEpoch ||
        session.engine_id !== snapshot.engineId ||
        snapshot.objectKey !== snapshotObjectKey(snapshot.sessionId, snapshot.snapshotId)
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
        result = sameSnapshot(existing, snapshot)
          ? { ok: true, created: false, snapshot: descriptor(existing, session.session_id) }
          : { ok: false, reason: "conflict" };
        return;
      }

      const createdAt = Date.now();
      this.sql.exec(
        `INSERT INTO snapshot
          (snapshot_id, session_epoch, cut_event_seq, next_pty_offset, engine_id,
           object_key, r2_version, etag, sha256, compressed_length,
           uncompressed_length, compression, state, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'servable', ?)`,
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
      );
      if (this.#shouldAdvanceLatest(session, snapshot, createdAt)) {
        this.sql.exec(
          "UPDATE session_state SET latest_snapshot_id = ?, updated_at = ? WHERE singleton = 1",
          snapshot.snapshotId,
          Date.now(),
        );
      }
      result = { ok: true, created: true, snapshot };
    });
    if (result === undefined) throw new Error("snapshot finalize transaction did not resolve");
    return result;
  }

  #shouldAdvanceLatest(
    session: SessionRow,
    snapshot: FinalizedSnapshot,
    createdAt: number,
  ): boolean {
    if (session.latest_snapshot_id === null) return true;
    const latest = this.#row(session.latest_snapshot_id);
    if (latest === undefined) return true;
    const cutComparison = BigInt(snapshot.cutEventSeq) - BigInt(latest.cut_event_seq);
    if (cutComparison !== 0n) return cutComparison > 0n;
    const offsetComparison = BigInt(snapshot.nextPtyOffset) - BigInt(latest.next_pty_offset);
    if (offsetComparison !== 0n) return offsetComparison > 0n;
    if (createdAt !== latest.created_at) return createdAt > latest.created_at;
    return snapshot.snapshotId > latest.snapshot_id;
  }

  #row(snapshotId: string): SnapshotRow | undefined {
    return this.sql
      .exec("SELECT * FROM snapshot WHERE snapshot_id = ?", snapshotId)
      .toArray()[0] as SnapshotRow | undefined;
  }
}
