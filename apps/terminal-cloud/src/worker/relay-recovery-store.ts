import {
  AuthorityCursorSchema,
  DataFrameKind,
  DecimalU64Schema,
  DeliveryReceivedSchema,
  PositiveDecimalU64Schema,
  RecoveryAdoptedSchema,
  RecoveryStartFenceSchema,
  RecoveryStartSchema,
  RecoveryV3HostPrepareSchema,
  RecoveryV3HostSourceClosedSchema,
  RecoveryV3HostSourceGrantSchema,
  RecoveryV3HostSourceReceivedSchema,
  RecoveryV3HostSourceResetSchema,
  RecoveryV3HostStartReadySchema,
  advanceDeliveryLaneCursor,
  decodeDataFrame,
  decodeDeliveryEnvelopeV3,
  toBrowserRecoverySourceClosed,
  type AuthorityCursor,
  type DataFrame,
  type DeliveryLaneCursor,
  type DeliveryReceived,
  type RecoveryAdopted,
  type RecoveryStart,
  type RecoveryStartFence,
  type RecoveryV3HostPrepare,
  type RecoveryV3HostSourceClosed,
  type RecoveryV3HostSourceReset,
} from "@zhongduan/protocol";
import type { RelayStore } from "./relay-store";

export type RecoveryAttemptState =
  | "preparing"
  | "installed"
  | "assembling"
  | "complete"
  | "resetting";

export type RecoveryLane = "live" | "recovery";

export type RecoveryDeliveryRecordState = "queued" | "sending" | "sent";

export type RecoveryOutboxKind =
  | "recovery-prepare"
  | "recovery-start"
  | "recovery-start-ready"
  | "recovery-source-grant"
  | "recovery-source-received"
  | "recovery-source-closed"
  | "recovery-source-reset";

export interface RecoveryAttemptRow {
  adopted_json: string | null;
  base_cursor_json: string;
  client_id: string;
  committed_through_json: string | null;
  connection_id: string;
  created_at: number;
  delivery_generation: string;
  engine_id: string;
  granted_cumulative_encoded_bytes: string;
  hard_deadline_at: number;
  host_fence: string;
  live_floor_json: string | null;
  no_progress_deadline_at: number;
  no_progress_timeout_ms: number;
  prepare_json: string;
  recovery_done_cumulative_encoded_bytes: string | null;
  recovery_done_ordinal: string | null;
  recovery_done_through_json: string | null;
  recovery_id: string;
  replica_applied_json: string;
  reset_reason: RecoveryV3HostSourceReset["reason"] | null;
  source_closed_json: string | null;
  start_json: string | null;
  state: RecoveryAttemptState;
  stream_id: number;
  updated_at: number;
}

export interface RecoveryDeliveryLaneRow {
  lane: RecoveryLane;
  received_authority_cursor_json: string;
  received_cumulative_encoded_bytes: string;
  received_delivery_ordinal: string;
  recovery_id: string;
  sent_authority_cursor_json: string;
  sent_cumulative_encoded_bytes: string;
  sent_delivery_ordinal: string;
  updated_at: number;
}

export interface RecoveryDeliveryRecordRow {
  authority_cursor_after_json: string;
  cumulative_encoded_bytes: string;
  delivery_ordinal: string;
  encoded_bytes: number;
  lane: RecoveryLane;
  recovery_id: string;
  state: RecoveryDeliveryRecordState;
}

export interface RecoveryDeliveryRecordIdentity {
  authorityCursorAfter: AuthorityCursor;
  cumulativeEncodedBytes: string;
  deliveryOrdinal: string;
  encodedBytes: number;
  lane: RecoveryLane;
  recoveryId: string;
}

export interface RecoveryDeliveryUsage {
  encodedBytes: number;
  records: number;
}

/**
 * Session-wide delivery accounting. Recovery payload bytes are charged once
 * through the outstanding Host grant, while their durable scalar rows still
 * consume record capacity.
 */
export interface RecoverySessionDeliveryUsage {
  encodedBytes: number;
  liveEncodedBytes: number;
  liveRecords: number;
  observerEncodedBytes: number;
  observerRecords: number;
  records: number;
  recoveryEncodedBytes: number;
  recoveryRecords: number;
  recoveryReservedEncodedBytes: number;
  recoveryReservedRecords: number;
  writerEncodedBytes: number;
  writerRecords: number;
}

export interface RecoveryControlOutboxRow {
  created_at: number;
  destination: "browser" | "host";
  kind: RecoveryOutboxKind;
  payload_json: string;
  recovery_id: string;
  updated_at: number;
}

export interface RelayRecoveryStoreLimits {
  maxAttempts: number;
  maxAttemptLiveEncodedBytes: number;
  maxAttemptLiveRecords: number;
  maxDeliveryEncodedBytes: number;
  maxDeliveryRecords: number;
  maxDeliveryEnvelopeEncodedBytes: number;
  maxOutboxEntries: number;
  maxRecoveryGrantWindowEncodedBytes: number;
  maxSessionDeliveryEncodedBytes: number;
  maxSessionDeliveryRecords: number;
  maxSessionRecoveryEncodedBytes: number;
  maxSessionRecoveryRecords: number;
  minDeliveryEnvelopeEncodedBytes: number;
  writerDeliveryReserveEncodedBytes: number;
  writerDeliveryReserveRecords: number;
}

export interface BeginPreparingInput {
  clientId: string;
  hardDeadlineAt: number;
  hostFence: string;
  noProgressTimeoutMs: number;
  now: number;
  prepare: RecoveryV3HostPrepare;
}

export interface InstallFenceInput {
  cumulativeGrantedEncodedBytes: string;
  fence: RecoveryStartFence;
  now: number;
  start: RecoveryStart;
}

export interface ReceivedLaneProgress {
  receipt: DeliveryReceived;
  recoveryId: string;
}

export type RecoveryStoreRejectReason =
  | "attempt-capacity"
  | "attempt-resetting"
  | "client-missing"
  | "client-v2"
  | "deadline-expired"
  | "delivery-capacity"
  | "generation-owned"
  | "head-mismatch"
  | "identity-mismatch"
  | "immutable-conflict"
  | "invalid-progress"
  | "missing-attempt"
  | "outbox-capacity"
  | "progress-ahead"
  | "recovery-done"
  | "state-conflict";

export type RecoveryStoreResult =
  | { changed: boolean; ok: true }
  | { ok: false; reason: RecoveryStoreRejectReason };

export type RecoveryDeliveryEnqueueResult =
  | {
      changed: true;
      ok: true;
      record: RecoveryDeliveryRecordIdentity;
    }
  | { ok: false; reason: RecoveryStoreRejectReason };

interface RecoveryAttemptDeliveryContribution {
  attempt: RecoveryAttemptRow;
  encodedBytes: number;
  liveEncodedBytes: number;
  liveRecords: number;
  records: number;
  recoveryEncodedBytes: number;
  recoveryRecords: number;
  recoveryReservedEncodedBytes: number;
  recoveryReservedRecords: number;
  recoveryTailCumulativeEncodedBytes: bigint;
  recoveryReceivedCumulativeEncodedBytes: bigint;
}

const ok = (changed: boolean): RecoveryStoreResult => ({ changed, ok: true });
const MAX_U64 = (1n << 64n) - 1n;
const rejected = (
  reason: RecoveryStoreRejectReason,
): { ok: false; reason: RecoveryStoreRejectReason } => ({
  ok: false,
  reason,
});

function canonicalJson(value: unknown): string {
  return JSON.stringify(value);
}

function sameCursor(left: AuthorityCursor, right: AuthorityCursor): boolean {
  return (
    left.sessionEpoch === right.sessionEpoch &&
    left.eventSeq === right.eventSeq &&
    left.nextPtyOffset === right.nextPtyOffset
  );
}

function cursorAtOrAfter(candidate: AuthorityCursor, floor: AuthorityCursor): boolean {
  if (candidate.sessionEpoch !== floor.sessionEpoch) return false;
  const candidateEvent = BigInt(candidate.eventSeq);
  const floorEvent = BigInt(floor.eventSeq);
  const candidateOffset = BigInt(candidate.nextPtyOffset);
  const floorOffset = BigInt(floor.nextPtyOffset);
  return (
    candidateEvent >= floorEvent &&
    candidateOffset >= floorOffset &&
    (candidateEvent !== floorEvent || candidateOffset === floorOffset)
  );
}

function cursorAtOrBefore(candidate: AuthorityCursor, ceiling: AuthorityCursor): boolean {
  return cursorAtOrAfter(ceiling, candidate);
}

function parseCursor(json: string): AuthorityCursor {
  return AuthorityCursorSchema.parse(JSON.parse(json) as unknown);
}

function isPositiveSafeInteger(value: number): boolean {
  return Number.isSafeInteger(value) && value > 0;
}

/**
 * Durable scalar owner for one relay's Recovery v3 attempts.
 *
 * Every mutating method is deliberately synchronous and does not open its own
 * transaction. The Durable Object owner must call it from the transactionSync
 * that also owns the surrounding connection/generation transition.
 */
export class RelayRecoveryStore {
  constructor(
    private readonly sql: SqlStorage,
    private readonly relay: RelayStore,
    private readonly limits: RelayRecoveryStoreLimits,
  ) {
    const positiveLimits = [
      limits.maxAttempts,
      limits.maxAttemptLiveEncodedBytes,
      limits.maxAttemptLiveRecords,
      limits.maxDeliveryEncodedBytes,
      limits.maxDeliveryRecords,
      limits.maxDeliveryEnvelopeEncodedBytes,
      limits.maxOutboxEntries,
      limits.maxRecoveryGrantWindowEncodedBytes,
      limits.maxSessionDeliveryEncodedBytes,
      limits.maxSessionDeliveryRecords,
      limits.maxSessionRecoveryEncodedBytes,
      limits.maxSessionRecoveryRecords,
      limits.minDeliveryEnvelopeEncodedBytes,
      limits.writerDeliveryReserveEncodedBytes,
      limits.writerDeliveryReserveRecords,
    ];
    if (
      positiveLimits.some((value) => !isPositiveSafeInteger(value)) ||
      limits.maxAttempts === Number.MAX_SAFE_INTEGER ||
      limits.maxDeliveryRecords === Number.MAX_SAFE_INTEGER ||
      limits.maxSessionDeliveryRecords === Number.MAX_SAFE_INTEGER ||
      limits.maxAttemptLiveEncodedBytes > limits.maxDeliveryEncodedBytes ||
      limits.maxAttemptLiveRecords > limits.maxDeliveryRecords ||
      limits.maxDeliveryEnvelopeEncodedBytes > limits.maxRecoveryGrantWindowEncodedBytes ||
      limits.maxRecoveryGrantWindowEncodedBytes > limits.maxDeliveryEncodedBytes ||
      limits.maxRecoveryGrantWindowEncodedBytes > limits.maxSessionRecoveryEncodedBytes ||
      limits.maxSessionRecoveryEncodedBytes > limits.maxSessionDeliveryEncodedBytes ||
      limits.maxSessionRecoveryRecords > limits.maxSessionDeliveryRecords ||
      limits.minDeliveryEnvelopeEncodedBytes > limits.maxDeliveryEnvelopeEncodedBytes ||
      limits.writerDeliveryReserveEncodedBytes > limits.maxSessionDeliveryEncodedBytes ||
      limits.writerDeliveryReserveRecords > limits.maxSessionDeliveryRecords
    ) {
      throw new RangeError("invalid recovery store limits");
    }
  }

  attempt(recoveryId: string): RecoveryAttemptRow | undefined {
    return this.sql
      .exec("SELECT * FROM recovery_attempt WHERE recovery_id = ?", recoveryId)
      .toArray()[0] as RecoveryAttemptRow | undefined;
  }

  /**
   * Read-only routing lookup. The caller must still bind the returned row to
   * the current Host and Browser socket identities before using it.
   */
  attemptByDeliveryIdentity(
    streamId: number,
    deliveryGeneration: string,
  ): RecoveryAttemptRow | undefined {
    this.#validateStreamId(streamId);
    const generation = PositiveDecimalU64Schema.parse(deliveryGeneration);
    const matches = this.sql
      .exec(
        `SELECT * FROM recovery_attempt
         WHERE stream_id = ? AND delivery_generation = ?
         LIMIT 2`,
        streamId,
        generation,
      )
      .toArray() as unknown as RecoveryAttemptRow[];
    return matches.length === 1 ? matches[0] : undefined;
  }

  /**
   * Read-only reconnect lookup. Exact durable identity locates an attempt but
   * does not authorize a transition or an outbox acknowledgement.
   */
  attemptByConnectionIdentity(
    clientId: string,
    connectionId: string,
    streamId: number,
    deliveryGeneration: string,
  ): RecoveryAttemptRow | undefined {
    this.#validateStreamId(streamId);
    const generation = PositiveDecimalU64Schema.parse(deliveryGeneration);
    const matches = this.sql
      .exec(
        `SELECT * FROM recovery_attempt
         WHERE client_id = ? AND connection_id = ?
           AND stream_id = ? AND delivery_generation = ?
         LIMIT 2`,
        clientId,
        connectionId,
        streamId,
        generation,
      )
      .toArray() as unknown as RecoveryAttemptRow[];
    return matches.length === 1 ? matches[0] : undefined;
  }

  lanes(recoveryId: string): RecoveryDeliveryLaneRow[] {
    return this.sql
      .exec(
        `SELECT * FROM recovery_delivery_lane
         WHERE recovery_id = ? ORDER BY lane`,
        recoveryId,
      )
      .toArray() as unknown as RecoveryDeliveryLaneRow[];
  }

  deliveryRecords(recoveryId: string): RecoveryDeliveryRecordRow[] {
    const rows = this.sql
      .exec(
        `SELECT * FROM recovery_delivery_record
         WHERE recovery_id = ?
         ORDER BY lane, length(delivery_ordinal), delivery_ordinal
         LIMIT ?`,
        recoveryId,
        this.limits.maxDeliveryRecords + 1,
      )
      .toArray() as unknown as RecoveryDeliveryRecordRow[];
    if (rows.length > this.limits.maxDeliveryRecords) {
      throw new Error("recovery delivery record capacity exceeded");
    }
    return rows;
  }

  deliveryRecordsAwaitingReceipt(recoveryId: string): RecoveryDeliveryRecordRow[] {
    const rows = this.sql
      .exec(
        `SELECT * FROM recovery_delivery_record
         WHERE recovery_id = ? AND state = 'sent'
         ORDER BY lane, length(delivery_ordinal), delivery_ordinal
         LIMIT ?`,
        recoveryId,
        this.limits.maxDeliveryRecords + 1,
      )
      .toArray() as unknown as RecoveryDeliveryRecordRow[];
    if (rows.length > this.limits.maxDeliveryRecords) {
      throw new Error("recovery delivery record capacity exceeded");
    }
    return rows;
  }

  deliveryRecoveryIdsRequiringReset(): string[] {
    const rows = this.sql
      .exec(
        `SELECT DISTINCT recovery_id FROM recovery_delivery_record
           WHERE state IN ('queued', 'sending')
           ORDER BY recovery_id
           LIMIT ?`,
        this.limits.maxAttempts + 1,
      )
      .toArray() as unknown as { recovery_id: string }[];
    if (rows.length > this.limits.maxAttempts) {
      throw new Error("recovery delivery reset candidate capacity exceeded");
    }
    return rows.map((row) => row.recovery_id);
  }

  deliveryUsage(recoveryId: string): RecoveryDeliveryUsage {
    const records = this.deliveryRecords(recoveryId);
    let encodedBytes = 0;
    for (const record of records) {
      if (!isPositiveSafeInteger(record.encoded_bytes)) {
        throw new Error("invalid durable recovery delivery size");
      }
      encodedBytes += record.encoded_bytes;
      if (
        !Number.isSafeInteger(encodedBytes) ||
        encodedBytes > this.limits.maxDeliveryEncodedBytes
      ) {
        throw new Error("recovery delivery encoded-byte capacity exceeded");
      }
    }
    return { encodedBytes, records: records.length };
  }

  sessionDeliveryUsage(): RecoverySessionDeliveryUsage {
    const usage: RecoverySessionDeliveryUsage = {
      encodedBytes: 0,
      liveEncodedBytes: 0,
      liveRecords: 0,
      observerEncodedBytes: 0,
      observerRecords: 0,
      records: 0,
      recoveryEncodedBytes: 0,
      recoveryRecords: 0,
      recoveryReservedEncodedBytes: 0,
      recoveryReservedRecords: 0,
      writerEncodedBytes: 0,
      writerRecords: 0,
    };
    for (const contribution of this.#sessionDeliveryContributions()) {
      usage.liveEncodedBytes = this.#addUsage(
        usage.liveEncodedBytes,
        contribution.liveEncodedBytes,
      );
      usage.liveRecords = this.#addUsage(usage.liveRecords, contribution.liveRecords);
      usage.recoveryEncodedBytes = this.#addUsage(
        usage.recoveryEncodedBytes,
        contribution.recoveryEncodedBytes,
      );
      usage.recoveryRecords = this.#addUsage(usage.recoveryRecords, contribution.recoveryRecords);
      usage.recoveryReservedEncodedBytes = this.#addUsage(
        usage.recoveryReservedEncodedBytes,
        contribution.recoveryReservedEncodedBytes,
      );
      usage.recoveryReservedRecords = this.#addUsage(
        usage.recoveryReservedRecords,
        contribution.recoveryReservedRecords,
      );
      if (this.#attemptRole(contribution.attempt) === "writer") {
        usage.writerEncodedBytes = this.#addUsage(
          usage.writerEncodedBytes,
          contribution.encodedBytes,
        );
        usage.writerRecords = this.#addUsage(usage.writerRecords, contribution.records);
      } else {
        usage.observerEncodedBytes = this.#addUsage(
          usage.observerEncodedBytes,
          contribution.encodedBytes,
        );
        usage.observerRecords = this.#addUsage(usage.observerRecords, contribution.records);
      }
    }
    usage.encodedBytes = this.#addUsage(usage.liveEncodedBytes, usage.recoveryReservedEncodedBytes);
    usage.records = this.#addUsage(
      this.#addUsage(usage.liveRecords, usage.recoveryRecords),
      usage.recoveryReservedRecords,
    );
    return usage;
  }

  recoveryGrantReservation(recoveryId: string): string {
    const attempt = this.attempt(recoveryId);
    if (attempt === undefined) throw new Error("missing recovery attempt");
    if (attempt.state === "complete" || attempt.state === "resetting") return "0";
    if (attempt.source_closed_json !== null) return "0";
    const lane = this.#lane(recoveryId, "recovery");
    const received =
      lane === undefined
        ? 0n
        : BigInt(DecimalU64Schema.parse(lane.received_cumulative_encoded_bytes));
    const granted = BigInt(DecimalU64Schema.parse(attempt.granted_cumulative_encoded_bytes));
    if (received > granted) throw new Error("recovery received bytes exceed durable grant");
    return (granted - received).toString();
  }

  outbox(limit = this.limits.maxOutboxEntries): RecoveryControlOutboxRow[] {
    if (!isPositiveSafeInteger(limit) || limit > this.limits.maxOutboxEntries) {
      throw new RangeError("invalid recovery outbox limit");
    }
    return this.sql
      .exec(
        `SELECT * FROM recovery_control_outbox
         ORDER BY created_at, recovery_id, kind LIMIT ?`,
        limit,
      )
      .toArray() as unknown as RecoveryControlOutboxRow[];
  }

  /** Exact read-only candidate lookup for a trusted runtime outbox drainer. */
  outboxEntry(recoveryId: string, kind: RecoveryOutboxKind): RecoveryControlOutboxRow | undefined {
    const matches = this.sql
      .exec(
        `SELECT * FROM recovery_control_outbox
         WHERE recovery_id = ? AND kind = ?
         LIMIT 2`,
        recoveryId,
        kind,
      )
      .toArray() as unknown as RecoveryControlOutboxRow[];
    return matches.length === 1 ? matches[0] : undefined;
  }

  nextDeadline(): number | undefined {
    const row = this.sql
      .exec(
        `SELECT MIN(
           CASE WHEN hard_deadline_at < no_progress_deadline_at
             THEN hard_deadline_at ELSE no_progress_deadline_at END
         ) AS value
         FROM recovery_attempt
         WHERE state IN ('preparing', 'installed', 'assembling')`,
      )
      .one() as { value: number | null };
    return row.value ?? undefined;
  }

  pinnedSnapshotIds(): ReadonlySet<string> {
    const rows = this.sql
      .exec(
        `SELECT prepare_json, start_json FROM recovery_attempt
         WHERE state IN ('preparing', 'installed', 'assembling')
         ORDER BY recovery_id`,
      )
      .toArray() as unknown as { prepare_json: string; start_json: string | null }[];
    const pinned = new Set<string>();
    for (const row of rows) {
      const prepare = RecoveryV3HostPrepareSchema.parse(JSON.parse(row.prepare_json) as unknown);
      if (prepare.source.kind === "snapshot") pinned.add(prepare.source.snapshotId);
      if (row.start_json === null) continue;
      const start = RecoveryStartSchema.parse(JSON.parse(row.start_json) as unknown);
      if (
        prepare.source.kind !== start.source.kind ||
        (prepare.source.kind === "snapshot" &&
          (start.source.kind !== "snapshot" ||
            prepare.source.snapshotId !== start.source.snapshotId))
      ) {
        throw new Error("durable recovery source identity diverged");
      }
      if (start.source.kind === "snapshot") pinned.add(start.source.snapshotId);
    }
    return pinned;
  }

  beginPreparing(input: BeginPreparingInput): RecoveryStoreResult {
    const prepare = RecoveryV3HostPrepareSchema.parse(input.prepare);
    const hostFence = DecimalU64Schema.parse(input.hostFence);
    this.#validateTime(input.now);
    this.#validateTime(input.hardDeadlineAt);
    if (!isPositiveSafeInteger(input.noProgressTimeoutMs)) {
      throw new RangeError("invalid no-progress timeout");
    }
    if (input.hardDeadlineAt <= input.now) return rejected("deadline-expired");

    const ownership = this.#validateCurrentClient(
      input.clientId,
      prepare.streamId,
      prepare.deliveryGeneration,
    );
    if (ownership !== undefined) return rejected(ownership);
    const session = this.relay.session();
    if (
      session === undefined ||
      session.engine_id !== prepare.engineId ||
      session.session_epoch !== prepare.base.sessionEpoch ||
      session.host_fence !== hostFence
    ) {
      return rejected("identity-mismatch");
    }

    const prepareJson = canonicalJson(prepare);
    const existing = this.attempt(prepare.recoveryId);
    if (existing !== undefined) {
      if (
        existing.client_id !== input.clientId ||
        existing.prepare_json !== prepareJson ||
        existing.host_fence !== hostFence ||
        existing.hard_deadline_at !== input.hardDeadlineAt ||
        existing.no_progress_timeout_ms !== input.noProgressTimeoutMs
      ) {
        return rejected("immutable-conflict");
      }
      if (existing.state === "resetting") return rejected("attempt-resetting");
      return ok(false);
    }

    const generationOwner = this.sql
      .exec(
        `SELECT recovery_id FROM recovery_attempt
         WHERE client_id = ? AND delivery_generation = ?`,
        input.clientId,
        prepare.deliveryGeneration,
      )
      .toArray()[0] as { recovery_id: string } | undefined;
    if (generationOwner !== undefined) return rejected("generation-owned");

    this.#pruneTerminalClientAttempts(input.clientId, prepare.deliveryGeneration);
    const attemptCount = this.sql.exec("SELECT COUNT(*) AS value FROM recovery_attempt").one() as {
      value: number;
    };
    if (attemptCount.value >= this.limits.maxAttempts) return rejected("attempt-capacity");
    if (!this.#hasOutboxCapacity(1)) return rejected("outbox-capacity");

    const baseJson = canonicalJson(prepare.base);
    const noProgressDeadlineAt = Math.min(
      input.hardDeadlineAt,
      input.now + input.noProgressTimeoutMs,
    );
    this.sql.exec(
      `INSERT INTO recovery_attempt
        (recovery_id, client_id, connection_id, host_fence, stream_id, delivery_generation,
         engine_id, state, prepare_json, base_cursor_json, replica_applied_json,
         hard_deadline_at, no_progress_timeout_ms, no_progress_deadline_at,
         created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, 'preparing', ?, ?, ?, ?, ?, ?, ?, ?)`,
      prepare.recoveryId,
      input.clientId,
      prepare.connectionId,
      hostFence,
      prepare.streamId,
      prepare.deliveryGeneration,
      prepare.engineId,
      prepareJson,
      baseJson,
      baseJson,
      input.hardDeadlineAt,
      input.noProgressTimeoutMs,
      noProgressDeadlineAt,
      input.now,
      input.now,
    );
    this.#insertOutbox(prepare.recoveryId, "recovery-prepare", "host", prepareJson, input.now);
    return ok(true);
  }

  installFence(input: InstallFenceInput): RecoveryStoreResult {
    const fence = RecoveryStartFenceSchema.parse(input.fence);
    const start = RecoveryStartSchema.parse(input.start);
    const grant = DecimalU64Schema.parse(input.cumulativeGrantedEncodedBytes);
    this.#validateTime(input.now);

    const attempt = this.attempt(fence.recoveryId);
    if (attempt === undefined) return rejected("missing-attempt");
    const ownership = this.#validateAttemptIdentity(attempt, {
      connectionId: fence.connectionId,
      deliveryGeneration: fence.deliveryGeneration,
      engineId: fence.engineId,
      streamId: fence.streamId,
    });
    if (ownership !== undefined) return rejected(ownership);
    if (!this.#matchingFenceStart(attempt, fence, start)) {
      return rejected("immutable-conflict");
    }
    if (attempt.state === "resetting") return rejected("attempt-resetting");
    const startJson = canonicalJson(start);
    if (attempt.state !== "preparing") {
      if (attempt.start_json !== startJson || attempt.granted_cumulative_encoded_bytes !== grant) {
        return rejected("immutable-conflict");
      }
      return ok(false);
    }
    if (this.#deadlineExpired(attempt, input.now)) return rejected("deadline-expired");

    const session = this.relay.session();
    if (
      session === undefined ||
      session.session_epoch !== fence.committedThrough.sessionEpoch ||
      session.engine_id !== fence.engineId ||
      session.host_fence !== attempt.host_fence ||
      session.head_event_seq !== fence.committedThrough.eventSeq ||
      session.next_pty_offset !== fence.committedThrough.nextPtyOffset
    ) {
      return rejected("head-mismatch");
    }
    if (!this.#canReserveRecoveryGrant(attempt, BigInt(grant))) {
      return rejected("delivery-capacity");
    }

    const prepareOutbox = this.outboxEntry(fence.recoveryId, "recovery-prepare");
    const entriesAfterInstall = prepareOutbox === undefined ? 2 : 1;
    if (!this.#hasOutboxCapacity(entriesAfterInstall)) return rejected("outbox-capacity");

    const committedJson = canonicalJson(fence.committedThrough);
    const liveFloorJson = canonicalJson(fence.liveFloor);
    const nextNoProgressDeadlineAt = this.#nextNoProgressDeadline(attempt, input.now);
    this.sql.exec(
      `UPDATE recovery_attempt
       SET state = 'installed', start_json = ?, committed_through_json = ?,
           live_floor_json = ?, granted_cumulative_encoded_bytes = ?,
           no_progress_deadline_at = ?, updated_at = ?
       WHERE recovery_id = ? AND state = 'preparing'`,
      startJson,
      committedJson,
      liveFloorJson,
      grant,
      nextNoProgressDeadlineAt,
      input.now,
      fence.recoveryId,
    );
    for (const [lane, baseline] of [
      ["recovery", fence.base],
      ["live", fence.committedThrough],
    ] as const) {
      const baselineJson = canonicalJson(baseline);
      this.sql.exec(
        `INSERT INTO recovery_delivery_lane
          (recovery_id, lane, sent_delivery_ordinal, sent_cumulative_encoded_bytes,
           sent_authority_cursor_json, received_delivery_ordinal,
           received_cumulative_encoded_bytes, received_authority_cursor_json, updated_at)
         VALUES (?, ?, '0', '0', ?, '0', '0', ?, ?)`,
        fence.recoveryId,
        lane,
        baselineJson,
        baselineJson,
        input.now,
      );
    }
    this.sql.exec(
      `DELETE FROM recovery_control_outbox
       WHERE recovery_id = ? AND kind = 'recovery-prepare'`,
      fence.recoveryId,
    );
    this.#insertOutbox(fence.recoveryId, "recovery-start", "browser", startJson, input.now);
    const ready = RecoveryV3HostStartReadySchema.parse({
      type: "recovery-start-ready",
      recoveryId: fence.recoveryId,
      connectionId: fence.connectionId,
      streamId: fence.streamId,
      deliveryGeneration: fence.deliveryGeneration,
      committedThrough: fence.committedThrough,
      cumulativeGrantedEncodedBytes: grant,
    });
    this.#insertOutbox(
      fence.recoveryId,
      "recovery-start-ready",
      "host",
      canonicalJson(ready),
      input.now,
    );
    return ok(true);
  }

  /**
   * Admits one validated envelope into the bounded scalar ledger without
   * retaining its payload or hash. The returned identity is an in-memory CAS
   * token for the caller's queue -> send -> confirm sequence.
   */
  enqueueValidatedLaneDelivery(
    recoveryId: string,
    encoded: ArrayBuffer | Uint8Array,
    now: number,
  ): RecoveryDeliveryEnqueueResult {
    const envelope = decodeDeliveryEnvelopeV3(encoded);
    const frame = decodeDataFrame(envelope.payload);
    this.#validateTime(now);
    const encodedBytes = encoded.byteLength;
    if (
      !isPositiveSafeInteger(encodedBytes) ||
      encodedBytes < this.limits.minDeliveryEnvelopeEncodedBytes ||
      encodedBytes > this.limits.maxDeliveryEnvelopeEncodedBytes
    ) {
      return rejected("invalid-progress");
    }
    const attempt = this.attempt(recoveryId);
    if (attempt === undefined) return rejected("missing-attempt");
    const ownership = this.#validateAttemptGeneration(
      attempt,
      envelope.deliveryGeneration.toString(),
    );
    if (ownership !== undefined) return rejected(ownership);
    if (attempt.stream_id !== envelope.streamId) return rejected("identity-mismatch");
    if (!this.#canLaneProgress(attempt, envelope.lane)) return rejected("state-conflict");
    if (attempt.state !== "complete" && this.#deadlineExpired(attempt, now)) {
      return rejected("deadline-expired");
    }
    if (envelope.lane === "recovery" && attempt.recovery_done_ordinal !== null) {
      return rejected("recovery-done");
    }
    if (
      envelope.lane === "recovery" &&
      envelope.cumulativeEncodedBytes > BigInt(attempt.granted_cumulative_encoded_bytes)
    ) {
      return rejected("progress-ahead");
    }
    const lane = this.#lane(recoveryId, envelope.lane);
    if (lane === undefined) return rejected("state-conflict");
    const obligations = this.#validatedDeliveryObligations(lane);
    if (obligations === undefined) return rejected("invalid-progress");
    const tail = obligations.at(-1);
    const previousOrdinal = tail?.delivery_ordinal ?? lane.sent_delivery_ordinal;
    const previousCumulativeBytes =
      tail?.cumulative_encoded_bytes ?? lane.sent_cumulative_encoded_bytes;
    const previousAuthorityJson =
      tail?.authority_cursor_after_json ?? lane.sent_authority_cursor_json;
    let nextLane: DeliveryLaneCursor;
    let nextAuthority: AuthorityCursor;
    try {
      nextLane = advanceDeliveryLaneCursor(
        {
          cumulativeEncodedBytes: BigInt(previousCumulativeBytes),
          deliveryGeneration: BigInt(attempt.delivery_generation),
          deliveryOrdinal: BigInt(previousOrdinal),
          lane: lane.lane,
          streamId: attempt.stream_id,
        },
        envelope,
      );
      nextAuthority = this.#advanceSentAuthority(
        parseCursor(previousAuthorityJson),
        envelope.lane,
        frame,
      );
    } catch {
      return rejected("invalid-progress");
    }
    if (!this.#validSentAuthority(attempt, envelope.lane, nextAuthority)) {
      return rejected("invalid-progress");
    }
    if (
      nextLane.cumulativeEncodedBytes - BigInt(previousCumulativeBytes) !==
      BigInt(encodedBytes)
    ) {
      return rejected("invalid-progress");
    }
    const isDone = frame.kind === DataFrameKind.ReplayCommit;
    if (
      isDone &&
      (envelope.lane !== "recovery" ||
        attempt.committed_through_json === null ||
        !sameCursor(nextAuthority, parseCursor(attempt.committed_through_json)))
    ) {
      return rejected("invalid-progress");
    }
    if (!this.#hasDeliveryCapacity(attempt, envelope.lane, encodedBytes)) {
      return rejected("delivery-capacity");
    }
    const identity: RecoveryDeliveryRecordIdentity = {
      authorityCursorAfter: nextAuthority,
      cumulativeEncodedBytes: nextLane.cumulativeEncodedBytes.toString(),
      deliveryOrdinal: nextLane.deliveryOrdinal.toString(),
      encodedBytes,
      lane: envelope.lane,
      recoveryId,
    };
    this.sql.exec(
      `INSERT INTO recovery_delivery_record
        (recovery_id, lane, delivery_ordinal, cumulative_encoded_bytes,
         encoded_bytes, authority_cursor_after_json, state)
       VALUES (?, ?, ?, ?, ?, ?, 'queued')`,
      recoveryId,
      envelope.lane,
      identity.deliveryOrdinal,
      identity.cumulativeEncodedBytes,
      encodedBytes,
      canonicalJson(nextAuthority),
    );
    if (isDone) {
      this.sql.exec(
        `UPDATE recovery_attempt
         SET recovery_done_through_json = ?, recovery_done_ordinal = ?,
             recovery_done_cumulative_encoded_bytes = ?, updated_at = ?
         WHERE recovery_id = ? AND recovery_done_ordinal IS NULL`,
        canonicalJson(nextAuthority),
        nextLane.deliveryOrdinal.toString(),
        nextLane.cumulativeEncodedBytes.toString(),
        now,
        recoveryId,
      );
    }
    if (envelope.lane === "recovery") this.refillRecoveryGrantWindows(now);
    return { changed: true, ok: true, record: identity };
  }

  beginLaneDeliverySend(
    identityInput: RecoveryDeliveryRecordIdentity,
    now: number,
  ): RecoveryStoreResult {
    const identity = this.#parseDeliveryIdentity(identityInput);
    this.#validateTime(now);
    const attempt = this.attempt(identity.recoveryId);
    if (attempt === undefined) return rejected("missing-attempt");
    const ownership = this.#validateAttemptGeneration(attempt, attempt.delivery_generation);
    if (ownership !== undefined) return rejected(ownership);
    if (!this.#canLaneProgress(attempt, identity.lane)) return rejected("state-conflict");
    if (attempt.state !== "complete" && this.#deadlineExpired(attempt, now)) {
      return rejected("deadline-expired");
    }
    const lane = this.#lane(identity.recoveryId, identity.lane);
    if (lane === undefined) return rejected("state-conflict");
    const obligations = this.#validatedDeliveryObligations(lane);
    if (obligations === undefined) return rejected("invalid-progress");
    const record = obligations.find(
      (candidate) => candidate.delivery_ordinal === identity.deliveryOrdinal,
    );
    if (record === undefined || !this.#sameDeliveryRecord(record, identity)) {
      return rejected("invalid-progress");
    }
    if (record.state !== "queued") return rejected("state-conflict");
    const existingSending = this.sql
      .exec(
        `SELECT recovery_id FROM recovery_delivery_record
         WHERE recovery_id = ? AND state = 'sending' LIMIT 1`,
        identity.recoveryId,
      )
      .toArray()[0];
    if (existingSending !== undefined) return rejected("state-conflict");
    const sentOrdinal = BigInt(lane.sent_delivery_ordinal);
    const candidateOrdinal = BigInt(record.delivery_ordinal);
    if (candidateOrdinal !== sentOrdinal + 1n) return rejected("state-conflict");
    const previousCumulative = BigInt(lane.sent_cumulative_encoded_bytes);
    if (
      BigInt(record.cumulative_encoded_bytes) !==
      previousCumulative + BigInt(record.encoded_bytes)
    ) {
      return rejected("invalid-progress");
    }
    const updated = this.sql.exec(
      `UPDATE recovery_delivery_record SET state = 'sending'
       WHERE recovery_id = ? AND lane = ? AND delivery_ordinal = ?
         AND cumulative_encoded_bytes = ? AND encoded_bytes = ?
         AND authority_cursor_after_json = ? AND state = 'queued'
       RETURNING recovery_id`,
      identity.recoveryId,
      identity.lane,
      identity.deliveryOrdinal,
      identity.cumulativeEncodedBytes,
      identity.encodedBytes,
      canonicalJson(identity.authorityCursorAfter),
    );
    if (updated.toArray().length !== 1) return rejected("state-conflict");
    return ok(true);
  }

  confirmLaneDeliverySend(
    identityInput: RecoveryDeliveryRecordIdentity,
    now: number,
  ): RecoveryStoreResult {
    const identity = this.#parseDeliveryIdentity(identityInput);
    this.#validateTime(now);
    const attempt = this.attempt(identity.recoveryId);
    if (attempt === undefined) return rejected("missing-attempt");
    const ownership = this.#validateAttemptGeneration(attempt, attempt.delivery_generation);
    if (ownership !== undefined) return rejected(ownership);
    if (!this.#canLaneProgress(attempt, identity.lane)) return rejected("state-conflict");
    const lane = this.#lane(identity.recoveryId, identity.lane);
    if (lane === undefined) return rejected("state-conflict");
    const obligations = this.#validatedDeliveryObligations(lane);
    if (obligations === undefined) return rejected("invalid-progress");
    const record = obligations.find(
      (candidate) => candidate.delivery_ordinal === identity.deliveryOrdinal,
    );
    if (record === undefined || !this.#sameDeliveryRecord(record, identity)) {
      return rejected("invalid-progress");
    }
    if (record.state === "sent") return ok(false);
    if (record.state !== "sending") return rejected("state-conflict");
    const sentOrdinal = BigInt(lane.sent_delivery_ordinal);
    if (BigInt(record.delivery_ordinal) !== sentOrdinal + 1n) {
      return rejected("state-conflict");
    }
    if (
      BigInt(record.cumulative_encoded_bytes) !==
      BigInt(lane.sent_cumulative_encoded_bytes) + BigInt(record.encoded_bytes)
    ) {
      return rejected("invalid-progress");
    }
    const updated = this.sql.exec(
      `UPDATE recovery_delivery_record SET state = 'sent'
       WHERE recovery_id = ? AND lane = ? AND delivery_ordinal = ?
         AND cumulative_encoded_bytes = ? AND encoded_bytes = ?
         AND authority_cursor_after_json = ? AND state = 'sending'
       RETURNING recovery_id`,
      identity.recoveryId,
      identity.lane,
      identity.deliveryOrdinal,
      identity.cumulativeEncodedBytes,
      identity.encodedBytes,
      canonicalJson(identity.authorityCursorAfter),
    );
    if (updated.toArray().length !== 1) return rejected("state-conflict");
    const laneUpdated = this.sql.exec(
      `UPDATE recovery_delivery_lane
       SET sent_delivery_ordinal = ?, sent_cumulative_encoded_bytes = ?,
           sent_authority_cursor_json = ?, updated_at = ?
       WHERE recovery_id = ? AND lane = ?
         AND sent_delivery_ordinal = ?
         AND sent_cumulative_encoded_bytes = ?
         AND sent_authority_cursor_json = ?
       RETURNING recovery_id`,
      identity.deliveryOrdinal,
      identity.cumulativeEncodedBytes,
      canonicalJson(identity.authorityCursorAfter),
      now,
      identity.recoveryId,
      identity.lane,
      lane.sent_delivery_ordinal,
      lane.sent_cumulative_encoded_bytes,
      lane.sent_authority_cursor_json,
    );
    if (laneUpdated.toArray().length !== 1) {
      throw new Error("durable recovery lane disappeared during send confirmation");
    }
    return ok(true);
  }

  markLaneReceived(progress: ReceivedLaneProgress, now: number): RecoveryStoreResult {
    const receipt = DeliveryReceivedSchema.parse(progress.receipt);
    this.#validateTime(now);
    const attempt = this.attempt(progress.recoveryId);
    if (attempt === undefined) return rejected("missing-attempt");
    const ownership = this.#validateAttemptGeneration(attempt, receipt.deliveryGeneration);
    if (ownership !== undefined) return rejected(ownership);
    const lane = this.#lane(progress.recoveryId, receipt.lane);
    if (lane === undefined) return rejected("state-conflict");
    const comparison = this.#compareScalarProgress(
      receipt.contiguousDeliveryOrdinal,
      receipt.cumulativeEncodedBytes,
      lane.received_delivery_ordinal,
      lane.received_cumulative_encoded_bytes,
    );
    if (comparison === "same") return ok(false);
    if (comparison === "invalid") return rejected("invalid-progress");
    if (
      BigInt(receipt.contiguousDeliveryOrdinal) > BigInt(lane.sent_delivery_ordinal) ||
      BigInt(receipt.cumulativeEncodedBytes) > BigInt(lane.sent_cumulative_encoded_bytes)
    ) {
      return rejected("progress-ahead");
    }
    if (!this.#canLaneProgress(attempt, receipt.lane)) return rejected("state-conflict");
    if (attempt.state !== "complete" && this.#deadlineExpired(attempt, now)) {
      return rejected("deadline-expired");
    }
    const obligations = this.#validatedDeliveryObligations(lane);
    if (obligations === undefined) return rejected("invalid-progress");
    const target = obligations.find(
      (record) =>
        record.delivery_ordinal === receipt.contiguousDeliveryOrdinal &&
        record.cumulative_encoded_bytes === receipt.cumulativeEncodedBytes,
    );
    if (target === undefined || target.state !== "sent") return rejected("invalid-progress");
    const targetOrdinal = BigInt(target.delivery_ordinal);
    const prefix = obligations.filter((record) => BigInt(record.delivery_ordinal) <= targetOrdinal);
    if (prefix.length === 0 || prefix.some((record) => record.state !== "sent")) {
      return rejected("invalid-progress");
    }
    if (
      receipt.lane === "recovery" &&
      !this.#hasOutboxCapacityFor(progress.recoveryId, "recovery-source-received")
    ) {
      return rejected("outbox-capacity");
    }
    const laneUpdated = this.sql.exec(
      `UPDATE recovery_delivery_lane
       SET received_delivery_ordinal = ?, received_cumulative_encoded_bytes = ?,
           received_authority_cursor_json = ?, updated_at = ?
       WHERE recovery_id = ? AND lane = ?
         AND received_delivery_ordinal = ?
         AND received_cumulative_encoded_bytes = ?
         AND received_authority_cursor_json = ?
       RETURNING recovery_id`,
      receipt.contiguousDeliveryOrdinal,
      receipt.cumulativeEncodedBytes,
      target.authority_cursor_after_json,
      now,
      progress.recoveryId,
      receipt.lane,
      lane.received_delivery_ordinal,
      lane.received_cumulative_encoded_bytes,
      lane.received_authority_cursor_json,
    );
    if (laneUpdated.toArray().length !== 1) {
      throw new Error("durable recovery receipt lane CAS failed");
    }
    for (const record of prefix) {
      const deleted = this.sql.exec(
        `DELETE FROM recovery_delivery_record
         WHERE recovery_id = ? AND lane = ? AND delivery_ordinal = ?
           AND cumulative_encoded_bytes = ? AND encoded_bytes = ?
           AND authority_cursor_after_json = ? AND state = 'sent'
         RETURNING recovery_id`,
        record.recovery_id,
        record.lane,
        record.delivery_ordinal,
        record.cumulative_encoded_bytes,
        record.encoded_bytes,
        record.authority_cursor_after_json,
      );
      if (deleted.toArray().length !== 1) {
        throw new Error("durable recovery receipt prefix CAS failed");
      }
    }
    this.#touchProgress(attempt, now);
    if (receipt.lane === "recovery") {
      const routed = RecoveryV3HostSourceReceivedSchema.parse({
        type: "recovery-source-received",
        recoveryId: attempt.recovery_id,
        connectionId: attempt.connection_id,
        streamId: attempt.stream_id,
        deliveryGeneration: attempt.delivery_generation,
        lane: "recovery",
        contiguousDeliveryOrdinal: receipt.contiguousDeliveryOrdinal,
        cumulativeEncodedBytes: receipt.cumulativeEncodedBytes,
      });
      this.#upsertOutbox(
        attempt.recovery_id,
        "recovery-source-received",
        "host",
        canonicalJson(routed),
        now,
      );
    }
    this.refillRecoveryGrantWindows(now);
    return ok(true);
  }

  markReplicaApplied(
    recoveryId: string,
    deliveryGeneration: string,
    authorityCursor: AuthorityCursor,
    now: number,
  ): RecoveryStoreResult {
    const generation = PositiveDecimalU64Schema.parse(deliveryGeneration);
    const cursor = AuthorityCursorSchema.parse(authorityCursor);
    this.#validateTime(now);
    const attempt = this.attempt(recoveryId);
    if (attempt === undefined) return rejected("missing-attempt");
    const ownership = this.#validateAttemptGeneration(attempt, generation);
    if (ownership !== undefined) return rejected(ownership);
    const previous = parseCursor(attempt.replica_applied_json);
    if (sameCursor(previous, cursor)) return ok(false);
    if (!this.#canReplicaProgress(attempt)) return rejected("state-conflict");
    if (attempt.state !== "complete" && this.#deadlineExpired(attempt, now)) {
      return rejected("deadline-expired");
    }
    const receivedCeiling = this.#receivedAuthorityCeiling(attempt);
    const session = this.relay.session();
    const head =
      session === undefined
        ? undefined
        : AuthorityCursorSchema.safeParse({
            sessionEpoch: session.session_epoch,
            eventSeq: session.head_event_seq,
            nextPtyOffset: session.next_pty_offset,
          });
    if (
      !cursorAtOrAfter(cursor, previous) ||
      receivedCeiling === undefined ||
      !cursorAtOrBefore(cursor, receivedCeiling) ||
      head === undefined ||
      !head.success ||
      !cursorAtOrBefore(cursor, head.data)
    ) {
      return rejected("invalid-progress");
    }
    this.sql.exec(
      `UPDATE recovery_attempt
       SET replica_applied_json = ?, no_progress_deadline_at = ?, updated_at = ?
       WHERE recovery_id = ?`,
      canonicalJson(cursor),
      this.#nextNoProgressDeadline(attempt, now),
      now,
      recoveryId,
    );
    return ok(true);
  }

  markAdopted(adoptedInput: RecoveryAdopted, now: number): RecoveryStoreResult {
    const adopted = RecoveryAdoptedSchema.parse(adoptedInput);
    this.#validateTime(now);
    const attempt = this.attempt(adopted.recoveryId);
    if (attempt === undefined) return rejected("missing-attempt");
    const ownership = this.#validateAttemptGeneration(attempt, adopted.deliveryGeneration);
    if (ownership !== undefined) return rejected(ownership);
    const adoptedJson = canonicalJson(adopted);
    if (attempt.adopted_json !== null) {
      return attempt.adopted_json === adoptedJson ? ok(false) : rejected("immutable-conflict");
    }
    if (!this.#canProgress(attempt)) return rejected("state-conflict");
    if (this.#deadlineExpired(attempt, now)) return rejected("deadline-expired");
    const committed =
      attempt.committed_through_json === null
        ? undefined
        : parseCursor(attempt.committed_through_json);
    const applied = parseCursor(attempt.replica_applied_json);
    if (
      committed === undefined ||
      !cursorAtOrAfter(adopted.replicaApplied, committed) ||
      !cursorAtOrBefore(adopted.replicaApplied, applied) ||
      !this.#recoveryDoneReceived(attempt)
    ) {
      return rejected("invalid-progress");
    }
    this.sql.exec(
      `UPDATE recovery_attempt
       SET adopted_json = ?, no_progress_deadline_at = ?, updated_at = ?
       WHERE recovery_id = ?`,
      adoptedJson,
      this.#nextNoProgressDeadline(attempt, now),
      now,
      adopted.recoveryId,
    );
    this.#completeIfClosed(adopted.recoveryId, now);
    return ok(true);
  }

  markSourceClosed(closedInput: RecoveryV3HostSourceClosed, now: number): RecoveryStoreResult {
    const closed = RecoveryV3HostSourceClosedSchema.parse(closedInput);
    this.#validateTime(now);
    const attempt = this.attempt(closed.recoveryId);
    if (attempt === undefined) return rejected("missing-attempt");
    const ownership = this.#validateAttemptIdentity(attempt, closed);
    if (ownership !== undefined) return rejected(ownership);
    const closedJson = canonicalJson(closed);
    if (attempt.source_closed_json !== null) {
      return attempt.source_closed_json === closedJson ? ok(false) : rejected("immutable-conflict");
    }
    if (!this.#canProgress(attempt)) return rejected("state-conflict");
    if (this.#deadlineExpired(attempt, now)) return rejected("deadline-expired");
    if (
      attempt.recovery_done_ordinal !== closed.throughRecoveryOrdinal ||
      attempt.recovery_done_cumulative_encoded_bytes !==
        closed.throughRecoveryCumulativeEncodedBytes
    ) {
      return rejected("invalid-progress");
    }
    const recoveryLane = this.#lane(closed.recoveryId, "recovery");
    if (
      recoveryLane === undefined ||
      BigInt(recoveryLane.received_delivery_ordinal) < BigInt(closed.throughRecoveryOrdinal) ||
      BigInt(recoveryLane.received_cumulative_encoded_bytes) <
        BigInt(closed.throughRecoveryCumulativeEncodedBytes)
    ) {
      return rejected("progress-ahead");
    }
    if (!this.#hasOutboxCapacityFor(closed.recoveryId, "recovery-source-closed")) {
      return rejected("outbox-capacity");
    }
    this.sql.exec(
      `UPDATE recovery_attempt
       SET source_closed_json = ?, no_progress_deadline_at = ?, updated_at = ?
       WHERE recovery_id = ?`,
      closedJson,
      this.#nextNoProgressDeadline(attempt, now),
      now,
      closed.recoveryId,
    );
    this.#insertOutbox(
      closed.recoveryId,
      "recovery-source-closed",
      "browser",
      canonicalJson(toBrowserRecoverySourceClosed(closed)),
      now,
    );
    this.#completeIfClosed(closed.recoveryId, now);
    this.refillRecoveryGrantWindows(now);
    return ok(true);
  }

  advanceRecoveryGrant(
    recoveryId: string,
    cumulativeGrantedEncodedBytes: string,
    now: number,
  ): RecoveryStoreResult {
    const grant = DecimalU64Schema.parse(cumulativeGrantedEncodedBytes);
    this.#validateTime(now);
    const attempt = this.attempt(recoveryId);
    if (attempt === undefined) return rejected("missing-attempt");
    const ownership = this.#validateAttemptGeneration(attempt, attempt.delivery_generation);
    if (ownership !== undefined) return rejected(ownership);
    const previous = BigInt(attempt.granted_cumulative_encoded_bytes);
    const next = BigInt(grant);
    if (next === previous) return ok(false);
    if (!this.#canProgress(attempt)) return rejected("state-conflict");
    if (this.#deadlineExpired(attempt, now)) return rejected("deadline-expired");
    if (next < previous) return rejected("invalid-progress");
    if (!this.#hostReadyForRecoveryGrant(attempt)) return rejected("state-conflict");
    if (!this.#canReserveRecoveryGrant(attempt, next)) {
      return rejected("delivery-capacity");
    }
    if (!this.#hasOutboxCapacityFor(recoveryId, "recovery-source-grant")) {
      return rejected("outbox-capacity");
    }
    const frame = RecoveryV3HostSourceGrantSchema.parse({
      type: "recovery-source-grant",
      recoveryId: attempt.recovery_id,
      connectionId: attempt.connection_id,
      streamId: attempt.stream_id,
      deliveryGeneration: attempt.delivery_generation,
      cumulativeGrantedEncodedBytes: grant,
    });
    this.sql.exec(
      `UPDATE recovery_attempt
       SET granted_cumulative_encoded_bytes = ?, updated_at = ?
       WHERE recovery_id = ?`,
      grant,
      now,
      recoveryId,
    );
    this.#upsertOutbox(recoveryId, "recovery-source-grant", "host", canonicalJson(frame), now);
    return ok(true);
  }

  /**
   * Deterministically gives each eligible source at most one envelope-sized
   * credit slice. Repeated calls slide each source toward its byte window; a
   * grant is never created while its start-ready intent remains unacknowledged.
   */
  refillRecoveryGrantWindows(now: number): string[] {
    this.#validateTime(now);
    const candidates = this.#sessionDeliveryContributions()
      .filter(({ attempt }) => {
        return (
          (attempt.state === "installed" || attempt.state === "assembling") &&
          attempt.source_closed_json === null &&
          attempt.recovery_done_ordinal === null &&
          !this.#deadlineExpired(attempt, now) &&
          this.#validateAttemptGeneration(attempt, attempt.delivery_generation) === undefined &&
          this.#hostReadyForRecoveryGrant(attempt)
        );
      })
      .sort((left, right) => {
        const leftWriter = this.#attemptRole(left.attempt) === "writer";
        const rightWriter = this.#attemptRole(right.attempt) === "writer";
        if (leftWriter !== rightWriter) return leftWriter ? -1 : 1;
        if (left.recoveryReservedEncodedBytes !== right.recoveryReservedEncodedBytes) {
          return left.recoveryReservedEncodedBytes - right.recoveryReservedEncodedBytes;
        }
        if (left.attempt.updated_at !== right.attempt.updated_at) {
          return left.attempt.updated_at - right.attempt.updated_at;
        }
        return left.attempt.recovery_id < right.attempt.recovery_id
          ? -1
          : left.attempt.recovery_id > right.attempt.recovery_id
            ? 1
            : 0;
      });
    const changed: string[] = [];
    for (const { attempt } of candidates) {
      const current = this.attempt(attempt.recovery_id);
      if (current === undefined) continue;
      const contribution = this.#attemptDeliveryContribution(current);
      const currentGrant = BigInt(current.granted_cumulative_encoded_bytes);
      const windowTarget =
        contribution.recoveryReceivedCumulativeEncodedBytes +
        BigInt(this.limits.maxRecoveryGrantWindowEncodedBytes);
      const sliceTarget = currentGrant + BigInt(this.limits.maxDeliveryEnvelopeEncodedBytes);
      const target = [windowTarget, sliceTarget, MAX_U64].reduce((minimum, value) =>
        value < minimum ? value : minimum,
      );
      const next = this.#maximumGrantWithinCapacity(current, target);
      if (next <= currentGrant) continue;
      const currentHeadroom = currentGrant - contribution.recoveryTailCumulativeEncodedBytes;
      const nextHeadroom = next - contribution.recoveryTailCumulativeEncodedBytes;
      if (
        currentHeadroom < BigInt(this.limits.maxDeliveryEnvelopeEncodedBytes) &&
        nextHeadroom < BigInt(this.limits.maxDeliveryEnvelopeEncodedBytes)
      ) {
        continue;
      }
      const result = this.advanceRecoveryGrant(current.recovery_id, next.toString(), now);
      if (result.ok && result.changed) changed.push(current.recovery_id);
    }
    return changed;
  }

  /**
   * Trusted drain CAS. The runtime owner must first bind the entry's destination
   * to the current socket/connection identity; this is not an untrusted peer ACK.
   */
  acknowledgeOutbox(
    recoveryId: string,
    kind: RecoveryOutboxKind,
    payloadJson: string,
    now: number,
  ): RecoveryStoreResult {
    this.#validateTime(now);
    const entry = this.outboxEntry(recoveryId, kind);
    if (entry === undefined) return ok(false);
    if (entry.payload_json !== payloadJson) return rejected("immutable-conflict");
    this.sql.exec(
      `DELETE FROM recovery_control_outbox
       WHERE recovery_id = ? AND kind = ? AND payload_json = ?`,
      recoveryId,
      kind,
      payloadJson,
    );
    if (kind === "recovery-start") {
      this.sql.exec(
        `UPDATE recovery_attempt
         SET state = 'assembling', updated_at = ?
         WHERE recovery_id = ? AND state = 'installed'`,
        now,
        recoveryId,
      );
    }
    if (kind === "recovery-start-ready") this.refillRecoveryGrantWindows(now);
    this.#pruneFencedTerminalAttempt(recoveryId);
    return ok(true);
  }

  pruneFencedTerminalAttempts(): string[] {
    const candidates = this.sql
      .exec(
        `SELECT recovery_id FROM recovery_attempt AS attempt
         WHERE state IN ('complete', 'resetting')
           AND NOT EXISTS (
             SELECT 1 FROM recovery_control_outbox AS outbox
             WHERE outbox.recovery_id = attempt.recovery_id
           )
         ORDER BY updated_at, recovery_id
         LIMIT ?`,
        this.limits.maxAttempts,
      )
      .toArray() as unknown as { recovery_id: string }[];
    const removed: string[] = [];
    for (const row of candidates) {
      if (this.#pruneFencedTerminalAttempt(row.recovery_id)) removed.push(row.recovery_id);
    }
    return removed;
  }

  /**
   * Fences a delivery generation that retained payload-free queued/sending
   * obligations across a crash or hibernation boundary. A complete attempt has
   * no live Host recovery source, so it becomes a local terminal tombstone
   * without emitting a post-closure Host reset.
   */
  resetUnsafeDeliveryOutcome(recoveryId: string, now: number): RecoveryStoreResult {
    this.#validateTime(now);
    const attempt = this.attempt(recoveryId);
    if (attempt === undefined) return rejected("missing-attempt");
    if (attempt.state === "resetting") {
      return attempt.reset_reason === "ack-outcome-uncertain"
        ? ok(false)
        : rejected("immutable-conflict");
    }
    const unsafe = this.sql
      .exec(
        `SELECT recovery_id FROM recovery_delivery_record
         WHERE recovery_id = ? AND state IN ('queued', 'sending') LIMIT 1`,
        recoveryId,
      )
      .toArray()[0];
    if (unsafe === undefined) return rejected("state-conflict");
    return this.#fenceDeliveryOwner(attempt, "ack-outcome-uncertain", now);
  }

  /** Caller has already proven that the exact Browser receiver pair is gone. */
  resetUndeliverableDeliveryOwner(recoveryId: string, now: number): RecoveryStoreResult {
    this.#validateTime(now);
    const attempt = this.attempt(recoveryId);
    if (attempt === undefined) return rejected("missing-attempt");
    if (attempt.state === "resetting") {
      return attempt.reset_reason === "generation-reset"
        ? ok(false)
        : rejected("immutable-conflict");
    }
    if (attempt.state === "preparing") return rejected("state-conflict");
    return this.#fenceDeliveryOwner(attempt, "generation-reset", now);
  }

  reset(
    recoveryId: string,
    reasonInput: RecoveryV3HostSourceReset["reason"],
    now: number,
  ): RecoveryStoreResult {
    const reason = RecoveryV3HostSourceResetSchema.shape.reason.parse(reasonInput);
    this.#validateTime(now);
    const attempt = this.attempt(recoveryId);
    if (attempt === undefined) return rejected("missing-attempt");
    if (attempt.state === "resetting") {
      return attempt.reset_reason === reason ? ok(false) : rejected("immutable-conflict");
    }
    if (attempt.state === "complete") return rejected("state-conflict");
    const outboxCount = this.sql
      .exec("SELECT COUNT(*) AS value FROM recovery_control_outbox")
      .one() as { value: number };
    const attemptOutboxCount = this.sql
      .exec(
        "SELECT COUNT(*) AS value FROM recovery_control_outbox WHERE recovery_id = ?",
        recoveryId,
      )
      .one() as { value: number };
    if (outboxCount.value - attemptOutboxCount.value + 1 > this.limits.maxOutboxEntries) {
      return rejected("outbox-capacity");
    }
    this.sql.exec("DELETE FROM recovery_control_outbox WHERE recovery_id = ?", recoveryId);
    this.sql.exec("DELETE FROM recovery_delivery_lane WHERE recovery_id = ?", recoveryId);
    this.sql.exec(
      `UPDATE recovery_attempt
       SET state = 'resetting', reset_reason = ?, updated_at = ?
       WHERE recovery_id = ?`,
      reason,
      now,
      recoveryId,
    );
    const frame = RecoveryV3HostSourceResetSchema.parse({
      type: "recovery-source-reset",
      recoveryId: attempt.recovery_id,
      connectionId: attempt.connection_id,
      streamId: attempt.stream_id,
      deliveryGeneration: attempt.delivery_generation,
      reason,
    });
    this.#insertOutbox(recoveryId, "recovery-source-reset", "host", canonicalJson(frame), now);
    this.refillRecoveryGrantWindows(now);
    return ok(true);
  }

  fenceClientGeneration(clientId: string, currentGeneration: string, now: number): string[] {
    const generation = PositiveDecimalU64Schema.parse(currentGeneration);
    this.#validateTime(now);
    if (this.relay.clientById(clientId)?.delivery_generation !== generation) {
      throw new Error("cannot fence from a stale client generation");
    }
    const fenced = this.sql
      .exec(
        `SELECT recovery_id FROM recovery_attempt
         WHERE client_id = ? AND delivery_generation <> ?
           AND state IN ('preparing', 'installed', 'assembling')
         ORDER BY created_at, recovery_id`,
        clientId,
        generation,
      )
      .toArray() as unknown as { recovery_id: string }[];
    this.#requireResetOutboxCapacity(fenced);
    for (const row of fenced) this.#resetOrThrow(row.recovery_id, "generation-reset", now);
    return fenced.map((row) => row.recovery_id);
  }

  fenceHost(currentHostFence: string, now: number): string[] {
    const hostFence = DecimalU64Schema.parse(currentHostFence);
    this.#validateTime(now);
    if (this.relay.session()?.host_fence !== hostFence) {
      throw new Error("cannot fence from a stale host generation");
    }
    const fenced = this.sql
      .exec(
        `SELECT recovery_id FROM recovery_attempt
         WHERE host_fence <> ? AND state IN ('preparing', 'installed', 'assembling')
         ORDER BY created_at, recovery_id`,
        hostFence,
      )
      .toArray() as unknown as { recovery_id: string }[];
    this.#requireResetOutboxCapacity(fenced);
    for (const row of fenced) this.#resetOrThrow(row.recovery_id, "pair-fenced", now);
    return fenced.map((row) => row.recovery_id);
  }

  fenceRemovedClient(clientId: string, now: number): string[] {
    this.#validateTime(now);
    const fenced = this.sql
      .exec(
        `SELECT recovery_id FROM recovery_attempt
         WHERE client_id = ? AND state IN ('preparing', 'installed', 'assembling')
         ORDER BY created_at, recovery_id`,
        clientId,
      )
      .toArray() as unknown as { recovery_id: string }[];
    this.#requireResetOutboxCapacity(fenced);
    for (const row of fenced) this.#resetOrThrow(row.recovery_id, "pair-fenced", now);
    return fenced.map((row) => row.recovery_id);
  }

  /**
   * Fences valid legacy rows until the session satisfies the aggregate budget.
   * Victims are stable: observers first, then largest contribution, newest
   * update, and descending recovery id. Complete attempts are fenced locally
   * because no Host recovery source remains to reset.
   */
  reconcileSessionDeliveryCapacity(now: number): string[] {
    this.#validateTime(now);
    const fenced: string[] = [];
    for (let index = 0; index < this.limits.maxAttempts; index += 1) {
      const contributions = this.#sessionDeliveryContributions();
      const usage = this.sessionDeliveryUsage();
      const sessionOverBudget = !this.#sessionUsageWithinLimits(usage);
      const individuallyInvalid = contributions.filter(
        (contribution) => !this.#attemptUsageWithinLimits(contribution),
      );
      if (!sessionOverBudget && individuallyInvalid.length === 0) return fenced;
      const candidates = (sessionOverBudget ? contributions : individuallyInvalid)
        .filter(
          ({ attempt, encodedBytes, records }) =>
            (encodedBytes > 0 || records > 0) &&
            attempt.state !== "preparing" &&
            attempt.state !== "resetting",
        )
        .sort((left, right) => {
          const leftWriter = this.#attemptRole(left.attempt) === "writer";
          const rightWriter = this.#attemptRole(right.attempt) === "writer";
          if (leftWriter !== rightWriter) return leftWriter ? 1 : -1;
          if (left.encodedBytes !== right.encodedBytes) {
            return right.encodedBytes - left.encodedBytes;
          }
          if (left.records !== right.records) return right.records - left.records;
          if (left.attempt.updated_at !== right.attempt.updated_at) {
            return right.attempt.updated_at - left.attempt.updated_at;
          }
          return left.attempt.recovery_id < right.attempt.recovery_id
            ? 1
            : left.attempt.recovery_id > right.attempt.recovery_id
              ? -1
              : 0;
        });
      const victim = candidates[0]?.attempt;
      if (victim === undefined) {
        throw new Error("recovery session delivery capacity cannot be reconciled");
      }
      const result = this.#fenceDeliveryOwner(victim, "generation-reset", now);
      if (!result.ok || !result.changed) {
        throw new Error(
          `recovery capacity fence failed: ${result.ok ? "state did not advance" : result.reason}`,
        );
      }
      fenced.push(victim.recovery_id);
    }
    if (!this.#sessionUsageWithinLimits(this.sessionDeliveryUsage())) {
      throw new Error("recovery session delivery capacity cannot be reconciled");
    }
    return fenced;
  }

  expireDeadlines(now: number): string[] {
    this.#validateTime(now);
    const expired = this.sql
      .exec(
        `SELECT recovery_id FROM recovery_attempt
         WHERE state IN ('preparing', 'installed', 'assembling')
           AND (hard_deadline_at <= ? OR no_progress_deadline_at <= ?)
         ORDER BY hard_deadline_at, no_progress_deadline_at, recovery_id
         LIMIT ?`,
        now,
        now,
        this.limits.maxAttempts,
      )
      .toArray() as unknown as { recovery_id: string }[];
    const reset: string[] = [];
    for (const row of expired) {
      const result = this.reset(row.recovery_id, "deadline", now);
      if (result.ok && result.changed) reset.push(row.recovery_id);
    }
    return reset;
  }

  #validateTime(value: number): void {
    if (!Number.isSafeInteger(value) || value < 0) throw new RangeError("invalid recovery time");
  }

  #validateStreamId(value: number): void {
    if (!Number.isInteger(value) || value < 1 || value > 0xffff_ffff) {
      throw new RangeError("invalid recovery stream id");
    }
  }

  #resetOrThrow(
    recoveryId: string,
    reason: RecoveryV3HostSourceReset["reason"],
    now: number,
  ): void {
    const result = this.reset(recoveryId, reason, now);
    if (!result.ok || !result.changed) {
      throw new Error(
        `recovery reset failed: ${result.ok ? "state did not advance" : result.reason}`,
      );
    }
  }

  #fenceDeliveryOwner(
    attempt: RecoveryAttemptRow,
    reason: "ack-outcome-uncertain" | "generation-reset",
    now: number,
  ): RecoveryStoreResult {
    if (attempt.state !== "complete") return this.reset(attempt.recovery_id, reason, now);
    const updated = this.sql.exec(
      `UPDATE recovery_attempt
       SET state = 'resetting', reset_reason = ?, updated_at = ?
       WHERE recovery_id = ? AND state = 'complete'
       RETURNING recovery_id`,
      reason,
      now,
      attempt.recovery_id,
    );
    if (updated.toArray().length !== 1) return rejected("state-conflict");
    this.sql.exec("DELETE FROM recovery_control_outbox WHERE recovery_id = ?", attempt.recovery_id);
    this.sql.exec("DELETE FROM recovery_delivery_lane WHERE recovery_id = ?", attempt.recovery_id);
    this.refillRecoveryGrantWindows(now);
    return ok(true);
  }

  #requireResetOutboxCapacity(rows: readonly { recovery_id: string }[]): void {
    if (rows.length === 0) return;
    const count = this.sql.exec("SELECT COUNT(*) AS value FROM recovery_control_outbox").one() as {
      value: number;
    };
    const placeholders = rows.map(() => "?").join(", ");
    const replaced = this.sql
      .exec(
        `SELECT COUNT(*) AS value FROM recovery_control_outbox
         WHERE recovery_id IN (${placeholders})`,
        ...rows.map((row) => row.recovery_id),
      )
      .one() as { value: number };
    if (count.value - replaced.value + rows.length > this.limits.maxOutboxEntries) {
      throw new Error("recovery reset outbox capacity exceeded");
    }
  }

  #validateCurrentClient(
    clientId: string,
    streamId: number,
    generation: string,
  ): RecoveryStoreRejectReason | undefined {
    const client = this.relay.clientById(clientId);
    if (client === undefined || client.registered_at === null) return "client-missing";
    if (client.recovery_strategy !== "v3") return "client-v2";
    if (client.stream_id !== streamId || client.delivery_generation !== generation) {
      return "identity-mismatch";
    }
    return undefined;
  }

  #validateAttemptGeneration(
    attempt: RecoveryAttemptRow,
    generation: string,
  ): RecoveryStoreRejectReason | undefined {
    if (attempt.delivery_generation !== generation) return "identity-mismatch";
    const client = this.#validateCurrentClient(
      attempt.client_id,
      attempt.stream_id,
      attempt.delivery_generation,
    );
    if (client !== undefined) return client;
    if (this.relay.session()?.host_fence !== attempt.host_fence) return "identity-mismatch";
    return undefined;
  }

  #validateAttemptIdentity(
    attempt: RecoveryAttemptRow,
    identity: {
      connectionId: string;
      deliveryGeneration: string;
      engineId?: string;
      streamId: number;
    },
  ): RecoveryStoreRejectReason | undefined {
    if (
      attempt.connection_id !== identity.connectionId ||
      attempt.delivery_generation !== identity.deliveryGeneration ||
      attempt.stream_id !== identity.streamId ||
      (identity.engineId !== undefined && attempt.engine_id !== identity.engineId)
    ) {
      return "identity-mismatch";
    }
    return this.#validateAttemptGeneration(attempt, identity.deliveryGeneration);
  }

  #matchingFenceStart(
    attempt: RecoveryAttemptRow,
    fence: RecoveryStartFence,
    start: RecoveryStart,
  ): boolean {
    const prepare = RecoveryV3HostPrepareSchema.parse(JSON.parse(attempt.prepare_json) as unknown);
    const sourceMatches =
      prepare.source.kind === fence.source.kind &&
      fence.source.kind === start.source.kind &&
      (prepare.source.kind === "warm" ||
        (fence.source.kind === "snapshot" &&
          start.source.kind === "snapshot" &&
          prepare.source.snapshotId === fence.source.snapshotId &&
          fence.source.snapshotId === start.source.snapshotId));
    return (
      sourceMatches &&
      fence.recoveryId === start.recoveryId &&
      fence.deliveryGeneration === start.deliveryGeneration &&
      fence.streamId === start.streamId &&
      fence.engineId === start.engineId &&
      start.authorityDataVersion === 2 &&
      sameCursor(prepare.base, fence.base) &&
      sameCursor(fence.base, start.base) &&
      sameCursor(fence.committedThrough, start.committedThrough) &&
      canonicalJson(fence.liveFloor) === canonicalJson(start.liveFloor)
    );
  }

  #compareScalarProgress(
    nextOrdinal: string,
    nextBytes: string,
    previousOrdinal: string,
    previousBytes: string,
  ): "advance" | "invalid" | "same" {
    if (nextOrdinal === previousOrdinal && nextBytes === previousBytes) return "same";
    const ordinalDelta = BigInt(nextOrdinal) - BigInt(previousOrdinal);
    const byteDelta = BigInt(nextBytes) - BigInt(previousBytes);
    // Only the current received scalar is an authenticated idempotent retry.
    // Every advancing pair is authenticated separately against the exact
    // durable sent record and its complete sent prefix.
    if (ordinalDelta <= 0n || byteDelta <= 0n) return "invalid";
    return "advance";
  }

  #advanceSentAuthority(
    previous: AuthorityCursor,
    lane: RecoveryLane,
    frame: DataFrame,
  ): AuthorityCursor {
    if (
      frame.sessionEpoch.toString() !== previous.sessionEpoch ||
      frame.ptyOffset.toString() !== previous.nextPtyOffset
    ) {
      throw new Error("canonical authority identity changed");
    }
    const previousEvent = BigInt(previous.eventSeq);
    if (frame.kind === DataFrameKind.ReplayCommit) {
      if (lane !== "recovery" || frame.eventSeq !== previousEvent) {
        throw new Error("RecoveryDone is not at the current authority cursor");
      }
      return previous;
    }
    if (
      (frame.kind !== DataFrameKind.PtyOutput && frame.kind !== DataFrameKind.ResizeApplied) ||
      frame.eventSeq !== previousEvent + 1n
    ) {
      throw new Error("canonical mutation is not contiguous");
    }
    const nextPtyOffset =
      frame.kind === DataFrameKind.PtyOutput
        ? frame.ptyOffset + BigInt(frame.payload.byteLength)
        : frame.ptyOffset;
    return AuthorityCursorSchema.parse({
      sessionEpoch: previous.sessionEpoch,
      eventSeq: frame.eventSeq.toString(),
      nextPtyOffset: nextPtyOffset.toString(),
    });
  }

  #lane(recoveryId: string, lane: RecoveryLane): RecoveryDeliveryLaneRow | undefined {
    return this.sql
      .exec(
        `SELECT * FROM recovery_delivery_lane
         WHERE recovery_id = ? AND lane = ?`,
        recoveryId,
        lane,
      )
      .toArray()[0] as RecoveryDeliveryLaneRow | undefined;
  }

  #parseDeliveryIdentity(identity: RecoveryDeliveryRecordIdentity): RecoveryDeliveryRecordIdentity {
    if (
      typeof identity.recoveryId !== "string" ||
      identity.recoveryId.length < 16 ||
      identity.recoveryId.length > 128
    ) {
      throw new RangeError("invalid recovery delivery identity");
    }
    if (identity.lane !== "live" && identity.lane !== "recovery") {
      throw new RangeError("invalid recovery delivery lane");
    }
    if (!isPositiveSafeInteger(identity.encodedBytes)) {
      throw new RangeError("invalid recovery delivery size");
    }
    return {
      authorityCursorAfter: AuthorityCursorSchema.parse(identity.authorityCursorAfter),
      cumulativeEncodedBytes: PositiveDecimalU64Schema.parse(identity.cumulativeEncodedBytes),
      deliveryOrdinal: PositiveDecimalU64Schema.parse(identity.deliveryOrdinal),
      encodedBytes: identity.encodedBytes,
      lane: identity.lane,
      recoveryId: identity.recoveryId,
    };
  }

  #sameDeliveryRecord(
    row: RecoveryDeliveryRecordRow,
    identity: RecoveryDeliveryRecordIdentity,
  ): boolean {
    return (
      row.recovery_id === identity.recoveryId &&
      row.lane === identity.lane &&
      row.delivery_ordinal === identity.deliveryOrdinal &&
      row.cumulative_encoded_bytes === identity.cumulativeEncodedBytes &&
      row.encoded_bytes === identity.encodedBytes &&
      row.authority_cursor_after_json === canonicalJson(identity.authorityCursorAfter)
    );
  }

  #validatedDeliveryObligations(
    lane: RecoveryDeliveryLaneRow,
  ): RecoveryDeliveryRecordRow[] | undefined {
    try {
      this.deliveryUsage(lane.recovery_id);
    } catch {
      return undefined;
    }
    let receivedOrdinal: bigint;
    let receivedBytes: bigint;
    let receivedAuthority: AuthorityCursor;
    let sentAuthority: AuthorityCursor;
    try {
      receivedOrdinal = BigInt(DecimalU64Schema.parse(lane.received_delivery_ordinal));
      receivedBytes = BigInt(DecimalU64Schema.parse(lane.received_cumulative_encoded_bytes));
      receivedAuthority = parseCursor(lane.received_authority_cursor_json);
      sentAuthority = parseCursor(lane.sent_authority_cursor_json);
      if (
        DecimalU64Schema.parse(lane.sent_delivery_ordinal) !== lane.sent_delivery_ordinal ||
        DecimalU64Schema.parse(lane.sent_cumulative_encoded_bytes) !==
          lane.sent_cumulative_encoded_bytes ||
        canonicalJson(receivedAuthority) !== lane.received_authority_cursor_json ||
        canonicalJson(sentAuthority) !== lane.sent_authority_cursor_json
      ) {
        return undefined;
      }
    } catch {
      return undefined;
    }
    const rows = this.sql
      .exec(
        `SELECT * FROM recovery_delivery_record
         WHERE recovery_id = ? AND lane = ?
         ORDER BY length(delivery_ordinal), delivery_ordinal
         LIMIT ?`,
        lane.recovery_id,
        lane.lane,
        this.limits.maxDeliveryRecords + 1,
      )
      .toArray() as unknown as RecoveryDeliveryRecordRow[];
    if (rows.length > this.limits.maxDeliveryRecords) return undefined;

    let previousOrdinal = receivedOrdinal;
    let previousBytes = receivedBytes;
    let previousAuthority = receivedAuthority;
    let previousPhase = -1;
    let encodedBytes = 0;
    let lastSent: RecoveryDeliveryRecordRow | undefined;
    const phase: Record<RecoveryDeliveryRecordState, number> = {
      sent: 0,
      sending: 1,
      queued: 2,
    };
    for (const row of rows) {
      try {
        const ordinal = BigInt(PositiveDecimalU64Schema.parse(row.delivery_ordinal));
        const cumulative = BigInt(PositiveDecimalU64Schema.parse(row.cumulative_encoded_bytes));
        if (
          row.recovery_id !== lane.recovery_id ||
          row.lane !== lane.lane ||
          !isPositiveSafeInteger(row.encoded_bytes) ||
          ordinal !== previousOrdinal + 1n ||
          cumulative !== previousBytes + BigInt(row.encoded_bytes)
        ) {
          return undefined;
        }
        const authority = parseCursor(row.authority_cursor_after_json);
        if (
          canonicalJson(authority) !== row.authority_cursor_after_json ||
          !cursorAtOrAfter(authority, previousAuthority)
        ) {
          return undefined;
        }
        const currentPhase = phase[row.state];
        if (currentPhase === undefined || currentPhase < previousPhase) return undefined;
        previousPhase = currentPhase;
        if (row.state === "sent") lastSent = row;
        encodedBytes += row.encoded_bytes;
        if (
          !Number.isSafeInteger(encodedBytes) ||
          encodedBytes > this.limits.maxDeliveryEncodedBytes
        ) {
          return undefined;
        }
        previousOrdinal = ordinal;
        previousBytes = cumulative;
        previousAuthority = authority;
      } catch {
        return undefined;
      }
    }

    const expectedSentOrdinal = lastSent?.delivery_ordinal ?? lane.received_delivery_ordinal;
    const expectedSentBytes =
      lastSent?.cumulative_encoded_bytes ?? lane.received_cumulative_encoded_bytes;
    const expectedSentAuthority =
      lastSent?.authority_cursor_after_json ?? lane.received_authority_cursor_json;
    if (
      lane.sent_delivery_ordinal !== expectedSentOrdinal ||
      lane.sent_cumulative_encoded_bytes !== expectedSentBytes ||
      lane.sent_authority_cursor_json !== expectedSentAuthority
    ) {
      return undefined;
    }
    return rows;
  }

  #sessionDeliveryContributions(): RecoveryAttemptDeliveryContribution[] {
    const attempts = this.sql
      .exec(
        `SELECT * FROM recovery_attempt
         ORDER BY recovery_id LIMIT ?`,
        this.limits.maxAttempts + 1,
      )
      .toArray() as unknown as RecoveryAttemptRow[];
    if (attempts.length > this.limits.maxAttempts) {
      throw new Error("recovery attempt capacity exceeded");
    }
    return attempts.map((attempt) => this.#attemptDeliveryContribution(attempt));
  }

  #attemptDeliveryContribution(attempt: RecoveryAttemptRow): RecoveryAttemptDeliveryContribution {
    const laneUsage = this.sql
      .exec(
        `SELECT lane, COUNT(*) AS records, COALESCE(SUM(encoded_bytes), 0) AS encoded_bytes
         FROM recovery_delivery_record
         WHERE recovery_id = ?
         GROUP BY lane ORDER BY lane
         LIMIT 3`,
        attempt.recovery_id,
      )
      .toArray() as unknown as {
      encoded_bytes: number;
      lane: RecoveryLane;
      records: number;
    }[];
    if (laneUsage.length > 2) throw new Error("invalid durable recovery delivery lanes");
    let liveEncodedBytes = 0;
    let liveRecords = 0;
    let recoveryEncodedBytes = 0;
    let recoveryRecords = 0;
    for (const lane of laneUsage) {
      if (
        (lane.lane !== "live" && lane.lane !== "recovery") ||
        !Number.isSafeInteger(lane.records) ||
        lane.records < 0 ||
        !Number.isSafeInteger(lane.encoded_bytes) ||
        lane.encoded_bytes < 0
      ) {
        throw new Error("invalid durable session delivery usage");
      }
      if (lane.lane === "live") {
        liveEncodedBytes = lane.encoded_bytes;
        liveRecords = lane.records;
      } else {
        recoveryEncodedBytes = lane.encoded_bytes;
        recoveryRecords = lane.records;
      }
    }

    let recoveryReservedEncodedBytes = 0;
    let recoveryReservedRecords = 0;
    let recoveryReceivedCumulativeEncodedBytes = 0n;
    let recoveryTailCumulativeEncodedBytes = 0n;
    const lane = this.#lane(attempt.recovery_id, "recovery");
    if (lane !== undefined) {
      recoveryReceivedCumulativeEncodedBytes = BigInt(
        DecimalU64Schema.parse(lane.received_cumulative_encoded_bytes),
      );
      const tail = this.sql
        .exec(
          `SELECT cumulative_encoded_bytes FROM recovery_delivery_record
           WHERE recovery_id = ? AND lane = 'recovery'
           ORDER BY length(delivery_ordinal) DESC, delivery_ordinal DESC
           LIMIT 1`,
          attempt.recovery_id,
        )
        .toArray()[0] as { cumulative_encoded_bytes: string } | undefined;
      recoveryTailCumulativeEncodedBytes =
        tail === undefined
          ? recoveryReceivedCumulativeEncodedBytes
          : BigInt(DecimalU64Schema.parse(tail.cumulative_encoded_bytes));
      if (recoveryTailCumulativeEncodedBytes < recoveryReceivedCumulativeEncodedBytes) {
        throw new Error("recovery delivery tail precedes received progress");
      }
    }

    const reservesRecovery =
      (attempt.state === "installed" || attempt.state === "assembling") &&
      attempt.source_closed_json === null;
    const granted = BigInt(DecimalU64Schema.parse(attempt.granted_cumulative_encoded_bytes));
    if (reservesRecovery) {
      if (lane === undefined || granted < recoveryTailCumulativeEncodedBytes) {
        throw new Error("recovery grant precedes durable delivery tail");
      }
      const outstanding = granted - recoveryReceivedCumulativeEncodedBytes;
      const unmaterialized = granted - recoveryTailCumulativeEncodedBytes;
      recoveryReservedEncodedBytes = this.#usageNumber(outstanding);
      recoveryReservedRecords = this.#usageNumber(
        unmaterialized / BigInt(this.limits.minDeliveryEnvelopeEncodedBytes),
      );
    } else if (attempt.state === "preparing" && granted !== 0n) {
      throw new Error("preparing recovery attempt retained a grant");
    }

    return {
      attempt,
      encodedBytes: this.#addUsage(liveEncodedBytes, recoveryReservedEncodedBytes),
      liveEncodedBytes,
      liveRecords,
      records: this.#addUsage(
        this.#addUsage(liveRecords, recoveryRecords),
        recoveryReservedRecords,
      ),
      recoveryEncodedBytes,
      recoveryRecords,
      recoveryReservedEncodedBytes,
      recoveryReservedRecords,
      recoveryTailCumulativeEncodedBytes,
      recoveryReceivedCumulativeEncodedBytes,
    };
  }

  #addUsage(left: number, right: number): number {
    const result = left + right;
    if (!Number.isSafeInteger(result) || result < 0) {
      throw new Error("recovery session delivery usage overflow");
    }
    return result;
  }

  #usageNumber(value: bigint): number {
    if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new Error("recovery session delivery usage overflow");
    }
    return Number(value);
  }

  #hasDeliveryCapacity(
    attempt: RecoveryAttemptRow,
    lane: RecoveryLane,
    additionalEncodedBytes: number,
  ): boolean {
    let usage: RecoveryDeliveryUsage;
    let sessionUsage: RecoverySessionDeliveryUsage;
    let contribution: RecoveryAttemptDeliveryContribution;
    try {
      usage = this.deliveryUsage(attempt.recovery_id);
      sessionUsage = this.sessionDeliveryUsage();
      contribution = this.#attemptDeliveryContribution(attempt);
    } catch {
      return false;
    }
    if (
      usage.records >= this.limits.maxDeliveryRecords ||
      usage.encodedBytes + additionalEncodedBytes > this.limits.maxDeliveryEncodedBytes
    ) {
      return false;
    }
    if (lane === "live") {
      const observerAdmission =
        this.#attemptRole(attempt) !== "observer" ||
        !this.#hasActiveWriterDeliveryOwner() ||
        (sessionUsage.observerEncodedBytes + additionalEncodedBytes <=
          this.limits.maxSessionDeliveryEncodedBytes -
            this.limits.writerDeliveryReserveEncodedBytes &&
          sessionUsage.observerRecords + 1 <=
            this.limits.maxSessionDeliveryRecords - this.limits.writerDeliveryReserveRecords);
      return (
        contribution.liveRecords < this.limits.maxAttemptLiveRecords &&
        contribution.liveEncodedBytes + additionalEncodedBytes <=
          this.limits.maxAttemptLiveEncodedBytes &&
        sessionUsage.encodedBytes + additionalEncodedBytes <=
          this.limits.maxSessionDeliveryEncodedBytes &&
        sessionUsage.records + 1 <= this.limits.maxSessionDeliveryRecords &&
        observerAdmission
      );
    }

    const nextTail =
      contribution.recoveryTailCumulativeEncodedBytes + BigInt(additionalEncodedBytes);
    const grant = BigInt(attempt.granted_cumulative_encoded_bytes);
    if (nextTail > grant) return false;
    const nextReservedRecords = this.#usageNumber(
      (grant - nextTail) / BigInt(this.limits.minDeliveryEnvelopeEncodedBytes),
    );
    const nextSessionRecords =
      sessionUsage.records - contribution.recoveryReservedRecords + nextReservedRecords + 1;
    const nextRecoveryRecords =
      sessionUsage.recoveryRecords +
      1 +
      (sessionUsage.recoveryReservedRecords - contribution.recoveryReservedRecords) +
      nextReservedRecords;
    const nextAttemptRecords =
      contribution.records - contribution.recoveryReservedRecords + nextReservedRecords + 1;
    const observerAdmission =
      this.#attemptRole(attempt) !== "observer" ||
      !this.#hasActiveWriterDeliveryOwner() ||
      (sessionUsage.observerEncodedBytes <=
        this.limits.maxSessionDeliveryEncodedBytes -
          this.limits.writerDeliveryReserveEncodedBytes &&
        sessionUsage.observerRecords - contribution.records + nextAttemptRecords <=
          this.limits.maxSessionDeliveryRecords - this.limits.writerDeliveryReserveRecords);
    return (
      sessionUsage.recoveryRecords + 1 <= this.limits.maxSessionRecoveryRecords &&
      nextRecoveryRecords <= this.limits.maxSessionRecoveryRecords &&
      nextSessionRecords <= this.limits.maxSessionDeliveryRecords &&
      sessionUsage.encodedBytes <= this.limits.maxSessionDeliveryEncodedBytes &&
      sessionUsage.recoveryReservedEncodedBytes <= this.limits.maxSessionRecoveryEncodedBytes &&
      observerAdmission
    );
  }

  #canReserveRecoveryGrant(attempt: RecoveryAttemptRow, nextGrant: bigint): boolean {
    let usage: RecoverySessionDeliveryUsage;
    let contribution: RecoveryAttemptDeliveryContribution;
    try {
      usage = this.sessionDeliveryUsage();
      contribution = this.#attemptDeliveryContribution(attempt);
    } catch {
      return false;
    }
    const currentGrant = BigInt(attempt.granted_cumulative_encoded_bytes);
    if (nextGrant < currentGrant || nextGrant < contribution.recoveryTailCumulativeEncodedBytes) {
      return false;
    }
    const nextOutstanding = nextGrant - contribution.recoveryReceivedCumulativeEncodedBytes;
    if (nextOutstanding > BigInt(this.limits.maxRecoveryGrantWindowEncodedBytes)) {
      return false;
    }
    const nextReservedRecords = this.#usageNumber(
      (nextGrant - contribution.recoveryTailCumulativeEncodedBytes) /
        BigInt(this.limits.minDeliveryEnvelopeEncodedBytes),
    );
    const nextReservedEncodedBytes = this.#usageNumber(nextOutstanding);
    const nextSessionEncodedBytes =
      usage.encodedBytes - contribution.recoveryReservedEncodedBytes + nextReservedEncodedBytes;
    const nextSessionRecords =
      usage.records - contribution.recoveryReservedRecords + nextReservedRecords;
    const nextRecoveryReservedEncodedBytes =
      usage.recoveryReservedEncodedBytes -
      contribution.recoveryReservedEncodedBytes +
      nextReservedEncodedBytes;
    const nextRecoveryEffectiveRecords =
      usage.recoveryRecords +
      usage.recoveryReservedRecords -
      contribution.recoveryReservedRecords +
      nextReservedRecords;
    const nextContributionEncodedBytes = contribution.liveEncodedBytes + nextReservedEncodedBytes;
    const nextContributionRecords =
      contribution.liveRecords + contribution.recoveryRecords + nextReservedRecords;
    const observerAdmission =
      this.#attemptRole(attempt) !== "observer" ||
      !this.#hasActiveWriterDeliveryOwner() ||
      (usage.observerEncodedBytes - contribution.encodedBytes + nextContributionEncodedBytes <=
        this.limits.maxSessionDeliveryEncodedBytes -
          this.limits.writerDeliveryReserveEncodedBytes &&
        usage.observerRecords - contribution.records + nextContributionRecords <=
          this.limits.maxSessionDeliveryRecords - this.limits.writerDeliveryReserveRecords);
    return (
      nextSessionEncodedBytes <= this.limits.maxSessionDeliveryEncodedBytes &&
      nextSessionRecords <= this.limits.maxSessionDeliveryRecords &&
      nextRecoveryReservedEncodedBytes <= this.limits.maxSessionRecoveryEncodedBytes &&
      nextRecoveryEffectiveRecords <= this.limits.maxSessionRecoveryRecords &&
      observerAdmission
    );
  }

  #maximumGrantWithinCapacity(attempt: RecoveryAttemptRow, targetGrant: bigint): bigint {
    const currentGrant = BigInt(attempt.granted_cumulative_encoded_bytes);
    if (targetGrant <= currentGrant) return currentGrant;
    let usage: RecoverySessionDeliveryUsage;
    let contribution: RecoveryAttemptDeliveryContribution;
    try {
      usage = this.sessionDeliveryUsage();
      contribution = this.#attemptDeliveryContribution(attempt);
    } catch {
      return currentGrant;
    }
    let availableSessionBytes = this.limits.maxSessionDeliveryEncodedBytes - usage.encodedBytes;
    const availableRecoveryBytes =
      this.limits.maxSessionRecoveryEncodedBytes - usage.recoveryReservedEncodedBytes;
    let availableSessionRecords = this.limits.maxSessionDeliveryRecords - usage.records;
    const recoveryEffectiveRecords = usage.recoveryRecords + usage.recoveryReservedRecords;
    const availableRecoveryRecords =
      this.limits.maxSessionRecoveryRecords - recoveryEffectiveRecords;
    if (this.#attemptRole(attempt) === "observer" && this.#hasActiveWriterDeliveryOwner()) {
      availableSessionBytes = Math.min(
        availableSessionBytes,
        this.limits.maxSessionDeliveryEncodedBytes -
          this.limits.writerDeliveryReserveEncodedBytes -
          usage.observerEncodedBytes,
      );
      availableSessionRecords = Math.min(
        availableSessionRecords,
        this.limits.maxSessionDeliveryRecords -
          this.limits.writerDeliveryReserveRecords -
          usage.observerRecords,
      );
    }
    if (
      availableSessionBytes < 0 ||
      availableRecoveryBytes < 0 ||
      availableSessionRecords < 0 ||
      availableRecoveryRecords < 0
    ) {
      return currentGrant;
    }

    const byteDelta = Math.min(availableSessionBytes, availableRecoveryBytes);
    const maxByBytes = currentGrant + BigInt(byteDelta);
    const allowedReservedRecords =
      contribution.recoveryReservedRecords +
      Math.min(availableSessionRecords, availableRecoveryRecords);
    const maxUnmaterializedBytes =
      BigInt(allowedReservedRecords + 1) * BigInt(this.limits.minDeliveryEnvelopeEncodedBytes) - 1n;
    const maxByRecords = contribution.recoveryTailCumulativeEncodedBytes + maxUnmaterializedBytes;
    const maxByWindow =
      contribution.recoveryReceivedCumulativeEncodedBytes +
      BigInt(this.limits.maxRecoveryGrantWindowEncodedBytes);
    const next = [targetGrant, maxByBytes, maxByRecords, maxByWindow, MAX_U64].reduce(
      (minimum, value) => (value < minimum ? value : minimum),
    );
    return next > currentGrant && this.#canReserveRecoveryGrant(attempt, next)
      ? next
      : currentGrant;
  }

  #attemptRole(attempt: RecoveryAttemptRow): "observer" | "writer" {
    const client = this.relay.clientById(attempt.client_id);
    return client !== undefined &&
      client.registered_at !== null &&
      client.stream_id === attempt.stream_id &&
      client.delivery_generation === attempt.delivery_generation &&
      client.role === "writer"
      ? "writer"
      : "observer";
  }

  #hasActiveWriterDeliveryOwner(): boolean {
    const hostFence = this.relay.session()?.host_fence;
    if (hostFence === undefined) return false;
    const row = this.sql
      .exec(
        `SELECT attempt.recovery_id
         FROM recovery_attempt AS attempt
         JOIN client_delivery AS client ON client.client_id = attempt.client_id
         WHERE client.role = 'writer' AND client.registered_at IS NOT NULL
           AND client.stream_id = attempt.stream_id
           AND client.delivery_generation = attempt.delivery_generation
           AND attempt.host_fence = ?
           AND attempt.state IN ('preparing', 'installed', 'assembling', 'complete')
         ORDER BY attempt.recovery_id LIMIT 1`,
        hostFence,
      )
      .toArray()[0];
    return row !== undefined;
  }

  #sessionUsageWithinLimits(usage: RecoverySessionDeliveryUsage): boolean {
    const observerReserveSatisfied =
      !this.#hasActiveWriterDeliveryOwner() ||
      (usage.observerEncodedBytes <=
        this.limits.maxSessionDeliveryEncodedBytes -
          this.limits.writerDeliveryReserveEncodedBytes &&
        usage.observerRecords <=
          this.limits.maxSessionDeliveryRecords - this.limits.writerDeliveryReserveRecords);
    return (
      usage.encodedBytes <= this.limits.maxSessionDeliveryEncodedBytes &&
      usage.records <= this.limits.maxSessionDeliveryRecords &&
      usage.recoveryReservedEncodedBytes <= this.limits.maxSessionRecoveryEncodedBytes &&
      usage.recoveryRecords + usage.recoveryReservedRecords <=
        this.limits.maxSessionRecoveryRecords &&
      observerReserveSatisfied
    );
  }

  #attemptUsageWithinLimits(contribution: RecoveryAttemptDeliveryContribution): boolean {
    const actualEncodedBytes = this.#addUsage(
      contribution.liveEncodedBytes,
      contribution.recoveryEncodedBytes,
    );
    const actualRecords = this.#addUsage(contribution.liveRecords, contribution.recoveryRecords);
    return (
      actualEncodedBytes <= this.limits.maxDeliveryEncodedBytes &&
      actualRecords <= this.limits.maxDeliveryRecords &&
      contribution.liveEncodedBytes <= this.limits.maxAttemptLiveEncodedBytes &&
      contribution.liveRecords <= this.limits.maxAttemptLiveRecords &&
      contribution.recoveryReservedEncodedBytes <= this.limits.maxRecoveryGrantWindowEncodedBytes
    );
  }

  #hostReadyForRecoveryGrant(attempt: RecoveryAttemptRow): boolean {
    return this.outboxEntry(attempt.recovery_id, "recovery-start-ready") === undefined;
  }

  #canProgress(attempt: RecoveryAttemptRow): boolean {
    return attempt.state === "installed" || attempt.state === "assembling";
  }

  #canLaneProgress(attempt: RecoveryAttemptRow, lane: RecoveryLane): boolean {
    return this.#canProgress(attempt) || (attempt.state === "complete" && lane === "live");
  }

  #canReplicaProgress(attempt: RecoveryAttemptRow): boolean {
    return this.#canProgress(attempt) || attempt.state === "complete";
  }

  #deadlineExpired(attempt: RecoveryAttemptRow, now: number): boolean {
    return attempt.hard_deadline_at <= now || attempt.no_progress_deadline_at <= now;
  }

  #validSentAuthority(
    attempt: RecoveryAttemptRow,
    lane: RecoveryLane,
    cursor: AuthorityCursor,
  ): boolean {
    if (attempt.committed_through_json === null) return false;
    const committed = parseCursor(attempt.committed_through_json);
    const session = this.relay.session();
    if (session === undefined) return false;
    const head = AuthorityCursorSchema.safeParse({
      sessionEpoch: session.session_epoch,
      eventSeq: session.head_event_seq,
      nextPtyOffset: session.next_pty_offset,
    });
    if (!head.success || !cursorAtOrBefore(cursor, head.data)) return false;
    return lane === "recovery"
      ? cursorAtOrBefore(cursor, committed)
      : cursorAtOrAfter(cursor, committed);
  }

  #recoveryDoneReceived(attempt: RecoveryAttemptRow): boolean {
    const recovery = this.#lane(attempt.recovery_id, "recovery");
    return (
      recovery !== undefined &&
      attempt.recovery_done_ordinal !== null &&
      attempt.recovery_done_cumulative_encoded_bytes !== null &&
      BigInt(recovery.received_delivery_ordinal) >= BigInt(attempt.recovery_done_ordinal) &&
      BigInt(recovery.received_cumulative_encoded_bytes) >=
        BigInt(attempt.recovery_done_cumulative_encoded_bytes)
    );
  }

  #receivedAuthorityCeiling(attempt: RecoveryAttemptRow): AuthorityCursor | undefined {
    const recovery = this.#lane(attempt.recovery_id, "recovery");
    if (recovery === undefined) return undefined;
    if (!this.#recoveryDoneReceived(attempt)) {
      return parseCursor(recovery.received_authority_cursor_json);
    }
    const live = this.#lane(attempt.recovery_id, "live");
    return live === undefined ? undefined : parseCursor(live.received_authority_cursor_json);
  }

  #touchProgress(attempt: RecoveryAttemptRow, now: number): void {
    this.sql.exec(
      `UPDATE recovery_attempt
       SET no_progress_deadline_at = ?, updated_at = ? WHERE recovery_id = ?`,
      this.#nextNoProgressDeadline(attempt, now),
      now,
      attempt.recovery_id,
    );
  }

  #nextNoProgressDeadline(attempt: RecoveryAttemptRow, now: number): number {
    return Math.min(attempt.hard_deadline_at, now + attempt.no_progress_timeout_ms);
  }

  #completeIfClosed(recoveryId: string, now: number): void {
    this.sql.exec(
      `UPDATE recovery_attempt
       SET state = 'complete', updated_at = ?
       WHERE recovery_id = ? AND adopted_json IS NOT NULL AND source_closed_json IS NOT NULL
         AND state IN ('installed', 'assembling')`,
      now,
      recoveryId,
    );
  }

  #pruneTerminalClientAttempts(clientId: string, generation: string): void {
    const prunable = this.sql
      .exec(
        `SELECT recovery_id FROM recovery_attempt AS attempt
         WHERE client_id = ? AND delivery_generation <> ?
           AND state IN ('complete', 'resetting')
           AND NOT EXISTS (
             SELECT 1 FROM recovery_control_outbox AS outbox
             WHERE outbox.recovery_id = attempt.recovery_id
           )`,
        clientId,
        generation,
      )
      .toArray() as unknown as { recovery_id: string }[];
    for (const row of prunable) {
      this.sql.exec("DELETE FROM recovery_delivery_lane WHERE recovery_id = ?", row.recovery_id);
      this.sql.exec("DELETE FROM recovery_attempt WHERE recovery_id = ?", row.recovery_id);
    }
  }

  #pruneFencedTerminalAttempt(recoveryId: string): boolean {
    const attempt = this.attempt(recoveryId);
    if (
      attempt === undefined ||
      (attempt.state !== "complete" && attempt.state !== "resetting") ||
      this.#outboxCount(recoveryId) !== 0
    ) {
      return false;
    }
    const client = this.relay.clientById(attempt.client_id);
    const stillOwned =
      client !== undefined &&
      client.recovery_strategy === "v3" &&
      client.delivery_generation === attempt.delivery_generation &&
      client.stream_id === attempt.stream_id &&
      this.relay.session()?.host_fence === attempt.host_fence;
    if (stillOwned) return false;
    this.sql.exec("DELETE FROM recovery_delivery_lane WHERE recovery_id = ?", recoveryId);
    this.sql.exec("DELETE FROM recovery_attempt WHERE recovery_id = ?", recoveryId);
    return true;
  }

  #outboxCount(recoveryId: string): number {
    return (
      this.sql
        .exec(
          "SELECT COUNT(*) AS value FROM recovery_control_outbox WHERE recovery_id = ?",
          recoveryId,
        )
        .one() as { value: number }
    ).value;
  }

  #hasOutboxCapacity(additional: number): boolean {
    const count = this.sql.exec("SELECT COUNT(*) AS value FROM recovery_control_outbox").one() as {
      value: number;
    };
    return count.value + additional <= this.limits.maxOutboxEntries;
  }

  #hasOutboxCapacityFor(recoveryId: string, kind: RecoveryOutboxKind): boolean {
    const count = this.sql.exec("SELECT COUNT(*) AS value FROM recovery_control_outbox").one() as {
      value: number;
    };
    return (
      this.outboxEntry(recoveryId, kind) !== undefined || count.value < this.limits.maxOutboxEntries
    );
  }

  #insertOutbox(
    recoveryId: string,
    kind: RecoveryOutboxKind,
    destination: "browser" | "host",
    payloadJson: string,
    now: number,
  ): void {
    this.sql.exec(
      `INSERT INTO recovery_control_outbox
        (recovery_id, kind, destination, payload_json, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?)`,
      recoveryId,
      kind,
      destination,
      payloadJson,
      now,
      now,
    );
  }

  #upsertOutbox(
    recoveryId: string,
    kind: RecoveryOutboxKind,
    destination: "browser" | "host",
    payloadJson: string,
    now: number,
  ): void {
    this.sql.exec(
      `INSERT INTO recovery_control_outbox
        (recovery_id, kind, destination, payload_json, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT (recovery_id, kind) DO UPDATE SET
         destination = excluded.destination,
         payload_json = excluded.payload_json,
         updated_at = excluded.updated_at`,
      recoveryId,
      kind,
      destination,
      payloadJson,
      now,
      now,
    );
  }
}
