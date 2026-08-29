import type { RecoveryStrategy } from "@zhongduan/protocol";
import type { CapabilityRole } from "./auth";
import type { RelayChannel } from "./relay-socket";

export interface TicketRow {
  channel: RelayChannel;
  client_id: string | null;
  connection_id: string;
  connection_set_id: string;
  delivery_generation: string;
  expires_at: number;
  host_fence: string | null;
  recovery_strategy: RecoveryStrategy;
  relay_capabilities_json: string;
  peer: "host" | "browser";
  role: CapabilityRole;
  stream_id: number;
  subject: string;
  ticket_digest: string;
}

export interface SessionRow {
  authority_data_version: 2;
  engine_id: string;
  head_event_seq: string;
  host_fence: string;
  latest_snapshot_id: string | null;
  next_pty_offset: string;
  next_stream_id: number;
  recent_snapshot_id_1: string | null;
  recent_snapshot_id_2: string | null;
  snapshot_created_clock: number;
  snapshot_recent_candidates_json: string;
  snapshot_recent_scan_before: number | null;
  snapshot_recent_scan_done: number;
  snapshot_retention_backlog: number;
  session_epoch: string;
  session_id: string;
}

export interface ClientRow {
  client_id: string;
  delivery_generation: string;
  principal_id_hash: string;
  registered_at: number | null;
  reservation_expires_at: number | null;
  recovery_strategy: RecoveryStrategy;
  role: "writer" | "observer";
  stream_id: number;
}

export interface WriterLeaseRow {
  client_id: string;
  expires_at: number;
  fence: string;
  lease_digest: string;
}

export class RelayStore {
  constructor(private readonly sql: SqlStorage) {}

  session(): SessionRow | undefined {
    return this.sql.exec("SELECT * FROM session_state WHERE singleton = 1").toArray()[0] as
      | SessionRow
      | undefined;
  }

  clientById(clientId: string): ClientRow | undefined {
    return this.sql
      .exec("SELECT * FROM client_delivery WHERE client_id = ?", clientId)
      .toArray()[0] as ClientRow | undefined;
  }

  clientByStream(streamId: number): ClientRow | undefined {
    return this.sql
      .exec("SELECT * FROM client_delivery WHERE stream_id = ?", streamId)
      .toArray()[0] as ClientRow | undefined;
  }

  writerLease(): WriterLeaseRow | undefined {
    return this.sql.exec("SELECT * FROM writer_lease WHERE singleton = 1").toArray()[0] as
      | WriterLeaseRow
      | undefined;
  }

  ticket(ticketDigest: string): TicketRow | undefined {
    return this.sql
      .exec("SELECT * FROM connection_ticket WHERE ticket_digest = ?", ticketDigest)
      .toArray()[0] as TicketRow | undefined;
  }
}

export function migrateRelayStore(state: DurableObjectState, sql: SqlStorage): void {
  let version = state.storage.kv.get<number>("schema-version") ?? 0;
  if (version < 1) {
    state.storage.transactionSync(() => {
      sql.exec(`
        CREATE TABLE IF NOT EXISTS session_state (
          singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
          session_id TEXT NOT NULL UNIQUE,
          session_epoch TEXT NOT NULL,
          engine_id TEXT NOT NULL,
          host_fence TEXT NOT NULL DEFAULT '0',
          host_agent_epoch TEXT,
          latest_snapshot_id TEXT,
          next_stream_id INTEGER NOT NULL DEFAULT 1,
          terminated_at INTEGER,
          updated_at INTEGER NOT NULL
        )
      `);
      sql.exec(`
        CREATE TABLE IF NOT EXISTS client_delivery (
          client_id TEXT PRIMARY KEY,
          principal_id_hash TEXT NOT NULL,
          role TEXT NOT NULL CHECK (role IN ('writer', 'observer')),
          stream_id INTEGER NOT NULL UNIQUE,
          delivery_generation TEXT NOT NULL,
          updated_at INTEGER NOT NULL
        )
      `);
      sql.exec(`
        CREATE TABLE IF NOT EXISTS writer_lease (
          singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
          client_id TEXT NOT NULL,
          lease_digest TEXT NOT NULL,
          fence TEXT NOT NULL,
          expires_at INTEGER NOT NULL
        )
      `);
      sql.exec(`
        CREATE TABLE IF NOT EXISTS snapshot (
          snapshot_id TEXT PRIMARY KEY,
          session_epoch TEXT NOT NULL,
          cut_event_seq TEXT NOT NULL,
          next_pty_offset TEXT NOT NULL,
          engine_id TEXT NOT NULL,
          object_key TEXT NOT NULL UNIQUE,
          r2_version TEXT NOT NULL,
          etag TEXT NOT NULL,
          sha256 TEXT NOT NULL,
          compressed_length INTEGER NOT NULL,
          uncompressed_length TEXT NOT NULL,
          compression TEXT NOT NULL CHECK (compression IN ('none', 'zstd')),
          state TEXT NOT NULL CHECK (state IN ('servable', 'retired')),
          created_at INTEGER NOT NULL
        )
      `);
      sql.exec(`
        CREATE TABLE IF NOT EXISTS connection_ticket (
          ticket_digest TEXT PRIMARY KEY,
          connection_set_id TEXT NOT NULL,
          connection_id TEXT NOT NULL,
          peer TEXT NOT NULL CHECK (peer IN ('host', 'browser')),
          channel TEXT NOT NULL CHECK (channel IN ('control', 'data')),
          client_id TEXT,
          subject TEXT NOT NULL,
          role TEXT NOT NULL CHECK (role IN ('host', 'writer', 'observer')),
          stream_id INTEGER NOT NULL,
          delivery_generation TEXT NOT NULL,
          host_fence TEXT,
          expires_at INTEGER NOT NULL
        )
      `);
      sql.exec(
        "CREATE INDEX IF NOT EXISTS connection_ticket_expiry ON connection_ticket(expires_at)",
      );
      state.storage.kv.put("schema-version", 1);
    });
    version = 1;
  }

  if (version < 2) {
    state.storage.transactionSync(() => {
      sql.exec("ALTER TABLE session_state ADD COLUMN head_event_seq TEXT NOT NULL DEFAULT '0'");
      sql.exec("ALTER TABLE session_state ADD COLUMN next_pty_offset TEXT NOT NULL DEFAULT '0'");
      state.storage.kv.put("schema-version", 2);
    });
    version = 2;
  }

  if (version < 3) {
    state.storage.transactionSync(() => {
      sql.exec("ALTER TABLE client_delivery ADD COLUMN registered_at INTEGER");
      sql.exec("ALTER TABLE client_delivery ADD COLUMN reservation_expires_at INTEGER");
      sql.exec("UPDATE client_delivery SET registered_at = updated_at");
      state.storage.kv.put("schema-version", 3);
    });
    version = 3;
  }

  if (version < 4) {
    state.storage.transactionSync(() => {
      sql.exec(
        "ALTER TABLE snapshot ADD COLUMN upload_kind TEXT NOT NULL DEFAULT 'single-put-verified'",
      );
      sql.exec(
        "ALTER TABLE session_state ADD COLUMN snapshot_created_clock INTEGER NOT NULL DEFAULT 0",
      );
      sql.exec("ALTER TABLE session_state ADD COLUMN recent_snapshot_id_1 TEXT");
      sql.exec("ALTER TABLE session_state ADD COLUMN recent_snapshot_id_2 TEXT");
      sql.exec(
        "ALTER TABLE session_state ADD COLUMN snapshot_recent_candidates_json TEXT NOT NULL DEFAULT '[]'",
      );
      sql.exec("ALTER TABLE session_state ADD COLUMN snapshot_recent_scan_before INTEGER");
      sql.exec(
        "ALTER TABLE session_state ADD COLUMN snapshot_recent_scan_done INTEGER NOT NULL DEFAULT 0",
      );
      sql.exec(
        "ALTER TABLE session_state ADD COLUMN snapshot_retention_backlog INTEGER NOT NULL DEFAULT 1",
      );
      sql.exec(`
        CREATE TABLE snapshot_upload (
          snapshot_id TEXT PRIMARY KEY,
          object_key TEXT NOT NULL UNIQUE,
          metadata_json TEXT NOT NULL,
          state TEXT NOT NULL CHECK (
            state IN (
              'preparing', 'uploading', 'completing', 'uncertain',
              'aborted', 'completed', 'retired'
            )
          ),
          upload_id TEXT,
          part_etag TEXT,
          r2_version TEXT,
          etag TEXT,
          created_at INTEGER NOT NULL,
          expires_at INTEGER NOT NULL
        )
      `);
      sql.exec(`
        CREATE INDEX snapshot_upload_state_expiry
        ON snapshot_upload(state, expires_at)
      `);
      state.storage.kv.put("schema-version", 4);
    });
    version = 4;
  }

  if (version < 5) {
    state.storage.transactionSync(() => {
      const hasRelayCapabilities = sql
        .exec("PRAGMA table_info(connection_ticket)")
        .toArray()
        .some((column) => column.name === "relay_capabilities_json");
      if (!hasRelayCapabilities) {
        sql.exec(
          "ALTER TABLE connection_ticket ADD COLUMN relay_capabilities_json TEXT NOT NULL DEFAULT '[]'",
        );
      }
      state.storage.kv.put("schema-version", 5);
    });
    version = 5;
  }

  if (version < 6) {
    state.storage.transactionSync(() => {
      const hasAuthorityDataVersion = sql
        .exec("PRAGMA table_info(session_state)")
        .toArray()
        .some((column) => column.name === "authority_data_version");
      if (!hasAuthorityDataVersion) {
        sql.exec(
          `ALTER TABLE session_state
           ADD COLUMN authority_data_version INTEGER NOT NULL DEFAULT 2
           CHECK (authority_data_version = 2)`,
        );
      }
      const hasRecoveryStrategy = sql
        .exec("PRAGMA table_info(client_delivery)")
        .toArray()
        .some((column) => column.name === "recovery_strategy");
      if (!hasRecoveryStrategy) {
        sql.exec(
          `ALTER TABLE client_delivery
           ADD COLUMN recovery_strategy TEXT NOT NULL DEFAULT 'v2'
           CHECK (recovery_strategy IN ('v2', 'v3'))`,
        );
      }
      state.storage.kv.put("schema-version", 6);
    });
    version = 6;
  }

  if (version < 7) {
    state.storage.transactionSync(() => {
      sql.exec(`
        CREATE TABLE IF NOT EXISTS recovery_attempt (
          recovery_id TEXT PRIMARY KEY CHECK (length(recovery_id) BETWEEN 16 AND 128),
          client_id TEXT NOT NULL CHECK (length(client_id) BETWEEN 16 AND 128),
          connection_id TEXT NOT NULL CHECK (length(connection_id) BETWEEN 16 AND 128),
          host_fence TEXT NOT NULL CHECK (length(host_fence) BETWEEN 1 AND 20),
          stream_id INTEGER NOT NULL CHECK (stream_id BETWEEN 1 AND 4294967295),
          delivery_generation TEXT NOT NULL CHECK (length(delivery_generation) BETWEEN 1 AND 20),
          engine_id TEXT NOT NULL CHECK (length(engine_id) BETWEEN 1 AND 512),
          state TEXT NOT NULL CHECK (
            state IN ('preparing', 'installed', 'assembling', 'complete', 'resetting')
          ),
          prepare_json TEXT NOT NULL CHECK (
            json_valid(prepare_json) AND length(prepare_json) <= 4096
          ),
          start_json TEXT CHECK (
            start_json IS NULL OR (json_valid(start_json) AND length(start_json) <= 8192)
          ),
          base_cursor_json TEXT NOT NULL CHECK (
            json_valid(base_cursor_json) AND length(base_cursor_json) <= 512
          ),
          committed_through_json TEXT CHECK (
            committed_through_json IS NULL OR (
              json_valid(committed_through_json) AND length(committed_through_json) <= 512
            )
          ),
          live_floor_json TEXT CHECK (
            live_floor_json IS NULL OR (
              json_valid(live_floor_json) AND length(live_floor_json) <= 512
            )
          ),
          granted_cumulative_encoded_bytes TEXT NOT NULL DEFAULT '0'
            CHECK (length(granted_cumulative_encoded_bytes) BETWEEN 1 AND 20),
          recovery_done_through_json TEXT CHECK (
            recovery_done_through_json IS NULL OR (
              json_valid(recovery_done_through_json)
              AND length(recovery_done_through_json) <= 512
            )
          ),
          recovery_done_ordinal TEXT CHECK (
            recovery_done_ordinal IS NULL OR length(recovery_done_ordinal) BETWEEN 1 AND 20
          ),
          recovery_done_cumulative_encoded_bytes TEXT CHECK (
            recovery_done_cumulative_encoded_bytes IS NULL OR (
              length(recovery_done_cumulative_encoded_bytes) BETWEEN 1 AND 20
            )
          ),
          replica_applied_json TEXT NOT NULL CHECK (
            json_valid(replica_applied_json) AND length(replica_applied_json) <= 512
          ),
          adopted_json TEXT CHECK (
            adopted_json IS NULL OR (json_valid(adopted_json) AND length(adopted_json) <= 1024)
          ),
          source_closed_json TEXT CHECK (
            source_closed_json IS NULL OR (
              json_valid(source_closed_json) AND length(source_closed_json) <= 1024
            )
          ),
          hard_deadline_at INTEGER NOT NULL,
          no_progress_timeout_ms INTEGER NOT NULL CHECK (no_progress_timeout_ms > 0),
          no_progress_deadline_at INTEGER NOT NULL,
          reset_reason TEXT CHECK (
            reset_reason IS NULL OR reset_reason IN (
              'generation-reset', 'start-send-failed', 'ack-outcome-uncertain',
              'deadline', 'pair-fenced', 'session-disposed'
            )
          ),
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          UNIQUE (client_id, delivery_generation)
        ) STRICT
      `);
      sql.exec(`
        CREATE INDEX IF NOT EXISTS recovery_attempt_deadline
        ON recovery_attempt(state, no_progress_deadline_at, hard_deadline_at)
      `);
      sql.exec(`
        CREATE TABLE IF NOT EXISTS recovery_delivery_lane (
          recovery_id TEXT NOT NULL,
          lane TEXT NOT NULL CHECK (lane IN ('live', 'recovery')),
          sent_delivery_ordinal TEXT NOT NULL CHECK (
            length(sent_delivery_ordinal) BETWEEN 1 AND 20
          ),
          sent_cumulative_encoded_bytes TEXT NOT NULL CHECK (
            length(sent_cumulative_encoded_bytes) BETWEEN 1 AND 20
          ),
          sent_authority_cursor_json TEXT NOT NULL CHECK (
            json_valid(sent_authority_cursor_json)
            AND length(sent_authority_cursor_json) <= 512
          ),
          received_delivery_ordinal TEXT NOT NULL CHECK (
            length(received_delivery_ordinal) BETWEEN 1 AND 20
          ),
          received_cumulative_encoded_bytes TEXT NOT NULL CHECK (
            length(received_cumulative_encoded_bytes) BETWEEN 1 AND 20
          ),
          received_authority_cursor_json TEXT NOT NULL CHECK (
            json_valid(received_authority_cursor_json)
            AND length(received_authority_cursor_json) <= 512
          ),
          updated_at INTEGER NOT NULL,
          PRIMARY KEY (recovery_id, lane),
          FOREIGN KEY (recovery_id) REFERENCES recovery_attempt(recovery_id) ON DELETE CASCADE
        ) STRICT
      `);
      sql.exec(`
        CREATE TABLE IF NOT EXISTS recovery_control_outbox (
          recovery_id TEXT NOT NULL,
          kind TEXT NOT NULL CHECK (
            kind IN (
              'recovery-prepare', 'recovery-start', 'recovery-start-ready',
              'recovery-source-grant', 'recovery-source-received',
              'recovery-source-closed', 'recovery-source-reset'
            )
          ),
          destination TEXT NOT NULL CHECK (destination IN ('host', 'browser')),
          payload_json TEXT NOT NULL CHECK (
            json_valid(payload_json) AND length(payload_json) <= 16384
          ),
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          PRIMARY KEY (recovery_id, kind),
          FOREIGN KEY (recovery_id) REFERENCES recovery_attempt(recovery_id) ON DELETE CASCADE
        ) STRICT
      `);
      state.storage.kv.put("schema-version", 7);
    });
    version = 7;
  }

  if (version < 8) {
    state.storage.transactionSync(() => {
      const hasTicketRecoveryStrategy = sql
        .exec("PRAGMA table_info(connection_ticket)")
        .toArray()
        .some((column) => column.name === "recovery_strategy");
      if (!hasTicketRecoveryStrategy) {
        sql.exec(
          `ALTER TABLE connection_ticket
           ADD COLUMN recovery_strategy TEXT NOT NULL DEFAULT 'v2'
           CHECK (recovery_strategy IN ('v2', 'v3'))`,
        );
      }
      sql.exec(
        `CREATE UNIQUE INDEX IF NOT EXISTS connection_ticket_set_channel
         ON connection_ticket(connection_set_id, channel)`,
      );
      state.storage.kv.put("schema-version", 8);
    });
    version = 8;
  }

  if (version < 9) {
    state.storage.transactionSync(() => {
      sql.exec(`
        CREATE TABLE IF NOT EXISTS recovery_delivery_record (
          recovery_id TEXT NOT NULL,
          lane TEXT NOT NULL CHECK (lane IN ('live', 'recovery')),
          delivery_ordinal TEXT NOT NULL CHECK (
            length(delivery_ordinal) BETWEEN 1 AND 20
            AND delivery_ordinal <> '0'
            AND delivery_ordinal NOT GLOB '*[^0-9]*'
            AND (length(delivery_ordinal) = 1 OR substr(delivery_ordinal, 1, 1) <> '0')
            AND (
              length(delivery_ordinal) < 20
              OR delivery_ordinal <= '18446744073709551615'
            )
          ),
          cumulative_encoded_bytes TEXT NOT NULL CHECK (
            length(cumulative_encoded_bytes) BETWEEN 1 AND 20
            AND cumulative_encoded_bytes <> '0'
            AND cumulative_encoded_bytes NOT GLOB '*[^0-9]*'
            AND (
              length(cumulative_encoded_bytes) = 1
              OR substr(cumulative_encoded_bytes, 1, 1) <> '0'
            )
            AND (
              length(cumulative_encoded_bytes) < 20
              OR cumulative_encoded_bytes <= '18446744073709551615'
            )
          ),
          encoded_bytes INTEGER NOT NULL CHECK (
            encoded_bytes BETWEEN 1 AND 9007199254740991
          ),
          authority_cursor_after_json TEXT NOT NULL CHECK (
            json_valid(authority_cursor_after_json)
            AND length(authority_cursor_after_json) <= 512
          ),
          state TEXT NOT NULL CHECK (state IN ('queued', 'sending', 'sent')),
          PRIMARY KEY (recovery_id, lane, delivery_ordinal),
          UNIQUE (recovery_id, lane, cumulative_encoded_bytes),
          FOREIGN KEY (recovery_id, lane)
            REFERENCES recovery_delivery_lane(recovery_id, lane) ON DELETE CASCADE
        ) STRICT
      `);
      sql.exec(`
        CREATE INDEX IF NOT EXISTS recovery_delivery_record_state
        ON recovery_delivery_record(state, recovery_id, lane)
      `);
      sql.exec(`
        CREATE UNIQUE INDEX IF NOT EXISTS recovery_delivery_record_one_sending
        ON recovery_delivery_record(recovery_id) WHERE state = 'sending'
      `);

      const legacyLanes = sql
        .exec(
          `SELECT recovery_id, lane, sent_delivery_ordinal,
                  sent_cumulative_encoded_bytes, sent_authority_cursor_json,
                  received_delivery_ordinal, received_cumulative_encoded_bytes,
                  received_authority_cursor_json
           FROM recovery_delivery_lane
           ORDER BY recovery_id, lane`,
        )
        .toArray() as unknown as {
        lane: "live" | "recovery";
        received_cumulative_encoded_bytes: string;
        received_delivery_ordinal: string;
        received_authority_cursor_json: string;
        recovery_id: string;
        sent_authority_cursor_json: string;
        sent_cumulative_encoded_bytes: string;
        sent_delivery_ordinal: string;
      }[];
      const invalidRecoveryIds = new Set<string>();
      const maxU64 = (1n << 64n) - 1n;
      const parseCanonicalU64 = (value: string): bigint => {
        if (!/^(0|[1-9][0-9]{0,19})$/.test(value)) throw new Error("non-canonical u64");
        const parsed = BigInt(value);
        if (parsed > maxU64) throw new Error("u64 overflow");
        return parsed;
      };

      for (const lane of legacyLanes) {
        try {
          const sentOrdinal = parseCanonicalU64(lane.sent_delivery_ordinal);
          const receivedOrdinal = parseCanonicalU64(lane.received_delivery_ordinal);
          const sentBytes = parseCanonicalU64(lane.sent_cumulative_encoded_bytes);
          const receivedBytes = parseCanonicalU64(lane.received_cumulative_encoded_bytes);
          if (
            sentOrdinal === receivedOrdinal &&
            sentBytes === receivedBytes &&
            lane.sent_authority_cursor_json === lane.received_authority_cursor_json
          ) {
            continue;
          }
          // V8 wrote the sent scalar before the external WebSocket send. Any
          // outstanding or mixed row is therefore outcome-uncertain across an
          // upgrade/eviction and must fence the whole delivery owner.
          invalidRecoveryIds.add(lane.recovery_id);
        } catch {
          invalidRecoveryIds.add(lane.recovery_id);
        }
      }

      for (const recoveryId of invalidRecoveryIds) {
        const attempt = sql
          .exec(
            `SELECT recovery_id, connection_id, stream_id, delivery_generation, state, updated_at
             FROM recovery_attempt WHERE recovery_id = ?`,
            recoveryId,
          )
          .toArray()[0] as
          | {
              connection_id: string;
              delivery_generation: string;
              recovery_id: string;
              state: "assembling" | "complete" | "installed" | "preparing" | "resetting";
              stream_id: number;
              updated_at: number;
            }
          | undefined;
        sql.exec("DELETE FROM recovery_delivery_lane WHERE recovery_id = ?", recoveryId);
        if (attempt === undefined) continue;
        if (attempt.state === "resetting") continue;
        sql.exec("DELETE FROM recovery_control_outbox WHERE recovery_id = ?", recoveryId);
        sql.exec(
          `UPDATE recovery_attempt
           SET state = 'resetting', reset_reason = 'ack-outcome-uncertain'
           WHERE recovery_id = ?`,
          recoveryId,
        );
        // A complete attempt has already closed its Host source. Its terminal
        // delivery owner is fenced locally and retained as a tombstone; sending
        // a new source reset to Host would be an invalid post-closure command.
        if (attempt.state === "complete") continue;
        const resetPayload = JSON.stringify({
          type: "recovery-source-reset",
          recoveryId: attempt.recovery_id,
          connectionId: attempt.connection_id,
          streamId: attempt.stream_id,
          deliveryGeneration: attempt.delivery_generation,
          reason: "ack-outcome-uncertain",
        });
        sql.exec(
          `INSERT INTO recovery_control_outbox
            (recovery_id, kind, destination, payload_json, created_at, updated_at)
           VALUES (?, 'recovery-source-reset', 'host', ?, ?, ?)`,
          recoveryId,
          resetPayload,
          attempt.updated_at,
          attempt.updated_at,
        );
      }

      state.storage.kv.put("schema-version", 9);
    });
  }
}
