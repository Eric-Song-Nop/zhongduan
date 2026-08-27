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
  peer: "host" | "browser";
  role: CapabilityRole;
  stream_id: number;
  subject: string;
  ticket_digest: string;
}

export interface SessionRow {
  engine_id: string;
  head_event_seq: string;
  host_fence: string;
  latest_snapshot_id: string | null;
  next_pty_offset: string;
  next_stream_id: number;
  session_epoch: string;
  session_id: string;
}

export interface ClientRow {
  client_id: string;
  delivery_generation: string;
  principal_id_hash: string;
  registered_at: number | null;
  reservation_expires_at: number | null;
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
  }
}
