import { env, exports as workerExports } from "cloudflare:workers";
import { evictDurableObject, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { SocketAttachmentSchema } from "../src/worker/relay-socket";

const origin = "https://terminal.example.test";
let sessionCounter = 0;

async function createSession(): Promise<{
  observerCapability: string;
  sessionId: string;
}> {
  const sessionId = `session_relay_migration_${(++sessionCounter).toString().padStart(16, "0")}`;
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions`, {
      method: "POST",
      headers: {
        authorization: "Bearer test-bootstrap-token-with-at-least-32-bytes",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        sessionId,
        engineId: "ghostty:test+snapshot-v1+wterm:test",
        sessionEpoch: "7",
      }),
    }),
  );
  expect(response.status).toBe(201);
  return response.json();
}

function sessionStub(sessionId: string) {
  return env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${sessionId}`));
}

function installV5Fixture(durable: DurableObjectState): void {
  durable.storage.transactionSync(() => {
    const sql = durable.storage.sql;
    sql.exec("ALTER TABLE session_state RENAME TO session_state_v6_fixture");
    sql.exec(`
      CREATE TABLE session_state (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        session_id TEXT NOT NULL UNIQUE,
        session_epoch TEXT NOT NULL,
        engine_id TEXT NOT NULL,
        host_fence TEXT NOT NULL DEFAULT '0',
        host_agent_epoch TEXT,
        latest_snapshot_id TEXT,
        next_stream_id INTEGER NOT NULL DEFAULT 1,
        terminated_at INTEGER,
        updated_at INTEGER NOT NULL,
        head_event_seq TEXT NOT NULL DEFAULT '0',
        next_pty_offset TEXT NOT NULL DEFAULT '0',
        snapshot_created_clock INTEGER NOT NULL DEFAULT 0,
        recent_snapshot_id_1 TEXT,
        recent_snapshot_id_2 TEXT,
        snapshot_recent_candidates_json TEXT NOT NULL DEFAULT '[]',
        snapshot_recent_scan_before INTEGER,
        snapshot_recent_scan_done INTEGER NOT NULL DEFAULT 0,
        snapshot_retention_backlog INTEGER NOT NULL DEFAULT 1
      )
    `);
    sql.exec(`
      INSERT INTO session_state
        (singleton, session_id, session_epoch, engine_id, host_fence, host_agent_epoch,
         latest_snapshot_id, next_stream_id, terminated_at, updated_at, head_event_seq,
         next_pty_offset, snapshot_created_clock, recent_snapshot_id_1,
         recent_snapshot_id_2, snapshot_recent_candidates_json,
         snapshot_recent_scan_before, snapshot_recent_scan_done, snapshot_retention_backlog)
      SELECT singleton, session_id, session_epoch, engine_id, host_fence, host_agent_epoch,
             latest_snapshot_id, next_stream_id, terminated_at, updated_at, head_event_seq,
             next_pty_offset, snapshot_created_clock, recent_snapshot_id_1,
             recent_snapshot_id_2, snapshot_recent_candidates_json,
             snapshot_recent_scan_before, snapshot_recent_scan_done, snapshot_retention_backlog
      FROM session_state_v6_fixture
    `);
    sql.exec("DROP TABLE session_state_v6_fixture");

    sql.exec("ALTER TABLE client_delivery RENAME TO client_delivery_v6_fixture");
    sql.exec(`
      CREATE TABLE client_delivery (
        client_id TEXT PRIMARY KEY,
        principal_id_hash TEXT NOT NULL,
        role TEXT NOT NULL CHECK (role IN ('writer', 'observer')),
        stream_id INTEGER NOT NULL UNIQUE,
        delivery_generation TEXT NOT NULL,
        updated_at INTEGER NOT NULL,
        registered_at INTEGER,
        reservation_expires_at INTEGER
      )
    `);
    sql.exec(`
      INSERT INTO client_delivery
        (client_id, principal_id_hash, role, stream_id, delivery_generation,
         updated_at, registered_at, reservation_expires_at)
      SELECT client_id, principal_id_hash, role, stream_id, delivery_generation,
             updated_at, registered_at, reservation_expires_at
      FROM client_delivery_v6_fixture
    `);
    sql.exec("DROP TABLE client_delivery_v6_fixture");
    durable.storage.kv.put("schema-version", 5);
  });
}

describe("relay capability migration", () => {
  it("migrates v5 session and client rows to fixed v2 defaults", async () => {
    const session = await createSession();
    const connectionResponse = await workerExports.default.fetch(
      new Request(`${origin}/api/v1/sessions/${session.sessionId}/connection-sets`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${session.observerCapability}`,
          "content-type": "application/json",
        },
        body: "{}",
      }),
    );
    expect(connectionResponse.status).toBe(200);
    const connection = await connectionResponse.json<{ clientId: string }>();

    const stub = sessionStub(session.sessionId);
    const fixture = await runInDurableObject(stub, (_instance, durable) => {
      installV5Fixture(durable);
      return {
        clientColumns: durable.storage.sql
          .exec("PRAGMA table_info(client_delivery)")
          .toArray()
          .map((column) => column.name),
        sessionColumns: durable.storage.sql
          .exec("PRAGMA table_info(session_state)")
          .toArray()
          .map((column) => column.name),
        version: durable.storage.kv.get<number>("schema-version"),
      };
    });
    expect(fixture.version).toBe(5);
    expect(fixture.clientColumns).not.toContain("recovery_strategy");
    expect(fixture.sessionColumns).not.toContain("authority_data_version");
    await evictDurableObject(stub);

    const migrated = await runInDurableObject(stub, (_instance, durable) => ({
      client: durable.storage.sql
        .exec(
          "SELECT recovery_strategy FROM client_delivery WHERE client_id = ?",
          connection.clientId,
        )
        .one(),
      columns: {
        client: durable.storage.sql
          .exec("PRAGMA table_info(client_delivery)")
          .toArray()
          .map((column) => column.name),
        session: durable.storage.sql
          .exec("PRAGMA table_info(session_state)")
          .toArray()
          .map((column) => column.name),
      },
      session: durable.storage.sql
        .exec("SELECT authority_data_version FROM session_state WHERE singleton = 1")
        .one(),
      version: durable.storage.kv.get<number>("schema-version"),
    }));
    expect(migrated).toMatchObject({
      client: { recovery_strategy: "v2" },
      session: { authority_data_version: 2 },
      version: 7,
    });
    expect(migrated.columns.client).toContain("recovery_strategy");
    expect(migrated.columns.session).toContain("authority_data_version");

    const fixedAuthorityVersion = await runInDurableObject(stub, (_instance, durable) => {
      let rejected = false;
      try {
        durable.storage.sql.exec(
          "UPDATE session_state SET authority_data_version = 3 WHERE singleton = 1",
        );
      } catch {
        rejected = true;
      }
      return {
        rejected,
        value: durable.storage.sql
          .exec("SELECT authority_data_version FROM session_state WHERE singleton = 1")
          .one().authority_data_version,
      };
    });
    expect(fixedAuthorityVersion).toEqual({ rejected: true, value: 2 });
  });

  it("decodes a hibernated v2 attachment created before capability persistence", () => {
    const legacyAttachment = {
      version: 2,
      peer: "browser",
      channel: "data",
      connectionSetId: "connection_set_0000000001",
      connectionId: "connection_id_00000000001",
      subject: "subject_legacy_00000000001",
      clientId: "client_legacy_000000000001",
      role: "observer",
      streamId: 1,
      deliveryGeneration: "1",
      hostFence: null,
      leaseFence: null,
      controlState: null,
      dataState: "awaiting-attach",
      firstEventSeq: null,
      ackedEventSeq: null,
      sentEventSeq: null,
      firstPtyOffset: null,
      ackedPtyOffset: null,
      sentPtyOffset: null,
      replayMode: null,
      snapshotId: null,
      replayCommitEventSeq: null,
      replayCommitPtyOffset: null,
    };

    expect(SocketAttachmentSchema.parse(legacyAttachment)).toMatchObject({
      ...legacyAttachment,
      relayCapabilities: [],
    });
  });
});
