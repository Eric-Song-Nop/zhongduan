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

    sql.exec("ALTER TABLE connection_ticket RENAME TO connection_ticket_v8_fixture");
    sql.exec(`
      CREATE TABLE connection_ticket (
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
        expires_at INTEGER NOT NULL,
        relay_capabilities_json TEXT NOT NULL DEFAULT '[]'
      )
    `);
    sql.exec(`
      INSERT INTO connection_ticket
        (ticket_digest, connection_set_id, connection_id, peer, channel, client_id,
         subject, role, stream_id, delivery_generation, host_fence, expires_at,
         relay_capabilities_json)
      SELECT ticket_digest, connection_set_id, connection_id, peer, channel, client_id,
             subject, role, stream_id, delivery_generation, host_fence, expires_at,
             relay_capabilities_json
      FROM connection_ticket_v8_fixture
    `);
    sql.exec("DROP TABLE connection_ticket_v8_fixture");
    sql.exec("CREATE INDEX connection_ticket_expiry ON connection_ticket(expires_at)");
    durable.storage.kv.put("schema-version", 5);
  });
}

function installV8DeliveryFixture(durable: DurableObjectState): {
  activeRecoveryId: string;
  cleanRecoveryId: string;
  completeRecoveryId: string;
  v2ClientId: string;
} {
  const activeRecoveryId = "recovery_migration_active_0001";
  const completeRecoveryId = "recovery_migration_complete_01";
  const cleanRecoveryId = "recovery_migration_clean_00001";
  const v2ClientId = "client_migration_v2_000000001";
  const baseline = JSON.stringify({ sessionEpoch: "7", eventSeq: "5", nextPtyOffset: "12" });
  const sent = JSON.stringify({ sessionEpoch: "7", eventSeq: "6", nextPtyOffset: "14" });
  durable.storage.transactionSync(() => {
    const sql = durable.storage.sql;
    sql.exec(
      `UPDATE session_state
       SET host_fence = '3', head_event_seq = '6', next_pty_offset = '14', updated_at = 1
       WHERE singleton = 1`,
    );
    const fixtures = [
      {
        clientId: "client_migration_active_000001",
        connectionId: "connection_migration_active_001",
        recoveryId: activeRecoveryId,
        state: "assembling",
        streamId: 1,
      },
      {
        clientId: "client_migration_complete_0001",
        connectionId: "connection_migration_complete_1",
        recoveryId: completeRecoveryId,
        state: "complete",
        streamId: 2,
      },
      {
        clientId: "client_migration_clean_0000001",
        connectionId: "connection_migration_clean_00001",
        recoveryId: cleanRecoveryId,
        state: "assembling",
        streamId: 3,
      },
    ] as const;
    for (const fixture of fixtures) {
      sql.exec(
        `INSERT INTO client_delivery
          (client_id, principal_id_hash, role, stream_id, delivery_generation,
           updated_at, registered_at, reservation_expires_at, recovery_strategy)
         VALUES (?, ?, 'observer', ?, '1', 1, 1, NULL, 'v3')`,
        fixture.clientId,
        `principal_${fixture.clientId}`,
        fixture.streamId,
      );
      sql.exec(
        `INSERT INTO recovery_attempt
          (recovery_id, client_id, connection_id, host_fence, stream_id,
           delivery_generation, engine_id, state, prepare_json, start_json,
           base_cursor_json, committed_through_json, live_floor_json,
           granted_cumulative_encoded_bytes, replica_applied_json,
           hard_deadline_at, no_progress_timeout_ms, no_progress_deadline_at,
           created_at, updated_at)
         VALUES (?, ?, ?, '3', ?, '1', ?, ?, '{}', '{}', ?, ?, '{}', '1000', ?,
                 10000, 1000, 1100, 1, 1)`,
        fixture.recoveryId,
        fixture.clientId,
        fixture.connectionId,
        fixture.streamId,
        "ghostty:test+snapshot-v1+wterm:test",
        fixture.state,
        baseline,
        baseline,
        baseline,
      );
      for (const lane of ["live", "recovery"] as const) {
        const isOutstanding = lane === "live" && fixture.recoveryId !== cleanRecoveryId;
        sql.exec(
          `INSERT INTO recovery_delivery_lane
            (recovery_id, lane, sent_delivery_ordinal, sent_cumulative_encoded_bytes,
             sent_authority_cursor_json, received_delivery_ordinal,
             received_cumulative_encoded_bytes, received_authority_cursor_json, updated_at)
           VALUES (?, ?, ?, ?, ?, '0', '0', ?, 1)`,
          fixture.recoveryId,
          lane,
          isOutstanding ? "1" : "0",
          isOutstanding ? "90" : "0",
          isOutstanding ? sent : baseline,
          baseline,
        );
      }
      if (fixture.recoveryId !== cleanRecoveryId) {
        sql.exec(
          `INSERT INTO recovery_control_outbox
            (recovery_id, kind, destination, payload_json, created_at, updated_at)
           VALUES (?, 'recovery-start', 'browser', '{}', 1, 1)`,
          fixture.recoveryId,
        );
      }
    }
    sql.exec(
      `INSERT INTO client_delivery
        (client_id, principal_id_hash, role, stream_id, delivery_generation,
         updated_at, registered_at, reservation_expires_at, recovery_strategy)
       VALUES (?, 'principal_migration_v2', 'observer', 4, '1', 1, 1, NULL, 'v2')`,
      v2ClientId,
    );
    sql.exec("DROP TABLE recovery_delivery_record");
    durable.storage.kv.put("schema-version", 8);
  });
  return { activeRecoveryId, cleanRecoveryId, completeRecoveryId, v2ClientId };
}

describe("relay capability migration", () => {
  it("migrates v5 session, client, and ticket rows to fixed v2 defaults", async () => {
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
        ticketColumns: durable.storage.sql
          .exec("PRAGMA table_info(connection_ticket)")
          .toArray()
          .map((column) => column.name),
        version: durable.storage.kv.get<number>("schema-version"),
      };
    });
    expect(fixture.version).toBe(5);
    expect(fixture.clientColumns).not.toContain("recovery_strategy");
    expect(fixture.sessionColumns).not.toContain("authority_data_version");
    expect(fixture.ticketColumns).not.toContain("recovery_strategy");
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
        ticket: durable.storage.sql
          .exec("PRAGMA table_info(connection_ticket)")
          .toArray()
          .map((column) => column.name),
      },
      indexes: durable.storage.sql
        .exec(
          `SELECT name FROM sqlite_master
           WHERE type = 'index' AND tbl_name = 'connection_ticket' ORDER BY name`,
        )
        .toArray(),
      session: durable.storage.sql
        .exec("SELECT authority_data_version FROM session_state WHERE singleton = 1")
        .one(),
      tickets: durable.storage.sql
        .exec(
          `SELECT channel, recovery_strategy FROM connection_ticket
           WHERE connection_set_id = (
             SELECT connection_set_id FROM connection_ticket LIMIT 1
           ) ORDER BY channel`,
        )
        .toArray(),
      version: durable.storage.kv.get<number>("schema-version"),
    }));
    expect(migrated).toMatchObject({
      client: { recovery_strategy: "v2" },
      session: { authority_data_version: 2 },
      tickets: [
        { channel: "control", recovery_strategy: "v2" },
        { channel: "data", recovery_strategy: "v2" },
      ],
      version: 9,
    });
    expect(migrated.columns.client).toContain("recovery_strategy");
    expect(migrated.columns.session).toContain("authority_data_version");
    expect(migrated.columns.ticket).toContain("recovery_strategy");
    expect(migrated.indexes).toContainEqual({ name: "connection_ticket_set_channel" });

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

    const fixedTicketStrategy = await runInDurableObject(stub, (_instance, durable) => {
      let rejected = false;
      try {
        durable.storage.sql.exec(
          "UPDATE connection_ticket SET recovery_strategy = 'future' WHERE channel = 'data'",
        );
      } catch {
        rejected = true;
      }
      return rejected;
    });
    expect(fixedTicketStrategy).toBe(true);
  });

  it("defaults old-bundle ticket inserts to v2 and preserves them across cold restart", async () => {
    const session = await createSession();
    const stub = sessionStub(session.sessionId);
    const connectionSetId = "connection_set_old_bundle_0001";
    const insertOldTicket = (
      durable: DurableObjectState,
      channel: "control" | "data",
      ticketDigest: string,
    ): void => {
      durable.storage.sql.exec(
        `INSERT INTO connection_ticket
          (ticket_digest, connection_set_id, connection_id, peer, channel, client_id,
           subject, role, stream_id, delivery_generation, host_fence, expires_at,
           relay_capabilities_json)
         VALUES (?, ?, 'connection_old_bundle_000001', 'browser', ?,
                 'client_old_bundle_000000001', 'subject_old_bundle_00000001',
                 'observer', 1, '1', NULL, ?, '[]')`,
        ticketDigest,
        connectionSetId,
        channel,
        Date.now() + 60_000,
      );
    };

    const warm = await runInDurableObject(stub, (_instance, durable) => {
      insertOldTicket(durable, "control", "ticket_old_bundle_control_0001");
      insertOldTicket(durable, "data", "ticket_old_bundle_data_000001");
      let duplicateRejected = false;
      try {
        insertOldTicket(durable, "data", "ticket_old_bundle_duplicate_01");
      } catch {
        duplicateRejected = true;
      }
      return {
        duplicateRejected,
        rows: durable.storage.sql
          .exec(
            `SELECT channel, recovery_strategy FROM connection_ticket
             WHERE connection_set_id = ? ORDER BY channel`,
            connectionSetId,
          )
          .toArray(),
      };
    });
    expect(warm).toEqual({
      duplicateRejected: true,
      rows: [
        { channel: "control", recovery_strategy: "v2" },
        { channel: "data", recovery_strategy: "v2" },
      ],
    });

    await evictDurableObject(stub);
    const cold = await runInDurableObject(stub, (_instance, durable) =>
      durable.storage.sql
        .exec(
          `SELECT channel, recovery_strategy FROM connection_ticket
           WHERE connection_set_id = ? ORDER BY channel`,
          connectionSetId,
        )
        .toArray(),
    );
    expect(cold).toEqual(warm.rows);
  });

  it("fails closed v8 outcome-uncertain delivery while preserving clean and v2 rows", async () => {
    const session = await createSession();
    const stub = sessionStub(session.sessionId);
    const fixture = await runInDurableObject(stub, (_instance, durable) =>
      installV8DeliveryFixture(durable),
    );
    await evictDurableObject(stub);

    const migrated = await runInDurableObject(stub, (_instance, durable) => ({
      attempts: durable.storage.sql
        .exec(
          `SELECT recovery_id, state, reset_reason FROM recovery_attempt
           WHERE recovery_id IN (?, ?, ?) ORDER BY recovery_id`,
          fixture.activeRecoveryId,
          fixture.completeRecoveryId,
          fixture.cleanRecoveryId,
        )
        .toArray(),
      lanes: durable.storage.sql
        .exec(
          `SELECT recovery_id, lane FROM recovery_delivery_lane
           WHERE recovery_id IN (?, ?, ?) ORDER BY recovery_id, lane`,
          fixture.activeRecoveryId,
          fixture.completeRecoveryId,
          fixture.cleanRecoveryId,
        )
        .toArray(),
      outbox: durable.storage.sql
        .exec(
          `SELECT recovery_id, kind, destination, payload_json
           FROM recovery_control_outbox ORDER BY recovery_id, kind`,
        )
        .toArray(),
      records: durable.storage.sql.exec("SELECT * FROM recovery_delivery_record").toArray(),
      strict: durable.storage.sql
        .exec("SELECT strict FROM pragma_table_list WHERE name = 'recovery_delivery_record'")
        .one().strict,
      v2: durable.storage.sql
        .exec(
          "SELECT recovery_strategy FROM client_delivery WHERE client_id = ?",
          fixture.v2ClientId,
        )
        .one(),
      version: durable.storage.kv.get<number>("schema-version"),
    }));
    expect(migrated).toMatchObject({
      attempts: [
        {
          recovery_id: fixture.activeRecoveryId,
          reset_reason: "ack-outcome-uncertain",
          state: "resetting",
        },
        {
          recovery_id: fixture.cleanRecoveryId,
          reset_reason: "generation-reset",
          state: "resetting",
        },
        {
          recovery_id: fixture.completeRecoveryId,
          reset_reason: "ack-outcome-uncertain",
          state: "resetting",
        },
      ],
      lanes: [],
      records: [],
      strict: 1,
      v2: { recovery_strategy: "v2" },
      version: 9,
    });
    expect(migrated.outbox).toHaveLength(2);
    expect(migrated.outbox[0]).toMatchObject({
      destination: "host",
      kind: "recovery-source-reset",
      recovery_id: fixture.activeRecoveryId,
    });
    const resetPayload = migrated.outbox[0]?.payload_json;
    if (typeof resetPayload !== "string") throw new Error("migration reset payload missing");
    expect(JSON.parse(resetPayload)).toMatchObject({
      reason: "ack-outcome-uncertain",
      recoveryId: fixture.activeRecoveryId,
      type: "recovery-source-reset",
    });
    expect(migrated.outbox[1]).toMatchObject({
      destination: "host",
      kind: "recovery-source-reset",
      recovery_id: fixture.cleanRecoveryId,
    });

    await evictDurableObject(stub);
    const cold = await runInDurableObject(stub, (_instance, durable) => ({
      outbox: durable.storage.sql.exec("SELECT recovery_id FROM recovery_control_outbox").toArray(),
      records: durable.storage.sql.exec("SELECT * FROM recovery_delivery_record").toArray(),
      version: durable.storage.kv.get<number>("schema-version"),
    }));
    expect(cold).toEqual({
      outbox: [{ recovery_id: fixture.activeRecoveryId }, { recovery_id: fixture.cleanRecoveryId }],
      records: [],
      version: 9,
    });
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
