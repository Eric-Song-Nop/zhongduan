import { env } from "cloudflare:workers";
import { evictDurableObject, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { RelayStore } from "../src/worker/relay-store";
import { SnapshotStore } from "../src/worker/snapshot-store";
import { SnapshotUploadCoordinator } from "../src/worker/snapshot-upload-coordinator";
import {
  bucketWithOverrides,
  createSession,
  engineId,
  getSnapshot,
  sessionStub,
  uploadSnapshot,
  within,
} from "./snapshot-test-helpers";

const LEGACY_SNAPSHOT_ROWS = 1_001;

async function settleCoordinatorMaintenance(instance: object): Promise<void> {
  const coordinator = Reflect.get(instance, "snapshotUploads") as object;
  const tasks = Reflect.get(coordinator, "snapshotMaintenanceTasks") as Map<string, Promise<void>>;
  const maintenance = Reflect.get(coordinator, "snapshotMaintenance") as Promise<void> | undefined;
  await Promise.allSettled([
    ...tasks.values(),
    ...(maintenance === undefined ? [] : [maintenance]),
  ]);
}

async function settleInstanceMaintenance(sessionId: string): Promise<void>;
async function settleInstanceMaintenance<T>(
  sessionId: string,
  inspect: (durable: DurableObjectState) => T | Promise<T>,
): Promise<T>;
async function settleInstanceMaintenance<T>(
  sessionId: string,
  inspect?: (durable: DurableObjectState) => T | Promise<T>,
): Promise<T | void> {
  return runInDurableObject(sessionStub(sessionId), async (instance, durable) => {
    await settleCoordinatorMaintenance(instance);
    return inspect?.(durable);
  });
}

async function installV3Fixture(sessionId: string): Promise<void> {
  await runInDurableObject(sessionStub(sessionId), async (_instance, durable) => {
    const sql = durable.storage.sql;
    const session = sql
      .exec(
        `SELECT session_id, session_epoch, engine_id, host_fence, host_agent_epoch,
                next_stream_id, terminated_at, updated_at, head_event_seq, next_pty_offset
         FROM session_state WHERE singleton = 1`,
      )
      .one();
    await durable.storage.deleteAlarm();
    sql.exec("DROP TABLE snapshot_upload");
    sql.exec("DROP TABLE snapshot");
    sql.exec("DROP TABLE session_state");
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
        next_pty_offset TEXT NOT NULL DEFAULT '0'
      )
    `);
    sql.exec(
      `INSERT INTO session_state
        (singleton, session_id, session_epoch, engine_id, host_fence, host_agent_epoch,
         latest_snapshot_id, next_stream_id, terminated_at, updated_at,
         head_event_seq, next_pty_offset)
       VALUES (1, ?, ?, ?, ?, ?, 'snapshot_legacy_000848', ?, ?, ?, ?, ?)`,
      session.session_id,
      session.session_epoch,
      session.engine_id,
      session.host_fence,
      session.host_agent_epoch,
      session.next_stream_id,
      session.terminated_at,
      session.updated_at,
      session.head_event_seq,
      session.next_pty_offset,
    );
    sql.exec(`
      CREATE TABLE snapshot (
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
    sql.exec(
      `WITH RECURSIVE seq(value) AS (
         SELECT 1
         UNION ALL
         SELECT value + 1 FROM seq WHERE value < ?
       )
       INSERT INTO snapshot
        (snapshot_id, session_epoch, cut_event_seq, next_pty_offset, engine_id,
         object_key, r2_version, etag, sha256, compressed_length,
         uncompressed_length, compression, state, created_at)
       SELECT printf('snapshot_legacy_%06d', value), ?, '0', '0', ?,
              printf('v1/sessions/%s/snapshots/snapshot_legacy_%06d.bin', ?, value),
              printf('version-%06d', value), printf('etag-%06d', value),
              lower(hex(zeroblob(32))), 1, '1', 'none',
              CASE WHEN value BETWEEN 848 AND 850 THEN 'servable' ELSE 'retired' END,
              value
       FROM seq`,
      LEGACY_SNAPSHOT_ROWS,
      session.session_epoch,
      session.engine_id,
      session.session_id,
    );
    durable.storage.kv.put("schema-version", 3);
  });
}

describe("relay store migration", () => {
  it("preserves the writer fence but expires a pre-E2 lease without a connection owner", async () => {
    const session = await createSession();
    await runInDurableObject(sessionStub(session.sessionId), async (_instance, durable) => {
      const sql = durable.storage.sql;
      sql.exec("DROP TABLE writer_lease");
      sql.exec(`
        CREATE TABLE writer_lease (
          singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
          client_id TEXT NOT NULL,
          lease_digest TEXT NOT NULL,
          fence TEXT NOT NULL,
          expires_at INTEGER NOT NULL
        )
      `);
      sql.exec(
        `INSERT INTO writer_lease
          (singleton, client_id, lease_digest, fence, expires_at)
         VALUES (1, 'legacy-client', 'legacy-digest', '41', ?)`,
        Date.now() + 60_000,
      );
      durable.storage.kv.put("schema-version", 5);
    });
    await evictDurableObject(sessionStub(session.sessionId));

    const coldGet = await getSnapshot(session, "snapshot_e2_migration_probe");
    expect(coldGet.status).toBe(404);
    await coldGet.body?.cancel();

    const migrated = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) => ({
        columns: durable.storage.sql
          .exec("PRAGMA table_info(writer_lease)")
          .toArray()
          .map((column) => column.name),
        lease: durable.storage.sql
          .exec(
            `SELECT client_id, connection_id, fence, expires_at
             FROM writer_lease WHERE singleton = 1`,
          )
          .one(),
        version: durable.storage.kv.get<number>("schema-version"),
      }),
    );
    expect(migrated.columns).toContain("connection_id");
    expect(migrated.lease).toEqual({
      client_id: "legacy-client",
      connection_id: "",
      fence: "41",
      expires_at: 0,
    });
    expect(migrated.version).toBe(6);
  });
});

describe("snapshot retention migration", () => {
  // Preserve the 1,001-row stress fixture while other workerd files share the test scheduler.
  it("migrates v3 in O(1) and drains a large snapshot ledger in fixed alarm batches", async () => {
    const session = await createSession();
    await installV3Fixture(session.sessionId);
    await evictDurableObject(sessionStub(session.sessionId));

    const coldGet = await getSnapshot(session, "snapshot_cold_get_missing");
    expect(coldGet.status).toBe(404);
    await coldGet.body?.cancel();
    const migrated = await runInDurableObject(
      sessionStub(session.sessionId),
      async (_instance, durable) => ({
        alarm: await durable.storage.getAlarm(),
        backlog: durable.storage.sql
          .exec("SELECT snapshot_retention_backlog AS value FROM session_state")
          .one().value,
        currentUploads: durable.storage.sql
          .exec("SELECT COUNT(*) AS value FROM snapshot_upload")
          .one().value,
        indexes: durable.storage.sql
          .exec(
            `SELECT name, tbl_name FROM sqlite_master
             WHERE type = 'index' AND name LIKE 'snapshot_upload%'
             ORDER BY name`,
          )
          .toArray(),
        snapshots: durable.storage.sql.exec("SELECT COUNT(*) AS value FROM snapshot").one().value,
        version: durable.storage.kv.get<number>("schema-version"),
      }),
    );
    expect(migrated).toMatchObject({
      backlog: 1,
      currentUploads: 0,
      snapshots: LEGACY_SNAPSHOT_ROWS,
      version: 6,
    });
    expect(migrated.alarm).not.toBeNull();
    expect(migrated.indexes).toEqual([
      { name: "snapshot_upload_state_expiry", tbl_name: "snapshot_upload" },
    ]);

    const touchedKeys = new Set<string>();
    const migrationPins = new Set<string>();
    const legacyFixturePrefix = `v1/sessions/${session.sessionId}/snapshots/snapshot_legacy_`;
    let markMaintenanceBlocked: (() => void) | undefined;
    let releaseMaintenance: (() => void) | undefined;
    let maintenanceGate: Promise<void> | undefined;
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const coordinator = Reflect.get(instance, "snapshotUploads") as object;
      const base = Reflect.get(coordinator, "snapshotBucket") as R2Bucket;
      Reflect.set(
        coordinator,
        "snapshotBucket",
        bucketWithOverrides(base, {
          async delete(key: string | string[]): Promise<void> {
            touchedKeys.add(String(key));
            markMaintenanceBlocked?.();
            if (maintenanceGate !== undefined) await maintenanceGate;
            await base.delete(key);
          },
          async head(key: string): Promise<R2Object | null> {
            touchedKeys.add(key);
            markMaintenanceBlocked?.();
            if (maintenanceGate !== undefined) await maintenanceGate;
            if (key.startsWith(legacyFixturePrefix)) return null;
            return base.head(key);
          },
        }),
      );
      Reflect.set(coordinator, "pinnedSnapshotIds", () => migrationPins);
      Reflect.set(coordinator, "scheduleMaintenance", () => undefined);
    });
    const blocked = await uploadSnapshot(session, "snapshot_blocked_during_migration");
    expect(blocked.status).toBe(503);
    await blocked.body?.cancel();
    expect(touchedKeys.size).toBe(0);
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      Reflect.deleteProperty(
        Reflect.get(instance, "snapshotUploads") as object,
        "scheduleMaintenance",
      );
    });

    const scanCursors: number[] = [];
    let recentScanDone = false;
    for (let alarm = 0; alarm < 40; alarm += 1) {
      migrationPins.clear();
      migrationPins.add(`snapshot_legacy_00000${alarm % 2 === 0 ? "2" : "3"}`);
      touchedKeys.clear();
      expect(await runDurableObjectAlarm(sessionStub(session.sessionId))).toBe(true);
      const scan = await settleInstanceMaintenance(session.sessionId, (durable) =>
        durable.storage.sql
          .exec(
            `SELECT snapshot_recent_scan_before AS cursor,
                      snapshot_recent_scan_done AS done
               FROM session_state`,
          )
          .one(),
      );
      expect(touchedKeys.size).toBeLessThanOrEqual(32);
      scanCursors.push(Number(scan.cursor));
      recentScanDone = Number(scan.done) !== 0;
      if (recentScanDone) break;
    }
    expect(recentScanDone).toBe(true);
    expect(scanCursors.length).toBeGreaterThanOrEqual(5);
    expect(
      scanCursors.every((cursor, index) => index === 0 || cursor < scanCursors[index - 1]!),
    ).toBe(true);
    migrationPins.clear();

    const maintenanceBlocked = new Promise<void>((resolve) => {
      markMaintenanceBlocked = resolve;
    });
    maintenanceGate = new Promise<void>((resolve) => {
      releaseMaintenance = resolve;
    });
    touchedKeys.clear();
    const alarmDispatch = runDurableObjectAlarm(sessionStub(session.sessionId));
    await within(maintenanceBlocked, "bounded maintenance batch did not reach R2");
    const blockedDuringAlarm = await Promise.all(
      Array.from({ length: 24 }, (_, index) =>
        uploadSnapshot(session, `snapshot_blocked_alarm_${index.toString().padStart(4, "0")}`),
      ),
    );
    expect(blockedDuringAlarm.every((response) => response.status === 503)).toBe(true);
    for (const response of blockedDuringAlarm) await response.body?.cancel();
    releaseMaintenance?.();
    await alarmDispatch;
    await settleInstanceMaintenance(session.sessionId);
    expect(touchedKeys.size).toBeLessThanOrEqual(32);
    maintenanceGate = undefined;
    markMaintenanceBlocked = undefined;

    let finalState:
      | {
          backlog: number;
          currentUploads: number;
          latest: string | null;
          recent1: string | null;
          recent2: string | null;
          snapshots: number;
        }
      | undefined;
    finalState = await runInDurableObject(
      sessionStub(session.sessionId),
      async (instance, durable) => {
        let previousBacklog = 1;
        let previousSnapshotCount = LEGACY_SNAPSHOT_ROWS;
        let state: typeof finalState;
        for (let alarm = 0; alarm < 100; alarm += 1) {
          touchedKeys.clear();
          await durable.storage.deleteAlarm();
          await instance.alarm();
          await settleCoordinatorMaintenance(instance);
          const sql = durable.storage.sql;
          const row = sql
            .exec(
              `SELECT latest_snapshot_id, recent_snapshot_id_1, recent_snapshot_id_2,
                      snapshot_retention_backlog
               FROM session_state`,
            )
            .one();
          state = {
            backlog: Number(row.snapshot_retention_backlog),
            currentUploads: Number(
              sql.exec("SELECT COUNT(*) AS value FROM snapshot_upload").one().value,
            ),
            latest: row.latest_snapshot_id as string | null,
            recent1: row.recent_snapshot_id_1 as string | null,
            recent2: row.recent_snapshot_id_2 as string | null,
            snapshots: Number(sql.exec("SELECT COUNT(*) AS value FROM snapshot").one().value),
          };
          expect(await durable.storage.getAlarm()).not.toBeNull();
          expect(touchedKeys.size).toBeLessThanOrEqual(32);
          expect(state.backlog).toBeLessThanOrEqual(previousBacklog);
          expect(state.snapshots < previousSnapshotCount || state.backlog < previousBacklog).toBe(
            true,
          );
          previousBacklog = state.backlog;
          previousSnapshotCount = state.snapshots;
          if (state.backlog === 0) return state;
        }
        return state;
      },
    );
    expect(finalState).toMatchObject({
      backlog: 0,
      currentUploads: 0,
      latest: "snapshot_legacy_000848",
      snapshots: 3,
    });
    expect(new Set([finalState?.recent1, finalState?.recent2])).toEqual(
      new Set(["snapshot_legacy_000849", "snapshot_legacy_000850"]),
    );

    const queryPlans = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) => {
        const sql = durable.storage.sql;
        return [
          ...sql
            .exec(
              `EXPLAIN QUERY PLAN
               SELECT rowid, snapshot_id, state FROM snapshot
               ORDER BY rowid DESC LIMIT 32`,
            )
            .toArray(),
          ...sql
            .exec(
              `EXPLAIN QUERY PLAN
               SELECT * FROM snapshot
               WHERE snapshot_id NOT IN (?, ?, ?)
               ORDER BY rowid ASC LIMIT 32`,
              "snapshot_legacy_000848",
              "snapshot_legacy_000849",
              "snapshot_legacy_000850",
            )
            .toArray(),
        ].map((row) => (row as { detail: string }).detail);
      },
    );
    expect(queryPlans.some((detail) => detail.includes("USE TEMP B-TREE"))).toBe(false);

    const admitted = await uploadSnapshot(session, "snapshot_after_legacy_drain");
    expect(admitted.status).toBe(201);
    await admitted.body?.cancel();
  }, 15_000);

  it("carries released pins into grace without exceeding the protected-row bound", async () => {
    const session = await createSession();
    const state = await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      const sql = durable.storage.sql;
      sql.exec(
        `WITH RECURSIVE seq(value) AS (
             SELECT 1
             UNION ALL
             SELECT value + 1 FROM seq WHERE value < 44
           )
           INSERT INTO snapshot
            (snapshot_id, session_epoch, cut_event_seq, next_pty_offset, engine_id,
             object_key, r2_version, etag, sha256, compressed_length,
             uncompressed_length, compression, state, created_at, upload_kind)
           SELECT printf('snapshot_pool_%06d', value), ?, '0', '0', ?,
                  printf('v1/sessions/%s/snapshots/snapshot_pool_%06d.bin', ?, value),
                  printf('version-%06d', value), printf('etag-%06d', value),
                  lower(hex(zeroblob(32))), 1, '1', 'none', 'servable', value,
                  'single-put-verified'
           FROM seq`,
        "7",
        engineId,
        session.sessionId,
      );
      sql.exec(
        `UPDATE session_state
           SET latest_snapshot_id = 'snapshot_pool_000044',
               snapshot_retention_backlog = 1, snapshot_recent_scan_done = 0
           WHERE singleton = 1`,
      );
      const browserPins = new Set(
        Array.from(
          { length: 16 },
          (_, index) => `snapshot_pool_${(index + 1).toString().padStart(6, "0")}`,
        ),
      );
      const publishedOwners = new Set(
        Array.from(
          { length: 4 },
          (_, index) => `snapshot_pool_${(index + 17).toString().padStart(6, "0")}`,
        ),
      );
      const initialProtected = new Set([...browserPins, ...publishedOwners]);
      const snapshots = new SnapshotStore(durable, sql, new RelayStore(sql), 16);
      const retire = (pins: ReadonlySet<string>) => {
        const retired = snapshots.reconcile(pins).retired;
        for (const snapshot of retired) snapshots.deleteRetired(snapshot);
      };
      retire(initialProtected);
      const firstPass = new RelayStore(sql).session();
      const firstCandidates = JSON.parse(
        firstPass?.snapshot_recent_candidates_json ?? "[]",
      ) as string[];
      const firstRows = Number(sql.exec("SELECT COUNT(*) AS value FROM snapshot").one().value);
      const replacementPins = new Set([
        firstPass?.recent_snapshot_id_1 ?? "",
        firstPass?.recent_snapshot_id_2 ?? "",
      ]);
      retire(replacementPins);
      retire(replacementPins);
      const sessionRow = new RelayStore(sql).session();
      return {
        backlog: sessionRow?.snapshot_retention_backlog,
        firstBacklog: firstPass?.snapshot_retention_backlog,
        firstCandidates: firstCandidates.length,
        firstRecent1: firstPass?.recent_snapshot_id_1,
        firstRecent2: firstPass?.recent_snapshot_id_2,
        firstRows,
        recent1: sessionRow?.recent_snapshot_id_1,
        recent2: sessionRow?.recent_snapshot_id_2,
        rows: Number(sql.exec("SELECT COUNT(*) AS value FROM snapshot").one().value),
      };
    });

    expect(state).toEqual({
      backlog: 0,
      firstBacklog: 1,
      firstCandidates: 23,
      firstRecent1: "snapshot_pool_000043",
      firstRecent2: "snapshot_pool_000042",
      firstRows: 23,
      recent1: "snapshot_pool_000001",
      recent2: "snapshot_pool_000002",
      rows: 5,
    });
  });

  it("finishes an exhausted migration scan with fewer than two grace rows", async () => {
    const session = await createSession();
    const response = await uploadSnapshot(session, "snapshot_migration_single_01");
    expect(response.status).toBe(201);
    await response.body?.cancel();

    const state = await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      const sql = durable.storage.sql;
      sql.exec(
        `UPDATE session_state
           SET snapshot_retention_backlog = 1, snapshot_recent_scan_done = 0,
               snapshot_recent_scan_before = NULL, snapshot_recent_candidates_json = '[]',
               recent_snapshot_id_1 = NULL, recent_snapshot_id_2 = NULL
           WHERE singleton = 1`,
      );
      const snapshots = new SnapshotStore(durable, sql, new RelayStore(sql), 16);
      const retention = snapshots.reconcile(new Set());
      const current = new RelayStore(sql).session();
      return {
        backlog: current?.snapshot_retention_backlog,
        retired: retention.retired,
        rows: Number(sql.exec("SELECT COUNT(*) AS value FROM snapshot").one().value),
      };
    });
    expect(state).toEqual({ backlog: 0, retired: [], rows: 1 });
  });

  it("re-arms cold maintenance after an initial alarm persistence failure", async () => {
    const session = await createSession();
    await runInDurableObject(sessionStub(session.sessionId), async (_instance, durable) => {
      const sql = durable.storage.sql;
      sql.exec(
        `UPDATE session_state
         SET snapshot_retention_backlog = 1, snapshot_recent_scan_done = 0
         WHERE singleton = 1`,
      );
      await durable.storage.deleteAlarm();
      const snapshots = new SnapshotStore(durable, sql, new RelayStore(sql), 16);
      const failingState = {
        storage: {
          async getAlarm(): Promise<number | null> {
            throw new Error("injected cold alarm failure");
          },
        },
        waitUntil(): void {},
      } as unknown as DurableObjectState;
      const first = new SnapshotUploadCoordinator(
        failingState,
        env.SNAPSHOTS,
        snapshots,
        () => new Set(),
      );
      await expect(first.initialize()).rejects.toThrow("injected cold alarm failure");
      expect(await durable.storage.getAlarm()).toBeNull();

      const rebuilt = new SnapshotUploadCoordinator(
        durable,
        env.SNAPSHOTS,
        snapshots,
        () => new Set(),
      );
      await rebuilt.initialize();
      expect(await durable.storage.getAlarm()).not.toBeNull();
    });
  });
});
