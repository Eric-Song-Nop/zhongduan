import { SNAPSHOT_MEDIA_TYPE, type SnapshotMetadata } from "@zhongduan/protocol";
import { env } from "cloudflare:workers";
import { evictDurableObject, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { DURABLE_ALARM_FAILURE_RETRY_MS } from "../src/worker/durable-alarm-mux";
import {
  hexToBytes,
  snapshotAttemptObjectKey,
  snapshotCustomMetadata,
  type FinalizedSnapshot,
} from "../src/worker/snapshot-contract";
import { RelayStore } from "../src/worker/relay-store";
import { SnapshotStore } from "../src/worker/snapshot-store";
import {
  bucketWithOverrides,
  createSession,
  encoder,
  engineId,
  FORCED_SESSION_ALARM_NOW,
  forceNextSessionAlarmDue,
  forceSessionAlarmDue,
  getSnapshot,
  matchesSnapshotKey,
  metadataFor,
  overrideSnapshotBucket,
  sessionStub,
  sha256Hex,
  snapshotObjectKeys,
  storedSnapshotKey,
  uploadSnapshot,
  within,
  type UploadOverrides,
} from "./snapshot-test-helpers";

async function settleSnapshotMaintenance(sessionId: string): Promise<void> {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const remaining = await runInDurableObject(sessionStub(sessionId), async (instance) => {
      await Promise.resolve();
      const coordinator = Reflect.get(instance, "snapshotUploads") as object;
      const alarmMux = Reflect.get(instance, "alarmMux") as object;
      const tasks = Reflect.get(coordinator, "snapshotMaintenanceTasks") as Map<
        string,
        Promise<void>
      >;
      const maintenance = Reflect.get(coordinator, "snapshotMaintenance") as
        | Promise<void>
        | undefined;
      const alarmUpdate = Reflect.get(alarmMux, "reconcileRun") as Promise<void> | undefined;
      await Promise.allSettled([
        ...tasks.values(),
        ...(maintenance === undefined ? [] : [maintenance]),
        ...(alarmUpdate === undefined ? [] : [alarmUpdate]),
      ]);
      return (
        tasks.size +
        (Reflect.get(coordinator, "snapshotMaintenance") === undefined ? 0 : 1) +
        (Reflect.get(alarmMux, "reconcileRun") === undefined ? 0 : 1)
      );
    });
    if (remaining === 0) return;
  }
  throw new Error("snapshot maintenance did not settle");
}

describe("snapshot multipart retention", () => {
  it("keeps sixteen pins, latest, and two recent marker-grace snapshots servable", async () => {
    const session = await createSession();
    const state = await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      const sql = durable.storage.sql;
      const snapshots = new SnapshotStore(durable, sql, new RelayStore(sql), 16);
      const pins = new Set<string>();
      let reservationTime = 1_000;
      const finalize = (snapshotId: string) => {
        const metadata: SnapshotMetadata = {
          sessionId: session.sessionId,
          snapshotId,
          engineId,
          sessionEpoch: "7",
          cutEventSeq: "0",
          nextPtyOffset: "0",
          compression: "none",
          compressedLength: "1",
          uncompressedLength: "1",
          sha256: "a".repeat(64),
        };
        const reserved = snapshots.beginUpload(metadata, reservationTime);
        reservationTime += 1;
        if (!reserved.ok) throw new Error(`reservation failed: ${reserved.reason}`);
        if (reserved.state !== "started") throw new Error("upload unexpectedly published");
        const snapshot: FinalizedSnapshot = {
          ...metadata,
          objectKey: reserved.upload.object_key,
          r2Version: `version-${snapshotId}`,
          etag: `etag-${snapshotId}`,
        };
        const uploadId = `upload-${snapshotId}`;
        expect(snapshots.recordMultipartUpload(snapshotId, snapshot.objectKey, uploadId)).toBe(
          true,
        );
        expect(
          snapshots.recordMultipartCompleting(
            snapshotId,
            snapshot.objectKey,
            uploadId,
            `part-${snapshotId}`,
          ),
        ).toBe(true);
        expect(
          snapshots.recordMultipartCompleted(
            snapshotId,
            snapshot.objectKey,
            uploadId,
            snapshot.r2Version,
            snapshot.etag,
          ),
        ).toBeDefined();
        const finalized = snapshots.finalize(snapshot, pins);
        if (!finalized.ok) throw new Error(`finalize failed: ${finalized.reason}`);
        return finalized;
      };

      const pinnedIds = Array.from(
        { length: 16 },
        (_, index) => `snapshot_pin_${index.toString().padStart(6, "0")}`,
      );
      for (const snapshotId of pinnedIds) {
        finalize(snapshotId);
        pins.add(snapshotId);
      }
      const recentIds = Array.from(
        { length: 4 },
        (_, index) => `snapshot_recent_${index.toString().padStart(4, "0")}`,
      );
      let lastFinalized;
      for (const snapshotId of recentIds) lastFinalized = finalize(snapshotId);
      if (lastFinalized === undefined) throw new Error("latest snapshot missing");
      const refreshedPinned = snapshots.refreshPublishedExact(
        snapshots.published(pinnedIds[0]!)!,
        pins,
      );
      const overlapRetention = snapshots.reconcile(pins);
      const retry = snapshots.finalize(lastFinalized.snapshot, pins);

      return {
        latest: sql.exec("SELECT latest_snapshot_id AS value FROM session_state").one().value,
        overlapRetention,
        pinnedStates: pinnedIds.map(
          (snapshotId) =>
            sql.exec("SELECT state FROM snapshot WHERE snapshot_id = ?", snapshotId).one().state,
        ),
        recentStates: recentIds.map(
          (snapshotId) =>
            sql.exec("SELECT state FROM snapshot WHERE snapshot_id = ?", snapshotId).one().state,
        ),
        retiredByLastFinalize: lastFinalized.retired.map((snapshot) => snapshot.snapshotId),
        retry,
        recentSlots: sql
          .exec(
            "SELECT recent_snapshot_id_1, recent_snapshot_id_2 FROM session_state WHERE singleton = 1",
          )
          .one(),
        refreshedPinned,
        rowLimit: snapshots.rowLimit,
        counts: sql
          .exec("SELECT state, COUNT(*) AS value FROM snapshot GROUP BY state ORDER BY state")
          .toArray(),
      };
    });

    expect(state.latest).toBe("snapshot_recent_0003");
    expect(state.refreshedPinned).toMatchObject({ snapshotId: "snapshot_pin_000000" });
    expect(state.recentSlots).toEqual({
      recent_snapshot_id_1: "snapshot_recent_0002",
      recent_snapshot_id_2: "snapshot_recent_0001",
    });
    expect(state.overlapRetention.retired).toEqual([
      expect.objectContaining({ snapshotId: "snapshot_recent_0000" }),
    ]);
    expect(state.pinnedStates).toEqual(Array.from({ length: 16 }, () => "servable"));
    expect(state.recentStates).toEqual(["retired", "servable", "servable", "servable"]);
    expect(state.retiredByLastFinalize).toEqual(["snapshot_recent_0000"]);
    expect(state.retry).toMatchObject({ ok: true, created: false, retired: [] });
    expect(state.rowLimit).toBe(32);
    expect(state.counts).toEqual([
      { state: "retired", value: 1 },
      { state: "servable", value: 19 },
    ]);
  });

  it("carries released steady-state pins into empty grace slots", async () => {
    const session = await createSession();
    const state = await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      const sql = durable.storage.sql;
      const snapshotIds = Array.from(
        { length: 17 },
        (_, index) => `snapshot_dynamic_pin_${index.toString().padStart(4, "0")}`,
      );
      for (const [index, snapshotId] of snapshotIds.entries()) {
        sql.exec(
          `INSERT INTO snapshot
            (snapshot_id, session_epoch, cut_event_seq, next_pty_offset, engine_id,
             object_key, r2_version, etag, sha256, compressed_length,
             uncompressed_length, compression, state, created_at)
           VALUES (?, '7', '0', '0', ?, ?, ?, ?, ?, 1, '1', 'none',
                   'servable', ?)`,
          snapshotId,
          engineId,
          snapshotAttemptObjectKey(session.sessionId, snapshotId, "retention_seed_0001"),
          `version-${index}`,
          `etag-${index}`,
          "a".repeat(64),
          index + 1,
        );
      }
      const ordinaryPins = snapshotIds.slice(0, 14);
      const ownerIds = snapshotIds.slice(14, 16);
      const latest = snapshotIds[16]!;
      sql.exec(
        `UPDATE session_state
         SET latest_snapshot_id = ?, recent_snapshot_id_1 = ?, recent_snapshot_id_2 = ?,
             snapshot_recent_candidates_json = ?, snapshot_recent_scan_done = 1,
             snapshot_retention_backlog = 0
         WHERE singleton = 1`,
        latest,
        ownerIds[0]!,
        ownerIds[1]!,
        JSON.stringify([...ordinaryPins, latest, ...ownerIds]),
      );
      const snapshots = new SnapshotStore(durable, sql, new RelayStore(sql), 16);
      const first = snapshots.reconcile(new Set([...ordinaryPins, ...ownerIds]));
      const firstSlots = new RelayStore(sql).session();
      const retainedPins = new Set([...ordinaryPins.slice(2), ...ownerIds]);
      const second = snapshots.reconcile(retainedPins);
      const secondSlots = new RelayStore(sql).session();
      const third = snapshots.reconcile(new Set(ordinaryPins.slice(2)));
      const thirdSlots = new RelayStore(sql).session();
      return {
        first,
        firstSlots: [firstSlots?.recent_snapshot_id_1, firstSlots?.recent_snapshot_id_2],
        second,
        secondSlots: [secondSlots?.recent_snapshot_id_1, secondSlots?.recent_snapshot_id_2],
        third,
        thirdSlots: [thirdSlots?.recent_snapshot_id_1, thirdSlots?.recent_snapshot_id_2],
      };
    });

    expect(state.first.retired).toEqual([]);
    expect(state.firstSlots).toEqual([null, null]);
    expect(state.second.retired).toEqual([]);
    expect(state.secondSlots).toEqual(["snapshot_dynamic_pin_0000", "snapshot_dynamic_pin_0001"]);
    expect(state.third.retired.map((snapshot) => snapshot.snapshotId)).toEqual([
      "snapshot_dynamic_pin_0014",
      "snapshot_dynamic_pin_0015",
    ]);
    expect(state.thirdSlots).toEqual(state.secondSlots);
  });

  it("resamples browser pins after the maintenance alarm await", async () => {
    const session = await createSession();
    const snapshotIds = Array.from({ length: 4 }, (_, index) => `snapshot_pin_race_000${index}`);
    const metadata = await Promise.all(
      snapshotIds.map((snapshotId) => metadataFor(session, snapshotId)),
    );
    await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      for (const [index, snapshot] of metadata.entries()) {
        durable.storage.sql.exec(
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
          snapshotAttemptObjectKey(snapshot.sessionId, snapshot.snapshotId, "retention_seed_0002"),
          `version-${index}`,
          `etag-${index}`,
          snapshot.sha256,
          Number(snapshot.compressedLength),
          snapshot.uncompressedLength,
          snapshot.compression,
          index + 1,
        );
      }
      durable.storage.sql.exec(
        `UPDATE session_state
         SET latest_snapshot_id = ?, snapshot_recent_scan_done = 0
         WHERE singleton = 1`,
        snapshotIds.at(-1)!,
      );
    });

    const pins = new Set<string>();
    let markAlarmStarted: (() => void) | undefined;
    let releaseAlarm: (() => void) | undefined;
    const alarmStarted = new Promise<void>((resolve) => {
      markAlarmStarted = resolve;
    });
    const alarmGate = new Promise<void>((resolve) => {
      releaseAlarm = resolve;
    });
    const maintenance = runInDurableObject(sessionStub(session.sessionId), async (instance) => {
      const coordinator = Reflect.get(instance, "snapshotUploads") as object;
      const scheduleAlarm = Reflect.get(coordinator, "scheduleAlarm").bind(coordinator) as (
        timestamp: number,
      ) => Promise<void>;
      Reflect.set(coordinator, "pinnedSnapshotIds", () => pins);
      Reflect.set(coordinator, "scheduleAlarm", async (timestamp: number) => {
        markAlarmStarted?.();
        await alarmGate;
        await scheduleAlarm(timestamp);
      });
      await forceSessionAlarmDue(instance);
      await instance.alarm();
    });
    await within(alarmStarted, "maintenance alarm persistence did not start");
    pins.add(snapshotIds[0]!);
    releaseAlarm?.();
    await within(maintenance, "maintenance did not resume after alarm persistence");

    const states = await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) =>
      durable.storage.sql
        .exec("SELECT snapshot_id, state FROM snapshot ORDER BY snapshot_id")
        .toArray(),
    );
    expect(states).toEqual(
      snapshotIds.map((snapshotId) => ({ snapshot_id: snapshotId, state: "servable" })),
    );
  });

  it("bounds unfinished upload reservations before any fifth R2 write", async () => {
    const session = await createSession();
    let createCalls = 0;
    let headCalls = 0;
    await overrideSnapshotBucket(session, (base) =>
      bucketWithOverrides(base, {
        async createMultipartUpload(
          key: string,
          options?: R2MultipartOptions,
        ): Promise<R2MultipartUpload> {
          createCalls += 1;
          await base.createMultipartUpload(key, options);
          throw new Error("injected create response loss");
        },
        head(key: string): Promise<R2Object | null> {
          headCalls += 1;
          return base.head(key);
        },
      }),
    );
    const reservedIds = Array.from(
      { length: 4 },
      (_, index) => `snapshot_reserved_${index.toString().padStart(5, "0")}`,
    );
    for (const snapshotId of reservedIds) {
      const failedUpload = await uploadSnapshot(session, snapshotId);
      expect(failedUpload.status).toBe(503);
      expect(await failedUpload.clone().json()).toEqual({ error: "snapshot-upload-failed" });
      await failedUpload.body?.cancel();
    }
    expect({ createCalls, headCalls }).toEqual({ createCalls: 4, headCalls: 4 });
    const rejectedId = "snapshot_reserved_rejected";
    const rejected = await uploadSnapshot(session, rejectedId);
    expect(rejected.status).toBe(503);
    expect(await rejected.json()).toEqual({ error: "snapshot-reservation-failed" });
    expect(await snapshotObjectKeys(session.sessionId, rejectedId)).toEqual([]);
    expect({ createCalls, headCalls }).toEqual({ createCalls: 4, headCalls: 4 });
    const uploads = await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) =>
      durable.storage.sql
        .exec("SELECT snapshot_id, state FROM snapshot_upload ORDER BY snapshot_id")
        .toArray(),
    );
    expect(uploads).toEqual(
      reservedIds.map((snapshotId) => ({ snapshot_id: snapshotId, state: "preparing" })),
    );
  });

  it("keeps a committed multipart object tracked across termination and enforces four slots", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_complete_throw_01";
    const body = encoder.encode("snapshot-state");
    const metadata: SnapshotMetadata = {
      sessionId: session.sessionId,
      snapshotId,
      engineId,
      sessionEpoch: "7",
      cutEventSeq: "0",
      nextPtyOffset: "0",
      compression: "none",
      compressedLength: body.byteLength.toString(),
      uncompressedLength: body.byteLength.toString(),
      sha256: await sha256Hex(body),
    };
    const objectKey = await runInDurableObject(
      sessionStub(session.sessionId),
      async (_instance, durable) => {
        await durable.storage.setAlarm(Date.now() + 60_000);
        const sql = durable.storage.sql;
        const snapshots = new SnapshotStore(durable, sql, new RelayStore(sql), 16);
        const reserved = snapshots.beginUpload(metadata, Date.now());
        expect(reserved).toMatchObject({ ok: true, state: "started" });
        if (!reserved.ok || reserved.state !== "started") throw new Error("reservation failed");
        return reserved.upload.object_key;
      },
    );
    const multipart = await env.SNAPSHOTS.createMultipartUpload(objectKey, {
      httpMetadata: {
        contentType: SNAPSHOT_MEDIA_TYPE,
        cacheControl: "private, no-store",
      },
      customMetadata: snapshotCustomMetadata(metadata),
    });
    await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      const snapshots = new SnapshotStore(
        durable,
        durable.storage.sql,
        new RelayStore(durable.storage.sql),
        16,
      );
      expect(snapshots.recordMultipartUpload(snapshotId, objectKey, multipart.uploadId)).toBe(true);
    });
    const part = await multipart.uploadPart(1, body);
    await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      const snapshots = new SnapshotStore(
        durable,
        durable.storage.sql,
        new RelayStore(durable.storage.sql),
        16,
      );
      expect(
        snapshots.recordMultipartCompleting(snapshotId, objectKey, multipart.uploadId, part.etag),
      ).toBe(true);
    });
    await expect(
      (async () => {
        await multipart.complete([part]);
        throw new Error("simulated request termination after complete");
      })(),
    ).rejects.toThrow("simulated request termination");
    expect(await env.SNAPSHOTS.head(objectKey)).not.toBeNull();

    const otherIds = [
      "snapshot_uncertain_b_01",
      "snapshot_uncertain_c_01",
      "snapshot_uncertain_d_01",
    ];
    const bounded = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) => {
        const sql = durable.storage.sql;
        const snapshots = new SnapshotStore(durable, sql, new RelayStore(sql), 16);
        const reserve = (id: string) =>
          snapshots.beginUpload(
            {
              sessionId: session.sessionId,
              snapshotId: id,
              engineId,
              sessionEpoch: "7",
              cutEventSeq: "0",
              nextPtyOffset: "0",
              compression: "none",
              compressedLength: "1",
              uncompressedLength: "1",
              sha256: "a".repeat(64),
            },
            Date.now(),
          );
        for (const id of otherIds) {
          const reserved = reserve(id);
          expect(reserved).toMatchObject({ ok: true, state: "started" });
        }
        return {
          fifth: reserve("snapshot_uncertain_e_01"),
          rows: sql.exec("SELECT snapshot_id, state FROM snapshot_upload").toArray(),
        };
      },
    );
    expect(bounded.fifth).toEqual({ ok: false, reason: "retention-backlog" });
    expect(bounded.rows).toHaveLength(4);

    await evictDurableObject(sessionStub(session.sessionId));
    await overrideSnapshotBucket(session, (base) =>
      bucketWithOverrides(base, {
        resumeMultipartUpload(key: string, uploadId: string): R2MultipartUpload {
          const resumed = base.resumeMultipartUpload(key, uploadId);
          if (uploadId !== multipart.uploadId) return resumed;
          return {
            key: resumed.key,
            uploadId: resumed.uploadId,
            uploadPart: resumed.uploadPart.bind(resumed),
            complete: resumed.complete.bind(resumed),
            async abort(): Promise<void> {
              throw Object.assign(new Error("NoSuchUpload (10024)"), { code: 10024 });
            },
          };
        },
      }),
    );
    await runInDurableObject(sessionStub(session.sessionId), async (instance) => {
      await forceSessionAlarmDue(instance);
      await instance.alarm();
    });
    await settleSnapshotMaintenance(session.sessionId);
    const recoveredLedger = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) =>
        durable.storage.sql
          .exec("SELECT snapshot_id, state FROM snapshot_upload ORDER BY snapshot_id")
          .toArray(),
    );
    expect(recoveredLedger).toEqual([
      { snapshot_id: snapshotId, state: "completed" },
      ...otherIds.map((id) => ({ snapshot_id: id, state: "preparing" })),
    ]);
    expect(await env.SNAPSHOTS.head(objectKey)).not.toBeNull();

    await evictDurableObject(sessionStub(session.sessionId));
    await runInDurableObject(sessionStub(session.sessionId), async (instance, durable) => {
      durable.storage.sql.exec(
        "UPDATE snapshot_upload SET expires_at = 0 WHERE snapshot_id = ?",
        snapshotId,
      );
      await forceSessionAlarmDue(instance);
      await instance.alarm();
    });
    await settleSnapshotMaintenance(session.sessionId);
    const finalLedger = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) =>
        durable.storage.sql
          .exec("SELECT snapshot_id, state FROM snapshot_upload ORDER BY snapshot_id")
          .toArray(),
    );
    expect(finalLedger).toEqual(otherIds.map((id) => ({ snapshot_id: id, state: "preparing" })));
    expect(await env.SNAPSHOTS.head(objectKey)).toBeNull();
  });

  it("serializes maintenance per id without blocking a different snapshot upload", async () => {
    const session = await createSession();
    const recoveringId = "snapshot_gc_owner_0001";
    const metadata = await metadataFor(session, recoveringId);
    const recoveringKey = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) => {
        const snapshots = new SnapshotStore(
          durable,
          durable.storage.sql,
          new RelayStore(durable.storage.sql),
          16,
        );
        const reserved = snapshots.beginUpload(metadata, Date.now());
        expect(reserved).toMatchObject({ ok: true, state: "started" });
        if (!reserved.ok || reserved.state !== "started") throw new Error("reservation failed");
        return reserved.upload.object_key;
      },
    );
    const multipart = await env.SNAPSHOTS.createMultipartUpload(recoveringKey, {
      httpMetadata: { contentType: SNAPSHOT_MEDIA_TYPE, cacheControl: "private, no-store" },
      customMetadata: snapshotCustomMetadata(metadata),
    });
    await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      const snapshots = new SnapshotStore(
        durable,
        durable.storage.sql,
        new RelayStore(durable.storage.sql),
        16,
      );
      expect(snapshots.recordMultipartUpload(recoveringId, recoveringKey, multipart.uploadId)).toBe(
        true,
      );
      return durable.storage.setAlarm(Date.now() + 60_000);
    });

    let markAbortStarted: (() => void) | undefined;
    let releaseAbort: (() => void) | undefined;
    let abortCalls = 0;
    const abortStarted = new Promise<void>((resolve) => {
      markAbortStarted = resolve;
    });
    const abortGate = new Promise<void>((resolve) => {
      releaseAbort = resolve;
    });
    await overrideSnapshotBucket(session, (base) =>
      bucketWithOverrides(base, {
        resumeMultipartUpload(key: string, uploadId: string): R2MultipartUpload {
          const resumed = base.resumeMultipartUpload(key, uploadId);
          if (uploadId !== multipart.uploadId) return resumed;
          return {
            key: resumed.key,
            uploadId: resumed.uploadId,
            uploadPart: resumed.uploadPart.bind(resumed),
            complete: resumed.complete.bind(resumed),
            async abort(): Promise<void> {
              abortCalls += 1;
              markAbortStarted?.();
              await abortGate;
              await resumed.abort();
            },
          };
        },
      }),
    );

    await forceNextSessionAlarmDue(session.sessionId);
    const maintenance = runDurableObjectAlarm(sessionStub(session.sessionId));
    await abortStarted;
    const sameId = await uploadSnapshot(session, recoveringId);
    expect(sameId.status).toBe(503);
    expect(await sameId.json()).toEqual({ error: "snapshot-upload-in-progress" });
    expect(abortCalls).toBe(1);

    const otherId = "snapshot_during_gc_0001";
    const other = await uploadSnapshot(session, otherId);
    expect(other.status).toBe(201);
    expect(await other.json()).toMatchObject({ snapshot: { snapshotId: otherId } });
    releaseAbort?.();
    expect(await maintenance).toBe(true);

    const state = await runInDurableObject(
      sessionStub(session.sessionId),
      async (_instance, durable) => ({
        recovered: durable.storage.sql
          .exec("SELECT state FROM snapshot_upload WHERE snapshot_id = ?", recoveringId)
          .toArray()[0],
        published: durable.storage.sql
          .exec("SELECT state FROM snapshot WHERE snapshot_id = ?", otherId)
          .one().state,
        alarm: await durable.storage.getAlarm(),
      }),
    );
    expect(state).toMatchObject({ recovered: undefined, published: "servable" });
    expect(state.alarm).not.toBeNull();
  });

  it.each(["head", "delete"] as const)(
    "continues different-id GC while one maintenance %s never settles",
    async (mode) => {
      const session = await createSession();
      const blockedId = `snapshot_gc_blocked_${mode}`;
      const deletedId = `snapshot_gc_deleted_${mode}`;
      let blockedKey = snapshotAttemptObjectKey(
        session.sessionId,
        blockedId,
        "retention_block_0001",
      );
      const deletedKey = snapshotAttemptObjectKey(
        session.sessionId,
        deletedId,
        "retention_delete_0001",
      );
      const blockedMetadata = await metadataFor(session, blockedId);
      const deletedMetadata = await metadataFor(session, deletedId);
      const blockedObject =
        mode === "delete"
          ? await env.SNAPSHOTS.put(blockedKey, encoder.encode("blocked-retired"))
          : undefined;
      const deletedObject = await env.SNAPSHOTS.put(
        deletedKey,
        encoder.encode("independent-retired"),
      );
      if (deletedObject === null || blockedObject === null) {
        throw new Error("failed to seed retired snapshot objects");
      }
      await runInDurableObject(sessionStub(session.sessionId), async (_instance, durable) => {
        const sql = durable.storage.sql;
        const insertRetired = (
          metadata: SnapshotMetadata,
          object: Pick<R2Object, "etag" | "version">,
        ) => {
          sql.exec(
            `INSERT INTO snapshot
              (snapshot_id, session_epoch, cut_event_seq, next_pty_offset, engine_id,
               object_key, r2_version, etag, sha256, compressed_length,
               uncompressed_length, compression, state, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'retired', ?)`,
            metadata.snapshotId,
            metadata.sessionEpoch,
            metadata.cutEventSeq,
            metadata.nextPtyOffset,
            metadata.engineId,
            metadata.snapshotId === blockedId ? blockedKey : deletedKey,
            object.version,
            object.etag,
            metadata.sha256,
            Number(metadata.compressedLength),
            metadata.uncompressedLength,
            metadata.compression,
            Date.now(),
          );
        };
        insertRetired(deletedMetadata, deletedObject);
        if (blockedObject === undefined) {
          const snapshots = new SnapshotStore(durable, sql, new RelayStore(sql), 16);
          const reserved = snapshots.beginUpload(blockedMetadata, 0);
          expect(reserved).toMatchObject({ ok: true, state: "started" });
          if (!reserved.ok || reserved.state !== "started") {
            throw new Error("reservation failed");
          }
          blockedKey = reserved.upload.object_key;
        } else {
          insertRetired(blockedMetadata, blockedObject);
        }
        await durable.storage.setAlarm(Date.now() + 60_000);
      });

      let markBlocked: (() => void) | undefined;
      let releaseBlocked: (() => void) | undefined;
      let markDeleted: (() => void) | undefined;
      const blocked = new Promise<void>((resolve) => {
        markBlocked = resolve;
      });
      const blockedGate = new Promise<void>((resolve) => {
        releaseBlocked = resolve;
      });
      const independentlyDeleted = new Promise<void>((resolve) => {
        markDeleted = resolve;
      });
      await overrideSnapshotBucket(session, (base) =>
        bucketWithOverrides(base, {
          async head(key: string): Promise<R2Object | null> {
            if (mode === "head" && key === blockedKey) {
              markBlocked?.();
              await blockedGate;
            }
            return base.head(key);
          },
          async delete(key: string | string[]): Promise<void> {
            const objectKey = String(key);
            if (mode === "delete" && objectKey === blockedKey) {
              markBlocked?.();
              await blockedGate;
            }
            await base.delete(key);
            if (objectKey === deletedKey) markDeleted?.();
          },
        }),
      );

      await forceNextSessionAlarmDue(session.sessionId);
      const alarm = runDurableObjectAlarm(sessionStub(session.sessionId));
      await within(blocked, `blocked ${mode} did not start`);
      await within(independentlyDeleted, "independent retired object was not deleted");
      const independentRow = await runInDurableObject(
        sessionStub(session.sessionId),
        (_instance, durable) =>
          durable.storage.sql
            .exec("SELECT state FROM snapshot WHERE snapshot_id = ?", deletedId)
            .toArray()[0],
      );
      expect(independentRow).toBeUndefined();
      expect(await env.SNAPSHOTS.head(deletedKey)).toBeNull();

      const freshId = `snapshot_during_blocked_${mode}`;
      const fresh = await uploadSnapshot(session, freshId);
      expect(fresh.status).toBe(201);
      await fresh.body?.cancel();

      releaseBlocked?.();
      expect(await alarm).toBe(true);
    },
  );

  it("persists the alarm before reserving and does no R2 work when alarm setup fails", async () => {
    const session = await createSession();
    let createCalls = 0;
    let headCalls = 0;
    await overrideSnapshotBucket(session, (base) =>
      bucketWithOverrides(base, {
        createMultipartUpload(
          key: string,
          options?: R2MultipartOptions,
        ): Promise<R2MultipartUpload> {
          createCalls += 1;
          return base.createMultipartUpload(key, options);
        },
        head(key: string): Promise<R2Object | null> {
          headCalls += 1;
          return base.head(key);
        },
      }),
    );
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const coordinator = Reflect.get(instance, "snapshotUploads") as object;
      Reflect.set(coordinator, "scheduleAlarm", async () => {
        throw new Error("injected alarm failure");
      });
    });

    const snapshotId = "snapshot_alarm_failure_1";
    const response = await uploadSnapshot(session, snapshotId);
    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({ error: "snapshot-upload-failed" });
    expect({ createCalls, headCalls }).toEqual({ createCalls: 0, headCalls: 0 });
    const uploadCount = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) =>
        durable.storage.sql.exec("SELECT COUNT(*) AS value FROM snapshot_upload").one().value,
    );
    expect(uploadCount).toBe(0);
  });

  it("persists a maintenance watchdog before any per-id R2 recovery", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_gc_alarm_failure";
    const metadata = await metadataFor(session, snapshotId);
    await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      const snapshots = new SnapshotStore(
        durable,
        durable.storage.sql,
        new RelayStore(durable.storage.sql),
        16,
      );
      expect(snapshots.beginUpload(metadata, 0)).toMatchObject({ ok: true, state: "started" });
    });

    let abortCalls = 0;
    let deleteCalls = 0;
    let headCalls = 0;
    await overrideSnapshotBucket(session, (base) =>
      bucketWithOverrides(base, {
        async head(key: string): Promise<R2Object | null> {
          headCalls += 1;
          return base.head(key);
        },
        async delete(key: string | string[]): Promise<void> {
          deleteCalls += 1;
          return base.delete(key);
        },
        resumeMultipartUpload(key: string, uploadId: string): R2MultipartUpload {
          const multipart = base.resumeMultipartUpload(key, uploadId);
          return {
            ...multipart,
            async abort(): Promise<void> {
              abortCalls += 1;
              return multipart.abort();
            },
          };
        },
      }),
    );
    const state = await runInDurableObject(
      sessionStub(session.sessionId),
      async (instance, durable) => {
        const coordinator = Reflect.get(instance, "snapshotUploads") as object;
        Reflect.set(coordinator, "scheduleAlarm", async () => {
          throw new Error("injected maintenance alarm failure");
        });
        await forceSessionAlarmDue(instance);
        await expect(instance.alarm()).rejects.toThrow("injected maintenance alarm failure");
        const owners = Reflect.get(coordinator, "snapshotOperationOwners") as Map<string, object>;
        return {
          owners: owners.size,
          upload: durable.storage.sql
            .exec("SELECT state FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
            .one(),
        };
      },
    );
    expect({ abortCalls, deleteCalls, headCalls }).toEqual({
      abortCalls: 0,
      deleteCalls: 0,
      headCalls: 0,
    });
    expect(state).toEqual({ owners: 0, upload: { state: "preparing" } });
  });

  it("propagates an alarm watchdog failure and converges on the next durable dispatch", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_alarm_runtime_retry";
    const metadata = await metadataFor(session, snapshotId);
    await runInDurableObject(sessionStub(session.sessionId), async (_instance, durable) => {
      const snapshots = new SnapshotStore(
        durable,
        durable.storage.sql,
        new RelayStore(durable.storage.sql),
        16,
      );
      expect(snapshots.beginUpload(metadata, 0)).toMatchObject({ ok: true, state: "started" });
      await durable.storage.setAlarm(Date.now() + 60_000);
    });

    let headCalls = 0;
    let deleteCalls = 0;
    let failureObserved = false;
    let r2CallsAtFailure: { deleteCalls: number; headCalls: number } | undefined;
    await overrideSnapshotBucket(session, (base) =>
      bucketWithOverrides(base, {
        async delete(key: string | string[]): Promise<void> {
          deleteCalls += 1;
          await base.delete(key);
        },
        async head(key: string): Promise<R2Object | null> {
          headCalls += 1;
          return base.head(key);
        },
      }),
    );
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const coordinator = Reflect.get(instance, "snapshotUploads") as object;
      const scheduleAlarm = Reflect.get(coordinator, "scheduleAlarm").bind(coordinator) as (
        timestamp: number,
      ) => Promise<void>;
      let fail = true;
      Reflect.set(coordinator, "scheduleAlarm", (timestamp: number) => {
        if (fail) {
          fail = false;
          failureObserved = true;
          r2CallsAtFailure = { deleteCalls, headCalls };
          return Promise.reject(new Error("injected real alarm failure"));
        }
        return scheduleAlarm(timestamp);
      });
    });

    await forceNextSessionAlarmDue(session.sessionId);
    await expect(runDurableObjectAlarm(sessionStub(session.sessionId))).rejects.toThrow(
      "injected real alarm failure",
    );
    const failedState = await runInDurableObject(
      sessionStub(session.sessionId),
      async (_instance, durable) => ({
        alarm: await durable.storage.getAlarm(),
        upload: durable.storage.sql
          .exec("SELECT state FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
          .toArray()[0],
      }),
    );
    expect({ failureObserved, r2CallsAtFailure }).toEqual({
      failureObserved: true,
      r2CallsAtFailure: { deleteCalls: 0, headCalls: 0 },
    });
    expect(failedState).toEqual({
      alarm: FORCED_SESSION_ALARM_NOW + DURABLE_ALARM_FAILURE_RETRY_MS,
      upload: { state: "preparing" },
    });

    // The mux durably restored a bounded retry; advance that component for the next dispatch.
    await forceNextSessionAlarmDue(session.sessionId);
    expect(await runDurableObjectAlarm(sessionStub(session.sessionId))).toBe(true);
    await settleSnapshotMaintenance(session.sessionId);
    const afterRetry = await runInDurableObject(
      sessionStub(session.sessionId),
      async (_instance, durable) => ({
        alarm: await durable.storage.getAlarm(),
        upload: durable.storage.sql
          .exec("SELECT state FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
          .toArray()[0],
      }),
    );
    expect(afterRetry.upload).toBeUndefined();
    expect(afterRetry.alarm).not.toBeNull();
    expect(headCalls).toBeGreaterThan(0);
  });

  it("consumes an expired uncertain alarm without scheduling automatic polling", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_uncertain_expired";
    const metadata = await metadataFor(session, snapshotId);
    await runInDurableObject(sessionStub(session.sessionId), async (_instance, durable) => {
      const snapshots = new SnapshotStore(
        durable,
        durable.storage.sql,
        new RelayStore(durable.storage.sql),
        16,
      );
      const reserved = snapshots.beginUpload(metadata, 0);
      expect(reserved).toMatchObject({ ok: true, state: "started" });
      if (!reserved.ok || reserved.state !== "started") throw new Error("reservation failed");
      expect(
        snapshots.recordMultipartUpload(
          snapshotId,
          reserved.upload.object_key,
          "upload-uncertain-expired",
        ),
      ).toBe(true);
      expect(
        snapshots.recordMultipartUncertain(
          snapshotId,
          reserved.upload.object_key,
          "upload-uncertain-expired",
        ),
      ).toBe(true);
      durable.storage.sql.exec(
        "UPDATE snapshot_upload SET expires_at = 0 WHERE snapshot_id = ?",
        snapshotId,
      );
      await durable.storage.setAlarm(Date.now() + 1_000);
    });

    let r2Calls = 0;
    await overrideSnapshotBucket(session, (base) =>
      bucketWithOverrides(base, {
        createMultipartUpload(
          key: string,
          options?: R2MultipartOptions,
        ): Promise<R2MultipartUpload> {
          r2Calls += 1;
          return base.createMultipartUpload(key, options);
        },
        delete(key: string | string[]): Promise<void> {
          r2Calls += 1;
          return base.delete(key);
        },
        head(key: string): Promise<R2Object | null> {
          r2Calls += 1;
          return base.head(key);
        },
        resumeMultipartUpload(key: string, uploadId: string): R2MultipartUpload {
          r2Calls += 1;
          return base.resumeMultipartUpload(key, uploadId);
        },
      }),
    );

    await forceNextSessionAlarmDue(session.sessionId);
    expect(await runDurableObjectAlarm(sessionStub(session.sessionId))).toBe(true);
    await settleSnapshotMaintenance(session.sessionId);
    const state = await runInDurableObject(
      sessionStub(session.sessionId),
      async (_instance, durable) => ({
        alarm: await durable.storage.getAlarm(),
        upload: durable.storage.sql
          .exec("SELECT state FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
          .toArray()[0],
      }),
    );
    expect(state).toEqual({ alarm: null, upload: { state: "uncertain" } });
    expect(r2Calls).toBe(0);
  });

  it("retires an expired preparing fence after hibernation without exposing an object", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_preparing_expired";
    const metadata = await metadataFor(session, snapshotId);
    const objectKey = await runInDurableObject(
      sessionStub(session.sessionId),
      async (_instance, durable) => {
        await durable.storage.setAlarm(Date.now() + 1_000);
        const snapshots = new SnapshotStore(
          durable,
          durable.storage.sql,
          new RelayStore(durable.storage.sql),
          16,
        );
        const reserved = snapshots.beginUpload(metadata, 0);
        expect(reserved).toMatchObject({ ok: true, state: "started" });
        if (!reserved.ok || reserved.state !== "started") throw new Error("reservation failed");
        return reserved.upload.object_key;
      },
    );
    await env.SNAPSHOTS.createMultipartUpload(objectKey, {
      httpMetadata: { contentType: SNAPSHOT_MEDIA_TYPE, cacheControl: "private, no-store" },
      customMetadata: snapshotCustomMetadata(metadata),
    });
    expect(await env.SNAPSHOTS.head(objectKey)).toBeNull();

    await evictDurableObject(sessionStub(session.sessionId));
    await forceNextSessionAlarmDue(session.sessionId);
    expect(await runDurableObjectAlarm(sessionStub(session.sessionId))).toBe(true);
    await settleSnapshotMaintenance(session.sessionId);
    const afterMaintenance = await runInDurableObject(
      sessionStub(session.sessionId),
      async (_instance, durable) => ({
        alarm: await durable.storage.getAlarm(),
        upload: durable.storage.sql
          .exec("SELECT state FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
          .toArray()[0],
      }),
    );
    expect(afterMaintenance.upload).toBeUndefined();
    expect(afterMaintenance.alarm).not.toBeNull();
    expect(await env.SNAPSHOTS.head(objectKey)).toBeNull();

    await forceNextSessionAlarmDue(session.sessionId);
    expect(await runDurableObjectAlarm(sessionStub(session.sessionId))).toBe(true);
    const finalAlarm = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) => durable.storage.getAlarm(),
    );
    expect(finalAlarm).toBeNull();
  });

  it("keeps continuous successful uploads bounded to latest plus marker grace", async () => {
    const session = await createSession();
    const snapshotIds = Array.from(
      { length: 48 },
      (_, index) => `snapshot_stream_${index.toString().padStart(6, "0")}`,
    );
    const objectKeys: string[] = [];
    for (const snapshotId of snapshotIds) {
      const response = await uploadSnapshot(session, snapshotId);
      expect(response.status).toBe(201);
      await response.body?.cancel();
      objectKeys.push(await storedSnapshotKey(session.sessionId, snapshotId));
    }
    await runInDurableObject(sessionStub(session.sessionId), async (instance) => {
      await forceSessionAlarmDue(instance);
      await instance.alarm();
    });
    await settleSnapshotMaintenance(session.sessionId);
    const state = await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      return {
        snapshots: durable.storage.sql
          .exec("SELECT snapshot_id, state FROM snapshot ORDER BY snapshot_id")
          .toArray(),
        uploads: durable.storage.sql.exec("SELECT COUNT(*) AS value FROM snapshot_upload").one()
          .value,
      };
    });
    expect(state.uploads).toBe(0);
    expect(state.snapshots).toEqual(
      snapshotIds.slice(-3).map((snapshotId) => ({ snapshot_id: snapshotId, state: "servable" })),
    );
    expect(await env.SNAPSHOTS.head(objectKeys[0]!)).toBeNull();
    expect(await env.SNAPSHOTS.head(objectKeys.at(-1)!)).not.toBeNull();
  }, 15_000);

  it("rejects the thirty-third object before R2 when delete failures fill the shared ledger", async () => {
    const session = await createSession();
    const rejectedId = "snapshot_backlog_rejected";
    let rejectedR2Calls = 0;
    await overrideSnapshotBucket(session, (base) =>
      bucketWithOverrides(base, {
        createMultipartUpload(
          key: string,
          options?: R2MultipartOptions,
        ): Promise<R2MultipartUpload> {
          if (matchesSnapshotKey(key, session.sessionId, rejectedId)) rejectedR2Calls += 1;
          return base.createMultipartUpload(key, options);
        },
        async delete(): Promise<void> {
          throw new Error("injected R2 delete outage");
        },
        head(key: string): Promise<R2Object | null> {
          if (matchesSnapshotKey(key, session.sessionId, rejectedId)) rejectedR2Calls += 1;
          return base.head(key);
        },
      }),
    );

    const acceptedIds = Array.from(
      { length: 32 },
      (_, index) => `snapshot_backlog_${index.toString().padStart(5, "0")}`,
    );
    const body = encoder.encode("snapshot-state");
    const digest = await sha256Hex(body);
    const seeded = await Promise.all(
      acceptedIds.map(async (snapshotId, index) => {
        const metadata: SnapshotMetadata = {
          sessionId: session.sessionId,
          snapshotId,
          engineId,
          sessionEpoch: "7",
          cutEventSeq: "0",
          nextPtyOffset: "0",
          compression: "none",
          compressedLength: body.byteLength.toString(),
          uncompressedLength: body.byteLength.toString(),
          sha256: digest,
        };
        const objectKey = snapshotAttemptObjectKey(
          session.sessionId,
          snapshotId,
          "retention_backlog_0001",
        );
        const object = await env.SNAPSHOTS.put(objectKey, body, {
          sha256: hexToBytes(digest),
          httpMetadata: {
            contentType: SNAPSHOT_MEDIA_TYPE,
            cacheControl: "private, no-store",
          },
          customMetadata: snapshotCustomMetadata(metadata),
        });
        if (object === null) throw new Error(`failed to seed ${snapshotId}`);
        return { createdAt: index + 1, metadata, object, objectKey };
      }),
    );
    await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      for (const [index, entry] of seeded.entries()) {
        durable.storage.sql.exec(
          `INSERT INTO snapshot
            (snapshot_id, session_epoch, cut_event_seq, next_pty_offset, engine_id,
             object_key, r2_version, etag, sha256, compressed_length,
             uncompressed_length, compression, state, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          entry.metadata.snapshotId,
          entry.metadata.sessionEpoch,
          entry.metadata.cutEventSeq,
          entry.metadata.nextPtyOffset,
          entry.metadata.engineId,
          entry.objectKey,
          entry.object.version,
          entry.object.etag,
          entry.metadata.sha256,
          Number(entry.metadata.compressedLength),
          entry.metadata.uncompressedLength,
          entry.metadata.compression,
          index < 29 ? "retired" : "servable",
          entry.createdAt,
        );
      }
      durable.storage.sql.exec(
        `UPDATE session_state
         SET latest_snapshot_id = ?, snapshot_created_clock = ?
         WHERE singleton = 1`,
        acceptedIds.at(-1),
        acceptedIds.length,
      );
    });
    const acceptedKeys = seeded.map(({ objectKey }) => objectKey);
    const rejected = await uploadSnapshot(session, rejectedId);
    expect(rejected.status).toBe(503);
    expect(await rejected.json()).toEqual({ error: "snapshot-reservation-failed" });
    expect(rejectedR2Calls).toBe(0);
    expect(await snapshotObjectKeys(session.sessionId, rejectedId)).toEqual([]);

    const saturated = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) => ({
        snapshots: durable.storage.sql
          .exec("SELECT state, COUNT(*) AS value FROM snapshot GROUP BY state ORDER BY state")
          .toArray(),
        rejectedUpload: durable.storage.sql
          .exec("SELECT state FROM snapshot_upload WHERE snapshot_id = ?", rejectedId)
          .toArray()[0],
        uploads: durable.storage.sql.exec("SELECT COUNT(*) AS value FROM snapshot_upload").one()
          .value,
      }),
    );
    expect(saturated).toEqual({
      rejectedUpload: undefined,
      snapshots: [
        { state: "retired", value: 29 },
        { state: "servable", value: 3 },
      ],
      uploads: 0,
    });
    const retiredPointer = await getSnapshot(session, acceptedIds[0]!);
    expect(retiredPointer.status).toBe(404);
    await retiredPointer.body?.cancel();
    expect(await env.SNAPSHOTS.head(acceptedKeys[0]!)).not.toBeNull();

    await evictDurableObject(sessionStub(session.sessionId));
    await runInDurableObject(sessionStub(session.sessionId), async (instance) => {
      await forceSessionAlarmDue(instance);
      await instance.alarm();
    });
    await settleSnapshotMaintenance(session.sessionId);
    const recovered = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) =>
        durable.storage.sql
          .exec("SELECT snapshot_id, state FROM snapshot ORDER BY snapshot_id")
          .toArray(),
    );
    expect(recovered).toEqual(
      acceptedIds.slice(-3).map((snapshotId) => ({ snapshot_id: snapshotId, state: "servable" })),
    );
  });

  it("blocks GET after the SQLite retire commit and before a paused R2 delete", async () => {
    const session = await createSession();
    const snapshotIds = [
      "snapshot_gc_race_000001",
      "snapshot_gc_race_000002",
      "snapshot_gc_race_000003",
      "snapshot_gc_race_000004",
    ];
    const objectKeys: string[] = [];
    for (const snapshotId of snapshotIds.slice(0, 3)) {
      const response = await uploadSnapshot(session, snapshotId);
      expect(response.status).toBe(201);
      await response.body?.cancel();
      objectKeys.push(await storedSnapshotKey(session.sessionId, snapshotId));
    }

    let releaseDelete: (() => void) | undefined;
    const deleteGate = new Promise<void>((resolve) => {
      releaseDelete = resolve;
    });
    await overrideSnapshotBucket(session, (base) =>
      bucketWithOverrides(base, {
        async delete(key: string | string[]): Promise<void> {
          await deleteGate;
          await base.delete(key);
        },
      }),
    );

    const fourth = await uploadSnapshot(session, snapshotIds[3]!);
    expect(fourth.status).toBe(201);
    await fourth.body?.cancel();
    const retiredState = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) =>
        durable.storage.sql
          .exec("SELECT state FROM snapshot WHERE snapshot_id = ?", snapshotIds[0]!)
          .one().state,
    );
    expect(retiredState).toBe("retired");
    const blocked = await getSnapshot(session, snapshotIds[0]!);
    expect(blocked.status).toBe(404);
    await blocked.body?.cancel();
    expect(await env.SNAPSHOTS.head(objectKeys[0]!)).not.toBeNull();

    releaseDelete?.();
    await runInDurableObject(sessionStub(session.sessionId), async (instance) => {
      await forceSessionAlarmDue(instance);
      await instance.alarm();
    });
    await settleSnapshotMaintenance(session.sessionId);
    expect(await env.SNAPSHOTS.head(objectKeys[0]!)).toBeNull();
  });

  it("reserves cursor-ahead uploads for retry and rejects wrong session identity before R2", async () => {
    const session = await createSession();
    const cursorAheadId = "snapshot_orphan_cut_0001";
    let createCalls = 0;
    let partCalls = 0;
    await overrideSnapshotBucket(session, (base) =>
      bucketWithOverrides(base, {
        async createMultipartUpload(
          key: string,
          options?: R2MultipartOptions,
        ): Promise<R2MultipartUpload> {
          createCalls += 1;
          const multipart = await base.createMultipartUpload(key, options);
          return {
            key: multipart.key,
            uploadId: multipart.uploadId,
            async uploadPart(
              partNumber: number,
              value: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob,
              partOptions?: R2UploadPartOptions,
            ): Promise<R2UploadedPart> {
              partCalls += 1;
              return multipart.uploadPart(partNumber, value, partOptions);
            },
            abort: multipart.abort.bind(multipart),
            complete: multipart.complete.bind(multipart),
          };
        },
      }),
    );
    const cursorAhead = await uploadSnapshot(session, cursorAheadId, session.hostCapability, {
      cutEventSeq: "1",
    });
    expect(cursorAhead.status).toBe(409);
    expect(await cursorAhead.json()).toEqual({ error: "snapshot-cursor-ahead" });
    expect({ createCalls, partCalls }).toEqual({ createCalls: 1, partCalls: 1 });
    const cursorAheadKey = await storedSnapshotKey(session.sessionId, cursorAheadId);
    expect(await env.SNAPSHOTS.head(cursorAheadKey)).not.toBeNull();
    const privatePointer = await getSnapshot(session, cursorAheadId);
    expect(privatePointer.status).toBe(404);
    await privatePointer.body?.cancel();
    await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      durable.storage.sql.exec("UPDATE session_state SET head_event_seq = '1' WHERE singleton = 1");
    });
    const retried = await uploadSnapshot(session, cursorAheadId, session.hostCapability, {
      cutEventSeq: "1",
    });
    expect(retried.status).toBe(201);
    expect({ createCalls, partCalls }).toEqual({ createCalls: 1, partCalls: 1 });
    expect(await retried.json()).toMatchObject({
      created: true,
      snapshot: { snapshotId: cursorAheadId },
    });

    const expiringId = "snapshot_expiring_upload_01";
    const expiring = await uploadSnapshot(session, expiringId, session.hostCapability, {
      cutEventSeq: "2",
    });
    expect(expiring.status).toBe(409);
    const expiringKey = await storedSnapshotKey(session.sessionId, expiringId);
    expect(await env.SNAPSHOTS.head(expiringKey)).not.toBeNull();
    await runInDurableObject(sessionStub(session.sessionId), async (instance, durable) => {
      durable.storage.sql.exec(
        "UPDATE snapshot_upload SET expires_at = 0 WHERE snapshot_id = ?",
        expiringId,
      );
      await forceSessionAlarmDue(instance);
      await instance.alarm();
    });
    await settleSnapshotMaintenance(session.sessionId);
    expect(await env.SNAPSHOTS.head(expiringKey)).toBeNull();

    const invalidSnapshots: Array<[string, UploadOverrides]> = [
      ["snapshot_orphan_epoch_001", { sessionEpoch: "8" }],
      ["snapshot_orphan_engine_01", { engineId: `${engineId}:other` }],
    ];

    for (const [snapshotId, overrides] of invalidSnapshots) {
      const response = await uploadSnapshot(session, snapshotId, session.hostCapability, overrides);
      expect(response.status).toBe(409);
      expect(await response.json()).toEqual({ error: "snapshot-conflict" });
      expect(await snapshotObjectKeys(session.sessionId, snapshotId)).toEqual([]);
      const pointer = await getSnapshot(session, snapshotId);
      expect(pointer.status).toBe(404);
      await pointer.body?.cancel();
    }
  });

  it("persists an exact completed identity before same-id retry finalizes it", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_complete_gc_crash";
    const body = encoder.encode("snapshot-state");
    const metadata = await metadataFor(session, snapshotId, body);
    const objectKey = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) => {
        const snapshots = new SnapshotStore(
          durable,
          durable.storage.sql,
          new RelayStore(durable.storage.sql),
          16,
        );
        const reserved = snapshots.beginUpload(metadata, Date.now());
        expect(reserved).toMatchObject({ ok: true, state: "started" });
        if (!reserved.ok || reserved.state !== "started") throw new Error("reservation failed");
        return reserved.upload.object_key;
      },
    );
    const multipart = await env.SNAPSHOTS.createMultipartUpload(objectKey, {
      httpMetadata: { contentType: SNAPSHOT_MEDIA_TYPE, cacheControl: "private, no-store" },
      customMetadata: snapshotCustomMetadata(metadata),
    });
    const part = await multipart.uploadPart(1, body);
    await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      const snapshots = new SnapshotStore(
        durable,
        durable.storage.sql,
        new RelayStore(durable.storage.sql),
        16,
      );
      expect(snapshots.recordMultipartUpload(snapshotId, objectKey, multipart.uploadId)).toBe(true);
      expect(
        snapshots.recordMultipartCompleting(snapshotId, objectKey, multipart.uploadId, part.etag),
      ).toBe(true);
      durable.storage.sql.exec(
        "UPDATE snapshot_upload SET expires_at = 0 WHERE snapshot_id = ?",
        snapshotId,
      );
    });
    await multipart.complete([part]);
    await overrideSnapshotBucket(session, (base) =>
      bucketWithOverrides(base, {
        resumeMultipartUpload(key: string, uploadId: string): R2MultipartUpload {
          const resumed = base.resumeMultipartUpload(key, uploadId);
          return {
            key: resumed.key,
            uploadId: resumed.uploadId,
            uploadPart: resumed.uploadPart.bind(resumed),
            complete: resumed.complete.bind(resumed),
            async abort(): Promise<void> {
              throw Object.assign(new Error("NoSuchUpload (10024)"), { code: 10024 });
            },
          };
        },
      }),
    );

    let markCompleted: (() => void) | undefined;
    const completed = new Promise<void>((resolve) => {
      markCompleted = resolve;
    });
    await runInDurableObject(sessionStub(session.sessionId), async (instance) => {
      const coordinator = Reflect.get(instance, "snapshotUploads") as object;
      const snapshots = Reflect.get(coordinator, "snapshots") as SnapshotStore;
      const recordCompleted = snapshots.recordMultipartCompleted.bind(snapshots);
      Reflect.set(
        snapshots,
        "recordMultipartCompleted",
        (...args: Parameters<SnapshotStore["recordMultipartCompleted"]>) => {
          expect(recordCompleted(...args)).toBeDefined();
          markCompleted?.();
          throw new Error("simulated termination after completed identity commit");
        },
      );
      await forceSessionAlarmDue(instance);
      await instance.alarm();
    });
    await within(completed, "completed identity was not committed");
    await within(
      settleSnapshotMaintenance(session.sessionId),
      "completed maintenance task did not settle",
    );
    const committed = await runInDurableObject(
      sessionStub(session.sessionId),
      (instance, durable) => {
        const alarmMux = Reflect.get(instance, "alarmMux") as object;
        const coordinator = Reflect.get(instance, "snapshotUploads") as object;
        return {
          activeBody: Reflect.get(coordinator, "activeSnapshotBodyUploadId"),
          alarmUpdate: Reflect.get(alarmMux, "reconcileRun"),
          maintenance: Reflect.get(coordinator, "snapshotMaintenance"),
          maintenanceRequested: Reflect.get(coordinator, "snapshotMaintenanceRequested"),
          owners: (Reflect.get(coordinator, "snapshotOperationOwners") as Map<string, object>).size,
          row: durable.storage.sql
            .exec(
              "SELECT state, r2_version, etag FROM snapshot_upload WHERE snapshot_id = ?",
              snapshotId,
            )
            .one(),
          tasks: (Reflect.get(coordinator, "snapshotMaintenanceTasks") as Map<string, object>).size,
        };
      },
    );
    expect(committed).toMatchObject({
      activeBody: undefined,
      alarmUpdate: undefined,
      maintenance: undefined,
      maintenanceRequested: false,
      owners: 0,
      row: { state: "completed" },
      tasks: 0,
    });
    expect(committed.row.r2_version).toBeTruthy();
    expect(committed.row.etag).toBeTruthy();
    expect(await env.SNAPSHOTS.head(objectKey)).not.toBeNull();

    let createCalls = 0;
    let partCalls = 0;
    let resumeCalls = 0;
    await overrideSnapshotBucket(session, (base) =>
      bucketWithOverrides(base, {
        createMultipartUpload(
          key: string,
          options?: R2MultipartOptions,
        ): Promise<R2MultipartUpload> {
          createCalls += 1;
          return base.createMultipartUpload(key, options);
        },
        resumeMultipartUpload(key: string, uploadId: string): R2MultipartUpload {
          resumeCalls += 1;
          const resumed = base.resumeMultipartUpload(key, uploadId);
          return {
            key: resumed.key,
            uploadId: resumed.uploadId,
            async uploadPart(
              partNumber: number,
              value: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob,
              options?: R2UploadPartOptions,
            ): Promise<R2UploadedPart> {
              partCalls += 1;
              return resumed.uploadPart(partNumber, value, options);
            },
            abort: resumed.abort.bind(resumed),
            complete: resumed.complete.bind(resumed),
          };
        },
      }),
    );
    const retried = await within(
      uploadSnapshot(session, snapshotId),
      "completed same-id retry did not settle",
    );
    expect(retried.status).toBe(201);
    expect(await retried.json()).toMatchObject({ snapshot: { snapshotId } });
    expect({ createCalls, partCalls, resumeCalls }).toEqual({
      createCalls: 0,
      partCalls: 0,
      resumeCalls: 0,
    });
    const upload = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) =>
        durable.storage.sql
          .exec("SELECT state FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
          .toArray()[0],
    );
    expect(upload).toBeUndefined();
    expect(await env.SNAPSHOTS.head(objectKey)).not.toBeNull();
  });

  it("keeps uncertain and missing completed rows private with HEAD-only retries", async () => {
    const session = await createSession();
    const uncertainId = "snapshot_uncertain_null_1";
    const completedId = "snapshot_completed_missing";
    const uncertainMetadata = await metadataFor(session, uncertainId);
    const completedMetadata = await metadataFor(session, completedId);
    await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      const snapshots = new SnapshotStore(
        durable,
        durable.storage.sql,
        new RelayStore(durable.storage.sql),
        16,
      );
      const uncertain = snapshots.beginUpload(uncertainMetadata, Date.now());
      expect(uncertain).toMatchObject({ ok: true, state: "started" });
      if (!uncertain.ok || uncertain.state !== "started") throw new Error("reservation failed");
      const uncertainKey = uncertain.upload.object_key;
      expect(snapshots.recordMultipartUpload(uncertainId, uncertainKey, "upload-uncertain")).toBe(
        true,
      );
      expect(
        snapshots.recordMultipartUncertain(uncertainId, uncertainKey, "upload-uncertain"),
      ).toBe(true);

      const completed = snapshots.beginUpload(completedMetadata, Date.now());
      expect(completed).toMatchObject({ ok: true, state: "started" });
      if (!completed.ok || completed.state !== "started") throw new Error("reservation failed");
      const completedKey = completed.upload.object_key;
      expect(snapshots.recordMultipartUpload(completedId, completedKey, "upload-completed")).toBe(
        true,
      );
      expect(
        snapshots.recordMultipartCompleting(
          completedId,
          completedKey,
          "upload-completed",
          "part-completed",
        ),
      ).toBe(true);
      expect(
        snapshots.recordMultipartCompleted(
          completedId,
          completedKey,
          "upload-completed",
          "missing-version",
          "missing-etag-1",
        ),
      ).toBeDefined();
    });

    let createCalls = 0;
    let headCalls = 0;
    let resumeCalls = 0;
    await overrideSnapshotBucket(session, (base) =>
      bucketWithOverrides(base, {
        createMultipartUpload(
          key: string,
          options?: R2MultipartOptions,
        ): Promise<R2MultipartUpload> {
          createCalls += 1;
          return base.createMultipartUpload(key, options);
        },
        head(key: string): Promise<R2Object | null> {
          headCalls += 1;
          return base.head(key);
        },
        resumeMultipartUpload(key: string, uploadId: string): R2MultipartUpload {
          resumeCalls += 1;
          return base.resumeMultipartUpload(key, uploadId);
        },
      }),
    );

    for (const id of [uncertainId, completedId]) {
      const response = await uploadSnapshot(session, id);
      expect(response.status).toBe(503);
      await response.body?.cancel();
    }
    expect({ createCalls, headCalls, resumeCalls }).toEqual({
      createCalls: 0,
      headCalls: 2,
      resumeCalls: 0,
    });
    await runInDurableObject(sessionStub(session.sessionId), async (instance) => {
      await forceSessionAlarmDue(instance);
      await instance.alarm();
    });
    expect({ createCalls, headCalls, resumeCalls }).toEqual({
      createCalls: 0,
      headCalls: 2,
      resumeCalls: 0,
    });
    const states = await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) =>
      durable.storage.sql
        .exec("SELECT snapshot_id, state FROM snapshot_upload ORDER BY snapshot_id")
        .toArray(),
    );
    expect(states).toEqual([
      { snapshot_id: completedId, state: "completed" },
      { snapshot_id: uncertainId, state: "uncertain" },
    ]);
  });

  it("finalizes an uncertain row only after the exact object appears on same-id retry", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_uncertain_exact_1";
    const body = encoder.encode("snapshot-state");
    const metadata = await metadataFor(session, snapshotId, body);
    const objectKey = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) => {
        const snapshots = new SnapshotStore(
          durable,
          durable.storage.sql,
          new RelayStore(durable.storage.sql),
          16,
        );
        const reserved = snapshots.beginUpload(metadata, Date.now());
        expect(reserved).toMatchObject({ ok: true, state: "started" });
        if (!reserved.ok || reserved.state !== "started") throw new Error("reservation failed");
        return reserved.upload.object_key;
      },
    );
    const multipart = await env.SNAPSHOTS.createMultipartUpload(objectKey, {
      httpMetadata: { contentType: SNAPSHOT_MEDIA_TYPE, cacheControl: "private, no-store" },
      customMetadata: snapshotCustomMetadata(metadata),
    });
    await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      const snapshots = new SnapshotStore(
        durable,
        durable.storage.sql,
        new RelayStore(durable.storage.sql),
        16,
      );
      expect(snapshots.recordMultipartUpload(snapshotId, objectKey, multipart.uploadId)).toBe(true);
    });
    const part = await multipart.uploadPart(1, body);
    await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      const snapshots = new SnapshotStore(
        durable,
        durable.storage.sql,
        new RelayStore(durable.storage.sql),
        16,
      );
      expect(
        snapshots.recordMultipartCompleting(snapshotId, objectKey, multipart.uploadId, part.etag),
      ).toBe(true);
      expect(snapshots.recordMultipartUncertain(snapshotId, objectKey, multipart.uploadId)).toBe(
        true,
      );
    });
    await multipart.complete([part]);

    let createCalls = 0;
    let partCalls = 0;
    let resumeCalls = 0;
    await overrideSnapshotBucket(session, (base) =>
      bucketWithOverrides(base, {
        createMultipartUpload(
          key: string,
          options?: R2MultipartOptions,
        ): Promise<R2MultipartUpload> {
          createCalls += 1;
          return base.createMultipartUpload(key, options);
        },
        resumeMultipartUpload(key: string, uploadId: string): R2MultipartUpload {
          resumeCalls += 1;
          const resumed = base.resumeMultipartUpload(key, uploadId);
          return {
            key: resumed.key,
            uploadId: resumed.uploadId,
            async uploadPart(
              partNumber: number,
              value: ReadableStream | ArrayBuffer | ArrayBufferView | string | Blob,
              options?: R2UploadPartOptions,
            ): Promise<R2UploadedPart> {
              partCalls += 1;
              return resumed.uploadPart(partNumber, value, options);
            },
            abort: resumed.abort.bind(resumed),
            complete: resumed.complete.bind(resumed),
          };
        },
      }),
    );
    const response = await uploadSnapshot(session, snapshotId);
    expect(response.status).toBe(201);
    expect(await response.json()).toMatchObject({ snapshot: { snapshotId } });
    expect({ createCalls, partCalls, resumeCalls }).toEqual({
      createCalls: 0,
      partCalls: 0,
      resumeCalls: 0,
    });
  });

  it("never moves the latest pointer backwards under out-of-order finalization", async () => {
    const session = await createSession();
    await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      durable.storage.sql.exec(
        `UPDATE session_state
         SET head_event_seq = '10000000000000000', next_pty_offset = '10000000000000000'
         WHERE singleton = 1`,
      );
    });

    const uploadAt = async (snapshotId: string, cutEventSeq: string, nextPtyOffset: string) => {
      const response = await uploadSnapshot(session, snapshotId, session.hostCapability, {
        cutEventSeq,
        nextPtyOffset,
      });
      expect(response.status).toBe(201);
      await response.body?.cancel();
      return runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
        return durable.storage.sql
          .exec("SELECT latest_snapshot_id FROM session_state WHERE singleton = 1")
          .one().latest_snapshot_id;
      });
    };

    expect(
      await uploadAt("snapshot_latest_cut10_01", "10000000000000000", "9999999999999999"),
    ).toBe("snapshot_latest_cut10_01");
    expect(
      await uploadAt("snapshot_latest_cut09_01", "9999999999999999", "10000000000000000"),
    ).toBe("snapshot_latest_cut10_01");
    expect(
      await uploadAt("snapshot_latest_lowoff_1", "10000000000000000", "9007199254740999"),
    ).toBe("snapshot_latest_cut10_01");
    expect(
      await uploadAt("snapshot_latest_highoff1", "10000000000000000", "10000000000000000"),
    ).toBe("snapshot_latest_highoff1");
  });
});
