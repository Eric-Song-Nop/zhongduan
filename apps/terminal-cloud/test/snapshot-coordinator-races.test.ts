import { SnapshotMetadataSchema } from "@zhongduan/protocol";
import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { RelayStore } from "../src/worker/relay-store";
import { SnapshotStore } from "../src/worker/snapshot-store";
import { SnapshotUploadCoordinator } from "../src/worker/snapshot-upload-coordinator";
import {
  bucketWithOverrides,
  createSession,
  encoder,
  installMiniflareMultipartEtagShimOnCoordinator,
  metadataFor,
  sessionStub,
  snapshotHeaders,
  storedSnapshotKey,
  uploadSnapshot,
  within,
} from "./snapshot-test-helpers";

async function settleCoordinator(coordinator: SnapshotUploadCoordinator): Promise<void> {
  for (let attempt = 0; attempt < 4; attempt += 1) {
    const tasks = Reflect.get(coordinator, "snapshotMaintenanceTasks") as Map<
      string,
      Promise<void>
    >;
    const maintenance = Reflect.get(coordinator, "snapshotMaintenance") as
      | Promise<void>
      | undefined;
    await Promise.allSettled([
      ...tasks.values(),
      ...(maintenance === undefined ? [] : [maintenance]),
    ]);
    if (tasks.size === 0 && Reflect.get(coordinator, "snapshotMaintenance") === undefined) return;
  }
  throw new Error("snapshot coordinator did not settle");
}

function coordinatorUploadRequest(
  session: Awaited<ReturnType<typeof createSession>>,
  snapshotId: string,
  sha256: string,
): Request {
  const body = encoder.encode("snapshot-state");
  const headers = snapshotHeaders(session, body, { sha256 });
  headers.set("content-length", body.byteLength.toString());
  return new Request(`https://do.internal/internal/snapshots/upload/${snapshotId}`, {
    method: "POST",
    headers,
    body: Uint8Array.from(body).buffer,
  });
}

describe("snapshot coordinator replacement races", () => {
  it("bounds public upload admission before the shared alarm await", async () => {
    const session = await createSession();
    let scheduleCalls = 0;
    let markFourStarted: (() => void) | undefined;
    let releaseAlarm: (() => void) | undefined;
    const fourStarted = new Promise<void>((resolve) => {
      markFourStarted = resolve;
    });
    const alarmGate = new Promise<void>((resolve) => {
      releaseAlarm = resolve;
    });
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const coordinator = Reflect.get(instance, "snapshotUploads") as object;
      const scheduleAlarm = Reflect.get(coordinator, "scheduleAlarm").bind(coordinator) as (
        timestamp: number,
      ) => Promise<void>;
      Reflect.set(coordinator, "scheduleAlarm", async (timestamp: number) => {
        scheduleCalls += 1;
        if (scheduleCalls === 4) markFourStarted?.();
        await alarmGate;
        await scheduleAlarm(timestamp);
      });
    });

    const uploads = Array.from({ length: 24 }, (_, index) =>
      uploadSnapshot(session, `snapshot_admission_${index.toString().padStart(4, "0")}`).then(
        async (response) => ({ body: await response.json(), status: response.status }),
      ),
    );
    await within(
      fourStarted,
      "four admitted snapshot requests did not reach the alarm gate",
      10_000,
    );
    const active = await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const coordinator = Reflect.get(instance, "snapshotUploads") as object;
      return {
        owners: (Reflect.get(coordinator, "snapshotOperationOwners") as Map<string, object>).size,
        requests: Reflect.get(coordinator, "activeSnapshotUploadRequests") as number,
      };
    });
    expect({ active, scheduleCalls }).toEqual({
      active: { owners: 4, requests: 4 },
      scheduleCalls: 4,
    });

    releaseAlarm?.();
    const results = await Promise.all(uploads);
    expect(results.filter((result) => result.status !== 503).length).toBeLessThanOrEqual(4);
    expect(results.filter((result) => result.status === 503).length).toBeGreaterThanOrEqual(20);
  });

  it("admits only four published retries before any fifth R2 HEAD", async () => {
    const session = await createSession();
    const snapshotIds = Array.from(
      { length: 5 },
      (_, index) => `snapshot_published_admission_${index.toString().padStart(4, "0")}`,
    );
    const pins = new Set(snapshotIds);
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      Reflect.set(
        Reflect.get(instance, "snapshotUploads") as object,
        "pinnedSnapshotIds",
        () => pins,
      );
    });
    for (const snapshotId of snapshotIds) {
      const response = await uploadSnapshot(session, snapshotId);
      expect(response.status).toBe(201);
      await response.body?.cancel();
    }

    let headCalls = 0;
    let markFourHeads: (() => void) | undefined;
    let releaseHeads: (() => void) | undefined;
    const fourHeads = new Promise<void>((resolve) => {
      markFourHeads = resolve;
    });
    const headGate = new Promise<void>((resolve) => {
      releaseHeads = resolve;
    });
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const coordinator = Reflect.get(instance, "snapshotUploads") as object;
      const base = Reflect.get(coordinator, "snapshotBucket") as R2Bucket;
      Reflect.set(
        coordinator,
        "snapshotBucket",
        bucketWithOverrides(base, {
          async head(key: string): Promise<R2Object | null> {
            headCalls += 1;
            if (headCalls === 4) markFourHeads?.();
            await headGate;
            return base.head(key);
          },
        }),
      );
    });

    const retries = snapshotIds.map((snapshotId) =>
      uploadSnapshot(session, snapshotId).then(async (response) => ({
        body: await response.text(),
        status: response.status,
      })),
    );
    await within(fourHeads, "four published retries did not reach R2 HEAD", 10_000);
    const active = await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const coordinator = Reflect.get(instance, "snapshotUploads") as object;
      return {
        owners: (Reflect.get(coordinator, "snapshotOperationOwners") as Map<string, object>).size,
        requests: Reflect.get(coordinator, "activeSnapshotUploadRequests") as number,
      };
    });
    expect({ active, headCalls }).toEqual({
      active: { owners: 4, requests: 4 },
      headCalls: 4,
    });

    releaseHeads?.();
    const responses = await Promise.all(retries);
    expect(
      responses.map((response) => response.status).sort((left, right) => left - right),
    ).toEqual([200, 200, 200, 200, 503]);
    expect(headCalls).toBe(4);
  });

  it("coalesces alarm targets across pending storage calls and restarts after rejection", async () => {
    const session = await createSession();
    await runInDurableObject(sessionStub(session.sessionId), async (_instance, durable) => {
      let persistedAlarm: number | null = null;
      let getCalls = 0;
      let setCalls = 0;
      let rejectNextGet = false;
      let rejectNextSet = false;
      let markFirstGet: (() => void) | undefined;
      let releaseFirstGet: (() => void) | undefined;
      let markFirstSet: (() => void) | undefined;
      let releaseFirstSet: (() => void) | undefined;
      const firstGet = new Promise<void>((resolve) => {
        markFirstGet = resolve;
      });
      const firstGetGate = new Promise<void>((resolve) => {
        releaseFirstGet = resolve;
      });
      const firstSet = new Promise<void>((resolve) => {
        markFirstSet = resolve;
      });
      const firstSetGate = new Promise<void>((resolve) => {
        releaseFirstSet = resolve;
      });
      const fakeState = {
        storage: {
          async getAlarm(): Promise<number | null> {
            getCalls += 1;
            if (rejectNextGet) {
              rejectNextGet = false;
              throw new Error("injected alarm read failure");
            }
            if (getCalls === 1) {
              markFirstGet?.();
              await firstGetGate;
            }
            return persistedAlarm;
          },
          async setAlarm(timestamp: number): Promise<void> {
            setCalls += 1;
            if (rejectNextSet) {
              rejectNextSet = false;
              throw new Error("injected alarm write failure");
            }
            if (setCalls === 1) {
              markFirstSet?.();
              await firstSetGate;
            }
            persistedAlarm = timestamp;
          },
        },
        waitUntil(promise: Promise<unknown>): void {
          void promise.catch(() => undefined);
        },
      } as unknown as DurableObjectState;
      const sql = durable.storage.sql;
      const coordinator = new SnapshotUploadCoordinator(
        fakeState,
        env.SNAPSHOTS,
        new SnapshotStore(durable, sql, new RelayStore(sql), 16),
        () => new Set(),
      );
      const scheduleAlarm = Reflect.get(coordinator, "scheduleAlarm").bind(coordinator) as (
        timestamp: number,
      ) => Promise<void>;

      const first = scheduleAlarm(100);
      await within(firstGet, "first alarm read did not start");
      const second = scheduleAlarm(50);
      expect(second).toBe(first);
      releaseFirstGet?.();
      await within(firstSet, "first alarm write did not start");
      const third = scheduleAlarm(25);
      expect(third).toBe(first);
      releaseFirstSet?.();
      await Promise.all([first, second, third]);
      expect({ getCalls, persistedAlarm, setCalls }).toEqual({
        getCalls: 2,
        persistedAlarm: 25,
        setCalls: 2,
      });

      rejectNextGet = true;
      await expect(scheduleAlarm(10)).rejects.toThrow("injected alarm read failure");
      await scheduleAlarm(20);
      expect(persistedAlarm).toBe(10);

      rejectNextSet = true;
      await expect(scheduleAlarm(5)).rejects.toThrow("injected alarm write failure");
      await scheduleAlarm(20);
      expect(persistedAlarm).toBe(5);
    });
  });

  it("defers a concurrent maintenance request to a later durable alarm", async () => {
    const session = await createSession();
    await runInDurableObject(sessionStub(session.sessionId), async (instance, durable) => {
      const coordinator = Reflect.get(instance, "snapshotUploads") as SnapshotUploadCoordinator;
      let runCount = 0;
      let markStarted: (() => void) | undefined;
      let releaseRun: (() => void) | undefined;
      const started = new Promise<void>((resolve) => {
        markStarted = resolve;
      });
      const gate = new Promise<void>((resolve) => {
        releaseRun = resolve;
      });
      Reflect.set(coordinator, "runMaintenance", async () => {
        runCount += 1;
        markStarted?.();
        await gate;
      });

      const first = coordinator.maintain();
      await within(started, "maintenance batch did not start");
      coordinator.scheduleMaintenance();
      coordinator.scheduleMaintenance();
      releaseRun?.();
      await first;
      await Promise.resolve();
      const alarmUpdate = Reflect.get(coordinator, "snapshotAlarmUpdate") as
        | Promise<void>
        | undefined;
      if (alarmUpdate !== undefined) await alarmUpdate;

      expect(runCount).toBe(1);
      expect(await durable.storage.getAlarm()).not.toBeNull();
    });
  });

  it("keeps a verified published retry recent through its barrier window", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_published_refresh_01";
    const pins = new Set([snapshotId]);
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const coordinator = Reflect.get(instance, "snapshotUploads") as object;
      Reflect.set(coordinator, "pinnedSnapshotIds", () => pins);
    });
    for (const id of [
      snapshotId,
      "snapshot_refresh_seed_01",
      "snapshot_refresh_seed_02",
      "snapshot_refresh_seed_03",
    ]) {
      const response = await uploadSnapshot(session, id);
      expect(response.status).toBe(201);
      await response.body?.cancel();
    }
    const objectKey = await storedSnapshotKey(session.sessionId, snapshotId);
    pins.delete(snapshotId);

    let markHeadCaptured: (() => void) | undefined;
    let releaseHead: (() => void) | undefined;
    const headCaptured = new Promise<void>((resolve) => {
      markHeadCaptured = resolve;
    });
    const headGate = new Promise<void>((resolve) => {
      releaseHead = resolve;
    });
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const coordinator = Reflect.get(instance, "snapshotUploads") as object;
      const base = Reflect.get(coordinator, "snapshotBucket") as R2Bucket;
      Reflect.set(
        coordinator,
        "snapshotBucket",
        bucketWithOverrides(base, {
          async head(key: string): Promise<R2Object | null> {
            const object = await base.head(key);
            if (key === objectKey) {
              markHeadCaptured?.();
              await headGate;
            }
            return object;
          },
        }),
      );
    });

    const retry = uploadSnapshot(session, snapshotId).then(async (response) => ({
      body: await response.json(),
      status: response.status,
    }));
    await within(headCaptured, "published retry HEAD did not pause");
    const concurrentId = "snapshot_refresh_concurrent_01";
    const concurrent = await uploadSnapshot(session, concurrentId);
    expect(concurrent.status).toBe(201);
    await concurrent.body?.cancel();
    const duringHead = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) =>
        durable.storage.sql
          .exec("SELECT state FROM snapshot WHERE snapshot_id = ?", snapshotId)
          .one().state,
    );
    expect(duringHead).toBe("servable");

    releaseHead?.();
    const retried = await retry;
    expect(retried.status).toBe(200);
    expect(retried.body).toMatchObject({ created: false, snapshot: { snapshotId } });
    pins.add(snapshotId);
    await runInDurableObject(sessionStub(session.sessionId), async (instance, durable) => {
      const coordinator = Reflect.get(instance, "snapshotUploads") as SnapshotUploadCoordinator;
      await settleCoordinator(coordinator);
      const sessionRow = new RelayStore(durable.storage.sql).session();
      expect(sessionRow).toMatchObject({
        latest_snapshot_id: concurrentId,
        recent_snapshot_id_1: snapshotId,
      });
      expect(
        durable.storage.sql
          .exec("SELECT state FROM snapshot WHERE snapshot_id = ?", snapshotId)
          .one().state,
      ).toBe("servable");
    });

    pins.delete(snapshotId);
    for (const id of ["snapshot_refresh_age_01", "snapshot_refresh_age_02"]) {
      const response = await uploadSnapshot(session, id);
      expect(response.status).toBe(201);
      await response.body?.cancel();
    }
    await runInDurableObject(sessionStub(session.sessionId), async (instance) => {
      await settleCoordinator(
        Reflect.get(instance, "snapshotUploads") as SnapshotUploadCoordinator,
      );
    });
    const retired = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) =>
        durable.storage.sql
          .exec("SELECT state FROM snapshot WHERE snapshot_id = ?", snapshotId)
          .toArray()[0],
    );
    expect(retired).toBeUndefined();
  });

  it("rejects a stale published HEAD after another coordinator retires its pointer", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_published_stale_01";
    const pins = new Set([snapshotId]);
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      Reflect.set(
        Reflect.get(instance, "snapshotUploads") as object,
        "pinnedSnapshotIds",
        () => pins,
      );
    });
    for (const id of [
      snapshotId,
      "snapshot_stale_seed_01",
      "snapshot_stale_seed_02",
      "snapshot_stale_seed_03",
    ]) {
      const response = await uploadSnapshot(session, id);
      expect(response.status).toBe(201);
      await response.body?.cancel();
    }
    const objectKey = await storedSnapshotKey(session.sessionId, snapshotId);

    let markHeadCaptured: (() => void) | undefined;
    let releaseHead: (() => void) | undefined;
    const headCaptured = new Promise<void>((resolve) => {
      markHeadCaptured = resolve;
    });
    const headGate = new Promise<void>((resolve) => {
      releaseHead = resolve;
    });
    const status = await runInDurableObject(
      sessionStub(session.sessionId),
      async (_instance, durable) => {
        const sql = durable.storage.sql;
        const staleBucket = bucketWithOverrides(env.SNAPSHOTS, {
          async head(key: string): Promise<R2Object | null> {
            const object = await env.SNAPSHOTS.head(key);
            if (key === objectKey) {
              markHeadCaptured?.();
              await headGate;
            }
            return object;
          },
        });
        const staleCoordinator = new SnapshotUploadCoordinator(
          durable,
          staleBucket,
          new SnapshotStore(durable, sql, new RelayStore(sql), 16),
          () => new Set(),
        );
        const replacementCoordinator = new SnapshotUploadCoordinator(
          durable,
          env.SNAPSHOTS,
          new SnapshotStore(durable, sql, new RelayStore(sql), 16),
          () => new Set(),
        );
        const metadata = await metadataFor(session, snapshotId);
        const staleResponse = staleCoordinator.upload(
          coordinatorUploadRequest(session, snapshotId, metadata.sha256),
          snapshotId,
          session.sessionId,
        );
        await within(headCaptured, "stale published HEAD did not pause");
        await replacementCoordinator.maintain();
        await settleCoordinator(replacementCoordinator);
        expect(
          sql.exec("SELECT state FROM snapshot WHERE snapshot_id = ?", snapshotId).toArray()[0],
        ).toBeUndefined();
        releaseHead?.();
        const response = await staleResponse;
        await response.body?.cancel();
        return response.status;
      },
    );
    expect(status).toBe(503);
  });

  it("keeps a replacement attempt safe from an old coordinator zombie delete", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_zombie_retired_01";
    const initial = await uploadSnapshot(session, snapshotId);
    expect(initial.status).toBe(201);
    await initial.body?.cancel();
    const oldKey = await storedSnapshotKey(session.sessionId, snapshotId);
    const metadata = await metadataFor(session, snapshotId);

    let markDeleteStarted: (() => void) | undefined;
    let releaseDelete: (() => void) | undefined;
    const deleteStarted = new Promise<void>((resolve) => {
      markDeleteStarted = resolve;
    });
    const deleteGate = new Promise<void>((resolve) => {
      releaseDelete = resolve;
    });

    const result = await runInDurableObject(
      sessionStub(session.sessionId),
      async (_instance, durable) => {
        const sql = durable.storage.sql;
        sql.exec("UPDATE snapshot SET state = 'retired' WHERE snapshot_id = ?", snapshotId);
        sql.exec(
          `UPDATE session_state
           SET latest_snapshot_id = NULL, recent_snapshot_id_1 = NULL,
               recent_snapshot_id_2 = NULL
           WHERE singleton = 1`,
        );
        const oldBucket = bucketWithOverrides(env.SNAPSHOTS, {
          async delete(key: string | string[]): Promise<void> {
            if (String(key) === oldKey) {
              markDeleteStarted?.();
              await deleteGate;
            }
            await env.SNAPSHOTS.delete(key);
          },
        });
        const oldCoordinator = new SnapshotUploadCoordinator(
          durable,
          oldBucket,
          new SnapshotStore(durable, sql, new RelayStore(sql), 16),
          () => new Set(),
        );
        const replacementCoordinator = new SnapshotUploadCoordinator(
          durable,
          env.SNAPSHOTS,
          new SnapshotStore(durable, sql, new RelayStore(sql), 16),
          () => new Set(),
        );
        installMiniflareMultipartEtagShimOnCoordinator(replacementCoordinator);

        await oldCoordinator.maintain();
        await within(deleteStarted, "old snapshot delete did not start");
        await replacementCoordinator.maintain();
        await settleCoordinator(replacementCoordinator);
        expect(
          sql.exec("SELECT 1 FROM snapshot WHERE snapshot_id = ?", snapshotId).toArray(),
        ).toHaveLength(0);

        const replacement = await replacementCoordinator.upload(
          coordinatorUploadRequest(session, snapshotId, metadata.sha256),
          snapshotId,
          session.sessionId,
        );
        expect(replacement.status).toBe(201);
        await replacement.body?.cancel();
        const newKey = sql
          .exec("SELECT object_key FROM snapshot WHERE snapshot_id = ?", snapshotId)
          .one().object_key as string;

        releaseDelete?.();
        await settleCoordinator(oldCoordinator);
        return {
          newKey,
          published: sql
            .exec("SELECT object_key, state FROM snapshot WHERE snapshot_id = ?", snapshotId)
            .one(),
        };
      },
    );

    expect(result.newKey).not.toBe(oldKey);
    expect(result.published).toEqual({ object_key: result.newKey, state: "servable" });
    expect(await env.SNAPSHOTS.head(result.newKey)).not.toBeNull();
  });

  it("rotates the attempt key before restarting an aborted multipart upload", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_zombie_abort_001";
    const metadata = await metadataFor(session, snapshotId);
    const oldKey = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) => {
        const snapshots = new SnapshotStore(
          durable,
          durable.storage.sql,
          new RelayStore(durable.storage.sql),
          16,
        );
        const reserved = snapshots.beginUpload(metadata, Date.now());
        if (!reserved.ok || reserved.state !== "started") throw new Error("reservation failed");
        return reserved.upload.object_key;
      },
    );
    const multipart = await env.SNAPSHOTS.createMultipartUpload(oldKey);
    await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      const snapshots = new SnapshotStore(
        durable,
        durable.storage.sql,
        new RelayStore(durable.storage.sql),
        16,
      );
      expect(snapshots.recordMultipartUpload(snapshotId, oldKey, multipart.uploadId)).toBe(true);
    });

    let markOldHeadStarted: (() => void) | undefined;
    let releaseOldHead: (() => void) | undefined;
    const oldHeadStarted = new Promise<void>((resolve) => {
      markOldHeadStarted = resolve;
    });
    const oldHeadGate = new Promise<void>((resolve) => {
      releaseOldHead = resolve;
    });

    const newKey = await runInDurableObject(
      sessionStub(session.sessionId),
      async (_instance, durable) => {
        const sql = durable.storage.sql;
        const oldBucket = bucketWithOverrides(env.SNAPSHOTS, {
          async head(key: string): Promise<R2Object | null> {
            if (key === oldKey) {
              markOldHeadStarted?.();
              await oldHeadGate;
            }
            return env.SNAPSHOTS.head(key);
          },
        });
        const oldCoordinator = new SnapshotUploadCoordinator(
          durable,
          oldBucket,
          new SnapshotStore(durable, sql, new RelayStore(sql), 16),
          () => new Set(),
        );
        const replacementCoordinator = new SnapshotUploadCoordinator(
          durable,
          env.SNAPSHOTS,
          new SnapshotStore(durable, sql, new RelayStore(sql), 16),
          () => new Set(),
        );
        installMiniflareMultipartEtagShimOnCoordinator(replacementCoordinator);

        await oldCoordinator.maintain();
        await within(oldHeadStarted, "old aborted cleanup HEAD did not start");
        const replacement = await replacementCoordinator.upload(
          coordinatorUploadRequest(session, snapshotId, metadata.sha256),
          snapshotId,
          session.sessionId,
        );
        expect(replacement.status).toBe(201);
        await replacement.body?.cancel();
        const key = sql
          .exec("SELECT object_key FROM snapshot WHERE snapshot_id = ?", snapshotId)
          .one().object_key as string;

        releaseOldHead?.();
        await settleCoordinator(oldCoordinator);
        return key;
      },
    );

    expect(newKey).not.toBe(oldKey);
    expect(await env.SNAPSHOTS.head(newKey)).not.toBeNull();
  });

  it("rejects a late completed result from an obsolete attempt", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_late_completed_01";
    const metadata = await metadataFor(session, snapshotId);
    const state = await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      const sql = durable.storage.sql;
      const snapshots = new SnapshotStore(durable, sql, new RelayStore(sql), 16);
      const first = snapshots.beginUpload(metadata, 1);
      if (!first.ok || first.state !== "started") throw new Error("first reservation failed");
      const firstKey = first.upload.object_key;
      expect(snapshots.recordMultipartUpload(snapshotId, firstKey, "upload-first")).toBe(true);
      expect(
        snapshots.recordMultipartCompleting(snapshotId, firstKey, "upload-first", "part-first"),
      ).toBe(true);

      sql.exec("DELETE FROM snapshot_upload WHERE snapshot_id = ?", snapshotId);
      const second = snapshots.beginUpload(metadata, 2);
      if (!second.ok || second.state !== "started") throw new Error("second reservation failed");
      const secondKey = second.upload.object_key;
      expect(snapshots.recordMultipartUpload(snapshotId, secondKey, "upload-second")).toBe(true);
      expect(
        snapshots.recordMultipartCompleting(snapshotId, secondKey, "upload-second", "part-second"),
      ).toBe(true);
      const secondCompleted = snapshots.recordMultipartCompleted(
        snapshotId,
        secondKey,
        "upload-second",
        "version-second",
        "etag-second",
      );
      const late = snapshots.recordMultipartCompleted(
        snapshotId,
        firstKey,
        "upload-first",
        "version-first",
        "etag-first",
      );
      return {
        firstKey,
        late,
        second: snapshots.completedUpload(snapshotId),
        secondCompleted,
        secondKey,
      };
    });

    expect(state.firstKey).not.toBe(state.secondKey);
    expect(state.late).toBeUndefined();
    expect(state.secondCompleted).toMatchObject({ objectKey: state.secondKey });
    expect(state.second).toMatchObject({
      objectKey: state.secondKey,
      r2Version: "version-second",
    });
  });

  it("does not finalize a replacement attempt after an old completed HEAD", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_completed_stale_01";
    const initial = await uploadSnapshot(session, snapshotId);
    expect(initial.status).toBe(201);
    await initial.body?.cancel();
    const metadata = await metadataFor(session, snapshotId);
    const oldKey = await storedSnapshotKey(session.sessionId, snapshotId);
    let markHeadCaptured: (() => void) | undefined;
    let releaseHead: (() => void) | undefined;
    const headCaptured = new Promise<void>((resolve) => {
      markHeadCaptured = resolve;
    });
    const headGate = new Promise<void>((resolve) => {
      releaseHead = resolve;
    });

    const result = await runInDurableObject(
      sessionStub(session.sessionId),
      async (instance, durable) => {
        const sql = durable.storage.sql;
        const published = sql
          .exec("SELECT * FROM snapshot WHERE snapshot_id = ?", snapshotId)
          .one();
        sql.exec("DELETE FROM snapshot WHERE snapshot_id = ?", snapshotId);
        sql.exec(
          `INSERT INTO snapshot_upload
            (snapshot_id, object_key, metadata_json, state, upload_id, part_etag,
             r2_version, etag, created_at, expires_at)
           VALUES (?, ?, ?, 'completed', 'upload-old', 'part-old', ?, ?, ?, ?)`,
          snapshotId,
          oldKey,
          JSON.stringify(SnapshotMetadataSchema.parse(metadata)),
          published.r2_version,
          published.etag,
          Date.now(),
          Date.now() + 60_000,
        );
        const liveCoordinator = Reflect.get(instance, "snapshotUploads") as object;
        const base = Reflect.get(liveCoordinator, "snapshotBucket") as R2Bucket;
        const staleBucket = bucketWithOverrides(base, {
          async head(key: string): Promise<R2Object | null> {
            const object = await base.head(key);
            if (key === oldKey) {
              markHeadCaptured?.();
              await headGate;
            }
            return object;
          },
        });
        const staleCoordinator = new SnapshotUploadCoordinator(
          durable,
          staleBucket,
          new SnapshotStore(durable, sql, new RelayStore(sql), 16),
          () => new Set(),
        );
        Reflect.set(staleCoordinator, "scheduleMaintenance", () => undefined);
        const staleResponse = staleCoordinator.upload(
          coordinatorUploadRequest(session, snapshotId, metadata.sha256),
          snapshotId,
          session.sessionId,
        );
        const progress = await within(
          Promise.race([
            headCaptured.then(() => "head"),
            staleResponse.then((response) => `response:${response.status}`),
          ]),
          "completed retry did not make progress",
        );
        expect(progress).toBe("head");

        await base.delete(oldKey);
        sql.exec("DELETE FROM snapshot_upload WHERE snapshot_id = ?", snapshotId);
        const replacementStore = new SnapshotStore(durable, sql, new RelayStore(sql), 16);
        const replacement = replacementStore.beginUpload(metadata, Date.now());
        if (!replacement.ok || replacement.state !== "started") {
          throw new Error("replacement reservation failed");
        }
        const replacementKey = replacement.upload.object_key;
        expect(
          replacementStore.recordMultipartUpload(snapshotId, replacementKey, "upload-new"),
        ).toBe(true);
        expect(
          replacementStore.recordMultipartCompleting(
            snapshotId,
            replacementKey,
            "upload-new",
            "part-new",
          ),
        ).toBe(true);
        expect(
          replacementStore.recordMultipartCompleted(
            snapshotId,
            replacementKey,
            "upload-new",
            "version-new",
            "etag-new",
          ),
        ).toBeDefined();
        releaseHead?.();

        const response = await staleResponse;
        await response.body?.cancel();
        return {
          replacement: sql
            .exec("SELECT object_key, state FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
            .one(),
          snapshotRows: sql
            .exec("SELECT 1 FROM snapshot WHERE snapshot_id = ?", snapshotId)
            .toArray().length,
          status: response.status,
        };
      },
    );

    expect(result).toMatchObject({
      replacement: { state: "completed" },
      snapshotRows: 0,
      status: 503,
    });
    expect(result.replacement.object_key).not.toBe(oldKey);
  });

  it("does not report checksum cleanup after another coordinator rotates the attempt", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_checksum_cleanup_race_01";
    let markCleanupHead: (() => void) | undefined;
    let releaseCleanupHead: (() => void) | undefined;
    const cleanupHead = new Promise<void>((resolve) => {
      markCleanupHead = resolve;
    });
    const cleanupGate = new Promise<void>((resolve) => {
      releaseCleanupHead = resolve;
    });

    const result = await runInDurableObject(
      sessionStub(session.sessionId),
      async (instance, durable) => {
        const sql = durable.storage.sql;
        const liveCoordinator = Reflect.get(instance, "snapshotUploads") as object;
        const base = Reflect.get(liveCoordinator, "snapshotBucket") as R2Bucket;
        let oldKey: string | undefined;
        const staleBucket = bucketWithOverrides(base, {
          async head(key: string): Promise<R2Object | null> {
            const upload = sql
              .exec("SELECT state FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
              .toArray()[0] as { state: string } | undefined;
            if (upload?.state === "aborted") {
              oldKey = key;
              markCleanupHead?.();
              await cleanupGate;
            }
            return base.head(key);
          },
        });
        const staleCoordinator = new SnapshotUploadCoordinator(
          durable,
          staleBucket,
          new SnapshotStore(durable, sql, new RelayStore(sql), 16),
          () => new Set(),
        );
        const staleResponse = staleCoordinator.upload(
          coordinatorUploadRequest(session, snapshotId, "0".repeat(64)),
          snapshotId,
          session.sessionId,
        );
        await within(cleanupHead, "checksum cleanup HEAD did not pause");

        const row = sql
          .exec("SELECT * FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
          .one() as unknown as {
          object_key: string;
          upload_id: string;
        };
        const replacement = new SnapshotStore(durable, sql, new RelayStore(sql), 16);
        const restarted = replacement.restartAfterMultipartAbort(
          snapshotId,
          row.object_key,
          row.upload_id,
        );
        expect(restarted).toBeDefined();
        releaseCleanupHead?.();

        const response = await staleResponse;
        await response.body?.cancel();
        return {
          oldKey,
          replacement: sql
            .exec("SELECT object_key, state FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
            .one(),
          status: response.status,
        };
      },
    );

    expect(result.status).toBe(503);
    expect(result.replacement.state).toBe("preparing");
    expect(result.replacement.object_key).not.toBe(result.oldKey);
  });
});
