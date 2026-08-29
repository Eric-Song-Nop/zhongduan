import { MAX_SNAPSHOT_COMPRESSED_BYTES, SNAPSHOT_MEDIA_TYPE } from "@zhongduan/protocol";
import { env } from "cloudflare:workers";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { snapshotCustomMetadata } from "../src/worker/snapshot-contract";
import { RelayStore } from "../src/worker/relay-store";
import { SnapshotStore } from "../src/worker/snapshot-store";
import {
  bucketWithOverrides,
  createSession,
  forceNextSessionAlarmDue,
  metadataFor,
  matchesSnapshotKey,
  overrideSnapshotBucket,
  sessionStub,
  snapshotHeaders,
  storedSnapshotKey,
  uploadSnapshot,
  within,
  type UploadOverrides,
} from "./snapshot-test-helpers";

describe("snapshot upload deadlines", () => {
  it.each(["throw", "reject"] as const)(
    "settles %s multipart failure without buffering an unread R2 part stream",
    async (mode) => {
      const session = await createSession();
      const snapshotId = `snapshot_part_${mode}_failure`;
      let objectKey: string | undefined;
      let producedBytes = 0;
      await overrideSnapshotBucket(session, (base) =>
        bucketWithOverrides(base, {
          async createMultipartUpload(
            key: string,
            options?: R2MultipartOptions,
          ): Promise<R2MultipartUpload> {
            const multipart = await base.createMultipartUpload(key, options);
            if (!matchesSnapshotKey(key, session.sessionId, snapshotId)) return multipart;
            objectKey = key;
            return {
              key: multipart.key,
              uploadId: multipart.uploadId,
              uploadPart(): Promise<R2UploadedPart> {
                if (mode === "throw") throw new Error("injected synchronous part failure");
                return Promise.reject(new Error("injected rejected part promise"));
              },
              abort: multipart.abort.bind(multipart),
              complete: multipart.complete.bind(multipart),
            };
          },
        }),
      );

      const declaredLength = MAX_SNAPSHOT_COMPRESSED_BYTES;
      const chunk = new Uint8Array(64 * 1024);
      const body = new ReadableStream<Uint8Array>({
        pull(controller) {
          if (producedBytes >= declaredLength) {
            controller.close();
            return;
          }
          const length = Math.min(chunk.byteLength, declaredLength - producedBytes);
          producedBytes += length;
          controller.enqueue(chunk.subarray(0, length));
        },
      });
      const headers = snapshotHeaders(session, chunk, {
        compressedLength: declaredLength.toString(),
        sha256: "0".repeat(64),
        uncompressedLength: declaredLength.toString(),
      });
      headers.set("content-length", declaredLength.toString());
      const response = await sessionStub(session.sessionId).fetch(
        new Request(`https://do.internal/internal/snapshots/upload/${snapshotId}`, {
          method: "POST",
          headers,
          body,
        }),
      );
      expect(response.status).toBe(503);
      expect(await response.json()).toEqual({ error: "snapshot-upload-failed" });
      expect(producedBytes).toBeLessThan(declaredLength);
      expect(objectKey).toBeDefined();
      expect(await env.SNAPSHOTS.head(objectKey!)).toBeNull();

      const followup = await uploadSnapshot(session, `snapshot_after_${mode}_fail`);
      expect(followup.status).toBe(201);
      await followup.body?.cancel();
    },
  );

  it("releases the body permit while part upload and maintenance abort are pending", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_part_abort_pending";
    let objectKey: string | undefined;
    let targetUploadId: string | undefined;
    let markAbortStarted: (() => void) | undefined;
    let releaseAbort: (() => void) | undefined;
    const abortStarted = new Promise<void>((resolve) => {
      markAbortStarted = resolve;
    });
    const abortGate = new Promise<void>((resolve) => {
      releaseAbort = resolve;
    });
    await overrideSnapshotBucket(session, (base) =>
      bucketWithOverrides(base, {
        async createMultipartUpload(
          key: string,
          options?: R2MultipartOptions,
        ): Promise<R2MultipartUpload> {
          const multipart = await base.createMultipartUpload(key, options);
          if (!matchesSnapshotKey(key, session.sessionId, snapshotId)) return multipart;
          objectKey = key;
          targetUploadId = multipart.uploadId;
          return {
            key: multipart.key,
            uploadId: multipart.uploadId,
            uploadPart(): Promise<R2UploadedPart> {
              return new Promise<R2UploadedPart>(() => undefined);
            },
            abort: multipart.abort.bind(multipart),
            complete: multipart.complete.bind(multipart),
          };
        },
        resumeMultipartUpload(key: string, uploadId: string): R2MultipartUpload {
          const multipart = base.resumeMultipartUpload(key, uploadId);
          if (key !== objectKey || uploadId !== targetUploadId) return multipart;
          return {
            key: multipart.key,
            uploadId: multipart.uploadId,
            uploadPart: multipart.uploadPart.bind(multipart),
            complete: multipart.complete.bind(multipart),
            async abort(): Promise<void> {
              markAbortStarted?.();
              await abortGate;
              throw new Error("injected pending abort failure");
            },
          };
        },
      }),
    );
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const coordinator = Reflect.get(instance, "snapshotUploads") as object;
      Reflect.set(coordinator, "snapshotPartUploadTimeoutMs", 25);
    });

    const declaredLength = MAX_SNAPSHOT_COMPRESSED_BYTES;
    const chunk = new Uint8Array(64 * 1024);
    let producedBytes = 0;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (producedBytes >= declaredLength) {
          controller.close();
          return;
        }
        const length = Math.min(chunk.byteLength, declaredLength - producedBytes);
        producedBytes += length;
        controller.enqueue(chunk.subarray(0, length));
      },
    });
    const headers = snapshotHeaders(session, chunk, {
      compressedLength: declaredLength.toString(),
      sha256: "0".repeat(64),
      uncompressedLength: declaredLength.toString(),
    });
    headers.set("content-length", declaredLength.toString());
    const failedRequest = sessionStub(session.sessionId)
      .fetch(
        new Request(`https://do.internal/internal/snapshots/upload/${snapshotId}`, {
          method: "POST",
          headers,
          body,
        }),
      )
      .then(async (response) => ({ body: await response.json(), status: response.status }));
    await within(abortStarted, "maintenance abort did not start");
    expect(producedBytes).toBeLessThan(declaredLength);

    const fenced = await within(
      runInDurableObject(sessionStub(session.sessionId), async (_instance, durable) => ({
        alarm: await durable.storage.getAlarm(),
        upload: durable.storage.sql
          .exec("SELECT state FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
          .one(),
      })),
      "durable ledger inspection was blocked",
    );
    expect(fenced.upload).toEqual({ state: "uploading" });
    expect(fenced.alarm).not.toBeNull();

    const sameId = await within(
      uploadSnapshot(session, snapshotId),
      "same-id request was blocked by maintenance",
    );
    expect(sameId.status).toBe(503);
    expect(await sameId.json()).toEqual({ error: "snapshot-upload-in-progress" });
    const otherId = "snapshot_after_abort_wait";
    const other = await within(
      uploadSnapshot(session, otherId),
      "different-id upload was blocked by maintenance",
    );
    expect(other.status).toBe(201);
    await other.body?.cancel();
    expect(
      await env.SNAPSHOTS.head(await storedSnapshotKey(session.sessionId, otherId)),
    ).not.toBeNull();

    releaseAbort?.();
    const failed = await within(failedRequest, "failed request did not settle after abort release");
    expect(failed.status).toBe(503);
    expect(failed.body).toEqual({ error: "snapshot-upload-failed" });
  });

  it("keeps a checksum failure fenced when its read-only cleanup HEAD is pending", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_checksum_head_pending";
    let objectKey: string | undefined;
    let targetHeadCalls = 0;
    await overrideSnapshotBucket(session, (base) =>
      bucketWithOverrides(base, {
        head(key: string): Promise<R2Object | null> {
          if (!matchesSnapshotKey(key, session.sessionId, snapshotId)) return base.head(key);
          objectKey = key;
          targetHeadCalls += 1;
          return targetHeadCalls === 1
            ? base.head(key)
            : new Promise<R2Object | null>(() => undefined);
        },
      }),
    );
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const coordinator = Reflect.get(instance, "snapshotUploads") as object;
      Reflect.set(coordinator, "snapshotR2OperationTimeoutMs", 25);
      Reflect.set(coordinator, "scheduleMaintenance", () => undefined);
    });

    const response = await within(
      uploadSnapshot(session, snapshotId, session.hostCapability, { sha256: "0".repeat(64) }),
      "checksum cleanup HEAD did not time out",
    );
    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({ error: "snapshot-upload-failed" });
    expect(targetHeadCalls).toBe(2);
    const fenced = await runInDurableObject(
      sessionStub(session.sessionId),
      async (_instance, durable) => ({
        alarm: await durable.storage.getAlarm(),
        upload: durable.storage.sql
          .exec("SELECT state FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
          .one(),
      }),
    );
    expect(fenced.upload).toEqual({ state: "aborted" });
    expect(fenced.alarm).not.toBeNull();
    expect(objectKey).toBeDefined();
    expect(await env.SNAPSHOTS.head(objectKey!)).toBeNull();

    const other = await uploadSnapshot(session, "snapshot_after_checksum_head");
    expect(other.status).toBe(201);
    await other.body?.cancel();
  });

  it("bounds same-id recovery abort and ignores its late old-upload result", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_recovery_abort_pending";
    const metadata = await metadataFor(session, snapshotId);
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
      customMetadata: snapshotCustomMetadata(metadata, "multipart-verified"),
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

    let releaseLateAbort: (() => void) | undefined;
    let markLateAbortSettled: (() => void) | undefined;
    const lateAbortGate = new Promise<void>((resolve) => {
      releaseLateAbort = resolve;
    });
    const lateAbortSettled = new Promise<void>((resolve) => {
      markLateAbortSettled = resolve;
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
              await lateAbortGate;
              try {
                await resumed.abort();
              } finally {
                markLateAbortSettled?.();
              }
            },
          };
        },
      }),
    );
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const coordinator = Reflect.get(instance, "snapshotUploads") as object;
      Reflect.set(coordinator, "snapshotR2OperationTimeoutMs", 25);
      Reflect.set(coordinator, "scheduleMaintenance", () => undefined);
    });

    const timedOut = await within(
      uploadSnapshot(session, snapshotId),
      "same-id recovery abort did not time out",
    );
    expect(timedOut.status).toBe(503);
    expect(await timedOut.json()).toEqual({ error: "snapshot-upload-failed" });
    const fenced = await runInDurableObject(
      sessionStub(session.sessionId),
      async (_instance, durable) => ({
        alarm: await durable.storage.getAlarm(),
        upload: durable.storage.sql
          .exec("SELECT state FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
          .one(),
      }),
    );
    expect(fenced.upload).toEqual({ state: "uploading" });
    expect(fenced.alarm).not.toBeNull();

    const other = await uploadSnapshot(session, "snapshot_during_recovery_abort");
    expect(other.status).toBe(201);
    await other.body?.cancel();

    await env.SNAPSHOTS.resumeMultipartUpload(objectKey, multipart.uploadId).abort();
    await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      const snapshots = new SnapshotStore(
        durable,
        durable.storage.sql,
        new RelayStore(durable.storage.sql),
        16,
      );
      expect(snapshots.recordMultipartAborted(snapshotId, objectKey, multipart.uploadId)).toBe(
        true,
      );
      snapshots.deleteAbortedMultipart(snapshotId, objectKey, multipart.uploadId);
    });
    const replacement = await uploadSnapshot(session, snapshotId);
    expect(replacement.status).toBe(201);
    await replacement.body?.cancel();
    const replacementKey = await storedSnapshotKey(session.sessionId, snapshotId);
    expect(replacementKey).not.toBe(objectKey);
    const replacementObject = await env.SNAPSHOTS.head(replacementKey);
    expect(replacementObject).not.toBeNull();

    releaseLateAbort?.();
    await within(lateAbortSettled, "late old-upload abort did not settle");
    expect(await env.SNAPSHOTS.head(replacementKey)).toMatchObject({
      version: replacementObject?.version,
    });
  });

  it.each(["head", "create"] as const)(
    "does not spend the body permit while initial multipart %s is pending",
    async (mode) => {
      const session = await createSession();
      const snapshotId = `snapshot_${mode}_pending`;
      let markStarted: (() => void) | undefined;
      let releaseOperation: (() => void) | undefined;
      const operationStarted = new Promise<void>((resolve) => {
        markStarted = resolve;
      });
      const operationGate = new Promise<void>((resolve) => {
        releaseOperation = resolve;
      });
      await overrideSnapshotBucket(session, (base) =>
        bucketWithOverrides(base, {
          async head(key: string): Promise<R2Object | null> {
            if (mode === "head" && matchesSnapshotKey(key, session.sessionId, snapshotId)) {
              markStarted?.();
              await operationGate;
            }
            return base.head(key);
          },
          async createMultipartUpload(
            key: string,
            options?: R2MultipartOptions,
          ): Promise<R2MultipartUpload> {
            if (mode === "create" && matchesSnapshotKey(key, session.sessionId, snapshotId)) {
              markStarted?.();
              await operationGate;
            }
            return base.createMultipartUpload(key, options);
          },
        }),
      );

      const pending = uploadSnapshot(session, snapshotId).then(async (response) => ({
        body: await response.json(),
        status: response.status,
      }));
      await operationStarted;
      const otherId = `snapshot_during_${mode}_pending`;
      const other = await uploadSnapshot(session, otherId);
      expect(other.status).toBe(201);
      await other.body?.cancel();
      expect(
        await env.SNAPSHOTS.head(await storedSnapshotKey(session.sessionId, otherId)),
      ).not.toBeNull();

      releaseOperation?.();
      const published = await pending;
      expect(published.status).toBe(201);
      expect(published.body).toMatchObject({ created: true, snapshot: { snapshotId } });
    },
  );

  it.each(["head", "create", "complete"] as const)(
    "bounds a pending public multipart %s and preserves its durable stage",
    async (mode) => {
      const session = await createSession();
      const snapshotId = `snapshot_${mode}_deadline`;
      let objectKey: string | undefined;
      let pendingCalls = 0;
      await overrideSnapshotBucket(session, (base) =>
        bucketWithOverrides(base, {
          head(key: string): Promise<R2Object | null> {
            if (mode === "head" && matchesSnapshotKey(key, session.sessionId, snapshotId)) {
              objectKey = key;
              pendingCalls += 1;
              return new Promise<R2Object | null>(() => undefined);
            }
            return base.head(key);
          },
          async createMultipartUpload(
            key: string,
            options?: R2MultipartOptions,
          ): Promise<R2MultipartUpload> {
            if (mode === "create" && matchesSnapshotKey(key, session.sessionId, snapshotId)) {
              objectKey = key;
              pendingCalls += 1;
              return new Promise<R2MultipartUpload>(() => undefined);
            }
            const multipart = await base.createMultipartUpload(key, options);
            if (mode !== "complete" || !matchesSnapshotKey(key, session.sessionId, snapshotId)) {
              return multipart;
            }
            objectKey = key;
            return {
              key: multipart.key,
              uploadId: multipart.uploadId,
              uploadPart: multipart.uploadPart.bind(multipart),
              abort: multipart.abort.bind(multipart),
              complete(): Promise<R2Object> {
                pendingCalls += 1;
                return new Promise<R2Object>(() => undefined);
              },
            };
          },
        }),
      );
      await runInDurableObject(sessionStub(session.sessionId), (instance) => {
        const coordinator = Reflect.get(instance, "snapshotUploads") as object;
        Reflect.set(coordinator, "snapshotR2OperationTimeoutMs", 25);
        Reflect.set(coordinator, "scheduleMaintenance", () => undefined);
      });

      const response = await within(
        uploadSnapshot(session, snapshotId),
        `${mode} deadline did not bound the public request`,
      );
      expect(response.status).toBe(503);
      expect(await response.json()).toEqual({ error: "snapshot-upload-failed" });
      expect(pendingCalls).toBe(1);
      const upload = await runInDurableObject(
        sessionStub(session.sessionId),
        (_instance, durable) =>
          durable.storage.sql
            .exec("SELECT state FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
            .toArray()[0],
      );
      expect(upload).toEqual(
        mode === "head" ? undefined : { state: mode === "create" ? "preparing" : "completing" },
      );
      expect(objectKey).toBeDefined();
      expect(await env.SNAPSHOTS.head(objectKey!)).toBeNull();

      const otherId = `snapshot_after_${mode}_deadline`;
      const other = await uploadSnapshot(session, otherId);
      expect(other.status).toBe(201);
      await other.body?.cancel();
    },
  );

  it.each(["published", "completed", "uncertain"] as const)(
    "bounds a pending %s snapshot verification HEAD",
    async (state) => {
      const session = await createSession();
      const snapshotId = `snapshot_${state}_head_deadline`;
      let objectKey: string;
      const overrides: UploadOverrides = state === "completed" ? { cutEventSeq: "1" } : {};
      if (state === "published") {
        const published = await uploadSnapshot(session, snapshotId);
        expect(published.status).toBe(201);
        await published.body?.cancel();
        objectKey = await storedSnapshotKey(session.sessionId, snapshotId);
      } else if (state === "completed") {
        const completed = await uploadSnapshot(
          session,
          snapshotId,
          session.hostCapability,
          overrides,
        );
        expect(completed.status).toBe(409);
        await completed.body?.cancel();
        objectKey = await storedSnapshotKey(session.sessionId, snapshotId);
      } else {
        const metadata = await metadataFor(session, snapshotId);
        await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
          const snapshots = new SnapshotStore(
            durable,
            durable.storage.sql,
            new RelayStore(durable.storage.sql),
            16,
          );
          const reserved = snapshots.beginUpload(metadata, Date.now());
          expect(reserved).toMatchObject({ ok: true, state: "started" });
          if (!reserved.ok || reserved.state !== "started") {
            throw new Error("reservation failed");
          }
          objectKey = reserved.upload.object_key;
          expect(snapshots.recordMultipartUpload(snapshotId, objectKey, "upload-uncertain")).toBe(
            true,
          );
          expect(
            snapshots.recordMultipartUncertain(snapshotId, objectKey, "upload-uncertain"),
          ).toBe(true);
        });
      }

      let headCalls = 0;
      await overrideSnapshotBucket(session, (base) =>
        bucketWithOverrides(base, {
          head(key: string): Promise<R2Object | null> {
            if (key !== objectKey) return base.head(key);
            headCalls += 1;
            return new Promise<R2Object | null>(() => undefined);
          },
        }),
      );
      await runInDurableObject(sessionStub(session.sessionId), (instance) => {
        const coordinator = Reflect.get(instance, "snapshotUploads") as object;
        Reflect.set(coordinator, "snapshotR2OperationTimeoutMs", 25);
        Reflect.set(coordinator, "scheduleMaintenance", () => undefined);
      });

      const response = await within(
        uploadSnapshot(session, snapshotId, session.hostCapability, overrides),
        `${state} verification HEAD did not time out`,
      );
      expect(response.status).toBe(503);
      expect(await response.json()).toEqual({
        error: state === "uncertain" ? "snapshot-upload-failed" : "snapshot-unavailable",
      });
      expect(headCalls).toBe(1);

      const other = await uploadSnapshot(session, `snapshot_after_${state}_head`);
      expect(other.status).toBe(201);
      await other.body?.cancel();
    },
  );

  it("keeps the watchdog armed while an owned complete is pending and rejects same-id overlap", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_active_watchdog";
    let objectKey: string | undefined;
    let markCompleteStarted: (() => void) | undefined;
    let releaseComplete: (() => void) | undefined;
    let createCalls = 0;
    let partCalls = 0;
    const completeStarted = new Promise<void>((resolve) => {
      markCompleteStarted = resolve;
    });
    const completeGate = new Promise<void>((resolve) => {
      releaseComplete = resolve;
    });
    await overrideSnapshotBucket(session, (base) =>
      bucketWithOverrides(base, {
        async createMultipartUpload(
          key: string,
          options?: R2MultipartOptions,
        ): Promise<R2MultipartUpload> {
          createCalls += 1;
          const multipart = await base.createMultipartUpload(key, options);
          if (!matchesSnapshotKey(key, session.sessionId, snapshotId)) return multipart;
          objectKey = key;
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
            async complete(parts: R2UploadedPart[]): Promise<R2Object> {
              markCompleteStarted?.();
              await completeGate;
              return multipart.complete(parts);
            },
          };
        },
      }),
    );

    const pending = uploadSnapshot(session, snapshotId).then(async (response) => ({
      body: await response.json(),
      status: response.status,
    }));
    await completeStarted;
    await runInDurableObject(sessionStub(session.sessionId), async (_instance, durable) => {
      durable.storage.sql.exec(
        "UPDATE snapshot_upload SET expires_at = 0 WHERE snapshot_id = ?",
        snapshotId,
      );
      await durable.storage.setAlarm(Date.now() + 60_000);
    });
    const alarmStartedAt = Date.now();
    await forceNextSessionAlarmDue(session.sessionId);
    expect(await runDurableObjectAlarm(sessionStub(session.sessionId))).toBe(true);
    const watchdog = await runInDurableObject(
      sessionStub(session.sessionId),
      async (_instance, durable) => ({
        alarm: await durable.storage.getAlarm(),
        state: durable.storage.sql
          .exec("SELECT state FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
          .one().state,
      }),
    );
    expect(watchdog.state).toBe("completing");
    expect(watchdog.alarm).not.toBeNull();
    expect(watchdog.alarm).toBeGreaterThanOrEqual(alarmStartedAt + 25_000);
    expect(watchdog.alarm).toBeLessThan(Date.now() + 60_000);
    expect(objectKey).toBeDefined();
    expect(await env.SNAPSHOTS.head(objectKey!)).toBeNull();

    const overlap = await uploadSnapshot(session, snapshotId);
    expect(overlap.status).toBe(503);
    expect(await overlap.json()).toEqual({ error: "snapshot-upload-in-progress" });
    expect({ createCalls, partCalls }).toEqual({ createCalls: 1, partCalls: 1 });

    const otherId = "snapshot_while_complete_pending";
    const other = await uploadSnapshot(session, otherId);
    expect(other.status).toBe(201);
    await other.body?.cancel();
    expect(
      await env.SNAPSHOTS.head(await storedSnapshotKey(session.sessionId, otherId)),
    ).not.toBeNull();
    expect({ createCalls, partCalls }).toEqual({ createCalls: 2, partCalls: 1 });

    releaseComplete?.();
    const published = await pending;
    expect(published).toMatchObject({
      status: 201,
      body: { created: true, snapshot: { snapshotId } },
    });
    expect(await env.SNAPSHOTS.head(objectKey!)).not.toBeNull();
  });
});
