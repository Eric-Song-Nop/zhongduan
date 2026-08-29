import { SNAPSHOT_MEDIA_TYPE, type SnapshotMetadata } from "@zhongduan/protocol";
import {
  bytesToHex,
  matchesMultipartPartEtag,
  matchesSnapshotObject,
  parseSnapshotUploadMetadata,
  snapshotCustomMetadata,
  type FinalizedSnapshot,
} from "./snapshot-contract";
import { deleteRetiredSnapshotObjects } from "./snapshot-gc";
import {
  SnapshotStore,
  SNAPSHOT_MAINTENANCE_BATCH_LIMIT,
  SNAPSHOT_UPLOAD_RESERVATION_LIMIT,
  SNAPSHOT_UPLOAD_RESERVATION_MS,
  type RecoverableMultipartUpload,
  type RetiredSnapshotObject,
  type SnapshotUploadRecord,
} from "./snapshot-store";

interface SnapshotOperationOwner {
  readonly snapshotId: string;
  readonly source: "maintenance" | "upload";
}

type PreparedMultipartBody =
  | { readonly kind: "checksum-mismatch"; readonly multipart: R2MultipartUpload }
  | {
      readonly kind: "ready";
      readonly multipart: R2MultipartUpload;
      readonly part: R2UploadedPart;
    }
  | { readonly kind: "response"; readonly response: Response };

type R2DeadlineResult<T> =
  | { readonly kind: "fulfilled"; readonly value: T }
  | { readonly error: unknown; readonly kind: "rejected" }
  | { readonly kind: "timeout" };

type MultipartRecoveryResult =
  | { readonly kind: "response"; readonly response: Response }
  | { readonly kind: "restart"; readonly upload: SnapshotUploadRecord };

const SNAPSHOT_GC_RETRY_MS = 30_000;

function json(data: unknown, status = 200): Response {
  return Response.json(data, {
    status,
    headers: { "cache-control": "no-store" },
  });
}

async function cancelBody(body: ReadableStream | null, reason: string): Promise<void> {
  if (body === null || body.locked) return;
  try {
    await body.cancel(reason);
  } catch {
    // The runtime may already have consumed or released the inbound stream.
  }
}

function r2ErrorCode(error: unknown): number | undefined {
  if (typeof error === "object" && error !== null && "code" in error) {
    const code = (error as { code?: unknown }).code;
    if (typeof code === "number") return code;
  }
  if (!(error instanceof Error)) return undefined;
  const match = /\((\d+)\)$/u.exec(error.message);
  return match === null ? undefined : Number(match[1]);
}

function publicSnapshot(snapshot: FinalizedSnapshot): SnapshotMetadata {
  const {
    objectKey: _objectKey,
    r2Version: _r2Version,
    etag: _etag,
    uploadKind: _uploadKind,
    ...metadata
  } = snapshot;
  return metadata;
}

function normalizedEtag(etag: string): string {
  return etag.replaceAll('"', "").toLowerCase();
}

export class SnapshotUploadCoordinator {
  private readonly snapshotOperationOwners = new Map<string, SnapshotOperationOwner>();
  private activeSnapshotUploadRequests = 0;
  private activeSnapshotBodyUploadId: string | undefined;
  private snapshotAlarmTarget: number | undefined;
  private snapshotAlarmUpdate: Promise<void> | undefined;
  private snapshotMaintenance: Promise<void> | undefined;
  private readonly snapshotMaintenanceTasks = new Map<string, Promise<void>>();
  private snapshotMaintenanceRequested = false;
  private snapshotPartUploadTimeoutMs = SNAPSHOT_UPLOAD_RESERVATION_MS;
  private snapshotR2OperationTimeoutMs = SNAPSHOT_GC_RETRY_MS;

  constructor(
    private readonly ctx: DurableObjectState,
    private snapshotBucket: R2Bucket,
    private readonly snapshots: SnapshotStore,
    private readonly pinnedSnapshotIds: () => ReadonlySet<string>,
  ) {}

  async upload(
    request: Request,
    snapshotId: string,
    sessionId: string | undefined,
  ): Promise<Response> {
    const metadata =
      sessionId === undefined
        ? undefined
        : parseSnapshotUploadMetadata(request, sessionId, snapshotId);
    if (metadata === undefined || request.body === null) {
      await cancelBody(request.body, "invalid snapshot metadata");
      return json({ error: "invalid-snapshot-metadata" }, 400);
    }

    if (this.activeSnapshotUploadRequests >= SNAPSHOT_UPLOAD_RESERVATION_LIMIT) {
      await cancelBody(request.body, "snapshot request admission is full");
      return json({ error: "snapshot-upload-in-progress" }, 503);
    }
    this.activeSnapshotUploadRequests += 1;

    const owner = this.acquireSnapshotOperation(snapshotId, "upload");
    if (owner === undefined) {
      this.activeSnapshotUploadRequests -= 1;
      await cancelBody(request.body, "snapshot storage is busy");
      return json({ error: "snapshot-upload-in-progress" }, 503);
    }
    try {
      const now = Date.now();
      try {
        await this.scheduleAlarm(now + SNAPSHOT_UPLOAD_RESERVATION_MS);
      } catch {
        await cancelBody(request.body, "snapshot recovery alarm is unavailable");
        return json({ error: "snapshot-upload-failed" }, 503);
      }
      const started = this.snapshots.beginUpload(metadata, now);
      if (!started.ok) {
        await cancelBody(request.body, "snapshot upload reservation failed");
        return json(
          {
            error:
              started.reason === "retention-backlog"
                ? "snapshot-reservation-failed"
                : "snapshot-conflict",
          },
          started.reason === "retention-backlog" ? 503 : 409,
        );
      }
      if (started.state === "published") {
        await cancelBody(request.body, "snapshot is already published");
        return await this.verifyPublishedUpload(started.snapshot);
      }
      return await this.continueSnapshotUpload(request.body, metadata, started.upload);
    } finally {
      this.releaseSnapshotOperation(owner);
      this.activeSnapshotUploadRequests -= 1;
    }
  }

  scheduleMaintenance(): void {
    this.snapshotMaintenanceRequested = true;
    this.ctx.waitUntil(this.maintain().catch(() => undefined));
  }

  async initialize(): Promise<void> {
    const now = Date.now();
    const uploadExpiry = this.snapshots.nextUploadExpiry();
    if (this.snapshots.needsMaintenance(now, this.retentionPins(), this.activeSnapshotIds())) {
      await this.scheduleAlarm(now + SNAPSHOT_GC_RETRY_MS);
      return;
    }
    if (uploadExpiry !== undefined) await this.scheduleAlarm(uploadExpiry);
  }

  maintain(): Promise<void> {
    this.snapshotMaintenanceRequested = true;
    if (this.snapshotMaintenance !== undefined) return this.snapshotMaintenance;
    this.snapshotMaintenanceRequested = false;
    const maintenance = this.runMaintenance().finally(() => {
      if (this.snapshotMaintenance === maintenance) this.snapshotMaintenance = undefined;
      if (this.snapshotMaintenanceRequested) {
        this.snapshotMaintenanceRequested = false;
        this.ctx.waitUntil(
          this.scheduleAlarm(Date.now() + SNAPSHOT_GC_RETRY_MS).catch(() => undefined),
        );
      }
    });
    this.snapshotMaintenance = maintenance;
    return maintenance;
  }

  private acquireSnapshotOperation(
    snapshotId: string,
    source: SnapshotOperationOwner["source"],
  ): SnapshotOperationOwner | undefined {
    if (this.snapshotOperationOwners.has(snapshotId)) return undefined;
    const owner = { snapshotId, source } as const;
    this.snapshotOperationOwners.set(snapshotId, owner);
    return owner;
  }

  private releaseSnapshotOperation(owner: SnapshotOperationOwner, requestMaintenance = true): void {
    if (this.snapshotOperationOwners.get(owner.snapshotId) !== owner) return;
    this.snapshotOperationOwners.delete(owner.snapshotId);
    if (requestMaintenance) this.scheduleMaintenance();
  }

  private async verifyPublishedUpload(snapshot: FinalizedSnapshot): Promise<Response> {
    const heading = await this.runR2WithinDeadline(() =>
      this.snapshotBucket.head(snapshot.objectKey),
    );
    if (heading.kind !== "fulfilled") {
      return json({ error: "snapshot-unavailable" }, 503);
    }
    const object = heading.value;
    if (
      object === null ||
      object.version !== snapshot.r2Version ||
      object.etag !== snapshot.etag ||
      !matchesSnapshotObject(object, snapshot, snapshot.uploadKind)
    ) {
      return json({ error: "snapshot-unavailable" }, 503);
    }
    try {
      const refreshed = this.snapshots.refreshPublishedExact(snapshot, this.retentionPins());
      return refreshed === undefined
        ? json({ error: "snapshot-unavailable" }, 503)
        : json({ created: false, snapshot: publicSnapshot(refreshed) });
    } catch {
      return json({ error: "snapshot-unavailable" }, 503);
    }
  }

  private async continueSnapshotUpload(
    body: ReadableStream<Uint8Array>,
    metadata: SnapshotMetadata,
    initialUpload: SnapshotUploadRecord,
  ): Promise<Response> {
    let upload = initialUpload;
    if (upload.state === "completed") {
      await cancelBody(body, "snapshot object is already complete");
      return this.verifyCompletedUpload(metadata.snapshotId);
    }

    if (
      upload.state === "uploading" ||
      upload.state === "completing" ||
      upload.state === "uncertain" ||
      upload.state === "aborted"
    ) {
      const recovered = await this.recoverMultipartUpload(upload, metadata);
      if (recovered.kind === "response") {
        await cancelBody(body, "snapshot multipart recovery did not permit a new upload");
        return recovered.response;
      }
      upload = recovered.upload;
    }

    const prepared = await this.uploadPreparedSnapshotBody(body, metadata, upload);

    if (prepared.kind === "response") return prepared.response;
    if (prepared.kind === "checksum-mismatch") {
      return this.rejectChecksumMismatch(upload, prepared.multipart);
    }
    return this.completeMultipartUpload(metadata, upload, prepared.multipart, prepared.part);
  }

  private async uploadPreparedSnapshotBody(
    body: ReadableStream<Uint8Array>,
    metadata: SnapshotMetadata,
    upload: SnapshotUploadRecord,
  ): Promise<PreparedMultipartBody> {
    const heading = await this.runR2WithinDeadline(() =>
      this.snapshotBucket.head(upload.object_key),
    );
    if (heading.kind !== "fulfilled") {
      this.snapshots.deletePreparedUpload(upload.snapshot_id, upload.object_key);
      await cancelBody(body, "snapshot storage is unavailable");
      return { kind: "response", response: json({ error: "snapshot-upload-failed" }, 503) };
    }
    const existing = heading.value;
    if (existing !== null) {
      this.snapshots.retirePreparedUpload(upload.snapshot_id, upload.object_key);
      await cancelBody(body, "immutable snapshot key already exists");
      return { kind: "response", response: json({ error: "snapshot-conflict" }, 409) };
    }

    const creating = await this.runR2WithinDeadline(() =>
      this.snapshotBucket.createMultipartUpload(upload.object_key, {
        httpMetadata: {
          contentType: SNAPSHOT_MEDIA_TYPE,
          cacheControl: "private, no-store",
        },
        customMetadata: snapshotCustomMetadata(metadata, "multipart-verified"),
      }),
    );
    if (creating.kind !== "fulfilled") {
      await cancelBody(body, "snapshot multipart creation is uncertain");
      return { kind: "response", response: json({ error: "snapshot-upload-failed" }, 503) };
    }
    const multipart = creating.value;
    if (
      !this.snapshots.recordMultipartUpload(
        upload.snapshot_id,
        upload.object_key,
        multipart.uploadId,
      )
    ) {
      await cancelBody(body, "snapshot multipart ownership changed");
      return { kind: "response", response: json({ error: "snapshot-upload-failed" }, 503) };
    }

    if (this.activeSnapshotBodyUploadId !== undefined) {
      await cancelBody(body, "another snapshot body upload is active");
      return {
        kind: "response",
        response: json({ error: "snapshot-upload-in-progress" }, 503),
      };
    }
    this.activeSnapshotBodyUploadId = upload.snapshot_id;
    try {
      return await this.uploadMultipartBody(body, metadata, upload, multipart);
    } finally {
      if (this.activeSnapshotBodyUploadId === upload.snapshot_id) {
        this.activeSnapshotBodyUploadId = undefined;
      }
    }
  }

  private async verifyCompletedUpload(snapshotId: string): Promise<Response> {
    const snapshot = this.snapshots.completedUpload(snapshotId);
    if (snapshot === undefined) return json({ error: "snapshot-upload-failed" }, 503);
    const heading = await this.runR2WithinDeadline(() =>
      this.snapshotBucket.head(snapshot.objectKey),
    );
    if (heading.kind !== "fulfilled") {
      return json({ error: "snapshot-unavailable" }, 503);
    }
    const object = heading.value;
    if (
      object === null ||
      object.version !== snapshot.r2Version ||
      object.etag !== snapshot.etag ||
      !matchesSnapshotObject(object, snapshot, snapshot.uploadKind)
    ) {
      return json({ error: "snapshot-unavailable" }, 503);
    }
    return this.finalizeCompletedUpload(snapshot);
  }

  private async recoverMultipartUpload(
    upload: SnapshotUploadRecord,
    metadata: SnapshotMetadata,
  ): Promise<MultipartRecoveryResult> {
    const failed = (): MultipartRecoveryResult => ({
      kind: "response",
      response: json({ error: "snapshot-upload-failed" }, 503),
    });
    if (upload.upload_id === null) return failed();
    if (upload.state === "aborted") {
      const heading = await this.runR2WithinDeadline(() =>
        this.snapshotBucket.head(upload.object_key),
      );
      if (heading.kind !== "fulfilled" || heading.value !== null) {
        return failed();
      }
      const restarted = this.snapshots.restartAfterMultipartAbort(
        upload.snapshot_id,
        upload.object_key,
        upload.upload_id,
      );
      return restarted === undefined ? failed() : { kind: "restart", upload: restarted };
    }
    if (upload.state === "uncertain") {
      const heading = await this.runR2WithinDeadline(() =>
        this.snapshotBucket.head(upload.object_key),
      );
      if (heading.kind !== "fulfilled") {
        return failed();
      }
      const object = heading.value;
      if (object === null || !matchesSnapshotObject(object, metadata, "multipart-verified")) {
        return failed();
      }
      const completed = this.snapshots.recordMultipartCompleted(
        upload.snapshot_id,
        upload.object_key,
        upload.upload_id,
        object.version,
        object.etag,
      );
      return {
        kind: "response",
        response:
          completed === undefined
            ? json({ error: "snapshot-upload-failed" }, 503)
            : this.finalizeCompletedUpload(completed),
      };
    }

    const multipart = this.snapshotBucket.resumeMultipartUpload(
      upload.object_key,
      upload.upload_id,
    );
    const abort = await this.runR2WithinDeadline(() => multipart.abort());
    if (abort.kind !== "fulfilled") {
      if (abort.kind !== "rejected" || r2ErrorCode(abort.error) !== 10024) {
        return failed();
      }
      const heading = await this.runR2WithinDeadline(() =>
        this.snapshotBucket.head(upload.object_key),
      );
      if (heading.kind !== "fulfilled") {
        return failed();
      }
      const object = heading.value;
      if (
        upload.state !== "completing" ||
        object === null ||
        !matchesSnapshotObject(object, metadata, "multipart-verified")
      ) {
        this.snapshots.recordMultipartUncertain(
          upload.snapshot_id,
          upload.object_key,
          upload.upload_id,
        );
        return failed();
      }
      const completed = this.snapshots.recordMultipartCompleted(
        upload.snapshot_id,
        upload.object_key,
        upload.upload_id,
        object.version,
        object.etag,
      );
      return {
        kind: "response",
        response:
          completed === undefined
            ? json({ error: "snapshot-upload-failed" }, 503)
            : this.finalizeCompletedUpload(completed),
      };
    }

    if (
      !this.snapshots.recordMultipartAborted(
        upload.snapshot_id,
        upload.object_key,
        upload.upload_id,
      )
    ) {
      return failed();
    }
    const heading = await this.runR2WithinDeadline(() =>
      this.snapshotBucket.head(upload.object_key),
    );
    if (heading.kind !== "fulfilled" || heading.value !== null) {
      return failed();
    }
    const restarted = this.snapshots.restartAfterMultipartAbort(
      upload.snapshot_id,
      upload.object_key,
      upload.upload_id,
    );
    return restarted === undefined ? failed() : { kind: "restart", upload: restarted };
  }

  private async uploadMultipartBody(
    body: ReadableStream<Uint8Array>,
    metadata: SnapshotMetadata,
    upload: SnapshotUploadRecord,
    multipart: R2MultipartUpload,
  ): Promise<PreparedMultipartBody> {
    const DigestStreamConstructor = (crypto as Crypto & { DigestStream: typeof DigestStream })
      .DigestStream;
    const sha256 = new DigestStreamConstructor("SHA-256");
    const md5 = new DigestStreamConstructor("MD5");
    const shaWriter = sha256.getWriter();
    const md5Writer = md5.getWriter();
    const expectedLength = Number(metadata.compressedLength);
    let observedLength = 0;
    const verifiedBody = body.pipeThrough(
      new TransformStream<Uint8Array, Uint8Array>({
        async transform(chunk, controller) {
          observedLength += chunk.byteLength;
          if (observedLength > expectedLength) throw new RangeError("snapshot body exceeds length");
          await shaWriter.write(chunk);
          await md5Writer.write(chunk);
          controller.enqueue(chunk);
        },
        async flush() {
          if (observedLength !== expectedLength)
            throw new RangeError("snapshot body length mismatch");
          await shaWriter.close();
          await md5Writer.close();
        },
      }),
    );
    const fixedLength = new FixedLengthStream(expectedLength);
    const pipelineAbort = new AbortController();
    const piping = verifiedBody.pipeTo(fixedLength.writable, { signal: pipelineAbort.signal });
    const partUpload = Promise.resolve().then(() => multipart.uploadPart(1, fixedLength.readable));
    void partUpload.then(
      () => undefined,
      () => undefined,
    );

    const uploadAndDigest = Promise.all([partUpload, piping]).then(async ([part]) => {
      const [shaDigest, md5Digest] = await Promise.all([sha256.digest, md5.digest]);
      return { md5Digest, part, shaDigest };
    });
    let timeoutId: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_resolve, reject) => {
      timeoutId = setTimeout(
        () => reject(new Error("snapshot part upload timed out")),
        this.snapshotPartUploadTimeoutMs,
      );
    });

    let completedUpload: Awaited<typeof uploadAndDigest>;
    try {
      completedUpload = await Promise.race([uploadAndDigest, timeout]);
    } catch (error) {
      pipelineAbort.abort(error);
      const teardown = [
        piping,
        shaWriter.abort(error),
        md5Writer.abort(error),
        sha256.digest,
        md5.digest,
      ];
      for (const promise of teardown) void promise.catch(() => undefined);
      return { kind: "response", response: json({ error: "snapshot-upload-failed" }, 503) };
    } finally {
      if (timeoutId !== undefined) clearTimeout(timeoutId);
    }

    const expectedDigestBytes = BigInt(expectedLength);
    if (
      BigInt(sha256.bytesWritten) !== expectedDigestBytes ||
      BigInt(md5.bytesWritten) !== expectedDigestBytes ||
      !this.snapshotPartEtagMatches(
        completedUpload.part.etag,
        bytesToHex(completedUpload.md5Digest),
      )
    )
      return { kind: "response", response: json({ error: "snapshot-upload-failed" }, 503) };
    if (bytesToHex(completedUpload.shaDigest) !== metadata.sha256) {
      return { kind: "checksum-mismatch", multipart };
    }
    if (
      !this.snapshots.recordMultipartCompleting(
        upload.snapshot_id,
        upload.object_key,
        multipart.uploadId,
        normalizedEtag(completedUpload.part.etag),
      )
    ) {
      return { kind: "response", response: json({ error: "snapshot-upload-failed" }, 503) };
    }

    return { kind: "ready", multipart, part: completedUpload.part };
  }

  private async completeMultipartUpload(
    metadata: SnapshotMetadata,
    upload: SnapshotUploadRecord,
    multipart: R2MultipartUpload,
    part: R2UploadedPart,
  ): Promise<Response> {
    const completing = await this.runR2WithinDeadline(() => multipart.complete([part]));
    if (completing.kind !== "fulfilled") {
      return json({ error: "snapshot-upload-failed" }, 503);
    }
    const completedObject = completing.value;
    const heading = await this.runR2WithinDeadline(() =>
      this.snapshotBucket.head(upload.object_key),
    );
    if (heading.kind !== "fulfilled") {
      return json({ error: "snapshot-upload-failed" }, 503);
    }
    const object = heading.value;
    if (
      object === null ||
      object.version !== completedObject.version ||
      object.etag !== completedObject.etag ||
      !matchesSnapshotObject(object, metadata, "multipart-verified")
    ) {
      return json({ error: "snapshot-upload-failed" }, 503);
    }
    const completed = this.snapshots.recordMultipartCompleted(
      upload.snapshot_id,
      upload.object_key,
      multipart.uploadId,
      object.version,
      object.etag,
    );
    return completed === undefined
      ? json({ error: "snapshot-upload-failed" }, 503)
      : this.finalizeCompletedUpload(completed);
  }

  private async rejectChecksumMismatch(
    upload: SnapshotUploadRecord,
    multipart: R2MultipartUpload,
  ): Promise<Response> {
    const abort = await this.runR2WithinDeadline(() => multipart.abort());
    if (abort.kind !== "fulfilled") return json({ error: "snapshot-upload-failed" }, 503);

    if (
      !this.snapshots.recordMultipartAborted(
        upload.snapshot_id,
        upload.object_key,
        multipart.uploadId,
      )
    ) {
      return json({ error: "snapshot-upload-failed" }, 503);
    }
    const head = await this.runR2WithinDeadline(() => this.snapshotBucket.head(upload.object_key));
    if (head.kind !== "fulfilled" || head.value !== null) {
      return json({ error: "snapshot-upload-failed" }, 503);
    }
    if (
      !this.snapshots.deleteAbortedMultipart(
        upload.snapshot_id,
        upload.object_key,
        multipart.uploadId,
      )
    ) {
      return json({ error: "snapshot-upload-failed" }, 503);
    }
    return json({ error: "snapshot-checksum-mismatch" }, 422);
  }

  private async runR2WithinDeadline<T>(operation: () => Promise<T>): Promise<R2DeadlineResult<T>> {
    const running = Promise.resolve().then(operation);
    const observed = running.then<R2DeadlineResult<T>, R2DeadlineResult<T>>(
      (value) => ({ kind: "fulfilled", value }),
      (error: unknown) => ({ error, kind: "rejected" }),
    );
    let timeoutId: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<R2DeadlineResult<T>>((resolve) => {
      timeoutId = setTimeout(() => resolve({ kind: "timeout" }), this.snapshotR2OperationTimeoutMs);
    });
    try {
      return await Promise.race([observed, timeout]);
    } finally {
      if (timeoutId !== undefined) clearTimeout(timeoutId);
    }
  }

  private snapshotPartEtagMatches(partEtag: string, expectedMd5: string): boolean {
    return matchesMultipartPartEtag(partEtag, expectedMd5);
  }

  private async cleanupAbortedMultipart(objectKey: string): Promise<boolean> {
    let object: R2Object | null;
    try {
      object = await this.snapshotBucket.head(objectKey);
      if (object !== null) await this.snapshotBucket.delete(objectKey);
      object = await this.snapshotBucket.head(objectKey);
    } catch {
      return false;
    }
    return object === null;
  }

  private finalizeCompletedUpload(snapshot: FinalizedSnapshot): Response {
    const finalized = this.snapshots.finalize(snapshot, this.retentionPins());
    if (!finalized.ok) {
      return json(
        {
          error:
            finalized.reason === "cursor-ahead" ? "snapshot-cursor-ahead" : "snapshot-conflict",
        },
        finalized.reason === "retention-backlog" || finalized.reason === "attempt-changed"
          ? 503
          : 409,
      );
    }
    return json(
      { created: finalized.created, snapshot: publicSnapshot(finalized.snapshot) },
      finalized.created ? 201 : 200,
    );
  }

  private scheduleAlarm(timestamp: number): Promise<void> {
    this.snapshotAlarmTarget = Math.min(this.snapshotAlarmTarget ?? timestamp, timestamp);
    if (this.snapshotAlarmUpdate !== undefined) return this.snapshotAlarmUpdate;
    const update = this.flushAlarmTargets();
    this.snapshotAlarmUpdate = update;
    return update;
  }

  private async flushAlarmTargets(): Promise<void> {
    try {
      while (this.snapshotAlarmTarget !== undefined) {
        let target = this.snapshotAlarmTarget;
        const alarm = await this.ctx.storage.getAlarm();
        if (this.snapshotAlarmTarget !== undefined) {
          target = Math.min(target, this.snapshotAlarmTarget);
        }
        if (alarm === null || target < alarm) await this.ctx.storage.setAlarm(target);
        if (this.snapshotAlarmTarget === target) this.snapshotAlarmTarget = undefined;
      }
    } finally {
      this.snapshotAlarmUpdate = undefined;
    }
  }

  private async recoverMultipartForMaintenance(
    upload: RecoverableMultipartUpload,
  ): Promise<boolean> {
    if (upload.state === "aborted") {
      if (!(await this.cleanupAbortedMultipart(upload.objectKey))) return false;
      return this.snapshots.deleteAbortedMultipart(
        upload.snapshotId,
        upload.objectKey,
        upload.uploadId,
      );
    }

    const multipart = this.snapshotBucket.resumeMultipartUpload(upload.objectKey, upload.uploadId);
    try {
      await multipart.abort();
    } catch (error) {
      if (r2ErrorCode(error) !== 10024) return false;
      let object: R2Object | null;
      try {
        object = await this.snapshotBucket.head(upload.objectKey);
      } catch {
        return false;
      }
      if (
        upload.state !== "completing" ||
        object === null ||
        !matchesSnapshotObject(object, upload.metadata, "multipart-verified")
      ) {
        return this.snapshots.recordMultipartUncertain(
          upload.snapshotId,
          upload.objectKey,
          upload.uploadId,
        );
      }
      return (
        this.snapshots.recordMultipartCompleted(
          upload.snapshotId,
          upload.objectKey,
          upload.uploadId,
          object.version,
          object.etag,
        ) !== undefined
      );
    }

    if (
      !this.snapshots.recordMultipartAborted(
        upload.snapshotId,
        upload.objectKey,
        upload.uploadId,
      ) ||
      !(await this.cleanupAbortedMultipart(upload.objectKey))
    ) {
      return false;
    }
    this.snapshots.deleteAbortedMultipart(upload.snapshotId, upload.objectKey, upload.uploadId);
    return true;
  }

  private activeSnapshotIds(): ReadonlySet<string> {
    return new Set(this.snapshotOperationOwners.keys());
  }

  private retentionPins(): ReadonlySet<string> {
    return new Set([...this.pinnedSnapshotIds(), ...this.snapshotOperationOwners.keys()]);
  }

  private launchRetiredMaintenance(
    owner: SnapshotOperationOwner,
    snapshot: RetiredSnapshotObject,
  ): void {
    const task = (async () => {
      let succeeded = false;
      try {
        const deleted = await deleteRetiredSnapshotObjects(this.snapshotBucket, [snapshot]);
        if (deleted.deleted.length === 1) {
          this.snapshots.deleteRetired(snapshot);
          succeeded = true;
        }
      } catch {
        // The durable row and watchdog remain the source of truth for retry.
      } finally {
        if (!succeeded) {
          try {
            await this.scheduleAlarm(Date.now() + SNAPSHOT_GC_RETRY_MS);
          } catch {
            // The preflight watchdog remains the durable retry source.
          }
        }
        this.releaseSnapshotOperation(owner, false);
      }
    })();
    this.trackMaintenanceTask(owner.snapshotId, task);
    this.ctx.waitUntil(task);
  }

  private launchMultipartMaintenance(
    owner: SnapshotOperationOwner,
    upload: RecoverableMultipartUpload,
  ): void {
    const task = (async () => {
      let succeeded = false;
      try {
        const current = this.snapshots.recoverableMultipartUpload(upload.snapshotId);
        if (
          current !== undefined &&
          current.uploadId === upload.uploadId &&
          current.state === upload.state
        ) {
          succeeded = await this.recoverMultipartForMaintenance(current);
        } else {
          succeeded = true;
        }
      } catch {
        // The durable row and watchdog remain the source of truth for retry.
      } finally {
        if (!succeeded) {
          try {
            await this.scheduleAlarm(Date.now() + SNAPSHOT_GC_RETRY_MS);
          } catch {
            // The preflight watchdog remains the durable retry source.
          }
        }
        this.releaseSnapshotOperation(owner, false);
      }
    })();
    this.trackMaintenanceTask(owner.snapshotId, task);
    this.ctx.waitUntil(task);
  }

  private trackMaintenanceTask(snapshotId: string, task: Promise<void>): void {
    this.snapshotMaintenanceTasks.set(snapshotId, task);
    void task.then(
      () => {
        if (this.snapshotMaintenanceTasks.get(snapshotId) === task) {
          this.snapshotMaintenanceTasks.delete(snapshotId);
        }
      },
      () => {
        if (this.snapshotMaintenanceTasks.get(snapshotId) === task) {
          this.snapshotMaintenanceTasks.delete(snapshotId);
        }
      },
    );
  }

  private async runMaintenance(): Promise<void> {
    let now = Date.now();
    let activeIds = this.activeSnapshotIds();
    let retentionPins = this.retentionPins();
    const uploadExpiryBeforeMaintenance = this.snapshots.nextUploadExpiry();
    const activeMaintenanceBeforeMaintenance = [...this.snapshotOperationOwners.values()].some(
      (owner) => owner.source === "maintenance",
    );
    const maintenanceNeeded = this.snapshots.needsMaintenance(now, retentionPins, activeIds);
    if (!maintenanceNeeded) {
      const retryAt =
        activeMaintenanceBeforeMaintenance ||
        (uploadExpiryBeforeMaintenance !== undefined && uploadExpiryBeforeMaintenance <= now)
          ? now + SNAPSHOT_GC_RETRY_MS
          : uploadExpiryBeforeMaintenance;
      if (retryAt !== undefined) await this.scheduleAlarm(retryAt);
      return;
    }
    await this.scheduleAlarm(now + SNAPSHOT_GC_RETRY_MS);

    now = Date.now();
    activeIds = this.activeSnapshotIds();
    retentionPins = this.retentionPins();
    if (!this.snapshots.needsMaintenance(now, retentionPins, activeIds)) return;

    let jobCapacity = Math.max(
      0,
      SNAPSHOT_MAINTENANCE_BATCH_LIMIT - this.snapshotMaintenanceTasks.size,
    );
    if (jobCapacity === 0) return;
    const snapshotCandidates = this.snapshots
      .reconcile(retentionPins, jobCapacity)
      .retired.filter((snapshot) => !activeIds.has(snapshot.snapshotId));
    const retiredJobs = snapshotCandidates.flatMap((snapshot) => {
      const owner = this.acquireSnapshotOperation(snapshot.snapshotId, "maintenance");
      return owner === undefined ? [] : [{ owner, snapshot }];
    });

    jobCapacity -= retiredJobs.length;
    activeIds = this.activeSnapshotIds();
    const uploadCandidates = this.snapshots
      .retired(now, activeIds, jobCapacity)
      .filter((snapshot) => !activeIds.has(snapshot.snapshotId));
    for (const snapshot of uploadCandidates) {
      const owner = this.acquireSnapshotOperation(snapshot.snapshotId, "maintenance");
      if (owner === undefined) continue;
      retiredJobs.push({ owner, snapshot });
      jobCapacity -= 1;
    }

    activeIds = this.activeSnapshotIds();
    const recoverable = this.snapshots.recoverableMultipartUploads(activeIds, jobCapacity);
    const multipartJobs = recoverable.flatMap((upload) => {
      const owner = this.acquireSnapshotOperation(upload.snapshotId, "maintenance");
      return owner === undefined ? [] : [{ owner, upload }];
    });

    for (const job of retiredJobs) {
      this.launchRetiredMaintenance(job.owner, job.snapshot);
    }
    for (const job of multipartJobs) {
      this.launchMultipartMaintenance(job.owner, job.upload);
    }

    activeIds = this.activeSnapshotIds();
    retentionPins = this.retentionPins();
    const pendingMaintenance = this.snapshots.needsMaintenance(now, retentionPins, activeIds);
    const activeMaintenance = [...this.snapshotOperationOwners.values()].some(
      (owner) => owner.source === "maintenance",
    );
    const uploadExpiry = this.snapshots.nextUploadExpiry();
    const retryAt =
      pendingMaintenance || activeMaintenance || (uploadExpiry !== undefined && uploadExpiry <= now)
        ? now + SNAPSHOT_GC_RETRY_MS
        : undefined;
    const nextAlarm =
      retryAt === undefined
        ? uploadExpiry
        : uploadExpiry === undefined
          ? retryAt
          : Math.min(retryAt, uploadExpiry <= now ? retryAt : uploadExpiry);
    if (nextAlarm !== undefined) await this.scheduleAlarm(nextAlarm);
  }
}
