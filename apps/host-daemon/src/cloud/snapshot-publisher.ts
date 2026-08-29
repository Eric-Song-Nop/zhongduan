import { createHash, randomBytes } from "node:crypto";
import { constants as zlibConstants, zstdCompress } from "node:zlib";

import {
  MAX_SNAPSHOT_COMPRESSED_BYTES,
  MAX_SNAPSHOT_UNCOMPRESSED_BYTES,
  SnapshotMetadataSchema,
  type SnapshotMetadata,
} from "@zhongduan/protocol";

import type { SnapshotCapture } from "../session";
import { CloudApiClient, CloudApiError } from "./cloud-api";

export interface SnapshotUploadApi {
  uploadSnapshot: CloudApiClient["uploadSnapshot"];
}

export interface HostCapabilityProvider {
  current(signal?: AbortSignal): Promise<string>;
  recoverRejected(rejectedCapability: string, signal?: AbortSignal): Promise<string>;
}

export type SnapshotCompressor = (bytes: Uint8Array) => Promise<Uint8Array>;

export type SnapshotPendingAdmission = (metadata: Readonly<SnapshotMetadata>) => boolean;

export const SNAPSHOT_CURSOR_AHEAD_MAX_WAIT_MS = 120_000;
const SNAPSHOT_CLEANUP_ATTEMPT_TIMEOUT_MS = 120_000;

export interface SnapshotPublisherOptions {
  api: SnapshotUploadApi;
  buildMinIntervalMs?: number;
  capabilities: HostCapabilityProvider;
  cleanupAttemptTimeoutMs?: number;
  compress?: SnapshotCompressor;
  cursorAheadMaxWaitMs?: number;
  failureBackoffBaseMs?: number;
  failureBackoffMaxMs?: number;
  monotonicNow?: () => number;
  random?: () => number;
  retryAttempts?: number;
  retryDelayMs?: number;
  sessionId: string;
  unavailableRetryMs?: number;
}

export interface PublishedSnapshot {
  metadata: SnapshotMetadata;
}

interface PendingSnapshot {
  activeOperations: number;
  activeTerminal?: "cleanup" | "published" | "superseded";
  readonly body: Uint8Array<ArrayBuffer>;
  cursorAheadController?: AbortController;
  readonly metadata: SnapshotMetadata;
  readonly operationController: AbortController;
  cursorAheadDeadline?: number;
  cursorAheadTimeout?: ReturnType<typeof setTimeout>;
  exposure: "ambiguous" | "cursor-ahead" | "local";
  failureCount: number;
  nextUploadAt: number;
}

type SnapshotUploadAttempt =
  | {
      readonly kind: "failure";
      readonly error: unknown;
    }
  | {
      readonly kind: "success";
    };

export class RetryableSnapshotPublishError extends Error {
  constructor(
    cause: unknown,
    readonly retryAfterMs: number,
  ) {
    super("snapshot publish retries were exhausted", { cause });
    this.name = "RetryableSnapshotPublishError";
  }
}

export class SnapshotUnavailableError extends Error {
  constructor(
    cause: unknown,
    readonly retryAfterMs: number,
  ) {
    super("snapshot is temporarily unavailable", { cause });
    this.name = "SnapshotUnavailableError";
  }
}

export class SnapshotCleanupConfirmedError extends Error {
  constructor(cause: unknown) {
    super("cloud confirmed that the pending snapshot was removed", { cause });
    this.name = "SnapshotCleanupConfirmedError";
  }
}

export class SnapshotPendingSupersededError extends Error {
  constructor(cause?: unknown) {
    super("pending snapshot was superseded by a newer authority cut", { cause });
    this.name = "SnapshotPendingSupersededError";
  }
}

export class SnapshotPublisher {
  readonly #api: SnapshotUploadApi;
  readonly #buildMinIntervalMs: number;
  readonly #capabilities: HostCapabilityProvider;
  readonly #cleanupAttemptTimeoutMs: number;
  readonly #compress: SnapshotCompressor;
  readonly #cursorAheadMaxWaitMs: number;
  readonly #failureBackoffBaseMs: number;
  readonly #failureBackoffMaxMs: number;
  readonly #monotonicNow: () => number;
  readonly #random: () => number;
  readonly #retryAttempts: number;
  readonly #retryDelayMs: number;
  readonly #sessionId: string;
  readonly #unavailableRetryMs: number;

  #buildInFlight: Promise<PendingSnapshot> | undefined;
  #cleanupController: AbortController | undefined;
  #cleanupInFlight: Promise<void> | undefined;
  #cleanupPending: PendingSnapshot | undefined;
  #cleanupTimeout: ReturnType<typeof setTimeout> | undefined;
  #disposedReason: Error | undefined;
  #nextBuildAt = 0;
  #pending: PendingSnapshot | undefined;

  constructor(options: SnapshotPublisherOptions) {
    this.#api = options.api;
    this.#buildMinIntervalMs = nonNegativeInteger(
      options.buildMinIntervalMs ?? 1_000,
      "buildMinIntervalMs",
    );
    this.#capabilities = options.capabilities;
    this.#cleanupAttemptTimeoutMs = positiveInteger(
      options.cleanupAttemptTimeoutMs ?? SNAPSHOT_CLEANUP_ATTEMPT_TIMEOUT_MS,
      "cleanupAttemptTimeoutMs",
    );
    this.#compress = options.compress ?? compressZstd;
    this.#cursorAheadMaxWaitMs = positiveInteger(
      options.cursorAheadMaxWaitMs ?? SNAPSHOT_CURSOR_AHEAD_MAX_WAIT_MS,
      "cursorAheadMaxWaitMs",
    );
    this.#failureBackoffBaseMs = positiveInteger(
      options.failureBackoffBaseMs ?? 1_000,
      "failureBackoffBaseMs",
    );
    this.#failureBackoffMaxMs = positiveInteger(
      options.failureBackoffMaxMs ?? 30_000,
      "failureBackoffMaxMs",
    );
    if (this.#failureBackoffMaxMs < this.#failureBackoffBaseMs) {
      throw new RangeError("failureBackoffMaxMs must be at least failureBackoffBaseMs");
    }
    this.#monotonicNow = options.monotonicNow ?? performance.now.bind(performance);
    this.#random = options.random ?? Math.random;
    this.#retryAttempts = positiveInteger(options.retryAttempts ?? 3, "retryAttempts");
    this.#retryDelayMs = nonNegativeInteger(options.retryDelayMs ?? 50, "retryDelayMs");
    this.#sessionId = options.sessionId;
    this.#unavailableRetryMs = positiveInteger(
      options.unavailableRetryMs ?? 30_000,
      "unavailableRetryMs",
    );
  }

  async publish(
    snapshot: SnapshotCapture,
    signal?: AbortSignal,
    admit?: SnapshotPendingAdmission,
  ): Promise<PublishedSnapshot> {
    signal?.throwIfAborted();
    this.#throwIfDisposed();
    this.#kickCleanup();
    const existing = this.resumePending(signal, admit);
    if (existing !== undefined) return existing;
    const now = this.#monotonicNow();
    if (now < this.#nextBuildAt) {
      throw new RetryableSnapshotPublishError(
        new Error("snapshot build is rate limited"),
        Math.ceil(this.#nextBuildAt - now),
      );
    }
    if (snapshot.bytes.byteLength === 0) {
      throw new RangeError("snapshot encoder returned an empty payload");
    }
    if (snapshot.bytes.byteLength > MAX_SNAPSHOT_UNCOMPRESSED_BYTES) {
      throw this.#snapshotUnavailable(
        new RangeError("snapshot exceeds the uncompressed upload bound"),
      );
    }
    this.#nextBuildAt = now + this.#buildMinIntervalMs;
    const pending = await raceAbort(this.#startBuild(snapshot), signal);
    return this.#uploadPending(pending, signal, admit);
  }

  resumePending(
    signal?: AbortSignal,
    admit?: SnapshotPendingAdmission,
  ): Promise<PublishedSnapshot> | undefined {
    signal?.throwIfAborted();
    this.#throwIfDisposed();
    this.#kickCleanup();
    const pending =
      this.#pending === undefined ? this.#buildInFlight : Promise.resolve(this.#pending);
    if (pending === undefined) return undefined;
    return this.#resumePending(pending, signal, admit);
  }

  dispose(reason: unknown = new Error("snapshot publisher disposed")): void {
    if (this.#disposedReason !== undefined) return;
    this.#disposedReason =
      reason instanceof Error
        ? reason
        : new Error("snapshot publisher disposed", { cause: reason });
    if (this.#cleanupTimeout !== undefined) clearTimeout(this.#cleanupTimeout);
    this.#cleanupTimeout = undefined;
    this.#cleanupController?.abort(this.#disposedReason);
    this.#cleanupController = undefined;
    if (this.#pending !== undefined) {
      this.#clearCursorAheadDeadline(this.#pending);
      this.#pending.operationController.abort(this.#disposedReason);
    }
    if (this.#cleanupPending !== undefined) {
      this.#clearCursorAheadDeadline(this.#cleanupPending);
    }
    this.#pending = undefined;
    this.#cleanupPending = undefined;
  }

  async #resumePending(
    pending: Promise<PendingSnapshot>,
    signal?: AbortSignal,
    admit?: SnapshotPendingAdmission,
  ): Promise<PublishedSnapshot> {
    const built = await raceAbort(pending, signal);
    signal?.throwIfAborted();
    this.#throwIfDisposed();
    return this.#uploadPending(built, signal, admit);
  }

  #startBuild(snapshot: SnapshotCapture): Promise<PendingSnapshot> {
    if (this.#buildInFlight !== undefined || this.#pending !== undefined) {
      throw new Error("snapshot publisher already owns a pending build");
    }
    const operation = this.#build(snapshot).then((pending) => {
      this.#throwIfDisposed();
      this.#pending = pending;
      return pending;
    });
    this.#buildInFlight = operation;
    void operation.then(
      () => {
        if (this.#buildInFlight === operation) this.#buildInFlight = undefined;
      },
      () => {
        if (this.#buildInFlight === operation) this.#buildInFlight = undefined;
      },
    );
    return operation;
  }

  async #build(snapshot: SnapshotCapture): Promise<PendingSnapshot> {
    let compressed: Uint8Array;
    try {
      compressed = await this.#compress(snapshot.bytes);
    } catch (error) {
      if (isCompressionLimitError(error)) throw this.#snapshotUnavailable(error);
      throw error;
    }
    if (compressed.byteLength === 0) {
      throw new RangeError("snapshot compressor returned an empty payload");
    }
    if (compressed.byteLength > MAX_SNAPSHOT_COMPRESSED_BYTES) {
      throw this.#snapshotUnavailable(
        new RangeError("snapshot exceeds the compressed upload bound"),
      );
    }
    const body = ownBoundedBody(compressed);
    const metadata = SnapshotMetadataSchema.parse({
      sessionId: this.#sessionId,
      snapshotId: randomBytes(18).toString("base64url"),
      engineId: snapshot.engineId,
      sessionEpoch: snapshot.sessionEpoch.toString(),
      cutEventSeq: snapshot.cutEventSeq.toString(),
      nextPtyOffset: snapshot.nextPtyOffset.toString(),
      compression: "zstd",
      compressedLength: body.byteLength.toString(),
      uncompressedLength: snapshot.bytes.byteLength.toString(),
      sha256: createHash("sha256").update(body).digest("hex"),
    });
    return {
      activeOperations: 0,
      body,
      metadata: Object.freeze(metadata),
      operationController: new AbortController(),
      exposure: "local",
      failureCount: 0,
      nextUploadAt: 0,
    };
  }

  async #uploadPending(
    pending: PendingSnapshot,
    signal?: AbortSignal,
    admit?: SnapshotPendingAdmission,
  ): Promise<PublishedSnapshot> {
    if (pending.activeTerminal !== undefined) throw new SnapshotPendingSupersededError();
    pending.activeOperations += 1;
    try {
      return await this.#runActiveUpload(pending, signal, admit);
    } finally {
      pending.activeOperations -= 1;
      this.#releaseActiveTerminal(pending);
      if (this.#cleanupPending === pending) this.#kickCleanup();
    }
  }

  async #runActiveUpload(
    pending: PendingSnapshot,
    signal?: AbortSignal,
    admit?: SnapshotPendingAdmission,
  ): Promise<PublishedSnapshot> {
    signal?.throwIfAborted();
    this.#throwIfDisposed();
    this.#requireActiveAdmission(pending, admit);
    const now = this.#monotonicNow();
    if (now < pending.nextUploadAt) {
      throw new RetryableSnapshotPublishError(
        new Error("snapshot upload is rate limited"),
        Math.ceil(pending.nextUploadAt - now),
      );
    }

    let lastError: unknown;
    for (let attempt = 1; attempt <= this.#retryAttempts; attempt += 1) {
      signal?.throwIfAborted();
      this.#throwIfDisposed();
      this.#requireActiveAdmission(pending, admit);
      const attemptSignal = this.#activeOperationSignal(pending, signal);
      const result = await this.#attemptUpload(pending, attemptSignal, () => {
        attemptSignal.throwIfAborted();
        this.#throwIfDisposed();
        this.#requireActiveAdmission(pending, admit);
        if (pending.exposure === "local") pending.exposure = "ambiguous";
      });
      this.#throwIfDisposed();
      this.#throwIfActiveTerminal(pending);
      if (this.#cursorAheadExpired(pending)) this.#supersedeActive(pending);
      signal?.throwIfAborted();
      if (result.kind === "success") {
        return this.#finishPublished(pending, admit);
      }

      const error = result.error;
      if (isCleanupSafeTerminal(error)) {
        if (this.#pending === pending) {
          this.#markActiveTerminal(pending, "superseded");
          throw new SnapshotCleanupConfirmedError(error);
        }
        if (this.#cleanupPending === pending) {
          this.#cleanupPending = undefined;
        }
        throw new SnapshotPendingSupersededError(error);
      }
      if (isExactCursorAhead(error)) {
        if (this.#pending === pending) {
          pending.exposure = "cursor-ahead";
          this.#startCursorAheadDeadline(pending);
        } else if (this.#cleanupPending === pending) {
          this.#cleanupPending = undefined;
          throw new SnapshotPendingSupersededError(error);
        } else {
          throw new SnapshotPendingSupersededError(error);
        }
      }
      this.#requireActiveAdmission(pending, admit);
      lastError = error;
      if (attempt === this.#retryAttempts) {
        throw new RetryableSnapshotPublishError(lastError, this.#recordTransientFailure(pending));
      }
      try {
        await delay(this.#retryDelayMs, this.#activeOperationSignal(pending, signal));
      } catch (error) {
        if (this.#cursorAheadExpired(pending)) this.#supersedeActive(pending);
        throw error;
      }
    }
    throw lastError;
  }

  async #attemptUpload(
    pending: PendingSnapshot,
    signal: AbortSignal | undefined,
    beforePut: () => void,
  ): Promise<SnapshotUploadAttempt> {
    let capability: string;
    try {
      capability = await raceAbort(this.#capabilities.current(signal), signal);
    } catch (error) {
      return { error, kind: "failure" };
    }

    beforePut();
    try {
      const uploaded = await raceAbort(
        this.#api.uploadSnapshot(pending.metadata, pending.body, capability, signal),
        signal,
      );
      assertExactUpload(uploaded, pending.metadata);
      return { kind: "success" };
    } catch (error) {
      if (!(error instanceof CloudApiError) || (error.status !== 401 && error.status !== 403)) {
        return { error, kind: "failure" };
      }
    }

    try {
      capability = await raceAbort(this.#capabilities.recoverRejected(capability, signal), signal);
    } catch (error) {
      return { error, kind: "failure" };
    }
    beforePut();
    try {
      const uploaded = await raceAbort(
        this.#api.uploadSnapshot(pending.metadata, pending.body, capability, signal),
        signal,
      );
      assertExactUpload(uploaded, pending.metadata);
      return { kind: "success" };
    } catch (error) {
      return { error, kind: "failure" };
    }
  }

  #finishPublished(
    pending: PendingSnapshot,
    admit: SnapshotPendingAdmission | undefined,
  ): PublishedSnapshot {
    if (this.#pending === pending) {
      let admitted = false;
      let admissionError: unknown;
      try {
        const serviceable = admit?.(pending.metadata) ?? true;
        admitted = !this.#cursorAheadExpired(pending) && serviceable;
      } catch (error) {
        admissionError = error;
      }
      this.#markActiveTerminal(pending, admitted ? "published" : "superseded");
      if (admissionError !== undefined) throw admissionError;
      if (!admitted) throw new SnapshotPendingSupersededError();
      return { metadata: pending.metadata };
    }
    if (this.#cleanupPending === pending) this.#cleanupPending = undefined;
    this.#clearCursorAheadDeadline(pending);
    throw new SnapshotPendingSupersededError();
  }

  #requireActiveAdmission(
    pending: PendingSnapshot,
    admit: SnapshotPendingAdmission | undefined,
  ): void {
    if (this.#pending !== pending) throw new SnapshotPendingSupersededError();
    this.#throwIfActiveTerminal(pending);
    if (this.#cursorAheadExpired(pending)) {
      this.#supersedeActive(pending);
    }
    if (admit !== undefined && !admit(pending.metadata)) this.#supersedeActive(pending);
  }

  #supersedeActive(pending: PendingSnapshot): never {
    if (this.#pending !== pending) throw new SnapshotPendingSupersededError();
    if (pending.exposure === "local" || pending.exposure === "cursor-ahead") {
      this.#markActiveTerminal(pending, "superseded");
      throw new SnapshotPendingSupersededError();
    }
    if (this.#cleanupPending === undefined) {
      this.#markActiveTerminal(pending, "cleanup");
      throw new SnapshotPendingSupersededError();
    }
    this.#kickCleanup();
    throw new RetryableSnapshotPublishError(
      new Error("snapshot cleanup slot is occupied"),
      this.#recordTransientFailure(pending),
    );
  }

  #kickCleanup(): void {
    const pending = this.#cleanupPending;
    if (
      this.#disposedReason !== undefined ||
      pending === undefined ||
      this.#cleanupInFlight !== undefined ||
      pending.activeOperations !== 0 ||
      this.#monotonicNow() < pending.nextUploadAt
    ) {
      return;
    }
    const controller = new AbortController();
    const timeout = setTimeout(
      () => controller.abort(new DOMException("snapshot cleanup timed out", "TimeoutError")),
      this.#cleanupAttemptTimeoutMs,
    );
    this.#cleanupController = controller;
    this.#cleanupTimeout = timeout;
    const operation = this.#reconcileCleanup(pending, controller.signal)
      .catch(() => {
        if (this.#disposedReason === undefined && this.#cleanupPending === pending) {
          this.#recordTransientFailure(pending);
        }
      })
      .finally(() => {
        clearTimeout(timeout);
        if (this.#cleanupController === controller) this.#cleanupController = undefined;
        if (this.#cleanupTimeout === timeout) this.#cleanupTimeout = undefined;
        if (this.#cleanupInFlight === operation) this.#cleanupInFlight = undefined;
        this.#kickCleanup();
      });
    this.#cleanupInFlight = operation;
    void operation;
  }

  async #reconcileCleanup(pending: PendingSnapshot, signal: AbortSignal): Promise<void> {
    if (this.#cleanupPending !== pending) return;
    const result = await this.#attemptUpload(pending, signal, () => {
      signal.throwIfAborted();
      this.#throwIfDisposed();
      if (this.#cleanupPending !== pending) throw new SnapshotPendingSupersededError();
    });
    signal.throwIfAborted();
    this.#throwIfDisposed();
    if (this.#cleanupPending !== pending) return;
    if (
      result.kind === "success" ||
      isCleanupSafeTerminal(result.error) ||
      isExactCursorAhead(result.error)
    ) {
      this.#cleanupPending = undefined;
      this.#clearCursorAheadDeadline(pending);
      return;
    }
    this.#recordTransientFailure(pending);
  }

  #recordTransientFailure(pending: PendingSnapshot): number {
    pending.failureCount = Math.min(pending.failureCount + 1, 30);
    const exponential = Math.min(
      this.#failureBackoffMaxMs,
      this.#failureBackoffBaseMs * 2 ** (pending.failureCount - 1),
    );
    const jitter = 0.8 + Math.min(1, Math.max(0, this.#random())) * 0.4;
    const delayMs = Math.min(this.#failureBackoffMaxMs, Math.ceil(exponential * jitter));
    pending.nextUploadAt = Math.max(pending.nextUploadAt, this.#monotonicNow() + delayMs);
    return Math.max(1, Math.ceil(pending.nextUploadAt - this.#monotonicNow()));
  }

  #throwIfDisposed(): void {
    if (this.#disposedReason !== undefined) throw this.#disposedReason;
  }

  #activeOperationSignal(pending: PendingSnapshot, signal: AbortSignal | undefined): AbortSignal {
    const operationSignal = pending.operationController.signal;
    const deadlineSignal = pending.cursorAheadController?.signal;
    if (deadlineSignal === undefined && signal === undefined) return operationSignal;
    return AbortSignal.any(
      [operationSignal, deadlineSignal, signal].filter(
        (candidate): candidate is AbortSignal => candidate !== undefined,
      ),
    );
  }

  #startCursorAheadDeadline(pending: PendingSnapshot): void {
    if (pending.cursorAheadDeadline !== undefined) return;
    pending.cursorAheadDeadline = this.#monotonicNow() + this.#cursorAheadMaxWaitMs;
    const controller = new AbortController();
    pending.cursorAheadController = controller;
    pending.cursorAheadTimeout = setTimeout(() => {
      delete pending.cursorAheadTimeout;
      controller.abort(new SnapshotPendingSupersededError());
      if (this.#pending === pending && pending.activeTerminal === undefined) {
        this.#markActiveTerminal(pending, "superseded");
      }
    }, this.#cursorAheadMaxWaitMs);
  }

  #clearCursorAheadDeadline(pending: PendingSnapshot): void {
    if (pending.cursorAheadTimeout !== undefined) clearTimeout(pending.cursorAheadTimeout);
    delete pending.cursorAheadTimeout;
    delete pending.cursorAheadController;
  }

  #markActiveTerminal(
    pending: PendingSnapshot,
    terminal: NonNullable<PendingSnapshot["activeTerminal"]>,
  ): void {
    if (this.#pending !== pending || pending.activeTerminal !== undefined) return;
    pending.activeTerminal = terminal;
    this.#clearCursorAheadDeadline(pending);
    pending.operationController.abort(new SnapshotPendingSupersededError());
    this.#releaseActiveTerminal(pending);
  }

  #releaseActiveTerminal(pending: PendingSnapshot): void {
    if (
      pending.activeTerminal === undefined ||
      pending.activeOperations !== 0 ||
      this.#pending !== pending
    ) {
      return;
    }
    this.#pending = undefined;
    if (pending.activeTerminal === "cleanup") {
      this.#cleanupPending = pending;
      this.#kickCleanup();
    }
  }

  #throwIfActiveTerminal(pending: PendingSnapshot): void {
    if (pending.activeTerminal !== undefined || this.#pending !== pending) {
      throw new SnapshotPendingSupersededError();
    }
  }

  #cursorAheadExpired(pending: PendingSnapshot): boolean {
    return (
      pending.exposure === "cursor-ahead" &&
      pending.cursorAheadDeadline !== undefined &&
      (pending.cursorAheadController?.signal.aborted === true ||
        this.#monotonicNow() >= pending.cursorAheadDeadline)
    );
  }

  #snapshotUnavailable(cause: unknown): SnapshotUnavailableError {
    this.#nextBuildAt = Math.max(
      this.#nextBuildAt,
      this.#monotonicNow() + this.#unavailableRetryMs,
    );
    return new SnapshotUnavailableError(
      cause,
      Math.max(1, Math.ceil(this.#nextBuildAt - this.#monotonicNow())),
    );
  }
}

function compressZstd(bytes: Uint8Array): Promise<Uint8Array> {
  return new Promise((resolve, reject) => {
    zstdCompress(
      bytes,
      {
        maxOutputLength: MAX_SNAPSHOT_COMPRESSED_BYTES,
        params: { [zlibConstants.ZSTD_c_compressionLevel]: 1 },
      },
      (error, compressed) => {
        if (error !== null) {
          reject(error);
          return;
        }
        resolve(new Uint8Array(compressed.buffer, compressed.byteOffset, compressed.byteLength));
      },
    );
  });
}

function isCleanupSafeTerminal(error: unknown): boolean {
  return (
    error instanceof CloudApiError &&
    error.status === 422 &&
    error.errorCode === "snapshot-checksum-mismatch"
  );
}

function isExactCursorAhead(error: unknown): boolean {
  return (
    error instanceof CloudApiError &&
    error.status === 409 &&
    error.errorCode === "snapshot-cursor-ahead"
  );
}

function isCompressionLimitError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === "ERR_BUFFER_TOO_LARGE"
  );
}

function ownBoundedBody(bytes: Uint8Array): Uint8Array<ArrayBuffer> {
  if (
    bytes.buffer instanceof ArrayBuffer &&
    bytes.byteOffset === 0 &&
    bytes.byteLength === bytes.buffer.byteLength
  ) {
    return bytes as Uint8Array<ArrayBuffer>;
  }
  return Uint8Array.from(bytes);
}

function assertExactUpload(
  uploaded: Awaited<ReturnType<SnapshotUploadApi["uploadSnapshot"]>>,
  expected: SnapshotMetadata,
): void {
  if (!sameSnapshotMetadata(uploaded.snapshot, expected)) {
    throw new CloudApiError(200, "snapshot-identity-mismatch");
  }
}

function sameSnapshotMetadata(left: SnapshotMetadata, right: SnapshotMetadata): boolean {
  return (
    left.sessionId === right.sessionId &&
    left.snapshotId === right.snapshotId &&
    left.engineId === right.engineId &&
    left.sessionEpoch === right.sessionEpoch &&
    left.cutEventSeq === right.cutEventSeq &&
    left.nextPtyOffset === right.nextPtyOffset &&
    left.compression === right.compression &&
    left.compressedLength === right.compressedLength &&
    left.uncompressedLength === right.uncompressedLength &&
    left.sha256 === right.sha256
  );
}

function raceAbort<T>(operation: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal === undefined) return operation;
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise((resolve, reject) => {
    const onAbort = () => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    void operation.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}

function delay(milliseconds: number, signal?: AbortSignal): Promise<void> {
  if (milliseconds === 0) return Promise.resolve();
  if (signal?.aborted) return Promise.reject(signal.reason);
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, milliseconds);
    const onAbort = () => {
      clearTimeout(timeout);
      reject(signal?.reason);
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function positiveInteger(value: number, label: string): number {
  if (!Number.isInteger(value) || value <= 0) throw new RangeError(`${label} must be positive`);
  return value;
}

function nonNegativeInteger(value: number, label: string): number {
  if (!Number.isInteger(value) || value < 0) throw new RangeError(`${label} must not be negative`);
  return value;
}
