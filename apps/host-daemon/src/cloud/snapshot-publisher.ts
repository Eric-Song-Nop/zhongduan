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

export interface SnapshotPublisherOptions {
  api: SnapshotUploadApi;
  buildMinIntervalMs?: number;
  capabilities: HostCapabilityProvider;
  compress?: SnapshotCompressor;
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
  readonly body: Uint8Array<ArrayBuffer>;
  readonly metadata: SnapshotMetadata;
}

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

export class SnapshotPublisher {
  readonly #api: SnapshotUploadApi;
  readonly #buildMinIntervalMs: number;
  readonly #capabilities: HostCapabilityProvider;
  readonly #compress: SnapshotCompressor;
  readonly #failureBackoffBaseMs: number;
  readonly #failureBackoffMaxMs: number;
  readonly #monotonicNow: () => number;
  readonly #random: () => number;
  readonly #retryAttempts: number;
  readonly #retryDelayMs: number;
  readonly #sessionId: string;
  readonly #unavailableRetryMs: number;

  #buildInFlight: Promise<PendingSnapshot> | undefined;
  #failureCount = 0;
  #nextBuildAt = 0;
  #nextUploadAt = 0;
  #pending: PendingSnapshot | undefined;

  constructor(options: SnapshotPublisherOptions) {
    this.#api = options.api;
    this.#buildMinIntervalMs = nonNegativeInteger(
      options.buildMinIntervalMs ?? 1_000,
      "buildMinIntervalMs",
    );
    this.#capabilities = options.capabilities;
    this.#compress = options.compress ?? compressZstd;
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

  async publish(snapshot: SnapshotCapture, signal?: AbortSignal): Promise<PublishedSnapshot> {
    signal?.throwIfAborted();
    const existing = this.resumePending(signal);
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
    return this.#uploadPending(pending, signal);
  }

  resumePending(signal?: AbortSignal): Promise<PublishedSnapshot> | undefined {
    signal?.throwIfAborted();
    const pending =
      this.#pending === undefined ? this.#buildInFlight : Promise.resolve(this.#pending);
    if (pending === undefined) return undefined;
    return this.#resumePending(pending, signal);
  }

  async #resumePending(
    pending: Promise<PendingSnapshot>,
    signal?: AbortSignal,
  ): Promise<PublishedSnapshot> {
    const built = await raceAbort(pending, signal);
    signal?.throwIfAborted();
    return this.#uploadPending(built, signal);
  }

  #startBuild(snapshot: SnapshotCapture): Promise<PendingSnapshot> {
    if (this.#buildInFlight !== undefined || this.#pending !== undefined) {
      throw new Error("snapshot publisher already owns a pending build");
    }
    const operation = this.#build(snapshot).then((pending) => {
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
    return { body, metadata };
  }

  async #uploadPending(pending: PendingSnapshot, signal?: AbortSignal): Promise<PublishedSnapshot> {
    const now = this.#monotonicNow();
    if (now < this.#nextUploadAt) {
      throw new RetryableSnapshotPublishError(
        new Error("snapshot upload is rate limited"),
        Math.ceil(this.#nextUploadAt - now),
      );
    }

    let lastError: unknown;
    for (let attempt = 1; attempt <= this.#retryAttempts; attempt += 1) {
      signal?.throwIfAborted();
      try {
        let capability = await raceAbort(this.#capabilities.current(signal), signal);
        try {
          const uploaded = await raceAbort(
            this.#api.uploadSnapshot(pending.metadata, pending.body, capability, signal),
            signal,
          );
          assertExactUpload(uploaded, pending.metadata);
          return this.#rememberPublished(pending);
        } catch (error) {
          if (error instanceof CloudApiError && (error.status === 401 || error.status === 403)) {
            capability = await raceAbort(
              this.#capabilities.recoverRejected(capability, signal),
              signal,
            );
            const uploaded = await raceAbort(
              this.#api.uploadSnapshot(pending.metadata, pending.body, capability, signal),
              signal,
            );
            assertExactUpload(uploaded, pending.metadata);
            return this.#rememberPublished(pending);
          }
          throw error;
        }
      } catch (error) {
        signal?.throwIfAborted();
        if (isCleanupSafeTerminal(error)) {
          if (this.#pending === pending) this.#pending = undefined;
          this.#failureCount = 0;
          this.#nextUploadAt = 0;
          throw new SnapshotCleanupConfirmedError(error);
        }
        lastError = error;
      }
      if (attempt === this.#retryAttempts) {
        throw new RetryableSnapshotPublishError(lastError, this.#recordTransientFailure());
      }
      await delay(this.#retryDelayMs, signal);
    }
    throw lastError;
  }

  #rememberPublished(pending: PendingSnapshot): PublishedSnapshot {
    if (this.#pending === pending) this.#pending = undefined;
    this.#failureCount = 0;
    this.#nextUploadAt = 0;
    return { metadata: pending.metadata };
  }

  #recordTransientFailure(): number {
    this.#failureCount = Math.min(this.#failureCount + 1, 30);
    const exponential = Math.min(
      this.#failureBackoffMaxMs,
      this.#failureBackoffBaseMs * 2 ** (this.#failureCount - 1),
    );
    const jitter = 0.8 + Math.min(1, Math.max(0, this.#random())) * 0.4;
    const delayMs = Math.min(this.#failureBackoffMaxMs, Math.ceil(exponential * jitter));
    this.#nextUploadAt = Math.max(this.#nextUploadAt, this.#monotonicNow() + delayMs);
    return Math.max(1, Math.ceil(this.#nextUploadAt - this.#monotonicNow()));
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
