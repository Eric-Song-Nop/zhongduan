import { randomBytes } from "node:crypto";
import { zstdDecompress } from "node:zlib";

import {
  MAX_SNAPSHOT_COMPRESSED_BYTES,
  SnapshotHeader,
  type SnapshotMetadata,
} from "@zhongduan/protocol";
import { describe, expect, it, vi } from "vitest";

import type { SnapshotCapture } from "../session";
import { BootstrapTokenUnavailableError } from "./capability-manager";
import { CloudApiClient, CloudApiError, CloudTransportError } from "./cloud-api";
import {
  SnapshotPublisher,
  SnapshotUnavailableError,
  type HostCapabilityProvider,
  type SnapshotUploadApi,
} from "./snapshot-publisher";

function snapshot(byte = 0x41): SnapshotCapture {
  return {
    bytes: Uint8Array.of(byte, byte + 1, byte + 2),
    cutEventSeq: BigInt(byte),
    encodeMs: 1,
    engineId: "ghostty/test",
    nextPtyOffset: BigInt(byte + 10),
    sessionEpoch: 7n,
  };
}

function capabilities(): HostCapabilityProvider {
  return {
    current: async () => "host-cap",
    recoverRejected: async () => "host-cap-recovered",
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

function snapshotMetadataFromRequest(
  input: RequestInfo | URL,
  init: RequestInit | undefined,
): SnapshotMetadata {
  const url = new URL(input instanceof Request ? input.url : input.toString());
  const headers = new Headers(init?.headers);
  return {
    sessionId: "session_AAAAAAAAA",
    snapshotId: decodeURIComponent(url.pathname.split("/").at(-1)!),
    engineId: headers.get(SnapshotHeader.engineId)!,
    sessionEpoch: headers.get(SnapshotHeader.sessionEpoch)!,
    cutEventSeq: headers.get(SnapshotHeader.cutEventSeq)!,
    nextPtyOffset: headers.get(SnapshotHeader.nextPtyOffset)!,
    compression: "zstd",
    compressedLength: headers.get(SnapshotHeader.compressedLength)!,
    uncompressedLength: headers.get(SnapshotHeader.uncompressedLength)!,
    sha256: headers.get(SnapshotHeader.sha256)!,
  };
}

describe("SnapshotPublisher", () => {
  it("retries 409 with the same immutable id, metadata, and compressed body", async () => {
    const calls: Array<{ body: Uint8Array; metadata: SnapshotMetadata }> = [];
    const api: SnapshotUploadApi = {
      uploadSnapshot: vi.fn(async (metadata, body) => {
        calls.push({ body, metadata });
        if (calls.length === 1) throw new CloudApiError(409, "snapshot-conflict");
        return { created: false, snapshot: metadata };
      }),
    };
    const publisher = new SnapshotPublisher({
      api,
      capabilities: capabilities(),
      compress: async () => Uint8Array.of(1, 2, 3),
      retryAttempts: 2,
      retryDelayMs: 0,
      sessionId: "session_AAAAAAAAA",
    });

    const published = await publisher.publish(snapshot());

    expect(calls).toHaveLength(2);
    expect(calls[0]!.metadata).toBe(calls[1]!.metadata);
    expect(calls[0]!.body).toBe(calls[1]!.body);
    expect(calls[0]!.metadata.snapshotId).toBe(published.metadata.snapshotId);
  });

  it("retains the immutable upload after a local or response contract error", async () => {
    let now = 0;
    const calls: Array<{ body: Uint8Array; metadata: SnapshotMetadata }> = [];
    const uploadSnapshot = vi.fn<SnapshotUploadApi["uploadSnapshot"]>(async () => {
      throw new RangeError("response contract bug");
    });
    const publisher = new SnapshotPublisher({
      api: { uploadSnapshot },
      capabilities: capabilities(),
      compress: async () => Uint8Array.of(1),
      failureBackoffBaseMs: 1,
      failureBackoffMaxMs: 1,
      monotonicNow: () => now,
      retryAttempts: 1,
      retryDelayMs: 0,
      sessionId: "session_AAAAAAAAA",
    });

    await expect(publisher.publish(snapshot())).rejects.toMatchObject({
      name: "RetryableSnapshotPublishError",
      cause: expect.objectContaining({ message: "response contract bug" }),
    });
    expect(uploadSnapshot).toHaveBeenCalledOnce();
    now = 1;
    uploadSnapshot.mockImplementationOnce(async (metadata, body) => {
      calls.push({ body, metadata });
      return { created: false, snapshot: metadata };
    });
    await expect(publisher.resumePending()).resolves.toMatchObject({
      metadata: { snapshotId: expect.any(String) },
    });
    expect(uploadSnapshot).toHaveBeenCalledTimes(2);
  });

  it.each([
    ["transport", new CloudTransportError(new Error("refresh offline"))],
    ["bootstrap credential", new BootstrapTokenUnavailableError(new Error("token missing"))],
  ])("applies the same bounded retry policy to %s failures", async (_name, firstError) => {
    const current = vi
      .fn<HostCapabilityProvider["current"]>()
      .mockRejectedValueOnce(firstError)
      .mockResolvedValue("host-cap");
    const uploadSnapshot = vi.fn(async (uploaded: SnapshotMetadata) => ({
      created: true,
      snapshot: uploaded,
    }));
    const publisher = new SnapshotPublisher({
      api: { uploadSnapshot },
      capabilities: { current, recoverRejected: async () => "host-cap-recovered" },
      compress: async () => Uint8Array.of(1),
      retryAttempts: 2,
      retryDelayMs: 0,
      sessionId: "session_AAAAAAAAA",
    });

    await expect(publisher.publish(snapshot())).resolves.toMatchObject({
      metadata: { snapshotId: expect.any(String) },
    });
    expect(current).toHaveBeenCalledTimes(2);
    expect(uploadSnapshot).toHaveBeenCalledOnce();
  });

  it("reclaims a rejected 403 credential and retries the same immutable upload", async () => {
    const uploadSnapshot = vi
      .fn<SnapshotUploadApi["uploadSnapshot"]>()
      .mockRejectedValueOnce(new CloudApiError(403, "revoked-capability"))
      .mockImplementation(async (metadata) => ({ created: true, snapshot: metadata }));
    const recoverRejected = vi.fn(async () => "rotated-host-cap");
    const publisher = new SnapshotPublisher({
      api: { uploadSnapshot },
      capabilities: { current: async () => "revoked-host-cap", recoverRejected },
      compress: async () => Uint8Array.of(1),
      retryAttempts: 1,
      retryDelayMs: 0,
      sessionId: "session_AAAAAAAAA",
    });

    await expect(publisher.publish(snapshot())).resolves.toMatchObject({
      metadata: { snapshotId: expect.any(String) },
    });
    expect(recoverRejected).toHaveBeenCalledWith("revoked-host-cap", undefined);
    expect(uploadSnapshot).toHaveBeenCalledTimes(2);
    expect(uploadSnapshot.mock.calls[0]![0]).toBe(uploadSnapshot.mock.calls[1]![0]);
    expect(uploadSnapshot.mock.calls[0]![1]).toBe(uploadSnapshot.mock.calls[1]![1]);
  });

  it("keeps one build alive after caller abort and resumes it without recompressing", async () => {
    const firstCompression = deferred<Uint8Array>();
    let active = 0;
    let maximumActive = 0;
    let compressionCalls = 0;
    const compress = vi.fn((bytes: Uint8Array) => {
      compressionCalls += 1;
      active += 1;
      maximumActive = Math.max(maximumActive, active);
      const operation = compressionCalls === 1 ? firstCompression.promise : Promise.resolve(bytes);
      return operation.then(
        (value) => {
          active -= 1;
          return value;
        },
        (error: unknown) => {
          active -= 1;
          throw error;
        },
      );
    });
    const publisher = new SnapshotPublisher({
      api: {
        uploadSnapshot: async (metadata) => ({ created: true, snapshot: metadata }),
      },
      buildMinIntervalMs: 0,
      capabilities: capabilities(),
      compress,
      retryDelayMs: 0,
      sessionId: "session_AAAAAAAAA",
    });
    const controller = new AbortController();
    const abandoned = publisher.publish(snapshot(0x41), controller.signal);
    const abandonedExpectation = expect(abandoned).rejects.toMatchObject({ name: "AbortError" });
    await Promise.resolve();
    controller.abort(new DOMException("freeze timed out", "AbortError"));
    await abandonedExpectation;

    const resumed = publisher.resumePending();
    expect(resumed).toBeDefined();
    expect(compress).toHaveBeenCalledOnce();

    firstCompression.resolve(Uint8Array.of(1));
    await expect(resumed).resolves.toMatchObject({ metadata: { cutEventSeq: "65" } });
    await expect(publisher.publish(snapshot(0x61))).resolves.toMatchObject({
      metadata: { cutEventSeq: "97" },
    });
    expect(compress).toHaveBeenCalledTimes(2);
    expect(maximumActive).toBe(1);
  });

  it("survives more than four lost responses with one id, body, and compression", async () => {
    let now = 0;
    const calls: Array<{ body: Uint8Array; metadata: SnapshotMetadata }> = [];
    const uploadSnapshot = vi.fn(async (metadata: SnapshotMetadata, body: Uint8Array) => {
      calls.push({ body, metadata });
      if (calls.length <= 5) throw new CloudTransportError(new Error("response lost"));
      return { created: false, snapshot: metadata };
    });
    const compress = vi.fn(async () => Uint8Array.of(1, 2, 3));
    const publisher = new SnapshotPublisher({
      api: { uploadSnapshot },
      buildMinIntervalMs: 0,
      capabilities: capabilities(),
      compress,
      failureBackoffBaseMs: 1,
      failureBackoffMaxMs: 1,
      monotonicNow: () => now,
      retryAttempts: 1,
      retryDelayMs: 0,
      sessionId: "session_AAAAAAAAA",
    });

    await expect(publisher.publish(snapshot())).rejects.toMatchObject({
      name: "RetryableSnapshotPublishError",
    });
    for (let attempt = 1; attempt < 5; attempt += 1) {
      now += 1;
      await expect(publisher.resumePending()).rejects.toMatchObject({
        name: "RetryableSnapshotPublishError",
      });
    }
    now += 1;
    const published = await publisher.resumePending();

    expect(published?.metadata.snapshotId).toBe(calls[0]!.metadata.snapshotId);
    expect(compress).toHaveBeenCalledOnce();
    expect(new Set(calls.map(({ metadata }) => metadata.snapshotId))).toEqual(
      new Set([calls[0]!.metadata.snapshotId]),
    );
    expect(calls.every(({ metadata }) => metadata === calls[0]!.metadata)).toBe(true);
    expect(calls.every(({ body }) => body === calls[0]!.body)).toBe(true);
  });

  it("bounds a black-holed upload wait and resumes the same pending body", async () => {
    const blackHole = deferred<never>();
    const calls: Array<{ body: Uint8Array; metadata: SnapshotMetadata }> = [];
    const uploadSnapshot = vi
      .fn<SnapshotUploadApi["uploadSnapshot"]>()
      .mockImplementationOnce((metadata, body) => {
        calls.push({ body, metadata });
        return blackHole.promise;
      })
      .mockImplementationOnce(async (metadata, body) => {
        calls.push({ body, metadata });
        return { created: false, snapshot: metadata };
      });
    const publisher = new SnapshotPublisher({
      api: { uploadSnapshot },
      capabilities: capabilities(),
      compress: async () => Uint8Array.of(1),
      retryAttempts: 1,
      retryDelayMs: 0,
      sessionId: "session_AAAAAAAAA",
    });
    const controller = new AbortController();
    const abandoned = publisher.publish(snapshot(), controller.signal);
    const rejected = expect(abandoned).rejects.toMatchObject({ name: "AbortError" });
    await vi.waitFor(() => expect(uploadSnapshot).toHaveBeenCalledOnce());
    controller.abort(new DOMException("connection replaced", "AbortError"));
    await rejected;

    await expect(publisher.resumePending()).resolves.toMatchObject({
      metadata: { snapshotId: calls[0]!.metadata.snapshotId },
    });
    expect(calls[1]!.metadata).toBe(calls[0]!.metadata);
    expect(calls[1]!.body).toBe(calls[0]!.body);
  });

  it("escapes a 201 response-body black hole and retries its exact request", async () => {
    let blackHoleController: ReadableStreamDefaultController<Uint8Array> | undefined;
    const requestBodies: BodyInit[] = [];
    let requestedMetadata: SnapshotMetadata | undefined;
    const cloudFetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      requestBodies.push(init!.body!);
      requestedMetadata = snapshotMetadataFromRequest(input, init);
      if (cloudFetch.mock.calls.length === 1) {
        return new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              blackHoleController = controller;
              controller.enqueue(new TextEncoder().encode('{"created":'));
            },
          }),
          { status: 201, headers: { "content-type": "application/json" } },
        );
      }
      return new Response(JSON.stringify({ created: false, snapshot: requestedMetadata }), {
        status: 201,
        headers: { "content-type": "application/json" },
      });
    });
    const publisher = new SnapshotPublisher({
      api: new CloudApiClient("https://cloud.example", { fetch: cloudFetch as typeof fetch }),
      capabilities: capabilities(),
      compress: async () => Uint8Array.of(1),
      retryAttempts: 1,
      retryDelayMs: 0,
      sessionId: "session_AAAAAAAAA",
    });
    const controller = new AbortController();
    const abandoned = publisher.publish(snapshot(), controller.signal);
    await vi.waitFor(() => expect(cloudFetch).toHaveBeenCalledOnce());
    const rejected = expect(abandoned).rejects.toMatchObject({ name: "AbortError" });
    controller.abort(new DOMException("publish deadline elapsed", "AbortError"));
    await rejected;
    blackHoleController?.close();

    await expect(publisher.resumePending()).resolves.toMatchObject({
      metadata: { snapshotId: requestedMetadata!.snapshotId },
    });
    expect(cloudFetch).toHaveBeenCalledTimes(2);
    expect(requestBodies[1]).toBe(requestBodies[0]);
  });

  it("retains the exact request after an unexpected successful status", async () => {
    let now = 0;
    const requestBodies: BodyInit[] = [];
    const requestedIds: string[] = [];
    const cloudFetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const requestedMetadata = snapshotMetadataFromRequest(input, init);
      requestBodies.push(init!.body!);
      requestedIds.push(requestedMetadata.snapshotId);
      return new Response(
        JSON.stringify({
          created: cloudFetch.mock.calls.length === 1,
          snapshot: requestedMetadata,
        }),
        {
          status: cloudFetch.mock.calls.length === 1 ? 202 : 200,
          headers: { "content-type": "application/json" },
        },
      );
    });
    const publisher = new SnapshotPublisher({
      api: new CloudApiClient("https://cloud.example", { fetch: cloudFetch as typeof fetch }),
      capabilities: capabilities(),
      compress: async () => Uint8Array.of(1),
      failureBackoffBaseMs: 1,
      failureBackoffMaxMs: 1,
      monotonicNow: () => now,
      retryAttempts: 1,
      retryDelayMs: 0,
      sessionId: "session_AAAAAAAAA",
    });

    await expect(publisher.publish(snapshot())).rejects.toMatchObject({
      name: "RetryableSnapshotPublishError",
      cause: expect.objectContaining({ status: 202, errorCode: "invalid-response" }),
    });
    now = 1;
    await expect(publisher.resumePending()).resolves.toMatchObject({
      metadata: { snapshotId: requestedIds[0] },
    });

    expect(cloudFetch).toHaveBeenCalledTimes(2);
    expect(new Set(requestedIds)).toEqual(new Set([requestedIds[0]]));
    expect(requestBodies[1]).toBe(requestBodies[0]);
  });

  it("retains cursor-ahead conflicts until created:false confirms the exact object", async () => {
    let now = 0;
    const ids: string[] = [];
    const uploadSnapshot = vi.fn(async (metadata: SnapshotMetadata) => {
      ids.push(metadata.snapshotId);
      if (ids.length <= 4) throw new CloudApiError(409, "snapshot-conflict");
      return { created: false, snapshot: metadata };
    });
    const publisher = new SnapshotPublisher({
      api: { uploadSnapshot },
      buildMinIntervalMs: 0,
      capabilities: capabilities(),
      compress: async () => Uint8Array.of(1),
      failureBackoffBaseMs: 1,
      failureBackoffMaxMs: 1,
      monotonicNow: () => now,
      retryAttempts: 1,
      retryDelayMs: 0,
      sessionId: "session_AAAAAAAAA",
    });

    await publisher.publish(snapshot()).catch(() => undefined);
    for (let attempt = 1; attempt <= 4; attempt += 1) {
      now += 1;
      await publisher.resumePending()?.catch(() => undefined);
    }

    expect(ids).toHaveLength(5);
    expect(new Set(ids).size).toBe(1);
    expect(publisher.resumePending()).toBeUndefined();
  });

  it("retains invalid success and other 422 results but releases cleanup-safe checksum failure", async () => {
    let now = 0;
    const compress = vi.fn(async () => Uint8Array.of(1));
    const uploadSnapshot = vi
      .fn<SnapshotUploadApi["uploadSnapshot"]>()
      .mockImplementationOnce(async (metadata) => ({
        created: true,
        snapshot: { ...metadata, cutEventSeq: (BigInt(metadata.cutEventSeq) + 1n).toString() },
      }))
      .mockImplementationOnce(async () => {
        throw new CloudApiError(422, "unknown-error");
      })
      .mockImplementationOnce(async () => {
        throw new CloudApiError(422, "snapshot-checksum-mismatch");
      })
      .mockImplementationOnce(async (metadata) => ({ created: true, snapshot: metadata }));
    const publisher = new SnapshotPublisher({
      api: { uploadSnapshot },
      buildMinIntervalMs: 0,
      capabilities: capabilities(),
      compress,
      failureBackoffBaseMs: 1,
      failureBackoffMaxMs: 1,
      monotonicNow: () => now,
      retryAttempts: 1,
      retryDelayMs: 0,
      sessionId: "session_AAAAAAAAA",
    });

    await expect(publisher.publish(snapshot())).rejects.toMatchObject({
      name: "RetryableSnapshotPublishError",
    });
    now = 1;
    await expect(publisher.resumePending()).rejects.toMatchObject({
      name: "RetryableSnapshotPublishError",
      cause: expect.objectContaining({ status: 422, errorCode: "unknown-error" }),
    });
    now = 2;
    await expect(publisher.resumePending()).rejects.toMatchObject({
      name: "SnapshotCleanupConfirmedError",
      cause: expect.objectContaining({
        status: 422,
        errorCode: "snapshot-checksum-mismatch",
      }),
    });
    expect(publisher.resumePending()).toBeUndefined();
    await expect(publisher.publish(snapshot(0x51))).resolves.toMatchObject({
      metadata: { cutEventSeq: "81" },
    });
    expect(compress).toHaveBeenCalledTimes(2);
  });

  it("copies a small view out of an oversized backing buffer before retaining it", async () => {
    const backing = new Uint8Array(MAX_SNAPSHOT_COMPRESSED_BYTES + 1);
    const view = backing.subarray(backing.byteLength - 1);
    let retained: Uint8Array | undefined;
    const publisher = new SnapshotPublisher({
      api: {
        uploadSnapshot: async (metadata, body) => {
          retained = body;
          return { created: true, snapshot: metadata };
        },
      },
      capabilities: capabilities(),
      compress: async () => view,
      sessionId: "session_AAAAAAAAA",
    });

    await publisher.publish(snapshot());
    expect(retained?.byteLength).toBe(1);
    expect(retained?.buffer.byteLength).toBe(1);
    expect(retained).not.toBe(view);
  });

  it("emits real zstd bytes that restore the exact Ghostty snapshot payload", async () => {
    const original = snapshot();
    let uploadedBody: Uint8Array | undefined;
    const publisher = new SnapshotPublisher({
      api: {
        uploadSnapshot: async (metadata, body) => {
          uploadedBody = body;
          return { created: true, snapshot: metadata };
        },
      },
      capabilities: capabilities(),
      sessionId: "session_AAAAAAAAA",
    });

    const published = await publisher.publish(original);
    const restored = await new Promise<Uint8Array>((resolve, reject) => {
      zstdDecompress(uploadedBody!, (error, bytes) => {
        if (error !== null) reject(error);
        else resolve(new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength).slice());
      });
    });

    expect(restored).toEqual(original.bytes);
    expect(published.metadata.compression).toBe("zstd");
  });

  it("stops incompressible output at the zstd maxOutputLength before upload", async () => {
    const uploadSnapshot = vi.fn();
    const publisher = new SnapshotPublisher({
      api: { uploadSnapshot },
      capabilities: capabilities(),
      sessionId: "session_AAAAAAAAA",
    });
    const incompressible = snapshot();
    incompressible.bytes = randomBytes(MAX_SNAPSHOT_COMPRESSED_BYTES + 1024 * 1024);

    await expect(publisher.publish(incompressible)).rejects.toMatchObject({
      name: "SnapshotUnavailableError",
      retryAfterMs: 30_000,
      cause: { code: "ERR_BUFFER_TOO_LARGE" },
    } satisfies Partial<SnapshotUnavailableError>);
    expect(uploadSnapshot).not.toHaveBeenCalled();
  }, 10_000);

  it("persists a long build backoff after a compression limit without repeating work", async () => {
    let now = 0;
    const limitError = Object.assign(new Error("compressed output exceeded limit"), {
      code: "ERR_BUFFER_TOO_LARGE",
    });
    const compress = vi.fn(async () => {
      throw limitError;
    });
    const publisher = new SnapshotPublisher({
      api: { uploadSnapshot: vi.fn() },
      capabilities: capabilities(),
      compress,
      monotonicNow: () => now,
      sessionId: "session_AAAAAAAAA",
      unavailableRetryMs: 30_000,
    });

    for (now = 0; now < 60_000; now += 250) {
      await publisher.publish(snapshot()).catch(() => undefined);
    }

    expect(compress).toHaveBeenCalledTimes(2);
  });

  it("bounds permanent PUT failures over one minute with session-scoped backoff", async () => {
    let now = 0;
    const uploadSnapshot = vi.fn(async () => {
      throw new CloudApiError(503, "r2-unavailable");
    });
    const compress = vi.fn(async () => Uint8Array.of(1));
    const publisher = new SnapshotPublisher({
      api: { uploadSnapshot },
      capabilities: capabilities(),
      compress,
      failureBackoffBaseMs: 1_000,
      failureBackoffMaxMs: 30_000,
      monotonicNow: () => now,
      random: () => 0.5,
      retryAttempts: 3,
      retryDelayMs: 0,
      sessionId: "session_AAAAAAAAA",
    });

    for (now = 0; now < 60_000; now += 250) {
      await publisher.publish(snapshot()).catch(() => undefined);
    }

    expect(compress.mock.calls.length).toBeLessThanOrEqual(6);
    expect(uploadSnapshot.mock.calls.length).toBeLessThanOrEqual(18);
  });

  it("uses a fresh immutable id on the next cold build instead of permanently caching a blob", async () => {
    let now = 0;
    const publisher = new SnapshotPublisher({
      api: {
        uploadSnapshot: async (metadata) => ({ created: true, snapshot: metadata }),
      },
      capabilities: capabilities(),
      compress: async () => Uint8Array.of(1),
      monotonicNow: () => now,
      sessionId: "session_AAAAAAAAA",
    });

    const first = await publisher.publish(snapshot());
    now = 1_000;
    const replacement = await publisher.publish(snapshot());

    expect(replacement.metadata.snapshotId).not.toBe(first.metadata.snapshotId);
  });
});
