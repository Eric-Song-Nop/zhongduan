import { createHash } from "node:crypto";
import { constants as zlibConstants, zstdCompressSync } from "node:zlib";

import {
  MAX_SNAPSHOT_UNCOMPRESSED_BYTES,
  SNAPSHOT_MEDIA_TYPE,
  SnapshotHeader,
} from "@zhongduan/protocol";
import { describe, expect, it, vi } from "vitest";

import { HttpSnapshotTransport } from "./http-snapshot-transport";
import type { SnapshotManifest } from "./types";

function manifest(bytes: Uint8Array, overrides: Partial<SnapshotManifest> = {}): SnapshotManifest {
  return {
    type: "snapshot-manifest",
    snapshotId: "snapshot_0000000001",
    engineId: "eng1-test",
    sessionEpoch: "7",
    streamId: 41,
    deliveryGeneration: "3",
    cutEventSeq: "10",
    nextPtyOffset: "100",
    commitEventSeq: "10",
    commitPtyOffset: "100",
    compression: "none",
    compressedLength: String(bytes.byteLength),
    uncompressedLength: String(bytes.byteLength),
    sha256: createHash("sha256").update(bytes).digest("hex"),
    downloadPath: "/api/v1/sessions/session_000000000001/snapshots/snapshot_0000000001",
    restoreThrough: "finish",
    ...overrides,
  };
}

function responseHeaders(snapshotManifest: SnapshotManifest): Headers {
  return new Headers({
    "content-length": snapshotManifest.compressedLength,
    "content-type": SNAPSHOT_MEDIA_TYPE,
    [SnapshotHeader.compression]: snapshotManifest.compression,
    [SnapshotHeader.compressedLength]: snapshotManifest.compressedLength,
    [SnapshotHeader.cutEventSeq]: snapshotManifest.cutEventSeq,
    [SnapshotHeader.engineId]: snapshotManifest.engineId,
    [SnapshotHeader.nextPtyOffset]: snapshotManifest.nextPtyOffset,
    [SnapshotHeader.sessionEpoch]: snapshotManifest.sessionEpoch,
    [SnapshotHeader.sha256]: snapshotManifest.sha256,
    [SnapshotHeader.uncompressedLength]: snapshotManifest.uncompressedLength,
  });
}

function snapshotResponse(bytes: Uint8Array, snapshotManifest: SnapshotManifest): Response {
  const body = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(body).set(bytes);
  return new Response(body, { headers: responseHeaders(snapshotManifest) });
}

function concatenate(...parts: Uint8Array[]): Uint8Array {
  const combined = new Uint8Array(parts.reduce((length, part) => length + part.byteLength, 0));
  let offset = 0;
  for (const part of parts) {
    combined.set(part, offset);
    offset += part.byteLength;
  }
  return combined;
}

async function loadZstd(
  compressed: Uint8Array,
  uncompressedLength: number,
  maxUncompressedBytes?: number,
): Promise<Uint8Array> {
  const snapshotManifest = manifest(compressed, {
    compression: "zstd",
    uncompressedLength: String(uncompressedLength),
  });
  const transport = new HttpSnapshotTransport({
    fetch: async () => snapshotResponse(compressed, snapshotManifest),
    ...(maxUncompressedBytes === undefined ? {} : { maxUncompressedBytes }),
  });
  return transport.load(snapshotManifest, new AbortController().signal);
}

describe("HttpSnapshotTransport", () => {
  it("does not issue a request for an already-aborted restore", async () => {
    const bytes = new Uint8Array([1, 2, 3, 4]);
    const fetchSnapshot = vi.fn<typeof fetch>();
    const transport = new HttpSnapshotTransport({ fetch: fetchSnapshot });
    const abort = new AbortController();
    abort.abort();

    await expect(transport.load(manifest(bytes), abort.signal)).rejects.toMatchObject({
      name: "AbortError",
    });
    expect(fetchSnapshot).not.toHaveBeenCalled();
  });

  it("cancels a stream whose fetch implementation ignores abort", async () => {
    const bytes = new Uint8Array([1, 2, 3, 4]);
    const snapshotManifest = manifest(bytes);
    const abort = new AbortController();
    const cancel = vi.fn();
    let pulled = false;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (pulled) return;
        pulled = true;
        controller.enqueue(bytes.subarray(0, 2));
        abort.abort();
      },
      cancel,
    });
    const transport = new HttpSnapshotTransport({
      fetch: async () => new Response(body, { headers: responseHeaders(snapshotManifest) }),
    });

    await expect(transport.load(snapshotManifest, abort.signal)).rejects.toMatchObject({
      name: "AbortError",
    });
    expect(cancel).toHaveBeenCalledOnce();
  });

  it("streams and authenticates an immutable snapshot", async () => {
    const bytes = new Uint8Array([1, 2, 3, 4]);
    const snapshotManifest = manifest(bytes);
    const fetchSnapshot = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
      snapshotResponse(bytes, snapshotManifest),
    );
    const transport = new HttpSnapshotTransport({
      fetch: fetchSnapshot,
      getHeaders: () => ({ Authorization: "Bearer capability" }),
    });

    const restored = await transport.load(snapshotManifest, new AbortController().signal);

    expect(restored).toEqual(bytes);
    const headers = new Headers(fetchSnapshot.mock.calls[0]?.[1]?.headers);
    expect(headers.get("authorization")).toBe("Bearer capability");
    expect(headers.get("accept")).toBe(SNAPSHOT_MEDIA_TYPE);
    expect(fetchSnapshot.mock.calls[0]?.[1]).toMatchObject({
      credentials: "same-origin",
      redirect: "error",
    });
  });

  it.each([
    ["content-type", "application/octet-stream"],
    ["content-length", "04"],
    [SnapshotHeader.compression, "zstd"],
    [SnapshotHeader.engineId, "eng1-other"],
    [SnapshotHeader.sessionEpoch, "8"],
    [SnapshotHeader.cutEventSeq, "11"],
    [SnapshotHeader.nextPtyOffset, "101"],
    [SnapshotHeader.sha256, "f".repeat(64)],
    [SnapshotHeader.compressedLength, "04"],
    [SnapshotHeader.uncompressedLength, "5"],
    ["content-encoding", "identity"],
  ])("cancels before reading when response header %s differs", async (name, value) => {
    const bytes = new Uint8Array([1, 2, 3, 4]);
    const snapshotManifest = manifest(bytes);
    const cancel = vi.fn();
    const body = new ReadableStream<Uint8Array>({ cancel });
    const headers = responseHeaders(snapshotManifest);
    headers.set(name, value);
    const transport = new HttpSnapshotTransport({
      fetch: async () => new Response(body, { headers }),
    });

    await expect(transport.load(snapshotManifest, new AbortController().signal)).rejects.toThrow(
      /snapshot/u,
    );
    expect(cancel).toHaveBeenCalledOnce();
  });

  it("rejects a body larger than its authenticated compressed length", async () => {
    const expected = new Uint8Array([1, 2, 3, 4]);
    const snapshotManifest = manifest(expected);
    const transport = new HttpSnapshotTransport({
      fetch: async () => snapshotResponse(new Uint8Array([1, 2, 3, 4, 5]), snapshotManifest),
    });

    await expect(transport.load(snapshotManifest, new AbortController().signal)).rejects.toThrow(
      "snapshot exceeds manifest length",
    );
  });

  it("rejects a truncated body", async () => {
    const expected = new Uint8Array([1, 2, 3, 4]);
    const snapshotManifest = manifest(expected);
    const transport = new HttpSnapshotTransport({
      fetch: async () => snapshotResponse(expected.subarray(0, 3), snapshotManifest),
    });

    await expect(transport.load(snapshotManifest, new AbortController().signal)).rejects.toThrow(
      "snapshot is truncated",
    );
  });

  it("rejects oversized metadata before issuing a request", async () => {
    const fetchSnapshot = vi.fn<typeof fetch>();
    const transport = new HttpSnapshotTransport({
      fetch: fetchSnapshot,
      maxCompressedBytes: 3,
    });

    await expect(
      transport.load(manifest(new Uint8Array(4)), new AbortController().signal),
    ).rejects.toThrow("compressed snapshot exceeds configured limit");
    expect(fetchSnapshot).not.toHaveBeenCalled();
  });

  it("uses the relay snapshot budgets by default", async () => {
    const bytes = new Uint8Array([1]);
    const fetchSnapshot = vi.fn<typeof fetch>();
    const transport = new HttpSnapshotTransport({ fetch: fetchSnapshot });

    await expect(
      transport.load(
        manifest(bytes, {
          compressedLength: String(32 * 1024 * 1024 + 1),
          uncompressedLength: String(32 * 1024 * 1024 + 1),
        }),
        new AbortController().signal,
      ),
    ).rejects.toThrow(`must not exceed ${32 * 1024 * 1024} bytes`);
    await expect(
      transport.load(
        manifest(bytes, {
          compression: "zstd",
          uncompressedLength: String(128 * 1024 * 1024 + 1),
        }),
        new AbortController().signal,
      ),
    ).rejects.toThrow(`must not exceed ${128 * 1024 * 1024} bytes`);
    expect(fetchSnapshot).not.toHaveBeenCalled();
  });

  it("rejects a non-protocol download path before forwarding credentials", async () => {
    const bytes = new Uint8Array([1, 2, 3, 4]);
    const fetchSnapshot = vi.fn<typeof fetch>();
    const transport = new HttpSnapshotTransport({
      fetch: fetchSnapshot,
      getHeaders: () => ({ Authorization: "Bearer capability" }),
    });

    await expect(
      transport.load(
        manifest(bytes, {
          downloadPath: "/api/v1/sessions/short/snapshots/snapshot_0000000001",
        }),
        new AbortController().signal,
      ),
    ).rejects.toThrow("snapshot download path is not session scoped");
    expect(fetchSnapshot).not.toHaveBeenCalled();
  });

  it("rejects a blob whose digest differs from published metadata", async () => {
    const bytes = new Uint8Array([1, 2, 3, 4]);
    const snapshotManifest = manifest(bytes, { sha256: "f".repeat(64) });
    const transport = new HttpSnapshotTransport({
      fetch: async () => snapshotResponse(bytes, snapshotManifest),
    });

    await expect(transport.load(snapshotManifest, new AbortController().signal)).rejects.toThrow(
      "snapshot SHA-256 mismatch",
    );
  });

  it("decompresses a bounded zstd snapshot with the default browser codec", async () => {
    const snapshot = new TextEncoder().encode("ghostty snapshot fixture".repeat(16_384));
    const compressed = new Uint8Array(zstdCompressSync(snapshot));
    const restored = await loadZstd(compressed, snapshot.byteLength);

    expect(restored).toEqual(snapshot);
  });

  it("accepts a standard zstd frame with its optional checksum", async () => {
    const snapshot = new TextEncoder().encode("checksummed Ghostty snapshot".repeat(64));
    const compressed = new Uint8Array(
      zstdCompressSync(snapshot, {
        params: { [zlibConstants.ZSTD_c_checksumFlag]: 1 },
      }),
    );

    await expect(loadZstd(compressed, snapshot.byteLength)).resolves.toEqual(snapshot);
  });

  it.each([
    ["shorter", -1],
    ["longer", 1],
  ])("rejects a manifest %s than the real zstd output", async (_label, difference) => {
    const snapshot = new TextEncoder().encode("measured zstd output".repeat(64));
    const compressed = new Uint8Array(zstdCompressSync(snapshot));

    await expect(loadZstd(compressed, snapshot.byteLength + difference)).rejects.toThrow(
      "zstd frame content size does not match manifest",
    );
  });

  it("measures streamed output when a real zstd frame omits content size", async () => {
    const snapshot = new TextEncoder().encode("content-size-free zstd output".repeat(64));
    const compressed = new Uint8Array(
      zstdCompressSync(snapshot, {
        params: { [zlibConstants.ZSTD_c_contentSizeFlag]: 0 },
      }),
    );

    await expect(loadZstd(compressed, snapshot.byteLength)).resolves.toEqual(snapshot);
    await expect(loadZstd(compressed, snapshot.byteLength + 1)).rejects.toThrow(
      "snapshot decompressed length does not match manifest",
    );
    await expect(loadZstd(compressed, snapshot.byteLength - 1)).rejects.toThrow(
      "snapshot decompressed length exceeds configured limit",
    );
  });

  it.each([
    [
      "content size",
      new Uint8Array([0x28, 0xb5, 0x2f, 0xfd, 0xa0, 0x01, 0x00, 0x00, 0x08, 0x01, 0x00, 0x00]),
      "zstd frame content size exceeds configured limit",
    ],
    [
      "window",
      new Uint8Array([0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x90, 0x01, 0x00, 0x00]),
      "zstd frame window exceeds configured limit",
    ],
    [
      "block",
      new Uint8Array([0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x00, 0x09, 0x20, 0x00]),
      "zstd frame block exceeds its bounded window",
    ],
  ])("rejects an oversized zstd %s before decoding", async (_label, compressed, message) => {
    await expect(loadZstd(compressed, 1)).rejects.toThrow(message);
  });

  it("does not allow configuration to loosen the protocol window cap", async () => {
    const oversizedWindow = new Uint8Array([0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x90, 0x01, 0x00, 0x00]);

    await expect(loadZstd(oversizedWindow, 1, MAX_SNAPSHOT_UNCOMPRESSED_BYTES * 2)).rejects.toThrow(
      "zstd frame window exceeds configured limit",
    );
  });

  it("rejects a skippable zstd frame", async () => {
    const skippable = new Uint8Array([0x50, 0x2a, 0x4d, 0x18, 0x00, 0x00, 0x00, 0x00]);

    await expect(loadZstd(skippable, 1)).rejects.toThrow("snapshot is not a standard zstd frame");
  });

  it.each([
    ["concatenated frame", (frame: Uint8Array) => concatenate(frame, frame)],
    ["trailing byte", (frame: Uint8Array) => concatenate(frame, new Uint8Array([0]))],
  ])("rejects zstd input with a %s", async (_label, append) => {
    const snapshot = new TextEncoder().encode("one exact zstd frame".repeat(64));
    const frame = new Uint8Array(zstdCompressSync(snapshot));

    await expect(loadZstd(append(frame), snapshot.byteLength)).rejects.toThrow(
      "zstd frame has trailing or concatenated data",
    );
  });

  it("rejects a truncated real zstd frame", async () => {
    const snapshot = new TextEncoder().encode("complete zstd frame".repeat(64));
    const frame = new Uint8Array(zstdCompressSync(snapshot));
    const truncated = frame.slice(0, -1);

    await expect(loadZstd(truncated, snapshot.byteLength)).rejects.toThrow(
      "zstd frame is truncated",
    );
  });

  it("does not return bytes when cancellation happens during decompression", async () => {
    const compressed = new Uint8Array([1, 2, 3, 4]);
    const snapshot = new Uint8Array([5, 6, 7, 8]);
    const snapshotManifest = manifest(compressed, {
      compression: "zstd",
      uncompressedLength: String(snapshot.byteLength),
    });
    const abort = new AbortController();
    const transport = new HttpSnapshotTransport({
      fetch: async () => snapshotResponse(compressed, snapshotManifest),
      decompressZstd: async () => {
        abort.abort();
        return snapshot;
      },
    });

    await expect(transport.load(snapshotManifest, abort.signal)).rejects.toMatchObject({
      name: "AbortError",
    });
  });
});
