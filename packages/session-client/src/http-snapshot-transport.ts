import {
  MAX_SNAPSHOT_COMPRESSED_BYTES,
  MAX_SNAPSHOT_UNCOMPRESSED_BYTES,
  SNAPSHOT_MEDIA_TYPE,
  ServerControlFrameSchema,
  SnapshotHeader,
  SnapshotResourceIdSchema,
} from "@zhongduan/protocol";

import type { SnapshotManifest, SnapshotTransport } from "./types";
import { decompressBoundedZstdFrame } from "./zstd-frame";

export interface HttpSnapshotTransportOptions {
  fetch?: typeof fetch;
  maxCompressedBytes?: number;
  maxUncompressedBytes?: number;
  decompressZstd?: (bytes: Uint8Array, expectedLength: number) => Promise<Uint8Array>;
  getHeaders?: () => HeadersInit;
}

function configuredMaximum(
  value: number | undefined,
  protocolMaximum: number,
  field: string,
): number {
  if (value === undefined) return protocolMaximum;
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${field} must be a positive safe integer`);
  }
  return Math.min(value, protocolMaximum);
}

function parseBoundedLength(value: string, maximum: number, field: string): number {
  const parsed = BigInt(value);
  if (parsed < 0n || parsed > BigInt(maximum)) {
    throw new Error(`${field} exceeds configured limit`);
  }
  return Number(parsed);
}

function toHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function assertSessionSnapshotPath(path: string, snapshotId: string): void {
  const match = /^\/api\/v1\/sessions\/([^/?#]+)\/snapshots\/([^/?#]+)$/.exec(path);
  if (
    match === null ||
    !SnapshotResourceIdSchema.safeParse(match[1]).success ||
    !SnapshotResourceIdSchema.safeParse(snapshotId).success ||
    match[2] !== snapshotId
  ) {
    throw new Error("snapshot download path is not session scoped");
  }
}

async function cancelBody(body: ReadableStream<Uint8Array> | null, reason: unknown): Promise<void> {
  if (body === null || body.locked) return;
  await body.cancel(reason).catch(() => undefined);
}

function parseSnapshotManifest(manifest: SnapshotManifest): SnapshotManifest {
  const parsed = ServerControlFrameSchema.parse(manifest);
  if (parsed.type !== "snapshot-manifest") {
    throw new Error("expected a snapshot manifest");
  }
  return parsed;
}

function responseMetadataMismatch(
  response: Response,
  manifest: SnapshotManifest,
): string | undefined {
  if (response.headers.get("content-type") !== SNAPSHOT_MEDIA_TYPE) {
    return "snapshot content-type does not match protocol";
  }
  if (response.headers.has("content-encoding")) {
    return "snapshot content-encoding is not allowed";
  }

  const expectedHeaders: ReadonlyArray<readonly [string, string]> = [
    ["content-length", manifest.compressedLength],
    [SnapshotHeader.compression, manifest.compression],
    [SnapshotHeader.engineId, manifest.engineId],
    [SnapshotHeader.sessionEpoch, manifest.sessionEpoch],
    [SnapshotHeader.cutEventSeq, manifest.cutEventSeq],
    [SnapshotHeader.nextPtyOffset, manifest.nextPtyOffset],
    [SnapshotHeader.sha256, manifest.sha256],
    [SnapshotHeader.compressedLength, manifest.compressedLength],
    [SnapshotHeader.uncompressedLength, manifest.uncompressedLength],
  ];
  for (const [name, expected] of expectedHeaders) {
    if (response.headers.get(name) !== expected) {
      return `snapshot ${name} does not match manifest`;
    }
  }
  return undefined;
}

export class HttpSnapshotTransport implements SnapshotTransport {
  readonly #fetch: typeof fetch;
  readonly #maxCompressedBytes: number;
  readonly #maxUncompressedBytes: number;
  readonly #decompressZstd?: HttpSnapshotTransportOptions["decompressZstd"];
  readonly #getHeaders?: HttpSnapshotTransportOptions["getHeaders"];

  constructor(options: HttpSnapshotTransportOptions = {}) {
    this.#fetch = options.fetch ?? ((input, init) => globalThis.fetch(input, init));
    this.#maxCompressedBytes = configuredMaximum(
      options.maxCompressedBytes,
      MAX_SNAPSHOT_COMPRESSED_BYTES,
      "maxCompressedBytes",
    );
    this.#maxUncompressedBytes = configuredMaximum(
      options.maxUncompressedBytes,
      MAX_SNAPSHOT_UNCOMPRESSED_BYTES,
      "maxUncompressedBytes",
    );
    this.#decompressZstd =
      options.decompressZstd ??
      (async (bytes, expectedLength) =>
        decompressBoundedZstdFrame(bytes, expectedLength, this.#maxUncompressedBytes));
    this.#getHeaders = options.getHeaders;
  }

  async load(manifest: SnapshotManifest, signal: AbortSignal): Promise<Uint8Array> {
    signal.throwIfAborted();
    const expected = parseSnapshotManifest(manifest);
    assertSessionSnapshotPath(expected.downloadPath, expected.snapshotId);
    const compressedLength = parseBoundedLength(
      expected.compressedLength,
      this.#maxCompressedBytes,
      "compressed snapshot",
    );
    const uncompressedLength = parseBoundedLength(
      expected.uncompressedLength,
      this.#maxUncompressedBytes,
      "uncompressed snapshot",
    );

    const headers = new Headers(this.#getHeaders?.());
    headers.set("Accept", SNAPSHOT_MEDIA_TYPE);
    const response = await this.#fetch(expected.downloadPath, {
      credentials: "same-origin",
      headers,
      redirect: "error",
      signal,
    });
    if (signal.aborted) {
      await cancelBody(response.body, signal.reason);
      signal.throwIfAborted();
    }
    if (!response.ok || !response.body) {
      await cancelBody(response.body, "snapshot download failed");
      throw new Error(`snapshot download failed with ${response.status}`);
    }
    const metadataMismatch = responseMetadataMismatch(response, expected);
    if (metadataMismatch !== undefined) {
      await cancelBody(response.body, metadataMismatch);
      throw new Error(metadataMismatch);
    }

    const reader = response.body.getReader();
    const compressed = new Uint8Array(compressedLength);
    let received = 0;
    try {
      while (true) {
        if (signal.aborted) {
          await reader.cancel(signal.reason).catch(() => undefined);
          signal.throwIfAborted();
        }
        const { done, value } = await reader.read();
        if (signal.aborted) {
          await reader.cancel(signal.reason).catch(() => undefined);
          signal.throwIfAborted();
        }
        if (done) break;
        const nextOffset = received + value.byteLength;
        if (nextOffset > compressedLength) {
          await reader.cancel("snapshot exceeds manifest length");
          throw new Error("snapshot exceeds manifest length");
        }
        compressed.set(value, received);
        received = nextOffset;
      }
    } catch (error) {
      await reader.cancel(error).catch(() => undefined);
      throw error;
    }
    if (received !== compressedLength) {
      throw new Error("snapshot is truncated");
    }

    signal.throwIfAborted();
    const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", compressed));
    signal.throwIfAborted();
    if (toHex(digest) !== expected.sha256) {
      throw new Error("snapshot SHA-256 mismatch");
    }

    signal.throwIfAborted();
    const snapshot =
      expected.compression === "none"
        ? compressed
        : await this.#decompress(compressed, uncompressedLength);
    signal.throwIfAborted();
    if (snapshot.byteLength > this.#maxUncompressedBytes) {
      throw new Error("snapshot decompressed length exceeds configured limit");
    }
    if (snapshot.byteLength !== uncompressedLength) {
      throw new Error("snapshot decompressed length does not match manifest");
    }
    return snapshot;
  }

  async #decompress(bytes: Uint8Array, expectedLength: number): Promise<Uint8Array> {
    if (!this.#decompressZstd) {
      throw new Error("zstd snapshot support is not configured");
    }
    return this.#decompressZstd(bytes, expectedLength);
  }
}
