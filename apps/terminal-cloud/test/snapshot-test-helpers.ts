import { SNAPSHOT_MEDIA_TYPE, SnapshotHeader, type SnapshotMetadata } from "@zhongduan/protocol";
import { env, exports as workerExports } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { expect } from "vitest";
import { isSnapshotObjectKey } from "../src/worker/snapshot-contract";

export interface CreatedSession {
  hostCapability: string;
  observerCapability: string;
  sessionId: string;
  writerCapability: string;
}

export interface UploadOverrides {
  body?: Uint8Array;
  compression?: "none" | "zstd";
  compressedLength?: string;
  cutEventSeq?: string;
  engineId?: string;
  nextPtyOffset?: string;
  sessionEpoch?: string;
  sha256?: string;
  uncompressedLength?: string;
}

export const origin = "https://terminal.example.test";
export const engineId = "ghostty:test+snapshot-v1+wterm:test";
export const encoder = new TextEncoder();

export async function within<T>(promise: Promise<T>, label: string, timeoutMs = 1_000): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(() => reject(new Error(label)), timeoutMs);
      }),
    ]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}

export async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const digest = new Uint8Array(
    await crypto.subtle.digest("SHA-256", Uint8Array.from(bytes).buffer),
  );
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

export async function metadataFor(
  session: CreatedSession,
  snapshotId: string,
  body = encoder.encode("snapshot-state"),
): Promise<SnapshotMetadata> {
  return {
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
}

export async function createSession(): Promise<CreatedSession> {
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions`, {
      method: "POST",
      headers: {
        authorization: "Bearer test-bootstrap-token-with-at-least-32-bytes",
        "content-type": "application/json",
      },
      body: JSON.stringify({ engineId, sessionEpoch: "7" }),
    }),
  );
  expect(response.status).toBe(201);
  const session = await response.json<CreatedSession>();
  await installMiniflareMultipartEtagShim(session.sessionId);
  return session;
}

export function sessionStub(sessionId: string) {
  return env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${sessionId}`));
}

export function matchesSnapshotKey(key: string, sessionId: string, snapshotId: string): boolean {
  return isSnapshotObjectKey(key, sessionId, snapshotId);
}

export async function storedSnapshotKey(sessionId: string, snapshotId: string): Promise<string> {
  return runInDurableObject(sessionStub(sessionId), (_instance, durable) => {
    const published = durable.storage.sql
      .exec("SELECT object_key FROM snapshot WHERE snapshot_id = ?", snapshotId)
      .toArray()[0] as { object_key: string } | undefined;
    const upload = durable.storage.sql
      .exec("SELECT object_key FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
      .toArray()[0] as { object_key: string } | undefined;
    const objectKey = published?.object_key ?? upload?.object_key;
    if (objectKey === undefined) throw new Error(`snapshot key missing for ${snapshotId}`);
    return objectKey;
  });
}

export function bucketWithOverrides(base: R2Bucket, overrides: object): R2Bucket {
  return new Proxy(base, {
    get(target, property) {
      const receiver = Reflect.has(overrides, property) ? overrides : target;
      const value = Reflect.get(receiver, property);
      return typeof value === "function" ? value.bind(receiver) : value;
    },
  });
}

export async function uploadSnapshot(
  session: CreatedSession,
  snapshotId: string,
  capability = session.hostCapability,
  overrides: UploadOverrides = {},
): Promise<Response> {
  const body = overrides.body ?? encoder.encode("snapshot-state");
  const compressedLength = overrides.compressedLength ?? body.byteLength.toString();
  const uncompressedLength = overrides.uncompressedLength ?? compressedLength;
  const digest = overrides.sha256 ?? (await sha256Hex(body));
  const headers = snapshotHeaders(session, body, {
    ...overrides,
    compressedLength,
    sha256: digest,
    uncompressedLength,
  });
  headers.set("authorization", `Bearer ${capability}`);
  return workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions/${session.sessionId}/snapshots/${snapshotId}`, {
      method: "PUT",
      headers,
      body: Uint8Array.from(body).buffer,
    }),
  );
}

export async function overrideSnapshotBucket(
  session: CreatedSession,
  build: (base: R2Bucket) => R2Bucket,
): Promise<void> {
  await runInDurableObject(sessionStub(session.sessionId), (instance) => {
    const coordinator = Reflect.get(instance, "snapshotUploads") as object;
    const base = Reflect.get(coordinator, "snapshotBucket") as R2Bucket;
    Reflect.set(coordinator, "snapshotBucket", build(base));
  });
}

export function getSnapshot(
  session: CreatedSession,
  snapshotId: string,
  capability = session.observerCapability,
): Promise<Response> {
  return workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions/${session.sessionId}/snapshots/${snapshotId}`, {
      headers: { authorization: `Bearer ${capability}` },
    }),
  );
}

export function snapshotHeaders(
  session: CreatedSession,
  body: Uint8Array,
  overrides: UploadOverrides = {},
): Headers {
  const compressedLength = overrides.compressedLength ?? body.byteLength.toString();
  return new Headers({
    authorization: `Bearer ${session.hostCapability}`,
    "content-type": SNAPSHOT_MEDIA_TYPE,
    [SnapshotHeader.compression]: overrides.compression ?? "none",
    [SnapshotHeader.compressedLength]: compressedLength,
    [SnapshotHeader.cutEventSeq]: overrides.cutEventSeq ?? "0",
    [SnapshotHeader.engineId]: overrides.engineId ?? engineId,
    [SnapshotHeader.nextPtyOffset]: overrides.nextPtyOffset ?? "0",
    [SnapshotHeader.sessionEpoch]: overrides.sessionEpoch ?? "7",
    [SnapshotHeader.sha256]: overrides.sha256 ?? "0".repeat(64),
    [SnapshotHeader.uncompressedLength]: overrides.uncompressedLength ?? compressedLength,
  });
}

export async function installMiniflareMultipartEtagShim(sessionId: string): Promise<void> {
  const stub = env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${sessionId}`));
  await runInDurableObject(stub, (instance) => {
    // Miniflare uses opaque multipart part ETags; production R2 returns the part MD5.
    const coordinator = Reflect.get(instance, "snapshotUploads") as object;
    Reflect.set(Object.getPrototypeOf(coordinator), "snapshotPartEtagMatches", () => true);
  });
}
