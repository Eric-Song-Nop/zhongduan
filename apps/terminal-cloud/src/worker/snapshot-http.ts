import {
  SNAPSHOT_MEDIA_TYPE,
  SnapshotHeader,
  SnapshotMetadataSchema,
  type SnapshotMetadata,
} from "@zhongduan/protocol";
import { z } from "zod";
import { AuthError, verifyBearerCapability, type CapabilityRole } from "./auth";
import type { CloudEnv } from "./env";
import {
  FinalizedSnapshotSchema,
  hexToBytes,
  matchesSnapshotObject,
  snapshotCustomMetadata,
  snapshotObjectKey,
  type FinalizedSnapshot,
} from "./snapshot-contract";

const FinalizeResponseSchema = z.strictObject({
  created: z.boolean(),
  snapshot: FinalizedSnapshotSchema,
});
const PublicFinalizeResponseSchema = z.strictObject({
  created: z.boolean(),
  snapshot: SnapshotMetadataSchema,
});

function json(data: unknown, status = 200): Response {
  return Response.json(data, {
    status,
    headers: { "cache-control": "no-store" },
  });
}

function sessionStub(env: CloudEnv, sessionId: string) {
  return env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${sessionId}`));
}

async function authorize(
  request: Request,
  env: CloudEnv,
  sessionId: string,
  roles: readonly CapabilityRole[],
): Promise<Response | undefined> {
  try {
    await verifyBearerCapability(request, env.CAPABILITY_SIGNING_KEY, { sessionId, roles });
    return undefined;
  } catch (error) {
    if (error instanceof AuthError) {
      return json({ error: "unauthorized" }, error.code === "wrong-role" ? 403 : 401);
    }
    throw error;
  }
}

function uploadMetadata(
  request: Request,
  sessionId: string,
  snapshotId: string,
): SnapshotMetadata | undefined {
  if (
    request.headers.get("content-type") !== SNAPSHOT_MEDIA_TYPE ||
    request.headers.has("content-encoding")
  ) {
    return undefined;
  }
  const parsed = SnapshotMetadataSchema.safeParse({
    sessionId,
    snapshotId,
    engineId: request.headers.get(SnapshotHeader.engineId),
    sessionEpoch: request.headers.get(SnapshotHeader.sessionEpoch),
    cutEventSeq: request.headers.get(SnapshotHeader.cutEventSeq),
    nextPtyOffset: request.headers.get(SnapshotHeader.nextPtyOffset),
    compression: request.headers.get(SnapshotHeader.compression),
    compressedLength: request.headers.get(SnapshotHeader.compressedLength),
    uncompressedLength: request.headers.get(SnapshotHeader.uncompressedLength),
    sha256: request.headers.get(SnapshotHeader.sha256),
  });
  if (!parsed.success) return undefined;
  const contentLength = request.headers.get("content-length");
  if (contentLength !== parsed.data.compressedLength) return undefined;
  return parsed.data;
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

async function cancelBody(body: ReadableStream | null, reason: string): Promise<void> {
  if (body === null || body.locked) return;
  try {
    await body.cancel(reason);
  } catch {
    // The runtime may already have consumed or released the inbound stream.
  }
}

async function finalizeSnapshot(env: CloudEnv, snapshot: FinalizedSnapshot): Promise<Response> {
  let response: Response;
  try {
    response = await sessionStub(env, snapshot.sessionId).fetch(
      "https://do.internal/internal/snapshots/finalize",
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(snapshot),
      },
    );
  } catch {
    return json({ error: "snapshot-finalize-failed" }, 503);
  }
  if (!response.ok) {
    await response.body?.cancel();
    return json(
      { error: response.status === 409 ? "snapshot-conflict" : "snapshot-finalize-failed" },
      response.status === 409 ? 409 : 503,
    );
  }
  let body: unknown;
  try {
    body = await response.json();
  } catch {
    return json({ error: "snapshot-finalize-failed" }, 503);
  }
  const parsed = FinalizeResponseSchema.safeParse(body);
  if (!parsed.success || parsed.data.snapshot.r2Version !== snapshot.r2Version) {
    return json({ error: "snapshot-finalize-failed" }, 503);
  }
  const {
    objectKey: _objectKey,
    r2Version: _r2Version,
    etag: _etag,
    ...publicSnapshot
  } = parsed.data.snapshot;
  const published = PublicFinalizeResponseSchema.parse({
    created: parsed.data.created,
    snapshot: publicSnapshot,
  });
  return json(published, parsed.data.created ? 201 : 200);
}

async function putSnapshot(
  request: Request,
  env: CloudEnv,
  sessionId: string,
  snapshotId: string,
): Promise<Response> {
  const unauthorized = await authorize(request, env, sessionId, ["host"]);
  if (unauthorized !== undefined) {
    await cancelBody(request.body, "snapshot upload is not authorized");
    return unauthorized;
  }
  const metadata = uploadMetadata(request, sessionId, snapshotId);
  if (metadata === undefined || request.body === null) {
    await cancelBody(request.body, "invalid snapshot metadata");
    return json({ error: "invalid-snapshot-metadata" }, 400);
  }

  const objectKey = snapshotObjectKey(sessionId, snapshotId);
  let object: R2Object | null;
  try {
    object = await env.SNAPSHOTS.head(objectKey);
  } catch {
    await cancelBody(request.body, "snapshot storage is unavailable");
    return json({ error: "snapshot-upload-failed" }, 503);
  }
  let createdObject = false;
  if (object !== null) {
    await cancelBody(request.body, "immutable snapshot already exists");
    if (!matchesSnapshotObject(object, metadata)) {
      return json({ error: "snapshot-conflict" }, 409);
    }
  } else {
    try {
      object = await env.SNAPSHOTS.put(objectKey, request.body, {
        onlyIf: { etagDoesNotMatch: "*" },
        sha256: hexToBytes(metadata.sha256),
        httpMetadata: {
          contentType: SNAPSHOT_MEDIA_TYPE,
          cacheControl: "private, no-store",
        },
        customMetadata: snapshotCustomMetadata(metadata),
      });
      createdObject = object !== null;
    } catch (error) {
      await cancelBody(request.body, "snapshot upload failed");
      const code = r2ErrorCode(error);
      return json(
        { error: code === 10037 ? "snapshot-checksum-mismatch" : "snapshot-upload-failed" },
        code === 10037 || code === 10014 || code === 10013 ? 422 : 503,
      );
    }
    if (object === null) {
      try {
        object = await env.SNAPSHOTS.head(objectKey);
      } catch {
        return json({ error: "snapshot-upload-failed" }, 503);
      }
    }
    if (object === null || !matchesSnapshotObject(object, metadata)) {
      if (createdObject) {
        try {
          await env.SNAPSHOTS.delete(objectKey);
        } catch {
          // A failed cleanup remains private because the DO pointer is not finalized.
        }
      }
      return json({ error: "snapshot-conflict" }, 409);
    }
  }

  return finalizeSnapshot(
    env,
    FinalizedSnapshotSchema.parse({
      ...metadata,
      objectKey,
      r2Version: object.version,
      etag: object.etag,
    }),
  );
}

async function getSnapshot(
  request: Request,
  env: CloudEnv,
  sessionId: string,
  snapshotId: string,
): Promise<Response> {
  const unauthorized = await authorize(request, env, sessionId, ["host", "writer", "observer"]);
  if (unauthorized !== undefined) return unauthorized;

  let pointerResponse: Response;
  try {
    pointerResponse = await sessionStub(env, sessionId).fetch(
      `https://do.internal/internal/snapshots/${snapshotId}`,
    );
  } catch {
    return json({ error: "snapshot-pointer-unavailable" }, 503);
  }
  if (pointerResponse.status === 404) {
    await pointerResponse.body?.cancel();
    return json({ error: "snapshot-not-found" }, 404);
  }
  let pointerBody: unknown;
  try {
    pointerBody = await pointerResponse.json();
  } catch {
    return json({ error: "snapshot-pointer-unavailable" }, 503);
  }
  const pointer = FinalizedSnapshotSchema.safeParse(pointerBody);
  if (!pointerResponse.ok || !pointer.success) {
    return json({ error: "snapshot-pointer-unavailable" }, 503);
  }

  let object: R2ObjectBody | null;
  try {
    object = await env.SNAPSHOTS.get(pointer.data.objectKey);
  } catch {
    return json({ error: "snapshot-unavailable" }, 503);
  }
  if (
    object === null ||
    object.version !== pointer.data.r2Version ||
    object.etag !== pointer.data.etag ||
    !matchesSnapshotObject(object, pointer.data)
  ) {
    if (object !== null) await cancelBody(object.body, "snapshot pointer mismatch");
    return json({ error: "snapshot-unavailable" }, 503);
  }

  const headers = new Headers({
    "cache-control": "private, no-store",
    "content-length": pointer.data.compressedLength,
    "content-type": SNAPSHOT_MEDIA_TYPE,
    etag: object.httpEtag,
    "x-content-type-options": "nosniff",
    [SnapshotHeader.compression]: pointer.data.compression,
    [SnapshotHeader.compressedLength]: pointer.data.compressedLength,
    [SnapshotHeader.cutEventSeq]: pointer.data.cutEventSeq,
    [SnapshotHeader.engineId]: pointer.data.engineId,
    [SnapshotHeader.nextPtyOffset]: pointer.data.nextPtyOffset,
    [SnapshotHeader.sessionEpoch]: pointer.data.sessionEpoch,
    [SnapshotHeader.sha256]: pointer.data.sha256,
    [SnapshotHeader.uncompressedLength]: pointer.data.uncompressedLength,
  });
  return new Response(object.body, { status: 200, headers });
}

export async function handleSnapshotRequest(
  request: Request,
  env: CloudEnv,
  sessionId: string,
  snapshotId: string,
): Promise<Response> {
  if (request.method === "PUT") {
    return putSnapshot(request, env, sessionId, snapshotId);
  }
  if (request.method === "GET") {
    return getSnapshot(request, env, sessionId, snapshotId);
  }
  return new Response(null, { status: 405, headers: { allow: "GET, PUT" } });
}
