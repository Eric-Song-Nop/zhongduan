import {
  SNAPSHOT_MEDIA_TYPE,
  SnapshotHeader,
  SnapshotUploadResponseSchema,
} from "@zhongduan/protocol";
import { z } from "zod";
import { AuthError, verifyBearerCapability, type CapabilityRole } from "./auth";
import type { CloudEnv } from "./env";
import {
  FinalizedSnapshotSchema,
  matchesSnapshotObject,
  parseSnapshotUploadMetadata,
  snapshotUploadHeaders,
} from "./snapshot-contract";

const UploadErrorSchema = z.strictObject({
  error: z.enum([
    "invalid-snapshot-metadata",
    "snapshot-checksum-mismatch",
    "snapshot-conflict",
    "snapshot-cursor-ahead",
    "snapshot-reservation-failed",
    "snapshot-unavailable",
    "snapshot-upload-failed",
    "snapshot-upload-in-progress",
  ]),
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

async function cancelBody(body: ReadableStream | null, reason: string): Promise<void> {
  if (body === null || body.locked) return;
  try {
    await body.cancel(reason);
  } catch {
    // The downstream request may already own or have consumed the stream.
  }
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
  const metadata = parseSnapshotUploadMetadata(request, sessionId, snapshotId);
  if (metadata === undefined || request.body === null) {
    await cancelBody(request.body, "invalid snapshot metadata");
    return json({ error: "invalid-snapshot-metadata" }, 400);
  }

  let response: Response;
  try {
    response = await sessionStub(env, sessionId).fetch(
      new Request(`https://do.internal/internal/snapshots/upload/${snapshotId}`, {
        method: "POST",
        headers: snapshotUploadHeaders(metadata),
        body: request.body,
      }),
    );
  } catch {
    return json({ error: "snapshot-finalize-failed" }, 503);
  }

  let body: unknown;
  try {
    body = await response.json();
  } catch {
    return json({ error: "snapshot-finalize-failed" }, 503);
  }
  if (response.status === 200 || response.status === 201) {
    const parsed = SnapshotUploadResponseSchema.safeParse(body);
    return parsed.success
      ? json(parsed.data, response.status === 201 ? 201 : 200)
      : json({ error: "snapshot-finalize-failed" }, 503);
  }
  if (response.ok) return json({ error: "snapshot-finalize-failed" }, 503);
  const parsed = UploadErrorSchema.safeParse(body);
  return parsed.success
    ? json(parsed.data, response.status)
    : json({ error: "snapshot-finalize-failed" }, 503);
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
