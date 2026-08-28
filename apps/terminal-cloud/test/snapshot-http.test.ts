import {
  MAX_SNAPSHOT_COMPRESSED_BYTES,
  MAX_SNAPSHOT_UNCOMPRESSED_BYTES,
  SNAPSHOT_MEDIA_TYPE,
  SnapshotHeader,
  type SnapshotMetadata,
} from "@zhongduan/protocol";
import { env, exports as workerExports } from "cloudflare:workers";
import { evictDurableObject, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import {
  hexToBytes,
  snapshotCustomMetadata,
  snapshotObjectKey,
} from "../src/worker/snapshot-contract";
import {
  createSession,
  encoder,
  engineId,
  getSnapshot,
  metadataFor,
  origin,
  sessionStub,
  sha256Hex,
  snapshotHeaders,
  storedSnapshotKey,
  uploadSnapshot,
} from "./snapshot-test-helpers";

describe("private HTTP snapshots", () => {
  it("enforces capability role, session, and path identity", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_auth_00000001";

    const wrongRole = await uploadSnapshot(session, snapshotId, session.observerCapability);
    expect(wrongRole.status).toBe(403);
    await wrongRole.body?.cancel();

    const wrongSession = await workerExports.default.fetch(
      new Request(`${origin}/api/v1/sessions/AAAAAAAAAAAAAAAA/snapshots/${snapshotId}`, {
        headers: { authorization: `Bearer ${session.hostCapability}` },
      }),
    );
    expect(wrongSession.status).toBe(401);
    await wrongSession.body?.cancel();

    const invalidPath = await workerExports.default.fetch(
      new Request(`${origin}/api/v1/sessions/${session.sessionId}/snapshots/short`, {
        headers: { authorization: `Bearer ${session.hostCapability}` },
      }),
    );
    expect(invalidPath.status).toBe(404);
    await invalidPath.body?.cancel();
  });

  it("requires a canonical Content-Length before accepting a streaming upload", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_chunked_000001";
    const body = encoder.encode("chunked-snapshot");
    const headers = snapshotHeaders(session, body, { sha256: await sha256Hex(body) });
    const request = new Request(
      `${origin}/api/v1/sessions/${session.sessionId}/snapshots/${snapshotId}`,
      {
        method: "PUT",
        headers,
        body: new ReadableStream({
          start(controller) {
            controller.enqueue(body);
            controller.close();
          },
        }),
      },
    );
    expect(request.headers.get("content-length")).toBeNull();

    const response = await workerExports.default.fetch(request);
    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({ error: "invalid-snapshot-metadata" });
    expect(await env.SNAPSHOTS.head(snapshotObjectKey(session.sessionId, snapshotId))).toBeNull();
  });

  it("aborts multipart ownership when the upload body stream fails", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_stream_abort_01";
    const body = encoder.encode("interrupted-snapshot");
    const headers = snapshotHeaders(session, body, { sha256: await sha256Hex(body) });
    headers.set("content-length", body.byteLength.toString());
    let sentPrefix = false;
    const response = await workerExports.default.fetch(
      new Request(`${origin}/api/v1/sessions/${session.sessionId}/snapshots/${snapshotId}`, {
        method: "PUT",
        headers,
        body: new ReadableStream<Uint8Array>({
          pull(controller) {
            if (!sentPrefix) {
              sentPrefix = true;
              controller.enqueue(body.subarray(0, 4));
              return;
            }
            controller.error(new Error("simulated upload body failure"));
          },
        }),
      }),
    );
    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({ error: "snapshot-upload-failed" });
    expect(await env.SNAPSHOTS.head(snapshotObjectKey(session.sessionId, snapshotId))).toBeNull();
    const upload = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) =>
        durable.storage.sql
          .exec("SELECT state FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
          .toArray()[0],
    );
    expect(upload).toBeUndefined();
  });

  it("publishes immutable R2 metadata and the DO pointer after the blob", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_publish_000001";
    const response = await uploadSnapshot(session, snapshotId);
    expect(response.status).toBe(201);
    const published = await response.json<{
      created: boolean;
      snapshot: { snapshotId: string };
    }>();

    const object = await env.SNAPSHOTS.head(await storedSnapshotKey(session.sessionId, snapshotId));
    expect(object).not.toBeNull();
    expect(published).toMatchObject({
      created: true,
      snapshot: { snapshotId },
    });
    expect(JSON.stringify(published)).not.toContain(object?.version);
    const state = await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      return {
        latest: durable.storage.sql.exec("SELECT latest_snapshot_id FROM session_state").one(),
        snapshot: durable.storage.sql
          .exec("SELECT r2_version, state FROM snapshot WHERE snapshot_id = ?", snapshotId)
          .one(),
      };
    });
    expect(state.latest).toMatchObject({ latest_snapshot_id: snapshotId });
    expect(state.snapshot).toMatchObject({ r2_version: object?.version, state: "servable" });
  });

  it("serves legacy single-put rows whose R2 metadata predates uploadKind", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_legacy_single_01";
    const body = encoder.encode("legacy-single-put");
    const metadata = await metadataFor(session, snapshotId, body);
    const objectKey = snapshotObjectKey(session.sessionId, snapshotId);
    const object = await env.SNAPSHOTS.put(objectKey, body, {
      sha256: hexToBytes(metadata.sha256),
      httpMetadata: {
        contentType: SNAPSHOT_MEDIA_TYPE,
        cacheControl: "private, no-store",
      },
      customMetadata: snapshotCustomMetadata(metadata),
    });
    await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      durable.storage.sql.exec(
        `INSERT INTO snapshot
          (snapshot_id, session_epoch, cut_event_seq, next_pty_offset, engine_id,
           object_key, r2_version, etag, sha256, compressed_length,
           uncompressed_length, compression, state, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'servable', ?)`,
        snapshotId,
        metadata.sessionEpoch,
        metadata.cutEventSeq,
        metadata.nextPtyOffset,
        metadata.engineId,
        objectKey,
        object.version,
        object.etag,
        metadata.sha256,
        Number(metadata.compressedLength),
        metadata.uncompressedLength,
        metadata.compression,
        Date.now(),
      );
      durable.storage.sql.exec(
        "UPDATE session_state SET latest_snapshot_id = ? WHERE singleton = 1",
        snapshotId,
      );
    });

    const response = await getSnapshot(session, snapshotId);
    expect(response.status).toBe(200);
    expect(new Uint8Array(await response.arrayBuffer())).toEqual(body);
    expect(object.customMetadata).not.toHaveProperty("uploadKind");
  });

  it("does not publish a body whose SHA-256 fails R2 validation", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_bad_digest_001";
    const response = await uploadSnapshot(session, snapshotId, session.hostCapability, {
      sha256: "0".repeat(64),
    });
    expect(response.status).toBe(422);
    expect(await response.json()).toEqual({ error: "snapshot-checksum-mismatch" });
    expect(await env.SNAPSHOTS.head(snapshotObjectKey(session.sessionId, snapshotId))).toBeNull();
    const count = await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      return durable.storage.sql.exec("SELECT COUNT(*) AS value FROM snapshot").one() as {
        value: number;
      };
    });
    expect(count.value).toBe(0);
  });

  it("makes exact retries idempotent and rejects immutable conflicts", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_idempotent_001";
    const first = await uploadSnapshot(session, snapshotId);
    expect(first.status).toBe(201);
    expect(await first.json()).toMatchObject({ created: true, snapshot: { snapshotId } });
    const objectKey = await storedSnapshotKey(session.sessionId, snapshotId);
    const firstObject = await env.SNAPSHOTS.head(objectKey);
    expect(firstObject).not.toBeNull();

    const retry = await uploadSnapshot(session, snapshotId);
    expect(retry.status).toBe(200);
    expect(await retry.json()).toMatchObject({ created: false, snapshot: { snapshotId } });

    const conflict = await uploadSnapshot(session, snapshotId, session.hostCapability, {
      body: encoder.encode("different-snapshot"),
    });
    expect(conflict.status).toBe(409);
    expect(await conflict.json()).toEqual({ error: "snapshot-conflict" });
    const object = await env.SNAPSHOTS.head(objectKey);
    expect(object?.version).toBe(firstObject?.version);
  });

  it("never recreates a missing or replaced object behind a published pointer", async () => {
    const session = await createSession();
    const missingId = "snapshot_published_missing";
    const missingUpload = await uploadSnapshot(session, missingId);
    expect(missingUpload.status).toBe(201);
    await missingUpload.body?.cancel();
    const missingKey = await storedSnapshotKey(session.sessionId, missingId);
    await env.SNAPSHOTS.delete(missingKey);

    const missingRetry = await uploadSnapshot(session, missingId);
    expect(missingRetry.status).toBe(503);
    expect(await missingRetry.json()).toEqual({ error: "snapshot-unavailable" });
    expect(await env.SNAPSHOTS.head(missingKey)).toBeNull();

    const replacedId = "snapshot_published_replace";
    const replacedUpload = await uploadSnapshot(session, replacedId);
    expect(replacedUpload.status).toBe(201);
    await replacedUpload.body?.cancel();
    const replacedKey = await storedSnapshotKey(session.sessionId, replacedId);
    const body = encoder.encode("snapshot-state");
    const sha256 = await sha256Hex(body);
    const metadata: SnapshotMetadata = {
      sessionId: session.sessionId,
      snapshotId: replacedId,
      engineId,
      sessionEpoch: "7",
      cutEventSeq: "0",
      nextPtyOffset: "0",
      compression: "none",
      compressedLength: body.byteLength.toString(),
      uncompressedLength: body.byteLength.toString(),
      sha256,
    };
    const replacedObject = await env.SNAPSHOTS.put(replacedKey, body, {
      sha256: hexToBytes(sha256),
      httpMetadata: {
        contentType: SNAPSHOT_MEDIA_TYPE,
        cacheControl: "private, no-store",
      },
      customMetadata: snapshotCustomMetadata(metadata),
    });
    expect(replacedObject).not.toBeNull();

    const replacedRetry = await uploadSnapshot(session, replacedId);
    expect(replacedRetry.status).toBe(503);
    expect(await replacedRetry.json()).toEqual({ error: "snapshot-unavailable" });
    expect((await env.SNAPSHOTS.head(replacedKey))?.version).toBe(replacedObject?.version);
  });

  it("keeps a committed blob when the finalize response is unparseable and reconciles by id", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_unknown_reply_01";
    await runInDurableObject(sessionStub(session.sessionId), (instance) => {
      const originalFetch = instance.fetch.bind(instance);
      Reflect.set(instance, "fetch", async (request: Request) => {
        const response = await originalFetch(request);
        if (
          new URL(request.url).pathname.startsWith("/internal/snapshots/upload/") &&
          response.ok
        ) {
          await response.body?.cancel();
          return Response.json({ committed: true }, { status: 201 });
        }
        return response;
      });
    });

    const uncertain = await uploadSnapshot(session, snapshotId);
    expect(uncertain.status).toBe(503);
    expect(await uncertain.json()).toEqual({ error: "snapshot-finalize-failed" });
    expect(
      await env.SNAPSHOTS.head(await storedSnapshotKey(session.sessionId, snapshotId)),
    ).not.toBeNull();
    const committed = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) => ({
        snapshot: durable.storage.sql
          .exec("SELECT state FROM snapshot WHERE snapshot_id = ?", snapshotId)
          .one(),
        uploadCount: durable.storage.sql
          .exec("SELECT COUNT(*) AS value FROM snapshot_upload WHERE snapshot_id = ?", snapshotId)
          .one().value,
      }),
    );
    expect(committed).toEqual({ snapshot: { state: "servable" }, uploadCount: 0 });

    await evictDurableObject(sessionStub(session.sessionId));
    const retry = await uploadSnapshot(session, snapshotId);
    expect(retry.status).toBe(200);
    expect(await retry.json()).toMatchObject({ created: false, snapshot: { snapshotId } });
  });

  it("streams published snapshots, supports cancellation, and fails closed without the blob", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_stream_0000001";
    const body = new Uint8Array(1024 * 1024);
    crypto.getRandomValues(body.subarray(0, 65_536));
    body.copyWithin(65_536, 0, 65_536);
    const uploaded = await uploadSnapshot(session, snapshotId, session.hostCapability, { body });
    expect(uploaded.status).toBe(201);
    await uploaded.body?.cancel();
    const durableBytes = await runInDurableObject(
      sessionStub(session.sessionId),
      (_instance, durable) => durable.storage.sql.databaseSize,
    );
    expect(durableBytes).toBeLessThan(256 * 1024);

    const cancellable = await getSnapshot(session, snapshotId);
    expect(cancellable.status).toBe(200);
    expect(cancellable.headers.get("cache-control")).toBe("private, no-store");
    const reader = cancellable.body?.getReader();
    expect((await reader?.read())?.done).toBe(false);
    await reader?.cancel("test cancellation");

    const complete = await getSnapshot(session, snapshotId, session.hostCapability);
    expect(complete.headers.get("content-type")).toBe(SNAPSHOT_MEDIA_TYPE);
    expect(complete.headers.get(SnapshotHeader.compression)).toBe("none");
    expect(complete.headers.get(SnapshotHeader.compressedLength)).toBe(body.byteLength.toString());
    expect(complete.headers.get(SnapshotHeader.cutEventSeq)).toBe("0");
    expect(complete.headers.get(SnapshotHeader.engineId)).toBe(engineId);
    expect(complete.headers.get(SnapshotHeader.nextPtyOffset)).toBe("0");
    expect(complete.headers.get(SnapshotHeader.sessionEpoch)).toBe("7");
    expect(complete.headers.get(SnapshotHeader.sha256)).toBe(await sha256Hex(body));
    expect(complete.headers.get(SnapshotHeader.uncompressedLength)).toBe(
      body.byteLength.toString(),
    );
    expect(new Uint8Array(await complete.arrayBuffer())).toEqual(body);

    await env.SNAPSHOTS.delete(await storedSnapshotKey(session.sessionId, snapshotId));
    const missing = await getSnapshot(session, snapshotId);
    expect(missing.status).toBe(503);
    expect(await missing.json()).toEqual({ error: "snapshot-unavailable" });
  });

  it("fails closed when a published R2 version or metadata no longer matches", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_mismatch_00001";
    const uploaded = await uploadSnapshot(session, snapshotId);
    expect(uploaded.status).toBe(201);
    await uploaded.body?.cancel();

    await env.SNAPSHOTS.put(
      await storedSnapshotKey(session.sessionId, snapshotId),
      encoder.encode("tampered"),
    );
    const response = await getSnapshot(session, snapshotId);
    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({ error: "snapshot-unavailable" });
  });

  it("preserves finalized metadata across Durable Object hibernation", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_hibernate_0001";
    const uploaded = await uploadSnapshot(session, snapshotId);
    expect(uploaded.status).toBe(201);
    await uploaded.body?.cancel();

    await evictDurableObject(sessionStub(session.sessionId));
    const response = await getSnapshot(session, snapshotId);
    expect(response.status).toBe(200);
    expect(new TextDecoder().decode(await response.arrayBuffer())).toBe("snapshot-state");
  });

  it("rejects declared compressed and decompressed sizes above the fixed caps", async () => {
    const session = await createSession();
    const oversizedCompressed = await uploadSnapshot(
      session,
      "snapshot_large_compressed",
      session.hostCapability,
      {
        compressedLength: (MAX_SNAPSHOT_COMPRESSED_BYTES + 1).toString(),
        uncompressedLength: (MAX_SNAPSHOT_COMPRESSED_BYTES + 1).toString(),
      },
    );
    expect(oversizedCompressed.status).toBe(400);
    await oversizedCompressed.body?.cancel();

    const oversizedUncompressed = await uploadSnapshot(
      session,
      "snapshot_large_unpacked_01",
      session.hostCapability,
      {
        compression: "zstd",
        uncompressedLength: (MAX_SNAPSHOT_UNCOMPRESSED_BYTES + 1).toString(),
      },
    );
    expect(oversizedUncompressed.status).toBe(400);
    await oversizedUncompressed.body?.cancel();
  });
});
