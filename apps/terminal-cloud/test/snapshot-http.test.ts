import {
  MAX_SNAPSHOT_COMPRESSED_BYTES,
  MAX_SNAPSHOT_UNCOMPRESSED_BYTES,
  SNAPSHOT_MEDIA_TYPE,
  SnapshotHeader,
} from "@zhongduan/protocol";
import { env, exports as workerExports } from "cloudflare:workers";
import { evictDurableObject, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { snapshotObjectKey } from "../src/worker/snapshot-contract";

interface CreatedSession {
  hostCapability: string;
  observerCapability: string;
  sessionId: string;
  writerCapability: string;
}

interface UploadOverrides {
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

const origin = "https://terminal.example.test";
const engineId = "ghostty:test+snapshot-v1+wterm:test";
const encoder = new TextEncoder();

async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const digest = new Uint8Array(
    await crypto.subtle.digest("SHA-256", Uint8Array.from(bytes).buffer),
  );
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function createSession(): Promise<CreatedSession> {
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
  return response.json<CreatedSession>();
}

function sessionStub(sessionId: string) {
  return env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${sessionId}`));
}

async function uploadSnapshot(
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

async function getSnapshot(
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

function snapshotHeaders(
  session: CreatedSession,
  body: Uint8Array,
  overrides: UploadOverrides = {},
): Headers {
  const compressedLength = overrides.compressedLength ?? body.byteLength.toString();
  const headers = new Headers({
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
  return headers;
}

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

  it("publishes immutable R2 metadata and the DO pointer after the blob", async () => {
    const session = await createSession();
    const snapshotId = "snapshot_publish_000001";
    const response = await uploadSnapshot(session, snapshotId);
    expect(response.status).toBe(201);
    const published = await response.json<{
      created: boolean;
      snapshot: { snapshotId: string };
    }>();

    const object = await env.SNAPSHOTS.head(snapshotObjectKey(session.sessionId, snapshotId));
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
    const firstObject = await env.SNAPSHOTS.head(snapshotObjectKey(session.sessionId, snapshotId));
    expect(firstObject).not.toBeNull();

    const retry = await uploadSnapshot(session, snapshotId);
    expect(retry.status).toBe(200);
    expect(await retry.json()).toMatchObject({ created: false, snapshot: { snapshotId } });

    const conflict = await uploadSnapshot(session, snapshotId, session.hostCapability, {
      body: encoder.encode("different-snapshot"),
    });
    expect(conflict.status).toBe(409);
    expect(await conflict.json()).toEqual({ error: "snapshot-conflict" });
    const object = await env.SNAPSHOTS.head(snapshotObjectKey(session.sessionId, snapshotId));
    expect(object?.version).toBe(firstObject?.version);
  });

  it("leaves only private orphans when finalized session metadata is invalid", async () => {
    const session = await createSession();
    const invalidSnapshots: Array<[string, UploadOverrides]> = [
      ["snapshot_orphan_cut_0001", { cutEventSeq: "1" }],
      ["snapshot_orphan_epoch_001", { sessionEpoch: "8" }],
      ["snapshot_orphan_engine_01", { engineId: `${engineId}:other` }],
    ];

    for (const [snapshotId, overrides] of invalidSnapshots) {
      const response = await uploadSnapshot(session, snapshotId, session.hostCapability, overrides);
      expect(response.status).toBe(409);
      expect(
        await env.SNAPSHOTS.head(snapshotObjectKey(session.sessionId, snapshotId)),
      ).not.toBeNull();
      const pointer = await getSnapshot(session, snapshotId);
      expect(pointer.status).toBe(404);
      await pointer.body?.cancel();
    }
  });

  it("never moves the latest pointer backwards under out-of-order finalization", async () => {
    const session = await createSession();
    await runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
      durable.storage.sql.exec(
        "UPDATE session_state SET head_event_seq = '10', next_pty_offset = '1000' WHERE singleton = 1",
      );
    });

    const uploadAt = async (snapshotId: string, cutEventSeq: string, nextPtyOffset: string) => {
      const response = await uploadSnapshot(session, snapshotId, session.hostCapability, {
        cutEventSeq,
        nextPtyOffset,
      });
      expect(response.status).toBe(201);
      await response.body?.cancel();
      return runInDurableObject(sessionStub(session.sessionId), (_instance, durable) => {
        return durable.storage.sql
          .exec("SELECT latest_snapshot_id FROM session_state WHERE singleton = 1")
          .one().latest_snapshot_id;
      });
    };

    expect(await uploadAt("snapshot_latest_cut10_01", "10", "100")).toBe(
      "snapshot_latest_cut10_01",
    );
    expect(await uploadAt("snapshot_latest_cut09_01", "9", "900")).toBe("snapshot_latest_cut10_01");
    expect(await uploadAt("snapshot_latest_lowoff_1", "10", "99")).toBe("snapshot_latest_cut10_01");
    expect(await uploadAt("snapshot_latest_highoff1", "10", "101")).toBe(
      "snapshot_latest_highoff1",
    );
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

    await env.SNAPSHOTS.delete(snapshotObjectKey(session.sessionId, snapshotId));
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
      snapshotObjectKey(session.sessionId, snapshotId),
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
