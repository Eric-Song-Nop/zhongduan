import { SNAPSHOT_MEDIA_TYPE, SnapshotHeader } from "@zhongduan/protocol";
import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import browserE2EWorker, { isBrowserE2ELoopbackRequest } from "../src/worker/browser-e2e";
import productionWorker from "../src/worker/index";

const origin = "https://terminal.example.test";
const engineId = "ghostty:test+snapshot-v1+wterm:test";

function callWorker(
  worker: typeof browserE2EWorker,
  request: Request,
): ReturnType<typeof browserE2EWorker.fetch> {
  return worker.fetch(
    request as unknown as Parameters<typeof browserE2EWorker.fetch>[0],
    env as Parameters<typeof browserE2EWorker.fetch>[1],
  );
}

async function sha256Hex(body: Uint8Array): Promise<string> {
  const digest = new Uint8Array(
    await crypto.subtle.digest("SHA-256", Uint8Array.from(body).buffer),
  );
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

describe("local Browser E2E Worker entry", () => {
  it("accepts only loopback request hosts", () => {
    expect(isBrowserE2ELoopbackRequest(new Request("http://127.0.0.1/"))).toBe(true);
    expect(isBrowserE2ELoopbackRequest(new Request("http://localhost/"))).toBe(true);
    expect(isBrowserE2ELoopbackRequest(new Request("http://[::1]/"))).toBe(true);
    expect(isBrowserE2ELoopbackRequest(new Request("https://terminal.example.test/"))).toBe(false);
  });

  it("fails closed before dispatching a non-loopback request", async () => {
    const response = await callWorker(
      browserE2EWorker,
      new Request("https://terminal.example.test/api/v1/sessions"),
    );

    expect(response.status).toBe(404);
    expect(await response.json()).toEqual({ error: "not-found" });
  });

  it("leaves the production entry fail-closed on Miniflare's opaque part ETag", async () => {
    const suffix = crypto.randomUUID().replaceAll("-", "");
    const sessionId = `session_strict_${suffix}`;
    const created = await callWorker(
      productionWorker,
      new Request(`${origin}/api/v1/sessions`, {
        method: "POST",
        headers: {
          authorization: "Bearer test-bootstrap-token-with-at-least-32-bytes",
          "content-type": "application/json",
        },
        body: JSON.stringify({ sessionId, engineId, sessionEpoch: "7" }),
      }),
    );
    expect(created.status).toBe(201);
    const session = await created.json<{ hostCapability: string }>();
    const body = new Uint8Array([1, 2, 3]);
    const length = body.byteLength.toString();
    const snapshotId = `snapshot_strict_${suffix}`;
    const uploaded = await callWorker(
      productionWorker,
      new Request(`${origin}/api/v1/sessions/${sessionId}/snapshots/${snapshotId}`, {
        method: "PUT",
        headers: {
          authorization: `Bearer ${session.hostCapability}`,
          "content-length": length,
          "content-type": SNAPSHOT_MEDIA_TYPE,
          [SnapshotHeader.compression]: "none",
          [SnapshotHeader.compressedLength]: length,
          [SnapshotHeader.cutEventSeq]: "0",
          [SnapshotHeader.engineId]: engineId,
          [SnapshotHeader.nextPtyOffset]: "0",
          [SnapshotHeader.sessionEpoch]: "7",
          [SnapshotHeader.sha256]: await sha256Hex(body),
          [SnapshotHeader.uncompressedLength]: length,
        },
        body,
      }),
    );

    expect(await uploaded.json()).toEqual({ error: "snapshot-upload-failed" });
    expect(uploaded.status).toBe(503);
  });
});
