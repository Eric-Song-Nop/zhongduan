/// <reference lib="webworker" />

import {
  MAX_SNAPSHOT_COMPRESSED_BYTES,
  MAX_SNAPSHOT_UNCOMPRESSED_BYTES,
} from "@zhongduan/protocol";
import { HttpSnapshotTransport } from "@zhongduan/session-client";

import {
  SNAPSHOT_HTTP_TIMEOUT_MS,
  transferableSnapshot,
  type SnapshotWorkerRequest,
  type SnapshotWorkerResponse,
} from "./snapshot-worker-contract";

const worker = self as DedicatedWorkerGlobalScope;

worker.addEventListener("message", (event: MessageEvent<SnapshotWorkerRequest>) => {
  if (event.data.type !== "load") {
    worker.postMessage({ type: "error" } satisfies SnapshotWorkerResponse);
    return;
  }
  void loadSnapshot(event.data);
});

async function loadSnapshot(request: SnapshotWorkerRequest): Promise<void> {
  const abort = new AbortController();
  const deadline = setTimeout(() => {
    abort.abort(new DOMException("snapshot download timed out", "TimeoutError"));
  }, SNAPSHOT_HTTP_TIMEOUT_MS);
  try {
    const transport = new HttpSnapshotTransport({
      getHeaders: () => ({ Authorization: `Bearer ${request.capability}` }),
      maxCompressedBytes: MAX_SNAPSHOT_COMPRESSED_BYTES,
      maxUncompressedBytes: MAX_SNAPSHOT_UNCOMPRESSED_BYTES,
    });
    const snapshot = await transport.load(request.manifest, abort.signal);
    const bytes = transferableSnapshot(snapshot);
    const response = { type: "loaded", bytes } satisfies SnapshotWorkerResponse;
    worker.postMessage(response, [bytes]);
  } catch {
    worker.postMessage({ type: "error" } satisfies SnapshotWorkerResponse);
  } finally {
    clearTimeout(deadline);
  }
}
