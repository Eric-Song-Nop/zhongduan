/// <reference lib="webworker" />

import {
  MAX_SNAPSHOT_COMPRESSED_BYTES,
  MAX_SNAPSHOT_UNCOMPRESSED_BYTES,
} from "@zhongduan/protocol";
import { HttpSnapshotTransport } from "@zhongduan/session-client";

import {
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
  try {
    const transport = new HttpSnapshotTransport({
      getHeaders: () => ({ Authorization: `Bearer ${request.capability}` }),
      maxCompressedBytes: MAX_SNAPSHOT_COMPRESSED_BYTES,
      maxUncompressedBytes: MAX_SNAPSHOT_UNCOMPRESSED_BYTES,
    });
    const snapshot = await transport.load(request.manifest, new AbortController().signal);
    const bytes = transferableSnapshot(snapshot);
    const response = { type: "loaded", bytes } satisfies SnapshotWorkerResponse;
    worker.postMessage(response, [bytes]);
  } catch {
    worker.postMessage({ type: "error" } satisfies SnapshotWorkerResponse);
  }
}
