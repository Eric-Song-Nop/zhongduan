import type { SnapshotManifest } from "@zhongduan/session-client";

export interface SnapshotWorkerRequest {
  capability: string;
  manifest: SnapshotManifest;
  type: "load";
}

export type SnapshotWorkerResponse = { bytes: ArrayBuffer; type: "loaded" } | { type: "error" };

export function transferableSnapshot(bytes: Uint8Array): ArrayBuffer {
  if (bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength) {
    return bytes.buffer as ArrayBuffer;
  }
  return bytes.slice().buffer as ArrayBuffer;
}
