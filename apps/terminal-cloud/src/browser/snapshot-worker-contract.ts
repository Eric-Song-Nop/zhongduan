import type { SnapshotRestoreSource } from "@zhongduan/session-client";

export const SNAPSHOT_HTTP_TIMEOUT_MS = 30_000;
export const SNAPSHOT_LOAD_TIMEOUT_MS = 35_000;

export interface SnapshotWorkerRequest {
  capability: string;
  source: SnapshotRestoreSource;
  type: "load";
}

export type SnapshotWorkerResponse = { bytes: ArrayBuffer; type: "loaded" } | { type: "error" };

export function transferableSnapshot(bytes: Uint8Array): ArrayBuffer {
  if (bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength) {
    return bytes.buffer as ArrayBuffer;
  }
  return bytes.slice().buffer as ArrayBuffer;
}
