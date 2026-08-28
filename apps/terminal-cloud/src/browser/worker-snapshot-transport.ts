import {
  MAX_SNAPSHOT_COMPRESSED_BYTES,
  MAX_SNAPSHOT_UNCOMPRESSED_BYTES,
} from "@zhongduan/protocol";
import type { SnapshotManifest, SnapshotTransport } from "@zhongduan/session-client";

import {
  SNAPSHOT_LOAD_TIMEOUT_MS,
  type SnapshotWorkerRequest,
  type SnapshotWorkerResponse,
} from "./snapshot-worker-contract";

interface SnapshotWorker {
  addEventListener(type: "error", listener: (event: ErrorEvent) => void): void;
  addEventListener(
    type: "message",
    listener: (event: MessageEvent<SnapshotWorkerResponse>) => void,
  ): void;
  postMessage(message: SnapshotWorkerRequest): void;
  terminate(): void;
}

export interface WorkerSnapshotTransportOptions {
  createWorker?: () => SnapshotWorker;
  getCapability: () => string;
  setTimer?: (callback: () => void, delayMs: number) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
}

function defaultWorker(): SnapshotWorker {
  return new Worker(new URL("./snapshot.worker.ts", import.meta.url), {
    name: "zhongduan-snapshot",
    type: "module",
  });
}

function assertProtocolLimit(manifest: SnapshotManifest): void {
  if (
    BigInt(manifest.compressedLength) > BigInt(MAX_SNAPSHOT_COMPRESSED_BYTES) ||
    BigInt(manifest.uncompressedLength) > BigInt(MAX_SNAPSHOT_UNCOMPRESSED_BYTES)
  ) {
    throw new Error("snapshot exceeds the protocol limit");
  }
}

export class WorkerSnapshotTransport implements SnapshotTransport {
  readonly #createWorker: () => SnapshotWorker;
  readonly #getCapability: () => string;
  readonly #setTimer: NonNullable<WorkerSnapshotTransportOptions["setTimer"]>;
  readonly #clearTimer: NonNullable<WorkerSnapshotTransportOptions["clearTimer"]>;

  constructor(options: WorkerSnapshotTransportOptions) {
    this.#createWorker = options.createWorker ?? defaultWorker;
    this.#getCapability = options.getCapability;
    this.#setTimer =
      options.setTimer ?? ((callback, delayMs) => globalThis.setTimeout(callback, delayMs));
    this.#clearTimer = options.clearTimer ?? ((timer) => globalThis.clearTimeout(timer));
  }

  load(manifest: SnapshotManifest, signal: AbortSignal): Promise<Uint8Array> {
    signal.throwIfAborted();
    assertProtocolLimit(manifest);
    const worker = this.#createWorker();
    return new Promise((resolve, reject) => {
      let settled = false;
      let deadline: ReturnType<typeof setTimeout> | null = null;
      const finish = (result: { bytes: Uint8Array } | { error: unknown }): void => {
        if (settled) return;
        settled = true;
        if (deadline !== null) this.#clearTimer(deadline);
        signal.removeEventListener("abort", onAbort);
        worker.terminate();
        if ("bytes" in result) resolve(result.bytes);
        else reject(result.error);
      };
      const onAbort = () => {
        finish({ error: signal.reason ?? new DOMException("snapshot load aborted", "AbortError") });
      };
      worker.addEventListener("message", (event) => {
        if (event.data.type !== "loaded" || !(event.data.bytes instanceof ArrayBuffer)) {
          finish({ error: new Error("snapshot worker failed") });
          return;
        }
        finish({ bytes: new Uint8Array(event.data.bytes) });
      });
      worker.addEventListener("error", () => {
        finish({ error: new Error("snapshot worker failed") });
      });
      signal.addEventListener("abort", onAbort, { once: true });
      deadline = this.#setTimer(() => {
        finish({ error: new DOMException("snapshot load timed out", "TimeoutError") });
      }, SNAPSHOT_LOAD_TIMEOUT_MS);
      if (signal.aborted) {
        onAbort();
        return;
      }
      try {
        worker.postMessage({
          type: "load",
          capability: this.#getCapability(),
          manifest,
        });
      } catch {
        finish({ error: new Error("snapshot worker failed") });
      }
    });
  }
}
