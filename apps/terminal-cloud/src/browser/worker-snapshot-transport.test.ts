import {
  MAX_SNAPSHOT_COMPRESSED_BYTES,
  MAX_SNAPSHOT_UNCOMPRESSED_BYTES,
} from "@zhongduan/protocol";
import type { SnapshotRestoreSource } from "@zhongduan/session-client";
import { describe, expect, it, vi } from "vitest";

import {
  SNAPSHOT_LOAD_TIMEOUT_MS,
  transferableSnapshot,
  type SnapshotWorkerRequest,
  type SnapshotWorkerResponse,
} from "./snapshot-worker-contract";
import { WorkerSnapshotTransport } from "./worker-snapshot-transport";

const SOURCE: SnapshotRestoreSource = {
  kind: "snapshot",
  sessionId: "session_123456789",
  snapshotId: "snapshot_12345678",
  engineId: "ghostty:test",
  sessionEpoch: "1",
  cutEventSeq: "10",
  nextPtyOffset: "20",
  compression: "none",
  compressedLength: "3",
  uncompressedLength: "3",
  sha256: "a".repeat(64),
  downloadPath: "/api/v1/sessions/session_123456789/snapshots/snapshot_12345678",
  restoreThrough: "finish",
};

class FakeWorker {
  readonly terminate = vi.fn();
  readonly requests: SnapshotWorkerRequest[] = [];
  #message: ((event: MessageEvent<SnapshotWorkerResponse>) => void) | null = null;
  #error: ((event: ErrorEvent) => void) | null = null;

  addEventListener(type: "error" | "message", listener: (event: never) => void): void {
    if (type === "message") {
      this.#message = listener as (event: MessageEvent<SnapshotWorkerResponse>) => void;
    } else {
      this.#error = listener as (event: ErrorEvent) => void;
    }
  }

  postMessage(message: SnapshotWorkerRequest): void {
    this.requests.push(message);
  }

  loaded(bytes: ArrayBuffer): void {
    this.#message?.(new MessageEvent("message", { data: { type: "loaded", bytes } }));
  }

  fail(): void {
    this.#error?.({} as ErrorEvent);
  }
}

class ManualTimers {
  readonly pending = new Map<ReturnType<typeof setTimeout>, () => void>();
  #nextId = 1;

  readonly setTimer = (callback: () => void, _delayMs: number): ReturnType<typeof setTimeout> => {
    const id = this.#nextId as unknown as ReturnType<typeof setTimeout>;
    this.#nextId += 1;
    this.pending.set(id, callback);
    return id;
  };

  readonly clearTimer = (timer: ReturnType<typeof setTimeout>): void => {
    this.pending.delete(timer);
  };

  runOnly(): void {
    const entry = [...this.pending].at(0);
    if (entry === undefined || this.pending.size !== 1) {
      throw new Error("expected exactly one pending timer");
    }
    this.pending.delete(entry[0]);
    entry[1]();
  }
}

describe("WorkerSnapshotTransport", () => {
  it("invokes default browser timers with the global receiver", async () => {
    const setTimer = vi.fn(function (this: unknown): ReturnType<typeof setTimeout> {
      if (this !== globalThis) throw new TypeError("Illegal invocation");
      return 1 as unknown as ReturnType<typeof setTimeout>;
    });
    const clearTimer = vi.fn(function (this: unknown): void {
      if (this !== globalThis) throw new TypeError("Illegal invocation");
    });
    vi.stubGlobal("setTimeout", setTimer);
    vi.stubGlobal("clearTimeout", clearTimer);
    try {
      const worker = new FakeWorker();
      const transport = new WorkerSnapshotTransport({
        createWorker: () => worker,
        getCapability: () => "secret-capability",
      });
      const abort = new AbortController();
      const loading = transport.load(SOURCE, abort.signal);
      abort.abort(new DOMException("cancelled", "AbortError"));

      await expect(loading).rejects.toMatchObject({ name: "AbortError" });
      expect(setTimer).toHaveBeenCalledOnce();
      expect(clearTimer).toHaveBeenCalledOnce();
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("receives the transferred snapshot and terminates the dedicated worker", async () => {
    const worker = new FakeWorker();
    const transport = new WorkerSnapshotTransport({
      createWorker: () => worker,
      getCapability: () => "secret-capability",
    });
    const loading = transport.load(SOURCE, new AbortController().signal);
    expect(worker.requests).toEqual([
      { type: "load", capability: "secret-capability", source: SOURCE },
    ]);
    const bytes = Uint8Array.from([1, 2, 3]);
    worker.loaded(bytes.buffer);

    await expect(loading).resolves.toEqual(bytes);
    expect(worker.terminate).toHaveBeenCalledOnce();
  });

  it("terminates on abort and enforces the shared compressed and uncompressed limits", async () => {
    const worker = new FakeWorker();
    const timers = new ManualTimers();
    const transport = new WorkerSnapshotTransport({
      createWorker: () => worker,
      getCapability: () => "secret-capability",
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });
    const abort = new AbortController();
    const loading = transport.load(SOURCE, abort.signal);
    abort.abort(new DOMException("cancelled", "AbortError"));
    await expect(loading).rejects.toMatchObject({ name: "AbortError" });
    expect(worker.terminate).toHaveBeenCalledOnce();
    expect(timers.pending.size).toBe(0);

    const legalLargeWorker = new FakeWorker();
    const legalLargeTransport = new WorkerSnapshotTransport({
      createWorker: () => legalLargeWorker,
      getCapability: () => "secret-capability",
    });
    const legalLarge = legalLargeTransport.load(
      {
        ...SOURCE,
        compression: "zstd",
        compressedLength: MAX_SNAPSHOT_COMPRESSED_BYTES.toString(),
        uncompressedLength: (MAX_SNAPSHOT_COMPRESSED_BYTES + 1).toString(),
      },
      new AbortController().signal,
    );
    legalLargeWorker.loaded(new ArrayBuffer(MAX_SNAPSHOT_COMPRESSED_BYTES + 1));
    await expect(legalLarge).resolves.toHaveLength(MAX_SNAPSHOT_COMPRESSED_BYTES + 1);

    expect(() =>
      transport.load(
        {
          ...SOURCE,
          compression: "zstd",
          compressedLength: (MAX_SNAPSHOT_COMPRESSED_BYTES + 1).toString(),
        },
        new AbortController().signal,
      ),
    ).toThrow(/protocol limit/);
    expect(() =>
      transport.load(
        {
          ...SOURCE,
          compression: "zstd",
          uncompressedLength: (MAX_SNAPSHOT_UNCOMPRESSED_BYTES + 1).toString(),
        },
        new AbortController().signal,
      ),
    ).toThrow(/protocol limit/);
  });

  it("settles once when a worker error races a late transferred message", async () => {
    const worker = new FakeWorker();
    const transport = new WorkerSnapshotTransport({
      createWorker: () => worker,
      getCapability: () => "secret-capability",
    });
    const loading = transport.load(SOURCE, new AbortController().signal);

    worker.fail();
    worker.loaded(Uint8Array.from([1, 2, 3]).buffer);

    await expect(loading).rejects.toThrow("snapshot worker failed");
    expect(worker.terminate).toHaveBeenCalledOnce();
  });

  it("terminates a worker that exceeds the independent load deadline", async () => {
    const worker = new FakeWorker();
    const timers = new ManualTimers();
    const setTimer = vi.fn(timers.setTimer);
    const transport = new WorkerSnapshotTransport({
      createWorker: () => worker,
      getCapability: () => "secret-capability",
      setTimer,
      clearTimer: timers.clearTimer,
    });
    const loading = transport.load(SOURCE, new AbortController().signal);

    expect(setTimer).toHaveBeenCalledWith(expect.any(Function), SNAPSHOT_LOAD_TIMEOUT_MS);
    timers.runOnly();
    worker.loaded(Uint8Array.from([1, 2, 3]).buffer);

    await expect(loading).rejects.toMatchObject({ name: "TimeoutError" });
    expect(worker.terminate).toHaveBeenCalledOnce();
    expect(timers.pending.size).toBe(0);
  });

  it("copies a view before transfer so unrelated bytes never leave the worker", () => {
    const backing = Uint8Array.from([9, 1, 2, 3, 9]);
    const transferred = transferableSnapshot(backing.subarray(1, 4));
    expect([...new Uint8Array(transferred)]).toEqual([1, 2, 3]);
    expect(transferred).not.toBe(backing.buffer);
  });
});
