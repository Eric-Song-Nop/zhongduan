// @vitest-environment jsdom

import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";

import { GhosttyCore, GhosttyRuntime } from "@wterm/ghostty";
import type { ReplicaCursor } from "@zhongduan/protocol";
import type { SnapshotManifest } from "@zhongduan/session-client";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

import { WTermReplicaHost } from "./replica-host";

const encoder = new TextEncoder();
const resolveFromTest = createRequire(import.meta.url);
let committedWasm: Uint8Array;
let animationFrames: Map<number, FrameRequestCallback>;
let nextAnimationFrame: number;

beforeAll(async () => {
  committedWasm = await readFile(resolveFromTest.resolve("@wterm/ghostty/ghostty-vt.wasm"));
});

beforeEach(() => {
  animationFrames = new Map();
  nextAnimationFrame = 1;
  vi.stubGlobal(
    "requestAnimationFrame",
    vi.fn((callback: FrameRequestCallback) => {
      const id = nextAnimationFrame;
      nextAnimationFrame += 1;
      animationFrames.set(id, callback);
      return id;
    }),
  );
  vi.stubGlobal(
    "cancelAnimationFrame",
    vi.fn((id: number) => {
      animationFrames.delete(id);
    }),
  );
});

afterEach(() => {
  document.body.innerHTML = "";
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("WTermReplicaHost committed-WASM adoption", () => {
  it("keeps the visible owner atomic across restore and DOM replacement failure, then fences it once", async () => {
    const element = document.createElement("div");
    document.body.appendChild(element);
    const onAdopt = vi.fn();
    const onAuthoritativeResize = vi.fn();
    const host = await WTermReplicaHost.create({
      element,
      inputSink: { send: vi.fn() },
      wasmSource: committedWasm,
      onAdopt,
      onAuthoritativeResize,
    });

    try {
      const previous = host.active;
      expect(previous).not.toBeNull();
      previous!.writePty(encoder.encode("\u001b[2J\u001b[HOLD-ACTIVE"));
      flushAnimationFrames();
      const previousGrid = element.querySelector(".term-grid");
      const previousHtml = previousGrid?.innerHTML;
      expect(previousGrid?.textContent).toContain("OLD-ACTIVE");

      const snapshot = await candidateSnapshot();
      const manifest = snapshotManifest(host.engineId, snapshot.byteLength);
      await expect(
        host.restore(
          snapshot.subarray(0, snapshot.byteLength - 1),
          manifest,
          new AbortController().signal,
        ),
      ).rejects.toThrow();
      expect(host.active).toBe(previous);
      expect(element.querySelector(".term-grid")).toBe(previousGrid);
      expect(previousGrid?.innerHTML).toBe(previousHtml);
      expect(onAdopt).not.toHaveBeenCalled();

      const candidate = await host.restore(snapshot, manifest, new AbortController().signal);
      candidate.writePty(encoder.encode("-EXACT-TAIL"));
      candidate.resize({ cols: 14, rows: 4, widthPx: 140, heightPx: 68 });
      expect(host.active).toBe(previous);
      expect(element.querySelector(".term-grid")).toBe(previousGrid);
      expect(previousGrid?.innerHTML).toBe(previousHtml);

      const cursor: ReplicaCursor = {
        sessionEpoch: 7n,
        deliveryGeneration: 4n,
        lastEventSeq: 12n,
        nextPtyOffset: 24n,
      };
      const replacement = vi.spyOn(Element.prototype, "replaceWith").mockImplementationOnce(() => {
        throw new Error("injected DOM replacement failure");
      });
      expect(() => host.adopt(candidate, cursor)).toThrow("injected DOM replacement failure");
      replacement.mockRestore();

      expect(host.active).toBe(previous);
      expect(element.querySelector(".term-grid")).toBe(previousGrid);
      expect(previousGrid?.innerHTML).toBe(previousHtml);
      expect(onAdopt).not.toHaveBeenCalled();
      previous!.writePty(encoder.encode("\u001b[2J\u001b[HOLD-STILL-LIVE"));
      flushAnimationFrames();
      expect(previousGrid?.textContent).toContain("OLD-STILL-LIVE");
      expect(() => candidate.writePty(encoder.encode("-STILL-DETACHED"))).not.toThrow();

      host.adopt(candidate, cursor);

      expect(host.active).toBe(candidate);
      expect(element.querySelector(".term-grid")).not.toBe(previousGrid);
      expect(element.textContent).toContain("CANDIDATE");
      expect(onAdopt).toHaveBeenCalledOnce();
      expect(onAdopt).toHaveBeenCalledWith(cursor);
      expect(onAuthoritativeResize).toHaveBeenCalledOnce();
      expect(onAuthoritativeResize).toHaveBeenCalledWith({
        cols: 14,
        rows: 4,
        widthPx: 140,
        heightPx: 68,
      });
      expect(() => previous!.writePty(encoder.encode("late-old-write"))).toThrow(/disposed/);
      expect(() => previous!.resize({ cols: 10, rows: 3, widthPx: 100, heightPx: 51 })).toThrow(
        /disposed/,
      );

      candidate.writePty(encoder.encode("\u001b[2J\u001b[HAFTER-ADOPT"));
      flushAnimationFrames();
      expect(element.textContent).toContain("AFTER-ADOPT");
      expect(onAdopt).toHaveBeenCalledOnce();
    } finally {
      host.dispose();
    }
  });
});

async function candidateSnapshot(): Promise<Uint8Array> {
  const runtime = await GhosttyRuntime.load(committedWasm);
  const core = GhosttyCore.fromRuntime(runtime, {
    effects: "discard",
    scrollbackLimit: 1024 * 1024,
  });
  try {
    core.init(12, 3);
    core.writeRaw(
      encoder.encode(
        "\u001b[2J\u001b[HCANDIDATE\r\n" + "history-0\r\nhistory-1\r\nhistory-2\r\n" + "checkpoint",
      ),
    );
    return core.encodeSnapshot();
  } finally {
    core.dispose();
  }
}

function snapshotManifest(engineId: string, byteLength: number): SnapshotManifest {
  return {
    type: "snapshot-manifest",
    snapshotId: "snapshot_12345678",
    engineId,
    sessionEpoch: "7",
    streamId: 3,
    deliveryGeneration: "4",
    cutEventSeq: "10",
    nextPtyOffset: "20",
    commitEventSeq: "12",
    commitPtyOffset: "24",
    compression: "none",
    compressedLength: byteLength.toString(),
    uncompressedLength: byteLength.toString(),
    sha256: "a".repeat(64),
    downloadPath: "/api/v1/sessions/session_123456789/snapshots/snapshot_12345678",
    restoreThrough: "finish",
  };
}

function flushAnimationFrames(): void {
  let turns = 0;
  while (animationFrames.size > 0) {
    turns += 1;
    if (turns > 20) throw new Error("animation frame queue did not settle");
    const callbacks = [...animationFrames.values()];
    animationFrames.clear();
    for (const callback of callbacks) callback(performance.now());
  }
}
