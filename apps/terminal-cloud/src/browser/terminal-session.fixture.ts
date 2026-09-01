import type { TerminalInputEvent, TerminalMouseInputEvent } from "@wterm/core";
import {
  DataFrameFlag,
  DataFrameKind,
  encodeDataFrame,
  type ResizePayload,
} from "@zhongduan/protocol";
import type { ReplicaHost, ReplicaSink } from "@zhongduan/session-client";
import { expect, vi } from "vitest";

import { CapabilityManager } from "./capability";
import { InputDispatcher } from "./input-dispatcher";
import { TerminalSession } from "./terminal-session";

export const SESSION_ID = "session_123456789";
export const ENGINE_ID = "ghostty:test-engine";
export const RESIZE = {
  type: "resize",
  cols: 100,
  rows: 30,
  widthPx: 900,
  heightPx: 600,
} satisfies TerminalInputEvent;

export function interruptKey(): TerminalInputEvent {
  return {
    type: "key",
    action: "press",
    altGraph: false,
    code: "KeyC",
    composing: false,
    consumedModifiers: 2,
    key: "c",
    modifiers: 2,
    repeat: false,
    text: "\u0003",
    unshiftedCodepoint: 99,
  };
}

export function mousePress(): TerminalMouseInputEvent {
  return {
    type: "mouse",
    action: "press",
    button: 0,
    buttons: 1,
    modifiers: 0,
    altGraph: false,
    surface: { x: 24, y: 24 },
    cell: { column: 2, row: 2 },
    viewport: {
      columns: 100,
      rows: 30,
      width: 900,
      height: 600,
      cellWidth: 9,
      cellHeight: 20,
    },
  };
}

export class FakeSocket extends EventTarget {
  binaryType: BinaryType = "blob";
  bufferedAmount = 0;
  readyState = 0;
  autoPong = true;
  readonly sent: Array<string | ArrayBufferLike | Blob | ArrayBufferView> = [];

  open(): void {
    this.readyState = 1;
    this.dispatchEvent(new Event("open"));
  }

  message(data: string | ArrayBuffer): void {
    this.dispatchEvent(new MessageEvent("message", { data }));
  }

  send(data: string | ArrayBufferLike | Blob | ArrayBufferView): void {
    if (this.readyState !== 1) throw new Error("socket is not open");
    this.sent.push(data);
    if (data === "ping" && this.autoPong) {
      queueMicrotask(() => {
        if (this.readyState === 1) this.message("pong");
      });
    }
  }

  close(): void {
    if (this.readyState >= 2) return;
    this.readyState = 3;
    this.dispatchEvent(new Event("close"));
  }

  remoteClose(code: number, reason: string): void {
    if (this.readyState >= 2) return;
    this.readyState = 3;
    const event = new Event("close");
    Object.defineProperties(event, {
      code: { value: code },
      reason: { value: reason },
    });
    this.dispatchEvent(event);
  }
}

export class VisibleSink implements ReplicaSink {
  readonly engineId = ENGINE_ID;
  readonly writes: number[][] = [];
  readonly resizes: ResizePayload[] = [];
  disposed = false;

  writePty(data: Uint8Array): void {
    this.writes.push([...data]);
  }

  resize(dimensions: ResizePayload): void {
    this.resizes.push(dimensions);
  }

  dispose(): void {
    this.disposed = true;
  }
}

export function controlFrames(socket: FakeSocket): Array<Record<string, unknown>> {
  return socket.sent.flatMap((value) => {
    if (typeof value !== "string" || !value.startsWith("{")) return [];
    return [JSON.parse(value) as Record<string, unknown>];
  });
}

export function dataFrame(
  generation: bigint,
  eventSeq: bigint,
  ptyOffset: bigint,
  payload: number[],
  kind: DataFrameKind = DataFrameKind.PtyOutput,
): ArrayBuffer {
  const encoded = encodeDataFrame({
    kind,
    flags: DataFrameFlag.None,
    sessionEpoch: 1n,
    deliveryGeneration: generation,
    eventSeq,
    ptyOffset,
    streamId: 7,
    payload: Uint8Array.from(payload),
  });
  return encoded.buffer as ArrayBuffer;
}

export function snapshotManifest(generation: bigint): Record<string, unknown> {
  return {
    type: "snapshot-manifest",
    snapshotId: `snapshot_retry_000${generation}`,
    engineId: ENGINE_ID,
    sessionEpoch: "1",
    streamId: 7,
    deliveryGeneration: generation.toString(),
    cutEventSeq: "5",
    nextPtyOffset: "10",
    commitEventSeq: "6",
    commitPtyOffset: "11",
    compression: "none",
    compressedLength: "1",
    uncompressedLength: "1",
    sha256: "a".repeat(64),
    downloadPath: `/api/v1/sessions/${SESSION_ID}/snapshots/snapshot_retry_000${generation}`,
    restoreThrough: "finish",
  };
}

export async function waitForSockets(sockets: FakeSocket[], count: number): Promise<void> {
  await vi.waitFor(() => expect(sockets).toHaveLength(count));
}

export class ManualTimers {
  now = 0;
  #nextId = 1;
  readonly #scheduled = new Map<number, { at: number; callback: () => void }>();

  readonly setTimer = (callback: () => void, delayMs: number): ReturnType<typeof setTimeout> => {
    const id = this.#nextId++;
    this.#scheduled.set(id, { at: this.now + delayMs, callback });
    return id as unknown as ReturnType<typeof setTimeout>;
  };

  readonly clearTimer = (timer: ReturnType<typeof setTimeout>): void => {
    this.#scheduled.delete(timer as unknown as number);
  };

  async runUntil(predicate: () => boolean): Promise<void> {
    for (let step = 0; step < 100 && !predicate(); step += 1) {
      const next = [...this.#scheduled.entries()].sort(
        ([leftId, left], [rightId, right]) => left.at - right.at || leftId - rightId,
      )[0];
      if (next === undefined) throw new Error("manual timer queue drained before predicate");
      const [id, task] = next;
      this.#scheduled.delete(id);
      this.now = task.at;
      task.callback();
      for (let turn = 0; turn < 8; turn += 1) await Promise.resolve();
    }
    if (!predicate()) throw new Error("manual timer predicate did not become true");
  }

  async advanceBy(delayMs: number): Promise<void> {
    const target = this.now + delayMs;
    while (true) {
      const next = [...this.#scheduled.entries()].sort(
        ([leftId, left], [rightId, right]) => left.at - right.at || leftId - rightId,
      )[0];
      if (next === undefined || next[1].at > target) break;
      const [id, task] = next;
      this.#scheduled.delete(id);
      this.now = task.at;
      task.callback();
      for (let turn = 0; turn < 8; turn += 1) await Promise.resolve();
    }
    this.now = target;
  }

  nextDueIn(): number | undefined {
    const next = [...this.#scheduled.values()].sort((left, right) => left.at - right.at)[0];
    return next === undefined ? undefined : next.at - this.now;
  }
}

export async function openRecoveryUnderWatchdog(
  timers: ManualTimers,
  mode: "cold-pending" | "warm-start",
): Promise<{
  control: FakeSocket;
  data: FakeSocket;
  session: TerminalSession;
}> {
  const response = {
    connectionSetId: "connection_set_warm_watchdog",
    connectionId: "connection_warm_watchdog",
    clientId: "browser_warm_watchdog",
    streamId: 7,
    deliveryGeneration: mode === "warm-start" ? "2" : "1",
    expiresAt: timers.now + 30_000,
    controlTicket: "control_ticket_warm_watchdog",
    dataTicket: "data_ticket_warm_watchdog",
    selectedCapabilities: ["browser-data-batch-v1", "browser-input-admission-v1"],
  };
  const fetch = vi.fn(
    async () =>
      new Response(JSON.stringify(response), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
  );
  const capabilities = new CapabilityManager({
    bootstrap: {
      capability: "opaque",
      expiresAt: Math.floor(timers.now / 1_000) + 1_000,
      issuedAt: Math.floor(timers.now / 1_000),
      role: "observer",
      sessionId: SESSION_ID,
    },
    fetch,
    setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
    clearTimer: vi.fn(),
  });
  const sockets: FakeSocket[] = [];
  const session = new TerminalSession({
    capabilities,
    engineId: ENGINE_ID,
    host: {
      engineId: ENGINE_ID,
      active: new VisibleSink(),
      restore: vi.fn<ReplicaHost["restore"]>(),
      adopt: vi.fn(),
    },
    input: new InputDispatcher({
      getObservedEventSeq: () => null,
      inputEpoch: "input_epoch_warm_watchdog",
    }),
    ...(mode === "warm-start"
      ? {
          initialCursor: {
            sessionEpoch: 1n,
            deliveryGeneration: 1n,
            lastEventSeq: 10n,
            nextPtyOffset: 20n,
          },
        }
      : {}),
    sessionId: SESSION_ID,
    snapshots: { load: vi.fn() },
    fetch,
    createWebSocket: () => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket as unknown as WebSocket;
    },
    makeWebSocketUrl: (path) => path,
    now: () => timers.now,
    random: () => 0,
    setTimer: timers.setTimer,
    clearTimer: timers.clearTimer,
  });

  session.start();
  await waitForSockets(sockets, 1);
  const control = sockets[0]!;
  control.open();
  await waitForSockets(sockets, 2);
  const data = sockets[1]!;
  data.open();
  await vi.waitFor(() => {
    expect(controlFrames(control).some((frame) => frame.type === "attach")).toBe(true);
  });
  if (mode === "cold-pending") return { control, data, session };
  control.message(
    JSON.stringify({
      type: "replay-start",
      sessionEpoch: "1",
      streamId: 7,
      deliveryGeneration: "2",
      baseEventSeq: "10",
      basePtyOffset: "20",
      commitEventSeq: "10",
      commitPtyOffset: "20",
    }),
  );
  expect(session.snapshot).toMatchObject({ deliveryState: "replaying", phase: "restoring" });
  return { control, data, session };
}
