import type { InputSink, TerminalInputEvent, TerminalMouseInputEvent } from "@wterm/core";
import {
  DataFrameFlag,
  DataFrameKind,
  encodeDataFrame,
  type ReplicaCursor,
  type ResizePayload,
} from "@zhongduan/protocol";
import type {
  ReplicaHost,
  ReplicaSink,
  SnapshotManifest,
  SnapshotTransport,
} from "@zhongduan/session-client";
import type { BrowserTelemetryEvent } from "@zhongduan/telemetry";
import { describe, expect, it, vi } from "vitest";

import { CapabilityManager } from "./capability";
import { createBrowserDiagnostics, type BrowserDiagnostics } from "./diagnostics-ring";
import { InputDispatcher } from "./input-dispatcher";
import {
  createBrowserPresentationDiagnostics,
  type BrowserPresentationDiagnostics,
} from "./presentation-diagnostics";
import { ATTACH_START_TIMEOUT_MS, TerminalSession } from "./terminal-session";

const SESSION_ID = "session_123456789";
const ENGINE_ID = "ghostty:test-engine";
const RESIZE: TerminalInputEvent = {
  type: "resize",
  cols: 100,
  rows: 30,
  widthPx: 900,
  heightPx: 600,
};

function interruptKey(): TerminalInputEvent {
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

function mousePress(): TerminalMouseInputEvent {
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

class FakeSocket extends EventTarget {
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
}

class VisibleSink implements ReplicaSink {
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

function controlFrames(socket: FakeSocket): Array<Record<string, unknown>> {
  return socket.sent.flatMap((value) => {
    if (typeof value !== "string" || !value.startsWith("{")) return [];
    return [JSON.parse(value) as Record<string, unknown>];
  });
}

function dataFrame(
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

function snapshotManifest(generation: bigint): Record<string, unknown> {
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

async function waitForSockets(sockets: FakeSocket[], count: number): Promise<void> {
  await vi.waitFor(() => expect(sockets).toHaveLength(count));
}

class ManualTimers {
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

interface DiagnosticHarness {
  control: FakeSocket;
  data: FakeSocket;
  diagnostics: BrowserDiagnostics;
  input: InputDispatcher;
  response: {
    connectionId: string;
    deliveryGeneration: string;
    streamId: number;
  };
  session: TerminalSession;
  sockets: FakeSocket[];
}

interface DiagnosticHarnessOptions {
  diagnostics?: BrowserDiagnostics;
  liveReplica?: boolean;
  monotonicNow?: () => number;
  now?: () => number;
  presentation?: BrowserPresentationDiagnostics;
  timers?: ManualTimers;
}

async function createDiagnosticHarness(
  options: DiagnosticHarnessOptions = {},
): Promise<DiagnosticHarness> {
  const timers = options.timers;
  const liveReplica = options.liveReplica ?? true;
  const visible = liveReplica ? new VisibleSink() : null;
  const response = {
    connectionSetId: "connection_set_diagnostics",
    connectionId: "connection_id_diagnostics",
    clientId: "browser_client_diagnostics",
    streamId: 7,
    deliveryGeneration: liveReplica ? "2" : "1",
    expiresAt: 2_000_000_030_000,
    controlTicket: "control_ticket_diagnostics",
    dataTicket: "data_ticket_diagnostics",
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
      expiresAt: 2_000_000_100,
      issuedAt: 2_000_000_000,
      role: "writer",
      sessionId: SESSION_ID,
    },
    fetch,
    setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
    clearTimer: vi.fn(),
  });
  const diagnostics = options.diagnostics ?? createBrowserDiagnostics();
  const input = new InputDispatcher({
    getObservedEventSeq: () => 0n,
    inputEpoch: "input_epoch_diagnostics",
    ...(options.presentation === undefined ? {} : { presentation: options.presentation }),
  });
  const sockets: FakeSocket[] = [];
  const session = new TerminalSession({
    capabilities,
    engineId: ENGINE_ID,
    host: {
      engineId: ENGINE_ID,
      active: visible,
      restore: vi.fn(async () => new VisibleSink()),
      adopt: vi.fn(),
    },
    input,
    ...(liveReplica
      ? {
          initialCursor: {
            sessionEpoch: 1n,
            deliveryGeneration: 1n,
            lastEventSeq: 0n,
            nextPtyOffset: 0n,
          },
        }
      : {}),
    sessionId: SESSION_ID,
    snapshots: { load: vi.fn(async () => Uint8Array.of(1)) },
    fetch,
    createWebSocket: () => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket as unknown as WebSocket;
    },
    diagnostics,
    ...(options.presentation === undefined ? {} : { presentation: options.presentation }),
    makeWebSocketUrl: (path) => path,
    ...(options.monotonicNow === undefined ? {} : { monotonicNow: options.monotonicNow }),
    ...(options.now === undefined ? {} : { now: options.now }),
    ...(timers === undefined ? {} : { setTimer: timers.setTimer, clearTimer: timers.clearTimer }),
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
  return { control, data, diagnostics, input, response, session, sockets };
}

function welcomeDiagnosticWriter(harness: DiagnosticHarness, writerLease = "writer_lease_diag01") {
  harness.control.message(
    JSON.stringify({
      type: "welcome",
      connectionId: harness.response.connectionId,
      streamId: harness.response.streamId,
      writerLease,
      engineId: ENGINE_ID,
      sessionEpoch: "1",
      deliveryGeneration: harness.response.deliveryGeneration,
      headEventSeq: "0",
      nextPtyOffset: "0",
    }),
  );
}

describe("TerminalSession delivery activation", () => {
  it("buffers data before replay-start and invalidates the old data callback before replacement", async () => {
    const sink = new VisibleSink();
    const host: ReplicaHost = {
      engineId: ENGINE_ID,
      active: sink,
      restore: vi.fn<ReplicaHost["restore"]>(),
      adopt: vi.fn(),
    };
    const snapshots: SnapshotTransport = {
      load: vi.fn<(manifest: SnapshotManifest, signal: AbortSignal) => Promise<Uint8Array>>(),
    };
    const sockets: FakeSocket[] = [];
    const connectionSets = [
      {
        connectionSetId: "connection_set_0001",
        connectionId: "connection_id_00001",
        clientId: "browser_client_0001",
        streamId: 7,
        deliveryGeneration: "2",
        expiresAt: Date.now() + 30_000,
        controlTicket: "control_ticket_0001",
        dataTicket: "data_ticket_000001",
      },
    ];
    const fetch = vi.fn(
      async () =>
        new Response(JSON.stringify(connectionSets.shift()), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
    );
    const capabilities = new CapabilityManager({
      bootstrap: {
        capability: "not-logged-or-decoded-here",
        expiresAt: Math.floor(Date.now() / 1_000) + 1_000,
        issuedAt: Math.floor(Date.now() / 1_000),
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch,
      setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
      clearTimer: vi.fn(),
    });
    let session!: TerminalSession;
    const input: InputSink & InputDispatcher = new InputDispatcher({
      getObservedEventSeq: () => session.coordinator.activeCursor?.lastEventSeq ?? null,
      inputEpoch: "input_epoch_00001",
    });
    const initialCursor: ReplicaCursor = {
      sessionEpoch: 1n,
      deliveryGeneration: 1n,
      lastEventSeq: 10n,
      nextPtyOffset: 20n,
    };
    session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host,
      input,
      initialCursor,
      sessionId: SESSION_ID,
      snapshots,
      fetch,
      createWebSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
      makeWebSocketUrl: (path) => path,
    });

    session.start();
    await waitForSockets(sockets, 1);
    const control = sockets[0]!;
    control.open();
    await waitForSockets(sockets, 2);
    const firstData = sockets[1]!;
    firstData.open();
    await vi.waitFor(() => {
      expect(controlFrames(control).some((frame) => frame.type === "attach")).toBe(true);
    });
    expect(controlFrames(control).find((frame) => frame.type === "attach")).toMatchObject({
      deliveryGeneration: "2",
    });

    firstData.message(dataFrame(2n, 11n, 20n, [65]));
    control.message(
      JSON.stringify({
        type: "welcome",
        connectionId: "connection_id_00001",
        streamId: 7,
        writerLease: "writer_lease_0001",
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "2",
        headEventSeq: "11",
        nextPtyOffset: "21",
      }),
    );
    expect(controlFrames(control).filter((frame) => frame.type === "ack")).toHaveLength(0);

    control.message(
      JSON.stringify({
        type: "replay-start",
        sessionEpoch: "1",
        streamId: 7,
        deliveryGeneration: "2",
        baseEventSeq: "10",
        basePtyOffset: "20",
        commitEventSeq: "11",
        commitPtyOffset: "21",
      }),
    );
    expect(sink.writes).toEqual([[65]]);
    expect(controlFrames(control).filter((frame) => frame.type === "ack")).toHaveLength(1);
    firstData.message(dataFrame(2n, 11n, 21n, [], DataFrameKind.ReplayCommit));
    expect(session.snapshot.phase).toBe("live");
    input.send(RESIZE);
    await vi.waitFor(() => {
      expect(
        controlFrames(control).filter((frame) => frame.type === "resize-request"),
      ).toHaveLength(1);
    });
    input.confirmAuthoritativeResize(RESIZE);
    control.message(JSON.stringify({ type: "host-offline" }));
    expect(session.snapshot.hostOnline).toBe(false);

    control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "3",
        reason: "data-disconnected",
        dataTicket: "replacement_ticket_1",
        expiresAt: Date.now() + 30_000,
      }),
    );
    expect(input.status.writable).toBe(true);
    expect(input.status.replicaCurrent).toBe(false);
    input.send(interruptKey());
    input.send(mousePress());
    await vi.waitFor(() => {
      expect(controlFrames(control).filter((frame) => frame.type === "key")).toHaveLength(1);
    });
    expect(controlFrames(control).filter((frame) => frame.type === "mouse")).toHaveLength(0);
    control.message(
      JSON.stringify({
        type: "replay-start",
        sessionEpoch: "1",
        streamId: 7,
        deliveryGeneration: "2",
        baseEventSeq: "10",
        basePtyOffset: "20",
        commitEventSeq: "11",
        commitPtyOffset: "21",
      }),
    );
    expect(session.snapshot.lastError).toBeNull();
    expect(session.snapshot.hostOnline).toBe(false);
    firstData.message(dataFrame(3n, 12n, 21n, [88]));
    await waitForSockets(sockets, 3);
    const replacementData = sockets[2]!;
    expect(control.readyState).toBe(1);
    replacementData.open();
    await vi.waitFor(() => {
      expect(controlFrames(control).filter((frame) => frame.type === "attach")).toHaveLength(2);
    });
    expect(
      controlFrames(control)
        .filter((frame) => frame.type === "attach")
        .at(-1),
    ).toMatchObject({
      deliveryGeneration: "3",
    });
    replacementData.message(dataFrame(3n, 12n, 21n, [66]));
    expect(
      controlFrames(control).filter(
        (frame) => frame.type === "ack" && frame.deliveryGeneration === "3",
      ),
    ).toHaveLength(0);
    control.message(
      JSON.stringify({
        type: "replay-start",
        sessionEpoch: "1",
        streamId: 7,
        deliveryGeneration: "3",
        baseEventSeq: "11",
        basePtyOffset: "21",
        commitEventSeq: "12",
        commitPtyOffset: "22",
      }),
    );
    expect(session.snapshot.hostOnline).toBe(true);
    expect(input.status.writable).toBe(true);
    expect(session.snapshot.controlOwnership).toBe("writer");
    replacementData.message(dataFrame(3n, 12n, 22n, [], DataFrameKind.ReplayCommit));
    input.send(mousePress());
    await vi.waitFor(() => {
      expect(controlFrames(control).filter((frame) => frame.type === "mouse")).toHaveLength(1);
    });

    expect(sink.writes).toEqual([[65], [66]]);
    expect(session.snapshot.phase).toBe("live");
    expect(input.status.replicaCurrent).toBe(true);
    expect(firstData.readyState).toBe(3);

    const acknowledgementsBeforeFailure = controlFrames(control).filter(
      (frame) => frame.type === "ack",
    ).length;
    control.message(
      JSON.stringify({
        type: "replay-start",
        sessionEpoch: "1",
        streamId: 8,
        deliveryGeneration: "3",
        baseEventSeq: "11",
        basePtyOffset: "21",
        commitEventSeq: "12",
        commitPtyOffset: "22",
      }),
    );
    expect(session.snapshot.lastError).toBe("protocol");
    expect(control.readyState).toBe(3);
    expect(replacementData.readyState).toBe(3);
    replacementData.message(dataFrame(3n, 13n, 22n, [67]));
    expect(sink.writes).toEqual([[65], [66]]);
    expect(controlFrames(control).filter((frame) => frame.type === "ack")).toHaveLength(
      acknowledgementsBeforeFailure,
    );
    session.close();
  });

  it("renews writer ownership across data replacement and stops after lease loss", async () => {
    const timers = new ManualTimers();
    timers.now = 2_000_000_000_000;
    const sink = new VisibleSink();
    const response = {
      connectionSetId: "connection_set_lease01",
      connectionId: "connection_id_lease001",
      clientId: "browser_client_lease01",
      streamId: 7,
      deliveryGeneration: "2",
      expiresAt: timers.now + 30_000,
      controlTicket: "control_ticket_lease01",
      dataTicket: "data_ticket_lease001",
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
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch,
      setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
      clearTimer: vi.fn(),
    });
    const input = new InputDispatcher({
      getObservedEventSeq: () => 10n,
      inputEpoch: "input_epoch_lease01",
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host: {
        engineId: ENGINE_ID,
        active: sink,
        restore: vi.fn<ReplicaHost["restore"]>(),
        adopt: vi.fn(),
      },
      input,
      initialCursor: {
        sessionEpoch: 1n,
        deliveryGeneration: 1n,
        lastEventSeq: 10n,
        nextPtyOffset: 20n,
      },
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
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });

    session.start();
    await waitForSockets(sockets, 1);
    const control = sockets[0]!;
    control.open();
    await waitForSockets(sockets, 2);
    sockets[1]!.open();
    control.message(
      JSON.stringify({
        type: "welcome",
        connectionId: response.connectionId,
        streamId: 7,
        writerLease: "writer_lease_lease01",
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "2",
        headEventSeq: "10",
        nextPtyOffset: "20",
      }),
    );

    await timers.advanceBy(10_000);
    expect(controlFrames(control).filter((frame) => frame.type === "writer-lease-renew")).toEqual([
      { type: "writer-lease-renew", writerLease: "writer_lease_lease01" },
    ]);

    control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "3",
        reason: "data-disconnected",
        dataTicket: "replacement_ticket_lease3",
        expiresAt: timers.now + 30_000,
      }),
    );
    await waitForSockets(sockets, 3);
    sockets[2]!.open();
    await timers.advanceBy(10_000);
    expect(
      controlFrames(control).filter((frame) => frame.type === "writer-lease-renew"),
    ).toHaveLength(2);
    expect(input.status.writable).toBe(true);

    control.message(JSON.stringify({ type: "writer-lease-status", active: false }));
    const renewalsAtLoss = controlFrames(control).filter(
      (frame) => frame.type === "writer-lease-renew",
    ).length;
    input.send(interruptKey());
    await timers.advanceBy(30_000);

    expect(session.snapshot.controlOwnership).toBe("waiting");
    expect(input.status.writable).toBe(false);
    expect(controlFrames(control).filter((frame) => frame.type === "key")).toHaveLength(0);
    expect(
      controlFrames(control).filter((frame) => frame.type === "writer-lease-renew"),
    ).toHaveLength(renewalsAtLoss);
    session.close();
  });

  it.each(["control", "data"] as const)(
    "fences and reconnects when the %s channel becomes a silent half-open socket",
    async (silentChannel) => {
      const timers = new ManualTimers();
      timers.now = 2_000_000_000_000;
      const response = {
        connectionSetId: `connection_set_${silentChannel}`,
        connectionId: `connection_id_${silentChannel}`,
        clientId: `browser_client_${silentChannel}`,
        streamId: 7,
        deliveryGeneration: "1",
        expiresAt: timers.now + 30_000,
        controlTicket: `control_ticket_${silentChannel}`,
        dataTicket: `data_ticket_${silentChannel}`,
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
          inputEpoch: `input_epoch_${silentChannel}`,
        }),
        sessionId: SESSION_ID,
        snapshots: { load: vi.fn(() => new Promise<Uint8Array>(() => undefined)) },
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
      control.message(JSON.stringify(snapshotManifest(1n)));
      (silentChannel === "control" ? control : data).autoPong = false;

      await timers.advanceBy(44_999);
      expect(control.readyState).toBe(1);
      expect(data.readyState).toBe(1);
      await timers.advanceBy(1);

      expect(control.readyState).toBe(3);
      expect(data.readyState).toBe(3);
      expect(session.snapshot.phase).toBe("reconnecting");
      session.close();
    },
  );

  it("allows the maximum concurrent cold service window, then fences a lost attach", async () => {
    const timers = new ManualTimers();
    timers.now = 2_000_000_000_000;
    const response = {
      connectionSetId: "connection_set_attach_timeout",
      connectionId: "connection_attach_timeout",
      clientId: "browser_attach_timeout",
      streamId: 7,
      deliveryGeneration: "1",
      expiresAt: timers.now + 30_000,
      controlTicket: "control_ticket_attach_timeout",
      dataTicket: "data_ticket_attach_timeout",
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
        inputEpoch: "input_epoch_attach_timeout",
      }),
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

    await timers.advanceBy(ATTACH_START_TIMEOUT_MS - 1);
    expect(control.readyState).toBe(1);
    expect(data.readyState).toBe(1);
    await timers.advanceBy(1);

    expect(control.readyState).toBe(3);
    expect(data.readyState).toBe(3);
    expect(session.snapshot).toMatchObject({ lastError: "connection", phase: "reconnecting" });
    session.close();
  });

  it("bounds a connection-set request even when fetch ignores abort", async () => {
    const timers = new ManualTimers();
    timers.now = 2_000_000_000_000;
    let requestSignal: AbortSignal | undefined;
    const fetch = vi.fn((_input: RequestInfo | URL, init?: RequestInit) => {
      requestSignal = init?.signal ?? undefined;
      return new Promise<Response>(() => undefined);
    });
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
        inputEpoch: "input_epoch_timeout1",
      }),
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
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledOnce());
    await timers.advanceBy(10_000);
    await vi.waitFor(() => expect(session.snapshot.phase).toBe("reconnecting"));

    expect(requestSignal?.aborted).toBe(true);
    expect(sockets).toHaveLength(0);
    session.close();
  });

  it("invalidates both sockets when a user input would exceed the native control queue", async () => {
    const sink = new VisibleSink();
    const host: ReplicaHost = {
      engineId: ENGINE_ID,
      active: sink,
      restore: vi.fn<ReplicaHost["restore"]>(),
      adopt: vi.fn(),
    };
    const response = {
      connectionSetId: "connection_set_queue1",
      connectionId: "connection_id_queue01",
      clientId: "browser_client_queue1",
      streamId: 7,
      deliveryGeneration: "2",
      expiresAt: Date.now() + 30_000,
      controlTicket: "control_ticket_queue1",
      dataTicket: "data_ticket_queue001",
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
        expiresAt: Math.floor(Date.now() / 1_000) + 1_000,
        issuedAt: Math.floor(Date.now() / 1_000),
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch,
      setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
      clearTimer: vi.fn(),
    });
    const input = new InputDispatcher({
      getObservedEventSeq: () => 10n,
      inputEpoch: "input_epoch_queue1",
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host,
      input,
      initialCursor: {
        sessionEpoch: 1n,
        deliveryGeneration: 1n,
        lastEventSeq: 10n,
        nextPtyOffset: 20n,
      },
      sessionId: SESSION_ID,
      snapshots: { load: vi.fn() },
      fetch,
      createWebSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
      makeWebSocketUrl: (path) => path,
    });

    session.start();
    await waitForSockets(sockets, 1);
    const control = sockets[0]!;
    control.open();
    await waitForSockets(sockets, 2);
    const data = sockets[1]!;
    data.open();
    control.message(
      JSON.stringify({
        type: "welcome",
        connectionId: response.connectionId,
        streamId: 7,
        writerLease: "writer_lease_queue1",
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "2",
        headEventSeq: "10",
        nextPtyOffset: "20",
      }),
    );

    control.bufferedAmount = Number.MAX_SAFE_INTEGER;
    const sentBeforeBackpressure = control.sent.length;
    input.send({ type: "text", text: "bounded", source: "input" });
    await vi.waitFor(() => expect(input.status.lastStatus).toBe("uncertain"));
    expect(control.sent).toHaveLength(sentBeforeBackpressure);
    expect(control.readyState).toBe(3);
    expect(data.readyState).toBe(3);
    session.close();
  });

  it("aborts superseded and closed data replacement attempts", async () => {
    const sink = new VisibleSink();
    const host: ReplicaHost = {
      engineId: ENGINE_ID,
      active: sink,
      restore: vi.fn<ReplicaHost["restore"]>(),
      adopt: vi.fn(),
    };
    const response = {
      connectionSetId: "connection_set_replace1",
      connectionId: "connection_id_replace1",
      clientId: "browser_client_replace1",
      streamId: 7,
      deliveryGeneration: "2",
      expiresAt: Date.now() + 30_000,
      controlTicket: "control_ticket_replace1",
      dataTicket: "data_ticket_replace01",
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
        expiresAt: Math.floor(Date.now() / 1_000) + 1_000,
        issuedAt: Math.floor(Date.now() / 1_000),
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch,
      setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
      clearTimer: vi.fn(),
    });
    const input = new InputDispatcher({
      getObservedEventSeq: () => 10n,
      inputEpoch: "input_epoch_replace1",
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host,
      input,
      initialCursor: {
        sessionEpoch: 1n,
        deliveryGeneration: 1n,
        lastEventSeq: 10n,
        nextPtyOffset: 20n,
      },
      sessionId: SESSION_ID,
      snapshots: { load: vi.fn() },
      fetch,
      createWebSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
      makeWebSocketUrl: (path) => path,
    });

    session.start();
    await waitForSockets(sockets, 1);
    const control = sockets[0]!;
    control.open();
    await waitForSockets(sockets, 2);
    sockets[1]!.open();
    await vi.waitFor(() => {
      expect(controlFrames(control).filter((frame) => frame.type === "attach")).toHaveLength(1);
    });

    control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "3",
        reason: "data-disconnected",
        dataTicket: "replacement_ticket_3",
        expiresAt: Date.now() + 30_000,
      }),
    );
    await waitForSockets(sockets, 3);
    const superseded = sockets[2]!;
    control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "4",
        reason: "data-disconnected",
        dataTicket: "replacement_ticket_4",
        expiresAt: Date.now() + 30_000,
      }),
    );
    await waitForSockets(sockets, 4);
    const current = sockets[3]!;
    expect(superseded.readyState).toBe(3);

    current.open();
    await vi.waitFor(() => {
      expect(controlFrames(control).filter((frame) => frame.type === "attach")).toHaveLength(2);
    });
    superseded.open();
    expect(controlFrames(control).filter((frame) => frame.type === "attach")).toHaveLength(2);

    control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "5",
        reason: "data-disconnected",
        dataTicket: "replacement_ticket_5",
        expiresAt: Date.now() + 30_000,
      }),
    );
    await waitForSockets(sockets, 5);
    const pendingAtClose = sockets[4]!;
    session.close();
    expect(pendingAtClose.readyState).toBe(3);
  });

  it("retries after a protocol failure aborts a pending data open", async () => {
    const sink = new VisibleSink();
    const host: ReplicaHost = {
      engineId: ENGINE_ID,
      active: sink,
      restore: vi.fn<ReplicaHost["restore"]>(),
      adopt: vi.fn(),
    };
    const responses = [2, 3].map((generation) => ({
      connectionSetId: `connection_set_000${generation}`,
      connectionId: `connection_id_0000${generation}`,
      clientId: "browser_client_0001",
      streamId: 7,
      deliveryGeneration: generation.toString(),
      expiresAt: Date.now() + 30_000,
      controlTicket: `control_ticket_000${generation}`,
      dataTicket: `data_ticket_00000${generation}`,
    }));
    const fetch = vi.fn(
      async () =>
        new Response(JSON.stringify(responses.shift()), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
    );
    const capabilities = new CapabilityManager({
      bootstrap: {
        capability: "opaque",
        expiresAt: Math.floor(Date.now() / 1_000) + 1_000,
        issuedAt: Math.floor(Date.now() / 1_000),
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch,
      setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
      clearTimer: vi.fn(),
    });
    const input = new InputDispatcher({
      getObservedEventSeq: () => 10n,
      inputEpoch: "input_epoch_00001",
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host,
      input,
      initialCursor: {
        sessionEpoch: 1n,
        deliveryGeneration: 1n,
        lastEventSeq: 10n,
        nextPtyOffset: 20n,
      },
      sessionId: SESSION_ID,
      snapshots: { load: vi.fn() },
      fetch,
      createWebSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
      makeWebSocketUrl: (path) => path,
      random: () => 0,
    });

    session.start();
    await waitForSockets(sockets, 1);
    sockets[0]!.open();
    await waitForSockets(sockets, 2);
    sockets[0]!.message("{}");

    await vi.waitFor(() => expect(fetch).toHaveBeenCalledTimes(2), { timeout: 2_000 });
    await waitForSockets(sockets, 3);
    expect(session.snapshot.attempt).toBe(2);
    session.close();
  });

  it("fails closed without reconnecting on a same-generation engine mismatch", async () => {
    const sink = new VisibleSink();
    const host: ReplicaHost = {
      engineId: ENGINE_ID,
      active: sink,
      restore: vi.fn<ReplicaHost["restore"]>(),
      adopt: vi.fn(),
    };
    const response = {
      connectionSetId: "connection_set_engine1",
      connectionId: "connection_id_engine01",
      clientId: "browser_client_engine1",
      streamId: 7,
      deliveryGeneration: "2",
      expiresAt: Date.now() + 30_000,
      controlTicket: "control_ticket_engine1",
      dataTicket: "data_ticket_engine001",
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
        expiresAt: Math.floor(Date.now() / 1_000) + 1_000,
        issuedAt: Math.floor(Date.now() / 1_000),
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch,
      setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
      clearTimer: vi.fn(),
    });
    const input = new InputDispatcher({
      getObservedEventSeq: () => 10n,
      inputEpoch: "input_epoch_engine1",
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host,
      input,
      initialCursor: {
        sessionEpoch: 1n,
        deliveryGeneration: 1n,
        lastEventSeq: 10n,
        nextPtyOffset: 20n,
      },
      sessionId: SESSION_ID,
      snapshots: { load: vi.fn() },
      fetch,
      createWebSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
      makeWebSocketUrl: (path) => path,
      random: () => 0,
    });

    session.start();
    await waitForSockets(sockets, 1);
    const control = sockets[0]!;
    control.open();
    await waitForSockets(sockets, 2);
    const pendingData = sockets[1]!;
    control.message(
      JSON.stringify({
        type: "welcome",
        connectionId: response.connectionId,
        streamId: 7,
        writerLease: "writer_lease_engine1",
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "2",
        headEventSeq: "10",
        nextPtyOffset: "20",
      }),
    );
    expect(input.status.writable).toBe(true);
    control.message(
      JSON.stringify({
        type: "welcome",
        connectionId: response.connectionId,
        streamId: 7,
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "2",
        headEventSeq: "10",
        nextPtyOffset: "20",
      }),
    );
    expect(input.status.writable).toBe(false);
    expect(session.snapshot.controlOwnership).toBe("waiting");
    control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "2",
        reason: "engine-mismatch",
      }),
    );

    expect(session.snapshot).toMatchObject({ lastError: "engine", phase: "failed" });
    expect(control.readyState).toBe(3);
    expect(pendingData.readyState).toBe(3);
    await new Promise((resolve) => setTimeout(resolve, 400));
    expect(fetch).toHaveBeenCalledOnce();
    expect(sockets).toHaveLength(2);
    session.close();
  });

  it("bounds permanent snapshot failures to six connection sets in the first minute", async () => {
    const timers = new ManualTimers();
    timers.now = 2_000_000_000_000;
    const visible = new VisibleSink();
    const host: ReplicaHost = {
      engineId: ENGINE_ID,
      active: visible,
      restore: vi.fn<ReplicaHost["restore"]>(),
      adopt: vi.fn(),
    };
    const snapshots: SnapshotTransport = {
      load: vi.fn(async () => {
        throw new Error("snapshot download failed with 503");
      }),
    };
    let nextGeneration = 1;
    const fetch = vi.fn(async () => {
      const generation = nextGeneration++;
      return new Response(
        JSON.stringify({
          connectionSetId: `connection_set_retry${generation}`,
          connectionId: `connection_id_retry${generation}`,
          clientId: "browser_client_retry1",
          streamId: 7,
          deliveryGeneration: generation.toString(),
          expiresAt: timers.now + 30_000,
          controlTicket: `control_ticket_retry${generation}`,
          dataTicket: `data_ticket_retry${generation}`,
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    });
    const capabilities = new CapabilityManager({
      bootstrap: {
        capability: "opaque",
        expiresAt: Math.floor(timers.now / 1_000) + 1_000,
        issuedAt: Math.floor(timers.now / 1_000),
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch,
      setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
      clearTimer: vi.fn(),
    });
    const input = new InputDispatcher({
      getObservedEventSeq: () => null,
      inputEpoch: "input_epoch_retry01",
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host,
      input,
      sessionId: SESSION_ID,
      snapshots,
      fetch,
      createWebSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
      makeWebSocketUrl: (path) => path,
      now: () => timers.now,
      random: () => 0.5,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });
    const startedAt = timers.now;

    session.start();
    for (let attempt = 0; attempt < 5; attempt += 1) {
      const controlIndex = attempt * 2;
      await waitForSockets(sockets, controlIndex + 1);
      const control = sockets[controlIndex]!;
      control.open();
      await waitForSockets(sockets, controlIndex + 2);
      const data = sockets[controlIndex + 1]!;
      data.open();
      await vi.waitFor(() => {
        expect(controlFrames(control).some((frame) => frame.type === "attach")).toBe(true);
      });
      const generation = BigInt(attempt + 1);
      control.message(
        JSON.stringify({
          type: "welcome",
          connectionId: `connection_id_retry${generation}`,
          streamId: 7,
          writerLease: "writer_lease_retry1",
          engineId: ENGINE_ID,
          sessionEpoch: "1",
          deliveryGeneration: generation.toString(),
          headEventSeq: "6",
          nextPtyOffset: "11",
        }),
      );
      control.message(JSON.stringify(snapshotManifest(generation)));
      await vi.waitFor(() => {
        expect(session.snapshot).toMatchObject({ lastError: "connection", phase: "reconnecting" });
      });
      expect(control.readyState).toBe(1);
      expect(data.readyState).toBe(1);
      expect(input.status.writable).toBe(true);
      expect(visible.disposed).toBe(false);

      const nextControlCount = controlIndex + 3;
      await timers.runUntil(() => sockets.length >= nextControlCount);
    }

    expect(timers.now - startedAt).toBe(60_000);
    expect(fetch).toHaveBeenCalledTimes(6);
    expect(sockets).toHaveLength(11);
    session.close();
  });

  it("cancels an old snapshot retry when a higher generation starts and resets backoff at live", async () => {
    const timers = new ManualTimers();
    timers.now = 2_000_000_000_000;
    const visible = new VisibleSink();
    const candidate = new VisibleSink();
    const adopted: ReplicaSink[] = [];
    const host: ReplicaHost = {
      engineId: ENGINE_ID,
      active: visible,
      restore: vi.fn(async () => candidate),
      adopt: vi.fn((replica) => adopted.push(replica)),
    };
    let resolveSecond!: (bytes: Uint8Array) => void;
    const loadSnapshot = vi
      .fn<SnapshotTransport["load"]>()
      .mockRejectedValueOnce(new Error("snapshot SHA-256 mismatch"))
      .mockImplementationOnce(
        () =>
          new Promise<Uint8Array>((resolve) => {
            resolveSecond = resolve;
          }),
      )
      .mockRejectedValueOnce(new Error("snapshot download failed with 503"));
    const snapshots: SnapshotTransport = { load: loadSnapshot };
    const response = {
      connectionSetId: "connection_set_snapshot1",
      connectionId: "connection_id_snapshot1",
      clientId: "browser_client_snapshot1",
      streamId: 7,
      deliveryGeneration: "1",
      expiresAt: timers.now + 30_000,
      controlTicket: "control_ticket_snapshot1",
      dataTicket: "data_ticket_snapshot01",
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
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch,
      setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
      clearTimer: vi.fn(),
    });
    const input = new InputDispatcher({
      getObservedEventSeq: () => null,
      inputEpoch: "input_epoch_snapshot1",
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host,
      input,
      sessionId: SESSION_ID,
      snapshots,
      fetch,
      createWebSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
      makeWebSocketUrl: (path) => path,
      now: () => timers.now,
      random: () => 0.5,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });

    session.start();
    await waitForSockets(sockets, 1);
    const control = sockets[0]!;
    control.open();
    await waitForSockets(sockets, 2);
    sockets[1]!.open();
    await vi.waitFor(() => {
      expect(controlFrames(control).some((frame) => frame.type === "attach")).toBe(true);
    });
    control.message(
      JSON.stringify({
        type: "welcome",
        connectionId: response.connectionId,
        streamId: 7,
        writerLease: "writer_lease_snapshot1",
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "1",
        headEventSeq: "6",
        nextPtyOffset: "11",
      }),
    );
    control.message(JSON.stringify(snapshotManifest(1n)));
    await vi.waitFor(() => expect(session.snapshot.lastError).toBe("connection"));

    control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "2",
        reason: "data-disconnected",
        dataTicket: "replacement_ticket_snapshot2",
        expiresAt: timers.now + 30_000,
      }),
    );
    await waitForSockets(sockets, 3);
    const generationTwoData = sockets[2]!;
    generationTwoData.open();
    await vi.waitFor(() => {
      expect(controlFrames(control).filter((frame) => frame.type === "attach")).toHaveLength(2);
    });
    control.message(
      JSON.stringify({
        type: "welcome",
        connectionId: response.connectionId,
        streamId: 7,
        writerLease: "writer_lease_snapshot1",
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "2",
        headEventSeq: "6",
        nextPtyOffset: "11",
      }),
    );
    control.message(JSON.stringify(snapshotManifest(2n)));
    await vi.waitFor(() => expect(loadSnapshot).toHaveBeenCalledTimes(2));

    await timers.advanceBy(3_000);
    expect(control.readyState).toBe(1);
    expect(generationTwoData.readyState).toBe(1);
    expect(fetch).toHaveBeenCalledOnce();

    generationTwoData.message(dataFrame(2n, 6n, 10n, [65]));
    generationTwoData.message(dataFrame(2n, 6n, 11n, [], DataFrameKind.ReplayCommit));
    resolveSecond(Uint8Array.of(1));
    await vi.waitFor(() => expect(session.snapshot.phase).toBe("live"));
    expect(adopted).toEqual([candidate]);

    control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "3",
        reason: "data-disconnected",
        dataTicket: "replacement_ticket_snapshot3",
        expiresAt: timers.now + 30_000,
      }),
    );
    await waitForSockets(sockets, 4);
    sockets[3]!.open();
    control.message(JSON.stringify(snapshotManifest(3n)));
    await vi.waitFor(() => expect(loadSnapshot).toHaveBeenCalledTimes(3));
    expect(timers.nextDueIn()).toBe(2_000);
    session.close();
  });

  it("disposes a detached candidate after a snapshot-tail failure without dropping control input", async () => {
    const visible = new VisibleSink();
    const candidate = new VisibleSink();
    const restore = vi.fn(async () => candidate);
    const adopt = vi.fn();
    const host: ReplicaHost = {
      engineId: ENGINE_ID,
      active: visible,
      restore,
      adopt,
    };
    const response = {
      connectionSetId: "connection_set_candidate1",
      connectionId: "connection_id_candidate1",
      clientId: "browser_client_candidate1",
      streamId: 7,
      deliveryGeneration: "1",
      expiresAt: Date.now() + 30_000,
      controlTicket: "control_ticket_candidate1",
      dataTicket: "data_ticket_candidate01",
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
        expiresAt: Math.floor(Date.now() / 1_000) + 1_000,
        issuedAt: Math.floor(Date.now() / 1_000),
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch,
      setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
      clearTimer: vi.fn(),
    });
    const input = new InputDispatcher({
      getObservedEventSeq: () => null,
      inputEpoch: "input_epoch_candidate1",
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host,
      input,
      sessionId: SESSION_ID,
      snapshots: { load: vi.fn(async () => Uint8Array.of(1)) },
      fetch,
      createWebSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
      makeWebSocketUrl: (path) => path,
    });

    session.start();
    await waitForSockets(sockets, 1);
    const control = sockets[0]!;
    control.open();
    await waitForSockets(sockets, 2);
    const data = sockets[1]!;
    data.open();
    control.message(
      JSON.stringify({
        type: "welcome",
        connectionId: response.connectionId,
        streamId: 7,
        writerLease: "writer_lease_candidate1",
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "1",
        headEventSeq: "6",
        nextPtyOffset: "11",
      }),
    );
    control.message(JSON.stringify(snapshotManifest(1n)));
    await vi.waitFor(() => expect(restore).toHaveBeenCalledOnce());
    for (let turn = 0; turn < 4; turn += 1) await Promise.resolve();

    data.message(dataFrame(1n, 5n, 10n, [], DataFrameKind.ReplayCommit));
    await vi.waitFor(() => expect(candidate.disposed).toBe(true));
    expect(visible.disposed).toBe(false);
    expect(adopt).not.toHaveBeenCalled();
    expect(control.readyState).toBe(1);
    expect(input.status.writable).toBe(true);
    expect(session.snapshot).toMatchObject({ lastError: "connection", phase: "reconnecting" });
    session.close();
  });

  it("keeps the old replica visible until a cold snapshot and its early tail commit", async () => {
    const visible = new VisibleSink();
    const candidate = new VisibleSink();
    const adopted: ReplicaSink[] = [];
    const host: ReplicaHost = {
      engineId: ENGINE_ID,
      active: visible,
      restore: vi.fn(async () => candidate),
      adopt: vi.fn((replica) => adopted.push(replica)),
    };
    const snapshots: SnapshotTransport = { load: vi.fn(async () => Uint8Array.of(1)) };
    const response = {
      connectionSetId: "connection_set_cold1",
      connectionId: "connection_id_cold01",
      clientId: "browser_client_cold1",
      streamId: 7,
      deliveryGeneration: "1",
      expiresAt: Date.now() + 30_000,
      controlTicket: "control_ticket_cold1",
      dataTicket: "data_ticket_cold001",
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
        expiresAt: Math.floor(Date.now() / 1_000) + 1_000,
        issuedAt: Math.floor(Date.now() / 1_000),
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch,
      setTimer: () => 1 as unknown as ReturnType<typeof setTimeout>,
      clearTimer: vi.fn(),
    });
    const input = new InputDispatcher({
      getObservedEventSeq: () => null,
      inputEpoch: "input_epoch_cold01",
    });
    const sockets: FakeSocket[] = [];
    const session = new TerminalSession({
      capabilities,
      engineId: ENGINE_ID,
      host,
      input,
      sessionId: SESSION_ID,
      snapshots,
      fetch,
      createWebSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
      makeWebSocketUrl: (path) => path,
    });

    session.start();
    await waitForSockets(sockets, 1);
    const control = sockets[0]!;
    control.open();
    await waitForSockets(sockets, 2);
    const data = sockets[1]!;
    data.open();
    await vi.waitFor(() => {
      expect(controlFrames(control).find((frame) => frame.type === "attach")).toMatchObject({
        hasLiveReplica: false,
      });
    });
    data.message(dataFrame(1n, 6n, 10n, [67]));
    data.message(dataFrame(1n, 6n, 11n, [], DataFrameKind.ReplayCommit));
    expect(visible.writes).toEqual([]);
    expect(adopted).toEqual([]);
    control.message(
      JSON.stringify({
        type: "welcome",
        connectionId: "connection_id_cold01",
        streamId: 7,
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        deliveryGeneration: "1",
        headEventSeq: "6",
        nextPtyOffset: "11",
      }),
    );
    control.message(
      JSON.stringify({
        type: "snapshot-manifest",
        snapshotId: "snapshot_cold_0001",
        engineId: ENGINE_ID,
        sessionEpoch: "1",
        streamId: 7,
        deliveryGeneration: "1",
        cutEventSeq: "5",
        nextPtyOffset: "10",
        commitEventSeq: "6",
        commitPtyOffset: "11",
        compression: "none",
        compressedLength: "1",
        uncompressedLength: "1",
        sha256: "a".repeat(64),
        downloadPath: "/api/v1/sessions/session_123456789/snapshots/snapshot_cold_0001",
        restoreThrough: "finish",
      }),
    );

    await vi.waitFor(() => expect(adopted).toEqual([candidate]));
    expect(visible.writes).toEqual([]);
    expect(candidate.writes).toEqual([[67]]);
    expect(session.snapshot.phase).toBe("live");
    session.close();
  });

  it("separates wall expiry from monotonic RTT and does not read the clock for binary data", async () => {
    const timers = new ManualTimers();
    let wallNow = 2_000_000_000_000;
    let monotonicReads = 0;
    const harness = await createDiagnosticHarness({
      timers,
      now: () => wallNow,
      monotonicNow: () => {
        monotonicReads += 1;
        return timers.now;
      },
    });
    harness.control.autoPong = false;
    harness.data.autoPong = false;

    const readsBeforeData = monotonicReads;
    harness.data.message(dataFrame(2n, 1n, 0n, [65]));
    expect(monotonicReads).toBe(readsBeforeData);

    await timers.advanceBy(15_000);
    wallNow += 1_000_000_000;
    await timers.advanceBy(25);
    harness.control.message("pong");
    await harness.diagnostics.flush();
    expect(
      harness.session.diagnostics.events.filter(
        (event) => event.name === "browser.relay.rtt" && event.outcome === "success",
      ),
    ).toContainEqual(
      expect.objectContaining({ channel: "control", durationMs: 25, outstandingPings: 0 }),
    );

    harness.control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "3",
        reason: "data-disconnected",
        dataTicket: "expired_replacement_ticket",
        expiresAt: wallNow - 1,
      }),
    );
    expect(harness.control.readyState).toBe(3);
    expect(harness.session.snapshot.lastError).toBe("protocol");
    harness.session.close();
  });

  it("observes only an exact visible canonical apply before render and frame opportunity", async () => {
    const timers = new ManualTimers();
    const diagnostics = createBrowserDiagnostics();
    const animationFrames: Array<(timestamp: number) => void> = [];
    const presentation = createBrowserPresentationDiagnostics("memory-v2", {
      telemetry: (event) => diagnostics.record(event),
      monotonicNow: () => timers.now,
      randomUint32: () => 0,
      setTimer: timers.setTimer,
      clearTimer: (handle) => timers.clearTimer(handle as ReturnType<typeof setTimeout>),
      requestAnimationFrame: (callback) => {
        animationFrames.push(callback);
        return animationFrames.length;
      },
      cancelAnimationFrame: vi.fn(),
    })!;
    const harness = await createDiagnosticHarness({
      diagnostics,
      monotonicNow: () => timers.now,
      presentation,
      timers,
    });
    welcomeDiagnosticWriter(harness);
    harness.control.message(
      JSON.stringify({
        type: "replay-start",
        sessionEpoch: "1",
        streamId: 7,
        deliveryGeneration: "2",
        baseEventSeq: "0",
        basePtyOffset: "0",
        commitEventSeq: "0",
        commitPtyOffset: "0",
      }),
    );
    harness.data.message(dataFrame(2n, 0n, 0n, [], DataFrameKind.ReplayCommit));
    expect(harness.session.snapshot.phase).toBe("live");

    await timers.advanceBy(10);
    harness.data.message(dataFrame(2n, 1n, 0n, [65]));
    presentation.renderCommitted();
    expect(animationFrames).toHaveLength(1);
    await timers.advanceBy(5);
    animationFrames[0]!(timers.now);
    await diagnostics.flush();

    expect(
      harness.session.diagnostics.events.filter(
        (event) => event.name === "browser.presentation.canonical",
      ),
    ).toEqual([
      expect.objectContaining({
        schemaVersion: 2,
        outcome: "next-frame-opportunity",
        frameKind: "pty-output",
        ingressToReplicaApplyMs: 0,
        replicaApplyToRenderCommitMs: 0,
        renderCommitToFrameOpportunityMs: 5,
        totalDurationMs: 5,
      }),
    ]);
    harness.session.close();
  });

  it("does not treat a detached snapshot candidate apply as visible presentation", async () => {
    const timers = new ManualTimers();
    const diagnostics = createBrowserDiagnostics();
    const presentation = createBrowserPresentationDiagnostics("memory-v2", {
      telemetry: (event) => diagnostics.record(event),
      monotonicNow: () => timers.now,
      randomUint32: () => 0,
      setTimer: timers.setTimer,
      clearTimer: (handle) => timers.clearTimer(handle as ReturnType<typeof setTimeout>),
      requestAnimationFrame: vi.fn(() => 1),
      cancelAnimationFrame: vi.fn(),
    })!;
    const harness = await createDiagnosticHarness({
      diagnostics,
      liveReplica: false,
      monotonicNow: () => timers.now,
      presentation,
      timers,
    });
    harness.control.message(JSON.stringify(snapshotManifest(1n)));
    harness.data.message(dataFrame(1n, 6n, 10n, [65]));
    presentation.renderCommitted();
    await diagnostics.flush();

    expect(
      harness.session.diagnostics.events.filter(
        (event) => event.name === "browser.presentation.canonical",
      ),
    ).toEqual([]);
    harness.session.close();
  });

  it("pairs overlapping control and data heartbeat probes independently in FIFO order", async () => {
    const timers = new ManualTimers();
    const harness = await createDiagnosticHarness({ timers, monotonicNow: () => timers.now });
    harness.control.autoPong = false;
    harness.data.autoPong = false;

    await timers.advanceBy(15_000);
    await timers.advanceBy(15_000);
    await timers.advanceBy(2_000);
    harness.control.message("pong");
    await timers.advanceBy(1_000);
    harness.data.message("pong");
    await timers.advanceBy(2_000);
    harness.control.message("pong");
    await timers.advanceBy(1_000);
    harness.data.message("pong");
    await harness.diagnostics.flush();

    expect(
      harness.session.diagnostics.events
        .filter((event) => event.name === "browser.relay.rtt" && event.outcome === "success")
        .map((event) => ({
          channel: event.channel,
          durationMs: event.durationMs,
          outstandingPings: event.outstandingPings,
        })),
    ).toEqual([
      { channel: "control", durationMs: 17_000, outstandingPings: 1 },
      { channel: "data", durationMs: 18_000, outstandingPings: 1 },
      { channel: "control", durationMs: 5_000, outstandingPings: 0 },
      { channel: "data", durationMs: 6_000, outstandingPings: 0 },
    ]);
    harness.session.close();
  });

  it("records heartbeat timeout before reconnect and resets probe pairing on new sockets", async () => {
    const timers = new ManualTimers();
    const harness = await createDiagnosticHarness({ timers, monotonicNow: () => timers.now });
    harness.data.autoPong = false;

    await timers.advanceBy(45_000);
    await harness.diagnostics.flush();
    expect(harness.session.diagnostics.events).toContainEqual(
      expect.objectContaining({
        name: "browser.relay.rtt",
        channel: "data",
        outcome: "timeout",
        silenceMs: 45_000,
        outstandingPings: 2,
      }),
    );
    expect(harness.control.readyState).toBe(3);
    expect(harness.data.readyState).toBe(3);
    harness.diagnostics.clear();

    await timers.runUntil(() => harness.sockets.length >= 3);
    const replacementControl = harness.sockets[2]!;
    replacementControl.autoPong = false;
    replacementControl.open();
    await waitForSockets(harness.sockets, 4);
    const replacementData = harness.sockets[3]!;
    replacementData.autoPong = false;
    replacementData.open();
    await vi.waitFor(() => expect(harness.session.snapshot.dataConnected).toBe(true));
    await timers.advanceBy(15_000);
    await timers.advanceBy(20);
    replacementControl.message("pong");
    replacementData.message("pong");
    await harness.diagnostics.flush();

    expect(
      harness.session.diagnostics.events.filter(
        (event) => event.name === "browser.relay.rtt" && event.outcome === "success",
      ),
    ).toEqual([
      expect.objectContaining({ channel: "control", durationMs: 20, outstandingPings: 0 }),
      expect.objectContaining({ channel: "data", durationMs: 20, outstandingPings: 0 }),
    ]);
    harness.session.close();
  });

  it("keeps heartbeat liveness and FIFO pairing correct when the diagnostic clock fails", async () => {
    const timers = new ManualTimers();
    let clockAvailable = true;
    const harness = await createDiagnosticHarness({
      timers,
      monotonicNow: () => {
        if (!clockAvailable) throw new Error("diagnostic clock unavailable");
        return timers.now;
      },
    });
    harness.control.autoPong = false;
    harness.data.autoPong = false;

    await timers.advanceBy(15_000);
    clockAvailable = false;
    harness.control.message("pong");
    harness.data.message("pong");
    clockAvailable = true;
    await timers.advanceBy(15_000);
    await timers.advanceBy(25);
    harness.control.message("pong");
    harness.data.message("pong");
    await harness.diagnostics.flush();

    expect(
      harness.session.diagnostics.events
        .filter((event) => event.name === "browser.relay.rtt" && event.outcome === "success")
        .map((event) => ({ channel: event.channel, durationMs: event.durationMs })),
    ).toEqual([
      { channel: "control", durationMs: 25 },
      { channel: "data", durationMs: 25 },
    ]);

    clockAvailable = false;
    await timers.advanceBy(45_000);
    expect(harness.control.readyState).toBe(3);
    expect(harness.data.readyState).toBe(3);
    harness.session.close();
  });

  it("preserves control RTT identity across a data-only socket replacement", async () => {
    const timers = new ManualTimers();
    const harness = await createDiagnosticHarness({
      timers,
      now: () => 2_000_000_000_000,
      monotonicNow: () => timers.now,
    });
    harness.control.autoPong = false;
    harness.data.autoPong = false;

    await timers.advanceBy(15_000);
    harness.control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "3",
        reason: "data-disconnected",
        dataTicket: "replacement_ticket_heartbeat",
        expiresAt: 2_000_000_030_000,
      }),
    );
    await waitForSockets(harness.sockets, 3);
    const replacementData = harness.sockets[2]!;
    replacementData.autoPong = false;
    replacementData.open();
    await vi.waitFor(() => expect(harness.session.snapshot.dataConnected).toBe(true));

    await timers.advanceBy(15_000);
    await timers.advanceBy(1);
    harness.control.message("pong");
    await timers.advanceBy(1);
    harness.control.message("pong");
    await harness.diagnostics.flush();

    expect(
      harness.session.diagnostics.events.flatMap((event) =>
        event.name === "browser.relay.rtt" &&
        event.outcome === "success" &&
        event.channel === "control"
          ? [
              {
                durationMs: event.durationMs,
                outstandingPings: event.outstandingPings,
              },
            ]
          : [],
      ),
    ).toEqual([
      { durationMs: 15_001, outstandingPings: 1 },
      { durationMs: 2, outstandingPings: 0 },
    ]);
    harness.session.close();
  });

  it("does not extend a silent control socket deadline across repeated data replacements", async () => {
    const timers = new ManualTimers();
    const harness = await createDiagnosticHarness({
      timers,
      now: () => 2_000_000_000_000,
      monotonicNow: () => timers.now,
    });
    harness.control.autoPong = false;
    harness.data.autoPong = false;

    for (const [generation, at] of [
      [3, 10_000],
      [4, 20_000],
    ] as const) {
      await timers.advanceBy(at - timers.now);
      harness.control.message(
        JSON.stringify({
          type: "resync-required",
          deliveryGeneration: generation.toString(),
          reason: "data-disconnected",
          dataTicket: `replacement_ticket_deadline_${generation}`,
          expiresAt: 2_000_000_030_000,
        }),
      );
      await waitForSockets(harness.sockets, generation);
      const replacementData = harness.sockets[generation - 1]!;
      replacementData.autoPong = false;
      replacementData.open();
      await vi.waitFor(() => expect(harness.session.snapshot.dataConnected).toBe(true));
    }

    await timers.advanceBy(25_000);
    expect(timers.now).toBe(45_000);
    expect(harness.control.readyState).toBe(3);
    expect(harness.session.snapshot.phase).toBe("reconnecting");
    harness.session.close();
  });

  it.each([
    { liveReplica: true, mode: "warm" as const, startingReplica: "live" as const },
    { liveReplica: false, mode: "snapshot" as const, startingReplica: "empty" as const },
  ])(
    "records $mode attach send to the matching accepted start",
    async ({ liveReplica, mode, startingReplica }) => {
      const timers = new ManualTimers();
      const harness = await createDiagnosticHarness({
        liveReplica,
        timers,
        monotonicNow: () => timers.now,
      });
      await timers.advanceBy(123);
      if (mode === "warm") {
        harness.control.message(
          JSON.stringify({
            type: "replay-start",
            sessionEpoch: "1",
            streamId: 7,
            deliveryGeneration: "2",
            baseEventSeq: "0",
            basePtyOffset: "0",
            commitEventSeq: "0",
            commitPtyOffset: "0",
          }),
        );
      } else {
        harness.control.message(JSON.stringify(snapshotManifest(1n)));
      }
      await harness.diagnostics.flush();

      expect(harness.session.diagnostics.events).toContainEqual(
        expect.objectContaining({
          name: "browser.recovery.attach-start",
          outcome: "matching-start-received",
          mode,
          startingReplica,
          durationMs: 123,
        }),
      );
      harness.session.close();
    },
  );

  it("records only a live attach watchdog timeout and never invents an accepted mode", async () => {
    const timers = new ManualTimers();
    const harness = await createDiagnosticHarness({ timers, monotonicNow: () => timers.now });

    await timers.advanceBy(ATTACH_START_TIMEOUT_MS);
    await harness.diagnostics.flush();
    const attachEvents = harness.session.diagnostics.events.filter(
      (event) => event.name === "browser.recovery.attach-start",
    );
    expect(attachEvents).toEqual([
      expect.objectContaining({
        outcome: "timeout",
        startingReplica: "live",
        durationMs: ATTACH_START_TIMEOUT_MS,
      }),
    ]);
    expect(attachEvents[0]).not.toHaveProperty("mode");
    expect(harness.session.snapshot.phase).toBe("reconnecting");
    harness.session.close();
  });

  it("records generation replacement and session close as terminal attach outcomes", async () => {
    const timers = new ManualTimers();
    const harness = await createDiagnosticHarness({
      timers,
      now: () => 2_000_000_000_000,
      monotonicNow: () => timers.now,
    });

    await timers.advanceBy(25);
    harness.control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "3",
        reason: "data-disconnected",
        dataTicket: "replacement_ticket_attach",
        expiresAt: 2_000_000_030_000,
      }),
    );
    await harness.diagnostics.flush();
    expect(
      harness.session.diagnostics.events.filter(
        (event) => event.name === "browser.recovery.attach-start",
      ),
    ).toEqual([
      expect.objectContaining({
        outcome: "cancelled",
        reason: "generation-replaced",
        durationMs: 25,
      }),
    ]);

    const closing = await createDiagnosticHarness({ timers, monotonicNow: () => timers.now });
    await timers.advanceBy(10);
    closing.session.close();
    await closing.diagnostics.flush();
    expect(
      closing.session.diagnostics.events.filter(
        (event) => event.name === "browser.recovery.attach-start",
      ),
    ).toEqual([
      expect.objectContaining({
        outcome: "cancelled",
        reason: "session-closed",
        durationMs: 10,
      }),
    ]);

    const invalidated = await createDiagnosticHarness({
      timers,
      monotonicNow: () => timers.now,
    });
    await timers.advanceBy(5);
    invalidated.control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "2",
        reason: "epoch-changed",
      }),
    );
    await invalidated.diagnostics.flush();
    expect(
      invalidated.session.diagnostics.events.filter(
        (event) => event.name === "browser.recovery.attach-start",
      ),
    ).toEqual([
      expect.objectContaining({
        outcome: "cancelled",
        reason: "connection-invalidated",
        durationMs: 5,
      }),
    ]);
    invalidated.session.close();
    harness.session.close();
  });

  it("routes an accepted input ACK into one sampled local lifecycle fact", async () => {
    const timers = new ManualTimers();
    const diagnostics = createBrowserDiagnostics();
    const presentation = createBrowserPresentationDiagnostics("memory-v2", {
      telemetry: (event) => diagnostics.record(event),
      monotonicNow: () => timers.now,
      randomUint32: () => 0,
      setTimer: timers.setTimer,
      clearTimer: (handle) => timers.clearTimer(handle as ReturnType<typeof setTimeout>),
    })!;
    const harness = await createDiagnosticHarness({
      diagnostics,
      monotonicNow: () => timers.now,
      presentation,
      timers,
    });
    welcomeDiagnosticWriter(harness);
    harness.input.send(interruptKey());
    await vi.waitFor(() => {
      expect(controlFrames(harness.control).filter((frame) => frame.type === "key")).toHaveLength(
        1,
      );
    });
    const frame = controlFrames(harness.control).find((candidate) => candidate.type === "key")!;
    await timers.advanceBy(40);
    const acknowledgement = JSON.stringify({
      type: "input-ack",
      inputEpoch: frame.inputEpoch,
      clientInputSeq: frame.clientInputSeq,
      status: "written",
      authorityEventSeq: "0",
    });
    harness.control.message(acknowledgement);
    harness.control.message(acknowledgement);
    await diagnostics.flush();

    const inputEvents = harness.session.diagnostics.events.filter(
      (event) => event.name === "browser.input.lifecycle",
    );
    expect(inputEvents).toEqual([
      expect.objectContaining({
        schemaVersion: 2,
        outcome: "ack-received",
        inputKind: "key",
        status: "written",
        dispatchToSendDecisionMs: 0,
        sendDecisionToAckMs: 40,
        dispatchToAckMs: 40,
      }),
    ]);
    const encodedDiagnostics = JSON.stringify(inputEvents);
    expect(encodedDiagnostics).not.toContain("input_epoch_diagnostics");
    expect(encodedDiagnostics).not.toContain("KeyC");
    expect(encodedDiagnostics).not.toContain("writer_lease_diag01");
    harness.session.close();
  });

  it.each(["throw", "full"] as const)(
    "keeps wire and input state live when diagnostics %s",
    async (mode) => {
      const diagnostics: BrowserDiagnostics =
        mode === "throw"
          ? {
              get droppedEvents() {
                return 0;
              },
              get pendingEvents() {
                return 0;
              },
              get retainedEvents() {
                return 0;
              },
              record(_event: BrowserTelemetryEvent) {
                throw new Error("diagnostic sink failed");
              },
              snapshot: () => ({ events: [], droppedEvents: 0, pendingEvents: 0 }),
              flush: () => Promise.resolve(),
              clear: () => undefined,
            }
          : createBrowserDiagnostics({
              capacity: 1,
              maxPendingEvents: 1,
              schedule: () => undefined,
            });
      const timers = new ManualTimers();
      const harness = await createDiagnosticHarness({
        diagnostics,
        timers,
        monotonicNow: () => timers.now,
      });
      welcomeDiagnosticWriter(harness);
      harness.input.send(interruptKey());
      await vi.waitFor(() => {
        expect(controlFrames(harness.control).filter((frame) => frame.type === "key")).toHaveLength(
          1,
        );
      });
      const frame = controlFrames(harness.control).find((candidate) => candidate.type === "key")!;
      harness.control.message(
        JSON.stringify({
          type: "input-ack",
          inputEpoch: frame.inputEpoch,
          clientInputSeq: frame.clientInputSeq,
          status: "written",
          authorityEventSeq: "0",
        }),
      );

      expect(harness.input.status).toMatchObject({ lastStatus: "written", pending: 0 });
      expect(harness.control.readyState).toBe(1);
      expect(harness.data.readyState).toBe(1);
      expect(
        controlFrames(harness.control).filter((candidate) => candidate.type === "key"),
      ).toEqual([frame]);
      harness.session.close();
    },
  );
});
