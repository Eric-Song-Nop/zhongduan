import type { InputSink, TerminalInputEvent } from "@wterm/core";
import {
  DELIVERY_ENVELOPE_HEADER_BYTES,
  DataFrameFlag,
  DataFrameKind,
  encodeDataFrame,
  encodeDeliveryEnvelope,
  type DeliveryLane,
  type RecoveryStart,
  type ReplicaCursor,
  type ResizePayload,
} from "@zhongduan/protocol";
import type { ReplicaHost, ReplicaSink, SnapshotTransport } from "@zhongduan/session-client";
import { describe, expect, it, vi } from "vitest";

import { CapabilityManager } from "./capability";
import { InputDispatcher } from "./input-dispatcher";
import { TerminalSession } from "./terminal-session";

const SESSION_ID = "session_123456789";
const ENGINE_ID = "ghostty:test-engine";
const GENERATION = 3n;
const STREAM_ID = 7;

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

async function flushMicrotasks(turns = 12): Promise<void> {
  for (let index = 0; index < turns; index += 1) await Promise.resolve();
}

class ManualTimers {
  now = 0;
  readonly #scheduled = new Map<number, { at: number; callback: () => void }>();
  #nextId = 1;

  readonly setTimer = (callback: () => void, delayMs: number): ReturnType<typeof setTimeout> => {
    const id = this.#nextId++;
    this.#scheduled.set(id, { at: this.now + Math.max(0, delayMs), callback });
    return id as unknown as ReturnType<typeof setTimeout>;
  };

  readonly clearTimer = (timer: ReturnType<typeof setTimeout>): void => {
    this.#scheduled.delete(timer as unknown as number);
  };

  async advanceBy(delayMs: number): Promise<void> {
    const target = this.now + delayMs;
    for (;;) {
      await flushMicrotasks();
      const next = [...this.#scheduled.entries()]
        .filter(([, scheduled]) => scheduled.at <= target)
        .sort(([leftId, left], [rightId, right]) => left.at - right.at || leftId - rightId)[0];
      if (next === undefined) {
        this.now = target;
        await flushMicrotasks();
        return;
      }
      const [id, scheduled] = next;
      this.#scheduled.delete(id);
      this.now = scheduled.at;
      scheduled.callback();
    }
  }
}

class FakeSink implements ReplicaSink {
  readonly engineId = ENGINE_ID;
  readonly writes: number[][] = [];
  readonly resizes: ResizePayload[] = [];
  disposed = false;

  writePty(data: Uint8Array): void {
    this.writes.push([...data]);
  }

  resize(dimensions: ResizePayload): void {
    this.resizes.push({ ...dimensions });
  }

  dispose(): void {
    this.disposed = true;
  }
}

class FakeHost implements ReplicaHost {
  readonly engineId = ENGINE_ID;
  readonly restore = vi.fn<ReplicaHost["restore"]>();
  readonly adopt = vi.fn<ReplicaHost["adopt"]>((replica) => {
    this.current = replica;
  });

  constructor(public current: ReplicaSink | null) {}

  get active(): ReplicaSink | null {
    return this.current;
  }
}

function connectionSet(generation = GENERATION) {
  return {
    connectionSetId: "connection_set_canonical",
    connectionId: "connection_canonical",
    clientId: "browser_client_canonical",
    streamId: STREAM_ID,
    deliveryGeneration: generation.toString(),
    expiresAt: Date.now() + 30_000,
    controlTicket: "control_ticket_canonical",
    dataTicket: "data_ticket_canonical",
  } as const;
}

function controlFrames(socket: FakeSocket): Array<Record<string, unknown>> {
  return socket.sent.flatMap((value) => {
    if (typeof value !== "string" || !value.startsWith("{")) return [];
    return [JSON.parse(value) as Record<string, unknown>];
  });
}

function recoveryEnvelope(
  lane: DeliveryLane,
  ordinal: bigint,
  previousBytes: bigint,
  eventSeq: bigint,
  ptyOffset: bigint,
  payload: number[],
  kind: DataFrameKind = DataFrameKind.PtyOutput,
  generation = GENERATION,
): { bytes: bigint; raw: ArrayBuffer } {
  const canonical = encodeDataFrame({
    kind,
    flags: DataFrameFlag.None,
    sessionEpoch: 1n,
    deliveryGeneration: 0n,
    eventSeq,
    ptyOffset,
    streamId: 0,
    payload: Uint8Array.from(payload),
  });
  const bytes = previousBytes + BigInt(DELIVERY_ENVELOPE_HEADER_BYTES + canonical.byteLength);
  const envelope = encodeDeliveryEnvelope({
    lane,
    deliveryGeneration: generation,
    deliveryOrdinal: ordinal,
    cumulativeEncodedBytes: bytes,
    streamId: STREAM_ID,
    payload: canonical,
  });
  return { bytes, raw: envelope.buffer as ArrayBuffer };
}

function warmRecoveryStart(generation = GENERATION): RecoveryStart {
  return {
    type: "recovery-start",
    recoveryId: "recovery_browser_0001",
    deliveryGeneration: generation.toString(),
    streamId: STREAM_ID,
    engineId: ENGINE_ID,
    authorityDataFormat: 1,
    base: { sessionEpoch: "1", eventSeq: "10", nextPtyOffset: "20" },
    source: { kind: "warm" },
    committedThrough: { sessionEpoch: "1", eventSeq: "11", nextPtyOffset: "21" },
    liveFloor: { sessionEpoch: "1", nextEventSeq: "12", nextPtyOffset: "21" },
  };
}

function snapshotRecoveryStart(generation = GENERATION): RecoveryStart {
  return {
    ...warmRecoveryStart(generation),
    source: {
      kind: "snapshot",
      sessionId: SESSION_ID,
      snapshotId: "snapshot_browser_001",
      engineId: ENGINE_ID,
      sessionEpoch: "1",
      cutEventSeq: "10",
      nextPtyOffset: "20",
      compression: "none",
      compressedLength: "1",
      uncompressedLength: "1",
      sha256: "a".repeat(64),
      downloadPath: `/api/v1/sessions/${SESSION_ID}/snapshots/snapshot_browser_001`,
      restoreThrough: "finish",
    },
  };
}

function recoverySourceClosed(bytes: bigint, generation = GENERATION) {
  return {
    type: "recovery-source-closed",
    recoveryId: "recovery_browser_0001",
    deliveryGeneration: generation.toString(),
    throughRecoveryOrdinal: "2",
    throughRecoveryCumulativeEncodedBytes: bytes.toString(),
  } as const;
}

function welcome(writerLease?: string) {
  return {
    type: "welcome",
    connectionId: "connection_canonical",
    streamId: STREAM_ID,
    ...(writerLease === undefined ? {} : { writerLease }),
    engineId: ENGINE_ID,
    sessionEpoch: "1",
    deliveryGeneration: GENERATION.toString(),
    headEventSeq: "12",
    nextPtyOffset: "22",
  } as const;
}

interface OpenSessionOptions {
  readonly host?: FakeHost;
  readonly initialCursor?: ReplicaCursor;
  readonly input?: InputDispatcher;
  readonly response?: ReturnType<typeof connectionSet>;
  readonly responses?: readonly ReturnType<typeof connectionSet>[];
  readonly snapshots?: SnapshotTransport;
  readonly fetch?: typeof fetch;
  readonly timers?: ManualTimers;
}

function createSession(options: OpenSessionOptions = {}) {
  const requests: RequestInit[] = [];
  const responses = options.responses ?? [options.response ?? connectionSet()];
  let responseIndex = 0;
  const upstreamFetch: typeof fetch =
    options.fetch ??
    (async () => {
      const response = responses[Math.min(responseIndex, responses.length - 1)];
      responseIndex += 1;
      return Response.json(response);
    });
  const fetch: typeof globalThis.fetch = async (input, init) => {
    requests.push(init ?? {});
    return upstreamFetch(input, init);
  };
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
  const host = options.host ?? new FakeHost(new FakeSink());
  const input =
    options.input ??
    new InputDispatcher({ getObservedEventSeq: () => null, inputEpoch: "input_epoch_00001" });
  const sockets: FakeSocket[] = [];
  const session = new TerminalSession({
    capabilities,
    engineId: ENGINE_ID,
    host,
    input,
    ...(options.initialCursor === undefined ? {} : { initialCursor: options.initialCursor }),
    sessionId: SESSION_ID,
    snapshots: options.snapshots ?? { load: vi.fn() },
    fetch,
    createWebSocket: () => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket as unknown as WebSocket;
    },
    makeWebSocketUrl: (path) => path,
    ...(options.timers === undefined
      ? {}
      : {
          now: () => options.timers!.now,
          random: () => 0,
          setTimer: options.timers.setTimer,
          clearTimer: options.timers.clearTimer,
        }),
  });

  session.start();
  return { fetch, host, input, requests, session, sockets };
}

async function openSession(options: OpenSessionOptions = {}) {
  const opened = createSession(options);
  const { sockets } = opened;
  await vi.waitFor(() => expect(sockets).toHaveLength(1));
  const control = sockets[0]!;
  control.open();
  await vi.waitFor(() => expect(sockets).toHaveLength(2));
  const data = sockets[1]!;
  data.open();
  await vi.waitFor(() =>
    expect(controlFrames(control).some((frame) => frame.type === "attach")).toBe(true),
  );
  return {
    ...opened,
    control,
    data,
    request: opened.requests[0],
  };
}

async function openControlWithPendingData(options: OpenSessionOptions = {}) {
  const opened = createSession(options);
  await vi.waitFor(() => expect(opened.sockets).toHaveLength(1));
  const control = opened.sockets[0]!;
  control.open();
  await vi.waitFor(() => expect(opened.sockets).toHaveLength(2));
  return { ...opened, control, pendingData: opened.sockets[1]! };
}

function completeWarmRecovery(control: FakeSocket, data: FakeSocket): void {
  const recovery = recoveryEnvelope("recovery", 1n, 0n, 11n, 20n, [65]);
  const done = recoveryEnvelope(
    "recovery",
    2n,
    recovery.bytes,
    11n,
    21n,
    [],
    DataFrameKind.RecoveryDone,
  );
  const live = recoveryEnvelope("live", 1n, 0n, 12n, 21n, [66]);
  data.message(live.raw);
  data.message(recovery.raw);
  data.message(done.raw);
  control.message(JSON.stringify(warmRecoveryStart()));
  control.message(JSON.stringify(recoverySourceClosed(done.bytes)));
}

describe("TerminalSession", () => {
  it("bootstraps the sole connection protocol with its canonical fields", async () => {
    const { control, request, session } = await openSession();

    expect(new Headers(request?.headers).get("content-type")).toBe("application/json");
    expect(controlFrames(control).find((frame) => frame.type === "attach")).toMatchObject({
      deliveryGeneration: "3",
      hasLiveReplica: false,
    });
    expect(session.snapshot).toMatchObject({
      controlConnected: true,
      dataConnected: true,
      deliveryState: "awaiting-control",
      phase: "attaching",
    });
    session.close();
  });

  it("owns one generation, admits pre-start envelopes, and publishes canonical progress", async () => {
    const visible = new FakeSink();
    const initialCursor = {
      sessionEpoch: 1n,
      deliveryGeneration: 2n,
      lastEventSeq: 10n,
      nextPtyOffset: 20n,
    } as const;
    const { control, data, session } = await openSession({
      host: new FakeHost(visible),
      initialCursor,
    });

    expect(controlFrames(control).find((frame) => frame.type === "attach")).toMatchObject({
      deliveryGeneration: "3",
      hasLiveReplica: true,
      lastEventSeq: "10",
    });
    completeWarmRecovery(control, data);

    expect(visible.writes).toEqual([[65], [66]]);
    expect(session.snapshot).toMatchObject({ deliveryState: "live", phase: "live" });
    expect(session.activeCursor).toMatchObject({
      deliveryGeneration: 3n,
      lastEventSeq: 12n,
      nextPtyOffset: 22n,
    });
    const frames = controlFrames(control);
    expect(frames.some((frame) => frame.type === "recovery-adopted")).toBe(true);
    expect(frames.some((frame) => frame.type === "replica-applied")).toBe(true);
    expect(
      frames.some((frame) => frame.type === "delivery-received" && frame.lane === "recovery"),
    ).toBe(true);
    expect(frames.some((frame) => frame.type === "ack")).toBe(false);
    session.close();
  });

  it("feeds the runtime active cursor into semantic input", async () => {
    const visible = new FakeSink();
    let session!: TerminalSession;
    const input: InputSink & InputDispatcher = new InputDispatcher({
      getObservedEventSeq: () => session.activeCursor?.lastEventSeq ?? null,
      inputEpoch: "input_epoch_cursor",
    });
    const opened = await openSession({
      host: new FakeHost(visible),
      input,
      initialCursor: {
        sessionEpoch: 1n,
        deliveryGeneration: 2n,
        lastEventSeq: 10n,
        nextPtyOffset: 20n,
      },
    });
    session = opened.session;
    completeWarmRecovery(opened.control, opened.data);
    opened.control.message(JSON.stringify(welcome("writer_lease_cursor")));

    input.send(interruptKey());
    await vi.waitFor(() =>
      expect(controlFrames(opened.control).some((frame) => frame.type === "key")).toBe(true),
    );
    expect(controlFrames(opened.control).find((frame) => frame.type === "key")).toMatchObject({
      observedEventSeq: "12",
      writerLease: "writer_lease_cursor",
    });
    session.close();
  });

  it("restores directly from RecoveryStart.source and atomically adopts the cold target", async () => {
    const visible = new FakeSink();
    const candidate = new FakeSink();
    const host = new FakeHost(visible);
    host.restore.mockResolvedValue(candidate);
    const load = vi.fn<SnapshotTransport["load"]>().mockResolvedValue(Uint8Array.of(9));
    const { control, data, session } = await openSession({ host, snapshots: { load } });
    const recovery = recoveryEnvelope("recovery", 1n, 0n, 11n, 20n, [65]);
    const done = recoveryEnvelope(
      "recovery",
      2n,
      recovery.bytes,
      11n,
      21n,
      [],
      DataFrameKind.RecoveryDone,
    );
    const live = recoveryEnvelope("live", 1n, 0n, 12n, 21n, [66]);

    control.message(JSON.stringify(snapshotRecoveryStart()));
    data.message(recovery.raw);
    data.message(done.raw);
    data.message(live.raw);
    control.message(JSON.stringify(recoverySourceClosed(done.bytes)));

    await vi.waitFor(() => expect(session.snapshot.deliveryState).toBe("live"));
    const source = snapshotRecoveryStart().source;
    expect(source.kind).toBe("snapshot");
    expect(load).toHaveBeenCalledWith(source, expect.any(AbortSignal));
    expect(host.restore).toHaveBeenCalledWith(Uint8Array.of(9), source, expect.any(AbortSignal));
    expect(host.adopt).toHaveBeenCalledOnce();
    expect(host.active).toBe(candidate);
    expect(candidate.writes).toEqual([[65], [66]]);
    expect(visible.writes).toEqual([]);
    session.close();
  });

  it("reconnects the complete pair on a higher-generation resync", async () => {
    const opened = await openSession();
    opened.control.message(
      JSON.stringify({
        type: "resync-required",
        deliveryGeneration: "4",
        reason: "host-reconnect",
      }),
    );

    await vi.waitFor(() => expect(opened.control.readyState).toBe(3));
    expect(opened.data.readyState).toBe(3);
    expect(opened.session.snapshot).toMatchObject({
      controlConnected: false,
      dataConnected: false,
      phase: "reconnecting",
    });
    opened.session.close();
  });

  it("renews an active writer lease and stops renewing immediately after lease loss", async () => {
    const timers = new ManualTimers();
    const opened = await openSession({
      timers,
      initialCursor: {
        sessionEpoch: 1n,
        deliveryGeneration: 2n,
        lastEventSeq: 10n,
        nextPtyOffset: 20n,
      },
    });
    opened.control.message(JSON.stringify(welcome("writer_lease_renewal")));
    completeWarmRecovery(opened.control, opened.data);

    expect(opened.session.snapshot).toMatchObject({
      controlOwnership: "writer",
      deliveryState: "live",
      phase: "live",
    });
    expect(opened.input.status.writable).toBe(true);
    await timers.advanceBy(9_999);
    expect(
      controlFrames(opened.control).filter((frame) => frame.type === "writer-lease-renew"),
    ).toHaveLength(0);

    await timers.advanceBy(1);
    expect(
      controlFrames(opened.control).filter((frame) => frame.type === "writer-lease-renew"),
    ).toEqual([
      expect.objectContaining({
        type: "writer-lease-renew",
        writerLease: "writer_lease_renewal",
      }),
    ]);

    opened.control.message(JSON.stringify({ type: "writer-lease-status", active: false }));
    expect(opened.session.snapshot.controlOwnership).toBe("waiting");
    expect(opened.input.status.writable).toBe(false);
    await timers.advanceBy(30_000);
    expect(
      controlFrames(opened.control).filter((frame) => frame.type === "writer-lease-renew"),
    ).toHaveLength(1);
    expect(opened.control.readyState).toBe(1);
    expect(opened.data.readyState).toBe(1);
    opened.session.close();
  });

  it("reconnects the full pair when RecoveryStart makes no progress for 15 seconds", async () => {
    const timers = new ManualTimers();
    const opened = await openSession({ timers });

    expect(opened.session.snapshot.deliveryState).toBe("awaiting-control");
    await timers.advanceBy(14_999);
    expect(opened.control.readyState).toBe(1);
    expect(opened.data.readyState).toBe(1);

    await timers.advanceBy(1);
    expect(opened.control.readyState).toBe(3);
    expect(opened.data.readyState).toBe(3);
    expect(opened.session.snapshot).toMatchObject({
      controlConnected: false,
      dataConnected: false,
      lastError: "protocol",
      phase: "reconnecting",
    });
    opened.session.close();
  });

  it("enforces the 10 second connection-set deadline when fetch ignores abort", async () => {
    const timers = new ManualTimers();
    let requestSignal: AbortSignal | null | undefined;
    const ignoringFetch: typeof fetch = async (_input, init) => {
      requestSignal = init?.signal;
      return new Promise<Response>(() => {});
    };
    const opened = createSession({ fetch: ignoringFetch, timers });

    expect(opened.requests).toHaveLength(1);
    expect(requestSignal?.aborted).toBe(false);
    await timers.advanceBy(9_999);
    expect(opened.session.snapshot.phase).toBe("connecting");

    await timers.advanceBy(1);
    expect(requestSignal?.aborted).toBe(true);
    expect(opened.requests).toHaveLength(1);
    expect(opened.sockets).toHaveLength(0);
    expect(opened.session.snapshot).toMatchObject({
      lastError: "connection",
      phase: "reconnecting",
    });
    opened.session.close();
  });

  it.each(["resync", "close"] as const)(
    "cancels a pending data open on %s and fences its late callbacks",
    async (cause) => {
      const timers = new ManualTimers();
      const opened = await openControlWithPendingData({
        responses: [connectionSet(), connectionSet(4n)],
        timers,
      });

      if (cause === "resync") {
        opened.control.message(
          JSON.stringify({
            type: "resync-required",
            deliveryGeneration: "4",
            reason: "host-reconnect",
          }),
        );
      } else {
        opened.session.close();
      }
      await flushMicrotasks();

      expect(opened.control.readyState).toBe(3);
      expect(opened.pendingData.readyState).toBe(3);
      opened.pendingData.dispatchEvent(new Event("open"));
      opened.pendingData.message(new ArrayBuffer(1));
      await flushMicrotasks();
      expect(opened.session.snapshot.dataConnected).toBe(false);

      if (cause === "close") {
        expect(opened.sockets).toHaveLength(2);
        expect(opened.session.snapshot.phase).toBe("closed");
        return;
      }

      await timers.advanceBy(200);
      await vi.waitFor(() => expect(opened.sockets).toHaveLength(3));
      const replacementControl = opened.sockets[2]!;
      replacementControl.open();
      await vi.waitFor(() => expect(opened.sockets).toHaveLength(4));
      const replacementData = opened.sockets[3]!;
      replacementData.open();
      await vi.waitFor(() => expect(opened.session.snapshot.dataConnected).toBe(true));

      opened.pendingData.dispatchEvent(new Event("open"));
      opened.pendingData.message(new ArrayBuffer(1));
      await flushMicrotasks();
      expect(opened.session.snapshot).toMatchObject({
        controlConnected: true,
        dataConnected: true,
        phase: "attaching",
      });
      expect(
        controlFrames(replacementControl).find((frame) => frame.type === "attach"),
      ).toMatchObject({ deliveryGeneration: "4" });
      opened.session.close();
    },
  );

  it("cancels a pending data open on protocol failure and retries a fresh connection set", async () => {
    const timers = new ManualTimers();
    const opened = await openControlWithPendingData({
      responses: [connectionSet(), connectionSet(4n)],
      timers,
    });

    opened.control.message("not-json");
    await flushMicrotasks();
    expect(opened.control.readyState).toBe(3);
    expect(opened.pendingData.readyState).toBe(3);
    expect(opened.requests).toHaveLength(1);

    await timers.advanceBy(200);
    await vi.waitFor(() => expect(opened.requests).toHaveLength(2));
    await vi.waitFor(() => expect(opened.sockets).toHaveLength(3));
    const replacementControl = opened.sockets[2]!;
    replacementControl.open();
    await vi.waitFor(() => expect(opened.sockets).toHaveLength(4));
    const replacementData = opened.sockets[3]!;
    replacementData.open();
    await vi.waitFor(() => expect(opened.session.snapshot.dataConnected).toBe(true));

    opened.pendingData.dispatchEvent(new Event("open"));
    opened.pendingData.message(new ArrayBuffer(1));
    await flushMicrotasks();
    expect(opened.session.snapshot).toMatchObject({
      controlConnected: true,
      dataConnected: true,
      phase: "attaching",
    });
    expect(
      controlFrames(replacementControl).find((frame) => frame.type === "attach"),
    ).toMatchObject({ deliveryGeneration: "4" });
    opened.session.close();
  });

  it.each(["control", "data"] as const)(
    "reconnects the full pair when the %s heartbeat is silent for 45 seconds",
    async (silentSide) => {
      const timers = new ManualTimers();
      const opened = await openSession({
        timers,
        initialCursor: {
          sessionEpoch: 1n,
          deliveryGeneration: 2n,
          lastEventSeq: 10n,
          nextPtyOffset: 20n,
        },
      });
      completeWarmRecovery(opened.control, opened.data);
      opened[silentSide].autoPong = false;

      await timers.advanceBy(44_999);
      expect(opened.control.readyState).toBe(1);
      expect(opened.data.readyState).toBe(1);

      await timers.advanceBy(1);
      expect(opened.control.readyState).toBe(3);
      expect(opened.data.readyState).toBe(3);
      expect(opened.session.snapshot).toMatchObject({
        controlConnected: false,
        dataConnected: false,
        phase: "reconnecting",
      });
      opened.session.close();
    },
  );

  it("marks live writer input uncertain and closes both sockets on native queue overflow", async () => {
    const input = new InputDispatcher({
      getObservedEventSeq: () => 12n,
      inputEpoch: "input_epoch_backpressure",
    });
    const opened = await openSession({
      input,
      initialCursor: {
        sessionEpoch: 1n,
        deliveryGeneration: 2n,
        lastEventSeq: 10n,
        nextPtyOffset: 20n,
      },
    });
    opened.control.message(JSON.stringify(welcome("writer_lease_backpressure")));
    completeWarmRecovery(opened.control, opened.data);
    expect(input.status.writable).toBe(true);

    opened.control.bufferedAmount = 7 * 1024 * 1024;
    input.send(interruptKey());
    await vi.waitFor(() => expect(input.status.lastStatus).toBe("uncertain"));

    expect(input.status).toMatchObject({ connected: false, pending: 0, writable: false });
    expect(opened.control.readyState).toBe(3);
    expect(opened.data.readyState).toBe(3);
    expect(opened.session.snapshot).toMatchObject({
      controlConnected: false,
      dataConnected: false,
      phase: "reconnecting",
    });
    opened.session.close();
  });

  it("fails closed when a critical progress frame cannot enter the control queue", async () => {
    const visible = new FakeSink();
    const opened = await openSession({
      host: new FakeHost(visible),
      initialCursor: {
        sessionEpoch: 1n,
        deliveryGeneration: 2n,
        lastEventSeq: 10n,
        nextPtyOffset: 20n,
      },
    });
    opened.control.bufferedAmount = 7 * 1024 * 1024;
    opened.control.message(JSON.stringify(warmRecoveryStart()));
    opened.data.message(recoveryEnvelope("recovery", 1n, 0n, 11n, 20n, [65]).raw);

    await vi.waitFor(() => expect(opened.control.readyState).toBe(3));
    expect(opened.data.readyState).toBe(3);
    expect(opened.session.snapshot.phase).toBe("reconnecting");
    opened.session.close();
  });

  it("rejects a mismatched welcome engine without automatic reconnect", async () => {
    const opened = await openSession();
    opened.control.message(JSON.stringify({ ...welcome(), engineId: "other-engine" }));

    await vi.waitFor(() => expect(opened.session.snapshot.phase).toBe("failed"));
    expect(opened.session.snapshot.lastError).toBe("engine");
    expect(opened.sockets).toHaveLength(2);
    opened.session.close();
  });
});
