import {
  DataFrameKind,
  HostControlFrameSchema,
  decodeControlFrame,
  decodeDataFrame,
  decodeDeliveryEnvelope,
  decodeRecoveryStartFence,
  type ConnectionSetResponse,
  type ResizePayload,
} from "@zhongduan/protocol";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { FakeTerminalAuthority } from "../fake-terminal-authority";
import { EventJournal } from "../journal";
import type { PtyProcess } from "../pty-process";
import { TerminalSession } from "../session";
import type { SemanticMouse } from "../terminal-authority";
import { HostRelayConnection, HOST_CONTROL_QUEUE_LIMITS } from "./host-relay-connection";
import type { HostSocketPair } from "./paired-websocket";
import { RecoverySourceManager, type RecoverySourceManagerLimits } from "./recovery-source-manager";
import { HOST_RECOVERY_DATA_HIGH_WATER_BYTES } from "./recovery-source-scheduler";

class FakeWebSocket extends EventTarget {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  readonly sent: Array<string | ArrayBufferLike | Blob | ArrayBufferView> = [];
  binaryType: BinaryType = "arraybuffer";
  bufferedAmount = 0;
  readyState = FakeWebSocket.OPEN;
  closeCode: number | undefined;
  closeReason: string | undefined;
  bufferedAmountAfterBinarySend: number | undefined;
  throwOnBinarySend = false;

  close(code?: number, reason?: string): void {
    if (this.readyState >= FakeWebSocket.CLOSING) return;
    this.closeCode = code;
    this.closeReason = reason;
    this.readyState = FakeWebSocket.CLOSED;
    this.dispatchEvent(new Event("close"));
  }

  send(data: string | ArrayBufferLike | Blob | ArrayBufferView): void {
    if (this.throwOnBinarySend && data instanceof Uint8Array) {
      throw new Error("injected binary send failure");
    }
    this.sent.push(data);
    if (data instanceof Uint8Array && this.bufferedAmountAfterBinarySend !== undefined) {
      this.bufferedAmount = this.bufferedAmountAfterBinarySend;
    }
  }

  message(data: string): void {
    this.dispatchEvent(new MessageEvent("message", { data }));
  }
}

class ManualPty implements PtyProcess {
  readonly pid = 42;
  readonly writes: Uint8Array[] = [];
  onWrite: (() => void) | undefined;
  #dataListener: ((data: Uint8Array) => void) | undefined;
  #exitListener: ((exitCode: number, signal: number) => void) | undefined;

  onData(listener: (data: Uint8Array) => void): () => void {
    this.#dataListener = listener;
    return () => {
      if (this.#dataListener === listener) this.#dataListener = undefined;
    };
  }

  onExit(listener: (exitCode: number, signal: number) => void): () => void {
    this.#exitListener = listener;
    return () => {
      if (this.#exitListener === listener) this.#exitListener = undefined;
    };
  }

  write(data: Uint8Array): void {
    this.writes.push(data.slice());
    this.onWrite?.();
  }

  resize(_dimensions: ResizePayload): void {}
  kill(): void {
    this.#exitListener?.(0, 0);
  }
  emit(data: Uint8Array): void {
    this.#dataListener?.(data);
  }
}

const connectionSet: ConnectionSetResponse = {
  connectionSetId: "connection_set_AA",
  connectionId: "connection_AAAAA",
  clientId: null,
  streamId: 0,
  deliveryGeneration: "0",
  expiresAt: 2_000,
  controlTicket: "control_ticket_A",
  dataTicket: "data_ticket_AAAA",
};

const RECOVERY_LIMITS: RecoverySourceManagerLimits = {
  maxCanonicalBytesPerSource: 256 * 1024,
  maxCanonicalFramesPerSource: 512,
  maxOwnedRecords: 1_024,
  maxOwnedWireBytes: 2 * 1024 * 1024,
  maxSources: 32,
  noProgressDeadlineMs: 1_000,
  recoveryDeadlineMs: 10_000,
};

function createRelayHarness(options: {
  connection?: ConnectionSetResponse;
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  readyTimeoutMs?: number;
  monotonicNow?: () => number;
  recoveryDeadlineTickMs?: number;
  recoveryLimits?: RecoverySourceManagerLimits;
  recoverySourceManager?: RecoverySourceManager;
  session: TerminalSession;
}) {
  const control = new FakeWebSocket();
  const data = new FakeWebSocket();
  const pair: HostSocketPair = {
    connection: options.connection ?? connectionSet,
    control: control as unknown as WebSocket,
    data: data as unknown as WebSocket,
    close(code, reason) {
      data.close(code, reason);
      control.close(code, reason);
    },
  };
  const recoverySourceManager =
    options.recoverySourceManager ??
    new RecoverySourceManager({
      limits: options.recoveryLimits ?? RECOVERY_LIMITS,
      monotonicNow: options.monotonicNow ?? (() => performance.now()),
      session: options.session,
    });
  const relay = new HostRelayConnection({
    pair,
    ...(options.heartbeatIntervalMs === undefined
      ? {}
      : { heartbeatIntervalMs: options.heartbeatIntervalMs }),
    ...(options.heartbeatTimeoutMs === undefined
      ? {}
      : { heartbeatTimeoutMs: options.heartbeatTimeoutMs }),
    ...(options.monotonicNow === undefined ? {} : { monotonicNow: options.monotonicNow }),
    ...(options.readyTimeoutMs === undefined ? {} : { readyTimeoutMs: options.readyTimeoutMs }),
    ...(options.recoveryDeadlineTickMs === undefined
      ? {}
      : { recoveryDeadlineTickMs: options.recoveryDeadlineTickMs }),
    recoverySourceManager,
    session: options.session,
  });
  return { control, data, pair, recoverySourceManager, relay, session: options.session };
}

function createHarness(
  options: {
    authority?: FakeTerminalAuthority;
    connection?: ConnectionSetResponse;
    heartbeatIntervalMs?: number;
    heartbeatTimeoutMs?: number;
    monotonicNow?: () => number;
    readyTimeoutMs?: number;
    recoveryDeadlineTickMs?: number;
    recoveryLimits?: RecoverySourceManagerLimits;
  } = {},
) {
  const pty = new ManualPty();
  const session = new TerminalSession({
    authority: options.authority ?? new FakeTerminalAuthority(),
    journal: new EventJournal(),
    pty,
    sessionEpoch: 1n,
  });
  const relayHarness = createRelayHarness({
    ...options,
    session,
  });
  return { ...relayHarness, pty };
}

async function acknowledgeReady(harness: ReturnType<typeof createRelayHarness>): Promise<void> {
  const started = harness.relay.start();
  const ready = decodeControlFrame(harness.control.sent[0] as string, HostControlFrameSchema);
  if (ready.type !== "host-ready") throw new Error("expected host-ready");
  harness.control.message(
    JSON.stringify({
      type: "host-ready-ack",
      sessionEpoch: ready.sessionEpoch,
      headEventSeq: ready.headEventSeq,
      nextPtyOffset: ready.nextPtyOffset,
    }),
  );
  await started;
}

async function settle(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise<void>((resolve) => setImmediate(resolve));
  await Promise.resolve();
}

async function settleMicrotasks(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

function recoveryPrepare(session: TerminalSession, deliveryGeneration = "1") {
  return {
    type: "recovery-prepare" as const,
    recoveryId: "recovery-host-00000001",
    connectionId: "connection-browser-0001",
    streamId: 1,
    deliveryGeneration,
    engineId: session.engineId,
    base: {
      sessionEpoch: session.sessionEpoch.toString(),
      eventSeq: "0",
      nextPtyOffset: "0",
    },
    source: { kind: "warm" as const },
  };
}

beforeEach(() => vi.stubGlobal("WebSocket", FakeWebSocket));
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("HostRelayConnection", () => {
  it("does not pump canonical data before matching host-ready-ack", async () => {
    const harness = createHarness();
    const started = harness.relay.start();
    harness.pty.emit(Uint8Array.of(0x41));

    expect(harness.data.sent).toEqual([]);
    const ready = decodeControlFrame(harness.control.sent[0] as string, HostControlFrameSchema);
    expect(ready).toMatchObject({ type: "host-ready", headEventSeq: "0" });
    harness.control.message(
      JSON.stringify({
        type: "host-ready-ack",
        sessionEpoch: "1",
        headEventSeq: "0",
        nextPtyOffset: "0",
      }),
    );
    await started;

    expect(harness.data.sent).toHaveLength(1);
    expect(harness.data.sent[0]).toBeInstanceOf(Uint8Array);
    expect(decodeDataFrame(harness.data.sent[0] as Uint8Array)).toMatchObject({
      kind: DataFrameKind.PtyOutput,
      eventSeq: 1n,
    });
    harness.relay.close();
    harness.session.dispose();
  });

  it("strictly handles a recovery source through ordered fence, bounded Done, and exact close", async () => {
    const target = createHarness();
    await acknowledgeReady(target);
    const prepare = recoveryPrepare(target.session);
    const beforePrepareControl = target.control.sent.length;

    target.control.message(JSON.stringify(prepare));
    await vi.waitFor(() => expect(target.data.sent).toHaveLength(1));
    expect(target.control.sent).toHaveLength(beforePrepareControl);
    expect(decodeRecoveryStartFence(target.data.sent[0] as Uint8Array)).toMatchObject({
      recoveryId: prepare.recoveryId,
      connectionId: prepare.connectionId,
      streamId: prepare.streamId,
      deliveryGeneration: prepare.deliveryGeneration,
      base: prepare.base,
      committedThrough: prepare.base,
    });

    target.control.message(
      JSON.stringify({
        type: "recovery-start-ready",
        recoveryId: prepare.recoveryId,
        connectionId: prepare.connectionId,
        streamId: prepare.streamId,
        deliveryGeneration: prepare.deliveryGeneration,
        committedThrough: prepare.base,
        cumulativeGrantedEncodedBytes: "88",
      }),
    );
    await vi.waitFor(() => expect(target.data.sent).toHaveLength(2));
    const done = decodeDeliveryEnvelope(target.data.sent[1] as Uint8Array);
    expect(done).toMatchObject({
      lane: "recovery",
      deliveryGeneration: 1n,
      deliveryOrdinal: 1n,
      cumulativeEncodedBytes: 88n,
      streamId: 1,
    });
    expect(decodeDataFrame(done.payload)).toMatchObject({ kind: DataFrameKind.RecoveryDone });

    target.control.message(
      JSON.stringify({
        type: "recovery-source-received",
        recoveryId: prepare.recoveryId,
        connectionId: prepare.connectionId,
        streamId: prepare.streamId,
        deliveryGeneration: prepare.deliveryGeneration,
        lane: "recovery",
        contiguousDeliveryOrdinal: "1",
        cumulativeEncodedBytes: "88",
      }),
    );
    await vi.waitFor(() => expect(target.control.sent).toHaveLength(beforePrepareControl + 1));
    expect(
      decodeControlFrame(target.control.sent.at(-1) as string, HostControlFrameSchema),
    ).toEqual({
      type: "recovery-source-closed",
      recoveryId: prepare.recoveryId,
      connectionId: prepare.connectionId,
      streamId: prepare.streamId,
      deliveryGeneration: prepare.deliveryGeneration,
      throughRecoveryOrdinal: "1",
      throughRecoveryCumulativeEncodedBytes: "88",
    });
    expect(target.control.readyState).toBe(FakeWebSocket.OPEN);
    target.relay.close();
    target.session.dispose();
  });

  it("sends multiple granted records across fair data turns before an intermediate receipt", async () => {
    const target = createHarness();
    await acknowledgeReady(target);
    target.pty.emit(Uint8Array.of(0x41));
    await vi.waitFor(() => expect(target.data.sent).toHaveLength(1));
    const prepare = recoveryPrepare(target.session);
    target.control.message(JSON.stringify(prepare));
    await vi.waitFor(() => expect(target.data.sent).toHaveLength(2));
    const fence = decodeRecoveryStartFence(target.data.sent[1] as Uint8Array);

    target.control.message(
      JSON.stringify({
        type: "recovery-start-ready",
        recoveryId: prepare.recoveryId,
        connectionId: prepare.connectionId,
        streamId: prepare.streamId,
        deliveryGeneration: prepare.deliveryGeneration,
        committedThrough: fence.committedThrough,
        cumulativeGrantedEncodedBytes: "177",
      }),
    );
    await vi.waitFor(() => expect(target.data.sent).toHaveLength(4));
    const first = decodeDeliveryEnvelope(target.data.sent[2] as Uint8Array);
    expect(first).toMatchObject({ deliveryOrdinal: 1n, cumulativeEncodedBytes: 89n });
    const done = decodeDeliveryEnvelope(target.data.sent[3] as Uint8Array);
    expect(done).toMatchObject({ deliveryOrdinal: 2n, cumulativeEncodedBytes: 177n });
    expect(decodeDataFrame(done.payload)).toMatchObject({ kind: DataFrameKind.RecoveryDone });

    target.control.message(
      JSON.stringify({
        type: "recovery-source-received",
        recoveryId: prepare.recoveryId,
        connectionId: prepare.connectionId,
        streamId: prepare.streamId,
        deliveryGeneration: prepare.deliveryGeneration,
        lane: "recovery",
        contiguousDeliveryOrdinal: "1",
        cumulativeEncodedBytes: "89",
      }),
    );
    await settle();
    expect(target.data.sent).toHaveLength(4);
    expect(target.control.readyState).toBe(FakeWebSocket.OPEN);

    target.control.message(
      JSON.stringify({
        type: "recovery-source-received",
        recoveryId: prepare.recoveryId,
        connectionId: prepare.connectionId,
        streamId: prepare.streamId,
        deliveryGeneration: prepare.deliveryGeneration,
        lane: "recovery",
        contiguousDeliveryOrdinal: "2",
        cumulativeEncodedBytes: "177",
      }),
    );
    await vi.waitFor(() => expect(target.control.sent).toHaveLength(2));
    expect(
      decodeControlFrame(target.control.sent[1] as string, HostControlFrameSchema),
    ).toMatchObject({ type: "recovery-source-closed", throughRecoveryOrdinal: "2" });
    target.relay.close();
    target.session.dispose();
  });

  it("returns exact prepare rejection but fails the pair for an invalid source identity", async () => {
    const target = createHarness();
    await acknowledgeReady(target);
    const prepare = recoveryPrepare(target.session);

    target.control.message(JSON.stringify({ ...prepare, engineId: "wrong-engine/v1" }));
    await vi.waitFor(() => expect(target.control.sent).toHaveLength(2));
    expect(
      decodeControlFrame(target.control.sent[1] as string, HostControlFrameSchema),
    ).toMatchObject({ type: "recovery-prepare-rejected", reason: "engine-mismatch" });
    expect(target.control.readyState).toBe(FakeWebSocket.OPEN);

    target.control.message(
      JSON.stringify({
        type: "recovery-source-received",
        recoveryId: prepare.recoveryId,
        connectionId: prepare.connectionId,
        streamId: prepare.streamId,
        deliveryGeneration: prepare.deliveryGeneration,
        lane: "recovery",
        contiguousDeliveryOrdinal: "1",
        cumulativeEncodedBytes: "88",
      }),
    );
    await settle();
    expect(target.control.readyState).toBe(FakeWebSocket.CLOSED);
    expect(target.data.readyState).toBe(FakeWebSocket.CLOSED);
    target.session.dispose();
  });

  it("accepts an exact source reset before prepare without closing the Host pair", async () => {
    const target = createHarness();
    await acknowledgeReady(target);
    const prepare = recoveryPrepare(target.session);
    const beforeResetControl = target.control.sent.length;

    target.control.message(
      JSON.stringify({
        type: "recovery-source-reset",
        recoveryId: prepare.recoveryId,
        connectionId: prepare.connectionId,
        streamId: prepare.streamId,
        deliveryGeneration: prepare.deliveryGeneration,
        reason: "generation-reset",
      }),
    );
    await settle();

    expect(target.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(target.data.readyState).toBe(FakeWebSocket.OPEN);
    expect(target.recoverySourceManager.counters).toMatchObject({
      ownedRecords: 0,
      sources: 1,
    });

    target.control.message(JSON.stringify(prepare));
    await vi.waitFor(() => expect(target.control.sent).toHaveLength(beforeResetControl + 1));
    expect(
      decodeControlFrame(target.control.sent.at(-1) as string, HostControlFrameSchema),
    ).toMatchObject({
      type: "recovery-prepare-rejected",
      recoveryId: prepare.recoveryId,
      reason: "generation-fenced",
    });
    expect(target.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(target.data.readyState).toBe(FakeWebSocket.OPEN);
    target.relay.close();
    target.session.dispose();
  });

  it("fences a replaced generation across late control and an already-scheduled data turn", async () => {
    vi.useFakeTimers();
    const target = createHarness();
    await acknowledgeReady(target);
    const generationOne = recoveryPrepare(target.session, "1");
    const generationTwo = recoveryPrepare(target.session, "2");

    target.control.message(JSON.stringify(generationOne));
    await settleMicrotasks();
    expect(target.data.sent).toHaveLength(1);
    target.control.message(
      JSON.stringify({
        type: "recovery-start-ready",
        recoveryId: generationOne.recoveryId,
        connectionId: generationOne.connectionId,
        streamId: generationOne.streamId,
        deliveryGeneration: generationOne.deliveryGeneration,
        committedThrough: generationOne.base,
        cumulativeGrantedEncodedBytes: "88",
      }),
    );
    await settleMicrotasks();
    expect(target.data.sent).toHaveLength(1);

    target.control.message(JSON.stringify(generationTwo));
    await settleMicrotasks();
    await settleMicrotasks();
    expect(target.data.sent).toHaveLength(2);
    expect(decodeRecoveryStartFence(target.data.sent[1] as Uint8Array)).toMatchObject({
      deliveryGeneration: "2",
    });

    target.control.message(
      JSON.stringify({
        type: "recovery-start-ready",
        recoveryId: generationTwo.recoveryId,
        connectionId: generationTwo.connectionId,
        streamId: generationTwo.streamId,
        deliveryGeneration: generationTwo.deliveryGeneration,
        committedThrough: generationTwo.base,
        cumulativeGrantedEncodedBytes: "88",
      }),
    );
    target.control.message(
      JSON.stringify({
        type: "recovery-source-grant",
        recoveryId: generationOne.recoveryId,
        connectionId: generationOne.connectionId,
        streamId: generationOne.streamId,
        deliveryGeneration: generationOne.deliveryGeneration,
        cumulativeGrantedEncodedBytes: "88",
      }),
    );
    target.control.message(
      JSON.stringify({
        type: "recovery-source-received",
        recoveryId: generationOne.recoveryId,
        connectionId: generationOne.connectionId,
        streamId: generationOne.streamId,
        deliveryGeneration: generationOne.deliveryGeneration,
        lane: "recovery",
        contiguousDeliveryOrdinal: "1",
        cumulativeEncodedBytes: "88",
      }),
    );
    target.control.message(
      JSON.stringify({
        type: "recovery-source-reset",
        recoveryId: generationOne.recoveryId,
        connectionId: generationOne.connectionId,
        streamId: generationOne.streamId,
        deliveryGeneration: generationOne.deliveryGeneration,
        reason: "generation-reset",
      }),
    );
    await settleMicrotasks();
    await settleMicrotasks();
    expect(target.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(target.data.readyState).toBe(FakeWebSocket.OPEN);

    await vi.advanceTimersByTimeAsync(0);
    expect(target.data.sent).toHaveLength(3);
    expect(decodeDeliveryEnvelope(target.data.sent[2] as Uint8Array)).toMatchObject({
      deliveryGeneration: 2n,
      deliveryOrdinal: 1n,
    });
    expect(target.control.readyState).toBe(FakeWebSocket.OPEN);

    target.control.message(
      JSON.stringify({
        type: "recovery-source-grant",
        recoveryId: "recovery-host-divergent",
        connectionId: generationOne.connectionId,
        streamId: generationOne.streamId,
        deliveryGeneration: generationOne.deliveryGeneration,
        cumulativeGrantedEncodedBytes: "88",
      }),
    );
    await settleMicrotasks();
    expect(target.control.readyState).toBe(FakeWebSocket.CLOSED);
    expect(target.data.readyState).toBe(FakeWebSocket.CLOSED);
    target.session.dispose();
  });

  it("fences the owner on a recovery data send throw before later grants can reuse the pair", async () => {
    const target = createHarness();
    await acknowledgeReady(target);
    const prepare = recoveryPrepare(target.session);
    target.control.message(JSON.stringify(prepare));
    await vi.waitFor(() => expect(target.data.sent).toHaveLength(1));
    target.data.throwOnBinarySend = true;

    target.control.message(
      JSON.stringify({
        type: "recovery-start-ready",
        recoveryId: prepare.recoveryId,
        connectionId: prepare.connectionId,
        streamId: prepare.streamId,
        deliveryGeneration: prepare.deliveryGeneration,
        committedThrough: prepare.base,
        cumulativeGrantedEncodedBytes: "88",
      }),
    );
    await vi.waitFor(() => expect(target.control.readyState).toBe(FakeWebSocket.CLOSED));
    expect(target.control.closeReason).toContain("injected binary send failure");
    expect(target.recoverySourceManager.counters).toEqual({
      ownedRecords: 0,
      ownedWireBytes: 0,
      pendingSources: 0,
      sources: 0,
    });
    const sentBeforeLateGrant = target.data.sent.length;
    target.control.message(
      JSON.stringify({
        type: "recovery-source-grant",
        recoveryId: prepare.recoveryId,
        connectionId: prepare.connectionId,
        streamId: prepare.streamId,
        deliveryGeneration: prepare.deliveryGeneration,
        cumulativeGrantedEncodedBytes: "176",
      }),
    );
    await settle();
    expect(target.data.sent).toHaveLength(sentBeforeLateGrant);
    target.session.dispose();
  });

  it("yields recovery at shared data backpressure while canonical traffic stays live", async () => {
    vi.useFakeTimers();
    const open = async () => {
      const harness = createHarness();
      await acknowledgeReady(harness);
      const prepare = recoveryPrepare(harness.session);
      harness.control.message(JSON.stringify(prepare));
      await vi.waitFor(() => expect(harness.data.sent).toHaveLength(1));
      return { harness, prepare };
    };
    const start = (
      harness: ReturnType<typeof createHarness>,
      prepare: ReturnType<typeof recoveryPrepare>,
    ) =>
      harness.control.message(
        JSON.stringify({
          type: "recovery-start-ready",
          recoveryId: prepare.recoveryId,
          connectionId: prepare.connectionId,
          streamId: prepare.streamId,
          deliveryGeneration: prepare.deliveryGeneration,
          committedThrough: prepare.base,
          cumulativeGrantedEncodedBytes: "88",
        }),
      );

    const blocked = await open();
    blocked.harness.data.bufferedAmount = HOST_RECOVERY_DATA_HIGH_WATER_BYTES;
    start(blocked.harness, blocked.prepare);
    await vi.advanceTimersByTimeAsync(0);
    expect(blocked.harness.data.sent).toHaveLength(1);
    expect(blocked.harness.control.readyState).toBe(FakeWebSocket.OPEN);

    blocked.harness.pty.emit(Uint8Array.of(0x41));
    await settleMicrotasks();
    expect(blocked.harness.data.sent).toHaveLength(2);
    expect(decodeDataFrame(blocked.harness.data.sent[1] as Uint8Array)).toMatchObject({
      kind: DataFrameKind.PtyOutput,
    });

    blocked.harness.data.bufferedAmount = HOST_RECOVERY_DATA_HIGH_WATER_BYTES - 88;
    await vi.advanceTimersByTimeAsync(10);
    expect(blocked.harness.data.sent).toHaveLength(3);
    expect(decodeDeliveryEnvelope(blocked.harness.data.sent[2] as Uint8Array)).toMatchObject({
      lane: "recovery",
      deliveryOrdinal: 1n,
    });
    expect(blocked.harness.control.readyState).toBe(FakeWebSocket.OPEN);
    blocked.harness.session.dispose();

    const accepted = await open();
    accepted.harness.data.bufferedAmount = HOST_RECOVERY_DATA_HIGH_WATER_BYTES - 88;
    accepted.harness.data.bufferedAmountAfterBinarySend = HOST_RECOVERY_DATA_HIGH_WATER_BYTES + 1;
    start(accepted.harness, accepted.prepare);
    await vi.advanceTimersByTimeAsync(0);
    expect(accepted.harness.data.sent).toHaveLength(2);
    expect((accepted.harness.data.sent[1] as Uint8Array).byteLength).toBe(88);
    expect(accepted.harness.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(accepted.harness.recoverySourceManager.counters).toMatchObject({ ownedRecords: 1 });
    accepted.harness.relay.close();
    accepted.harness.session.dispose();
  });

  it("retires only an expired source and ignores its late exact control on the healthy pair", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const target = createHarness({
      monotonicNow: () => Date.now(),
      recoveryDeadlineTickMs: 10,
      recoveryLimits: {
        ...RECOVERY_LIMITS,
        noProgressDeadlineMs: 10,
        recoveryDeadlineMs: 100,
      },
    });
    await acknowledgeReady(target);
    const prepare = recoveryPrepare(target.session);
    target.control.message(JSON.stringify(prepare));
    await settleMicrotasks();
    expect(target.data.sent).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(10);
    expect(target.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(target.data.readyState).toBe(FakeWebSocket.OPEN);
    expect(target.recoverySourceManager.counters).toMatchObject({ ownedRecords: 0, sources: 1 });

    target.control.message(
      JSON.stringify({
        type: "recovery-source-grant",
        recoveryId: prepare.recoveryId,
        connectionId: prepare.connectionId,
        streamId: prepare.streamId,
        deliveryGeneration: prepare.deliveryGeneration,
        cumulativeGrantedEncodedBytes: "88",
      }),
    );
    target.control.message(
      JSON.stringify({
        type: "recovery-source-received",
        recoveryId: prepare.recoveryId,
        connectionId: prepare.connectionId,
        streamId: prepare.streamId,
        deliveryGeneration: prepare.deliveryGeneration,
        lane: "recovery",
        contiguousDeliveryOrdinal: "1",
        cumulativeEncodedBytes: "88",
      }),
    );
    await settleMicrotasks();
    expect(target.control.closeReason).toBeUndefined();
    expect(target.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(target.data.readyState).toBe(FakeWebSocket.OPEN);
    target.session.dispose();
  });

  it("keeps the pair live when a pending prepare crosses its exact deadline", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const target = createHarness({
      monotonicNow: () => Date.now(),
      recoveryDeadlineTickMs: 10,
      recoveryLimits: {
        ...RECOVERY_LIMITS,
        noProgressDeadlineMs: 100,
        recoveryDeadlineMs: 10,
      },
    });
    const pending = deferred<Awaited<ReturnType<TerminalSession["prepareRecoveryGap"]>>>();
    vi.spyOn(target.session, "prepareRecoveryGap").mockReturnValue(pending.promise);
    await acknowledgeReady(target);
    const prepare = recoveryPrepare(target.session);

    target.control.message(JSON.stringify(prepare));
    await settleMicrotasks();
    expect(target.recoverySourceManager.counters).toMatchObject({ pendingSources: 1 });

    await vi.advanceTimersByTimeAsync(10);
    expect(target.recoverySourceManager.counters).toMatchObject({ pendingSources: 0, sources: 1 });
    expect(target.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(target.data.readyState).toBe(FakeWebSocket.OPEN);

    pending.resolve({ status: "unavailable", reason: "fence-unavailable" });
    await settleMicrotasks();
    expect(target.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(target.data.readyState).toBe(FakeWebSocket.OPEN);
    target.relay.close();
    target.session.dispose();
  });

  it("lets a live source continue after another source on the same pair expires", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const target = createHarness({
      monotonicNow: () => Date.now(),
      recoveryDeadlineTickMs: 10,
      recoveryLimits: {
        ...RECOVERY_LIMITS,
        noProgressDeadlineMs: 10,
        recoveryDeadlineMs: 100,
      },
    });
    await acknowledgeReady(target);
    target.pty.emit(Uint8Array.of(0x41));
    await settleMicrotasks();
    const expired = recoveryPrepare(target.session);
    const live = {
      ...expired,
      recoveryId: "recovery-host-00000002",
      connectionId: "connection-browser-0002",
      streamId: 2,
    };

    target.control.message(JSON.stringify(expired));
    target.control.message(JSON.stringify(live));
    await settleMicrotasks();
    await vi.advanceTimersByTimeAsync(0);
    expect(target.data.sent).toHaveLength(3);
    const liveFence = decodeRecoveryStartFence(target.data.sent[2] as Uint8Array);

    target.control.message(
      JSON.stringify({
        type: "recovery-start-ready",
        recoveryId: live.recoveryId,
        connectionId: live.connectionId,
        streamId: live.streamId,
        deliveryGeneration: live.deliveryGeneration,
        committedThrough: liveFence.committedThrough,
        cumulativeGrantedEncodedBytes: "89",
      }),
    );
    await vi.advanceTimersByTimeAsync(0);
    expect(decodeDeliveryEnvelope(target.data.sent[3] as Uint8Array)).toMatchObject({
      streamId: 2,
      deliveryOrdinal: 1n,
    });

    await vi.advanceTimersByTimeAsync(5);
    target.data.bufferedAmount = HOST_RECOVERY_DATA_HIGH_WATER_BYTES;
    target.control.message(
      JSON.stringify({
        type: "recovery-source-received",
        recoveryId: live.recoveryId,
        connectionId: live.connectionId,
        streamId: live.streamId,
        deliveryGeneration: live.deliveryGeneration,
        lane: "recovery",
        contiguousDeliveryOrdinal: "1",
        cumulativeEncodedBytes: "89",
      }),
    );
    target.control.message(
      JSON.stringify({
        type: "recovery-source-grant",
        recoveryId: live.recoveryId,
        connectionId: live.connectionId,
        streamId: live.streamId,
        deliveryGeneration: live.deliveryGeneration,
        cumulativeGrantedEncodedBytes: "177",
      }),
    );
    await vi.advanceTimersByTimeAsync(0);
    expect(target.data.sent).toHaveLength(4);

    await vi.advanceTimersByTimeAsync(5);
    expect(target.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(target.recoverySourceManager.counters).toMatchObject({ sources: 2 });

    target.data.bufferedAmount = HOST_RECOVERY_DATA_HIGH_WATER_BYTES - 88;
    await vi.advanceTimersByTimeAsync(5);
    expect(decodeDeliveryEnvelope(target.data.sent[4] as Uint8Array)).toMatchObject({
      streamId: 2,
      deliveryOrdinal: 2n,
      cumulativeEncodedBytes: 177n,
    });
    expect(target.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(target.data.readyState).toBe(FakeWebSocket.OPEN);
    target.relay.close();
    target.session.dispose();
  });

  it("does not expire a terminally closed recovery source or close its healthy pair", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const target = createHarness({
      monotonicNow: () => Date.now(),
      recoveryDeadlineTickMs: 10,
      recoveryLimits: {
        ...RECOVERY_LIMITS,
        noProgressDeadlineMs: 10,
        recoveryDeadlineMs: 20,
      },
    });
    await acknowledgeReady(target);
    const prepare = recoveryPrepare(target.session);
    target.control.message(JSON.stringify(prepare));
    await settleMicrotasks();
    target.control.message(
      JSON.stringify({
        type: "recovery-start-ready",
        recoveryId: prepare.recoveryId,
        connectionId: prepare.connectionId,
        streamId: prepare.streamId,
        deliveryGeneration: prepare.deliveryGeneration,
        committedThrough: prepare.base,
        cumulativeGrantedEncodedBytes: "88",
      }),
    );
    await vi.advanceTimersByTimeAsync(0);
    target.control.message(
      JSON.stringify({
        type: "recovery-source-received",
        recoveryId: prepare.recoveryId,
        connectionId: prepare.connectionId,
        streamId: prepare.streamId,
        deliveryGeneration: prepare.deliveryGeneration,
        lane: "recovery",
        contiguousDeliveryOrdinal: "1",
        cumulativeEncodedBytes: "88",
      }),
    );
    await settleMicrotasks();
    expect(target.control.sent).toHaveLength(2);

    await vi.advanceTimersByTimeAsync(100);
    expect(target.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(target.data.readyState).toBe(FakeWebSocket.OPEN);
    expect(target.recoverySourceManager.counters).toMatchObject({ ownedRecords: 0, sources: 1 });
    target.relay.close();
    target.session.dispose();
  });

  it("returns exact duplicate ACKs and fences an ACK if the pair closes during PTY write", async () => {
    const harness = createHarness();
    await acknowledgeReady(harness);
    const text = {
      type: "text",
      connectionId: "connection_browser_A",
      clientId: "client_AAAAAAAAA",
      writerLease: "writer-lease",
      inputEpoch: "input-epoch",
      clientInputSeq: "1",
      writerFence: "1",
      data: "hello",
    } as const;

    harness.control.message(JSON.stringify(text));
    await settle();
    harness.control.message(
      JSON.stringify({
        type: "focus",
        connectionId: text.connectionId,
        clientId: text.clientId,
        writerLease: text.writerLease,
        inputEpoch: text.inputEpoch,
        clientInputSeq: text.clientInputSeq,
        writerFence: text.writerFence,
        focused: true,
      }),
    );
    await settle();
    const acknowledgements = harness.control.sent
      .slice(1)
      .map((encoded) => decodeControlFrame(encoded as string, HostControlFrameSchema));
    expect(acknowledgements).toEqual([
      {
        type: "input-ack",
        connectionId: "connection_browser_A",
        inputEpoch: "input-epoch",
        clientInputSeq: "1",
        status: "written",
        authorityEventSeq: "0",
      },
      {
        type: "input-ack",
        connectionId: "connection_browser_A",
        inputEpoch: "input-epoch",
        clientInputSeq: "1",
        status: "duplicate",
        authorityEventSeq: "0",
      },
    ]);
    expect(harness.pty.writes).toEqual([new TextEncoder().encode("hello")]);

    const beforeFencedInput = harness.control.sent.length;
    harness.pty.onWrite = () => harness.data.close();
    harness.control.message(JSON.stringify({ ...text, clientInputSeq: "2", data: "fenced" }));
    await settle();
    expect(harness.pty.writes.at(-1)).toEqual(new TextEncoder().encode("fenced"));
    expect(harness.control.sent).toHaveLength(beforeFencedInput);
    harness.session.dispose();
  });

  it("applies Relay writer fences before dispatching semantic input", async () => {
    const harness = createHarness();
    await acknowledgeReady(harness);
    const send = (frame: {
      clientId: string;
      clientInputSeq: string;
      inputEpoch: string;
      writerFence: string;
    }) => {
      harness.control.message(
        JSON.stringify({
          ...frame,
          type: "text",
          connectionId: "connection_browser_A",
          writerLease: "writer-lease",
          data: `${frame.writerFence}:${frame.clientInputSeq}`,
        }),
      );
    };

    send({ clientId: "client-1", clientInputSeq: "1", inputEpoch: "epoch-1", writerFence: "1" });
    send({ clientId: "client-2", clientInputSeq: "2", inputEpoch: "epoch-2", writerFence: "2" });
    send({ clientId: "client-1", clientInputSeq: "2", inputEpoch: "epoch-1", writerFence: "1" });
    send({ clientId: "client-2", clientInputSeq: "1", inputEpoch: "epoch-2", writerFence: "2" });
    send({ clientId: "client-2", clientInputSeq: "2", inputEpoch: "changed", writerFence: "2" });
    await settle();

    expect(
      harness.control.sent
        .slice(1)
        .map((encoded) => decodeControlFrame(encoded as string, HostControlFrameSchema))
        .map((ack) => (ack.type === "input-ack" ? ack.status : ack.type)),
    ).toEqual(["written", "rejected", "rejected", "written", "rejected"]);
    expect(harness.pty.writes).toEqual([
      new TextEncoder().encode("1:1"),
      new TextEncoder().encode("2:1"),
    ]);
    harness.relay.close();
    harness.session.dispose();
  });

  it("forwards focus and mouse through the shared exact ACK and dedup path", async () => {
    let mouseCalls = 0;
    const authority = new (class extends FakeTerminalAuthority {
      override encodeFocus(focused: boolean): Uint8Array {
        return new TextEncoder().encode(focused ? "focus-in" : "focus-out");
      }

      override encodeMouse(mouse: SemanticMouse): Uint8Array {
        mouseCalls += 1;
        return new TextEncoder().encode(
          `mouse:${mouse.action}:${mouse.surface.x}:${mouse.surface.y}`,
        );
      }
    })();
    const harness = createHarness({ authority });
    await acknowledgeReady(harness);
    const common = {
      connectionId: "connection_browser_A",
      clientId: "client_AAAAAAAAA",
      writerLease: "writer-lease",
      inputEpoch: "input-epoch",
      writerFence: "1",
    };

    harness.control.message(
      JSON.stringify({ ...common, type: "focus", clientInputSeq: "1", focused: true }),
    );
    harness.control.message(
      JSON.stringify({
        ...common,
        type: "mouse",
        clientInputSeq: "2",
        action: "press",
        altGraph: false,
        button: 0,
        buttons: 1,
        modifiers: 0,
        surface: { x: 10, y: 20 },
      }),
    );
    harness.control.message(
      JSON.stringify({ ...common, type: "text", clientInputSeq: "2", data: "duplicate" }),
    );
    await settle();

    expect(harness.pty.writes).toEqual([
      new TextEncoder().encode("focus-in"),
      new TextEncoder().encode("mouse:press:10:20"),
    ]);
    expect(mouseCalls).toBe(1);
    expect(
      harness.control.sent
        .slice(1)
        .map((encoded) => decodeControlFrame(encoded as string, HostControlFrameSchema)),
    ).toEqual([
      {
        type: "input-ack",
        connectionId: common.connectionId,
        inputEpoch: common.inputEpoch,
        clientInputSeq: "1",
        status: "written",
        authorityEventSeq: "0",
      },
      {
        type: "input-ack",
        connectionId: common.connectionId,
        inputEpoch: common.inputEpoch,
        clientInputSeq: "2",
        status: "written",
        authorityEventSeq: "0",
      },
      {
        type: "input-ack",
        connectionId: common.connectionId,
        inputEpoch: common.inputEpoch,
        clientInputSeq: "2",
        status: "duplicate",
        authorityEventSeq: "0",
      },
    ]);
    harness.relay.close();
    harness.session.dispose();
  });

  it("bounds queued relay control before async input processing", async () => {
    const harness = createHarness();
    await acknowledgeReady(harness);
    const frame = {
      type: "text",
      connectionId: "connection_browser_A",
      clientId: "client_AAAAAAAAA",
      writerLease: "writer-lease",
      inputEpoch: "input-epoch",
      clientInputSeq: "1",
      writerFence: "1",
      data: "x",
    };

    for (let index = 0; index < HOST_CONTROL_QUEUE_LIMITS.maxCount + 1; index += 1) {
      harness.control.message(JSON.stringify({ ...frame, clientInputSeq: String(index + 1) }));
    }
    await settle();

    expect(harness.control.readyState).toBe(FakeWebSocket.CLOSED);
    expect(harness.data.readyState).toBe(FakeWebSocket.CLOSED);
    harness.session.dispose();
  });

  it("times out a black-holed host-ready handshake", async () => {
    vi.useFakeTimers();
    const harness = createHarness({ readyTimeoutMs: 100 });
    const started = harness.relay.start();
    const rejected = expect(started).rejects.toMatchObject({ name: "TimeoutError" });

    await vi.advanceTimersByTimeAsync(100);
    await rejected;
    expect(harness.control.readyState).toBe(FakeWebSocket.CLOSED);
    expect(harness.data.readyState).toBe(FakeWebSocket.CLOSED);
    harness.session.dispose();
  });

  it.each(["control", "data"] as const)(
    "fences a %s-only heartbeat blackhole without killing the PTY authority epoch",
    async (blackhole) => {
      vi.useFakeTimers();
      const harness = createHarness({ heartbeatIntervalMs: 100, heartbeatTimeoutMs: 300 });
      await acknowledgeReady(harness);

      const live = blackhole === "control" ? harness.data : harness.control;
      for (let elapsed = 100; elapsed < 300; elapsed += 100) {
        await vi.advanceTimersByTimeAsync(100);
        expect(harness.control.sent.at(-1)).toBe("ping");
        expect(harness.data.sent.at(-1)).toBe("ping");
        live.message("pong");
        await Promise.resolve();
      }
      await vi.advanceTimersByTimeAsync(99);
      expect(harness.control.readyState).toBe(FakeWebSocket.OPEN);
      await vi.advanceTimersByTimeAsync(1);

      expect(harness.control.readyState).toBe(FakeWebSocket.CLOSED);
      expect(harness.data.readyState).toBe(FakeWebSocket.CLOSED);
      expect(harness.pty.pid).toBe(42);
      expect(harness.session.sessionEpoch).toBe(1n);
      await expect(
        harness.session.submitText(
          {
            clientId: "client_AAAAAAAAA",
            clientInputSeq: 1n,
            inputEpoch: "input-epoch",
            writerFence: 1n,
          },
          "alive",
        ),
      ).resolves.toMatchObject({ status: "written" });
      expect(harness.pty.writes.at(-1)).toEqual(new TextEncoder().encode("alive"));
      harness.session.dispose();
    },
  );
});
