import {
  DataFrameKind,
  HostControlFrameSchema,
  RecoveryV3HostToCloudControlFrameSchema,
  RelayCapability,
  decodeControlFrame,
  decodeDataFrame,
  decodeDeliveryEnvelopeV3,
  decodeRecoveryStartFence,
  decodeDeliveryBarrierPayload,
  type ConnectionSetResponse,
  type ResizePayload,
} from "@zhongduan/protocol";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { FakeTerminalAuthority } from "../fake-terminal-authority";
import { EventJournal } from "../journal";
import type { PtyProcess } from "../pty-process";
import { TerminalSession, type SnapshotCapture } from "../session";
import type { SemanticMouse } from "../terminal-authority";
import { HostRelayConnection, HOST_CONTROL_QUEUE_LIMITS } from "./host-relay-connection";
import type { HostSocketPair } from "./paired-websocket";
import { RecoverySourceManager, type RecoverySourceManagerLimits } from "./recovery-source-manager";
import { HOST_RECOVERY_DATA_HIGH_WATER_BYTES } from "./recovery-source-scheduler";
import type { PublishedSnapshot } from "./snapshot-publisher";
import {
  SnapshotCheckpointManager,
  type SnapshotPublisherLike,
} from "./snapshot-checkpoint-manager";

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

const HOST_V3_CAPABILITIES = [
  RelayCapability.capabilityNegotiationV1,
  RelayCapability.authorityDataV2,
  RelayCapability.deliveryBarrierOutcomeV1,
  RelayCapability.wireEndpointV3,
  RelayCapability.recoveryV3GapFillV1,
] as const;

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
  snapshotCheckpointManager: SnapshotCheckpointManager;
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
    snapshotCheckpointManager: options.snapshotCheckpointManager,
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
  const snapshotPublisher: SnapshotPublisherLike = {
    publish: async (snapshot) => ({
      metadata: {
        sessionId: "session_AAAAAAAAA",
        snapshotId: "snapshot_AAAAAAAA",
        engineId: snapshot.engineId,
        sessionEpoch: snapshot.sessionEpoch.toString(),
        cutEventSeq: snapshot.cutEventSeq.toString(),
        nextPtyOffset: snapshot.nextPtyOffset.toString(),
        compression: "zstd",
        compressedLength: "1",
        uncompressedLength: snapshot.bytes.byteLength.toString(),
        sha256: "0".repeat(64),
      },
    }),
  };
  const snapshotCheckpointManager = new SnapshotCheckpointManager({
    publisher: snapshotPublisher,
    session,
    sessionId: "session_AAAAAAAAA",
  });
  const relayHarness = createRelayHarness({
    ...options,
    session,
    snapshotCheckpointManager,
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

function sentDataFrames(socket: FakeWebSocket) {
  return socket.sent.flatMap((encoded) =>
    encoded instanceof Uint8Array ? [decodeDataFrame(encoded)] : [],
  );
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

  it("keeps Recovery v3 unreachable unless the complete Host capability set was confirmed", async () => {
    const harness = createHarness({
      connection: {
        ...connectionSet,
        negotiatedCapabilities: HOST_V3_CAPABILITIES.filter(
          (capability) => capability !== RelayCapability.recoveryV3GapFillV1,
        ),
      },
    });
    await acknowledgeReady(harness);

    harness.control.message(JSON.stringify(recoveryPrepare(harness.session)));
    await settle();

    expect(harness.control.readyState).toBe(FakeWebSocket.CLOSED);
    expect(harness.data.readyState).toBe(FakeWebSocket.CLOSED);
    expect(harness.recoverySourceManager.counters).toMatchObject({ sources: 0 });
    harness.session.dispose();
  });

  it("strictly handles a confirmed Recovery v3 source through ordered fence, bounded Done, and exact close", async () => {
    const v3 = createHarness({
      connection: {
        ...connectionSet,
        negotiatedCapabilities: [...HOST_V3_CAPABILITIES],
      },
    });
    await acknowledgeReady(v3);
    const prepare = recoveryPrepare(v3.session);
    const beforePrepareControl = v3.control.sent.length;

    v3.control.message(JSON.stringify(prepare));
    await vi.waitFor(() => expect(v3.data.sent).toHaveLength(1));
    expect(v3.control.sent).toHaveLength(beforePrepareControl);
    expect(decodeRecoveryStartFence(v3.data.sent[0] as Uint8Array)).toMatchObject({
      recoveryId: prepare.recoveryId,
      connectionId: prepare.connectionId,
      streamId: prepare.streamId,
      deliveryGeneration: prepare.deliveryGeneration,
      base: prepare.base,
      committedThrough: prepare.base,
    });

    v3.control.message(
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
    await vi.waitFor(() => expect(v3.data.sent).toHaveLength(2));
    const done = decodeDeliveryEnvelopeV3(v3.data.sent[1] as Uint8Array);
    expect(done).toMatchObject({
      lane: "recovery",
      deliveryGeneration: 1n,
      deliveryOrdinal: 1n,
      cumulativeEncodedBytes: 88n,
      streamId: 1,
    });
    expect(decodeDataFrame(done.payload)).toMatchObject({ kind: DataFrameKind.ReplayCommit });

    v3.control.message(
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
    await vi.waitFor(() => expect(v3.control.sent).toHaveLength(beforePrepareControl + 1));
    expect(
      decodeControlFrame(v3.control.sent.at(-1) as string, RecoveryV3HostToCloudControlFrameSchema),
    ).toEqual({
      type: "recovery-source-closed",
      recoveryId: prepare.recoveryId,
      connectionId: prepare.connectionId,
      streamId: prepare.streamId,
      deliveryGeneration: prepare.deliveryGeneration,
      throughRecoveryOrdinal: "1",
      throughRecoveryCumulativeEncodedBytes: "88",
    });
    expect(v3.control.readyState).toBe(FakeWebSocket.OPEN);
    v3.relay.close();
    v3.session.dispose();
  });

  it("sends multiple granted records across fair data turns before an intermediate receipt", async () => {
    const v3 = createHarness({
      connection: {
        ...connectionSet,
        negotiatedCapabilities: [...HOST_V3_CAPABILITIES],
      },
    });
    await acknowledgeReady(v3);
    v3.pty.emit(Uint8Array.of(0x41));
    await vi.waitFor(() => expect(v3.data.sent).toHaveLength(1));
    const prepare = recoveryPrepare(v3.session);
    v3.control.message(JSON.stringify(prepare));
    await vi.waitFor(() => expect(v3.data.sent).toHaveLength(2));
    const fence = decodeRecoveryStartFence(v3.data.sent[1] as Uint8Array);

    v3.control.message(
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
    await vi.waitFor(() => expect(v3.data.sent).toHaveLength(4));
    const first = decodeDeliveryEnvelopeV3(v3.data.sent[2] as Uint8Array);
    expect(first).toMatchObject({ deliveryOrdinal: 1n, cumulativeEncodedBytes: 89n });
    const done = decodeDeliveryEnvelopeV3(v3.data.sent[3] as Uint8Array);
    expect(done).toMatchObject({ deliveryOrdinal: 2n, cumulativeEncodedBytes: 177n });
    expect(decodeDataFrame(done.payload)).toMatchObject({ kind: DataFrameKind.ReplayCommit });

    v3.control.message(
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
    expect(v3.data.sent).toHaveLength(4);
    expect(v3.control.readyState).toBe(FakeWebSocket.OPEN);

    v3.control.message(
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
    await vi.waitFor(() => expect(v3.control.sent).toHaveLength(2));
    expect(
      decodeControlFrame(v3.control.sent[1] as string, RecoveryV3HostToCloudControlFrameSchema),
    ).toMatchObject({ type: "recovery-source-closed", throughRecoveryOrdinal: "2" });
    v3.relay.close();
    v3.session.dispose();
  });

  it("returns exact prepare rejection but fails the pair for an invalid v3 source identity", async () => {
    const v3 = createHarness({
      connection: {
        ...connectionSet,
        negotiatedCapabilities: [...HOST_V3_CAPABILITIES],
      },
    });
    await acknowledgeReady(v3);
    const prepare = recoveryPrepare(v3.session);

    v3.control.message(JSON.stringify({ ...prepare, engineId: "wrong-engine/v1" }));
    await vi.waitFor(() => expect(v3.control.sent).toHaveLength(2));
    expect(
      decodeControlFrame(v3.control.sent[1] as string, RecoveryV3HostToCloudControlFrameSchema),
    ).toMatchObject({ type: "recovery-prepare-rejected", reason: "engine-mismatch" });
    expect(v3.control.readyState).toBe(FakeWebSocket.OPEN);

    v3.control.message(
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
    expect(v3.control.readyState).toBe(FakeWebSocket.CLOSED);
    expect(v3.data.readyState).toBe(FakeWebSocket.CLOSED);
    v3.session.dispose();
  });

  it("accepts an exact source reset before prepare without closing the Host pair", async () => {
    const v3 = createHarness({
      connection: {
        ...connectionSet,
        negotiatedCapabilities: [...HOST_V3_CAPABILITIES],
      },
    });
    await acknowledgeReady(v3);
    const prepare = recoveryPrepare(v3.session);
    const beforeResetControl = v3.control.sent.length;

    v3.control.message(
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

    expect(v3.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(v3.data.readyState).toBe(FakeWebSocket.OPEN);
    expect(v3.recoverySourceManager.counters).toMatchObject({
      ownedRecords: 0,
      sources: 1,
    });

    v3.control.message(JSON.stringify(prepare));
    await vi.waitFor(() => expect(v3.control.sent).toHaveLength(beforeResetControl + 1));
    expect(
      decodeControlFrame(v3.control.sent.at(-1) as string, RecoveryV3HostToCloudControlFrameSchema),
    ).toMatchObject({
      type: "recovery-prepare-rejected",
      recoveryId: prepare.recoveryId,
      reason: "generation-fenced",
    });
    expect(v3.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(v3.data.readyState).toBe(FakeWebSocket.OPEN);
    v3.relay.close();
    v3.session.dispose();
  });

  it("fences a replaced generation across late control and an already-scheduled data turn", async () => {
    vi.useFakeTimers();
    const v3 = createHarness({
      connection: {
        ...connectionSet,
        negotiatedCapabilities: [...HOST_V3_CAPABILITIES],
      },
    });
    await acknowledgeReady(v3);
    const generationOne = recoveryPrepare(v3.session, "1");
    const generationTwo = recoveryPrepare(v3.session, "2");

    v3.control.message(JSON.stringify(generationOne));
    await settleMicrotasks();
    expect(v3.data.sent).toHaveLength(1);
    v3.control.message(
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
    expect(v3.data.sent).toHaveLength(1);

    v3.control.message(JSON.stringify(generationTwo));
    await settleMicrotasks();
    await settleMicrotasks();
    expect(v3.data.sent).toHaveLength(2);
    expect(decodeRecoveryStartFence(v3.data.sent[1] as Uint8Array)).toMatchObject({
      deliveryGeneration: "2",
    });

    v3.control.message(
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
    v3.control.message(
      JSON.stringify({
        type: "recovery-source-grant",
        recoveryId: generationOne.recoveryId,
        connectionId: generationOne.connectionId,
        streamId: generationOne.streamId,
        deliveryGeneration: generationOne.deliveryGeneration,
        cumulativeGrantedEncodedBytes: "88",
      }),
    );
    v3.control.message(
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
    v3.control.message(
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
    expect(v3.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(v3.data.readyState).toBe(FakeWebSocket.OPEN);

    await vi.advanceTimersByTimeAsync(0);
    expect(v3.data.sent).toHaveLength(3);
    expect(decodeDeliveryEnvelopeV3(v3.data.sent[2] as Uint8Array)).toMatchObject({
      deliveryGeneration: 2n,
      deliveryOrdinal: 1n,
    });
    expect(v3.control.readyState).toBe(FakeWebSocket.OPEN);

    v3.control.message(
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
    expect(v3.control.readyState).toBe(FakeWebSocket.CLOSED);
    expect(v3.data.readyState).toBe(FakeWebSocket.CLOSED);
    v3.session.dispose();
  });

  it("fences the v3 owner on a recovery data send throw before later grants can reuse the pair", async () => {
    const v3 = createHarness({
      connection: {
        ...connectionSet,
        negotiatedCapabilities: [...HOST_V3_CAPABILITIES],
      },
    });
    await acknowledgeReady(v3);
    const prepare = recoveryPrepare(v3.session);
    v3.control.message(JSON.stringify(prepare));
    await vi.waitFor(() => expect(v3.data.sent).toHaveLength(1));
    v3.data.throwOnBinarySend = true;

    v3.control.message(
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
    await vi.waitFor(() => expect(v3.control.readyState).toBe(FakeWebSocket.CLOSED));
    expect(v3.control.closeReason).toContain("injected binary send failure");
    expect(v3.recoverySourceManager.counters).toEqual({
      ownedRecords: 0,
      ownedWireBytes: 0,
      pendingSources: 0,
      sources: 0,
    });
    const sentBeforeLateGrant = v3.data.sent.length;
    v3.control.message(
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
    expect(v3.data.sent).toHaveLength(sentBeforeLateGrant);
    v3.session.dispose();
  });

  it("yields recovery at shared data backpressure while canonical traffic stays live", async () => {
    vi.useFakeTimers();
    const open = async () => {
      const harness = createHarness({
        connection: {
          ...connectionSet,
          negotiatedCapabilities: [...HOST_V3_CAPABILITIES],
        },
      });
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
    expect(decodeDeliveryEnvelopeV3(blocked.harness.data.sent[2] as Uint8Array)).toMatchObject({
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
    const v3 = createHarness({
      connection: {
        ...connectionSet,
        negotiatedCapabilities: [...HOST_V3_CAPABILITIES],
      },
      monotonicNow: () => Date.now(),
      recoveryDeadlineTickMs: 10,
      recoveryLimits: {
        ...RECOVERY_LIMITS,
        noProgressDeadlineMs: 10,
        recoveryDeadlineMs: 100,
      },
    });
    await acknowledgeReady(v3);
    const prepare = recoveryPrepare(v3.session);
    v3.control.message(JSON.stringify(prepare));
    await settleMicrotasks();
    expect(v3.data.sent).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(10);
    expect(v3.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(v3.data.readyState).toBe(FakeWebSocket.OPEN);
    expect(v3.recoverySourceManager.counters).toMatchObject({ ownedRecords: 0, sources: 1 });

    v3.control.message(
      JSON.stringify({
        type: "recovery-source-grant",
        recoveryId: prepare.recoveryId,
        connectionId: prepare.connectionId,
        streamId: prepare.streamId,
        deliveryGeneration: prepare.deliveryGeneration,
        cumulativeGrantedEncodedBytes: "88",
      }),
    );
    v3.control.message(
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
    expect(v3.control.closeReason).toBeUndefined();
    expect(v3.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(v3.data.readyState).toBe(FakeWebSocket.OPEN);
    v3.session.dispose();
  });

  it("keeps the pair live when a pending prepare crosses its exact deadline", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const v3 = createHarness({
      connection: {
        ...connectionSet,
        negotiatedCapabilities: [...HOST_V3_CAPABILITIES],
      },
      monotonicNow: () => Date.now(),
      recoveryDeadlineTickMs: 10,
      recoveryLimits: {
        ...RECOVERY_LIMITS,
        noProgressDeadlineMs: 100,
        recoveryDeadlineMs: 10,
      },
    });
    const pending = deferred<Awaited<ReturnType<TerminalSession["prepareRecoveryGap"]>>>();
    vi.spyOn(v3.session, "prepareRecoveryGap").mockReturnValue(pending.promise);
    await acknowledgeReady(v3);
    const prepare = recoveryPrepare(v3.session);

    v3.control.message(JSON.stringify(prepare));
    await settleMicrotasks();
    expect(v3.recoverySourceManager.counters).toMatchObject({ pendingSources: 1 });

    await vi.advanceTimersByTimeAsync(10);
    expect(v3.recoverySourceManager.counters).toMatchObject({ pendingSources: 0, sources: 1 });
    expect(v3.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(v3.data.readyState).toBe(FakeWebSocket.OPEN);

    pending.resolve({ status: "unavailable", reason: "fence-unavailable" });
    await settleMicrotasks();
    expect(v3.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(v3.data.readyState).toBe(FakeWebSocket.OPEN);
    v3.relay.close();
    v3.session.dispose();
  });

  it("lets a live source continue after another source on the same pair expires", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const v3 = createHarness({
      connection: {
        ...connectionSet,
        negotiatedCapabilities: [...HOST_V3_CAPABILITIES],
      },
      monotonicNow: () => Date.now(),
      recoveryDeadlineTickMs: 10,
      recoveryLimits: {
        ...RECOVERY_LIMITS,
        noProgressDeadlineMs: 10,
        recoveryDeadlineMs: 100,
      },
    });
    await acknowledgeReady(v3);
    v3.pty.emit(Uint8Array.of(0x41));
    await settleMicrotasks();
    const expired = recoveryPrepare(v3.session);
    const live = {
      ...expired,
      recoveryId: "recovery-host-00000002",
      connectionId: "connection-browser-0002",
      streamId: 2,
    };

    v3.control.message(JSON.stringify(expired));
    v3.control.message(JSON.stringify(live));
    await settleMicrotasks();
    await vi.advanceTimersByTimeAsync(0);
    expect(v3.data.sent).toHaveLength(3);
    const liveFence = decodeRecoveryStartFence(v3.data.sent[2] as Uint8Array);

    v3.control.message(
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
    expect(decodeDeliveryEnvelopeV3(v3.data.sent[3] as Uint8Array)).toMatchObject({
      streamId: 2,
      deliveryOrdinal: 1n,
    });

    await vi.advanceTimersByTimeAsync(5);
    v3.data.bufferedAmount = HOST_RECOVERY_DATA_HIGH_WATER_BYTES;
    v3.control.message(
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
    v3.control.message(
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
    expect(v3.data.sent).toHaveLength(4);

    await vi.advanceTimersByTimeAsync(5);
    expect(v3.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(v3.recoverySourceManager.counters).toMatchObject({ sources: 2 });

    v3.data.bufferedAmount = HOST_RECOVERY_DATA_HIGH_WATER_BYTES - 88;
    await vi.advanceTimersByTimeAsync(5);
    expect(decodeDeliveryEnvelopeV3(v3.data.sent[4] as Uint8Array)).toMatchObject({
      streamId: 2,
      deliveryOrdinal: 2n,
      cumulativeEncodedBytes: 177n,
    });
    expect(v3.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(v3.data.readyState).toBe(FakeWebSocket.OPEN);
    v3.relay.close();
    v3.session.dispose();
  });

  it("does not expire a terminally closed recovery source or close its healthy pair", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const v3 = createHarness({
      connection: {
        ...connectionSet,
        negotiatedCapabilities: [...HOST_V3_CAPABILITIES],
      },
      monotonicNow: () => Date.now(),
      recoveryDeadlineTickMs: 10,
      recoveryLimits: {
        ...RECOVERY_LIMITS,
        noProgressDeadlineMs: 10,
        recoveryDeadlineMs: 20,
      },
    });
    await acknowledgeReady(v3);
    const prepare = recoveryPrepare(v3.session);
    v3.control.message(JSON.stringify(prepare));
    await settleMicrotasks();
    v3.control.message(
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
    v3.control.message(
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
    expect(v3.control.sent).toHaveLength(2);

    await vi.advanceTimersByTimeAsync(100);
    expect(v3.control.readyState).toBe(FakeWebSocket.OPEN);
    expect(v3.data.readyState).toBe(FakeWebSocket.OPEN);
    expect(v3.recoverySourceManager.counters).toMatchObject({ ownedRecords: 0, sources: 1 });
    v3.relay.close();
    v3.session.dispose();
  });

  it("hands an unresolved session refresh to a replacement connection without duplicating work", async () => {
    const authority = new FakeTerminalAuthority();
    const encodeSnapshot = vi.spyOn(authority, "encodeSnapshot");
    const pty = new ManualPty();
    const session = new TerminalSession({
      authority,
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });
    const publishStarted = deferred<SnapshotCapture>();
    const upload = deferred<PublishedSnapshot>();
    let commonPublisherSignal: AbortSignal | undefined;
    const publish = vi.fn((snapshot: SnapshotCapture, signal?: AbortSignal) => {
      commonPublisherSignal = signal;
      publishStarted.resolve(snapshot);
      return upload.promise;
    });
    const snapshotCheckpointManager = new SnapshotCheckpointManager({
      publisher: { publish },
      session,
      sessionId: "session_AAAAAAAAA",
    });
    const refresh = vi.spyOn(snapshotCheckpointManager, "refresh");
    const original = createRelayHarness({ session, snapshotCheckpointManager });
    await acknowledgeReady(original);

    original.control.message(
      JSON.stringify({
        type: "attach-request",
        connectionId: "connection_browser_A",
        streamId: 1,
        deliveryGeneration: "1",
        engineId: session.engineId,
        hasLiveReplica: false,
      }),
    );
    const snapshot = await publishStarted.promise;
    expect(refresh).toHaveBeenCalledOnce();
    const originalWaiterSignal = refresh.mock.calls[0]?.[0].signal;
    expect(originalWaiterSignal?.aborted).toBe(false);
    expect(commonPublisherSignal?.aborted).toBe(false);

    original.relay.close();
    await original.relay.waitClosed();
    expect(originalWaiterSignal?.aborted).toBe(true);
    expect(commonPublisherSignal?.aborted).toBe(false);

    const replacement = createRelayHarness({
      connection: {
        ...connectionSet,
        connectionSetId: "connection_set_BB",
        connectionId: "connection_BBBBB",
        controlTicket: "control_ticket_B",
        dataTicket: "data_ticket_BBBB",
      },
      session,
      snapshotCheckpointManager,
    });
    await acknowledgeReady(replacement);
    replacement.control.message(
      JSON.stringify({
        type: "attach-request",
        connectionId: "connection_browser_B",
        streamId: 2,
        deliveryGeneration: "1",
        engineId: session.engineId,
        hasLiveReplica: false,
      }),
    );
    await vi.waitFor(() => expect(refresh).toHaveBeenCalledTimes(2));
    expect(refresh.mock.calls[1]?.[0].signal?.aborted).toBe(false);
    expect(encodeSnapshot).toHaveBeenCalledOnce();
    expect(publish).toHaveBeenCalledOnce();
    expect(commonPublisherSignal?.aborted).toBe(false);

    upload.resolve({
      metadata: {
        sessionId: "session_AAAAAAAAA",
        snapshotId: "snapshot_RECONNECT_A",
        engineId: snapshot.engineId,
        sessionEpoch: snapshot.sessionEpoch.toString(),
        cutEventSeq: snapshot.cutEventSeq.toString(),
        nextPtyOffset: snapshot.nextPtyOffset.toString(),
        compression: "zstd",
        compressedLength: "1",
        uncompressedLength: snapshot.bytes.byteLength.toString(),
        sha256: "0".repeat(64),
      },
    });
    await vi.waitFor(() =>
      expect(
        sentDataFrames(replacement.data).filter(
          (frame) => frame.kind === DataFrameKind.DeliveryBarrier,
        ),
      ).toHaveLength(1),
    );

    const replacementFrames = sentDataFrames(replacement.data);
    const barrier = replacementFrames.find((frame) => frame.kind === DataFrameKind.DeliveryBarrier);
    if (barrier === undefined) throw new Error("expected replacement delivery barrier");
    const barrierPayload = decodeDeliveryBarrierPayload(barrier.payload);
    if (barrierPayload.mode !== "snapshot") throw new Error("expected snapshot barrier");
    replacement.control.message(
      JSON.stringify({
        type: "delivery-barrier-result",
        status: "ready",
        mode: "snapshot",
        connectionId: barrierPayload.connectionId,
        snapshotId: barrierPayload.snapshotId,
        streamId: barrier.streamId,
        deliveryGeneration: barrier.deliveryGeneration.toString(),
        commitEventSeq: barrier.eventSeq.toString(),
        commitPtyOffset: barrier.ptyOffset.toString(),
      }),
    );
    await vi.waitFor(() =>
      expect(
        sentDataFrames(replacement.data).filter(
          (frame) => frame.kind === DataFrameKind.ReplayCommit,
        ),
      ).toHaveLength(1),
    );

    expect(
      sentDataFrames(original.data).filter(
        (frame) =>
          frame.kind === DataFrameKind.DeliveryBarrier || frame.kind === DataFrameKind.ReplayCommit,
      ),
    ).toEqual([]);
    expect(sentDataFrames(replacement.data).map((frame) => frame.kind)).toEqual([
      DataFrameKind.DeliveryBarrier,
      DataFrameKind.ReplayCommit,
    ]);
    expect(encodeSnapshot).toHaveBeenCalledOnce();
    expect(publish).toHaveBeenCalledOnce();

    replacement.relay.close();
    snapshotCheckpointManager.dispose();
    session.dispose();
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
