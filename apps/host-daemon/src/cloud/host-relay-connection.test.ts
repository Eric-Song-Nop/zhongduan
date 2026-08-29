import {
  DataFrameKind,
  HostControlFrameSchema,
  decodeControlFrame,
  decodeDataFrame,
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

  close(): void {
    if (this.readyState >= FakeWebSocket.CLOSING) return;
    this.readyState = FakeWebSocket.CLOSED;
    this.dispatchEvent(new Event("close"));
  }

  send(data: string | ArrayBufferLike | Blob | ArrayBufferView): void {
    this.sent.push(data);
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

function createRelayHarness(options: {
  connection?: ConnectionSetResponse;
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  readyTimeoutMs?: number;
  session: TerminalSession;
  snapshotCheckpointManager: SnapshotCheckpointManager;
}) {
  const control = new FakeWebSocket();
  const data = new FakeWebSocket();
  const pair: HostSocketPair = {
    connection: options.connection ?? connectionSet,
    control: control as unknown as WebSocket,
    data: data as unknown as WebSocket,
    close() {
      data.close();
      control.close();
    },
  };
  const relay = new HostRelayConnection({
    pair,
    ...(options.heartbeatIntervalMs === undefined
      ? {}
      : { heartbeatIntervalMs: options.heartbeatIntervalMs }),
    ...(options.heartbeatTimeoutMs === undefined
      ? {}
      : { heartbeatTimeoutMs: options.heartbeatTimeoutMs }),
    ...(options.readyTimeoutMs === undefined ? {} : { readyTimeoutMs: options.readyTimeoutMs }),
    session: options.session,
    snapshotCheckpointManager: options.snapshotCheckpointManager,
  });
  return { control, data, pair, relay, session: options.session };
}

function createHarness(
  options: {
    authority?: FakeTerminalAuthority;
    heartbeatIntervalMs?: number;
    heartbeatTimeoutMs?: number;
    readyTimeoutMs?: number;
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
