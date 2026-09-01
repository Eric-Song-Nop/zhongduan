import {
  DataFrameKind,
  HostControlFrameSchema,
  RelayCapability,
  decodeControlFrame,
  decodeDataFrame,
  decodeDataFrameBatch,
  type ConnectionSetResponse,
  type ResizePayload,
} from "@zhongduan/protocol";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { FakeTerminalAuthority } from "../fake-terminal-authority";
import { EventJournal } from "../journal";
import type { PtyProcess } from "../pty-process";
import { TerminalSession } from "../session";
import type { SemanticMouse } from "../terminal-authority";
import type { SnapshotPublisherLike } from "./delivery-scheduler";
import { HostRelayConnection, HOST_CONTROL_QUEUE_LIMITS } from "./host-relay-connection";
import type { HostSocketPair } from "./paired-websocket";

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

function createHarness(
  options: {
    authority?: FakeTerminalAuthority;
    heartbeatIntervalMs?: number;
    heartbeatTimeoutMs?: number;
    hostDataBatch?: boolean;
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
  const control = new FakeWebSocket();
  const data = new FakeWebSocket();
  const pair: HostSocketPair = {
    connection:
      options.hostDataBatch === true
        ? { ...connectionSet, selectedCapabilities: [RelayCapability.hostDataBatchV1] }
        : connectionSet,
    control: control as unknown as WebSocket,
    data: data as unknown as WebSocket,
    close() {
      data.close();
      control.close();
    },
  };
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
  const relay = new HostRelayConnection({
    pair,
    ...(options.heartbeatIntervalMs === undefined
      ? {}
      : { heartbeatIntervalMs: options.heartbeatIntervalMs }),
    ...(options.heartbeatTimeoutMs === undefined
      ? {}
      : { heartbeatTimeoutMs: options.heartbeatTimeoutMs }),
    ...(options.readyTimeoutMs === undefined ? {} : { readyTimeoutMs: options.readyTimeoutMs }),
    session,
    snapshotPublisher,
  });
  return { control, data, pair, pty, relay, session };
}

async function acknowledgeReady(harness: ReturnType<typeof createHarness>): Promise<void> {
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

beforeEach(() => vi.stubGlobal("WebSocket", FakeWebSocket));
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("HostRelayConnection", () => {
  it("holds the next negotiated batch for Cloud credit without changing logical order", async () => {
    const harness = createHarness({ hostDataBatch: true });
    const started = harness.relay.start();
    for (let index = 0; index < 128; index += 1) {
      harness.pty.emit(Uint8Array.of(index));
    }
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
    await settle();

    expect(harness.data.sent).toHaveLength(1);
    harness.data.message("data-ack");
    await settle();
    expect(harness.data.sent).toHaveLength(2);
    const frames = harness.data.sent.flatMap((batch) => decodeDataFrameBatch(batch as Uint8Array));
    expect(frames).toHaveLength(128);
    expect(frames.map((frame) => frame.eventSeq)).toEqual(
      Array.from({ length: 128 }, (_, index) => BigInt(index + 1)),
    );
    harness.relay.close();
    harness.session.dispose();
  });

  it("keeps a latency-sensitive data frame out of the surrounding bulk batches", async () => {
    const harness = createHarness({ hostDataBatch: true });
    const started = harness.relay.start();
    harness.pty.emit(new Uint8Array(1_024));
    harness.pty.emit(new Uint8Array(60));
    harness.pty.emit(new Uint8Array(1_024));
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
    await settle();

    expect(decodeDataFrameBatch(harness.data.sent[0] as Uint8Array)).toHaveLength(1);
    harness.data.message("data-ack");
    expect(decodeDataFrameBatch(harness.data.sent[1] as Uint8Array)[0]?.payload).toHaveLength(60);
    harness.data.message("data-ack");
    expect(decodeDataFrameBatch(harness.data.sent[2] as Uint8Array)).toHaveLength(1);
    harness.relay.close();
    harness.session.dispose();
  });

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
