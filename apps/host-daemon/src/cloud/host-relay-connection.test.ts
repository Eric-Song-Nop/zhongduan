import {
  DataFrameKind,
  HostControlFrameSchema,
  decodeControlFrame,
  decodeDataFrame,
  decodeDeliveryBarrierPayload,
  type ConnectionSetResponse,
  type ResizePayload,
} from "@zhongduan/protocol";
import {
  createBufferedTelemetrySink,
  type TelemetrySink,
  type TerminalTelemetryEvent,
} from "@zhongduan/telemetry";
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
    monotonicNow?: () => number;
    readyTimeoutMs?: number;
    telemetry?: TelemetrySink;
  } = {},
) {
  const telemetryBuffer =
    options.telemetry === undefined ? undefined : createBufferedTelemetrySink(options.telemetry);
  const pty = new ManualPty();
  const session = new TerminalSession({
    authority: options.authority ?? new FakeTerminalAuthority(),
    journal: new EventJournal(),
    ...(options.monotonicNow === undefined ? {} : { monotonicNow: options.monotonicNow }),
    pty,
    sessionEpoch: 1n,
  });
  const control = new FakeWebSocket();
  const data = new FakeWebSocket();
  const pair: HostSocketPair = {
    connection: connectionSet,
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
    ...(options.monotonicNow === undefined ? {} : { monotonicNow: options.monotonicNow }),
    ...(options.readyTimeoutMs === undefined ? {} : { readyTimeoutMs: options.readyTimeoutMs }),
    session,
    snapshotPublisher,
    ...(telemetryBuffer === undefined ? {} : { telemetryBuffer }),
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

  it("records bounded Host control-queue and input-apply latency facts", async () => {
    const events: TerminalTelemetryEvent[] = [];
    const harness = createHarness({ telemetry: (event) => events.push(event) });
    await acknowledgeReady(harness);
    await settle();
    events.length = 0;

    harness.control.message(
      JSON.stringify({
        type: "text",
        connectionId: "connection_browser_A",
        clientId: "client_AAAAAAAAA",
        writerLease: "writer-lease",
        inputEpoch: "input-epoch",
        clientInputSeq: "1",
        writerFence: "1",
        data: "must-not-enter-telemetry",
      }),
    );
    harness.control.message(
      JSON.stringify({
        type: "focus",
        connectionId: "connection_browser_A",
        clientId: "client_AAAAAAAAA",
        writerLease: "writer-lease",
        inputEpoch: "input-epoch",
        clientInputSeq: "2",
        writerFence: "1",
        focused: true,
      }),
    );
    harness.control.message(
      JSON.stringify({
        type: "resize-request",
        connectionId: "connection_browser_A",
        clientId: "client_AAAAAAAAA",
        writerLease: "writer-lease",
        inputEpoch: "input-epoch",
        clientInputSeq: "3",
        writerFence: "1",
        cols: 100,
        rows: 30,
        widthPx: 800,
        heightPx: 600,
      }),
    );
    await settle();

    expect(events).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "host.input.apply",
          inputKind: "text",
          outcome: "written",
          effectStage: "completed",
          encodeKind: "utf8",
          ackSendOutcome: "send-returned",
          ptyBytesBucket: "9-64",
        }),
        expect.objectContaining({
          name: "host.input.apply",
          inputKind: "focus",
          outcome: "written",
          effectStage: "completed",
          ptyWriteAttempted: false,
          ptyBytesBucket: "0",
        }),
        expect.objectContaining({
          name: "host.input.apply",
          inputKind: "resize",
          outcome: "written",
          effectStage: "completed",
          ptyResizeAttempted: true,
          effectWriteAttempted: false,
          effectBytesBucket: "0",
        }),
        expect.objectContaining({
          name: "host.control.queue",
          messageClass: "input",
          outcome: "handled",
          queuedCount: 1,
        }),
      ]),
    );
    expect(JSON.stringify(events)).not.toContain("must-not-enter-telemetry");
    harness.relay.close();
    harness.session.dispose();
  });

  it("records a thrown input effect as uncertain without serializing the error", async () => {
    const events: TerminalTelemetryEvent[] = [];
    const authority = new (class extends FakeTerminalAuthority {
      override encodeFocus(): Uint8Array {
        throw new Error("secret encoder state");
      }
    })();
    const harness = createHarness({ authority, telemetry: (event) => events.push(event) });
    await acknowledgeReady(harness);
    events.length = 0;

    harness.control.message(
      JSON.stringify({
        type: "focus",
        connectionId: "connection_browser_A",
        clientId: "client_AAAAAAAAA",
        writerLease: "writer-lease",
        inputEpoch: "input-epoch",
        clientInputSeq: "1",
        writerFence: "1",
        focused: true,
      }),
    );
    await settle();

    expect(events).toContainEqual(
      expect.objectContaining({
        name: "host.input.apply",
        inputKind: "focus",
        outcome: "uncertain",
        effectStage: "threw",
        ptyWriteAttempted: false,
      }),
    );
    expect(JSON.stringify(events)).not.toContain("secret encoder state");
    harness.relay.close();
    harness.session.dispose();
  });

  it("records independent control and data heartbeat RTT on the Host clock", async () => {
    vi.useFakeTimers();
    let now = 0;
    const events: TerminalTelemetryEvent[] = [];
    const harness = createHarness({
      heartbeatIntervalMs: 100,
      heartbeatTimeoutMs: 300,
      monotonicNow: () => now,
      telemetry: (event) => events.push(event),
    });
    await acknowledgeReady(harness);
    await Promise.resolve();
    await Promise.resolve();
    events.length = 0;

    now = 100;
    await vi.advanceTimersByTimeAsync(100);
    now = 125;
    harness.control.message("pong");
    harness.data.message("pong");
    for (let index = 0; index < 6; index += 1) await Promise.resolve();

    expect(events.filter((event) => event.name === "host.relay.rtt")).toEqual([
      {
        schemaVersion: 1,
        monotonicAtMs: 125,
        name: "host.relay.rtt",
        channel: "control",
        outcome: "ok",
        durationMs: 25,
        outstandingPings: 0,
      },
      {
        schemaVersion: 1,
        monotonicAtMs: 125,
        name: "host.relay.rtt",
        channel: "data",
        outcome: "ok",
        durationMs: 25,
        outstandingPings: 0,
      },
    ]);
    harness.relay.close();
    harness.session.dispose();
  });

  it("matches overlapping heartbeat probes in FIFO order without a wire nonce", async () => {
    vi.useFakeTimers();
    let now = 0;
    const events: TerminalTelemetryEvent[] = [];
    const harness = createHarness({
      heartbeatIntervalMs: 100,
      heartbeatTimeoutMs: 300,
      monotonicNow: () => now,
      telemetry: (event) => events.push(event),
    });
    await acknowledgeReady(harness);
    await Promise.resolve();
    events.length = 0;

    now = 100;
    await vi.advanceTimersByTimeAsync(100);
    now = 200;
    await vi.advanceTimersByTimeAsync(100);
    now = 225;
    harness.control.message("pong");
    harness.data.message("pong");
    now = 250;
    harness.control.message("pong");
    harness.data.message("pong");
    for (let index = 0; index < 6; index += 1) await Promise.resolve();

    expect(
      events
        .filter((event) => event.name === "host.relay.rtt" && event.outcome === "ok")
        .map((event) => ({
          channel: event.channel,
          durationMs: event.durationMs,
          outstandingPings: event.outstandingPings,
        })),
    ).toEqual([
      { channel: "control", durationMs: 125, outstandingPings: 1 },
      { channel: "data", durationMs: 125, outstandingPings: 1 },
      { channel: "control", durationMs: 50, outstandingPings: 0 },
      { channel: "data", durationMs: 50, outstandingPings: 0 },
    ]);
    harness.relay.close();
    harness.session.dispose();
  });

  it("keeps ACK, PTY effects, and relay frames identical with telemetry enabled", async () => {
    const baseline = createHarness();
    const observed = createHarness({ telemetry: () => undefined });
    await acknowledgeReady(baseline);
    await acknowledgeReady(observed);
    const frame = JSON.stringify({
      type: "text",
      connectionId: "connection_browser_A",
      clientId: "client_AAAAAAAAA",
      writerLease: "writer-lease",
      inputEpoch: "input-epoch",
      clientInputSeq: "1",
      writerFence: "1",
      data: "same-input",
    });

    baseline.control.message(frame);
    observed.control.message(frame);
    await settle();

    expect(observed.control.sent).toEqual(baseline.control.sent);
    expect(observed.data.sent).toEqual(baseline.data.sent);
    expect(observed.pty.writes).toEqual(baseline.pty.writes);
    expect(observed.session.cursor).toEqual(baseline.session.cursor);
    baseline.relay.close();
    observed.relay.close();
    baseline.session.dispose();
    observed.session.dispose();
  });

  it("defers input diagnostics while the shared recovery buffer is paused", async () => {
    const events: TerminalTelemetryEvent[] = [];
    const harness = createHarness({ telemetry: (event) => events.push(event) });
    await acknowledgeReady(harness);
    await settle();
    events.length = 0;

    harness.control.message(
      JSON.stringify({
        type: "attach-request",
        connectionId: "connection_browser_A",
        streamId: 1,
        deliveryGeneration: "1",
        engineId: harness.session.engineId,
        hasLiveReplica: false,
      }),
    );
    await settle();
    const barrierEncoded = harness.data.sent.find(
      (value): value is Uint8Array =>
        value instanceof Uint8Array &&
        decodeDataFrame(value).kind === DataFrameKind.DeliveryBarrier,
    );
    expect(barrierEncoded).toBeDefined();

    harness.control.message(
      JSON.stringify({
        type: "text",
        connectionId: "connection_browser_A",
        clientId: "client_AAAAAAAAA",
        writerLease: "writer-lease",
        inputEpoch: "input-epoch",
        clientInputSeq: "1",
        writerFence: "1",
        data: "input-during-barrier",
      }),
    );
    await settle();
    expect(harness.pty.writes).toEqual([new TextEncoder().encode("input-during-barrier")]);
    expect(events.some((event) => event.name === "host.input.apply")).toBe(false);

    const barrier = decodeDataFrame(barrierEncoded!);
    const payload = decodeDeliveryBarrierPayload(barrier.payload);
    if (payload.mode !== "snapshot") throw new Error("expected snapshot barrier");
    harness.control.message(
      JSON.stringify({
        type: "delivery-barrier-result",
        status: "ready",
        mode: "snapshot",
        connectionId: payload.connectionId,
        streamId: barrier.streamId,
        deliveryGeneration: barrier.deliveryGeneration.toString(),
        commitEventSeq: barrier.eventSeq.toString(),
        commitPtyOffset: barrier.ptyOffset.toString(),
        snapshotId: payload.snapshotId,
      }),
    );
    await settle();

    expect(events).toContainEqual(
      expect.objectContaining({
        name: "host.input.apply",
        outcome: "written",
        ackSendOutcome: "send-returned",
      }),
    );
    harness.relay.close();
    harness.session.dispose();
  });

  it("releases deferred diagnostics when a paused recovery is disposed", async () => {
    const events: TerminalTelemetryEvent[] = [];
    const harness = createHarness({ telemetry: (event) => events.push(event) });
    await acknowledgeReady(harness);
    await settle();
    events.length = 0;

    harness.control.message(
      JSON.stringify({
        type: "attach-request",
        connectionId: "connection_browser_A",
        streamId: 1,
        deliveryGeneration: "1",
        engineId: harness.session.engineId,
        hasLiveReplica: false,
      }),
    );
    await settle();
    expect(
      harness.data.sent.some(
        (value) =>
          value instanceof Uint8Array &&
          decodeDataFrame(value).kind === DataFrameKind.DeliveryBarrier,
      ),
    ).toBe(true);
    harness.control.message(
      JSON.stringify({
        type: "text",
        connectionId: "connection_browser_A",
        clientId: "client_AAAAAAAAA",
        writerLease: "writer-lease",
        inputEpoch: "input-epoch",
        clientInputSeq: "1",
        writerFence: "1",
        data: "input-before-dispose",
      }),
    );
    await settle();
    expect(events.some((event) => event.name === "host.input.apply")).toBe(false);

    harness.relay.close();
    await settle();

    expect(events).toContainEqual(
      expect.objectContaining({ name: "host.input.apply", outcome: "written" }),
    );
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
    const events: TerminalTelemetryEvent[] = [];
    const harness = createHarness({ telemetry: (event) => events.push(event) });
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
    expect(events).toContainEqual(
      expect.objectContaining({
        name: "host.control.queue",
        outcome: "capacity",
        queuedCount: HOST_CONTROL_QUEUE_LIMITS.maxCount,
      }),
    );
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
      const events: TerminalTelemetryEvent[] = [];
      const harness = createHarness({
        heartbeatIntervalMs: 100,
        heartbeatTimeoutMs: 300,
        telemetry: (event) => events.push(event),
      });
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
      for (let index = 0; index < 4; index += 1) await Promise.resolve();

      expect(harness.control.readyState).toBe(FakeWebSocket.CLOSED);
      expect(harness.data.readyState).toBe(FakeWebSocket.CLOSED);
      expect(events).toContainEqual(
        expect.objectContaining({
          name: "host.relay.rtt",
          channel: blackhole,
          outcome: "timeout",
          silenceMs: 300,
        }),
      );
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
