import type { ConnectionSetResponse, ResizePayload } from "@zhongduan/protocol";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { FakeTerminalAuthority } from "../fake-terminal-authority";
import { EventJournal } from "../journal";
import type { PtyProcess } from "../pty-process";
import { TerminalSession } from "../session";
import { CloudApiError } from "./cloud-api";
import { HostCloudRelay, type HostCloudApi } from "./host-cloud-relay";

class ManualPty implements PtyProcess {
  readonly pid = 42;
  readonly writes: Uint8Array[] = [];
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
  }

  resize(_dimensions: ResizePayload): void {}

  kill(): void {
    this.#exitListener?.(0, 0);
  }
}

class ShortLivedWebSocket extends EventTarget {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  binaryType: BinaryType = "blob";
  bufferedAmount = 0;
  readyState = ShortLivedWebSocket.CONNECTING;

  constructor(
    readonly url: string,
    private readonly closeAfterReady: boolean,
  ) {
    super();
    queueMicrotask(() => {
      this.readyState = ShortLivedWebSocket.OPEN;
      this.dispatchEvent(new Event("open"));
    });
  }

  close(): void {
    if (this.readyState >= ShortLivedWebSocket.CLOSING) return;
    this.readyState = ShortLivedWebSocket.CLOSED;
    this.dispatchEvent(new Event("close"));
  }

  send(data: string | ArrayBufferLike | Blob | ArrayBufferView): void {
    if (!this.url.includes("/control") || typeof data !== "string") return;
    const frame = JSON.parse(data) as { type?: string; [key: string]: unknown };
    if (frame.type !== "host-ready") return;
    queueMicrotask(() => {
      this.dispatchEvent(
        new MessageEvent("message", {
          data: JSON.stringify({
            type: "host-ready-ack",
            sessionEpoch: frame.sessionEpoch,
            headEventSeq: frame.headEventSeq,
            nextPtyOffset: frame.nextPtyOffset,
          }),
        }),
      );
      if (this.closeAfterReady) setTimeout(() => this.close(), 1);
    });
  }
}

class HeartbeatWebSocket extends EventTarget {
  binaryType: BinaryType = "blob";
  bufferedAmount = 0;
  readyState = ShortLivedWebSocket.CONNECTING;

  constructor(
    readonly url: string,
    private readonly blackhole: "control" | "data",
  ) {
    super();
    queueMicrotask(() => {
      this.readyState = ShortLivedWebSocket.OPEN;
      this.dispatchEvent(new Event("open"));
    });
  }

  close(): void {
    if (this.readyState >= ShortLivedWebSocket.CLOSING) return;
    this.readyState = ShortLivedWebSocket.CLOSED;
    this.dispatchEvent(new Event("close"));
  }

  send(data: string | ArrayBufferLike | Blob | ArrayBufferView): void {
    if (data === "ping") {
      if (!this.url.includes(`/${this.blackhole}`)) {
        queueMicrotask(() => {
          this.dispatchEvent(new MessageEvent("message", { data: "pong" }));
        });
      }
      return;
    }
    if (!this.url.includes("/control") || typeof data !== "string") return;
    const frame = JSON.parse(data) as { type?: string; [key: string]: unknown };
    if (frame.type !== "host-ready") return;
    queueMicrotask(() => {
      this.dispatchEvent(
        new MessageEvent("message", {
          data: JSON.stringify({
            type: "host-ready-ack",
            sessionEpoch: frame.sessionEpoch,
            headEventSeq: frame.headEventSeq,
            nextPtyOffset: frame.nextPtyOffset,
          }),
        }),
      );
    });
  }
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(0);
  vi.stubGlobal("WebSocket", ShortLivedWebSocket);
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("HostCloudRelay", () => {
  it("keeps a shell writable while permanent Cloud 4xx responses remain degraded", async () => {
    const capabilityAttempts: number[] = [];
    const pty = new ManualPty();
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal: new EventJournal(),
      pty,
      sessionEpoch: 7n,
    });
    const relay = new HostCloudRelay({
      api: {
        createConnectionSet: async () => connectionSet(1),
        uploadSnapshot: async (metadata) => ({ created: true, snapshot: metadata }),
        webSocketUrl: (_sessionId, channel, ticket) =>
          `wss://cloud.example/${channel}?ticket=${ticket}`,
      },
      capabilities: {
        current: async () => {
          capabilityAttempts.push(Date.now());
          throw new CloudApiError(403, "invalid-bootstrap");
        },
        recoverRejected: async () => "unused",
      },
      degradedReconnectDelayMs: 100,
      maxReconnectDelayMs: 80,
      monotonicNow: () => Date.now(),
      random: () => 0.5,
      reconnectDelayMs: 10,
      session,
      sessionId: "session_AAAAAAAAA",
      snapshotPublisher: { publish: async () => Promise.reject(new Error("unused")) },
      stableConnectionMs: 1_000,
    });

    const firstReady = relay.start();
    const stoppedBeforeReady = expect(firstReady).rejects.toThrow(/stopped before becoming ready/);
    await vi.advanceTimersByTimeAsync(300);
    await expect(
      session.submitText(
        { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1", writerFence: 1n },
        "alive",
      ),
    ).resolves.toMatchObject({ status: "written" });

    expect(capabilityAttempts).toEqual([0, 100, 200, 300]);
    expect(pty.writes).toEqual([new TextEncoder().encode("alive")]);
    expect(session.sessionEpoch).toBe(7n);
    await relay.stop();
    await stoppedBeforeReady;
    session.dispose();
  });

  it("keeps the PTY alive through persistent credential errors and later recovers", async () => {
    const capabilityAttempts: number[] = [];
    const api: HostCloudApi = {
      createConnectionSet: async () => connectionSet(1),
      uploadSnapshot: async (metadata) => ({ created: true, snapshot: metadata }),
      webSocketUrl: (_sessionId, channel, ticket) =>
        `wss://cloud.example/${channel}?ticket=${ticket}`,
    };
    const pty = new ManualPty();
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal: new EventJournal(),
      pty,
      sessionEpoch: 7n,
    });
    const relay = new HostCloudRelay({
      api,
      capabilities: {
        current: async () => {
          capabilityAttempts.push(Date.now());
          if (capabilityAttempts.length < 3) {
            throw new CloudApiError(403, "invalid-bootstrap");
          }
          return "host-capability";
        },
        recoverRejected: async () => "host-capability-recovered",
      },
      degradedReconnectDelayMs: 100,
      maxReconnectDelayMs: 80,
      monotonicNow: () => Date.now(),
      random: () => 0.5,
      reconnectDelayMs: 10,
      session,
      sessionId: "session_AAAAAAAAA",
      snapshotPublisher: { publish: async () => Promise.reject(new Error("unused")) },
      stableConnectionMs: 1_000,
      webSocketFactory: (url) => new ShortLivedWebSocket(url, false) as unknown as WebSocket,
    });

    const firstReady = relay.start();
    await vi.advanceTimersByTimeAsync(199);
    expect(capabilityAttempts).toEqual([0, 100]);
    expect(pty.pid).toBe(42);
    expect(session.sessionEpoch).toBe(7n);
    await vi.advanceTimersByTimeAsync(1);
    await firstReady;
    expect(capabilityAttempts).toEqual([0, 100, 200]);

    await relay.stop();
    session.dispose();
  });

  it("backs off repeatedly short-lived ready pairs without replacing the PTY epoch", async () => {
    const connectionTimes: number[] = [];
    let connectionNumber = 0;
    const api: HostCloudApi = {
      createConnectionSet: async () => {
        connectionTimes.push(Date.now());
        connectionNumber += 1;
        return connectionSet(connectionNumber);
      },
      uploadSnapshot: async (metadata) => ({ created: true, snapshot: metadata }),
      webSocketUrl: (_sessionId, channel, ticket) =>
        `wss://cloud.example/${channel}?ticket=${ticket}`,
    };
    const pty = new ManualPty();
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal: new EventJournal(),
      pty,
      sessionEpoch: 7n,
    });
    const relay = new HostCloudRelay({
      api,
      capabilities: {
        current: async () => "host-capability",
        recoverRejected: async () => "host-capability-recovered",
      },
      maxReconnectDelayMs: 800,
      monotonicNow: () => Date.now(),
      random: () => 0.5,
      reconnectDelayMs: 100,
      session,
      sessionId: "session_AAAAAAAAA",
      snapshotPublisher: { publish: async () => Promise.reject(new Error("unused")) },
      stableConnectionMs: 1_000,
      webSocketFactory: (url) => new ShortLivedWebSocket(url, true) as unknown as WebSocket,
    });

    const firstReady = relay.start();
    await vi.advanceTimersByTimeAsync(0);
    await firstReady;
    await vi.advanceTimersByTimeAsync(750);

    expect(connectionTimes.slice(0, 4)).toEqual([0, 101, 302, 703]);
    expect(pty.pid).toBe(42);
    expect(session.sessionEpoch).toBe(7n);
    await expect(
      session.submitText(
        { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1", writerFence: 1n },
        "alive",
      ),
    ).resolves.toMatchObject({ status: "written" });
    expect(pty.writes).toEqual([new TextEncoder().encode("alive")]);

    await relay.stop();
    session.dispose();
  });

  it.each(["control", "data"] as const)(
    "reconnects a %s-only heartbeat blackhole without replacing the PTY epoch",
    async (blackhole) => {
      const connectionTimes: number[] = [];
      let connectionNumber = 0;
      const api: HostCloudApi = {
        createConnectionSet: async () => {
          connectionTimes.push(Date.now());
          connectionNumber += 1;
          return connectionSet(connectionNumber);
        },
        uploadSnapshot: async (metadata) => ({ created: true, snapshot: metadata }),
        webSocketUrl: (_sessionId, channel, ticket) =>
          `wss://cloud.example/${channel}?ticket=${ticket}`,
      };
      const pty = new ManualPty();
      const session = new TerminalSession({
        authority: new FakeTerminalAuthority(),
        journal: new EventJournal(),
        pty,
        sessionEpoch: 7n,
      });
      const relay = new HostCloudRelay({
        api,
        capabilities: {
          current: async () => "host-capability",
          recoverRejected: async () => "host-capability-recovered",
        },
        maxReconnectDelayMs: 800,
        monotonicNow: () => Date.now(),
        random: () => 0.5,
        reconnectDelayMs: 100,
        session,
        sessionId: "session_AAAAAAAAA",
        snapshotPublisher: { publish: async () => Promise.reject(new Error("unused")) },
        stableConnectionMs: 60_000,
        webSocketFactory: (url) => new HeartbeatWebSocket(url, blackhole) as unknown as WebSocket,
      });

      const firstReady = relay.start();
      await vi.advanceTimersByTimeAsync(0);
      await firstReady;
      await vi.advanceTimersByTimeAsync(45_100);

      expect(connectionTimes).toEqual([0, 45_100]);
      expect(pty.pid).toBe(42);
      expect(session.sessionEpoch).toBe(7n);
      await expect(
        session.submitText(
          { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1", writerFence: 1n },
          "alive",
        ),
      ).resolves.toMatchObject({ status: "written" });
      expect(pty.writes).toEqual([new TextEncoder().encode("alive")]);

      await relay.stop();
      session.dispose();
    },
  );
});

function connectionSet(index: number): ConnectionSetResponse {
  const suffix = index.toString().padStart(16, "0");
  return {
    connectionSetId: `connection_set_${suffix}`,
    connectionId: `connection_${suffix}`,
    clientId: null,
    streamId: 0,
    deliveryGeneration: "0",
    expiresAt: 2_000,
    controlTicket: `control_ticket_${suffix}`,
    dataTicket: `data_ticket_${suffix}`,
  };
}
