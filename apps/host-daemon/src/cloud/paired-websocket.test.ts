import type { ConnectionSetResponse } from "@zhongduan/protocol";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { openHostSocketPair, type HostConnectionApi } from "./paired-websocket";

const connection: ConnectionSetResponse = {
  connectionSetId: "connection_set_AA",
  connectionId: "connection_AAAAA",
  clientId: null,
  streamId: 0,
  deliveryGeneration: "0",
  expiresAt: 2_000,
  controlTicket: "control_ticket_A",
  dataTicket: "data_ticket_AAAA",
};

class FakeWebSocket extends EventTarget {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  readonly closes: Array<{ code: number; reason: string }> = [];
  readonly sent: Array<string | ArrayBufferLike | Blob | ArrayBufferView> = [];
  binaryType: BinaryType = "blob";
  bufferedAmount = 0;
  readyState = FakeWebSocket.CONNECTING;

  constructor(readonly url: string) {
    super();
  }

  open(): void {
    this.readyState = FakeWebSocket.OPEN;
    this.dispatchEvent(new Event("open"));
  }

  close(code = 1000, reason = ""): void {
    if (this.readyState >= FakeWebSocket.CLOSING) return;
    this.closes.push({ code, reason });
    this.readyState = FakeWebSocket.CLOSED;
    this.dispatchEvent(new Event("close"));
  }

  send(data: string | ArrayBufferLike | Blob | ArrayBufferView): void {
    this.sent.push(data);
  }
}

function api(): HostConnectionApi {
  return {
    createConnectionSet: async () => connection,
    webSocketUrl: (_sessionId, channel, ticket) =>
      `wss://cloud.example/${channel}?ticket=${ticket}`,
  };
}

beforeEach(() => vi.stubGlobal("WebSocket", FakeWebSocket));
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("openHostSocketPair", () => {
  it("opens control completely before constructing the data channel", async () => {
    const sockets: FakeWebSocket[] = [];
    const opening = openHostSocketPair({
      api: api(),
      capability: "host-cap",
      sessionId: "session_AAAAAAAAA",
      webSocketFactory(url) {
        const socket = new FakeWebSocket(url);
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
    });
    await Promise.resolve();

    expect(sockets.map((socket) => socket.url)).toEqual([
      "wss://cloud.example/control?ticket=control_ticket_A",
    ]);
    sockets[0]!.open();
    await Promise.resolve();
    expect(sockets.map((socket) => socket.url)).toEqual([
      "wss://cloud.example/control?ticket=control_ticket_A",
      "wss://cloud.example/data?ticket=data_ticket_AAAA",
    ]);
    sockets[1]!.open();

    const pair = await opening;
    expect(pair.control).toBe(sockets[0]);
    expect(pair.data).toBe(sockets[1]);
    expect(sockets[1]!.binaryType).toBe("arraybuffer");
    pair.close(1000, "done");
    expect(sockets.every((socket) => socket.readyState === FakeWebSocket.CLOSED)).toBe(true);
  });

  it("times out a black-holed pair and closes the partial control socket", async () => {
    vi.useFakeTimers();
    const sockets: FakeWebSocket[] = [];
    const opening = openHostSocketPair({
      api: api(),
      capability: "host-cap",
      sessionId: "session_AAAAAAAAA",
      timeoutMs: 100,
      webSocketFactory(url) {
        const socket = new FakeWebSocket(url);
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
    });
    await Promise.resolve();

    const rejected = expect(opening).rejects.toMatchObject({ name: "TimeoutError" });
    await vi.advanceTimersByTimeAsync(100);
    await rejected;
    expect(sockets).toHaveLength(1);
    expect(sockets[0]!.closes).toEqual([{ code: 1012, reason: "host pair connection failed" }]);
  });
});
