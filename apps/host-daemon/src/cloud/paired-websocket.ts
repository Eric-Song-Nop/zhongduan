import type { ConnectionSetResponse } from "@zhongduan/protocol";

import { CloudApiClient } from "./cloud-api";

export const HOST_PAIR_OPEN_TIMEOUT_MS = 10_000;

export type RelayWebSocketFactory = (url: string) => WebSocket;

export interface HostConnectionApi {
  createConnectionSet: CloudApiClient["createConnectionSet"];
  webSocketUrl: CloudApiClient["webSocketUrl"];
}

export interface HostSocketPair {
  readonly connection: ConnectionSetResponse;
  readonly control: WebSocket;
  readonly data: WebSocket;
  close(code?: number, reason?: string): void;
}

export interface OpenHostSocketPairOptions {
  api: HostConnectionApi;
  capability: string;
  sessionId: string;
  signal?: AbortSignal;
  timeoutMs?: number;
  webSocketFactory?: RelayWebSocketFactory;
}

export async function openHostSocketPair(
  options: OpenHostSocketPairOptions,
): Promise<HostSocketPair> {
  const timeoutMs = options.timeoutMs ?? HOST_PAIR_OPEN_TIMEOUT_MS;
  if (!Number.isInteger(timeoutMs) || timeoutMs <= 0) {
    throw new RangeError("Host pair open timeout must be a positive integer");
  }
  const deadline = new AbortController();
  const timeout = setTimeout(
    () => deadline.abort(new DOMException("Host pair open timed out", "TimeoutError")),
    timeoutMs,
  );
  const signal =
    options.signal === undefined
      ? deadline.signal
      : AbortSignal.any([options.signal, deadline.signal]);
  let control: WebSocket | undefined;
  let data: WebSocket | undefined;
  try {
    const connection = await options.api.createConnectionSet(
      options.sessionId,
      options.capability,
      {},
      signal,
    );
    if (connection.clientId !== null || connection.streamId !== 0) {
      throw new Error("cloud API returned a browser connection set for the Host");
    }

    const factory = options.webSocketFactory ?? ((url: string) => new WebSocket(url));
    control = factory(
      options.api.webSocketUrl(options.sessionId, "control", connection.controlTicket),
    );
    await waitForOpen(control, signal);

    data = factory(options.api.webSocketUrl(options.sessionId, "data", connection.dataTicket));
    data.binaryType = "arraybuffer";
    await waitForOpen(data, signal);

    let closed = false;
    return {
      connection,
      control,
      data,
      close(code = 1000, reason = "host relay stopped") {
        if (closed) return;
        closed = true;
        closeSocket(data!, code, reason);
        closeSocket(control!, code, reason);
      },
    };
  } catch (error) {
    if (data !== undefined) closeSocket(data, 1012, "host pair connection failed");
    if (control !== undefined) closeSocket(control, 1012, "host pair connection failed");
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function waitForOpen(socket: WebSocket, signal?: AbortSignal): Promise<void> {
  if (socket.readyState === WebSocket.OPEN) return Promise.resolve();
  if (signal?.aborted) return Promise.reject(signal.reason);

  return new Promise((resolve, reject) => {
    const cleanup = () => {
      socket.removeEventListener("open", onOpen);
      socket.removeEventListener("error", onError);
      socket.removeEventListener("close", onClose);
      signal?.removeEventListener("abort", onAbort);
    };
    const onOpen = () => {
      cleanup();
      resolve();
    };
    const onError = () => {
      cleanup();
      reject(new Error("relay WebSocket failed before opening"));
    };
    const onClose = () => {
      cleanup();
      reject(new Error("relay WebSocket closed before opening"));
    };
    const onAbort = () => {
      cleanup();
      reject(signal?.reason ?? new DOMException("aborted", "AbortError"));
    };
    socket.addEventListener("open", onOpen);
    socket.addEventListener("error", onError);
    socket.addEventListener("close", onClose);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function closeSocket(socket: WebSocket, code: number, reason: string): void {
  if (socket.readyState >= WebSocket.CLOSING) return;
  try {
    socket.close(code, reason);
  } catch {
    // The socket may have failed between the readyState check and close().
  }
}
