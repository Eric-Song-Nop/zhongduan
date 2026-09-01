import {
  ClientControlFrameSchema,
  decodeControlFrame,
  type ClientControlFrame,
} from "@zhongduan/protocol";

import { CloudInputTelemetry, type CloudInputDisposition } from "./cloud-input-telemetry";
import {
  BROWSER_CONTROL_LANE_LIMITS,
  BoundedKeyedQueue,
  type QueueExpirationReason,
  type QueueTaskTiming,
  type SocketQueueLimits,
} from "./relay-message-queue";
import type { SocketAttachment } from "./relay-socket";

export type SemanticInputFrame = Exclude<
  ClientControlFrame,
  { type: "ack" | "attach" | "writer-lease-renew" }
>;

export interface BrowserControlTiming extends QueueTaskTiming {
  receivedAtMs: number;
}

interface BrowserControlLaneOptions {
  now?: () => number;
  process: (
    webSocket: WebSocket,
    message: ArrayBuffer | string,
    timing: BrowserControlTiming,
  ) => Promise<void> | void;
  reject: (webSocket: WebSocket, attachment: SocketAttachment, reason: string) => void;
}

export function isSemanticInputFrame(frame: ClientControlFrame): frame is SemanticInputFrame {
  return frame.type !== "ack" && frame.type !== "attach" && frame.type !== "writer-lease-renew";
}

/** Owns Browser control admission, per-connection FIFO, isolation, and bounded input evidence. */
export class BrowserControlLane {
  readonly telemetry = new CloudInputTelemetry();
  readonly #now: () => number;
  readonly #process: BrowserControlLaneOptions["process"];
  readonly #queue: BoundedKeyedQueue<WebSocket>;
  readonly #reject: BrowserControlLaneOptions["reject"];

  constructor(options: BrowserControlLaneOptions) {
    this.#now = options.now ?? Date.now;
    this.#process = options.process;
    this.#queue = new BoundedKeyedQueue<WebSocket>(BROWSER_CONTROL_LANE_LIMITS, this.#now);
    this.#reject = options.reject;
  }

  get activeCount(): number {
    return this.#queue.activeCount;
  }

  get queuedBytes(): number {
    return this.#queue.queuedBytes;
  }

  get queuedCount(): number {
    return this.#queue.queuedCount;
  }

  enqueue(
    key: WebSocket,
    bytes: number,
    run: (timing: QueueTaskTiming) => Promise<void> | void,
    expire: (timing: QueueTaskTiming, reason: QueueExpirationReason) => Promise<void> | void,
    socketLimits?: SocketQueueLimits,
  ): Promise<void> | undefined {
    return this.#queue.enqueue(key, bytes, run, expire, socketLimits);
  }

  dispatch(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    message: ArrayBuffer | string,
    bytes: number,
    queueLimits: SocketQueueLimits,
    receivedAtMs: number,
  ): void {
    const processing = this.enqueue(
      webSocket,
      bytes,
      (timing) => this.#process(webSocket, message, { ...timing, receivedAtMs }),
      (timing, reason) => {
        const disposition = reason === "age" ? "lane-expired" : "lane-overload";
        this.#recordDroppedInput(attachment, message, { ...timing, receivedAtMs }, disposition);
        this.#reject(
          webSocket,
          attachment,
          reason === "age"
            ? "browser control lane age exceeded"
            : "browser control lane isolated an overloaded source",
        );
      },
      queueLimits,
    );
    if (processing === undefined) {
      const rejectedAtMs = this.#now();
      this.#recordDroppedInput(
        attachment,
        message,
        {
          enqueuedAtMs: receivedAtMs,
          receivedAtMs,
          startedAtMs: rejectedAtMs,
          waitMs: Math.max(0, rejectedAtMs - receivedAtMs),
        },
        "lane-overload",
      );
      this.#reject(webSocket, attachment, "browser control lane exceeded");
      return;
    }
    void processing.catch(() => {
      this.#reject(webSocket, attachment, "browser control message failed");
    });
  }

  recordInput(
    frame: SemanticInputFrame,
    attachment: SocketAttachment,
    timing: BrowserControlTiming | undefined,
    disposition: CloudInputDisposition,
    hostSendAtMs: number | null,
    writerFence: string | null,
  ): void {
    if (timing === undefined || attachment.clientId === null) return;
    this.telemetry.record({
      clientId: attachment.clientId,
      clientInputSeq: frame.clientInputSeq,
      connectionId: attachment.connectionId,
      disposition,
      hostSendAtMs,
      inputEpoch: frame.inputEpoch,
      queueEnteredAtMs: timing.enqueuedAtMs,
      queueLeftAtMs: timing.startedAtMs,
      receivedAtMs: timing.receivedAtMs,
      writerFence,
    });
  }

  #recordDroppedInput(
    attachment: SocketAttachment,
    message: ArrayBuffer | string,
    timing: BrowserControlTiming,
    disposition: Extract<CloudInputDisposition, "lane-expired" | "lane-overload">,
  ): void {
    if (typeof message !== "string" || attachment.clientId === null) return;
    let frame: ClientControlFrame;
    try {
      frame = decodeControlFrame(message, ClientControlFrameSchema);
    } catch {
      return;
    }
    if (!isSemanticInputFrame(frame)) return;
    this.recordInput(frame, attachment, timing, disposition, null, attachment.leaseFence);
  }
}
