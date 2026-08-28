import type { InputSink, TerminalInputEvent, TerminalMouseInputEvent } from "@wterm/core";
import {
  ClientControlFrameSchema,
  type ClientControlFrame,
  type ResizePayload,
  type ServerControlFrame,
} from "@zhongduan/protocol";

import type {
  BrowserInputLifecycleProbe,
  BrowserPresentationDiagnostics,
} from "./presentation-diagnostics";

const MAX_PENDING_ACKS = 1_024;
const MAX_QUEUED_INPUTS = 256;

type InputAck = Extract<ServerControlFrame, { type: "input-ack" }>;
type InputFrame = Exclude<ClientControlFrame, { type: "ack" | "attach" | "writer-lease-renew" }>;

export interface InputDispatcherStatus {
  connected: boolean;
  lastStatus: InputAck["status"] | "idle";
  pending: number;
  replicaCurrent: boolean;
  resizeConfirmed: boolean;
  writable: boolean;
}

export interface InputDispatcherOptions {
  getObservedEventSeq: () => bigint | null;
  inputEpoch?: string;
  createInputEpoch?: () => string;
  presentation?: BrowserPresentationDiagnostics;
  queueMicrotask?: (callback: () => void) => void;
}

type FrameSender = (frame: InputFrame) => boolean;

const SAMPLED_QUEUED_INPUT = Symbol("sampled queued input");

interface SampledQueuedInput {
  readonly [SAMPLED_QUEUED_INPUT]: true;
  event: TerminalInputEvent;
  probe: BrowserInputLifecycleProbe;
}

interface SampledPendingInput {
  probe: BrowserInputLifecycleProbe;
  type: InputFrame["type"];
}

type QueuedInput = TerminalInputEvent | SampledQueuedInput;
type PendingInput = InputFrame["type"] | SampledPendingInput;

function boundedInteger(value: number, maximum = 1_000_000): number {
  if (!Number.isFinite(value)) return 0;
  return Math.min(maximum, Math.max(0, Math.round(value)));
}

function deltaModeName(value: number | undefined): "pixel" | "line" | "page" {
  if (value === 1) return "line";
  if (value === 2) return "page";
  return "pixel";
}

function isMouseMove(event: TerminalInputEvent | undefined): boolean {
  return event?.type === "mouse" && event.action === "move";
}

function queuedEvent(input: QueuedInput | undefined): TerminalInputEvent | undefined {
  if (input === undefined) return undefined;
  return SAMPLED_QUEUED_INPUT in input ? input.event : input;
}

function queuedProbe(input: QueuedInput): BrowserInputLifecycleProbe | undefined {
  return SAMPLED_QUEUED_INPUT in input ? input.probe : undefined;
}

function pendingProbe(input: PendingInput | undefined): BrowserInputLifecycleProbe | undefined {
  return typeof input === "object" ? input.probe : undefined;
}

function sameDimensions(
  left: Extract<TerminalInputEvent, { type: "resize" }>,
  right: ResizePayload,
): boolean {
  return (
    left.cols === right.cols &&
    left.rows === right.rows &&
    left.widthPx === right.widthPx &&
    left.heightPx === right.heightPx
  );
}

/** Converts semantic WTerm input into authority-owned control frames. */
export class InputDispatcher implements InputSink {
  readonly #createInputEpoch: () => string;
  readonly #getObservedEventSeq: () => bigint | null;
  readonly #presentation: BrowserPresentationDiagnostics | undefined;
  readonly #queueMicrotask: (callback: () => void) => void;
  readonly #listeners = new Set<() => void>();
  readonly #pending = new Map<bigint, PendingInput>();
  #inputEpoch: string;
  #nextSequence = 1n;
  #sender: FrameSender | null = null;
  #writerLease: string | null = null;
  #queue: QueuedInput[] = [];
  #flushScheduled = false;
  #latestResize: Extract<TerminalInputEvent, { type: "resize" }> | null = null;
  #replicaCurrent = false;
  #resizeConfirmed = false;
  #lastStatus: InputDispatcherStatus["lastStatus"] = "idle";
  #statusSnapshot: InputDispatcherStatus = {
    connected: false,
    lastStatus: "idle",
    pending: 0,
    replicaCurrent: false,
    resizeConfirmed: false,
    writable: false,
  };

  constructor(options: InputDispatcherOptions) {
    this.#getObservedEventSeq = options.getObservedEventSeq;
    this.#createInputEpoch = options.createInputEpoch ?? (() => crypto.randomUUID());
    this.#inputEpoch = options.inputEpoch ?? this.#createInputEpoch();
    this.#presentation = options.presentation;
    this.#queueMicrotask =
      options.queueMicrotask ?? ((callback) => globalThis.queueMicrotask(callback));
  }

  get inputEpoch(): string {
    return this.#inputEpoch;
  }

  get status(): InputDispatcherStatus {
    return this.#statusSnapshot;
  }

  startNewInputEpoch(): void {
    this.#presentation?.endInputEpoch("input-epoch-ended");
    this.#markOutstandingUncertain();
    this.#inputEpoch = this.#createInputEpoch();
    this.#nextSequence = 1n;
    this.#queue = [];
    this.#emit();
  }

  subscribe(listener: () => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  /** A new transport never reuses uncertain sends or assumes prior geometry. */
  attachTransport(sender: FrameSender, writerLease: string | undefined): void {
    if (this.#presentation !== undefined) {
      for (const pending of this.#pending.values()) {
        const probe = pendingProbe(pending);
        if (probe !== undefined) this.#presentation.finishInput(probe, "transport-replaced");
      }
    }
    this.#markOutstandingUncertain();
    this.#sender = sender;
    this.#writerLease = writerLease ?? null;
    this.#replicaCurrent = false;
    this.#resizeConfirmed = false;
    if (this.#latestResize !== null) this.#enqueue(this.#latestResize);
    this.#emit();
  }

  detachTransport(): void {
    this.#presentation?.endInputEpoch("transport-replaced");
    this.#sender = null;
    this.#writerLease = null;
    this.#queue = [];
    this.#replicaCurrent = false;
    this.#markOutstandingUncertain();
    this.#emit();
  }

  send(event: TerminalInputEvent): void {
    const probe = this.#presentation?.beginInputDispatch(event.type);
    if (event.type === "resize") {
      const previous = this.#latestResize;
      this.#latestResize = { ...event };
      if (previous !== null && !sameDimensions(previous, event) && this.#resizeConfirmed) {
        this.#resizeConfirmed = false;
        this.#emit();
      }
    }
    if (this.#sender === null || this.#writerLease === null) {
      this.#presentation?.finishInput(probe, "not-writable");
      return;
    }
    if (event.type === "mouse" && (!this.#resizeConfirmed || !this.#replicaCurrent)) {
      this.#presentation?.finishInput(probe, "policy-rejected");
      return;
    }
    this.#enqueue(event, probe);
  }

  setReplicaCurrent(current: boolean): void {
    if (this.#replicaCurrent === current) return;
    this.#replicaCurrent = current;
    this.#emit();
  }

  acceptAcknowledgement(frame: InputAck): void {
    if (frame.inputEpoch !== this.#inputEpoch) return;
    const sequence = BigInt(frame.clientInputSeq);
    const pending = this.#pending.get(sequence);
    if (pending === undefined || !this.#pending.delete(sequence)) return;
    if (pendingProbe(pending) !== undefined) {
      this.#presentation?.recordInputAck({
        inputEpoch: frame.inputEpoch,
        clientInputSeq: frame.clientInputSeq,
        status: frame.status,
      });
    }
    this.#lastStatus = frame.status;
    this.#emit();
  }

  confirmAuthoritativeResize(dimensions: ResizePayload): void {
    if (
      this.#resizeConfirmed ||
      this.#latestResize === null ||
      !sameDimensions(this.#latestResize, dimensions)
    ) {
      return;
    }
    this.#resizeConfirmed = true;
    this.#emit();
  }

  #enqueue(event: TerminalInputEvent, probe?: BrowserInputLifecycleProbe): void {
    const input: QueuedInput =
      probe === undefined ? event : { [SAMPLED_QUEUED_INPUT]: true, event, probe };
    if (isMouseMove(event) && isMouseMove(queuedEvent(this.#queue.at(-1)))) {
      const replaced = this.#queue[this.#queue.length - 1];
      if (replaced !== undefined) {
        this.#presentation?.finishInput(queuedProbe(replaced), "coalesced");
      }
      this.#queue[this.#queue.length - 1] = input;
    } else {
      if (this.#queue.length >= MAX_QUEUED_INPUTS) {
        this.#flush();
        if (this.#sender === null || this.#writerLease === null) {
          this.#presentation?.finishInput(probe, "not-writable");
          this.#lastStatus = "uncertain";
          this.#emit();
          return;
        }
      }
      this.#queue.push(input);
    }
    if (probe !== undefined) this.#presentation?.markInputQueued(probe);
    if (this.#flushScheduled) return;
    this.#flushScheduled = true;
    this.#queueMicrotask(() => this.#flush());
  }

  #flush(): void {
    this.#flushScheduled = false;
    const events = this.#queue;
    this.#queue = [];
    for (const input of events) {
      const event = queuedEvent(input) as TerminalInputEvent;
      const probe = queuedProbe(input);
      let frame: InputFrame | null;
      try {
        frame = this.#toFrame(event);
      } catch {
        this.#presentation?.finishInput(probe, "validation-failed");
        this.#lastStatus = "rejected";
        continue;
      }
      if (frame === null) {
        this.#presentation?.finishInput(probe, "not-writable");
        continue;
      }
      const sender = this.#sender;
      let sent = false;
      let decision: "rejected" | "uncertain" = "rejected";
      try {
        sent = sender?.(frame) ?? false;
      } catch {
        decision = "uncertain";
        sent = false;
      }
      if (probe !== undefined) {
        this.#presentation?.recordInputSendDecision(
          probe,
          { inputEpoch: frame.inputEpoch, clientInputSeq: frame.clientInputSeq },
          sent ? "sent" : decision,
        );
      }
      if (!sent) {
        this.#lastStatus = "uncertain";
        continue;
      }
      const sequence = BigInt(frame.clientInputSeq);
      this.#pending.set(sequence, probe === undefined ? frame.type : { type: frame.type, probe });
      if (this.#pending.size > MAX_PENDING_ACKS) {
        const oldest = this.#pending.keys().next().value as bigint | undefined;
        if (oldest !== undefined) {
          const evicted = this.#pending.get(oldest);
          if (this.#pending.delete(oldest)) {
            this.#presentation?.finishInput(pendingProbe(evicted), "pending-capacity");
          }
        }
        this.#lastStatus = "uncertain";
      }
    }
    this.#emit();
  }

  #toFrame(event: TerminalInputEvent): InputFrame | null {
    const writerLease = this.#writerLease;
    if (writerLease === null) return null;
    const identity = {
      writerLease,
      inputEpoch: this.#inputEpoch,
      clientInputSeq: (this.#nextSequence++).toString(),
    };
    let frame: unknown;
    switch (event.type) {
      case "key": {
        const observedEventSeq = this.#getObservedEventSeq() ?? 0n;
        frame = {
          type: "key",
          ...identity,
          observedEventSeq: observedEventSeq.toString(),
          code: event.code,
          key: event.key,
          ...(event.text === undefined ? {} : { text: event.text }),
          modifiers: event.modifiers,
          action: event.action,
          altGraph: event.altGraph,
          composing: event.composing,
          consumedModifiers: event.consumedModifiers,
          ...(event.unshiftedCodepoint === 0
            ? {}
            : { unshiftedCodepoint: event.unshiftedCodepoint }),
        };
        break;
      }
      case "text":
        frame = { type: "text", ...identity, data: event.text };
        break;
      case "paste":
        frame = { type: "paste", ...identity, data: event.text };
        break;
      case "focus":
        frame = { type: "focus", ...identity, focused: event.focused };
        break;
      case "resize":
        frame = {
          type: "resize-request",
          ...identity,
          cols: event.cols,
          rows: event.rows,
          widthPx: event.widthPx,
          heightPx: event.heightPx,
        };
        break;
      case "mouse":
        frame = this.#mouseFrame(event, identity);
        break;
    }
    return ClientControlFrameSchema.parse(frame) as InputFrame;
  }

  #mouseFrame(
    event: TerminalMouseInputEvent,
    identity: { writerLease: string; inputEpoch: string; clientInputSeq: string },
  ): unknown {
    const common = {
      type: "mouse",
      ...identity,
      action: event.action,
      button: event.action === "press" || event.action === "release" ? event.button : null,
      buttons: event.buttons,
      modifiers: event.modifiers,
      altGraph: event.altGraph,
      surface: {
        x: boundedInteger(event.surface.x),
        y: boundedInteger(event.surface.y),
      },
    };
    return event.action === "wheel"
      ? {
          ...common,
          deltaX: event.deltaX,
          deltaY: event.deltaY,
          deltaMode: deltaModeName(event.deltaMode),
        }
      : common;
  }

  #markOutstandingUncertain(): void {
    if (this.#pending.size > 0) this.#lastStatus = "uncertain";
    this.#pending.clear();
  }

  #emit(): void {
    this.#statusSnapshot = {
      connected: this.#sender !== null,
      lastStatus: this.#lastStatus,
      pending: this.#pending.size,
      replicaCurrent: this.#replicaCurrent,
      resizeConfirmed: this.#resizeConfirmed,
      writable: this.#sender !== null && this.#writerLease !== null,
    };
    for (const listener of this.#listeners) listener();
  }
}
