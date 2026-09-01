import {
  ClientControlFrameSchema,
  type BrowserCapabilityRole,
  type ClientControlFrame,
  type ServerControlFrame,
} from "@zhongduan/protocol";

import type { InputDispatcher, InputTransportSendResult } from "./input-dispatcher";

const WRITER_LEASE_RENEW_INTERVAL_MS = 10_000;
type Timer = ReturnType<typeof setTimeout>;

interface BrowserWriterSessionOptions {
  clearTimer: (timer: Timer) => void;
  connectionEpoch: () => number;
  input: InputDispatcher;
  isConnectionCurrent: (connectionEpoch: number) => boolean;
  onControlReplacementRequired: () => void;
  onProtocolFailure: () => void;
  role: BrowserCapabilityRole;
  sendControl: (frame: ClientControlFrame, critical: boolean) => boolean;
  sendInput: (frame: ClientControlFrame) => InputTransportSendResult;
  setTimer: (callback: () => void, delayMs: number) => Timer;
}

/** Owns the connection-scoped writer lease, input transport, renewal timer, and fence revocation. */
export class BrowserWriterSession {
  readonly #clearTimer: (timer: Timer) => void;
  readonly #connectionEpoch: () => number;
  readonly #input: InputDispatcher;
  readonly #isConnectionCurrent: (connectionEpoch: number) => boolean;
  readonly #onControlReplacementRequired: () => void;
  readonly #onProtocolFailure: () => void;
  readonly #role: BrowserCapabilityRole;
  readonly #sendControl: BrowserWriterSessionOptions["sendControl"];
  readonly #sendInput: BrowserWriterSessionOptions["sendInput"];
  readonly #setTimer: BrowserWriterSessionOptions["setTimer"];
  #closed = false;
  #inputAdmissionEnabled = false;
  #replacementScheduled = false;
  #unsubscribeInput: (() => void) | null;
  #writerFence: string | null = null;
  #writerLease: string | null = null;
  #writerLeaseTimer: Timer | null = null;

  constructor(options: BrowserWriterSessionOptions) {
    this.#clearTimer = options.clearTimer;
    this.#connectionEpoch = options.connectionEpoch;
    this.#input = options.input;
    this.#isConnectionCurrent = options.isConnectionCurrent;
    this.#onControlReplacementRequired = options.onControlReplacementRequired;
    this.#onProtocolFailure = options.onProtocolFailure;
    this.#role = options.role;
    this.#sendControl = options.sendControl;
    this.#sendInput = options.sendInput;
    this.#setTimer = options.setTimer;
    this.#unsubscribeInput = this.#input.subscribe(() => {
      if (
        !this.#input.status.controlReplacementRequired ||
        this.#replacementScheduled ||
        this.#closed
      ) {
        return;
      }
      this.#replacementScheduled = true;
      globalThis.queueMicrotask(() => {
        this.#replacementScheduled = false;
        if (!this.#closed && this.#input.status.controlReplacementRequired) {
          this.#onControlReplacementRequired();
        }
      });
    });
  }

  get inputAdmissionEnabled(): boolean {
    return this.#inputAdmissionEnabled;
  }

  beginConnection(): void {
    this.#input.detachTransport();
    this.#writerLease = null;
    this.#writerFence = null;
    this.#inputAdmissionEnabled = false;
    this.#stopWriterLeaseRenewal();
  }

  setInputAdmissionEnabled(enabled: boolean): void {
    this.#inputAdmissionEnabled = enabled;
  }

  acceptWelcome(
    frame: Extract<ServerControlFrame, { type: "welcome" }>,
  ): "observer" | "waiting" | "writer" | undefined {
    const previousLease = this.#writerLease;
    const previousFence = this.#writerFence;
    if (this.#inputAdmissionEnabled && frame.writerLease !== undefined) {
      if (frame.writerFence === undefined) {
        this.#onProtocolFailure();
        return undefined;
      }
      this.#writerLease = frame.writerLease;
      this.#writerFence = frame.writerFence;
    } else {
      // During component skew a Browser cannot allocate a Browser-visible identity without a Cloud
      // fence. A new-new pair selects browser-input-admission-v1 and stays writable on v2.
      this.#writerLease = null;
      this.#writerFence = null;
    }
    const writable =
      this.#input.status.connected &&
      previousLease === this.#writerLease &&
      previousFence === this.#writerFence
        ? this.#input.status.writable
        : this.#attachInputTransport();
    if (this.#writerLease !== null && !writable) {
      this.#onProtocolFailure();
      return undefined;
    }
    if (this.#writerLease === null) this.#stopWriterLeaseRenewal();
    else this.#startWriterLeaseRenewal();
    return this.#role === "observer"
      ? "observer"
      : this.#writerLease === null
        ? "waiting"
        : "writer";
  }

  acceptLeaseStatus(
    frame: Extract<ServerControlFrame, { type: "writer-lease-status" }>,
  ): "waiting" | undefined {
    if (!this.#inputAdmissionEnabled || this.#role === "observer") return;
    if (frame.active) {
      if (
        frame.writerFence === undefined ||
        this.#writerFence === null ||
        frame.writerFence !== this.#writerFence ||
        this.#writerLease === null
      ) {
        this.#onProtocolFailure();
      }
      return;
    }
    this.#writerLease = null;
    this.#writerFence = null;
    this.#stopWriterLeaseRenewal();
    this.#input.revokeWriterAuthority();
    return "waiting";
  }

  acceptAcknowledgement(frame: Extract<ServerControlFrame, { type: "input-ack" }>): void {
    if (!this.#inputAdmissionEnabled) return;
    if (frame.writerFence === undefined) {
      this.#onProtocolFailure();
      return;
    }
    this.#input.acceptAcknowledgement(frame);
  }

  setReplicaCurrent(current: boolean): void {
    this.#input.setReplicaCurrent(current);
  }

  noteDataTransportReplacement(): void {
    this.#input.noteDataTransportReplacement();
  }

  detachInputTransport(): void {
    this.#input.detachTransport();
  }

  disconnect(reason?: "closed"): void {
    this.#stopWriterLeaseRenewal();
    this.#input.detachTransport(reason);
    this.#writerLease = null;
    this.#writerFence = null;
    this.#inputAdmissionEnabled = false;
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.disconnect("closed");
    this.#unsubscribeInput?.();
    this.#unsubscribeInput = null;
  }

  #startWriterLeaseRenewal(): void {
    this.#stopWriterLeaseRenewal();
    const connectionEpoch = this.#connectionEpoch();
    const tick = () => {
      this.#writerLeaseTimer = null;
      if (!this.#isConnectionCurrent(connectionEpoch)) return;
      const writerLease = this.#writerLease;
      if (writerLease === null) return;
      const sent = this.#sendControl(
        ClientControlFrameSchema.parse({ type: "writer-lease-renew", writerLease }),
        true,
      );
      if (!sent || !this.#isConnectionCurrent(connectionEpoch)) return;
      this.#writerLeaseTimer = this.#setTimer(tick, WRITER_LEASE_RENEW_INTERVAL_MS);
    };
    this.#writerLeaseTimer = this.#setTimer(tick, WRITER_LEASE_RENEW_INTERVAL_MS);
  }

  #stopWriterLeaseRenewal(): void {
    if (this.#writerLeaseTimer !== null) this.#clearTimer(this.#writerLeaseTimer);
    this.#writerLeaseTimer = null;
  }

  #attachInputTransport(): boolean {
    const writable = this.#input.attachTransport({
      generation: this.#connectionEpoch(),
      sender: (inputFrame) => this.#sendInput(inputFrame),
      ...(this.#writerLease === null ? {} : { writerLease: this.#writerLease }),
      ...(this.#writerFence === null ? {} : { writerFence: this.#writerFence }),
    });
    if (writable) this.#input.reconcileLatestResize();
    return writable;
  }
}
