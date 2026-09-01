import {
  MAX_DELIVERY_OUTSTANDING_BYTES,
  deliveryOutstandingBytes,
  type ReplicaCursor,
} from "@zhongduan/protocol";

const DELIVERY_ACK_FLUSH_INTERVAL_MS = 10;
const DELIVERY_ACK_FLUSH_BYTES = MAX_DELIVERY_OUTSTANDING_BYTES / 16;
const DELIVERY_ACK_FLUSH_EVENTS = 256n;

interface DeliveryAckCoalescerOptions {
  clearTimer: (timer: ReturnType<typeof setTimeout>) => void;
  connectionEpoch: () => number;
  deliveryGeneration: () => bigint | null;
  isConnectionCurrent: (connectionEpoch: number) => boolean;
  isDeliveryLive: () => boolean;
  protocolFailure: () => void;
  send: (cursor: ReplicaCursor) => boolean;
  setTimer: (callback: () => void, delayMs: number) => ReturnType<typeof setTimeout>;
}

/** Coalesces only live delivery progress while preserving immediate recovery/adoption ACKs. */
export class DeliveryAckCoalescer {
  readonly #clearTimer: DeliveryAckCoalescerOptions["clearTimer"];
  readonly #connectionEpoch: DeliveryAckCoalescerOptions["connectionEpoch"];
  readonly #deliveryGeneration: DeliveryAckCoalescerOptions["deliveryGeneration"];
  readonly #isConnectionCurrent: DeliveryAckCoalescerOptions["isConnectionCurrent"];
  readonly #isDeliveryLive: DeliveryAckCoalescerOptions["isDeliveryLive"];
  readonly #protocolFailure: DeliveryAckCoalescerOptions["protocolFailure"];
  readonly #send: DeliveryAckCoalescerOptions["send"];
  readonly #setTimer: DeliveryAckCoalescerOptions["setTimer"];
  #lastAcknowledgedCursor: ReplicaCursor | null = null;
  #pendingAcknowledgement: ReplicaCursor | null = null;
  #timer: ReturnType<typeof setTimeout> | null = null;

  constructor(options: DeliveryAckCoalescerOptions) {
    this.#clearTimer = options.clearTimer;
    this.#connectionEpoch = options.connectionEpoch;
    this.#deliveryGeneration = options.deliveryGeneration;
    this.#isConnectionCurrent = options.isConnectionCurrent;
    this.#isDeliveryLive = options.isDeliveryLive;
    this.#protocolFailure = options.protocolFailure;
    this.#send = options.send;
    this.#setTimer = options.setTimer;
  }

  acknowledge(cursor: ReplicaCursor): void {
    if (cursor.deliveryGeneration !== this.#deliveryGeneration()) return;
    const previous = this.#pendingAcknowledgement ?? this.#lastAcknowledgedCursor;
    if (
      previous !== null &&
      (cursor.sessionEpoch !== previous.sessionEpoch ||
        cursor.deliveryGeneration !== previous.deliveryGeneration ||
        cursor.lastEventSeq < previous.lastEventSeq ||
        cursor.nextPtyOffset < previous.nextPtyOffset)
    ) {
      this.#protocolFailure();
      return;
    }
    if (
      previous !== null &&
      cursor.lastEventSeq === previous.lastEventSeq &&
      cursor.nextPtyOffset === previous.nextPtyOffset
    ) {
      return;
    }

    this.#pendingAcknowledgement = { ...cursor };
    const last = this.#lastAcknowledgedCursor;
    if (
      last === null ||
      !this.#isDeliveryLive() ||
      cursor.lastEventSeq - last.lastEventSeq >= DELIVERY_ACK_FLUSH_EVENTS ||
      deliveryOutstandingBytes(
        { eventSeq: last.lastEventSeq, nextPtyOffset: last.nextPtyOffset },
        { eventSeq: cursor.lastEventSeq, nextPtyOffset: cursor.nextPtyOffset },
      ) >= BigInt(DELIVERY_ACK_FLUSH_BYTES)
    ) {
      this.#flush();
      return;
    }
    if (this.#timer !== null) return;
    const connectionEpoch = this.#connectionEpoch();
    const generation = cursor.deliveryGeneration;
    this.#timer = this.#setTimer(() => {
      this.#timer = null;
      if (
        !this.#isConnectionCurrent(connectionEpoch) ||
        this.#deliveryGeneration() !== generation
      ) {
        return;
      }
      this.#flush();
    }, DELIVERY_ACK_FLUSH_INTERVAL_MS);
  }

  reset(cursor: ReplicaCursor | null = null): void {
    if (this.#timer !== null) this.#clearTimer(this.#timer);
    this.#timer = null;
    this.#pendingAcknowledgement = null;
    this.#lastAcknowledgedCursor = cursor === null ? null : { ...cursor };
  }

  #flush(): void {
    if (this.#timer !== null) {
      this.#clearTimer(this.#timer);
      this.#timer = null;
    }
    const cursor = this.#pendingAcknowledgement;
    this.#pendingAcknowledgement = null;
    if (cursor === null || cursor.deliveryGeneration !== this.#deliveryGeneration()) return;
    if (this.#send(cursor)) this.#lastAcknowledgedCursor = cursor;
  }
}
