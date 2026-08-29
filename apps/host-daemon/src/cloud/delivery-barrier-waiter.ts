import {
  DataFrameFlag,
  DataFrameKind,
  encodeDataFrame,
  encodeDeliveryBarrierPayload,
  type RelayToHostControlFrame,
} from "@zhongduan/protocol";

export type DeliveryBarrierResult = Extract<
  RelayToHostControlFrame,
  { type: "delivery-barrier-result" }
>;

interface BarrierIdentityBase {
  readonly commitEventSeq: string;
  readonly commitPtyOffset: string;
  readonly connectionId: string;
  readonly deliveryGeneration: string;
  readonly streamId: number;
}

export type BarrierIdentity =
  | (BarrierIdentityBase & {
      readonly mode: "warm";
      readonly snapshotId?: never;
    })
  | (BarrierIdentityBase & {
      readonly mode: "snapshot";
      readonly snapshotId: string;
    });

interface PendingBarrier {
  readonly expected: BarrierIdentity;
  readonly onAbort: () => void;
  readonly reject: (error: unknown) => void;
  readonly resolve: (result: DeliveryBarrierResult) => void;
  readonly signal: AbortSignal;
}

interface DeliveryBarrierWaiterOptions {
  sendData: (frame: Uint8Array) => void;
  sessionEpoch: () => bigint;
}

export function isMatchingDeliveryBarrierResult(
  expected: BarrierIdentity,
  result: DeliveryBarrierResult,
): boolean {
  return (
    result.connectionId === expected.connectionId &&
    result.streamId === expected.streamId &&
    result.deliveryGeneration === expected.deliveryGeneration &&
    result.commitEventSeq === expected.commitEventSeq &&
    result.commitPtyOffset === expected.commitPtyOffset &&
    result.mode === expected.mode &&
    (expected.mode === "warm" ||
      (result.mode === "snapshot" && result.snapshotId === expected.snapshotId))
  );
}

export class DeliveryBarrierWaiter {
  readonly #sendData: (frame: Uint8Array) => void;
  readonly #sessionEpoch: () => bigint;

  #disposedReason: unknown;
  #pending: PendingBarrier | undefined;

  constructor(options: DeliveryBarrierWaiterOptions) {
    this.#sendData = options.sendData;
    this.#sessionEpoch = options.sessionEpoch;
  }

  wait(
    identity: BarrierIdentity,
    signal: AbortSignal,
    onMarkerSent: () => void,
  ): Promise<DeliveryBarrierResult> {
    if (this.#disposedReason !== undefined) throw this.#disposedReason;
    if (this.#pending !== undefined) throw new Error("a delivery barrier is already pending");
    signal.throwIfAborted();

    const expected = { ...identity } as BarrierIdentity;
    const encoded = encodeDataFrame({
      kind: DataFrameKind.DeliveryBarrier,
      flags: DataFrameFlag.None,
      sessionEpoch: this.#sessionEpoch(),
      deliveryGeneration: BigInt(expected.deliveryGeneration),
      eventSeq: BigInt(expected.commitEventSeq),
      ptyOffset: BigInt(expected.commitPtyOffset),
      streamId: expected.streamId,
      payload: encodeDeliveryBarrierPayload(
        expected.mode === "warm"
          ? { mode: "warm", connectionId: expected.connectionId }
          : {
              mode: "snapshot",
              connectionId: expected.connectionId,
              snapshotId: expected.snapshotId,
            },
      ),
    });

    return new Promise((resolve, reject) => {
      const onAbort = () => this.#rejectPending(pending, signal.reason);
      const pending: PendingBarrier = {
        expected,
        onAbort,
        reject,
        resolve,
        signal,
      };
      this.#pending = pending;
      signal.addEventListener("abort", onAbort, { once: true });
      if (signal.aborted) {
        onAbort();
        return;
      }

      try {
        onMarkerSent();
        signal.throwIfAborted();
        this.#sendData(encoded);
      } catch (error) {
        this.#rejectPending(pending, error);
      }
    });
  }

  handle(result: DeliveryBarrierResult): void {
    const pending = this.#pending;
    if (pending === undefined || !isMatchingDeliveryBarrierResult(pending.expected, result)) return;
    this.#pending = undefined;
    pending.signal.removeEventListener("abort", pending.onAbort);
    pending.resolve(result);
  }

  dispose(reason: unknown = new Error("delivery barrier waiter stopped")): void {
    if (this.#disposedReason !== undefined) return;
    this.#disposedReason = typeof reason === "string" ? new Error(reason) : reason;
    const pending = this.#pending;
    if (pending !== undefined) this.#rejectPending(pending, this.#disposedReason);
  }

  #rejectPending(pending: PendingBarrier, error: unknown): void {
    if (this.#pending !== pending) return;
    this.#pending = undefined;
    pending.signal.removeEventListener("abort", pending.onAbort);
    pending.reject(error);
  }
}
