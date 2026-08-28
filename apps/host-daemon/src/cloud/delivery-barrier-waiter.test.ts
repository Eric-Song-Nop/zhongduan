import { DataFrameKind, decodeDataFrame, decodeDeliveryBarrierPayload } from "@zhongduan/protocol";
import { describe, expect, it, vi } from "vitest";

import {
  DeliveryBarrierWaiter,
  isMatchingDeliveryBarrierResult,
  type BarrierIdentity,
  type DeliveryBarrierResult,
} from "./delivery-barrier-waiter";

const warmIdentity: BarrierIdentity = {
  mode: "warm",
  connectionId: "connection_AAAAA",
  streamId: 7,
  deliveryGeneration: "11",
  commitEventSeq: "13",
  commitPtyOffset: "17",
};

const snapshotIdentity: BarrierIdentity = {
  mode: "snapshot",
  connectionId: "connection_AAAAA",
  streamId: 7,
  deliveryGeneration: "11",
  commitEventSeq: "13",
  commitPtyOffset: "17",
  snapshotId: "snapshot_AAAAAAAA",
};

function resultFor(
  identity: BarrierIdentity,
  overrides: Partial<DeliveryBarrierResult> = {},
): DeliveryBarrierResult {
  const base = {
    type: "delivery-barrier-result" as const,
    status: "ready" as const,
    connectionId: identity.connectionId,
    streamId: identity.streamId,
    deliveryGeneration: identity.deliveryGeneration,
    commitEventSeq: identity.commitEventSeq,
    commitPtyOffset: identity.commitPtyOffset,
  };
  return identity.mode === "warm"
    ? ({ ...base, mode: "warm", ...overrides } as DeliveryBarrierResult)
    : ({
        ...base,
        mode: "snapshot",
        snapshotId: identity.snapshotId,
        ...overrides,
      } as DeliveryBarrierResult);
}

describe("DeliveryBarrierWaiter", () => {
  it("registers before sending a warm barrier and accepts a synchronous matching result", async () => {
    const order: string[] = [];
    let waiter: DeliveryBarrierWaiter;
    const sendData = vi.fn((encoded: Uint8Array) => {
      order.push("send");
      waiter.handle(resultFor(warmIdentity));
      const frame = decodeDataFrame(encoded);
      expect(frame).toMatchObject({
        kind: DataFrameKind.DeliveryBarrier,
        sessionEpoch: 5n,
        deliveryGeneration: 11n,
        eventSeq: 13n,
        ptyOffset: 17n,
        streamId: 7,
      });
      expect(decodeDeliveryBarrierPayload(frame.payload)).toEqual({
        mode: "warm",
        connectionId: "connection_AAAAA",
      });
    });
    waiter = new DeliveryBarrierWaiter({ sessionEpoch: () => 5n, sendData });

    const waiting = waiter.wait(warmIdentity, new AbortController().signal, () => {
      order.push("marker");
    });

    await expect(waiting).resolves.toEqual(resultFor(warmIdentity));
    expect(order).toEqual(["marker", "send"]);
    expect(sendData).toHaveBeenCalledOnce();
  });

  it("encodes snapshot identity and ignores every non-matching result", async () => {
    const sent: Uint8Array[] = [];
    const waiter = new DeliveryBarrierWaiter({
      sessionEpoch: () => 23n,
      sendData: (encoded) => sent.push(encoded),
    });
    const waiting = waiter.wait(snapshotIdentity, new AbortController().signal, () => {});

    const mismatches: DeliveryBarrierResult[] = [
      resultFor(snapshotIdentity, { connectionId: "connection_BBBBB" }),
      resultFor(snapshotIdentity, { streamId: 8 }),
      resultFor(snapshotIdentity, { deliveryGeneration: "12" }),
      resultFor(snapshotIdentity, { commitEventSeq: "14" }),
      resultFor(snapshotIdentity, { commitPtyOffset: "18" }),
      resultFor(snapshotIdentity, { snapshotId: "snapshot_BBBBBBBB" }),
      resultFor(warmIdentity),
    ];
    for (const mismatch of mismatches) waiter.handle(mismatch);

    expect(() => waiter.wait(warmIdentity, new AbortController().signal, () => {})).toThrow(
      "a delivery barrier is already pending",
    );
    const frame = decodeDataFrame(sent[0]!);
    expect(frame.sessionEpoch).toBe(23n);
    expect(decodeDeliveryBarrierPayload(frame.payload)).toEqual({
      mode: "snapshot",
      connectionId: "connection_AAAAA",
      snapshotId: "snapshot_AAAAAAAA",
    });

    const matching = resultFor(snapshotIdentity, { status: "stale" });
    waiter.handle(matching);
    await expect(waiting).resolves.toEqual(matching);
  });

  it("cleans the slot after abort and can wait again", async () => {
    const abort = new AbortController();
    const sendData = vi.fn();
    const waiter = new DeliveryBarrierWaiter({ sessionEpoch: () => 1n, sendData });
    const waiting = waiter.wait(warmIdentity, abort.signal, () => {});
    const reason = new Error("delivery reset");

    abort.abort(reason);

    await expect(waiting).rejects.toBe(reason);
    const retry = waiter.wait(warmIdentity, new AbortController().signal, () => {});
    waiter.handle(resultFor(warmIdentity));
    await expect(retry).resolves.toEqual(resultFor(warmIdentity));
    expect(sendData).toHaveBeenCalledTimes(2);
  });

  it("does not mark or send when the signal is already aborted", () => {
    const abort = new AbortController();
    const reason = new Error("already stopped");
    abort.abort(reason);
    const onMarkerSent = vi.fn();
    const sendData = vi.fn();
    const waiter = new DeliveryBarrierWaiter({ sessionEpoch: () => 1n, sendData });

    expect(() => waiter.wait(warmIdentity, abort.signal, onMarkerSent)).toThrow(reason);
    expect(onMarkerSent).not.toHaveBeenCalled();
    expect(sendData).not.toHaveBeenCalled();
  });

  it("cleans the slot when marking or sending throws", async () => {
    const sendError = new Error("send failed");
    const sendData = vi.fn<(encoded: Uint8Array) => void>(() => {
      throw sendError;
    });
    const waiter = new DeliveryBarrierWaiter({ sessionEpoch: () => 1n, sendData });

    await expect(waiter.wait(warmIdentity, new AbortController().signal, () => {})).rejects.toBe(
      sendError,
    );

    const markerError = new Error("marker transition failed");
    await expect(
      waiter.wait(warmIdentity, new AbortController().signal, () => {
        throw markerError;
      }),
    ).rejects.toBe(markerError);
    expect(sendData).toHaveBeenCalledOnce();

    sendData.mockImplementation(() => undefined);
    const retry = waiter.wait(warmIdentity, new AbortController().signal, () => {});
    waiter.handle(resultFor(warmIdentity));
    await expect(retry).resolves.toEqual(resultFor(warmIdentity));
  });

  it("does not send when marking aborts the signal", async () => {
    const abort = new AbortController();
    const reason = new Error("recovery superseded");
    const sendData = vi.fn();
    const waiter = new DeliveryBarrierWaiter({ sessionEpoch: () => 1n, sendData });

    const waiting = waiter.wait(warmIdentity, abort.signal, () => abort.abort(reason));

    await expect(waiting).rejects.toBe(reason);
    expect(sendData).not.toHaveBeenCalled();
  });

  it("dispose rejects the active wait and permanently rejects later waits", async () => {
    const waiter = new DeliveryBarrierWaiter({ sessionEpoch: () => 1n, sendData: () => {} });
    const waiting = waiter.wait(warmIdentity, new AbortController().signal, () => {});

    waiter.dispose("scheduler stopped");

    await expect(waiting).rejects.toThrow("scheduler stopped");
    expect(() => waiter.wait(warmIdentity, new AbortController().signal, () => {})).toThrow(
      "scheduler stopped",
    );
    waiter.dispose(new Error("ignored"));
  });
});

describe("isMatchingDeliveryBarrierResult", () => {
  it("matches status independently but requires the complete delivery identity", () => {
    expect(
      isMatchingDeliveryBarrierResult(
        snapshotIdentity,
        resultFor(snapshotIdentity, { status: "rejected" }),
      ),
    ).toBe(true);
    expect(
      isMatchingDeliveryBarrierResult(
        snapshotIdentity,
        resultFor(snapshotIdentity, { snapshotId: "snapshot_BBBBBBBB" }),
      ),
    ).toBe(false);
  });
});
