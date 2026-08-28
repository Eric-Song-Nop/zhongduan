import {
  DataFrameFlag,
  DataFrameKind,
  decodeDataFrame,
  encodeDataFrame,
  encodeResizePayload,
} from "@zhongduan/protocol";
import { describe, expect, it, vi } from "vitest";

import { EventJournal } from "./journal";

function outputFrame(eventSeq: bigint, ptyOffset: bigint, byte: number): Uint8Array {
  return encodeDataFrame({
    kind: DataFrameKind.PtyOutput,
    flags: DataFrameFlag.None,
    sessionEpoch: 1n,
    deliveryGeneration: 0n,
    eventSeq,
    ptyOffset,
    streamId: 0,
    payload: Uint8Array.of(byte),
  });
}

function resizeFrame(eventSeq: bigint, ptyOffset: bigint): Uint8Array {
  return encodeDataFrame({
    kind: DataFrameKind.ResizeApplied,
    flags: DataFrameFlag.None,
    sessionEpoch: 1n,
    deliveryGeneration: 0n,
    eventSeq,
    ptyOffset,
    streamId: 0,
    payload: encodeResizePayload({ cols: 80, rows: 24, widthPx: 0, heightPx: 0 }),
  });
}

describe("EventJournal", () => {
  it("evicts sealed segments by byte budget and reports an honest replay gap", () => {
    const journal = new EventJournal({
      maxBytes: 100,
      maxAgeMs: Number.POSITIVE_INFINITY,
      segmentBytes: 1,
    });
    journal.append(outputFrame(1n, 0n, 0x41));
    journal.append(outputFrame(2n, 1n, 0x42));
    journal.append(outputFrame(3n, 2n, 0x43));

    expect(journal.entries().map((frame) => decodeDataFrame(frame).eventSeq)).toEqual([2n, 3n]);
    expect(journal.replayFrom({ lastEventSeq: 0n, nextPtyOffset: 0n })).toEqual({
      status: "gap",
    });

    const replay = journal.replayFrom({ lastEventSeq: 1n, nextPtyOffset: 1n });
    expect(replay.status).toBe("ok");
    if (replay.status === "ok") {
      expect(replay.frames.map((frame) => decodeDataFrame(frame).eventSeq)).toEqual([2n, 3n]);
    }
  });

  it("expires old segments by age without allowing a mismatched byte cursor", () => {
    let now = 0;
    const journal = new EventJournal({
      maxAgeMs: 10,
      maxBytes: Number.POSITIVE_INFINITY,
      now: () => now,
      segmentBytes: 1,
    });
    journal.append(outputFrame(1n, 0n, 0x41));
    now = 11;
    journal.append(outputFrame(2n, 1n, 0x42));

    expect(journal.replayFrom({ lastEventSeq: 1n, nextPtyOffset: 999n })).toEqual({
      status: "gap",
    });
    expect(journal.replayFrom({ lastEventSeq: 1n, nextPtyOffset: 1n })).toMatchObject({
      status: "ok",
    });
  });

  it("replays only through the exact pinned commit", () => {
    const journal = new EventJournal();
    journal.append(outputFrame(1n, 0n, 0x41));
    journal.append(outputFrame(2n, 1n, 0x42));
    journal.append(outputFrame(3n, 2n, 0x43));

    const replay = journal.replayThrough(
      { lastEventSeq: 0n, nextPtyOffset: 0n },
      { lastEventSeq: 2n, nextPtyOffset: 2n },
    );
    expect(replay.status).toBe("ok");
    if (replay.status === "ok") {
      expect(replay.frames.map((frame) => decodeDataFrame(frame).eventSeq)).toEqual([1n, 2n]);
    }
    expect(
      journal.replayThrough(
        { lastEventSeq: 0n, nextPtyOffset: 0n },
        { lastEventSeq: 2n, nextPtyOffset: 99n },
      ),
    ).toEqual({ status: "gap" });
  });

  it("measures an exact retained range without materializing replay frames", () => {
    let now = 0;
    const journal = new EventJournal({ now: () => now, segmentBytes: 1 });
    const first = outputFrame(1n, 0n, 0x41);
    const second = outputFrame(2n, 1n, 0x42);
    journal.append(first);
    now = 5;
    journal.append(second);
    now = 8;

    expect(
      journal.measureRange(
        { lastEventSeq: 0n, nextPtyOffset: 0n },
        { lastEventSeq: 2n, nextPtyOffset: 2n },
      ),
    ).toEqual({
      status: "exact",
      deliveryCreditBytes: 130,
      encodedBytes: first.byteLength + second.byteLength,
      frames: 2,
      oldestMutationAgeMs: 8,
    });
    expect(
      journal.measureRange(
        { lastEventSeq: 1n, nextPtyOffset: 1n },
        { lastEventSeq: 2n, nextPtyOffset: 2n },
      ),
    ).toEqual({
      status: "exact",
      deliveryCreditBytes: 65,
      encodedBytes: second.byteLength,
      frames: 1,
      oldestMutationAgeMs: 3,
    });
    expect(
      journal.measureRange(
        { lastEventSeq: 2n, nextPtyOffset: 2n },
        { lastEventSeq: 2n, nextPtyOffset: 2n },
      ),
    ).toEqual({
      status: "exact",
      deliveryCreditBytes: 0,
      encodedBytes: 0,
      frames: 0,
      oldestMutationAgeMs: 0,
    });
  });

  it("measures without copying frames and binds replay to the same retained view", () => {
    let nowCalls = 0;
    const journal = new EventJournal({
      maxAgeMs: 10,
      maxBytes: Number.POSITIVE_INFINITY,
      monotonicNow: () => (nowCalls++ === 0 ? 0 : 11),
      segmentBytes: 1,
    });
    const first = outputFrame(1n, 0n, 0x41);
    journal.append(first);
    nowCalls = 0;
    const slice = vi.spyOn(Uint8Array.prototype, "slice");

    const measured = journal.measureRange(
      { lastEventSeq: 0n, nextPtyOffset: 0n },
      { lastEventSeq: 1n, nextPtyOffset: 1n },
    );
    expect(measured).toMatchObject({ status: "exact", encodedBytes: first.byteLength });
    expect(slice).not.toHaveBeenCalled();

    nowCalls = 0;
    const atomic = journal.replayAndMeasureThrough(
      { lastEventSeq: 0n, nextPtyOffset: 0n },
      { lastEventSeq: 1n, nextPtyOffset: 1n },
    );
    expect(atomic.measurement.status).toBe("exact");
    expect(atomic.replay.status).toBe("ok");
    expect(nowCalls).toBe(1);
    slice.mockRestore();
  });

  it("classifies range gaps without copying retained frames", () => {
    const journal = new EventJournal();
    journal.append(outputFrame(1n, 0n, 0x41));

    expect(
      journal.measureRange(
        { lastEventSeq: 1n, nextPtyOffset: 1n },
        { lastEventSeq: 0n, nextPtyOffset: 0n },
      ),
    ).toEqual({ status: "gap", reason: "reversed" });
    expect(
      journal.measureRange(
        { lastEventSeq: 1n, nextPtyOffset: 1n },
        { lastEventSeq: 2n, nextPtyOffset: 2n },
      ),
    ).toEqual({ status: "gap", reason: "head-ahead" });
    expect(
      journal.measureRange(
        { lastEventSeq: 0n, nextPtyOffset: 9n },
        { lastEventSeq: 1n, nextPtyOffset: 1n },
      ),
    ).toEqual({ status: "gap", reason: "base-cursor-mismatch" });
    expect(
      journal.measureRange(
        { lastEventSeq: 0n, nextPtyOffset: 0n },
        { lastEventSeq: 1n, nextPtyOffset: 9n },
      ),
    ).toEqual({ status: "gap", reason: "head-cursor-mismatch" });
  });

  it("measures PTY and resize mutations across segments with delivery-credit semantics", () => {
    let now = 0;
    const journal = new EventJournal({ monotonicNow: () => now, segmentBytes: 1 });
    const first = outputFrame(1n, 0n, 0x41);
    const resize = resizeFrame(2n, 1n);
    const third = outputFrame(3n, 1n, 0x42);
    journal.append(first);
    now = 2;
    journal.append(resize);
    now = 3;
    journal.append(third);
    now = 7;

    expect(
      journal.measureRange(
        { lastEventSeq: 1n, nextPtyOffset: 1n },
        { lastEventSeq: 3n, nextPtyOffset: 2n },
      ),
    ).toEqual({
      status: "exact",
      deliveryCreditBytes: 129,
      encodedBytes: resize.byteLength + third.byteLength,
      frames: 2,
      oldestMutationAgeMs: 5,
    });
  });

  it("reports a measured base evicted by retention", () => {
    const journal = new EventJournal({
      maxBytes: 100,
      maxAgeMs: Number.POSITIVE_INFINITY,
      segmentBytes: 1,
    });
    journal.append(outputFrame(1n, 0n, 0x41));
    journal.append(outputFrame(2n, 1n, 0x42));
    journal.append(outputFrame(3n, 2n, 0x43));

    expect(
      journal.measureRange(
        { lastEventSeq: 0n, nextPtyOffset: 0n },
        { lastEventSeq: 3n, nextPtyOffset: 3n },
      ),
    ).toEqual({ status: "gap", reason: "base-evicted" });
    expect(
      journal.measureRange(
        { lastEventSeq: 1n, nextPtyOffset: 1n },
        { lastEventSeq: 3n, nextPtyOffset: 3n },
      ),
    ).toMatchObject({ status: "exact", frames: 2 });
  });
});
