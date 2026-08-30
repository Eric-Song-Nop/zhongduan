import {
  DATA_HEADER_BYTES,
  DataFrameFlag,
  DataFrameKind,
  decodeDataFrame,
  encodeDataFrame,
  encodeResizePayload,
} from "@zhongduan/protocol";
import { describe, expect, it, vi } from "vitest";

import { EventJournal } from "./journal";

function outputFrame(
  eventSeq: bigint,
  ptyOffset: bigint,
  payload: number | Uint8Array,
): Uint8Array {
  return encodeDataFrame({
    kind: DataFrameKind.PtyOutput,
    flags: DataFrameFlag.None,
    sessionEpoch: 1n,
    deliveryGeneration: 0n,
    eventSeq,
    ptyOffset,
    streamId: 0,
    payload: typeof payload === "number" ? Uint8Array.of(payload) : payload,
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
    payload: encodeResizePayload({ cols: 120, rows: 40, widthPx: 960, heightPx: 800 }),
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
    expect(journal.readFrom({ lastEventSeq: 0n, nextPtyOffset: 0n })).toEqual({
      status: "gap",
    });

    const replay = journal.readFrom({ lastEventSeq: 1n, nextPtyOffset: 1n });
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

    expect(journal.readFrom({ lastEventSeq: 1n, nextPtyOffset: 999n })).toEqual({
      status: "gap",
    });
    expect(journal.readFrom({ lastEventSeq: 1n, nextPtyOffset: 1n })).toMatchObject({
      status: "ok",
    });
  });

  it("plans exact encoded bytes and frames without materializing the retained range", () => {
    const journal = new EventJournal();
    const first = outputFrame(1n, 0n, Uint8Array.of(0x41, 0x42, 0x43));
    const resize = resizeFrame(2n, 3n);
    const last = outputFrame(3n, 3n, Uint8Array.of(0x44, 0x45, 0x46, 0x47, 0x48));
    journal.append(first);
    journal.append(resize);
    journal.append(last);
    const slice = vi.spyOn(Uint8Array.prototype, "slice");

    const plan = journal.planRangeThrough(
      { lastEventSeq: 0n, nextPtyOffset: 0n },
      { lastEventSeq: 3n, nextPtyOffset: 8n },
    );

    expect(plan).toMatchObject({
      status: "ok",
      exactEncodedBytes: first.byteLength + resize.byteLength + last.byteLength,
      exactFrames: 3,
    });
    expect(first.byteLength + resize.byteLength + last.byteLength).toBe(
      DATA_HEADER_BYTES * 3 + 3 + 16 + 5,
    );
    expect(slice).not.toHaveBeenCalled();
    expect(plan.status === "ok" ? plan.materialize() : plan).toMatchObject({
      status: "ok",
      frames: [first, resize, last],
    });
    expect(slice).toHaveBeenCalledTimes(3);
    slice.mockRestore();

    expect(
      journal.planRangeThrough(
        { lastEventSeq: 1n, nextPtyOffset: 3n },
        { lastEventSeq: 2n, nextPtyOffset: 3n },
      ),
    ).toMatchObject({ status: "ok", exactEncodedBytes: resize.byteLength, exactFrames: 1 });
    expect(
      journal.planRangeThrough(
        { lastEventSeq: 3n, nextPtyOffset: 8n },
        { lastEventSeq: 3n, nextPtyOffset: 8n },
      ),
    ).toMatchObject({ status: "ok", exactEncodedBytes: 0, exactFrames: 0 });
  });

  it("fails a planned range closed when the retained journal revision changes", () => {
    const journal = new EventJournal();
    journal.append(outputFrame(1n, 0n, 0x41));
    const plan = journal.planRangeThrough(
      { lastEventSeq: 0n, nextPtyOffset: 0n },
      { lastEventSeq: 1n, nextPtyOffset: 1n },
    );
    expect(plan.status).toBe("ok");

    journal.append(outputFrame(2n, 1n, 0x42));

    expect(plan.status === "ok" ? plan.materialize() : plan).toEqual({ status: "gap" });
  });
});
