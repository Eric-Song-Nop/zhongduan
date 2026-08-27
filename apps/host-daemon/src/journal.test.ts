import {
  DataFrameFlag,
  DataFrameKind,
  decodeDataFrame,
  encodeDataFrame,
} from "@zhongduan/protocol";
import { describe, expect, it } from "vitest";

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
});
