import { describe, expect, it } from "vitest";

import {
  ClientControlFrameSchema,
  ServerControlFrameSchema,
  decodeControlFrame,
} from "./control-frame";

describe("control frame validation", () => {
  it("accepts a warm attach cursor with decimal uint64 values", () => {
    const frame = decodeControlFrame(
      JSON.stringify({
        type: "attach",
        engineId: "ghostty:f2d5758+snapshot-v1+wterm:local",
        hasLiveReplica: true,
        lastSessionEpoch: "7",
        lastEventSeq: "19",
        nextPtyOffset: "4221",
      }),
      ClientControlFrameSchema,
    );

    expect(frame.type).toBe("attach");
  });

  it("rejects unknown fields and unsafe uint64 encodings", () => {
    expect(() =>
      decodeControlFrame(
        JSON.stringify({
          type: "attach",
          engineId: "engine",
          hasLiveReplica: false,
          lastEventSeq: 42,
          privilege: "host",
        }),
        ClientControlFrameSchema,
      ),
    ).toThrow();
  });

  it("requires a reconnect-stable input epoch for every semantic input", () => {
    const input = {
      type: "key",
      writerLease: "lease_AAAAAAAAAAAA",
      inputEpoch: "input_AAAAAAAAAAAA",
      clientInputSeq: "42",
      observedEventSeq: "19",
      code: "Enter",
      key: "Enter",
      modifiers: 0,
      repeat: false,
    };

    expect(ClientControlFrameSchema.parse(input)).toMatchObject({
      inputEpoch: "input_AAAAAAAAAAAA",
      clientInputSeq: "42",
    });
    const { inputEpoch: _inputEpoch, ...missingEpoch } = input;
    expect(() => ClientControlFrameSchema.parse(missingEpoch)).toThrow();
  });

  it("echoes the input epoch in acknowledgements", () => {
    expect(
      ServerControlFrameSchema.parse({
        type: "input-ack",
        inputEpoch: "input_AAAAAAAAAAAA",
        clientInputSeq: "42",
        status: "written",
        authorityEventSeq: "19",
      }),
    ).toMatchObject({ inputEpoch: "input_AAAAAAAAAAAA", clientInputSeq: "42" });
  });

  it("validates an immutable HTTP snapshot manifest", () => {
    const frame = decodeControlFrame(
      JSON.stringify({
        type: "snapshot-manifest",
        snapshotId: "snap-88",
        engineId: "ghostty:f2d5758+wterm:local",
        sessionEpoch: "7",
        deliveryGeneration: "4",
        cutEventSeq: "441",
        nextPtyOffset: "9012",
        commitEventSeq: "450",
        commitPtyOffset: "9200",
        compression: "zstd",
        compressedLength: "260000",
        uncompressedLength: "13000000",
        sha256: "a".repeat(64),
        downloadPath: "/api/v1/sessions/s-1/snapshots/snap-88",
        restoreThrough: "finish",
      }),
      ServerControlFrameSchema,
    );

    expect(frame.type).toBe("snapshot-manifest");
  });
});
