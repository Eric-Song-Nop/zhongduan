import { describe, expect, it } from "vitest";

import {
  ClientControlFrameSchema,
  HostControlFrameSchema,
  RelayToHostControlFrameSchema,
  ServerControlFrameSchema,
  decodeControlFrame,
} from "./control-frame";
import { KeyModifier } from "./keyboard";
import { DecimalU64Schema, PositiveDecimalU64Schema } from "./scalars";

describe("control frame validation", () => {
  it("accepts a warm attach cursor with decimal uint64 values", () => {
    const frame = decodeControlFrame(
      JSON.stringify({
        type: "attach",
        engineId: "ghostty:f2d5758+snapshot-v1+wterm:local",
        deliveryGeneration: "3",
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
      ClientControlFrameSchema.parse({
        type: "attach",
        engineId: "engine",
        hasLiveReplica: false,
      }),
    ).toThrow();
    expect(() =>
      decodeControlFrame(
        JSON.stringify({
          type: "attach",
          engineId: "engine",
          deliveryGeneration: "1",
          hasLiveReplica: false,
          lastEventSeq: 42,
          privilege: "host",
        }),
        ClientControlFrameSchema,
      ),
    ).toThrow();
    expect(() =>
      ClientControlFrameSchema.parse({
        type: "attach",
        engineId: "engine",
        deliveryGeneration: "1",
        hasLiveReplica: true,
      }),
    ).toThrow();
    expect(() =>
      ClientControlFrameSchema.parse({
        type: "attach",
        engineId: "engine",
        deliveryGeneration: "1",
        hasLiveReplica: false,
        lastSessionEpoch: "7",
        lastEventSeq: "0",
        nextPtyOffset: "0",
      }),
    ).toThrow();
  });

  it("bounds decimal uint64 values before BigInt conversion", () => {
    expect(DecimalU64Schema.parse("18446744073709551615")).toBe("18446744073709551615");
    expect(() => DecimalU64Schema.parse("18446744073709551616")).toThrow();
    expect(() => DecimalU64Schema.parse("1".repeat(21))).toThrow();
    expect(PositiveDecimalU64Schema.parse("1")).toBe("1");
    expect(() => PositiveDecimalU64Schema.parse("0")).toThrow();
  });

  it("uses the Ghostty modifier bit layout on the wire", () => {
    expect(KeyModifier).toEqual({
      Shift: 1,
      Control: 2,
      Alt: 4,
      Super: 8,
      CapsLock: 16,
      NumLock: 32,
    });
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
      action: "press",
      altGraph: false,
      composing: false,
      consumedModifiers: 0,
      unshiftedCodepoint: 0x0d,
    };

    expect(ClientControlFrameSchema.parse(input)).toMatchObject({
      inputEpoch: "input_AAAAAAAAAAAA",
      clientInputSeq: "42",
    });
    const { inputEpoch: _inputEpoch, ...missingEpoch } = input;
    expect(() => ClientControlFrameSchema.parse(missingEpoch)).toThrow();
    expect(() => ClientControlFrameSchema.parse({ ...input, writerFence: "1" })).toThrow();
  });

  it("models writer lease renewal separately from semantic input", () => {
    expect(
      ClientControlFrameSchema.parse({
        type: "writer-lease-renew",
        writerLease: "lease_AAAAAAAAAAAA",
      }),
    ).toEqual({ type: "writer-lease-renew", writerLease: "lease_AAAAAAAAAAAA" });
    expect(
      ServerControlFrameSchema.parse({
        type: "writer-lease-status",
        active: true,
        expiresAt: 1_800_000_000_000,
      }),
    ).toMatchObject({ type: "writer-lease-status", active: true });
    expect(ServerControlFrameSchema.parse({ type: "writer-lease-status", active: false })).toEqual({
      type: "writer-lease-status",
      active: false,
    });
    expect(() =>
      ServerControlFrameSchema.parse({ type: "writer-lease-status", active: true }),
    ).toThrow();
    expect(() =>
      ServerControlFrameSchema.parse({
        type: "writer-lease-status",
        active: false,
        expiresAt: 1_800_000_000_000,
      }),
    ).toThrow();
  });

  it("preserves international keyboard metadata and rejects surrogate code points", () => {
    const input = {
      type: "key" as const,
      writerLease: "lease_AAAAAAAAAAAA",
      inputEpoch: "input_AAAAAAAAAAAA",
      clientInputSeq: "43",
      observedEventSeq: "19",
      code: "KeyE",
      key: "€",
      text: "€",
      modifiers: 0,
      action: "press" as const,
      altGraph: true,
      composing: false,
      consumedModifiers: 0,
      unshiftedCodepoint: 0x65,
    };

    expect(ClientControlFrameSchema.parse(input)).toEqual(input);
    expect(() =>
      ClientControlFrameSchema.parse({ ...input, unshiftedCodepoint: 0xd800 }),
    ).toThrow();
    expect(() =>
      ClientControlFrameSchema.parse({ ...input, consumedModifiers: KeyModifier.Shift }),
    ).toThrow();
  });

  it("measures paste limits in UTF-8 bytes", () => {
    expect(() =>
      ClientControlFrameSchema.parse({
        type: "paste",
        writerLease: "lease_AAAAAAAAAAAA",
        inputEpoch: "input_AAAAAAAAAAAA",
        clientInputSeq: "44",
        data: "中".repeat(350_000),
      }),
    ).toThrow();
  });

  it("validates text and focus as writer-fenced semantic input", () => {
    const identity = {
      writerLease: "lease_AAAAAAAAAAAA",
      inputEpoch: "input_AAAAAAAAAAAA",
      clientInputSeq: "45",
    };
    expect(
      ClientControlFrameSchema.parse({ type: "text", ...identity, data: "中文输入" }),
    ).toMatchObject({ type: "text", data: "中文输入" });
    expect(
      ClientControlFrameSchema.parse({ type: "focus", ...identity, focused: true }),
    ).toMatchObject({ type: "focus", focused: true });
    expect(() =>
      ClientControlFrameSchema.parse({
        type: "text",
        ...identity,
        data: "中".repeat(350_000),
      }),
    ).toThrow();
    expect(() =>
      ClientControlFrameSchema.parse({ type: "focus", ...identity, focused: true, mode: 1004 }),
    ).toThrow();
  });

  it("carries bounded mouse surface input without client-derived cells", () => {
    const identity = {
      writerLease: "lease_AAAAAAAAAAAA",
      inputEpoch: "input_AAAAAAAAAAAA",
      clientInputSeq: "46",
    };
    const press = {
      type: "mouse" as const,
      ...identity,
      action: "press" as const,
      button: 0,
      buttons: 1,
      modifiers: KeyModifier.Control,
      altGraph: false,
      surface: { x: 120, y: 80 },
    };
    expect(ClientControlFrameSchema.parse(press)).toEqual(press);
    expect(
      ClientControlFrameSchema.parse({
        ...press,
        action: "wheel",
        button: null,
        buttons: 0,
        deltaY: -120.5,
        deltaMode: "pixel",
      }),
    ).toMatchObject({ action: "wheel", deltaY: -120.5 });
    expect(() => ClientControlFrameSchema.parse({ ...press, cell: { col: 4, row: 2 } })).toThrow();
    expect(() =>
      ClientControlFrameSchema.parse({
        ...press,
        surface: { ...press.surface, width: 640 },
      }),
    ).toThrow();
    expect(() => ClientControlFrameSchema.parse({ ...press, button: 5 })).toThrow();
    expect(() => ClientControlFrameSchema.parse({ ...press, buttons: 32 })).toThrow();
    expect(() => ClientControlFrameSchema.parse({ ...press, modifiers: 0x40 })).toThrow();
    expect(() =>
      ClientControlFrameSchema.parse({
        ...press,
        action: "move",
        button: 0,
      }),
    ).toThrow();
    expect(() =>
      ClientControlFrameSchema.parse({
        ...press,
        action: "move",
        button: null,
        deltaX: 1,
      }),
    ).toThrow();
    expect(() =>
      ClientControlFrameSchema.parse({
        ...press,
        action: "wheel",
        button: null,
        buttons: 0,
        deltaX: 0,
        deltaY: 0,
        deltaMode: "line",
      }),
    ).toThrow();
    expect(() =>
      ClientControlFrameSchema.parse({
        ...press,
        action: "wheel",
        button: null,
        buttons: 0,
        deltaY: Number.POSITIVE_INFINITY,
        deltaMode: "pixel",
      }),
    ).toThrow();
  });

  it("requires Relay-injected identity on every forwarded semantic input", () => {
    const verified = {
      connectionId: "connection_AAAAAAAAA",
      clientId: "client_AAAAAAAAAAAAA",
      writerLease: "lease_AAAAAAAAAAAA",
      inputEpoch: "input_AAAAAAAAAAAA",
      clientInputSeq: "47",
      writerFence: "9",
    };
    for (const frame of [
      { type: "text", ...verified, data: "a" },
      { type: "focus", ...verified, focused: false },
      {
        type: "mouse",
        ...verified,
        action: "move",
        button: null,
        buttons: 1,
        modifiers: 0,
        altGraph: false,
        surface: { x: 1, y: 2 },
      },
    ]) {
      expect(RelayToHostControlFrameSchema.parse(frame)).toMatchObject({
        connectionId: verified.connectionId,
        clientId: verified.clientId,
        writerFence: verified.writerFence,
      });
      const { clientId: _clientId, ...unverified } = frame;
      expect(() => RelayToHostControlFrameSchema.parse(unverified)).toThrow();
      const { writerFence: _writerFence, ...unfenced } = frame;
      expect(() => RelayToHostControlFrameSchema.parse(unfenced)).toThrow();
      expect(() =>
        RelayToHostControlFrameSchema.parse({
          ...frame,
          writerFence: "18446744073709551616",
        }),
      ).toThrow();
    }
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

  it("validates a generation-scoped slow-client reset notification", () => {
    expect(
      RelayToHostControlFrameSchema.parse({
        type: "delivery-reset",
        connectionId: "connection_AAAAAAAAA",
        streamId: 42,
        deliveryGeneration: "9",
        reason: "slow-client",
      }),
    ).toEqual({
      type: "delivery-reset",
      connectionId: "connection_AAAAAAAAA",
      streamId: 42,
      deliveryGeneration: "9",
      reason: "slow-client",
    });
  });

  it("validates the Host data-channel readiness barrier", () => {
    expect(
      RelayToHostControlFrameSchema.parse({
        type: "host-ready-ack",
        sessionEpoch: "7",
        headEventSeq: "19",
        nextPtyOffset: "4221",
      }),
    ).toEqual({
      type: "host-ready-ack",
      sessionEpoch: "7",
      headEventSeq: "19",
      nextPtyOffset: "4221",
    });
  });

  it("validates the generic data-channel delivery barrier result", () => {
    const acknowledgement = {
      type: "delivery-barrier-result",
      status: "ready",
      mode: "snapshot",
      connectionId: "connection_AAAAAAAAA",
      snapshotId: "snapshot_AAAAAAAAAAA",
      streamId: 42,
      deliveryGeneration: "9",
      commitEventSeq: "19",
      commitPtyOffset: "4221",
    } as const;
    expect(RelayToHostControlFrameSchema.parse(acknowledgement)).toEqual(acknowledgement);
    const { snapshotId: _snapshotId, ...commonAcknowledgement } = acknowledgement;
    expect(
      RelayToHostControlFrameSchema.parse({
        ...commonAcknowledgement,
        mode: "warm",
        status: "stale",
      }),
    ).toMatchObject({ mode: "warm", status: "stale" });
    expect(
      RelayToHostControlFrameSchema.parse({
        ...commonAcknowledgement,
        mode: "warm",
        status: "stale",
        reason: "generation-fenced",
      }),
    ).toMatchObject({ reason: "generation-fenced" });
    const validRejected = [
      {
        ...commonAcknowledgement,
        mode: "warm",
        status: "rejected",
        reason: "missing-live-seed",
        retryScope: "same-generation",
      },
      {
        ...commonAcknowledgement,
        mode: "warm",
        status: "rejected",
        reason: "browser-control-send-failed",
        retryScope: "drop-client",
      },
      {
        ...acknowledgement,
        status: "rejected",
        reason: "snapshot-missing",
        retryScope: "refresh-checkpoint",
      },
      {
        ...acknowledgement,
        status: "rejected",
        reason: "snapshot-metadata-mismatch",
        retryScope: "refresh-checkpoint",
      },
      {
        ...acknowledgement,
        status: "rejected",
        reason: "browser-control-send-failed",
        retryScope: "drop-client",
      },
    ];
    for (const rejected of validRejected) {
      expect(RelayToHostControlFrameSchema.parse(rejected)).toEqual(rejected);
    }
    expect(() =>
      RelayToHostControlFrameSchema.parse({
        ...acknowledgement,
        status: "rejected",
        reason: "snapshot-missing",
      }),
    ).toThrow();
    expect(() =>
      RelayToHostControlFrameSchema.parse({
        ...acknowledgement,
        status: "rejected",
        reason: "snapshot-missing",
        retryScope: "drop-client",
      }),
    ).toThrow();
    for (const rejected of [
      {
        ...commonAcknowledgement,
        mode: "warm",
        status: "rejected",
        reason: "snapshot-missing",
        retryScope: "refresh-checkpoint",
      },
      {
        ...acknowledgement,
        status: "rejected",
        reason: "missing-live-seed",
        retryScope: "same-generation",
      },
      {
        ...acknowledgement,
        status: "rejected",
        reason: "cloud-head-behind-cut",
        retryScope: "same-generation",
      },
      {
        ...acknowledgement,
        status: "rejected",
        reason: "snapshot-missing",
        retryScope: "reset-generation",
      },
    ]) {
      expect(() => RelayToHostControlFrameSchema.parse(rejected)).toThrow();
    }
    expect(() =>
      RelayToHostControlFrameSchema.parse({
        ...acknowledgement,
        mode: "warm",
      }),
    ).toThrow();
    expect(() =>
      RelayToHostControlFrameSchema.parse({
        ...acknowledgement,
        mode: "snapshot",
        snapshotId: undefined,
      }),
    ).toThrow();
    expect(() =>
      HostControlFrameSchema.parse({ ...acknowledgement, type: "snapshot-offer" }),
    ).toThrow();
  });

  it("echoes a warm attach baseline and pinned commit in replay-start", () => {
    const replay = {
      type: "replay-start",
      sessionEpoch: "7",
      streamId: 42,
      deliveryGeneration: "9",
      baseEventSeq: "11",
      basePtyOffset: "42",
      commitEventSeq: "19",
      commitPtyOffset: "72",
    } as const;
    expect(ServerControlFrameSchema.parse(replay)).toEqual(replay);
    expect(() => ServerControlFrameSchema.parse({ ...replay, commitEventSeq: "10" })).toThrow();
  });

  it("accepts a replacement data ticket on resync", () => {
    expect(
      ServerControlFrameSchema.parse({
        type: "resync-required",
        deliveryGeneration: "9",
        reason: "slow-client",
        dataTicket: "ticket_AAAAAAAAAAAAAA",
        expiresAt: 1_800_000_000_000,
      }),
    ).toMatchObject({
      deliveryGeneration: "9",
      dataTicket: "ticket_AAAAAAAAAAAAAA",
    });
    expect(() =>
      ServerControlFrameSchema.parse({
        type: "resync-required",
        deliveryGeneration: "9",
        reason: "data-disconnected",
        dataTicket: "ticket_AAAAAAAAAAAAAA",
      }),
    ).toThrow();
  });

  it("validates an immutable HTTP snapshot manifest", () => {
    const frame = decodeControlFrame(
      JSON.stringify({
        type: "snapshot-manifest",
        snapshotId: "snapshot_AAAAAAAAAAA",
        engineId: "ghostty:f2d5758+wterm:local",
        sessionEpoch: "7",
        streamId: 42,
        deliveryGeneration: "4",
        cutEventSeq: "441",
        nextPtyOffset: "9012",
        commitEventSeq: "450",
        commitPtyOffset: "9200",
        compression: "zstd",
        compressedLength: "260000",
        uncompressedLength: "13000000",
        sha256: "a".repeat(64),
        downloadPath: "/api/v1/sessions/session_AAAAAAAAAAAA/snapshots/snapshot_AAAAAAAAAAA",
        restoreThrough: "finish",
      }),
      ServerControlFrameSchema,
    );

    expect(frame.type).toBe("snapshot-manifest");
    expect(() =>
      ServerControlFrameSchema.parse({
        ...frame,
        compressedLength: String(32 * 1024 * 1024 + 1),
      }),
    ).toThrow();
    expect(() =>
      ServerControlFrameSchema.parse({
        ...frame,
        compression: "none",
        compressedLength: "10",
        uncompressedLength: "11",
      }),
    ).toThrow();
    expect(() => ServerControlFrameSchema.parse({ ...frame, commitEventSeq: "440" })).toThrow();
  });
});
