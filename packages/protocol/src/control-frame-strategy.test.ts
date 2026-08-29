import { describe, expect, expectTypeOf, it } from "vitest";

import {
  ClientControlFrameSchema,
  ClientControlFrameV3Schema,
  HostControlFrameSchema,
  HostControlFrameV3Schema,
  RelayToHostControlFrameSchema,
  RelayToHostControlFrameV3Schema,
  ServerControlFrameSchema,
  ServerControlFrameV3Schema,
  decodeClientControlFrame,
  decodeHostControlFrame,
  decodeRelayToHostControlFrame,
  decodeServerControlFrame,
  type ClientControlFrame,
  type ClientControlFrameV3,
  type HostControlFrame,
  type HostControlFrameV3,
  type RelayToHostControlFrame,
  type RelayToHostControlFrameV3,
  type ServerControlFrame,
  type ServerControlFrameV3,
} from "./control-frame";

const routing = {
  recoveryId: "recovery_AAAAAAAAAAA",
  connectionId: "connection_AAAAAAAAA",
  streamId: 42,
  deliveryGeneration: "3",
} as const;

const browserToCloud = {
  type: "delivery-received",
  deliveryGeneration: "3",
  lane: "recovery",
  contiguousDeliveryOrdinal: "4",
  cumulativeEncodedBytes: "400",
} as const;

const cloudToBrowser = {
  type: "recovery-source-closed",
  recoveryId: routing.recoveryId,
  deliveryGeneration: "3",
  throughRecoveryOrdinal: "4",
  throughRecoveryCumulativeEncodedBytes: "400",
} as const;

const hostToCloud = {
  type: "recovery-prepare-rejected",
  ...routing,
  reason: "journal-gap",
} as const;

const cloudToHost = {
  type: "recovery-source-grant",
  ...routing,
  cumulativeGrantedEncodedBytes: "8192",
} as const;

const clientV2Ack = {
  type: "ack",
  sessionEpoch: "7",
  deliveryGeneration: "3",
  eventSeq: "12",
  nextPtyOffset: "120",
} as const;

const serverV2ReplayStart = {
  type: "replay-start",
  sessionEpoch: "7",
  streamId: 42,
  deliveryGeneration: "3",
  baseEventSeq: "10",
  basePtyOffset: "100",
  commitEventSeq: "12",
  commitPtyOffset: "120",
} as const;

const serverV2SnapshotManifest = {
  type: "snapshot-manifest",
  snapshotId: "snapshot_AAAAAAAAAAA",
  engineId: "engine",
  sessionEpoch: "7",
  streamId: 42,
  deliveryGeneration: "3",
  cutEventSeq: "10",
  nextPtyOffset: "100",
  commitEventSeq: "12",
  commitPtyOffset: "120",
  compression: "zstd",
  compressedLength: "90",
  uncompressedLength: "120",
  sha256: "a".repeat(64),
  downloadPath: "/api/v1/sessions/session_AAAAAAAAA/snapshots/snapshot_AAAAAAAAAAA",
  restoreThrough: "finish",
} as const;

const relayV2Attach = {
  type: "attach-request",
  connectionId: routing.connectionId,
  streamId: 42,
  deliveryGeneration: "3",
  engineId: "engine",
  hasLiveReplica: false,
} as const;

const relayV2BarrierResult = {
  type: "delivery-barrier-result",
  status: "ready",
  mode: "warm",
  connectionId: routing.connectionId,
  streamId: 42,
  deliveryGeneration: "3",
  commitEventSeq: "12",
  commitPtyOffset: "120",
} as const;

const relayV2DeliveryReset = {
  type: "delivery-reset",
  connectionId: routing.connectionId,
  streamId: 42,
  deliveryGeneration: "4",
  reason: "slow-client",
} as const;

const hostV2ReplayUnavailable = {
  type: "replay-unavailable",
  connectionId: routing.connectionId,
  reason: "journal-gap",
} as const;

const v3DirectionCases = [
  {
    frame: browserToCloud,
    v2Schema: ClientControlFrameSchema,
    v3Schema: ClientControlFrameV3Schema,
    decode: decodeClientControlFrame,
  },
  {
    frame: cloudToBrowser,
    v2Schema: ServerControlFrameSchema,
    v3Schema: ServerControlFrameV3Schema,
    decode: decodeServerControlFrame,
  },
  {
    frame: hostToCloud,
    v2Schema: HostControlFrameSchema,
    v3Schema: HostControlFrameV3Schema,
    decode: decodeHostControlFrame,
  },
  {
    frame: cloudToHost,
    v2Schema: RelayToHostControlFrameSchema,
    v3Schema: RelayToHostControlFrameV3Schema,
    decode: decodeRelayToHostControlFrame,
  },
] as const;

describe("strategy-scoped production control frames", () => {
  it("keeps default decoder types on v2 and narrows explicit v3 calls", () => {
    expectTypeOf(
      decodeClientControlFrame(JSON.stringify(clientV2Ack)),
    ).toEqualTypeOf<ClientControlFrame>();
    expectTypeOf(
      decodeClientControlFrame(JSON.stringify(browserToCloud), "v3"),
    ).toEqualTypeOf<ClientControlFrameV3>();
    expectTypeOf(
      decodeServerControlFrame(JSON.stringify(serverV2ReplayStart)),
    ).toEqualTypeOf<ServerControlFrame>();
    expectTypeOf(
      decodeServerControlFrame(JSON.stringify(cloudToBrowser), "v3"),
    ).toEqualTypeOf<ServerControlFrameV3>();
    expectTypeOf(
      decodeHostControlFrame(JSON.stringify(hostV2ReplayUnavailable)),
    ).toEqualTypeOf<HostControlFrame>();
    expectTypeOf(
      decodeHostControlFrame(JSON.stringify(hostToCloud), "v3"),
    ).toEqualTypeOf<HostControlFrameV3>();
    expectTypeOf(
      decodeRelayToHostControlFrame(JSON.stringify(relayV2Attach)),
    ).toEqualTypeOf<RelayToHostControlFrame>();
    expectTypeOf(
      decodeRelayToHostControlFrame(JSON.stringify(cloudToHost), "v3"),
    ).toEqualTypeOf<RelayToHostControlFrameV3>();
  });

  it("keeps Recovery v3 frames outside every default and explicit-v2 decoder", () => {
    for (const { decode, frame, v2Schema } of v3DirectionCases) {
      const encoded = JSON.stringify(frame);
      expect(v2Schema.safeParse(frame).success).toBe(false);
      expect(() => decode(encoded)).toThrow();
      expect(() => decode(encoded, "v2")).toThrow();
    }
  });

  it("admits each Recovery v3 frame only through its explicit strategy union", () => {
    for (const { decode, frame, v3Schema } of v3DirectionCases) {
      expect(v3Schema.parse(frame)).toEqual(frame);
      expect(decode(JSON.stringify(frame), "v3")).toEqual(frame);
    }

    expect(() => decodeHostControlFrame(JSON.stringify(cloudToHost), "v3")).toThrow();
    expect(() => decodeRelayToHostControlFrame(JSON.stringify(hostToCloud), "v3")).toThrow();
    expect(() => decodeClientControlFrame(JSON.stringify(cloudToBrowser), "v3")).toThrow();
    expect(() => decodeServerControlFrame(JSON.stringify(browserToCloud), "v3")).toThrow();
  });

  it("keeps ordinary production frames valid under both strategies", () => {
    const clientAttach = {
      type: "attach",
      engineId: "engine",
      deliveryGeneration: "3",
      hasLiveReplica: false,
    } as const;
    const serverOffline = { type: "host-offline" } as const;
    const hostReady = {
      type: "host-ready",
      engineId: "engine",
      sessionEpoch: "7",
      headEventSeq: "12",
      nextPtyOffset: "120",
    } as const;
    const relayReady = {
      type: "host-ready-ack",
      sessionEpoch: "7",
      headEventSeq: "12",
      nextPtyOffset: "120",
    } as const;

    expect(decodeClientControlFrame(JSON.stringify(clientAttach))).toEqual(clientAttach);
    expect(decodeClientControlFrame(JSON.stringify(clientAttach), "v3")).toEqual(clientAttach);
    expect(decodeServerControlFrame(JSON.stringify(serverOffline))).toEqual(serverOffline);
    expect(decodeServerControlFrame(JSON.stringify(serverOffline), "v3")).toEqual(serverOffline);
    expect(decodeHostControlFrame(JSON.stringify(hostReady))).toEqual(hostReady);
    expect(decodeHostControlFrame(JSON.stringify(hostReady), "v3")).toEqual(hostReady);
    expect(decodeRelayToHostControlFrame(JSON.stringify(relayReady))).toEqual(relayReady);
    expect(decodeRelayToHostControlFrame(JSON.stringify(relayReady), "v3")).toEqual(relayReady);
  });

  it("excludes v2 delivery orchestration from the v3 strategy unions", () => {
    expect(ClientControlFrameSchema.parse(clientV2Ack)).toEqual(clientV2Ack);
    expect(() => decodeClientControlFrame(JSON.stringify(clientV2Ack), "v3")).toThrow();

    for (const frame of [serverV2ReplayStart, serverV2SnapshotManifest]) {
      expect(ServerControlFrameSchema.parse(frame)).toEqual(frame);
      expect(() => decodeServerControlFrame(JSON.stringify(frame), "v3")).toThrow();
    }

    for (const frame of [relayV2Attach, relayV2BarrierResult, relayV2DeliveryReset]) {
      expect(RelayToHostControlFrameSchema.parse(frame)).toEqual(frame);
      expect(() => decodeRelayToHostControlFrame(JSON.stringify(frame), "v3")).toThrow();
    }
  });

  it("retains strategy-independent input, status, and Host authority controls", () => {
    const clientInput = {
      type: "text",
      writerLease: "lease_AAAAAAAAAAAA",
      inputEpoch: "input_AAAAAAAAAAAA",
      clientInputSeq: "42",
      data: "input",
    } as const;
    const serverResync = {
      type: "resync-required",
      deliveryGeneration: "4",
      reason: "epoch-changed",
    } as const;
    const forwardedInput = {
      ...clientInput,
      connectionId: routing.connectionId,
      clientId: "client_AAAAAAAAAAAAA",
      writerFence: "9",
    } as const;

    expect(decodeClientControlFrame(JSON.stringify(clientInput), "v3")).toEqual(clientInput);
    expect(decodeServerControlFrame(JSON.stringify(serverResync), "v3")).toEqual(serverResync);
    expect(decodeHostControlFrame(JSON.stringify(hostV2ReplayUnavailable), "v3")).toEqual(
      hostV2ReplayUnavailable,
    );
    expect(decodeRelayToHostControlFrame(JSON.stringify(forwardedInput), "v3")).toEqual(
      forwardedInput,
    );
  });

  it("rejects unknown strategies and preserves strict v3 frame shapes", () => {
    expect(() =>
      decodeClientControlFrame(JSON.stringify(browserToCloud), "future" as "v3"),
    ).toThrow();
    expect(() =>
      decodeClientControlFrame(JSON.stringify({ ...browserToCloud, unknown: true }), "v3"),
    ).toThrow();
  });
});
