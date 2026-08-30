import { describe, expect, expectTypeOf, it } from "vitest";

import {
  ClientControlFrameSchema,
  ClientInputFrameSchema,
  HostControlFrameSchema,
  InputAcknowledgementFrameSchema,
  RelayToHostControlFrameSchema,
  ServerControlFrameSchema,
  decodeClientControlFrame,
  decodeHostControlFrame,
  decodeRelayToHostControlFrame,
  decodeServerControlFrame,
  type ClientControlFrame,
  type ClientInputFrame,
  type HostControlFrame,
  type RelayToHostControlFrame,
  type ServerControlFrame,
} from "./control-frame";

const attach = {
  type: "attach",
  engineId: "engine",
  deliveryGeneration: "3",
  hasLiveReplica: true,
  lastSessionEpoch: "7",
  lastEventSeq: "10",
  nextPtyOffset: "100",
} as const;

const key = {
  type: "key",
  writerLease: "lease",
  inputEpoch: "input-epoch",
  clientInputSeq: "1",
  observedEventSeq: "10",
  code: "KeyA",
  key: "a",
  text: "a",
  modifiers: 0,
  action: "press",
  altGraph: false,
  composing: false,
  consumedModifiers: 0,
} as const;

const welcome = {
  type: "welcome",
  connectionId: "connection",
  streamId: 42,
  writerLease: "lease",
  engineId: "engine",
  sessionEpoch: "7",
  deliveryGeneration: "3",
  headEventSeq: "10",
  nextPtyOffset: "100",
} as const;

const inputAcknowledgement = {
  type: "input-ack",
  inputEpoch: "input-epoch",
  clientInputSeq: "1",
  status: "written",
  authorityEventSeq: "11",
} as const;

const recoveryStart = {
  type: "recovery-start",
  recoveryId: "recovery_AAAAAAAAAAA",
  deliveryGeneration: "3",
  streamId: 42,
  engineId: "engine",
  authorityDataFormat: 1,
  base: { sessionEpoch: "7", eventSeq: "10", nextPtyOffset: "100" },
  source: { kind: "warm" },
  committedThrough: { sessionEpoch: "7", eventSeq: "12", nextPtyOffset: "120" },
  liveFloor: { sessionEpoch: "7", nextEventSeq: "13", nextPtyOffset: "120" },
} as const;

const recoveryProgress = {
  type: "delivery-received",
  deliveryGeneration: "3",
  lane: "live",
  contiguousDeliveryOrdinal: "1",
  cumulativeEncodedBytes: "100",
} as const;

const hostRecoveryClosed = {
  type: "recovery-source-closed",
  recoveryId: "recovery_AAAAAAAAAAA",
  connectionId: "connection_AAAAAAAAA",
  streamId: 42,
  deliveryGeneration: "3",
  throughRecoveryOrdinal: "2",
  throughRecoveryCumulativeEncodedBytes: "200",
} as const;

const relayRecoveryPrepare = {
  type: "recovery-prepare",
  recoveryId: "recovery_AAAAAAAAAAA",
  connectionId: "connection_AAAAAAAAA",
  streamId: 42,
  deliveryGeneration: "3",
  engineId: "engine",
  base: { sessionEpoch: "7", eventSeq: "10", nextPtyOffset: "100" },
  source: { kind: "warm" },
} as const;

describe("control frame contracts", () => {
  it("decodes the canonical frame union in each direction", () => {
    expect(decodeClientControlFrame(JSON.stringify(attach))).toEqual(attach);
    expect(decodeClientControlFrame(JSON.stringify(recoveryProgress))).toEqual(recoveryProgress);
    expect(decodeServerControlFrame(JSON.stringify(welcome))).toEqual(welcome);
    expect(decodeServerControlFrame(JSON.stringify(recoveryStart))).toEqual(recoveryStart);
    expect(decodeHostControlFrame(JSON.stringify(hostRecoveryClosed))).toEqual(hostRecoveryClosed);
    expect(decodeRelayToHostControlFrame(JSON.stringify(relayRecoveryPrepare))).toEqual(
      relayRecoveryPrepare,
    );

    expectTypeOf(decodeClientControlFrame).returns.toEqualTypeOf<ClientControlFrame>();
    expectTypeOf(decodeServerControlFrame).returns.toEqualTypeOf<ServerControlFrame>();
    expectTypeOf(decodeHostControlFrame).returns.toEqualTypeOf<HostControlFrame>();
    expectTypeOf(decodeRelayToHostControlFrame).returns.toEqualTypeOf<RelayToHostControlFrame>();
  });

  it("exports semantic input independently from delivery progress", () => {
    expect(ClientInputFrameSchema.parse(key)).toEqual(key);
    expectTypeOf(ClientInputFrameSchema.parse(key)).toEqualTypeOf<ClientInputFrame>();
    expect(ClientInputFrameSchema.safeParse(recoveryProgress).success).toBe(false);
    expect(ClientInputFrameSchema.safeParse(attach).success).toBe(false);
  });

  it("keeps input acknowledgements independent from Host routing", () => {
    expect(InputAcknowledgementFrameSchema.parse(inputAcknowledgement)).toEqual(
      inputAcknowledgement,
    );
    expect(
      HostControlFrameSchema.parse({ ...inputAcknowledgement, connectionId: "connection" }),
    ).toBeDefined();
    expect(
      InputAcknowledgementFrameSchema.safeParse({ ...inputAcknowledgement, connectionId: "x" })
        .success,
    ).toBe(false);
  });

  it("preserves input, lease, welcome, status, and host-ready frames", () => {
    expect(ClientControlFrameSchema.parse(key)).toEqual(key);
    expect(
      ClientControlFrameSchema.parse({ type: "writer-lease-renew", writerLease: "lease" }),
    ).toBeDefined();
    expect(ServerControlFrameSchema.parse(welcome)).toEqual(welcome);
    expect(ServerControlFrameSchema.parse(inputAcknowledgement)).toEqual(inputAcknowledgement);
    expect(
      HostControlFrameSchema.parse({
        type: "host-ready",
        engineId: "engine",
        sessionEpoch: "7",
        headEventSeq: "10",
        nextPtyOffset: "100",
      }),
    ).toBeDefined();
    expect(
      RelayToHostControlFrameSchema.parse({
        type: "host-ready-ack",
        sessionEpoch: "7",
        headEventSeq: "10",
        nextPtyOffset: "100",
      }),
    ).toBeDefined();
  });

  it("rejects unknown fields", () => {
    expect(() => ClientControlFrameSchema.parse({ ...key, unknown: true })).toThrow();
    expect(() => ServerControlFrameSchema.parse({ ...welcome, unknown: true })).toThrow();
  });

  it("rejects malformed JSON and frames in the wrong direction", () => {
    expect(() => decodeClientControlFrame("not json")).toThrow();
    expect(() => decodeClientControlFrame(JSON.stringify(welcome))).toThrow();
    expect(() => decodeServerControlFrame(JSON.stringify(recoveryProgress))).toThrow();
    expect(() => decodeHostControlFrame(JSON.stringify(relayRecoveryPrepare))).toThrow();
    expect(() => decodeRelayToHostControlFrame(JSON.stringify(hostRecoveryClosed))).toThrow();
  });
});
