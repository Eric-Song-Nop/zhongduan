import { describe, expect, it } from "vitest";
import {
  RelayV3DeliveryRing,
  type RelayV3DeliveryRefIdentity,
} from "../src/worker/relay-v3-delivery-ring";

function identity(
  suffix: string,
  lane: "live" | "recovery",
  overrides: Partial<RelayV3DeliveryRefIdentity> = {},
): RelayV3DeliveryRefIdentity {
  return {
    recoveryId: `recovery-${suffix}`,
    clientId: `client-${suffix}`,
    connectionId: `connection-${suffix}`,
    streamId: Number(suffix.replace(/\D/g, "")) + 1,
    deliveryGeneration: "7",
    lane,
    deliveryOrdinal: "1",
    cumulativeEncodedBytes: "3",
    ...overrides,
  };
}

describe("RelayV3DeliveryRing", () => {
  it("owns one immutable live payload behind independent exact references", () => {
    const ring = new RelayV3DeliveryRing({
      maxPhysicalBytes: 3,
      maxPhysicalEntries: 1,
      maxReferences: 3,
    });
    const source = new Uint8Array([1, 2, 3]);
    const identities = [identity("1", "live"), identity("2", "live"), identity("3", "live")];

    const retained = ring.retainLiveCanonical(source, identities, 43);
    expect(retained).toMatchObject({ ok: true });
    if (!retained.ok) throw new Error("live payload was not retained");
    expect(ring.usage).toEqual({ physicalBytes: 3, physicalEntries: 1, references: 3 });

    source.fill(9);
    expect([...ring.payload(retained.refs[0]!)!]).toEqual([1, 2, 3]);
    const callerCopy = ring.payload(retained.refs[0]!)!;
    callerCopy.fill(8);
    expect([...ring.payload(retained.refs[1]!)!]).toEqual([1, 2, 3]);
    expect(ring.identity(retained.refs[2]!)).toEqual(identities[2]);
    expect(ring.encodedBytes(retained.refs[2]!)).toBe(43);

    expect(ring.confirm(retained.refs[0]!)).toBe(true);
    expect(ring.confirm(retained.refs[0]!)).toBe(false);
    expect(ring.cancel(retained.refs[1]!)).toBe(true);
    expect(
      ring.forgetGeneration({
        recoveryId: identities[2]!.recoveryId,
        clientId: identities[2]!.clientId,
        connectionId: identities[2]!.connectionId,
        streamId: identities[2]!.streamId,
        deliveryGeneration: identities[2]!.deliveryGeneration,
      }),
    ).toBe(1);
    expect(ring.usage).toEqual({ physicalBytes: 0, physicalEntries: 0, references: 0 });
  });

  it("owns one exact recovery encoding and releases it only once", () => {
    const ring = new RelayV3DeliveryRing({
      maxPhysicalBytes: 8,
      maxPhysicalEntries: 2,
      maxReferences: 2,
    });
    const source = new Uint8Array([4, 5, 6, 7]);
    const retained = ring.retainRecoveryEncoded(source, identity("8", "recovery"));
    expect(retained).toMatchObject({ ok: true });
    if (!retained.ok) throw new Error("recovery payload was not retained");

    source.fill(0);
    expect([...ring.payload(retained.ref)!]).toEqual([4, 5, 6, 7]);
    expect(ring.cancel(retained.ref)).toBe(true);
    expect(ring.cancel(retained.ref)).toBe(false);
    expect(ring.confirm(retained.ref)).toBe(false);
    expect(ring.dispose()).toBe(0);
    expect(ring.dispose()).toBe(0);
    expect(ring.usage).toEqual({ physicalBytes: 0, physicalEntries: 0, references: 0 });
  });

  it("charges a live canonical reference for the complete v3 wire envelope", () => {
    const ring = new RelayV3DeliveryRing({
      maxPhysicalBytes: 3,
      maxPhysicalEntries: 1,
      maxReferences: 1,
    });
    const exactIdentity = identity("9", "live");

    expect(() => ring.retainLiveCanonical(new Uint8Array([1, 2, 3]), [exactIdentity], 3)).toThrow(
      "encodedBytes must equal the complete live delivery envelope size",
    );
    expect(ring.usage).toEqual({ physicalBytes: 0, physicalEntries: 0, references: 0 });

    const retained = ring.retainLiveCanonical(new Uint8Array([1, 2, 3]), [exactIdentity], 43);
    expect(retained.ok).toBe(true);
  });

  it("rejects physical cap plus one atomically", () => {
    const bytesRing = new RelayV3DeliveryRing({
      maxPhysicalBytes: 3,
      maxPhysicalEntries: 2,
      maxReferences: 2,
    });
    const first = bytesRing.retainRecoveryEncoded(
      new Uint8Array([1, 2, 3]),
      identity("10", "recovery"),
    );
    expect(first.ok).toBe(true);
    expect(
      bytesRing.retainRecoveryEncoded(new Uint8Array([4]), identity("11", "recovery")),
    ).toEqual({ ok: false, reason: "physical-capacity" });
    expect(bytesRing.usage).toEqual({ physicalBytes: 3, physicalEntries: 1, references: 1 });

    const entryRing = new RelayV3DeliveryRing({
      maxPhysicalBytes: 2,
      maxPhysicalEntries: 1,
      maxReferences: 2,
    });
    expect(
      entryRing.retainRecoveryEncoded(new Uint8Array([1]), identity("12", "recovery")).ok,
    ).toBe(true);
    expect(
      entryRing.retainRecoveryEncoded(new Uint8Array([2]), identity("13", "recovery")),
    ).toEqual({ ok: false, reason: "physical-capacity" });
    expect(entryRing.usage).toEqual({ physicalBytes: 1, physicalEntries: 1, references: 1 });
  });

  it("rejects reference cap plus one and duplicate exact identities atomically", () => {
    const ring = new RelayV3DeliveryRing({
      maxPhysicalBytes: 8,
      maxPhysicalEntries: 2,
      maxReferences: 2,
    });
    const firstIdentity = identity("20", "live");
    const secondIdentity = identity("21", "live");
    const retained = ring.retainLiveCanonical(
      new Uint8Array([1]),
      [firstIdentity, secondIdentity],
      41,
    );
    expect(retained.ok).toBe(true);

    expect(ring.retainLiveCanonical(new Uint8Array([2]), [identity("22", "live")], 41)).toEqual({
      ok: false,
      reason: "reference-capacity",
    });
    expect(ring.retainLiveCanonical(new Uint8Array([2]), [firstIdentity], 41)).toEqual({
      ok: false,
      reason: "identity-conflict",
    });
    expect(ring.usage).toEqual({ physicalBytes: 1, physicalEntries: 1, references: 2 });
  });

  it("forgets only an exact generation identity and dispose releases the rest once", () => {
    const ring = new RelayV3DeliveryRing({
      maxPhysicalBytes: 8,
      maxPhysicalEntries: 4,
      maxReferences: 4,
    });
    const exact = identity("30", "recovery", { deliveryGeneration: "2" });
    const otherConnection = identity("31", "recovery", {
      recoveryId: exact.recoveryId,
      clientId: exact.clientId,
      connectionId: "other-connection",
      streamId: exact.streamId,
      deliveryGeneration: exact.deliveryGeneration,
    });
    const exactRef = ring.retainRecoveryEncoded(new Uint8Array([1]), exact);
    const otherRef = ring.retainRecoveryEncoded(new Uint8Array([2]), otherConnection);
    if (!exactRef.ok || !otherRef.ok) throw new Error("test setup exceeded ring limits");

    expect(
      ring.forgetGeneration({
        recoveryId: exact.recoveryId,
        clientId: exact.clientId,
        connectionId: exact.connectionId,
        streamId: exact.streamId,
        deliveryGeneration: exact.deliveryGeneration,
      }),
    ).toBe(1);
    expect(ring.payload(exactRef.ref)).toBeUndefined();
    expect([...ring.payload(otherRef.ref)!]).toEqual([2]);
    expect(ring.dispose()).toBe(1);
    expect(ring.dispose()).toBe(0);
    expect(ring.cancel(otherRef.ref)).toBe(false);
  });
});
