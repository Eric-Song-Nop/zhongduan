import { DELIVERY_ENVELOPE_HEADER_BYTES } from "@zhongduan/protocol";

export type RelayDeliveryLane = "live" | "recovery";

export interface RelayDeliveryRefIdentity {
  readonly recoveryId: string;
  readonly clientId: string;
  readonly connectionId: string;
  readonly streamId: number;
  readonly deliveryGeneration: string;
  readonly lane: RelayDeliveryLane;
  readonly deliveryOrdinal: string;
  readonly cumulativeEncodedBytes: string;
}

export type RelayDeliveryGenerationIdentity = Pick<
  RelayDeliveryRefIdentity,
  "recoveryId" | "clientId" | "connectionId" | "streamId" | "deliveryGeneration"
>;

export interface RelayDeliveryRingLimits {
  readonly maxPhysicalBytes: number;
  readonly maxPhysicalEntries: number;
  readonly maxReferences: number;
}

export interface RelayDeliveryRingUsage {
  readonly physicalBytes: number;
  readonly physicalEntries: number;
  readonly references: number;
}

const deliveryRefBrand: unique symbol = Symbol("RelayDeliveryRef");

export interface RelayDeliveryRef {
  readonly [deliveryRefBrand]: true;
}

export type RelayDeliveryRetainFailure = {
  readonly ok: false;
  readonly reason: "disposed" | "identity-conflict" | "physical-capacity" | "reference-capacity";
};

export type RelayLiveRetainResult =
  | { readonly ok: true; readonly refs: readonly RelayDeliveryRef[] }
  | RelayDeliveryRetainFailure;

export type RelayRecoveryRetainResult =
  | { readonly ok: true; readonly ref: RelayDeliveryRef }
  | RelayDeliveryRetainFailure;

interface PhysicalEntry {
  readonly payload: Uint8Array;
  references: number;
}

interface ReferenceEntry {
  readonly encodedBytes: number;
  readonly entryKey: symbol;
  readonly identity: RelayDeliveryRefIdentity;
  readonly identityKey: string;
}

const canonicalDecimal = /^(0|[1-9]\d*)$/;

function requirePositiveSafeInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive safe integer`);
  }
}

function validateIdentity(identity: RelayDeliveryRefIdentity): void {
  for (const [name, value] of [
    ["recoveryId", identity.recoveryId],
    ["clientId", identity.clientId],
    ["connectionId", identity.connectionId],
  ] as const) {
    if (value.length === 0) throw new RangeError(`${name} must not be empty`);
  }
  requirePositiveSafeInteger(identity.streamId, "streamId");
  for (const [name, value] of [
    ["deliveryGeneration", identity.deliveryGeneration],
    ["deliveryOrdinal", identity.deliveryOrdinal],
    ["cumulativeEncodedBytes", identity.cumulativeEncodedBytes],
  ] as const) {
    if (!canonicalDecimal.test(value)) {
      throw new RangeError(`${name} must be a canonical unsigned decimal`);
    }
  }
}

function immutableIdentity(identity: RelayDeliveryRefIdentity): RelayDeliveryRefIdentity {
  validateIdentity(identity);
  return Object.freeze({
    recoveryId: identity.recoveryId,
    clientId: identity.clientId,
    connectionId: identity.connectionId,
    streamId: identity.streamId,
    deliveryGeneration: identity.deliveryGeneration,
    lane: identity.lane,
    deliveryOrdinal: identity.deliveryOrdinal,
    cumulativeEncodedBytes: identity.cumulativeEncodedBytes,
  });
}

function identityKey(identity: RelayDeliveryRefIdentity): string {
  return JSON.stringify([
    identity.recoveryId,
    identity.clientId,
    identity.connectionId,
    identity.streamId,
    identity.deliveryGeneration,
    identity.lane,
    identity.deliveryOrdinal,
    identity.cumulativeEncodedBytes,
  ]);
}

function payloadView(payload: ArrayBuffer | Uint8Array): Uint8Array {
  const source = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
  if (source.byteLength === 0) throw new RangeError("delivery payload must not be empty");
  return source;
}

function newOpaqueRef(): RelayDeliveryRef {
  return Object.freeze({ [deliveryRefBrand]: true as const });
}

export class RelayDeliveryRing {
  readonly #limits: RelayDeliveryRingLimits;
  readonly #physical = new Map<symbol, PhysicalEntry>();
  readonly #references = new Map<RelayDeliveryRef, ReferenceEntry>();
  readonly #refsByIdentity = new Map<string, RelayDeliveryRef>();
  #physicalBytes = 0;
  #disposed = false;

  constructor(limits: RelayDeliveryRingLimits) {
    requirePositiveSafeInteger(limits.maxPhysicalBytes, "maxPhysicalBytes");
    requirePositiveSafeInteger(limits.maxPhysicalEntries, "maxPhysicalEntries");
    requirePositiveSafeInteger(limits.maxReferences, "maxReferences");
    this.#limits = Object.freeze({ ...limits });
  }

  get usage(): RelayDeliveryRingUsage {
    return {
      physicalBytes: this.#physicalBytes,
      physicalEntries: this.#physical.size,
      references: this.#references.size,
    };
  }

  retainLiveCanonical(
    payload: ArrayBuffer | Uint8Array,
    identities: readonly RelayDeliveryRefIdentity[],
    encodedBytes: number,
  ): RelayLiveRetainResult {
    if (identities.length === 0)
      throw new RangeError("live delivery requires at least one reference");
    for (const identity of identities) {
      if (identity.lane !== "live") {
        throw new RangeError("live canonical references must use the live lane");
      }
    }
    if (encodedBytes !== payload.byteLength + DELIVERY_ENVELOPE_HEADER_BYTES) {
      throw new RangeError("encodedBytes must equal the complete live delivery envelope size");
    }
    const result = this.#retain(payload, identities, encodedBytes);
    return result.ok ? { ok: true, refs: result.refs } : result;
  }

  retainRecoveryEncoded(
    payload: ArrayBuffer | Uint8Array,
    identity: RelayDeliveryRefIdentity,
  ): RelayRecoveryRetainResult {
    if (identity.lane !== "recovery") {
      throw new RangeError("exact recovery references must use the recovery lane");
    }
    const result = this.#retain(payload, [identity], payload.byteLength);
    return result.ok ? { ok: true, ref: result.refs[0]! } : result;
  }

  identity(ref: RelayDeliveryRef): RelayDeliveryRefIdentity | undefined {
    return this.#references.get(ref)?.identity;
  }

  encodedBytes(ref: RelayDeliveryRef): number | undefined {
    return this.#references.get(ref)?.encodedBytes;
  }

  payload(ref: RelayDeliveryRef): Uint8Array | undefined {
    const reference = this.#references.get(ref);
    if (reference === undefined) return undefined;
    const entry = this.#physical.get(reference.entryKey);
    return entry === undefined ? undefined : entry.payload.slice();
  }

  confirm(ref: RelayDeliveryRef): boolean {
    return this.#release(ref);
  }

  cancel(ref: RelayDeliveryRef): boolean {
    return this.#release(ref);
  }

  forgetGeneration(identity: RelayDeliveryGenerationIdentity): number {
    const forgotten: RelayDeliveryRef[] = [];
    for (const [ref, reference] of this.#references) {
      if (
        reference.identity.recoveryId === identity.recoveryId &&
        reference.identity.clientId === identity.clientId &&
        reference.identity.connectionId === identity.connectionId &&
        reference.identity.streamId === identity.streamId &&
        reference.identity.deliveryGeneration === identity.deliveryGeneration
      ) {
        forgotten.push(ref);
      }
    }
    for (const ref of forgotten) this.#release(ref);
    return forgotten.length;
  }

  dispose(): number {
    if (this.#disposed) return 0;
    this.#disposed = true;
    const refs = [...this.#references.keys()];
    for (const ref of refs) this.#release(ref);
    return refs.length;
  }

  #retain(
    payload: ArrayBuffer | Uint8Array,
    identities: readonly RelayDeliveryRefIdentity[],
    encodedBytes: number,
  ):
    | { readonly ok: true; readonly refs: readonly RelayDeliveryRef[] }
    | RelayDeliveryRetainFailure {
    if (this.#disposed) return { ok: false, reason: "disposed" };
    const retainedIdentities = identities.map(immutableIdentity);
    const requestedKeys = retainedIdentities.map(identityKey);
    const distinctKeys = new Set(requestedKeys);
    if (
      distinctKeys.size !== requestedKeys.length ||
      requestedKeys.some((key) => this.#refsByIdentity.has(key))
    ) {
      return { ok: false, reason: "identity-conflict" };
    }
    if (identities.length > this.#limits.maxReferences - this.#references.size) {
      return { ok: false, reason: "reference-capacity" };
    }
    const source = payloadView(payload);
    if (!Number.isSafeInteger(encodedBytes) || encodedBytes < source.byteLength) {
      throw new RangeError("encodedBytes must cover the retained payload");
    }
    if (
      this.#physical.size >= this.#limits.maxPhysicalEntries ||
      source.byteLength > this.#limits.maxPhysicalBytes - this.#physicalBytes
    ) {
      return { ok: false, reason: "physical-capacity" };
    }

    const owned = Uint8Array.from(source);
    const entryKey = Symbol("delivery-payload");
    const refs = retainedIdentities.map(() => newOpaqueRef());
    this.#physical.set(entryKey, { payload: owned, references: refs.length });
    this.#physicalBytes += owned.byteLength;
    for (let index = 0; index < refs.length; index += 1) {
      const ref = refs[index]!;
      const refIdentity = retainedIdentities[index]!;
      const refIdentityKey = requestedKeys[index]!;
      this.#references.set(ref, {
        encodedBytes,
        entryKey,
        identity: refIdentity,
        identityKey: refIdentityKey,
      });
      this.#refsByIdentity.set(refIdentityKey, ref);
    }
    return { ok: true, refs: Object.freeze(refs) };
  }

  #release(ref: RelayDeliveryRef): boolean {
    const reference = this.#references.get(ref);
    if (reference === undefined) return false;
    const entry = this.#physical.get(reference.entryKey);
    if (entry === undefined || entry.references <= 0) {
      throw new Error("delivery ring reference lost its physical payload");
    }

    this.#references.delete(ref);
    this.#refsByIdentity.delete(reference.identityKey);
    entry.references -= 1;
    if (entry.references === 0) {
      this.#physical.delete(reference.entryKey);
      this.#physicalBytes -= entry.payload.byteLength;
    }
    return true;
  }
}
