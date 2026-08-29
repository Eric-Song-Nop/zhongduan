import {
  DataFrameFlag,
  DataFrameKind,
  RecoveryV3HostPrepareRejectedSchema,
  RecoveryV3HostPrepareSchema,
  RecoveryV3HostRoutingIdentitySchema,
  RecoveryV3HostSourceClosedSchema,
  RecoveryV3HostSourceGrantSchema,
  RecoveryV3HostSourceReceivedSchema,
  RecoveryV3HostSourceResetSchema,
  RecoveryV3HostStartReadySchema,
  decodeDataFrame,
  encodeDataFrame,
  encodeDeliveryEnvelopeV3,
  encodeRecoveryStartFence,
  successorBoundary,
  type AuthorityCursor,
  type RecoveryV3HostPrepare,
  type RecoveryV3HostPrepareRejected,
  type RecoveryV3HostRoutingIdentity,
  type RecoveryV3HostSourceClosed,
  type RecoveryV3HostSourceGrant,
  type RecoveryV3HostSourceReceived,
  type RecoveryV3HostSourceReset,
  type RecoveryV3HostStartReady,
} from "@zhongduan/protocol";

import type { PreparedRecoveryGap, ReplayCursor, TerminalSession } from "../session";

export interface RecoverySourceManagerLimits {
  readonly maxCanonicalBytesPerSource: number;
  readonly maxCanonicalFramesPerSource: number;
  readonly maxOwnedRecords: number;
  readonly maxOwnedWireBytes: number;
  readonly maxSources: number;
  readonly noProgressDeadlineMs: number;
  readonly recoveryDeadlineMs: number;
}

export interface RecoverySourceManagerOptions {
  readonly limits: RecoverySourceManagerLimits;
  readonly monotonicNow: () => number;
  readonly session: TerminalSession;
}

/** Opaque relay/connection lifetime identity. Object identity, not wire data, is the fence. */
export type RecoverySourceOwnerToken = object;

export type RecoverySourcePrepareResult =
  | {
      readonly status: "prepared";
      readonly committedThrough: AuthorityCursor;
    }
  | {
      readonly status: "rejected";
      readonly rejection: RecoveryV3HostPrepareRejected;
    }
  | {
      readonly status: "conflict";
      readonly reason: "divergent-retry";
    }
  | {
      readonly status: "unavailable";
      readonly reason: "deadline" | "disposed" | "fence-unavailable";
    };

export interface RecoverySourceDrainLimits {
  readonly maxRecords: number;
  readonly maxWireBytes: number;
}

export interface RecoverySourceDrainResult {
  readonly records: number;
  readonly wireBytes: number;
}

export interface RecoverySourceManagerCounters {
  readonly ownedRecords: number;
  readonly ownedWireBytes: number;
  readonly pendingSources: number;
  readonly sources: number;
}

export type EnqueueRecoveryStartFence = (encoded: Uint8Array) => boolean;

interface RetainedRecord {
  readonly bytes: Uint8Array;
  readonly cumulativeEncodedBytes: bigint;
  readonly deliveryOrdinal: bigint;
}

interface PendingPrepare {
  cancelled: boolean;
  commitFailure:
    | "capacity-exceeded"
    | "fence-unavailable"
    | "generation-fenced"
    | "journal-gap"
    | null;
  readonly ownerToken: RecoverySourceOwnerToken;
  readonly prepare: RecoveryV3HostPrepare;
  readonly promise: Promise<RecoverySourcePrepareResult>;
  readonly replacedTombstone: RecoveryGenerationTombstone | null;
  readonly resolve: (result: RecoverySourcePrepareResult) => void;
  readonly startedAtMs: number;
  readonly token: symbol;
}

interface RecoverySource {
  closed: RecoveryV3HostSourceClosed | null;
  cumulativeGrantedEncodedBytes: bigint;
  readonly committedThrough: AuthorityCursor;
  readonly finalCumulativeEncodedBytes: bigint;
  readonly finalDeliveryOrdinal: bigint;
  lastProgressAtMs: number;
  lastReceivedOrdinal: bigint;
  nextSendIndex: number;
  readonly ownerToken: RecoverySourceOwnerToken;
  readonly prepare: RecoveryV3HostPrepare;
  readonly preparePromise: Promise<RecoverySourcePrepareResult>;
  readonly prepareResult: Extract<RecoverySourcePrepareResult, { status: "prepared" }>;
  ready: RecoveryV3HostStartReady | null;
  records: readonly RetainedRecord[] | null;
  sentCumulativeEncodedBytes: bigint;
  sentDeliveryOrdinal: bigint;
  readonly startedAtMs: number;
  readonly totalRecords: number;
  readonly totalWireBytes: number;
}

interface RecoveryGenerationTombstone {
  readonly deliveryGeneration: bigint;
  readonly ownerToken: RecoverySourceOwnerToken;
}

interface Deferred<T> {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
}

/**
 * Pure, session-scoped owner for bounded Recovery v3 Host sources.
 *
 * It deliberately owns no socket, scheduling policy, or timers. Callers supply an opaque owner
 * token, enqueue the ordered start fence from the TerminalSession actor commit, drive bounded
 * granted drains, and invoke deadline checks.
 */
export class RecoverySourceManager {
  readonly #limits: RecoverySourceManagerLimits;
  readonly #monotonicNow: () => number;
  readonly #pendingByStream = new Map<number, PendingPrepare>();
  readonly #session: TerminalSession;
  readonly #sourcesByStream = new Map<number, RecoverySource>();
  readonly #staleOwners = new WeakSet<RecoverySourceOwnerToken>();
  readonly #tombstonesByStream = new Map<number, RecoveryGenerationTombstone>();

  #disposed = false;
  #lastNowMs: number;
  #ownedRecords = 0;
  #ownedWireBytes = 0;

  constructor(options: RecoverySourceManagerOptions) {
    this.#session = options.session;
    this.#monotonicNow = options.monotonicNow;
    this.#limits = Object.freeze({
      maxCanonicalBytesPerSource: nonNegativeSafeInteger(
        options.limits.maxCanonicalBytesPerSource,
        "maxCanonicalBytesPerSource",
      ),
      maxCanonicalFramesPerSource: nonNegativeSafeInteger(
        options.limits.maxCanonicalFramesPerSource,
        "maxCanonicalFramesPerSource",
      ),
      maxOwnedRecords: positiveSafeInteger(options.limits.maxOwnedRecords, "maxOwnedRecords"),
      maxOwnedWireBytes: positiveSafeInteger(options.limits.maxOwnedWireBytes, "maxOwnedWireBytes"),
      maxSources: positiveSafeInteger(options.limits.maxSources, "maxSources"),
      noProgressDeadlineMs: positiveFinite(
        options.limits.noProgressDeadlineMs,
        "noProgressDeadlineMs",
      ),
      recoveryDeadlineMs: positiveFinite(options.limits.recoveryDeadlineMs, "recoveryDeadlineMs"),
    });
    this.#lastNowMs = finiteNow(this.#monotonicNow());
  }

  get counters(): RecoverySourceManagerCounters {
    return {
      ownedRecords: this.#ownedRecords,
      ownedWireBytes: this.#ownedWireBytes,
      pendingSources: this.#pendingByStream.size,
      sources: this.#sourcesByStream.size + this.#tombstonesByStream.size,
    };
  }

  prepare(
    ownerToken: RecoverySourceOwnerToken,
    input: RecoveryV3HostPrepare,
    enqueueFence: EnqueueRecoveryStartFence,
  ): Promise<RecoverySourcePrepareResult> {
    assertOwnerToken(ownerToken);
    const prepare = RecoveryV3HostPrepareSchema.parse(input);
    const nowMs = this.#recordNow();

    if (this.#disposed) return Promise.resolve({ status: "unavailable", reason: "disposed" });
    if (this.#staleOwners.has(ownerToken)) {
      return Promise.resolve(this.#rejected(prepare, "client-gone"));
    }
    if (prepare.engineId !== this.#session.engineId) {
      return Promise.resolve(this.#rejected(prepare, "engine-mismatch"));
    }
    if (BigInt(prepare.base.sessionEpoch) !== this.#session.sessionEpoch) {
      return Promise.resolve(this.#rejected(prepare, "epoch-changed"));
    }

    const existing = this.#existingPrepare(ownerToken, prepare);
    if (existing !== null) return existing;
    const replacedTombstone = this.#tombstonesByStream.get(prepare.streamId) ?? null;
    const replacesTombstone =
      replacedTombstone !== null &&
      replacedTombstone.ownerToken === ownerToken &&
      BigInt(prepare.deliveryGeneration) > replacedTombstone.deliveryGeneration;
    if (
      this.#pendingByStream.size +
        this.#sourcesByStream.size +
        this.#tombstonesByStream.size -
        (replacesTombstone ? 1 : 0) >=
      this.#limits.maxSources
    ) {
      return Promise.resolve(this.#rejected(prepare, "capacity-exceeded"));
    }

    const deferred = createDeferred<RecoverySourcePrepareResult>();
    const pending: PendingPrepare = {
      cancelled: false,
      commitFailure: null,
      ownerToken,
      prepare,
      promise: deferred.promise,
      replacedTombstone: replacesTombstone ? replacedTombstone : null,
      resolve: deferred.resolve,
      startedAtMs: nowMs,
      token: Symbol("recovery-source-prepare"),
    };
    if (replacesTombstone) this.#tombstonesByStream.delete(prepare.streamId);
    this.#pendingByStream.set(prepare.streamId, pending);

    let sessionPrepare: ReturnType<TerminalSession["prepareRecoveryGap"]>;
    try {
      sessionPrepare = this.#session.prepareRecoveryGap(
        replayCursor(prepare.base),
        {
          maxEncodedBytes: this.#limits.maxCanonicalBytesPerSource,
          maxFrames: this.#limits.maxCanonicalFramesPerSource,
        },
        (gap) => this.#commitPreparedGap(pending, gap, enqueueFence),
      );
    } catch {
      this.#finishPending(pending, { status: "unavailable", reason: "fence-unavailable" });
      return pending.promise;
    }

    void sessionPrepare.then(
      (result) => {
        if (pending.cancelled) return;
        if (result.status === "prepared") {
          const source = this.#sourcesByStream.get(prepare.streamId);
          if (source !== undefined && source.preparePromise === pending.promise) {
            this.#finishPending(pending, source.prepareResult);
            return;
          }
          this.#finishPending(pending, this.#rejected(prepare, "generation-fenced"));
          return;
        }
        if (pending.commitFailure === "capacity-exceeded" || result.reason === "capacity") {
          this.#finishPending(pending, this.#rejected(prepare, "capacity-exceeded"));
          return;
        }
        if (pending.commitFailure === "generation-fenced") {
          this.#finishPending(pending, this.#rejected(prepare, "generation-fenced"));
          return;
        }
        if (pending.commitFailure === "journal-gap" || result.reason === "journal-gap") {
          this.#finishPending(pending, this.#rejected(prepare, "journal-gap"));
          return;
        }
        this.#finishPending(pending, { status: "unavailable", reason: "fence-unavailable" });
      },
      () => {
        if (!pending.cancelled) {
          this.#rollbackPendingSource(pending);
          this.#finishPending(pending, { status: "unavailable", reason: "fence-unavailable" });
        }
      },
    );
    return pending.promise;
  }

  startReady(ownerToken: RecoverySourceOwnerToken, input: RecoveryV3HostStartReady): boolean {
    assertOwnerToken(ownerToken);
    const ready = RecoveryV3HostStartReadySchema.parse(input);
    const nowMs = this.#recordNow();
    const source = this.#sourceFor(ownerToken, ready);
    if (source === null || source.closed !== null) return false;
    if (!sameAuthorityCursor(source.committedThrough, ready.committedThrough)) return false;
    if (source.ready !== null) return sameStartReady(source.ready, ready);

    source.ready = Object.freeze({
      ...ready,
      committedThrough: Object.freeze({ ...ready.committedThrough }),
    });
    source.cumulativeGrantedEncodedBytes = BigInt(ready.cumulativeGrantedEncodedBytes);
    source.lastProgressAtMs = nowMs;
    return true;
  }

  grant(ownerToken: RecoverySourceOwnerToken, input: RecoveryV3HostSourceGrant): boolean {
    assertOwnerToken(ownerToken);
    const grant = RecoveryV3HostSourceGrantSchema.parse(input);
    this.#recordNow();
    const source = this.#sourceFor(ownerToken, grant);
    if (source === null || source.ready === null || source.closed !== null) return false;
    const cumulative = BigInt(grant.cumulativeGrantedEncodedBytes);
    if (cumulative < source.cumulativeGrantedEncodedBytes) return false;
    source.cumulativeGrantedEncodedBytes = cumulative;
    return true;
  }

  drainGranted(
    ownerToken: RecoverySourceOwnerToken,
    identity: RecoveryV3HostRoutingIdentity,
    limits: RecoverySourceDrainLimits,
    send: (encoded: Uint8Array) => void,
  ): RecoverySourceDrainResult {
    assertOwnerToken(ownerToken);
    const routing = RecoveryV3HostRoutingIdentitySchema.parse(identity);
    const maxRecords = nonNegativeSafeInteger(limits.maxRecords, "maxRecords");
    const maxWireBytes = nonNegativeSafeInteger(limits.maxWireBytes, "maxWireBytes");
    this.#recordNow();
    const source = this.#sourceFor(ownerToken, routing);
    if (source === null || source.ready === null || source.records === null) {
      return { records: 0, wireBytes: 0 };
    }

    let sentRecords = 0;
    let sentWireBytes = 0;
    while (sentRecords < maxRecords) {
      const record = source.records[source.nextSendIndex];
      if (record === undefined) break;
      if (record.cumulativeEncodedBytes > source.cumulativeGrantedEncodedBytes) break;
      if (record.bytes.byteLength > maxWireBytes - sentWireBytes) break;

      send(record.bytes.slice());
      if (
        this.#sourcesByStream.get(source.prepare.streamId) !== source ||
        source.records === null
      ) {
        break;
      }
      source.nextSendIndex += 1;
      source.sentDeliveryOrdinal = record.deliveryOrdinal;
      source.sentCumulativeEncodedBytes = record.cumulativeEncodedBytes;
      source.lastProgressAtMs = Math.max(source.lastProgressAtMs, this.#lastNowMs);
      sentRecords += 1;
      sentWireBytes += record.bytes.byteLength;
    }
    return { records: sentRecords, wireBytes: sentWireBytes };
  }

  received(
    ownerToken: RecoverySourceOwnerToken,
    input: RecoveryV3HostSourceReceived,
  ): RecoveryV3HostSourceClosed | null {
    assertOwnerToken(ownerToken);
    const received = RecoveryV3HostSourceReceivedSchema.parse(input);
    const nowMs = this.#recordNow();
    const source = this.#sourceFor(ownerToken, received);
    if (source === null) return null;

    const deliveryOrdinal = BigInt(received.contiguousDeliveryOrdinal);
    const cumulativeEncodedBytes = BigInt(received.cumulativeEncodedBytes);
    if (source.closed !== null) {
      return deliveryOrdinal === source.finalDeliveryOrdinal &&
        cumulativeEncodedBytes === source.finalCumulativeEncodedBytes
        ? source.closed
        : null;
    }
    const records = source.records;
    if (
      records === null ||
      deliveryOrdinal > source.sentDeliveryOrdinal ||
      deliveryOrdinal <= 0n ||
      deliveryOrdinal > BigInt(records.length)
    ) {
      return null;
    }
    const record = records[Number(deliveryOrdinal - 1n)];
    if (record === undefined || record.cumulativeEncodedBytes !== cumulativeEncodedBytes) {
      return null;
    }
    if (deliveryOrdinal > source.lastReceivedOrdinal) {
      source.lastReceivedOrdinal = deliveryOrdinal;
      source.lastProgressAtMs = nowMs;
    }
    if (deliveryOrdinal !== source.finalDeliveryOrdinal) return null;

    source.closed = Object.freeze(
      RecoveryV3HostSourceClosedSchema.parse({
        type: "recovery-source-closed",
        ...routingIdentity(source.prepare),
        throughRecoveryOrdinal: source.finalDeliveryOrdinal.toString(),
        throughRecoveryCumulativeEncodedBytes: source.finalCumulativeEncodedBytes.toString(),
      }),
    );
    this.#releaseRecords(source);
    return source.closed;
  }

  reset(ownerToken: RecoverySourceOwnerToken, input: RecoveryV3HostSourceReset): boolean {
    assertOwnerToken(ownerToken);
    const reset = RecoveryV3HostSourceResetSchema.parse(input);
    this.#recordNow();
    const source = this.#sourceFor(ownerToken, reset);
    if (source === null) return false;
    this.#retireSource(source);
    return true;
  }

  resetOwner(ownerToken: RecoverySourceOwnerToken): number {
    assertOwnerToken(ownerToken);
    this.#recordNow();
    this.#staleOwners.add(ownerToken);
    let reset = 0;
    for (const pending of this.#pendingByStream.values()) {
      if (pending.ownerToken !== ownerToken) continue;
      this.#cancelPending(pending, this.#rejected(pending.prepare, "client-gone"));
      reset += 1;
    }
    for (const source of this.#sourcesByStream.values()) {
      if (source.ownerToken !== ownerToken) continue;
      this.#removeSource(source);
      reset += 1;
    }
    for (const [streamId, tombstone] of this.#tombstonesByStream) {
      if (tombstone.ownerToken !== ownerToken) continue;
      this.#tombstonesByStream.delete(streamId);
      reset += 1;
    }
    return reset;
  }

  checkDeadlines(): number {
    const nowMs = this.#recordNow();
    let expired = 0;
    for (const pending of this.#pendingByStream.values()) {
      if (nowMs - pending.startedAtMs < this.#limits.recoveryDeadlineMs) continue;
      this.#cancelPending(pending, { status: "unavailable", reason: "deadline" }, true);
      expired += 1;
    }
    for (const source of this.#sourcesByStream.values()) {
      if (
        nowMs - source.startedAtMs < this.#limits.recoveryDeadlineMs &&
        nowMs - source.lastProgressAtMs < this.#limits.noProgressDeadlineMs
      ) {
        continue;
      }
      this.#retireSource(source);
      expired += 1;
    }
    return expired;
  }

  dispose(): void {
    if (this.#disposed) return;
    this.#recordNow();
    this.#disposed = true;
    for (const pending of this.#pendingByStream.values()) {
      this.#cancelPending(pending, { status: "unavailable", reason: "disposed" });
    }
    for (const source of this.#sourcesByStream.values()) this.#removeSource(source);
    this.#tombstonesByStream.clear();
  }

  #existingPrepare(
    ownerToken: RecoverySourceOwnerToken,
    prepare: RecoveryV3HostPrepare,
  ): Promise<RecoverySourcePrepareResult> | null {
    const generation = BigInt(prepare.deliveryGeneration);
    const pending = this.#pendingByStream.get(prepare.streamId);
    if (pending !== undefined) {
      if (pending.ownerToken !== ownerToken) {
        return Promise.resolve(this.#rejected(prepare, "client-gone"));
      }
      const pendingGeneration = BigInt(pending.prepare.deliveryGeneration);
      if (generation < pendingGeneration) {
        return Promise.resolve(this.#rejected(prepare, "generation-fenced"));
      }
      if (generation === pendingGeneration) {
        return samePrepare(pending.prepare, prepare)
          ? pending.promise
          : Promise.resolve({ status: "conflict", reason: "divergent-retry" });
      }
      this.#cancelPending(pending, this.#rejected(pending.prepare, "generation-fenced"), true);
    }

    const source = this.#sourcesByStream.get(prepare.streamId);
    if (source !== undefined) {
      if (source.ownerToken !== ownerToken) {
        return Promise.resolve(this.#rejected(prepare, "client-gone"));
      }
      const sourceGeneration = BigInt(source.prepare.deliveryGeneration);
      if (generation < sourceGeneration) {
        return Promise.resolve(this.#rejected(prepare, "generation-fenced"));
      }
      if (generation === sourceGeneration) {
        return samePrepare(source.prepare, prepare)
          ? source.preparePromise
          : Promise.resolve({ status: "conflict", reason: "divergent-retry" });
      }
      this.#retireSource(source);
    }

    const tombstone = this.#tombstonesByStream.get(prepare.streamId);
    if (tombstone === undefined) return null;
    if (tombstone.ownerToken !== ownerToken) {
      return Promise.resolve(this.#rejected(prepare, "client-gone"));
    }
    if (generation <= tombstone.deliveryGeneration) {
      return Promise.resolve(this.#rejected(prepare, "generation-fenced"));
    }
    return null;
  }

  #commitPreparedGap(
    pending: PendingPrepare,
    gap: PreparedRecoveryGap,
    enqueueFence: EnqueueRecoveryStartFence,
  ): boolean {
    if (
      this.#disposed ||
      pending.cancelled ||
      this.#staleOwners.has(pending.ownerToken) ||
      this.#pendingByStream.get(pending.prepare.streamId)?.token !== pending.token
    ) {
      pending.commitFailure = "generation-fenced";
      return false;
    }

    let records: readonly RetainedRecord[];
    try {
      records = buildRetainedRecords(pending.prepare, gap);
    } catch {
      pending.commitFailure = "journal-gap";
      return false;
    }
    const totalWireBytes = records.reduce((total, record) => total + record.bytes.byteLength, 0);
    if (
      records.length > this.#limits.maxOwnedRecords - this.#ownedRecords ||
      totalWireBytes > this.#limits.maxOwnedWireBytes - this.#ownedWireBytes
    ) {
      pending.commitFailure = "capacity-exceeded";
      return false;
    }

    const committedThrough = authorityCursor(gap.committedThrough);
    const prepareResult = Object.freeze({
      status: "prepared" as const,
      committedThrough: Object.freeze({ ...committedThrough }),
    });
    const final = records.at(-1)!;
    const source: RecoverySource = {
      closed: null,
      committedThrough,
      cumulativeGrantedEncodedBytes: 0n,
      finalCumulativeEncodedBytes: final.cumulativeEncodedBytes,
      finalDeliveryOrdinal: final.deliveryOrdinal,
      lastProgressAtMs: pending.startedAtMs,
      lastReceivedOrdinal: 0n,
      nextSendIndex: 0,
      ownerToken: pending.ownerToken,
      prepare: pending.prepare,
      preparePromise: pending.promise,
      prepareResult,
      ready: null,
      records,
      sentCumulativeEncodedBytes: 0n,
      sentDeliveryOrdinal: 0n,
      startedAtMs: pending.startedAtMs,
      totalRecords: records.length,
      totalWireBytes,
    };

    this.#sourcesByStream.set(pending.prepare.streamId, source);
    this.#ownedRecords += source.totalRecords;
    this.#ownedWireBytes += source.totalWireBytes;
    try {
      const fence = encodeRecoveryStartFence({
        type: "recovery-start-fence",
        ...routingIdentity(pending.prepare),
        engineId: pending.prepare.engineId,
        base: pending.prepare.base,
        source: pending.prepare.source,
        committedThrough,
        liveFloor: successorBoundary(committedThrough),
      });
      if (!enqueueFence(fence.slice())) {
        pending.commitFailure = "fence-unavailable";
        this.#removeSource(source);
        return false;
      }
    } catch {
      pending.commitFailure = "fence-unavailable";
      this.#removeSource(source);
      return false;
    }
    return true;
  }

  #finishPending(pending: PendingPrepare, result: RecoverySourcePrepareResult): void {
    if (pending.cancelled) return;
    pending.cancelled = true;
    if (this.#pendingByStream.get(pending.prepare.streamId)?.token === pending.token) {
      this.#pendingByStream.delete(pending.prepare.streamId);
    }
    if (result.status !== "prepared" && pending.replacedTombstone !== null) {
      this.#installTombstone(pending.replacedTombstone.ownerToken, {
        ...routingIdentity(pending.prepare),
        deliveryGeneration: pending.replacedTombstone.deliveryGeneration.toString(),
      });
    }
    pending.resolve(result);
  }

  #cancelPending(
    pending: PendingPrepare,
    result: RecoverySourcePrepareResult,
    retireGeneration = false,
  ): void {
    this.#rollbackPendingSource(pending);
    this.#finishPending(pending, result);
    if (retireGeneration) this.#installTombstone(pending.ownerToken, pending.prepare);
  }

  #rollbackPendingSource(pending: PendingPrepare): void {
    const source = this.#sourcesByStream.get(pending.prepare.streamId);
    if (source !== undefined && source.preparePromise === pending.promise)
      this.#removeSource(source);
  }

  #sourceFor(
    ownerToken: RecoverySourceOwnerToken,
    identity: RecoveryV3HostRoutingIdentity,
  ): RecoverySource | null {
    if (this.#disposed || this.#staleOwners.has(ownerToken)) return null;
    const source = this.#sourcesByStream.get(identity.streamId);
    return source !== undefined &&
      source.ownerToken === ownerToken &&
      sameRouting(source.prepare, identity)
      ? source
      : null;
  }

  #releaseRecords(source: RecoverySource): void {
    if (source.records === null) return;
    source.records = null;
    this.#ownedRecords -= source.totalRecords;
    this.#ownedWireBytes -= source.totalWireBytes;
  }

  #removeSource(source: RecoverySource): void {
    if (this.#sourcesByStream.get(source.prepare.streamId) === source) {
      this.#sourcesByStream.delete(source.prepare.streamId);
    }
    this.#releaseRecords(source);
  }

  #retireSource(source: RecoverySource): void {
    this.#removeSource(source);
    this.#installTombstone(source.ownerToken, source.prepare);
  }

  #installTombstone(
    ownerToken: RecoverySourceOwnerToken,
    identity: RecoveryV3HostRoutingIdentity,
  ): void {
    const deliveryGeneration = BigInt(identity.deliveryGeneration);
    const existing = this.#tombstonesByStream.get(identity.streamId);
    if (
      existing === undefined ||
      existing.ownerToken !== ownerToken ||
      deliveryGeneration > existing.deliveryGeneration
    ) {
      this.#tombstonesByStream.set(identity.streamId, { deliveryGeneration, ownerToken });
    }
  }

  #rejected(
    prepare: RecoveryV3HostPrepare,
    reason: RecoveryV3HostPrepareRejected["reason"],
  ): Extract<RecoverySourcePrepareResult, { status: "rejected" }> {
    return {
      status: "rejected",
      rejection: RecoveryV3HostPrepareRejectedSchema.parse({
        type: "recovery-prepare-rejected",
        ...routingIdentity(prepare),
        reason,
      }),
    };
  }

  #recordNow(): number {
    const nowMs = finiteNow(this.#monotonicNow());
    if (nowMs < this.#lastNowMs) throw new RangeError("monotonicNow must not move backwards");
    this.#lastNowMs = nowMs;
    return nowMs;
  }
}

function buildRetainedRecords(
  prepare: RecoveryV3HostPrepare,
  gap: PreparedRecoveryGap,
): readonly RetainedRecord[] {
  if (
    !sameReplayCursor(replayCursor(prepare.base), gap.base) ||
    gap.exactFrames !== gap.frames.length ||
    gap.frames.reduce((total, frame) => total + frame.byteLength, 0) !== gap.exactEncodedBytes
  ) {
    throw new Error("prepared gap facts changed");
  }

  const records: RetainedRecord[] = [];
  let cumulativeEncodedBytes = 0n;
  let deliveryOrdinal = 0n;
  for (const canonical of gap.frames) {
    const decoded = decodeDataFrame(canonical);
    if (
      (decoded.kind !== DataFrameKind.PtyOutput && decoded.kind !== DataFrameKind.ResizeApplied) ||
      decoded.deliveryGeneration !== 0n ||
      decoded.streamId !== 0 ||
      decoded.flags !== DataFrameFlag.None
    ) {
      throw new Error("prepared gap contains a non-canonical mutation");
    }
    deliveryOrdinal += 1n;
    cumulativeEncodedBytes += 40n + BigInt(canonical.byteLength);
    const bytes = encodeDeliveryEnvelopeV3({
      lane: "recovery",
      deliveryGeneration: BigInt(prepare.deliveryGeneration),
      deliveryOrdinal,
      cumulativeEncodedBytes,
      streamId: prepare.streamId,
      payload: canonical,
    });
    records.push(Object.freeze({ bytes, cumulativeEncodedBytes, deliveryOrdinal }));
  }

  const done = encodeDataFrame({
    kind: DataFrameKind.ReplayCommit,
    flags: DataFrameFlag.None,
    sessionEpoch: gap.committedThrough.sessionEpoch,
    deliveryGeneration: 0n,
    eventSeq: gap.committedThrough.lastEventSeq,
    ptyOffset: gap.committedThrough.nextPtyOffset,
    streamId: 0,
    payload: new Uint8Array(),
  });
  deliveryOrdinal += 1n;
  cumulativeEncodedBytes += 40n + BigInt(done.byteLength);
  records.push(
    Object.freeze({
      bytes: encodeDeliveryEnvelopeV3({
        lane: "recovery",
        deliveryGeneration: BigInt(prepare.deliveryGeneration),
        deliveryOrdinal,
        cumulativeEncodedBytes,
        streamId: prepare.streamId,
        payload: done,
      }),
      cumulativeEncodedBytes,
      deliveryOrdinal,
    }),
  );
  return Object.freeze(records);
}

function replayCursor(cursor: AuthorityCursor): ReplayCursor {
  return {
    sessionEpoch: BigInt(cursor.sessionEpoch),
    lastEventSeq: BigInt(cursor.eventSeq),
    nextPtyOffset: BigInt(cursor.nextPtyOffset),
  };
}

function authorityCursor(cursor: ReplayCursor): AuthorityCursor {
  return {
    sessionEpoch: cursor.sessionEpoch.toString(),
    eventSeq: cursor.lastEventSeq.toString(),
    nextPtyOffset: cursor.nextPtyOffset.toString(),
  };
}

function routingIdentity(input: RecoveryV3HostRoutingIdentity): RecoveryV3HostRoutingIdentity {
  return {
    recoveryId: input.recoveryId,
    connectionId: input.connectionId,
    streamId: input.streamId,
    deliveryGeneration: input.deliveryGeneration,
  };
}

function sameRouting(
  left: RecoveryV3HostRoutingIdentity,
  right: RecoveryV3HostRoutingIdentity,
): boolean {
  return (
    left.recoveryId === right.recoveryId &&
    left.connectionId === right.connectionId &&
    left.streamId === right.streamId &&
    left.deliveryGeneration === right.deliveryGeneration
  );
}

function samePrepare(left: RecoveryV3HostPrepare, right: RecoveryV3HostPrepare): boolean {
  return (
    sameRouting(left, right) &&
    left.engineId === right.engineId &&
    sameAuthorityCursor(left.base, right.base) &&
    left.source.kind === right.source.kind &&
    (left.source.kind === "warm" ||
      (right.source.kind === "snapshot" && left.source.snapshotId === right.source.snapshotId))
  );
}

function sameStartReady(left: RecoveryV3HostStartReady, right: RecoveryV3HostStartReady): boolean {
  return (
    sameRouting(left, right) &&
    sameAuthorityCursor(left.committedThrough, right.committedThrough) &&
    left.cumulativeGrantedEncodedBytes === right.cumulativeGrantedEncodedBytes
  );
}

function sameAuthorityCursor(left: AuthorityCursor, right: AuthorityCursor): boolean {
  return (
    left.sessionEpoch === right.sessionEpoch &&
    left.eventSeq === right.eventSeq &&
    left.nextPtyOffset === right.nextPtyOffset
  );
}

function sameReplayCursor(left: ReplayCursor, right: ReplayCursor): boolean {
  return (
    left.sessionEpoch === right.sessionEpoch &&
    left.lastEventSeq === right.lastEventSeq &&
    left.nextPtyOffset === right.nextPtyOffset
  );
}

function createDeferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function assertOwnerToken(ownerToken: RecoverySourceOwnerToken): void {
  if (ownerToken === null || typeof ownerToken !== "object") {
    throw new TypeError("ownerToken must be an object identity");
  }
}

function nonNegativeSafeInteger(value: number, field: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${field} must be a non-negative safe integer`);
  }
  return value;
}

function positiveSafeInteger(value: number, field: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${field} must be a positive safe integer`);
  }
  return value;
}

function positiveFinite(value: number, field: string): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new RangeError(`${field} must be a positive finite number`);
  }
  return value;
}

function finiteNow(value: number): number {
  if (!Number.isFinite(value)) throw new RangeError("monotonicNow must return a finite number");
  return value;
}
