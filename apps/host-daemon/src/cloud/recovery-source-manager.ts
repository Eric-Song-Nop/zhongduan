import {
  DELIVERY_ENVELOPE_HEADER_BYTES,
  DataFrameFlag,
  DataFrameKind,
  RecoveryHostPrepareRejectedSchema,
  RecoveryHostPrepareSchema,
  RecoveryHostRoutingIdentitySchema,
  RecoveryHostSourceClosedSchema,
  RecoveryHostSourceGrantSchema,
  RecoveryHostSourceReceivedSchema,
  RecoveryHostSourceResetSchema,
  RecoveryHostStartReadySchema,
  decodeDataFrame,
  encodeDataFrame,
  encodeDeliveryEnvelope,
  encodeRecoveryStartFence,
  successorBoundary,
  type AuthorityCursor,
  type RecoveryHostPrepare,
  type RecoveryHostPrepareRejected,
  type RecoveryHostRoutingIdentity,
  type RecoveryHostSourceClosed,
  type RecoveryHostSourceGrant,
  type RecoveryHostSourceReceived,
  type RecoveryHostSourceReset,
  type RecoveryHostStartReady,
} from "@zhongduan/protocol";

import type { PreparedRecoveryGap, ReplayCursor, TerminalSession } from "../session";

const MAX_RETIRED_GENERATIONS_PER_STREAM = 16;

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
      readonly rejection: RecoveryHostPrepareRejected;
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

export type RecoverySourceDrainResult = {
  readonly status: "runnable" | "credit-blocked" | "complete" | "stale";
  readonly records: number;
  readonly wireBytes: number;
};

export type RecoverySourceReceivedResult =
  | {
      readonly status: "partial";
      readonly advanced: boolean;
      readonly contiguousDeliveryOrdinal: string;
      readonly cumulativeEncodedBytes: string;
    }
  | {
      readonly status: "closed";
      readonly closed: RecoveryHostSourceClosed;
      readonly duplicate: boolean;
    }
  | {
      readonly status: "invalid";
      readonly reason: "unknown-source" | "beyond-sent" | "cumulative-mismatch" | "non-monotonic";
    };

export interface RecoverySourceDeadlineExpiration {
  readonly ownerToken: RecoverySourceOwnerToken;
  readonly identity: RecoveryHostRoutingIdentity;
  readonly reason: "prepare-deadline" | "no-progress-deadline" | "recovery-deadline";
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
  readonly prepare: RecoveryHostPrepare;
  readonly promise: Promise<RecoverySourcePrepareResult>;
  readonly resolve: (result: RecoverySourcePrepareResult) => void;
  readonly startedAtMs: number;
  readonly token: symbol;
}

interface RecoverySource {
  closed: RecoveryHostSourceClosed | null;
  cumulativeGrantedEncodedBytes: bigint;
  readonly committedThrough: AuthorityCursor;
  readonly finalCumulativeEncodedBytes: bigint;
  readonly finalDeliveryOrdinal: bigint;
  lastProgressAtMs: number;
  lastReceivedOrdinal: bigint;
  nextSendIndex: number;
  ownedRecords: number;
  ownedWireBytes: number;
  readonly ownerToken: RecoverySourceOwnerToken;
  readonly prepare: RecoveryHostPrepare;
  readonly preparePromise: Promise<RecoverySourcePrepareResult>;
  readonly prepareResult: Extract<RecoverySourcePrepareResult, { status: "prepared" }>;
  ready: RecoveryHostStartReady | null;
  readonly recordCumulativeEncodedBytes: readonly bigint[];
  records: Array<RetainedRecord | null> | null;
  sentCumulativeEncodedBytes: bigint;
  sentDeliveryOrdinal: bigint;
  readonly startedAtMs: number;
  readonly totalRecords: number;
  readonly totalWireBytes: number;
}

interface RecoveryGenerationTombstone {
  readonly identity: RecoveryHostRoutingIdentity;
  readonly ownerToken: RecoverySourceOwnerToken;
}

interface Deferred<T> {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
}

/**
 * Pure, session-scoped owner for bounded Host recovery sources.
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
  readonly #tombstonesByStream = new Map<number, readonly RecoveryGenerationTombstone[]>();

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
      sources: this.#streamCount(),
    };
  }

  prepare(
    ownerToken: RecoverySourceOwnerToken,
    input: RecoveryHostPrepare,
    enqueueFence: EnqueueRecoveryStartFence,
  ): Promise<RecoverySourcePrepareResult> {
    assertOwnerToken(ownerToken);
    const prepare = RecoveryHostPrepareSchema.parse(input);
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
    if (!this.#hasStream(prepare.streamId) && this.#streamCount() >= this.#limits.maxSources) {
      return Promise.resolve(this.#rejected(prepare, "capacity-exceeded"));
    }
    // Every accepted pending/active generation reserves one history slot so a
    // later reset or deadline can always retire it without losing exact identity.
    if (!this.#hasRetirementCapacity(prepare.streamId, 1)) {
      return Promise.resolve(this.#rejected(prepare, "capacity-exceeded"));
    }

    const deferred = createDeferred<RecoverySourcePrepareResult>();
    const pending: PendingPrepare = {
      cancelled: false,
      commitFailure: null,
      ownerToken,
      prepare,
      promise: deferred.promise,
      resolve: deferred.resolve,
      startedAtMs: nowMs,
      token: Symbol("recovery-source-prepare"),
    };
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

  startReady(ownerToken: RecoverySourceOwnerToken, input: RecoveryHostStartReady): boolean {
    assertOwnerToken(ownerToken);
    const ready = RecoveryHostStartReadySchema.parse(input);
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

  grant(ownerToken: RecoverySourceOwnerToken, input: RecoveryHostSourceGrant): boolean {
    assertOwnerToken(ownerToken);
    const grant = RecoveryHostSourceGrantSchema.parse(input);
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
    identity: RecoveryHostRoutingIdentity,
    limits: RecoverySourceDrainLimits,
    send: (encoded: Uint8Array) => void,
  ): RecoverySourceDrainResult {
    assertOwnerToken(ownerToken);
    const routing = RecoveryHostRoutingIdentitySchema.parse(identity);
    const maxRecords = nonNegativeSafeInteger(limits.maxRecords, "maxRecords");
    const maxWireBytes = nonNegativeSafeInteger(limits.maxWireBytes, "maxWireBytes");
    this.#recordNow();
    const source = this.#sourceFor(ownerToken, routing);
    if (source === null || source.ready === null || source.records === null) {
      return { status: "stale", records: 0, wireBytes: 0 };
    }

    let sentRecords = 0;
    let sentWireBytes = 0;
    while (sentRecords < maxRecords) {
      const record = source.records[source.nextSendIndex];
      if (record === undefined) break;
      if (record === null) throw new Error("unsent Recovery source record was released");
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
    return {
      status: this.#drainStatus(ownerToken, routing, source),
      records: sentRecords,
      wireBytes: sentWireBytes,
    };
  }

  received(
    ownerToken: RecoverySourceOwnerToken,
    input: RecoveryHostSourceReceived,
  ): RecoverySourceReceivedResult {
    assertOwnerToken(ownerToken);
    const received = RecoveryHostSourceReceivedSchema.parse(input);
    const nowMs = this.#recordNow();
    const source = this.#sourceFor(ownerToken, received);
    if (source === null) return { status: "invalid", reason: "unknown-source" };

    const deliveryOrdinal = BigInt(received.contiguousDeliveryOrdinal);
    const cumulativeEncodedBytes = BigInt(received.cumulativeEncodedBytes);
    if (source.closed !== null) {
      return deliveryOrdinal === source.finalDeliveryOrdinal &&
        cumulativeEncodedBytes === source.finalCumulativeEncodedBytes
        ? { status: "closed", closed: source.closed, duplicate: true }
        : { status: "invalid", reason: "cumulative-mismatch" };
    }
    const records = source.records;
    if (
      records === null ||
      deliveryOrdinal > source.sentDeliveryOrdinal ||
      deliveryOrdinal <= 0n ||
      deliveryOrdinal > BigInt(records.length)
    ) {
      return { status: "invalid", reason: "beyond-sent" };
    }
    const expectedCumulative = source.recordCumulativeEncodedBytes[Number(deliveryOrdinal - 1n)];
    if (expectedCumulative === undefined || expectedCumulative !== cumulativeEncodedBytes) {
      return { status: "invalid", reason: "cumulative-mismatch" };
    }
    if (deliveryOrdinal < source.lastReceivedOrdinal) {
      return { status: "invalid", reason: "non-monotonic" };
    }
    const advanced = deliveryOrdinal > source.lastReceivedOrdinal;
    if (advanced) {
      this.#releaseReceivedPrefix(source, deliveryOrdinal);
      source.lastReceivedOrdinal = deliveryOrdinal;
      source.lastProgressAtMs = nowMs;
    }
    if (deliveryOrdinal !== source.finalDeliveryOrdinal) {
      return {
        status: "partial",
        advanced,
        contiguousDeliveryOrdinal: received.contiguousDeliveryOrdinal,
        cumulativeEncodedBytes: received.cumulativeEncodedBytes,
      };
    }

    source.closed = Object.freeze(
      RecoveryHostSourceClosedSchema.parse({
        type: "recovery-source-closed",
        ...routingIdentity(source.prepare),
        throughRecoveryOrdinal: source.finalDeliveryOrdinal.toString(),
        throughRecoveryCumulativeEncodedBytes: source.finalCumulativeEncodedBytes.toString(),
      }),
    );
    this.#releaseRecords(source);
    return { status: "closed", closed: source.closed, duplicate: false };
  }

  reset(ownerToken: RecoverySourceOwnerToken, input: RecoveryHostSourceReset): boolean {
    assertOwnerToken(ownerToken);
    const reset = RecoveryHostSourceResetSchema.parse(input);
    this.#recordNow();
    if (this.#disposed || this.#staleOwners.has(ownerToken)) return false;

    const source = this.#sourceFor(ownerToken, reset);
    if (source !== null) {
      return this.#retireSource(source);
    }
    // An occupied stream with a different routing identity is not an
    // "unknown" source. Never let a stale or forged reset retire/fence the
    // current generation implicitly.
    if (this.#sourcesByStream.has(reset.streamId)) return false;

    const pending = this.#pendingByStream.get(reset.streamId);
    if (pending !== undefined) {
      if (pending.ownerToken !== ownerToken || !sameRouting(pending.prepare, reset)) return false;
      this.#cancelPending(pending, this.#rejected(pending.prepare, "generation-fenced"), true);
      return true;
    }

    const tombstones = this.#tombstonesByStream.get(reset.streamId);
    if (tombstones !== undefined) {
      if (tombstones.some((tombstone) => tombstone.ownerToken !== ownerToken)) return false;
      if (tombstones.some((tombstone) => sameRouting(tombstone.identity, reset))) return true;
      if (BigInt(reset.deliveryGeneration) <= this.#latestRetiredGeneration(tombstones)) {
        return false;
      }
      return this.#installTombstone(ownerToken, reset);
    }

    // Cloud may durably replace an unsent recovery-prepare outbox entry with
    // recovery-source-reset. The current authenticated pair must accept that
    // reset even though it has never observed the source. Retaining the exact
    // generation identity prevents a late prepare from resurrecting it.
    if (!this.#hasStream(reset.streamId) && this.#streamCount() >= this.#limits.maxSources) {
      return false;
    }
    return this.#installTombstone(ownerToken, reset);
  }

  resetOwner(ownerToken: RecoverySourceOwnerToken): number {
    assertOwnerToken(ownerToken);
    this.#recordNow();
    this.#staleOwners.add(ownerToken);
    const resetStreams = new Set<number>();
    for (const pending of this.#pendingByStream.values()) {
      if (pending.ownerToken !== ownerToken) continue;
      this.#cancelPending(pending, this.#rejected(pending.prepare, "client-gone"));
      resetStreams.add(pending.prepare.streamId);
    }
    for (const source of this.#sourcesByStream.values()) {
      if (source.ownerToken !== ownerToken) continue;
      this.#removeSource(source);
      resetStreams.add(source.prepare.streamId);
    }
    for (const [streamId, tombstones] of this.#tombstonesByStream) {
      const retained = tombstones.filter((tombstone) => tombstone.ownerToken !== ownerToken);
      if (retained.length === tombstones.length) continue;
      if (retained.length === 0) this.#tombstonesByStream.delete(streamId);
      else this.#tombstonesByStream.set(streamId, Object.freeze(retained));
      resetStreams.add(streamId);
    }
    return resetStreams.size;
  }

  isRetiredIdentity(
    ownerToken: RecoverySourceOwnerToken,
    identity: RecoveryHostRoutingIdentity,
  ): boolean {
    assertOwnerToken(ownerToken);
    const routing = RecoveryHostRoutingIdentitySchema.parse(identity);
    const tombstones = this.#tombstonesByStream.get(routing.streamId);
    return (
      tombstones?.some(
        (tombstone) =>
          tombstone.ownerToken === ownerToken && sameRouting(tombstone.identity, routing),
      ) ?? false
    );
  }

  checkDeadlines(ownerToken: RecoverySourceOwnerToken): RecoverySourceDeadlineExpiration[] {
    assertOwnerToken(ownerToken);
    const nowMs = this.#recordNow();
    const expired: RecoverySourceDeadlineExpiration[] = [];
    for (const pending of this.#pendingByStream.values()) {
      if (pending.ownerToken !== ownerToken) continue;
      if (nowMs - pending.startedAtMs < this.#limits.recoveryDeadlineMs) continue;
      expired.push({
        ownerToken: pending.ownerToken,
        identity: routingIdentity(pending.prepare),
        reason: "prepare-deadline",
      });
      this.#cancelPending(pending, { status: "unavailable", reason: "deadline" }, true);
    }
    for (const source of this.#sourcesByStream.values()) {
      if (source.ownerToken !== ownerToken) continue;
      if (source.closed !== null) continue;
      if (
        nowMs - source.startedAtMs < this.#limits.recoveryDeadlineMs &&
        nowMs - source.lastProgressAtMs < this.#limits.noProgressDeadlineMs
      ) {
        continue;
      }
      expired.push({
        ownerToken: source.ownerToken,
        identity: routingIdentity(source.prepare),
        reason:
          nowMs - source.startedAtMs >= this.#limits.recoveryDeadlineMs
            ? "recovery-deadline"
            : "no-progress-deadline",
      });
      if (!this.#retireSource(source)) {
        throw new Error("Recovery source retirement history capacity was exhausted");
      }
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
    prepare: RecoveryHostPrepare,
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
      if (!this.#hasRetirementCapacity(prepare.streamId, 2)) {
        return Promise.resolve(this.#rejected(prepare, "capacity-exceeded"));
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
      if (!this.#hasRetirementCapacity(prepare.streamId, 2)) {
        return Promise.resolve(this.#rejected(prepare, "capacity-exceeded"));
      }
      if (!this.#retireSource(source)) {
        throw new Error("Recovery source retirement history capacity changed unexpectedly");
      }
    }

    const tombstones = this.#tombstonesByStream.get(prepare.streamId);
    if (tombstones === undefined) return null;
    if (tombstones.some((tombstone) => tombstone.ownerToken !== ownerToken)) {
      return Promise.resolve(this.#rejected(prepare, "client-gone"));
    }
    if (generation <= this.#latestRetiredGeneration(tombstones)) {
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
      ownedRecords: records.length,
      ownedWireBytes: totalWireBytes,
      ownerToken: pending.ownerToken,
      prepare: pending.prepare,
      preparePromise: pending.promise,
      prepareResult,
      ready: null,
      recordCumulativeEncodedBytes: Object.freeze(
        records.map((record) => record.cumulativeEncodedBytes),
      ),
      records: [...records],
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
    pending.resolve(result);
  }

  #cancelPending(
    pending: PendingPrepare,
    result: RecoverySourcePrepareResult,
    retireGeneration = false,
  ): void {
    this.#rollbackPendingSource(pending);
    this.#finishPending(pending, result);
    if (retireGeneration && !this.#installTombstone(pending.ownerToken, pending.prepare)) {
      throw new Error("Recovery pending retirement history capacity was exhausted");
    }
  }

  #rollbackPendingSource(pending: PendingPrepare): void {
    const source = this.#sourcesByStream.get(pending.prepare.streamId);
    if (source !== undefined && source.preparePromise === pending.promise)
      this.#removeSource(source);
  }

  #sourceFor(
    ownerToken: RecoverySourceOwnerToken,
    identity: RecoveryHostRoutingIdentity,
  ): RecoverySource | null {
    if (this.#disposed || this.#staleOwners.has(ownerToken)) return null;
    const source = this.#sourcesByStream.get(identity.streamId);
    return source !== undefined &&
      source.ownerToken === ownerToken &&
      sameRouting(source.prepare, identity)
      ? source
      : null;
  }

  #drainStatus(
    ownerToken: RecoverySourceOwnerToken,
    identity: RecoveryHostRoutingIdentity,
    source: RecoverySource,
  ): RecoverySourceDrainResult["status"] {
    if (this.#sourceFor(ownerToken, identity) !== source || source.records === null) return "stale";
    const next = source.records[source.nextSendIndex];
    if (next === undefined) return "complete";
    if (next === null) throw new Error("unsent Recovery source record was released");
    return next.cumulativeEncodedBytes <= source.cumulativeGrantedEncodedBytes
      ? "runnable"
      : "credit-blocked";
  }

  #releaseReceivedPrefix(source: RecoverySource, throughDeliveryOrdinal: bigint): void {
    const records = source.records;
    if (records === null) return;
    const from = Number(source.lastReceivedOrdinal);
    const through = Number(throughDeliveryOrdinal);
    for (let index = from; index < through; index += 1) {
      const record = records[index];
      if (record === undefined) throw new Error("Recovery source receipt prefix is incomplete");
      if (record === null) continue;
      records[index] = null;
      source.ownedRecords -= 1;
      source.ownedWireBytes -= record.bytes.byteLength;
      this.#ownedRecords -= 1;
      this.#ownedWireBytes -= record.bytes.byteLength;
    }
  }

  #releaseRecords(source: RecoverySource): void {
    if (source.records === null) return;
    source.records = null;
    this.#ownedRecords -= source.ownedRecords;
    this.#ownedWireBytes -= source.ownedWireBytes;
    source.ownedRecords = 0;
    source.ownedWireBytes = 0;
  }

  #removeSource(source: RecoverySource): void {
    if (this.#sourcesByStream.get(source.prepare.streamId) === source) {
      this.#sourcesByStream.delete(source.prepare.streamId);
    }
    this.#releaseRecords(source);
  }

  #retireSource(source: RecoverySource): boolean {
    if (!this.#installTombstone(source.ownerToken, source.prepare)) return false;
    this.#removeSource(source);
    return true;
  }

  #installTombstone(
    ownerToken: RecoverySourceOwnerToken,
    identity: RecoveryHostRoutingIdentity,
  ): boolean {
    const existing = this.#tombstonesByStream.get(identity.streamId) ?? [];
    if (existing.some((tombstone) => tombstone.ownerToken !== ownerToken)) return false;
    if (
      existing.some(
        (tombstone) =>
          tombstone.ownerToken === ownerToken && sameRouting(tombstone.identity, identity),
      )
    ) {
      return true;
    }
    if (existing.length >= MAX_RETIRED_GENERATIONS_PER_STREAM) return false;
    this.#tombstonesByStream.set(
      identity.streamId,
      Object.freeze([
        ...existing,
        {
          identity: Object.freeze(routingIdentity(identity)),
          ownerToken,
        },
      ]),
    );
    return true;
  }

  #hasRetirementCapacity(streamId: number, additionalIdentities: number): boolean {
    return (
      (this.#tombstonesByStream.get(streamId)?.length ?? 0) + additionalIdentities <=
      MAX_RETIRED_GENERATIONS_PER_STREAM
    );
  }

  #hasStream(streamId: number): boolean {
    return (
      this.#pendingByStream.has(streamId) ||
      this.#sourcesByStream.has(streamId) ||
      this.#tombstonesByStream.has(streamId)
    );
  }

  #latestRetiredGeneration(tombstones: readonly RecoveryGenerationTombstone[]): bigint {
    let latest = -1n;
    for (const tombstone of tombstones) {
      const generation = BigInt(tombstone.identity.deliveryGeneration);
      if (generation > latest) latest = generation;
    }
    return latest;
  }

  #streamCount(): number {
    const streamIds = new Set<number>();
    for (const streamId of this.#pendingByStream.keys()) streamIds.add(streamId);
    for (const streamId of this.#sourcesByStream.keys()) streamIds.add(streamId);
    for (const streamId of this.#tombstonesByStream.keys()) streamIds.add(streamId);
    return streamIds.size;
  }

  #rejected(
    prepare: RecoveryHostPrepare,
    reason: RecoveryHostPrepareRejected["reason"],
  ): Extract<RecoverySourcePrepareResult, { status: "rejected" }> {
    return {
      status: "rejected",
      rejection: RecoveryHostPrepareRejectedSchema.parse({
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
  prepare: RecoveryHostPrepare,
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
    cumulativeEncodedBytes += BigInt(DELIVERY_ENVELOPE_HEADER_BYTES + canonical.byteLength);
    const bytes = encodeDeliveryEnvelope({
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
    kind: DataFrameKind.RecoveryDone,
    flags: DataFrameFlag.None,
    sessionEpoch: gap.committedThrough.sessionEpoch,
    deliveryGeneration: 0n,
    eventSeq: gap.committedThrough.lastEventSeq,
    ptyOffset: gap.committedThrough.nextPtyOffset,
    streamId: 0,
    payload: new Uint8Array(),
  });
  deliveryOrdinal += 1n;
  cumulativeEncodedBytes += BigInt(DELIVERY_ENVELOPE_HEADER_BYTES + done.byteLength);
  records.push(
    Object.freeze({
      bytes: encodeDeliveryEnvelope({
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

function routingIdentity(input: RecoveryHostRoutingIdentity): RecoveryHostRoutingIdentity {
  return {
    recoveryId: input.recoveryId,
    connectionId: input.connectionId,
    streamId: input.streamId,
    deliveryGeneration: input.deliveryGeneration,
  };
}

function sameRouting(
  left: RecoveryHostRoutingIdentity,
  right: RecoveryHostRoutingIdentity,
): boolean {
  return (
    left.recoveryId === right.recoveryId &&
    left.connectionId === right.connectionId &&
    left.streamId === right.streamId &&
    left.deliveryGeneration === right.deliveryGeneration
  );
}

function samePrepare(left: RecoveryHostPrepare, right: RecoveryHostPrepare): boolean {
  return (
    sameRouting(left, right) &&
    left.engineId === right.engineId &&
    sameAuthorityCursor(left.base, right.base) &&
    left.source.kind === right.source.kind &&
    (left.source.kind === "warm" ||
      (right.source.kind === "snapshot" && left.source.snapshotId === right.source.snapshotId))
  );
}

function sameStartReady(left: RecoveryHostStartReady, right: RecoveryHostStartReady): boolean {
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
