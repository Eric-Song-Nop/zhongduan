import {
  DELIVERY_ENVELOPE_HEADER_BYTES,
  DataFrameKind,
  encodeDataFrame,
  encodeDeliveryEnvelope,
  encodeResizePayload,
  type AuthorityCursor,
  type DeliveryLane,
  type DeliveryReceived,
  type RecoverySourceClosed,
  type RecoveryStart,
  type ReplicaCursor,
  type ResizePayload,
} from "@zhongduan/protocol";
import { describe, expect, it } from "vitest";

import {
  RecoveryAssembler,
  type RecoveryAssemblerLimits,
  type RecoveryAssemblerOptions,
} from "./recovery-assembler";
import type { ReplicaSink } from "./types";

const ENGINE_ID = "engine-recovery";
const RECOVERY_ID = "recovery_AAAAAAAAAAA";
const DELIVERY_GENERATION = 3n;
const STREAM_ID = 42;

type MutationSpec =
  | {
      readonly kind: "pty";
      readonly eventSeq: bigint;
      readonly ptyOffset: bigint;
      readonly payload: readonly number[];
    }
  | {
      readonly kind: "resize";
      readonly eventSeq: bigint;
      readonly ptyOffset: bigint;
      readonly resize: ResizePayload;
    };

interface EncodedRecord {
  readonly cumulativeEncodedBytes: bigint;
  readonly deliveryOrdinal: bigint;
  readonly lane: DeliveryLane;
  readonly mutation?: MutationSpec;
  readonly raw: Uint8Array;
  readonly type: "done" | "mutation";
}

type ReplicaEffect =
  | { readonly type: "pty"; readonly payload: readonly number[] }
  | { readonly type: "resize"; readonly resize: ResizePayload };

class FakeReplica implements ReplicaSink {
  readonly effects: ReplicaEffect[] = [];
  disposeCalls = 0;

  constructor(
    readonly engineId = ENGINE_ID,
    private readonly failAtEffect: number | null = null,
  ) {}

  writePty(data: Uint8Array): void {
    this.#beforeEffect();
    this.effects.push({ type: "pty", payload: [...data] });
  }

  resize(resize: ResizePayload): void {
    this.#beforeEffect();
    this.effects.push({ type: "resize", resize: { ...resize } });
  }

  dispose(): void {
    this.disposeCalls += 1;
  }

  #beforeEffect(): void {
    if (this.failAtEffect === this.effects.length) throw new Error("injected apply failure");
  }
}

const BASE: AuthorityCursor = {
  sessionEpoch: "7",
  eventSeq: "10",
  nextPtyOffset: "100",
};
const COMMITTED: AuthorityCursor = {
  sessionEpoch: "7",
  eventSeq: "12",
  nextPtyOffset: "102",
};

const RECOVERY_MUTATIONS: readonly MutationSpec[] = [
  { kind: "pty", eventSeq: 11n, ptyOffset: 100n, payload: [0x61, 0x62] },
  {
    kind: "resize",
    eventSeq: 12n,
    ptyOffset: 102n,
    resize: { cols: 120, rows: 40, widthPx: 960, heightPx: 800 },
  },
];
const LIVE_MUTATIONS: readonly MutationSpec[] = [
  { kind: "pty", eventSeq: 13n, ptyOffset: 102n, payload: [0x63] },
  {
    kind: "resize",
    eventSeq: 14n,
    ptyOffset: 103n,
    resize: { cols: 100, rows: 30, widthPx: 900, heightPx: 700 },
  },
];

function limits(overrides: Partial<RecoveryAssemblerLimits> = {}): RecoveryAssemblerLimits {
  return {
    maxApplyFramesPerCall: 2,
    maxGapSpan: 8n,
    maxOwnedBytes: 64 * 1024,
    maxOwnedFrames: 32,
    noProgressDeadlineMs: 1_000,
    recoveryDeadlineMs: 10_000,
    ...overrides,
  };
}

function warmStart(overrides: Partial<RecoveryStart> = {}): RecoveryStart {
  return {
    type: "recovery-start",
    recoveryId: RECOVERY_ID,
    deliveryGeneration: DELIVERY_GENERATION.toString(),
    streamId: STREAM_ID,
    engineId: ENGINE_ID,
    authorityDataFormat: 1,
    base: { ...BASE },
    source: { kind: "warm" },
    committedThrough: { ...COMMITTED },
    liveFloor: { sessionEpoch: "7", nextEventSeq: "13", nextPtyOffset: "102" },
    ...overrides,
  };
}

function coldStart(): RecoveryStart {
  return {
    ...warmStart(),
    source: {
      kind: "snapshot",
      sessionId: "session_AAAAAAAAA",
      snapshotId: "snapshot_AAAAAAAAAAA",
      engineId: ENGINE_ID,
      sessionEpoch: BASE.sessionEpoch,
      cutEventSeq: BASE.eventSeq,
      nextPtyOffset: BASE.nextPtyOffset,
      compression: "none",
      compressedLength: "4",
      uncompressedLength: "4",
      sha256: "a".repeat(64),
      downloadPath: "/api/v1/sessions/session_AAAAAAAAA/snapshots/snapshot_AAAAAAAAAAA",
      restoreThrough: "finish",
    },
  };
}

function canonicalMutation(spec: MutationSpec): Uint8Array {
  return encodeDataFrame({
    kind: spec.kind === "pty" ? DataFrameKind.PtyOutput : DataFrameKind.ResizeApplied,
    flags: 0,
    sessionEpoch: 7n,
    deliveryGeneration: 0n,
    eventSeq: spec.eventSeq,
    ptyOffset: spec.ptyOffset,
    streamId: 0,
    payload: spec.kind === "pty" ? new Uint8Array(spec.payload) : encodeResizePayload(spec.resize),
  });
}

function canonicalDone(eventSeq = 12n, ptyOffset = 102n): Uint8Array {
  return encodeDataFrame({
    kind: DataFrameKind.RecoveryDone,
    flags: 0,
    sessionEpoch: 7n,
    deliveryGeneration: 0n,
    eventSeq,
    ptyOffset,
    streamId: 0,
    payload: new Uint8Array(),
  });
}

function recordsForLane(
  lane: DeliveryLane,
  entries: readonly (MutationSpec | "done")[],
): EncodedRecord[] {
  let cumulativeEncodedBytes = 0n;
  return entries.map((entry, index) => {
    const payload = entry === "done" ? canonicalDone() : canonicalMutation(entry);
    // The public wire header is exactly 40 bytes. Keep this oracle literal independent from
    // the assembler's accounting and cursor helpers.
    cumulativeEncodedBytes += BigInt(DELIVERY_ENVELOPE_HEADER_BYTES + payload.byteLength);
    const deliveryOrdinal = BigInt(index + 1);
    return {
      lane,
      deliveryOrdinal,
      cumulativeEncodedBytes,
      type: entry === "done" ? "done" : "mutation",
      ...(entry === "done" ? {} : { mutation: entry }),
      raw: encodeDeliveryEnvelope({
        lane,
        deliveryGeneration: DELIVERY_GENERATION,
        deliveryOrdinal,
        cumulativeEncodedBytes,
        streamId: STREAM_ID,
        payload,
      }),
    };
  });
}

function fixture() {
  return {
    recovery: recordsForLane("recovery", [...RECOVERY_MUTATIONS, "done"]),
    live: recordsForLane("live", LIVE_MUTATIONS),
  };
}

function createWarmAssembler(
  overrides: Partial<Omit<RecoveryAssemblerOptions, "limits" | "warm">> & {
    limits?: Partial<RecoveryAssemblerLimits>;
    replica?: FakeReplica;
  } = {},
) {
  const replica = overrides.replica ?? new FakeReplica();
  const assembler = new RecoveryAssembler({
    deliveryGeneration: overrides.deliveryGeneration ?? DELIVERY_GENERATION,
    streamId: overrides.streamId ?? STREAM_ID,
    engineId: overrides.engineId ?? ENGINE_ID,
    startedAtMs: overrides.startedAtMs ?? 0,
    limits: limits(overrides.limits),
    warm: { cursor: { ...BASE }, replica },
  });
  return { assembler, replica };
}

function createColdAssembler(
  overrides: Partial<Omit<RecoveryAssemblerOptions, "limits" | "warm">> & {
    limits?: Partial<RecoveryAssemblerLimits>;
  } = {},
) {
  return new RecoveryAssembler({
    deliveryGeneration: overrides.deliveryGeneration ?? DELIVERY_GENERATION,
    streamId: overrides.streamId ?? STREAM_ID,
    engineId: overrides.engineId ?? ENGINE_ID,
    startedAtMs: overrides.startedAtMs ?? 0,
    limits: limits(overrides.limits),
  });
}

function candidateIdentity(start: RecoveryStart = coldStart()) {
  return {
    recoveryId: start.recoveryId,
    deliveryGeneration: start.deliveryGeneration,
    base: { ...start.base },
  };
}

function replicaCursor(eventSeq: bigint, nextPtyOffset: bigint): ReplicaCursor {
  return {
    sessionEpoch: 7n,
    deliveryGeneration: DELIVERY_GENERATION,
    lastEventSeq: eventSeq,
    nextPtyOffset,
  };
}

function sourceClosed(done: EncodedRecord): RecoverySourceClosed {
  return {
    type: "recovery-source-closed",
    recoveryId: RECOVERY_ID,
    deliveryGeneration: DELIVERY_GENERATION.toString(),
    throughRecoveryOrdinal: done.deliveryOrdinal.toString(),
    throughRecoveryCumulativeEncodedBytes: done.cumulativeEncodedBytes.toString(),
  };
}

function orderedMerges<T>(left: readonly T[], right: readonly T[]): T[][] {
  if (left.length === 0) return [[...right]];
  if (right.length === 0) return [[...left]];
  return [
    ...orderedMerges(left.slice(1), right).map((tail) => [left[0]!, ...tail]),
    ...orderedMerges(left, right.slice(1)).map((tail) => [right[0]!, ...tail]),
  ];
}

interface ReferenceState {
  readonly admitted: Map<bigint, MutationSpec>;
  readonly effects: ReplicaEffect[];
  readonly laneProgress: Record<DeliveryLane, { bytes: bigint; ordinal: bigint }>;
  appliedEventSeq: bigint;
  appliedOffset: bigint;
  done: boolean;
  started: boolean;
  targetReady: boolean;
}

type ReferenceCommand =
  | { readonly type: "admit"; readonly record: EncodedRecord }
  | { readonly type: "continue"; readonly quantum: number }
  | { readonly type: "start" }
  | { readonly type: "target-ready" };

function referenceState(): ReferenceState {
  return {
    admitted: new Map(),
    effects: [],
    laneProgress: {
      live: { bytes: 0n, ordinal: 0n },
      recovery: { bytes: 0n, ordinal: 0n },
    },
    appliedEventSeq: 10n,
    appliedOffset: 100n,
    done: false,
    started: false,
    targetReady: false,
  };
}

/** Independent reducer: it never decodes wire or calls a production cursor/assembler helper. */
function reduceReference(state: ReferenceState, command: ReferenceCommand): void {
  if (command.type === "start") {
    state.started = true;
    return;
  }
  if (command.type === "target-ready") {
    state.targetReady = true;
    return;
  }
  if (command.type === "admit") {
    const { record } = command;
    state.laneProgress[record.lane] = {
      ordinal: record.deliveryOrdinal,
      bytes: record.cumulativeEncodedBytes,
    };
    if (record.type === "done") state.done = true;
    else state.admitted.set(record.mutation!.eventSeq, record.mutation!);
    return;
  }
  if (!state.started || !state.targetReady) return;
  for (let count = 0; count < command.quantum; count += 1) {
    const next = state.admitted.get(state.appliedEventSeq + 1n);
    if (next === undefined) return;
    if (next.ptyOffset !== state.appliedOffset) throw new Error("invalid reference fixture offset");
    if (next.kind === "pty") {
      state.effects.push({ type: "pty", payload: [...next.payload] });
      state.appliedOffset += BigInt(next.payload.length);
    } else {
      state.effects.push({ type: "resize", resize: { ...next.resize } });
    }
    state.appliedEventSeq = next.eventSeq;
  }
}

function expectedReceipt(state: ReferenceState, lane: DeliveryLane): DeliveryReceived | null {
  const progress = state.laneProgress[lane];
  if (!state.started || progress.ordinal === 0n) return null;
  return {
    type: "delivery-received",
    deliveryGeneration: DELIVERY_GENERATION.toString(),
    lane,
    contiguousDeliveryOrdinal: progress.ordinal.toString(),
    cumulativeEncodedBytes: progress.bytes.toString(),
  };
}

function assertReference(
  assembler: RecoveryAssembler,
  replica: FakeReplica,
  reference: ReferenceState,
): void {
  expect(replica.effects).toEqual(reference.effects);
  expect(assembler.latestReceipts).toEqual({
    live: expectedReceipt(reference, "live"),
    recovery: expectedReceipt(reference, "recovery"),
  });
  if (!reference.started || !reference.targetReady) {
    expect(assembler.replicaApplied).toBeNull();
    return;
  }
  expect(assembler.replicaApplied).toEqual({
    type: "replica-applied",
    deliveryGeneration: DELIVERY_GENERATION.toString(),
    authorityCursor: {
      sessionEpoch: "7",
      eventSeq: reference.appliedEventSeq.toString(),
      nextPtyOffset: reference.appliedOffset.toString(),
    },
  });
  const eligible = reference.done && reference.appliedEventSeq >= 12n;
  if (eligible) {
    expect(assembler.handoff?.cursor).toEqual(
      replicaCursor(reference.appliedEventSeq, reference.appliedOffset),
    );
  } else {
    expect(assembler.handoff).toBeNull();
  }
}

function driveWarmToHandoff(includeLive = true) {
  const { assembler, replica } = createWarmAssembler({
    limits: { maxApplyFramesPerCall: 8 },
  });
  const records = fixture();
  expect(assembler.acceptStart(warmStart(), 0)).toBe(true);
  let now = 1;
  for (const record of records.recovery) {
    expect(assembler.acceptEnvelope(record.raw, now++)).toBe(true);
  }
  if (includeLive) {
    for (const record of records.live) {
      expect(assembler.acceptEnvelope(record.raw, now++)).toBe(true);
    }
  }
  expect(assembler.continueApply(now++)).toBe(includeLive ? 4 : 2);
  expect(assembler.state).toBe("handoff-eligible");
  return { assembler, now, records, replica };
}

describe("RecoveryAssembler", () => {
  it("matches an independent continuous-prefix reducer for every lane-order-preserving merge", () => {
    const records = fixture();
    const merges = orderedMerges(records.recovery, records.live);
    expect(merges).toHaveLength(10);

    for (const merged of merges) {
      for (let startIndex = 0; startIndex <= merged.length; startIndex += 1) {
        const { assembler, replica } = createWarmAssembler();
        const reference = referenceState();
        let now = 0;

        for (let index = 0; index <= merged.length; index += 1) {
          if (index === startIndex) {
            expect(assembler.acceptStart(warmStart(), now++)).toBe(true);
            reduceReference(reference, { type: "start" });
            reduceReference(reference, { type: "target-ready" });
            assertReference(assembler, replica, reference);
          }
          if (index === merged.length) continue;
          const record = merged[index]!;
          expect(assembler.acceptEnvelope(record.raw, now++)).toBe(true);
          reduceReference(reference, { type: "admit", record });
          expect(assembler.continueApply(now++)).toBeGreaterThanOrEqual(0);
          reduceReference(reference, { type: "continue", quantum: 2 });
          assertReference(assembler, replica, reference);
        }

        while (assembler.continueApply(now++) > 0) {
          reduceReference(reference, { type: "continue", quantum: 2 });
          assertReference(assembler, replica, reference);
        }
        reduceReference(reference, { type: "continue", quantum: 2 });
        assertReference(assembler, replica, reference);
        expect(reference.appliedEventSeq).toBe(14n);
        expect(reference.appliedOffset).toBe(103n);
        expect(assembler.handoff).toMatchObject({ mode: "warm", candidate: replica });
      }
    }
  });

  it("receipts pre-start data only after a cold start and applies it only after candidate install", () => {
    const assembler = createColdAssembler({ limits: { maxApplyFramesPerCall: 2 } });
    const records = fixture();
    let now = 0;
    const arrival = [
      records.live[0]!,
      records.recovery[0]!,
      records.live[1]!,
      records.recovery[1]!,
      records.recovery[2]!,
    ];
    for (const record of arrival) expect(assembler.acceptEnvelope(record.raw, now++)).toBe(true);

    expect(assembler.latestReceipts).toEqual({ live: null, recovery: null });
    expect(assembler.replicaApplied).toBeNull();
    expect(assembler.acceptStart(coldStart(), now++)).toBe(true);
    expect(assembler.latestReceipts.live?.contiguousDeliveryOrdinal).toBe("2");
    expect(assembler.latestReceipts.recovery?.contiguousDeliveryOrdinal).toBe("3");
    expect(assembler.continueApply(now++)).toBe(0);
    expect(assembler.replicaApplied).toBeNull();

    const candidate = new FakeReplica();
    expect(assembler.installSnapshotCandidate(candidateIdentity(), candidate, now++)).toBe(true);
    expect(assembler.replicaApplied?.authorityCursor).toEqual(BASE);
    expect(assembler.continueApply(now++)).toBe(2);
    expect(assembler.replicaApplied?.authorityCursor).toEqual(COMMITTED);
    expect(assembler.continueApply(now++)).toBe(2);
    expect(assembler.replicaApplied?.authorityCursor).toEqual({
      sessionEpoch: "7",
      eventSeq: "14",
      nextPtyOffset: "103",
    });
    expect(assembler.handoff).toMatchObject({
      candidate,
      cursor: replicaCursor(14n, 103n),
      mode: "cold",
    });
  });

  it("honors apply quantum and can hand off beyond committed-through H", () => {
    const { assembler, replica } = createWarmAssembler({
      limits: { maxApplyFramesPerCall: 2 },
    });
    const records = fixture();
    expect(assembler.acceptStart(warmStart(), 0)).toBe(true);
    let now = 1;
    for (const record of [...records.live, ...records.recovery]) {
      expect(assembler.acceptEnvelope(record.raw, now++)).toBe(true);
    }

    expect(assembler.continueApply(now++)).toBe(2);
    expect(replica.effects).toHaveLength(2);
    expect(assembler.handoff?.cursor).toEqual(replicaCursor(12n, 102n));
    expect(assembler.continueApply(now++)).toBe(2);
    expect(replica.effects).toHaveLength(4);
    expect(assembler.handoff?.cursor).toEqual(replicaCursor(14n, 103n));
  });

  it("copies raw input and accepts exact same-ordinal retries without duplicate effects", () => {
    const { assembler, replica } = createWarmAssembler();
    const records = fixture();
    const mutable = records.recovery[0]!.raw.slice();
    expect(assembler.acceptEnvelope(mutable, 0)).toBe(true);
    mutable.fill(0);
    expect(assembler.acceptEnvelope(records.recovery[0]!.raw, 1)).toBe(true);
    expect(assembler.acceptEnvelope(records.recovery[1]!.raw, 2)).toBe(true);
    expect(assembler.acceptEnvelope(records.recovery[2]!.raw, 3)).toBe(true);
    expect(assembler.acceptStart(warmStart(), 4)).toBe(true);
    expect(assembler.continueApply(5)).toBe(2);
    expect(replica.effects[0]).toEqual({ type: "pty", payload: [0x61, 0x62] });
    expect(assembler.acceptEnvelope(records.recovery[0]!.raw, 6)).toBe(true);
    expect(assembler.continueApply(7)).toBe(0);
    expect(replica.effects).toHaveLength(2);
  });

  it("treats a divergent same-ordinal retry, cross-lane duplicate, and new-ordinal duplicate as conflicts", () => {
    const original = recordsForLane("recovery", [RECOVERY_MUTATIONS[0]!])[0]!;
    const divergent = recordsForLane("recovery", [
      { ...RECOVERY_MUTATIONS[0]!, kind: "pty", payload: [0x78, 0x79] },
    ])[0]!;
    const liveDuplicate = recordsForLane("live", [RECOVERY_MUTATIONS[0]!])[0]!;
    const newOrdinalDuplicate = recordsForLane("recovery", [
      RECOVERY_MUTATIONS[0]!,
      RECOVERY_MUTATIONS[0]!,
    ])[1]!;

    for (const conflicting of [divergent, liveDuplicate, newOrdinalDuplicate]) {
      const assembler = createColdAssembler();
      expect(assembler.acceptEnvelope(original.raw, 0)).toBe(true);
      expect(assembler.acceptEnvelope(conflicting.raw, 1)).toBe(false);
      expect(assembler.resetResult).toMatchObject({ reason: "protocol-conflict" });
    }
  });

  it("binds immutable start identity and exact generation, stream, ordinal, and byte progress", () => {
    const exactStart = createWarmAssembler();
    expect(exactStart.assembler.acceptStart(warmStart(), 0)).toBe(true);
    expect(exactStart.assembler.acceptStart(warmStart(), 1)).toBe(true);
    expect(
      exactStart.assembler.acceptStart({ ...warmStart(), recoveryId: "recovery_BBBBBBBBBBB" }, 2),
    ).toBe(false);
    expect(exactStart.assembler.resetResult?.reason).toBe("protocol-conflict");
    expect(exactStart.replica.effects).toEqual([]);

    const payload = canonicalMutation(RECOVERY_MUTATIONS[0]!);
    const encodedBytes = BigInt(DELIVERY_ENVELOPE_HEADER_BYTES + payload.byteLength);
    const invalidEnvelopes = [
      encodeDeliveryEnvelope({
        lane: "recovery",
        deliveryGeneration: DELIVERY_GENERATION + 1n,
        deliveryOrdinal: 1n,
        cumulativeEncodedBytes: encodedBytes,
        streamId: STREAM_ID,
        payload,
      }),
      encodeDeliveryEnvelope({
        lane: "recovery",
        deliveryGeneration: DELIVERY_GENERATION,
        deliveryOrdinal: 1n,
        cumulativeEncodedBytes: encodedBytes,
        streamId: STREAM_ID + 1,
        payload,
      }),
      encodeDeliveryEnvelope({
        lane: "recovery",
        deliveryGeneration: DELIVERY_GENERATION,
        deliveryOrdinal: 2n,
        cumulativeEncodedBytes: encodedBytes * 2n,
        streamId: STREAM_ID,
        payload,
      }),
    ];
    for (const invalid of invalidEnvelopes) {
      const assembler = createColdAssembler();
      expect(assembler.acceptEnvelope(invalid, 0)).toBe(false);
      expect(assembler.resetResult?.reason).toBe("protocol-conflict");
    }

    const first = recordsForLane("recovery", [RECOVERY_MUTATIONS[0]!])[0]!;
    const secondPayload = canonicalMutation(RECOVERY_MUTATIONS[1]!);
    const invalidCumulativeBytes = encodeDeliveryEnvelope({
      lane: "recovery",
      deliveryGeneration: DELIVERY_GENERATION,
      deliveryOrdinal: 2n,
      cumulativeEncodedBytes:
        first.cumulativeEncodedBytes +
        BigInt(DELIVERY_ENVELOPE_HEADER_BYTES + secondPayload.byteLength) +
        1n,
      streamId: STREAM_ID,
      payload: secondPayload,
    });
    const bytes = createColdAssembler();
    expect(bytes.acceptEnvelope(first.raw, 0)).toBe(true);
    expect(bytes.acceptEnvelope(invalidCumulativeBytes, 1)).toBe(false);
    expect(bytes.resetResult?.reason).toBe("protocol-conflict");
  });

  it("rejects incomplete Done and every recovery record after exact Done", () => {
    const incomplete = recordsForLane("recovery", [RECOVERY_MUTATIONS[0]!, "done"]);
    const first = createColdAssembler();
    expect(first.acceptStart(coldStart(), 0)).toBe(true);
    expect(first.acceptEnvelope(incomplete[0]!.raw, 1)).toBe(true);
    expect(first.acceptEnvelope(incomplete[1]!.raw, 2)).toBe(false);
    expect(first.handoff).toBeNull();
    expect(first.resetResult?.reason).toBe("protocol-conflict");

    const afterDone = recordsForLane("recovery", [
      ...RECOVERY_MUTATIONS,
      "done",
      LIVE_MUTATIONS[0]!,
    ]);
    const second = createColdAssembler();
    expect(second.acceptStart(coldStart(), 0)).toBe(true);
    expect(second.acceptEnvelope(afterDone[0]!.raw, 1)).toBe(true);
    expect(second.acceptEnvelope(afterDone[1]!.raw, 2)).toBe(true);
    expect(second.acceptEnvelope(afterDone[2]!.raw, 3)).toBe(true);
    expect(second.acceptEnvelope(afterDone[3]!.raw, 4)).toBe(false);
    expect(second.resetResult?.reason).toBe("protocol-conflict");
  });

  it("rejects mutations outside the immutable recovery and live authority ranges", () => {
    const recoveryPastH = recordsForLane("recovery", [LIVE_MUTATIONS[0]!])[0]!;
    const recovery = createColdAssembler();
    expect(recovery.acceptStart(coldStart(), 0)).toBe(true);
    expect(recovery.acceptEnvelope(recoveryPastH.raw, 1)).toBe(false);
    expect(recovery.resetResult?.reason).toBe("protocol-conflict");

    const liveAtH = recordsForLane("live", [RECOVERY_MUTATIONS[1]!])[0]!;
    const live = createColdAssembler();
    expect(live.acceptStart(coldStart(), 0)).toBe(true);
    expect(live.acceptEnvelope(liveAtH.raw, 1)).toBe(false);
    expect(live.resetResult?.reason).toBe("protocol-conflict");
  });

  it("admits pre-start future bytes without effects and fails closed when their offset becomes checkable", () => {
    const wrongFuture = recordsForLane("live", [
      { kind: "pty", eventSeq: 13n, ptyOffset: 999n, payload: [0x63] },
    ])[0]!;
    const { assembler, replica } = createWarmAssembler();
    expect(assembler.acceptEnvelope(wrongFuture.raw, 0)).toBe(true);
    expect(replica.effects).toEqual([]);
    expect(assembler.acceptStart(warmStart(), 1)).toBe(false);
    expect(replica.effects).toEqual([]);
    expect(assembler.resetResult).toMatchObject({
      reason: "protocol-conflict",
      reusableWarmCursor: BASE,
      warmTargetTainted: false,
    });
  });

  it("enforces unique-record byte/frame caps and gap span at exact literal boundaries", () => {
    const records = fixture();
    const one = records.recovery[0]!;
    expect(one.raw.byteLength).toBe(90);
    const atCap = createColdAssembler({
      limits: { maxOwnedBytes: 90, maxOwnedFrames: 1 },
    });
    expect(atCap.acceptEnvelope(one.raw, 0)).toBe(true);
    expect(atCap.acceptEnvelope(one.raw, 1)).toBe(true);
    expect(atCap.state).toBe("awaiting-start");

    const byteOverflow = createColdAssembler({
      limits: { maxOwnedBytes: 90, maxOwnedFrames: 2 },
    });
    expect(byteOverflow.acceptEnvelope(one.raw, 0)).toBe(true);
    expect(byteOverflow.acceptEnvelope(records.live[0]!.raw, 1)).toBe(false);
    expect(byteOverflow.resetResult?.reason).toBe("capacity-exceeded");

    const frameOverflow = createColdAssembler({
      limits: { maxOwnedBytes: 64 * 1024, maxOwnedFrames: 1 },
    });
    expect(frameOverflow.acceptEnvelope(one.raw, 0)).toBe(true);
    expect(frameOverflow.acceptEnvelope(records.live[0]!.raw, 1)).toBe(false);
    expect(frameOverflow.resetResult?.reason).toBe("capacity-exceeded");

    // Gap span is measured from the available continuous prefix, not from the fixed R..H plan.
    const exactGap = createColdAssembler({ limits: { maxGapSpan: 2n } });
    expect(exactGap.acceptStart(coldStart(), 0)).toBe(true);
    expect(exactGap.acceptEnvelope(records.recovery[0]!.raw, 1)).toBe(true);
    expect(exactGap.acceptEnvelope(records.live[0]!.raw, 2)).toBe(true);
    expect(exactGap.state).toBe("restoring");
    expect(exactGap.acceptEnvelope(records.live[1]!.raw, 3)).toBe(false);
    expect(exactGap.resetResult?.reason).toBe("gap-span-exceeded");

    const pendingGap = createColdAssembler({ limits: { maxGapSpan: 2n } });
    expect(pendingGap.acceptStart(coldStart(), 0)).toBe(true);
    expect(pendingGap.acceptEnvelope(records.live[0]!.raw, 1)).toBe(false);
    expect(pendingGap.resetResult?.reason).toBe("gap-span-exceeded");
  });

  it("enforces independent no-progress and generation deadlines without gap spam renewal", () => {
    const noProgress = createColdAssembler({
      limits: { noProgressDeadlineMs: 10, recoveryDeadlineMs: 100 },
    });
    expect(noProgress.acceptStart(coldStart(), 0)).toBe(true);
    expect(noProgress.checkDeadlines(9)).toBe(true);
    expect(noProgress.checkDeadlines(10)).toBe(false);
    expect(noProgress.resetResult?.reason).toBe("no-progress-deadline");

    const generation = createColdAssembler({
      limits: { noProgressDeadlineMs: 100, recoveryDeadlineMs: 20 },
    });
    expect(generation.acceptStart(coldStart(), 0)).toBe(true);
    expect(generation.checkDeadlines(19)).toBe(true);
    expect(generation.checkDeadlines(20)).toBe(false);
    expect(generation.resetResult?.reason).toBe("generation-deadline");

    const spam = createColdAssembler({
      limits: { maxGapSpan: 4n, noProgressDeadlineMs: 10, recoveryDeadlineMs: 100 },
    });
    const live = fixture().live;
    expect(spam.acceptStart(coldStart(), 0)).toBe(true);
    expect(spam.acceptEnvelope(live[0]!.raw, 5)).toBe(true);
    expect(spam.checkDeadlines(10)).toBe(false);
    expect(spam.resetResult?.reason).toBe("no-progress-deadline");
  });

  it("exposes the exact next caller-driven deadline without extending it for non-progress", () => {
    const assembler = createColdAssembler({
      limits: { noProgressDeadlineMs: 10, recoveryDeadlineMs: 100 },
    });
    expect(assembler.nextDeadlineAtMs).toBe(10);
    expect(assembler.acceptEnvelope(fixture().live[0]!.raw, 2)).toBe(true);
    expect(assembler.nextDeadlineAtMs).toBe(10);
    expect(assembler.acceptStart(coldStart(), 3)).toBe(true);
    expect(assembler.nextDeadlineAtMs).toBe(13);
    assembler.reset("protocol-conflict");
    expect(assembler.nextDeadlineAtMs).toBeNull();
  });

  it("requires the exact Done ordinal and bytes and completes in either closure/adoption order", () => {
    for (const order of ["closure-first", "adoption-first"] as const) {
      const { assembler, now: initialNow, records } = driveWarmToHandoff(false);
      let now = initialNow;
      const closed = sourceClosed(records.recovery[2]!);
      const handoffCursor = replicaCursor(12n, 102n);
      if (order === "closure-first") {
        expect(assembler.acceptSourceClosed(closed, now++)).toBe(true);
        expect(assembler.state).toBe("handoff-eligible");
        expect(assembler.confirmHandoff(handoffCursor, now++)).toBe(true);
      } else {
        expect(assembler.confirmHandoff(handoffCursor, now++)).toBe(true);
        expect(assembler.state).toBe("adopted");
        expect(assembler.acceptSourceClosed(closed, now++)).toBe(true);
      }
      expect(assembler.state).toBe("complete");
      expect(assembler.completion).toMatchObject({
        recoveryAdopted: {
          recoveryId: RECOVERY_ID,
          replicaApplied: COMMITTED,
        },
        replicaApplied: { authorityCursor: COMMITTED },
      });
      expect(assembler.confirmHandoff(handoffCursor, now++)).toBe(true);
      expect(assembler.acceptStart(warmStart(), now++)).toBe(true);
      expect(assembler.acceptSourceClosed(closed, now++)).toBe(true);
      expect(assembler.state).toBe("complete");
    }

    for (const field of ["ordinal", "bytes"] as const) {
      const { assembler, now, records } = driveWarmToHandoff(false);
      const exact = sourceClosed(records.recovery[2]!);
      const invalid: RecoverySourceClosed =
        field === "ordinal"
          ? {
              ...exact,
              throughRecoveryOrdinal: (BigInt(exact.throughRecoveryOrdinal) + 1n).toString(),
            }
          : {
              ...exact,
              throughRecoveryCumulativeEncodedBytes: (
                BigInt(exact.throughRecoveryCumulativeEncodedBytes) + 1n
              ).toString(),
            };
      expect(assembler.acceptSourceClosed(invalid, now)).toBe(false);
      expect(assembler.resetResult?.reason).toBe("protocol-conflict");
    }
  });

  it("accepts source closure before a cold candidate and completes after the later handoff", () => {
    const assembler = createColdAssembler({ limits: { maxApplyFramesPerCall: 8 } });
    const records = fixture();
    let now = 0;
    expect(assembler.acceptStart(coldStart(), now++)).toBe(true);
    for (const record of records.recovery) {
      expect(assembler.acceptEnvelope(record.raw, now++)).toBe(true);
    }
    const closed = sourceClosed(records.recovery[2]!);
    expect(assembler.acceptSourceClosed(closed, now++)).toBe(true);
    expect(assembler.state).toBe("restoring");

    const candidate = new FakeReplica();
    expect(assembler.installSnapshotCandidate(candidateIdentity(), candidate, now++)).toBe(true);
    expect(assembler.continueApply(now++)).toBe(2);
    expect(assembler.handoff?.cursor).toEqual(replicaCursor(12n, 102n));
    expect(assembler.confirmHandoff(replicaCursor(12n, 102n), now++)).toBe(true);
    expect(assembler.state).toBe("complete");
    expect(candidate.disposeCalls).toBe(0);
  });

  it("keeps applying live data after adoption until exact closure completes the attempt", () => {
    const { assembler, replica } = createWarmAssembler({
      limits: { maxApplyFramesPerCall: 8 },
    });
    const records = fixture();
    let now = 0;
    expect(assembler.acceptStart(warmStart(), now++)).toBe(true);
    for (const record of records.recovery) {
      expect(assembler.acceptEnvelope(record.raw, now++)).toBe(true);
    }
    expect(assembler.continueApply(now++)).toBe(2);
    expect(assembler.confirmHandoff(replicaCursor(12n, 102n), now++)).toBe(true);
    expect(assembler.state).toBe("adopted");

    for (const record of records.live) {
      expect(assembler.acceptEnvelope(record.raw, now++)).toBe(true);
    }
    expect(assembler.continueApply(now++)).toBe(2);
    expect(replica.effects).toHaveLength(4);
    expect(assembler.replicaApplied?.authorityCursor).toEqual({
      sessionEpoch: "7",
      eventSeq: "14",
      nextPtyOffset: "103",
    });
    expect(assembler.recoveryAdopted?.replicaApplied).toEqual(COMMITTED);

    expect(assembler.acceptSourceClosed(sourceClosed(records.recovery[2]!), now++)).toBe(true);
    expect(assembler.state).toBe("complete");
    expect(assembler.completion?.replicaApplied.authorityCursor).toEqual({
      sessionEpoch: "7",
      eventSeq: "14",
      nextPtyOffset: "103",
    });
  });

  it("does not complete while already received live mutations still await apply", () => {
    const { assembler, replica } = createWarmAssembler({
      limits: { maxApplyFramesPerCall: 2 },
    });
    const records = fixture();
    let now = 0;
    expect(assembler.acceptStart(warmStart(), now++)).toBe(true);
    for (const record of [...records.recovery, ...records.live]) {
      expect(assembler.acceptEnvelope(record.raw, now++)).toBe(true);
    }
    expect(assembler.continueApply(now++)).toBe(2);
    expect(assembler.confirmHandoff(replicaCursor(12n, 102n), now++)).toBe(true);
    expect(assembler.acceptSourceClosed(sourceClosed(records.recovery[2]!), now++)).toBe(true);
    expect(assembler.state).toBe("adopted");
    expect(assembler.completion).toBeNull();

    expect(assembler.continueApply(now++)).toBe(2);
    expect(replica.effects).toHaveLength(4);
    expect(assembler.state).toBe("complete");
    expect(assembler.completion?.replicaApplied.authorityCursor).toEqual({
      sessionEpoch: "7",
      eventSeq: "14",
      nextPtyOffset: "103",
    });
  });

  it("treats the exact installed cold candidate as idempotent through handoff and adoption", () => {
    const assembler = createColdAssembler({ limits: { maxApplyFramesPerCall: 8 } });
    const records = fixture();
    const candidate = new FakeReplica();
    const identity = candidateIdentity();
    let now = 0;
    expect(assembler.acceptStart(coldStart(), now++)).toBe(true);
    expect(assembler.installSnapshotCandidate(identity, candidate, now++)).toBe(true);
    for (const record of records.recovery) {
      expect(assembler.acceptEnvelope(record.raw, now++)).toBe(true);
    }
    expect(assembler.continueApply(now++)).toBe(2);
    expect(assembler.state).toBe("handoff-eligible");
    expect(assembler.installSnapshotCandidate(identity, candidate, now++)).toBe(true);
    expect(assembler.confirmHandoff(replicaCursor(12n, 102n), now++)).toBe(true);
    expect(assembler.state).toBe("adopted");
    expect(assembler.installSnapshotCandidate(identity, candidate, now++)).toBe(true);
    expect(assembler.acceptSourceClosed(sourceClosed(records.recovery[2]!), now++)).toBe(true);
    expect(assembler.state).toBe("complete");
    expect(assembler.installSnapshotCandidate(identity, candidate, now++)).toBe(true);
    expect(candidate.disposeCalls).toBe(0);
  });

  it("uses the install result as an exact cold candidate ownership fence", () => {
    const first = createColdAssembler();
    const accepted = new FakeReplica();
    const rejected = new FakeReplica();
    expect(first.acceptStart(coldStart(), 0)).toBe(true);
    expect(first.installSnapshotCandidate(candidateIdentity(), accepted, 1)).toBe(true);
    expect(first.installSnapshotCandidate(candidateIdentity(), rejected, 2)).toBe(false);
    expect(first.resetResult?.reason).toBe("candidate-conflict");
    expect(accepted.disposeCalls).toBe(1);
    expect(rejected.disposeCalls).toBe(0);

    const badIdentity = createColdAssembler();
    const callerOwned = new FakeReplica("engine-other");
    expect(badIdentity.acceptStart(coldStart(), 0)).toBe(true);
    expect(badIdentity.installSnapshotCandidate(candidateIdentity(), callerOwned, 1)).toBe(false);
    expect(badIdentity.resetResult?.reason).toBe("candidate-conflict");
    expect(callerOwned.disposeCalls).toBe(0);
  });

  it("relinquishes cold ownership before rejecting a caller-confirmed wrong handoff cursor", () => {
    const assembler = createColdAssembler({ limits: { maxApplyFramesPerCall: 8 } });
    const records = fixture();
    const candidate = new FakeReplica();
    let now = 0;
    expect(assembler.acceptStart(coldStart(), now++)).toBe(true);
    expect(assembler.installSnapshotCandidate(candidateIdentity(), candidate, now++)).toBe(true);
    for (const record of records.recovery) {
      expect(assembler.acceptEnvelope(record.raw, now++)).toBe(true);
    }
    expect(assembler.continueApply(now++)).toBe(2);
    expect(assembler.confirmHandoff(replicaCursor(12n, 999n), now++)).toBe(false);
    expect(assembler.resetResult?.reason).toBe("ownership-uncertain");
    expect(candidate.disposeCalls).toBe(0);
    assembler.close();
    expect(candidate.disposeCalls).toBe(0);
  });

  it("relinquishes only the exact current cold handoff after an uncertain host outcome", () => {
    const assembler = createColdAssembler({ limits: { maxApplyFramesPerCall: 8 } });
    const records = fixture();
    const candidate = new FakeReplica();
    let now = 0;
    expect(assembler.acceptStart(coldStart(), now++)).toBe(true);
    expect(assembler.installSnapshotCandidate(candidateIdentity(), candidate, now++)).toBe(true);
    for (const record of records.recovery) {
      expect(assembler.acceptEnvelope(record.raw, now++)).toBe(true);
    }
    expect(assembler.continueApply(now++)).toBe(2);
    expect(assembler.abandonHandoffOutcomeUncertain(now++)).toBe(true);
    expect(assembler.resetResult?.reason).toBe("ownership-uncertain");
    expect(assembler.recoveryAdopted).toBeNull();
    expect(candidate.disposeCalls).toBe(0);
    expect(assembler.abandonHandoffOutcomeUncertain(now++)).toBe(false);
    assembler.close();
    expect(candidate.disposeCalls).toBe(0);

    const warm = driveWarmToHandoff(false);
    expect(warm.assembler.abandonHandoffOutcomeUncertain(warm.now)).toBe(false);
    expect(warm.assembler.state).toBe("handoff-eligible");
    expect(warm.replica.disposeCalls).toBe(0);
  });

  it("relinquishes a visible cold handoff before deadline checks in both outcome paths", () => {
    for (const outcome of ["confirm", "uncertain"] as const) {
      const assembler = createColdAssembler({
        limits: {
          maxApplyFramesPerCall: 8,
          noProgressDeadlineMs: 10,
          recoveryDeadlineMs: 10,
        },
      });
      const records = fixture();
      const candidate = new FakeReplica();
      expect(assembler.acceptStart(coldStart(), 0)).toBe(true);
      expect(assembler.installSnapshotCandidate(candidateIdentity(), candidate, 0)).toBe(true);
      for (const record of records.recovery) {
        expect(assembler.acceptEnvelope(record.raw, 0)).toBe(true);
      }
      expect(assembler.continueApply(0)).toBe(2);
      expect(assembler.state).toBe("handoff-eligible");

      if (outcome === "confirm") {
        expect(assembler.confirmHandoff(replicaCursor(12n, 102n), 10)).toBe(false);
        expect(assembler.resetResult?.reason).toBe("generation-deadline");
      } else {
        expect(assembler.abandonHandoffOutcomeUncertain(10)).toBe(true);
        expect(assembler.resetResult?.reason).toBe("ownership-uncertain");
      }
      expect(candidate.disposeCalls).toBe(0);
      assembler.close();
      expect(candidate.disposeCalls).toBe(0);
    }
  });

  it("taints a mutated warm target, disposes an owned cold candidate once, and ignores late actions", () => {
    const records = fixture();
    const warm = createWarmAssembler({ limits: { maxApplyFramesPerCall: 1 } });
    expect(warm.assembler.acceptStart(warmStart(), 0)).toBe(true);
    expect(warm.assembler.acceptEnvelope(records.recovery[0]!.raw, 1)).toBe(true);
    expect(warm.assembler.continueApply(2)).toBe(1);
    const divergent = recordsForLane("recovery", [
      { ...RECOVERY_MUTATIONS[0]!, kind: "pty", payload: [0x78, 0x79] },
    ])[0]!;
    expect(warm.assembler.acceptEnvelope(divergent.raw, 3)).toBe(false);
    expect(warm.assembler.resetResult).toEqual({
      reason: "protocol-conflict",
      reusableWarmCursor: null,
      warmTargetTainted: true,
    });
    expect(warm.replica.disposeCalls).toBe(0);

    const cold = createColdAssembler({ limits: { maxApplyFramesPerCall: 1 } });
    const candidate = new FakeReplica();
    expect(cold.acceptStart(coldStart(), 0)).toBe(true);
    expect(cold.installSnapshotCandidate(candidateIdentity(), candidate, 1)).toBe(true);
    expect(cold.acceptEnvelope(records.recovery[0]!.raw, 2)).toBe(true);
    expect(cold.continueApply(3)).toBe(1);
    expect(cold.acceptEnvelope(divergent.raw, 4)).toBe(false);
    expect(cold.resetResult).toMatchObject({
      reason: "protocol-conflict",
      warmTargetTainted: false,
    });
    expect(candidate.disposeCalls).toBe(1);

    const effects = [...candidate.effects];
    cold.reset("ownership-uncertain");
    cold.close();
    expect(cold.acceptEnvelope(records.live[0]!.raw, 5)).toBe(false);
    expect(cold.acceptSourceClosed(sourceClosed(records.recovery[2]!), 6)).toBe(false);
    expect(cold.continueApply(7)).toBe(0);
    expect(candidate.disposeCalls).toBe(1);
    expect(candidate.effects).toEqual(effects);
    expect(cold.state).toBe("closed");
  });

  it("taints warm and disposes cold when applying a front mutation fails", () => {
    const records = fixture();
    const warmReplica = new FakeReplica(ENGINE_ID, 0);
    const warm = createWarmAssembler({ replica: warmReplica });
    expect(warm.assembler.acceptStart(warmStart(), 0)).toBe(true);
    expect(warm.assembler.acceptEnvelope(records.recovery[0]!.raw, 1)).toBe(true);
    expect(warm.assembler.continueApply(2)).toBe(0);
    expect(warm.assembler.resetResult).toMatchObject({
      reason: "apply-failed",
      warmTargetTainted: true,
    });
    expect(warmReplica.disposeCalls).toBe(0);

    const cold = createColdAssembler();
    const coldReplica = new FakeReplica(ENGINE_ID, 0);
    expect(cold.acceptStart(coldStart(), 0)).toBe(true);
    expect(cold.installSnapshotCandidate(candidateIdentity(), coldReplica, 1)).toBe(true);
    expect(cold.acceptEnvelope(records.recovery[0]!.raw, 2)).toBe(true);
    expect(cold.continueApply(3)).toBe(0);
    expect(cold.resetResult?.reason).toBe("apply-failed");
    expect(coldReplica.disposeCalls).toBe(1);
  });

  it("keeps a partial post-adoption apply failure terminally reset", () => {
    const replica = new FakeReplica(ENGINE_ID, 3);
    const { assembler } = createWarmAssembler({
      replica,
      limits: { maxApplyFramesPerCall: 2 },
    });
    const records = fixture();
    let now = 0;
    expect(assembler.acceptStart(warmStart(), now++)).toBe(true);
    for (const record of [...records.recovery, ...records.live]) {
      expect(assembler.acceptEnvelope(record.raw, now++)).toBe(true);
    }
    expect(assembler.continueApply(now++)).toBe(2);
    expect(assembler.confirmHandoff(replicaCursor(12n, 102n), now++)).toBe(true);
    expect(assembler.acceptSourceClosed(sourceClosed(records.recovery[2]!), now++)).toBe(true);

    expect(assembler.continueApply(now++)).toBe(1);
    expect(replica.effects).toHaveLength(3);
    expect(assembler.state).toBe("reset");
    expect(assembler.resetResult).toMatchObject({
      reason: "apply-failed",
      reusableWarmCursor: null,
      warmTargetTainted: true,
    });
    expect(assembler.completion).toBeNull();
  });
});
