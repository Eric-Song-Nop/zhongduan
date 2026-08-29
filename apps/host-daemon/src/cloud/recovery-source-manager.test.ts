import {
  DataFrameKind,
  decodeDataFrame,
  decodeRecoveryStartFence,
  type AuthorityCursor,
  type RecoveryV3HostPrepare,
  type RecoveryV3HostRoutingIdentity,
  type ResizePayload,
} from "@zhongduan/protocol";
import { describe, expect, it } from "vitest";

import { FakeTerminalAuthority } from "../fake-terminal-authority";
import { EventJournal } from "../journal";
import type { PtyProcess } from "../pty-process";
import { TerminalSession, type PreparedRecoveryGap } from "../session";
import { CanonicalPublisher } from "./canonical-publisher";
import {
  RecoverySourceManager,
  type RecoverySourceManagerLimits,
  type RecoverySourcePrepareResult,
} from "./recovery-source-manager";

const RECOVERY_ID = "recovery-source-00000001";
const CONNECTION_ID = "connection-source-000001";
const EMPTY_BASE: AuthorityCursor = {
  sessionEpoch: "7",
  eventSeq: "0",
  nextPtyOffset: "0",
};
const DEFAULT_LIMITS: RecoverySourceManagerLimits = {
  maxCanonicalBytesPerSource: 256 * 1024,
  maxCanonicalFramesPerSource: 512,
  maxOwnedRecords: 1_024,
  maxOwnedWireBytes: 2 * 1024 * 1024,
  maxSources: 32,
  noProgressDeadlineMs: 1_000,
  recoveryDeadlineMs: 10_000,
};

class ManualPty implements PtyProcess {
  readonly pid = 42;

  #dataListener: ((data: Uint8Array) => void) | undefined;
  #exitListener: ((exitCode: number, signal: number) => void) | undefined;

  onData(listener: (data: Uint8Array) => void): () => void {
    this.#dataListener = listener;
    return () => {
      this.#dataListener = undefined;
    };
  }

  onExit(listener: (exitCode: number, signal: number) => void): () => void {
    this.#exitListener = listener;
    return () => {
      this.#exitListener = undefined;
    };
  }

  write(_data: Uint8Array): void {}

  resize(_dimensions: ResizePayload): void {}

  kill(): void {
    this.#exitListener?.(0, 0);
  }

  emit(data: Uint8Array): void {
    this.#dataListener?.(data);
  }
}

function createHarness(limitOverrides: Partial<RecoverySourceManagerLimits> = {}) {
  const clock = { now: 0 };
  const pty = new ManualPty();
  const session = new TerminalSession({
    authority: new FakeTerminalAuthority(),
    journal: new EventJournal(),
    monotonicNow: () => clock.now,
    pty,
    sessionEpoch: 7n,
  });
  const manager = new RecoverySourceManager({
    limits: { ...DEFAULT_LIMITS, ...limitOverrides },
    monotonicNow: () => clock.now,
    session,
  });
  return { clock, manager, pty, session };
}

function prepareFor(
  streamId: number,
  deliveryGeneration = "1",
  base: AuthorityCursor = EMPTY_BASE,
): RecoveryV3HostPrepare {
  return {
    type: "recovery-prepare",
    recoveryId: RECOVERY_ID,
    connectionId: CONNECTION_ID,
    streamId,
    deliveryGeneration,
    engineId: "fake-terminal-authority/v1",
    base,
    source: { kind: "warm" },
  };
}

function routing(input: RecoveryV3HostPrepare): RecoveryV3HostRoutingIdentity {
  return {
    recoveryId: input.recoveryId,
    connectionId: input.connectionId,
    streamId: input.streamId,
    deliveryGeneration: input.deliveryGeneration,
  };
}

function expectPrepared(
  result: RecoverySourcePrepareResult,
): asserts result is Extract<RecoverySourcePrepareResult, { status: "prepared" }> {
  expect(result.status).toBe("prepared");
  if (result.status !== "prepared") throw new Error("expected a prepared recovery source");
}

function expectCapacity(result: RecoverySourcePrepareResult): void {
  expect(result).toMatchObject({
    status: "rejected",
    rejection: { reason: "capacity-exceeded" },
  });
}

/** Independent wire oracle: DeliveryEnvelopeV3 has a literal 40-byte fixed header. */
function inspectEnvelope(encoded: Uint8Array) {
  const view = new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength);
  const payloadLength = view.getUint32(36, true);
  expect(encoded.byteLength).toBe(40 + payloadLength);
  return {
    cumulativeEncodedBytes: view.getBigUint64(24, true),
    deliveryOrdinal: view.getBigUint64(16, true),
    payload: encoded.slice(40),
  };
}

describe("RecoverySourceManager", () => {
  it("owns an R=H Done record with literal ordinal 1 and 88 cumulative wire bytes", async () => {
    const { manager, session } = createHarness();
    const owner = {};
    const prepare = prepareFor(1);
    const fences: Uint8Array[] = [];

    const result = await manager.prepare(owner, prepare, (encoded) => {
      fences.push(encoded.slice());
      return true;
    });
    expectPrepared(result);

    expect(decodeRecoveryStartFence(fences[0]!)).toMatchObject({
      base: EMPTY_BASE,
      committedThrough: EMPTY_BASE,
      liveFloor: { sessionEpoch: "7", nextEventSeq: "1", nextPtyOffset: "0" },
    });
    expect(manager.counters).toEqual({
      ownedRecords: 1,
      ownedWireBytes: 88,
      pendingSources: 0,
      sources: 1,
    });
    expect(
      manager.drainGranted(owner, routing(prepare), { maxRecords: 1, maxWireBytes: 88 }, () => {
        throw new Error("must not send before start-ready");
      }),
    ).toEqual({ records: 0, wireBytes: 0 });

    expect(
      manager.startReady(owner, {
        type: "recovery-start-ready",
        ...routing(prepare),
        committedThrough: result.committedThrough,
        cumulativeGrantedEncodedBytes: "0",
      }),
    ).toBe(true);
    expect(
      manager.grant(owner, {
        type: "recovery-source-grant",
        ...routing(prepare),
        cumulativeGrantedEncodedBytes: "87",
      }),
    ).toBe(true);
    expect(
      manager.drainGranted(owner, routing(prepare), { maxRecords: 1, maxWireBytes: 88 }, () => {
        throw new Error("must not split a record at a partial grant");
      }),
    ).toEqual({ records: 0, wireBytes: 0 });

    expect(
      manager.grant(owner, {
        type: "recovery-source-grant",
        ...routing(prepare),
        cumulativeGrantedEncodedBytes: "88",
      }),
    ).toBe(true);
    const sent: Uint8Array[] = [];
    expect(
      manager.drainGranted(owner, routing(prepare), { maxRecords: 1, maxWireBytes: 88 }, (bytes) =>
        sent.push(bytes),
      ),
    ).toEqual({ records: 1, wireBytes: 88 });
    const envelope = inspectEnvelope(sent[0]!);
    expect(envelope).toMatchObject({ deliveryOrdinal: 1n, cumulativeEncodedBytes: 88n });
    expect(decodeDataFrame(envelope.payload)).toMatchObject({
      kind: DataFrameKind.ReplayCommit,
      sessionEpoch: 7n,
      eventSeq: 0n,
      ptyOffset: 0n,
      payload: new Uint8Array(),
    });
    expect(session.cursor).toEqual({ sessionEpoch: 7n, lastEventSeq: 0n, nextPtyOffset: 0n });
  });

  it("enforces per-source and session caps at cap + 1 and rolls back a refused fence", async () => {
    const frameLimited = createHarness({ maxCanonicalFramesPerSource: 0 });
    frameLimited.pty.emit(Uint8Array.of(0x41));
    expectCapacity(await frameLimited.manager.prepare({}, prepareFor(1), () => true));
    expect(frameLimited.manager.counters).toMatchObject({ ownedRecords: 0, ownedWireBytes: 0 });

    const byteLimited = createHarness({ maxCanonicalBytesPerSource: 48 });
    byteLimited.pty.emit(Uint8Array.of(0x41));
    expectCapacity(await byteLimited.manager.prepare({}, prepareFor(1), () => true));

    const wireLimited = createHarness({ maxOwnedWireBytes: 87 });
    expectCapacity(await wireLimited.manager.prepare({}, prepareFor(1), () => true));
    expect(wireLimited.manager.counters).toEqual({
      ownedRecords: 0,
      ownedWireBytes: 0,
      pendingSources: 0,
      sources: 0,
    });

    const recordLimited = createHarness({
      maxOwnedRecords: 1,
      maxOwnedWireBytes: 176,
      maxSources: 2,
    });
    expectPrepared(await recordLimited.manager.prepare({}, prepareFor(1), () => true));
    expectCapacity(await recordLimited.manager.prepare({}, prepareFor(2), () => true));
    expect(recordLimited.manager.counters).toMatchObject({ ownedRecords: 1, ownedWireBytes: 88 });

    const aggregateByteLimited = createHarness({
      maxOwnedRecords: 2,
      maxOwnedWireBytes: 175,
      maxSources: 2,
    });
    expectPrepared(await aggregateByteLimited.manager.prepare({}, prepareFor(1), () => true));
    expectCapacity(await aggregateByteLimited.manager.prepare({}, prepareFor(2), () => true));

    const refused = createHarness();
    const refusedResult = await refused.manager.prepare({}, prepareFor(1), () => false);
    expect(refusedResult).toEqual({ status: "unavailable", reason: "fence-unavailable" });
    expect(refused.manager.counters).toEqual({
      ownedRecords: 0,
      ownedWireBytes: 0,
      pendingSources: 0,
      sources: 0,
    });
  });

  it("freezes the actor cut, retries thrown sends byte-identically, and closes only on sent Done", async () => {
    const { manager, pty, session } = createHarness();
    const owner = {};
    const prepare = prepareFor(1);
    const canonicalSent: Uint8Array[] = [];
    const publisherFailures: string[] = [];
    const publisher = new CanonicalPublisher({
      canInterruptRecovery: () => false,
      onFailure: (reason) => publisherFailures.push(reason),
      onIngress: () => {},
      onRecoveryPressure: () => {},
      sendData: (encoded) => canonicalSent.push(encoded.slice()),
      session,
      yieldIo: () => Promise.resolve(),
    });
    const baseline = publisher.prepare();
    pty.emit(Uint8Array.of(0x41));

    const result = await manager.prepare(owner, prepare, (encoded) => {
      const accepted = publisher.tryEnqueueRecoveryStartFence(encoded);
      pty.emit(Uint8Array.of(0x42));
      return accepted;
    });
    expectPrepared(result);
    expect(result.committedThrough).toEqual({
      sessionEpoch: "7",
      eventSeq: "1",
      nextPtyOffset: "1",
    });
    publisher.activate(baseline);
    expect(publisherFailures).toEqual([]);
    expect(canonicalSent.map((encoded) => decodeDataFrame(encoded).kind)).toEqual([
      DataFrameKind.PtyOutput,
      DataFrameKind.DeliveryBarrier,
      DataFrameKind.PtyOutput,
    ]);
    expect(canonicalSent.map((encoded) => decodeDataFrame(encoded).eventSeq)).toEqual([1n, 1n, 2n]);
    expect(decodeRecoveryStartFence(canonicalSent[1]!)).toMatchObject({
      committedThrough: result.committedThrough,
      liveFloor: { sessionEpoch: "7", nextEventSeq: "2", nextPtyOffset: "1" },
    });
    expect(manager.counters).toMatchObject({ ownedRecords: 2, ownedWireBytes: 177 });

    expect(
      manager.startReady(owner, {
        type: "recovery-start-ready",
        ...routing(prepare),
        committedThrough: result.committedThrough,
        cumulativeGrantedEncodedBytes: "89",
      }),
    ).toBe(true);
    expect(
      manager.received(owner, {
        type: "recovery-source-received",
        ...routing(prepare),
        lane: "recovery",
        contiguousDeliveryOrdinal: "2",
        cumulativeEncodedBytes: "177",
      }),
    ).toEqual({ status: "invalid", reason: "beyond-sent" });

    let firstAttempt: Uint8Array | undefined;
    expect(() =>
      manager.drainGranted(
        owner,
        routing(prepare),
        { maxRecords: 1, maxWireBytes: 89 },
        (bytes) => {
          firstAttempt = bytes.slice();
          bytes.fill(0xff);
          throw new Error("transport refused the record");
        },
      ),
    ).toThrow("transport refused the record");

    const retry: Uint8Array[] = [];
    expect(
      manager.drainGranted(
        owner,
        routing(prepare),
        { maxRecords: 1, maxWireBytes: 89 },
        (bytes) => {
          retry.push(bytes);
        },
      ),
    ).toEqual({ records: 1, wireBytes: 89 });
    expect(retry[0]).toEqual(firstAttempt);
    const first = inspectEnvelope(retry[0]!);
    expect(first).toMatchObject({ deliveryOrdinal: 1n, cumulativeEncodedBytes: 89n });
    expect(decodeDataFrame(first.payload)).toMatchObject({
      kind: DataFrameKind.PtyOutput,
      eventSeq: 1n,
      payload: Uint8Array.of(0x41),
    });

    expect(
      manager.received(owner, {
        type: "recovery-source-received",
        ...routing(prepare),
        lane: "recovery",
        contiguousDeliveryOrdinal: "1",
        cumulativeEncodedBytes: "88",
      }),
    ).toEqual({ status: "invalid", reason: "cumulative-mismatch" });
    expect(
      manager.received(owner, {
        type: "recovery-source-received",
        ...routing(prepare),
        lane: "recovery",
        contiguousDeliveryOrdinal: "1",
        cumulativeEncodedBytes: "89",
      }),
    ).toEqual({
      status: "partial",
      contiguousDeliveryOrdinal: "1",
      cumulativeEncodedBytes: "89",
      advanced: true,
    });
    expect(
      manager.received(owner, {
        type: "recovery-source-received",
        ...routing(prepare),
        lane: "recovery",
        contiguousDeliveryOrdinal: "1",
        cumulativeEncodedBytes: "89",
      }),
    ).toEqual({
      status: "partial",
      contiguousDeliveryOrdinal: "1",
      cumulativeEncodedBytes: "89",
      advanced: false,
    });
    expect(
      manager.received(owner, {
        type: "recovery-source-received",
        ...routing(prepare),
        connectionId: "connection-source-unknown",
        lane: "recovery",
        contiguousDeliveryOrdinal: "1",
        cumulativeEncodedBytes: "89",
      }),
    ).toEqual({ status: "invalid", reason: "unknown-source" });
    expect(
      manager.grant(owner, {
        type: "recovery-source-grant",
        ...routing(prepare),
        cumulativeGrantedEncodedBytes: "176",
      }),
    ).toBe(true);
    expect(
      manager.drainGranted(owner, routing(prepare), { maxRecords: 1, maxWireBytes: 88 }, () => {
        throw new Error("Done must wait for its complete cumulative grant");
      }),
    ).toEqual({ records: 0, wireBytes: 0 });
    expect(
      manager.grant(owner, {
        type: "recovery-source-grant",
        ...routing(prepare),
        cumulativeGrantedEncodedBytes: "177",
      }),
    ).toBe(true);

    const doneRecords: Uint8Array[] = [];
    expect(
      manager.drainGranted(owner, routing(prepare), { maxRecords: 1, maxWireBytes: 88 }, (bytes) =>
        doneRecords.push(bytes),
      ),
    ).toEqual({ records: 1, wireBytes: 88 });
    const done = inspectEnvelope(doneRecords[0]!);
    expect(done).toMatchObject({ deliveryOrdinal: 2n, cumulativeEncodedBytes: 177n });
    expect(decodeDataFrame(done.payload)).toMatchObject({
      kind: DataFrameKind.ReplayCommit,
      eventSeq: 1n,
      ptyOffset: 1n,
    });

    const receipt = {
      type: "recovery-source-received" as const,
      ...routing(prepare),
      lane: "recovery" as const,
      contiguousDeliveryOrdinal: "2",
      cumulativeEncodedBytes: "177",
    };
    const closed = manager.received(owner, receipt);
    expect(closed).toMatchObject({
      status: "closed",
      closed: {
        type: "recovery-source-closed",
        throughRecoveryOrdinal: "2",
        throughRecoveryCumulativeEncodedBytes: "177",
      },
    });
    expect(manager.received(owner, receipt)).toEqual({
      status: "closed",
      closed: closed.status === "closed" ? closed.closed : undefined,
      duplicate: true,
    });
    expect(manager.counters).toEqual({
      ownedRecords: 0,
      ownedWireBytes: 0,
      pendingSources: 0,
      sources: 1,
    });
  });

  it("keeps one successful record outstanding until receipt unlocks the next granted record", async () => {
    const { clock, manager, pty } = createHarness();
    const owner = {};
    const prepare = prepareFor(1);
    pty.emit(Uint8Array.of(0x41));
    const result = await manager.prepare(owner, prepare, () => true);
    expectPrepared(result);
    const ready = {
      type: "recovery-start-ready" as const,
      ...routing(prepare),
      committedThrough: result.committedThrough,
      cumulativeGrantedEncodedBytes: "177",
    };
    expect(manager.startReady(owner, ready)).toBe(true);

    const first: Uint8Array[] = [];
    expect(
      manager.drainGranted(owner, routing(prepare), { maxRecords: 2, maxWireBytes: 178 }, (bytes) =>
        first.push(bytes),
      ),
    ).toEqual({ records: 1, wireBytes: 89 });
    expect(inspectEnvelope(first[0]!)).toMatchObject({
      deliveryOrdinal: 1n,
      cumulativeEncodedBytes: 89n,
    });

    expect(manager.startReady(owner, ready)).toBe(true);
    const retries: Uint8Array[] = [];
    expect(
      manager.drainGranted(owner, routing(prepare), { maxRecords: 1, maxWireBytes: 89 }, (bytes) =>
        retries.push(bytes),
      ),
    ).toEqual({ records: 1, wireBytes: 89 });
    expect(
      manager.grant(owner, {
        type: "recovery-source-grant",
        ...routing(prepare),
        cumulativeGrantedEncodedBytes: "177",
      }),
    ).toBe(true);
    expect(
      manager.drainGranted(owner, routing(prepare), { maxRecords: 1, maxWireBytes: 89 }, (bytes) =>
        retries.push(bytes),
      ),
    ).toEqual({ records: 1, wireBytes: 89 });
    expect(retries).toEqual([first[0], first[0]]);

    expect(
      manager.received(owner, {
        type: "recovery-source-received",
        ...routing(prepare),
        lane: "recovery",
        contiguousDeliveryOrdinal: "1",
        cumulativeEncodedBytes: "89",
      }),
    ).toMatchObject({ status: "partial", advanced: true });
    const done: Uint8Array[] = [];
    expect(
      manager.drainGranted(owner, routing(prepare), { maxRecords: 1, maxWireBytes: 88 }, (bytes) =>
        done.push(bytes),
      ),
    ).toEqual({ records: 1, wireBytes: 88 });
    expect(inspectEnvelope(done[0]!)).toMatchObject({
      deliveryOrdinal: 2n,
      cumulativeEncodedBytes: 177n,
    });
    expect(
      manager.received(owner, {
        type: "recovery-source-received",
        ...routing(prepare),
        lane: "recovery",
        contiguousDeliveryOrdinal: "2",
        cumulativeEncodedBytes: "177",
      }),
    ).toMatchObject({ status: "closed", duplicate: false });

    clock.now = DEFAULT_LIMITS.recoveryDeadlineMs;
    expect(manager.checkDeadlines(owner)).toEqual([]);
    expect(manager.counters).toEqual({
      ownedRecords: 0,
      ownedWireBytes: 0,
      pendingSources: 0,
      sources: 1,
    });
  });

  it("deduplicates exact prepares and fences divergent generations and stale owners", async () => {
    const { manager } = createHarness();
    const owner = {};
    const nextOwner = {};
    const generationOne = prepareFor(1, "1");
    const first = manager.prepare(owner, generationOne, () => true);
    const exactRetry = manager.prepare(owner, { ...generationOne }, () => {
      throw new Error("an exact retry must reuse the original commit");
    });
    expect(exactRetry).toBe(first);

    await expect(
      manager.prepare(
        owner,
        {
          ...generationOne,
          base: { sessionEpoch: "7", eventSeq: "1", nextPtyOffset: "0" },
        },
        () => true,
      ),
    ).resolves.toEqual({ status: "conflict", reason: "divergent-retry" });
    await expect(manager.prepare(nextOwner, generationOne, () => true)).resolves.toMatchObject({
      status: "rejected",
      rejection: { reason: "client-gone" },
    });

    const generationTwo = prepareFor(1, "2");
    const replacement = manager.prepare(owner, generationTwo, () => true);
    expectPrepared(await first);
    expectPrepared(await replacement);
    expect(
      manager.reset(owner, {
        type: "recovery-source-reset",
        ...routing(generationOne),
        reason: "generation-reset",
      }),
    ).toBe(false);
    await expect(manager.prepare(nextOwner, prepareFor(1, "3"), () => true)).resolves.toMatchObject(
      {
        status: "rejected",
        rejection: { reason: "client-gone" },
      },
    );

    expect(manager.resetOwner(owner)).toBe(1);
    await expect(manager.prepare(owner, prepareFor(1, "3"), () => true)).resolves.toMatchObject({
      status: "rejected",
      rejection: { reason: "client-gone" },
    });
    const adopted = prepareFor(1, "3");
    expectPrepared(await manager.prepare(nextOwner, adopted, () => true));
    expect(
      manager.reset(nextOwner, {
        type: "recovery-source-reset",
        ...routing(adopted),
        reason: "generation-reset",
      }),
    ).toBe(true);
    expect(
      manager.reset(nextOwner, {
        type: "recovery-source-reset",
        ...routing(adopted),
        reason: "pair-fenced",
      }),
    ).toBe(true);
    expect(
      manager.reset(nextOwner, {
        type: "recovery-source-reset",
        ...routing(adopted),
        connectionId: "connection-source-diverged",
        reason: "pair-fenced",
      }),
    ).toBe(false);
    expect(manager.counters).toMatchObject({ ownedRecords: 0, ownedWireBytes: 0, sources: 1 });
    await expect(manager.prepare(nextOwner, adopted, () => true)).resolves.toMatchObject({
      status: "rejected",
      rejection: { reason: "generation-fenced" },
    });
    await expect(manager.prepare(nextOwner, prepareFor(1, "2"), () => true)).resolves.toMatchObject(
      {
        status: "rejected",
        rejection: { reason: "generation-fenced" },
      },
    );
    await expect(manager.prepare(nextOwner, prepareFor(1, "4"), () => false)).resolves.toEqual({
      status: "unavailable",
      reason: "fence-unavailable",
    });
    await expect(manager.prepare(nextOwner, adopted, () => true)).resolves.toMatchObject({
      status: "rejected",
      rejection: { reason: "generation-fenced" },
    });
    expectPrepared(await manager.prepare(nextOwner, prepareFor(1, "4"), () => true));
    expect(manager.resetOwner(nextOwner)).toBe(1);
    expect(manager.counters).toMatchObject({ ownedRecords: 0, ownedWireBytes: 0, sources: 0 });
  });

  it("bounds 16 isolated owners without promising scheduler fairness", async () => {
    const { manager } = createHarness({
      maxOwnedRecords: 16,
      maxOwnedWireBytes: 16 * 88,
      maxSources: 16,
    });
    const owners = Array.from({ length: 17 }, () => ({}));
    const prepares = Array.from({ length: 17 }, (_, index) => prepareFor(index + 1));

    for (let index = 0; index < 16; index += 1) {
      expectPrepared(await manager.prepare(owners[index]!, prepares[index]!, () => true));
    }
    expect(manager.counters).toEqual({
      ownedRecords: 16,
      ownedWireBytes: 16 * 88,
      pendingSources: 0,
      sources: 16,
    });
    expectCapacity(await manager.prepare(owners[16]!, prepares[16]!, () => true));

    const first = prepares[0]!;
    expect(
      manager.startReady(owners[0]!, {
        type: "recovery-start-ready",
        ...routing(first),
        committedThrough: EMPTY_BASE,
        cumulativeGrantedEncodedBytes: "88",
      }),
    ).toBe(true);
    expect(
      manager.drainGranted(
        owners[0]!,
        routing(first),
        { maxRecords: 1, maxWireBytes: 88 },
        () => {},
      ),
    ).toEqual({ records: 1, wireBytes: 88 });
    expect(
      manager.received(owners[0]!, {
        type: "recovery-source-received",
        ...routing(first),
        lane: "recovery",
        contiguousDeliveryOrdinal: "1",
        cumulativeEncodedBytes: "88",
      }),
    ).toMatchObject({ status: "closed", duplicate: false });
    expect(manager.counters).toMatchObject({
      ownedRecords: 15,
      ownedWireBytes: 15 * 88,
      sources: 16,
    });
    expectCapacity(await manager.prepare(owners[16]!, prepares[16]!, () => true));

    expect(
      manager.reset(owners[0]!, {
        type: "recovery-source-reset",
        ...routing(first),
        reason: "generation-reset",
      }),
    ).toBe(true);
    expectCapacity(await manager.prepare(owners[16]!, prepares[16]!, () => true));
    const firstReplacement = prepareFor(1, "2");
    expectPrepared(await manager.prepare(owners[0]!, firstReplacement, () => true));
    expect(manager.counters).toEqual({
      ownedRecords: 16,
      ownedWireBytes: 16 * 88,
      pendingSources: 0,
      sources: 16,
    });

    const unaffected = prepares[1]!;
    expect(
      manager.startReady(owners[1]!, {
        type: "recovery-start-ready",
        ...routing(unaffected),
        committedThrough: EMPTY_BASE,
        cumulativeGrantedEncodedBytes: "88",
      }),
    ).toBe(true);
    expect(
      manager.drainGranted(
        owners[1]!,
        routing(unaffected),
        { maxRecords: 1, maxWireBytes: 88 },
        () => {},
      ),
    ).toEqual({ records: 1, wireBytes: 88 });
  });

  it("fences a pending actor prepare by opaque owner before any late commit can publish", async () => {
    let lateCommit: ((gap: PreparedRecoveryGap) => boolean) | undefined;
    const session = {
      engineId: "fake-terminal-authority/v1",
      sessionEpoch: 7n,
      prepareRecoveryGap(
        _base: unknown,
        _limits: unknown,
        commit: (gap: PreparedRecoveryGap) => boolean,
      ) {
        lateCommit = commit;
        return new Promise<never>(() => {});
      },
    } as unknown as TerminalSession;
    const manager = new RecoverySourceManager({
      limits: DEFAULT_LIMITS,
      monotonicNow: () => 0,
      session,
    });
    const owner = {};
    const pending = manager.prepare(owner, prepareFor(1), () => {
      throw new Error("a fenced owner must not enqueue a start fence");
    });

    expect(manager.resetOwner(owner)).toBe(1);
    await expect(pending).resolves.toMatchObject({
      status: "rejected",
      rejection: { reason: "client-gone" },
    });
    expect(lateCommit!({} as PreparedRecoveryGap)).toBe(false);
    expect(manager.counters).toEqual({
      ownedRecords: 0,
      ownedWireBytes: 0,
      pendingSources: 0,
      sources: 0,
    });
  });

  it("checks deadlines for only the requested owner without retiring another pair", async () => {
    const { clock, manager } = createHarness({
      noProgressDeadlineMs: 10,
      recoveryDeadlineMs: 100,
    });
    const ownerA = {};
    const ownerB = {};
    const prepareA = prepareFor(1);
    const prepareB = prepareFor(2);
    const resultA = await manager.prepare(ownerA, prepareA, () => true);
    const resultB = await manager.prepare(ownerB, prepareB, () => true);
    expectPrepared(resultA);
    expectPrepared(resultB);
    expect(
      manager.startReady(ownerA, {
        type: "recovery-start-ready",
        ...routing(prepareA),
        committedThrough: resultA.committedThrough,
        cumulativeGrantedEncodedBytes: "88",
      }),
    ).toBe(true);
    expect(
      manager.startReady(ownerB, {
        type: "recovery-start-ready",
        ...routing(prepareB),
        committedThrough: resultB.committedThrough,
        cumulativeGrantedEncodedBytes: "88",
      }),
    ).toBe(true);

    clock.now = 10;
    expect(manager.checkDeadlines(ownerA)).toEqual([
      {
        ownerToken: ownerA,
        identity: routing(prepareA),
        reason: "no-progress-deadline",
      },
    ]);
    expect(manager.checkDeadlines(ownerA)).toEqual([]);
    expect(manager.counters).toEqual({
      ownedRecords: 1,
      ownedWireBytes: 88,
      pendingSources: 0,
      sources: 2,
    });
    expect(manager.checkDeadlines(ownerB)).toEqual([
      {
        ownerToken: ownerB,
        identity: routing(prepareB),
        reason: "no-progress-deadline",
      },
    ]);
    expect(manager.counters).toEqual({
      ownedRecords: 0,
      ownedWireBytes: 0,
      pendingSources: 0,
      sources: 2,
    });
  });

  it("expires pending, stalled, and absolute recovery lifetimes and disposes idempotently", async () => {
    const stalledClock = { now: 0 };
    let lateCommit: ((gap: PreparedRecoveryGap) => boolean) | undefined;
    let settleSessionPrepare:
      | ((value: { status: "unavailable"; reason: "fence-unavailable" }) => void)
      | undefined;
    const stalledSession = {
      engineId: "fake-terminal-authority/v1",
      sessionEpoch: 7n,
      prepareRecoveryGap(
        _base: unknown,
        _limits: unknown,
        commit: (gap: PreparedRecoveryGap) => boolean,
      ) {
        lateCommit = commit;
        return new Promise<{ status: "unavailable"; reason: "fence-unavailable" }>((resolve) => {
          settleSessionPrepare = resolve;
        });
      },
    } as unknown as TerminalSession;
    const stalledManager = new RecoverySourceManager({
      limits: { ...DEFAULT_LIMITS, recoveryDeadlineMs: 10 },
      monotonicNow: () => stalledClock.now,
      session: stalledSession,
    });
    const stalledOwner = {};
    const pendingPrepare = prepareFor(1);
    const pending = stalledManager.prepare(stalledOwner, pendingPrepare, () => true);
    stalledClock.now = 10;
    expect(stalledManager.checkDeadlines(stalledOwner)).toEqual([
      {
        ownerToken: stalledOwner,
        identity: routing(pendingPrepare),
        reason: "prepare-deadline",
      },
    ]);
    await expect(pending).resolves.toEqual({ status: "unavailable", reason: "deadline" });
    expect(lateCommit!({} as PreparedRecoveryGap)).toBe(false);
    settleSessionPrepare!({ status: "unavailable", reason: "fence-unavailable" });
    await Promise.resolve();
    expect(stalledManager.counters).toMatchObject({
      ownedRecords: 0,
      ownedWireBytes: 0,
      pendingSources: 0,
      sources: 1,
    });

    const noProgress = createHarness({
      noProgressDeadlineMs: 10,
      recoveryDeadlineMs: 100,
    });
    const noProgressOwner = {};
    const noProgressPrepare = prepareFor(1);
    const noProgressResult = await noProgress.manager.prepare(
      noProgressOwner,
      noProgressPrepare,
      () => true,
    );
    expectPrepared(noProgressResult);
    expect(
      noProgress.manager.startReady(noProgressOwner, {
        type: "recovery-start-ready",
        ...routing(noProgressPrepare),
        committedThrough: noProgressResult.committedThrough,
        cumulativeGrantedEncodedBytes: "0",
      }),
    ).toBe(true);
    noProgress.clock.now = 9;
    expect(
      noProgress.manager.grant(noProgressOwner, {
        type: "recovery-source-grant",
        ...routing(noProgressPrepare),
        cumulativeGrantedEncodedBytes: "1",
      }),
    ).toBe(true);
    noProgress.clock.now = 10;
    expect(noProgress.manager.checkDeadlines(noProgressOwner)).toEqual([
      {
        ownerToken: noProgressOwner,
        identity: routing(noProgressPrepare),
        reason: "no-progress-deadline",
      },
    ]);
    expect(
      noProgress.manager.received(noProgressOwner, {
        type: "recovery-source-received",
        ...routing(noProgressPrepare),
        lane: "recovery",
        contiguousDeliveryOrdinal: "1",
        cumulativeEncodedBytes: "88",
      }),
    ).toEqual({ status: "invalid", reason: "unknown-source" });

    const absolute = createHarness({
      noProgressDeadlineMs: 100,
      recoveryDeadlineMs: 20,
    });
    const absoluteOwner = {};
    const absolutePrepare = prepareFor(1);
    const absoluteResult = await absolute.manager.prepare(
      absoluteOwner,
      absolutePrepare,
      () => true,
    );
    expectPrepared(absoluteResult);
    expect(
      absolute.manager.startReady(absoluteOwner, {
        type: "recovery-start-ready",
        ...routing(absolutePrepare),
        committedThrough: absoluteResult.committedThrough,
        cumulativeGrantedEncodedBytes: "88",
      }),
    ).toBe(true);
    absolute.clock.now = 19;
    expect(
      absolute.manager.drainGranted(
        absoluteOwner,
        routing(absolutePrepare),
        { maxRecords: 1, maxWireBytes: 88 },
        () => {},
      ),
    ).toEqual({ records: 1, wireBytes: 88 });
    absolute.clock.now = 20;
    expect(absolute.manager.checkDeadlines(absoluteOwner)).toEqual([
      {
        ownerToken: absoluteOwner,
        identity: routing(absolutePrepare),
        reason: "recovery-deadline",
      },
    ]);

    const disposable = createHarness();
    const disposableOwner = {};
    expectPrepared(await disposable.manager.prepare(disposableOwner, prepareFor(1), () => true));
    disposable.manager.dispose();
    disposable.manager.dispose();
    expect(disposable.manager.counters).toEqual({
      ownedRecords: 0,
      ownedWireBytes: 0,
      pendingSources: 0,
      sources: 0,
    });
    await expect(
      disposable.manager.prepare(disposableOwner, prepareFor(2), () => true),
    ).resolves.toEqual({ status: "unavailable", reason: "disposed" });
    expect(
      disposable.manager.reset(disposableOwner, {
        type: "recovery-source-reset",
        ...routing(prepareFor(1)),
        reason: "session-disposed",
      }),
    ).toBe(false);
  });
});
