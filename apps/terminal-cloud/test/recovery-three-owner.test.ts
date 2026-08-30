import { decodeDeliveryEnvelope, decodeRecoveryStartFence } from "@zhongduan/protocol";
import { afterEach, describe, expect, it } from "vitest";

import {
  LiteralReplicaHost,
  ThreeOwnerHarness,
  literalOracle,
  nextClose,
  seededMutations,
  sinkOracle,
  waitForCondition,
  type OpenRawRecoveryBrowser,
} from "./recovery-three-owner-harness";

let harness: ThreeOwnerHarness | undefined;

afterEach(async () => {
  await harness?.close();
  harness = undefined;
});

describe("deterministic three-owner recovery continuity", () => {
  it("converges Host, durable Cloud, and Browser across cross-channel reorder and stable progress retries", async () => {
    harness = await ThreeOwnerHarness.create();
    const base = seededMutations(0x26_5c_2000, 3);
    for (const mutation of base) harness.emit(mutation);
    await harness.waitForCloudCursor(literalOracle(base));
    await harness.publishBaseSnapshot("snapshot_three_owner_faults_01", base);

    const gap = seededMutations(0x26_5c_2003, 6);
    for (const mutation of gap) harness.emit(mutation);
    const mutations = [...base, ...gap];
    await harness.waitForCloudCursor(literalOracle(mutations));

    const replicaHost = new LiteralReplicaHost();
    replicaHost.holdRestore();
    const sourceClosed = harness.holdNextHostSourceClosed();
    const browser = await harness.openBrowser({
      host: replicaHost,
      faults: {
        dropFirstAdopted: true,
        dropFinalRecoveryReceipt: true,
        holdStart: true,
      },
    });

    const start = await browser.waitForHeldStart();
    expect(start.source).toMatchObject({
      kind: "snapshot",
      cutEventSeq: base.length.toString(),
      nextPtyOffset: literalOracle(base).nextPtyOffset.toString(),
    });
    try {
      await waitForCondition(
        () => browser.trace.some((entry) => entry.startsWith("data:recovery:")),
        "a Recovery envelope before Browser accepts RecoveryStart",
      );
    } catch (error) {
      const diagnostics = await harness.diagnostics(start.recoveryId);
      throw new Error(
        `${error instanceof Error ? error.message : "Recovery envelope wait failed"}: ${JSON.stringify(diagnostics)}`,
      );
    }
    expect(browser.runtime.state).toBe("awaiting-start");
    expect(browser.trace.indexOf("control:recovery-start")).toBeLessThan(
      browser.trace.findIndex((entry) => entry.startsWith("data:recovery:")),
    );

    browser.releaseStart();
    await replicaHost.waitForRestoreLoad();
    await waitForCondition(
      () => browser.progress.some(({ frame, sent }) => frame.type === "delivery-received" && !sent),
      "the first final Recovery receipt to be dropped",
    );
    expect(browser.runtime.state).toBe("restoring");
    expect(browser.host.active).toBeNull();
    const attemptBeforeHibernate = await harness.attempt(start.recoveryId);
    await waitForCondition(
      async () => (await harness!.deliveryStates(start.recoveryId)).includes("sent"),
      "a durable sent delivery with its receipt withheld",
    );
    const droppedReceipt = browser.progress.find(
      ({ frame, sent }) => frame.type === "delivery-received" && frame.lane === "recovery" && !sent,
    );
    expect(droppedReceipt).toBeDefined();
    expect(
      browser.progress.some(({ encoded, sent }) => sent && encoded === droppedReceipt?.encoded),
    ).toBe(false);
    expect(sourceClosed.isHeld()).toBe(false);
    const recoveryDeliveriesBefore = browser.trace.filter((entry) =>
      entry.startsWith("data:recovery:"),
    ).length;
    await harness.hibernate();
    expect(browser.trace.filter((entry) => entry.startsWith("data:recovery:")).length).toBe(
      recoveryDeliveriesBefore,
    );
    expect((await harness.attempt(start.recoveryId)).state).toBe(attemptBeforeHibernate.state);
    expect(
      browser.progress.some(({ encoded, sent }) => sent && encoded === droppedReceipt?.encoded),
    ).toBe(false);
    expect(sourceClosed.isHeld()).toBe(false);

    expect(browser.timers.runNext()).toBe(true);
    const heldSourceClosed = await sourceClosed.wait();
    expect(heldSourceClosed).toContain('"type":"recovery-source-closed"');
    const receiptAttempts = browser.progress.filter(
      ({ frame }) => frame.type === "delivery-received" && frame.lane === "recovery",
    );
    expect(
      receiptAttempts.some(({ encoded, sent }) => sent && encoded === droppedReceipt?.encoded),
    ).toBe(true);

    await harness.pauseRecoveryOutboxDrain();
    sourceClosed.publish();
    await waitForCondition(
      async () =>
        (await harness!.recoveryOutboxKinds(start.recoveryId)).includes("recovery-source-closed"),
      "durable Browser SourceClosed outbox",
    );
    expect(browser.trace).not.toContain("control:recovery-source-closed");
    await harness.hibernate();
    await waitForCondition(
      () => browser.trace.includes("control:recovery-source-closed"),
      "SourceClosed outbox replay after hibernation",
    );
    expect(browser.runtime.state).toBe("restoring");
    expect(browser.host.active).toBeNull();

    const liveTail = seededMutations(0x26_5c_2027, 3);
    for (const mutation of liveTail) harness.emit(mutation);
    mutations.push(...liveTail);
    await harness.waitForCloudCursor(literalOracle(mutations));

    replicaHost.releaseRestore();
    await waitForCondition(() => browser.runtime.state === "live", "Browser handoff to live");
    await waitForCondition(
      () => browser.progress.some(({ frame, sent }) => frame.type === "recovery-adopted" && !sent),
      "first RecoveryAdopted loss",
    );
    expect((await harness.attempt(start.recoveryId)).state).not.toBe("complete");
    expect(browser.timers.runNext()).toBe(true);
    await waitForCondition(
      async () => (await harness!.attempt(start.recoveryId)).state === "complete",
      "Cloud completion after stable RecoveryAdopted retry",
    );
    const adoptedAttempts = browser.progress.filter(
      ({ frame }) => frame.type === "recovery-adopted",
    );
    expect(adoptedAttempts.length).toBeGreaterThanOrEqual(2);
    expect(adoptedAttempts[0]?.encoded).toBe(adoptedAttempts[1]?.encoded);

    const active = browser.host.active;
    expect(active).not.toBeNull();
    expect(sinkOracle(active!)).toEqual(literalOracle(mutations));
    expect(browser.runtime.activeCursor).toEqual({
      sessionEpoch: 7n,
      deliveryGeneration: BigInt(browser.connection.deliveryGeneration),
      lastEventSeq: BigInt(mutations.length),
      nextPtyOffset: literalOracle(mutations).nextPtyOffset,
    });
    expect(browser.failures).toEqual([]);
    expect(harness.currentHost.pair.control.readyState).toBe(WebSocket.OPEN);
  }, 15_000);

  it("fences queued and sending generations on wake while preserving the Host owner", async () => {
    harness = await ThreeOwnerHarness.create();
    await harness.publishBaseSnapshot("snapshot_three_owner_unsafe_01", []);

    const heldRecoveryFrames: Uint8Array[] = [];
    const hostData = harness.currentHost.pair.data;
    const originalSend = hostData.send.bind(hostData);
    Object.defineProperty(hostData, "send", {
      configurable: true,
      value: (data: string | ArrayBufferLike | Blob | ArrayBufferView) => {
        if (data instanceof Uint8Array) {
          try {
            decodeRecoveryStartFence(data);
            Reflect.apply(originalSend, hostData, [data]);
            return;
          } catch {
            // Recovery envelopes are held below; other data is forwarded.
          }
          try {
            decodeDeliveryEnvelope(data);
            heldRecoveryFrames.push(data.slice());
            return;
          } catch {
            // Non-envelope Host data continues through the real socket.
          }
        }
        Reflect.apply(originalSend, hostData, [data]);
      },
    });

    let queued: OpenRawRecoveryBrowser | undefined;
    let sending: OpenRawRecoveryBrowser | undefined;
    try {
      queued = await harness.openRawWarmBrowser();
      sending = await harness.openRawWarmBrowser();
      await harness.seedUnsafeRecoveryDelivery(queued, "queued");
      await harness.seedUnsafeRecoveryDelivery(sending, "sending");
      expect(await harness.deliveryStates(queued.start.recoveryId)).toEqual(["queued"]);
      expect(await harness.deliveryStates(sending.start.recoveryId)).toEqual(["sending"]);

      const queuedControlClosed = nextClose(queued.control);
      const queuedDataClosed = nextClose(queued.data);
      const sendingControlClosed = nextClose(sending.control);
      const sendingDataClosed = nextClose(sending.data);
      await harness.hibernate();
      await Promise.all([
        expect(queuedControlClosed).resolves.toMatchObject({ code: 4400 }),
        expect(queuedDataClosed).resolves.toMatchObject({ code: 4400 }),
        expect(sendingControlClosed).resolves.toMatchObject({ code: 4400 }),
        expect(sendingDataClosed).resolves.toMatchObject({ code: 4400 }),
      ]);
      expect(await harness.attempt(queued.start.recoveryId)).toMatchObject({
        reset_reason: "ack-outcome-uncertain",
        state: "resetting",
      });
      expect(await harness.attempt(sending.start.recoveryId)).toMatchObject({
        reset_reason: "ack-outcome-uncertain",
        state: "resetting",
      });
      expect(await harness.deliveryStates(queued.start.recoveryId)).toEqual([]);
      expect(await harness.deliveryStates(sending.start.recoveryId)).toEqual([]);
      expect(harness.currentHost.pair.control.readyState).toBe(WebSocket.OPEN);
      expect(heldRecoveryFrames.length).toBeGreaterThanOrEqual(2);
    } finally {
      Object.defineProperty(hostData, "send", { configurable: true, value: originalSend });
    }
  }, 15_000);
});
