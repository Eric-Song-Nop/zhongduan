import type { RecoveryHostRoutingIdentity } from "@zhongduan/protocol";
import { describe, expect, it, vi } from "vitest";

import type {
  RecoverySourceDrainLimits,
  RecoverySourceDrainResult,
  RecoverySourceOwnerToken,
} from "./recovery-source-manager";
import {
  HOST_RECOVERY_BACKPRESSURE_RETRY_MS,
  HOST_RECOVERY_SOURCE_QUANTUM_BYTES,
  RecoverySourceScheduler,
} from "./recovery-source-scheduler";

interface PendingTurn {
  readonly delayMs: number;
  readonly resolve: () => void;
}

function routing(streamId: number): RecoveryHostRoutingIdentity {
  return {
    recoveryId: `recovery-source-${streamId.toString().padStart(8, "0")}`,
    connectionId: `connection_${streamId.toString().padStart(5, "A")}`,
    streamId,
    deliveryGeneration: "1",
  };
}

function controlledTurns() {
  const pending: PendingTurn[] = [];
  return {
    pending,
    yieldDataTurn: (delayMs: number): Promise<void> =>
      new Promise((resolve) => pending.push({ delayMs, resolve })),
    async runNext(): Promise<void> {
      const next = pending.shift();
      if (next === undefined) throw new Error("No Recovery scheduler turn was pending");
      next.resolve();
      await Promise.resolve();
      await Promise.resolve();
    },
  };
}

describe("RecoverySourceScheduler", () => {
  it("gives all sixteen bounded sources one deterministic turn before cycling", async () => {
    const turns = controlledTurns();
    const sent: number[] = [];
    const manager = {
      drainGranted(
        _owner: RecoverySourceOwnerToken,
        identity: RecoveryHostRoutingIdentity,
        _limits: RecoverySourceDrainLimits,
        send: (encoded: Uint8Array) => void,
      ): RecoverySourceDrainResult {
        send(Uint8Array.of(identity.streamId));
        sent.push(identity.streamId);
        return { status: "complete", records: 1, wireBytes: 1 };
      },
    };
    const scheduler = new RecoverySourceScheduler({
      bufferedAmount: () => 0,
      dataHighWaterBytes: 1024 * 1024,
      manager,
      onFailure: vi.fn(),
      ownerToken: {},
      sendData: vi.fn(),
      yieldDataTurn: turns.yieldDataTurn,
    });

    for (let streamId = 1; streamId <= 16; streamId += 1) {
      scheduler.notify(routing(streamId));
    }
    for (let turn = 0; turn < 16; turn += 1) await turns.runNext();

    expect(sent).toEqual(Array.from({ length: 16 }, (_, index) => index + 1));
    expect(turns.pending).toEqual([]);
  });

  it("keeps an active DRR visit across asynchronous record turns", async () => {
    const turns = controlledTurns();
    const owner: RecoverySourceOwnerToken = {};
    const remaining = new Map([
      [1, 2],
      [2, 2],
    ]);
    const sent: number[] = [];
    const events: string[] = [];
    const manager = {
      drainGranted(
        actualOwner: RecoverySourceOwnerToken,
        identity: RecoveryHostRoutingIdentity,
        limits: RecoverySourceDrainLimits,
        send: (encoded: Uint8Array) => void,
      ): RecoverySourceDrainResult {
        expect(actualOwner).toBe(owner);
        expect(limits.maxRecords).toBe(1);
        expect(limits.maxWireBytes).toBeGreaterThan(0);
        const before = remaining.get(identity.streamId) ?? 0;
        send(Uint8Array.of(identity.streamId));
        sent.push(identity.streamId);
        events.push(`recovery-${identity.streamId}`);
        remaining.set(identity.streamId, before - 1);
        return {
          status: before === 1 ? "complete" : "runnable",
          records: 1,
          wireBytes: 1,
        };
      },
    };
    const scheduler = new RecoverySourceScheduler({
      bufferedAmount: () => 0,
      dataHighWaterBytes: 1024 * 1024,
      manager,
      onFailure: vi.fn(),
      ownerToken: owner,
      sendData: vi.fn(),
      yieldDataTurn: turns.yieldDataTurn,
    });

    scheduler.notify(routing(1));
    scheduler.notify(routing(2));
    expect(sent).toEqual([]);
    expect(turns.pending.map((turn) => turn.delayMs)).toEqual([0]);

    events.push("canonical");
    await turns.runNext();
    expect(sent).toEqual([1]);
    events.push("control");
    await turns.runNext();
    expect(sent).toEqual([1, 1]);
    events.push("canonical");
    await turns.runNext();
    expect(sent).toEqual([1, 1, 2]);
    events.push("control");
    await turns.runNext();
    expect(sent).toEqual([1, 1, 2, 2]);
    expect(events).toEqual([
      "canonical",
      "recovery-1",
      "control",
      "recovery-1",
      "canonical",
      "recovery-2",
      "control",
      "recovery-2",
    ]);
    expect(turns.pending).toEqual([]);
  });

  it("balances sustained mixed record sizes by bytes rather than record count", async () => {
    const turns = controlledTurns();
    const smallBytes = HOST_RECOVERY_SOURCE_QUANTUM_BYTES / 4;
    const largeBytes = HOST_RECOVERY_SOURCE_QUANTUM_BYTES;
    const remaining = new Map([
      [1, { records: 8, wireBytes: smallBytes }],
      [2, { records: 2, wireBytes: largeBytes }],
    ]);
    const sent: Array<{ streamId: number; wireBytes: number }> = [];
    const manager = {
      drainGranted(
        _owner: RecoverySourceOwnerToken,
        identity: RecoveryHostRoutingIdentity,
        limits: RecoverySourceDrainLimits,
        send: (encoded: Uint8Array) => void,
      ): RecoverySourceDrainResult {
        const source = remaining.get(identity.streamId);
        if (source === undefined) return { status: "stale", records: 0, wireBytes: 0 };
        if (source.wireBytes > limits.maxWireBytes) {
          return { status: "runnable", records: 0, wireBytes: 0 };
        }
        send(new Uint8Array(source.wireBytes));
        sent.push({ streamId: identity.streamId, wireBytes: source.wireBytes });
        source.records -= 1;
        return {
          status: source.records === 0 ? "complete" : "runnable",
          records: 1,
          wireBytes: source.wireBytes,
        };
      },
    };
    const scheduler = new RecoverySourceScheduler({
      bufferedAmount: () => 0,
      dataHighWaterBytes: 1024 * 1024,
      manager,
      onFailure: vi.fn(),
      ownerToken: {},
      sendData: vi.fn(),
      yieldDataTurn: turns.yieldDataTurn,
    });
    scheduler.notify(routing(1));
    scheduler.notify(routing(2));

    for (let turn = 0; turn < 10; turn += 1) {
      const before = sent.length;
      await turns.runNext();
      expect(sent).toHaveLength(before + 1);
    }

    expect(sent.map(({ streamId }) => streamId)).toEqual([1, 1, 1, 1, 2, 1, 1, 1, 1, 2]);
    expect(sent.slice(0, 5).reduce((bytes, record) => bytes + record.wireBytes, 0)).toBe(
      2 * HOST_RECOVERY_SOURCE_QUANTUM_BYTES,
    );
    expect(sent.slice(5).reduce((bytes, record) => bytes + record.wireBytes, 0)).toBe(
      2 * HOST_RECOVERY_SOURCE_QUANTUM_BYTES,
    );
    expect(turns.pending).toEqual([]);
  });

  it("accumulates a bounded byte deficit without letting a large source block a small one", async () => {
    const turns = controlledTurns();
    const sent: number[] = [];
    const sizes = new Map([
      [1, HOST_RECOVERY_SOURCE_QUANTUM_BYTES + 1],
      [2, 1],
    ]);
    const manager = {
      drainGranted(
        _owner: RecoverySourceOwnerToken,
        identity: RecoveryHostRoutingIdentity,
        limits: RecoverySourceDrainLimits,
        send: (encoded: Uint8Array) => void,
      ): RecoverySourceDrainResult {
        const size = sizes.get(identity.streamId);
        if (size === undefined) return { status: "stale", records: 0, wireBytes: 0 };
        if (size > limits.maxWireBytes) {
          return { status: "runnable", records: 0, wireBytes: 0 };
        }
        send(Uint8Array.of(identity.streamId));
        sent.push(identity.streamId);
        sizes.delete(identity.streamId);
        return { status: "complete", records: 1, wireBytes: size };
      },
    };
    const scheduler = new RecoverySourceScheduler({
      bufferedAmount: () => 0,
      dataHighWaterBytes: 1024 * 1024,
      manager,
      onFailure: vi.fn(),
      ownerToken: {},
      sendData: vi.fn(),
      yieldDataTurn: turns.yieldDataTurn,
    });
    scheduler.notify(routing(1));
    scheduler.notify(routing(2));

    await turns.runNext();
    expect(sent).toEqual([]);
    await turns.runNext();
    expect(sent).toEqual([2]);
    await turns.runNext();
    expect(sent).toEqual([2, 1]);
  });

  it("backs off at the shared data high-water mark and resumes without failing the pair", async () => {
    const turns = controlledTurns();
    let bufferedAmount = 100;
    const onFailure = vi.fn();
    const sendData = vi.fn();
    const drainGranted = vi.fn(
      (
        _owner: RecoverySourceOwnerToken,
        _identity: RecoveryHostRoutingIdentity,
        _limits: RecoverySourceDrainLimits,
        send: (encoded: Uint8Array) => void,
      ): RecoverySourceDrainResult => {
        send(Uint8Array.of(1));
        return { status: "complete", records: 1, wireBytes: 1 };
      },
    );
    const scheduler = new RecoverySourceScheduler({
      bufferedAmount: () => bufferedAmount,
      dataHighWaterBytes: 100,
      manager: { drainGranted },
      onFailure,
      ownerToken: {},
      sendData,
      yieldDataTurn: turns.yieldDataTurn,
    });
    scheduler.notify(routing(1));

    await turns.runNext();
    expect(drainGranted).not.toHaveBeenCalled();
    expect(onFailure).not.toHaveBeenCalled();
    expect(turns.pending.map((turn) => turn.delayMs)).toEqual([
      HOST_RECOVERY_BACKPRESSURE_RETRY_MS,
    ]);

    bufferedAmount = 99;
    await turns.runNext();
    expect(drainGranted).toHaveBeenCalledOnce();
    expect(sendData).toHaveBeenCalledOnce();
    expect(onFailure).not.toHaveBeenCalled();
  });

  it("treats a real data send throw as outcome-uncertain and stops all later turns", async () => {
    const turns = controlledTurns();
    const onFailure = vi.fn();
    const manager = {
      drainGranted(
        _owner: RecoverySourceOwnerToken,
        _identity: RecoveryHostRoutingIdentity,
        _limits: RecoverySourceDrainLimits,
        send: (encoded: Uint8Array) => void,
      ): RecoverySourceDrainResult {
        send(Uint8Array.of(1));
        return { status: "complete", records: 1, wireBytes: 1 };
      },
    };
    const scheduler = new RecoverySourceScheduler({
      bufferedAmount: () => 0,
      dataHighWaterBytes: 100,
      manager,
      onFailure,
      ownerToken: {},
      sendData: () => {
        throw new Error("injected recovery send failure");
      },
      yieldDataTurn: turns.yieldDataTurn,
    });
    scheduler.notify(routing(1));
    scheduler.notify(routing(2));

    await turns.runNext();
    expect(onFailure).toHaveBeenCalledWith("injected recovery send failure");
    expect(turns.pending).toEqual([]);
  });
});
