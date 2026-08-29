import type { RecoveryV3HostPrepare } from "@zhongduan/protocol";
import { env, exports as workerExports } from "cloudflare:workers";
import { evictDurableObject, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { DurableAlarmComponent, type DurableAlarmMux } from "../src/worker/durable-alarm-mux";
import { RelayRecoveryMaintenance } from "../src/worker/relay-recovery-maintenance";
import type { RelayRecoveryStore } from "../src/worker/relay-recovery-store";

const origin = "https://terminal.example.test";
const engineId = "ghostty:test+snapshot-v1+wterm:test";
let sessionCounter = 0;

async function createSession(): Promise<string> {
  sessionCounter += 1;
  const sessionId = `session_recovery_maintenance_${sessionCounter.toString().padStart(16, "0")}`;
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions`, {
      method: "POST",
      headers: {
        authorization: "Bearer test-bootstrap-token-with-at-least-32-bytes",
        "content-type": "application/json",
      },
      body: JSON.stringify({ sessionId, engineId, sessionEpoch: "7" }),
    }),
  );
  expect(response.status).toBe(201);
  await response.body?.cancel();
  return sessionId;
}

function sessionStub(sessionId: string) {
  return env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${sessionId}`));
}

function terminalPinnedSnapshotIds(instance: object, v2SnapshotId?: string): Set<string> {
  const pinnedSnapshotIds = Reflect.get(instance, "pinnedSnapshotIds") as () => ReadonlySet<string>;
  if (v2SnapshotId === undefined) return new Set(pinnedSnapshotIds.call(instance));

  const originalContext = Reflect.get(instance, "ctx");
  const v2DataSocket = {
    deserializeAttachment: () => ({
      version: 2,
      peer: "browser",
      channel: "data",
      connectionSetId: "connection_set_pin_union_0001",
      connectionId: "connection_pin_union_000001",
      subject: "subject_pin_union_00000001",
      clientId: "client_pin_union_000000001",
      role: "observer",
      streamId: 9,
      deliveryGeneration: "1",
      hostFence: null,
      leaseFence: null,
      controlState: null,
      dataState: "catching-up",
      firstEventSeq: "0",
      ackedEventSeq: "0",
      sentEventSeq: "0",
      firstPtyOffset: "0",
      ackedPtyOffset: "0",
      sentPtyOffset: "0",
      replayMode: "snapshot",
      snapshotId: v2SnapshotId,
      replayCommitEventSeq: "0",
      replayCommitPtyOffset: "0",
      relayCapabilities: [],
    }),
    readyState: WebSocket.OPEN,
  } as unknown as WebSocket;
  Reflect.set(instance, "ctx", {
    getWebSockets: (tag?: string) => (tag === "peer:browser" ? [v2DataSocket] : []),
  });
  try {
    return new Set(pinnedSnapshotIds.call(instance));
  } finally {
    Reflect.set(instance, "ctx", originalContext);
  }
}

class FakeRecoveryStore {
  readonly calls: string[] = [];
  deadline: number | undefined;

  expireDeadlines(now: number): readonly string[] {
    this.calls.push(`expire:${now}`);
    return [];
  }

  nextDeadline(): number | undefined {
    this.calls.push("next");
    return this.deadline;
  }

  pruneFencedTerminalAttempts(): readonly string[] {
    this.calls.push("prune");
    return [];
  }
}

class FakeAlarmScheduler {
  readonly calls: string[] = [];

  async clear(): Promise<void> {
    this.calls.push("clear");
  }

  async schedule(timestamp: number): Promise<void> {
    this.calls.push(`schedule:${timestamp}`);
  }
}

function fakeState(events: string[]): DurableObjectState {
  return {
    storage: {
      transactionSync<T>(closure: () => T): T {
        events.push("transaction:start");
        try {
          return closure();
        } finally {
          events.push("transaction:end");
        }
      },
    },
  } as unknown as DurableObjectState;
}

describe("RelayRecoveryMaintenance", () => {
  it("rebuilds the next durable recovery deadline without expiring it", async () => {
    const events: string[] = [];
    const recoveries = new FakeRecoveryStore();
    recoveries.deadline = 4_000;
    const alarm = new FakeAlarmScheduler();
    const maintenance = new RelayRecoveryMaintenance(fakeState(events), recoveries, alarm, {
      now: () => 1_000,
      retryDelayMs: 50,
    });

    await expect(maintenance.initialize()).resolves.toEqual({ expired: [], pruned: [] });

    expect(events).toEqual(["transaction:start", "transaction:end"]);
    expect(recoveries.calls).toEqual(["prune", "next"]);
    expect(alarm.calls).toEqual(["schedule:4000"]);
  });

  it("expires, prunes, and reads the replacement deadline in one transaction", async () => {
    const events: string[] = [];
    const recoveries = new FakeRecoveryStore();
    recoveries.deadline = 4_000;
    const alarm = new FakeAlarmScheduler();
    const maintenance = new RelayRecoveryMaintenance(fakeState(events), recoveries, alarm, {
      now: () => 4_000,
      retryDelayMs: 50,
    });

    await expect(maintenance.maintain()).resolves.toEqual({ expired: [], pruned: [] });

    expect(events).toEqual(["transaction:start", "transaction:end"]);
    expect(recoveries.calls).toEqual(["prune", "expire:4000", "next"]);
    expect(alarm.calls).toEqual(["schedule:4050"]);
  });

  it("clears an obsolete recovery alarm when SQL has no active deadline", async () => {
    const recoveries = new FakeRecoveryStore();
    const alarm = new FakeAlarmScheduler();
    const maintenance = new RelayRecoveryMaintenance(fakeState([]), recoveries, alarm, {
      now: () => 9_000,
      retryDelayMs: 50,
    });

    await expect(maintenance.refresh()).resolves.toEqual({ expired: [], pruned: [] });

    expect(alarm.calls).toEqual(["clear"]);
  });

  it("serializes refresh behind an in-flight scheduler update", async () => {
    const recoveries = new FakeRecoveryStore();
    recoveries.deadline = 2_000;
    let releaseFirst: (() => void) | undefined;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const calls: string[] = [];
    const alarm = {
      async clear(): Promise<void> {
        calls.push("clear");
      },
      async schedule(timestamp: number): Promise<void> {
        calls.push(`schedule:${timestamp}:start`);
        if (calls.length === 1) await firstGate;
        calls.push(`schedule:${timestamp}:end`);
      },
    };
    const maintenance = new RelayRecoveryMaintenance(fakeState([]), recoveries, alarm, {
      now: () => 1_000,
      retryDelayMs: 50,
    });

    const first = maintenance.refresh();
    recoveries.deadline = 3_000;
    const second = maintenance.refresh();
    await Promise.resolve();
    expect(calls).toEqual(["schedule:3000:start"]);
    releaseFirst?.();
    await Promise.all([first, second]);

    expect(calls).toEqual([
      "schedule:3000:start",
      "schedule:3000:end",
      "schedule:3000:start",
      "schedule:3000:end",
    ]);
  });

  it("rebuilds a SQL deadline after eviction and expires it once through the shared alarm", async () => {
    const sessionId = await createSession();
    const stub = sessionStub(sessionId);
    const clientId = "client_recovery_maintenance_0001";
    const connectionId = "connection_recovery_maintenance_01";
    const recoveryId = "recovery_maintenance_attempt_0001";
    const snapshotId = "snapshot_recovery_maintenance_01";
    const v2SnapshotId = "snapshot_v2_pin_maintenance_01";
    const deadline = Date.now() + 10_000;

    await runInDurableObject(stub, async (instance, durable) => {
      durable.storage.sql.exec(
        `INSERT INTO client_delivery
          (client_id, principal_id_hash, role, stream_id, delivery_generation,
           updated_at, registered_at, reservation_expires_at, recovery_strategy)
         VALUES (?, 'principal_recovery_maintenance', 'observer', 1, '1', 1, 1, NULL, 'v3')`,
        clientId,
      );
      durable.storage.sql.exec(
        `UPDATE session_state
         SET host_fence = '3', head_event_seq = '5', next_pty_offset = '12'
         WHERE singleton = 1`,
      );
      const prepare = {
        type: "recovery-prepare",
        recoveryId,
        connectionId,
        streamId: 1,
        deliveryGeneration: "1",
        engineId,
        base: { sessionEpoch: "7", eventSeq: "5", nextPtyOffset: "12" },
        source: { kind: "snapshot", snapshotId },
      } satisfies RecoveryV3HostPrepare;
      const recoveries = Reflect.get(instance, "recoveries") as RelayRecoveryStore;
      const maintenance = Reflect.get(instance, "recoveryMaintenance") as RelayRecoveryMaintenance;
      expect(
        durable.storage.transactionSync(() =>
          recoveries.beginPreparing({
            clientId,
            hardDeadlineAt: deadline + 10_000,
            hostFence: "3",
            noProgressTimeoutMs: 10_000,
            now: deadline - 10_000,
            prepare,
          }),
        ),
      ).toEqual({ changed: true, ok: true });
      const prepareOutbox = recoveries.outbox()[0];
      if (prepareOutbox === undefined) throw new Error("prepare outbox missing");
      durable.storage.transactionSync(() =>
        recoveries.acknowledgeOutbox(
          recoveryId,
          prepareOutbox.kind,
          prepareOutbox.payload_json,
          deadline - 9_999,
        ),
      );
      await maintenance.refresh();
      expect(await durable.storage.getAlarm()).toBe(deadline);
      expect([...recoveries.pinnedSnapshotIds()]).toEqual([snapshotId]);
      expect([...terminalPinnedSnapshotIds(instance, v2SnapshotId)].sort()).toEqual(
        [snapshotId, v2SnapshotId].sort(),
      );

      // Remove the mux fact but retain SQL ownership so cold initialization
      // must rebuild the component deadline from recovery_attempt.
      const mux = Reflect.get(instance, "alarmMux") as DurableAlarmMux;
      await mux.scheduler(DurableAlarmComponent.recovery).clear();
      expect(await durable.storage.getAlarm()).toBeNull();
    });

    await evictDurableObject(stub);
    await runInDurableObject(stub, async (instance, durable) => {
      const recoveries = Reflect.get(instance, "recoveries") as RelayRecoveryStore;
      const mux = Reflect.get(instance, "alarmMux") as DurableAlarmMux;
      const maintenance = Reflect.get(instance, "recoveryMaintenance") as RelayRecoveryMaintenance;
      expect(recoveries.attempt(recoveryId)).toMatchObject({ state: "preparing" });
      expect([...recoveries.pinnedSnapshotIds()]).toEqual([snapshotId]);
      expect([...terminalPinnedSnapshotIds(instance, v2SnapshotId)].sort()).toEqual(
        [snapshotId, v2SnapshotId].sort(),
      );
      expect(await durable.storage.getAlarm()).toBe(deadline);
      Reflect.set(mux, "now", () => deadline);
      Reflect.set(maintenance, "now", () => deadline);

      await instance.alarm();
      expect(recoveries.attempt(recoveryId)).toMatchObject({
        reset_reason: "deadline",
        state: "resetting",
      });
      expect(recoveries.outbox()).toMatchObject([
        { kind: "recovery-source-reset", recovery_id: recoveryId },
      ]);
      expect(recoveries.pinnedSnapshotIds().size).toBe(0);
      expect([...terminalPinnedSnapshotIds(instance, v2SnapshotId)]).toEqual([v2SnapshotId]);

      await instance.alarm();
      expect(
        durable.storage.sql
          .exec(
            `SELECT COUNT(*) AS value FROM recovery_control_outbox
             WHERE recovery_id = ? AND kind = 'recovery-source-reset'`,
            recoveryId,
          )
          .one(),
      ).toEqual({ value: 1 });
    });
  });
});
