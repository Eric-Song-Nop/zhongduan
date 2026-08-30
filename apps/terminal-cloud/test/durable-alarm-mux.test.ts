import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import {
  DURABLE_ALARM_FAILURE_RETRY_MS,
  DurableAlarmComponent,
  DurableAlarmDispatch,
  DurableAlarmMux,
} from "../src/worker/durable-alarm-mux";

const ALARM_FACTS_KEY = "terminal-session:alarm-mux";

function alarmMuxStub(name: string) {
  return env.TERMINAL_SESSIONS.get(
    env.TERMINAL_SESSIONS.idFromName(`durable-alarm-mux-test:${name}`),
  );
}

describe("durable alarm mux", () => {
  it("persists independent snapshot and recovery deadlines without overwriting either owner", async () => {
    await runInDurableObject(alarmMuxStub("independent-deadlines"), async (_instance, durable) => {
      const now = Date.now();
      const snapshotDue = now + 60_000;
      const recoveryDue = now + 90_000;
      const mux = new DurableAlarmMux(durable, {});

      await mux.schedule(DurableAlarmComponent.snapshot, snapshotDue);
      await mux.schedule(DurableAlarmComponent.recovery, recoveryDue);
      expect(await durable.storage.getAlarm()).toBe(snapshotDue);

      await mux.clear(DurableAlarmComponent.snapshot);
      expect(await durable.storage.getAlarm()).toBe(recoveryDue);

      const earlierSnapshotDue = now + 30_000;
      await mux.schedule(DurableAlarmComponent.snapshot, earlierSnapshotDue);
      expect(await durable.storage.getAlarm()).toBe(earlierSnapshotDue);

      await durable.storage.deleteAlarm();
      const rebuilt = new DurableAlarmMux(durable, {});
      await rebuilt.initialize();
      expect(await durable.storage.getAlarm()).toBe(earlierSnapshotDue);

      await rebuilt.clear(DurableAlarmComponent.snapshot);
      expect(await durable.storage.getAlarm()).toBe(recoveryDue);
      await rebuilt.clear(DurableAlarmComponent.recovery);
      expect(await durable.storage.getAlarm()).toBeNull();
    });
  });

  it("restores a failed component deadline and recomputes after each idempotent handler", async () => {
    await runInDurableObject(alarmMuxStub("handler-retry"), async (_instance, durable) => {
      const now = Date.now();
      const recoveryDue = now + 30_000;
      const snapshotDue = now + 60_000;
      const nextSnapshotDue = now + 90_000;
      let clock = recoveryDue;
      let recoveryRuns = 0;
      let snapshotRuns = 0;
      let mux: DurableAlarmMux;
      mux = new DurableAlarmMux(
        durable,
        {
          async [DurableAlarmComponent.recovery]() {
            recoveryRuns += 1;
            if (recoveryRuns === 1) throw new Error("injected recovery alarm failure");
          },
          async [DurableAlarmComponent.snapshot]() {
            snapshotRuns += 1;
            await mux.schedule(DurableAlarmComponent.snapshot, nextSnapshotDue);
          },
        },
        () => clock,
      );

      await Promise.all([
        mux.schedule(DurableAlarmComponent.snapshot, snapshotDue),
        mux.schedule(DurableAlarmComponent.recovery, recoveryDue),
      ]);
      await expect(mux.alarm()).rejects.toThrow("injected recovery alarm failure");
      const recoveryRetryDue = recoveryDue + DURABLE_ALARM_FAILURE_RETRY_MS;
      expect({ alarm: await durable.storage.getAlarm(), recoveryRuns, snapshotRuns }).toEqual({
        alarm: recoveryRetryDue,
        recoveryRuns: 1,
        snapshotRuns: 0,
      });

      clock = recoveryRetryDue;
      await expect(mux.alarm()).resolves.toBe(DurableAlarmDispatch.dispatched);
      expect({ alarm: await durable.storage.getAlarm(), recoveryRuns, snapshotRuns }).toEqual({
        alarm: snapshotDue,
        recoveryRuns: 2,
        snapshotRuns: 0,
      });

      clock = snapshotDue;
      await expect(mux.alarm()).resolves.toBe(DurableAlarmDispatch.dispatched);
      expect({ alarm: await durable.storage.getAlarm(), recoveryRuns, snapshotRuns }).toEqual({
        alarm: nextSnapshotDue,
        recoveryRuns: 2,
        snapshotRuns: 1,
      });
      await mux.clear(DurableAlarmComponent.snapshot);
    });
  });

  it("schedules a failed long-running handler from its completion clock", async () => {
    await runInDurableObject(alarmMuxStub("long-handler-retry"), async (_instance, durable) => {
      const due = Date.now();
      const finishedAt = due + 30_000;
      let clock = due;
      const mux = new DurableAlarmMux(
        durable,
        {
          async [DurableAlarmComponent.recovery]() {
            clock = finishedAt;
            throw new Error("injected late recovery failure");
          },
        },
        () => clock,
      );

      await mux.schedule(DurableAlarmComponent.recovery, due);
      await expect(mux.alarm()).rejects.toThrow("injected late recovery failure");
      expect(await durable.storage.getAlarm()).toBe(finishedAt + DURABLE_ALARM_FAILURE_RETRY_MS);
      await mux.clear(DurableAlarmComponent.recovery);
    });
  });

  it("does not dispatch a stale alarm before the next durable component deadline", async () => {
    await runInDurableObject(alarmMuxStub("stale-early-delivery"), async (_instance, durable) => {
      const now = Date.now();
      const recoveryDue = now + 60_000;
      let recoveryRuns = 0;
      const mux = new DurableAlarmMux(
        durable,
        {
          async [DurableAlarmComponent.recovery]() {
            recoveryRuns += 1;
          },
        },
        () => now,
      );

      await mux.schedule(DurableAlarmComponent.recovery, recoveryDue);
      await expect(mux.alarm()).resolves.toBe(DurableAlarmDispatch.early);
      expect({ alarm: await durable.storage.getAlarm(), recoveryRuns }).toEqual({
        alarm: recoveryDue,
        recoveryRuns: 0,
      });
      await mux.clear(DurableAlarmComponent.recovery);
    });
  });

  it("deletes a platform alarm that has no current component fact", async () => {
    await runInDurableObject(alarmMuxStub("unowned-platform-alarm"), async (_instance, durable) => {
      const due = Date.now() + 30_000;
      await durable.storage.delete(ALARM_FACTS_KEY);
      await durable.storage.setAlarm(due);

      const mux = new DurableAlarmMux(durable, {}, () => due);
      await mux.initialize();

      expect(await durable.storage.getAlarm()).toBeNull();
      await expect(mux.alarm()).resolves.toBe(DurableAlarmDispatch.empty);
    });
  });

  it("re-arms a persisted active deadline during cold initialization", async () => {
    await runInDurableObject(alarmMuxStub("active-cold-start"), async (_instance, durable) => {
      const due = Date.now() + 30_000;
      let markStarted: (() => void) | undefined;
      let releaseHandler: (() => void) | undefined;
      const started = new Promise<void>((resolve) => {
        markStarted = resolve;
      });
      const handlerGate = new Promise<void>((resolve) => {
        releaseHandler = resolve;
      });
      const first = new DurableAlarmMux(
        durable,
        {
          async [DurableAlarmComponent.recovery]() {
            markStarted?.();
            await handlerGate;
          },
        },
        () => due,
      );

      await first.schedule(DurableAlarmComponent.recovery, due);
      const handling = first.alarm();
      await started;
      await durable.storage.deleteAlarm();

      const rebuilt = new DurableAlarmMux(durable, {}, () => due);
      await rebuilt.initialize();
      expect(await durable.storage.getAlarm()).toBe(due);

      releaseHandler?.();
      await expect(handling).resolves.toBe(DurableAlarmDispatch.dispatched);
      expect(await durable.storage.getAlarm()).toBeNull();
    });
  });

  it("rejects invalid deadlines without changing the durable alarm", async () => {
    await runInDurableObject(alarmMuxStub("invalid-deadlines"), async (_instance, durable) => {
      const mux = new DurableAlarmMux(durable, {});
      await expect(mux.schedule(DurableAlarmComponent.snapshot, Number.NaN)).rejects.toThrow(
        "non-negative safe integer",
      );
      await expect(mux.schedule(DurableAlarmComponent.recovery, -1)).rejects.toThrow(
        "non-negative safe integer",
      );
      expect(await durable.storage.getAlarm()).toBeNull();
    });
  });
});
