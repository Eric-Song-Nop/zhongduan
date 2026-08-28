import {
  BoundedSerialQueue,
  RELAY_MESSAGE_QUEUE_PROFILES,
  type QueueLimits,
  type RelayQueueObservation,
  type RelayQueueTaskContext,
} from "../src/worker/relay-message-queue";
import { describe, expect, it } from "vitest";

async function flushDetachedObservers(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

describe("BoundedSerialQueue", () => {
  it("bounds a burst while the first task is awaiting and releases every reservation", async () => {
    const queue = new BoundedSerialQueue<object>({
      globalBytes: 8,
      globalCount: 4,
      socketBytes: 6,
      socketCount: 3,
    });
    const firstSocket = {};
    const secondSocket = {};
    let releaseFirst!: () => void;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const processed: number[] = [];

    const first = queue.enqueue(firstSocket, 2, async () => {
      await firstGate;
      processed.push(1);
    });
    const second = queue.enqueue(firstSocket, 2, async () => {
      processed.push(2);
    });
    const third = queue.enqueue(firstSocket, 2, async () => {
      processed.push(3);
    });
    const otherSocket = queue.enqueue(secondSocket, 2, async () => {
      processed.push(4);
    });

    expect(queue.enqueue(firstSocket, 1, async () => undefined)).toBeUndefined();
    expect(queue.enqueue(secondSocket, 1, async () => undefined)).toBeUndefined();
    expect(queue.queuedCount).toBe(4);
    expect(queue.queuedBytes).toBe(8);

    releaseFirst();
    await Promise.all([first, second, third, otherSocket]);
    expect(processed).toEqual([1, 2, 3, 4]);
    expect(queue.queuedCount).toBe(0);
    expect(queue.queuedBytes).toBe(0);
  });

  it("releases a failed task and continues the serial queue", async () => {
    const queue = new BoundedSerialQueue<object>({
      globalBytes: 4,
      globalCount: 2,
      socketBytes: 4,
      socketCount: 2,
    });
    const socket = {};
    const failed = queue.enqueue(socket, 2, async () => {
      throw new Error("failed");
    });
    let continued = false;
    const next = queue.enqueue(socket, 2, async () => {
      continued = true;
    });

    await expect(failed).rejects.toThrow("failed");
    await next;
    expect(continued).toBe(true);
    expect(queue.queuedCount).toBe(0);
  });

  it("accepts a bounded 16 MiB host data burst before applying backpressure", async () => {
    const queue = new BoundedSerialQueue<object>();
    const socket = {};
    let releaseFirst!: () => void;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const pending: Promise<void>[] = [];
    for (let index = 0; index < 1_024; index += 1) {
      const task = queue.enqueue(
        socket,
        16 * 1024,
        index === 0 ? () => firstGate : async () => undefined,
        RELAY_MESSAGE_QUEUE_PROFILES.hostData,
      );
      expect(task).toBeDefined();
      pending.push(task!);
    }
    expect(queue.queuedBytes).toBe(16 * 1024 * 1024);
    expect(
      queue.enqueue(socket, 1, async () => undefined, RELAY_MESSAGE_QUEUE_PROFILES.hostData),
    ).toBeUndefined();

    releaseFirst();
    await Promise.all(pending);
    expect(queue.queuedCount).toBe(0);
  });

  it("passes immutable admission context and reports fake-clock task boundaries", async () => {
    const clockValues = [10, 11, 20, 21, 30, 40, 50, 60];
    const observations: RelayQueueObservation[] = [];
    const contexts: Readonly<RelayQueueTaskContext>[] = [];
    const queue = new BoundedSerialQueue<object>(
      {
        globalBytes: 8,
        globalCount: 4,
        socketBytes: 8,
        socketCount: 4,
      },
      {
        clock: () => {
          const value = clockValues.shift();
          if (value === undefined) throw new Error("fake clock exhausted");
          return value;
        },
        observer: (observation) => {
          observations.push(observation);
        },
      },
    );
    const socket = {};
    let releaseFirst!: () => void;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });

    const first = queue.enqueue(
      socket,
      2,
      async (context) => {
        contexts.push(context);
        await firstGate;
      },
      undefined,
      { queueProfile: "browser-control" },
    );
    const second = queue.enqueue(
      socket,
      3,
      async (context) => {
        contexts.push(context);
      },
      undefined,
      { queueProfile: "browser-control" },
    );

    await Promise.resolve();
    releaseFirst();
    await Promise.all([first, second]);
    await flushDetachedObservers();

    expect(clockValues).toEqual([]);
    expect(contexts).toHaveLength(2);
    expect(contexts[0]).toMatchObject({
      queueProfile: "browser-control",
      reservationBytes: 2,
      globalOutstandingBytes: 2,
      globalOutstandingCount: 1,
      socketOutstandingBytes: 2,
      socketOutstandingCount: 1,
      admissionStartedAt: 10,
      admittedAt: 11,
      observedAdmissionMs: 1,
    });
    expect(contexts[1]).toMatchObject({
      queueProfile: "browser-control",
      reservationBytes: 3,
      globalOutstandingBytes: 5,
      globalOutstandingCount: 2,
      socketOutstandingBytes: 5,
      socketOutstandingCount: 2,
      admissionStartedAt: 20,
      admittedAt: 21,
      observedAdmissionMs: 1,
    });
    for (const context of contexts) {
      expect(Object.isFrozen(context)).toBe(true);
      expect(context.observedQueueWaitMs).toBe(context.startedAt! - context.admittedAt!);
    }

    expect(observations).toHaveLength(2);
    expect(observations.map((observation) => observation.outcome)).toEqual([
      "completed",
      "completed",
    ]);
    for (const observation of observations) {
      expect(Object.isFrozen(observation)).toBe(true);
      expect(observation.queueProfile).toBe("browser-control");
      if (observation.outcome !== "capacity") {
        expect(observation.admittedAt).toBeLessThanOrEqual(observation.startedAt);
        expect(observation.startedAt).toBeLessThanOrEqual(observation.finishedAt);
      }
    }
  });

  it.each<{
    bytes: number;
    limits: QueueLimits;
    reason: "global-count" | "global-bytes" | "socket-count" | "socket-bytes" | "invalid-size";
  }>([
    {
      bytes: -1,
      limits: { globalBytes: 8, globalCount: 8, socketBytes: 8, socketCount: 8 },
      reason: "invalid-size",
    },
    {
      bytes: 1,
      limits: { globalBytes: 8, globalCount: 0, socketBytes: 8, socketCount: 8 },
      reason: "global-count",
    },
    {
      bytes: 1,
      limits: { globalBytes: 0, globalCount: 8, socketBytes: 8, socketCount: 8 },
      reason: "global-bytes",
    },
    {
      bytes: 1,
      limits: { globalBytes: 8, globalCount: 8, socketBytes: 8, socketCount: 0 },
      reason: "socket-count",
    },
    {
      bytes: 1,
      limits: { globalBytes: 8, globalCount: 8, socketBytes: 0, socketCount: 8 },
      reason: "socket-bytes",
    },
  ])("reports the $reason capacity decision without running the task", async (testCase) => {
    const observations: RelayQueueObservation[] = [];
    let taskRan = false;
    const queue = new BoundedSerialQueue<object>(testCase.limits, {
      clock: () => 42,
      observer: (observation) => {
        observations.push(observation);
      },
    });

    expect(
      queue.enqueue(
        {},
        testCase.bytes,
        async () => {
          taskRan = true;
        },
        undefined,
        { queueProfile: "host-control" },
      ),
    ).toBeUndefined();
    await flushDetachedObservers();

    expect(taskRan).toBe(false);
    expect(queue.queuedBytes).toBe(0);
    expect(queue.queuedCount).toBe(0);
    expect(observations).toEqual([
      expect.objectContaining({
        outcome: "capacity",
        capacityReason: testCase.reason,
        queueProfile: "host-control",
        reservationBytes: testCase.reason === "invalid-size" ? 0 : testCase.bytes,
        admissionStartedAt: 42,
        admittedAt: 42,
        globalOutstandingBytes: 0,
        globalOutstandingCount: 0,
        socketOutstandingBytes: 0,
        socketOutstandingCount: 0,
      }),
    ]);
  });

  it("contains observer failures outside the returned task promises", async () => {
    const observedOutcomes: RelayQueueObservation["outcome"][] = [];
    let now = 0;
    const queue = new BoundedSerialQueue<object>(
      {
        globalBytes: 8,
        globalCount: 3,
        socketBytes: 8,
        socketCount: 3,
      },
      {
        clock: () => (now += 1),
        observer: async (observation) => {
          observedOutcomes.push(observation.outcome);
          throw new Error("observer unavailable");
        },
      },
    );
    const socket = {};
    const originalFailure = new Error("task failed");
    const completed = queue.enqueue(socket, 1, async () => undefined);
    const failed = queue.enqueue(socket, 1, async () => {
      throw originalFailure;
    });
    let continued = false;
    const continuedTask = queue.enqueue(socket, 1, async () => {
      continued = true;
    });

    await completed;
    await expect(failed).rejects.toBe(originalFailure);
    await continuedTask;
    await flushDetachedObservers();

    expect(continued).toBe(true);
    expect(observedOutcomes).toEqual(["completed", "failed", "completed"]);
    expect(queue.queuedCount).toBe(0);
  });

  it("skips observations when the clock throws without changing admission or completion", async () => {
    const observations: RelayQueueObservation[] = [];
    const queue = new BoundedSerialQueue<object>(
      {
        globalBytes: 2,
        globalCount: 1,
        socketBytes: 2,
        socketCount: 1,
      },
      {
        clock: () => {
          throw new Error("clock unavailable");
        },
        observer: (observation) => {
          observations.push(observation);
        },
      },
    );
    const socket = {};
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let taskContext: Readonly<RelayQueueTaskContext> | undefined;
    const accepted = queue.enqueue(socket, 1, async (context) => {
      taskContext = context;
      await gate;
    });

    expect(queue.enqueue(socket, 1, async () => undefined)).toBeUndefined();
    await Promise.resolve();
    expect(taskContext).toMatchObject({
      reservationBytes: 1,
      globalOutstandingBytes: 1,
      globalOutstandingCount: 1,
      socketOutstandingBytes: 1,
      socketOutstandingCount: 1,
    });
    expect(taskContext?.admittedAt).toBeUndefined();
    expect(taskContext?.admissionStartedAt).toBeUndefined();
    expect(taskContext?.observedAdmissionMs).toBeUndefined();
    expect(taskContext?.startedAt).toBeUndefined();
    expect(taskContext?.observedQueueWaitMs).toBeUndefined();

    release();
    await accepted;
    await flushDetachedObservers();
    expect(observations).toEqual([]);
    expect(queue.queuedCount).toBe(0);
  });

  it("does not read the clock when the observer kill switch is off", async () => {
    let clockReads = 0;
    const queue = new BoundedSerialQueue<object>(
      {
        globalBytes: 2,
        globalCount: 1,
        socketBytes: 2,
        socketCount: 1,
      },
      {
        clock: () => {
          clockReads += 1;
          return clockReads;
        },
      },
    );
    const socket = {};
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let taskContext: Readonly<RelayQueueTaskContext> | undefined;
    const accepted = queue.enqueue(socket, 1, async (context) => {
      taskContext = context;
      await gate;
    });

    expect(queue.enqueue(socket, 1, async () => undefined)).toBeUndefined();
    await Promise.resolve();
    expect(taskContext).toMatchObject({
      reservationBytes: 1,
      globalOutstandingBytes: 1,
      globalOutstandingCount: 1,
      socketOutstandingBytes: 1,
      socketOutstandingCount: 1,
    });
    expect(taskContext?.admittedAt).toBeUndefined();
    expect(taskContext?.admissionStartedAt).toBeUndefined();
    expect(taskContext?.observedAdmissionMs).toBeUndefined();
    expect(taskContext?.startedAt).toBeUndefined();
    expect(taskContext?.observedQueueWaitMs).toBeUndefined();

    release();
    await accepted;
    expect(clockReads).toBe(0);
    expect(queue.queuedCount).toBe(0);
  });
});
