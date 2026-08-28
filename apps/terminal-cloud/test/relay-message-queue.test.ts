import {
  BoundedSerialQueue,
  RELAY_MESSAGE_QUEUE_PROFILES,
} from "../src/worker/relay-message-queue";
import { describe, expect, it } from "vitest";

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
});
