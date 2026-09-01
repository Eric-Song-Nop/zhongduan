import {
  BoundedKeyedQueue,
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

describe("BoundedKeyedQueue", () => {
  const limits = {
    globalBytes: 16,
    globalCount: 8,
    socketBytes: 8,
    socketCount: 4,
    maxAgeMs: 250,
    maxConcurrency: 2,
  };

  it("preserves connection FIFO while an independent connection makes progress", async () => {
    const queue = new BoundedKeyedQueue<object>(limits);
    const firstSocket = {};
    const secondSocket = {};
    let releaseFirst!: () => void;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const processed: string[] = [];
    const expire = () => {
      throw new Error("task unexpectedly expired");
    };

    const first = queue.enqueue(
      firstSocket,
      2,
      async () => {
        await firstGate;
        processed.push("first-1");
      },
      expire,
    );
    const second = queue.enqueue(
      firstSocket,
      2,
      () => {
        processed.push("first-2");
      },
      expire,
    );
    const independent = queue.enqueue(
      secondSocket,
      2,
      () => {
        processed.push("second-1");
      },
      expire,
    );

    await independent;
    expect(processed).toEqual(["second-1"]);
    expect(queue.activeCount).toBe(1);
    releaseFirst();
    await Promise.all([first, second]);
    expect(processed).toEqual(["second-1", "first-1", "first-2"]);
    expect(queue.queuedCount).toBe(0);
    expect(queue.queuedBytes).toBe(0);
  });

  it("settles synchronous same-connection work without building an asynchronous tail", async () => {
    const queue = new BoundedKeyedQueue<object>({
      ...limits,
      globalBytes: 64,
      globalCount: 64,
      socketBytes: 64,
      socketCount: 64,
      maxConcurrency: 1,
    });
    const socket = {};
    const processed: number[] = [];
    const pending: Promise<void>[] = [];

    for (let index = 0; index < 64; index += 1) {
      const task = queue.enqueue(
        socket,
        1,
        () => {
          processed.push(index);
        },
        () => undefined,
      );
      expect(task).toBeDefined();
      pending.push(task!);
    }

    expect(processed).toEqual(Array.from({ length: 64 }, (_, index) => index));
    expect(queue.queuedCount).toBe(0);
    expect(queue.queuedBytes).toBe(0);
    await Promise.all(pending);
  });

  it("rotates ready connections instead of letting one source monopolize a serial lane", async () => {
    const queue = new BoundedKeyedQueue<object>({ ...limits, maxConcurrency: 1 });
    const noisySocket = {};
    const healthySocket = {};
    let releaseFirst!: () => void;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const processed: string[] = [];
    const expire = () => undefined;

    const pending = [
      queue.enqueue(
        noisySocket,
        1,
        async () => {
          await firstGate;
          processed.push("noisy-1");
        },
        expire,
      ),
      queue.enqueue(
        noisySocket,
        1,
        () => {
          processed.push("noisy-2");
        },
        expire,
      ),
      queue.enqueue(
        noisySocket,
        1,
        () => {
          processed.push("noisy-3");
        },
        expire,
      ),
      queue.enqueue(
        healthySocket,
        1,
        () => {
          processed.push("healthy-1");
        },
        expire,
      ),
    ];

    releaseFirst();
    expect(pending.every((task) => task !== undefined)).toBe(true);
    await Promise.all(pending.filter((task): task is Promise<void> => task !== undefined));
    expect(processed).toEqual(["noisy-1", "healthy-1", "noisy-2", "noisy-3"]);
  });

  it("expires an aged item without invoking its work and releases the reservation", async () => {
    let now = 0;
    const queue = new BoundedKeyedQueue<object>({ ...limits, maxConcurrency: 1 }, () => now);
    const socket = {};
    let releaseFirst!: () => void;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    let ranExpired = false;
    let expiredWait = 0;

    const first = queue.enqueue(
      socket,
      4,
      () => firstGate,
      () => undefined,
    );
    const expired = queue.enqueue(
      socket,
      4,
      () => {
        ranExpired = true;
      },
      (timing) => {
        expiredWait = timing.waitMs;
      },
    );
    expect(
      queue.enqueue(
        socket,
        1,
        () => undefined,
        () => undefined,
      ),
    ).toBeUndefined();
    now = 251;
    releaseFirst();
    await Promise.all([first, expired]);

    expect(ranExpired).toBe(false);
    expect(expiredWait).toBe(251);
    expect(queue.queuedCount).toBe(0);
    expect(queue.queuedBytes).toBe(0);
  });

  it("enforces per-connection and lane-level count and byte reservations", async () => {
    const queue = new BoundedKeyedQueue<object>({ ...limits, maxConcurrency: 1 });
    const firstSocket = {};
    const secondSocket = {};
    let releaseFirst!: () => void;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const expire = () => undefined;

    const pending = [
      queue.enqueue(firstSocket, 4, () => firstGate, expire),
      queue.enqueue(firstSocket, 4, () => undefined, expire),
    ];
    expect(queue.enqueue(firstSocket, 1, () => undefined, expire)).toBeUndefined();
    pending.push(
      queue.enqueue(secondSocket, 4, () => undefined, expire),
      queue.enqueue(secondSocket, 4, () => undefined, expire),
    );
    expect(pending.every((task) => task !== undefined)).toBe(true);
    expect(queue.queuedCount).toBe(4);
    expect(queue.queuedBytes).toBe(16);
    expect(queue.enqueue({}, 1, () => undefined, expire)).toBeUndefined();

    releaseFirst();
    await Promise.all(pending.filter((task): task is Promise<void> => task !== undefined));
    expect(queue.queuedCount).toBe(0);
    expect(queue.queuedBytes).toBe(0);

    const countQueue = new BoundedKeyedQueue<object>(
      {
        ...limits,
        globalBytes: 1_024,
        globalCount: 2,
        socketBytes: 1_024,
        socketCount: 1,
        maxConcurrency: 1,
      },
      () => 0,
    );
    let releaseCount!: () => void;
    const countGate = new Promise<void>((resolve) => {
      releaseCount = resolve;
    });
    const countFirst = countQueue.enqueue(firstSocket, 1, () => countGate, expire);
    expect(countQueue.enqueue(firstSocket, 1, () => undefined, expire)).toBeUndefined();
    const countSecond = countQueue.enqueue(secondSocket, 1, () => undefined, expire);
    expect(countQueue.enqueue({}, 1, () => undefined, expire)).toBeUndefined();
    releaseCount();
    await Promise.all([countFirst, countSecond]);
    expect(countQueue.queuedCount).toBe(0);
  });
});
