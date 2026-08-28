import { BrowserTelemetryEventSchema, type BrowserTelemetryEvent } from "@zhongduan/telemetry";

const DEFAULT_MAX_PENDING_EVENTS = 256;
const DEFAULT_MAX_RING_EVENTS = 256;
const MAX_DIAGNOSTIC_CAPACITY = 256;

export interface BrowserDiagnosticsOptions {
  capacity?: number;
  maxPendingEvents?: number;
  /** @deprecated Use capacity. */
  maxRingEvents?: number;
  schedule?: (task: () => void) => void;
}

export interface BrowserDiagnosticsSnapshot {
  readonly events: readonly BrowserTelemetryEvent[];
  readonly droppedEvents: number;
  readonly pendingEvents: number;
}

export interface BrowserDiagnostics {
  readonly droppedEvents: number;
  readonly pendingEvents: number;
  readonly retainedEvents: number;
  record(event: BrowserTelemetryEvent): void;
  snapshot(): BrowserDiagnosticsSnapshot;
  flush(): Promise<void>;
  clear(): void;
}

function arrayCapacity(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0 || value > MAX_DIAGNOSTIC_CAPACITY) {
    throw new RangeError(`${name} must be an integer between 1 and 256`);
  }
  return value;
}

/**
 * Keeps validated Browser diagnostics in memory only. Admission is constant-time; strict schema
 * validation and ring mutation happen from the deferred drain, outside terminal callbacks.
 */
export function createBrowserDiagnostics(
  options: BrowserDiagnosticsOptions = {},
): BrowserDiagnostics {
  const maxPendingEvents = arrayCapacity(
    options.maxPendingEvents ?? DEFAULT_MAX_PENDING_EVENTS,
    "maxPendingEvents",
  );
  const maxRingEvents = arrayCapacity(
    options.capacity ?? options.maxRingEvents ?? DEFAULT_MAX_RING_EVENTS,
    "capacity",
  );
  const schedule = options.schedule ?? ((task: () => void) => globalThis.queueMicrotask(task));

  const pending: Array<BrowserTelemetryEvent | undefined> = Array.from({
    length: maxPendingEvents,
  });
  const ring: Array<BrowserTelemetryEvent | undefined> = Array.from({ length: maxRingEvents });
  let flushPromise: Promise<void> | null = null;
  let resolveFlush: (() => void) | null = null;
  let pendingStart = 0;
  let pendingSize = 0;
  let ringStart = 0;
  let ringSize = 0;
  let droppedEvents = 0;
  let scheduled = false;

  const countDropped = (count = 1) => {
    droppedEvents = Math.min(Number.MAX_SAFE_INTEGER, droppedEvents + count);
  };

  const settleFlush = () => {
    if (scheduled || pendingSize > 0) return;
    const resolve = resolveFlush;
    resolveFlush = null;
    flushPromise = null;
    resolve?.();
  };

  const clearPending = (countAsDropped: boolean) => {
    if (countAsDropped) countDropped(pendingSize);
    for (let offset = 0; offset < pendingSize; offset += 1) {
      pending[(pendingStart + offset) % maxPendingEvents] = undefined;
    }
    pendingStart = 0;
    pendingSize = 0;
  };

  const retain = (event: BrowserTelemetryEvent) => {
    if (ringSize < maxRingEvents) {
      ring[(ringStart + ringSize) % maxRingEvents] = event;
      ringSize += 1;
      return;
    }
    ring[ringStart] = event;
    ringStart = (ringStart + 1) % maxRingEvents;
    countDropped();
  };

  const drain = () => {
    scheduled = false;
    while (pendingSize > 0) {
      const index = pendingStart;
      const event = pending[index];
      pending[index] = undefined;
      pendingStart = (pendingStart + 1) % maxPendingEvents;
      pendingSize -= 1;
      try {
        retain(BrowserTelemetryEventSchema.parse(event));
      } catch {
        countDropped();
      }
    }
    if (pendingSize === 0) pendingStart = 0;
    settleFlush();
  };

  const scheduleDrain = () => {
    if (scheduled || pendingSize === 0) return;
    scheduled = true;
    try {
      schedule(drain);
    } catch {
      scheduled = false;
      clearPending(true);
      settleFlush();
    }
  };

  return {
    get droppedEvents() {
      return droppedEvents;
    },
    get pendingEvents() {
      return pendingSize;
    },
    get retainedEvents() {
      return ringSize;
    },
    record(event) {
      try {
        if (pendingSize >= maxPendingEvents) {
          countDropped();
          return;
        }
        const index = (pendingStart + pendingSize) % maxPendingEvents;
        pending[index] = { ...event };
        pendingSize += 1;
        scheduleDrain();
      } catch {
        countDropped();
      }
    },
    snapshot() {
      const events: BrowserTelemetryEvent[] = [];
      for (let offset = 0; offset < ringSize; offset += 1) {
        const event = ring[(ringStart + offset) % maxRingEvents];
        if (event !== undefined) events.push({ ...event });
      }
      events.sort((left, right) => left.monotonicAtMs - right.monotonicAtMs);
      return {
        events,
        droppedEvents,
        pendingEvents: pendingSize,
      };
    },
    flush() {
      scheduleDrain();
      if (!scheduled && pendingSize === 0) return Promise.resolve();
      if (flushPromise !== null) return flushPromise;
      flushPromise = new Promise((resolve) => {
        resolveFlush = resolve;
      });
      return flushPromise;
    },
    clear() {
      clearPending(false);
      for (let offset = 0; offset < ringSize; offset += 1) {
        ring[(ringStart + offset) % maxRingEvents] = undefined;
      }
      ringStart = 0;
      ringSize = 0;
      settleFlush();
    },
  };
}
