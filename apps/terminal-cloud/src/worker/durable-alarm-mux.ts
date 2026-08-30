export const DurableAlarmComponent = {
  recovery: "recovery",
  snapshot: "snapshot",
} as const;

export type DurableAlarmComponent =
  (typeof DurableAlarmComponent)[keyof typeof DurableAlarmComponent];

export type DurableAlarmHandler = () => Promise<void>;

export const DurableAlarmDispatch = {
  dispatched: "dispatched",
  empty: "empty",
  early: "early",
} as const;

export type DurableAlarmDispatch = (typeof DurableAlarmDispatch)[keyof typeof DurableAlarmDispatch];

export interface DurableAlarmScheduler {
  clear(): Promise<void>;
  schedule(timestamp: number): Promise<void>;
}

export const DURABLE_ALARM_FAILURE_RETRY_MS = 2_000;

interface ComponentAlarmFact {
  activeDueAt?: number;
  dueAt?: number;
}

interface PersistedAlarmFacts {
  components: Partial<Record<DurableAlarmComponent, ComponentAlarmFact>>;
}

const ALARM_FACTS_KEY = "terminal-session:alarm-mux";
const COMPONENTS = [DurableAlarmComponent.snapshot, DurableAlarmComponent.recovery] as const;

function assertTimestamp(timestamp: number): void {
  if (!Number.isSafeInteger(timestamp) || timestamp < 0) {
    throw new RangeError("durable alarm timestamp must be a non-negative safe integer");
  }
}

function isAlarmFact(value: unknown): value is ComponentAlarmFact {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const fact = value as Record<string, unknown>;
  if (!Object.keys(fact).every((key) => key === "activeDueAt" || key === "dueAt")) return false;
  for (const timestamp of [fact.activeDueAt, fact.dueAt]) {
    if (
      timestamp !== undefined &&
      (typeof timestamp !== "number" || !Number.isSafeInteger(timestamp) || timestamp < 0)
    )
      return false;
  }
  return true;
}

function parseAlarmFacts(value: unknown): PersistedAlarmFacts {
  if (value === undefined) return { components: {} };
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("invalid durable alarm facts");
  }
  const stored = value as Record<string, unknown>;
  if (
    Object.keys(stored).length !== 1 ||
    typeof stored.components !== "object" ||
    stored.components === null ||
    Array.isArray(stored.components)
  ) {
    throw new Error("invalid durable alarm facts");
  }
  const components = stored.components as Record<string, unknown>;
  if (!Object.keys(components).every((component) => COMPONENTS.includes(component as never))) {
    throw new Error("invalid durable alarm facts");
  }
  const parsed: PersistedAlarmFacts = { components: {} };
  for (const component of COMPONENTS) {
    const fact = components[component];
    if (fact === undefined) continue;
    if (!isAlarmFact(fact)) throw new Error("invalid durable alarm facts");
    parsed.components[component] = { ...fact };
  }
  return parsed;
}

function minimum(left: number | undefined, right: number): number {
  return left === undefined ? right : Math.min(left, right);
}

function hasFacts(facts: PersistedAlarmFacts): boolean {
  return COMPONENTS.some((component) => facts.components[component] !== undefined);
}

function shouldPersist(facts: PersistedAlarmFacts): boolean {
  return hasFacts(facts);
}

/**
 * Owns the single Durable Object alarm for all TerminalSessionDO components.
 *
 * Component deadlines are durable facts. A claimed deadline stays durable while
 * its handler performs external work, so eviction or at-least-once delivery can
 * safely run the idempotent handler again.
 */
export class DurableAlarmMux {
  private alarmRun: Promise<DurableAlarmDispatch> | undefined;
  private factsRevision = 0;
  private mutationTail: Promise<void> = Promise.resolve();
  private reconcileRequested = false;
  private reconcileRun: Promise<void> | undefined;

  constructor(
    private readonly ctx: DurableObjectState,
    private readonly handlers: Readonly<
      Partial<Record<DurableAlarmComponent, DurableAlarmHandler>>
    >,
    private readonly now: () => number = Date.now,
  ) {}

  scheduler(component: DurableAlarmComponent): DurableAlarmScheduler {
    return {
      clear: () => this.clear(component),
      schedule: (timestamp) => this.schedule(component, timestamp),
    };
  }

  async schedule(component: DurableAlarmComponent, timestamp: number): Promise<void> {
    assertTimestamp(timestamp);
    await this.updateFacts((facts) => {
      const fact = facts.components[component] ?? {};
      fact.dueAt = minimum(fact.dueAt, timestamp);
      facts.components[component] = fact;
    });
    await this.reconcile();
  }

  async clear(component: DurableAlarmComponent): Promise<void> {
    await this.updateFacts((facts) => {
      const fact = facts.components[component];
      if (fact === undefined) return;
      delete fact.dueAt;
      if (fact.activeDueAt === undefined) delete facts.components[component];
    });
    await this.reconcile();
  }

  async initialize(): Promise<void> {
    await this.reconcile();
  }

  alarm(): Promise<DurableAlarmDispatch> {
    if (this.alarmRun !== undefined) return this.alarmRun;
    const run = this.runAlarm().finally(() => {
      if (this.alarmRun === run) this.alarmRun = undefined;
    });
    this.alarmRun = run;
    return run;
  }

  private async runAlarm(): Promise<DurableAlarmDispatch> {
    const now = this.now();
    assertTimestamp(now);
    const claim = await this.updateFacts((facts) => {
      const components = COMPONENTS.filter((component) => {
        const fact = facts.components[component];
        return fact?.activeDueAt !== undefined || (fact?.dueAt !== undefined && fact.dueAt <= now);
      });

      for (const component of components) {
        const fact = facts.components[component];
        const dueAt = fact?.dueAt;
        if (fact === undefined || dueAt === undefined || fact.activeDueAt !== undefined) continue;
        fact.activeDueAt = dueAt;
        delete fact.dueAt;
      }
      return { components, pending: hasFacts(facts) };
    });
    const claimed = claim.components;

    if (claimed.length === 0) {
      await this.reconcile();
      return claim.pending ? DurableAlarmDispatch.early : DurableAlarmDispatch.empty;
    }

    const failed = new Map<DurableAlarmComponent, unknown>();
    for (const component of claimed) {
      const handler = this.handlers[component];
      if (handler === undefined) {
        failed.set(component, new Error(`durable alarm handler is not registered: ${component}`));
        continue;
      }
      try {
        await handler();
      } catch (error) {
        failed.set(component, error);
      }
    }

    const retryClock = failed.size === 0 ? now : this.now();
    assertTimestamp(retryClock);
    const retryBase = Math.max(now, retryClock);
    const retryAt =
      retryBase > Number.MAX_SAFE_INTEGER - DURABLE_ALARM_FAILURE_RETRY_MS
        ? Number.MAX_SAFE_INTEGER
        : retryBase + DURABLE_ALARM_FAILURE_RETRY_MS;
    await this.updateFacts((facts) => {
      for (const component of claimed) {
        const fact = facts.components[component];
        if (fact === undefined) continue;
        if (failed.has(component)) {
          fact.dueAt = Math.max(fact.dueAt ?? retryAt, retryAt);
        }
        delete fact.activeDueAt;
        if (fact.dueAt === undefined) delete facts.components[component];
      }
    });

    let reconcileError: unknown;
    let reconcileFailed = false;
    try {
      await this.reconcile();
    } catch (error) {
      reconcileFailed = true;
      reconcileError = error;
    }
    const handlerError = failed.values().next();
    if (!handlerError.done) throw handlerError.value;
    if (reconcileFailed) throw reconcileError;
    return DurableAlarmDispatch.dispatched;
  }

  private updateFacts<T>(update: (facts: PersistedAlarmFacts) => T | Promise<T>): Promise<T> {
    const operation = this.mutationTail.then(async () => {
      const facts = parseAlarmFacts(await this.ctx.storage.get<unknown>(ALARM_FACTS_KEY));
      const result = await update(facts);
      if (shouldPersist(facts)) {
        await this.ctx.storage.put(ALARM_FACTS_KEY, facts);
      } else {
        await this.ctx.storage.delete(ALARM_FACTS_KEY);
      }
      this.factsRevision += 1;
      return result;
    });
    this.mutationTail = operation.then(
      () => undefined,
      () => undefined,
    );
    return operation;
  }

  private readFacts(): Promise<PersistedAlarmFacts> {
    const operation = this.mutationTail.then(async () =>
      parseAlarmFacts(await this.ctx.storage.get<unknown>(ALARM_FACTS_KEY)),
    );
    this.mutationTail = operation.then(
      () => undefined,
      () => undefined,
    );
    return operation;
  }

  private reconcile(): Promise<void> {
    this.reconcileRequested = true;
    if (this.reconcileRun !== undefined) return this.reconcileRun;
    const run = this.flushReconcile().finally(() => {
      if (this.reconcileRun === run) this.reconcileRun = undefined;
    });
    this.reconcileRun = run;
    return run;
  }

  private async flushReconcile(): Promise<void> {
    while (this.reconcileRequested) {
      this.reconcileRequested = false;
      const revision = this.factsRevision;
      const facts = await this.readFacts();
      const due = COMPONENTS.flatMap((component) => {
        const fact = facts.components[component];
        if (fact === undefined) return [];
        const timestamps = fact.dueAt === undefined ? [] : [fact.dueAt];
        if (this.alarmRun === undefined && fact.activeDueAt !== undefined) {
          timestamps.push(fact.activeDueAt);
        }
        return timestamps;
      });
      const target = due.length === 0 ? undefined : Math.min(...due);
      const current = await this.ctx.storage.getAlarm();
      if (revision !== this.factsRevision || this.reconcileRequested) {
        this.reconcileRequested = true;
        continue;
      }
      if (target === undefined) {
        if (current !== null) await this.ctx.storage.deleteAlarm();
      } else if (current !== target) {
        await this.ctx.storage.setAlarm(target);
      }
      if (revision !== this.factsRevision) this.reconcileRequested = true;
    }
  }
}
