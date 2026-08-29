import type { DurableAlarmScheduler } from "./durable-alarm-mux";

interface RecoveryMaintenanceStore {
  expireDeadlines(now: number): readonly string[];
  nextDeadline(): number | undefined;
  pruneFencedTerminalAttempts(): readonly string[];
}

export interface RelayRecoveryMaintenanceOptions {
  now?: () => number;
  retryDelayMs?: number;
}

export interface RelayRecoveryMaintenanceResult {
  expired: readonly string[];
  pruned: readonly string[];
}

const DEFAULT_RECOVERY_ALARM_RETRY_MS = 1_000;

function assertTimestamp(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${label} must be a non-negative safe integer`);
  }
}

/**
 * Reconciles SQL-owned recovery deadlines with the recovery component of the
 * shared Durable Object alarm. Payload/control delivery remains a separate
 * owner; this class only advances durable cleanup facts.
 */
export class RelayRecoveryMaintenance {
  private readonly now: () => number;
  private readonly retryDelayMs: number;
  private operationTail: Promise<void> = Promise.resolve();

  constructor(
    private readonly state: DurableObjectState,
    private readonly recoveries: RecoveryMaintenanceStore,
    private readonly alarm: DurableAlarmScheduler,
    options: RelayRecoveryMaintenanceOptions = {},
  ) {
    this.now = options.now ?? Date.now;
    this.retryDelayMs = options.retryDelayMs ?? DEFAULT_RECOVERY_ALARM_RETRY_MS;
    if (!Number.isSafeInteger(this.retryDelayMs) || this.retryDelayMs <= 0) {
      throw new RangeError("recovery alarm retry delay must be a positive safe integer");
    }
  }

  initialize(): Promise<RelayRecoveryMaintenanceResult> {
    return this.enqueue(false);
  }

  refresh(): Promise<RelayRecoveryMaintenanceResult> {
    return this.enqueue(false);
  }

  maintain(): Promise<RelayRecoveryMaintenanceResult> {
    return this.enqueue(true);
  }

  private enqueue(expire: boolean): Promise<RelayRecoveryMaintenanceResult> {
    const operation = this.operationTail.then(() => this.reconcile(expire));
    this.operationTail = operation.then(
      () => undefined,
      () => undefined,
    );
    return operation;
  }

  private async reconcile(expire: boolean): Promise<RelayRecoveryMaintenanceResult> {
    const now = this.now();
    assertTimestamp(now, "recovery maintenance clock");
    let expired: readonly string[] = [];
    let nextDeadline: number | undefined;
    let pruned: readonly string[] = [];
    this.state.storage.transactionSync(() => {
      pruned = this.recoveries.pruneFencedTerminalAttempts();
      if (expire) expired = this.recoveries.expireDeadlines(now);
      nextDeadline = this.recoveries.nextDeadline();
    });

    if (nextDeadline === undefined) {
      await this.alarm.clear();
      return { expired, pruned };
    }
    assertTimestamp(nextDeadline, "recovery deadline");
    const retryAt = Math.min(Number.MAX_SAFE_INTEGER, now + this.retryDelayMs);
    await this.alarm.schedule(nextDeadline <= now ? retryAt : nextDeadline);
    return { expired, pruned };
  }
}
