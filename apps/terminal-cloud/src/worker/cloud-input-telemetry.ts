export const CLOUD_INPUT_TELEMETRY_LIMITS = Object.freeze({
  maxAgeMs: 60_000,
  maxRecords: 256,
});

export type CloudInputDisposition =
  | "host-sent"
  | "host-send-uncertain"
  | "lane-expired"
  | "lane-overload"
  | "rejected";

export interface CloudInputSpan {
  readonly clientId: string;
  readonly clientInputSeq: string;
  readonly connectionId: string;
  readonly disposition: CloudInputDisposition;
  readonly hostSendAtMs: number | null;
  readonly inputEpoch: string;
  readonly queueEnteredAtMs: number;
  readonly queueLeftAtMs: number;
  readonly receivedAtMs: number;
  readonly writerFence: string | null;
}

export interface CloudInputTelemetrySnapshot {
  readonly dispositionCounts: Readonly<Record<CloudInputDisposition, number>>;
  readonly maxQueueWaitMs: number;
  readonly records: readonly CloudInputSpan[];
  readonly retainedCount: number;
  readonly totalCount: number;
}

function emptyDispositionCounts(): Record<CloudInputDisposition, number> {
  return {
    "host-sent": 0,
    "host-send-uncertain": 0,
    "lane-expired": 0,
    "lane-overload": 0,
    rejected: 0,
  };
}

/**
 * Bounded in-memory E2 evidence. It retains identities and phase timestamps, never payloads, lease
 * tokens, hashes, or durable per-key results. Hibernation may discard it without changing behavior.
 */
export class CloudInputTelemetry {
  readonly #records: CloudInputSpan[] = [];
  #totalCount = 0;

  record(span: CloudInputSpan): void {
    if (
      span.queueEnteredAtMs < span.receivedAtMs ||
      span.queueLeftAtMs < span.queueEnteredAtMs ||
      (span.hostSendAtMs !== null && span.hostSendAtMs < span.queueLeftAtMs)
    ) {
      throw new Error("cloud input span timestamps are not monotonic");
    }
    this.#records.push(Object.freeze({ ...span }));
    this.#totalCount += 1;
    this.#prune(span.queueLeftAtMs);
  }

  snapshot(now = Date.now()): CloudInputTelemetrySnapshot {
    this.#prune(now);
    const dispositionCounts = emptyDispositionCounts();
    let maxQueueWaitMs = 0;
    for (const record of this.#records) {
      dispositionCounts[record.disposition] += 1;
      maxQueueWaitMs = Math.max(maxQueueWaitMs, record.queueLeftAtMs - record.queueEnteredAtMs);
    }
    return Object.freeze({
      dispositionCounts: Object.freeze(dispositionCounts),
      maxQueueWaitMs,
      records: Object.freeze(this.#records.map((record) => Object.freeze({ ...record }))),
      retainedCount: this.#records.length,
      totalCount: this.#totalCount,
    });
  }

  #prune(now: number): void {
    const minimumTimestamp = now - CLOUD_INPUT_TELEMETRY_LIMITS.maxAgeMs;
    while (
      this.#records.length > CLOUD_INPUT_TELEMETRY_LIMITS.maxRecords ||
      (this.#records[0]?.queueLeftAtMs ?? Number.POSITIVE_INFINITY) < minimumTimestamp
    ) {
      this.#records.shift();
    }
  }
}
