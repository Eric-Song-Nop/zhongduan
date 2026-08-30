import {
  type RelayDeliveryGenerationIdentity,
  type RelayDeliveryLane,
  type RelayDeliveryRef,
  type RelayDeliveryRefIdentity,
  RelayDeliveryRing,
} from "./relay-delivery-ring";

export const RELAY_DELIVERY_CLASSES = [
  "writer-live",
  "observer-live",
  "writer-recovery",
  "observer-recovery",
] as const;

export type RelayDeliveryClass = (typeof RELAY_DELIVERY_CLASSES)[number];

export const RELAY_DELIVERY_CLASS_WEIGHTS: Readonly<Record<RelayDeliveryClass, number>> =
  Object.freeze({
    "writer-live": 4,
    "observer-live": 2,
    "writer-recovery": 2,
    "observer-recovery": 1,
  });

const weightedClassRound: readonly RelayDeliveryClass[] = RELAY_DELIVERY_CLASSES.flatMap(
  (deliveryClass) =>
    Array<RelayDeliveryClass>(RELAY_DELIVERY_CLASS_WEIGHTS[deliveryClass]).fill(deliveryClass),
);

export const RELAY_DELIVERY_BASE_QUANTUM_BYTES = 64 * 1024;
export const RELAY_DELIVERY_MAX_DEFICIT_BYTES = 512 * 1024;
export const RELAY_DELIVERY_MAX_RECORDS_PER_VISIT = 4;

export type RelayDeliverySendResult = "sent" | "stale" | "fatal";

export interface RelayDeliveryJobView {
  readonly deliveryClass: RelayDeliveryClass;
  readonly ref: RelayDeliveryRef;
  readonly identity: RelayDeliveryRefIdentity;
  readonly encodedBytes: number;
}

export interface RelayDeliverySendTurn extends RelayDeliveryJobView {
  readonly payload: Uint8Array;
}

export interface RelayDeliverySchedulerOptions {
  readonly ring: RelayDeliveryRing;
  readonly yieldDataTurn: (delayMs: number) => Promise<void>;
  readonly send: (
    turn: RelayDeliverySendTurn,
  ) => RelayDeliverySendResult | Promise<RelayDeliverySendResult>;
  readonly onFailure: (job: RelayDeliveryJobView) => void | Promise<void>;
}

export interface RelayDeliveryEnqueue {
  readonly deliveryClass: RelayDeliveryClass;
  readonly ref: RelayDeliveryRef;
}

type JobState = "queued" | "yielding" | "sending" | "done" | "cancelled";

interface PendingJob extends RelayDeliveryJobView {
  readonly flowKey: string;
  state: JobState;
}

interface DeliveryFlow {
  deficit: number;
  readonly jobs: PendingJob[];
}

interface DeliveryClassState {
  readonly flowOrder: string[];
  readonly flows: Map<string, DeliveryFlow>;
}

function laneForClass(deliveryClass: RelayDeliveryClass): RelayDeliveryLane {
  return deliveryClass === "writer-live" || deliveryClass === "observer-live" ? "live" : "recovery";
}

function flowKey(deliveryClass: RelayDeliveryClass, identity: RelayDeliveryRefIdentity): string {
  return JSON.stringify([
    deliveryClass,
    identity.recoveryId,
    identity.clientId,
    identity.connectionId,
    identity.streamId,
    identity.deliveryGeneration,
    identity.lane,
  ]);
}

function sameGeneration(
  candidate: RelayDeliveryRefIdentity,
  expected: RelayDeliveryGenerationIdentity,
): boolean {
  return (
    candidate.recoveryId === expected.recoveryId &&
    candidate.clientId === expected.clientId &&
    candidate.connectionId === expected.connectionId &&
    candidate.streamId === expected.streamId &&
    candidate.deliveryGeneration === expected.deliveryGeneration
  );
}

export class RelayDeliveryScheduler {
  readonly #ring: RelayDeliveryRing;
  readonly #yieldDataTurn: (delayMs: number) => Promise<void>;
  readonly #send: RelayDeliverySchedulerOptions["send"];
  readonly #onFailure: RelayDeliverySchedulerOptions["onFailure"];
  readonly #classes = new Map<RelayDeliveryClass, DeliveryClassState>();
  readonly #jobs = new Map<RelayDeliveryRef, PendingJob>();
  #classCursor = 0;
  #drainPromise: Promise<void> | undefined;
  #disposed = false;

  constructor(options: RelayDeliverySchedulerOptions) {
    this.#ring = options.ring;
    this.#yieldDataTurn = options.yieldDataTurn;
    this.#send = options.send;
    this.#onFailure = options.onFailure;
    for (const deliveryClass of RELAY_DELIVERY_CLASSES) {
      this.#classes.set(deliveryClass, { flowOrder: [], flows: new Map() });
    }
  }

  get queuedRecords(): number {
    return this.#jobs.size;
  }

  enqueue(input: RelayDeliveryEnqueue): boolean {
    if (this.#disposed || this.#jobs.has(input.ref)) return false;
    const identity = this.#ring.identity(input.ref);
    const encodedBytes = this.#ring.encodedBytes(input.ref);
    if (
      identity === undefined ||
      encodedBytes === undefined ||
      encodedBytes <= 0 ||
      encodedBytes > RELAY_DELIVERY_MAX_DEFICIT_BYTES ||
      identity.lane !== laneForClass(input.deliveryClass)
    ) {
      return false;
    }

    const key = flowKey(input.deliveryClass, identity);
    const classState = this.#classes.get(input.deliveryClass)!;
    let flow = classState.flows.get(key);
    if (flow === undefined) {
      flow = { deficit: 0, jobs: [] };
      classState.flows.set(key, flow);
      classState.flowOrder.push(key);
    }
    const job: PendingJob = {
      deliveryClass: input.deliveryClass,
      ref: input.ref,
      identity,
      encodedBytes,
      flowKey: key,
      state: "queued",
    };
    flow.jobs.push(job);
    this.#jobs.set(job.ref, job);
    this.#ensureDrain();
    return true;
  }

  cancel(ref: RelayDeliveryRef): boolean {
    const job = this.#jobs.get(ref);
    if (job === undefined || job.state === "sending" || job.state === "done") return false;
    job.state = "cancelled";
    this.#detach(job);
    this.#ring.cancel(ref);
    return true;
  }

  forgetGeneration(identity: RelayDeliveryGenerationIdentity): number {
    const refs = [...this.#jobs.values()]
      .filter((job) => sameGeneration(job.identity, identity))
      .map((job) => job.ref);
    let forgotten = 0;
    for (const ref of refs) {
      if (this.cancel(ref)) forgotten += 1;
    }
    return forgotten;
  }

  dispose(): number {
    if (this.#disposed) return 0;
    this.#disposed = true;
    const refs = [...this.#jobs.keys()];
    let cancelled = 0;
    for (const ref of refs) {
      if (this.cancel(ref)) cancelled += 1;
    }
    return cancelled;
  }

  async whenIdle(): Promise<void> {
    while (this.#drainPromise !== undefined) {
      await this.#drainPromise;
    }
  }

  #ensureDrain(): void {
    if (this.#disposed || this.#drainPromise !== undefined || this.#jobs.size === 0) return;
    this.#drainPromise = this.#drainAndFinalize();
  }

  async #drainAndFinalize(): Promise<void> {
    try {
      await this.#drain();
    } catch {
      this.dispose();
    } finally {
      this.#drainPromise = undefined;
      this.#ensureDrain();
    }
  }

  async #drain(): Promise<void> {
    while (!this.#disposed && this.#jobs.size > 0) {
      const deliveryClass = weightedClassRound[this.#classCursor]!;
      this.#classCursor = (this.#classCursor + 1) % weightedClassRound.length;
      await this.#visitClass(deliveryClass);
    }
  }

  async #visitClass(deliveryClass: RelayDeliveryClass): Promise<void> {
    const classState = this.#classes.get(deliveryClass)!;
    const key = classState.flowOrder[0];
    if (key === undefined) return;
    const flow = classState.flows.get(key);
    if (flow === undefined) throw new Error("delivery class references a missing flow");

    flow.deficit = Math.min(
      RELAY_DELIVERY_MAX_DEFICIT_BYTES,
      flow.deficit + RELAY_DELIVERY_BASE_QUANTUM_BYTES,
    );
    let attempted = 0;
    while (
      !this.#disposed &&
      attempted < RELAY_DELIVERY_MAX_RECORDS_PER_VISIT &&
      flow.jobs.length > 0
    ) {
      const job = flow.jobs[0]!;
      if (job.encodedBytes > flow.deficit) break;
      job.state = "yielding";
      try {
        await this.#yieldDataTurn(0);
      } catch {
        if (job.state === "yielding") {
          flow.deficit -= job.encodedBytes;
          attempted += 1;
          await this.#complete(job, "fatal");
        }
        continue;
      }
      if (job.state !== "yielding" || this.#disposed) continue;

      const payload = this.#ring.payload(job.ref);
      if (payload === undefined) {
        job.state = "done";
        this.#detach(job);
        continue;
      }

      job.state = "sending";
      let result: RelayDeliverySendResult;
      try {
        result = await this.#send({
          deliveryClass: job.deliveryClass,
          ref: job.ref,
          identity: job.identity,
          encodedBytes: job.encodedBytes,
          payload,
        });
      } catch {
        result = "fatal";
      }
      flow.deficit -= job.encodedBytes;
      attempted += 1;
      await this.#complete(job, result);
    }

    if (classState.flows.get(key) === flow && flow.jobs.length > 0) {
      const index = classState.flowOrder.indexOf(key);
      if (index < 0) throw new Error("delivery flow is missing from its class order");
      classState.flowOrder.splice(index, 1);
      classState.flowOrder.push(key);
    }
  }

  async #complete(job: PendingJob, result: RelayDeliverySendResult): Promise<void> {
    job.state = "done";
    this.#detach(job);
    if (result === "sent") this.#ring.confirm(job.ref);
    else this.#ring.cancel(job.ref);
    if (result === "fatal") {
      this.forgetGeneration(job.identity);
      try {
        await this.#onFailure(
          Object.freeze({
            deliveryClass: job.deliveryClass,
            ref: job.ref,
            identity: job.identity,
            encodedBytes: job.encodedBytes,
          }),
        );
      } catch {
        // Failure reporting must not strand unrelated committed delivery jobs.
      }
      // The callback may yield or re-enter the scheduler. Exact-generation work admitted while
      // it ran is stale by definition and must not survive the fatal fence.
      this.forgetGeneration(job.identity);
    }
  }

  #detach(job: PendingJob): void {
    const classState = this.#classes.get(job.deliveryClass)!;
    const flow = classState.flows.get(job.flowKey);
    if (flow !== undefined) {
      const jobIndex = flow.jobs.indexOf(job);
      if (jobIndex >= 0) flow.jobs.splice(jobIndex, 1);
      if (flow.jobs.length === 0) {
        classState.flows.delete(job.flowKey);
        const flowIndex = classState.flowOrder.indexOf(job.flowKey);
        if (flowIndex >= 0) classState.flowOrder.splice(flowIndex, 1);
      }
    }
    this.#jobs.delete(job.ref);
  }
}
