import {
  CloudTelemetryEventSchema,
  createBufferedTelemetrySink,
  type CloudTelemetryEvent,
} from "@zhongduan/telemetry";

export const CLOUD_TELEMETRY_RECORD_TYPE = "zhongduan.telemetry";
export const CLOUD_TELEMETRY_RUNTIME = "cloud-do";

export type CloudTelemetryLogRecord = CloudTelemetryEvent & {
  type: typeof CLOUD_TELEMETRY_RECORD_TYPE;
  runtime: typeof CLOUD_TELEMETRY_RUNTIME;
};

export type CloudTelemetryCollector = (record: CloudTelemetryLogRecord) => unknown;

export interface CloudTelemetryOptions {
  collector?: CloudTelemetryCollector;
  maxPendingEvents?: number;
  schedule?: (task: () => void) => void;
}

export interface CloudTelemetry {
  readonly droppedEvents: number;
  readonly pendingEvents: number;
  record(event: CloudTelemetryEvent): void;
}

const DEFAULT_MAX_PENDING_EVENTS = 64;

export function createCloudTelemetry(options: CloudTelemetryOptions = {}): CloudTelemetry {
  const collector: CloudTelemetryCollector =
    options.collector ?? ((record) => console.info(record));
  const buffered = createBufferedTelemetrySink(
    (event) => {
      const cloudEvent = CloudTelemetryEventSchema.parse(event);
      const record: CloudTelemetryLogRecord = {
        type: CLOUD_TELEMETRY_RECORD_TYPE,
        runtime: CLOUD_TELEMETRY_RUNTIME,
        ...cloudEvent,
      };
      return collector(record);
    },
    {
      maxPendingEvents: options.maxPendingEvents ?? DEFAULT_MAX_PENDING_EVENTS,
      ...(options.schedule === undefined ? {} : { schedule: options.schedule }),
    },
  );

  return {
    get droppedEvents() {
      return buffered.droppedEvents;
    },
    get pendingEvents() {
      return buffered.pendingEvents;
    },
    record(event) {
      try {
        buffered.sink(event);
      } catch {
        // Cloud diagnostics must never alter relay authority, recovery, or socket lifetime.
      }
    },
  };
}
