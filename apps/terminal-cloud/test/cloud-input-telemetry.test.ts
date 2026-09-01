import {
  CLOUD_INPUT_TELEMETRY_LIMITS,
  CloudInputTelemetry,
  type CloudInputSpan,
} from "../src/worker/cloud-input-telemetry";
import { afterEach, describe, expect, it, vi } from "vitest";

afterEach(() => vi.restoreAllMocks());

function span(index: number, disposition: CloudInputSpan["disposition"] = "host-sent") {
  return {
    clientId: "client_telemetry_0001",
    clientInputSeq: String(index),
    connectionId: "connection_telemetry_0001",
    disposition,
    hostSendAtMs: disposition === "host-sent" ? index + 3 : null,
    inputEpoch: "epoch_telemetry_0001",
    queueEnteredAtMs: index + 1,
    queueLeftAtMs: index + 2,
    receivedAtMs: index,
    writerFence: "1",
  } satisfies CloudInputSpan;
}

describe("CloudInputTelemetry", () => {
  it("retains bounded phase evidence without retaining input payloads or lease tokens", () => {
    const telemetry = new CloudInputTelemetry();
    for (let index = 0; index < CLOUD_INPUT_TELEMETRY_LIMITS.maxRecords + 8; index += 1) {
      telemetry.record(span(index, index % 2 === 0 ? "host-sent" : "rejected"));
    }

    const snapshot = telemetry.snapshot(CLOUD_INPUT_TELEMETRY_LIMITS.maxRecords + 10);
    expect(snapshot.totalCount).toBe(CLOUD_INPUT_TELEMETRY_LIMITS.maxRecords + 8);
    expect(snapshot.retainedCount).toBe(CLOUD_INPUT_TELEMETRY_LIMITS.maxRecords);
    expect(snapshot.maxQueueWaitMs).toBe(1);
    expect(snapshot.dispositionCounts["host-sent"]).toBe(128);
    expect(snapshot.dispositionCounts.rejected).toBe(128);
    expect(snapshot.records[0]?.clientInputSeq).toBe("8");
    expect(Object.keys(snapshot.records[0] ?? {})).not.toContain("writerLease");
    expect(Object.keys(snapshot.records[0] ?? {})).not.toContain("payload");
  });

  it("expires old evidence by age and rejects non-monotonic phase clocks", () => {
    const telemetry = new CloudInputTelemetry();
    telemetry.record(span(1));
    expect(telemetry.snapshot(CLOUD_INPUT_TELEMETRY_LIMITS.maxAgeMs + 4).retainedCount).toBe(0);

    expect(() =>
      telemetry.record({
        ...span(10),
        queueLeftAtMs: 9,
      }),
    ).toThrow("timestamps are not monotonic");
  });

  it("uses the same wall clock as recorded input spans for default age pruning", () => {
    const telemetry = new CloudInputTelemetry();
    telemetry.record(span(1));
    vi.spyOn(Date, "now").mockReturnValue(CLOUD_INPUT_TELEMETRY_LIMITS.maxAgeMs + 4);

    expect(telemetry.snapshot().retainedCount).toBe(0);
  });
});
