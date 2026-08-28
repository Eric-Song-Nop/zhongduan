import { TerminalTelemetryEventSchema, type TelemetrySink } from "@zhongduan/telemetry";

export const HOST_TELEMETRY_ENV = "ZHONGDUAN_TELEMETRY";

export interface TelemetryLineWriter {
  once(event: "drain" | "error", listener: () => void): unknown;
  write(line: string): boolean;
}

export function createNdjsonTelemetrySink(writer: TelemetryLineWriter): TelemetrySink {
  let failed = false;
  let writable = true;
  try {
    writer.once("error", () => {
      failed = true;
      writable = false;
    });
  } catch {
    failed = true;
    writable = false;
  }
  return (event) => {
    if (!writable) return;
    let line: string;
    try {
      const parsed = TerminalTelemetryEventSchema.parse(event);
      line = `${JSON.stringify({ type: "zhongduan.telemetry", ...parsed })}\n`;
    } catch {
      return;
    }
    try {
      if (!writer.write(line)) {
        writable = false;
        writer.once("drain", () => {
          if (!failed) writable = true;
        });
      }
    } catch {
      failed = true;
      writable = false;
      // Diagnostics must never alter terminal authority, recovery, or process lifetime.
    }
  };
}

export function telemetrySinkForTarget(
  target: string | undefined,
  writer: TelemetryLineWriter,
): TelemetrySink | undefined {
  if (target === undefined || target.length === 0) return undefined;
  if (target === "stderr") return createNdjsonTelemetrySink(writer);
  throw new Error(`${HOST_TELEMETRY_ENV} must be unset or "stderr"`);
}
