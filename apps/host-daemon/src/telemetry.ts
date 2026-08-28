import { TerminalTelemetryEventSchema, type TelemetrySink } from "@zhongduan/telemetry";

export const HOST_TELEMETRY_ENV = "ZHONGDUAN_TELEMETRY";

export function createNdjsonTelemetrySink(writeLine: (line: string) => void): TelemetrySink {
  return (event) => {
    try {
      const parsed = TerminalTelemetryEventSchema.parse(event);
      writeLine(`${JSON.stringify({ type: "zhongduan.telemetry", ...parsed })}\n`);
    } catch {
      // Diagnostics must never alter terminal authority, recovery, or process lifetime.
    }
  };
}

export function telemetrySinkForTarget(
  target: string | undefined,
  writeLine: (line: string) => void,
): TelemetrySink | undefined {
  if (target === undefined || target.length === 0) return undefined;
  if (target === "stderr") return createNdjsonTelemetrySink(writeLine);
  throw new Error(`${HOST_TELEMETRY_ENV} must be unset or "stderr"`);
}
