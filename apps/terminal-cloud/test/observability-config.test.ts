import wranglerConfigSource from "../wrangler.jsonc?raw";
import { describe, expect, it } from "vitest";

interface WranglerObservabilityConfig {
  vars?: { CLOUD_TELEMETRY_MODE?: string };
  observability?: {
    logs?: { invocation_logs?: boolean };
    traces?: { enabled?: boolean };
  };
}

function parseRepositoryConfig(source: string): WranglerObservabilityConfig {
  // The checked-in config uses JSONC trailing commas but no comments.
  return JSON.parse(source.replace(/,\s*([}\]])/gu, "$1")) as WranglerObservabilityConfig;
}

describe("production observability configuration", () => {
  it("does not retain request URLs through automatic logs or traces", () => {
    const config = parseRepositoryConfig(wranglerConfigSource);

    expect(config.observability?.logs?.invocation_logs).toBe(false);
    expect(config.observability?.traces?.enabled).toBe(false);
    expect(config.vars?.CLOUD_TELEMETRY_MODE).toBe("workers-logs-v1");
  });
});
