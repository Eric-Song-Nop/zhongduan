import { readFile } from "node:fs/promises";

import {
  ObservabilityOperationError,
  validateManifestKeyTypes,
  validateTimeframe,
} from "./cloudflare-observability.mjs";

export const DEFAULT_FIXTURE_URL = new URL("./fixtures/synthetic-query-api.json", import.meta.url);

function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function exactKeys(value, expected) {
  const actual = Object.keys(value).sort((left, right) => left.localeCompare(right));
  const wanted = [...expected].sort((left, right) => left.localeCompare(right));
  return actual.length === wanted.length && actual.every((key, index) => key === wanted[index]);
}

export function validateFixture(value) {
  if (
    !isRecord(value) ||
    !exactKeys(value, [
      "fixtureVersion",
      "status",
      "sanitized",
      "requiresStagingReplacement",
      "keysResult",
      "queryResult",
    ])
  ) {
    throw new ObservabilityOperationError("invalid observability fixture shape");
  }
  if (value.fixtureVersion !== 1 || value.sanitized !== true) {
    throw new ObservabilityOperationError("invalid observability fixture metadata");
  }
  const synthetic = value.status === "synthetic-sanitized";
  const staging = value.status === "staging-captured-sanitized";
  if (
    (!synthetic && !staging) ||
    (synthetic && value.requiresStagingReplacement !== true) ||
    (staging && value.requiresStagingReplacement !== false)
  ) {
    throw new ObservabilityOperationError("invalid observability fixture provenance");
  }
  if (!Array.isArray(value.keysResult) || !isRecord(value.queryResult)) {
    throw new ObservabilityOperationError("invalid observability fixture API shape");
  }
  return structuredClone(value);
}

export async function loadFixture(url = DEFAULT_FIXTURE_URL) {
  let source;
  try {
    source = await readFile(url, "utf8");
  } catch {
    throw new ObservabilityOperationError("could not read the observability fixture");
  }
  let parsed;
  try {
    parsed = JSON.parse(source);
  } catch {
    throw new ObservabilityOperationError("observability fixture is not valid JSON");
  }
  return validateFixture(parsed);
}

export function fixtureStatus(fixture) {
  return {
    status: fixture.status,
    sanitized: fixture.sanitized,
    requiresStagingReplacement: fixture.requiresStagingReplacement,
  };
}

export async function runReport(client, manifest, timeframe) {
  const window = validateTimeframe(timeframe);
  const discoveredKeys = await client.discoverKeys(manifest.datasets, window);
  validateManifestKeyTypes(manifest, discoveredKeys);
  const queries = [];
  for (const query of manifest.queries.filter((candidate) => candidate.uses.includes("report"))) {
    queries.push(await client.runVerifiedQuery(query, window, { discoveredKeys }));
  }
  return {
    kind: "cloud-observability-report",
    timeframe: window,
    approximate: queries.some((query) => query.approximate),
    queries,
  };
}

export async function runArrivalSmoke(client, manifest, timeframe) {
  const window = validateTimeframe(timeframe);
  const discoveredKeys = await client.discoverKeys(manifest.datasets, window);
  validateManifestKeyTypes(manifest, discoveredKeys);
  const queriesById = new Map(manifest.queries.map((query) => [query.id, query]));
  const checks = [];
  for (const check of manifest.arrivalChecks) {
    const query = queriesById.get(check.queryId);
    if (query === undefined) {
      throw new ObservabilityOperationError("arrival check references an unknown query");
    }
    const result = await client.runVerifiedQuery(query, window, { discoveredKeys });
    if (result.approximate) {
      checks.push({
        id: check.id,
        status: "approximate",
        hardGateApplied: false,
        value: result.aggregates.length === 1 ? result.aggregates[0].value : null,
        minimumValue: check.minimumValue,
        approximationReasons: result.approximationReasons,
      });
      continue;
    }
    const aggregate = result.aggregates.length === 1 ? result.aggregates[0] : undefined;
    const passed = aggregate !== undefined && aggregate.value >= check.minimumValue;
    checks.push({
      id: check.id,
      status: passed ? "passed" : "failed",
      hardGateApplied: true,
      value: aggregate?.value ?? null,
      minimumValue: check.minimumValue,
      approximationReasons: [],
    });
  }
  return {
    kind: "cloud-query-smoke",
    performanceGate: false,
    timeframe: window,
    approximate: checks.some((check) => check.status === "approximate"),
    blocked: checks.some((check) => check.status === "approximate"),
    failed: checks.some((check) => check.status === "failed"),
    checks,
  };
}
