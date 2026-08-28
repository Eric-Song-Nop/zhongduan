import { canonicalJson, requiredKeyTypes } from "./manifest.mjs";

const DEFAULT_BASE_URL = "https://api.cloudflare.com/client/v4";
const VALUE_TYPES = new Set(["string", "number", "boolean"]);
const AGGREGATE_KEYS = new Set(["count", "interval", "sampleInterval", "value", "groups"]);

export class ObservabilityOperationError extends Error {
  constructor(message) {
    super(message);
    this.name = "ObservabilityOperationError";
  }
}

function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function assertRecord(value, label) {
  if (!isRecord(value)) throw new ObservabilityOperationError(`invalid ${label} response shape`);
  return value;
}

function assertArray(value, label) {
  if (!Array.isArray(value))
    throw new ObservabilityOperationError(`invalid ${label} response shape`);
  return value;
}

function assertFinite(value, label, { nonnegative = false } = {}) {
  if (typeof value !== "number" || !Number.isFinite(value) || (nonnegative && value < 0)) {
    throw new ObservabilityOperationError(`invalid ${label} response shape`);
  }
  return value;
}

function sanitizedStatus(status) {
  return Number.isInteger(status) && status >= 100 && status <= 599 ? status : "unknown";
}

function encodeAccountId(accountId) {
  return encodeURIComponent(accountId);
}

export function credentialsFromEnvironment(environment = process.env) {
  const token = environment.CLOUDFLARE_OBSERVABILITY_TOKEN;
  const accountId = environment.CLOUDFLARE_OBSERVABILITY_ACCOUNT_ID;
  if (typeof token !== "string" || token.length === 0) {
    throw new ObservabilityOperationError("missing CLOUDFLARE_OBSERVABILITY_TOKEN");
  }
  if (typeof accountId !== "string" || accountId.length === 0) {
    throw new ObservabilityOperationError("missing CLOUDFLARE_OBSERVABILITY_ACCOUNT_ID");
  }
  return { token, accountId };
}

export function validateTimeframe(value, label = "timeframe") {
  const timeframe = assertRecord(value, label);
  const from = assertFinite(timeframe.from, `${label}.from`, { nonnegative: true });
  const to = assertFinite(timeframe.to, `${label}.to`, { nonnegative: true });
  if (!Number.isSafeInteger(from) || !Number.isSafeInteger(to) || from >= to) {
    throw new ObservabilityOperationError(`invalid ${label}`);
  }
  return { from, to };
}

export function extractDiscoveredKeyTypes(result) {
  const rows = assertArray(result, "telemetry keys");
  const discovered = new Map();
  for (const rowValue of rows) {
    const row = assertRecord(rowValue, "telemetry keys");
    const keys = Object.keys(row).sort();
    if (keys.join(",") !== "key,lastSeenAt,type") {
      throw new ObservabilityOperationError("invalid telemetry keys response shape");
    }
    if (
      typeof row.key !== "string" ||
      !VALUE_TYPES.has(row.type) ||
      typeof row.lastSeenAt !== "number" ||
      !Number.isFinite(row.lastSeenAt)
    ) {
      throw new ObservabilityOperationError("invalid telemetry keys response shape");
    }
    const previous = discovered.get(row.key);
    if (previous !== undefined && previous !== row.type) {
      throw new ObservabilityOperationError("telemetry keys response contains conflicting types");
    }
    discovered.set(row.key, row.type);
  }
  return discovered;
}

export function validateExactKeyTypes(query, discovered) {
  const required = requiredKeyTypes(query);
  for (const [key, type] of required) {
    if (discovered.get(key) !== type) {
      throw new ObservabilityOperationError(
        `required direct telemetry key unavailable or type-mismatched: ${key} (${type})`,
      );
    }
  }
}

export function validateManifestKeyTypes(manifest, discovered) {
  const expected = new Map([
    ...Object.entries(manifest.platformKeyTypes),
    ...Object.entries(manifest.directKeyTypes),
  ]);
  for (const [key, type] of expected) {
    if (discovered.get(key) !== type) {
      throw new ObservabilityOperationError(
        `required manifest telemetry key unavailable or type-mismatched: ${key} (${type})`,
      );
    }
  }
}

function validateGroups(value, query) {
  const expected = new Map(
    query.parameters.groupBys.map((groupBy) => [groupBy.value, groupBy.type]),
  );
  if (value === undefined) {
    if (expected.size !== 0) throw new ObservabilityOperationError("missing aggregate groups");
    return [];
  }
  const groups = assertArray(value, "aggregate groups");
  if (groups.length !== expected.size) {
    throw new ObservabilityOperationError("aggregate groups do not match the query manifest");
  }
  const safeGroups = [];
  const seen = new Set();
  for (const groupValue of groups) {
    const group = assertRecord(groupValue, "aggregate group");
    if (Object.keys(group).sort().join(",") !== "key,value") {
      throw new ObservabilityOperationError("invalid aggregate group response shape");
    }
    if (typeof group.key !== "string" || seen.has(group.key)) {
      throw new ObservabilityOperationError("invalid aggregate group response shape");
    }
    const expectedType = expected.get(group.key);
    if (expectedType === undefined || typeof group.value !== expectedType) {
      throw new ObservabilityOperationError("aggregate groups do not match the query manifest");
    }
    seen.add(group.key);
    safeGroups.push({ key: group.key, value: group.value });
  }
  return safeGroups;
}

function parseAggregate(value, query) {
  const aggregate = assertRecord(value, "aggregate");
  for (const key of Object.keys(aggregate)) {
    if (!AGGREGATE_KEYS.has(key)) {
      throw new ObservabilityOperationError("invalid aggregate response shape");
    }
  }
  for (const required of ["count", "interval", "sampleInterval", "value"]) {
    if (!(required in aggregate)) {
      throw new ObservabilityOperationError("invalid aggregate response shape");
    }
  }
  const count = assertFinite(aggregate.count, "aggregate.count", { nonnegative: true });
  const interval = assertFinite(aggregate.interval, "aggregate.interval", { nonnegative: true });
  const sampleInterval = assertFinite(aggregate.sampleInterval, "aggregate.sampleInterval", {
    nonnegative: true,
  });
  const metricValue = assertFinite(aggregate.value, "aggregate.value");
  return {
    value: metricValue,
    count,
    interval,
    sampleInterval,
    groups: validateGroups(aggregate.groups, query),
  };
}

export function extractAggregateResult(resultValue, query) {
  const result = assertRecord(resultValue, "telemetry query");
  const statistics = assertRecord(result.statistics, "telemetry query statistics");
  let abrLevel = 1;
  if (statistics.abr_level !== undefined) {
    abrLevel = assertFinite(statistics.abr_level, "telemetry query statistics.abr_level", {
      nonnegative: true,
    });
  }
  const calculations = assertArray(result.calculations, "telemetry query calculations");
  const matches = calculations.filter(
    (calculation) => isRecord(calculation) && calculation.alias === query.metricAlias,
  );
  if (matches.length !== 1) {
    throw new ObservabilityOperationError(
      "telemetry query did not return exactly one requested metric",
    );
  }
  const calculation = matches[0];
  if (typeof calculation.calculation !== "string") {
    throw new ObservabilityOperationError("invalid telemetry query calculation response shape");
  }
  const aggregates = assertArray(calculation.aggregates, "telemetry query aggregates").map(
    (aggregate) => parseAggregate(aggregate, query),
  );
  if (query.parameters.groupBys.length === 0 && aggregates.length > 1) {
    throw new ObservabilityOperationError("ungrouped telemetry query returned multiple aggregates");
  }
  const approximationReasons = [];
  if (abrLevel !== 1) approximationReasons.push("abr-level");
  if (aggregates.some((aggregate) => aggregate.sampleInterval !== 1)) {
    approximationReasons.push("sample-interval");
  }
  return {
    queryId: query.id,
    metricAlias: query.metricAlias,
    unit: query.unit,
    approximate: approximationReasons.length !== 0,
    approximationReasons,
    abrLevel,
    // The public contract does not define a client-side recomputation. Preserve value verbatim,
    // and use sampleInterval/abr_level only to prevent approximate results from becoming gates.
    aggregates,
  };
}

function validateSavedQueryRow(value) {
  const row = assertRecord(value, "saved query");
  if (
    typeof row.id !== "string" ||
    typeof row.name !== "string" ||
    (row.description !== null && typeof row.description !== "string") ||
    !isRecord(row.parameters)
  ) {
    throw new ObservabilityOperationError("invalid saved query response shape");
  }
  return {
    id: row.id,
    name: row.name,
    description: row.description,
    parameters: row.parameters,
  };
}

export function createCloudflareObservabilityClient(options) {
  const { accountId, token } = options;
  const fetchImpl = options.fetchImpl ?? globalThis.fetch;
  const baseUrl = options.baseUrl ?? DEFAULT_BASE_URL;
  if (typeof fetchImpl !== "function") {
    throw new ObservabilityOperationError("fetch is unavailable");
  }
  if (
    typeof token !== "string" ||
    token.length === 0 ||
    typeof accountId !== "string" ||
    accountId.length === 0
  ) {
    throw new ObservabilityOperationError("invalid observability credentials");
  }
  const accountPath = `/accounts/${encodeAccountId(accountId)}/workers/observability`;

  async function request(method, path, body) {
    let response;
    try {
      response = await fetchImpl(`${baseUrl}${accountPath}${path}`, {
        method,
        headers: {
          authorization: `Bearer ${token}`,
          ...(body === undefined ? {} : { "content-type": "application/json" }),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      });
    } catch {
      throw new ObservabilityOperationError("Cloudflare Observability API request failed");
    }
    if (!response.ok) {
      throw new ObservabilityOperationError(
        `Cloudflare Observability API request failed (status ${sanitizedStatus(response.status)})`,
      );
    }
    let envelope;
    try {
      envelope = await response.json();
    } catch {
      throw new ObservabilityOperationError("Cloudflare Observability API returned invalid JSON");
    }
    if (!isRecord(envelope) || envelope.success !== true || !("result" in envelope)) {
      throw new ObservabilityOperationError("Cloudflare Observability API rejected the request");
    }
    return envelope.result;
  }

  async function discoverKeys(datasets, timeframe) {
    const window = validateTimeframe(timeframe);
    const result = await request("POST", "/telemetry/keys", {
      datasets: [...datasets],
      from: window.from,
      to: window.to,
      limit: 1000,
    });
    return extractDiscoveredKeyTypes(result);
  }

  async function runVerifiedQuery(query, timeframe, options = {}) {
    const window = validateTimeframe(timeframe);
    const discovered =
      options.discoveredKeys ?? (await discoverKeys(query.parameters.datasets, window));
    validateExactKeyTypes(query, discovered);
    const result = await request("POST", "/telemetry/query", {
      queryId: query.id,
      timeframe: window,
      chart: false,
      chartType: "aggregate",
      dry: true,
      ignoreSeries: true,
      parameters: structuredClone(query.parameters),
    });
    return extractAggregateResult(result, query);
  }

  async function listSavedQueries() {
    const saved = [];
    const pageSize = 50;
    for (let page = 1; page <= 200; page += 1) {
      const result = await request("GET", `/queries?page=${page}&perPage=${pageSize}`);
      const rows = assertArray(result, "saved query list");
      saved.push(...rows.map(validateSavedQueryRow));
      if (rows.length < pageSize) return saved;
    }
    throw new ObservabilityOperationError("saved query list exceeded the bounded pagination limit");
  }

  async function createSavedQuery(specification) {
    const result = await request("POST", "/queries", specification);
    return validateSavedQueryRow(result);
  }

  return { discoverKeys, runVerifiedQuery, listSavedQueries, createSavedQuery };
}

export async function provisionSavedQueries(client, manifest, { apply = false } = {}) {
  if (apply !== true) {
    throw new ObservabilityOperationError("provision requires explicit --apply");
  }
  const existing = await client.listSavedQueries();
  const plan = [];
  for (const query of manifest.queries) {
    const matches = existing.filter((saved) => saved.name === query.name);
    if (matches.length > 1) {
      throw new ObservabilityOperationError(`saved query name is ambiguous: ${query.name}`);
    }
    const desired = {
      name: query.name,
      description: query.description,
      parameters: structuredClone(query.parameters),
    };
    if (matches.length === 1) {
      const current = matches[0];
      const comparable = {
        name: current.name,
        description: current.description,
        parameters: current.parameters,
      };
      if (canonicalJson(comparable) !== canonicalJson(desired)) {
        throw new ObservabilityOperationError(`saved query drift detected: ${query.name}`);
      }
      plan.push({ query, desired, action: "no-op" });
      continue;
    }
    plan.push({ query, desired, action: "created" });
  }
  const results = [];
  for (const item of plan) {
    if (item.action === "created") await client.createSavedQuery(item.desired);
    results.push({ queryId: item.query.id, action: item.action });
  }
  return results;
}
