import { readFile } from "node:fs/promises";

export const DEFAULT_MANIFEST_URL = new URL("./manifest.json", import.meta.url);

const VALUE_TYPES = new Set(["string", "number", "boolean"]);
const QUERY_USES = new Set(["report", "arrival"]);
const QUERY_UNITS = new Set(["events", "frames", "milliseconds"]);
const CALCULATION_OPERATORS = new Set(["sum", "p95"]);
const DIRECT_KEY = /^[A-Za-z][A-Za-z0-9]*$/u;
const IDENTIFIER = /^[a-z][a-z0-9-]*$/u;
const PLATFORM_KEYS = new Set(["$metadata.service", "$metadata.type"]);
const REQUIRED_BASE_FILTERS = new Map([
  ["$metadata.service", { type: "string", value: "zhongduan-terminal-cloud" }],
  ["$metadata.type", { type: "string", value: "cf-worker-log" }],
  ["type", { type: "string", value: "zhongduan.telemetry" }],
  ["runtime", { type: "string", value: "cloud-do" }],
]);

export class ManifestValidationError extends Error {
  constructor(path, reason) {
    super(`invalid observability manifest at ${path}: ${reason}`);
    this.name = "ManifestValidationError";
  }
}

function fail(path, reason) {
  throw new ManifestValidationError(path, reason);
}

function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function record(value, path) {
  if (!isRecord(value)) fail(path, "expected an object");
  return value;
}

function array(value, path) {
  if (!Array.isArray(value)) fail(path, "expected an array");
  return value;
}

function string(value, path, maxLength) {
  if (typeof value !== "string" || value.length === 0) fail(path, "expected a non-empty string");
  if (maxLength !== undefined && value.length > maxLength) {
    fail(path, `must contain at most ${maxLength} characters`);
  }
  return value;
}

function exactKeys(value, path, expected) {
  const actual = Object.keys(value).sort((left, right) => left.localeCompare(right));
  const wanted = [...expected].sort((left, right) => left.localeCompare(right));
  if (actual.length !== wanted.length || actual.some((key, index) => key !== wanted[index])) {
    fail(path, `expected exactly: ${wanted.join(", ")}`);
  }
}

function unique(values, path) {
  if (new Set(values).size !== values.length) fail(path, "values must be unique");
}

function validateDatasets(value, path, expected) {
  const datasets = array(value, path).map((entry, index) => string(entry, `${path}[${index}]`));
  if (datasets.length === 0) fail(path, "must not be empty");
  unique(datasets, path);
  if (
    expected !== undefined &&
    (datasets.length !== expected.length ||
      datasets.some((entry, index) => entry !== expected[index]))
  ) {
    fail(path, "must exactly match the manifest datasets");
  }
  return datasets;
}

function validateKeyTypeMap(value, path, keyPolicy) {
  const keyTypes = record(value, path);
  const keys = Object.keys(keyTypes);
  if (keys.length === 0) fail(path, "must not be empty");
  for (const key of keys) {
    if (!keyPolicy(key)) fail(`${path}.${key}`, "key is outside this exact namespace");
    if (!VALUE_TYPES.has(keyTypes[key])) fail(`${path}.${key}`, "unsupported key type");
  }
  return keyTypes;
}

function validateFilter(value, path, keyTypes) {
  const filter = record(value, path);
  exactKeys(filter, path, ["kind", "key", "operation", "type", "value"]);
  if (filter.kind !== "filter") fail(`${path}.kind`, 'must be "filter"');
  if (filter.operation !== "eq") fail(`${path}.operation`, 'must be "eq"');
  const key = string(filter.key, `${path}.key`);
  const type = string(filter.type, `${path}.type`);
  if (keyTypes[key] !== type) fail(`${path}.type`, `does not match the declared type for ${key}`);
  if (typeof filter.value !== type) fail(`${path}.value`, `must be a ${type}`);
  if (type === "number" && !Number.isFinite(filter.value)) {
    fail(`${path}.value`, "must be finite");
  }
  return structuredClone(filter);
}

function validateBaseFilters(value, path, keyTypes) {
  const filters = array(value, path).map((filter, index) =>
    validateFilter(filter, `${path}[${index}]`, keyTypes),
  );
  if (filters.length !== REQUIRED_BASE_FILTERS.size) {
    fail(
      path,
      "must contain exactly the required service, log type, record type, and runtime filters",
    );
  }
  unique(
    filters.map((filter) => filter.key),
    path,
  );
  for (const filter of filters) {
    const expected = REQUIRED_BASE_FILTERS.get(filter.key);
    if (
      expected === undefined ||
      filter.type !== expected.type ||
      filter.value !== expected.value
    ) {
      fail(`${path}.${filter.key}`, "does not match the fixed production envelope selector");
    }
  }
  return filters;
}

function validateCalculation(value, path, keyTypes) {
  const calculation = record(value, path);
  exactKeys(calculation, path, ["operator", "alias", "key", "keyType"]);
  const operator = string(calculation.operator, `${path}.operator`);
  const alias = string(calculation.alias, `${path}.alias`);
  const key = string(calculation.key, `${path}.key`);
  const keyType = string(calculation.keyType, `${path}.keyType`);
  if (!CALCULATION_OPERATORS.has(operator)) fail(`${path}.operator`, "unsupported operator");
  if (!IDENTIFIER.test(alias.replaceAll("_", "-"))) {
    fail(`${path}.alias`, "must be a lowercase identifier");
  }
  if (keyTypes[key] !== keyType)
    fail(`${path}.keyType`, `does not match the declared type for ${key}`);
  if (operator === "sum" && (key !== "sampleWeight" || keyType !== "number")) {
    fail(path, "count queries must sum producer sampleWeight");
  }
  return structuredClone(calculation);
}

function validateGroupBy(value, path, keyTypes) {
  const groupBy = record(value, path);
  exactKeys(groupBy, path, ["type", "value"]);
  const key = string(groupBy.value, `${path}.value`);
  const type = string(groupBy.type, `${path}.type`);
  if (keyTypes[key] !== type) fail(`${path}.type`, `does not match the declared type for ${key}`);
  return structuredClone(groupBy);
}

function validateFixedWeightPercentile(query, path) {
  if (query.calculation.operator !== "p95") return;
  const weights = query.filters.filter((filter) => filter.key === "sampleWeight");
  if (
    weights.length !== 1 ||
    weights[0].type !== "number" ||
    !Number.isSafeInteger(weights[0].value) ||
    weights[0].value < 1
  ) {
    fail(path, "percentile queries must statically select exactly one positive sampleWeight");
  }
}

function validateQuery(value, path, datasets, baseFilters, keyTypes) {
  const query = record(value, path);
  exactKeys(query, path, [
    "id",
    "name",
    "description",
    "uses",
    "metricAlias",
    "unit",
    "filters",
    "calculation",
    "groupBys",
    "limit",
  ]);
  const id = string(query.id, `${path}.id`);
  if (!IDENTIFIER.test(id)) fail(`${path}.id`, "must be a lowercase kebab-case identifier");
  const name = string(query.name, `${path}.name`, 250);
  const description = string(query.description, `${path}.description`, 1000);
  const uses = array(query.uses, `${path}.uses`).map((use, index) =>
    string(use, `${path}.uses[${index}]`),
  );
  if (uses.length === 0) fail(`${path}.uses`, "must not be empty");
  unique(uses, `${path}.uses`);
  for (const use of uses) if (!QUERY_USES.has(use)) fail(`${path}.uses`, `unsupported use: ${use}`);
  const metricAlias = string(query.metricAlias, `${path}.metricAlias`);
  const unit = string(query.unit, `${path}.unit`);
  if (!QUERY_UNITS.has(unit)) fail(`${path}.unit`, "unsupported unit");
  const filters = array(query.filters, `${path}.filters`).map((filter, index) =>
    validateFilter(filter, `${path}.filters[${index}]`, keyTypes),
  );
  unique(
    filters.map((filter) => filter.key),
    `${path}.filters`,
  );
  if (filters.some((filter) => REQUIRED_BASE_FILTERS.has(filter.key))) {
    fail(`${path}.filters`, "must not replace or duplicate a fixed base filter");
  }
  const calculation = validateCalculation(query.calculation, `${path}.calculation`, keyTypes);
  if (calculation.alias !== metricAlias)
    fail(`${path}.metricAlias`, "must match calculation.alias");
  const groupBys = array(query.groupBys, `${path}.groupBys`).map((groupBy, index) =>
    validateGroupBy(groupBy, `${path}.groupBys[${index}]`, keyTypes),
  );
  unique(
    groupBys.map((groupBy) => groupBy.value),
    `${path}.groupBys`,
  );
  if (!Number.isSafeInteger(query.limit) || query.limit < 1 || query.limit > 100) {
    fail(`${path}.limit`, "must be an integer from 1 through 100");
  }
  validateFixedWeightPercentile({ filters, calculation }, path);
  return {
    id,
    name,
    description,
    uses,
    metricAlias,
    unit,
    parameters: {
      view: "calculations",
      datasets: [...datasets],
      filterCombination: "and",
      filters: [...structuredClone(baseFilters), ...filters],
      calculations: [calculation],
      groupBys,
      limit: query.limit,
    },
  };
}

function validateArrivalCheck(value, path, queriesById) {
  const check = record(value, path);
  exactKeys(check, path, ["id", "queryId", "metricAlias", "minimumValue", "requiresExact"]);
  const id = string(check.id, `${path}.id`);
  if (!IDENTIFIER.test(id)) fail(`${path}.id`, "must be a lowercase kebab-case identifier");
  const queryId = string(check.queryId, `${path}.queryId`);
  const query = queriesById.get(queryId);
  if (query === undefined) fail(`${path}.queryId`, "references an unknown query");
  if (!query.uses.includes("arrival"))
    fail(`${path}.queryId`, "query is not marked for arrival use");
  if (query.parameters.groupBys.length !== 0) fail(`${path}.queryId`, "must be an ungrouped query");
  if (check.metricAlias !== query.metricAlias) fail(`${path}.metricAlias`, "must match the query");
  if (
    typeof check.minimumValue !== "number" ||
    !Number.isFinite(check.minimumValue) ||
    check.minimumValue < 0
  ) {
    fail(`${path}.minimumValue`, "must be a non-negative finite number");
  }
  if (check.requiresExact !== true) fail(`${path}.requiresExact`, "must be true");
  return structuredClone(check);
}

export function validateManifest(value) {
  const manifest = record(value, "$manifest");
  exactKeys(manifest, "$manifest", [
    "manifestVersion",
    "apiContract",
    "datasets",
    "platformKeyTypes",
    "directKeyTypes",
    "baseFilters",
    "queries",
    "arrivalChecks",
  ]);
  if (manifest.manifestVersion !== 1) fail("$manifest.manifestVersion", "unsupported version");
  if (manifest.apiContract !== "cloudflare-workers-observability-v1") {
    fail("$manifest.apiContract", "unsupported API contract");
  }
  const datasets = validateDatasets(manifest.datasets, "$manifest.datasets");
  const platformKeyTypes = validateKeyTypeMap(
    manifest.platformKeyTypes,
    "$manifest.platformKeyTypes",
    (key) => PLATFORM_KEYS.has(key),
  );
  if (Object.keys(platformKeyTypes).length !== PLATFORM_KEYS.size) {
    fail("$manifest.platformKeyTypes", "must contain the exact supported platform keys");
  }
  const directKeyTypes = validateKeyTypeMap(
    manifest.directKeyTypes,
    "$manifest.directKeyTypes",
    (key) => DIRECT_KEY.test(key),
  );
  const overlap = Object.keys(platformKeyTypes).filter((key) => key in directKeyTypes);
  if (overlap.length !== 0) fail("$manifest", "platform and direct payload keys must be disjoint");
  const keyTypes = { ...platformKeyTypes, ...directKeyTypes };
  const baseFilters = validateBaseFilters(manifest.baseFilters, "$manifest.baseFilters", keyTypes);
  const rawQueries = array(manifest.queries, "$manifest.queries");
  if (rawQueries.length === 0) fail("$manifest.queries", "must not be empty");
  const queries = rawQueries.map((query, index) =>
    validateQuery(query, `$manifest.queries[${index}]`, datasets, baseFilters, keyTypes),
  );
  unique(
    queries.map((query) => query.id),
    "$manifest.queries.id",
  );
  unique(
    queries.map((query) => query.name),
    "$manifest.queries.name",
  );
  const queriesById = new Map(queries.map((query) => [query.id, query]));
  const rawChecks = array(manifest.arrivalChecks, "$manifest.arrivalChecks");
  if (rawChecks.length === 0) fail("$manifest.arrivalChecks", "must not be empty");
  const arrivalChecks = rawChecks.map((check, index) =>
    validateArrivalCheck(check, `$manifest.arrivalChecks[${index}]`, queriesById),
  );
  unique(
    arrivalChecks.map((check) => check.id),
    "$manifest.arrivalChecks.id",
  );
  return {
    manifestVersion: 1,
    apiContract: manifest.apiContract,
    datasets: [...datasets],
    platformKeyTypes: structuredClone(platformKeyTypes),
    directKeyTypes: structuredClone(directKeyTypes),
    queries,
    arrivalChecks,
  };
}

export async function loadManifest(url = DEFAULT_MANIFEST_URL) {
  let source;
  try {
    source = await readFile(url, "utf8");
  } catch {
    throw new ManifestValidationError("$manifest", "could not read the manifest file");
  }
  let parsed;
  try {
    parsed = JSON.parse(source);
  } catch {
    throw new ManifestValidationError("$manifest", "manifest is not valid JSON");
  }
  return validateManifest(parsed);
}

export function requiredKeyTypes(query) {
  const required = new Map();
  const add = (key, type) => {
    const previous = required.get(key);
    if (previous !== undefined && previous !== type) {
      throw new ManifestValidationError(`query.${query.id}`, `conflicting types for ${key}`);
    }
    required.set(key, type);
  };
  for (const filter of query.parameters.filters) add(filter.key, filter.type);
  for (const calculation of query.parameters.calculations)
    add(calculation.key, calculation.keyType);
  for (const groupBy of query.parameters.groupBys) add(groupBy.value, groupBy.type);
  return required;
}

export function savedQuerySpec(query) {
  return {
    name: query.name,
    description: query.description,
    parameters: structuredClone(query.parameters),
  };
}

export function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (isRecord(value)) {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}
