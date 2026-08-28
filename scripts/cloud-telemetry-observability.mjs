#!/usr/bin/env node

import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

import {
  ObservabilityOperationError,
  createCloudflareObservabilityClient,
  credentialsFromEnvironment,
  extractAggregateResult,
  extractDiscoveredKeyTypes,
  provisionSavedQueries,
  validateManifestKeyTypes,
} from "../apps/terminal-cloud/observability/cloudflare-observability.mjs";
import {
  DEFAULT_MANIFEST_URL,
  ManifestValidationError,
  loadManifest,
} from "../apps/terminal-cloud/observability/manifest.mjs";
import {
  DEFAULT_FIXTURE_URL,
  fixtureStatus,
  loadFixture,
  runArrivalSmoke,
  runReport,
} from "../apps/terminal-cloud/observability/operations.mjs";

const COMMANDS = new Set(["validate", "fixture-gate", "report", "arrival-smoke", "provision"]);

function parseArguments(argv) {
  const [command, ...rest] = argv;
  if (!COMMANDS.has(command)) {
    throw new ObservabilityOperationError(
      "usage: cloud-telemetry-observability.mjs <validate|fixture-gate|report|arrival-smoke|provision>",
    );
  }
  const options = { apply: false };
  for (let index = 0; index < rest.length; index += 1) {
    const option = rest[index];
    if (option === "--apply") {
      if (options.apply) throw new ObservabilityOperationError("duplicate option: --apply");
      options.apply = true;
      continue;
    }
    if (!["--from", "--to", "--manifest", "--fixture"].includes(option)) {
      throw new ObservabilityOperationError(`unsupported option: ${option}`);
    }
    const value = rest[index + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new ObservabilityOperationError(`missing value for option: ${option}`);
    }
    const key = option.slice(2);
    if (options[key] !== undefined) {
      throw new ObservabilityOperationError(`duplicate option: ${option}`);
    }
    options[key] = value;
    index += 1;
  }

  const allowed = {
    validate: new Set(["apply", "manifest", "fixture"]),
    "fixture-gate": new Set(["apply", "fixture"]),
    report: new Set(["apply", "manifest", "from", "to"]),
    "arrival-smoke": new Set(["apply", "manifest", "from", "to"]),
    provision: new Set(["apply", "manifest"]),
  }[command];
  for (const key of Object.keys(options)) {
    if (!allowed.has(key) || (key === "apply" && options.apply && command !== "provision")) {
      throw new ObservabilityOperationError(`option is not valid for ${command}: --${key}`);
    }
  }
  return { command, options };
}

function optionUrl(value, fallback) {
  return value === undefined ? fallback : pathToFileURL(resolve(value));
}

function parseInstant(value, option) {
  if (typeof value !== "string") throw new ObservabilityOperationError(`missing option: ${option}`);
  const timestamp = /^\d+$/u.test(value) ? Number(value) : Date.parse(value);
  if (!Number.isSafeInteger(timestamp) || timestamp < 0) {
    throw new ObservabilityOperationError(`invalid timestamp for ${option}`);
  }
  return timestamp;
}

function timeframeFromOptions(options) {
  return {
    from: parseInstant(options.from, "--from"),
    to: parseInstant(options.to, "--to"),
  };
}

function writeJson(stream, value) {
  stream.write(`${JSON.stringify(value, null, 2)}\n`);
}

export async function main(argv = process.argv.slice(2), dependencies = {}) {
  const stdout = dependencies.stdout ?? process.stdout;
  const stderr = dependencies.stderr ?? process.stderr;
  const environment = dependencies.environment ?? process.env;
  try {
    const { command, options } = parseArguments(argv);
    if (command === "validate") {
      const [manifest, fixture] = await Promise.all([
        loadManifest(optionUrl(options.manifest, DEFAULT_MANIFEST_URL)),
        loadFixture(optionUrl(options.fixture, DEFAULT_FIXTURE_URL)),
      ]);
      const discoveredKeys = extractDiscoveredKeyTypes(fixture.keysResult);
      validateManifestKeyTypes(manifest, discoveredKeys);
      const arrivalQuery = manifest.queries.find((query) => query.uses.includes("arrival"));
      if (arrivalQuery === undefined) {
        throw new ObservabilityOperationError("manifest does not define an arrival query");
      }
      extractAggregateResult(fixture.queryResult, arrivalQuery);
      writeJson(stdout, {
        status: "valid",
        manifestVersion: manifest.manifestVersion,
        queryCount: manifest.queries.length,
        arrivalCheckCount: manifest.arrivalChecks.length,
        fixture: fixtureStatus(fixture),
      });
      return 0;
    }
    if (command === "fixture-gate") {
      const fixture = await loadFixture(optionUrl(options.fixture, DEFAULT_FIXTURE_URL));
      const status = fixtureStatus(fixture);
      writeJson(stdout, status);
      return status.requiresStagingReplacement ? 2 : 0;
    }
    const manifest = await loadManifest(optionUrl(options.manifest, DEFAULT_MANIFEST_URL));
    if (command === "provision" && options.apply !== true) {
      throw new ObservabilityOperationError("provision requires explicit --apply");
    }
    const credentials = credentialsFromEnvironment(environment);
    const client = createCloudflareObservabilityClient({
      ...credentials,
      ...(dependencies.fetchImpl === undefined ? {} : { fetchImpl: dependencies.fetchImpl }),
      ...(dependencies.baseUrl === undefined ? {} : { baseUrl: dependencies.baseUrl }),
    });
    if (command === "report") {
      writeJson(stdout, await runReport(client, manifest, timeframeFromOptions(options)));
      return 0;
    }
    if (command === "arrival-smoke") {
      const smoke = await runArrivalSmoke(client, manifest, timeframeFromOptions(options));
      writeJson(stdout, smoke);
      return smoke.failed || smoke.blocked ? 2 : 0;
    }
    const results = await provisionSavedQueries(client, manifest, { apply: options.apply });
    writeJson(stdout, { status: "applied", results });
    return 0;
  } catch (error) {
    const safeMessage =
      error instanceof ObservabilityOperationError || error instanceof ManifestValidationError
        ? error.message
        : "observability command failed";
    stderr.write(`error: ${safeMessage}\n`);
    return 1;
  }
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.exitCode = await main();
}
