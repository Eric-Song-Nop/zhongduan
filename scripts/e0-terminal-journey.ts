#!/usr/bin/env node

/** Executable E0 contract, oracle, bundle, and latency-report helpers. */

import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { dirname, extname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { gzipSync, gunzipSync } from "node:zlib";
import { parse as parseLosslessJson, stringify as stringifyLosslessJson } from "lossless-json";

export type Data = Record<string, unknown>;

export const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
export const CONTRACT_PATH = join(ROOT, "benchmarks", "terminal-journey", "contract.json");
export const BASELINE_PATH = join(ROOT, "benchmarks", "terminal-journey", "current-baseline.json");

export const EXPECTED_ORACLES = new Set([
  "ui-consumed-silent-loss",
  "duplicate-pty-effect",
  "uncertain-auto-retry",
  "output-flood-ctrl-c-once",
  "old-writer-effect-after-transfer",
  "cold-candidate-visible-before-validation",
  "snapshot-authority-divergence",
  "secure-input-speculative-presentation",
]);
export const EXPECTED_SPANS = new Set([
  "browser-keydown-to-send-decision",
  "cloud-browser-receive-to-host-send",
  "host-receive-to-pty-write",
  "input-to-matching-browser-render",
  "pty-output-to-browser-useful-render",
  "ctrl-c-to-pty-write",
  "ctrl-c-to-application-quiet",
]);
export const EXPECTED_THRESHOLDS = new Set([
  "cloudBulkIsolation",
  "snapshotHostInput",
  "outputFloodCtrlC",
]);
export const EXPECTED_VARIANTS = new Set([
  "steady",
  "output-flood",
  "bulk-backlog-0",
  "bulk-backlog-262144",
  "bulk-backlog-1048576",
  "bulk-backlog-4194304",
  "snapshot-disabled",
  "snapshot-enabled",
  "correctness-faults",
]);
const TERMINAL_OUTCOMES = new Set(["not-sent", "deterministic", "uncertain"]);
export const MIN_MEASURED_SAMPLES = 24;
export const REPORT_BUNDLE_SCHEMA = "zhongduan-terminal-journey-bundle-v1";
const REPORT_BUNDLE_PAYLOAD_FIELDS = new Set([
  "events",
  "latencySamples",
  "observations",
  "scenarioReports",
]);
export const MAX_SCENARIO_ARCHIVE_COMPRESSED_BYTES = 16 * 1024 * 1024;
export const MAX_SCENARIO_ARCHIVE_UNCOMPRESSED_BYTES = 64 * 1024 * 1024;

export class ContractError extends Error {
  override name = "ContractError";
}

export function isData(value: unknown): value is Data {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function data(value: unknown, label: string): Data {
  if (!isData(value)) throw new ContractError(`${label} must be an object`);
  return value;
}

function values(value: unknown, label: string): unknown[] {
  if (!Array.isArray(value)) throw new ContractError(`${label} must be an array`);
  return value;
}

function records(value: unknown, label: string): Data[] {
  return values(value, label).map((item) => data(item, label));
}

function strings(value: unknown, label: string): string[] {
  const result = values(value, label);
  if (result.some((item) => typeof item !== "string")) {
    throw new ContractError(`${label} must contain strings`);
  }
  return result as string[];
}

function numberValue(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function integerValue(value: unknown): number | null {
  return typeof value === "number" && Number.isSafeInteger(value) ? value : null;
}

function stringValue(value: unknown, fallback: string): string {
  return typeof value === "string" ? value : fallback;
}

function nanoseconds(value: unknown): bigint | null {
  if (typeof value === "bigint") return value;
  if (typeof value === "number" && Number.isSafeInteger(value)) return BigInt(value);
  return null;
}

function sortedJsonValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortedJsonValue);
  if (isData(value)) {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, sortedJsonValue(value[key])]),
    );
  }
  return value;
}

export function parseJson(text: string): unknown {
  return parseLosslessJson(text, null, {
    parseNumber: (raw) => {
      const value = Number(raw);
      if (/^-?\d+$/.test(raw) && !Number.isSafeInteger(value)) return BigInt(raw);
      return value;
    },
  });
}

export function stringifyJson(value: unknown, pretty = false): string {
  const encoded = stringifyLosslessJson(sortedJsonValue(value), null, pretty ? 2 : undefined);
  if (encoded === undefined) throw new ContractError("cannot serialize an undefined JSON value");
  return encoded;
}

export function loadJson(path: string): Data {
  try {
    return data(parseJson(readFileSync(path, "utf8")), `JSON artifact ${path}`);
  } catch (error: unknown) {
    if (error instanceof ContractError) throw error;
    throw new ContractError(
      `cannot read JSON contract artifact ${path}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

export function canonicalSha256(value: unknown): string {
  return createHash("sha256").update(stringifyJson(value)).digest("hex");
}

function sha256Bytes(value: Uint8Array): string {
  return createHash("sha256").update(value).digest("hex");
}

function scenarioJsonl(scenarios: Data[]): Buffer {
  return Buffer.from(
    `${scenarios.map((scenario) => stringifyJson(scenario)).join("\n")}\n`,
    "utf8",
  );
}

export function reportArchivePath(reportPath: string): string {
  const extension = extname(reportPath);
  return `${reportPath.slice(0, extension.length === 0 ? undefined : -extension.length)}.scenarios.jsonl.gz`;
}

export function buildReportBundle(
  report: Data,
  reportPath: string,
): { bundle: Data; compressed: Buffer } {
  const scenarios = records(report["scenarioReports"], "report scenarioReports");
  const latencySamples = values(report["latencySamples"], "report latencySamples");
  const raw = scenarioJsonl(scenarios);
  // Node emits a zero gzip timestamp, so identical canonical JSONL stays byte-for-byte stable.
  const compressed = gzipSync(raw, { level: 9 });
  if (compressed.length > MAX_SCENARIO_ARCHIVE_COMPRESSED_BYTES) {
    throw new ContractError("scenario archive exceeds its compressed size limit");
  }
  if (raw.length > MAX_SCENARIO_ARCHIVE_UNCOMPRESSED_BYTES) {
    throw new ContractError("scenario archive exceeds its uncompressed size limit");
  }
  const bundle = Object.fromEntries(
    Object.entries(report).filter(([key]) => !REPORT_BUNDLE_PAYLOAD_FIELDS.has(key)),
  );
  bundle["schemaVersion"] = REPORT_BUNDLE_SCHEMA;
  bundle["reportSchemaVersion"] = report["schemaVersion"];
  bundle["reconstructedReportSha256"] = canonicalSha256(report);
  bundle["latencySampleCount"] = latencySamples.length;
  bundle["scenarioArchive"] = {
    path: reportArchivePath(reportPath).split("/").at(-1),
    format: "canonical-jsonl",
    compression: "gzip-9-mtime-0",
    scenarioCount: scenarios.length,
    compressedBytes: compressed.length,
    compressedSha256: sha256Bytes(compressed),
    uncompressedBytes: raw.length,
    uncompressedSha256: sha256Bytes(raw),
  };
  return { bundle, compressed };
}

export function writeReportBundle(reportPath: string, report: Data): Data {
  const { bundle, compressed } = buildReportBundle(report, reportPath);
  mkdirSync(dirname(reportPath), { recursive: true });
  writeFileSync(reportArchivePath(reportPath), compressed);
  writeFileSync(reportPath, `${stringifyJson(bundle, true)}\n`, "utf8");
  return bundle;
}

export function loadReportBundle(reportPath: string): Data {
  const bundle = loadJson(reportPath);
  if (bundle["schemaVersion"] !== REPORT_BUNDLE_SCHEMA) return bundle;
  const metadata = data(bundle["scenarioArchive"], "report bundle scenarioArchive");
  const archiveName = metadata["path"];
  if (
    typeof archiveName !== "string" ||
    archiveName.length === 0 ||
    archiveName.includes("/") ||
    archiveName.includes("\\")
  ) {
    throw new ContractError("scenario archive path must be a local basename");
  }
  if (metadata["format"] !== "canonical-jsonl" || metadata["compression"] !== "gzip-9-mtime-0") {
    throw new ContractError("report bundle uses an unsupported scenario archive format");
  }
  const compressedBytes = integerValue(metadata["compressedBytes"]);
  const uncompressedBytes = integerValue(metadata["uncompressedBytes"]);
  const scenarioCount = integerValue(metadata["scenarioCount"]);
  if (
    compressedBytes === null ||
    compressedBytes <= 0 ||
    compressedBytes > MAX_SCENARIO_ARCHIVE_COMPRESSED_BYTES ||
    uncompressedBytes === null ||
    uncompressedBytes <= 0 ||
    uncompressedBytes > MAX_SCENARIO_ARCHIVE_UNCOMPRESSED_BYTES ||
    scenarioCount === null ||
    scenarioCount <= 0
  ) {
    throw new ContractError("report bundle has invalid scenario archive bounds");
  }
  const archivePath = join(dirname(reportPath), archiveName);
  let actualCompressedBytes: number;
  let compressed: Buffer;
  try {
    actualCompressedBytes = statSync(archivePath).size;
    if (
      actualCompressedBytes !== compressedBytes ||
      actualCompressedBytes > MAX_SCENARIO_ARCHIVE_COMPRESSED_BYTES
    ) {
      throw new ContractError("scenario archive compressed size does not match its manifest");
    }
    compressed = readFileSync(archivePath);
  } catch (error: unknown) {
    if (error instanceof ContractError) throw error;
    throw new ContractError(
      `cannot read scenario archive ${archivePath}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  if (sha256Bytes(compressed) !== metadata["compressedSha256"]) {
    throw new ContractError("scenario archive compressed digest does not match its manifest");
  }
  let raw: Buffer;
  try {
    raw = gunzipSync(compressed, {
      maxOutputLength: MAX_SCENARIO_ARCHIVE_UNCOMPRESSED_BYTES + 1,
    });
  } catch (error: unknown) {
    throw new ContractError(
      `cannot decompress scenario archive: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  if (
    raw.length !== uncompressedBytes ||
    raw.length > MAX_SCENARIO_ARCHIVE_UNCOMPRESSED_BYTES ||
    sha256Bytes(raw) !== metadata["uncompressedSha256"]
  ) {
    throw new ContractError("scenario archive raw digest does not match its manifest");
  }
  const rawText = raw.toString("utf8");
  if (!rawText.endsWith("\n")) {
    throw new ContractError("canonical scenario archive must end with a newline");
  }
  let scenarios: Data[];
  try {
    scenarios = rawText
      .slice(0, -1)
      .split("\n")
      .map((line) => data(parseJson(line), "scenario archive record"));
  } catch (error: unknown) {
    throw new ContractError(
      `scenario archive contains invalid JSONL: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  if (scenarios.length !== scenarioCount) {
    throw new ContractError("scenario archive count does not match its manifest");
  }
  if (!scenarioJsonl(scenarios).equals(raw)) {
    throw new ContractError("scenario archive is not canonical JSONL");
  }
  const omitted = new Set([
    "latencySampleCount",
    "reconstructedReportSha256",
    "reportSchemaVersion",
    "scenarioArchive",
  ]);
  const report: Data = Object.fromEntries(
    Object.entries(bundle).filter(([key]) => !omitted.has(key)),
  );
  report["schemaVersion"] = bundle["reportSchemaVersion"];
  report["scenarioReports"] = scenarios;
  report["events"] = scenarios.flatMap((scenario) =>
    Array.isArray(scenario["events"]) ? scenario["events"].filter((event) => isData(event)) : [],
  );
  const authorityOracle = data(report["authorityOracle"], "report bundle authorityOracle");
  report["observations"] = mergeObservations(scenarios, authorityOracle);
  report["latencySamples"] = collectScenarioSpanSamples(loadJson(CONTRACT_PATH), scenarios);
  if ((report["latencySamples"] as unknown[]).length !== bundle["latencySampleCount"]) {
    throw new ContractError("reconstructed latency sample count does not match the bundle");
  }
  if (canonicalSha256(report) !== bundle["reconstructedReportSha256"]) {
    throw new ContractError("reconstructed report digest does not match the bundle");
  }
  return report;
}

function setEquals(left: Set<string>, right: Set<string>): boolean {
  return left.size === right.size && [...left].every((value) => right.has(value));
}

function uniqueIds(items: unknown, label: string): Set<string> {
  const identifiers = records(items, label).map((item) => item["id"]);
  if (identifiers.some((identifier) => typeof identifier !== "string")) {
    throw new ContractError(`every ${label} entry must have a string id`);
  }
  const result = new Set(identifiers as string[]);
  if (result.size !== identifiers.length) throw new ContractError(`${label} ids must be unique`);
  return result;
}

function arrayEquals(left: unknown, right: unknown[]): boolean {
  return Array.isArray(left) && isDeepStrictEqual(left, right);
}

export function validateContract(contract: Data): void {
  if (contract["schemaVersion"] !== "zhongduan-terminal-journey-contract-v1") {
    throw new ContractError("unsupported terminal journey contract schema");
  }
  if (contract["stage"] !== "E0" || contract["status"] !== "frozen") {
    throw new ContractError("the E0 benchmark contract must be frozen");
  }
  if (!setEquals(uniqueIds(contract["oracles"], "oracle"), EXPECTED_ORACLES)) {
    throw new ContractError("the E0 contract must define exactly the eight roadmap oracles");
  }
  if (!setEquals(uniqueIds(contract["latencySpans"], "latency span"), EXPECTED_SPANS)) {
    throw new ContractError("the E0 contract must define exactly the seven roadmap latency spans");
  }
  const matrix = data(contract["matrix"], "matrix");
  if (!arrayEquals(matrix["browserCloudRttMs"], [20, 100, 300, 600])) {
    throw new ContractError("Browser/Cloud RTT matrix must be 20/100/300/600 ms");
  }
  if (!arrayEquals(matrix["cloudHostRttMs"], [20, 100, 300, 600])) {
    throw new ContractError("Cloud/Host RTT matrix must be 20/100/300/600 ms");
  }
  const requiredDimensions: Record<string, string[]> = {
    networkFaults: ["jitter", "disconnect", "reconnect"],
    loadAndRecovery: ["output-flood", "cold-attach"],
    lifecycleAndReplacement: ["do-hibernation", "host-relay-replacement"],
  };
  for (const [name, required] of Object.entries(requiredDimensions)) {
    const actual = strings(matrix[name], `matrix.${name}`);
    if (required.some((item) => !actual.includes(item))) {
      throw new ContractError(`matrix.${name} is missing required scenarios`);
    }
  }
  if (!strings(matrix["stagingRequired"], "matrix.stagingRequired").includes("do-hibernation")) {
    throw new ContractError("real DO hibernation must remain a staging-required scenario");
  }
  const workload = data(contract["workload"], "workload");
  if (workload["id"] !== "raw-semantic-pty-v1") {
    throw new ContractError("the workload must identify the raw semantic PTY journey");
  }
  if (workload["samplesPerVariant"] !== 24 || workload["warmupSamples"] !== 4) {
    throw new ContractError("the E0 workload must retain 24 measured and 4 warmup samples");
  }
  if (
    !setEquals(
      new Set(strings(workload["requiredVariants"], "workload.requiredVariants")),
      EXPECTED_VARIANTS,
    )
  ) {
    throw new ContractError("the E0 workload must define every executable comparison variant");
  }
  const thresholds = data(contract["relativeThresholds"], "relativeThresholds");
  if (!setEquals(new Set(Object.keys(thresholds)), EXPECTED_THRESHOLDS)) {
    throw new ContractError("all three relative threshold families must be source-controlled");
  }
  for (const [thresholdId, rawThreshold] of Object.entries(thresholds)) {
    const threshold = data(rawThreshold, `relative threshold ${thresholdId}`);
    if (threshold["status"] !== "frozen-relative-fail-closed") {
      throw new ContractError(`relative threshold ${thresholdId} must be frozen`);
    }
    if (threshold["maximumRegressionRatioToCurrent"] !== 1) {
      throw new ContractError(`relative threshold ${thresholdId} must compare against CURRENT`);
    }
    if (threshold["requireFiniteCurrentMeasurement"] !== true) {
      throw new ContractError(`relative threshold ${thresholdId} must fail closed`);
    }
    if (
      threshold["currentMeasurementSource"] !==
      `current-baseline.json#/relativeThresholdMeasurements/${thresholdId}`
    ) {
      throw new ContractError(
        `relative threshold ${thresholdId} must bind its CURRENT measurement`,
      );
    }
  }
  const deadline = data(contract["deadlinePolicy"], "deadlinePolicy");
  if (!stringValue(deadline["statement"], "").includes("never evaluated as a latency SLO")) {
    throw new ContractError("the hang deadline must be distinguished from latency thresholds");
  }
}

function exactGitOid(value: unknown): value is string {
  return typeof value === "string" && /^[0-9a-f]{40}$/.test(value);
}

function deepEqual(left: unknown, right: unknown): boolean {
  return isDeepStrictEqual(left, right);
}

function round(value: number, digits: number): number {
  const scale = 10 ** digits;
  return Math.round((value + Number.EPSILON) * scale) / scale;
}

export function validateAuthorityOracle(authorityOracle: Data): void {
  if (authorityOracle["schemaVersion"] !== "zhongduan-e0-authority-oracle-v3") {
    throw new ContractError("unsupported E0 authority oracle schema");
  }
  if (authorityOracle["artifactVerified"] !== true) {
    throw new ContractError("authority oracle must use the verified committed Ghostty artifact");
  }
  if (typeof authorityOracle["engineId"] !== "string") {
    throw new ContractError("authority oracle must identify its Ghostty engine");
  }
  for (const field of ["sourceRevision", "sourceTreeGitOid"]) {
    if (!exactGitOid(authorityOracle[field])) {
      throw new ContractError(`authority oracle must identify its exact ${field}`);
    }
  }
  if (authorityOracle["sourceTreeDirty"] !== false) {
    throw new ContractError("mergeable authority evidence must come from a clean committed tree");
  }
  const corpus = records(authorityOracle["corpus"], "authority snapshot corpus");
  if (corpus.length === 0)
    throw new ContractError("authority oracle must retain its snapshot corpus");
  const stateFields = [
    "snapshotCaptureStateEqual",
    "checkpointSourceStateEqual",
    "recoveredStateEqual",
    "normalizedStateEqual",
    "checkpointSourceContinuationEqual",
    "recoveredContinuationEqual",
    "continuationEqual",
  ];
  for (const item of corpus) {
    if (
      typeof item["id"] !== "string" ||
      stateFields.some((field) => typeof item[field] !== "boolean")
    ) {
      throw new ContractError("authority snapshot corpus has an invalid comparison case");
    }
    const expectedState = Boolean(
      item["snapshotCaptureStateEqual"] &&
      item["checkpointSourceStateEqual"] &&
      item["recoveredStateEqual"],
    );
    const expectedContinuation = Boolean(
      item["checkpointSourceContinuationEqual"] && item["recoveredContinuationEqual"],
    );
    if (
      item["normalizedStateEqual"] !== expectedState ||
      item["continuationEqual"] !== expectedContinuation
    ) {
      throw new ContractError("authority snapshot case aggregate is not derivable");
    }
  }
  const effectCorpus = data(authorityOracle["effectCorpus"], "authority effect corpus");
  const effectCases = records(effectCorpus["cases"], "authority effect corpus cases");
  if (effectCases.length === 0)
    throw new ContractError("authority oracle must retain its effect corpus");
  if (
    effectCases.some(
      (item) => typeof item["id"] !== "string" || typeof item["effectsEqual"] !== "boolean",
    )
  ) {
    throw new ContractError("authority effect corpus has an invalid comparison case");
  }
  const every = (field: string): boolean => corpus.every((item) => item[field] === true);
  const expected: Data = {
    snapshotCaptureStateEqual: every("snapshotCaptureStateEqual"),
    checkpointSourceStateEqual: every("checkpointSourceStateEqual"),
    recoveredStateEqual: every("recoveredStateEqual"),
    normalizedStateEqual: every("normalizedStateEqual"),
    checkpointSourceContinuationEqual: every("checkpointSourceContinuationEqual"),
    recoveredContinuationEqual: every("recoveredContinuationEqual"),
    continuationEqual: every("continuationEqual"),
    effectsEqual: effectCases.every((item) => item["effectsEqual"] === true),
  };
  const comparison = data(authorityOracle["comparison"], "authority oracle comparison");
  if (Object.entries(expected).some(([field, value]) => comparison[field] !== value)) {
    throw new ContractError("authority oracle aggregate does not match its retained corpus");
  }
  if (
    comparison["corpusCaseCount"] !== corpus.length ||
    comparison["effectCaseCount"] !== effectCases.length ||
    effectCorpus["effectsEqual"] !== expected["effectsEqual"]
  ) {
    throw new ContractError("authority oracle corpus counts or effect aggregate do not match");
  }
}

function oracleResult(
  metric: string,
  value: number | null,
  sampleCount: number,
  violations: string[] = [],
  reason?: string,
): Data {
  const result: Data = {
    status: value === null ? "not-measured" : value === 0 ? "passed" : "failed",
    metric,
    value,
    target: 0,
    sampleCount,
    violations,
  };
  if (reason !== undefined) result["reason"] = reason;
  return result;
}

export function evaluateOracles(observations: Data): Record<string, Data> {
  const intents = Array.isArray(observations["intents"])
    ? observations["intents"].filter(isData)
    : [];
  const consumed = intents.filter((item) => item["consumed"] === true);
  const silentViolations = consumed
    .filter((item) => {
      const outcomes = item["terminalOutcomes"];
      return (
        !Array.isArray(outcomes) ||
        outcomes.length !== 1 ||
        !TERMINAL_OUTCOMES.has(String(outcomes[0]))
      );
    })
    .map((item) => stringValue(item["localIntentId"], "unknown-intent"));
  const identified = intents.filter((item) => typeof item["wireIdentity"] === "string");
  const duplicateViolations = identified
    .filter((item) => typeof item["ptyEffectCount"] === "number" && item["ptyEffectCount"] > 1)
    .map((item) => String(item["wireIdentity"]));
  const uncertainty = intents.filter((item) => item["acceptanceUncertaintyInjected"] === true);
  const retryViolations = uncertainty
    .filter((item) => item["automaticRetryCount"] !== 0)
    .map((item) =>
      stringValue(item["wireIdentity"], stringValue(item["localIntentId"], "unknown-intent")),
    );
  const interrupts = Array.isArray(observations["ctrlC"])
    ? observations["ctrlC"].filter(isData)
    : [];
  const floodInterrupts = interrupts.filter((item) => item["outputFlood"] === true);
  const interruptViolations = floodInterrupts
    .filter((item) => item["ptyEffectCount"] !== 1)
    .map((item) => stringValue(item["sampleId"], "unknown-ctrl-c"));
  const transfers = Array.isArray(observations["writerTransfers"])
    ? observations["writerTransfers"].filter(isData)
    : [];
  const transferViolations = transfers
    .filter((item) => (item["oldWriterSuccessfulEffects"] ?? 0) !== 0)
    .map((item) => stringValue(item["sampleId"], "unknown-transfer"));
  const candidates = Array.isArray(observations["coldCandidates"])
    ? observations["coldCandidates"].filter(isData)
    : [];
  const candidateViolations = candidates
    .filter((item) => item["visibleBeforeValidation"] !== false)
    .map((item) => stringValue(item["sampleId"], "unknown-candidate"));
  const authority = Array.isArray(observations["authorityComparisons"])
    ? observations["authorityComparisons"].filter(isData)
    : [];
  const authorityViolations = authority
    .filter(
      (item) =>
        item["normalizedStateEqual"] !== true ||
        item["continuationEqual"] !== true ||
        item["effectsEqual"] !== true,
    )
    .map((item) => stringValue(item["sampleId"], "unknown-authority-comparison"));
  const secure = Array.isArray(observations["secureInput"])
    ? observations["secureInput"].filter(isData)
    : [];
  const secureViolations = secure
    .filter((item) => item["speculativePresentationCount"] !== 0)
    .map((item) => stringValue(item["sampleId"], "unknown-secure-input"));

  return {
    "ui-consumed-silent-loss": oracleResult(
      "silentLossCount",
      consumed.length === 0 ? null : silentViolations.length,
      consumed.length,
      silentViolations,
      consumed.length === 0 ? "no UI-consumed intent observations" : undefined,
    ),
    "duplicate-pty-effect": oracleResult(
      "duplicatePtyEffectCount",
      identified.length === 0 ? null : duplicateViolations.length,
      identified.length,
      duplicateViolations,
      identified.length === 0 ? "no wire-identity effect observations" : undefined,
    ),
    "uncertain-auto-retry": oracleResult(
      "uncertainAutoRetryCount",
      uncertainty.length === 0 ? null : retryViolations.length,
      uncertainty.length,
      retryViolations,
      uncertainty.length === 0 ? "acceptance uncertainty was not injected" : undefined,
    ),
    "output-flood-ctrl-c-once": oracleResult(
      "invalidCtrlCEffectCount",
      floodInterrupts.length === 0 ? null : interruptViolations.length,
      floodInterrupts.length,
      interruptViolations,
      floodInterrupts.length === 0 ? "no output-flood Ctrl-C observation" : undefined,
    ),
    "old-writer-effect-after-transfer": oracleResult(
      "oldWriterSuccessfulEffectCount",
      transfers.length === 0 ? null : transferViolations.length,
      transfers.length,
      transferViolations,
      transfers.length === 0 ? "writer transfer was not exercised" : undefined,
    ),
    "cold-candidate-visible-before-validation": oracleResult(
      "prematureCandidateVisibilityCount",
      candidates.length === 0 ? null : candidateViolations.length,
      candidates.length,
      candidateViolations,
      candidates.length === 0 ? "cold attach was not exercised" : undefined,
    ),
    "snapshot-authority-divergence": oracleResult(
      "authorityDivergenceCount",
      authority.length === 0 ? null : authorityViolations.length,
      authority.length,
      authorityViolations,
      authority.length === 0 ? "snapshot-on/off authority comparison was not exercised" : undefined,
    ),
    "secure-input-speculative-presentation": oracleResult(
      "secureInputSpeculationCount",
      secure.length === 0 ? null : secureViolations.length,
      secure.length,
      secureViolations,
      secure.length === 0 ? "secure-input presentation was not exercised" : undefined,
    ),
  };
}

export function percentile(input: number[], quantile: number): number {
  if (input.length === 0) throw new ContractError("cannot calculate a percentile without samples");
  if (quantile < 0 || quantile > 1)
    throw new ContractError("percentile quantile must be in [0, 1]");
  const ordered = [...input].sort((left, right) => left - right);
  return ordered[Math.max(1, Math.ceil(quantile * ordered.length)) - 1]!;
}

export function collectSpanSamples(contract: Data, events: Data[]): Data[] {
  const byKey = new Map<string, bigint[]>();
  for (const event of events) {
    const name = event["name"];
    const sampleId = event["sampleId"];
    const variant = event["variant"];
    const atNs = nanoseconds(event["atUnixNs"]);
    if (
      typeof name !== "string" ||
      typeof sampleId !== "string" ||
      typeof variant !== "string" ||
      atNs === null ||
      atNs < 0n
    )
      continue;
    const key = stringifyJson([sampleId, variant, name]);
    const entries = byKey.get(key) ?? [];
    entries.push(atNs);
    byKey.set(key, entries);
  }
  const result: Data[] = [];
  for (const span of records(contract["latencySpans"], "latency spans")) {
    const spanId = String(span["id"]);
    const startName = String(span["startEvent"]);
    const endName = String(span["endEvent"]);
    const pairs = new Map<string, [string, string]>();
    for (const key of byKey.keys()) {
      const [sampleId, variant, name] = parseJson(key) as [string, string, string];
      if (name === startName) pairs.set(stringifyJson([sampleId, variant]), [sampleId, variant]);
    }
    for (const [sampleId, variant] of [...pairs.values()].sort((a, b) =>
      stringifyJson(a).localeCompare(stringifyJson(b)),
    )) {
      const starts = byKey.get(stringifyJson([sampleId, variant, startName])) ?? [];
      const ends = byKey.get(stringifyJson([sampleId, variant, endName])) ?? [];
      if (starts.length !== 1 || ends.length !== 1 || ends[0]! < starts[0]!) continue;
      result.push({
        span: spanId,
        sampleId,
        variant,
        durationMs: round(Number(ends[0]! - starts[0]!) / 1_000_000, 6),
      });
    }
  }
  return result;
}

export function collectScenarioSpanSamples(contract: Data, scenarios: Data[]): Data[] {
  return scenarios.flatMap((scenario) =>
    Array.isArray(scenario["events"])
      ? collectSpanSamples(contract, scenario["events"].filter(isData))
      : [],
  );
}

export function summarizeSpanSamples(samples: Data[]): Data[] {
  const grouped = new Map<string, { span: string; variant: string; values: number[] }>();
  for (const sample of samples) {
    const span = sample["span"];
    const variant = sample["variant"];
    const duration = numberValue(sample["durationMs"]);
    if (typeof span !== "string" || typeof variant !== "string" || duration === null) continue;
    const key = stringifyJson([span, variant]);
    const group = grouped.get(key) ?? { span, variant, values: [] };
    group.values.push(duration);
    grouped.set(key, group);
  }
  return [...grouped.values()]
    .sort((left, right) =>
      stringifyJson([left.span, left.variant]).localeCompare(
        stringifyJson([right.span, right.variant]),
      ),
    )
    .map(({ span, variant, values: sampleValues }) => ({
      span,
      variant,
      sampleCount: sampleValues.length,
      p50Ms: round(percentile(sampleValues, 0.5), 6),
      p95Ms: round(percentile(sampleValues, 0.95), 6),
      p99Ms: round(percentile(sampleValues, 0.99), 6),
      maxMs: round(Math.max(...sampleValues), 6),
    }));
}

export function measureRelativeThresholds(
  summaries: Data[],
  samples: Data[],
): Record<string, Data> {
  const p99ByVariant = new Map<string, number>();
  for (const summary of summaries) {
    const span = summary["span"];
    const variant = summary["variant"];
    const p99 = numberValue(summary["p99Ms"]);
    const sampleCount = integerValue(summary["sampleCount"]);
    if (
      typeof span === "string" &&
      typeof variant === "string" &&
      p99 !== null &&
      p99 >= 0 &&
      sampleCount !== null &&
      sampleCount >= MIN_MEASURED_SAMPLES
    ) {
      p99ByVariant.set(`${span}\0${variant}`, p99);
    }
  }
  const missing = (metric: string, requiredVariants: string[]): Data => ({
    status: "not-measured",
    metric,
    value: null,
    requiredVariants,
    reason: "complete finite p99 samples for every required variant are unavailable",
  });
  const cloudVariants = [
    "bulk-backlog-0",
    "bulk-backlog-262144",
    "bulk-backlog-1048576",
    "bulk-backlog-4194304",
  ];
  const cloudSamples = Object.fromEntries(
    cloudVariants.map((variant) => [variant, [] as number[]]),
  );
  for (const sample of samples) {
    const variant = sample["variant"];
    const sampleId = sample["sampleId"];
    const duration = numberValue(sample["durationMs"]);
    if (
      sample["span"] === "cloud-browser-receive-to-host-send" &&
      typeof variant === "string" &&
      cloudVariants.includes(variant) &&
      typeof sampleId === "string" &&
      sampleId.startsWith("probe-measured-") &&
      duration !== null &&
      duration >= 0
    ) {
      cloudSamples[variant]!.push(duration);
    }
  }
  const cloudCounts = Object.fromEntries(
    cloudVariants.map((variant) => [variant, cloudSamples[variant]!.length]),
  );
  const cloudP99 = Object.fromEntries(
    cloudVariants
      .filter((variant) => cloudSamples[variant]!.length >= MIN_MEASURED_SAMPLES)
      .map((variant) => [variant, percentile(cloudSamples[variant]!, 0.99)]),
  ) as Record<string, number>;
  let cloud = missing("one-plus-nonnegative-normalized-p99-slope-per-MiB", cloudVariants);
  cloud["sampleSelector"] = {
    span: "cloud-browser-receive-to-host-send",
    sampleIdPrefix: "probe-measured-",
  };
  cloud["sampleCounts"] = cloudCounts;
  const cloudValues = cloudVariants.map((variant) => cloudP99[variant]);
  if (cloudValues.every((value) => value !== undefined) && cloudValues[0]! > 0) {
    const xValues = [0, 0.25, 1, 4];
    const normalized = cloudValues.map((value) => value! / cloudValues[0]!);
    const xMean = xValues.reduce((sum, value) => sum + value, 0) / xValues.length;
    const yMean = normalized.reduce((sum, value) => sum + value, 0) / normalized.length;
    const denominator = xValues.reduce((sum, value) => sum + (value - xMean) ** 2, 0);
    const slope =
      xValues.reduce(
        (sum, xValue, index) => sum + (xValue - xMean) * (normalized[index]! - yMean),
        0,
      ) / denominator;
    cloud = {
      status: "measured",
      metric: "one-plus-nonnegative-normalized-p99-slope-per-MiB",
      value: round(1 + Math.max(0, slope), 9),
      rawNormalizedSlopePerMiB: round(slope, 9),
      requiredVariants: cloudVariants,
      probeP99MsByVariant: Object.fromEntries(
        cloudVariants.map((variant) => [variant, round(cloudP99[variant]!, 6)]),
      ),
      sampleSelector: {
        span: "cloud-browser-receive-to-host-send",
        sampleIdPrefix: "probe-measured-",
      },
      sampleCounts: cloudCounts,
    };
  }
  const ratioMeasurement = (
    span: string,
    baselineVariant: string,
    candidateVariant: string,
    metric: string,
  ): Data => {
    const requiredVariants = [baselineVariant, candidateVariant];
    const baseline = p99ByVariant.get(`${span}\0${baselineVariant}`);
    const candidate = p99ByVariant.get(`${span}\0${candidateVariant}`);
    if (baseline === undefined || candidate === undefined || baseline <= 0) {
      return missing(metric, requiredVariants);
    }
    return {
      status: "measured",
      metric,
      value: round(candidate / baseline, 9),
      baselineP99Ms: baseline,
      candidateP99Ms: candidate,
      requiredVariants,
    };
  };
  return {
    cloudBulkIsolation: cloud,
    snapshotHostInput: ratioMeasurement(
      "host-receive-to-pty-write",
      "snapshot-disabled",
      "snapshot-enabled",
      "snapshot-enabled/snapshot-disabled",
    ),
    outputFloodCtrlC: ratioMeasurement(
      "ctrl-c-to-application-quiet",
      "steady",
      "output-flood",
      "output-flood/steady",
    ),
  };
}

export function compareRelativeThresholds(
  contract: Data,
  current: Record<string, Data>,
  candidate: Record<string, Data>,
  candidateOracles: Record<string, Data>,
): Record<string, Data> {
  const comparisons: Record<string, Data> = {};
  const thresholds = data(contract["relativeThresholds"], "relative thresholds");
  for (const thresholdId of [...EXPECTED_THRESHOLDS].sort()) {
    const currentResult = current[thresholdId];
    const candidateResult = candidate[thresholdId];
    const threshold = data(thresholds[thresholdId], `relative threshold ${thresholdId}`);
    const requiredOracle = threshold["requiresCorrectnessOracle"];
    let reason: string | undefined;
    if (!isData(currentResult) || currentResult["status"] !== "measured") {
      reason = "CURRENT denominator is not measured";
    } else if (!isData(candidateResult) || candidateResult["status"] !== "measured") {
      reason = "candidate measurement is not measured";
    } else if (
      typeof requiredOracle === "string" &&
      candidateOracles[requiredOracle]?.["status"] !== "passed"
    ) {
      reason = `required correctness oracle ${requiredOracle} did not pass`;
    }
    const currentValue = numberValue(currentResult?.["value"]);
    const candidateValue = numberValue(candidateResult?.["value"]);
    if (reason === undefined && (currentValue === null || currentValue <= 0)) {
      reason = "CURRENT denominator must be finite and greater than zero";
    }
    if (reason === undefined && (candidateValue === null || candidateValue < 0)) {
      reason = "candidate measurement must be finite and non-negative";
    }
    if (reason !== undefined) {
      comparisons[thresholdId] = { status: "not-measured", reason, ratio: null };
      continue;
    }
    const maximum = numberValue(threshold["maximumRegressionRatioToCurrent"]);
    if (maximum === null)
      throw new ContractError(`relative threshold ${thresholdId} has no maximum`);
    const ratio = candidateValue! / currentValue!;
    comparisons[thresholdId] = {
      status: ratio <= maximum ? "passed" : "failed",
      ratio: round(ratio, 9),
      maximumRatio: maximum,
    };
  }
  return comparisons;
}

const OBSERVATION_COLLECTIONS = [
  "intents",
  "ctrlC",
  "writerTransfers",
  "coldCandidates",
  "secureInput",
];

function validateTerminalIntents(input: unknown, label: string): Data[] {
  const intents = records(input, label);
  const localIds = new Set<string>();
  for (const item of intents) {
    const localId = item["localIntentId"];
    const sampleId = item["sampleId"];
    const outcomes = item["terminalOutcomes"];
    const terminalRecords = item["terminalRecords"];
    const validRecords = Array.isArray(terminalRecords) && terminalRecords.every(isData);
    if (
      item["consumed"] !== true ||
      typeof localId !== "string" ||
      localId.length === 0 ||
      typeof sampleId !== "string" ||
      sampleId.length === 0 ||
      !Array.isArray(outcomes) ||
      outcomes.length > 1 ||
      outcomes.some((outcome) => typeof outcome !== "string" || !TERMINAL_OUTCOMES.has(outcome)) ||
      !validRecords ||
      terminalRecords.length !== outcomes.length ||
      (terminalRecords.length === 1 &&
        (terminalRecords[0]!["localIntentId"] !== localId ||
          terminalRecords[0]!["outcome"] !== outcomes[0]))
    ) {
      throw new ContractError(
        `every ${label} item must retain zero or one passive terminal outcome`,
      );
    }
    if (localIds.has(localId)) throw new ContractError(`${label} LocalIntentIds must be unique`);
    localIds.add(localId);
  }
  return intents;
}

function validateObservationEffects(observations: Data, events: Data[], label: string): void {
  const attempts = new Map<string, string[]>();
  const fullIdentities = new Map<string, string[]>();
  const effects = new Map<string, number>();
  for (const event of events) {
    const sampleId = event["sampleId"];
    if (typeof sampleId !== "string") continue;
    if (
      event["name"] === "cloud.browser-receive-attempt" &&
      typeof event["browserIdentity"] === "string"
    ) {
      attempts.set(sampleId, [...(attempts.get(sampleId) ?? []), event["browserIdentity"]]);
    }
    if (event["name"] === "host.receive" && typeof event["wireIdentity"] === "string") {
      fullIdentities.set(sampleId, [
        ...(fullIdentities.get(sampleId) ?? []),
        event["wireIdentity"],
      ]);
    }
    if (event["name"] === "host.pty-write") effects.set(sampleId, (effects.get(sampleId) ?? 0) + 1);
  }
  for (const item of records(observations["intents"], `${label} intents`)) {
    const sampleId = String(item["sampleId"]);
    const sampleAttempts = attempts.get(sampleId) ?? [];
    const browserIdentities = [...new Set(sampleAttempts)].sort();
    const sampleFullIdentities = fullIdentities.get(sampleId) ?? [];
    const identities = [...new Set(sampleFullIdentities)].sort();
    if (item["ptyEffectCount"] !== (effects.get(sampleId) ?? 0)) {
      throw new ContractError(`${label} PTY effect count is not derived from raw events`);
    }
    if (
      !deepEqual(item["passiveBrowserIdentities"], browserIdentities) ||
      item["passiveSendAttemptCount"] !== sampleAttempts.length
    ) {
      throw new ContractError(`${label} passive send evidence disagrees with proxy events`);
    }
    if (browserIdentities.length > 0) {
      if (
        item["browserIdentity"] !== sampleAttempts[0] ||
        !deepEqual(item["browserIdentities"], browserIdentities)
      ) {
        throw new ContractError(`${label} Browser identities are not derived from raw events`);
      }
    } else if ("browserIdentity" in item || "browserIdentities" in item) {
      throw new ContractError(`${label} claims a Browser identity without a send event`);
    }
    if (identities.length > 0) {
      if (
        item["wireIdentity"] !== sampleFullIdentities[0] ||
        !deepEqual(item["wireIdentities"], identities)
      ) {
        throw new ContractError(`${label} wire identities are not derived from raw events`);
      }
      const terminalRecords = Array.isArray(item["terminalRecords"])
        ? item["terminalRecords"].filter(isData)
        : [];
      const terminalIdentity = terminalRecords.length > 0 ? terminalRecords[0]!["identity"] : null;
      if (isData(terminalIdentity)) {
        const wire = ["writerFence", "inputEpoch", "clientInputSeq"]
          .map((name) => String(terminalIdentity[name]))
          .join("/");
        if (!identities.includes(wire)) {
          throw new ContractError(`${label} terminal identity is absent from observed sends`);
        }
      }
    } else if ("wireIdentity" in item || "wireIdentities" in item) {
      throw new ContractError(`${label} claims a wire identity without a send event`);
    }
    if (item["acceptanceUncertaintyInjected"] === true) {
      const expectedRetryCount = Math.max(0, sampleAttempts.length - 1);
      if (
        item["automaticRetryCount"] !== expectedRetryCount ||
        item["identityChanged"] !== browserIdentities.length > 1
      ) {
        throw new ContractError(
          `${label} uncertainty retry evidence is not derived from send attempts`,
        );
      }
    }
  }
  for (const item of records(observations["ctrlC"], `${label} Ctrl-C`)) {
    const sampleId = item["sampleId"];
    if (typeof sampleId !== "string" || item["ptyEffectCount"] !== (effects.get(sampleId) ?? 0)) {
      throw new ContractError(`${label} Ctrl-C effect count is not derived from raw events`);
    }
  }
}

function uniqueStringList(value: unknown): value is string[] {
  return (
    Array.isArray(value) &&
    value.every((item) => typeof item === "string") &&
    new Set(value).size === value.length
  );
}

export function validateScenarioReport(
  scenario: Data,
  contract: Data,
  options: { requireClean?: boolean } = {},
): void {
  validateContract(contract);
  if (scenario["schemaVersion"] !== "zhongduan-terminal-journey-scenario-v1") {
    throw new ContractError("unsupported E0 scenario schema");
  }
  if (scenario["status"] !== "measured")
    throw new ContractError("an E0 scenario must complete before it can be merged");
  if (scenario["contractSha256"] !== canonicalSha256(contract)) {
    throw new ContractError("scenario does not reference the exact benchmark contract");
  }
  const variant = scenario["variant"];
  if (typeof variant !== "string" || !EXPECTED_VARIANTS.has(variant)) {
    throw new ContractError("scenario uses an unknown E0 variant");
  }
  const workload = data(contract["workload"], "contract workload");
  if (scenario["samples"] !== workload["samplesPerVariant"]) {
    throw new ContractError("scenario did not collect exactly 24 measured samples");
  }
  if (scenario["warmups"] !== workload["warmupSamples"]) {
    throw new ContractError("scenario did not execute exactly four warmup samples");
  }
  if ((options.requireClean ?? true) && scenario["sourceTreeDirty"] !== false) {
    throw new ContractError("mergeable E0 scenarios must come from a clean source tree");
  }
  for (const field of ["sourceRevision", "sourceTreeGitOid"]) {
    if (!exactGitOid(scenario[field]))
      throw new ContractError(`scenario ${field} must be an exact Git object id`);
  }
  const evidence = data(scenario["workloadEvidence"], "scenario workload evidence");
  const primary = evidence["primarySampleIds"];
  const warmupPrimary = evidence["warmupPrimarySampleIds"];
  if (!uniqueStringList(primary) || primary.length !== workload["samplesPerVariant"]) {
    throw new ContractError("workload evidence must identify 24 unique measured samples");
  }
  if (!uniqueStringList(warmupPrimary) || warmupPrimary.length !== workload["warmupSamples"]) {
    throw new ContractError("workload evidence must identify four unique warmup samples");
  }
  const measurementStart = integerValue(evidence["measurementStartedAtUnixMs"]);
  const measurementEnd = integerValue(evidence["measurementEndedAtUnixMs"]);
  if (measurementStart === null || measurementEnd === null || measurementStart > measurementEnd) {
    throw new ContractError("workload evidence must retain its measurement window");
  }
  const observations = data(scenario["observations"], "scenario observations");
  const warmupObservations = data(scenario["warmupObservations"], "scenario warmup observations");
  const measuredIntents = validateTerminalIntents(observations["intents"], "measured intents");
  const warmupIntents = validateTerminalIntents(warmupObservations["intents"], "warmup intents");
  const allLocalIds = [...measuredIntents, ...warmupIntents].map((item) =>
    String(item["localIntentId"]),
  );
  if (new Set(allLocalIds).size !== allLocalIds.length) {
    throw new ContractError("LocalIntentIds must remain unique across warmup and measurement");
  }
  const events = records(scenario["events"], "scenario events");
  if (events.some((event) => event["variant"] !== variant)) {
    throw new ContractError("scenario must retain raw events under its actual variant");
  }
  const consumedSamples = events
    .filter(
      (event) =>
        event["name"] === "browser.input-consumed" && typeof event["sampleId"] === "string",
    )
    .map((event) => event["sampleId"] as string)
    .sort();
  const observedSamples = measuredIntents.map((item) => String(item["sampleId"])).sort();
  if (!deepEqual(consumedSamples, observedSamples)) {
    throw new ContractError(
      "every measured UI-consumed event must map to one terminal intent observation",
    );
  }
  if (scenario["rawEventCount"] !== events.length)
    throw new ContractError("scenario rawEventCount does not match retained events");
  validateObservationEffects(observations, events, "measured observations");
  if (scenario["deadlineIsSlo"] !== false)
    throw new ContractError("scenario deadline must not be reported as a latency SLO");
  const cloudBoundary = data(scenario["cloudSpanBoundary"], "scenario Cloud span boundary");
  if (
    cloudBoundary["includesBrowserLink"] !== false ||
    cloudBoundary["includesHostLink"] !== false
  ) {
    throw new ContractError("Cloud span boundary must exclude both simulated links");
  }
  records(scenario["hostMeasurements"] ?? [], "scenario Host measurements");
  const ctrlC = records(observations["ctrlC"], "Ctrl-C observations");
  records(warmupObservations["ctrlC"], "warmup Ctrl-C observations");
  const expectedBulk = variant.startsWith("bulk-backlog-")
    ? Number(variant.slice("bulk-backlog-".length))
    : 0;
  if (evidence["configuredBulkBacklogBytes"] !== expectedBulk) {
    throw new ContractError("bulk backlog variant label does not match its applied bytes");
  }
  if (variant === "steady") {
    if (ctrlC.length !== 24 || ctrlC.some((item) => item["outputFlood"] !== false)) {
      throw new ContractError("steady must execute 24 non-flood Ctrl-C samples");
    }
  } else if (variant === "output-flood") {
    if (
      evidence["outputFlood"] !== true ||
      ctrlC.length !== 24 ||
      ctrlC.some((item) => item["outputFlood"] !== true)
    ) {
      throw new ContractError("output-flood must execute 24 real flood Ctrl-C samples");
    }
  } else if (variant.startsWith("bulk-backlog-")) {
    if (evidence["outputFlood"] !== expectedBulk > 0) {
      throw new ContractError("bulk variant did not apply its declared output workload");
    }
  } else if (variant === "snapshot-disabled") {
    if (evidence["snapshotFinalizationsDuringMeasurement"] !== 0) {
      throw new ContractError("snapshot-disabled observed snapshot work during measurement");
    }
    if (evidence["snapshotInputOverlap"] !== null) {
      throw new ContractError("snapshot-disabled must not claim snapshot/input overlap");
    }
  } else if (variant === "snapshot-enabled") {
    const count = integerValue(evidence["snapshotFinalizationsDuringMeasurement"]);
    if (count === null || count < 1)
      throw new ContractError("snapshot-enabled did not finalize a background snapshot");
    const overlap = data(evidence["snapshotInputOverlap"], "snapshot-enabled overlap evidence");
    const firstAt = nanoseconds(overlap["firstHostReceiveAtUnixNs"]);
    const finalizedAt = nanoseconds(overlap["snapshotFinalizationAtUnixNs"]);
    const lastAt = nanoseconds(overlap["lastHostReceiveAtUnixNs"]);
    const firstSample = overlap["firstSampleId"];
    const lastSample = overlap["lastSampleId"];
    const snapshotId = overlap["snapshotId"];
    if (
      firstAt === null ||
      finalizedAt === null ||
      lastAt === null ||
      typeof firstSample !== "string" ||
      typeof lastSample !== "string" ||
      typeof snapshotId !== "string" ||
      !(firstAt < finalizedAt && finalizedAt < lastAt)
    ) {
      throw new ContractError(
        "snapshot-enabled finalization must fall inside its overlap Host input window",
      );
    }
    const hasEvent = (name: string, expected: Record<string, unknown>): boolean =>
      events.some(
        (event) =>
          event["name"] === name &&
          Object.entries(expected).every(([key, value]) => deepEqual(event[key], value)),
      );
    if (
      !hasEvent("host.receive", {
        sampleId: firstSample,
        atUnixNs: overlap["firstHostReceiveAtUnixNs"],
      }) ||
      !hasEvent("host.receive", {
        sampleId: lastSample,
        atUnixNs: overlap["lastHostReceiveAtUnixNs"],
      })
    ) {
      throw new ContractError("snapshot overlap Host input evidence is not retained raw");
    }
    if (
      !hasEvent("host.snapshot-finalized", {
        snapshotId,
        atUnixNs: overlap["snapshotFinalizationAtUnixNs"],
      })
    ) {
      throw new ContractError("snapshot overlap finalization evidence is not retained raw");
    }
  } else if (variant === "correctness-faults") {
    for (const field of [
      "acceptanceDisconnect",
      "acceptanceReconnectObserved",
      "writerTransfer",
      "coldAttachValidation",
    ]) {
      if (evidence[field] !== true)
        throw new ContractError("correctness-faults did not execute every declared fault");
    }
    if (!measuredIntents.some((item) => item["acceptanceUncertaintyInjected"] === true)) {
      throw new ContractError("correctness-faults lacks acceptance-uncertainty evidence");
    }
    if (
      records(observations["writerTransfers"], "writer transfers").length === 0 ||
      records(observations["coldCandidates"], "cold candidates").length === 0
    ) {
      throw new ContractError("correctness-faults lacks transfer or cold-attach evidence");
    }
    if (records(observations["secureInput"], "secure input").length === 0) {
      throw new ContractError("correctness-faults lacks secure-input evidence");
    }
  }
  const measuredSampleIds = new Set(measuredIntents.map((item) => String(item["sampleId"])));
  for (const primarySample of primary) {
    const required =
      variant === "output-flood"
        ? [
            `flood-command-ctrl-c-${primarySample}`,
            `arm-ctrl-c-${primarySample}`,
            `ctrl-c-${primarySample}`,
          ]
        : variant === "steady"
          ? [`probe-${primarySample}`, `arm-ctrl-c-${primarySample}`, `ctrl-c-${primarySample}`]
          : variant.startsWith("bulk-backlog-") && expectedBulk > 0
            ? [`flood-command-${primarySample}`, `probe-${primarySample}`]
            : [`probe-${primarySample}`];
    if (required.some((sampleId) => !measuredSampleIds.has(sampleId))) {
      throw new ContractError("variant label is not backed by every primary workload sample");
    }
  }
}

export function mergeObservations(scenarios: Data[], authorityOracle: Data): Data {
  validateAuthorityOracle(authorityOracle);
  const merged: Data = Object.fromEntries(OBSERVATION_COLLECTIONS.map((name) => [name, []]));
  for (const scenario of scenarios) {
    const observations = data(scenario["observations"], "scenario observations");
    for (const name of OBSERVATION_COLLECTIONS) {
      const entries = values(observations[name] ?? [], `scenario observations.${name}`);
      (merged[name] as unknown[]).push(...entries);
    }
  }
  merged["authorityComparisons"] = [
    data(authorityOracle["comparison"], "authority oracle comparison"),
  ];
  return merged;
}

export interface ReportAssemblyOptions {
  environment: Data;
  evidenceBoundary: Data;
  generatedAt: string;
  sourceRevision: string;
  sourceTreeGitOid: string;
  deadlineMs: number;
}

function reportParts(
  contract: Data,
  scenarios: Data[],
  authorityOracle: Data,
): {
  observations: Data;
  events: Data[];
  latencySamples: Data[];
  latencySummaries: Data[];
  oracleResults: Record<string, Data>;
  relative: Record<string, Data>;
  measuredSpans: Set<string>;
} {
  const observations = mergeObservations(scenarios, authorityOracle);
  const events = scenarios.flatMap((scenario) =>
    Array.isArray(scenario["events"]) ? scenario["events"].filter(isData) : [],
  );
  const latencySamples = collectScenarioSpanSamples(contract, scenarios);
  const latencySummaries = summarizeSpanSamples(latencySamples);
  const oracleResults = evaluateOracles(observations);
  const relative = measureRelativeThresholds(latencySummaries, latencySamples);
  const measuredSpans = new Set(
    latencySamples
      .map((sample) => sample["span"])
      .filter((span): span is string => typeof span === "string"),
  );
  return {
    observations,
    events,
    latencySamples,
    latencySummaries,
    oracleResults,
    relative,
    measuredSpans,
  };
}

export function assembleCurrentReport(
  contract: Data,
  scenarios: Data[],
  authorityOracle: Data,
  options: ReportAssemblyOptions,
): Data {
  const parts = reportParts(contract, scenarios, authorityOracle);
  const workload = data(contract["workload"], "contract workload");
  const report: Data = {
    schemaVersion: "zhongduan-terminal-journey-report-v1",
    contractSha256: canonicalSha256(contract),
    baseline: "CURRENT",
    baselineStatus: "complete",
    sourceRevision: options.sourceRevision,
    sourceTreeGitOid: options.sourceTreeGitOid,
    sourceTreeDirty: false,
    generatedAt: options.generatedAt,
    environment: options.environment,
    workload: {
      id: workload["id"],
      samplesPerVariant: workload["samplesPerVariant"],
      warmupSamples: workload["warmupSamples"],
    },
    evidenceBoundary: options.evidenceBoundary,
    scenarioReports: scenarios,
    matrixCoverage: coverageForScenarios(contract, scenarios),
    oracleResults: parts.oracleResults,
    currentFailures: currentFailures(parts.oracleResults, parts.relative),
    events: parts.events,
    latencySamples: parts.latencySamples,
    latencySummaries: parts.latencySummaries,
    unmeasuredLatencySpans: [...EXPECTED_SPANS]
      .filter((span) => !parts.measuredSpans.has(span))
      .sort(),
    relativeThresholdMeasurements: parts.relative,
    authorityOracle,
    observations: parts.observations,
    rawEventCount: parts.events.length,
    deadlineMs: options.deadlineMs,
    deadlineIsSlo: false,
  };
  validateReport(report, contract);
  return report;
}

export function assembleCandidateReport(
  contract: Data,
  scenarios: Data[],
  authorityOracle: Data,
  currentReport: Data,
  options: ReportAssemblyOptions & { snapshotPhaseMeasurements?: Data[] },
): Data {
  validateReport(currentReport, contract);
  const parts = reportParts(contract, scenarios, authorityOracle);
  const workload = data(contract["workload"], "contract workload");
  const comparisons = compareRelativeThresholds(
    contract,
    data(currentReport["relativeThresholdMeasurements"], "CURRENT relative thresholds") as Record<
      string,
      Data
    >,
    parts.relative,
    parts.oracleResults,
  );
  const comparisonFailures = Object.entries(comparisons)
    .sort(([left], [right]) => left.localeCompare(right))
    .filter(([, result]) => result["status"] !== "passed")
    .map(([thresholdId, result]) => ({
      gate: `candidate-comparison/${thresholdId}`,
      status: result["status"],
      reason: result["reason"] ?? "candidate regressed CURRENT",
    }));
  const report: Data = {
    schemaVersion: "zhongduan-terminal-journey-candidate-v1",
    contractSha256: canonicalSha256(contract),
    baseline: "CANDIDATE",
    candidateStatus: "complete",
    sourceRevision: options.sourceRevision,
    sourceTreeGitOid: options.sourceTreeGitOid,
    sourceTreeDirty: false,
    generatedAt: options.generatedAt,
    environment: options.environment,
    workload: {
      id: workload["id"],
      samplesPerVariant: workload["samplesPerVariant"],
      warmupSamples: workload["warmupSamples"],
    },
    evidenceBoundary: options.evidenceBoundary,
    scenarioReports: scenarios,
    matrixCoverage: coverageForScenarios(contract, scenarios),
    oracleResults: parts.oracleResults,
    candidateFailures: [
      ...currentFailures(parts.oracleResults, parts.relative),
      ...comparisonFailures,
    ],
    comparisonToCurrent: {
      currentSourceRevision: currentReport["sourceRevision"],
      currentSourceTreeGitOid: currentReport["sourceTreeGitOid"],
      relativeThresholds: comparisons,
    },
    events: parts.events,
    latencySamples: parts.latencySamples,
    latencySummaries: parts.latencySummaries,
    unmeasuredLatencySpans: [...EXPECTED_SPANS]
      .filter((span) => !parts.measuredSpans.has(span))
      .sort(),
    relativeThresholdMeasurements: parts.relative,
    authorityOracle,
    observations: parts.observations,
    rawEventCount: parts.events.length,
    snapshotPhaseMeasurements: options.snapshotPhaseMeasurements ?? [],
    deadlineMs: options.deadlineMs,
    deadlineIsSlo: false,
  };
  validateCandidateReport(report, currentReport, contract);
  return report;
}

export function validateCandidateReport(report: Data, currentReport: Data, contract: Data): void {
  if (report["schemaVersion"] !== "zhongduan-terminal-journey-candidate-v1") {
    throw new ContractError("unsupported E0 candidate report schema");
  }
  if (report["baseline"] !== "CANDIDATE" || report["candidateStatus"] !== "complete") {
    throw new ContractError("candidate evidence must be generated as a complete CANDIDATE");
  }
  if ("baselineStatus" in report || "currentFailures" in report) {
    throw new ContractError("candidate evidence must not contain CURRENT-only fields");
  }
  validateReport(currentReport, contract);
  const validationView: Data = { ...report };
  validationView["schemaVersion"] = "zhongduan-terminal-journey-report-v1";
  validationView["baseline"] = "CURRENT";
  validationView["baselineStatus"] = "complete";
  validationView["currentFailures"] = currentFailures(
    data(report["oracleResults"], "candidate oracle results") as Record<string, Data>,
    data(report["relativeThresholdMeasurements"], "candidate relative thresholds") as Record<
      string,
      Data
    >,
  );
  for (const field of [
    "candidateStatus",
    "candidateFailures",
    "comparisonToCurrent",
    "snapshotPhaseMeasurements",
  ]) {
    delete validationView[field];
  }
  validateReport(validationView, contract);
  const expectedComparisons = compareRelativeThresholds(
    contract,
    data(currentReport["relativeThresholdMeasurements"], "CURRENT relative thresholds") as Record<
      string,
      Data
    >,
    data(report["relativeThresholdMeasurements"], "candidate relative thresholds") as Record<
      string,
      Data
    >,
    data(report["oracleResults"], "candidate oracle results") as Record<string, Data>,
  );
  const expectedReference = {
    currentSourceRevision: currentReport["sourceRevision"],
    currentSourceTreeGitOid: currentReport["sourceTreeGitOid"],
    relativeThresholds: expectedComparisons,
  };
  if (!deepEqual(report["comparisonToCurrent"], expectedReference)) {
    throw new ContractError("candidate comparison is not derived from checked CURRENT");
  }
  const expectedFailures = currentFailures(
    data(report["oracleResults"], "candidate oracle results") as Record<string, Data>,
    data(report["relativeThresholdMeasurements"], "candidate relative thresholds") as Record<
      string,
      Data
    >,
  );
  for (const [thresholdId, result] of Object.entries(expectedComparisons).sort(([left], [right]) =>
    left.localeCompare(right),
  )) {
    if (result["status"] !== "passed") {
      expectedFailures.push({
        gate: `candidate-comparison/${thresholdId}`,
        status: result["status"],
        reason: result["reason"] ?? "candidate regressed CURRENT",
      });
    }
  }
  if (!deepEqual(report["candidateFailures"], expectedFailures)) {
    throw new ContractError("candidateFailures is not derived from candidate evidence");
  }
  const phases = records(
    report["snapshotPhaseMeasurements"],
    "candidate snapshot phase measurements",
  );
  const requiredRefresh = new Set([
    "refresh-queue-wait",
    "authority-actor-wait",
    "authority-cut",
    "authority-encode",
    "publisher-total",
    "finalize-install",
  ]);
  const requiredPublish = new Set(["compress", "hash", "upload", "finalize"]);
  const refresh = new Set<string>();
  const publish = new Set<string>();
  const observedEvents = new Set<string>();
  for (const item of phases) {
    if (
      item["schemaVersion"] !== 1 ||
      item["source"] !== "host-cloud-relay" ||
      integerValue(item["recordedAtUnixMs"]) === null
    ) {
      throw new ContractError("candidate Host measurement has an invalid provenance envelope");
    }
    const event = item["event"];
    if (
      event !== "input-actor-queue" &&
      event !== "snapshot-refresh" &&
      event !== "snapshot-publish"
    ) {
      throw new ContractError("candidate Host measurement uses an unknown event");
    }
    observedEvents.add(event);
    const duration = numberValue(
      event === "input-actor-queue" ? item["queueWaitMs"] : item["durationMs"],
    );
    if (duration === null || duration < 0)
      throw new ContractError("candidate Host measurement duration must be finite");
    if (event === "snapshot-refresh" && typeof item["phase"] === "string")
      refresh.add(item["phase"]);
    if (event === "snapshot-publish" && typeof item["phase"] === "string")
      publish.add(item["phase"]);
  }
  if (
    !setEquals(
      observedEvents,
      new Set(["input-actor-queue", "snapshot-refresh", "snapshot-publish"]),
    )
  ) {
    throw new ContractError("candidate lacks input and snapshot Host measurements");
  }
  if ([...requiredRefresh].some((phase) => !refresh.has(phase))) {
    throw new ContractError("candidate lacks required snapshot refresh phases");
  }
  if ([...requiredPublish].some((phase) => !publish.has(phase))) {
    throw new ContractError("candidate lacks required snapshot publish phases");
  }
  for (const variant of ["snapshot-disabled", "snapshot-enabled"]) {
    const windowItems = phases.filter((item) => {
      const start = integerValue(item["measurementStartedAtUnixMs"]);
      const end = integerValue(item["measurementEndedAtUnixMs"]);
      const recorded = integerValue(item["recordedAtUnixMs"]);
      return (
        item["scenarioVariant"] === variant &&
        start !== null &&
        end !== null &&
        recorded !== null &&
        start <= recorded &&
        recorded <= end
      );
    });
    if (!windowItems.some((item) => item["event"] === "input-actor-queue")) {
      throw new ContractError(`${variant} lacks in-window input actor measurements`);
    }
    const snapshotItems = windowItems.filter(
      (item) => typeof item["event"] === "string" && item["event"].startsWith("snapshot-"),
    );
    if (variant === "snapshot-disabled" && snapshotItems.length > 0) {
      throw new ContractError("snapshot-disabled measurement window executed snapshot work");
    }
    if (variant === "snapshot-enabled" && snapshotItems.length === 0) {
      throw new ContractError("snapshot-enabled measurement window lacks snapshot work");
    }
  }
}

export function buildE4bDecision(currentReport: Data, candidateReport: Data, contract: Data): Data {
  validateCandidateReport(candidateReport, currentReport, contract);
  const comparisonToCurrent = data(candidateReport["comparisonToCurrent"], "candidate comparison");
  const comparisons = data(
    comparisonToCurrent["relativeThresholds"],
    "candidate threshold comparisons",
  );
  const phases = records(
    candidateReport["snapshotPhaseMeasurements"],
    "candidate snapshot phase measurements",
  );
  const authority = phases.filter(
    (item) =>
      item["event"] === "snapshot-refresh" &&
      (item["phase"] === "authority-cut" || item["phase"] === "authority-encode") &&
      item["outcome"] === "ok",
  );
  const authorityValues: Record<string, number[]> = { "authority-cut": [], "authority-encode": [] };
  for (const item of authority)
    authorityValues[String(item["phase"])]!.push(Number(item["durationMs"]));
  if (
    authorityValues["authority-cut"]!.length === 0 ||
    authorityValues["authority-encode"]!.length === 0
  ) {
    throw new ContractError("E4b decision requires finite authority cut and encode measurements");
  }
  const inputActor = phases.filter(
    (item) => item["event"] === "input-actor-queue" && item["outcome"] === "ok",
  );
  let overlapCount = 0;
  for (const inputItem of inputActor) {
    const inputEnd = Number(inputItem["recordedAtUnixMs"]);
    const inputStart = inputEnd - Number(inputItem["queueWaitMs"]);
    if (
      authority.some((item) => {
        const phaseEnd = Number(item["recordedAtUnixMs"]);
        const phaseStart = phaseEnd - Number(item["durationMs"]);
        return Math.max(inputStart, phaseStart) <= Math.min(inputEnd, phaseEnd);
      })
    )
      overlapCount += 1;
  }
  const allPass = Object.values(comparisons).every(
    (result) => isData(result) && result["status"] === "passed",
  );
  const snapshotFailed =
    data(comparisons["snapshotHostInput"], "snapshot comparison")["status"] === "failed";
  let decision: string;
  let reason: string;
  let skipAuthorized: boolean;
  let immutableCowCutAuthorized: boolean;
  let r0Authorized: boolean;
  if (allPass) {
    decision = "skip-immutable-cow-cut";
    reason =
      "All candidate latency ratios pass finite CURRENT denominators, the output-flood correctness oracle passes, and authority cut/encode phases are finite.";
    skipAuthorized = true;
    immutableCowCutAuthorized = false;
    r0Authorized = true;
  } else if (snapshotFailed && overlapCount > 0) {
    decision = "implement-immutable-cow-cut";
    reason =
      "The snapshot Host-input comparison regressed CURRENT and measured input actor queue residence overlaps synchronous authority cut/encode work.";
    skipAuthorized = false;
    immutableCowCutAuthorized = true;
    r0Authorized = false;
  } else {
    decision = "not-authorized-non-cut-failure";
    reason =
      "Finite candidate evidence failed a gate without locating the failure in synchronous authority cut/encode; E4b cannot be used as an unrelated workaround.";
    skipAuthorized = false;
    immutableCowCutAuthorized = false;
    r0Authorized = false;
  }
  const authorityPause = Object.fromEntries(
    Object.entries(authorityValues)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([phase, phaseValues]) => [
        phase,
        {
          sampleCount: phaseValues.length,
          p99Ms: round(percentile(phaseValues, 0.99), 6),
          maxMs: round(Math.max(...phaseValues), 6),
        },
      ]),
  );
  return {
    schemaVersion: "zhongduan-e4b-decision-v1",
    stage: "E4b",
    status: "complete-finite-measurement",
    decision,
    reason,
    evidence: {
      currentSourceRevision: currentReport["sourceRevision"],
      currentSourceTreeGitOid: currentReport["sourceTreeGitOid"],
      candidateSourceRevision: candidateReport["sourceRevision"],
      candidateSourceTreeGitOid: candidateReport["sourceTreeGitOid"],
      relativeThresholdComparisons: comparisons,
      authorityPause,
      inputActorSampleCount: inputActor.length,
      inputAuthorityOverlapCount: overlapCount,
    },
    currentAssessment: {
      finiteCurrentBaselineMeasurementAvailable: true,
      candidateE4aMeasurementAvailable: true,
      skipAuthorized,
      immutableCowCutAuthorized,
      r0Authorized,
    },
    implementationBoundary: {
      ghosttyChanged: false,
      wtermChanged: false,
      note: "This decision artifact does not itself modify Ghostty or WTerm; any authorized E4b implementation remains a separate fork-only change.",
    },
  };
}

export function validateReport(report: Data, contract: Data): void {
  validateContract(contract);
  if (report["schemaVersion"] !== "zhongduan-terminal-journey-report-v1") {
    throw new ContractError("unsupported E0 report schema");
  }
  if (report["contractSha256"] !== canonicalSha256(contract)) {
    throw new ContractError("report does not reference the exact checked-in benchmark contract");
  }
  if (report["baseline"] !== "CURRENT")
    throw new ContractError("the E0 baseline report must be labelled CURRENT");
  if (report["baselineStatus"] !== "complete")
    throw new ContractError("a checked CURRENT report must be complete");
  if (report["sourceTreeDirty"] !== false)
    throw new ContractError("CURRENT evidence must come from a clean committed source tree");
  const sourceRevision = report["sourceRevision"];
  const sourceTreeGitOid = report["sourceTreeGitOid"];
  if (!exactGitOid(sourceRevision))
    throw new ContractError("CURRENT evidence must identify its exact source commit");
  if (!exactGitOid(sourceTreeGitOid))
    throw new ContractError("CURRENT evidence must identify its exact source tree");
  const workload = data(report["workload"], "report workload");
  const contractWorkload = data(contract["workload"], "contract workload");
  const expectedWorkload = {
    id: contractWorkload["id"],
    samplesPerVariant: contractWorkload["samplesPerVariant"],
    warmupSamples: contractWorkload["warmupSamples"],
  };
  if (!deepEqual(workload, expectedWorkload))
    throw new ContractError("report workload must exactly match the frozen E0 workload");
  const scenarios = records(report["scenarioReports"], "report scenario reports");
  if (scenarios.length === 0)
    throw new ContractError("report must contain source scenario reports");
  const measuredVariants = new Set<string>();
  for (const scenario of scenarios) {
    validateScenarioReport(scenario, contract);
    if (
      scenario["sourceRevision"] !== sourceRevision ||
      scenario["sourceTreeGitOid"] !== sourceTreeGitOid ||
      scenario["sourceTreeDirty"] !== false
    ) {
      throw new ContractError("all scenarios must come from the same clean source tree");
    }
    const variant = scenario["variant"];
    if (typeof variant === "string") measuredVariants.add(variant);
  }
  const missingVariants = [...EXPECTED_VARIANTS]
    .filter((variant) => !measuredVariants.has(variant))
    .sort();
  if (missingVariants.length > 0) {
    throw new ContractError(
      `CURRENT report is missing required variants: ${stringifyJson(missingVariants)}`,
    );
  }
  const environment = data(report["environment"], "report environment");
  if (
    environment["executionTier"] !== "local-workerd" &&
    environment["executionTier"] !== "cloudflare-staging"
  ) {
    throw new ContractError("report environment must name its real execution tier");
  }
  const evidenceBoundary = data(report["evidenceBoundary"], "report evidence boundary");
  if (
    environment["executionTier"] === "local-workerd" &&
    evidenceBoundary["realCloudflareEdge"] !== false
  ) {
    throw new ContractError("a local Workerd report must not claim real Cloudflare edge evidence");
  }
  const observations = data(report["observations"], "report observations");
  const authorityOracle = data(report["authorityOracle"], "report authority oracle");
  if (!deepEqual(observations, mergeObservations(scenarios, authorityOracle))) {
    throw new ContractError("aggregate observations do not match merged scenarios");
  }
  const localIds = Array.isArray(observations["intents"])
    ? observations["intents"].filter(isData).map((item) => item["localIntentId"])
    : [];
  if (new Set(localIds).size !== localIds.length)
    throw new ContractError("merged LocalIntentIds must be globally unique");
  const recomputedOracles = evaluateOracles(observations);
  if (!deepEqual(report["oracleResults"], recomputedOracles)) {
    throw new ContractError("oracle results do not match recomputed raw observations");
  }
  const expectedCoverage = coverageForScenarios(contract, scenarios);
  if (!deepEqual(report["matrixCoverage"], expectedCoverage)) {
    throw new ContractError("matrix coverage does not match the exact executed scenarios");
  }
  if (expectedCoverage.some((cell) => cell["status"] === "not-run")) {
    throw new ContractError("complete CURRENT evidence must measure every local matrix cell");
  }
  const events = records(report["events"], "report events");
  const mergedEvents = scenarios.flatMap((scenario) =>
    records(scenario["events"], "scenario events"),
  );
  if (!deepEqual(events, mergedEvents))
    throw new ContractError("aggregate timing events do not match merged scenarios");
  if (report["rawEventCount"] !== events.length)
    throw new ContractError("raw event count does not match retained timing events");
  const recomputedSamples = collectScenarioSpanSamples(contract, scenarios);
  if (!deepEqual(report["latencySamples"], recomputedSamples)) {
    throw new ContractError("latency samples do not match recomputed raw events");
  }
  const recomputedSummaries = summarizeSpanSamples(recomputedSamples);
  if (!deepEqual(report["latencySummaries"], recomputedSummaries)) {
    throw new ContractError("latency summaries do not match recomputed samples");
  }
  const measuredSpans = new Set(
    recomputedSamples
      .map((sample) => sample["span"])
      .filter((span): span is string => typeof span === "string"),
  );
  const unmeasuredSpans = [...EXPECTED_SPANS].filter((span) => !measuredSpans.has(span)).sort();
  if (!deepEqual(report["unmeasuredLatencySpans"], unmeasuredSpans)) {
    throw new ContractError("unmeasured latency spans are not derived from raw events");
  }
  if (!setEquals(measuredSpans, EXPECTED_SPANS)) {
    throw new ContractError("complete CURRENT evidence must measure all seven latency spans");
  }
  const recomputedThresholds = measureRelativeThresholds(recomputedSummaries, recomputedSamples);
  if (!deepEqual(report["relativeThresholdMeasurements"], recomputedThresholds)) {
    throw new ContractError("relative thresholds do not match recomputed latency summaries");
  }
  if (Object.values(recomputedThresholds).some((result) => result["status"] !== "measured")) {
    throw new ContractError("complete CURRENT evidence requires finite relative denominators");
  }
  const thresholds = data(contract["relativeThresholds"], "contract relative thresholds");
  const outputFloodThreshold = data(thresholds["outputFloodCtrlC"], "output-flood threshold");
  const requiredOracle = outputFloodThreshold["requiresCorrectnessOracle"];
  if (
    typeof requiredOracle === "string" &&
    recomputedOracles[requiredOracle]?.["status"] !== "passed"
  ) {
    throw new ContractError("output-flood threshold requires its correctness oracle");
  }
  const expectedFailures = currentFailures(recomputedOracles, recomputedThresholds);
  if (!deepEqual(report["currentFailures"], expectedFailures)) {
    throw new ContractError("currentFailures is not derived from recomputed gates");
  }
  if (report["deadlineIsSlo"] !== false)
    throw new ContractError("the scenario deadline must never be reported as a latency SLO");
}

export function matrixCells(contract: Data): Data[] {
  const matrix = data(contract["matrix"], "contract matrix");
  const browserRtts = values(matrix["browserCloudRttMs"], "Browser/Cloud RTTs");
  const hostRtts = values(matrix["cloudHostRttMs"], "Cloud/Host RTTs");
  const cells: Data[] = [];
  for (const browserCloudRttMs of browserRtts) {
    for (const cloudHostRttMs of hostRtts) {
      cells.push({
        browserCloudRttMs,
        cloudHostRttMs,
        networkFault: "none",
        loadAndRecovery: "steady",
        lifecycleAndReplacement: "none",
      });
    }
  }
  for (const networkFault of ["jitter", "disconnect", "reconnect"]) {
    cells.push({
      browserCloudRttMs: 100,
      cloudHostRttMs: 100,
      networkFault,
      loadAndRecovery: "steady",
      lifecycleAndReplacement: "none",
    });
  }
  for (const loadAndRecovery of ["output-flood", "cold-attach"]) {
    cells.push({
      browserCloudRttMs: 100,
      cloudHostRttMs: 100,
      networkFault: "none",
      loadAndRecovery,
      lifecycleAndReplacement: "none",
    });
  }
  for (const lifecycleAndReplacement of ["do-hibernation", "host-relay-replacement"]) {
    cells.push({
      browserCloudRttMs: 100,
      cloudHostRttMs: 100,
      networkFault: "none",
      loadAndRecovery: "steady",
      lifecycleAndReplacement,
    });
  }
  return cells;
}

function matrixKey(cell: Data): string {
  return stringifyJson([
    cell["browserCloudRttMs"],
    cell["cloudHostRttMs"],
    cell["networkFault"],
    cell["loadAndRecovery"],
    cell["lifecycleAndReplacement"],
  ]);
}

export function scenarioMatrixCells(scenario: Data): Data[] {
  const profile = data(scenario["appliedProfile"], "scenario applied network profile");
  const browserCloudRttMs = integerValue(profile["browserCloudRttMs"]);
  const cloudHostRttMs = integerValue(profile["cloudHostRttMs"]);
  const networkFault = profile["networkFault"] ?? "none";
  if (browserCloudRttMs === null || cloudHostRttMs === null) {
    throw new ContractError("scenario RTT profile must use integer milliseconds");
  }
  if (networkFault !== "none" && networkFault !== "jitter") {
    throw new ContractError("ordinary scenarios support only none or jitter network profiles");
  }
  const base = { browserCloudRttMs, cloudHostRttMs, lifecycleAndReplacement: "none" };
  if (scenario["variant"] === "steady")
    return [{ ...base, networkFault, loadAndRecovery: "steady" }];
  if (scenario["variant"] === "output-flood")
    return [{ ...base, networkFault: "none", loadAndRecovery: "output-flood" }];
  if (scenario["variant"] === "correctness-faults") {
    return [
      { ...base, networkFault: "disconnect", loadAndRecovery: "steady" },
      { ...base, networkFault: "reconnect", loadAndRecovery: "steady" },
      { ...base, networkFault: "none", loadAndRecovery: "cold-attach" },
    ];
  }
  return [];
}

export function coverageForScenarios(contract: Data, scenarios: Data[]): Data[] {
  const measured = new Set(
    scenarios
      .filter((scenario) => scenario["status"] === "measured")
      .flatMap(scenarioMatrixCells)
      .map(matrixKey),
  );
  return matrixCells(contract).map((cell) => {
    if (
      cell["lifecycleAndReplacement"] === "do-hibernation" ||
      cell["lifecycleAndReplacement"] === "host-relay-replacement"
    ) {
      return {
        ...cell,
        status: "requires-staging",
        reason: "real infrastructure lifecycle evidence is collected at staging",
      };
    }
    if (measured.has(matrixKey(cell))) return { ...cell, status: "measured" };
    return {
      ...cell,
      status: "not-run",
      reason: "no merged scenario exercised this exact matrix cell",
    };
  });
}

export function currentFailures(
  oracleResults: Record<string, Data>,
  thresholdMeasurements: Record<string, Data>,
): Data[] {
  const failures: Data[] = [];
  for (const gate of Object.keys(oracleResults).sort()) {
    const result = oracleResults[gate]!;
    if (result["status"] !== "passed") {
      failures.push({
        gate,
        status: result["status"],
        reason: result["reason"] ?? "one or more measured violations were observed",
      });
    }
  }
  for (const thresholdId of Object.keys(thresholdMeasurements).sort()) {
    const result = thresholdMeasurements[thresholdId]!;
    if (result["status"] !== "measured") {
      failures.push({
        gate: `relative-threshold/${thresholdId}`,
        status: result["status"],
        reason: result["reason"] ?? "relative threshold is unavailable",
      });
    }
  }
  return failures;
}

export function main(): void {
  const contract = loadJson(CONTRACT_PATH);
  validateContract(contract);
  let baseline = "absent";
  try {
    statSync(BASELINE_PATH);
    validateReport(loadReportBundle(BASELINE_PATH), contract);
    baseline = "valid";
  } catch (error: unknown) {
    if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
  }
  process.stdout.write(
    `${stringifyJson({ contract: "valid", contractSha256: canonicalSha256(contract), matrixCells: matrixCells(contract).length, baseline })}\n`,
  );
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    main();
  } catch (error: unknown) {
    process.stderr.write(
      `${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`,
    );
    process.exitCode = 1;
  }
}
