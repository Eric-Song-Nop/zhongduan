#!/usr/bin/env node

/** Focused, fail-closed E1 Browser hot-path candidate evidence. */

import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { dirname, extname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { gzipSync, gunzipSync } from "node:zlib";
import { INPUT_QUEUE_CONTRACT } from "../apps/terminal-cloud/src/browser/input-intent-ledger.ts";
import {
  BASELINE_PATH,
  CONTRACT_PATH as E0_CONTRACT_PATH,
  ContractError,
  canonicalSha256,
  collectScenarioSpanSamples,
  isData,
  loadJson,
  loadReportBundle,
  parseJson,
  percentile,
  stringifyJson,
  validateReport,
  validateScenarioReport,
  type Data,
} from "./e0-terminal-journey.ts";

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
export const E1_BROWSER_LATENCY_CONTRACT_PATH = join(
  ROOT,
  "benchmarks",
  "browser-input-admission",
  "contract.json",
);
export const E1_BROWSER_LATENCY_CANDIDATE_PATH = join(
  ROOT,
  "benchmarks",
  "browser-input-admission",
  "e1-candidate.json",
);
const SCHEMA = "zhongduan-e1-browser-latency-candidate-v1";
const BUNDLE_FORMAT = "canonical-jsonl";
const BUNDLE_COMPRESSION = "gzip-9-mtime-0";
const MAX_COMPRESSED_BYTES = 4 * 1024 * 1024;
const MAX_UNCOMPRESSED_BYTES = 32 * 1024 * 1024;
const EXACT_GIT_OID = /^[0-9a-f]{40}$/u;

function data(value: unknown, label: string): Data {
  if (!isData(value)) throw new ContractError(`${label} must be an object`);
  return value;
}

function records(value: unknown, label: string): Data[] {
  if (!Array.isArray(value) || value.some((item) => !isData(item))) {
    throw new ContractError(`${label} must be an array of objects`);
  }
  return value as Data[];
}

function integer(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new ContractError(`${label} must be a safe integer`);
  }
  return value;
}

function finite(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new ContractError(`${label} must be finite`);
  }
  return value;
}

function round(value: number, digits: number): number {
  const scale = 10 ** digits;
  return Math.round(value * scale) / scale;
}

function sha256Bytes(value: Uint8Array): string {
  return createHash("sha256").update(value).digest("hex");
}

function expectedContract(e0Contract: Data): Data {
  const supportedLoad = data(
    data(e0Contract["workload"], "E0 workload")["supportedLoad"],
    "E0 supported load",
  );
  return {
    schemaVersion: "zhongduan-e1-browser-latency-contract-v1",
    stage: "E1",
    status: "frozen-relative-fail-closed",
    e0ContractSha256: canonicalSha256(e0Contract),
    workload: {
      e0Variant: "steady",
      candidateScenarioCount: 5,
      samplesPerScenario: 24,
      warmupSamplesPerScenario: 4,
      candidateSeeds: [450, 451, 452, 453, 454],
      candidateProfile: {
        browserCloudRttMs: 100,
        cloudHostRttMs: 100,
        jitterMs: 0,
        networkFault: "none",
        networkImplementation: "two independent Node.js HTTP/WebSocket userspace proxies",
      },
      e0SupportedLoad: supportedLoad,
      e1HardLimits: { ...INPUT_QUEUE_CONTRACT },
    },
    measurement: {
      span: "browser-keydown-to-send-decision",
      startEvent: "browser.keydown",
      endEvent: "browser.send-decision",
      statistic: "p99-nearest-rank",
      durationResolutionMs: 0.1,
      currentReport: "../terminal-journey/current-baseline.json",
      currentScenarioSelector: {
        variant: "steady",
        reason:
          "Both span boundaries precede injected link delay and the steady journey settles each input before the next sample. All source-controlled steady cells therefore form the least noisy CURRENT Browser population.",
      },
      expectedCurrentScenarioCount: 17,
      minimumCurrentSamples: 408,
      minimumCandidateSamples: 120,
      candidateComparison: "quantized-candidate-p99/quantized-current-p99",
      maximumRegressionRatioToCurrent: 1,
    },
    artifactLimits: {
      maximumCompressedBytes: MAX_COMPRESSED_BYTES,
      maximumUncompressedBytes: MAX_UNCOMPRESSED_BYTES,
    },
    evidenceBoundary: {
      measured:
        "Real Chromium keydown through the E1 Browser InputDispatcher send decision, using the E0 steady raw-semantic-PTY journey.",
      withinE0SupportedLoad: true,
      saturatesE0SupportedLoad: false,
      saturatesE1HardLimits: false,
      hardLimitEvidence:
        "E1 invariant, overload, bytes/count/pending, and deadline tests; not this latency population.",
      simulated: ["Browser/Cloud RTT", "Cloud/Host RTT"],
      notClaimed: [
        "Cloudflare edge latency",
        "queue-saturation throughput",
        "E2 Cloud input-lane isolation",
        "E3 Host contiguous-prefix behavior",
      ],
    },
  };
}

export function validateE1BrowserLatencyContract(contract: Data, e0Contract: Data): void {
  const expected = expectedContract(e0Contract);
  if (!isDeepStrictEqual(contract, expected)) {
    throw new ContractError("E1 Browser latency contract differs from its frozen executable shape");
  }
  const spans = records(e0Contract["latencySpans"], "E0 latency spans");
  const measurement = data(contract["measurement"], "E1 measurement");
  const span = spans.find((item) => item["id"] === measurement["span"]);
  if (
    span === undefined ||
    span["startEvent"] !== measurement["startEvent"] ||
    span["endEvent"] !== measurement["endEvent"]
  ) {
    throw new ContractError("E1 Browser span is not the exact frozen E0 span");
  }
}

function scenarioSeed(scenario: Data): number {
  return integer(
    data(scenario["appliedProfile"], "scenario appliedProfile")["seed"],
    "scenario seed",
  );
}

function orderedCandidateScenarios(contract: Data, e0Contract: Data, input: Data[]): Data[] {
  const workload = data(contract["workload"], "E1 workload");
  const expectedSeeds = workload["candidateSeeds"];
  if (!Array.isArray(expectedSeeds) || expectedSeeds.some((seed) => !Number.isSafeInteger(seed))) {
    throw new ContractError("candidateSeeds must contain safe integers");
  }
  if (input.length !== workload["candidateScenarioCount"]) {
    throw new ContractError("candidate evidence has the wrong scenario count");
  }
  const expectedProfile = data(workload["candidateProfile"], "candidate profile");
  const ordered = [...input].sort((left, right) => scenarioSeed(left) - scenarioSeed(right));
  const observedSeeds: number[] = [];
  const digests = new Set<string>();
  let sourceRevision: unknown;
  let sourceTreeGitOid: unknown;
  let environment: unknown;
  for (const scenario of ordered) {
    validateScenarioReport(scenario, e0Contract);
    if (scenario["variant"] !== workload["e0Variant"]) {
      throw new ContractError("candidate scenario is not the frozen E0 steady workload");
    }
    if (
      scenario["samples"] !== workload["samplesPerScenario"] ||
      scenario["warmups"] !== workload["warmupSamplesPerScenario"]
    ) {
      throw new ContractError("candidate scenario has the wrong measured or warmup population");
    }
    const applied = data(scenario["appliedProfile"], "scenario appliedProfile");
    const seed = scenarioSeed(scenario);
    observedSeeds.push(seed);
    const profileWithoutSeed = Object.fromEntries(
      Object.entries(applied).filter(([field]) => field !== "seed"),
    );
    if (!isDeepStrictEqual(profileWithoutSeed, expectedProfile)) {
      throw new ContractError("candidate scenario does not use the frozen network profile");
    }
    if (scenario["sourceTreeDirty"] !== false) {
      throw new ContractError("candidate scenarios must come from a clean source tree");
    }
    if (
      typeof scenario["sourceRevision"] !== "string" ||
      !EXACT_GIT_OID.test(scenario["sourceRevision"]) ||
      typeof scenario["sourceTreeGitOid"] !== "string" ||
      !EXACT_GIT_OID.test(scenario["sourceTreeGitOid"])
    ) {
      throw new ContractError("candidate scenario lacks exact source Git identities");
    }
    sourceRevision ??= scenario["sourceRevision"];
    sourceTreeGitOid ??= scenario["sourceTreeGitOid"];
    environment ??= scenario["environment"];
    if (
      scenario["sourceRevision"] !== sourceRevision ||
      scenario["sourceTreeGitOid"] !== sourceTreeGitOid ||
      !isDeepStrictEqual(scenario["environment"], environment)
    ) {
      throw new ContractError("candidate scenarios do not share one source tree and environment");
    }
    const digest = canonicalSha256(scenario);
    if (digests.has(digest)) throw new ContractError("candidate archive repeats a scenario");
    digests.add(digest);
  }
  if (!isDeepStrictEqual(observedSeeds, expectedSeeds)) {
    throw new ContractError("candidate archive does not contain each frozen seed exactly once");
  }
  return ordered;
}

function spanSamples(contract: Data, e0Contract: Data, scenarios: Data[], label: string): number[] {
  const measurement = data(contract["measurement"], "E1 measurement");
  const span = measurement["span"];
  const samples = collectScenarioSpanSamples(e0Contract, scenarios).filter(
    (sample) => sample["span"] === span,
  );
  const durations = samples.map((sample) => finite(sample["durationMs"], `${label} duration`));
  if (durations.some((duration) => duration < 0)) {
    throw new ContractError(`${label} contains a negative duration`);
  }
  return durations;
}

function measurementSummary(span: unknown, values: number[], resolutionMs: number): Data {
  if (typeof span !== "string" || values.length === 0) {
    throw new ContractError("Browser latency measurement has no samples");
  }
  if (resolutionMs <= 0) throw new ContractError("Browser duration resolution must be positive");
  const quantized = values.map((value) => round(value / resolutionMs, 0) * resolutionMs);
  return {
    span,
    statistic: "p99-nearest-rank",
    durationResolutionMs: resolutionMs,
    sampleCount: values.length,
    p50Ms: round(percentile(values, 0.5), 6),
    p95Ms: round(percentile(values, 0.95), 6),
    p99Ms: round(percentile(values, 0.99), 6),
    maxMs: round(Math.max(...values), 6),
    comparisonP99Ms: round(percentile(quantized, 0.99), 6),
  };
}

function sourceMetadata(scenarios: Data[]): Data {
  const first = scenarios[0];
  if (first === undefined) throw new ContractError("candidate evidence has no scenarios");
  const generated = scenarios.map((scenario) => scenario["generatedAt"]);
  if (generated.some((value) => typeof value !== "string")) {
    throw new ContractError("candidate scenarios lack generation timestamps");
  }
  return {
    sourceRevision: first["sourceRevision"],
    sourceTreeGitOid: first["sourceTreeGitOid"],
    sourceTreeDirty: false,
    generatedAt: (generated as string[]).sort().at(-1),
    environment: first["environment"],
  };
}

export function assembleE1BrowserLatencyEvidence(
  contract: Data,
  e0Contract: Data,
  currentReport: Data,
  inputScenarios: Data[],
): { report: Data; scenarios: Data[] } {
  validateE1BrowserLatencyContract(contract, e0Contract);
  validateReport(currentReport, e0Contract);
  const scenarios = orderedCandidateScenarios(contract, e0Contract, inputScenarios);
  const measurement = data(contract["measurement"], "E1 measurement");
  const currentScenarios = records(currentReport["scenarioReports"], "CURRENT scenarios").filter(
    (scenario) => scenario["variant"] === "steady",
  );
  if (currentScenarios.length !== measurement["expectedCurrentScenarioCount"]) {
    throw new ContractError("CURRENT does not contain the frozen steady scenario population");
  }
  const currentValues = spanSamples(contract, e0Contract, currentScenarios, "CURRENT");
  const candidateValues = spanSamples(contract, e0Contract, scenarios, "CANDIDATE");
  if (
    currentValues.length < integer(measurement["minimumCurrentSamples"], "minimum CURRENT samples")
  ) {
    throw new ContractError("CURRENT Browser population is below the frozen minimum");
  }
  if (
    candidateValues.length !==
    integer(measurement["minimumCandidateSamples"], "minimum CANDIDATE samples")
  ) {
    throw new ContractError("CANDIDATE Browser population is not the exact frozen population");
  }
  const durationResolutionMs = finite(
    measurement["durationResolutionMs"],
    "Browser duration resolution",
  );
  const currentMeasurement = measurementSummary(
    measurement["span"],
    currentValues,
    durationResolutionMs,
  );
  const candidateMeasurement = measurementSummary(
    measurement["span"],
    candidateValues,
    durationResolutionMs,
  );
  const currentP99Ms = finite(currentMeasurement["comparisonP99Ms"], "CURRENT p99");
  const candidateP99Ms = finite(candidateMeasurement["comparisonP99Ms"], "CANDIDATE p99");
  if (currentP99Ms <= 0) throw new ContractError("CURRENT p99 must be finite and non-zero");
  const ratio = round(candidateP99Ms / currentP99Ms, 9);
  const maximumRatio = finite(
    measurement["maximumRegressionRatioToCurrent"],
    "maximum regression ratio",
  );
  const status = ratio <= maximumRatio ? "passed" : "failed";
  const failures =
    status === "passed"
      ? []
      : [
          {
            gate: "browser-keydown-to-send-decision/p99-versus-current",
            status: "failed",
            reason: "CANDIDATE Browser p99 regressed the frozen CURRENT p99",
          },
        ];
  const firstCurrent = currentScenarios[0];
  if (firstCurrent === undefined) throw new ContractError("CURRENT steady population is empty");
  return {
    scenarios,
    report: {
      schemaVersion: SCHEMA,
      contractSha256: canonicalSha256(contract),
      e0ContractSha256: canonicalSha256(e0Contract),
      candidateStatus: "complete",
      status,
      ...sourceMetadata(scenarios),
      evidenceBoundary: contract["evidenceBoundary"],
      workload: contract["workload"],
      current: {
        reportPath: measurement["currentReport"],
        reconstructedReportSha256: canonicalSha256(currentReport),
        sourceRevision: currentReport["sourceRevision"],
        sourceTreeGitOid: currentReport["sourceTreeGitOid"],
        environment: currentReport["environment"],
        selector: measurement["currentScenarioSelector"],
        scenarioCount: currentScenarios.length,
        measurement: currentMeasurement,
      },
      candidate: {
        scenarioCount: scenarios.length,
        scenarioSha256: scenarios.map(canonicalSha256),
        rawEventCount: scenarios.reduce(
          (sum, scenario) => sum + integer(scenario["rawEventCount"], "scenario rawEventCount"),
          0,
        ),
        profile: data(contract["workload"], "E1 workload")["candidateProfile"],
        seeds: scenarios.map(scenarioSeed),
        measurement: candidateMeasurement,
      },
      comparison: {
        operation: measurement["candidateComparison"],
        currentRawP99Ms: currentMeasurement["p99Ms"],
        candidateRawP99Ms: candidateMeasurement["p99Ms"],
        currentP99Ms,
        candidateP99Ms,
        value: ratio,
        maximumRegressionRatioToCurrent: maximumRatio,
        status,
      },
      failures,
    },
  };
}

function archivePath(reportPath: string): string {
  const extension = extname(reportPath);
  return `${reportPath.slice(0, extension.length === 0 ? undefined : -extension.length)}.scenarios.jsonl.gz`;
}

function scenarioJsonl(scenarios: Data[]): Buffer {
  return Buffer.from(
    `${scenarios.map((scenario) => stringifyJson(scenario)).join("\n")}\n`,
    "utf8",
  );
}

export function writeE1BrowserLatencyEvidence(
  reportPath: string,
  report: Data,
  scenarios: Data[],
): Data {
  const raw = scenarioJsonl(scenarios);
  const compressed = gzipSync(raw, { level: 9 });
  if (compressed.length > MAX_COMPRESSED_BYTES || raw.length > MAX_UNCOMPRESSED_BYTES) {
    throw new ContractError("E1 Browser scenario archive exceeds its hard byte limit");
  }
  const output: Data = {
    ...report,
    scenarioArchive: {
      path: archivePath(reportPath).split("/").at(-1),
      format: BUNDLE_FORMAT,
      compression: BUNDLE_COMPRESSION,
      scenarioCount: scenarios.length,
      compressedBytes: compressed.length,
      compressedSha256: sha256Bytes(compressed),
      uncompressedBytes: raw.length,
      uncompressedSha256: sha256Bytes(raw),
    },
  };
  mkdirSync(dirname(resolve(reportPath)), { recursive: true });
  writeFileSync(archivePath(reportPath), compressed);
  writeFileSync(reportPath, `${stringifyJson(output, true)}\n`, "utf8");
  return output;
}

export function loadE1BrowserLatencyEvidence(reportPath: string): {
  report: Data;
  scenarios: Data[];
} {
  const report = loadJson(reportPath);
  const manifest = data(report["scenarioArchive"], "E1 scenario archive manifest");
  const name = manifest["path"];
  if (typeof name !== "string" || name.length === 0 || name.includes("/") || name.includes("\\")) {
    throw new ContractError("E1 scenario archive path must be a local basename");
  }
  if (manifest["format"] !== BUNDLE_FORMAT || manifest["compression"] !== BUNDLE_COMPRESSION) {
    throw new ContractError("E1 scenario archive uses an unsupported format");
  }
  const compressedBytes = integer(manifest["compressedBytes"], "compressed archive bytes");
  const uncompressedBytes = integer(manifest["uncompressedBytes"], "uncompressed archive bytes");
  const scenarioCount = integer(manifest["scenarioCount"], "archive scenario count");
  if (
    compressedBytes <= 0 ||
    compressedBytes > MAX_COMPRESSED_BYTES ||
    uncompressedBytes <= 0 ||
    uncompressedBytes > MAX_UNCOMPRESSED_BYTES ||
    scenarioCount <= 0
  ) {
    throw new ContractError("E1 scenario archive manifest exceeds its hard bounds");
  }
  const path = join(dirname(resolve(reportPath)), name);
  if (statSync(path).size !== compressedBytes) {
    throw new ContractError("E1 scenario archive compressed size does not match its manifest");
  }
  const compressed = readFileSync(path);
  if (sha256Bytes(compressed) !== manifest["compressedSha256"]) {
    throw new ContractError("E1 scenario archive compressed digest does not match its manifest");
  }
  let raw: Buffer;
  try {
    raw = gunzipSync(compressed, { maxOutputLength: MAX_UNCOMPRESSED_BYTES + 1 });
  } catch (error: unknown) {
    throw new ContractError(
      `cannot decompress E1 scenario archive: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  if (raw.length !== uncompressedBytes || sha256Bytes(raw) !== manifest["uncompressedSha256"]) {
    throw new ContractError("E1 scenario archive raw bytes do not match its manifest");
  }
  const text = raw.toString("utf8");
  if (!text.endsWith("\n")) throw new ContractError("E1 scenario archive must end in a newline");
  let scenarios: Data[];
  try {
    scenarios = text
      .slice(0, -1)
      .split("\n")
      .map((line) => data(parseJson(line), "E1 scenario archive record"));
  } catch (error: unknown) {
    if (error instanceof ContractError) throw error;
    throw new ContractError(
      `E1 scenario archive contains invalid JSONL: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  if (scenarios.length !== scenarioCount || !scenarioJsonl(scenarios).equals(raw)) {
    throw new ContractError("E1 scenario archive count or canonical encoding is invalid");
  }
  return { report, scenarios };
}

function verifySourceProvenance(report: Data): void {
  const revision = report["sourceRevision"];
  const tree = report["sourceTreeGitOid"];
  if (
    typeof revision !== "string" ||
    !EXACT_GIT_OID.test(revision) ||
    typeof tree !== "string" ||
    !EXACT_GIT_OID.test(tree)
  ) {
    throw new ContractError("E1 evidence lacks exact source Git identities");
  }
  let actualTree: string;
  try {
    actualTree = execFileSync("git", ["rev-parse", `${revision}^{tree}`], {
      cwd: ROOT,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    }).trim();
    execFileSync("git", ["merge-base", "--is-ancestor", revision, "HEAD"], {
      cwd: ROOT,
      stdio: "ignore",
    });
  } catch {
    throw new ContractError("E1 measurement commit is not reachable from the checked Git head");
  }
  if (actualTree !== tree) {
    throw new ContractError("E1 measurement tree does not belong to its claimed commit");
  }
}

export function validateE1BrowserLatencyEvidence(
  report: Data,
  scenarios: Data[],
  contract: Data,
  e0Contract: Data,
  currentReport: Data,
  options: { verifyGit?: boolean } = {},
): void {
  const manifest = report["scenarioArchive"];
  const core = Object.fromEntries(
    Object.entries(report).filter(([field]) => field !== "scenarioArchive"),
  );
  const expected = assembleE1BrowserLatencyEvidence(
    contract,
    e0Contract,
    currentReport,
    scenarios,
  ).report;
  if (!isDeepStrictEqual(core, expected)) {
    throw new ContractError(
      "E1 Browser evidence is not derived from its raw scenarios and CURRENT",
    );
  }
  if (manifest !== undefined && !isData(manifest)) {
    throw new ContractError("E1 scenario archive manifest must be an object");
  }
  if (options.verifyGit === true) verifySourceProvenance(report);
}

export function assertE1BrowserLatencyGate(report: Data): void {
  if (report["candidateStatus"] !== "complete" || report["status"] !== "passed") {
    throw new ContractError("E1 Browser latency candidate did not pass its frozen relative gate");
  }
}

interface CliArgs {
  report: string;
  mergeScenarios: string[] | null;
}

function parseArgs(argv: string[]): CliArgs {
  const args: CliArgs = { report: E1_BROWSER_LATENCY_CANDIDATE_PATH, mergeScenarios: null };
  const take = (index: number, option: string): string => {
    const value = argv[index + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new ContractError(`${option} requires a value`);
    }
    return value;
  };
  for (let index = 0; index < argv.length; index += 1) {
    const option = argv[index];
    if (option === "--report") args.report = take(index++, option);
    else if (option === "--merge-scenarios") {
      const paths: string[] = [];
      while (index + 1 < argv.length && !argv[index + 1]!.startsWith("--")) {
        paths.push(argv[++index]!);
      }
      if (paths.length === 0) throw new ContractError("--merge-scenarios requires input files");
      args.mergeScenarios = paths;
    } else if (option === "--help" || option === "-h") {
      process.stdout.write(
        "Usage: node scripts/e1-browser-latency.ts [--merge-scenarios FILE...] [--report FILE]\n",
      );
      process.exit(0);
    } else {
      throw new ContractError(`unknown argument ${String(option)}`);
    }
  }
  return args;
}

export function main(argv = process.argv.slice(2)): void {
  const args = parseArgs(argv);
  const contract = loadJson(E1_BROWSER_LATENCY_CONTRACT_PATH);
  const e0Contract = loadJson(E0_CONTRACT_PATH);
  const currentReport = loadReportBundle(BASELINE_PATH);
  if (args.mergeScenarios !== null) {
    const assembled = assembleE1BrowserLatencyEvidence(
      contract,
      e0Contract,
      currentReport,
      args.mergeScenarios.map(loadJson),
    );
    writeE1BrowserLatencyEvidence(args.report, assembled.report, assembled.scenarios);
  }
  const loaded = loadE1BrowserLatencyEvidence(args.report);
  validateE1BrowserLatencyEvidence(
    loaded.report,
    loaded.scenarios,
    contract,
    e0Contract,
    currentReport,
    { verifyGit: true },
  );
  assertE1BrowserLatencyGate(loaded.report);
  process.stdout.write(
    `${stringifyJson({ report: args.report, status: loaded.report["status"], comparison: loaded.report["comparison"] })}\n`,
  );
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    main();
  } catch (error: unknown) {
    process.stderr.write(
      `E1 Browser latency evidence failed: ${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`,
    );
    process.exitCode = 1;
  }
}
