import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  BASELINE_PATH,
  CONTRACT_PATH as E0_CONTRACT_PATH,
  ContractError,
  loadJson,
  loadReportBundle,
  type Data,
} from "./e0-terminal-journey.ts";
import {
  E1_BROWSER_LATENCY_CONTRACT_PATH,
  assembleE1BrowserLatencyEvidence,
  assertE1BrowserLatencyGate,
  loadE1BrowserLatencyEvidence,
  validateE1BrowserLatencyContract,
  validateE1BrowserLatencyEvidence,
  writeE1BrowserLatencyEvidence,
} from "./e1-browser-latency.ts";

const e0Contract = loadJson(E0_CONTRACT_PATH);
const contract = loadJson(E1_BROWSER_LATENCY_CONTRACT_PATH);
const current = loadReportBundle(BASELINE_PATH);

function clone<T>(value: T): T {
  return structuredClone(value);
}

function fixtureScenarios(): Data[] {
  const source = (current["scenarioReports"] as Data[]).find((scenario) => {
    const profile = scenario["appliedProfile"] as Data;
    return (
      scenario["variant"] === "steady" &&
      profile["browserCloudRttMs"] === 100 &&
      profile["cloudHostRttMs"] === 100 &&
      profile["jitterMs"] === 0 &&
      profile["networkFault"] === "none"
    );
  });
  if (source === undefined) throw new Error("checked E0 baseline lacks the fixture steady cell");
  return [450, 451, 452, 453, 454].map((seed, index) => {
    const scenario = clone(source);
    (scenario["appliedProfile"] as Data)["seed"] = seed;
    (scenario["appliedProfile"] as Data)["networkImplementation"] =
      "two independent Node.js HTTP/WebSocket userspace proxies";
    scenario["generatedAt"] = `2026-09-01T00:00:0${index}Z`;
    return scenario;
  });
}

function passingEvidence(): { report: Data; scenarios: Data[] } {
  return assembleE1BrowserLatencyEvidence(contract, e0Contract, current, fixtureScenarios());
}

describe("E1 Browser latency contract", () => {
  it("freezes the E0 span, supported load, E1 limits, sample population, and ratio", () => {
    expect(() => validateE1BrowserLatencyContract(contract, e0Contract)).not.toThrow();
    expect((contract["measurement"] as Data)["minimumCandidateSamples"]).toBe(120);
    expect((contract["measurement"] as Data)["maximumRegressionRatioToCurrent"]).toBe(1);
    expect((contract["evidenceBoundary"] as Data)["saturatesE1HardLimits"]).toBe(false);
  });

  it("derives the CURRENT denominator and passing candidate from raw E0 events", () => {
    const evidence = passingEvidence();
    const currentMeasurement = (evidence.report["current"] as Data)["measurement"] as Data;
    const candidateMeasurement = (evidence.report["candidate"] as Data)["measurement"] as Data;
    expect(currentMeasurement).toMatchObject({ sampleCount: 408, p99Ms: 0.4 });
    expect(candidateMeasurement).toMatchObject({ sampleCount: 120, p99Ms: 0.3001 });
    expect(evidence.report["status"]).toBe("passed");
    expect(() => assertE1BrowserLatencyGate(evidence.report)).not.toThrow();
  });

  it("rejects dirty, wrong-profile, repeated, and mixed-source candidates", () => {
    const mutations: Array<(scenarios: Data[]) => void> = [
      (scenarios) => {
        scenarios[0]!["sourceTreeDirty"] = true;
      },
      (scenarios) => {
        (scenarios[0]!["appliedProfile"] as Data)["browserCloudRttMs"] = 20;
      },
      (scenarios) => {
        scenarios[1] = clone(scenarios[0]!);
      },
      (scenarios) => {
        scenarios[0]!["sourceRevision"] = "a".repeat(40);
      },
    ];
    for (const mutate of mutations) {
      const scenarios = fixtureScenarios();
      mutate(scenarios);
      expect(() =>
        assembleE1BrowserLatencyEvidence(contract, e0Contract, current, scenarios),
      ).toThrow(ContractError);
    }
  });

  it("accepts an honestly red report but keeps the merge gate closed", () => {
    const scenarios = fixtureScenarios();
    for (const scenario of scenarios) {
      for (const event of scenario["events"] as Data[]) {
        if (event["name"] !== "browser.send-decision") continue;
        const at = event["atUnixNs"];
        if (typeof at === "bigint") event["atUnixNs"] = at + 2_000_000n;
        else if (typeof at === "number") event["atUnixNs"] = at + 2_000_000;
      }
    }
    const evidence = assembleE1BrowserLatencyEvidence(contract, e0Contract, current, scenarios);
    expect(evidence.report["status"]).toBe("failed");
    expect(() =>
      validateE1BrowserLatencyEvidence(
        evidence.report,
        evidence.scenarios,
        contract,
        e0Contract,
        current,
      ),
    ).not.toThrow();
    expect(() => assertE1BrowserLatencyGate(evidence.report)).toThrow(ContractError);
  });

  it("round-trips the bounded pair and rejects archive or summary tampering", () => {
    const directory = mkdtempSync(join(tmpdir(), "e1-browser-latency-test-"));
    const reportPath = join(directory, "candidate.json");
    try {
      const evidence = passingEvidence();
      writeE1BrowserLatencyEvidence(reportPath, evidence.report, evidence.scenarios);
      const loaded = loadE1BrowserLatencyEvidence(reportPath);
      expect(() =>
        validateE1BrowserLatencyEvidence(
          loaded.report,
          loaded.scenarios,
          contract,
          e0Contract,
          current,
        ),
      ).not.toThrow();

      const forged = clone(loaded.report);
      ((forged["candidate"] as Data)["measurement"] as Data)["p99Ms"] = 0;
      expect(() =>
        validateE1BrowserLatencyEvidence(forged, loaded.scenarios, contract, e0Contract, current),
      ).toThrow(ContractError);

      const archivePath = join(directory, "candidate.scenarios.jsonl.gz");
      const bytes = readFileSync(archivePath);
      bytes[Math.floor(bytes.length / 2)]! ^= 1;
      writeFileSync(archivePath, bytes);
      expect(() => loadE1BrowserLatencyEvidence(reportPath)).toThrow(ContractError);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  }, 30_000);
});
