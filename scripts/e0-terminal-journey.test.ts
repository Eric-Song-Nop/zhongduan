import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  BASELINE_PATH,
  CONTRACT_PATH,
  ContractError,
  EXPECTED_ORACLES,
  EXPECTED_SPANS,
  EXPECTED_VARIANTS,
  assembleCandidateReport,
  buildE4bDecision,
  canonicalSha256,
  collectScenarioSpanSamples,
  collectSpanSamples,
  evaluateOracles,
  loadJson,
  loadReportBundle,
  matrixCells,
  measureRelativeThresholds,
  parseJson,
  summarizeSpanSamples,
  validateContract,
  validateReport,
  validateScenarioReport,
  writeReportBundle,
  type Data,
} from "./e0-terminal-journey.ts";
import { FixtureProtocolError, FixtureState } from "./e0-terminal-fixture.ts";
import { JourneyError, TraceStore, parseArgs } from "./verify-e0-terminal-journey.ts";

const contract = loadJson(CONTRACT_PATH);

function current(): Data {
  return loadReportBundle(BASELINE_PATH);
}

function clone<T>(value: T): T {
  return structuredClone(value);
}

function passingObservations(): Data {
  return {
    intents: [
      {
        consumed: true,
        localIntentId: "intent-1",
        terminalOutcomes: ["deterministic"],
        wireIdentity: "fence/epoch/1",
        ptyEffectCount: 1,
      },
      {
        consumed: true,
        localIntentId: "intent-2",
        terminalOutcomes: ["uncertain"],
        wireIdentity: "fence/epoch/2",
        ptyEffectCount: 0,
        acceptanceUncertaintyInjected: true,
        automaticRetryCount: 0,
      },
    ],
    ctrlC: [{ sampleId: "ctrl-1", outputFlood: true, ptyEffectCount: 1 }],
    writerTransfers: [{ sampleId: "transfer-1", oldWriterSuccessfulEffects: 0 }],
    coldCandidates: [{ sampleId: "cold-1", visibleBeforeValidation: false }],
    authorityComparisons: [
      {
        sampleId: "authority-1",
        normalizedStateEqual: true,
        continuationEqual: true,
        effectsEqual: true,
      },
    ],
    secureInput: [{ sampleId: "secure-1", speculativePresentationCount: 0 }],
  };
}

function candidatePhases(): Data[] {
  const envelope = {
    schemaVersion: 1,
    source: "host-cloud-relay",
    outcome: "ok",
    recordedAtUnixMs: 100,
    measurementStartedAtUnixMs: 0,
    measurementEndedAtUnixMs: 200,
  };
  const phases: Data[] = [
    {
      ...envelope,
      event: "input-actor-queue",
      queueWaitMs: 1,
      scenarioVariant: "snapshot-disabled",
    },
    {
      ...envelope,
      event: "input-actor-queue",
      queueWaitMs: 1,
      scenarioVariant: "snapshot-enabled",
    },
  ];
  for (const phase of [
    "refresh-queue-wait",
    "authority-actor-wait",
    "authority-cut",
    "authority-encode",
    "publisher-total",
    "finalize-install",
  ]) {
    phases.push({
      ...envelope,
      event: "snapshot-refresh",
      phase,
      durationMs: 1,
      scenarioVariant: "snapshot-enabled",
    });
  }
  for (const phase of ["compress", "hash", "upload", "finalize"]) {
    phases.push({
      ...envelope,
      event: "snapshot-publish",
      phase,
      durationMs: 1,
      scenarioVariant: "snapshot-enabled",
    });
  }
  return phases;
}

describe("E0 TypeScript contract and evidence bundle", () => {
  it("freezes the exact roadmap surface", () => {
    expect(() => validateContract(contract)).not.toThrow();
    expect(new Set((contract["oracles"] as Data[]).map((item) => item["id"]))).toEqual(
      EXPECTED_ORACLES,
    );
    expect(new Set((contract["latencySpans"] as Data[]).map((item) => item["id"]))).toEqual(
      EXPECTED_SPANS,
    );
    expect(new Set((contract["workload"] as Data)["requiredVariants"] as string[])).toEqual(
      EXPECTED_VARIANTS,
    );
    expect(matrixCells(contract)).toHaveLength(23);
  });

  it("uses an order-stable TypeScript canonical hash", () => {
    expect(canonicalSha256({ z: 1, nested: { b: 2, a: 1 } })).toBe(
      canonicalSha256({ nested: { a: 1, b: 2 }, z: 1 }),
    );
  });

  it("parses unsafe integer timestamps without changing finite decimals", () => {
    const parsed = parseJson(
      '{"atUnixNs":1770000000000000000,"durationMs":0.016834000000002902,"count":3}',
    ) as Data;
    expect(parsed["atUnixNs"]).toBe(1_770_000_000_000_000_000n);
    expect(parsed["durationMs"]).toBe(0.016834000000002902);
    expect(parsed["count"]).toBe(3);
  });

  it("loads the small checked CURRENT pair and fully validates it", () => {
    expect(statSync(BASELINE_PATH).size).toBeLessThan(40_000);
    const summary = loadJson(BASELINE_PATH);
    for (const field of ["events", "latencySamples", "observations", "scenarioReports"]) {
      expect(summary).not.toHaveProperty(field);
    }
    const report = current();
    expect(report["scenarioReports"] as Data[]).toHaveLength(25);
    expect(report["rawEventCount"]).toBe(82_036);
    expect(report["latencySamples"] as Data[]).toHaveLength(5_964);
    expect(() => validateReport(report, contract)).not.toThrow();
  });

  it("round-trips a bundle and rejects archive tampering", () => {
    const directory = mkdtempSync(join(tmpdir(), "e0-bundle-test-"));
    const path = join(directory, "report.json");
    const report = current();
    try {
      writeReportBundle(path, report);
      expect(canonicalSha256(loadReportBundle(path))).toBe(canonicalSha256(report));
      const archivePath = join(directory, "report.scenarios.jsonl.gz");
      const bytes = readFileSync(archivePath);
      bytes[Math.floor(bytes.length / 2)]! ^= 1;
      writeFileSync(archivePath, bytes);
      expect(() => loadReportBundle(path)).toThrow(ContractError);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  }, 20_000);

  it("recomputes every claimed oracle, summary, threshold, effect and coverage cell", () => {
    const mutations: Array<(report: Data) => void> = [
      (report) =>
        (((report["oracleResults"] as Data)["duplicate-pty-effect"] as Data)["status"] = "failed"),
      (report) => ((report["latencySummaries"] as Data[])[0]!["p99Ms"] = 0),
      (report) =>
        (((report["relativeThresholdMeasurements"] as Data)["cloudBulkIsolation"] as Data)[
          "value"
        ] = 0),
      (report) => ((report["matrixCoverage"] as Data[])[0]!["status"] = "not-run"),
      (report) => {
        const scenario = (report["scenarioReports"] as Data[])[0]!;
        const intent = ((scenario["observations"] as Data)["intents"] as Data[])[0]!;
        intent["ptyEffectCount"] = Number(intent["ptyEffectCount"]) + 1;
      },
    ];
    for (const mutate of mutations) {
      const report = clone(current());
      mutate(report);
      expect(() => validateReport(report, contract)).toThrow(ContractError);
    }
  }, 20_000);

  it("requires snapshot finalization to remain inside retained Host input evidence", () => {
    const snapshotScenario = (current()["scenarioReports"] as Data[]).find(
      (item) => item["variant"] === "snapshot-enabled",
    )!;
    const forged = clone(snapshotScenario);
    const overlap = (forged["workloadEvidence"] as Data)["snapshotInputOverlap"] as Data;
    overlap["snapshotFinalizationAtUnixNs"] = overlap["lastHostReceiveAtUnixNs"];
    expect(() => validateScenarioReport(forged, contract)).toThrow(ContractError);
  });

  it("does not let passive wire inference satisfy declared E1 product-result evidence", () => {
    const correctnessScenario = (current()["scenarioReports"] as Data[]).find(
      (item) => item["variant"] === "correctness-faults",
    )!;
    const forged = clone(correctnessScenario);
    const observations = forged["observations"] as Data;
    const intents = observations["intents"] as Data[];
    const evidence = forged["workloadEvidence"] as Data;
    evidence["requiredProductIntentSampleIds"] = intents.map((item) => item["sampleId"]);

    expect(() => validateScenarioReport(forged, contract)).toThrow(
      /lacks an E1 product input result/u,
    );
  });
});

describe("E0 oracle and latency derivation", () => {
  it("passes all eight complete zero-violation observations", () => {
    const results = evaluateOracles(passingObservations());
    expect(Object.keys(results)).toHaveLength(8);
    expect(Object.values(results).every((result) => result["status"] === "passed")).toBe(true);
  });

  it("fails every roadmap violation and never turns missing evidence green", () => {
    const observations = passingObservations();
    (observations["intents"] as Data[])[0]!["terminalOutcomes"] = [];
    (observations["intents"] as Data[])[0]!["ptyEffectCount"] = 2;
    (observations["intents"] as Data[])[1]!["automaticRetryCount"] = 1;
    (observations["ctrlC"] as Data[])[0]!["ptyEffectCount"] = 2;
    (observations["writerTransfers"] as Data[])[0]!["oldWriterSuccessfulEffects"] = 1;
    (observations["coldCandidates"] as Data[])[0]!["visibleBeforeValidation"] = true;
    (observations["authorityComparisons"] as Data[])[0]!["effectsEqual"] = false;
    (observations["secureInput"] as Data[])[0]!["speculativePresentationCount"] = 1;
    expect(
      Object.values(evaluateOracles(observations)).every((result) => result["status"] === "failed"),
    ).toBe(true);
    expect(
      Object.values(evaluateOracles({})).every((result) => result["status"] === "not-measured"),
    ).toBe(true);
  });

  it("collects only complete non-negative endpoint pairs with bigint timestamps", () => {
    const minimal: Data = {
      latencySpans: [{ id: "span", startEvent: "start", endEvent: "end" }],
    };
    const events: Data[] = [
      { name: "start", sampleId: "good", variant: "steady", atUnixNs: 9_000_000_000_000_000_000n },
      { name: "end", sampleId: "good", variant: "steady", atUnixNs: 9_000_000_000_001_500_000n },
      { name: "start", sampleId: "missing", variant: "steady", atUnixNs: 1n },
      { name: "start", sampleId: "negative", variant: "steady", atUnixNs: 10n },
      { name: "end", sampleId: "negative", variant: "steady", atUnixNs: 9n },
    ];
    expect(collectSpanSamples(minimal, events)).toEqual([
      { span: "span", sampleId: "good", variant: "steady", durationMs: 1.5 },
    ]);
  });

  it("pairs reused sample IDs inside each independent scenario", () => {
    const minimal: Data = { latencySpans: [{ id: "span", startEvent: "start", endEvent: "end" }] };
    const scenarios = [1, 10].map((offset) => ({
      events: [
        { name: "start", sampleId: "same", variant: "steady", atUnixNs: BigInt(offset) },
        { name: "end", sampleId: "same", variant: "steady", atUnixNs: BigInt(offset + 1_000_000) },
      ],
    }));
    expect(collectScenarioSpanSamples(minimal, scenarios)).toHaveLength(2);
  });

  it("requires 24 samples and keeps a flat cloud slope denominator nonzero", () => {
    const samples: Data[] = [];
    for (const variant of [
      "bulk-backlog-0",
      "bulk-backlog-262144",
      "bulk-backlog-1048576",
      "bulk-backlog-4194304",
    ]) {
      for (let index = 0; index < 24; index += 1) {
        samples.push({
          span: "cloud-browser-receive-to-host-send",
          variant,
          sampleId: `probe-measured-${index}`,
          durationMs: 5,
        });
      }
    }
    for (const [span, variants] of [
      ["host-receive-to-pty-write", ["snapshot-disabled", "snapshot-enabled"]],
      ["ctrl-c-to-application-quiet", ["steady", "output-flood"]],
    ] as const) {
      for (const variant of variants) {
        for (let index = 0; index < 24; index += 1)
          samples.push({ span, variant, sampleId: `${variant}-${index}`, durationMs: 5 });
      }
    }
    const measured = measureRelativeThresholds(summarizeSpanSamples(samples), samples);
    expect(measured["cloudBulkIsolation"]!["value"]).toBe(1);
    expect(Object.values(measured).every((item) => item["status"] === "measured")).toBe(true);
    const partial = samples.filter((item) => item["sampleId"] !== "probe-measured-23");
    expect(
      measureRelativeThresholds(summarizeSpanSamples(partial), partial)["cloudBulkIsolation"]![
        "status"
      ],
    ).toBe("not-measured");
  });
});

describe("candidate and E4b evidence", () => {
  it("builds a distinct candidate and derives a finite skip decision", () => {
    const baseline = current();
    const candidate = assembleCandidateReport(
      contract,
      baseline["scenarioReports"] as Data[],
      baseline["authorityOracle"] as Data,
      baseline,
      {
        environment: baseline["environment"] as Data,
        evidenceBoundary: baseline["evidenceBoundary"] as Data,
        generatedAt: "2026-09-01T00:00:00Z",
        sourceRevision: String(baseline["sourceRevision"]),
        sourceTreeGitOid: String(baseline["sourceTreeGitOid"]),
        deadlineMs: Number(baseline["deadlineMs"]),
        snapshotPhaseMeasurements: candidatePhases(),
      },
    );
    expect(candidate["baseline"]).toBe("CANDIDATE");
    expect(candidate).not.toHaveProperty("baselineStatus");
    expect(buildE4bDecision(baseline, candidate, contract)["decision"]).toBe(
      "skip-immutable-cow-cut",
    );
  });
});

describe("deterministic fixture and runner", () => {
  it("parses split probe, named flood, secure, interrupt arm and raw Ctrl-C", () => {
    const fixture = new FixtureState();
    expect(fixture.accept(Buffer.from("ZHONGDUAN_E0_PRO", "ascii"))).toEqual([]);
    expect(
      fixture.accept(Buffer.from("BE:probe-1\rZHONGDUAN_E0_FLOOD:flood-1\r", "ascii")),
    ).toEqual([
      ["probe", "probe-1"],
      ["flood", "flood-1"],
    ]);
    expect(fixture.accept(Buffer.from("ZHONGDUAN_E0_SECURE:secure-1\r", "ascii"))).toEqual([
      ["secure", "secure-1"],
    ]);
    expect(
      fixture.accept(Buffer.from("ZHONGDUAN_E0_INTERRUPT_ARM:ctrl-1:arm-1\r\x03", "binary")),
    ).toEqual([
      ["arm-interrupt", "arm-1"],
      ["interrupt", "ctrl-1"],
    ]);
  });

  it("counts duplicate effects and rejects unexpected or unbounded commands", () => {
    const fixture = new FixtureState();
    fixture.accept(Buffer.from("ZHONGDUAN_E0_PROBE:same\rZHONGDUAN_E0_PROBE:same\r", "ascii"));
    expect(fixture.effectCounts.get("same")).toBe(2);
    expect(() => new FixtureState().accept(Buffer.from("unexpected\r", "ascii"))).toThrow(
      FixtureProtocolError,
    );
    expect(() => new FixtureState().accept(Buffer.alloc(65_537, "x"))).toThrow(
      FixtureProtocolError,
    );
  });

  it("attributes Ctrl-C presses and maps browser and full wire identities", () => {
    const trace = new TraceStore("steady");
    trace.pendingCtrlSample = "ctrl-c-1";
    const browser = trace.browserFrame(
      {
        type: "key",
        action: "press",
        code: "KeyC",
        key: "c",
        modifiers: 2,
        inputEpoch: "epoch",
        clientInputSeq: "1",
      },
      1n,
    );
    expect(browser.sample).toBe("ctrl-c-1");
    trace.hostFrame(
      {
        type: "key",
        action: "press",
        code: "KeyC",
        key: "c",
        modifiers: 2,
        inputEpoch: "epoch",
        clientInputSeq: "1",
        writerFence: "fence",
      },
      2n,
      3n,
    );
    expect(trace.sampleIdentities.get("ctrl-c-1")).toBe("fence/epoch/1");
  });

  it("rejects unsafe CLI relabelling and keeps the frozen sample counts", () => {
    expect(() => parseArgs(["--samples", "23"])).toThrow(JourneyError);
    expect(() => parseArgs(["--report", BASELINE_PATH])).toThrow(JourneyError);
    expect(parseArgs(["--matrix-plan"]).matrixPlan).toBe(true);
  });
});
