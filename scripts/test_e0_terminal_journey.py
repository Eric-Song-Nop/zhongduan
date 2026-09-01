#!/usr/bin/env python3

from __future__ import annotations

import ast
import copy
from pathlib import Path
import subprocess
import unittest

from scripts.e0_terminal_fixture import FixtureProtocolError, FixtureState
from scripts.e0_terminal_journey import (
    BASELINE_PATH,
    CONTRACT_PATH,
    EXPECTED_ORACLES,
    EXPECTED_SPANS,
    EXPECTED_VARIANTS,
    assemble_candidate_report,
    assemble_current_report,
    build_e4b_decision,
    canonical_sha256,
    collect_scenario_span_samples,
    collect_span_samples,
    compare_relative_thresholds,
    evaluate_oracles,
    load_json,
    matrix_cells,
    measure_relative_thresholds,
    summarize_span_samples,
    validate_contract,
    validate_candidate_report,
    validate_report,
    validate_scenario_report,
)


SOURCE_REVISION = "a" * 40
SOURCE_TREE = "b" * 40


def terminal_intent(
    scenario_id: str,
    sample_id: str,
    sequence: int,
    *,
    pty_effect: bool = True,
    uncertainty: bool = False,
) -> tuple[dict, list[dict]]:
    local_id = f"local-{scenario_id}-{sequence}"
    browser_identity = f"epoch-{scenario_id}/{sequence}"
    identity = f"fence-{scenario_id}/epoch-{scenario_id}/{sequence}"
    base = sequence * 20_000_000
    events = [
        {"name": "browser.input-consumed", "sampleId": sample_id, "atUnixNs": base},
        {
            "name": "browser.send-decision",
            "sampleId": sample_id,
            "atUnixNs": base + 1_000_000,
        },
        {
            "name": "cloud.browser-receive-attempt",
            "sampleId": sample_id,
            "atUnixNs": base + 2_000_000,
            "browserIdentity": browser_identity,
            "attempt": 1,
        },
        {
            "name": "cloud.browser-receive",
            "sampleId": sample_id,
            "atUnixNs": base + 2_000_000,
            "wireIdentity": identity,
        },
    ]
    if pty_effect:
        events.extend(
            [
                {
                    "name": "cloud.host-send",
                    "sampleId": sample_id,
                    "atUnixNs": base + 3_000_000,
                    "wireIdentity": identity,
                },
                {
                    "name": "host.receive",
                    "sampleId": sample_id,
                    "atUnixNs": base + 4_000_000,
                    "wireIdentity": identity,
                },
                {
                    "name": "host.pty-write",
                    "sampleId": sample_id,
                    "atUnixNs": base + 5_000_000,
                },
            ]
        )
    intent = {
        "sampleId": sample_id,
        "localIntentId": local_id,
        "consumed": True,
        "terminalOutcomes": ["deterministic"],
        "terminalRecords": [
            {
                "sampleId": sample_id,
                "localIntentId": local_id,
                "outcome": "deterministic",
                "identity": {
                    "writerFence": f"fence-{scenario_id}",
                    "inputEpoch": f"epoch-{scenario_id}",
                    "clientInputSeq": str(sequence),
                },
                "reason": "input-ack",
                "deterministicResult": "written" if pty_effect else "rejected",
                "observedAtUnixNs": base + 9_000_000,
            }
        ],
        "browserIdentity": browser_identity,
        "browserIdentities": [browser_identity],
        "passiveBrowserIdentities": [browser_identity],
        "passiveSendAttemptCount": 1,
        "ptyEffectCount": 1 if pty_effect else 0,
    }
    if pty_effect:
        intent["wireIdentity"] = identity
        intent["wireIdentities"] = [identity]
    if uncertainty:
        intent["terminalOutcomes"] = ["uncertain"]
        intent["terminalRecords"][0].update(
            {
                "outcome": "uncertain",
                "reason": "socket-closed-after-send",
                "deterministicResult": None,
            }
        )
        intent.update(
            {
                "acceptanceUncertaintyInjected": True,
                "automaticRetryCount": 0,
                "identityChanged": False,
            }
        )
    return intent, events


def add_render_events(
    events: list[dict], sample_id: str, sequence: int, *, ctrl_c: bool
) -> None:
    base = sequence * 20_000_000
    events.extend(
        [
            {"name": "browser.keydown", "sampleId": sample_id, "atUnixNs": base},
            {
                "name": "pty.output",
                "sampleId": sample_id,
                "atUnixNs": base + 6_000_000,
            },
            {
                "name": "browser.matching-render",
                "sampleId": sample_id,
                "atUnixNs": base + 7_000_000,
            },
            {
                "name": "browser.useful-render",
                "sampleId": sample_id,
                "atUnixNs": base + 8_000_000,
            },
        ]
    )
    if ctrl_c:
        events.extend(
            [
                {"name": "browser.ctrl-c", "sampleId": sample_id, "atUnixNs": base},
                {
                    "name": "browser.application-quiet",
                    "sampleId": sample_id,
                    "atUnixNs": base + 9_000_000,
                },
            ]
        )


def variant_samples(variant: str, primary: str) -> tuple[list[str], str, bool]:
    if variant == "output-flood":
        ctrl = f"ctrl-c-{primary}"
        return [f"flood-command-{ctrl}", f"arm-{ctrl}", ctrl], ctrl, True
    if variant == "steady":
        ctrl = f"ctrl-c-{primary}"
        return [f"probe-{primary}", f"arm-{ctrl}", ctrl], ctrl, True
    if variant.startswith("bulk-backlog-") and variant != "bulk-backlog-0":
        return [f"flood-command-{primary}", f"probe-{primary}"], f"probe-{primary}", False
    return [f"probe-{primary}"], f"probe-{primary}", False


def synthetic_scenario(
    contract: dict,
    scenario_id: str,
    variant: str,
    browser_rtt: int = 20,
    host_rtt: int = 20,
    network_fault: str = "none",
) -> dict:
    observations = {
        "intents": [],
        "ctrlC": [],
        "writerTransfers": [],
        "coldCandidates": [],
        "secureInput": [],
    }
    warmup_observations = {"intents": [], "ctrlC": []}
    events: list[dict] = []
    primary_ids = [f"measured-{scenario_id}-{index:03d}" for index in range(24)]
    warmup_ids = [f"warmup-{scenario_id}-{index:03d}" for index in range(4)]
    sequence = 1

    for primary in primary_ids:
        sample_ids, rendered_sample, rendered_is_ctrl = variant_samples(variant, primary)
        for sample_id in sample_ids:
            intent, intent_events = terminal_intent(scenario_id, sample_id, sequence)
            observations["intents"].append(intent)
            events.extend(intent_events)
            if sample_id == rendered_sample:
                add_render_events(events, sample_id, sequence, ctrl_c=rendered_is_ctrl)
            sequence += 1
        if variant == "steady":
            observations["ctrlC"].append(
                {"sampleId": rendered_sample, "outputFlood": False, "ptyEffectCount": 1}
            )
        elif variant == "output-flood":
            observations["ctrlC"].append(
                {"sampleId": rendered_sample, "outputFlood": True, "ptyEffectCount": 1}
            )

    for primary in warmup_ids:
        sample_ids, rendered_sample, _ = variant_samples(variant, primary)
        for sample_id in sample_ids:
            intent, _ = terminal_intent(scenario_id, sample_id, sequence)
            warmup_observations["intents"].append(intent)
            sequence += 1
        if variant in {"steady", "output-flood"}:
            warmup_observations["ctrlC"].append(
                {
                    "sampleId": rendered_sample,
                    "outputFlood": variant == "output-flood",
                    "ptyEffectCount": 1,
                }
            )

    if variant == "correctness-faults":
        for sample_id, pty_effect, uncertainty in (
            ("duplicate-000", True, False),
            ("uncertain-000", False, True),
            ("old-writer-000", False, False),
            ("new-writer-000", True, False),
            ("secure-000", True, False),
        ):
            intent, intent_events = terminal_intent(
                scenario_id,
                sample_id,
                sequence,
                pty_effect=pty_effect,
                uncertainty=uncertainty,
            )
            observations["intents"].append(intent)
            events.extend(intent_events)
            sequence += 1
        observations["writerTransfers"] = [
            {"sampleId": "writer-transfer-000", "oldWriterSuccessfulEffects": 0}
        ]
        observations["coldCandidates"] = [
            {"sampleId": "cold-attach-000", "visibleBeforeValidation": False}
        ]
        observations["secureInput"] = [
            {"sampleId": "secure-000", "speculativePresentationCount": 0}
        ]

    for event in events:
        event["variant"] = variant
    snapshot_input_overlap = None
    if variant == "snapshot-enabled":
        post_midpoint = {
            f"probe-{primary}" for primary in primary_ids[len(primary_ids) // 2 :]
        }
        receives = sorted(
            (
                event
                for event in events
                if event.get("name") == "host.receive"
                and event.get("sampleId") in post_midpoint
            ),
            key=lambda event: event["atUnixNs"],
        )
        first_receive = receives[0]
        last_receive = receives[-1]
        finalized_at = (first_receive["atUnixNs"] + last_receive["atUnixNs"]) // 2
        events.append(
            {
                "name": "host.snapshot-finalized",
                "sampleId": "snapshot-synthetic",
                "snapshotId": "snapshot-synthetic",
                "atUnixNs": finalized_at,
                "variant": variant,
            }
        )
        snapshot_input_overlap = {
            "firstHostReceiveAtUnixNs": first_receive["atUnixNs"],
            "firstSampleId": first_receive["sampleId"],
            "snapshotFinalizationAtUnixNs": finalized_at,
            "snapshotId": "snapshot-synthetic",
            "lastHostReceiveAtUnixNs": last_receive["atUnixNs"],
            "lastSampleId": last_receive["sampleId"],
        }
    backlog = (
        int(variant.removeprefix("bulk-backlog-")) if variant.startswith("bulk-") else 0
    )
    return {
        "schemaVersion": "zhongduan-terminal-journey-scenario-v1",
        "contractSha256": canonical_sha256(contract),
        "status": "measured",
        "sourceRevision": SOURCE_REVISION,
        "sourceTreeGitOid": SOURCE_TREE,
        "sourceTreeDirty": False,
        "generatedAt": "2026-09-01T00:00:00Z",
        "environment": {"executionTier": "local-workerd", "runtime": "synthetic-test"},
        "variant": variant,
        "samples": 24,
        "warmups": 4,
        "appliedProfile": {
            "browserCloudRttMs": browser_rtt,
            "cloudHostRttMs": host_rtt,
            "networkFault": network_fault,
            "jitterMs": 5 if network_fault == "jitter" else 0,
        },
        "cloudSpanBoundary": {
            "start": "cloud ingress",
            "end": "cloud egress",
            "includesBrowserLink": False,
            "includesHostLink": False,
        },
        "workloadEvidence": {
            "primarySampleIds": primary_ids,
            "warmupPrimarySampleIds": warmup_ids,
            "warmupCount": 4,
            "measurementStartedAtUnixMs": 1_000,
            "measurementEndedAtUnixMs": 2_000,
            "configuredBulkBacklogBytes": backlog,
            "snapshotFinalizationsDuringMeasurement": 1
            if variant == "snapshot-enabled"
            else 0,
            "snapshotInputOverlap": snapshot_input_overlap,
            "outputFlood": variant == "output-flood" or backlog > 0,
            "acceptanceDisconnect": variant == "correctness-faults",
            "acceptanceReconnectObserved": variant == "correctness-faults",
            "writerTransfer": variant == "correctness-faults",
            "coldAttachValidation": variant == "correctness-faults",
        },
        "observations": observations,
        "warmupObservations": warmup_observations,
        "events": events,
        "rawEventCount": len(events),
        "deadlineMs": 120_000,
        "deadlineIsSlo": False,
    }


def complete_scenarios(contract: dict) -> list[dict]:
    scenarios = []
    for browser_rtt in (20, 100, 300, 600):
        for host_rtt in (20, 100, 300, 600):
            scenarios.append(
                synthetic_scenario(
                    contract,
                    f"steady-{browser_rtt}-{host_rtt}",
                    "steady",
                    browser_rtt,
                    host_rtt,
                )
            )
    scenarios.append(
        synthetic_scenario(contract, "steady-jitter", "steady", 100, 100, "jitter")
    )
    scenarios.append(
        synthetic_scenario(contract, "output-flood", "output-flood", 100, 100)
    )
    scenarios.append(
        synthetic_scenario(
            contract, "correctness-faults", "correctness-faults", 100, 100
        )
    )
    remaining = EXPECTED_VARIANTS - {"steady", "output-flood", "correctness-faults"}
    for variant in sorted(remaining):
        scenarios.append(synthetic_scenario(contract, variant, variant))
    return scenarios


def complete_report(contract: dict) -> dict:
    authority = authority_result()
    return assemble_current_report(
        contract,
        complete_scenarios(contract),
        authority,
        environment={"executionTier": "local-workerd"},
        evidence_boundary={"realCloudflareEdge": False},
        generated_at="2026-09-01T00:00:00Z",
        source_revision=SOURCE_REVISION,
        source_tree_git_oid=SOURCE_TREE,
        deadline_ms=120_000,
    )


def authority_result() -> dict:
    return {
        "schemaVersion": "zhongduan-e0-authority-oracle-v3",
        "sourceRevision": SOURCE_REVISION,
        "sourceTreeGitOid": SOURCE_TREE,
        "sourceTreeDirty": False,
        "engineId": "test-engine",
        "artifactVerified": True,
        "corpus": [
            {
                "id": "synthetic-snapshot-case",
                "snapshotCaptureStateEqual": True,
                "checkpointSourceStateEqual": True,
                "recoveredStateEqual": True,
                "normalizedStateEqual": True,
                "checkpointSourceContinuationEqual": True,
                "recoveredContinuationEqual": True,
                "continuationEqual": True,
            }
        ],
        "effectCorpus": {
            "cases": [{"id": "synthetic-effect-case", "effectsEqual": True}],
            "effectsEqual": True,
        },
        "comparison": {
            "sampleId": "authority",
            "snapshotCaptureStateEqual": True,
            "checkpointSourceStateEqual": True,
            "recoveredStateEqual": True,
            "normalizedStateEqual": True,
            "checkpointSourceContinuationEqual": True,
            "recoveredContinuationEqual": True,
            "continuationEqual": True,
            "effectsEqual": True,
            "corpusCaseCount": 1,
            "effectCaseCount": 1,
        },
    }


def candidate_host_measurements() -> list[dict]:
    base = {
        "schemaVersion": 1,
        "source": "host-cloud-relay",
        "recordedAtUnixMs": 1_500,
        "scenarioVariant": "snapshot-enabled",
        "measurementStartedAtUnixMs": 1_000,
        "measurementEndedAtUnixMs": 2_000,
        "outcome": "ok",
    }
    values = [
        {
            **base,
            "scenarioVariant": variant,
            "event": "input-actor-queue",
            "bytes": 1,
            "messageType": "input",
            "queueWaitMs": 0.5,
        }
        for variant in ("snapshot-disabled", "snapshot-enabled")
    ]
    for phase in (
        "refresh-queue-wait",
        "authority-actor-wait",
        "authority-cut",
        "authority-encode",
        "publisher-total",
        "finalize-install",
    ):
        values.append(
            {
                **base,
                "event": "snapshot-refresh",
                "attemptId": "attempt-1",
                "phase": phase,
                "durationMs": 1.0,
            }
        )
    for phase in ("compress", "hash", "upload", "finalize"):
        values.append(
            {
                **base,
                "event": "snapshot-publish",
                "phase": phase,
                "durationMs": 1.0,
            }
        )
    return values


class E0ContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = load_json(CONTRACT_PATH)

    def test_contract_freezes_exact_roadmap_surface(self) -> None:
        validate_contract(self.contract)
        self.assertEqual({item["id"] for item in self.contract["oracles"]}, EXPECTED_ORACLES)
        self.assertEqual(
            {item["id"] for item in self.contract["latencySpans"]}, EXPECTED_SPANS
        )
        self.assertEqual(set(self.contract["workload"]["requiredVariants"]), EXPECTED_VARIANTS)
        self.assertEqual(len(matrix_cells(self.contract)), 23)

    def test_incomplete_checked_in_artifact_is_never_accepted_as_current(self) -> None:
        self.assertTrue(BASELINE_PATH.exists())
        baseline = load_json(BASELINE_PATH)
        if baseline.get("baselineStatus") == "complete":
            validate_report(baseline, self.contract)
        else:
            with self.assertRaises(ValueError):
                validate_report(baseline, self.contract)

    def test_complete_report_is_fully_recomputed(self) -> None:
        report = complete_report(self.contract)
        validate_report(report, self.contract)
        self.assertEqual(report["workload"]["samplesPerVariant"], 24)
        self.assertEqual(report["workload"]["warmupSamples"], 4)
        self.assertFalse(
            any(cell["status"] == "not-run" for cell in report["matrixCoverage"])
        )

    def test_truthful_missing_terminal_outcome_is_a_failed_oracle_not_invalid_evidence(
        self,
    ) -> None:
        scenarios = complete_scenarios(self.contract)
        scenarios[0]["observations"]["intents"][0]["terminalOutcomes"] = []
        scenarios[0]["observations"]["intents"][0]["terminalRecords"] = []
        report = assemble_current_report(
            self.contract,
            scenarios,
            authority_result(),
            environment={"executionTier": "local-workerd"},
            evidence_boundary={"realCloudflareEdge": False},
            generated_at="2026-09-01T00:00:00Z",
            source_revision=SOURCE_REVISION,
            source_tree_git_oid=SOURCE_TREE,
            deadline_ms=120_000,
        )
        self.assertEqual(report["oracleResults"]["ui-consumed-silent-loss"]["status"], "failed")
        validate_report(report, self.contract)

    def test_terminal_identity_may_be_null_when_current_ack_has_no_writer_fence(self) -> None:
        report = complete_report(self.contract)
        report["scenarioReports"][0]["observations"]["intents"][0]["terminalRecords"][0][
            "identity"
        ] = None
        report["observations"]["intents"][0]["terminalRecords"][0]["identity"] = None
        validate_report(report, self.contract)

    def test_runner_instrumentation_is_passive_and_production_has_no_e0_hook(self) -> None:
        root = Path(__file__).resolve().parents[1]
        runner = (root / "scripts" / "verify-e0-terminal-journey.py").read_text()
        terminal_app = (
            root / "apps" / "terminal-cloud" / "src" / "browser" / "terminal-app.tsx"
        ).read_text()
        self.assertIn("typeof frame.writerFence === 'string'", runner)
        self.assertIn("zhongduan:input-intent-result", runner)
        self.assertNotIn("__zhongduanE0", terminal_app)

    def test_runner_waits_for_host_ready_ack_before_opening_the_browser(self) -> None:
        root = Path(__file__).resolve().parents[1]
        runner = (root / "scripts" / "verify-e0-terminal-journey.py").read_text()
        self.assertIn('frame.get("type") == "host-ready-ack"', runner)
        wait = runner.index("trace.host_ready_acknowledged.wait()")
        journey = runner.index("= await browser_journey(")
        self.assertLess(wait, journey)

    def test_snapshot_enabled_requires_finalization_between_host_inputs(self) -> None:
        scenario = synthetic_scenario(
            self.contract, "snapshot-overlap", "snapshot-enabled"
        )
        overlap = scenario["workloadEvidence"]["snapshotInputOverlap"]
        self.assertLess(
            overlap["firstHostReceiveAtUnixNs"],
            overlap["snapshotFinalizationAtUnixNs"],
        )
        self.assertLess(
            overlap["snapshotFinalizationAtUnixNs"],
            overlap["lastHostReceiveAtUnixNs"],
        )
        validate_scenario_report(scenario, self.contract)

        overlap["snapshotFinalizationAtUnixNs"] = overlap[
            "lastHostReceiveAtUnixNs"
        ]
        with self.assertRaisesRegex(ValueError, "overlap Host input window"):
            validate_scenario_report(scenario, self.contract)

    def test_uncertain_no_effect_does_not_block_remaining_fault_oracles(self) -> None:
        scenario = synthetic_scenario(
            self.contract, "correctness-faults", "correctness-faults", 100, 100
        )
        validate_scenario_report(scenario, self.contract)
        uncertain = next(
            item
            for item in scenario["observations"]["intents"]
            if item["sampleId"] == "uncertain-000"
        )
        self.assertEqual(uncertain["terminalOutcomes"], ["uncertain"])
        self.assertEqual(uncertain["ptyEffectCount"], 0)

        root = Path(__file__).resolve().parents[1]
        runner = (root / "scripts" / "verify-e0-terminal-journey.py").read_text()
        fault_start = runner.index('uncertainty_sample = "uncertain-000"')
        fault_end = runner.index('old_sample = "old-writer-000"', fault_start)
        uncertainty_path = runner[fault_start:fault_end]
        self.assertNotIn('f"{RESULT_PREFIX}{uncertainty_sample}"', uncertainty_path)
        self.assertIn("acceptance_reconnect_observed = True", uncertainty_path)
        self.assertIn("async def run_fault_command(", runner)
        self.assertIn("observation[\"matchingOutputObserved\"] = rendered", runner)

        scenario["workloadEvidence"]["acceptanceReconnectObserved"] = False
        with self.assertRaisesRegex(ValueError, "every declared fault"):
            validate_scenario_report(scenario, self.contract)

    def test_proxy_treats_only_connection_reset_as_normal_shutdown(self) -> None:
        root = Path(__file__).resolve().parents[1]
        runner = (root / "scripts" / "verify-e0-terminal-journey.py").read_text()
        self.assertIn("except ConnectionResetError:", runner)
        self.assertIn("for task in done:\n                task.result()", runner)

    def test_runner_emulates_wterm_paste_and_attributes_only_ctrl_c_press(self) -> None:
        root = Path(__file__).resolve().parents[1]
        runner = (root / "scripts" / "verify-e0-terminal-journey.py").read_text()
        self.assertIn("format === 'text' || format === 'text/plain'", runner)
        control_down = runner.index('await page.keyboard.down("Control")')
        sample_start = runner.index("await set_sample(page, sample_id", control_down)
        c_down = runner.index('await page.keyboard.down("KeyC")', sample_start)
        sample_end = runner.index("await set_sample(page, None)", c_down)
        c_up = runner.index('await page.keyboard.up("KeyC")', sample_end)
        self.assertLess(control_down, sample_start)
        self.assertLess(sample_start, c_down)
        self.assertLess(c_down, sample_end)
        self.assertLess(sample_end, c_up)
        self.assertIn('frame.get("action") == "press"', runner)
        self.assertIn("frame[\"modifiers\"] & CONTROL_MODIFIER", runner)

    def test_runner_maps_v2_ack_without_writer_fence_to_null_identity(self) -> None:
        root = Path(__file__).resolve().parents[1]
        source = (root / "scripts" / "verify-e0-terminal-journey.py").read_text()
        module = ast.parse(source)
        init_script = next(
            ast.literal_eval(statement.value)
            for statement in module.body
            if isinstance(statement, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "INIT_SCRIPT"
                for target in statement.targets
            )
        )
        check = f"""
          globalThis.crypto = require('node:crypto').webcrypto;
          globalThis.window = new EventTarget();
          globalThis.document = new EventTarget();
          class FakeWebSocket extends EventTarget {{ send() {{}} }}
          globalThis.WebSocket = FakeWebSocket;
          {init_script}
          const state = window.__zhongduanE0;
          state.consume('v2-ack');
          state.currentSample = 'v2-ack';
          const socket = new WebSocket();
          socket.send(JSON.stringify({{
            type: 'text', data: 'x', inputEpoch: 'epoch-v2', clientInputSeq: '7'
          }}));
          state.currentSample = null;
          const ack = new Event('message');
          Object.defineProperty(ack, 'data', {{ value: JSON.stringify({{
            type: 'input-epoch-ack', results: [{{
              inputEpoch: 'epoch-v2', clientInputSeq: '7', status: 'written'
            }}]
          }}) }});
          socket.dispatchEvent(ack);
          const terminal = state.intentsBySample['v2-ack'].terminal;
          if (terminal?.outcome !== 'deterministic' || terminal.identity !== null) {{
            throw new Error(JSON.stringify(terminal));
          }}

          state.consume('candidate-result');
          state.currentSample = 'candidate-result';
          const result = new Event('zhongduan:input-intent-result');
          Object.defineProperty(result, 'detail', {{ value: {{
            localIntentId: 'product-1', outcome: 'not-sent', reason: 'admission-rejected',
            identity: null
          }} }});
          window.dispatchEvent(result);
          const candidate = state.intentsBySample['candidate-result'].terminal;
          if (candidate?.source !== 'product-intent-result-event' ||
              candidate?.outcome !== 'not-sent' || candidate.identity !== null) {{
            throw new Error(JSON.stringify(candidate));
          }}

          state.consume('duplicate-wire-attempt');
          state.currentSample = 'duplicate-wire-attempt';
          state.duplicateSample = 'duplicate-wire-attempt';
          socket.send(JSON.stringify({{
            type: 'text', data: 'x', inputEpoch: 'epoch-v2', clientInputSeq: '8'
          }}));
          const duplicate = state.intentsBySample['duplicate-wire-attempt'];
          if (duplicate.sends.length !== 2 ||
              duplicate.sends[0].identity !== duplicate.sends[1].identity) {{
            throw new Error(JSON.stringify(duplicate.sends));
          }}
          process.exit(0);
        """
        subprocess.run(
            ["node", "-e", check],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )

    def test_report_rejects_a_local_run_claiming_real_edge_evidence(self) -> None:
        report = complete_report(self.contract)
        report["evidenceBoundary"]["realCloudflareEdge"] = True
        with self.assertRaisesRegex(ValueError, "must not claim"):
            validate_report(report, self.contract)

    def test_contract_hash_is_stable_across_key_order(self) -> None:
        self.assertEqual(
            canonical_sha256({"b": 1, "a": 2}), canonical_sha256({"a": 2, "b": 1})
        )


class ValidatorForgeryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.contract = load_json(CONTRACT_PATH)
        cls.valid = complete_report(cls.contract)

    def test_rejects_fabricated_green_oracle(self) -> None:
        report = copy.deepcopy(self.valid)
        report["oracleResults"]["duplicate-pty-effect"]["value"] = 999
        with self.assertRaisesRegex(ValueError, "oracle results"):
            validate_report(report, self.contract)

    def test_rejects_fabricated_summary_and_threshold(self) -> None:
        report = copy.deepcopy(self.valid)
        report["latencySummaries"][0]["p99Ms"] += 1
        with self.assertRaisesRegex(ValueError, "summaries"):
            validate_report(report, self.contract)
        report = copy.deepcopy(self.valid)
        report["relativeThresholdMeasurements"]["cloudBulkIsolation"]["value"] = 99
        with self.assertRaisesRegex(ValueError, "relative thresholds"):
            validate_report(report, self.contract)

    def test_rejects_authority_aggregate_not_backed_by_retained_corpus(self) -> None:
        report = copy.deepcopy(self.valid)
        report["authorityOracle"]["corpus"][0]["recoveredStateEqual"] = False
        with self.assertRaisesRegex(ValueError, "snapshot case aggregate"):
            validate_report(report, self.contract)

    def test_rejects_observation_effect_count_not_backed_by_events(self) -> None:
        report = copy.deepcopy(self.valid)
        report["scenarioReports"][0]["observations"]["intents"][0][
            "ptyEffectCount"
        ] = 0
        report["observations"]["intents"][0]["ptyEffectCount"] = 0
        with self.assertRaisesRegex(ValueError, "PTY effect count"):
            validate_report(report, self.contract)

    def test_rejects_label_only_variant_and_false_coverage(self) -> None:
        scenario = copy.deepcopy(self.valid["scenarioReports"][-1])
        scenario["workloadEvidence"]["configuredBulkBacklogBytes"] = 123
        with self.assertRaisesRegex(ValueError, "backlog"):
            validate_scenario_report(scenario, self.contract)
        report = copy.deepcopy(self.valid)
        report["matrixCoverage"][0]["status"] = "requires-staging"
        with self.assertRaisesRegex(ValueError, "coverage"):
            validate_report(report, self.contract)


class CandidateReportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = load_json(CONTRACT_PATH)
        self.current = complete_report(self.contract)

    def candidate(self) -> dict:
        scenarios = complete_scenarios(self.contract)
        for scenario in scenarios:
            scenario["sourceRevision"] = "c" * 40
            scenario["sourceTreeGitOid"] = "d" * 40
        return assemble_candidate_report(
            self.contract,
            scenarios,
            authority_result(),
            self.current,
            environment={"executionTier": "local-workerd"},
            evidence_boundary={"realCloudflareEdge": False},
            generated_at="2026-09-01T01:00:00Z",
            source_revision="c" * 40,
            source_tree_git_oid="d" * 40,
            deadline_ms=120_000,
            snapshot_phase_measurements=candidate_host_measurements(),
        )

    def test_candidate_is_generated_and_compared_without_current_relabelling(self) -> None:
        candidate = self.candidate()
        self.assertEqual(candidate["schemaVersion"], "zhongduan-terminal-journey-candidate-v1")
        self.assertEqual(candidate["baseline"], "CANDIDATE")
        self.assertNotIn("baselineStatus", candidate)
        self.assertTrue(
            all(
                item["status"] == "passed"
                for item in candidate["comparisonToCurrent"]["relativeThresholds"].values()
            )
        )
        validate_candidate_report(candidate, self.current, self.contract)

    def test_candidate_rejects_relabelled_current_and_missing_phase_evidence(self) -> None:
        with self.assertRaisesRegex(ValueError, "candidate report schema"):
            validate_candidate_report(self.current, self.current, self.contract)
        candidate = self.candidate()
        candidate["snapshotPhaseMeasurements"] = []
        with self.assertRaisesRegex(ValueError, "Host measurements"):
            validate_candidate_report(candidate, self.current, self.contract)

    def test_e4b_decision_is_derived_from_finite_validated_evidence(self) -> None:
        candidate = self.candidate()
        decision = build_e4b_decision(self.current, candidate, self.contract)
        self.assertEqual(decision["status"], "complete-finite-measurement")
        self.assertEqual(decision["decision"], "skip-immutable-cow-cut")
        self.assertTrue(decision["currentAssessment"]["r0Authorized"])
        self.assertFalse(decision["currentAssessment"]["immutableCowCutAuthorized"])
        self.assertEqual(
            decision["evidence"]["candidateSourceRevision"],
            candidate["sourceRevision"],
        )


class OracleTest(unittest.TestCase):
    def complete_observations(self) -> dict:
        return {
            "intents": [
                {
                    "localIntentId": "local-1",
                    "consumed": True,
                    "terminalOutcomes": ["deterministic"],
                    "wireIdentity": "1/epoch/1",
                    "ptyEffectCount": 1,
                    "acceptanceUncertaintyInjected": True,
                    "automaticRetryCount": 0,
                }
            ],
            "ctrlC": [
                {"sampleId": "ctrl-c", "outputFlood": True, "ptyEffectCount": 1}
            ],
            "writerTransfers": [
                {"sampleId": "transfer", "oldWriterSuccessfulEffects": 0}
            ],
            "coldCandidates": [
                {"sampleId": "cold", "visibleBeforeValidation": False}
            ],
            "authorityComparisons": [
                {
                    "sampleId": "authority",
                    "normalizedStateEqual": True,
                    "continuationEqual": True,
                    "effectsEqual": True,
                }
            ],
            "secureInput": [
                {"sampleId": "secure", "speculativePresentationCount": 0}
            ],
        }

    def test_all_oracles_pass_with_complete_zero_violation_evidence(self) -> None:
        results = evaluate_oracles(self.complete_observations())
        self.assertEqual(set(results), EXPECTED_ORACLES)
        self.assertTrue(all(item["status"] == "passed" for item in results.values()))

    def test_oracles_fail_each_roadmap_violation(self) -> None:
        observations = self.complete_observations()
        intent = observations["intents"][0]
        intent["terminalOutcomes"] = []
        intent["ptyEffectCount"] = 2
        intent["automaticRetryCount"] = 1
        observations["ctrlC"][0]["ptyEffectCount"] = 2
        observations["writerTransfers"][0]["oldWriterSuccessfulEffects"] = 1
        observations["coldCandidates"][0]["visibleBeforeValidation"] = True
        observations["authorityComparisons"][0]["continuationEqual"] = False
        observations["secureInput"][0]["speculativePresentationCount"] = 1
        results = evaluate_oracles(observations)
        self.assertTrue(all(item["status"] == "failed" for item in results.values()))

    def test_missing_evidence_never_turns_green(self) -> None:
        results = evaluate_oracles({})
        self.assertTrue(all(item["status"] == "not-measured" for item in results.values()))


class LatencyTest(unittest.TestCase):
    def test_collects_only_complete_non_negative_pairs(self) -> None:
        contract = load_json(CONTRACT_PATH)
        events = [
            {
                "name": "browser.keydown",
                "sampleId": "a",
                "variant": "steady",
                "atUnixNs": 1_000_000,
            },
            {
                "name": "browser.send-decision",
                "sampleId": "a",
                "variant": "steady",
                "atUnixNs": 3_000_000,
            },
            {
                "name": "browser.keydown",
                "sampleId": "incomplete",
                "variant": "steady",
                "atUnixNs": 4_000_000,
            },
        ]
        samples = collect_span_samples(contract, events)
        self.assertEqual(samples[0]["durationMs"], 2.0)
        self.assertEqual(summarize_span_samples(samples)[0]["p99Ms"], 2.0)

    def test_pairs_reused_sample_ids_inside_each_matrix_scenario(self) -> None:
        contract = {
            "latencySpans": [
                {
                    "id": "browser-keydown-to-send-decision",
                    "startEvent": "browser.keydown",
                    "endEvent": "browser.send-decision",
                }
            ]
        }
        events = [
            {
                "name": "browser.keydown",
                "sampleId": "shared",
                "variant": "steady",
                "atUnixNs": 1_000_000,
            },
            {
                "name": "browser.send-decision",
                "sampleId": "shared",
                "variant": "steady",
                "atUnixNs": 3_000_000,
            },
        ]
        samples = collect_scenario_span_samples(
            contract,
            [{"events": copy.deepcopy(events)}, {"events": copy.deepcopy(events)}],
        )
        self.assertEqual(len(samples), 2)
        self.assertTrue(all(sample["durationMs"] == 2.0 for sample in samples))

    def test_relative_thresholds_require_24_and_flat_slope_has_nonzero_denominator(
        self,
    ) -> None:
        self.assertTrue(
            all(
                item["status"] == "not-measured"
                for item in measure_relative_thresholds([], []).values()
            )
        )
        summaries = [
            {
                "span": "cloud-browser-receive-to-host-send",
                "variant": f"bulk-backlog-{backlog}",
                "p99Ms": float(index + 1) * 100.0,
                "sampleCount": 24,
            }
            for index, backlog in enumerate((0, 262144, 1048576, 4194304))
        ]
        summaries.extend(
            [
                {
                    "span": "host-receive-to-pty-write",
                    "variant": "snapshot-disabled",
                    "p99Ms": 2.0,
                    "sampleCount": 24,
                },
                {
                    "span": "host-receive-to-pty-write",
                    "variant": "snapshot-enabled",
                    "p99Ms": 3.0,
                    "sampleCount": 24,
                },
                {
                    "span": "ctrl-c-to-application-quiet",
                    "variant": "steady",
                    "p99Ms": 4.0,
                    "sampleCount": 24,
                },
                {
                    "span": "ctrl-c-to-application-quiet",
                    "variant": "output-flood",
                    "p99Ms": 6.0,
                    "sampleCount": 24,
                },
            ]
        )
        samples = [
            {
                "span": "cloud-browser-receive-to-host-send",
                "variant": f"bulk-backlog-{backlog}",
                "sampleId": f"probe-measured-bulk-backlog-{backlog}-{index:03d}",
                "durationMs": 10.0,
            }
            for backlog in (0, 262144, 1048576, 4194304)
            for index in range(24)
        ]
        samples.extend(
            {
                "span": "cloud-browser-receive-to-host-send",
                "variant": f"bulk-backlog-{backlog}",
                "sampleId": f"flood-command-measured-bulk-backlog-{backlog}-{index:03d}",
                "durationMs": float(index + 1) * 1_000.0,
            }
            for backlog in (262144, 1048576, 4194304)
            for index in range(24)
        )
        measured = measure_relative_thresholds(summaries, samples)
        self.assertEqual(measured["cloudBulkIsolation"]["value"], 1.0)
        self.assertEqual(
            measured["cloudBulkIsolation"]["rawNormalizedSlopePerMiB"], 0.0
        )
        self.assertEqual(
            measured["cloudBulkIsolation"]["sampleCounts"],
            {
                "bulk-backlog-0": 24,
                "bulk-backlog-262144": 24,
                "bulk-backlog-1048576": 24,
                "bulk-backlog-4194304": 24,
            },
        )
        self.assertEqual(measured["snapshotHostInput"]["value"], 1.5)
        self.assertEqual(measured["outputFloodCtrlC"]["value"], 1.5)
        comparisons = compare_relative_thresholds(
            load_json(CONTRACT_PATH),
            measured,
            measured,
            {"output-flood-ctrl-c-once": {"status": "passed"}},
        )
        self.assertTrue(all(item["status"] == "passed" for item in comparisons.values()))


class FixtureTest(unittest.TestCase):
    def test_parses_split_probe_named_flood_interrupt_arm_secure_and_interrupt(self) -> None:
        state = FixtureState()
        self.assertEqual(state.accept(b"ZHONGDUAN_E0_PRO"), [])
        self.assertEqual(
            state.accept(
                b"BE:a\rZHONGDUAN_E0_FLOOD:flood-a\r"
                b"ZHONGDUAN_E0_INTERRUPT_ARM:ctrl-a:arm-a\r"
            ),
            [("probe", "a"), ("flood", "flood-a"), ("arm-interrupt", "arm-a")],
        )
        self.assertEqual(
            state.accept(b"ZHONGDUAN_E0_SECURE:s\r\x03"),
            [("secure", "s"), ("interrupt", "ctrl-a")],
        )

    def test_counts_duplicate_application_effects(self) -> None:
        state = FixtureState()
        state.accept(b"ZHONGDUAN_E0_PROBE:a\rZHONGDUAN_E0_PROBE:a\r")
        self.assertEqual(state.effect_counts["a"], 2)

    def test_rejects_unexpected_or_unbounded_commands(self) -> None:
        with self.assertRaisesRegex(FixtureProtocolError, "unexpected"):
            FixtureState().accept(b"wrong\r")
        with self.assertRaisesRegex(FixtureProtocolError, "64 KiB"):
            FixtureState().accept(b"x" * 65537)


if __name__ == "__main__":
    unittest.main()
