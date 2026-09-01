#!/usr/bin/env python3

"""Executable E0 contract, oracle and latency-report helpers.

This module deliberately uses only the Python standard library so contract and
checked-in report validation remain available in the ordinary repository test
suite. The real browser journey has an explicit Playwright/aiohttp dependency
and lives in ``verify-e0-terminal-journey.py``.
"""

from __future__ import annotations

from collections import defaultdict
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "benchmarks" / "terminal-journey" / "contract.json"
BASELINE_PATH = ROOT / "benchmarks" / "terminal-journey" / "current-baseline.json"

EXPECTED_ORACLES = {
    "ui-consumed-silent-loss",
    "duplicate-pty-effect",
    "uncertain-auto-retry",
    "output-flood-ctrl-c-once",
    "old-writer-effect-after-transfer",
    "cold-candidate-visible-before-validation",
    "snapshot-authority-divergence",
    "secure-input-speculative-presentation",
}

EXPECTED_SPANS = {
    "browser-keydown-to-send-decision",
    "cloud-browser-receive-to-host-send",
    "host-receive-to-pty-write",
    "input-to-matching-browser-render",
    "pty-output-to-browser-useful-render",
    "ctrl-c-to-pty-write",
    "ctrl-c-to-application-quiet",
}

EXPECTED_THRESHOLDS = {
    "cloudBulkIsolation",
    "snapshotHostInput",
    "outputFloodCtrlC",
}

EXPECTED_VARIANTS = {
    "steady",
    "output-flood",
    "bulk-backlog-0",
    "bulk-backlog-262144",
    "bulk-backlog-1048576",
    "bulk-backlog-4194304",
    "snapshot-disabled",
    "snapshot-enabled",
    "correctness-faults",
}

TERMINAL_OUTCOMES = {"not-sent", "deterministic", "uncertain"}
MIN_MEASURED_SAMPLES = 24
REPORT_BUNDLE_SCHEMA = "zhongduan-terminal-journey-bundle-v1"
REPORT_BUNDLE_PAYLOAD_FIELDS = {
    "events",
    "latencySamples",
    "observations",
    "scenarioReports",
}
MAX_SCENARIO_ARCHIVE_COMPRESSED_BYTES = 16 * 1024 * 1024
MAX_SCENARIO_ARCHIVE_UNCOMPRESSED_BYTES = 64 * 1024 * 1024


class ContractError(ValueError):
    pass


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ContractError(f"cannot read JSON contract artifact {path}: {error}") from error
    if not isinstance(value, dict):
        raise ContractError(f"JSON artifact {path} must contain an object")
    return value


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _scenario_jsonl(scenarios: list[dict[str, Any]]) -> bytes:
    return b"".join(
        json.dumps(
            scenario,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
        for scenario in scenarios
    )


def report_archive_path(report_path: Path) -> Path:
    return report_path.with_suffix(".scenarios.jsonl.gz")


def build_report_bundle(
    report: dict[str, Any], report_path: Path
) -> tuple[dict[str, Any], bytes]:
    """Split a validated report into a reviewable summary and canonical raw archive."""

    scenarios = report.get("scenarioReports")
    latency_samples = report.get("latencySamples")
    if not isinstance(scenarios, list) or any(
        not isinstance(scenario, dict) for scenario in scenarios
    ):
        raise ContractError("report bundle requires raw scenario reports")
    if not isinstance(latency_samples, list):
        raise ContractError("report bundle requires derived latency samples")
    archive_path = report_archive_path(report_path)
    raw = _scenario_jsonl(scenarios)
    compressed = gzip.compress(raw, compresslevel=9, mtime=0)
    if len(compressed) > MAX_SCENARIO_ARCHIVE_COMPRESSED_BYTES:
        raise ContractError("scenario archive exceeds its compressed size limit")
    if len(raw) > MAX_SCENARIO_ARCHIVE_UNCOMPRESSED_BYTES:
        raise ContractError("scenario archive exceeds its uncompressed size limit")

    bundle = {
        key: value
        for key, value in report.items()
        if key not in REPORT_BUNDLE_PAYLOAD_FIELDS
    }
    bundle["schemaVersion"] = REPORT_BUNDLE_SCHEMA
    bundle["reportSchemaVersion"] = report.get("schemaVersion")
    bundle["reconstructedReportSha256"] = canonical_sha256(report)
    bundle["latencySampleCount"] = len(latency_samples)
    bundle["scenarioArchive"] = {
        "path": archive_path.name,
        "format": "canonical-jsonl",
        "compression": "gzip-9-mtime-0",
        "scenarioCount": len(scenarios),
        "compressedBytes": len(compressed),
        "compressedSha256": _sha256_bytes(compressed),
        "uncompressedBytes": len(raw),
        "uncompressedSha256": _sha256_bytes(raw),
    }
    return bundle, compressed


def write_report_bundle(report_path: Path, report: dict[str, Any]) -> dict[str, Any]:
    bundle, compressed = build_report_bundle(report, report_path)
    archive_path = report_archive_path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    archive_path.write_bytes(compressed)
    report_path.write_text(
        json.dumps(bundle, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return bundle


def load_report_bundle(report_path: Path) -> dict[str, Any]:
    """Load a full legacy report or reconstruct one from a bounded evidence bundle."""

    bundle = load_json(report_path)
    if bundle.get("schemaVersion") != REPORT_BUNDLE_SCHEMA:
        return bundle
    metadata = bundle.get("scenarioArchive")
    if not isinstance(metadata, dict):
        raise ContractError("report bundle must identify its scenario archive")
    archive_name = metadata.get("path")
    if (
        not isinstance(archive_name, str)
        or not archive_name
        or Path(archive_name).name != archive_name
    ):
        raise ContractError("scenario archive path must be a local basename")
    if (
        metadata.get("format") != "canonical-jsonl"
        or metadata.get("compression") != "gzip-9-mtime-0"
    ):
        raise ContractError("report bundle uses an unsupported scenario archive format")
    compressed_bytes = metadata.get("compressedBytes")
    uncompressed_bytes = metadata.get("uncompressedBytes")
    scenario_count = metadata.get("scenarioCount")
    if (
        not isinstance(compressed_bytes, int)
        or not 0 < compressed_bytes <= MAX_SCENARIO_ARCHIVE_COMPRESSED_BYTES
        or not isinstance(uncompressed_bytes, int)
        or not 0 < uncompressed_bytes <= MAX_SCENARIO_ARCHIVE_UNCOMPRESSED_BYTES
        or not isinstance(scenario_count, int)
        or scenario_count <= 0
    ):
        raise ContractError("report bundle has invalid scenario archive bounds")

    archive_path = report_path.parent / archive_name
    try:
        actual_compressed_bytes = archive_path.stat().st_size
    except OSError as error:
        raise ContractError(f"cannot read scenario archive {archive_path}: {error}") from error
    if (
        actual_compressed_bytes != compressed_bytes
        or actual_compressed_bytes > MAX_SCENARIO_ARCHIVE_COMPRESSED_BYTES
    ):
        raise ContractError("scenario archive compressed size does not match its manifest")
    try:
        compressed = archive_path.read_bytes()
    except OSError as error:
        raise ContractError(f"cannot read scenario archive {archive_path}: {error}") from error
    if _sha256_bytes(compressed) != metadata.get("compressedSha256"):
        raise ContractError("scenario archive compressed digest does not match its manifest")
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(compressed), mode="rb") as archive:
            raw = archive.read(MAX_SCENARIO_ARCHIVE_UNCOMPRESSED_BYTES + 1)
    except (EOFError, OSError) as error:
        raise ContractError(f"cannot decompress scenario archive: {error}") from error
    if (
        len(raw) != uncompressed_bytes
        or len(raw) > MAX_SCENARIO_ARCHIVE_UNCOMPRESSED_BYTES
        or _sha256_bytes(raw) != metadata.get("uncompressedSha256")
    ):
        raise ContractError("scenario archive raw digest does not match its manifest")
    if not raw.endswith(b"\n"):
        raise ContractError("canonical scenario archive must end with a newline")
    try:
        scenarios = [json.loads(line) for line in raw.splitlines()]
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContractError(f"scenario archive contains invalid JSONL: {error}") from error
    if len(scenarios) != scenario_count or any(
        not isinstance(scenario, dict) for scenario in scenarios
    ):
        raise ContractError("scenario archive count does not match its manifest")
    if _scenario_jsonl(scenarios) != raw:
        raise ContractError("scenario archive is not canonical JSONL")

    report = {
        key: value
        for key, value in bundle.items()
        if key
        not in {
            "latencySampleCount",
            "reconstructedReportSha256",
            "reportSchemaVersion",
            "scenarioArchive",
        }
    }
    report["schemaVersion"] = bundle.get("reportSchemaVersion")
    report["scenarioReports"] = scenarios
    report["events"] = [
        event
        for scenario in scenarios
        for event in scenario.get("events", [])
        if isinstance(event, dict)
    ]
    authority_oracle = report.get("authorityOracle")
    if not isinstance(authority_oracle, dict):
        raise ContractError("report bundle must retain its authority oracle")
    report["observations"] = merge_observations(scenarios, authority_oracle)
    report["latencySamples"] = collect_scenario_span_samples(
        load_json(CONTRACT_PATH), scenarios
    )
    if len(report["latencySamples"]) != bundle.get("latencySampleCount"):
        raise ContractError("reconstructed latency sample count does not match the bundle")
    if canonical_sha256(report) != bundle.get("reconstructedReportSha256"):
        raise ContractError("reconstructed report digest does not match the bundle")
    return report


def _unique_ids(items: Any, label: str) -> set[str]:
    if not isinstance(items, list):
        raise ContractError(f"{label} must be an array")
    identifiers: list[str] = []
    for item in items:
        if not isinstance(item, dict) or not isinstance(item.get("id"), str):
            raise ContractError(f"every {label} entry must have a string id")
        identifiers.append(item["id"])
    if len(identifiers) != len(set(identifiers)):
        raise ContractError(f"{label} ids must be unique")
    return set(identifiers)


def validate_contract(contract: dict[str, Any]) -> None:
    if contract.get("schemaVersion") != "zhongduan-terminal-journey-contract-v1":
        raise ContractError("unsupported terminal journey contract schema")
    if contract.get("stage") != "E0" or contract.get("status") != "frozen":
        raise ContractError("the E0 benchmark contract must be frozen")
    if _unique_ids(contract.get("oracles"), "oracle") != EXPECTED_ORACLES:
        raise ContractError("the E0 contract must define exactly the eight roadmap oracles")
    if _unique_ids(contract.get("latencySpans"), "latency span") != EXPECTED_SPANS:
        raise ContractError("the E0 contract must define exactly the seven roadmap latency spans")

    matrix = contract.get("matrix")
    if not isinstance(matrix, dict):
        raise ContractError("matrix must be an object")
    if matrix.get("browserCloudRttMs") != [20, 100, 300, 600]:
        raise ContractError("Browser/Cloud RTT matrix must be 20/100/300/600 ms")
    if matrix.get("cloudHostRttMs") != [20, 100, 300, 600]:
        raise ContractError("Cloud/Host RTT matrix must be 20/100/300/600 ms")
    required_dimensions = {
        "networkFaults": {"jitter", "disconnect", "reconnect"},
        "loadAndRecovery": {"output-flood", "cold-attach"},
        "lifecycleAndReplacement": {"do-hibernation", "host-relay-replacement"},
    }
    for name, required in required_dimensions.items():
        values = matrix.get(name)
        if not isinstance(values, list) or not required.issubset(values):
            raise ContractError(f"matrix.{name} is missing required scenarios")
    staging = matrix.get("stagingRequired")
    if not isinstance(staging, list) or "do-hibernation" not in staging:
        raise ContractError("real DO hibernation must remain a staging-required scenario")

    workload = contract.get("workload")
    if not isinstance(workload, dict) or workload.get("id") != "raw-semantic-pty-v1":
        raise ContractError("the workload must identify the raw semantic PTY journey")
    if workload.get("samplesPerVariant") != 24 or workload.get("warmupSamples") != 4:
        raise ContractError("the E0 workload must retain 24 measured and 4 warmup samples")
    required_variants = workload.get("requiredVariants")
    if not isinstance(required_variants, list) or set(required_variants) != EXPECTED_VARIANTS:
        raise ContractError("the E0 workload must define every executable comparison variant")
    thresholds = contract.get("relativeThresholds")
    if not isinstance(thresholds, dict) or set(thresholds) != EXPECTED_THRESHOLDS:
        raise ContractError("all three relative threshold families must be source-controlled")
    for threshold_id, threshold in thresholds.items():
        if not isinstance(threshold, dict):
            raise ContractError(f"relative threshold {threshold_id} must be an object")
        if threshold.get("status") != "frozen-relative-fail-closed":
            raise ContractError(f"relative threshold {threshold_id} must be frozen")
        if threshold.get("maximumRegressionRatioToCurrent") != 1.0:
            raise ContractError(
                f"relative threshold {threshold_id} must reject regressions from CURRENT"
            )
        if threshold.get("requireFiniteCurrentMeasurement") is not True:
            raise ContractError(
                f"relative threshold {threshold_id} must fail closed without CURRENT evidence"
            )
        source = threshold.get("currentMeasurementSource")
        if not isinstance(source, str) or not source.startswith(
            "current-baseline.json#/relativeThresholdMeasurements/"
        ):
            raise ContractError(
                f"relative threshold {threshold_id} must bind its CURRENT measurement"
            )
    deadline = contract.get("deadlinePolicy")
    if not isinstance(deadline, dict) or "never evaluated as a latency SLO" not in str(
        deadline.get("statement", "")
    ):
        raise ContractError("the hang deadline must be distinguished from latency thresholds")


def validate_authority_oracle(authority_oracle: dict[str, Any]) -> None:
    """Validate derivable snapshot-on/off authority evidence and its provenance."""

    if authority_oracle.get("schemaVersion") != "zhongduan-e0-authority-oracle-v3":
        raise ContractError("unsupported E0 authority oracle schema")
    if authority_oracle.get("artifactVerified") is not True:
        raise ContractError("authority oracle must use the verified committed Ghostty artifact")
    if not isinstance(authority_oracle.get("engineId"), str):
        raise ContractError("authority oracle must identify its Ghostty engine")
    for field in ("sourceRevision", "sourceTreeGitOid"):
        value = authority_oracle.get(field)
        if not isinstance(value, str) or len(value) != 40 or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise ContractError(f"authority oracle must identify its exact {field}")
    if authority_oracle.get("sourceTreeDirty") is not False:
        raise ContractError("mergeable authority evidence must come from a clean committed tree")

    corpus = authority_oracle.get("corpus")
    if not isinstance(corpus, list) or not corpus:
        raise ContractError("authority oracle must retain its snapshot corpus")
    state_fields = (
        "snapshotCaptureStateEqual",
        "checkpointSourceStateEqual",
        "recoveredStateEqual",
        "normalizedStateEqual",
        "checkpointSourceContinuationEqual",
        "recoveredContinuationEqual",
        "continuationEqual",
    )
    for case in corpus:
        if (
            not isinstance(case, dict)
            or not isinstance(case.get("id"), str)
            or any(not isinstance(case.get(field), bool) for field in state_fields)
        ):
            raise ContractError("authority snapshot corpus has an invalid comparison case")
        expected_state = (
            case["snapshotCaptureStateEqual"]
            and case["checkpointSourceStateEqual"]
            and case["recoveredStateEqual"]
        )
        expected_continuation = (
            case["checkpointSourceContinuationEqual"]
            and case["recoveredContinuationEqual"]
        )
        if (
            case["normalizedStateEqual"] != expected_state
            or case["continuationEqual"] != expected_continuation
        ):
            raise ContractError("authority snapshot case aggregate is not derivable")

    effect_corpus = authority_oracle.get("effectCorpus")
    effect_cases = effect_corpus.get("cases") if isinstance(effect_corpus, dict) else None
    if not isinstance(effect_cases, list) or not effect_cases:
        raise ContractError("authority oracle must retain its effect corpus")
    if any(
        not isinstance(case, dict)
        or not isinstance(case.get("id"), str)
        or not isinstance(case.get("effectsEqual"), bool)
        for case in effect_cases
    ):
        raise ContractError("authority effect corpus has an invalid comparison case")

    expected = {
        "snapshotCaptureStateEqual": all(
            case["snapshotCaptureStateEqual"] for case in corpus
        ),
        "checkpointSourceStateEqual": all(
            case["checkpointSourceStateEqual"] for case in corpus
        ),
        "recoveredStateEqual": all(case["recoveredStateEqual"] for case in corpus),
        "normalizedStateEqual": all(case["normalizedStateEqual"] for case in corpus),
        "checkpointSourceContinuationEqual": all(
            case["checkpointSourceContinuationEqual"] for case in corpus
        ),
        "recoveredContinuationEqual": all(
            case["recoveredContinuationEqual"] for case in corpus
        ),
        "continuationEqual": all(case["continuationEqual"] for case in corpus),
        "effectsEqual": all(case["effectsEqual"] for case in effect_cases),
    }
    comparison = authority_oracle.get("comparison")
    if not isinstance(comparison, dict) or any(
        comparison.get(field) != value for field, value in expected.items()
    ):
        raise ContractError("authority oracle aggregate does not match its retained corpus")
    if (
        comparison.get("corpusCaseCount") != len(corpus)
        or comparison.get("effectCaseCount") != len(effect_cases)
        or effect_corpus.get("effectsEqual") != expected["effectsEqual"]
    ):
        raise ContractError("authority oracle corpus counts or effect aggregate do not match")


def _result(
    metric: str,
    value: int | None,
    samples: int,
    violations: Iterable[str] = (),
    reason: str | None = None,
) -> dict[str, Any]:
    violation_list = list(violations)
    if value is None:
        status = "not-measured"
    else:
        status = "passed" if value == 0 else "failed"
    result: dict[str, Any] = {
        "status": status,
        "metric": metric,
        "value": value,
        "target": 0,
        "sampleCount": samples,
        "violations": violation_list,
    }
    if reason is not None:
        result["reason"] = reason
    return result


def evaluate_oracles(observations: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Evaluate all eight roadmap gates without treating missing evidence as green."""

    intents = observations.get("intents")
    if not isinstance(intents, list):
        intents = []
    consumed = [item for item in intents if isinstance(item, dict) and item.get("consumed") is True]

    silent_violations: list[str] = []
    for item in consumed:
        outcomes = item.get("terminalOutcomes")
        if not isinstance(outcomes, list) or len(outcomes) != 1 or outcomes[0] not in TERMINAL_OUTCOMES:
            silent_violations.append(str(item.get("localIntentId", "unknown-intent")))
    silent = _result(
        "silentLossCount",
        len(silent_violations) if consumed else None,
        len(consumed),
        silent_violations,
        None if consumed else "no UI-consumed intent observations",
    )

    identified = [
        item
        for item in intents
        if isinstance(item, dict) and isinstance(item.get("wireIdentity"), str)
    ]
    duplicate_violations = [
        str(item["wireIdentity"])
        for item in identified
        if isinstance(item.get("ptyEffectCount"), int) and item["ptyEffectCount"] > 1
    ]
    duplicate = _result(
        "duplicatePtyEffectCount",
        len(duplicate_violations) if identified else None,
        len(identified),
        duplicate_violations,
        None if identified else "no wire-identity effect observations",
    )

    uncertainty = [
        item
        for item in intents
        if isinstance(item, dict) and item.get("acceptanceUncertaintyInjected") is True
    ]
    retry_violations = [
        str(item.get("wireIdentity", item.get("localIntentId", "unknown-intent")))
        for item in uncertainty
        if not isinstance(item.get("automaticRetryCount"), int)
        or item["automaticRetryCount"] != 0
    ]
    uncertain = _result(
        "uncertainAutoRetryCount",
        len(retry_violations) if uncertainty else None,
        len(uncertainty),
        retry_violations,
        None if uncertainty else "acceptance uncertainty was not injected",
    )

    interrupts = observations.get("ctrlC")
    if not isinstance(interrupts, list):
        interrupts = []
    flood_interrupts = [
        item
        for item in interrupts
        if isinstance(item, dict) and item.get("outputFlood") is True
    ]
    interrupt_violations = [
        str(item.get("sampleId", "unknown-ctrl-c"))
        for item in flood_interrupts
        if item.get("ptyEffectCount") != 1
    ]
    ctrl_c = _result(
        "invalidCtrlCEffectCount",
        len(interrupt_violations) if flood_interrupts else None,
        len(flood_interrupts),
        interrupt_violations,
        None if flood_interrupts else "no output-flood Ctrl-C observation",
    )

    transfers = observations.get("writerTransfers")
    if not isinstance(transfers, list):
        transfers = []
    transfer_violations = [
        str(item.get("sampleId", "unknown-transfer"))
        for item in transfers
        if isinstance(item, dict) and item.get("oldWriterSuccessfulEffects", 0) != 0
    ]
    writer_transfer = _result(
        "oldWriterSuccessfulEffectCount",
        len(transfer_violations) if transfers else None,
        len(transfers),
        transfer_violations,
        None if transfers else "writer transfer was not exercised",
    )

    candidates = observations.get("coldCandidates")
    if not isinstance(candidates, list):
        candidates = []
    candidate_violations = [
        str(item.get("sampleId", "unknown-candidate"))
        for item in candidates
        if isinstance(item, dict) and item.get("visibleBeforeValidation") is not False
    ]
    cold_candidate = _result(
        "prematureCandidateVisibilityCount",
        len(candidate_violations) if candidates else None,
        len(candidates),
        candidate_violations,
        None if candidates else "cold attach was not exercised",
    )

    authority = observations.get("authorityComparisons")
    if not isinstance(authority, list):
        authority = []
    authority_violations = [
        str(item.get("sampleId", "unknown-authority-comparison"))
        for item in authority
        if isinstance(item, dict)
        and (
            item.get("normalizedStateEqual") is not True
            or item.get("continuationEqual") is not True
            or item.get("effectsEqual") is not True
        )
    ]
    authority_result = _result(
        "authorityDivergenceCount",
        len(authority_violations) if authority else None,
        len(authority),
        authority_violations,
        None if authority else "snapshot-on/off authority comparison was not exercised",
    )

    secure = observations.get("secureInput")
    if not isinstance(secure, list):
        secure = []
    secure_violations = [
        str(item.get("sampleId", "unknown-secure-input"))
        for item in secure
        if isinstance(item, dict) and item.get("speculativePresentationCount") != 0
    ]
    secure_result = _result(
        "secureInputSpeculationCount",
        len(secure_violations) if secure else None,
        len(secure),
        secure_violations,
        None if secure else "secure-input presentation was not exercised",
    )

    return {
        "ui-consumed-silent-loss": silent,
        "duplicate-pty-effect": duplicate,
        "uncertain-auto-retry": uncertain,
        "output-flood-ctrl-c-once": ctrl_c,
        "old-writer-effect-after-transfer": writer_transfer,
        "cold-candidate-visible-before-validation": cold_candidate,
        "snapshot-authority-divergence": authority_result,
        "secure-input-speculative-presentation": secure_result,
    }


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        raise ContractError("cannot calculate a percentile without samples")
    if not 0 <= quantile <= 1:
        raise ContractError("percentile quantile must be in [0, 1]")
    ordered = sorted(values)
    rank = max(1, math.ceil(quantile * len(ordered)))
    return ordered[rank - 1]


def collect_span_samples(
    contract: dict[str, Any], events: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, str, str], list[int]] = defaultdict(list)
    for event in events:
        if not isinstance(event, dict):
            continue
        name = event.get("name")
        sample_id = event.get("sampleId")
        variant = event.get("variant")
        at_ns = event.get("atUnixNs")
        if (
            isinstance(name, str)
            and isinstance(sample_id, str)
            and isinstance(variant, str)
            and isinstance(at_ns, int)
            and at_ns >= 0
        ):
            by_key[(sample_id, variant, name)].append(at_ns)

    samples: list[dict[str, Any]] = []
    for span in contract["latencySpans"]:
        span_id = span["id"]
        start_name = span["startEvent"]
        end_name = span["endEvent"]
        keys = {(sample_id, variant) for sample_id, variant, name in by_key if name == start_name}
        for sample_id, variant in sorted(keys):
            starts = by_key[(sample_id, variant, start_name)]
            ends = by_key.get((sample_id, variant, end_name), [])
            if len(starts) != 1 or len(ends) != 1 or ends[0] < starts[0]:
                continue
            samples.append(
                {
                    "span": span_id,
                    "sampleId": sample_id,
                    "variant": variant,
                    "durationMs": round((ends[0] - starts[0]) / 1_000_000, 6),
                }
            )
    return samples


def collect_scenario_span_samples(
    contract: dict[str, Any], scenarios: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Pair span endpoints inside each scenario before combining matrix samples."""

    samples: list[dict[str, Any]] = []
    for scenario in scenarios:
        events = scenario.get("events")
        if isinstance(events, list):
            samples.extend(collect_span_samples(contract, events))
    return samples


def summarize_span_samples(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        span = sample.get("span")
        variant = sample.get("variant")
        duration = sample.get("durationMs")
        if isinstance(span, str) and isinstance(variant, str) and isinstance(
            duration, (int, float)
        ):
            grouped[(span, variant)].append(float(duration))
    summaries: list[dict[str, Any]] = []
    for (span, variant), values in sorted(grouped.items()):
        summaries.append(
            {
                "span": span,
                "variant": variant,
                "sampleCount": len(values),
                "p50Ms": round(percentile(values, 0.50), 6),
                "p95Ms": round(percentile(values, 0.95), 6),
                "p99Ms": round(percentile(values, 0.99), 6),
                "maxMs": round(max(values), 6),
            }
        )
    return summaries


def measure_relative_thresholds(
    summaries: list[dict[str, Any]],
    samples: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Measure the three relative denominators, failing closed on partial evidence."""

    p99_by_variant: dict[tuple[str, str], float] = {}
    for summary in summaries:
        if not isinstance(summary, dict):
            continue
        span = summary.get("span")
        variant = summary.get("variant")
        p99 = summary.get("p99Ms")
        sample_count = summary.get("sampleCount")
        if (
            isinstance(span, str)
            and isinstance(variant, str)
            and isinstance(p99, (int, float))
            and math.isfinite(float(p99))
            and p99 >= 0
            and isinstance(sample_count, int)
            and sample_count >= MIN_MEASURED_SAMPLES
        ):
            p99_by_variant[(span, variant)] = float(p99)

    def missing(metric: str, variants: list[str]) -> dict[str, Any]:
        return {
            "status": "not-measured",
            "metric": metric,
            "value": None,
            "requiredVariants": variants,
            "reason": "complete finite p99 samples for every required variant are unavailable",
        }

    cloud_variants = [
        "bulk-backlog-0",
        "bulk-backlog-262144",
        "bulk-backlog-1048576",
        "bulk-backlog-4194304",
    ]
    cloud_samples: dict[str, list[float]] = defaultdict(list)
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        variant = sample.get("variant")
        sample_id = sample.get("sampleId")
        duration = sample.get("durationMs")
        if (
            sample.get("span") == "cloud-browser-receive-to-host-send"
            and variant in cloud_variants
            and isinstance(sample_id, str)
            and sample_id.startswith("probe-measured-")
            and isinstance(duration, (int, float))
            and math.isfinite(float(duration))
            and duration >= 0
        ):
            cloud_samples[variant].append(float(duration))
    cloud_counts = {variant: len(cloud_samples[variant]) for variant in cloud_variants}
    cloud_p99 = {
        variant: percentile(cloud_samples[variant], 0.99)
        for variant in cloud_variants
        if len(cloud_samples[variant]) >= MIN_MEASURED_SAMPLES
    }
    cloud_values = [cloud_p99.get(variant) for variant in cloud_variants]
    cloud = missing(
        "one-plus-nonnegative-normalized-p99-slope-per-MiB", cloud_variants
    )
    cloud.update(
        {
            "sampleSelector": {
                "span": "cloud-browser-receive-to-host-send",
                "sampleIdPrefix": "probe-measured-",
            },
            "sampleCounts": cloud_counts,
        }
    )
    if all(value is not None for value in cloud_values) and cloud_values[0] > 0:
        x_values = [0.0, 0.25, 1.0, 4.0]
        normalized = [float(value) / cloud_values[0] for value in cloud_values]
        x_mean = sum(x_values) / len(x_values)
        y_mean = sum(normalized) / len(normalized)
        denominator = sum((value - x_mean) ** 2 for value in x_values)
        slope = sum(
            (x_value - x_mean) * (y_value - y_mean)
            for x_value, y_value in zip(x_values, normalized, strict=True)
        ) / denominator
        comparison_denominator = 1.0 + max(0.0, slope)
        cloud = {
            "status": "measured",
            "metric": "one-plus-nonnegative-normalized-p99-slope-per-MiB",
            "value": round(comparison_denominator, 9),
            "rawNormalizedSlopePerMiB": round(slope, 9),
            "requiredVariants": cloud_variants,
            "probeP99MsByVariant": {
                variant: round(cloud_p99[variant], 6) for variant in cloud_variants
            },
            "sampleSelector": {
                "span": "cloud-browser-receive-to-host-send",
                "sampleIdPrefix": "probe-measured-",
            },
            "sampleCounts": cloud_counts,
        }

    def ratio_measurement(
        span: str, baseline_variant: str, candidate_variant: str, metric: str
    ) -> dict[str, Any]:
        variants = [baseline_variant, candidate_variant]
        baseline = p99_by_variant.get((span, baseline_variant))
        candidate = p99_by_variant.get((span, candidate_variant))
        if baseline is None or candidate is None or baseline <= 0:
            return missing(metric, variants)
        return {
            "status": "measured",
            "metric": metric,
            "value": round(candidate / baseline, 9),
            "baselineP99Ms": baseline,
            "candidateP99Ms": candidate,
            "requiredVariants": variants,
        }

    return {
        "cloudBulkIsolation": cloud,
        "snapshotHostInput": ratio_measurement(
            "host-receive-to-pty-write",
            "snapshot-disabled",
            "snapshot-enabled",
            "snapshot-enabled/snapshot-disabled",
        ),
        "outputFloodCtrlC": ratio_measurement(
            "ctrl-c-to-application-quiet",
            "steady",
            "output-flood",
            "output-flood/steady",
        ),
    }


def compare_relative_thresholds(
    contract: dict[str, Any],
    current: dict[str, dict[str, Any]],
    candidate: dict[str, dict[str, Any]],
    candidate_oracles: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Compare a candidate with a finite CURRENT denominator without zero division."""

    comparisons: dict[str, dict[str, Any]] = {}
    for threshold_id in sorted(EXPECTED_THRESHOLDS):
        current_result = current.get(threshold_id)
        candidate_result = candidate.get(threshold_id)
        threshold = contract["relativeThresholds"][threshold_id]
        required_oracle = threshold.get("requiresCorrectnessOracle")
        reason: str | None = None
        if not isinstance(current_result, dict) or current_result.get("status") != "measured":
            reason = "CURRENT denominator is not measured"
        elif not isinstance(candidate_result, dict) or candidate_result.get("status") != "measured":
            reason = "candidate measurement is not measured"
        elif isinstance(required_oracle, str) and candidate_oracles.get(required_oracle, {}).get(
            "status"
        ) != "passed":
            reason = f"required correctness oracle {required_oracle} did not pass"

        current_value = None if current_result is None else current_result.get("value")
        candidate_value = None if candidate_result is None else candidate_result.get("value")
        if reason is None and (
            not isinstance(current_value, (int, float))
            or not math.isfinite(float(current_value))
            or current_value <= 0
        ):
            reason = "CURRENT denominator must be finite and greater than zero"
        if reason is None and (
            not isinstance(candidate_value, (int, float))
            or not math.isfinite(float(candidate_value))
            or candidate_value < 0
        ):
            reason = "candidate measurement must be finite and non-negative"
        if reason is not None:
            comparisons[threshold_id] = {
                "status": "not-measured",
                "reason": reason,
                "ratio": None,
            }
            continue

        assert isinstance(current_value, (int, float))
        assert isinstance(candidate_value, (int, float))
        ratio = float(candidate_value) / float(current_value)
        maximum = float(threshold["maximumRegressionRatioToCurrent"])
        comparisons[threshold_id] = {
            "status": "passed" if ratio <= maximum else "failed",
            "ratio": round(ratio, 9),
            "maximumRatio": maximum,
        }
    return comparisons


OBSERVATION_COLLECTIONS = (
    "intents",
    "ctrlC",
    "writerTransfers",
    "coldCandidates",
    "secureInput",
)


def _validate_terminal_intents(intents: Any, label: str) -> list[dict[str, Any]]:
    if not isinstance(intents, list):
        raise ContractError(f"{label} must be an array")
    local_ids: set[str] = set()
    validated: list[dict[str, Any]] = []
    for item in intents:
        if not isinstance(item, dict):
            raise ContractError(f"{label} must contain objects")
        local_id = item.get("localIntentId")
        sample_id = item.get("sampleId")
        outcomes = item.get("terminalOutcomes")
        records = item.get("terminalRecords")
        if (
            item.get("consumed") is not True
            or not isinstance(local_id, str)
            or not local_id
            or not isinstance(sample_id, str)
            or not sample_id
            or not isinstance(outcomes, list)
            or len(outcomes) > 1
            or any(outcome not in TERMINAL_OUTCOMES for outcome in outcomes)
            or not isinstance(records, list)
            or len(records) != len(outcomes)
            or (
                len(records) == 1
                and (
                    records[0].get("localIntentId") != local_id
                    or records[0].get("outcome") != outcomes[0]
                )
            )
        ):
            raise ContractError(
                f"every {label} item must retain zero or one passive terminal outcome"
            )
        if local_id in local_ids:
            raise ContractError(f"{label} LocalIntentIds must be unique")
        local_ids.add(local_id)
        validated.append(item)
    return validated


def _validate_observation_effects(
    observations: dict[str, Any], events: list[dict[str, Any]], label: str
) -> None:
    attempts: dict[str, list[str]] = defaultdict(list)
    full_identities: dict[str, list[str]] = defaultdict(list)
    effects: dict[str, int] = defaultdict(int)
    for event in events:
        if not isinstance(event, dict):
            continue
        sample_id = event.get("sampleId")
        if not isinstance(sample_id, str):
            continue
        if event.get("name") == "cloud.browser-receive-attempt" and isinstance(
            event.get("browserIdentity"), str
        ):
            attempts[sample_id].append(event["browserIdentity"])
        if event.get("name") == "host.receive" and isinstance(
            event.get("wireIdentity"), str
        ):
            full_identities[sample_id].append(event["wireIdentity"])
        if event.get("name") == "host.pty-write":
            effects[sample_id] += 1

    for item in observations.get("intents", []):
        sample_id = item["sampleId"]
        sample_attempts = attempts[sample_id]
        browser_identities = sorted(set(sample_attempts))
        identities = sorted(set(full_identities[sample_id]))
        if item.get("ptyEffectCount") != effects[sample_id]:
            raise ContractError(f"{label} PTY effect count is not derived from raw events")
        if (
            item.get("passiveBrowserIdentities") != browser_identities
            or item.get("passiveSendAttemptCount") != len(sample_attempts)
        ):
            raise ContractError(f"{label} passive send evidence disagrees with proxy events")
        if browser_identities:
            if (
                item.get("browserIdentity") != sample_attempts[0]
                or item.get("browserIdentities") != browser_identities
            ):
                raise ContractError(
                    f"{label} Browser identities are not derived from raw events"
                )
        elif "browserIdentity" in item or "browserIdentities" in item:
            raise ContractError(f"{label} claims a Browser identity without a send event")
        if identities:
            if (
                item.get("wireIdentity") != full_identities[sample_id][0]
                or item.get("wireIdentities") != identities
            ):
                raise ContractError(f"{label} wire identities are not derived from raw events")
            terminal_identity = (
                item["terminalRecords"][0].get("identity")
                if item.get("terminalRecords")
                else None
            )
            if isinstance(terminal_identity, dict):
                terminal_wire = "/".join(
                    str(terminal_identity.get(name))
                    for name in ("writerFence", "inputEpoch", "clientInputSeq")
                )
                if terminal_wire not in identities:
                    raise ContractError(
                        f"{label} terminal identity is absent from observed sends"
                    )
        elif "wireIdentity" in item or "wireIdentities" in item:
            raise ContractError(f"{label} claims a wire identity without a send event")
        if item.get("acceptanceUncertaintyInjected") is True:
            expected_retry_count = max(0, len(sample_attempts) - 1)
            if (
                item.get("automaticRetryCount") != expected_retry_count
                or item.get("identityChanged") is not (len(browser_identities) > 1)
            ):
                raise ContractError(
                    f"{label} uncertainty retry evidence is not derived from send attempts"
                )

    for item in observations.get("ctrlC", []):
        sample_id = item.get("sampleId")
        if not isinstance(sample_id, str) or item.get("ptyEffectCount") != effects[sample_id]:
            raise ContractError(f"{label} Ctrl-C effect count is not derived from raw events")


def validate_scenario_report(
    scenario: dict[str, Any], contract: dict[str, Any], *, require_clean: bool = True
) -> None:
    """Validate raw single-variant evidence before it can enter CURRENT."""

    validate_contract(contract)
    if scenario.get("schemaVersion") != "zhongduan-terminal-journey-scenario-v1":
        raise ContractError("unsupported E0 scenario schema")
    if scenario.get("status") != "measured":
        raise ContractError("an E0 scenario must complete before it can be merged")
    if scenario.get("contractSha256") != canonical_sha256(contract):
        raise ContractError("scenario does not reference the exact benchmark contract")
    variant = scenario.get("variant")
    if variant not in EXPECTED_VARIANTS:
        raise ContractError("scenario uses an unknown E0 variant")
    workload = contract["workload"]
    if scenario.get("samples") != workload["samplesPerVariant"]:
        raise ContractError("scenario did not collect exactly 24 measured samples")
    if scenario.get("warmups") != workload["warmupSamples"]:
        raise ContractError("scenario did not execute exactly four warmup samples")
    if require_clean and scenario.get("sourceTreeDirty") is not False:
        raise ContractError("mergeable E0 scenarios must come from a clean source tree")
    for field in ("sourceRevision", "sourceTreeGitOid"):
        value = scenario.get(field)
        if not isinstance(value, str) or len(value) != 40 or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise ContractError(f"scenario {field} must be an exact Git object id")

    evidence = scenario.get("workloadEvidence")
    if not isinstance(evidence, dict):
        raise ContractError("scenario must retain executable workload evidence")
    primary = evidence.get("primarySampleIds")
    warmup_primary = evidence.get("warmupPrimarySampleIds")
    if (
        not isinstance(primary, list)
        or len(primary) != workload["samplesPerVariant"]
        or len(set(primary)) != len(primary)
    ):
        raise ContractError("workload evidence must identify 24 unique measured samples")
    if (
        not isinstance(warmup_primary, list)
        or len(warmup_primary) != workload["warmupSamples"]
        or len(set(warmup_primary)) != len(warmup_primary)
    ):
        raise ContractError("workload evidence must identify four unique warmup samples")
    measurement_start = evidence.get("measurementStartedAtUnixMs")
    measurement_end = evidence.get("measurementEndedAtUnixMs")
    if (
        not isinstance(measurement_start, int)
        or not isinstance(measurement_end, int)
        or measurement_start > measurement_end
    ):
        raise ContractError("workload evidence must retain its measurement window")

    observations = scenario.get("observations")
    warmup_observations = scenario.get("warmupObservations")
    if not isinstance(observations, dict) or not isinstance(warmup_observations, dict):
        raise ContractError("scenario must retain measured and warmup observations")
    measured_intents = _validate_terminal_intents(
        observations.get("intents"), "measured intents"
    )
    warmup_intents = _validate_terminal_intents(
        warmup_observations.get("intents"), "warmup intents"
    )
    all_local_ids = [item["localIntentId"] for item in measured_intents + warmup_intents]
    if len(all_local_ids) != len(set(all_local_ids)):
        raise ContractError("LocalIntentIds must remain unique across warmup and measurement")

    events = scenario.get("events")
    if not isinstance(events, list) or any(
        not isinstance(event, dict) or event.get("variant") != variant for event in events
    ):
        raise ContractError("scenario must retain raw events under its actual variant")
    consumed_samples = sorted(
        event.get("sampleId")
        for event in events
        if event.get("name") == "browser.input-consumed"
        and isinstance(event.get("sampleId"), str)
    )
    observed_samples = sorted(item["sampleId"] for item in measured_intents)
    if consumed_samples != observed_samples:
        raise ContractError(
            "every measured UI-consumed event must map to one terminal intent observation"
        )
    if scenario.get("rawEventCount") != len(events):
        raise ContractError("scenario rawEventCount does not match retained events")
    _validate_observation_effects(observations, events, "measured observations")
    if scenario.get("deadlineIsSlo") is not False:
        raise ContractError("scenario deadline must not be reported as a latency SLO")
    cloud_boundary = scenario.get("cloudSpanBoundary")
    if (
        not isinstance(cloud_boundary, dict)
        or cloud_boundary.get("includesBrowserLink") is not False
        or cloud_boundary.get("includesHostLink") is not False
    ):
        raise ContractError("Cloud span boundary must exclude both simulated links")
    host_measurements = scenario.get("hostMeasurements", [])
    if not isinstance(host_measurements, list) or any(
        not isinstance(item, dict) for item in host_measurements
    ):
        raise ContractError("scenario Host measurements must be raw JSONL objects")

    ctrl_c = observations.get("ctrlC")
    warmup_ctrl_c = warmup_observations.get("ctrlC")
    if not isinstance(ctrl_c, list) or not isinstance(warmup_ctrl_c, list):
        raise ContractError("Ctrl-C observations must be retained as arrays")
    configured_bulk = evidence.get("configuredBulkBacklogBytes")
    expected_bulk = (
        int(variant.removeprefix("bulk-backlog-"))
        if variant.startswith("bulk-backlog-")
        else 0
    )
    if configured_bulk != expected_bulk:
        raise ContractError("bulk backlog variant label does not match its applied bytes")

    if variant == "steady":
        if len(ctrl_c) != 24 or any(item.get("outputFlood") is not False for item in ctrl_c):
            raise ContractError("steady must execute 24 non-flood Ctrl-C samples")
    elif variant == "output-flood":
        if (
            evidence.get("outputFlood") is not True
            or len(ctrl_c) != 24
            or any(item.get("outputFlood") is not True for item in ctrl_c)
        ):
            raise ContractError("output-flood must execute 24 real flood Ctrl-C samples")
    elif variant.startswith("bulk-backlog-"):
        if evidence.get("outputFlood") is not (expected_bulk > 0):
            raise ContractError("bulk variant did not apply its declared output workload")
    elif variant == "snapshot-disabled":
        if evidence.get("snapshotFinalizationsDuringMeasurement") != 0:
            raise ContractError("snapshot-disabled observed snapshot work during measurement")
        if evidence.get("snapshotInputOverlap") is not None:
            raise ContractError("snapshot-disabled must not claim snapshot/input overlap")
    elif variant == "snapshot-enabled":
        count = evidence.get("snapshotFinalizationsDuringMeasurement")
        if not isinstance(count, int) or count < 1:
            raise ContractError("snapshot-enabled did not finalize a background snapshot")
        overlap = evidence.get("snapshotInputOverlap")
        if not isinstance(overlap, dict):
            raise ContractError("snapshot-enabled lacks snapshot/input overlap evidence")
        first_at = overlap.get("firstHostReceiveAtUnixNs")
        finalized_at = overlap.get("snapshotFinalizationAtUnixNs")
        last_at = overlap.get("lastHostReceiveAtUnixNs")
        first_sample = overlap.get("firstSampleId")
        last_sample = overlap.get("lastSampleId")
        snapshot_id = overlap.get("snapshotId")
        if (
            not all(isinstance(value, int) for value in (first_at, finalized_at, last_at))
            or not isinstance(first_sample, str)
            or not isinstance(last_sample, str)
            or not isinstance(snapshot_id, str)
            or not first_at < finalized_at < last_at
        ):
            raise ContractError(
                "snapshot-enabled finalization must fall inside its overlap Host input window"
            )
        if not any(
            event.get("name") == "host.receive"
            and event.get("sampleId") == first_sample
            and event.get("atUnixNs") == first_at
            for event in events
        ) or not any(
            event.get("name") == "host.receive"
            and event.get("sampleId") == last_sample
            and event.get("atUnixNs") == last_at
            for event in events
        ):
            raise ContractError("snapshot overlap Host input evidence is not retained raw")
        if not any(
            event.get("name") == "host.snapshot-finalized"
            and event.get("snapshotId") == snapshot_id
            and event.get("atUnixNs") == finalized_at
            for event in events
        ):
            raise ContractError("snapshot overlap finalization evidence is not retained raw")
    elif variant == "correctness-faults":
        if not all(
            evidence.get(name) is True
            for name in (
                "acceptanceDisconnect",
                "acceptanceReconnectObserved",
                "writerTransfer",
                "coldAttachValidation",
            )
        ):
            raise ContractError("correctness-faults did not execute every declared fault")
        if not any(item.get("acceptanceUncertaintyInjected") is True for item in measured_intents):
            raise ContractError("correctness-faults lacks acceptance-uncertainty evidence")
        if not observations.get("writerTransfers") or not observations.get("coldCandidates"):
            raise ContractError("correctness-faults lacks transfer or cold-attach evidence")
        if not observations.get("secureInput"):
            raise ContractError("correctness-faults lacks secure-input evidence")

    measured_sample_ids = {item["sampleId"] for item in measured_intents}
    for primary_sample in primary:
        if variant == "output-flood":
            required = {
                f"flood-command-ctrl-c-{primary_sample}",
                f"arm-ctrl-c-{primary_sample}",
                f"ctrl-c-{primary_sample}",
            }
        elif variant == "steady":
            required = {
                f"probe-{primary_sample}",
                f"arm-ctrl-c-{primary_sample}",
                f"ctrl-c-{primary_sample}",
            }
        elif variant.startswith("bulk-backlog-") and expected_bulk > 0:
            required = {f"flood-command-{primary_sample}", f"probe-{primary_sample}"}
        else:
            required = {f"probe-{primary_sample}"}
        if not required.issubset(measured_sample_ids):
            raise ContractError("variant label is not backed by every primary workload sample")


def merge_observations(
    scenarios: list[dict[str, Any]], authority_oracle: dict[str, Any]
) -> dict[str, Any]:
    validate_authority_oracle(authority_oracle)
    merged: dict[str, Any] = {name: [] for name in OBSERVATION_COLLECTIONS}
    for scenario in scenarios:
        observations = scenario.get("observations")
        if not isinstance(observations, dict):
            raise ContractError("scenario must retain its raw correctness observations")
        for name in OBSERVATION_COLLECTIONS:
            values = observations.get(name, [])
            if not isinstance(values, list):
                raise ContractError(f"scenario observations.{name} must be an array")
            merged[name].extend(values)
    comparison = authority_oracle.get("comparison")
    if not isinstance(comparison, dict):
        raise ContractError("authority oracle must retain its comparison")
    merged["authorityComparisons"] = [comparison]
    return merged


def assemble_current_report(
    contract: dict[str, Any],
    scenarios: list[dict[str, Any]],
    authority_oracle: dict[str, Any],
    *,
    environment: dict[str, Any],
    evidence_boundary: dict[str, Any],
    generated_at: str,
    source_revision: str,
    source_tree_git_oid: str,
    deadline_ms: int,
) -> dict[str, Any]:
    """Merge independently executed variants into one derivable CURRENT report."""

    observations = merge_observations(scenarios, authority_oracle)
    events = [
        event
        for scenario in scenarios
        for event in scenario.get("events", [])
        if isinstance(event, dict)
    ]
    latency_samples = collect_scenario_span_samples(contract, scenarios)
    latency_summaries = summarize_span_samples(latency_samples)
    oracle_results = evaluate_oracles(observations)
    relative = measure_relative_thresholds(latency_summaries, latency_samples)
    measured_spans = {sample["span"] for sample in latency_samples}
    workload = contract["workload"]
    report = {
        "schemaVersion": "zhongduan-terminal-journey-report-v1",
        "contractSha256": canonical_sha256(contract),
        "baseline": "CURRENT",
        "baselineStatus": "complete",
        "sourceRevision": source_revision,
        "sourceTreeGitOid": source_tree_git_oid,
        "sourceTreeDirty": False,
        "generatedAt": generated_at,
        "environment": environment,
        "workload": {
            "id": workload["id"],
            "samplesPerVariant": workload["samplesPerVariant"],
            "warmupSamples": workload["warmupSamples"],
        },
        "evidenceBoundary": evidence_boundary,
        "scenarioReports": scenarios,
        "matrixCoverage": coverage_for_scenarios(contract, scenarios),
        "oracleResults": oracle_results,
        "currentFailures": current_failures(oracle_results, relative),
        "events": events,
        "latencySamples": latency_samples,
        "latencySummaries": latency_summaries,
        "unmeasuredLatencySpans": sorted(EXPECTED_SPANS - measured_spans),
        "relativeThresholdMeasurements": relative,
        "authorityOracle": authority_oracle,
        "observations": observations,
        "rawEventCount": len(events),
        "deadlineMs": deadline_ms,
        "deadlineIsSlo": False,
    }
    validate_report(report, contract)
    return report


def assemble_candidate_report(
    contract: dict[str, Any],
    scenarios: list[dict[str, Any]],
    authority_oracle: dict[str, Any],
    current_report: dict[str, Any],
    *,
    environment: dict[str, Any],
    evidence_boundary: dict[str, Any],
    generated_at: str,
    source_revision: str,
    source_tree_git_oid: str,
    deadline_ms: int,
    snapshot_phase_measurements: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a candidate artifact directly from raw candidate scenarios."""

    validate_report(current_report, contract)
    observations = merge_observations(scenarios, authority_oracle)
    events = [
        event
        for scenario in scenarios
        for event in scenario.get("events", [])
        if isinstance(event, dict)
    ]
    latency_samples = collect_scenario_span_samples(contract, scenarios)
    latency_summaries = summarize_span_samples(latency_samples)
    oracle_results = evaluate_oracles(observations)
    relative = measure_relative_thresholds(latency_summaries, latency_samples)
    comparisons = compare_relative_thresholds(
        contract,
        current_report["relativeThresholdMeasurements"],
        relative,
        oracle_results,
    )
    measured_spans = {sample["span"] for sample in latency_samples}
    workload = contract["workload"]
    intrinsic_failures = current_failures(oracle_results, relative)
    comparison_failures = [
        {
            "gate": f"candidate-comparison/{threshold_id}",
            "status": result.get("status"),
            "reason": result.get("reason", "candidate regressed CURRENT"),
        }
        for threshold_id, result in sorted(comparisons.items())
        if result.get("status") != "passed"
    ]
    report = {
        "schemaVersion": "zhongduan-terminal-journey-candidate-v1",
        "contractSha256": canonical_sha256(contract),
        "baseline": "CANDIDATE",
        "candidateStatus": "complete",
        "sourceRevision": source_revision,
        "sourceTreeGitOid": source_tree_git_oid,
        "sourceTreeDirty": False,
        "generatedAt": generated_at,
        "environment": environment,
        "workload": {
            "id": workload["id"],
            "samplesPerVariant": workload["samplesPerVariant"],
            "warmupSamples": workload["warmupSamples"],
        },
        "evidenceBoundary": evidence_boundary,
        "scenarioReports": scenarios,
        "matrixCoverage": coverage_for_scenarios(contract, scenarios),
        "oracleResults": oracle_results,
        "candidateFailures": [*intrinsic_failures, *comparison_failures],
        "comparisonToCurrent": {
            "currentSourceRevision": current_report["sourceRevision"],
            "currentSourceTreeGitOid": current_report["sourceTreeGitOid"],
            "relativeThresholds": comparisons,
        },
        "events": events,
        "latencySamples": latency_samples,
        "latencySummaries": latency_summaries,
        "unmeasuredLatencySpans": sorted(EXPECTED_SPANS - measured_spans),
        "relativeThresholdMeasurements": relative,
        "authorityOracle": authority_oracle,
        "observations": observations,
        "rawEventCount": len(events),
        "snapshotPhaseMeasurements": snapshot_phase_measurements or [],
        "deadlineMs": deadline_ms,
        "deadlineIsSlo": False,
    }
    validate_candidate_report(report, current_report, contract)
    return report


def validate_candidate_report(
    report: dict[str, Any], current_report: dict[str, Any], contract: dict[str, Any]
) -> None:
    """Validate candidate evidence without accepting a relabelled CURRENT artifact."""

    if report.get("schemaVersion") != "zhongduan-terminal-journey-candidate-v1":
        raise ContractError("unsupported E0 candidate report schema")
    if report.get("baseline") != "CANDIDATE" or report.get("candidateStatus") != "complete":
        raise ContractError("candidate evidence must be generated as a complete CANDIDATE")
    if "baselineStatus" in report or "currentFailures" in report:
        raise ContractError("candidate evidence must not contain CURRENT-only fields")
    validate_report(current_report, contract)

    validation_view = dict(report)
    validation_view["schemaVersion"] = "zhongduan-terminal-journey-report-v1"
    validation_view["baseline"] = "CURRENT"
    validation_view["baselineStatus"] = "complete"
    validation_view["currentFailures"] = current_failures(
        report.get("oracleResults", {}), report.get("relativeThresholdMeasurements", {})
    )
    validation_view.pop("candidateStatus", None)
    validation_view.pop("candidateFailures", None)
    validation_view.pop("comparisonToCurrent", None)
    validation_view.pop("snapshotPhaseMeasurements", None)
    validate_report(validation_view, contract)

    expected_comparisons = compare_relative_thresholds(
        contract,
        current_report["relativeThresholdMeasurements"],
        report["relativeThresholdMeasurements"],
        report["oracleResults"],
    )
    expected_reference = {
        "currentSourceRevision": current_report["sourceRevision"],
        "currentSourceTreeGitOid": current_report["sourceTreeGitOid"],
        "relativeThresholds": expected_comparisons,
    }
    if report.get("comparisonToCurrent") != expected_reference:
        raise ContractError("candidate comparison is not derived from checked CURRENT")
    expected_failures = current_failures(
        report["oracleResults"], report["relativeThresholdMeasurements"]
    )
    expected_failures.extend(
        {
            "gate": f"candidate-comparison/{threshold_id}",
            "status": result.get("status"),
            "reason": result.get("reason", "candidate regressed CURRENT"),
        }
        for threshold_id, result in sorted(expected_comparisons.items())
        if result.get("status") != "passed"
    )
    if report.get("candidateFailures") != expected_failures:
        raise ContractError("candidateFailures is not derived from candidate evidence")
    phases = report.get("snapshotPhaseMeasurements")
    if not isinstance(phases, list) or any(not isinstance(item, dict) for item in phases):
        raise ContractError("candidate snapshot phase measurements must be raw JSONL objects")
    required_refresh_phases = {
        "refresh-queue-wait",
        "authority-actor-wait",
        "authority-cut",
        "authority-encode",
        "publisher-total",
        "finalize-install",
    }
    required_publish_phases = {"compress", "hash", "upload", "finalize"}
    observed_refresh: set[str] = set()
    observed_publish: set[str] = set()
    observed_events: set[str] = set()
    for item in phases:
        if (
            item.get("schemaVersion") != 1
            or item.get("source") != "host-cloud-relay"
            or not isinstance(item.get("recordedAtUnixMs"), int)
        ):
            raise ContractError("candidate Host measurement has an invalid provenance envelope")
        event = item.get("event")
        if event not in {"input-actor-queue", "snapshot-refresh", "snapshot-publish"}:
            raise ContractError("candidate Host measurement uses an unknown event")
        observed_events.add(event)
        if event == "input-actor-queue":
            duration = item.get("queueWaitMs")
        else:
            duration = item.get("durationMs")
        if (
            not isinstance(duration, (int, float))
            or not math.isfinite(float(duration))
            or duration < 0
        ):
            raise ContractError("candidate Host measurement duration must be finite")
        if event == "snapshot-refresh" and isinstance(item.get("phase"), str):
            observed_refresh.add(item["phase"])
        if event == "snapshot-publish" and isinstance(item.get("phase"), str):
            observed_publish.add(item["phase"])
    if observed_events != {"input-actor-queue", "snapshot-refresh", "snapshot-publish"}:
        raise ContractError("candidate lacks input and snapshot Host measurements")
    if not required_refresh_phases.issubset(observed_refresh):
        raise ContractError("candidate lacks required snapshot refresh phases")
    if not required_publish_phases.issubset(observed_publish):
        raise ContractError("candidate lacks required snapshot publish phases")

    for variant in ("snapshot-disabled", "snapshot-enabled"):
        window_items = [
            item
            for item in phases
            if item.get("scenarioVariant") == variant
            and isinstance(item.get("measurementStartedAtUnixMs"), int)
            and isinstance(item.get("measurementEndedAtUnixMs"), int)
            and item["measurementStartedAtUnixMs"]
            <= item["recordedAtUnixMs"]
            <= item["measurementEndedAtUnixMs"]
        ]
        if not any(item.get("event") == "input-actor-queue" for item in window_items):
            raise ContractError(f"{variant} lacks in-window input actor measurements")
        snapshot_items = [
            item for item in window_items if str(item.get("event", "")).startswith("snapshot-")
        ]
        if variant == "snapshot-disabled" and snapshot_items:
            raise ContractError("snapshot-disabled measurement window executed snapshot work")
        if variant == "snapshot-enabled" and not snapshot_items:
            raise ContractError("snapshot-enabled measurement window lacks snapshot work")


def build_e4b_decision(
    current_report: dict[str, Any], candidate_report: dict[str, Any], contract: dict[str, Any]
) -> dict[str, Any]:
    """Derive the mandatory E4b decision from validated finite evidence."""

    validate_candidate_report(candidate_report, current_report, contract)
    comparisons = candidate_report["comparisonToCurrent"]["relativeThresholds"]
    phases = candidate_report["snapshotPhaseMeasurements"]
    authority = [
        item
        for item in phases
        if item.get("event") == "snapshot-refresh"
        and item.get("phase") in {"authority-cut", "authority-encode"}
        and item.get("outcome") == "ok"
    ]
    authority_values: dict[str, list[float]] = defaultdict(list)
    for item in authority:
        authority_values[item["phase"]].append(float(item["durationMs"]))
    if any(not authority_values[phase] for phase in ("authority-cut", "authority-encode")):
        raise ContractError("E4b decision requires finite authority cut and encode measurements")

    input_actor = [
        item
        for item in phases
        if item.get("event") == "input-actor-queue" and item.get("outcome") == "ok"
    ]
    overlap_count = 0
    for input_item in input_actor:
        input_end = float(input_item["recordedAtUnixMs"])
        input_start = input_end - float(input_item["queueWaitMs"])
        for phase in authority:
            phase_end = float(phase["recordedAtUnixMs"])
            phase_start = phase_end - float(phase["durationMs"])
            if max(input_start, phase_start) <= min(input_end, phase_end):
                overlap_count += 1
                break

    all_pass = all(result.get("status") == "passed" for result in comparisons.values())
    snapshot_failed = comparisons["snapshotHostInput"].get("status") == "failed"
    if all_pass:
        decision = "skip-immutable-cow-cut"
        reason = (
            "All candidate latency ratios pass finite CURRENT denominators, the output-flood "
            "correctness oracle passes, and authority cut/encode phases are finite."
        )
        skip_authorized = True
        cow_authorized = False
        r0_authorized = True
    elif snapshot_failed and overlap_count > 0:
        decision = "implement-immutable-cow-cut"
        reason = (
            "The snapshot Host-input comparison regressed CURRENT and measured input actor "
            "queue residence overlaps synchronous authority cut/encode work."
        )
        skip_authorized = False
        cow_authorized = True
        r0_authorized = False
    else:
        decision = "not-authorized-non-cut-failure"
        reason = (
            "Finite candidate evidence failed a gate without locating the failure in synchronous "
            "authority cut/encode; E4b cannot be used as an unrelated workaround."
        )
        skip_authorized = False
        cow_authorized = False
        r0_authorized = False

    authority_summary = {
        phase: {
            "sampleCount": len(values),
            "p99Ms": round(percentile(values, 0.99), 6),
            "maxMs": round(max(values), 6),
        }
        for phase, values in sorted(authority_values.items())
    }
    return {
        "schemaVersion": "zhongduan-e4b-decision-v1",
        "stage": "E4b",
        "status": "complete-finite-measurement",
        "decision": decision,
        "reason": reason,
        "evidence": {
            "currentSourceRevision": current_report["sourceRevision"],
            "currentSourceTreeGitOid": current_report["sourceTreeGitOid"],
            "candidateSourceRevision": candidate_report["sourceRevision"],
            "candidateSourceTreeGitOid": candidate_report["sourceTreeGitOid"],
            "relativeThresholdComparisons": comparisons,
            "authorityPause": authority_summary,
            "inputActorSampleCount": len(input_actor),
            "inputAuthorityOverlapCount": overlap_count,
        },
        "currentAssessment": {
            "finiteCurrentBaselineMeasurementAvailable": True,
            "candidateE4aMeasurementAvailable": True,
            "skipAuthorized": skip_authorized,
            "immutableCowCutAuthorized": cow_authorized,
            "r0Authorized": r0_authorized,
        },
        "implementationBoundary": {
            "ghosttyChanged": False,
            "wtermChanged": False,
            "note": (
                "This decision artifact does not itself modify Ghostty or WTerm; any authorized "
                "E4b implementation remains a separate fork-only change."
            ),
        },
    }


def validate_report(report: dict[str, Any], contract: dict[str, Any]) -> None:
    """Validate a complete CURRENT report and recompute every derived claim."""

    validate_contract(contract)
    if report.get("schemaVersion") != "zhongduan-terminal-journey-report-v1":
        raise ContractError("unsupported E0 report schema")
    if report.get("contractSha256") != canonical_sha256(contract):
        raise ContractError("report does not reference the exact checked-in benchmark contract")
    if report.get("baseline") != "CURRENT":
        raise ContractError("the E0 baseline report must be labelled CURRENT")
    if report.get("baselineStatus") != "complete":
        raise ContractError("a checked CURRENT report must be complete")
    if report.get("sourceTreeDirty") is not False:
        raise ContractError("CURRENT evidence must come from a clean committed source tree")
    source_revision = report.get("sourceRevision")
    source_tree = report.get("sourceTreeGitOid")
    if not isinstance(source_revision, str) or len(source_revision) != 40 or any(
        character not in "0123456789abcdef" for character in source_revision
    ):
        raise ContractError("CURRENT evidence must identify its exact source commit")
    if not isinstance(source_tree, str) or len(source_tree) != 40 or any(
        character not in "0123456789abcdef" for character in source_tree
    ):
        raise ContractError("CURRENT evidence must identify its exact source tree")

    workload = report.get("workload")
    contract_workload = contract["workload"]
    if not isinstance(workload, dict) or workload != {
        "id": contract_workload["id"],
        "samplesPerVariant": contract_workload["samplesPerVariant"],
        "warmupSamples": contract_workload["warmupSamples"],
    }:
        raise ContractError("report workload must exactly match the frozen E0 workload")

    scenarios = report.get("scenarioReports")
    if not isinstance(scenarios, list) or not scenarios:
        raise ContractError("report must contain source scenario reports")
    measured_variants: set[str] = set()
    for scenario in scenarios:
        if not isinstance(scenario, dict):
            raise ContractError("every merged E0 scenario must be an object")
        validate_scenario_report(scenario, contract)
        if (
            scenario.get("schemaVersion") != "zhongduan-terminal-journey-scenario-v1"
            or scenario.get("status") != "measured"
        ):
            raise ContractError("every merged E0 scenario must be measured")
        if scenario.get("contractSha256") != canonical_sha256(contract):
            raise ContractError("scenario does not reference the checked benchmark contract")
        if (
            scenario.get("sourceRevision") != source_revision
            or scenario.get("sourceTreeGitOid") != source_tree
            or scenario.get("sourceTreeDirty") is not False
        ):
            raise ContractError("all scenarios must come from the same clean source tree")
        variant = scenario.get("variant")
        if variant not in EXPECTED_VARIANTS:
            raise ContractError("scenario uses an unknown E0 variant")
        if scenario.get("samples") != contract_workload["samplesPerVariant"]:
            raise ContractError("scenario did not collect the frozen measured sample count")
        if scenario.get("warmups") != contract_workload["warmupSamples"]:
            raise ContractError("scenario did not execute the frozen warmup count")
        scenario_events = scenario.get("events")
        if not isinstance(scenario_events, list) or any(
            not isinstance(event, dict) or event.get("variant") != variant
            for event in scenario_events
        ):
            raise ContractError("scenario timing events must be retained under their real variant")
        measured_variants.add(variant)
    if not EXPECTED_VARIANTS.issubset(measured_variants):
        missing_variants = sorted(EXPECTED_VARIANTS - measured_variants)
        raise ContractError(f"CURRENT report is missing required variants: {missing_variants}")

    environment = report.get("environment")
    if not isinstance(environment, dict) or environment.get("executionTier") not in {
        "local-workerd",
        "cloudflare-staging",
    }:
        raise ContractError("report environment must name its real execution tier")
    truth = report.get("evidenceBoundary")
    if not isinstance(truth, dict):
        raise ContractError("report must state its evidence boundary")
    if environment.get("executionTier") == "local-workerd" and truth.get("realCloudflareEdge") is not False:
        raise ContractError("a local Workerd report must not claim real Cloudflare edge evidence")

    observations = report.get("observations")
    if not isinstance(observations, dict):
        raise ContractError("report must retain the observations used by its oracles")
    authority_oracle = report.get("authorityOracle")
    if not isinstance(authority_oracle, dict):
        raise ContractError("report must retain its authority oracle output")
    if observations != merge_observations(scenarios, authority_oracle):
        raise ContractError("aggregate observations do not match merged scenarios")
    local_ids = [
        item.get("localIntentId")
        for item in observations.get("intents", [])
        if isinstance(item, dict)
    ]
    if len(local_ids) != len(set(local_ids)):
        raise ContractError("merged LocalIntentIds must be globally unique")
    recomputed_oracles = evaluate_oracles(observations)
    if report.get("oracleResults") != recomputed_oracles:
        raise ContractError("oracle results do not match recomputed raw observations")

    coverage = report.get("matrixCoverage")
    expected_coverage = coverage_for_scenarios(contract, scenarios)
    if coverage != expected_coverage:
        raise ContractError("matrix coverage does not match the exact executed scenarios")
    if any(cell["status"] == "not-run" for cell in coverage):
        raise ContractError("complete CURRENT evidence must measure every local matrix cell")

    events = report.get("events")
    if not isinstance(events, list):
        raise ContractError("report must retain raw timing events")
    merged_events = [event for scenario in scenarios for event in scenario["events"]]
    if events != merged_events:
        raise ContractError("aggregate timing events do not match merged scenarios")
    if report.get("rawEventCount") != len(events):
        raise ContractError("raw event count does not match retained timing events")
    recomputed_samples = collect_scenario_span_samples(contract, scenarios)
    if report.get("latencySamples") != recomputed_samples:
        raise ContractError("latency samples do not match recomputed raw events")
    recomputed_summaries = summarize_span_samples(recomputed_samples)
    if report.get("latencySummaries") != recomputed_summaries:
        raise ContractError("latency summaries do not match recomputed samples")
    measured_spans = {sample["span"] for sample in recomputed_samples}
    if report.get("unmeasuredLatencySpans") != sorted(EXPECTED_SPANS - measured_spans):
        raise ContractError("unmeasured latency spans are not derived from raw events")
    if measured_spans != EXPECTED_SPANS:
        raise ContractError("complete CURRENT evidence must measure all seven latency spans")
    recomputed_thresholds = measure_relative_thresholds(
        recomputed_summaries, recomputed_samples
    )
    if report.get("relativeThresholdMeasurements") != recomputed_thresholds:
        raise ContractError("relative thresholds do not match recomputed latency summaries")
    if any(result["status"] != "measured" for result in recomputed_thresholds.values()):
        raise ContractError("complete CURRENT evidence requires finite relative denominators")
    required_oracle = contract["relativeThresholds"]["outputFloodCtrlC"].get(
        "requiresCorrectnessOracle"
    )
    if (
        isinstance(required_oracle, str)
        and recomputed_oracles.get(required_oracle, {}).get("status") != "passed"
    ):
        raise ContractError("output-flood threshold requires its correctness oracle")

    expected_failures = current_failures(recomputed_oracles, recomputed_thresholds)
    if report.get("currentFailures") != expected_failures:
        raise ContractError("currentFailures is not derived from recomputed gates")
    if report.get("deadlineIsSlo") is not False:
        raise ContractError("the scenario deadline must never be reported as a latency SLO")


def matrix_cells(contract: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the mandatory source-controlled matrix without pretending it ran."""

    matrix = contract["matrix"]
    cells: list[dict[str, Any]] = []
    for browser_rtt in matrix["browserCloudRttMs"]:
        for host_rtt in matrix["cloudHostRttMs"]:
            cells.append(
                {
                    "browserCloudRttMs": browser_rtt,
                    "cloudHostRttMs": host_rtt,
                    "networkFault": "none",
                    "loadAndRecovery": "steady",
                    "lifecycleAndReplacement": "none",
                }
            )
    for network_fault in ("jitter", "disconnect", "reconnect"):
        cells.append(
            {
                "browserCloudRttMs": 100,
                "cloudHostRttMs": 100,
                "networkFault": network_fault,
                "loadAndRecovery": "steady",
                "lifecycleAndReplacement": "none",
            }
        )
    for load in ("output-flood", "cold-attach"):
        cells.append(
            {
                "browserCloudRttMs": 100,
                "cloudHostRttMs": 100,
                "networkFault": "none",
                "loadAndRecovery": load,
                "lifecycleAndReplacement": "none",
            }
        )
    for lifecycle in ("do-hibernation", "host-relay-replacement"):
        cells.append(
            {
                "browserCloudRttMs": 100,
                "cloudHostRttMs": 100,
                "networkFault": "none",
                "loadAndRecovery": "steady",
                "lifecycleAndReplacement": lifecycle,
            }
        )
    return cells


def _matrix_key(cell: dict[str, Any]) -> tuple[Any, ...]:
    return (
        cell.get("browserCloudRttMs"),
        cell.get("cloudHostRttMs"),
        cell.get("networkFault"),
        cell.get("loadAndRecovery"),
        cell.get("lifecycleAndReplacement"),
    )


def scenario_matrix_cells(scenario: dict[str, Any]) -> list[dict[str, Any]]:
    """Return only matrix cells genuinely exercised by one scenario."""

    profile = scenario.get("appliedProfile")
    if not isinstance(profile, dict):
        raise ContractError("scenario must record its applied network profile")
    browser_rtt = profile.get("browserCloudRttMs")
    host_rtt = profile.get("cloudHostRttMs")
    network_fault = profile.get("networkFault", "none")
    if not isinstance(browser_rtt, int) or not isinstance(host_rtt, int):
        raise ContractError("scenario RTT profile must use integer milliseconds")
    if network_fault not in {"none", "jitter"}:
        raise ContractError("ordinary scenarios support only none or jitter network profiles")
    base = {
        "browserCloudRttMs": browser_rtt,
        "cloudHostRttMs": host_rtt,
        "lifecycleAndReplacement": "none",
    }
    variant = scenario.get("variant")
    if variant == "steady":
        return [
            {
                **base,
                "networkFault": network_fault,
                "loadAndRecovery": "steady",
            }
        ]
    if variant == "output-flood":
        return [
            {
                **base,
                "networkFault": "none",
                "loadAndRecovery": "output-flood",
            }
        ]
    if variant == "correctness-faults":
        return [
            {
                **base,
                "networkFault": "disconnect",
                "loadAndRecovery": "steady",
            },
            {
                **base,
                "networkFault": "reconnect",
                "loadAndRecovery": "steady",
            },
            {
                **base,
                "networkFault": "none",
                "loadAndRecovery": "cold-attach",
            },
        ]
    return []


def coverage_for_scenarios(
    contract: dict[str, Any], scenarios: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    measured = {
        _matrix_key(cell)
        for scenario in scenarios
        if isinstance(scenario, dict) and scenario.get("status") == "measured"
        for cell in scenario_matrix_cells(scenario)
    }
    coverage: list[dict[str, Any]] = []
    for cell in matrix_cells(contract):
        if cell["lifecycleAndReplacement"] in {"do-hibernation", "host-relay-replacement"}:
            coverage.append(
                {
                    **cell,
                    "status": "requires-staging",
                    "reason": "real infrastructure lifecycle evidence is collected at staging",
                }
            )
        elif _matrix_key(cell) in measured:
            coverage.append({**cell, "status": "measured"})
        else:
            coverage.append(
                {
                    **cell,
                    "status": "not-run",
                    "reason": "no merged scenario exercised this exact matrix cell",
                }
            )
    return coverage


def current_failures(
    oracle_results: dict[str, dict[str, Any]],
    threshold_measurements: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    failures: list[dict[str, Any]] = []
    for gate in sorted(oracle_results):
        result = oracle_results[gate]
        if result.get("status") == "passed":
            continue
        failures.append(
            {
                "gate": gate,
                "status": result.get("status"),
                "reason": result.get("reason", "one or more measured violations were observed"),
            }
        )
    for threshold_id in sorted(threshold_measurements):
        result = threshold_measurements[threshold_id]
        if result.get("status") == "measured":
            continue
        failures.append(
            {
                "gate": f"relative-threshold/{threshold_id}",
                "status": result.get("status"),
                "reason": result.get("reason", "relative threshold is unavailable"),
            }
        )
    return failures


def main() -> None:
    contract = load_json(CONTRACT_PATH)
    validate_contract(contract)
    if BASELINE_PATH.exists():
        validate_report(load_report_bundle(BASELINE_PATH), contract)
    print(
        json.dumps(
            {
                "contract": "valid",
                "contractSha256": canonical_sha256(contract),
                "matrixCells": len(matrix_cells(contract)),
                "baseline": "valid" if BASELINE_PATH.exists() else "absent",
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
