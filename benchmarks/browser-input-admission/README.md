# E1 Browser latency evidence

This directory closes the Browser-latency part of E1 without turning the defensive E1 queue ceiling
into a performance promise. The candidate uses the real E0 Chromium → Browser → local Workerd → Host
→ PTY steady journey and evaluates only `browser-keydown-to-send-decision`.

The span starts at the real Ctrl-C `keydown` and ends when the Browser control sender makes its send
decision. It therefore includes WTerm semantic dispatch, E1 normalization, validation, bounded
admission, identity allocation, owner scheduling, and the captured sender invocation. Both boundaries
are before the configured network delay.

## Frozen population and gate

- CURRENT is the 408 Browser samples from all 17 `steady` scenarios in the checked E0 baseline. The
  workload settles each input before starting the next sample, and the measured span precedes both
  injected links, so using all steady cells gives the source-controlled denominator with the least
  quantization noise.
- CANDIDATE is five clean runs of the 100/100 ms, no-jitter steady cell, with 24 measured samples and
  four warmups per run: 120 measured Browser samples total.
- The nearest-rank candidate p99 must not exceed CURRENT p99. Individual durations are normalized to
  the harness's declared 0.1 ms Browser-clock resolution before this comparison; the report also
  retains the unquantized p50/p95/p99/max so sub-bucket arithmetic is never hidden. A missing, zero,
  non-finite, stale, dirty, wrong-profile, incomplete, or non-recomputable population fails closed.
- The run is inside the E0 supported-load envelope. It deliberately does not claim queue saturation,
  E1 hard-limit throughput, Cloudflare edge latency, E2 lane isolation, or E3 Host behavior. The E1
  hard limits remain safety contracts proven by invariant and overload tests.

The small JSON summary and bounded gzip JSONL archive are a pair. Validation checks both byte digests,
decompresses with a hard output limit, validates every raw E0 scenario, recomputes every percentile and
comparison, and verifies that the clean measurement commit/tree is reachable from the checked head.

## Reproduce a candidate

Commit the benchmark contract and runner before measuring, and start from that clean commit. Then run:

```bash
mkdir -p /tmp/zhongduan-e1-browser-latency

for seed in 450 451 452 453 454; do
  node scripts/verify-e0-terminal-journey.ts \
    --variant steady \
    --browser-cloud-rtt-ms 100 \
    --cloud-host-rtt-ms 100 \
    --samples 24 \
    --warmups 4 \
    --seed "$seed" \
    --report "/tmp/zhongduan-e1-browser-latency/steady-$seed.json"
done

node scripts/e1-browser-latency.ts \
  --merge-scenarios /tmp/zhongduan-e1-browser-latency/steady-*.json \
  --report benchmarks/browser-input-admission/e1-candidate.json
```

Verify the checked artifact pair with:

```bash
node scripts/e1-browser-latency.ts
```

The scenario deadline only terminates a hung run. It is not a latency SLO.
