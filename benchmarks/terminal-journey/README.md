# E0 terminal journey

This directory defines the source-controlled E0 contract for the raw semantic PTY path. The local
harness runs the production Browser application, Vite Workerd/Miniflare, the Host daemon, `node-pty`,
the deterministic raw PTY fixture, and the committed WTerm/Ghostty runtime. Two userspace proxies
apply the Browser–Cloud and Cloud–Host profiles. The reported Cloud span starts after the former link
delay and ends before the latter, so neither simulated link is charged to Cloud execution.

`contract.json` freezes 24 measured samples plus four warmups per scenario, nine workload variants,
23 matrix cells, eight correctness oracles, seven latency spans, and three relative comparisons. Raw
scenario observations and events are retained; the validator recomputes coverage, oracles, samples,
summaries, and thresholds. Missing evidence is never green. In particular, a report assembled from
only one run of each of the nine variants is incomplete: all sixteen 20/100/300/600 RTT pairs and the
local jitter, disconnect, reconnect, output-flood, and cold-attach cells are mandatory. Real edge,
Durable Object hibernation, and Host relay replacement remain explicitly `requires-staging`.

The checked-in CURRENT is a two-file evidence bundle. `current-baseline.json` is the small,
reviewable summary; `current-baseline.scenarios.jsonl.gz` contains the canonical raw scenario JSONL.
The summary records compressed and uncompressed sizes and SHA-256 digests. Loading the bundle applies
strict size bounds, verifies both digests, reconstructs aggregate events, observations, and latency
samples in memory, and then recomputes every derived claim. Neither file alone can serve as an E1–E4
denominator, and raw payloads are never duplicated inside the summary.

During aggregation, latency endpoints are paired inside each source scenario before samples are
combined across the matrix. This preserves repeated workload-local sample IDs across independent RTT
runs without treating them as duplicate endpoints. The Cloud bulk-isolation denominator uses only
the 24 `probe-measured-*` samples sent after each configured backlog is established; the separate
`flood-command-*` inputs that create the backlog cannot contaminate that measurement.

The checked-in complete CURRENT scenario matrix was measured from clean commit
`f56205fd99b6ea0133fc387098221ebf2906ca3b` (tree
`ef3922f354493327e41adae24c1258f7eaa07c50`). Its bounded archive retains 25 scenario reports,
82,036 raw events, and 5,964 derivable latency samples while the JSON summary remains directly
reviewable. It covers 21 local cells with two lifecycle cells explicitly marked `requires-staging`,
passes all eight correctness oracles, and measures all seven spans. Its finite relative denominators
are:

- Cloud bulk isolation: `1.043997829`
- Snapshot-enabled/disabled Host input p99: `1.286902287`
- Output-flood/steady Ctrl-C quiet p99: `23.047255048`

The strengthened v3 authority corpus was separately revalidated from clean commit
`b02a8f29496d85a2cf89d98fb012149b8bd803df` (tree
`cb56a0418f6462963ed2d041905b44958ff5cebc`); the baseline embeds both provenances instead of
implying that regenerated oracle evidence came from the older scenario commit.

## Static checks

From the repository root:

```bash
node scripts/prepare-wterm.mjs
node scripts/e0-terminal-journey.ts
pnpm exec vp test --run scripts/e0-terminal-journey.test.ts
node scripts/e0_authority_oracle.mjs
node scripts/verify-e0-terminal-journey.ts --matrix-plan
```

The authority oracle executes the committed Ghostty WASM and compares a true snapshot-disabled
control with both the snapshot source and its passively restored core after identical canonical I/O.
It also verifies that snapshot capture itself does not mutate normalized screen/history, cursor,
modes, parser continuation, cell/grapheme state, or effect-relevant state, and compares equivalent
byte-split `WRITE_PTY` effects. Its standalone command exits nonzero on divergence; report assembly
retains a failing result so it cannot be mistaken for missing evidence. The 120-second scenario
deadline is a hang guard, never a latency SLO.

## Install the runner

```bash
pnpm install
pnpm exec playwright install chromium
```

The runner refuses to overwrite an existing `apps/terminal-cloud/.dev.vars`, does not deploy, and
removes local generated state on exit. It also refuses dirty-tree evidence unless
`--allow-dirty-development` is supplied; artifacts made with that flag cannot be merged. If pnpm lost
the executable bit on `node-pty`'s platform helper, use the exact repair command printed by the runner.

## Generate a complete CURRENT

Run this only from the clean committed E0 stack revision. It produces 25 independently validated raw
scenario files: sixteen steady RTT cells, one jitter cell, output/fault cells, and the remaining
comparison variants.

```bash
mkdir -p /tmp/zhongduan-e0-current
for browser_rtt in 20 100 300 600; do
  for host_rtt in 20 100 300 600; do
    node scripts/verify-e0-terminal-journey.ts \
      --variant steady \
      --browser-cloud-rtt-ms "$browser_rtt" \
      --cloud-host-rtt-ms "$host_rtt" \
      --samples 24 --warmups 4 \
      --report "/tmp/zhongduan-e0-current/steady-${browser_rtt}-${host_rtt}.json"
  done
done

node scripts/verify-e0-terminal-journey.ts \
  --variant steady --network-fault jitter --jitter-ms 10 \
  --browser-cloud-rtt-ms 100 --cloud-host-rtt-ms 100 \
  --samples 24 --warmups 4 --report /tmp/zhongduan-e0-current/steady-jitter.json

for variant in output-flood correctness-faults; do
  node scripts/verify-e0-terminal-journey.ts \
    --variant "$variant" --browser-cloud-rtt-ms 100 --cloud-host-rtt-ms 100 \
    --samples 24 --warmups 4 --report "/tmp/zhongduan-e0-current/${variant}.json"
done

for variant in bulk-backlog-0 bulk-backlog-262144 bulk-backlog-1048576 \
  bulk-backlog-4194304 snapshot-disabled snapshot-enabled; do
  node scripts/verify-e0-terminal-journey.ts \
    --variant "$variant" --browser-cloud-rtt-ms 20 --cloud-host-rtt-ms 20 \
    --samples 24 --warmups 4 --report "/tmp/zhongduan-e0-current/${variant}.json"
done

node scripts/verify-e0-terminal-journey.ts \
  --merge-scenarios /tmp/zhongduan-e0-current/*.json \
  --artifact-kind current \
  --report benchmarks/terminal-journey/current-baseline.json
```

The merge writes both `current-baseline.json` and its deterministic
`current-baseline.scenarios.jsonl.gz` companion. Commit both files. The same bundling applies to a
merged candidate report, so later evidence does not reintroduce monolithic expanded JSON.

The runner-only Browser instrumentation assigns a harness-local ID at UI consumption, observes
semantic `WebSocket.send`, maps the v2 Browser identity (`inputEpoch/clientInputSeq`), promotes it to
the full Host identity when `writerFence` becomes observable, and passively records ACK, close, or
deadline outcomes. It also consumes the generic `zhongduan:input-intent-result` event when a candidate
provides exact product outcomes. A consumed intent with no result remains a failed silent-loss oracle;
the harness does not manufacture a successful terminal result or retry it. Fault inputs terminate on
their observed local result and treat matching output as optional, because a deterministic rejection
or acceptance-uncertain no-effect result cannot truthfully promise terminal output.

The `snapshot-enabled` workload paces its final 12 inputs across the CURRENT 30-second checkpoint TTL
and E4a's 15-second refresh cadence. At checkpoint expiry it coordinates a fresh observer attach with
one of those inputs. The scenario is valid only when a retained snapshot finalization timestamp falls
strictly between retained Host-receive timestamps from the paced half; a finalization performed during
an idle gap or after the measured input window cannot satisfy the overlap evidence.

## Generate and decide an E4a candidate

Repeat the same 25 commands from the clean final E4a commit, changing the output directory to
`/tmp/zhongduan-e4a-candidate`. The runner automatically captures the opt-in Host JSONL phase sink.
Then generate a distinct candidate artifact and the finite E4b decision together:

```bash
node scripts/verify-e0-terminal-journey.ts \
  --merge-scenarios /tmp/zhongduan-e4a-candidate/*.json \
  --artifact-kind candidate \
  --current-report benchmarks/terminal-journey/current-baseline.json \
  --report benchmarks/terminal-journey/e4a-candidate.json \
  --e4b-decision benchmarks/terminal-journey/e4b-decision.json
```

The candidate validator requires in-window Host input-actor measurements for both snapshot variants,
zero snapshot work in the disabled window, snapshot work plus every refresh/publish phase in the
enabled window, and finite authority cut/encode measurements. The decision is derived from the
validated CURRENT/candidate comparison; a CURRENT report must never be copied or relabelled as a
candidate, and `e4b-decision.json` must not be hand-edited into a pass.
