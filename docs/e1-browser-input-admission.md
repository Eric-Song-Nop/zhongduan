# E1 Browser input admission

E1 is implemented from the E0 `origin/main` tree at `4a79423`. The Browser `InputDispatcher` is the
single owner from WTerm semantic consumption through the control-socket send decision and terminal
local result. CURRENT v2 recovery remains unchanged.

## Dispatch contract

Each call to `InputDispatcher.send()` creates a unique `LocalIntentId`, including malformed,
read-only, mouse-gated, and overloaded calls. The owner then performs these steps in order:

1. copy and normalize the semantic payload;
2. validate schema, encoded size, policy, writer authority, and replica/mouse gates;
3. reserve exact encoded queue bytes, queue count, and pending-identity capacity;
4. atomically allocate `{writerFence, inputEpoch, clientInputSeq}` with that reservation;
5. invoke the captured control sender in FIFO order;
6. converge the identity once to `not-sent`, `deterministic`, or `uncertain`.

Validation and admission rejection do not advance `clientInputSeq`. A sequence allocated before a
proven local non-send seals that epoch; it is never skipped or replayed. Acceptance uncertainty also
seals the epoch and waits only for the bounded proof interval. Terminal results are immutable and are
published as `zhongduan:input-intent-result` `CustomEvent`s for UI, journey, and telemetry observers.

Non-coalescible key input completes validation, admission, identity allocation, and the healthy send
decision in its originating owner turn. Other semantic input retains one pre-admission microtask,
which provides the coalescing window for adjacent resize and mouse-move state; after admission the
production sender drain is inline. Tests inject the send scheduler only when they need to hold the
otherwise-transient queued state at an exact replacement or age-expiry boundary. Reentrant sends join
the current FIFO drain instead of recursing.

The source-of-truth limits are exported as `INPUT_QUEUE_CONTRACT` from
`apps/terminal-cloud/src/browser/input-dispatcher.ts`:

| Resource                             |    Hard limit | Over-limit behavior                           |
| ------------------------------------ | ------------: | --------------------------------------------- |
| Pre-admission + admitted queue bytes | 6 MiB + 4 KiB | `not-sent/overload`, no identity              |
| Pre-admission + admitted queue count |           256 | `not-sent/overload`, no identity              |
| Outstanding identities               |         1,024 | `not-sent/overload`, no identity              |
| Pre-admission residence              |        250 ms | `not-sent/pre-admission-expired`, no identity |
| Admitted queue residence             |        250 ms | `not-sent/queue-expired`, seal epoch          |
| Sent ACK wait                        |          30 s | enter terminating and seal epoch              |
| Termination proof wait               |           5 s | `uncertain`, once                             |
| Retained terminal results            |         4,096 | evict oldest local lookup record              |

`InputDispatcherStatus` exposes pre-admission count/bytes, admitted count/bytes, pending identities,
writer/mouse gates, and whether a full control replacement is required. `getIntent(LocalIntentId)`
exposes identity, state, encoded bytes, admission/send timestamps, transport generation, deadline,
and retained result.

Timers are wake-up hints rather than the source of deadline truth. Every owner transaction, including
new consumption, ACK handling, and tombstone proof handling, first expires work against the current
clock. A delayed sent-age transition preserves the original absolute ACK deadline, so its proof
interval cannot be extended by Browser suspension or background timer throttling.

After an outer owner transaction has committed its records and published status, it synchronously
drains product-result notifications before returning. The reentrant worklist therefore cannot grow
across calls or remain captured by a delayed microtask; observer exceptions still cannot roll back an
owner transition.

## Internal owner structure

`InputDispatcher` remains the only public writer and the only owner that invokes callbacks or commits
effects. Its implementation is split without introducing independently mutable managers:

- `input-codec.ts` contains pure normalization, schema validation, encoding, and coalescing predicates;
- `input-intent-ledger.ts` owns the single canonical record map, LocalIntentId-only indexes, byte
  accounting, retention, and typed intent transitions;
- `input-authority.ts` represents Browser authority as a `detached | read-only | open | sealed`
  discriminated union. Writable state and replacement-required status are derived from that union;
- `input-dispatcher.ts` retains owner turns, external effects, scheduling, deadline catch-up, status
  publication, and reentrancy ordering.

Tests enable a deep invariant audit after every outer owner turn, except the dedicated 50,000-result
throughput case that would otherwise benchmark the test oracle. A deterministic command trace mixes
input, admission, ACK, stale/higher-fence attach, detach, revocation, timer firing, timer suspension,
replica gates, and resize confirmation while checking the global indexes, bytes, identities, retention,
and immutable-result constraints. This is a behavior-preserving E1 structure; it adds no E2 lane or E3
wire/Host state.

## Replacement and coalescing

- A full/control replacement makes pre-admission work `not-sent/control-replaced`, makes admitted but
  not sender-invoked work `not-sent/control-replaced`, and moves sender-invoked work into the bounded
  terminating proof wait. ACKed work remains immutable.
- A data-only replacement does not change input transport, writer fence, input epoch, or sequence.
- A Cloud `connection replaced` close permanently fences that page's current control transport and
  waits for an explicit user reconnect. This prevents two pages sharing one stable Browser client
  identity from repeatedly displacing each other while preserving manual control reclaim.
- Adjacent resize and mouse-move intents may supersede only while both lack an identity. Once admitted,
  each identity receives its own send decision.
- The latest resize is remembered as semantic state. Reconnection dispatches a new reconciliation
  consumption with a new `LocalIntentId` and identity; it never flushes the old not-sent record.

## Rollout boundary

E1 uses `browser-input-admission-v1` only to expose the Cloud's existing writer fence to the Browser:

| Browser | Cloud | Behavior                                                                                               |
| ------- | ----- | ------------------------------------------------------------------------------------------------------ |
| E0      | E1    | Browser does not request the capability; legacy welcome, lease status, and ACK stay unchanged          |
| E1      | E1    | capability selected; welcome, lease status, ACK, and local rejection include `writerFence`             |
| E1      | E0    | capability absent; Browser fails closed to read-only because it cannot construct the required identity |

This capability does not add a new input frame, `BeginInputEpoch`, input-epoch ACK, tombstone frame,
strict Host prefix, Cloud input lane, or recovery protocol. Tombstone proof is represented only as a
Browser-owner test/API boundary for bounded replacement convergence; its wire implementation remains
E3 work. A fresh higher Cloud fence starts a new Browser epoch at sequence 1. Same-fence rollover is
not supported in E1.

## Browser latency candidate

The checked E1 candidate measures the real Chromium `keydown` through the Browser control sender's
send decision in the E0 steady journey. Its five clean 100/100 ms, no-jitter runs contribute 120
measured samples after warmup. The raw nearest-rank p99 is 0.4002 ms; at the harness's declared 0.1 ms
Browser-clock resolution it is 0.4 ms. The frozen E0 CURRENT population contains 408 samples across
all 17 steady scenarios and has a 0.4 ms raw and comparison p99. The resulting no-regression ratio is
1.0, exactly at the frozen maximum.

The summary and bounded raw archive live in `benchmarks/browser-input-admission/`. The verifier checks
their hashes, replays every scenario, recomputes the samples and percentiles, and requires the clean
measurement commit and tree to be reachable from the checked head. This is Browser hot-path evidence
inside the E0 supported-load envelope; it is not a queue-saturation, hard-limit throughput, edge,
E2, or E3 claim.

## Verification entry points

```bash
pnpm exec vp test --run \
  packages/protocol/src/cloud-api.test.ts \
  packages/protocol/src/control-frame.test.ts

pnpm --filter @zhongduan/terminal-cloud exec vp test \
  --config vitest.browser.config.ts --run \
  src/browser/input-dispatcher.test.ts \
  src/browser/terminal-session.test.ts

pnpm --filter @zhongduan/terminal-cloud exec vp test \
  --config vitest.worker.config.ts --run test/relay.test.ts \
  -t "negotiates E1 Browser fences"

node scripts/e1-browser-latency.ts

node scripts/verify-e0-terminal-journey.ts \
  --allow-dirty-development \
  --variant correctness-faults \
  --browser-cloud-rtt-ms 100 \
  --cloud-host-rtt-ms 100 \
  --samples 24 \
  --warmups 4 \
  --report /tmp/zhongduan-e1-correctness-faults.json
```

The dispatcher suite covers sequence/no-gap properties, bytes/count/pending overload, pre-admission
coalescing, admitted identity immutability, missing/throwing/uncertain sender outcomes, full versus
data-only replacement, both age bounds, suspended-timer ACK/proof/admission expiry, tombstone proof,
late ACK immutability, result retention, 50,000 synchronous local-result notifications, and
callback/replacement reentrancy. The Worker/DO test exercises the negotiated welcome → semantic input
→ Host ACK and local rejection path while the unchanged relay suite continues to exercise legacy v2.
The correctness-fault journey requires product terminal-result events for every measured E1 input;
passive socket-close inference cannot satisfy that evidence. It also observes acceptance uncertainty
without automatic retry and a writer transfer whose displaced page cannot automatically reclaim
control.
