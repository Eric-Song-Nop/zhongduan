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
data-only replacement, both age bounds, tombstone proof, late ACK immutability, result retention, and
callback/replacement reentrancy. The Worker/DO test exercises the negotiated welcome → semantic input
→ Host ACK and local rejection path while the unchanged relay suite continues to exercise legacy v2.
The correctness-fault journey observes product terminal-result events, acceptance uncertainty without
automatic retry, and a writer transfer whose displaced page cannot automatically reclaim control.
