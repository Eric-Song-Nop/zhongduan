# E2 Cloud input lane and connection-scoped writer

E2 is implemented from the merged E1 tree at `be258b5`. The Durable Object now gives Browser
control/input its own bounded execution owner and treats the live Browser control connection as part
of writer authority. CURRENT v2 logical data frames and the recovery state machine stay unchanged;
opt-in Host and Browser transport batches add the backpressure and event-count reduction needed to
keep Durable Object runtime event ingress bounded during output floods.

## Execution and ordering

Every Browser control WebSocket is a key in `BoundedKeyedQueue`. Frames from one connection remain
strict FIFO, so attach/fence acquisition and heartbeat renewal finish before later input on that
connection. Ready connections rotate after each frame and up to four independent connections may run
at once. Host control and Host data keep the pre-existing bounded serial relay queue; Browser input no
longer joins that bulk tail. A Browser control WebSocket event returns after bounded admission and a
rejection handler is attached to the processing promise. Durable Objects keep in-progress work alive,
so `waitUntil()` is neither needed nor used for this queue, matching Cloudflare's
[Durable Object state guidance](https://developers.cloudflare.com/durable-objects/api/state/).

Returning early is not sufficient by itself: an unconstrained Host can otherwise fill the runtime's
WebSocket event queue before a Browser event is dispatched. A Host that negotiates
`host-data-batch-v1` may therefore have exactly one data message in flight. Cloud returns the fixed
`data-ack` credit only after every logical frame in that message has been processed. A second message
before the first settles fails the current Host pair. Legacy Host peers retain the original one-frame
message and per-message processing backpressure.

After bounded admission reserves that credit, a negotiated bulk message yields one runtime event turn
before decoding. A Promise-tail microtask alone still runs before an already-arrived WebSocket event;
the explicit turn boundary lets Browser control enter its independent lane first. Isolated Host frames
up to 256 payload bytes stay on the immediate path, so this bulk fairness boundary does not delay small
interactive output.

This split intentionally gives no ordering precedence to Host data broadcast, snapshot work, R2 work,
or maintenance. It preserves only the E2-required order:

1. FIFO within one Browser control connection;
2. that connection's writer transition before its later input;
3. Host ACK routing back to the original connection and input identity.

The Browser-lane source-of-truth limits are exported from
`apps/terminal-cloud/src/worker/relay-message-queue.ts`; input send-buffer admission is enforced in
`apps/terminal-cloud/src/worker/terminal-session-do.ts`, and Host batch retention is enforced in
`apps/host-daemon/src/cloud/host-relay-connection.ts`:

| Resource                         |                  Hard limit | Over-limit behavior                                     |
| -------------------------------- | --------------------------: | ------------------------------------------------------- |
| Browser lane bytes               |                      32 MiB | reject admission and isolate the source connection      |
| Browser lane frames              |                         512 | reject admission and isolate the source connection      |
| One Browser connection bytes     |                      16 MiB | isolate only that connection set                        |
| One Browser connection frames    |                          64 | isolate only that connection set                        |
| Queue residence                  |                      250 ms | expire the frame and isolate only that connection set   |
| Independent active connections   |                           4 | retain per-connection FIFO and rotate ready connections |
| Host input socket buffered bytes |                       8 MiB | terminate the writer input epoch before accepting more  |
| Host transport batch             | 256 KiB / 64 logical frames | reject the batch and fail the current Host pair         |
| Host data messages in flight     |                           1 | fail the current Host pair                               |
| Host retained batch work         | 8 MiB / 8192 logical frames | close the Host relay pair                                |

Reservations include executing work and are released on success, expiry, or exception. A noisy
Browser cannot close a healthy Host or consume another Browser's per-connection allowance.

The negotiated Host message is the exact concatenation of independently valid v2 frames, without a
second envelope. For an all-canonical live batch, Cloud validates every logical frame in order, then
commits the session cursor and each Browser delivery cursor once. When exactly one synced Browser also
negotiates `browser-data-batch-v1`, Cloud rewrites the owned batch views in place and sends their exact
concatenation in one Browser data message. A legacy or additional Browser still receives one original
v2 frame per message. Mixed canonical/recovery batches keep the existing per-frame state machine and
yield between logical frames. Host frames whose payload is 3–256 bytes are kept out of surrounding
bulk batches, so small interactive output is not coalesced with the bulk bytes that immediately
overwrite it. Older peers neither send batches nor expect `data-ack`, which makes rollout
capability-gated in each direction.

This follows Cloudflare's recommendation to batch logical WebSocket messages when per-message context
switching becomes material, while the credit adds the E2-specific ingress bound; see
[WebSocket best practices](https://developers.cloudflare.com/durable-objects/best-practices/websockets/).

## Connection-scoped writer authority

Writer authority is the conjunction of the current live control socket, stable `clientId`,
`connectionId`, monotonic `writerFence`, and an unexpired attachment deadline. Schema version 6 adds
`connection_id` to `writer_lease`. A pre-E2 row has no provable connection owner, so migration keeps
its fence high-water but expires it and requires a fresh acquisition.

- A successful attach stores the same fence and expiry in the control-socket attachment and lease
  row. A higher-fence acquisition closes every displaced writer connection.
- Heartbeat hashes the lease token and renews only the exact live
  `{clientId, connectionId, writerFence}` tuple. Hashing and SQLite mutation happen on heartbeat, not
  on semantic input.
- The semantic-input path proves the current socket attachment, connection identity, fence, expiry,
  and active tagged control socket without reading or mutating the lease row. The legacy
  `writerLease` frame field remains wire-compatible but is not a transferable per-key capability.
- Close, replacement, timeout, or a send outcome that cannot be proven seals the connection. A
  writer identity receives `uncertain` and the whole Browser connection set closes; the old socket
  cannot continue the same input epoch. Negotiated E1 peers also receive `writerFence`; legacy peers
  retain their v2 field shape.
- After Durable Object hibernation, the serialized attachment must still prove the exact current
  connection. A pre-E2 or otherwise incomplete attachment fails closed instead of reconstructing
  authority from the session-wide lease row.

## Input fast path and telemetry

The healthy semantic-input path performs bounded frame-size admission, keyed enqueue, schema/policy
parsing, attachment/fence validation, bounded control-frame encoding, and the Host control send. It
does not hash the lease token, mutate SQLite, access R2, or invoke snapshot/hash/compression work per
key. Host send is rejected before the socket's buffered amount would exceed 8 MiB.

`CloudInputTelemetry` retains at most 256 in-memory records for at most 60 seconds. Each record has
only `{clientId, connectionId, writerFence, inputEpoch, clientInputSeq}`, disposition, and Browser
receive / queue enter / queue leave / Host send timestamps. It never retains input payloads or lease
tokens, and hibernation may discard it without changing correctness.

The Browser coalesces cumulative live-delivery ACKs to the latest cursor. It flushes after 10 ms,
32 KiB, or 256 events, whichever comes first; recovery/adoption ACKs remain immediate. Replacement,
generation changes, and full invalidation cancel any pending ACK, so an old cursor cannot cross a
delivery boundary. This bounds reverse control traffic during a flood without weakening cumulative
cursor validation.

## Source-controlled evidence

The Worker tests cover the E2 failure and load boundaries:

- the exact E0 supported load of 32 inputs and at most 64 KiB drains in FIFO order while the unrelated
  Host bulk owner is held behind a 4 MiB reservation; all 32 inputs reach Host, the interrupt payload
  appears once, the Browser lane drains to zero, and measured queue residence stays within 250 ms;
- a dispatch regression holds both queue-processing promises open and proves both WebSocket event
  handlers return immediately after bounded admission;
- negotiated Host data preserves the exact logical v2 sequence, waits for one `data-ack` before the
  next batch, isolates latency-sensitive frames, and fails a Host that spends two credits at once;
- a compatible sole synced Browser receives the same canonical logical frames in one negotiated
  Browser batch, while an unnegotiated Browser retains one-frame-per-message delivery;
- a legacy Host keeps one frame per WebSocket message and never waits for the new acknowledgement;
- 64 live cursor advances produce one cumulative Browser ACK for the latest cursor, while recovery
  ACKs and replacement resets remain immediate;
- per-connection FIFO, cross-connection progress, fair rotation, bytes/count rejection, age expiry,
  and reservation cleanup are deterministic queue tests;
- higher-fence replacement, heartbeat expiry, socket close, hibernation, and delayed displaced input
  prove that an old writer connection has zero successful Host sends;
- a throwing Host send returns `uncertain`, closes both Host authority and the writer input epoch, and
  expires the exact connection lease;
- schema migration preserves the old fence while expiring authority that lacks a connection owner;
- telemetry retention, timestamp monotonicity, disposition counts, and payload/token exclusion are
  executable tests.

Run the local evidence from the repository root:

```bash
pnpm --filter @zhongduan/terminal-cloud exec vp test \
  --config vitest.worker.config.ts --run \
  test/relay-message-queue.test.ts \
  test/cloud-input-telemetry.test.ts \
  test/relay.test.ts \
  test/snapshot-migration.test.ts

pnpm --filter @zhongduan/terminal-cloud typecheck

pnpm --filter @zhongduan/terminal-cloud exec vp test \
  --config vitest.browser.config.ts --run \
  src/browser/terminal-session.test.ts

pnpm --filter @zhongduan/host-daemon exec vp test --run \
  src/cloud/cloud-api.test.ts \
  src/cloud/host-relay-connection.test.ts

pnpm --filter @zhongduan/protocol exec vp test --run \
  src/cloud-api.test.ts \
  src/data-frame.test.ts
```

The E0 journey remains the source-controlled deployed-path latency oracle. A clean candidate can
rerun its four `bulk-backlog-*` cells and `output-flood` cell with
`scripts/verify-e0-terminal-journey.ts`; the frozen comparison is
`cloudBulkIsolation <= 1.043997829` relative to CURRENT, and the output-flood oracle requires exactly
one Ctrl-C PTY effect. Dirty local runs are development evidence only and cannot be merged.

These tests prove local Workerd structure and the declared queue/load bounds. They do not claim
Cloudflare edge latency, real lifecycle behavior, E3 Host contiguous-prefix/dedupe semantics, or E4
snapshot p99. Those remain staging or later-stage gates.
