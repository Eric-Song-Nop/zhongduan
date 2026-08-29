# Terminal telemetry

`@zhongduan/telemetry` defines strict, content-free diagnostic events shared by the terminal
runtimes. It is deliberately separate from the terminal wire protocol:

- events never consume authority `eventSeq` and never enter journal, snapshot, or replay data;
- recovery owners wrap sinks in a bounded deferred queue; full queues drop diagnostics, and
  validation, scheduling, synchronous failures, and rejected async collectors are contained;
- durations use a monotonic clock from one runtime only; timestamps from Browser, Cloud, and Host
  must never be subtracted from one another;
- schemas allow only bounded enums and numeric measurements. Terminal text, input, cells, tokens,
  raw identifiers, and error messages are forbidden.

Phase 0a exposes low-frequency Host snapshot and retained-journal facts. The next Host slice adds:

- control queue wait/handling depth with control-frame byte counts bucketed rather than retained;
- semantic input actor wait, authority encoding, `pty.write`/`pty.resize` call timing, and effect-stage
  classification;
- independent Host-observed control/data relay socket RTT using bounded FIFO heartbeat probes.

The session actor only returns numeric timing. The relay attempts the input ACK first, then enqueues a
strict event into the same process-owned bounded buffer used by recovery. Schema validation, JSON, and
the collector run from the deferred drain, never in the actor or canonical-pause critical section.
The buffer is paused with canonical handoff and is resumed on success, failure, or disposal.

`host.input.apply.outcome=written` still means the existing Host input result: encoding completed and,
when the encoding was non-empty, the synchronous PTY call returned. It does not prove that the child
read the bytes, produced output, or painted a result. `host.relay.rtt` includes local event-loop/socket
queue time plus the Host-to-Cloud auto-response round trip; it is not pure network RTT.

Cloud schema v2 retains the Phase 0b content-free events for the current single relay queue, Browser
semantic-input lease/Host-send decisions, delivery barriers, attach, and delivery reset. Phase 0c adds
three facts at the existing authoritative `TerminalSessionDO` paths:

- `cloud.input.ack-forward` observes a Host input ACK from relay-queue ingress through the Browser
  control target/send decision. Its outcomes are `send-returned`, `target-missing`, and `send-failed`;
  the ACK status remains the bounded `written`, `duplicate`, `rejected`, or `uncertain` enum.
- `cloud.data.fanout` emits one aggregate per Host data frame, not one event per Browser target. It
  distinguishes canonical and directed paths, classifies the local result as completed, degraded,
  not-targeted, stale, or host-failed, and accounts for every selected target as send-returned, stale,
  sequence-error, credit-reset, or send-uncertain-reset. Credit utilization is bucketed. Reset counts
  describe a local reset decision/request; the existing reset transition records issuance and Host
  notification decisions, and neither event proves reset completion or peer receipt.
- `cloud.writer.lease` observes writer acquire during attach and verify-renew during heartbeat. Its
  bounded outcomes distinguish acquired/renewed, unavailable/inactive, stale-context, released-stale,
  and uncertain paths without exporting the lease or its identity.

Every Cloud event carries `sampleWeight`; the existing successful high-rate queue/input facts remain
sampled. ACK forwarding is systematically sampled at weight 16. A canonical Host data frame is
selected before its Browser loop at weight 64, so the other 63 frames install no per-Browser observer;
the same weight applies whether the sampled outcome is completed or exceptional. Heartbeat
`renewed-current`/`inactive-current` use weight 64. Directed fanout, attach acquire, other heartbeat
outcomes, capacity decisions, and recovery transitions use weight 1. Sampling has a randomized
per-DO/key starting phase; if secure
phase selection fails, that series is dropped rather than assigned a biased weight. Weight 1 is still
best-effort, not a delivery guarantee, so aggregation must use `sampleWeight` and must not treat raw
event counts as complete traffic counts.

Cloud durations use `clockKind=workers-io`: production Workers clocks advance only at I/O boundaries,
so ingress-to-decision, queue, fanout, and lease values are local lower bounds and may report zero for
synchronous CPU work. A local `WebSocket.send()` return is not proof that the frame left the socket
queue, reached a peer, affected the child application, or painted.

The production Cloud sink is enabled only by the exact versioned mode `workers-logs-v2` and buffers at
most 64 events per DO instance. Pending events, drop state, and sampling phases/counters are in-memory
only: capacity pressure, collector/runtime failure, version rollout, eviction, or hibernation may drop
or reset them. They are not stored in SQLite or a WebSocket attachment and are never backfilled or
replayed. This loss cannot affect relay authority, recovery, or socket lifetime.

Queries and dashboards must accept both historical/in-flight `schemaVersion=1` records and
`schemaVersion=2` records throughout rollout and rollback, and interpret each version independently.
`CloudTelemetryWriteEventSchema` is the strict v2 producer boundary;
`CloudTelemetryReadEventSchema` retains the four v1 Phase 0b shapes alongside all v2 shapes for
ingestion during that compatibility window. `CloudTelemetryReadLogRecordSchema` validates the
flattened `type=zhongduan.telemetry` / `runtime=cloud-do` record and the versioned producer
`sampleWeight`; `CloudflareCloudTelemetryLogEventSchema` projects only `event.source` from a Workers
Logs envelope and never returns platform metadata. New Phase 0c event names are never accepted with
v1, and changing a producer sampling policy requires a new event schema version.
The runtime gate itself accepts only the mode paired with its code. Separately deploying or rolling
back code and `CLOUD_TELEMETRY_MODE` can therefore disable custom logging and create an irrecoverable
telemetry blind window; code and binding should ship as one version, and v1 queries should remain until
v2 arrival is verified.

The source-controlled Cloud observability manifest is a query contract, not a terminal protocol. Every
query includes fixed platform service/log-type filters plus the custom wrapper filters, and a preflight
must discover the exact direct key names and types before executing it. Producer-volume estimates sum
`sampleWeight`; raw row counts only describe stored/query rows. A percentile query is valid only when
its filters select one fixed producer weight, so differently sampled success and failure paths are
never mixed into one percentile. Cloudflare `sampleInterval` and `abr_level` are reported as quality
signals: if either differs from one, the result is approximate and cannot close a hard rollout or SLO
gate. The client does not blindly multiply an aggregate by `sampleInterval` a second time.

Local validation is credential-free. Query smoke/report operations use a dedicated account-scoped
observability token and are read-intent operations even though the current Cloudflare run-query API
requires Observability Write permission. Saved-query provisioning is a separate explicit `--apply`
operation: missing definitions may be created, identical definitions are no-ops, and same-name drift
fails closed instead of overwriting or deleting account-level state. No token, raw log source, platform
envelope, or API error body is printed. Real staging key/source-shape validation and saved-query
provisioning remain deployment gates; repository fixtures and unit tests cannot substitute for them.

Automatic invocation logs and tracing remain explicitly disabled because request URLs contain terminal
session metadata and one-time WebSocket tickets. Strict custom event payloads must not contain terminal,
input, or frame payloads; text, paste, command, cell, or key material; tickets, capabilities, or writer
leases; session, client, connection, stream, generation, epoch, or sequence identifiers; journal or
snapshot identifiers/cursors; URLs; error/exception strings; or hashes/digests of those sensitive
values. Sizes are bucketed. The Cloudflare log envelope can still add platform identifiers and must be
handled as sensitive metadata.

Browser facts use `clockKind=browser-performance` and only compare monotonic timestamps from the same
page. Schema v1 remains accepted for read compatibility. Its historical `browser.input.ack` producer
has been superseded by the schema-v2 lifecycle below; the current low-frequency v1 producers still
record control/data relay probes, attach terminal outcomes, snapshot load, passive restore,
buffered-tail application, synchronous replica `adopt()` return, and the terminal recovery outcome.
An adopt return is not proof that pixels were painted, and an input acknowledgement is not a child-read
or application-effect acknowledgement.

The schema-v2 Browser lifecycle/presentation slice adds two sampled, content-free lifecycles without
changing terminal behavior:

- `browser.input.lifecycle` starts at semantic dispatch, observes the local send decision, and, when
  an identity was successfully assigned and sent, ends at the matching ACK. Not-writable, policy,
  validation, send, coalescing, capacity/deadline, input-epoch, transport, and session terminal paths
  expose only bounded reason/stage enums. This start is not raw DOM `keydown`.
- `browser.presentation.canonical` selects canonical `PTY_OUTPUT` or `RESIZE_APPLIED` after frame
  decode when that data callback entered with the session already `live` or warm `replaying`, requires
  the visible active replica cursor to advance exactly, observes the synchronous WTerm DOM render
  commit, and ends at the next animation-frame callback opportunity. Detached restore candidates and
  buffered-only work while `restoring` are excluded.

A WTerm render commit only means that synchronous DOM/scroll/title/response work committed. A
next-frame opportunity only means that the browser invoked a subsequent animation-frame callback.
Neither is, or may be labelled as, pixel paint/composite evidence. Real paint remains a later fact only
if the Browser exposes a reliable signal.

Input and canonical series each select a randomized starting phase and sample systematically before
the outcome is known. Both success and terminal outcomes have the literal `sampleWeight=64`; if the
secure random phase cannot be obtained, the corresponding series is dropped instead of selecting a
biased first sample. Their tracker holds at most 64 pending probes in total and gives every probe a
two-second deadline. It schedules at most the necessary earliest-deadline timer and coalesced
animation-frame opportunity. Capacity admission produces one terminal fact; deadline, page-hidden,
generation, and close paths terminate each owned probe exactly once. None can affect input, replica,
recovery, or rendering decisions.

Browser diagnostic mode is selected with the exact query value `browserDiagnostics=off|memory-v2`.
Missing, empty, or exact `memory-v2` selects the bounded local-memory sink; unknown values fail closed
to `off`. This diagnostic mode is independent of terminal input modes such as Raw or Mirrored. The
`off` path creates no tracker, ring, telemetry-only clock work, map, page-visibility listener, WTerm
observer, presentation timer, or animation-frame callback. The monotonic clock and deadline timer used
for WebSocket heartbeat correctness remain active.

In `memory-v2`, `createBrowserDiagnostics()` remains an in-memory-only sink. Admission is deferred
behind a bounded queue; strict validation happens during its drain into a fixed-size circular ring.
The pending queue and ring are each hard-bounded at 256 events (and may be configured smaller). A full
pending queue, invalid event, scheduler failure, or overwritten ring entry increments the drop count
without affecting terminal behavior. Snapshots are chronological copies; the sink never writes
console, storage, terminal wire, journal/snapshot/replay, SQLite, WebSocket attachment, or network
output. Production aggregation/query and any operator-controlled export remain a separate layer.

The two schema-v2 strict events contain only bounded enums, same-page monotonic durations, counts, and
bucketed frame sizes. Input/frame/cell/key content; session/client/connection/stream/generation/epoch/
sequence or snapshot identifiers and cursors; credentials; URLs; error strings; and hashes/digests of
these values are not fields.

The Host stderr adapter stops writing while the stream reports backpressure and drops diagnostics
until `drain`, so telemetry cannot grow an unbounded application-owned output queue. Phase 0c is only a
Cloud fact layer: per-input SHA-256 plus SQLite lease renewal and the larger Phase A correctness/hot-path
work remain unchanged. The Cloud query-contract layer makes these Cloud-local facts inspectable without
claiming a cross-runtime dashboard. Browser presentation segmentation is now locally observable. A
separate local synthetic pre-live input/recovery-liveness smoke exercises one fixed scenario: it holds a
cold snapshot GET at a test-only control point, verifies one pre-live Ctrl-C has one synthetic PTY effect,
then releases recovery and requires final `live` plus one fixed probe/result. Bounded retry/resync is
allowed; the invariant is eventual `live` without duplicate PTY effect. Its pass/fail result proves only
pending-recovery input reachability and eventual liveness, with no latency, throughput, rendering, or SLO evidence.
The smoke is one run of one scenario over loopback and Miniflare with a local-only multipart ETag
compatibility entry. It has no independently controlled Browser-to-Cloud and Cloud-to-Host RTT, jitter,
or loss, and proves no p95/p99, 99.9% success rate, at-most-5% instrumentation overhead, throughput, or
resource bound. Its fixed synthetic child/result is not a generic application effect, and a DOM result
is not pixel paint/composite. It does not replace model/property/fuzz, fault injection, multi-client,
fairness/load/soak, or production-staging validation. It also does not cover the IME, Unicode, CapsLock,
mouse, or paste input matrix. The local-only ETag
compatibility entry does not validate production Cloudflare Durable Objects or R2, and its timeouts are
hang guards rather than latency or recovery SLOs. Observing the snapshot GET does not prove that snapshot
was restored or adopted; fallback is allowed, so final `live` does not establish snapshot or tail continuity.
Future validation needs separate state-model/property/fuzz tests, fault injection, multi-client and
writer-transfer scenarios, load/soak tests, production staging, and independently controlled link
profiles, plus snapshot-adoption/tail-continuity and ACK identity/status/dedup tests. Performance
validation needs a separately designed, controlled, and reproducible test; this smoke does not implement
it, and those possible future tests are not completion conditions for the current smoke.
Ordinary directed recovery fanout currently remains weight 1 as a deliberate
P1 follow-up: measure its volume first, then give successful/stale/not-targeted outcomes an unbiased
producer sample without hiding bounded recovery failures. URL query-string redaction is also tracked
as platform-envelope hardening in addition to keeping automatic invocation logs and traces disabled.
