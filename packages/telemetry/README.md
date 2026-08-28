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
ingestion during that compatibility window. It validates the event payload after ingestion has
projected and checked the `type=zhongduan.telemetry` / `runtime=cloud-do` log wrapper; a strict
record-level read schema and wrapped-log integration fixture remain a query-layer follow-up. New
Phase 0c event names are never accepted with v1.
The runtime gate itself accepts only the mode paired with its code. Separately deploying or rolling
back code and `CLOUD_TELEMETRY_MODE` can therefore disable custom logging and create an irrecoverable
telemetry blind window; code and binding should ship as one version, and v1 queries should remain until
v2 arrival is verified.

Automatic invocation logs and tracing remain explicitly disabled because request URLs contain terminal
session metadata and one-time WebSocket tickets. Strict custom event payloads must not contain terminal,
input, or frame payloads; text, paste, command, cell, or key material; tickets, capabilities, or writer
leases; session, client, connection, stream, generation, epoch, or sequence identifiers; journal or
snapshot identifiers/cursors; URLs; error/exception strings; or hashes/digests of those sensitive
values. Sizes are bucketed. The Cloudflare log envelope can still add platform identifiers and must be
handled as sensitive metadata.

The Browser fact slice uses `clockKind=browser-performance` and records only same-page monotonic
durations for control/data relay probes, semantic-input acknowledgement, attach-start terminal outcomes, snapshot load,
passive restore, buffered-tail application, synchronous replica `adopt()` return, and the terminal recovery
outcome. An adopt event is not proof that pixels were painted, and an input acknowledgement is not an
application-effect acknowledgement. Input sequence, delivery generation, session/client identifiers,
snapshot identifiers, credentials, URLs, terminal content, and error strings are not event fields.

`createBrowserDiagnostics()` is an in-memory-only sink. Admission is deferred behind a bounded queue;
strict validation happens during its drain into a fixed-size circular ring. The pending queue and ring
are each hard-bounded at 256 events (and may be configured smaller). A full pending queue, invalid event, scheduler failure, or overwritten ring
entry increments the drop count without affecting terminal behavior. Snapshots are chronological copies;
the sink never writes console, storage, or network output. Production aggregation/query and any
operator-controlled export remain a separate layer.

The Host stderr adapter stops writing while the stream reports backpressure and drops diagnostics
until `drain`, so telemetry cannot grow an unbounded application-owned output queue. Phase 0c is only a
Cloud fact layer: per-input SHA-256 plus SQLite lease renewal and the larger Phase A correctness/hot-path
work remain unchanged. Browser paint/end-to-end facts, production aggregation/query/dashboard, and the
enabled-instrumentation input-latency/canonical-throughput canary proving at most 5% regression remain
open Phase 0 gates. Ordinary directed recovery fanout currently remains weight 1 as a deliberate
P1 follow-up: measure its volume first, then give successful/stale/not-targeted outcomes an unbiased
producer sample without hiding bounded recovery failures. URL query-string redaction is also tracked
as platform-envelope hardening in addition to keeping automatic invocation logs and traces disabled.
