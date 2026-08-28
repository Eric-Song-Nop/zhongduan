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

The Cloud Phase 0b slice adds content-free events for the current single relay queue, Browser semantic
input lease/Host-send decisions, delivery barriers, attach, and delivery reset. Successful high-rate
queue/input events use a randomized per-DO phase with systematic producer sampling and carry
`sampleWeight`; failures, capacity decisions, and recovery transitions are retained best-effort. If
secure phase selection fails, that sampled series is dropped rather than assigned a biased weight.
Cloud durations use `clockKind=workers-io`: production
Workers clocks advance only at I/O boundaries, so these values are local lower bounds and may report
zero for synchronous CPU work. A local `WebSocket.send()` return is not peer receipt or application.

The production Cloud sink is enabled only by the exact versioned mode `workers-logs-v1`, buffers at
most 64 events per DO instance, and may lose pending diagnostics on hibernation. Automatic invocation
logs and tracing remain explicitly disabled because request URLs contain terminal session metadata and
one-time WebSocket tickets. The strict event payload contains no raw identifiers or content; the
Cloudflare log envelope can still add platform identifiers and must be handled as sensitive metadata.

The Host stderr adapter stops writing while the stream reports backpressure and drops diagnostics
until `drain`, so telemetry cannot grow an unbounded application-owned output queue. Cloud Host-ACK/data
fanout, Browser restore/adopt, Browser RTT, production aggregation/query, and a unified dashboard remain
part of the open Phase 0 gate.
