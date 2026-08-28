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

The Host stderr adapter stops writing while the stream reports backpressure and drops diagnostics
until `drain`, so telemetry cannot grow an unbounded application-owned output queue. Cloud queue/lease,
Browser restore/adopt, Browser RTT, production ingestion/query, and a unified dashboard remain part of
the open Phase 0 gate.
