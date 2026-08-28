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

The initial Phase 0a integration exposes low-frequency Host snapshot and retained-journal facts.
The Host stderr adapter stops writing while the stream reports backpressure and drops diagnostics
until `drain`, so telemetry cannot grow an unbounded application-owned output queue.
Cloud queue/lease, Browser restore/adopt, input-to-PTY, link RTT, ingestion, and dashboard work remain
part of the open Phase 0 gate.
