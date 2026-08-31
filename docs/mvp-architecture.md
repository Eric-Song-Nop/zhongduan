# MVP 架构

Zhongduan 把本地 PTY/terminal authority 通过 Cloudflare Durable Object提供给 Browser。当前架构只有一套
连接、恢复和输入实践；协议细节见[终端协议架构](terminal-protocol-architecture.md)与
[Recovery 协议](recovery-protocol.md)。

## Host daemon

Host 进程拥有 PTY、GhosttyCore、authority actor、mutation journal和 snapshot capture。`HostCloudRelay`
维护与 Cloud 的 control/data socket pair，并在 session生命周期内持有：

- `CanonicalPublisher`：持续发布 canonical mutation和有序 start fence；
- `SnapshotRefreshOwner`：独立 prime/refresh immutable snapshot；
- `RecoverySourceManager`：固化 recovery gap、grant、receipt、deadline与retained bytes；
- `RecoverySourceScheduler`：连接内 byte-DRR 与 shared socket backpressure。

Browser gap-fill 不使用全局 authority pause，也不保留另一套 barrier/replay scheduler。Snapshot capture 仍在
authority actor 内同步 encode；它造成的 pause 尚未通过 Roadmap R3 的严格有界 gate。

## Terminal Cloud

Worker负责认证、HTTP、snapshot upload/download和 WebSocket upgrade。每个 session映射到一个 SQLite-backed
`TerminalSessionDO`。DO持有 connection/generation、writer lease、authority head、recovery/lane ledger、
control outbox、snapshot retention、shared payload ring与 weighted delivery scheduler。

Durable scalar先提交，socket I/O后执行；休眠恢复从 SQL与strict attachment重建。任何非当前 schema marker
在首次启动时都会直接重建产品 SQL/KV 并关闭旧 socket，不提供兼容迁移。

## Browser

Browser `TerminalSession`为每个 connection set启动 generation-scoped `RecoveryRuntime`。Browser 提供 exact
visible replica candidate，Cloud 据此绑定 warm source，或绑定当前 immutable snapshot 作为 cold source；Runtime
只验证并执行已经绑定的 source，持续接受两条 lane、apply mutation，并通过 `WTermReplicaHost`原子 adopt。
完成后由 `RecoveryLiveReceiver`长期接管 live lane。Writer input使用独立 lease fence和 input epoch。

## 数据流

```text
PTY / semantic input
  -> Host authority actor
  -> canonical mutation journal
  -> Host data WebSocket
  -> TerminalSessionDO authority commit
  -> shared delivery ring + per-client envelope
  -> Browser RecoveryRuntime
  -> Ghostty/WTerm replica
```

Cold recovery另外通过 Cloudflare R2 snapshot建立 base；gap与live仍走同一 delivery owner。

## MVP 边界

当前 correctness gates覆盖本地 Host/DO/Browser owner组合、DO hibernation、故障注入、committed Ghostty WASM
continuation和 WTerm atomic adoption，但这些证据不等于 production readiness。阶段完成度、已知实现偏差和
唯一的全局 release gates 由 [MVP Roadmap](mvp-roadmap.md)维护；本页不另建一份容易漂移的清单。
