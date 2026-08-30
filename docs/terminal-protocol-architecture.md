# 终端协议架构

本文定义 Zhongduan 当前唯一实现必须遵守的 owner、顺序和故障边界。具体 frame 见
[Wire protocol](wire-protocol.md)，恢复状态机见 [Recovery 协议](recovery-protocol.md)。

## 最高级不变量

一个 terminal session 只有一个 authority。每个可见 replica 必须能证明自己是 authority log 的连续前缀：

```text
snapshot base + recovery gap + live continuation = one ordered mutation prefix
```

任何 receipt、socket send 成功、snapshot 下载完成或 DOM replacement 尝试都不能替代“mutation 已应用到
当前可见 exact owner”的事实。

## 三个逻辑平面

### Authority plane

Host terminal actor 串行化 PTY output、resize 与 semantic input，生成 canonical mutation identity 和最高已
提交 cursor。Journal、snapshot capture、writer fence 与 input dedupe 都从这个 actor 取得顺序。

### Delivery plane

Host、Cloud 和 Browser 分别拥有 retained source、durable scalar ledger、in-memory payload obligation 与
replica apply state。Delivery 可以并发、重试、休眠或隔离，但不能改变 canonical identity。

### Control/input plane

Writer lease 与 input epoch 独立于 delivery credit。Control/input handler 不等待 data pump；Host input ACK
必须按 exact writer fence、epoch 和 sequence返回。Recovery 的慢 client 不能阻塞 authority、writer input或
其他 Browser。

## Owner 层级

```text
TerminalSession actor
  ├── canonical journal + snapshot cut
  ├── writer/input truth
  └── HostCloudRelay
       ├── CanonicalPublisher
       ├── SnapshotRefreshOwner
       └── RecoverySourceManager
            └── connection-local RecoverySourceScheduler

TerminalSessionDO
  ├── connection/generation + writer lease SQL
  ├── recovery/lane/outbox/deadline SQL
  ├── shared delivery ring
  └── weighted delivery scheduler

Browser TerminalSession
  └── generation-scoped RecoveryRuntime
       ├── RecoveryAssembler
       ├── detached snapshot candidate
       └── RecoveryLiveReceiver
```

每个 async completion 在写入 owner 前都必须重新验证 generation、connection、stream、Host fence、recovery
id 和 owner token。旧 callback 只能 no-op 或使旧 owner失败，不能触碰新 owner。

## Warm 与 cold

Warm 和 cold 使用同一 RecoveryStart、lane、receipt、apply、adopt 与 live handoff：

- warm base 来自 Browser 当前可复用的 exact visible replica；
- cold base 来自 Host capture 并由 Cloud发布的 immutable snapshot；
- 两者都以 Host 固化 `(R,H]` gap，并以 `H / fence(H) / H+1` 安装 live floor；
- 两者都只有在 apply 与 handoff完成后才发布 active cursor。

差异只在 base 的来源与 cold candidate 的 dispose ownership，不存在另一套 replay protocol。

## Progress scalar

必须分别持久化和验证：

- Host authority committed cursor；
- 每条 delivery lane 的 admitted、sent、received ordinal/cumulative bytes；
- Browser latest replica-applied cursor；
- recovery adopted identity；
- source done/closed prefix。

`received` 只释放 transport/payload credit；`replica-applied` 才能推进可复用 authority prefix；`adopted` 只表示
exact candidate 已成为 visible owner。它们不能根据大小关系互相补写。

## Snapshot 与 identity

Snapshot metadata绑定 session、session epoch、engine/artifact identity、cut cursor、压缩、摘要与 object key。
Restore 必须使用 committed runtime artifact；engine mismatch、摘要错误、partial parser continuation或非原子 DOM
swap 全部 fail closed。

Host 的 snapshot refresh 生命周期独立于 Browser 连接。恢复请求只选择已经 finalized 的 snapshot，不触发
临时 capture 来改变 start 顺序。

## Backpressure 与公平性

- Host canonical queue 有独立上限，不因 recovery 暂停；
- Host source scheduler 采用 byte-DRR，每 record yield，socket pressure 只 block source；
- Cloud shared ring按 unique payload 计物理容量，按 client obligation 计逻辑容量；
- Cloud weighted scheduler保留 writer/live进展，并让 recovery 获得有界服务；
- credit 与悲观 record reservation在发给 Host 前即纳入 session aggregate；
- 任一 client/source超限只 fence exact owner。

正确性测试使用确定性的 bytes、records、turns 和 identity，不用 wall-clock throughput 代替公平性证明。

## Durable Object 边界

Durable Object 在 transaction 内只写同步 scalar。WebSocket I/O、R2、serialization 和任何 await 都在 commit
后发生。Hibernation 后只从 SQL 与 strict attachment恢复；内存 payload不被假装存在。

构造顺序必须先建立共享 alarm owner，再 reconcile delivery owner，之后才初始化 snapshot/recovery
maintenance 和 drain outbox。Alarm 取所有 component deadline 的最小值，旧 handler 失败不能吞掉更早事实。

## Failure semantics

无法证明“send前失败”或“send后失败”时，结果一律 outcome uncertain：

- data payload不猜测重发；fence exact generation并重新规划；
- durable control JSON允许 exact idempotent replay；
- apply sink throw使visible target tainted，active cursor不可复用；
- adopt after-effect throw放弃candidate dispose ownership并要求cold replan；
- Host/source deadline只retire exact source；shared pair保持健康；
- unknown或divergent identity关闭其当前安全边界。

## 不存在的模式

connection set始终以control/data pair整体换代，socket attachment只有当前严格结构，恢复期间Host canonical
发布持续前进。发现非当前开发态durable schema时直接重建；已删除设计的原因见
[Recovery 实现沿革](recovery-history.md)。
