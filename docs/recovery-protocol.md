# Recovery 协议

本文是 Zhongduan 唯一 Recovery 实现的规范。代码不得以产品版本或兼容策略描述另一套恢复路径；历史
设计只记录在 [Recovery 实现沿革](recovery-history.md)。

## 目标

当 Browser 新建或重建 delivery generation 时，系统必须在 Host authority 持续产生 mutation 的同时：

1. 选择可信的 warm cursor，或选择已发布 snapshot 作为 cold base；
2. 固化 base 之后、安装 fence 之前的 recovery gap；
3. 在 canonical `H` 与 `H+1` 之间安装有序 start fence；
4. 独立传输 recovery lane 与 live lane；
5. 只在 Browser 实际 apply 并原子 adopt 后公布新的可见 owner；
6. 在任意断开、休眠、超时或 outcome-uncertain 时只隔离精确 generation。

Authority cursor、delivery receipt 和 replica apply progress 是三种不同事实，任何实现都不得相互推断。

## 唯一连接模型

一个 session 使用 Host control/data socket pair，以及每个 Browser connection set 的 control/data socket
pair。connection set 建立时 Cloud 必须已经拥有：

- exact ready Host fence；
- 可服务的 published snapshot；
- 新的 Browser delivery generation。

不存在协议协商、策略选择或降级。缺少任一前置条件时，连接请求失败；同一 generation 不切换恢复实现，
data socket 也不能脱离 control socket 单独换代。

Socket attachment 仅保存严格的路由、租约与 readiness 身份：peer、channel、subject、client、role、
connection、stream、generation、Host fence、writer lease fence、ready 状态，以及 Browser recovery lookup
key。恢复进度和 payload 不写入 attachment；未知字段或不完整身份一律关闭。

## 数据模型

### Canonical mutation

Host authority 为每个 mutation 生成不可变的 canonical frame。其身份由 session epoch、event sequence、PTY
offset、kind 和 payload 共同约束。Cloud 只验证并提交连续的 authority head，不能为某个 Browser 重写
canonical identity。

### Delivery envelope

Cloud 向 Browser 发送的每个 mutation 都包在 generation-scoped envelope 中。Envelope 至少携带：

- stream 与 delivery generation；
- `recovery` 或 `live` lane；
- lane ordinal；
- cumulative encoded bytes；
- 完整 canonical frame。

Ordinal 从 1 开始且逐一连续。累计字节必须精确等于上一记录累计值加当前完整 wire bytes。Browser 只对
完整校验并进入有界 owner 的 envelope 发布 receipt；Cloud 只接受 durable ledger 中已发送记录的 exact
prefix receipt。

### Start fence

Host 在 canonical `H` 后、`H+1` 前插入 RecoveryStartFence。Cloud 在同一 Host data FIFO 内事务性安装
RecoveryStart，并在安装提交后才处理 `H+1`。Start 固定：

- recovery id 与 exact routing identity；
- warm 或 snapshot source；
- base cursor、fence cursor 与 live floor；
- engine/artifact identity；
- 两条 lane 的初始 scalar。

Host 只有收到 start-ready 后才获得 source grant。初始 grant 为零；Cloud durable ACK start-ready 后再按
session 容量分配正 credit，避免 grant 越过 start-ready。

## Host owner

`RecoverySourceManager` 是 session-scoped retained-source owner。每条 Host connection 使用独立 owner token；
连接替换先 fence token，再释放其 source。Manager 固化 `(R,H]` 的 exact canonical bytes，并负责：

- per-source 与 session aggregate 上限；
- cumulative byte grant；
- multi-outstanding prefix receipt；
- release-once 与 source-closed certificate；
- generation tombstone、deadline 和 late-control isolation。

`RecoverySourceScheduler` 只拥有 runnable queue、deficit 与 yield，不拥有 payload truth。它采用 byte-DRR，
每条记录独立让出 event-loop turn；shared data pressure 只暂停 recovery source，不关闭 Host pair。真正的
socket send throw 才是 pair-level outcome uncertainty。

Canonical publisher 不为 recovery 暂停。它保留 `H / fence(H) / H+1` 顺序、队列上限、socket pressure
检查和 bounded yield。

## Cloud durable owner

Durable Object 的 SQL 是 generation、lane、outbox、deadline 与容量的唯一 durable truth。Data payload
只存在于有界内存 ring：

- live canonical payload 在多个 Browser obligation 之间物理共享一次；
- recovery payload 保留 exact source copy，直到 send 结果被持久化；
- SQL ledger 只保存 ordinal、累计字节、authority cursor、wire size 和 `queued/sending/sent` phase；
- SQL 不保存 mutation payload 或 hash。

发送顺序固定为：事务提交 `queued`，事务 CAS 到 `sending`，事务外执行 `WebSocket.send`，再用独立事务
确认 `sent`。send throw、socket replacement，或休眠后发现 `queued/sending` 都 fence 精确 generation。
`sent` 记录在休眠后只等待 receipt，绝不猜测或重发 data payload。

Control outbox 持久化完整、有界 JSON。每次发送前重读 exact outbox row 与 current destination identity；send
成功后用 exact payload CAS 删除。send 后 crash 允许同一 control JSON 重放，peer 必须幂等。

Session admission 同时约束实际 live obligation、recovery grant reservation、悲观 record reservation、writer
headroom 和共享 ring 的物理用量。Writer admission 可事务性 fence observer generation，但不能回滚 authority
head 或影响其他健康 owner。

## Browser owner

`RecoveryRuntime` 是一个 delivery generation 的唯一 Browser owner：

- `RecoveryAssembler` 接收可乱序到达的 start、recovery envelope、live envelope 和 source closure；
- warm source 写入 exact current replica；cold source在 detached candidate 上 restore snapshot；
- apply 成功后才推进 safe scalar；sink throw 一律视为 apply outcome uncertain；
- cold handoff 通过 ReplicaHost 原子 adopt，失败不得同时暴露两个可写 owner；
- completion 后 `RecoveryLiveReceiver` 以 exact completion seed 接管长期 live lane；
- visible replica identity 一旦与 runtime 持有的 exact owner 不同，active cursor 永久不可复用。

Receipt、ReplicaApplied 和 RecoveryAdopted control 会在稳定进度计时器上重复发送，直到对端状态收敛；同一
exact frame 幂等，身份偏差、gap 或 mixed scalar fail closed。

## Snapshot

Host relay 独立维护 snapshot refresh owner，不依赖 Browser attach 或 recovery scheduler 触发。Snapshot capture
在 authority actor barrier 内取得 immutable cut；发布使用 engine/artifact identity、内容摘要和 immutable
object key。Cloud 只有 finalized、可验证的 snapshot 才能成为 cold source。

Browser 直接把 RecoveryStart 的 snapshot source交给 transport和 ReplicaHost，不再合成另一种 manifest
协议。Restore 在 passive/discard-effects 模式完成，tail 继续后才允许 atomic adopt。

## 失败与隔离

- Host replacement：fence old Host token、所有绑定旧 fence 的 Browser generation，并要求新连接集。
- Browser control/data 任一关闭：fence整套 generation、释放 writer lease 和 ring references。
- Data send outcome uncertain：只 fence对应 generation；authority 与其他 client继续。
- Control send outcome uncertain：依 destination owner fence Host pair或 Browser generation。
- Deadline：只 retire exact recovery source/attempt，不关闭健康的共享 Host pair。
- 重复、旧 generation 或已退休 exact control：幂等 no-op；identity divergence fail closed。
- 休眠恢复：先初始化 alarm owner，再 reconcile SQL/attachment/ring ownership，之后才 drain outbox。

## 实现约束

- `transactionSync` 内只能读写同步 durable scalar；不能 send/close socket，不能 await，也不能访问 R2。
- Durable Object 的 `waitUntil()` 不延长生命周期；data pump 由成员 promise与每记录的 pending timer/I/O跟踪。
- control/input 处理不等待 data pump；每条 data record 之间必须 yield。
- 所有 collection、outbox、queue、ring、source、generation 与 deadline都有显式上限。
- 默认运行时只有本文实现；不存在 kill switch、兼容 decoder、策略字段或旧代码路径。
