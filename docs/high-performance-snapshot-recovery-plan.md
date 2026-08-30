# 高性能 Snapshot 与 Recovery 计划

本文记录当前实现、MVP gate和后续优化。规范性协议见 [Recovery 协议](recovery-protocol.md)；已经删除的
早期方案见 [Recovery 实现沿革](recovery-history.md)。

## 正确性目标

无论 warm 或 cold，Browser可见状态必须是同一 authority mutation log 的连续前缀：

```text
base R + retained gap (R,H] + live [H+1,...]
```

系统不允许用socket交付、累计字节、下载完成或DOM替换尝试推断replica已应用。Receipt、ReplicaApplied和
Adopted分别由其真实owner发布。

## 当前实现

### Host

- `TerminalSession`串行化PTY、resize和input，拥有journal、cursor与snapshot cut；
- `CanonicalPublisher`持续发送canonical mutation，并在`H/H+1`之间插入start fence；
- `SnapshotRefreshOwner`独立prime和刷新immutable snapshot；
- `RecoverySourceManager`固化gap、校验grant/receipt、管理retained bytes与deadline；
- `RecoverySourceScheduler`使用byte-DRR，每record yield，共享data pressure只block source。

### Cloud

- SQLite保存generation、attempt、lane scalar、no-payload delivery ledger、control outbox与deadline；
- SQL phase为`queued/sending/sent`，socket send始终在transaction外；
- shared ring为live fanout只存一份physical payload，并持有per-client references；
- weighted scheduler在writer-live、observer-live、writer-recovery、observer-recovery之间提供确定性服务；
- admission同时计算实际obligation、recovery grant reservation、悲观record slot与writer headroom；
- alarm mux统一snapshot maintenance与recovery deadline的最早唤醒点。

### Browser

- `RecoveryAssembler`有界接受start、recovery、live和source closure的合法乱序；
- cold path通过HTTP/R2 restore detached candidate，warm path复用exact visible replica；
- mutation sink throw使target tainted，safe scalar不前进；
- `ReplicaHost`原子adopt，after-effect不确定时不dispose可能已visible的candidate；
- completion后`RecoveryLiveReceiver`接管长期live lane；
- active cursor始终绑定exact visible replica，ownership conflict后不可warm复用。

## 资源预算

生产常量由owner测试冻结；调整必须同时更新Host、Cloud和Browser的数学模型。当前重要边界：

- 一个session最多16个active recovery source/attempt；
- 单attempt delivery最多2 MiB / 1024 records；
- 单attempt live window最多512 KiB / 64 records；
- Host source aggregate retained上限2 MiB / 1024 records；
- Cloud session logical delivery上限2 MiB / 1024 records；
- 单envelope最大为header加16 KiB canonical payload；
- control outbox、canonical queue、shared ring和snapshot metadata另有独立上限。

Grant是Cloud已经承诺接收的byte/record reservation，不能在Host合法发送后反悔。Receipt释放exact prefix，
allocator再以稳定顺序补window。

## Hibernation 与 crash cut

- `queued`或`sending`跨休眠表示payload结果不可证明，fence exact generation；
- `sent`跨休眠只等receipt，不重发data；
- control outbox可以重放exact JSON，并以payload CAS ACK；
- Browser pair缺失、重复或lookup key不一致时fence owner；
- complete owner缺pair只做local delivery fence，不向已关闭Host source发送reset；
- Host/Browser replacement在任何late async callback前先改变owner token/generation。

## Snapshot

Snapshot capture在authority actor barrier内完成；metadata绑定session epoch、engine/artifact、cut cursor、摘要、
压缩和immutable object key。Cloud只发布finalized对象，retention union所有active recovery pin。Browser restore
必须使用匹配artifact的passive engine并在tail连续后才adopt。

当前实现继续使用full snapshot。Rolling/delta snapshot只有在以下gate同时满足后进入实现：immutable cut、
content integrity、bounded chain、cancellation、GC pin、restore reference model和真实enginecontinuity。

## 测试证据

直接owner测试覆盖：

- scalar continuity、same-ordinal conflict、prefix receipt和u64边界；
- H/fence/H+1、warm/cold、Done/SourceClosed到达顺序；
- queued/sending/sent crash cut、outbox replay与exact destination；
- Host/Browser replacement、slow observer、writer reserve与generation isolation；
- variable-size DRR、每record yield、control/input interleave和capacity边界；
- real local Durable Object/SQL/R2/hibernating WebSocket integration；
- committed Ghostty WASM uninterrupted-vs-restored continuation；
- jsdom中的production WTermReplicaHost atomic adoption。

本地three-owner harness的authority/replica和fault seams包含可控测试owner；它不是三进程真实网络、OS crash、
物理send cut、socket pressure或性能/SLO证据。

## MVP 前 gate

- production-like Cloudflare WebSocket/R2/DO hibernation演练；
- 真PTY与真实Browser的长会话continuity；
- 16-client容量、slow-client和writer input soak；
- snapshot upload/download失败与长期retention/GC；
- 可观测性、告警、故障诊断与部署回滚演练；
- security review、resource-limit复核与显式上线批准。

MVP尚未发布，因此当前实现不保留已删除协议的runtime、wire、schema或兼容分支。
