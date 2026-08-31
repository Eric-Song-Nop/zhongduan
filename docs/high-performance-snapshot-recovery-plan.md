# 高性能 Snapshot 与 Recovery 计划

本文是 Recovery/Snapshot 主路线 R0–R5 的技术计划。阶段状态、发布范围和全局 MVP gate 以
[MVP Roadmap](mvp-roadmap.md)为准；当前 runtime 规范见 [Recovery 协议](recovery-protocol.md)；已经删除的
早期实现见 [Recovery 实现沿革](recovery-history.md)。

> 状态基线：2026-08-31。R0–R2 已完成并合入 `main`；R3、R4 部分完成但关键 gate 未过；R5 产品层未开始。
> 本文恢复 2026-08-28 正式重排后的 Phase 0–5，不恢复更早初稿的编号，也不恢复已经删除的兼容 runtime。

## 产品目标与最高不变量

Browser 采用的 execution terminal state 必须始终等于一个 immutable checkpoint，加同一 authority mutation
log 的连续前缀：

```text
base checkpoint@R + retained gap (R,H] + live [H+1,K]
```

这意味着：

- Snapshot 可以吸收 cut 之前的 mutation，但不能授权跳过 cut 之后尚未 apply 的 mutation；
- transport receipt、累计字节、下载结束和 DOM replace attempt 都不能冒充 replica apply；
- Host Ghostty core 是唯一 authority；Browser prediction 永远只是可丢弃 presentation branch；
- recovery source、ledger、queue、ring、outbox、snapshot body、deadline 与 retry 都必须有 owner 和 hard cap；
- 一个慢 client、失败 generation 或休眠恢复只能隔离自己的 owner，不能暂停 authority 或拖垮健康 client；
- 当前计划先保证 correctness 与 boundedness，再用真实 workload 决定 rolling/delta、history page 和调优。

## 当前数据与 ownership 模型

```text
PTY / resize / semantic input
  -> TerminalSession authority actor
  -> canonical mutation + EventJournal
  -> CanonicalPublisher
  -> Host data WebSocket
  -> TerminalSessionDO durable authority head
  -> RelayDeliveryRing / RelayDeliveryScheduler
  -> Browser RecoveryRuntime / RecoveryLiveReceiver
  -> Ghostty/WTerm replica
```

Cold path 另外从 Cloudflare R2 snapshot 建立 immutable base；warm path 复用 exact visible replica。两者随后都接收 retained
gap 与 live lane，并且只能按 authority cursor 连续 apply。

### Host owner

- `TerminalSession` 串行化 PTY output、resize、semantic input、snapshot cut 与 recovery gap preparation；
- `EventJournal.planRangeThrough()` 先返回 exact frames/bytes scalar，materialize 时复核同一 revision；
- `CanonicalPublisher` 持续发送 canonical mutation，并在 `H` 与 `H+1` 之间插入 start fence；
- `SnapshotCheckpointManager` 拥有 capture→compress→publish→install single-flight；
- `SnapshotRefreshOwner` 在 session 生命周期内 prime，并按固定周期刷新 full snapshot；
- `RecoverySourceManager` 拥有 retained gap、grant、receipt、deadline、tombstone 和 release-once；
- `RecoverySourceScheduler` 使用 byte-DRR，每 record yield；connection-wide data pressure 会暂停该 Host connection
  的 recovery 调度，但不会关闭 pair 或推进 source cursor，control/canonical 仍由各自 owner 调度。

### Cloud owner

- SQLite 保存 connection/generation、attempt、lane scalar、no-payload delivery ledger、control outbox、deadline 和 pin；
- delivery phase 为 `queued -> sending -> sent`，socket send 必须在 transaction 外；
- `queued`/`sending` 跨 hibernation 表示结果不可证明，必须 fence exact generation；
- `sent` 跨 hibernation只等 exact receipt，绝不猜测重发；
- shared ring 为一条 live canonical payload 保留一个 physical copy 和 per-client references；
- weighted scheduler 在 writer-live、observer-live、writer-recovery、observer-recovery 间确定性调度；
- admission 同时计算实际 obligation、recovery grant reservation、悲观 record slot 和 writer headroom；
- snapshot retention union latest、active recovery pins、recent snapshots 与 pending upload owner。

### Browser owner

- `RecoveryAssembler` 有界接受 start、recovery、live 与 source closure 的合法乱序；
- cold restore 使用 detached candidate 和 passive/discard-effects Ghostty runtime；
- warm restore 只复用 exact visible replica；ownership 不匹配后永久失去 warm reuse 资格；
- `ReplicaApplied` 只能在 mutation sink 成功后推进；sink throw 使 target tainted；
- `WTermReplicaHost` 原子 adopt；after-effect 不确定时不能 dispose 可能已 visible 的 candidate；
- completion 后 `RecoveryLiveReceiver` 接管长期 live lane；
- active cursor 始终绑定 exact visible replica，不能把已经失去 ownership 的 cursor 当 warm base。

## 当前资源边界

下列是代码和 owner tests 锁定的 implementation hard caps，不等于已经由 staging 批准的 release limits：

- Host journal 保留最多 60 秒 / 8 MiB，segment target 为 250 毫秒 / 256 KiB；
- Host canonical queue 最多 8 MiB / 1024 frames；
- Host recovery manager 最多占用 16 个 stream slots；slot 计入 pending、active 或 retained tombstone，不能只称
  active source。单 source gap 最多 256 KiB / 512 canonical frames，所有 source retained envelope 合计最多
  2 MiB / 1024 records；
- Cloud 最多 16 个 attempt rows；单 attempt delivery 最多 2 MiB / 1024 records，live 子窗口最多
  512 KiB / 64 records，recovery grant window 最多 96 KiB；
- Cloud session logical delivery 最多 2 MiB / 1024 records，其中 recovery reservation 最多
  1.5 MiB / 1008 records，并为 writer 保留一个最大 envelope 和一个 record；shared physical ring 也独立限制为
  2 MiB / 1024 entries/references；
- Browser assembler 最多拥有 2 MiB / 1024 frames，gap span 最多 1024 events，每次 apply 最多 32 frames；
  no-progress deadline 为 15 秒，generation deadline 为 60 秒；
- 单 canonical payload 最大 16 KiB；snapshot compressed body 最大 32 MiB、uncompressed 最大 128 MiB；
- Cloud snapshot retention 的 `snapshot` 与 `snapshot_upload` SQL rows 合计最多 32，其中所有状态的
  `snapshot_upload` rows 合计最多 4；保护集合另保留 latest、最多 16 个 active recovery pins 与 2 个 recent grace；
- control/outbox、Host canonical queue、snapshot metadata 和各 socket message 另有独立 cap。

Grant 是 Cloud 已承诺接收的 byte/record reservation，不能在 Host 合法发送后反悔。Receipt 释放 exact prefix，
allocator 再按稳定顺序补 window。

## Phase 状态总表

| Phase | 正式目标                                      | 当前状态       | 核心偏差                                                 |
| ----- | --------------------------------------------- | -------------- | -------------------------------------------------------- |
| R0    | Recovery 活性、closed outcome owner、deadline | 已完成并被吸收 | 旧 runtime 已删除，只保留不变量                          |
| R1    | Checkpoint ownership 与动态 serviceability    | 已完成         | serviceability 从 attach admission 移到 Host gap prepare |
| R2    | Concurrent gap-fill Recovery                  | 已完成         | 过渡协商被破坏性单实现切换取代                           |
| R3    | 严格有界 immutable cut                        | 部分完成       | 同步 WASM encode 仍不可抢占                              |
| R4    | Rolling snapshot 与 snapshot-aware planner    | 部分完成       | 只有固定周期 full refresh，没有 planner                  |
| R5    | Execution checkpoint 与 history pages         | 产品层未开始   | fork 有 primitive，产品仍完整 FINISH 后 adopt            |

## R0：Recovery 活性

R0 原本针对已经删除的 fixed-commit runtime，要求：

- non-ready outcome 必须有真实 producer、owner 和状态转换；
- warm seed 缺失必须转 cold 或明确 fence，不能永久 catching-up；
- snapshot missing/mismatch 必须 refresh 或失败；
- Browser control failure 只隔离对应 client；
- attach/start/completion 都必须有 bounded watchdog；
- outcome uncertainty 不能 same-generation 猜测 retry。

当前 runtime 已用 generation-scoped attempt、full-pair fence、progress retry 和 assembler deadline 承接这些原则。
旧 barrier、directed replay、pin state 与 compatibility outcome 不属于当前实现，也不是迁移要求。

## R1：Checkpoint ownership 与 serviceability

R1 已完成的核心语义：

1. session-owned manager 跨 relay connection/Browser attach 复用 latest immutable checkpoint；
2. latest/high-water 单调，只有 validated forward replacement 或 dispose 才释放；
3. journal 先 plan exact range scalar，再从同一 revision materialize；
4. capture、compress、upload、publish 和 cleanup body 都有独立 owner 与 cap；
5. pending upload 在 resume/publish/install 边界复核 identity、lineage 与 minimum cut；
6. 多 waiter 共享 manager flight，单 waiter cancel 不取消公共工作；
7. cursor-ahead、ambiguous result、upload failure 和 replacement follow-up 有明确 deadline/backoff。

计划偏差：Snapshot production 后来从 attach/request-driven 变成独立 session owner 主动 prime/refresh。
`SnapshotRefreshOwner` 使用 unconditional snapshot admission；某个 checkpoint 是否能为当前 recovery 服务，改由
`TerminalSession.prepareRecoveryGap()` 在冻结 `(R,H]` 时判断 journal gap 和 capacity。

## R2：Concurrent gap-fill Recovery

R2 已完成的完整 owner 链：

1. Browser claim generation 并报告 exact visible candidate；Cloud 绑定 ready Host fence，并选择 exact warm replica
   source 或当前 immutable snapshot source；
2. Host 在 authority actor 中冻结 `H` 与 `(R,H]`；
3. `CanonicalPublisher` 保证 `H / RecoveryStartFence(H) / H+1`；
4. Host source 在累计 grant 内发送多个 retained envelope，最终以 `RecoveryDone` 闭合；
5. Cloud 将 start/attempt/lane/outbox 作为 SQL scalar truth，不持久化 mutation payload/hash；
6. Browser 可在 restore 尚未完成时接受 recovery/live/closure，但只 apply 连续 authority prefix；
7. cold candidate restore、gap apply、atomic adopt、Adopted/SourceClosed 完成后进入长期 live；
8. receipt、replica apply 与 adoption 分别由 transport、mutation sink、visible owner 发布；
9. Host/Browser replacement、hibernate wake、send throw 和 deadline 按 exact generation 隔离；
10. Host 和 Cloud scheduler 在本地 owner model 与 hard cap 内提供多 source/client 有界、公平的调度机会；
    真实 transport progress 仍需 staging 证明。

### R2 实施切片与当前保留面

| Slice | 原交付                                                 | 当前保留面                                             |
| ----- | ------------------------------------------------------ | ------------------------------------------------------ |
| 2.0   | strict wire、cursor、lane、fence                       | 保留并改成无版本 canonical API                         |
| 2.1   | pure Browser assembler                                 | 保留，已接 `RecoveryRuntime`                           |
| 2.2   | Host PreparedGap/source                                | 保留，旧 pause/barrier scheduler 删除                  |
| 2.3   | Cloud durable scalar/outbox/alarm                      | 保留，旧 schema migration 改成 destructive initializer |
| 2.4   | 三端 runtime wiring                                    | 保留，capability gate/kill switch 删除                 |
| 2.5a  | no-payload ledger 与 cold-wake fence                   | 保留                                                   |
| 2.5b  | Host multi-outstanding/DRR/backpressure/deadline       | 保留                                                   |
| 2.5c  | Cloud shared ring/capacity/writer reserve/weighted DRR | 保留                                                   |
| 2.6   | three-owner、Ghostty continuation、WTerm adoption      | 保留；旧 strategy rollout/downgrade gate 删除          |

### R2 证据边界

- pure assembler 有独立 reference reducer、lane interleaving、conflict、budget、deadline 与 dispose-once tests；
- Host owner tests 覆盖 fixed gap、`H/fence/H+1`、grant/receipt、send throw、DRR 和 source isolation；
- Worker tests 覆盖 SQL transaction、outbox CAS、hibernate、ring refs、capacity、writer reserve 和 replacement；
- local three-owner harness 接入真实 Host owners、本地 workerd DO/SQL/R2/hibernating WebSocket 与 Browser runtime；
- committed-WASM tests 独立验证 Ghostty uninterrupted 与 snapshot+tail continuation；
- jsdom test 独立验证 production `WTermReplicaHost` atomic adoption。

这些是互补的本地 correctness 证据，不是单一真实三进程 E2E，也不证明真实网络、物理 backpressure、OS crash、
像素输出、性能或 SLO。

## R3：严格有界 immutable cut

### 问题

当前 snapshot capture 在 `TerminalSession` actor queue 中执行：先采样 cursor，再同步调用
`authority.encodeSnapshot().slice()`。这保证 cut 原子和 bytes ownership，但同步 WASM encode 返回前，PTY output、
input、resize、Ctrl-C 和 snapshot timer 都不能在同一 event loop 上运行。

`SnapshotCheckpointManager` 的 5 秒 timer 只能与异步 Promise race；它不能抢占正在执行的同步 WASM。
`encodeMs` 也只是完成后的检查，不是 actor hard max。

### 已完成的前置

- cut `eventSeq/nextPtyOffset` 在一个 actor turn 中原子采样；
- reentrant PTY ingress 在 capture 中只排队，不会混进该 cut；
- snapshot bytes 独立，不别名 authority memory；
- 压缩、hash、upload 和 publish 已移出 actor；
- snapshot lineage、engine/artifact identity、digest 和 immutable object 已受校验；
- Ghostty parser continuation、UTF-8/CSI/OSC/DCS、resize、modes 和 scrollback 有真实 WASM fixtures。

### Target contract

概念上的 ownership transition 是：

```text
authority actor: freeze -> immutable cut + authority cursor
bounded encoder: immutable cut + budget + cancellation -> snapshot | unavailable
```

具体 API 名称不在本计划中冻结，语义要求是：

- freeze 在 authority actor 中严格有界；
- frozen state immutable，后续 encode 不读取可变 authority；
- COW、bounded copy/off-actor 或 incremental/yield 至少选择一种并证明 upper bound；
- 超预算只使本次 snapshot unavailable，不得使 authority session fail；
- cancel/dispose/late completion release once；
- snapshot 与后续 exact tail 等于 uninterrupted authority。

### Exit criteria

1. `capture_actor_pause_ms` 有 per-session monotonic measurement、p50/p95/p99/max；
2. hard max 是可执行边界，不是事后日志；
3. 大 history、wide screen、partial parser state 与同步输出下仍满足相同边界；
4. capture 期间持续 PTY output、10–60 key/s、Ctrl-C 和 resize 不发生 silent loss、duplicate 或无界等待；
5. encode throw、OOM、timeout、cancel 只报告 unavailable 并保持 authority 可用；
6. 此 gate 未过不得启用真实 rolling hard freshness deadline。

## R4：Rolling snapshot 与 snapshot-aware planner

### 当前已有的子集

- `SnapshotRefreshOwner.start()` 立即 prime；
- flight 成功后 30 秒、失败后 30 秒再次尝试；
- authority cursor 未前进时复用 latest checkpoint；
- manager 保持 single-flight、minimum cut、follow-up 与 cancellation；
- manager/publisher 路径有 minimum build interval、failure backoff、publish deadline 与 body cap；
- Cloud retention/pin owner 有 records、uploads、recent 与 active recovery 上限。

这是一套固定周期 full snapshot refresh，不是 R4 planner。由于 timer 在一次 capture/publish 完成后才重新开始，
长 encode/upload 也会把下一次 refresh 任意后移。

### Target planner

Planner 的输入至少包括：

- latest checkpoint age、cut、encoded size 和 engine/artifact identity；
- journal tail frames、bytes、age 和是否存在 gap；
- capture/encode/upload/download/restore/apply 的实测成本；
- authority dirty/output rate、quiet opportunity 与 hard freshness pressure；
- active/preparing/started attempt、attach pressure 和 snapshot pins；
- current resource pressure、failure backoff 与 retention capacity。

Planner 的动作只能是：

```text
reuse latest
request bounded full cut
request bounded rolling/delta cut   // 只有对应 schema/gate 完成后
defer until quiet/deadline
report unavailable
```

不能用 planner 跳过未被 snapshot 吸收的 mutation，也不能修改已 start attempt 的 source。新 checkpoint 只允许
影响尚未 start 的 attempt；已 start owner 若必须换 base，必须换 generation。

### Rolling/delta 要求

Rolling/delta 不是“把当前 full blob 拆小”这么简单。实现前必须同时冻结：

- immutable base/delta identity、hash 和 engine lineage；
- bounded chain length、总 bytes、decode work 与 cancellation；
- concurrent publish、replacement、pin、retention 与 GC；
- Browser reference model、partial failure、fallback-to-full 与 exact tail；
- 不同 checkpoint 被吸收后 journal/tail 的安全回收条件。

### Exit criteria

1. R3 已通过；
2. 持续 50–100 ms output、从不 quiet 时，cut 仍有界前进；
3. snapshot refresh 不使 input/control SLO 回归，无 snapshot storm；
4. planner 决策可由 scalar telemetry 离线重放解释；
5. old tail 只有在已发布、可验证、仍被 pin 的新 checkpoint 吸收后才能回收；
6. 若 MVP 继续只用 full snapshot，必须用 staging 数据证明它满足批准 workload，而不是把 R4 标成完成。

## R5：Execution checkpoint 与 history pages

### 目标

R5 将“可执行 terminal state”和“完整 scrollback/history”拆成两个独立 owner：

```text
execution checkpoint + exact tail -> READY -> adopt
history pages                      -> background hydrate / abandon
```

- execution checkpoint 必须包含继续 parser、cursor、modes、screen 和后续 tail 所需的完整状态；
- history page 使用独立 schema、hash、ordering、dedupe、retention 与 cancellation；
- background history merge 不得改变 authority cursor、active screen 或 parser continuation；
- slow/failed history 可以永久 abandon，不影响 execution owner；
- copy/search/selection/ARIA 对尚未 hydrate history 必须有明确产品语义；
- 独立 benchmark 后才决定默认开启。

### 当前基础与缺口

WTerm/Ghostty passive restore 已能报告 `READY/HISTORY/FINISH`，逐页 decode/yield，并允许永久 abandon history；
`takeCore()` 保持一次性 ownership transfer。

产品层尚未使用这些能力：当前 control schema 只允许 restore through `FINISH`，`WTermReplicaHost` 等完整 history
完成后才返回 candidate/adopt。当前没有独立 execution/history object、page address、Cloud storage、dedupe、GC、
background hydrate、merge owner 或 benchmark。因此 R5 仍是 `not-started`。

R5 不阻塞当前 Recovery、R3、R4 或高 RTT input。

## 计划偏差记录

### 单实现切换

R2 原计划曾用 capability、strategy 和 kill switch 与旧 runtime 并存。pre-MVP 最终决策是删除兼容：

- 不协商 Recovery strategy；
- 不接受旧 wire、attachment 或 Durable Object schema；
- Host、Cloud、Browser 必须来自同一提交；
- 非当前 schema 破坏性重建；
- 不提供原地 downgrade 或 mixed generation。

这项偏差已经完成，不是待办。

### R1 serviceability owner 改变

Snapshot 从 attach-owned serviceability 请求改成 session-owned proactive baseline。发布 admission 不再证明某个未来
`(R,H]` gap 可用；gap serviceability 在 recovery prepare actor turn 中重新判断。这保留了 R1 ownership，改变了
原计划中的 admission policy。

### 周期 refresh 越过 R3

原计划要求先证明 immutable cut 的 actor hard bound，再启用滚动 freshness。当前实现先加入固定 30 秒 full
refresh，但 snapshot encode 仍同步不可抢占。这是已知技术债和 R3 风险，不是 R4 完成证据。

### R4 发布范围变化

正式阶段重排时 R4 位于完整 Recovery 完成序列中；单实现清理后的精简 roadmap 曾把 rolling/delta 全部移到
post-MVP，却没有明确取消 R4 技术目标。当前恢复后的规则是：R4 技术定义继续有效；MVP 可以在 staging 数据
证明 periodic full snapshot 满足批准 envelope 后显式暂缓 rolling/delta，但必须记录该决定。

## 与高 RTT 输入计划的关系

[输入稳定性计划](input-stability-plan.md)使用 I-A–I-E：

- R3 与 I-A 共享 snapshot actor-pause/SLO gate；
- ASCII Mirrored 不依赖 rolling/delta 或 history pages；
- prediction 只影响 presentation，绝不进入 snapshot、journal、tail 或 Recovery ledger；
- input validation/context sideband 不消耗 authority `eventSeq`；
- R5 与 shell-owned buffer 相互独立。

## 验证矩阵

### Correctness 与状态机

- 随机选择 `R/H/K`，比较 `snapshot@R + continuous log(R,K]` 与 uninterrupted authority；
- UTF-8、CSI/OSC/DCS continuation、primary/alternate、resize/reflow、sync output、scrollback；
- gap/live 各 lane 保序但跨 lane 任意交错，Done/SourceClosed/Adopted/receipt retry；
- same-ordinal exact retry、divergent duplicate、gap、错误 epoch/engine/generation/offset；
- warm/cold ownership、candidate failure、handoff uncertainty、dispose once；
- Host/Browser replacement、DO hibernation、queued/sending/sent crash cut 与 outbox replay。

### 生命周期、容量与多 client

- single-flight、16 recovery sources、supersede、relay reconnect、writer replacement；
- grant/receipt exhaustion、variable-size DRR、slow observer、writer reserve；
- snapshot encode/compress/upload/download failure、response loss、checksum、Cloudflare R2 404；
- retention/pin/GC、pending body、cursor-ahead、orphan cleanup；
- Browser restore timeout、assembler cap、history abort 和 candidate adoption。

### R3–R5 专项

- real WASM snapshot capture pause 与 Host actor input/output 并发；
- sustained output、never-quiet、hard freshness、planner replay 和 snapshot storm；
- full/delta chain corruption、partial publish、pin/GC race 与 fallback-to-full；
- execution READY adoption、history background hydrate/abandon、merge 和 cancellation。

### 证据等级

- `unit`：纯 owner、scalar、codec、reference reducer；
- `owner-integration`：真实相邻 owner，但 fake clock/socket/storage 可存在；
- `local-e2e`：本地 workerd/R2/WebSocket 和真实包组合；
- `staging`：真实 Cloudflare、网络、R2、Linux PTY 和 Browser；
- `production`：批准 workload、长期观测、故障演练和 release evidence。

测试数、CI 绿色和 timeout 值本身不提升证据等级。

## 指标

必须分段记录单机 monotonic duration，不能直接相减不同机器的 wall clock：

- Host：actor queue、snapshot freeze/copy/encode、PTY input/output、source scheduler；
- Cloud：control/data queue、SQL transaction、outbox、ring/scheduler、R2；
- Browser：download、restore READY/FINISH、gap apply、adopt、time-to-visible/current；
- Snapshot：age、tail bytes/frames、capture/publish/download/restore cost、pin/retention/GC；
- Recovery：start/done/adopt、receipt/apply lag、reset reason、resource high-water。

完整指标与全局发布 gate 只在 [MVP Roadmap](mvp-roadmap.md) 维护。

## 非目标

- 任意丢弃未被 adopted checkpoint 吸收的 terminal mutation；
- 把 input result、prediction 或 context metadata 写入 canonical journal/snapshot/tail；
- 用完整 snapshot 做逐键 clone；
- 无限 transcript、无限 history、无界 retry 或无 owner cleanup；
- daemon 崩溃后复活 Unix child/PTY；
- 跨 `engineId` 猜测恢复 snapshot；
- 以本地 workerd 测试宣称 production Cloudflare、性能或 SLO；
- 为历史实现恢复 runtime、wire、schema 或 fallback。

## Phase 完成定义

- R0：当前 owner 不再存在无 deadline、无 producer 或跨 generation 拼接；
- R1：checkpoint/body/range/waiter owner 与 serviceability 有界且 release-once；
- R2：并发 gap/live、三端 owner、credit/fairness/hibernate 和 atomic adoption 的本地 correctness 完成；
- R3：snapshot authority pause 有可执行 hard max，失败不终止 authority；
- R4：planner/refresh 在批准 workload 下有界前进、无 input 回归和 snapshot storm；
- R5：execution 与 history owner 分离，background history 不影响 execution correctness。

当前只有 R0–R2 达到各自完成定义。
