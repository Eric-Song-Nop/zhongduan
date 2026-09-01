# Zhongduan 输入核心实施计划

> 状态：E1–E4 active implementation contract
>
> 上位产品约束：[产品契约与协议边界](terminal-protocol-architecture.md)
>
> 阶段依赖与发布 gate：[MVP 路线](mvp-roadmap.md)
>
> CURRENT 实现事实：[MVP 架构](mvp-architecture.md)与 [Wire Protocol V2](wire-protocol.md)

本文只负责 input correctness、input latency 和 snapshot/background-work isolation，不设计 speculative
presentation。本次 A1 文档 PR 不修改 runtime 或 wire；后续 E1–E4 可以修改 input/hot-path 实现，但不能
改变 CURRENT v2 的 recovery behavior。文中的“必须”描述 TARGET 完成条件，不表示 PR #24 已具备相应能力。

## 执行边界

- E0 journey baseline 完成并冻结 workload、oracle 与相对阈值后，才能依次实施 E1、E2、E3、E4a。
- E4b 是 E4a 后的强制 decision；只有测量证明现有 snapshot cut 无法通过 gate 时才实施 immutable/COW cut。
- v2 在 destructive cutover 前仍是唯一生产 runtime，且 recovery behavior 保持冻结。E1–E4 不得顺手
  修改 v2 recovery pause/barrier/pin，也不得形成第二套 production recovery protocol。
- 每个阶段必须把 owner、状态转换、bytes/count/age/concurrency 上限、失败收敛和 evidence 一起提交；
  只增加 helper、telemetry 或测试数量不算完成。
- E0 之前不凭空设置绝对 latency SLO。后续 hard limit 与 relative threshold 必须写进 source control，
  并绑定明确 workload、环境、样本与 oracle。

本计划明确不做：prediction、input-region protocol、application driver、owned shell command buffer、
multi-writer、跨 Host crash 的 exactly-once application effect，以及 recovery replacement R1–R4。

## CURRENT gap

PR #24 的真实行为详见 [MVP 架构](mvp-architecture.md)。E1–E4 需要收口的直接差距是：

- Browser 某些路径在 schema validation 或 transport admission 前分配 sequence；无 sender/lease、mouse gate、
  queue overflow 和 coalescing 没有统一的逐 intent 可见结果。
- Browser 有 count-bounded queue 和 pending ACK map，但还没有完整 bytes/age contract；full/control
  replacement 只给出聚合 `uncertain`，不能逐 identity 解释结果。
- Cloud 的 Browser control、Host control 与 Host data 共用一个全 DO Promise tail；即使 bytes/count 有界，
  input queue wait 仍可能随无关 bulk backlog 增长。
- Host 以 high-water 和 4096-entry result cache 去重，但会接受大于 high-water 的 gap，并非 strict
  contiguous input stream。
- Snapshot encode 与 PTY output、resize、semantic input 共用 Host authority actor；现有 deadline 不能
  抢占正在执行的同步 encode。

这些事实是迁移输入，不是对 TARGET 的豁免。E1–E4 必须保持 CURRENT v2 的既有 correctness contract，
直到后续 destructive cutover。

## 共同状态与 identity

每个被 UI 消费的 semantic intent 先获得 Browser-local `LocalIntentId`，以便 UI、telemetry 和测试观察；
它不是 wire identity。只有完成 normalization、validation，并成功预留有界队列容量后，才能分配：

```text
InputIdentity {
  writerFence
  inputEpoch
  clientInputSeq
}
```

Browser 不能声明可信 `clientId`；Cloud 必须从已认证 connection 注入它。Host 实际去重/排序 key 因而是
`{writerFence, clientId, inputEpoch, clientInputSeq}`，而 Browser-visible identity 仍是上面的三元组。

一个 intent 的本地生命周期是：

```text
consumed(LocalIntentId)
  -> validation rejected ----------------------> not-sent
  -> admission rejected -----------------------> not-sent
  -> admitted(InputIdentity) -> queued -> send decision
       -> proven not accepted by transport ----> not-sent + seal epoch
       -> accepted / acceptance uncertain -----> sent
            -> retained no-effect result ------> deterministic(rejected)
            -> retained PTY-write result ------> deterministic(written|duplicate)
            -> outcome unresolved -------------> terminating/awaiting-tombstone
                 -> Host proof ----------------> deterministic(rejected)
                 -> deadline / no proof -------> uncertain
```

`queued`、`sent` 和 `terminating/awaiting-tombstone` 是有 age limit 的中间状态，不是额外的产品结果。
每个 intent 最终都必须且只能一次收敛到 `not-sent`、`deterministic` 或 `uncertain`；UI 和测试必须能按
`LocalIntentId` 观察该结果。终态不可被后续证据改写。

共同规则如下：

- `not-sent` 是终态，不是等待下次 transport 的 replay queue。若已分配 sequence 的 intent 最终为
  `not-sent`，当前 input epoch 必须 seal，后续 intent 使用新 epoch，不能让 Host 看见 sequence gap。
- 无法立即证明 transport 是否接受某个已分配 identity 时，必须 seal epoch 并进入有界
  `terminating/awaiting-tombstone`；Host proof 到达前不得重发。Deadline 到期或证明失败时一次性收敛为
  `uncertain`。
- `deterministic` 只在声明的 result-retention window 内可重复返回。结果被淘汰后不得重新施加旧 effect；
  无法证明时只能返回 `uncertain` 并结束 epoch。
- 每次 writer transfer 都用更高 fence 封住旧 writer。新 writer 创建新 input epoch，并从 sequence 1 开始。
- Epoch termination 必须为每个已分配 identity 收口：已有 result 的返回缓存结果；Host 能以 strict-prefix
  tombstone 证明未执行的返回 deterministic no-effect；其余一律 `uncertain`。不能只把 epoch 标成 closed，
  却让 Browser outstanding intent 永久停在 `queued`/`sent`。
- Resize、mouse move 等 coalescible state intent 在 admission 前可以被更新 intent 取代；被取代项只有
  `LocalIntentId`，不分配 wire identity，并收敛为 `not-sent/superseded`。一旦分配 identity，就不能再
  静默 coalesce。
- 重连后的 resize/state reconciliation 是新的 consumed intent；它在 validate/admit 后获得新 identity，
  不能复用或自动重放旧 intent。

## E1 — Browser validation and admission

### 目标 owner 与处理顺序

Browser `InputDispatcher` 是 UI consumption 到 transport handoff 的唯一 owner。一次 dispatch 必须按以下顺序：

1. 规范化 key/text/paste/resize/focus/mouse 等 semantic payload；
2. 完成 schema、size、policy、writer lease、replica/mouse gate 等同步 validation；
3. 为 encoded bytes、item count 和最大 residence age 原子预留本地 queue capacity；
4. 分配 `InputIdentity`，把 `LocalIntentId` 与 identity 一对一绑定；
5. 按当前 control transport FIFO 执行 send decision，并记录 transition timestamp；
6. 按 identity 处理 ACK，或在 replacement/failure 时逐项收敛。

validation 或 admission 失败不得推进 `clientInputSeq`。Queue reservation 与 identity allocation 必须位于同一
同步临界区，不能出现“sequence 已消耗、item 未入队”的半完成状态。

### Queue、replacement 与 coalescing

- Queue 必须同时声明 bytes、count 和 age hard limit；每项保存 encoded size、admitted-at、transport
  generation 与当前状态。只有 count limit 不足以防止大 paste 占满内存。
- Queue overflow 对当前 intent 返回 `not-sent/overload`。已分配 identity 后发生的本地取消必须 seal epoch；
  不能跳过该 sequence 继续发送后项。
- Data-only replacement 不改变 input transport 或 input epoch。Full/control replacement 必须逐 identity
  区分仍在 Browser queue、已调用 sender、已收到 ACK 三类：能证明未交 transport 的 queue item 收敛为
  `not-sent`；已调用 sender 的 identity 进入 `terminating/awaiting-tombstone`，由 Host proof 或 deadline
  一次性决定 `deterministic`/`uncertain`；已 ACK item 保留原终态。不能只更新一个 aggregate status。
- `latestResize`、mouse move 等只能在 admission 前 coalesce。Mouse gate 拒绝、失去 writer lease、replica
  非 current 等路径仍必须产生逐 intent 本地结果。
- 不允许在 attach/reconnect 时自动 flush 旧 `not-sent` intent；reconciliation 必须走一次新的 dispatch。

### E1 evidence 与完成条件

实现契约、hard limits、rollout matrix 与验证入口见
[E1 Browser input admission](./e1-browser-input-admission.md)。

- Property/sequence test 证明 malformed、oversize、policy rejection 和 queue overflow 都不分配 sequence；
  下一个合法 intent 不产生 gap。
- Fault test 覆盖 sender 缺失、send 前抛错、acceptance uncertainty、full/control replacement、data-only
  replacement、queue age expiry、tombstone proof 和 tombstone wait timeout；每个 `LocalIntentId` 只得到
  一个不可改写的终态结果。
- Coalescing test 证明 superseded intent 无 wire identity，admitted identity 不被静默替换，reconciliation
  获得新 identity。
- Source-controlled queue contract 固定 bytes/count/age limit、overload 行为和观测字段。
- E0 journey 中 UI-consumed silent loss 为 0，uncertain input 自动重发为 0。

## E2 — Cloud control hot path

### 执行 owner 与 ordering

Cloud 必须为 Browser control/input 建立不受无关 Host bulk/data backlog 线性影响的执行 lane。必须保留的
ordering 只有：同一 Browser control connection 的 frame FIFO、writer fence/lease transition 先于其后 input、
以及 ACK 与原 identity 对应；snapshot upload、R2、Host data broadcast 和 maintenance 不得借此获得 input
前置顺序。

如果实现继续共享某个串行 owner，必须由 E0 benchmark 证明 input queue wait 与无关 bulk backlog 解耦；
“全局 queue 有 count/bytes 上限”本身不是隔离证据。

### Connection-scoped writer capability

- Writer authority 绑定当前 Browser control connection、稳定 client identity 与单调 `writerFence`，不能只绑定
  可跨 replacement 继续使用的 session row。
- Heartbeat 只续租同一 live connection 的 capability；timeout、close、replacement 或更高 fence 会永久
  封住旧 connection。旧 socket 上延迟到达的 input 不得成功。
- Full/control replacement 必须先取得更高 `writerFence` 才能发送新 input；同一 fence 下的
  `BeginInputEpoch` 只允许仍然存活的同一 current control connection 使用。
- DO hibernation 后若无法证明 attachment 仍是同一 current connection，必须关闭/fence 后重新取得 writer，
  不能猜测恢复旧 authority。

### Fast path、overload 与 rejection

Input fast path 只做 bounded parse/copy、schema/policy、connection/fence validation、有界 enqueue 和 Host
control send。每个 key 不得执行 SQLite mutation、R2、snapshot hash/compress 或其他 background work。

Cloud queue 必须声明 per-connection 与 lane-level bytes/count/age/concurrency 上限，并记录 receive、queue-enter、
queue-leave、Host-send timestamp。Overload 默认只隔离产生负载的 connection/lane，不能关闭健康 Host
authority 或阻塞其他 Browser。

Cloud 在 identity 已分配后产生 no-effect rejection 时只有两条合法路径：

1. 把该 rejection 纳入与 Host `nextExpectedSeq` 一致、可重复返回的有序结果流；或
2. seal/fence 当前 input epoch，并让该 identity 进入有界 `terminating/awaiting-tombstone`；Host proof
   到达时一次性收敛为 deterministic no-effect，deadline/证明失败时一次性收敛为 `uncertain`。

E1–E4 不新增 durable Cloud per-key result ledger，因此默认采用第二条。若未来要直接返回
`deterministic(rejected)`，必须先定义 bounded result retention、retry 与 ACK-loss 语义。任何可能已经
Host-send 的路径也只能等待同一 Host proof；不能证明时必须返回 `uncertain` 并终止 epoch。

### E2 evidence 与完成条件

当前实现与可复跑证据见
[E2 Cloud input lane and connection-scoped writer](./e2-cloud-input-lane.md)。

- Writer replacement、heartbeat expiry、socket close、DO hibernation 和 delayed old-frame fault test 证明旧
  connection 成功 input 次数为 0。
- Output flood、snapshot maintenance 和 R2 workload 下，input lane 的 queue bytes/count/age 均不越界；
  overload 只隔离对应 source。
- Instrumentation 能分解 Browser receive → queue enter → queue leave → Host send，且不会在每个 key 上执行
  storage/hash。
- 在 E0 supported load 内，Cloud input queue wait / latency 不随无关 Host bulk/data backlog 线性增长；
  若共享 owner，benchmark 必须证明等价隔离。
- Output flood 下 Ctrl-C 仍准确 Host-send 一次，并满足 source-controlled bounded completion gate。

## E3 — Host contiguous input epoch

### Epoch state

Host 对当前 `{writerFence, clientId, inputEpoch}` 明确持有：

```text
InputEpochState {
  nextExpectedSeq
  boundedResultCache
  status: active | terminated
}

EpochTombstone {
  writerFence
  clientId
  inputEpoch
  firstProvenNoEffectSeq
  reason
  retentionEnd
}
```

高一级 `writerFence` 到达时，旧 state 永久 terminated；新 writer 必须以新 epoch 的 sequence 1 开始。同一
epoch 的处理规则是：

```text
seq < nextExpectedSeq
  -> cache hit: return the retained deterministic result
  -> cache miss: uncertain; terminate epoch; never reapply

seq == nextExpectedSeq
  -> validate and decide one terminal result
  -> cache deterministic result, then increment nextExpectedSeq
  -> uncertain result terminates epoch

seq > nextExpectedSeq
  -> missing input; do not consume or buffer the future seq
  -> terminate/fence the epoch; never skip the gap
```

同一 writer fence 下切换到新 `inputEpoch` 必须经过一个由 current control connection 发起、与 input FIFO
有序的逻辑 `BeginInputEpoch` transition。Host 先终止旧 epoch、提交 `EpochTombstone`，再 ACK 新 epoch；
Browser 只有收到该 ACK 后才能发送新 epoch 的 sequence 1。Host 必须在有界 retention 内记住已终止 epoch，
拒绝其延迟 frame。这里规定的是状态转换，不强制具体 wire frame；若实现无法证明 transition ordering，
就必须取得更高 writer fence，不能仅凭一个新字符串重置 Host state。

Result cache 必须同时声明 count/bytes/age retention，并记录 `written`、`rejected-no-effect` 等可重复结果；
淘汰不能授权重新执行旧 identity。

`firstProvenNoEffectSeq` 的值取决于终止边界：若 Host 在尝试 `nextExpectedSeq` 前因 gap、ordered epoch
transition 或 fence 终止，则从 `nextExpectedSeq` 起都可证明 no-effect；若该 sequence 的 write/commit
可能已经产生 effect 后抛错，则触发 sequence 保持 `uncertain`，只有更高 sequence 可证明 no-effect。
Browser 只能把大于等于 tombstone 边界、且仍有精确 identity 的 outstanding intent 收敛为 deterministic
no-effect；更小 sequence 必须使用 retained result，cache miss 则为 `uncertain`。若 fail-closed 导致
tombstone 本身未提交，所有缺少 retained result 的 outstanding identity 都按 `uncertain` 处理。Tombstone
过期后旧 frame 仍然不得执行，但无法重建原结果时也只能回答 `uncertain`。

### 哪些 rejection 消耗 sequence

- Browser validation/admission rejection 没有 `InputIdentity`，不消耗 sequence。
- 只有 identity 属于 current fence/client/epoch、且 `seq == nextExpectedSeq` 时，Host 能证明未施加 PTY effect
  的 semantic/size/policy rejection 才消耗 sequence；它作为 `deterministic(rejected-no-effect)` 缓存，
  然后推进 `nextExpectedSeq`。
- Gap/future sequence 不消耗 sequence；Host 提交 tombstone 后，触发 gap 的 identity 与同 epoch 更高
  outstanding identity 都是 deterministic no-effect，随后终止 epoch。
- Stale fence、错误 client/epoch 和已经 terminated 的 epoch 不消耗 current sequence。只有 retained
  result/tombstone 能证明原 outcome 时才返回 deterministic；否则返回 `uncertain`，且始终不得重新施加 effect。
- Cloud pre-Host rejection 只有在进入同一有序 result stream 时才能消耗 Host sequence；本阶段默认不建该
  ledger，因此按 E2 规则 seal epoch。

### `pty.write()` ACK 的精确语义

`written` 只表示：Host 已验证 expected identity，调用对应同步 PTY write/resize commit，且该调用正常返回；
它不表示 PTY slave 或 child 已读取 bytes，也不表示 shell/application transaction 已提交。

- 正常返回后，Host 缓存 `written` 并推进 `nextExpectedSeq`；dedupe window 内 duplicate 只返回缓存结果，
  不再次调用 PTY。
- Write/commit 抛错时 effect 可能已经发生，结果只能是 `uncertain`；Host 终止 input epoch，并按 authority
  failure policy fail closed。
- Host crash、connection loss 或 ACK loss 后若 result 不可证明，Browser 只能得到 `uncertain`；不得把
  application 没有明显变化当作 no-effect 证据。

### E3 evidence 与完成条件

- Model/property test 覆盖 first seq、strict prefix、duplicate、future gap、stale fence、cache eviction、
  deterministic rejection、write-before-throw 和 ACK loss。
- Tombstone boundary test 必须证明 gap 使 `nextExpectedSeq` 起为 no-effect，而 write-before-throw 的触发
  sequence 始终是 `uncertain`，只有其后的 sequence 可以由已提交 tombstone 证明 no-effect。
- dedupe retention 内 duplicate PTY effect 为 0；gap 后成功 input 为 0；writer transfer 后旧 writer 成功
  input 为 0。
- 每种 rejection 是否消耗 sequence 都有直接 state assertion；测试不能只检查某个 ACK string。
- `written` 的 API、wire 文档和 UI 文案都不得暗示 child read 或 application commit。
- Result cache 与 input actor queue 的 count/bytes/age limit、eviction 和 fail-closed 行为进入 source control。

## E4 — Snapshot/input isolation

### E4a：finalized background snapshots

Attach 只能选择已经完整上传、校验并原子发布 metadata 的 finalized immutable checkpoint；不得在 attach
hot path 内触发 snapshot build，也不得等待一个 pending upload 变成可用。没有 finalized checkpoint 时，
attach 使用仍受支持的 warm path，或明确失败/重试，不能采用 partial blob。

E4a 明确以下 owner：

- Snapshot coordinator 在 attach 之外按有界 schedule 决定 refresh，并负责 single-flight、retry、取消和
  minimum interval；
- Host authority actor 只负责在 canonical mutation 顺序中取得 cut identity，以及当前 Ghostty API 必需的
  同步 capture/encode；这段 pause 必须单独测量，完成后检查的 timeout 不能声称可以抢占它；
- Background publisher 负责 compress、hash、multipart upload、metadata validation 和 finalize；这些工作
  不得进入 input/control owner；
- Cloud/R2 只在 blob 与 metadata 全部验证后发布新 pointer；旧 finalized checkpoint 在 retention 规则允许时
  继续可用。

所有 refresh queue 必须有 bytes/count/age/concurrency 上限。Capture、compress、upload、finalize 任一步失败
只终止该 background attempt，清理 partial ownership，并保留先前 finalized checkpoint；不得阻塞 writer
input、改变 authority state 或让 attach 采用半成品。

### E4b：immutable/COW cut decision

E4a 测量后必须记录一个 source-controlled decision：

- 若同步 capture/encode 已满足 Host local input p99、Ctrl-C 和 authority pause gate，明确跳过 E4b；
- 若不能满足，E4b 才允许让 authority actor 在硬上限内创建以 `AuthorityRevision` 标记的
  immutable/COW engine cut，随后立即恢复 mutation；encode/compress/upload 全部由独立 background
  owner 完成。

若实施 E4b，必须先证明 immutable cut 覆盖 parser continuation、screen/history、modes、pending wrap 和其他
会影响后续解析的 state；定义 handle lifetime、引用计数/释放、内存 hard limit、并发 cut 上限，以及失败时
回到上一 finalized checkpoint 的路径。它仍是 snapshot implementation detail，不能成为新的 terminal truth
或 wire-format mandate。

### E4 evidence 与完成条件

- Fault test 覆盖 capture/encode/compress/upload/finalize 失败、取消、超限、stale completion 和 concurrent
  attach；未 finalized checkpoint 可见次数为 0。
- Snapshot/recovery 开启与关闭时，相同 canonical input/output 产生相同 Host authority revision；background
  attempt 失败不会改变 journal/head。
- 分别记录 authority cut/capture、encode、compress、upload、finalize 和 input actor queue wait，不能把
  总 timeout 当作 pause measurement。
- Snapshot 开启后的 Host local input p99 不超过 E0 baseline contract 的约定倍数，output flood 中 Ctrl-C
  仍有有界完成时间；所有 queue 和 temporary bytes 保持在声明上限内。
- 若实施 E4b，immutable cut + canonical suffix 必须通过产品契约定义的 normalized-state 与 continuation
  oracle；若跳过，E4a 通过证据和跳过理由必须进入 source control。

## 跨阶段交付要求

每个 E1–E4 PR 必须在正文中列出：

- 翻转的具体 gate、修改前失败状态、修改后通过状态；
- owner/state transition 与 cleanup responsibility；
- bytes/count/age/concurrency hard limit 及超限结果；
- fault-injection case、E0 workload、指标、样本和 source-controlled 报告入口；
- 对 CURRENT v2 的影响；正常情况应为“不改变 recovery pause/barrier/pin 或 recovery wire”。若 input
  correctness blocker 必须扩展 input control schema，还必须提交 capability、rollout、component-skew 和
  fallback 契约，且不得改变冻结的 recovery behavior；
- rollback 边界，以及 uncertain outcome 如何终止而不是自动重试。

E4b decision 完成前不能进入 R0；E1–E4 的绿色组件测试也不能替代 [MVP 路线](mvp-roadmap.md)要求的
端到端 correctness、latency 和真实 journey evidence。
