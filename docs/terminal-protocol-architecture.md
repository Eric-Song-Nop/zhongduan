# Zhongduan 产品契约与协议边界

> 状态：稳定的规范性产品与架构决策框架
>
> 决策日期：2026-08-31
>
> 适用范围：Host authority、Browser replica、input effect、snapshot/recovery、Cloud relay、
> presentation 与后续协议演进

本文只定义 Zhongduan 做产品和协议决策时必须同时满足的稳定边界，不承担 CURRENT runtime inventory、
阶段测试矩阵或 PR 排期。当前实现事实记录在 [MVP 架构](mvp-architecture.md)和
[Wire Protocol V2](wire-protocol.md)中；active 阶段、证据和依赖记录在
[MVP 路线](mvp-roadmap.md)中；输入热路径的实施契约记录在[输入核心实施计划](input-core-plan.md)中。

## 文档职责与阅读约定

本文中的“必须”和“不能”都是后续变更与发布的 acceptance gate，不是对某个实现 revision
已经具备相应能力的声明。Roadmap 可以调整阶段划分和顺序，但不能弱化这里的产品不变量；CURRENT
实现只能作为事实基线，不能反向定义未来架构的合规标准。

规范性产品决策、roadmap 和新协议边界发生冲突时，以本文为准。CURRENT runtime 文档与代码发生冲突时，
以对应基线 commit 的代码为准并修正 CURRENT 文档，不能用未来 gate 重新解释现状。

## 产品判断

Zhongduan 的目标是提供一个状态正确、输入结果明确、健康路径持续可用的远程终端，而不是精确持久化
每一次三方 recovery attempt。系统必须同时守住三项地位相同的产品级不变量：

1. **Adopted-state safety**：Browser 不能采用错误的 terminal-engine state。
2. **Input/effect safety**：每个被 UI 消费的 semantic input 都有明确、不会被误重试的结果分类。
3. **Hot-path liveness and bounded latency**：恢复和后台工作不能无限期阻塞 writer input 或 live output。

三项不变量没有主次关系。只证明状态安全、但长期拒绝恢复或阻塞输入的实现不合格；只追求低延迟、
但可能采用错误状态或重复输入 effect 的实现也不合格。任何权衡都必须记录影响、边界和验证证据，
不能把其中一项静默降级成“后续优化”。

## 1. Adopted-state safety

> Browser 已采用的 terminal-engine state，必须与 Host authority 在某个 committed revision 的状态
> 观察等价。

### Authority revision 与 committed

最小逻辑 revision identity 是：

```text
AuthorityRevision {
  engineId
  sessionEpoch
  eventSeq
  nextPtyOffset
}
```

- `engineId` 绑定 terminal-engine 实现、snapshot schema 和必要 codec；
- `sessionEpoch` 绑定同一个 PTY/child-process 生命周期；
- `eventSeq` 是该 revision 已包含的最高连续 canonical terminal mutation；
- `nextPtyOffset` 是已消费 PTY byte prefix 的 exclusive end。Resize 等非 PTY-byte mutation 消耗
  `eventSeq`，但不推进该 offset。

这是 correctness 所需的最小逻辑 identity，不要求未来 wire 永久使用同一字段布局。实现可以增加
lineage、checksum 或 generation fence，但不能让同一个 revision identity 指向两份不同 authority state。

`committed` 表示 Host authority actor 已完成该 canonical mutation 对 terminal-engine state 的 transition，
并把对应连续 identity 提交到 authority journal/head。WebSocket send、Cloud receive、delivery ACK、Browser
receive 或 Browser apply 都不能创建 committed authority revision。若 engine transition 已发生、但
journal/head commit 失败，authority actor 必须 fail closed；该未提交状态不能被发布为可采用 revision。

### 观察等价与 correctness oracle

“观察等价”不是 DOM cells 当前看起来相同，也不要求某个 snapshot serialization 的 bytes 完全相同。
同一 `engineId` 和 `sessionEpoch` 下的两个 engine state，只有在以下条件同时成立时才观察等价：

- screen/history 内容、cursor 与 attributes、terminal modes、pending wrap、margins、tab stops、palette、
  parser continuation 以及其他会影响后续解析或可见行为的规范化状态等价；
- 从两者继续应用同一合法 canonical suffix 时，产生的 terminal state、query response 和后续行为不会分叉；
- presentation-only overlay、DOM layout、缓存布局和 serialization 非语义差异不参与判断。

实现必须提供不依赖特定 wire encoding 的对照 oracle：

```text
uninterrupted authority engine at H

vs.

restore checkpoint at R
  + apply canonical suffix (R, H]
```

先比较两边的 normalized engine state，再继续输入覆盖 UTF-8、CSI、OSC/DCS、resize、pending-wrap 和
mode-sensitive continuation 的共同 fixtures，验证状态与行为仍然等价。测试可以使用有限且不断扩充的
fixture/property corpus，但不能把 DOM equality 或 snapshot byte equality 当作长期正确性定义。

默认正确性参考模型是：

```text
StateAt(H)
  = Apply(
      ImmutableCheckpointAt(R),
      CanonicalSuffix(R, H]
    )
```

其中：

- checkpoint 的 `sessionEpoch`、engine identity、schema 和 checksum 必须匹配；
- checkpoint 必须包含从其 cut 之后继续解析所需的 terminal-engine state，包括 parser continuation；
- suffix 必须来自同一 authority lineage，并且无 gap、无重复、顺序一致；
- PTY byte offset、resize 顺序和其他 canonical mutation identity 必须连续；
- cold restore 必须在 detached candidate 中完成，验证通过后才能原子 adopt；
- cold/detached recovery 失败时必须丢弃 detached candidate，不能污染仍在显示的 active replica；
- prediction 或其他 presentation state 永远不能进入 adopted core、journal、snapshot 或 canonical suffix。

`checkpoint + suffix` 是 correctness reference model，不是对物理网络格式的永久规定。未来可以传输
raw PTY mutation、active-screen checkpoint、state delta 或 coalesced synchronized frame；前提是实现
能够证明最终采用的 state 与某个 Host committed revision 观察等价，并通过相同的连续性 oracle。

这里的 state 是 **terminal emulator engine state**，不是 application execution state。它不保存 shell、
Vim、Codex 或其他 child process 的内存，也不能证明 child 是否读取、执行或提交了某个 input。

## 2. Input/effect safety

> 每个被 Browser UI 消费的 semantic input，必须明确落入 `not-sent`、`deterministic` 或
> `uncertain` 三类之一。

```text
not-sent
  已确定 input 没有离开 Browser。
  它不得在恢复、重连或 writer transfer 后自动发送。

deterministic
  Host 已产生可重复返回且不会重复施加 PTY effect 的结果，
  或更早的 owner 已确定拒绝且证明没有施加 effect。

uncertain
  input 可能已跨过某一边界，但无法证明 effect 是否发生。
  当前 input epoch 必须终止，该 input 永不自动重发。
```

这是端到端产品分类，不是任何一版 wire status 的同义词。每版协议都必须给出到这三类结果的完整映射，
并声明 result-retention/dedupe window。超过该 window 后不能重新施加旧 effect；无法继续证明原结果时
只能转为 `uncertain`。`not-sent` 也不是“稍后自动发送”的本地队列状态。

这项不变量要求：

- Browser 必须先完成 schema/semantic validation，并成功 admit 到有界发送队列，再分配 input sequence；
- 同一 `{writerFence,inputEpoch}` 内只允许严格连续的 input prefix；
- duplicate 只能返回已记录的确定结果，不能重复写 PTY；
- writer transfer 后，旧 fence 不能继续产生成功 input；
- UI 已 `preventDefault` 或表示已消费的 input 不能静默消失；
- transport 或 owner 结果不确定时必须显式进入 `uncertain`，不能靠猜测重试；
- secure-input 场景不启用 speculative presentation。

每个被 UI 消费的 resize、mouse move 等可合并 state intent 仍需要本地可观察分类。被较新 intent 取代且
尚未 admit 的 intent 不分配 wire identity/sequence，并归为 `not-sent/superseded`；重连后的 state
reconciliation 是新 intent，必须在 validate/admit 后分配新 identity，不能自动重放旧 input。

`pty.write()` 成功只表示 Host 的同步 write 调用没有失败，不表示 PTY slave 或 child 已经读取，
更不表示 application transaction 已提交。Zhongduan 不宣称 PTY input 能提供跨 Host crash 的
exactly-once application effect。

阶段拆分、CURRENT gap 和每层 evidence 见[输入核心实施计划](input-core-plan.md)。

## 3. Hot-path liveness and bounded latency

> 在受支持负载内，snapshot、recovery、observer、R2、bulk output 和 Durable Object maintenance
> 不得无限期阻塞 writer input 或 live terminal output，也不得让 input queue wait 随无关 bulk backlog
> 线性增长。

这项不变量要求：

- 所有 queue、buffer、waiter 和 background work 都有明确的 bytes、frames、age 或 concurrency 上限；
- overload 必须隔离到具体 client、generation 或 background task，不能默认阻塞整个 authority；
- 在 supported load 内，control/input 不得与无关 bulk work 共用会让其 queue wait 随 bulk backlog
  线性增长的串行执行 owner；若继续共享，必须用 source-controlled benchmark 证明等价隔离；
- snapshot 不能成为 input actor 上任意长的同步任务；
- recovery 可以失败、被丢弃并重新开始，新架构不能靠阻塞 authority 获得正确性；
- performance 和 liveness 是 MVP gate，不能等功能完成后再补；
- timeout 只有在其 workload、环境和 oracle 明确时才能作为 latency 证据；防挂死 deadline 不是 SLO。

## 后续架构必须分离的四类对象

下表是后续架构的 owner/lifetime 约束，不是 CURRENT v2 的实现清单。“不要求持久化”表示产品契约不要求
跨所列生命周期恢复同一对象，不表示进程内可以提前丢弃仍由当前 attempt 使用的 state。

| 对象                | 含义与 owner                              | 必须保留的逻辑生命周期    | 跨生命周期 durability 要求 |
| ------------------- | ----------------------------------------- | ------------------------- | -------------------------- |
| Authority state     | Host Ghostty + PTY 的唯一权威状态         | Host session 生命周期     | 不承诺 Host crash 后恢复   |
| Adopted replica     | Browser 当前正式采用的 terminal state     | Browser replica 生命周期  | 不要求                     |
| Presentation branch | prediction、overlay、stale 标记等展示状态 | 当前 presentation attempt | 不要求                     |
| Recovery attempt    | 某次 snapshot/replay/transport 过程       | 当前 recovery generation  | 不要求跨 hibernation 延续  |

由此得到以下架构规则：

- recovery attempt 不是 terminal truth，也不是 application transaction；
- transport outcome 不确定时，可以 fence/结束 generation、丢弃 candidate 并重新 attach；
- 重试可以从仍然正确的 active cursor 或更新 checkpoint 开始，不必继续同一 ordinal 或 send cut；
- cold detached candidate 在 adopt 前不能写入 active replica；
- warm recovery 只能向 active replica 原地 apply 已验证的连续 suffix，失败时不能把 cursor 推进到实际
  apply prefix 之外，也不能把 non-contiguous state 宣称为已采用；
- 新增长期 durable owner、ledger 或 convergence state 前，必须证明它改善已测得的产品指标，
  而不是仅让中间过程更容易被形式化描述。

## CURRENT v2 冻结原则

CURRENT runtime 的结构、限制和已知缺陷只由 [MVP 架构](mvp-architecture.md)与
[Wire Protocol V2](wire-protocol.md)记录。Active cutover 阶段与证据见 [MVP 路线](mvp-roadmap.md)。

在 roadmap 的 destructive cutover 完成前，v2 保持唯一生产运行时；它的 recovery correctness invariant
与状态机作为一个整体冻结，recovery 路径只接受 blocker、安全问题和验证既有行为的测试修复。输入与
hot-path 阶段可以在不改变 pause/barrier/pin 等 recovery 行为的前提下收口本身的 gate，但不能借机局部
删除或扩展 v2 recovery，也不能并行维护第二套长期生产协议。Replacement 必须作为完整路径通过本文
三个产品 gate 后再切换，而不是逐步渗入 v2 recovery。

## 新提案的决策门槛

每个新 protocol、owner、state machine 或持久化字段在进入实现前，必须回答：

```text
它改善哪个已测得的用户指标？
raw PTY baseline 哪里不够？
失败时能否安全关闭或丢弃？
是否值得增加新的长期 owner/state machine？
```

并且必须同时写明：

- 受影响的三个产品级不变量及其 oracle；
- CURRENT owner、拟新增 owner、生命周期和清理责任；
- queue/bytes/frames/age/concurrency 的硬上限；
- reconnect、replacement、hibernation 和 outcome uncertainty 时的收敛路径；
- capability、rollout、rollback 和 destructive cleanup 边界；
- 能证明收益的 workload、指标和 source-controlled gate。

没有这些证据时，默认保留 raw PTY path 和当时的 CURRENT recovery runtime；destructive cutover
完成前保持 CURRENT baseline 冻结，并选择能够丢弃的临时过程，而不是新增 durable recovery state。

Zhongduan 的核心边界最终表述为：

> **精确采用 terminal state，明确处理 input effect，让健康路径始终快速，并允许任何 recovery
> attempt 被安全丢弃。**
