# Zhongduan 产品契约与协议边界

> 状态：规范性产品与架构决策框架
>
> 决策日期：2026-08-31
>
> 当前运行时基线：PR #24 head，`5d7511782542de511292d25908b8d92be4319636`
>
> 适用范围：Host authority、Browser replica、input effect、snapshot/recovery、Cloud relay、
> presentation 与后续协议演进

本文定义 Zhongduan 以后做产品和协议决策时必须同时满足的边界。它不设计新的 wire protocol，
也不改变 CURRENT protocol v2 的任何 runtime 行为。当前实现事实分别记录在
[MVP 架构](mvp-architecture.md)、[Wire Protocol V2](wire-protocol.md)和
[Phase 0 验收契约](phase-0-acceptance-contract.md)中。

## 阅读约定：GATE 与 CURRENT

除明确标为 **CURRENT** 的章节外，本文中的“必须”和“不能”都是后续变更与发布的 acceptance gate，
不是对 PR #24 已实现能力的声明。PR #24 已知尚未完全满足 input 分类/连续性、hot-path 隔离和
non-blocking recovery 等 gate；它在对应 cutover 前作为冻结的 grandfathered baseline 保留，不能被当作
新架构的合规先例。

规范性产品决策、roadmap 和新协议边界发生冲突时，以本文为准。CURRENT runtime 行为发生冲突时，
以基线 commit 的代码、[MVP 架构](mvp-architecture.md)和 [Wire Protocol V2](wire-protocol.md)为准，
并修正文档，不能用未来 gate 重新解释现状。[历史设计归档](archive/README.md)只保留研究上下文，
不是 roadmap、兼容性承诺或实施依据。

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

这是端到端产品分类，不是 CURRENT v2 wire status 的同义词。v2 的过渡映射是：

| v2 事实或 status                                     | 产品分类        | 限定                                                   |
| ---------------------------------------------------- | --------------- | ------------------------------------------------------ |
| Browser 证明尚未进入 transport                       | `not-sent`      | 当前尚无稳定 wire status，UI 必须能观察结果            |
| `written` / `duplicate`                              | `deterministic` | 仅在声明的 result-retention/dedupe window 内可重复返回 |
| owner 能证明无 PTY effect 的 `rejected`              | `deterministic` | rejection 必须绑定明确 identity 和 owner               |
| `uncertain`、owner 丢失或过期后无法证明的旧 identity | `uncertain`     | 终止 epoch，禁止自动重发                               |

超过 result-retention window 后不能重新施加旧 effect；无法继续证明原结果时只能转为 `uncertain`。
`not-sent` 也不是“稍后自动发送”的本地队列状态。

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
reconciliation 是新 intent，必须在 validate/admit 后分配新 identity，不能自动重放旧 input。CURRENT v2
的 latest-resize resend 和 mouse coalescing 是等待 E1 明确收口的 grandfathered 行为，不是此 gate 已满足的证据。

`pty.write()` 成功只表示 Host 的同步 write 调用没有失败，不表示 PTY slave 或 child 已经读取，
更不表示 application transaction 已提交。Zhongduan 不宣称 PTY input 能提供跨 Host crash 的
exactly-once application effect。

## 3. Hot-path liveness and bounded latency

> 在受支持负载内，snapshot、recovery、observer、R2、bulk output 和 Durable Object maintenance
> 不得无限期阻塞 writer input 或 live terminal output。

这项不变量要求：

- 所有 queue、buffer、waiter 和 background work 都有明确的 bytes、frames、age 或 concurrency 上限；
- overload 必须隔离到具体 client、generation 或 background task，不能默认阻塞整个 authority；
- control/input 不能与 bulk data 共用一个无界串行尾部；
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

CURRENT v2 会把 delivery generation、cursor、pin/commit 等 attachment state 序列化到 Cloudflare
WebSocket attachment，以便跨 Durable Object hibernation 继续。这是 R4 前冻结保留的 legacy 实现，
不构成未来 recovery attempt 必须跨 hibernation 延续的产品要求。

## CURRENT：冻结的 protocol v2 基线

PR #24 是当前唯一运行时基线：

```text
control WebSocket    attach / lease / input / ACK / resync
data WebSocket       PTY_OUTPUT / RESIZE_APPLIED / directed replay / barrier / commit
snapshot HTTP        immutable compressed Ghostty snapshot
```

它还存在计划一明确要由后续 PR 收口的事实：Browser 尚未统一做到 validate/admit 后再分配 sequence，
Host input 还不是 strict contiguous stream，某些无 transport/lease 路径没有逐 input 可见结果，Cloud
仍有跨 socket 的全局串行尾部，recovery 仍依赖 global pause。A1 只把这些事实列为 gate，不在本次
纯文档变更中修改它们。

CURRENT recovery 使用 fixed commit、barrier、pinned delivery 和 global canonical publisher pause：

```text
pause canonical publisher
  -> select immutable base R and fixed commit C
  -> barrier / pin delivery generation
  -> warm: send exact tail(base, C]
     cold: send snapshot@R + exact tail(R, C]
  -> ReplayCommit(C)
  -> resume canonical publisher
```

这些行为是 v2 整体 correctness contract。Global pause 是与新架构 liveness 边界冲突的已知历史债务，
不是以后 recovery 的设计先例；但在 v2 内局部删除它会先破坏 adopted-state safety，因此只能在完整
replacement 通过 gate 后由 R4 一次性移除。计划二完成 destructive cutover 前：

- v2 继续作为唯一运行时；
- 只接受 blocker、安全问题和验证 v2 既有行为的测试修复；
- 不继续扩展 checkpoint serviceability、recovery fairness 或 durable attempt continuity；
- 不从 v2 局部删除 pause、barrier 或 pinned commit；
- 不同时维护一个逐步渗入 v2 的第二套 recovery protocol。

本次 A1 文档变更不修改 source、runtime、wire schema、capability 或部署行为。

## 后续实现的证据基线

计划一只定义 gate，不在本 PR 实现测试 harness 或功能。后续首先建立 raw semantic PTY path 的真实
E2E baseline；在该 baseline 完成前不做 prediction，也不开始 recovery replacement。

### 网络与故障矩阵

```text
Browser <-> Cloud RTT: 20 / 100 / 300 / 600 ms
Cloud <-> Host RTT:    20 / 100 / 300 / 600 ms
jitter
disconnect
reconnect
output flood
cold attach
DO hibernation
Host relay replacement
```

### 正确性 gate

1. 被 UI 消费但结果为 silent loss 的 input：0。
2. dedupe window 内 duplicate PTY effect：0。
3. uncertain input 自动重发：0。
4. output flood 下 Ctrl-C 准确写入一次。
5. writer transfer 后旧 writer input 成功次数：0。
6. cold candidate 在验证完成前 visible 次数：0。
7. snapshot/recovery 开启与关闭时，Host authority state 仍由相同 canonical input/output 决定。
8. secure-input 场景启用 speculative presentation 的次数：0。

### 延迟 gate

E0 第一轮只产生 baseline，不凭空写绝对阈值。至少采集：

- Browser keydown 到 send decision；
- Cloud Browser receive 到 Host send；
- Host receive 到 `pty.write`；
- input 到 matching Browser render；
- PTY output 到 Browser useful render；
- Ctrl-C 到 `pty.write`；
- Ctrl-C 到应用 quiet 或 prompt 恢复。

基线完成后，把相对阈值写入 source-controlled benchmark contract。至少必须验证：

```text
Cloud input latency 不随 Host data queue depth 线性增长
snapshot/recovery 开启后的 Host local input p99
  不超过关闭时 baseline 的约定倍数
output flood 中 Ctrl-C 仍有有界完成时间
```

## 主线依赖顺序

三个计划按依赖推进，不并行铺开：

```text
计划一：重新定义项目边界
        |
        v
计划三前半：建立 E2E baseline 并修复输入热路径
        |
        v
计划二：在 baseline 和测试约束下替换 v2 recovery
        |
        v
计划三后半：真实 Cloudflare / TUI / 长会话发布验证
```

推荐 PR 顺序：

```text
A1  docs: reset product and protocol boundaries

E0  test: terminal journey baseline
E1  browser validate/admit
E2  Cloud input lanes and connection-scoped writer
E3  Host contiguous input
E4a finalized background snapshots
E4b immutable Ghostty cut (only if measurements prove it necessary)

R0  freeze v2 behavioral contract (codify the A1 policy in executable tests)
R1  Host ordered generation stream
R2  Browser stream runtime
R3  ephemeral Cloud relay
R4  destructive cutover and delete v2

E5  real applications and Cloudflare staging release gate
```

计划二不能在 E0 之前开始。A1 先冻结允许的政策边界，R0 再把 v2 既有行为固化成 replacement 的
executable contract。v2 在 R4 前保持唯一运行时；替换必须由完整 gate 约束并在切换后删除旧路径，
不能长期维护 v2 与新 recovery 的兼容矩阵。

## 当前明确不做

在 E5 完成前，不进入本轮计划：

- Mirrored prediction；
- Owned shell command buffer；
- Codex/Claude application driver；
- input-region protocol；
- rolling/delta snapshot；
- history pages；
- recovery fairness 调参；
- multi-writer；
- Recovery v3 compatibility；
- terminal state delta 网络格式；
- Mosh 式 UDP roaming。

这些主题可以在以后重新提案，但不能从归档文档直接恢复为 roadmap。

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

没有这些证据时，默认保留 raw PTY path 和当时的 CURRENT recovery runtime；R4 前保持 v2 冻结，
并选择能够丢弃的临时过程，而不是新增 durable recovery state。

Zhongduan 的核心边界最终表述为：

> **精确采用 terminal state，明确处理 input effect，让健康路径始终快速，并允许任何 recovery
> attempt 被安全丢弃。**
