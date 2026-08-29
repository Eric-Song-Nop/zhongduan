# 终端协议架构与不变量

> 状态：规范性架构边界；同时记录 CURRENT protocol v2 和 TARGET 演进方向
>
> 决策日期：2026-08-28
>
> 适用范围：Host authority、input、snapshot/journal、Cloud delivery、Browser replica 与 prediction

本文定义所有实施计划都必须服从的正确性边界。标为 **CURRENT** 的内容已经存在于 protocol v2；
标为 **TARGET** 的内容需要代码、wire capability 和灰度验证，不能当作当前能力。性能优化可以改变
checkpoint 频率、传输调度和 presentation，但不能放宽本文件中的不变量。

## 最高级不变量

> 客户端采用的 execution terminal state，始终等于一个 immutable checkpoint 加同一
> authority mutation log 的连续前缀；所有 prediction 仅是其上的 disposable
> presentation branch。

形式化地，对同一 `engineId` 和 `sessionEpoch`：

```text
AdoptedExecutionState(C)
  = Restore(ImmutableCheckpoint@R)
  + Apply(AuthorityMutationLog(R, C])

DisplayedState
  = Compose(AdoptedExecutionState(C), DisposablePresentationBranch?)
```

其中 `(R,C]` 必须是同一 authority lineage 上无 gap、无乱序的连续前缀。重复 mutation 只有在
kind、cursor、payload 等字段完全相同时才能去重；重复 identity 内容不一致必须 fail closed。首版可以
逐字段比较，不要求 mutation hash chain。采用 candidate 必须是一次原子操作，不能暴露恢复一半的 core。

这个不变量带来以下强制规则：

- Host session actor、真实 PTY 和 authoritative Ghostty core 构成唯一 effect path；Browser 永不生成
  PTY query response 或重放 terminal effect。
- `eventSeq` 只排序重建 authority 所需的 terminal mutation。当前是 `PTY_OUTPUT` 和
  `RESIZE_APPLIED`；未来只有确实改变可恢复 execution state 的事件才能加入。
- Input receipt、validation cut、writer state、context token、delivery ACK 和 telemetry 都不消耗
  `eventSeq`，也不进入 terminal journal、snapshot 或 replay tail。
- Snapshot 只能通过选择更新的 immutable cut 吸收旧 mutation。不能在旧 snapshot 后任意跳过
  中间 mutation，也不能把 transport batching 误写成 presentation-frame 丢弃。
- Prediction 只影响 renderer 合成；清除 prediction 前后，权威 replica 的 cursor、snapshot 和 hash
  必须完全不变。Prediction 不进入 selection、copy、search、ARIA、日志、网络或恢复状态。
- 任一 cursor、engine、epoch、generation、offset、duplicate 或 payload 校验失败，都必须丢弃 candidate、
  reset/rebase 或断开；不能猜测修复后继续 adopt。
- 所有 queue、reorder buffer、snapshot、tail、waiter 和 prediction branch 都必须按风险维度设置硬上限，
  至少覆盖适用的 bytes/frames、attempt age/deadline 和 session concurrency。无法在 envelope 内收敛时，
  必须在最小可证明安全的 scope 内 fail/reset，不能无限缓存。

这里的 execution state 是继续正确解析和显示未来 mutation 所需的 terminal state。无限 transcript、
child process state 和非恢复型瞬时 effect 不因此变成产品承诺。若未来把 history 拆成可延迟 hydrate 的
页面，execution candidate 的 adopt 仍必须满足它自身声明的 immutable checkpoint 与连续 mutation 前缀。

## CURRENT：protocol v2

当前实现使用：

```text
control WebSocket    attach / lease / input / ACK / resync
data WebSocket       PTY_OUTPUT / RESIZE_APPLIED / directed replay / barrier / commit
snapshot HTTP        immutable compressed Ghostty snapshot
```

Host 用一个严格 actor 顺序更新 PTY authority、Ghostty core、`eventSeq` 和 `nextPtyOffset`。Browser
健康时直接应用 canonical mutation；发生缺口时恢复 live core 的短 tail，或在 detached core 中恢复
snapshot 后应用 tail，再原子 adopt。

CURRENT recovery 的线性交接依赖：

```text
pause canonical publisher
  -> select immutable base R and fixed commit C
  -> barrier / pin delivery generation
  -> send snapshot + exact tail(R,C]
  -> ReplayCommit(C)
  -> resume canonical publisher
```

在 v2 中，global pause 是 correctness invariant。Cloud 在 pinned delivery 存在时不能推进 canonical
head；Browser 可以在收到匹配的 ReplayCommit 后、candidate 尚未 adopt 时有界缓存后续 live frame，但不能
让 `C+1` 在 pinned commit 之前越过 directed tail，也没有把 recovery/live 任意交错重排的 assembler。只删除
Host pause 会先触发 Cloud pinned check，或在错误实现中造成 commit 前缺口/错拼。TARGET Recovery v3 完整
上线前，任何 v2 session epoch 都必须保留 pause、barrier 和 pinned commit 的既有语义。

CURRENT 还具有以下边界：

- control 与 data 是两条物理 WebSocket，但 Cloud 调度仍可能产生跨 lane head-of-line blocking；
- delivery credit 与 terminal apply progress 尚未完全分离；
- input sequence 当前不是 TARGET 定义的严格连续 stream；
- 没有 `InputValidationCut`、visible `Mirrored` prediction 或可靠 generic TUI context；
- 没有可用于 v2/v3 混合部署的完整 capability negotiation。

因此本文件后续的 TARGET 消息和状态机不能通过单个 feature flag 在 v2 上局部开启。

## TARGET：三个逻辑平面

目标协议把职责拆成三个由 causal cut 关联、但不共享 sequence 的逻辑平面：

| 逻辑平面                       | 保存与排序的内容                                               | 明确不负责                                           |
| ------------------------------ | -------------------------------------------------------------- | ---------------------------------------------------- |
| Terminal authority state plane | Immutable checkpoint、canonical mutation log、authority cursor | Input validation、writer/context 和 transport credit |
| Interaction sideband           | Ordered semantic input、validation cut、可选 context assertion | 重建 terminal state 或广播 writer 输入节奏           |
| Delivery plane                 | 每客户端 generation、lane ordinal、credit、重排和 reset        | 决定 terminal mutation 的语义或 replica 已 apply     |

“三个逻辑平面”不等于“必须使用三条物理 WebSocket”。首版可以把不同 lane 多路复用到现有连接，只要
每条 lane 的顺序、credit、容量和跨平面的 causal cut 是显式的。只有测量证明 TCP/WebSocket HOL 仍然影响
输入或 live output 时，才把 control/input、live-data、recovery-data 映射到独立 WebSocket 或 QUIC stream。
Snapshot blob 继续适合使用可取消、有背压的 HTTP stream。

### Cursor 职责

四种 cursor 是需要隔离的语义时钟，不要求一次 wire migration 同时暴露四个最终类型：

```text
AuthorityCursor {
  sessionEpoch,
  eventSeq,       // 已连续 apply 的最后一个 authority mutation
  nextPtyOffset   // 下一段 PTY output 必须开始的位置
}

InputCursor {
  writerFence,
  inputEpoch,
  nextExpectedSeq // Host 要求的下一个 semantic input sequence
}

ContextCursor {   // 仅显式 opt-in integration 存在时有效
  contextGeneration,
  contextToken
}

DeliveryCursor {
  deliveryGeneration,
  lane,
  deliveryOrdinal,
  cumulativeEncodedBytes // 该 lane 已安全进入 Browser 有界 buffer 的累计 encoded bytes
}
```

- `AuthorityCursor` 证明 replica 对同一 authority log 连续 apply 到哪里。Snapshot manifest、tail、
  prediction validation cut 和 `ReplicaApplied` 都可以引用它，但不能由 delivery ACK 推断。
- `InputCursor` 只描述同一 writer fence/epoch 的有序输入。Host 只推进连续、已有确定结果的前缀；
  高 seq 不能越过缺口并提升 high-water。Transport 结果变成 `uncertain` 时终止 input epoch，禁止自动重发。
- `ContextCursor` 是 prompt、input region、modal 或 geometry 的可选 optimistic-concurrency assertion。
  它只能来自明确 integration/Host assertion；没有 opt-in context 时不构造 token，也不能通过屏幕 heuristic
  声称 Enter、approval 或 Owned buffer 是 context-safe。
- `DeliveryCursor` 只释放 transport credit，表示 frame 已被 Browser 的有界 assembler 接收；它不证明
  Ghostty 已 apply，更不证明 candidate 已 adopt。Terminal apply progress 仍用 `AuthorityCursor` 报告。

Recovery v3 必须先把 Authority progress 与 Delivery receive progress 分开；ordered input 需要明确
`InputCursor`。`ContextCursor` 可以长期保持可选。稳定 hash chain、最终字段编码和物理 lane 数量都不是
首版 correctness 前置条件。

## Terminal authority state plane

Authority plane 只保存采用 execution state 所必需的事实：

```text
ImmutableCheckpoint@R
AuthorityMutation(R+1)
AuthorityMutation(R+2)
...
AuthorityMutation(C)
```

Mutation 必须具有严格 `eventSeq` 和连续 `ptyOffset` 规则。Resize 即使没有 PTY bytes 也改变 terminal
execution state，因此消耗 `eventSeq`；input receipt 或 validation metadata 即使与输入有关，也不改变
可恢复 terminal state，因此不能进入该序列。

Checkpoint 有三个不同属性：

- `valid`：blob、checksum、engine、epoch 和 cut 自洽；
- `usable`：从 cut 到目标 head 的 mutation 仍可在 age、bytes 和 frames 上连续补齐；
- `fresh`：策略上值得优先使用，或已经值得异步刷新。

TTL 只能影响 freshness/refresh，不能让 idle 且仍可服务的 checkpoint 自动失效。Pending snapshot 每次
retry/install 前都必须重新检查 tail serviceability；旧 cut 已不可补齐时允许 abandon/supersede，不能让
幂等 retry 永久阻止新 cut。

Rolling capture 只有在取得 immutable cut 的 actor pause 已满足 input SLO 后才能启用 hard deadline；
同步 WASM timeout 不是可抢占保护。具体 capture、retry 和 freshness policy 由 Snapshot 实施计划定义。

## Interaction sideband

### Ordered semantic input

TARGET 的同一 `{writerFence,inputEpoch}` 是严格连续的 semantic input stream：

```text
seq == nextExpectedSeq  -> 产生确定结果并推进
seq <  nextExpectedSeq  -> duplicate/result lookup，不重复 effect
seq >  nextExpectedSeq  -> missing-input，不推进并终止该 input epoch
transport uncertain     -> 终止整个 input epoch，不自动 resend
```

Browser 先完成 schema/semantic 校验并成功 admission 到有界发送队列，再分配 sequence。Urgent input 可以
越过 snapshot、recovery、bulk output 和无关数据库工作，但不能越过已经分配的 earlier semantic input。
若 Ctrl-C 要取消尚未发送的本地 edit，必须先把它们明确标为 `not-sent/cancelled`，再为 Ctrl-C 分配顺序；
不能让 lane 竞态决定 input 语义。

`Raw` 只表示“不预测”，不表示“输入安全”。Replica catching-up 时应由独立 policy allowlist interrupt，
并默认阻止 Enter、Tab、Esc、paste、mouse 等危险输入；被阻止的输入不缓存，也不在 recovery 后自动重放。

### `InputValidationCut`

Prediction 的验证资格使用 writer-only、live-only 的 causal certificate：

```text
InputValidationCut {
  writerFence,
  inputEpoch,
  settledThroughContiguous,
  authorityCut: AuthorityCursor,
  synchronizedOutputGeneration?
}
```

Host 在 session actor 已按连续输入顺序产生确定结果，并经过最小 settle window 或 synchronized-output
边界后，原子采样 `authorityCut`。消息只发给当前 writer，可以 piggyback，也可以走有明确 lane ordering
的 sideband。

它具有以下硬边界：

- 不消耗 `eventSeq`，不写 journal、snapshot/tail、Cloud canonical head 或 observer fanout；
- 丢失、乱序、reconnect 或 generation change 只让 prediction timeout/reset，不触发 replay；
- Browser 必须等 `AuthoritativeReplica.cursor >= authorityCut` 且完整 synchronized frame 结束后才能验证；
- 它只证明“到这个 authority cut 已有资格观察预测”，不证明 child 已消费输入，也不是 app-effect ACK；
- `settledThroughContiguous` 只能覆盖无 gap、已有确定 Host 处理结果的输入前缀。

因此 sideband 可以因果地引用 authority plane，但不能混入 authority mutation log。

### Context 是 opt-in 能力

Generic PTY 画面不能证明 Codex/Claude 当前是 composer 还是 approval，也不能证明 shell/readline buffer 的
ownership。TARGET 可以为支持的 shell 或 TUI 定义 input-region/context integration，由 Host 签发绑定
region revision、mode/geometry 和 authority cut 的 token。未协商该能力时：

- `Mirrored` 只能在保守 eligibility 和隐藏学习下预测 presentation；
- `Owned` 不得仅凭 OSC 133 或“看起来像 prompt”接管 command buffer；
- commit/approval 不能被宣传为 guarded-context safe。

## Delivery plane 与 Recovery v3

TARGET Recovery v3 用 concurrent gap-fill 取代 v2 的 global pause，但不改变最高级不变量。

### Start fence、committed-through 与 live floor

一次 generation 的起点固定为：

```text
RecoveryStart {
  recoveryId,
  deliveryGeneration,
  engineId,
  base: AuthorityCursor R,
  source: { kind: warm } | { kind: snapshot, complete immutable manifest },
  committedThrough: AuthorityCursor H,
  liveFloor: MutationBoundary {
    sessionEpoch: H.sessionEpoch,
    nextEventSeq: H.eventSeq + 1,
    nextPtyOffset: H.nextPtyOffset
  }
}
```

选择 base 之后，Host 必须在同一个有序 canonical publisher 中原子取得严格有界、可保留的
`PreparedGap(R,H]`，并在 mutation `H` 之后、任何 `H+1` 之前插入不消耗 `eventSeq` 的
`RecoveryStartFence`。Cloud 处理这个 fence 时必须先确认自己的 committed head 恰好是 `H`，再与 canonical
ingress 原子地安装从 `H+1` 开始的 live-delivery obligation、固定 start metadata，之后才允许处理下一条
Host data message 并向 Browser 发送 `RecoveryStart`。若未来把 live/recovery 拆到不同物理 stream，也必须
提供等价的 cross-stream fence，不能依赖到达时序猜测。

因此在 attach 与 start 之间到达的 mutation 要么包含在 `PreparedGap(R,H]`，要么属于从 `liveFloor`
开始的 live lane，不存在无人负责的窗口。准备失败、Cloud head 不等于 H、generation 已失效或 source 无法
保留到发送完成，都只能 reset/replan，不能发送截断 gap-fill。

`liveFloor` 是该 generation 中 live lane 必须开始覆盖的第一条 mutation boundary，通常是 `H+1`。它不是
一个尚未存在的 authority cursor，也不是固定全局 head；authority 可以立即继续提交
`H+1,H+2,...`。Cloud 不必长期保存第二份 per-client payload holdback，
但必须保证这些 mutation 经有界 delivery lane 到达 Browser assembler；承诺无法履行时只能 reset generation。

`{deliveryGeneration, base R, source, committedThrough H, liveFloor}` 一旦对 Browser 可见就不可原地
更换。需要新 snapshot 或更近 base 时增加 generation，丢弃旧 candidate。Recovery lane 只覆盖 `(R,H]`，
live lane 只覆盖 `[H+1,...)`；同一 canonical eventSeq 出现在两个 lane 表示错误 floor，必须 fail closed。
同 lane、同 delivery ordinal 的 retry 只有 envelope 与 canonical mutation 逐字节一致时才是幂等 duplicate。

### 并发 gap-fill

```text
recovery lane:  snapshot@R + exact mutations (R,H] + RecoveryDone(H)
live lane:      H+1, H+2, H+3, ...
                                      \
Browser bounded RecoveryAssembler -----+--> continuous apply
```

Browser 可以并行下载/restore snapshot、接收 gap-fill 和接收 live mutation。Assembler 按 `eventSeq`
组装连续前缀，仅对同 lane、同 ordinal 的逐字节一致 retry 幂等；跨 lane 同 eventSeq 立即 fail closed。它检查
`ptyOffset`，只把从 `nextExpected` 开始的连续 mutation apply 到 recovery target：warm 使用已存在且
cursor 精确等于 R 的 active replica，cold 使用从 snapshot@R 创建的 detached candidate。它必须同时限制：

- 每客户端和每 generation 的 bytes、frames 与最大 age；
- 每 session 所有 recovering client 的合计预算和并发公平性；
- 单 frame/payload 上限、gap 数量、duplicate 检查成本和 apply work slice；
- snapshot download、restore、recovery lane 和 live lane 的 deadline。

Frame 从 reorder map 被 apply 后，assembler 仍须在可能产生同 lane late retry 的窗口内保留有界的完整
canonical identity；跨 lane 同 eventSeq 直接 fail closed，不进入 duplicate 比较。Browser 发送 receipt 不证明 source 已收到；只有它收到匹配的
`RecoverySourceClosed`，且本地 recovery lane cursor 的 contiguous delivery ordinal 与 cumulative encoded
bytes 同时覆盖 certificate 声明的位置，才能释放该 late-retry cache。同 lane late duplicate 必须逐字段/逐
payload 比较；divergent duplicate 会 taint target。Cold 直接 discard candidate；warm active core 必须标记
non-current/tainted，并从新 generation 的 cold base 恢复，绝不能把已污染 cursor 当作 warm base。

超过任一上限、source 无法履行 `(R,H]`、live lane 从 floor 出现不可修复 gap，或 duplicate 不一致时，
在上述 taint 规则下 reset 该客户端并提升 generation。不能无限扩大 reorder/identity map，也不能让慢
observer 阻止其他 synced client 或 authority head。

```text
RecoveryDone {
  recoveryId,
  deliveryGeneration,
  replayedThrough: AuthorityCursor H
}
```

它只表示 recovery source 已完成该 attempt 承诺的 `(R,H]`，不表示 Browser 已 apply，也不要求 authority
停在 H。Browser 满足以下条件后才能完成 handoff：

1. warm base 已验证，或 cold immutable checkpoint/base R 已完整恢复；
2. recovery target 已连续 apply 到至少 H；
3. 已收到匹配 `recoveryId/generation` 的 `RecoveryDone(H)`；
4. engine、epoch、offset、duplicate 和资源检查全部通过。

Cold 必须原子 adopt detached core；warm 已在 active core 上逐前缀 catch up，不做无意义 clone/swap，但同样要
原子地把 delivery 状态从 assembling 切到 synced。完成后发送：

```text
RecoveryAdopted {
  recoveryId,
  deliveryGeneration,
  replicaApplied: AuthorityCursor // 实际已连续 apply 的位置，可以高于 H
}

RecoverySourceClosed {
  recoveryId,
  deliveryGeneration,
  throughRecoveryOrdinal,
  throughRecoveryCumulativeEncodedBytes
}
```

资源按 ownership 分层释放：Host/source 只有实际收到覆盖 recovery lane `RecoveryDone` 的
`DeliveryReceived` 后，才能原子记录 source closed、release-once `PreparedGap` payload/lease，并通过 Cloud
幂等发送 `RecoverySourceClosed`。Browser 收到 closure，且本地 recovery lane cursor 同时覆盖 certificate
声明的 ordinal 与 cumulative encoded bytes 后，才释放 applied identity cache。Cloud 的 generation/start
bookkeeping 保留到 matching closure 与 `RecoveryAdopted` 都成立，之后才把客户端视为 synced。任一
ACK/closure 丢失只会延长有界 retention 或触发 deadline/reset，不能提前释放比较证据。
提前到达的 `H+1...` 可以已在 recovery target 上连续 apply，或仍留在有界 assembler 中继续 apply；两种情况
都不能丢失或重复执行 effect。

### Receive credit 与 apply progress 分离

乱序数据进入 Browser buffer 后，前方 gap 可能暂时阻止 Ghostty apply。TARGET 因此使用两类进度：

```text
DeliveryReceived(DeliveryCursor)
  // frame 已被校验并安全 admission 到有界 Browser buffer，可释放 transport credit

ReplicaApplied(deliveryGeneration, AuthorityCursor)
  // authority mutation 已被 Ghostty 连续 apply
```

Browser 只有成功保留 frame 所需内存后才发送 `DeliveryReceived`；一旦自身预算不足就停止 receive credit
或 reset。Cloud 不能用 `DeliveryReceived` 推断 terminal cursor，Host 也不能用 `ReplicaApplied` 代替
per-lane transport credit。这种分离允许 gap-fill 与 live lane 乱序到达，又不让一个旧 gap 永久锁死
network window。

Cloud 到 Browser 的每条 v3 delivery 使用 generation-scoped envelope，显式携带 logical lane、从 1 开始的
delivery ordinal，以及该 lane 的 cumulative encoded bytes。Envelope payload 是完整、未改写的 canonical
data-v2 frame；delivery metadata 不进入 authority log。`RecoveryDone` 是 recovery lane 的最后一个 record，
占用稳定 ordinal，因此 Host source 的 release certificate 可以由 receipt 精确覆盖，而不是根据 authority
cursor 猜测 transport ownership。

### CURRENT pause 的移除条件

只有以下能力在同一 session epoch 内全部协商并通过故障测试，才允许选择 Recovery v3：

- immutable `RecoveryStart` fence、committed-through H 和 live floor；
- catching-up client 的 uninterrupted live delivery obligation；
- bounded assembler、duplicate/gap/offset verification；
- `RecoveryDone`、atomic local handoff、`RecoveryAdopted` 与双向 `RecoverySourceClosed` closure；
- receive credit 与 authority apply progress 分离；
- per-client reset、generation fence 和 multi-client aggregate budget。

缺少任一项都回退完整 v2 recovery，而不是关闭 pause 后混用一半 v3。

## Prediction 是 disposable presentation branch

`Raw` 不预测；`Mirrored` 仍由 application 拥有 buffer、每键立即发 PTY，Browser 只叠加可撤销 overlay；
`Owned` 只有在显式 integration 转移 ownership 后才持有本地 draft。`Mirrored` 的 authority path 与 `Raw`
完全相同。

Browser 应按 predicted prefix 而不是“一键对应一次 echo”对账：`InputValidationCut` 只开放验证资格，
verifier 退休最大 informative compatible prefix，再从新 authority base 重算未确认 suffix；冲突则清空整个
prediction epoch。Libghostty 只计算 grapheme、width、wrap、cells 和 cursor geometry，不能猜应用语义。
Overlay failure 只能关闭 prediction，不能阻断真实 input 或修改 authority replica。

## Capability negotiation 与灰度

Wire 变化不能只依赖 deployment feature flag。Host、Cloud 和 Browser 必须先 advertise 支持的 protocol
versions 与 capability families；未知 version/kind/status 必须在发送新语义前被拒绝，不能让 strict v2
decoder 收到 v3 frame 后才失败。不同能力按其实际 ownership 选择，不能为了一个 writer sideband 把所有
observer 或整个 session 强绑到同一升级节奏：

- 改变 canonical authority log 语义的 data protocol 固定到 `sessionEpoch`；
- recovery strategy 在新 `deliveryGeneration` 上从三方交集选择，并且必须兼容该 session 的 authority
  protocol；同一 generation 内不可热切换；
- writer sideband 在新的 writer control connection 上协商，只发给当前 writer；observer 无需支持；
- context/input-region capability 还必须由具体 application integration 显式 opt in。

Recovery v3 另外要求显式 negotiation confirmation。Client 在 bounded capability header 中包含
`capability-negotiation-v1`；只有 Cloud 返回同 token 的 `negotiatedCapabilities` 才算确认。缺字段、缺 bootstrap
token、未知旧 Cloud，或仅看到历史 `selectedCapabilities`，都必须完整回退 v2。Production endpoint 只能
advertise 已实现的 owner capability；schema 先行不等于 runtime 可用。

Capability family 至少需要覆盖以下依赖关系（最终 wire 名称可在实现时版本化）：

```text
ordered-input + writer-sideband
        |
        +--> input-validation-cut --> mirrored-presentation

delivery-receive-credit + authority-apply-progress
        |
        +--> recovery-v3-gap-fill

explicit-context/input-region
        |
        +--> guarded-context / owned-buffer
```

Rollout 规则：

- 现存 long-lived v2 authority session 保持 v2 canonical 语义；若其协议无法承载完整 Recovery v3，则继续
  使用 global pause，等新 session epoch 升级，不能热切换 authority cursor。
- 同一 delivery generation 不混用 v2 barrier/pinned commit 与 v3 concurrent gap-fill。
- 不支持 sideband 的参与者不能收到 `InputValidationCut`，observer 永远不接收 writer validation metadata。
- `Mirrored`、Recovery v3、context integration 和物理 transport split 使用独立 capability/kill switch；
  一个失败不应迫使其他无依赖能力一起回滚。
- Shadow 不改变 wire/effect；回滚只影响新 generation/epoch，已有 attempt 完成既定协议或明确 reset。

具体实现依赖和灰度比例由两份实施计划维护；总纲要求每项 capability 只能在上述对应边界生效，并且任何
feature 都不能绕过 authority 连续前缀不变量。

## 验证要求

实现必须把最高级不变量直接编码成 model/property tests，而不只依赖端到端截图：

- 对随机 checkpoint cut、gap-fill/live interleaving、duplicate、resize、UTF-8/VT continuation，adopt 后的
  authority cursor 和 future parser behavior 必须等同于 uninterrupted continuous prefix；
- 任意 missing/divergent mutation、错误 live floor、跨 epoch/generation 或 offset mismatch 均不得 adopt；
- `InputValidationCut` 丢失、重复、先于 data 到达、reconnect 或 observer attach 不改变 authority log；
- prediction overlay 在 clear/reset/crash 前后不改变 core snapshot、selection/copy/search/ARIA 和 network；
- 16 个 recovering client、慢 Browser、持续输出、snapshot retry/supersede 下，各层预算有界且 synced
  client 与 input/control 不被单一 recovery 全局阻塞；
- v2/v3 rolling deployment 中，未协商 capability 的 endpoint 永远收不到未知 frame。

实施细节和阶段 gate 分别见[高性能 Snapshot 恢复计划](high-performance-snapshot-recovery-plan.md)与
[输入稳定性计划](input-stability-plan.md)。若实施计划与本文件冲突，以这里的 authority/prediction
不变量和 CURRENT/TARGET 边界为准，并先更新架构决策再改代码。
