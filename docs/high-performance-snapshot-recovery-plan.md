# 高性能远程终端 Snapshot 与 Recovery v3 实施计划

> 状态：根据 PR review 重写，等待分阶段实施
>
> 决策日期：2026-08-28
>
> 适用范围：Host authority、snapshot/checkpoint、journal、Cloud delivery 和 Browser replica

本文是[终端三平面协议总纲](terminal-protocol-architecture.md)的 snapshot/recovery 子计划；输入顺序、
高 RTT 反馈和 prediction 见[输入稳定性计划](input-stability-plan.md)。跨文档最高优先级不变量是：

> 客户端终端当前采用的状态，永远等于某个 immutable checkpoint，加上同一 authority mutation
> log 的一个连续前缀；所有输入预测都只是这个状态之上的可丢弃 presentation branch。

任何性能优化、恢复捷径和 rollout 决策都必须先证明保持这个不变量。

## 产品目标

Zhongduan 的目标是高性能、稳定、browser-native 的远程终端体验，不是把 Ghostty 的所有内部对象
永久序列化，也不是保存无限 transcript。Snapshot 的作用是吸收已经发生的旧历史，使恢复成本主要
取决于一个较新的 baseline 和 baseline 之后仍然必需的 mutation：

```text
健康连接：       canonical mutation stream
小型同源缺口：   existing replica@R + exact gap-fill(R,H]
页面刷新/长缺口：immutable snapshot@R + exact gap-fill(R,H]
tail 变得过长：  发布 snapshot@R'，让 R' 吸收 (R,R']
```

“使用 snapshot 丢弃历史”只能表示：

- `eventSeq <= R` 的状态已经由采用的 `snapshot@R` 表示，不再传输；
- `eventSeq > R` 且尚未被更新 snapshot 吸收的 mutation 必须无洞、无歧义地交给 replica；
- transport 可以批量、压缩、跨逻辑 lane 乱序到达，但 terminal apply 顺序不能改变；
- 不能在旧 snapshot 后任意截断 tail，也不能用较新的画面替代 terminal execution 所需的中间 mutation。

实现完成后，fresh attach 和长缺口将复用较新的 checkpoint；下载 snapshot、补历史缺口和接收 live
output 可以并发；单个 Browser 恢复不会暂停 authority 或其他客户端；所有队列、重排、等待和资源
ownership 都有界。

## Authority 正确性边界

### Canonical mutation log

当前 terminal authority log 只包含会改变可恢复 terminal state 的 mutation：

- `PTY_OUTPUT`；
- `RESIZE_APPLIED`。

它们共用严格递增的 `eventSeq`，并用 `ptyOffset` 验证 PTY byte 位置。Snapshot 内的 parser
continuation 属于 cut 之前的状态，恢复后不能作为新的 PTY output 再发送。

`InputValidationCut`、输入 ACK、writer lease、context token、delivery marker 和 recovery marker 都不属于
terminal mutation，不消耗 `eventSeq`，不进入 journal/snapshot tail，也不广播给 observer。输入验证使用
writer-only、live-only、可丢失的 causal certificate；详细定义归输入计划和三平面总纲所有。

并发 live/recovery 使同一个 mutation 可能重复到达。第一版按 canonical 字段逐项比较
`{sessionEpoch, eventSeq, kind, ptyOffset, semantic flags, payload}`；完全相同才可去重，不同则 fail
closed。Mutation hash chain 可以作为后续诊断和快速校验优化，但不是 Recovery v3 的正确性前置条件。

### Snapshot 覆盖范围

Snapshot 必须自洽恢复后续 terminal execution 所需的主/备用 screen、有限 history、cursor、modes、
margins 和未完成的 UTF-8/CSI/OSC/DCS parser continuation。它不承诺：

- 超过 scrollback 限额的历史或审计 archive；
- Unix child/PTY 进程状态；
- 默认未纳入 schema 的 Kitty graphics；
- bell、clipboard、query 等瞬时 effect 的重放。

固定 wterm submodule 当前仍使用完整同步 snapshot encode，见 pinned commit 的
[WASM `encode_snapshot`](https://github.com/Eric-Song-Nop/wterm/blob/4764f3b5e81cb76cdb08d4ffbfba1257fd33efd6/packages/%40wterm/ghostty/zig/src/wasm_api.zig#L560-L581)；
passive restore 的 READY/history/FINISH ownership 见同一 commit 的
[restore contract](https://github.com/Eric-Song-Nop/wterm/blob/4764f3b5e81cb76cdb08d4ffbfba1257fd33efd6/packages/%40wterm/ghostty/README.md#passive-snapshot-restore)。
这些链接固定到 submodule SHA，不能用指向 `vendor/wterm/...` 的仓库内相对 GitHub 链接代替。

## CURRENT：data protocol v2 的正确性条件

当前 v2 恢复流程是：

```text
Host pause canonical publisher
  -> 采样 commit C
  -> flush canonical through C
  -> Cloud barrier 固定 {deliveryGeneration, snapshot/base R, commit C}
  -> directed tail(R,C]
  -> ReplayCommit(C)
  -> resume canonical
```

这套流程性能上保守，但以下组合是**当前实现的 correctness invariant**：

1. Host 在选择 `C` 后全局暂停 canonical publisher；
2. Cloud barrier 把同一 generation 的 baseline 和 commit 固定下来；
3. pinned delivery 存在时 Cloud 不接受新的 canonical head；
4. directed replay 精确到达 `C` 后，Cloud 只有在转发 `ReplayCommit(C)` 时才把 delivery dataState 切为
   synced；Browser 仍须等 detached candidate 完整 restore、apply 并验证 commit 后才 adopt；
5. marker outcome uncertain 时不能换 snapshot/tail/commit，只能完成原 attempt 或 fence generation。

CURRENT 只能在 fixed commit 之后把新 live frame 交给 Browser 的有界 pending buffer；它没有让
`C+1...` 在 commit 前越过 directed tail、再与 `(R,C]` 任意交错重排的 handoff。因此不能单独删除 pause、
放宽 Cloud pinned check 或让 live frame 越过 barrier；任何一项单删都会造成拒绝、漏帧、错拼 baseline
或错误 synced。v2 fallback 在 Recovery v3 完整通过 gate 前保留上述行为。

CURRENT 的主要代价是 barrier 与 directed replay 到 fixed commit 之间会暂停全局 canonical ingress，Host
用 8 MiB/1024-frame queue 临时吸收 output，Cloud/Host recovery 近似串行。Host 发出 ReplayCommit 后会
立即 resume，snapshot HTTP/restore 尚未完成时到达的新 live frame 可由 Browser 继续有界缓存；CURRENT
缺少的是 commit 前的 concurrent gap-fill/live handoff，而不是整个 snapshot 下载期都停止 live。这些 v2
ordering 条件要由 Recovery v3 整体替换，不能逐个关闭。

## TARGET：Recovery v3 并发 gap-fill

Recovery v3 对 warm 和 cold 使用同一套协议，只改变 base 的来源：

```text
warm: existing replica@R + gap-fill(R,H]
cold: snapshot@R         + gap-fill(R,H]
```

终端 state plane 继续产生唯一 canonical log；delivery plane 为每个客户端并发发送 recovery gap-fill 和
新的 live mutation；Browser `RecoveryAssembler` 有界重排并只连续 apply。Interaction sideband 不进入
这条 log。

### Committed-through-H start fence

`H` 必须同时是 Host 准备好的 recovery source 上界和 Cloud 已提交的 authority cursor。开始一次 recovery
使用两段式、同一 canonical transport 内有序的 start fence：

1. Cloud 验证 client、session epoch、engine、generation 和候选 base `R`，向 Host 发 prepare request；
2. Host 在 canonical publisher 的有序临界点，原子取得有硬预算且可保留到发送完成的
   `PreparedGap(R,H]`，并把 `RecoveryStartFence(recoveryId,generation,R,H)` 排在 canonical mutation `H`
   之后、任何 `H+1` 之前；它是 delivery marker，不消耗 `eventSeq`、不进 journal；
3. Cloud 从同一 Host data stream 收到 fence 时要求 committed head 恰好等于 `H`；在处理下一条 Host data
   message 前，原子把客户端置为 `assembling`、固定 start metadata，并安装
   `liveFloor = successor(H)` 的 delivery obligation；
4. Cloud 向 Browser 发 `RecoveryStart` 并向 Host ACK 可以发送 prepared gap；此后 `H+1...` 持续 fanout
   给该客户端和所有 synced 客户端，Browser 允许在 start 前有界缓存同 generation live frame；
5. 任一 prepare、head、generation 或 source-retention 校验失败都 reset/replan，不发送截断 tail。

这个短临界点只排序一个 marker，不等待 Browser、snapshot download 或 recovery RTT，也不冻结 `H` 之后的
authority。若未来拆分物理 stream，必须增加等价 cross-stream fence；不能让 attach/control 的到达顺序代替
Host canonical ordering。

`PreparedGap` 的 owner 是精确 `recoveryId/generation`。正常路径只有在 Host/source 实际收到覆盖 recovery
lane `RecoveryDone` 的 `DeliveryReceived` 后，才原子记录 closed 并 release-once payload/lease；随后幂等返回
`RecoverySourceClosed`。Cloud generation bookkeeping 保留到 source closed 与 `RecoveryAdopted` 都成立。
generation reset、Browser Start send failure、Cloud ACK outcome uncertain、deadline、pair fence 和 session
dispose 都必须进入明确的幂等 cleanup；source lease/copy 不得因失联永久 pin journal，也不能被新 generation
误用。

建议的协议形状是：

```text
RecoveryStart {
  recoveryId,
  deliveryGeneration,
  engineId,
  base: AuthorityCursor R,
  snapshotId?,
  committedThrough: AuthorityCursor H,
  liveFloor: successor(H)
}

RecoveryDone {
  recoveryId,
  deliveryGeneration,
  replayedThrough: AuthorityCursor H
}

RecoveryAdopted {
  recoveryId,
  deliveryGeneration,
  replicaApplied: AuthorityCursor K   // K >= H
}

RecoverySourceClosed {
  recoveryId,
  deliveryGeneration,
  throughRecoveryOrdinal
}
```

`RecoveryStart` 一旦可见，同 generation 的 `{recoveryId, engineId, base R, snapshotId}` 不可更换；
rebase 必须增加 generation。`H` 之后的 head 可以持续前进。

### Browser RecoveryAssembler

每个 generation 建立独立 assembler：

```text
base R
nextExpected = R + 1
framesByEventSeq = bounded map
appliedIdentityByEventSeq = bounded overlap cache
recoveryDoneThrough = optional H
bufferedBytes / bufferedFrames / oldestBufferedAt
targetCore = existing active replica@R（warm）或 detached snapshot replica@R（cold）
```

处理规则：

1. recovery lane 只接受 `(R,H]`，live lane 从 `liveFloor` 开始；start 到达前的同 generation frame
   可以进入有界 pre-start buffer；
2. 任一 lane 到达的 mutation 先验证 epoch、cursor、offset 和 exact duplicate identity；已经 apply 并从
   reorder map 删除的 overlap 仍与 `appliedIdentityByEventSeq` 中的完整 canonical fields/payload 比较；
3. 只有 `nextExpected` 存在时才 apply，并循环推进连续前缀；较新 frame 留在 map 中；warm 可在 active
   replica 上逐个展示这些连续前缀，cold 只更新 detached candidate；
4. cold snapshot 尚未下载/restore 完成时只拥有并缓存 bounded copy，不推进 candidate cursor；
5. 收到 `RecoveryDone(H)`、target core 已连续 apply 到至少 `H` 后，handoff 才 eligible；
6. cold 原子 adopt candidate；warm 不 clone/swap core，只原子提交 local handoff。随后 Browser 发送
   `RecoveryAdopted(K)`；Cloud 记录 generation/cursor，但只有 matching `RecoverySourceClosed` 也成立后才
   释放 generation state 并标记 synced；
7. ACK、Done 或 Adopted 丢失可以在同 generation 幂等重发；任何 identity 冲突、hole deadline 或
   ownership 不确定都 reset 该客户端，不影响 authority 和其他客户端。Cold conflict 直接 discard candidate；
   warm conflict 必须把 active replica 标记为 non-current/tainted，下一 generation 强制 cold rebase，不能复用。

Applied identity cache 计入 per-generation bytes/frames；只保留可能被 recovery/live overlap 或 late retry
命中的区间。Browser 发出 `DeliveryReceived` 后仍须保留 cache；只有收到 Host/source 在实际处理 receipt 后
返回的 matching `RecoverySourceClosed`，且本地 ordinal 已达到 `throughRecoveryOrdinal`，才可释放。Closure
丢失时幂等请求/重发，最终走有界 deadline/reset；不能为了完整比较保留无界 session history。

live `H+1...` 可能先于 `RecoveryStart`、snapshot 或 recovery frame 到达，也可能在 adopt 前已经连续
apply 到 `K > H`；这不改变 adoption 条件。Assembler 始终采用同一 log 的连续前缀，不能因“画面看起来
更新”跳过缺口。

### Holdback 放在 Browser，而不是 Cloud durable payload

TARGET 优先让 Browser assembler 有界保存已收到的 live/recovery payload。Cloud 只持久化或挂载必要的
generation、start fence、live floor、发送/接收 cursor 和 credit，不为每个 catching-up Browser 复制
一份 durable mutation payload holdback。理由是：

- payload 的 authority 来源已经是 Host journal；Cloud 再持久化会引入 DO hibernation、清理和双份预算；
- Browser 已安全拥有 bounded copy 后即可释放 transport credit；断连时本来就必须换 generation 重放；
- per-client Browser cap 能隔离慢客户端，不把一个 observer 的恢复成本扩散到 session；
- 超限时 reset + 更新 snapshot，比无界 Cloud holdback 更明确。

Cloud/Host 仍需对未被 Browser receipt ACK 的 socket/queue bytes 设硬上限；“holdback 在 Browser”不表示
transport 可以无限发送。

### Transport receipt 与 ReplicaApplied 分离

CURRENT 的同一个 data ACK 同时携带 authority cursor 并释放 delivery credit，但含义已经随路径变化：live/warm
通常在 apply 后 ACK；cold restore 尚无 candidate 时，会先推进 received cursor、把 payload 放进有界 buffer，
再 ACK，真正 apply/adopt 发生在之后。Recovery v3 允许跨 lane 乱序后，这种“有时 receipt、有时 apply”的
cursor 更不能作为单一进度，因此拆成：

```text
DeliveryReceived {
  deliveryGeneration,
  lane,
  contiguousDeliveryOrdinal,
  receivedBytes
}

ReplicaApplied {
  deliveryGeneration,
  authorityCursor
}
```

`DeliveryReceived` 只在 payload 已复制进有界 assembler 后发送，用于释放对应 logical lane 的 transport
credit；ordinal/bytes 必须单调且不超过已发送值。`ReplicaApplied` 表示 Ghostty 已连续 apply 的
authority cursor，用于正确性、进度和 adoption。两者不能互相推导。

### 逻辑 lane、物理连接与公平性

第一版建立 `control-input`、`live-data`、`recovery-data` 三个逻辑 lane，并为 live/control 保留 credit 和
scheduler priority；snapshot blob 继续走 HTTP。先允许 live/recovery 复用当前 data WebSocket，通过有界
quantum 调度。只有指标证明 recovery 的 TCP/WebSocket HOL 仍显著推高 live latency，才升级为第三条物理
WebSocket 或未来 QUIC/WebTransport stream。三条物理 WebSocket 不是正确性前置条件。

Recovery scheduler 必须有 session 级并发上限、每客户端 bytes/frames credit、deadline 和
deficit/round-robin quantum。16 个客户端可以共享同一个 immutable checkpoint/build；慢客户端耗尽自身
credit 后暂停或 reset，不能独占 recovery emitter，也不能推迟 writer 的 live/control。当前单 active
recovery 可以作为 rollout 起点，但不能继续依赖全局 pause 来串行化正确性。

每个 generation 至少限制：

- pre-start、reorder 和 snapshot-wait 的 bytes/frames；
- 最大 eventSeq gap span；
- wall-clock recovery deadline 与无进展 deadline；
- worker/decoder/candidate ownership 数量；
- Cloud/Host 每 session 聚合 outstanding；
- reset 次数和重试 backoff。

任一限制触发时，取消 HTTP/decoder/candidate，清空该 generation 的 assembler，增加 generation，并从
可服务 base 重新规划；旧 payload 和旧 `RecoveryAdopted` 不得进入新 attempt。

## SnapshotCheckpointManager

Snapshot 构建归 session-owned manager，而不是单个 attach request。Manager 至少持有：

```text
latestValid       最近一个通过完整性和 lineage 校验的 immutable checkpoint
refreshInFlight   至多一个 capture/compress/publish pipeline
pendingBody       至多一个可幂等重试、也可被 supersede 的 immutable body
needsNewerCut     合并后的刷新压力和最低 cut 需求
```

多个 waiter 共享 build；单个 request 被 supersede 只移除 waiter，不取消仍有价值的公共 build。Manager 只
安装同 engine、同 epoch、cut 单调前进的 checkpoint，并确保 dispose、epoch change 和 failure 恰好释放
一次资源。

`snapshotId` 与 body 仍是 immutable identity：结果不确定的 upload retry 必须继续使用同 ID、同 bytes，
不能以新 body 覆盖。所谓 supersede 只表示 planner 不再把旧 cut 分配给新 recovery；旧 upload 只有在 Cloud
ledger 到达 cleanup-safe terminal state，或已转交给独立有界 cleanup owner 后，才能释放本地 body 并允许
新 ID。迟到 abort/delete 也不得命中 replacement。

Phase 1 按可直接验收的 ownership gate 分层实现。P1.1 只把现有 publisher 和一个 immutable
`latestValid` checkpoint 收归 `HostCloudRelay` 生命周期内的 manager，并把 30 秒阈值改成只读
`ageFresh` 分类。它不把 `refreshInFlight`、waiter、capture 或 recovery generation controller 从 scheduler
移入 manager；这些 ownership 变化必须在后续 sub-gate 中连同新的取消语义和直接测试一起落地。

P1.1 的 `latestValid` 表示 publisher 已确认、metadata 与固定 session/engine/epoch/cut identity 自洽。它不表示
Cloud blob 此刻可取得，也不表示 checkpoint 对某个目标 `H` usable。相同 identity 的幂等结果不续 freshness；
迟到的旧 cut 或同 cut 不同 identity 不能覆盖 latest。只有 exact-identity invalidation 清空 latest 后，才允许在
未变化的 authority cut 安装新 snapshot identity。

### valid、usable 与 fresh 是三个概念

| 属性     | 含义                                                                        | 过期/失败动作                         |
| -------- | --------------------------------------------------------------------------- | ------------------------------------- |
| `valid`  | schema、checksum、engine、epoch、cut 和 immutable body 自洽                 | 标记损坏，不得再采用                  |
| `usable` | 针对目标 `H`，blob 可取得且 journal 精确覆盖 `(R,H]`，bytes/frames 在预算内 | 换更近 cut、reset 或报告 unavailable  |
| `fresh`  | age、tail cost、dirty state 达到策略目标，值得后台刷新                      | 触发 refresh，不删除仍 usable 的 body |

P1.1 起，原 cache 的 30 秒 TTL 只产生 `ageFresh=false`，不再按年龄删除或隐藏 checkpoint，也不会因读取而
续期。Idle session 状态未变化时，一个超过 30 秒但 identity-valid 且 tail 为空的 checkpoint 仍可由 CURRENT
静态 tail gate 使用。`ageFresh` 不是完整 `fresh`：tail cost、dirty state 与后台 refresh policy 仍由后续
sub-gate 实现。

### Serviceability 必须动态测量

每次 resume pending upload、安装 checkpoint、选择 `RecoveryStart`、retry 和 rebase 前，都对实际目标
`H` 调用等价于下面的 range measure：

```text
measure(R,H) -> {
  status: ok | cursor-ahead | gap | blob-unavailable,
  exactTailBytes,
  exactTailFrames,
  oldestRequiredMutationAge,
  estimatedDeliveryMs
}
```

P1.2 只关闭其中 Host 当前内存 journal 的 `gap`、`exactTailFrames` 和 `exactEncodedBytes` 事实，以及
CURRENT cold path 的 tail-envelope eligibility。它不会把 `cursor-ahead`、blob availability、mutation age、
estimated delivery、Browser budget 或 refresh policy 伪装成已经实现的 `usable`。Range plan 在 canonical pause
后固定同一个 `{R,H}`，先返回 scalar facts；只有现有 256 KiB credit / 512 frame gate 通过后才 materialize，且
materialize 必须来自同一个 journal revision。`exactEncodedBytes` 是 stored frame 长度之和；现有 256 KiB
准入仍使用 `payload bytes + 64 bytes/event`，两者不能混称或借此修改策略。

只有 `status=ok` 且 bytes、frames、age、delivery/browser budgets 全部满足时才 usable。不能把
serviceability 简化成 capture timestamp 加固定 `serviceableUntil`：output byte rate、frame rate、journal
segment retention 和 Cloud head 都会动态变化。

`cursor-ahead` 表示 snapshot cut 已在 Host 产生，但 Cloud committed head 尚未到达该 cut；可以有界等待
Cloud 追上并重试同一 body。`gap`，或任一 budget check 失败，会派生 planner reason
`tail-unavailable`，表示 `(R,H]` 已缺失或超预算，等待通常只会更坏；planner 必须停止给新 recovery
分配这个 body，并在满足上一节 cleanup-safe ownership 后
abandon/supersede，允许更新 cut，而不是让 publisher 永远优先 resume 旧 body。
若实现 journal pin，pin 也必须有独立 bytes/frames/age hard cap，超限后 abandon。

## Capture pause 与 rolling snapshot 前置条件

CURRENT `session.captureSnapshot()` 在 session actor turn 内调用完整同步 WASM encode。JavaScript timeout
只能在同步调用返回后运行；现有 5 秒 timer 和事后 `encodeMs` 检查不能抢占卡住的 actor。因此在持续
output 下更积极触发 capture，可能周期性阻塞 input、PTY output 和 Ctrl-C。

Rolling hard deadline 之前必须先交付“严格有界 immutable cut”能力。接口概念可以是：

```text
freezeTerminalCut() -> ImmutableTerminalCut + AuthorityCursor
                            |
                            +-> bounded/off-actor/incremental encode
                            +-> compress / publish
```

具体实现可以是 Ghostty persistent/COW state、actor 内严格有界 copy 后 off-actor encode，或保持同一
immutable cut 的增量/yield encoder；计划不预先锁定一种。但必须满足：

- cut 与 cursor 在一个 authority turn 内原子对应；
- 后续 PTY/output/input 可以继续推进，不修改已冻结 cut；
- actor pause 有可执行的 hard max，而不是事后 timeout；
- 超过 pause envelope 时跳过本次 capture 并记录 unavailable，不循环同步重试；
- capture actor pause P99 和 hard max 通过输入计划定义的本地处理 SLO。

在此 gate 之前，Recovery v3 可以继续使用现有 full snapshot 做 request-driven recovery，但不得启用持续
output 下的 rolling hard capture deadline。

Gate 通过后，quiet 只作为低成本机会：有 quiet 就提前 capture；达到 hard refresh deadline 才在受控
rate limit 下 capture。Single-flight、minimum interval、failure backoff 和 input/control priority 始终保留。

## Planner 与容量模型

`RecoveryPlanner` 比较 Browser live core、journal gap、动态 tail bytes/frames、checkpoint
valid/usable/fresh、snapshot restore 成本和预计收敛时间。小缺口走 warm；其余选择最新 usable
checkpoint。若 tail 超预算，只能更新 baseline 或 reset，不能截断。

强制 refresh/replace 条件：

- 没有 valid checkpoint，或 blob/engine/epoch/schema 不再 valid；
- 对计划中的 `H` 已出现 journal gap；
- exact tail bytes、frames、age 或预计 delivery/apply 时间超硬预算；
- pending body 已失去 serviceability；
- immutable cut/protocol generation 已变化。

机会性 refresh 条件：checkpoint 超 freshness 目标且 authority 已改变、cut 后 dirty cost 超软目标、近期
attach 压力，或 staging 数据证明能降低 P95/P99。Idle 且 authority 未变化时不重复上传等价 snapshot。

容量必须按 bytes 和 frames 分开建模。设 `lambda_bytes/lambda_frames` 为 authority 产生速率，
`d_*` 为 delivery rate，`a_*` 为 Browser apply rate：

```text
lambda_bytes  * T_publish < B_host_bytes
lambda_frames * T_publish < B_host_frames
T_publish < journal_retention_age

d_bytes  > lambda_bytes       d_frames > lambda_frames
a_bytes  > lambda_bytes       a_frames > lambda_frames

lambda_bytes  * T_restore < B_browser_live_bytes
lambda_frames * T_restore < B_browser_live_frames
```

短时不满足时更新 snapshot/reset；长期不满足时保持 control 可用、资源有界并明确 unavailable。不能用
无限提高 snapshot 频率或 buffer 掩盖吞吐不收敛。

### 当前运行时保护值

这些值属于不同层，不能合并成一个 tail budget：

| 层级/用途                        | CURRENT 值                                                               |
| -------------------------------- | ------------------------------------------------------------------------ |
| Journal segment / retention      | 256 KiB 或 250 ms；60 s / 8 MiB                                          |
| Host warm/cold tail admission    | 256 KiB / 512 frames                                                     |
| Relay delivery outstanding       | 512 KiB                                                                  |
| Host v2 paused canonical queue   | 8 MiB / 1024 frames                                                      |
| Browser recovery tail            | 2 MiB / 1024 frames；5 s snapshot load + restore deadline                |
| Checkpoint cache / build cadence | 30 s TTL；1 s minimum build interval                                     |
| Snapshot blob                    | 32 MiB compressed / 128 MiB uncompressed                                 |
| Cloud retention                  | 32 snapshot/upload rows；4 reservations；latest + active pins + 2 recent |

迁移初期不顺手放宽这些值。Recovery v3 要新增 per-lane receipt credit、assembler gap span、per-generation
deadline 和 session aggregate cap；阈值先 shadow 观测再调整。

## READY-first 与 history 后置

CURRENT Browser 明确要求 manifest `restoreThrough="finish"`，完整恢复 history 后才返回 candidate。固定
wterm API 虽可在 READY 后继续 PAGE/FINISH，或显式永久 abandon 剩余 history，但当前 owner 一旦
`takeCore()`/adopt 就不能后台继续使用同一 decoder hydrate history。

因此“execution READY 后立即 adopt、history 后台加载”不是打开现有 flag 就能实现。它是独立的 snapshot
format/ownership v2：execution checkpoint 要有独立完整性边界，history page 要可寻址、校验、取消和
一次性挂接，且后台挂接不能改变 authority execution cursor。这个工作后置于 Recovery v3、manager 和
immutable cut；Recovery v3 首版继续完整 FINISH，或在产品明确选择时永久 abandon history，不能假装
abandon 后仍会补齐。

同页 data resync 可以让旧 core 保持可见直到 candidate adopt；硬刷新/fresh attach 没有旧 core，指标和
UI 必须分别统计。

## 错误分类与 retry taxonomy

CURRENT 的非-ready barrier 结果缺少足够的 reason/scope：scheduler resume 后返回，上层仍会无条件
complete request 并丢弃 per-request preparation，可能让 Browser 永久停在 catching-up。它不等于所有路径
都会 invalidate 共享 checkpoint。v2 修复和 v3 `RecoveryStart` 都使用带原因和作用域的结果：

```text
Start/BarrierResult =
  ready(details)
  | stale(reason)
  | rejected(reason, retryScope)

retryScope =
  same-generation
  | refresh-checkpoint
  | drop-client

SchedulerAttempt =
  committed/adopted
  | superseded
  | retrying
  | reset
  | failed
```

CURRENT barrier 原因严格限定为 `generation-fenced`、`client-gone`、`missing-live-seed`、
`snapshot-missing`、`snapshot-metadata-mismatch` 和 `browser-control-send-failed`。warm 只允许
`missing-live-seed/same-generation` 或 `browser-control-send-failed/drop-client`；snapshot 只允许
`snapshot-missing|snapshot-metadata-mismatch/refresh-checkpoint` 或
`browser-control-send-failed/drop-client`。Browser control 发送失败只能 isolate/drop 当前客户端，不能全局
invalidate 健康 checkpoint；snapshot missing 才进入 refresh-checkpoint。

CURRENT barrier rejection 不提供无 owner 的 `reset-generation` 占位符。generation reset 由现有
`replay-unavailable` 或 Cloud delivery-reset 路径的明确 owner 发起；Recovery v3 若需要新的 reset outcome，
必须在对应 owner 和状态转换同时落地时重新定义。`tail-unavailable` 是 Phase 1 Host range gap/budget 检查
派生的 planner reason，不伪装成基础设施 retry。

只有协议状态机明确证明 marker/start 未产生不确定结果时，才允许 same-generation retry。否则 fence
generation，避免跨 attempt 拼接。

## 实施阶段与依赖

### Phase 0：修 CURRENT recovery 活性

Phase 0 的规范范围、直接测试证据与局限见 [Phase 0 验收契约](phase-0-acceptance-contract.md)。本阶段只收口
CURRENT protocol v2 的 recovery 活性：

- 保留 v2 pause/barrier/pinned commit correctness invariant；
- 落地 `reason + retryScope`，修复 non-ready 被错误 complete 的 Browser freeze；
- 增加 attach-start 与 warm-completion watchdog；明确 retry/drop，generation reset 只由已有显式 owner 发起；
- 用 scheduler、queue、Browser session、Cloud relay 和 protocol 的状态机测试直接验证每个声明的转换。

Gate：验收契约的四个 required gate 全部通过。性能、production dashboard、纯 network 分解和通用
application effect 验证后置，不阻止 Phase 0 收口，也不得由本阶段的 timeout 或状态机测试代替。

Phase 1 只能从通过上述契约和完整 CI 的 exact Phase 0 revision 开始；它可以作为 stacked PR 的 parent，
但在 Phase 0 合入 `main` 前不能把主线状态写成已完成。

### Phase 1：Checkpoint manager 与动态 serviceability

- **P1.1：latest-valid ownership 与 age freshness**。每个 HostCloudRelay/TerminalSession 构造一个
  `SnapshotCheckpointManager`，跨 relay connection 复用现有 publisher 和 immutable latest；30 秒只改变
  `ageFresh`，不删除 checkpoint。Scheduler 仍用现有 journal replay、frame/byte 上限决定当前 delivery，且
  capture/quiet/backoff/generation cancellation 与 v2 ordering 不变。
- **P1.2：Host journal range facts 与 CURRENT cold-tail admission**。在 materialize replay 前，对 canonical
  pause 后固定的 `{R,H}` 计算实际 encoded bytes、frames 和 gap；超出现有 credit/frame envelope 时不复制旧
  tail，仍走 exact-checkpoint invalidation 与 quiet 后 recapture。它只证明 Host-local eligibility，不证明
  Cloud blob、Browser budget、pending body 或完整 `usable`，也不决定 refresh policy；v2 delivery ordering
  不变。
- **P1.3：pending body serviceability**。pending body 只在仍 serviceable 或 bounded cursor-ahead 时 resume，
  否则按 cleanup-safe ownership abandon/supersede；失败不能 starvation、storm 或泄漏 immutable body。
- **P1.4：manager-owned refresh single-flight**。把 `refreshInFlight`、合并后的更新压力和 waiter ownership 移入
  manager；单个 waiter supersede 不取消仍有价值的公共 build，connection replacement 可以接续同一 session
  refresh。每个 waiter 的 signal 只拥有该 waiter；公共 capture、publish 和 install 只由 manager dispose 或
  manager-owned deadline 终止。`minimumCut` 是 inclusive floor：旧 waiter 可以接收已满足自身 floor 的 `R`，
  较新的 waiter 不得被同一结果降级，并至多合并出一个 follow-up flight。Generation replacement 本身不抬高
  floor；只有 exact checkpoint invalidation 或 P1.3 pending supersede 提供新的 replacement floor。此步必须
  显式更新当前 generation-supersede 的 capture/publish 契约。

P1.1 的直接 gate 是：manager 在 30 秒边界后仍返回同一 latest 且 `ageFresh=false`；两个无 mutation 的 cold
attach 相隔超过 30 秒仍使用同一 `snapshotId`，authority encode 和 publish 总计各一次。现有 16 attach
单 build 只是回归保护，不作为本层新增进展。旧 checkpoint 的 tail gap/静态超预算仍必须 invalidate 并 recapture。
P1.1 单独通过不表示 Phase 1 完成；下列总 gate 仍需 P1.2–P1.4 的直接证据。

P1.2 的直接 gate 是：mixed output/resize range 返回精确 scalar facts 且不复制 frames；CURRENT cold tail
超 256 KiB credit 或 512 frames 时不 materialize 旧 range，under-limit range 才从同一 journal revision
materialize。Materialized frame count/encoded bytes 与 plan 不一致时 fail closed，不能发送 barrier、截断 tail
或 commit。Warm policy、pending serviceability 与完整 `usable` 不在本层声明内。

P1.3 的直接 gate 是：build 后首次 PUT 前、每次 retry/resume 前以及 exact upload success 安装前，重新以
最新 `H=session.cursor` 检查 P1.2 的 Host-local range、现有 256 KiB credit 和 512-frame 上限；失去 eligibility
的 body 立即不再作为 recovery candidate。尚未 PUT 的 local body 可 exact-CAS 释放；结果不确定的 PUT 必须
转入同一 session 最多一个 local cleanup owner，并继续使用同一 ID/body 做 reconciliation；只有 exact success、
checksum cleanup-confirmed，或 Cloud 明确接管的 completed object 才能释放。Active/build 与 cleanup 合计最多
保留两个 immutable body，cleanup 满时不得构造第三个。

Cloud 只在 exact completed row 和 R2 object 已验证、且唯一失败是 committed head 尚未追上 cut 时返回
`409 snapshot-cursor-ahead`。该 identity 从第一次收到此精确结果起最多等待 120 秒；attach、relay reconnect、
caller timeout、backoff 和后续相同响应都不能续期。generic `snapshot-conflict`、transport/5xx 和 unknown 结果
不能冒充 cursor-ahead，也不能直接释放 body。部署顺序必须 Cloud-first：旧 Host 会保守重试新错误码；新 Host
连接旧 Cloud 时仍保守保留 generic 409，因此在 rolling 窗口中只会暂时退化为旧行为，不牺牲 immutable retry。
本层不改变 Cloud generic `uncertain` 的 alarm/GC 语义；Host 进程退出后该类 reservation 的 durable cleanup
仍是后续 gate，不能由运行期的单个 local cleanup slot 推导。

P1.4 的直接 gate 是：16 个尚未完成的 cold attach 都注册为真实 waiter，但 authority capture、encode、publish
和 install 各至多一次；取消其中 15 个 waiter 不取消公共 flight。旧 relay connection 在 unresolved publish
期间关闭时只取消旧 waiter，replacement connection 接续同一 flight，旧 connection 不收到 barrier/commit，
replacement 才能完成 delivery。若 flight 在 `R` 完成时已有最低 `H>R` 的 live waiter，`R` 只满足允许它的旧
waiter，manager 合并压力并启动恰好一个满足 `H` 的 follow-up；所有 waiter 已离开时，已有 flight 可以安装
有用 checkpoint，但不得无 owner 地启动 follow-up。Exact shared checkpoint invalidation 必须清除所有 scheduler
preparation 中的旧引用；迟到结果不得把它重新分配给 recovery。Waiter abort 不取消公共 flight；manager
dispose 或 manager-owned deadline 会终止公共 flight，并使迟到 completion 无法安装。

Phase 1 stacked-candidate gate 是：P1.1–P1.4 的直接证据和完整 CI 在同一 exact revision 上同时通过。其可声明
16 个 attach 共享一个 session-owned refresh、90–120 秒 upload/retry 中失去 Host-local serviceability 的
checkpoint 不会被 Host 安装或交付（已经发出的 PUT 仍可能在 Cloud 完成）、idle checkpoint 不因 TTL 被错误
删除，且运行中的 Host 不会出现 pending body starvation/storm 或无界 retained-body 增长。它仍不证明 Cloud
blob 对任意 Browser 完整 usable，不提供 Host 崩溃后 generic `uncertain` 的 durable cleanup，不把同步 full
snapshot capture 伪装成可抢占的 actor hard deadline，也不关闭 Recovery v3、性能/SLO 或生产 rollout gate。
在 parent PR 合入 `main` 前，只能称 stacked candidate 完成，不能称主线 Phase 1 已完成。

### Phase 2：Recovery v3，继续使用 full snapshot

- 完成 protocol capability negotiation：authority data version 固定到 session epoch，recovery strategy 在
  新 delivery generation 选择三方兼容交集；同 generation 不混用 v2/v3 ordering；
- 实现 committed-through-H start fence、live floor、`RecoveryStart/Done/Adopted/SourceClosed`；
- 实现 Browser bounded assembler、exact duplicate、gap/deadline/reset；
- 分离 `DeliveryReceived` credit 与 `ReplicaApplied`；
- 建立 live/recovery logical lane 和有界公平调度；
- 端到端 gate 通过后，一次性删除 v3 路径的 global pause、fixed-commit barrier 和 pin；保留有序
  `RecoveryStartFence`，v2 fallback 不变。

Gate：snapshot 下载/restore 时 `(R,H]` 与 `H+1...` 任意交错仍只采用连续前缀；
ACK/Done/Adopted/SourceClosed 丢失可恢复；一个慢客户端不阻塞 authority、writer 或其他客户端；full
snapshot 路径通过 model/fuzz/E2E。

### Phase 3：严格有界 immutable cut

- 实现并验证 COW、bounded copy/off-actor 或 incremental/yield 方案之一；
- cut/cursor 原子，encode 后续不占用可变 authority ownership；
- capture pause hard max 可执行，失败只报告 unavailable；
- 建立 libghostty/wterm fixtures 和 actor input/output 并发测试。

Gate：`capture_actor_pause_p99` 与 hard max 满足本地 input SLO；同步大 snapshot 不能绕过 timeout 卡住
actor；此 gate 未过不得启用 rolling hard deadline。

### Phase 4：Rolling snapshot 与 snapshot-aware planner

- quiet 作为机会，hard deadline 作为受控 freshness 保证；
- 根据动态 tail cost、restore cost、dirty state 和 attach pressure 规划；
- 新 checkpoint 只 rebase 尚未 start 的 attempt；已 start 必须换 generation；
- 保持 single-flight、最小间隔、失败 backoff 和 R2 retention/pin hard cap。

Gate：持续 50–100 ms output、从不 quiet 时 cut 仍有界前进，且 input/control SLO 不回归；旧 tail 只在
新 snapshot 吸收后消失；无 snapshot storm。

### Phase 5：Execution checkpoint / history pages v2（独立后置）

- 为 execution READY 与 history pages 建立独立 schema、integrity、ownership 和 cancellation；
- execution + exact tail 达到 adoption gate 后，允许 history 后台 hydrate；
- page dedupe、retention 和 scrollback merge 不影响 authority cursor；
- 先独立 benchmark，再决定是否默认启用。

这不是 Recovery v3、rolling snapshot 或输入 Mirrored 的前置条件。

## 验证矩阵与上线 gate

以下矩阵服务于后续 TARGET recovery、完整上线与项目完成定义；除
[Phase 0 验收契约](phase-0-acceptance-contract.md)明确列出的项目外，不是 Phase 0 的阻塞条件。

### 正确性与状态机

- 随机选择 `R/H/K`，验证 `snapshot@R + continuous log(R,K]` 与 uninterrupted authority 的可见
  screen、cursor、modes、offset 和后续 parser 行为一致；
- 覆盖 UTF-8、CSI/OSC/DCS continuation、primary/alternate、resize/reflow、sync output 和 scrollback；
- live/recovery 任意乱序、重复、Done-before-frame、Start-after-live、Adopted/ACK/SourceClosed loss；
- duplicate payload 一致时幂等，不一致、gap、错误 epoch/engine/generation/offset 一律 fail closed；
- v2 pause/barrier 和 v3 gap-fill 分别模型测试，不允许半 v2/半 v3 状态。

### 生命周期、容量与多客户端

- single-flight、16 waiters/observers、supersede、relay reconnect、DO hibernation、cursor-ahead；
- encode/compress/upload 失败、response loss、R2 404/checksum mismatch、pending body 失去 serviceability；
- Browser download/restore timeout、assembler cap、generation bump、candidate/history abort、dispose once；
- recovery credit exhaustion、fair scheduling、慢 observer 隔离和 writer/live reservation；
- 30 分钟 soak 验证 Host RSS/CPU、Browser heap/WASM、R2 rows/bytes 和 ownership 无持续增长。

### 性能与 SLO

核心指标包括：

- `snapshot_capture_actor_pause_ms`、encode/compress/publish/download/restore；
- `checkpoint_valid/usable/fresh`、range bytes/frames/age、pending abandon/supersede；
- recovery start/done/adopt、assembler bytes/frames/gap span、logical lane credit/HOL；
- `delivery_received_ordinal` 与 `replica_applied_event_seq` lag；
- `input_ack_ms - measured_transport_rtt_ms`、Cloud/Host local queue、Ctrl-C 到 PTY write；
- time-to-first-visible、time-to-current、fresh attach 与 same-page resync 分布。

初始 gate：

- 支持 envelope 内 cold recovery 成功率至少 99.9%，无错误 adoption、tail hole 或跨 generation 拼接；
- input ACK 上限写成 `measured path RTT + local processing budget`；Cloud/Host 本地 processing 单独满足
  输入计划 SLO，恢复不能用 RTT 掩盖本地排队；
- output 持续但吞吐可收敛时无需 quiet 也能恢复，overload 时资源有界并明确 reset；
- 无 recovery 时 input latency、canonical throughput 和 Host CPU 相比 v2 回归不超过 5%；
- 三条物理连接只有在 logical-lane HOL 指标越过批准阈值后才进入独立 ADR/实现。

## 与 Mosh 的边界

两者都利用“新状态可以吸收旧输出”的思想，但语义不同：

```text
Mosh:       同步最新可见 screen state，可以跳过中间 presentation frame
Zhongduan:  immutable checkpoint 吸收旧 mutation，之后仍 apply exact required mutations
```

本计划优化 browser refresh、fresh attach、多 observer、writer fencing 和现代 Ghostty TUI 的恢复，不实现
UDP roaming、Mosh predictive echo 或任意 presentation-frame 丢弃。输入预测属于独立 presentation
branch，不能放宽 recovery invariant。因此不宣称全面超过 Mosh；目标是在 Zhongduan 场景中获得更可控的
高性能稳定体验。

## 非目标

- 任意丢弃尚未被采用 snapshot 吸收的 terminal mutation；
- 把 writer-only validation/context metadata 塞进 canonical journal；
- 为 Recovery v3 强制 mutation hash chain 或三条物理 WebSocket；
- 无限 transcript、daemon 崩溃后复活 Unix child/PTY、跨 `engineId` snapshot 迁移；
- 在 execution/history v2 完成前把 READY-first 描述成现有 feature flag；
- 没有数据和 schema 设计前重写全部 Ghostty snapshot。

## 完成定义

- Phase 0–4 依赖顺序通过并默认启用，v2 fallback 有明确退役条件；
- README/总纲、wire protocol、input 与本计划使用同一 invariant、cursor 和 CURRENT/TARGET 术语；
- model、fuzz、E2E、故障注入、多客户端公平性和 soak gate 全部通过；
- dashboard 能把恢复慢定位到 cut、encode、publish、download、gap-fill、assembler、apply 或 network；
- rollback 只影响新 generation，已开始 attempt 完成或明确 reset，不跨 generation 拼接；
- 同环境 Mosh/SSH 基准只陈述观测结果，不用架构推断宣称“全面超过”。
