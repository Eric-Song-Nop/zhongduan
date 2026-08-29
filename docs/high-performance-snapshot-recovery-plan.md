# 高性能远程终端 Snapshot 与 Recovery v3 实施计划

> 状态：根据 PR review 重写，分阶段实施中
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
[WASM `encode_snapshot`](https://github.com/Eric-Song-Nop/wterm/blob/bd7e930f41165f91b2b4863bc4d1b91db7b1c8a1/packages/%40wterm/ghostty/zig/src/wasm_api.zig#L560-L581)；
passive restore 的 READY/history/FINISH ownership 见同一 commit 的
[restore contract](https://github.com/Eric-Song-Nop/wterm/blob/bd7e930f41165f91b2b4863bc4d1b91db7b1c8a1/packages/%40wterm/ghostty/README.md#passive-snapshot-restore)。
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

### valid、usable 与 fresh 是三个概念

| 属性     | 含义                                                                        | 过期/失败动作                         |
| -------- | --------------------------------------------------------------------------- | ------------------------------------- |
| `valid`  | schema、checksum、engine、epoch、cut 和 immutable body 自洽                 | 标记损坏，不得再采用                  |
| `usable` | 针对目标 `H`，blob 可取得且 journal 精确覆盖 `(R,H]`，bytes/frames 在预算内 | 换更近 cut、reset 或报告 unavailable  |
| `fresh`  | age、tail cost、dirty state 达到策略目标，值得后台刷新                      | 触发 refresh，不删除仍 usable 的 body |

当前 cache 的 30 秒 TTL 在 TARGET 中只作为 freshness/refresh 信号。Idle session 状态未变化时，一个超过
30 秒但 valid 且 tail 为空的 checkpoint 仍然 usable，不能仅按 wall clock 删除。

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
  | reset-generation
  | drop-client

SchedulerAttempt =
  committed/adopted
  | superseded
  | retrying
  | reset
  | failed
```

原因至少区分 `generation-fenced`、`client-gone`、`missing-live-seed`、`snapshot-missing`、
`snapshot-metadata-mismatch`、`browser-control-send-failed`、`cloud-head-behind-cut`。例如 Browser control
发送失败只能 reset/drop 当前客户端，不能全局 invalidate 健康 checkpoint；snapshot missing 才进入
refresh-checkpoint；tail-unavailable 是 Host range gap/budget 检查派生的 planner reason，不伪装成基础设施
retry。

只有协议状态机明确证明 marker/start 未产生不确定结果时，才允许 same-generation retry。否则 fence
generation，避免跨 attempt 拼接。

## 实施阶段与依赖

### Phase 0：修 CURRENT 活性并建立事实

实施进度（2026-08-29）：

- `stability/recovery-v2-liveness` 已完成 non-ready retry/reset 活性、attach-start watchdog、滚动部署兼容和
  持续输出下的 bounded retry；
- `observability/recovery-serviceability` 是 Phase 0a：只建立严格 telemetry 基础、Host snapshot
  capture/publish timing，以及与真实 replay 绑定到同一 retained view 的 journal range 事实；这些事实不参与
  recovery 选择，不改变 v2 wire、barrier、TTL 或 admission；
- `observability/terminal-latency-pipeline` 是后续 Host 事实切片：记录 control queue、session actor、authority
  encode、PTY call 与 Host-observed control/data relay socket RTT；actor 只返回数字，Relay 在 ACK send attempt
  后才向共享有界 buffer enqueue，schema/JSON/collector 不进入 actor 或 canonical-pause 临界区；
- `observability/cloud-relay-latency` 是 Cloud Phase 0b：记录现有全局 relay queue、Browser input 的当前 lease
  verify/renew 与 Host send decision、barrier、attach 和 delivery reset；它只产生 Workers I/O-clock 可见的本地
  lower-bound，不改 wire、SQLite、socket attachment 或恢复状态机；
- `observability/cloud-delivery-facts` 是 Cloud Phase 0c：在同一个 `TerminalSessionDO` authority owner 上记录 Host
  ACK 到 Browser control `send()` return/failure、每个 Host canonical/directed data frame 的聚合 fanout/credit/reset
  decision，以及 writer attach acquire 和 heartbeat verify-renew；事件升级为 `workers-logs-v2`，但不改变 delivery、
  lease、reset、wire、SQLite 或 socket attachment 行为；
- `observability/browser-recovery-latency` 是 Browser 本机事实切片：用独立 monotonic clock 记录 control/data
  socket RTT、当时的 input send-return 到 ACK、attach matching/timeout/cancelled 终点，以及 snapshot load-total、
  restore、buffer apply、adopt call 与最终 recovery outcome。旧 `browser.input.ack` schema 仍供历史数据读取，
  但当前 producer 已由后续 v2 lifecycle 取代；事件只进入 256 条有界内存 ring，不写 console、storage 或网络；
- `observability/cloud-telemetry-query` 是 Cloud 查询契约切片：严格解析 Workers Logs flattened wrapper 与 v1/v2
  payload、固定每个版本的 producer sampling policy、执行前核验 exact key/type，并提供 Cloud-local report/smoke
  与显式 saved-query provisioning；它不上传 Browser/Host 事实，也不把 Cloud smoke 冒充性能 canary；
- `observability/browser-presentation-facts` 是 Browser 本机表示事实切片：它用精确
  `browserDiagnostics=off|memory-v2` 运行时模式，在 `memory-v2` 中记录 semantic
  dispatch→send decision→ACK，以及 canonical post-decode data callback→visible active replica exact
  apply→WTerm render commit→next-frame opportunity。后一条只包含完成 frame decode 后 callback 进入时已经
  处于 `live`/warm `replaying` 的 `PTY_OUTPUT` 与
  `RESIZE_APPLIED`，不把 `restoring` 的 detached/buffered work 当成 visible presentation；
- 新 Browser 序列在 outcome 之前分别做随机起始相位的固定无偏系统采样，
  `sampleWeight=64`，共享 pending hard cap 64 与 2 秒 deadline。它们只写严格、content-free 的
  Browser 内存 ring，不写 wire、journal/snapshot/replay、SQLite/attachment 或 export；`off` 不创建
  tracker/ring、telemetry-only clock/maps/listener/WTerm hook/presentation timer/RAF，但 heartbeat
  correctness 自身的 monotonic clock/deadline timer 仍保留；
- local synthetic pre-live input/recovery-liveness smoke 在 Browser 发起
  cold snapshot GET 后用测试控制点暂时 hold 请求，固定 pre-live 输入窗口；Playwright 通过真实 Chromium 的键盘事件
  路径发出 `Control+KeyC`，在 pre-live `attaching` 或 `restoring` phase 进入现有 input path，并在 synthetic PTY 产生恰好一次
  `0x03` effect；放行 snapshot 后最终进入 `live`，固定 probe/result 恰好一次，PTY capture 无重复 effect。两次
  输入由 PTY 精确字节和唯一 DOM result 直接证明；bounded retry/resync 被允许，只要最终 `live`；
- 结果只表示上述 input reachability 与 eventual liveness 路径通过，不提供 latency、throughput、rendering 或 SLO 证据。它只在单机
  loopback 和带 local-only multipart ETag 兼容层的 Miniflare 上运行一个固定场景、单次执行；不提供两条网络
  link 独立 RTT/jitter/loss，不证明 p95/p99、99.9% 成功率、`<=5%` 插桩开销、throughput 或资源上限。Synthetic
  child/result 不是通用 application effect，DOM result 不是 pixel paint/composite；该 smoke 不能替代
  model/property/fuzz、故障注入、多客户端、公平性/load/soak 或 production staging 验收，也不覆盖
  IME/Unicode/CapsLock/mouse/paste 输入矩阵。local-only ETag 兼容层不验证生产 Cloudflare DO/R2，timeout 只是防挂死预算。
  观察到 snapshot GET 不证明 snapshot 被 restore/adopt；允许 fallback 意味着它只证明 pending-recovery 输入可达与最终活性；
- 后续需要独立设计状态模型/property/fuzz、故障注入、多客户端/writer transfer/output flood/load/soak、生产
  staging DO/R2 和两条网络 profile，以及 snapshot adoption/tail continuity、ACK identity/status/dedup；性能验证
  需要另行设计受控且可复现的测试，本层暂不实现。这些是可能需要的独立验证，不是当前 smoke 的完成条件。

- 保留 v2 pause/barrier/pinned commit correctness invariant；
- 落地 `reason + retryScope`，修复 non-ready 被错误 complete 的 Browser freeze；
- 增加 attach-start watchdog、明确 retry/reset/unavailable；
- 测量同步 `encodeSnapshot()` actor pause、journal/serviceability、各队列和分段 recovery latency；
- 将 input/interrupt SLO 拆成 transport RTT 与本地排队/处理，禁止用 600 ms RTT 场景要求固定 500 ms
  端到端 ACK。

Gate：所有 rejected 路径最终 retry/live/reset；无静默 catching-up；dashboard 能区分网络 RTT、Cloud、
Host queue、PTY write 和 app effect。

### Phase 1：Checkpoint manager 与动态 serviceability

- 抽取 session-owned `SnapshotCheckpointManager`；
- 实现 valid/usable/fresh、动态 range bytes/frames/gap 计算；
- pending body 只在仍 serviceable 或 bounded cursor-ahead 时 resume，否则 abandon/supersede；
- 多 waiter single-flight，共享 build，30 秒只触发 freshness refresh；
- 保留 v2 delivery，不同时修改 recovery ordering。

Gate：16 个 attach 只 build 一次；90–120 秒 upload/retry 不会发布出生即不可服务的 checkpoint；idle
checkpoint 不因 TTL 被错误删除；无 pending body starvation/storm/leak。

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

Cloud Phase 0b 补 relay queue admission/wait/depth/capacity/completion、Browser input 中现有 lease verify/renew
outcome 与 Host send decision、recovery barrier，以及 attach/delivery-reset transition 本地事实。Phase 0c 在现有
authority path 上再补三类事实：`cloud.input.ack-forward` 量 Host ACK ingress 到 Browser control target/
`send()` decision；`cloud.data.fanout` 为每个 Host canonical/directed data frame 产生一条聚合 event，完整计数
selected、send-returned、stale、sequence-error、credit-reset 和 send-uncertain-reset targets，并记录最大 credit
utilization bucket；`cloud.writer.lease` 分开记录 attach acquire 与 heartbeat verify-renew 的 current/stale/
inactive/unavailable/uncertain outcome。Fanout reset count 只表示本地 reset decision/request，既有 reset transition
记录 issuance 与 Host notify decision，两者都不证明 reset 完成或 Browser 收到。

Queue 从本地 admission 起算，input、Host ACK、Host data、attach 和 barrier 从 JavaScript message callback
admission 起算；reset 从 `resetBrowserDelivery()` 操作入口起算，因为 socket close 等非 message callback 也能触发
它。Workers runtime clock 只在 I/O 边界推进，同步 CPU 工作可能不可见。量到 decision/handling 或
`WebSocket.send()` 返回的字段只是 Cloud local lower-bound，不包含各自起点前的 edge/唤醒/调度，也不证明
Host/Browser 已收到。它们不能标成网络 RTT、端到端 latency 或 CPU time，更不能与 Browser/Host 的 monotonic
timestamp 直接相减。

Cloud 自定义 event payload 不记录 terminal/input/frame payload、text/paste/command/cell/key、ticket/capability/
writer lease、session/client/connection/stream/generation/epoch/sequence、journal/snapshot ID 或 cursor、URL、异常或
error string，以及上述敏感值的 hash/digest；大小只使用 bucket。自动 invocation logs 和 tracing 保持关闭，避免
query 中的 WebSocket ticket 和 path 中的 session ID 被自动 URL telemetry 留存。该保证只覆盖 Zhongduan
payload，Cloudflare 平台 envelope 仍可能带 request/invocation/Durable Object 等平台标识。

`workers-logs-v2` 的所有 Cloud event 都带 `sampleWeight`；ACK 和常见 heartbeat outcome 会被系统采样；canonical
frame 在 Browser loop 前以 weight 64 固定选择，未选中时不安装 per-Browser observer，选中的正常或异常 outcome
使用同一权重。Weight 1 事实仍可能因有界 buffer 或 runtime failure 丢失。每个 DO 的 pending event、drop 状态和
sampling counter/phase 只驻内存，不写 SQLite 或 WebSocket attachment，可在 capacity、版本切换、eviction/
hibernation 时丢失或重置，且不 backfill/replay。生产查询在 rollout/rollback 窗口必须同时接受 schema v1/v2；
代码与精确 `CLOUD_TELEMETRY_MODE` 不匹配会关闭自定义出口并形成 blind window，因此必须成对发布并先验证 v2
到达再退休 v1 query。原始 event count 不是完整流量计数，聚合必须使用 `sampleWeight`。Browser 本机事实使用
`performance.now()`，其中 snapshot `load-total` 包含 HTTP、完整性校验、解压和 worker transfer，`adopt
call-returned` 也只表示本地调用返回；socket RTT 不等于纯网络 RTT，input ACK 不等于应用
effect。新 `browser.input.lifecycle` 补上 semantic dispatch 与 send decision 分段；新
`browser.presentation.canonical` 只选择完成 frame decode 后 callback 进入时已经 `live`/warm `replaying` 的帧，并在
active replica 的 canonical cursor 精确推进后串起 WTerm render
commit 与 next-frame opportunity。Render commit 只表示同步 DOM/scroll/title/response 提交，RAF callback
opportunity 也不证明 pixel 已 paint/composite。

新 Browser 事实为 schema v2，固定 `sampleWeight=64`，在 outcome 前采样，随机 phase 失败时丢弃
该序列；正常与 terminal outcome 使用同一权重。Tracker 对 input/canonical 合计最多保留 64 个
pending probe，每个 2 秒 deadline；输出只包含枚举、monotonic duration、计数和 frame-size bucket。
任何 Browser 事件都不包含配对所用的 seq/epoch/generation、cursor，也不包含 input/frame/cell/key
内容、URL、error string 或这些值的 hash/digest。`memory-v2` ring 只驻内存且有硬上限，不进入
terminal snapshot、storage、console 或网络；`off` 路径连 ring/tracker 和 telemetry-only 调度资源都不创建，
但不移除 WebSocket heartbeat 的 correctness clock/timer。

Cloud 查询契约固定平台 service/log-type 与 Zhongduan wrapper filter，并在运行 query 前用 keys API 验证 exact
direct key/type。Producer volume 使用 `SUM(sampleWeight)`；raw row count 只表示 stored/query rows。Percentile 只允许
在一个固定 producer weight 的子群内计算，不能混合不同采样率的 success/failure。Cloudflare 返回的
`sampleInterval` 与 `abr_level` 是 query quality signal；任一不为 1 时结果标为 approximate，不通过 hard gate，
也不盲目二次乘 `sampleInterval`。Saved query 是 account-level state，只有显式 apply 才创建；同名漂移 fail closed，
不自动覆盖或删除。真实 staging source/key shape 与非 approximate aggregate 仍必须在部署时验证。

Phase 0c 没有移除每次 semantic input 上的 writer token SHA-256 和 SQLite lease renewal，Phase A correctness/hot
path 仍未完成。Cloud-local query contract 也不等于跨 runtime dashboard。本地 pre-live input/recovery-liveness
smoke 可按部署文档在隔离 venv 中安装 pinned Python requirements/Chromium 后用
`pnpm exec vp run verify-browser-recovery-smoke` 重现，安全输出只含固定 label 与 count。它只覆盖单机
loopback+Miniflare 下一个固定场景、单次运行；snapshot GET hold 只是测试控制点，观察到 GET 不证明 snapshot
restore/adopt。该结果只证明 pending-recovery 输入可达与最终活性，不提供 latency、throughput、rendering、SLO、
通用 app effect 或 production DO/R2 证据。后续可能需要状态模型/property/fuzz、故障注入、多客户端/load/soak、
生产 staging、独立网络 profile、snapshot adoption/tail continuity 和 ACK identity/status/dedup 测试；性能验证
需要另行设计受控且可复现的测试，本层不实现。这些观测切片也不改 wire、journal、snapshot、replay、SQLite
schema 或 WebSocket attachment。
普通 directed recovery fanout 当前仍为 weight 1；先用本层事实量出实际体量，再在后续性能/简化 PR 对
completed/stale/not-targeted outcome 做无偏 producer sampling，异常恢复转移继续保留，不在本 PR 猜测调参。

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
