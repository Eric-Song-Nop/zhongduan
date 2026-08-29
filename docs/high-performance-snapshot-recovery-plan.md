# 高性能远程终端 Snapshot 与 Recovery v3 实施计划

> 状态：Phase 0/1 为 CURRENT stacked candidates；Phase 2 的 P2.0–P2.5b（wire/capability、
> Browser/Host/Cloud owner、capability-gated runtime wiring、no-payload delivery ledger/cold safety，以及
> Host recovery source 的 multi-outstanding/DRR scheduling）
> 为 stacked candidates。默认生产 kill switch 与 Host/Browser capability offer 仍选择 v2，尚未 rollout；
> P2.5c、P2.6 与 Phase 3–5 为 TARGET
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

Recovery lane 只覆盖 `(R,H]`，live lane 只覆盖 `[H+1,...)`；同一个 `eventSeq` 出现在两个 lane 表示
start/floor 已经错误，必须 reset。仅同 lane、同 delivery ordinal 的 retry 可以按 canonical 字段逐项比较
`{sessionEpoch, eventSeq, kind, ptyOffset, semantic flags, payload}`；完全相同才幂等，不同则 fail closed。
Mutation hash chain 可以作为后续诊断和快速校验优化，但不是 Recovery v3 的正确性前置条件。

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

Recovery v3 对 warm 和 cold 使用同一套协议，只改变 base 的来源。精确 binary/control shape 与滚动
协商见 [Recovery v3 Wire Contract](recovery-v3-wire.md)：

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
   `liveFloor = MutationBoundary{H.eventSeq+1,H.nextPtyOffset}` 的 delivery obligation；
4. Cloud 向 Browser 发 `RecoveryStart`，并向 Host ACK 初始 bounded recovery grant；Host 只在累计 grant
   范围内发送 prepared gap；此后 `H+1...` 持续 fanout 给该客户端和所有 synced 客户端，Browser 允许在
   start 前有界缓存同 generation live frame；
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
  source: { kind: warm } | { kind: snapshot, complete immutable manifest },
  committedThrough: AuthorityCursor H,
  liveFloor: MutationBoundary(H)
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
  throughRecoveryOrdinal,
  throughRecoveryCumulativeEncodedBytes
}
```

`RecoveryStart` 一旦可见，同 generation 的
`{recoveryId, engineId, base R, source, committedThrough H, liveFloor}` 不可更换；cold source 包含
session/snapshot id、engine/epoch/cut、compression、长度、checksum、与两个 id 精确绑定的 download path
和 restore-through；rebase 必须增加 generation。`H` 之后的 head 可以持续前进。

### Browser RecoveryAssembler 与 runtime owner

P2.1 stacked candidate 冻结 `packages/session-client` 内的 pure、generation-scoped assembler；P2.4
由 `RecoveryRuntime` 和 Browser `TerminalSession` 把它接到显式协商的 v3 generation。默认生产
Browser offer 仍只包含 v2 baseline，所以这是 capability-gated wiring，不是已 rollout 的默认路径。
每个 generation 的 assembler 纯状态为：

```text
base R
nextExpected = R + 1
framesByEventSeq = bounded map
appliedIdentityByEventSeq = bounded same-lane late-retry cache
recoveryDoneThrough = optional H
bufferedBytes / bufferedFrames / startedAt / lastContinuousProgressAt
targetCore = caller 提供的 existing replica@R（warm）或 detached replica@R（cold）
```

P2.1 保留的 pure 行为是：

- start 前同 generation envelope 先复制进有硬 bytes/frames 上限的 pre-start buffer；完整且 immutable 的
  start 到达后再校验，matching start 幂等，divergent start fail closed；start 未验证前不产生 receipt progress；
- recovery lane 只接受 `(R,H]`，live lane 只接受 `liveFloor = MutationBoundary(H)` 之后的 mutation；同 lane
  ordinal 与 cumulative encoded bytes 必须连续，同 ordinal retry 必须逐字节一致；
- 每条 mutation 验证 generation、stream、engine、epoch、kind、event sequence、PTY offset、semantic flags 和
  payload exact identity；新 ordinal 重复 canonical event 或跨 lane 同 event sequence 均 fail closed；
- 只有 `nextExpected` 存在时才按小 quantum apply 连续 authority 前缀。cold target 尚未由 caller 安装时可以
  receipt 已拥有的 bounded copy，但不推进 applied cursor；cold install 必须匹配 recovery attempt、base 与
  engine，warm target 必须精确位于 base `R`。candidate 是否确由 start 中的 snapshot manifest restore 而来，
  不属于 P2.1 pure owner 的证明边界；P2.4 runtime 会把 exact start manifest 交给现有
  snapshot transport，真实 WTerm/Ghostty continuation oracle 仍属于 P2.6；
- `RecoveryDone(H)` 必须是 recovery lane 的最后一条 record，并拥有稳定 ordinal/bytes。Done 可以早于
  snapshot restore 或连续 apply 完成到达，但不能越过缺失的较低 recovery ordinal；只有 Done 已验证且
  applied cursor 至少为 `H` 时 handoff 才 eligible；
- assembler 只报告 eligible handoff；caller 完成真实 warm handoff 或 cold adopt 后，必须回传 matching
  confirmation，assembler 才产生稳定、可幂等重发的 `RecoveryAdopted(K)` progress，其中 `K >= H`；
- confirmation 只把 cold core 的 dispose/visibility ownership 交给长期 replica host；在 attempt completion 前，
  assembler 仍是这个 core 唯一的 mutation ordering writer，并继续排空已经 receipt 的 live prefix；
- matching `RecoverySourceClosed` 可先于或后于 adoption 到达；只有 closure certificate 被本地 recovery
  ordinal/bytes receipt 覆盖后才 release recovery late-retry identity cache。两者都成立且已经 receipt 的 live
  mutation 全部 apply 后才输出 completion authority/lane cursors，且只有此时 mutation delivery ownership 才交给
  长期 live receiver；
- identity conflict、hole/no-progress deadline、预算溢出或 target apply failure 都 fail closed。cold candidate
  只有在 install 返回 accepted 后才把 ownership 转给 assembler；返回 rejected 时仍由 caller 负责。转移后
  failure/reset/close 由 assembler dispose once，handoff confirmation 后只转移 dispose/visibility ownership；
  warm target 只有在已经尝试 write/resize 后发生 failure 时才被标记 tainted、不得作为下一 generation 的
  base；零 effect 的 start/admission conflict 仍可返回 reusable base。所有 deadline 使用 caller 提供的
  monotonic tick，不在 pure core 内启动 timer。

Applied identity cache 计入 per-generation bytes/frames；只保留仍可能被同 lane late retry 命中的区间。
pure assembler 暴露稳定 receipt/apply/adopt progress 供 caller 幂等发送，但自己不发送 socket control，也不
授予或回收 credit。caller 发送 `DeliveryReceived` 后仍须保留 recovery identity cache；只有收到 Host/source
在实际处理 receipt 后返回的 matching `RecoverySourceClosed`，且本地 recovery lane cursor 同时覆盖
`throughRecoveryOrdinal` 与 `throughRecoveryCumulativeEncodedBytes`，才可释放。Closure 丢失时幂等
请求/重发，最终走有界 deadline/reset；不能为了完整比较保留无界 session history。

live `H+1...` 可能先于 `RecoveryStart`、snapshot 或 recovery frame 到达，也可能在 adopt 前已经连续
apply 到 `K > H`；这不改变 adoption 条件。Assembler 始终采用同一 log 的连续前缀，不能因“画面看起来
更新”跳过缺口。P2.4 runtime 现在负责 exact manifest 的 HTTP snapshot restore、detached cold
candidate 的 adopt、warm target handoff、receipt/apply/adopt 三类 progress 的稳定重试、source closure
以及 completion 后长期 live receiver。它使用 caller 提供的 monotonic clock/deadline owner；若
adopt 或 live sink 的结果不可判定，则放弃该 target 作为下一代 warm base 并 fail closed。

### Host PreparedGap、source owner 与 relay runtime

P2.2 stacked candidate 只冻结 Host 内尚未接 production relay 的 bounded source primitives：

- `TerminalSession` 在同一个 actor turn 内固定 `H`，验证 `(R,H]` 的 exact range、encoded bytes 与 frame count，
  materialize 自有 copy，并同步执行 fence commit；失败不会暂停或终止 authority；
- `CanonicalPublisher` 把 strict `RecoveryStartFence(H)` 作为不推进 cursor 的 ordered marker，保证实际出队
  `H / fence(H) / H+1`，且不调用 v2 global pause；
- session-scoped source manager 在 commit 内预编码 `(R,H] + RecoveryDone(H)` 的唯一 retained envelope copy，
  只在累计 grant 内发送。send throw 不推进 cursor；matching retry 仍得到逐字节一致 record；
- 只有覆盖实际已发送 Done ordinal/cumulative bytes 的 receipt 才 release-once payload 并产生 stable
  `RecoverySourceClosed`。reset、deadline 和 generation replacement 释放 payload，但保留计入 source cap 的
  owner+stream generation tombstone，防止同代迟到 prepare 重新建立 source；
- per-source canonical cap、session aggregate envelope cap、source count、no-progress/absolute deadline 和
  owner-token fence 都是直接状态事实；manager 本身不拥有 WebSocket、timer 或公平 scheduling policy。

P2.4 已把这些 primitives 接入 strict Host v3 control union 与 `HostRelayConnection`：prepare 在
authority actor 内建立 source 并排序 `H / fence(H) / H+1`；matching start-ready/grant 驱动
bounded source drain；matching receipt 驱动后续 record 与 release-once closure；reset、pair fence、send
outcome uncertain 和显式 deadline 都收口到 generation owner。Cloud 可能在 prepare 尚未到达
Host 前就持久化 reset，所以 exact unknown-source reset 会安装有界 tombstone，阻止同代迟到
prepare 复活，而不会关闭健康 Host pair。P2.5a 的 Cloud recovery path 已能接受连续
obligation 并确认 exact sent prefix；当时 Host source emitter 每个 control turn 只 drain 一条，
尚未主动填满已提交 grant。P2.5b 再将 Host retained source 打开为有界 multi-outstanding，
并由 connection-local DRR 调度多 source；Cloud session aggregate/shared ring 与多 client/lane DRR 仍属于
P2.5c。

### Cloud durable scalar owner、hibernation runtime 与 shared alarm

P2.3 stacked candidate 建立 Cloud 侧 hibernation-safe 的 durable scalar truth；P2.4 把它接入
capability-gated v3 control/data runtime；P2.5a 再把 data send obligation 收进 no-payload scalar ledger：

- schema v7 在同一 migration transaction 中新增 STRICT `recovery_attempt`、`recovery_delivery_lane` 与
  `recovery_control_outbox`。attempt 保存 immutable prepare/start identity、base/committed/live floor、grant、
  Done、replica-applied、adopted/source-closed、deadline 与 reset state；每个 logical lane 分别保存 sent/received
  ordinal、cumulative encoded bytes 与对应 authority cursor；bounded outbox 只保存待发 control intent；
- schema v9 新增 STRICT `recovery_delivery_record`。它按 recovery/lane/ordinal 保存 cumulative encoded bytes、
  当前 record 的 encoded byte length、发送后的 authority cursor 与 `queued|sending|sent` 状态；复合
  lane foreign key 负责 cascade。表中没有 envelope payload、payload hash 或 shared-ring handle；decimal u64
  以 canonical text 保存并按长度、再按文本排序，不能转成 SQLite signed integer；
- `RelayRecoveryStore` 是同步、caller-owned transaction participant。prepare-before-outbox、exact start-fence
  install、lane receipt、apply、adopt、source-closed、completion 与 reset 都先成为 SQL scalar fact；outbox capacity
  不足时整个 caller transaction fail closed，而不是留下半个 generation transition；
- V2/V3 hibernation attachment 使用 strict versioned shape。V2 继续完整保存 legacy delivery state，并在内存中
  normalize 为 `recoveryStrategy=v2`、空 recovery lookup；V3 只保存 connection identity、capability、
  `recoveryStrategy=v3` 与可选 Browser recovery lookup key，Host lookup 必须为空，legacy delivery fields 不写入
  V3 attachment；未知 version/field/identity fail closed；
- Browser generation replacement/activation、Host fence 更新和 inactive client removal 在各自已有的
  `transactionSync` 内同步 fence 旧 recovery attempt。若 reset outbox 无容量，connection/generation/client
  mutation 与 recovery fence 一起回滚；事务完成后才请求 recovery alarm 与 snapshot cleanup，不在 SQL
  transaction 内做 socket、R2 或其他外部 I/O；
- cold attempt 引用的 snapshot id 与现有 V2 socket pin 做 union，作为 snapshot retention 的 durable pin；
  reset/complete 后不再把该 attempt 当 active pin。冷启动从 SQL 重建 deadline 与 pin，不依赖内存 attachment
  delivery state；
- `DurableAlarmMux` 是 `TerminalSessionDO` 唯一的 platform alarm writer。Snapshot 与 recovery 各自持久化
  component due fact，mux 总是设置两者最早 deadline；dispatch 前把 due 变成 durable active fact，handler 在
  transaction 外执行，成功后重算。stale early delivery 不执行 future handler；首次 initialize 用持久化
  `initialized` marker 一次性接管 pre-mux snapshot alarm，之后 empty state 的残留 alarm 不会再次被接管；
- handler failure 从 handler **完成时钟** 后安排 bounded retry，不从已过期 claim time 重排；recovery
  maintenance 若处理后仍只剩 past deadline，也安排短的 future retry，避免成功但无进展时 hot-loop。alarm
  at-least-once retry、eviction 后 active fact 重建以及 snapshot/recovery deadline 不互相覆盖均由同一 owner
  收口。

Cloud 刻意不持久化 delivery mutation envelope payload，也不持久化其 hash；SQL/outbox 中的 JSON 是 bounded
control identity/intent，不是 mutation holdback。因而 Cloud 看到已记录 ordinal 的 envelope retry 时，无法仅凭
scalar cursor 证明它与先前 bytes 完全一致，必须 fail closed；P2.5a runtime 会 reset/isolate
当前 client generation 并 replan，不能把“相同 ordinal”当幂等。逐字节 retry 证明仍由持有
exact copy/hash 的 Host source 与 Browser assembler 完成。

P2.5a 的 ledger 可以验证由多个 obligation 组成的连续链：新 record 以最后一个 obligation 为基线，不能只看
`lane.sent`；Browser 的累计 receipt 必须精确命中 `sent` record，且被覆盖的 prefix 全部为 `sent`，再从该
record 取得 authority cursor 并原子推进 received cursor。这个 Store contract 是 P2.5b 打开 Host
recovery window 的前置条件；它本身不包含 Host 侧公平调度，也不打开 live lane window。

P2.4 runtime 在这些 durable facts 之上增加：

- control outbox 只对 exact current Host/Browser socket identity 发送，发送在 SQL transaction 之外；
  ACK 必须对 exact destination、kind 和 payload JSON 做 CAS，stale socket 或不可判定的发送结果
  不能消费 intent。DO hibernation 后从 strict attachment 与 SQL outbox 重建路由并继续 drain；
- Host canonical queue 内的 `H / fence(H) / H+1` 在 Cloud 依次 commit；start fence 在处理
  `H+1` 前原子安装 attempt/start/live floor。Host 只能注入 recovery-lane envelope，live
  envelope 一律由 Cloud 从 canonical mutation 包装；Host 伪造 live lane 会 fence Host；
- recovery/start/progress/source-closed 全部绑定 exact recovery/client/connection/stream/generation 与
  Host fence。`DeliveryReceived`、`ReplicaApplied`、`RecoveryAdopted` 驱动对应 SQL
  transition 和 Host receipt/closure；complete 但仍拥有 live socket 的 generation 在 Host fence 时也会被关闭；
- writer activation 先同步发送带新 token 的 Welcome，再把 exact attempt identity 写入 socket
  attachment；激活 crash cut 可用同 client 新 fence/token 重试，semantic input、lease renew 和 Host
  input ACK 均只走 exact active v3 owner；
- P2.4 对每个 lane 使用 strict stop-and-wait，Cloud 不保存 mutation payload/hash。当前
  client 存在 outstanding envelope 时收到下一条 canonical mutation、same-ordinal replay 或 data send
  outcome uncertain，只 reset/isolate 该 client generation；authority、v2 客户端和其他 v3 客户端继续。

P2.5a 里程碑保留当时的 sender scheduling，但把一次 data send 的 durable 顺序固定为：在 caller-owned
`transactionSync` 内依次 enqueue `queued` obligation 并 CAS 到 `sending`，事务提交后向重新核对过的 exact
Browser data socket 调用同步 `send`，最后在新的 `transactionSync` 内把同一 record CAS 为 `sent` 并推进
`lane.sent`。`send` 发生在 SQL transaction 外；confirm 必须实际改变 exact record，否则结果按 uncertain
处理并只 fence 当前 generation。

`queued`/`sending` 只允许作为当前 DO turn 的短生命周期见证；Cloud 没有可在 hibernation 后恢复它们的
payload。fresh instance 先初始化 alarm mux，再在 snapshot/recovery maintenance 与 v3 outbox drain 之前做
bounded delivery-owner reconciliation：看到 `queued`/`sending` 就调用 durable unsafe-outcome reset 并隔离
exact socket generation；`sent` 在 exact control/data pair 仍存在时只等待 ledger receipt，绝不重发。已
installed/assembling/complete 的 attempt 若失去 exact pair，同样 fence。complete attempt 已无 live Host recovery
source，因此只删除其 delivery owner/control intent、变成 local terminal tombstone、释放 lease 并关闭 exact
Browser generation，不生成 post-closure Host reset。v8 迁移若发现旧 scalar `sent != received`，也按 outcome
uncertain fence，绝不能推测为已经发送。

当 `RECOVERY_V3_ENABLED` 不是 `"true"` 时，Cloud 在 v2 decode 或 outbox drain 前 fail closed 已持久化
v3 attachment/attempt；默认 Wrangler 配置为 `false`，Host `CloudApiClient` 与 Browser `TerminalSession`
的默认 capability offer 也仍是 v2 baseline。因此 P2.5a 证明的是 capability-gated runtime 加上
no-payload ledger/cold safety，不表示生产已启用或已 rollout Recovery v3。

### P2.5b Host recovery window 与 source scheduler

P2.5b 不改 Cloud/Browser/protocol，只放宽 Host recovery-lane emitter：

- `RecoverySourceManager` 只在 socket `send` 成功返回后推进 sent high-water；已 sent 但未 receipt
  的 record 不再重发。同一累计 grant 可连续发出多条 retained record，send throw 前不推进，
  direct retry 仍必须 byte-identical；
- intermediate exact receipt 按连续 prefix 释放 Host 持有的 record/bytes，但保留有界 cumulative
  identity 用于幂等 current duplicate 与倒退拒绝；只有覆盖 Done 的 final receipt 产生
  `RecoverySourceClosed`；
- 独立 `RecoverySourceScheduler` 只持有 exact identity、runnable queue 和 bounded deficit，不持有
  payload 或 source truth。它以有界 byte quantum 做 deterministic DRR，每个异步 data turn 最多发
  一条 record 后 yield，使 control/input/canonical live 先于下一个 recovery turn；
- shared data socket 达到 high-water 只使 recovery source blocked 并经 bounded yield 重试，不关闭
  Host pair、不推进 source cursor。真实 send throw 仍是 pair-level outcome-uncertain failure；
- no-progress/absolute deadline 只 retire exact source/tombstone 并清理 scheduler entry，不关闭健康
  Host pair。该 identity 的迟到 control 幂等忽略，其他 generation/identity 冲突仍 fail closed。

Host 的 per-source/session `256 KiB / 512 canonical frames`、`2 MiB / 1024 retained records`、
source-count 与 deadline 上限没有放宽。Cloud recovery lane 依靠 P2.5a ledger 可按顺序接收这个
window；live lane 仍是 stop-and-wait。

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

P2.5a 对单个 recovery ledger 使用与 Browser assembler 对齐的 `2 MiB / 1024 records` 上限，并暴露
`granted cumulative bytes - recovery received cumulative bytes` 的 reservation 查询。它不宣称 Cloud 已经实现
session aggregate reservation、multi-client allocator 或公平性；当前 Host source manager 仍以既有
`2 MiB / 1024 records` session retained-envelope cap 约束实际 payload owner；P2.5b 只在该上限内增加
Host source DRR。P2.5c 才把 immutable canonical
bytes 放入 session-scoped ephemeral shared ring，统一计算 recovery grant reservation 与 live outstanding，
并加入 writer/live reservation 和 Cloud 多 client/lane DRR。shared-ring payload/refcount 仍只存在于内存，不能写入 v9 ledger；
hibernation 后若只剩 `queued`/`sending` scalar witness，仍沿用 P2.5a 的 fail-closed fence。

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
  cumulativeEncodedBytes
}

ReplicaApplied {
  deliveryGeneration,
  authorityCursor
}
```

`DeliveryReceived` 只在完整 v3 envelope 已校验、payload 已复制进有界 assembler 后发送，用于释放对应
logical lane 的 transport credit；ordinal/cumulative bytes 必须单调且精确匹配已发送 lane cursor。
`ReplicaApplied` 表示 Ghostty 已连续 apply 的
authority cursor，用于正确性、进度和 adoption。两者不能互相推导。

### 逻辑 lane、物理连接与公平性

第一版建立 `control-input`、`live-data`、`recovery-data` 三个逻辑 lane，并为 live/control 保留 credit 和
scheduler priority；snapshot blob 继续走 HTTP。Cloud→Browser 的 live/recovery 先复用同一 v3 data
WebSocket，每条 record 由 generation-scoped envelope 显式标记 lane、ordinal 和 cumulative encoded bytes，
并通过有界 quantum 调度。只有以后经批准的环境方法证明 TCP/WebSocket HOL 仍显著，才讨论第三条物理
WebSocket 或未来 QUIC/WebTransport stream；这不是当前正确性前置条件，也不是本阶段性能 gate。

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

CURRENT snapshot refresh 归 session-owned manager，而不是单个 attach request 或 relay connection。
Manager 持有：

```text
latestValid           最近一个通过完整性和 lineage 校验的 immutable checkpoint
installedAt           当前 checkpoint 的 age-only freshness 起点
refreshInFlight       至多一个 capture/compress/publish pipeline
waiters               当前等待 checkpoint 的 attach requests
replacementMinimumCut exact invalidation 或 pending supersede 合并出的最低 cut
```

`latestValid` 一直保留到 validated forward replacement、exact checkpoint invalidation 或 manager dispose；
时间流逝和读取都不会删除、隐藏或更新它。Manager 只安装同 session、同 engine、同 epoch、cut 单调不回退的
checkpoint。相同 immutable
identity 的迟到结果必须完全一致；旧 cut、同 cut 的未授权 replacement 和已 invalidated identity 都不能重新
安装。Exact invalidation 可以提供 inclusive replacement floor，并清除所有 scheduler preparation 中指向该
checkpoint 的引用。

每个需要 refresh、无法直接复用 latest 的 cold attach 注册一个 waiter；多个 waiter 共享同一 capture →
compress/publish → install flight。Waiter signal 只拥有该 waiter；generation supersede 或 relay connection
关闭不会取消公共 flight。只有 manager
dispose 或 manager-owned capture/publish deadline 可以终止公共 flight 并 fence 迟到 completion。若已有 flight
在 `R` 完成，而 live waiter 要求最低 `H>R`，`R` 仍可满足允许它的旧 waiter，manager 合并压力并至多启动
一个 follow-up。所有 waiter 已离开时，当前 flight 可以安装有用 checkpoint，但不能无 owner 地启动
follow-up。

`snapshotId` 与 body 仍是 immutable identity：结果不确定的 upload retry 必须继续使用同 ID、同 bytes，
不能以新 body 覆盖。所谓 supersede 只表示 planner 不再把旧 cut 分配给新 recovery；旧 upload 只有在 Cloud
ledger 到达 cleanup-safe terminal state，或已转交给独立有界 cleanup owner 后，才能释放本地 body 并允许
新 ID。迟到 abort/delete 也不得命中 replacement。

CURRENT manager 的 `latestValid` 只表示 publisher 已确认、metadata 与固定 session/engine/epoch/cut identity
自洽。`isAgeFresh(checkpoint)` 只对 exact current identity 给出 30 秒 age classification；读取或复用不会续期，
`false` 也不会删除或隐藏 checkpoint。它不表示 Cloud blob 此刻可取得，也不表示 checkpoint 对某个目标 `H`
可服务。结合 tail cost、dirty state 或 attach pressure 的 background refresh policy 尚未实现；未来策略只能触发
refresh，不能让仍可服务的 checkpoint 因时间流逝自动失效。

### CURRENT serviceability 动态检查

CURRENT Host 对 canonical pause 后固定的 `{R,H}` 先规划 journal range，再决定是否复制 frames：

```text
planReplayThrough(R,H) -> {
  status: ok | gap,
  exactEncodedBytes,
  exactFrames,
  materializeSameRevision()
}
```

只有 range 无 gap、同 revision materialization 保持相同 facts，且现有 256 KiB delivery credit / 512-frame
envelope 通过时，CURRENT cold path 才可发送 barrier、directed tail 和 commit。`exactEncodedBytes` 是 stored
canonical frame 长度之和；256 KiB 准入仍按 PTY payload bytes 加每 event 64 bytes 计算，两者不能混称。
Gap、预算溢出、revision change 或 materialized facts mismatch 都会 exact-invalidate checkpoint，且在 recapture
前不得发送 barrier、截断 tail 或 commit。

Pending body 在 build 后、每次 PUT/retry/resume 前以及 exact upload success 安装前，都以最新
`H=session.cursor` 重做上述 Host-local 检查。失去 serviceability 的 body 不会被 Host 安装或交付；已经发出的
PUT 仍可能在 Cloud 完成。每个 session 最多保留一个 active/build body 加一个 ambiguity-cleanup body；cleanup
slot 被占用时不得构造第三个 immutable body。

`cursor-ahead` 表示 snapshot cut 已在 Host 产生，但 Cloud committed head 尚未到达该 cut；可以有界等待
Cloud 追上并重试同一 body。Cloud 只有在 completed row 与 R2 object 已精确验证、唯一失败是 committed head
落后于 cut 时，才返回 `snapshot-cursor-ahead`。该 identity 的 120 秒 deadline 不因 attach、reconnect、caller
timeout、backoff 或相同响应续期。Generic conflict、transport/5xx 和 unknown 结果继续 fail closed，不能冒充
cursor-ahead 或释放 immutable body。

这些检查仍不构成完整 `usable` policy：CURRENT 没有证明 blob 对任意 Browser 可取得，也没有 mutation age、
estimated delivery、Browser restore/apply budget 或 freshness planner。若未来实现 journal pin，pin 同样必须有
独立 bytes/frames/age hard cap。

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

| 层级/用途                      | CURRENT 值                                                               |
| ------------------------------ | ------------------------------------------------------------------------ |
| Journal segment / retention    | 256 KiB 或 250 ms；60 s / 8 MiB                                          |
| Host warm/cold tail admission  | 256 KiB / 512 frames                                                     |
| Relay delivery outstanding     | 512 KiB                                                                  |
| Host v2 paused canonical queue | 8 MiB / 1024 frames                                                      |
| Browser recovery tail          | 2 MiB / 1024 frames；5 s snapshot load + restore deadline                |
| Checkpoint age / build cadence | 30 s age classification；1 s minimum build interval                      |
| Snapshot blob                  | 32 MiB compressed / 128 MiB uncompressed                                 |
| Cloud retention                | 32 snapshot/upload rows；4 reservations；latest + active pins + 2 recent |

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

CURRENT v2 已使用带原因和作用域的 closed barrier outcome；只有协商了
`delivery-barrier-outcome-v1` 的 Host 收到 enriched shape，旧 Host 继续收到 legacy status。Scheduler 对每个
non-ready outcome 都有明确 owner 和收敛动作，不会把 rejection 当作成功完成。TARGET v3 的
`RecoveryStart` 必须保留同样的 producer/owner 约束：

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

## 当前阶段状态与依赖

### Phase 0：CURRENT recovery 活性

Phase 0 stacked candidate 已满足 [Phase 0 验收契约](phase-0-acceptance-contract.md)的 P0.1–P0.4：v2
pause/barrier/pinned commit invariant 保持不变；enriched non-ready outcome 有 closed producer/owner 集；warm
rejection、checkpoint refresh 和 isolated-client drop 都会收敛；attach-start 与 warm-completion watchdog 防止
Browser 永久停在 catching-up。它尚未合入 `main`，因此不能称为主线完成。

### Phase 1：CURRENT checkpoint ownership 与 Host-local serviceability

Phase 1 stacked candidate 的最终行为是：

- session-owned manager 跨 attach 和 relay connection 复用一个 immutable latest checkpoint；它保留到
  validated forward replacement、exact invalidation 或 dispose；30 秒 age classification 不驱逐或续期它；
- canonical pause 后先规划固定 `{R,H}` 的 gap、encoded bytes 与 frame count，通过现有 envelope 后才从同一
  journal revision materialize；
- pending body 在 publish/install 生命周期的每个可变边界重查最新 Host-local serviceability，运行中的 Host
  最多持有一个 active/build body 和一个 cleanup body；
- exact `snapshot-cursor-ahead` 使用不续期的 identity deadline，generic ambiguity 继续 fail closed；
- 多个 waiter 共享 manager-owned refresh flight，单 waiter 或旧 connection 离开不取消公共工作；inclusive
  minimum cut 压力至多合并出一个有 live owner 的 follow-up。

这些能力和完整 CI 已在同一 stacked revision 上通过；在 parent stack 合入 `main` 前仍不能称为主线 Phase 1
完成。

### 保留的直接测试

- `apps/host-daemon/src/cloud/delivery-barrier-waiter.test.ts`、`delivery-recovery-queue.test.ts` 与
  `delivery-scheduler.test.ts` 检查 v2 marker identity、non-ready 收敛、fixed ordering、range admission、共享
  checkpoint invalidation 和 recovery generation fence；
- `apps/terminal-cloud/src/browser/terminal-session.test.ts` 检查 attach start、warm commit、snapshot
  restore/candidate deadline owner 和 matching generation transition；
- `apps/terminal-cloud/test/relay.test.ts`、`runtime.test.ts` 与 protocol schema tests 检查 exact barrier outcome、
  capability negotiation、hibernation 和 legacy/enriched rolling compatibility；
- `apps/host-daemon/src/cloud/snapshot-checkpoint-manager.test.ts` 检查 age classification 不驱逐或续期、
  lineage/high-water、single-flight、waiter detach、minimum cut、invalidation floor、dispose 和 manager deadline；
- `apps/host-daemon/src/cloud/snapshot-publisher.test.ts` 检查 immutable retry、pending re-admission、cursor-ahead
  deadline、cleanup ownership 与 retained-body 上限；
- `apps/host-daemon/src/journal.test.ts` 检查 no-copy scalar plan 与 same-revision materialization；
- `apps/host-daemon/src/cloud/host-relay-connection.test.ts` 检查旧 connection 关闭后 replacement 接续同一
  unresolved flight，且旧 connection 不接收 marker；
- `apps/terminal-cloud/test/snapshot-retention.test.ts` 检查 completed row、R2 object verification、cursor-ahead 和
  bounded upload/cleanup ledger。
- P2.0 wire/capability direct tests 检查 strict v3 schema/codec、显式 capability downgrade、authority version
  migration、generation strategy 的 v2 default，以及 v2/v3 decoder 隔离；
- P2.1 `packages/session-client` owner tests 在 pure target 上检查 start-before/after-data、immutable start、lane
  ordinal/bytes、同 lane retry、跨 lane conflict、cold receipt/apply 分离、warm/cold ownership、Done/handoff/
  target-readiness 时序、adoption/source-closed 两种顺序、budget/deadline 与 release-once；它们还穷举保持各
  lane 顺序的短 interleaving，并用
  不调用 production cursor/assembler helper 的独立 reference reducer 逐步核对 effect、receipt 和 applied
  progress。当前 gate 使用仓库已有 Vitest 工具，不新增随机 generator 或 `fast-check` 依赖。
- P2.2 Host owner tests 检查 actor 内 `(R,H]` fixed cut、真实 `TerminalSession -> CanonicalPublisher` 的
  `H / fence(H) / H+1` 顺序、R=H 的 Done-only envelope、per-source/session cap、grant/send retry、actual Done
  receipt closure、generation tombstone、session aggregate bound、deadline/reset/dispose；既有 v2 scheduler
  regression 继续独立证明 pause/barrier/pin fallback 未改变。
- P2.3 Cloud owner tests 保留 `relay-recovery-store.test.ts`、`relay-connection-recovery.test.ts`、
  `relay-socket.test.ts`、`relay-recovery-maintenance.test.ts` 与 `durable-alarm-mux.test.ts`：它们检查 v6→v7
  strict migration 与 V2 default、prepare/install/outbox 原子性、独立 lane sent/received、receipt/apply/adopt/
  source-closed/completion scalar、deadline/pin/fence、outbox capacity rollback、V2/V3 attachment strict roundtrip、
  eviction 后 deadline 重建，以及 shared earliest alarm、one-time initialized marker、stale early delivery、
  active fact 与 completion-clock bounded retry。既有 snapshot tests 继续检查 snapshot alarm 语义与 recovery
  component 不互相覆盖。
- P2.4 owner-level wiring tests 检查 Host `H / fence(H) / H+1`、source reset/closure/deadline，Cloud
  exact socket-bound outbox CAS、hibernation 重建、strict envelope/progress/closure、writer activation crash cut、
  kill-switch fence 和 per-client stop-and-wait isolation，以及 Browser HTTP restore/adopt、progress retry、deadline、
  outcome-uncertain ownership 和 completion 后 live receiver。这些是本地 workerd 与 fake WebSocket 的 wiring
  evidence，不是真实网络、真实 Ghostty continuation 或性能/公平性证明。
- P2.5a direct Store 与 migration evidence 检查 v9 strict no-payload ledger、decimal-u64 ordering、以最后
  obligation 为基线的 chain、`queued -> sending -> sent` CAS、exact sent-prefix receipt、bounded usage/
  grant reservation，以及 v8 uncertain outstanding 的 fail-closed migration。runtime evidence 检查
  enqueue/begin/send/confirm ordering、wake 时 transient state reset、`sent` 零重发、缺失 exact socket pair 的
  owner fence，以及 complete generation 只做本地 terminal fence。这些是 Cloud ledger/cold-owner evidence，
  不是 Host emitter 公平性 evidence；
- P2.5b direct Host evidence 检查同 grant 下多个 sent-but-unreceived record、不重发、exact
  intermediate prefix release、send-throw 不推进、多 source deterministic DRR、每 record yield、
  shared-socket backpressure 只 blocked/retry、单 source deadline retire 以及健康 pair/canonical/input/V2
  regression。这些测试不声称 Cloud 多 client 公平性、真实网络或性能/SLO。

这些测试的证据来自明确状态、identity、frame ordering、资源 owner 和本地 Worker storage/R2 oracle。测试总数、
绿色 CI、timeout 数值或 clean build 本身不证明这些状态。

### 当前局限

- Host suite 大量使用 fake authority、fake timer、mock publisher 和 mock WebSocket；Worker suite 使用本地
  workerd/Miniflare，不代表 production Cloudflare、真实 R2 或真实网络；
- 现有测试不证明任意 Browser 都能取得 blob，也不证明真实 WTerm/Ghostty restore/adopt 后的完整 tail
  continuity；
- JavaScript deadline 不能抢占同步 full snapshot WASM encode，因此 5 秒 capture budget 不是 actor hard max；
- 本地 cleanup slot 只约束运行中的 Host；进程崩溃后 generic `uncertain` upload 的 durable reconciliation 仍未
  完成；
- P2.5a 已在 P2.4 Host/Cloud/Browser runtime 上增加 no-payload ledger 与 cold-owner fence，但默认
  production Cloud kill switch 为 false，Host 与
  Browser 的默认 offer 也不 advertise v3 family，因此未 rollout；
- Cloud scalar store 不保存 mutation payload/hash，不可证 same-ordinal retry 或 cold transient send state
  时仍按 per-client generation fail closed。P2.5b 已在现有 cap/grant 内打开 Host recovery source
  multi-outstanding，但 live lane 仍是 stop-and-wait；P2.5c 的 Cloud session aggregate、shared ring、
  writer/live reservation 与多 client/lane DRR 仍未交付；
- P2.4 的 snapshot transport/replica-host seam 与本地 runtime tests 不证明真实 WTerm/Ghostty 的
  parser continuation、atomic adopt 或 terminal state equivalence。P2.6 的真实 continuity oracle、完整三方
  deterministic fault integration 与 rolling downgrade/rollback gate 仍待完成；
- 当前 evidence 不证明真实跨进程网络、Cloudflare/R2 环境、load/soak、性能/SLO、
  production dashboard 或真实滚动发布。

### 后续需要的测试

- Cloud-first rolling/rollback 的 production-like staging 验证；v5→v6 authority-version/strategy 与 v6→v7
  recovery scalar direct migration fixtures 已保留；
- 真实 Cloudflare alarm retry/eviction、DO hibernation、R2 HEAD/PUT/delete、response loss、Host crash/restart 和
  generic uncertain reconciliation 的 fault injection；
- P2.5c 建立 Cloud session aggregate、ephemeral shared ring、writer/live reservation、live window
  与多 client/lane DRR；
- P2.6 在完整三方 wiring 上扩展独立 reference model/property/fuzz 与 deterministic
  crash/loss integration，并用真实 WTerm/Ghostty parser continuation 与 exact tail-continuity oracle 验证
  snapshot restore/adopt 后续，最后验证 rolling capability downgrade/rollback；
- 性能验证只在 workload、link profile、环境隔离、warm-up、样本量和统计方法另行批准后实施。

### Phase 2：Recovery v3，继续使用 full snapshot

- P2.0 已冻结 protocol capability negotiation、v2 authority identity、v3 delivery envelope 与 generation
  strategy 的 v2 default；P2.1 已冻结 pure Browser bounded assembler、同 lane exact retry、跨 lane
  conflict、gap/deadline/reset 与 receipt/apply/adopt progress；P2.2 已冻结 Host committed-through-H
  actor cut、ordered start fence、PreparedGap envelope source 与 release-once closure；P2.3 已冻结 Cloud
  v7 SQL scalar truth、V2/V3 attachment、same-transaction fence、cold pin union、bounded control outbox 与 shared
  earliest alarm owner；
- P2.4 已把 Cloud exact socket-bound outbox CAS、Host source/control/data owner、Browser assembler、
  `TerminalSession`、HTTP snapshot restore/adopt、socket progress/retry 和 completion 后长期 live 接通；
  `H / fence(H) / H+1`、strict recovery envelope/closure、hibernation、writer lease 与 per-client
  stop-and-wait/reset isolation 都由 capability-gated runtime 持有；
- P2.5a 已增加 v9 no-payload delivery ledger、exact sent-prefix receipt、bounded usage/reservation query，
  并把 runtime send 固定为 enqueue/begin、transaction 外 socket send、confirm。wake 时 transient
  `queued`/`sending` 与失去 exact Browser pair 的 owner 会 fail closed；complete owner 只做本地 terminal
  fence，不发送 Host reset；
- P2.5b 已使 Host retained recovery source 在累计 grant 内保持多个 outstanding record，
  并增加 connection-local source DRR、shared data backpressure yield 与 per-source deadline isolation。
  control/input/canonical live 优先，live lane 仍是 stop-and-wait；
- P2.5c 才增加 Cloud session aggregate、ephemeral shared ring、writer/live reservation、live window
  与多 client/lane DRR。性能、load/soak、dashboard 与 SLO 继续后置，不作为 correctness
  的替代证据；
- P2.6 才用 deterministic three-owner fault integration 和真实 WTerm/Ghostty snapshot/parser
  continuation/exact-tail oracle 完成 continuity 与 rolling gate。默认 kill switch/offer 在此前保持
  v2；v3 generation 不走 global pause/fixed-commit barrier/pin，但 v2 fallback 必须继续保留
  现有 pause/barrier/pin invariant。

Gate：snapshot 下载/restore 时 `(R,H]` 与 `H+1...` 任意交错仍只采用连续前缀；
ACK/Done/Adopted/SourceClosed 丢失可恢复；一个慢客户端不阻塞 authority、writer 或其他客户端；full
snapshot 路径通过独立 reference model/property、owner-level deterministic fault integration 与真实 Ghostty
snapshot/continuation equivalence。浏览器端到端只保留 capability downgrade 和 wiring smoke，并明确不作为
lane interleaving、ownership、terminal state 或性能证明。

P2.0–P2.5b 没有修改 WTerm/Ghostty。若 P2.5c/P2.6 暴露真实 API 缺口，必须先在
`Eric-Song-Nop` 的对应 fork 创建正式 PR、完成 review/验证，再由 Zhongduan 的独立 stacked PR
更新固定 submodule 指针；不得直接修改
vendor、指向 upstream 或 pin 未审 commit。

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
- live/recovery 跨 lane 任意乱序但各 lane ordinal 保持连续、同 lane retry、Done-before-restore/apply（不越过
  recovery lane lower ordinal）、Start-after-live、Adopted/ACK/SourceClosed loss；
- 同 lane、同 ordinal duplicate 逐字节一致时幂等；跨 lane 同 eventSeq、divergent duplicate、gap、错误
  epoch/engine/generation/offset 一律 fail closed；
- v2 pause/barrier 和 v3 gap-fill 分别模型测试，不允许半 v2/半 v3 状态。

### 生命周期、容量与多客户端

- single-flight、16 waiters/observers、supersede、relay reconnect、DO hibernation、cursor-ahead；
- encode/compress/upload 失败、response loss、R2 404/checksum mismatch、pending body 失去 serviceability；
- Browser download/restore timeout、assembler cap、generation bump、candidate/history abort、dispose once；
- recovery credit exhaustion、fair scheduling、慢 observer 隔离和 writer/live reservation。

### 后置的性能、soak 与 SLO

本节只记录未来可能需要的环境验证，不是当前 Phase 2 实现或完成 gate。只有 workload、link profile、环境
隔离、warm-up、样本量和统计方法另行批准后，才实施 30 分钟 soak、性能回归或 SLO；当前不编写这些测试，
也不从 correctness suite 推断性能。

核心指标包括：

- `snapshot_capture_actor_pause_ms`、encode/compress/publish/download/restore；
- `checkpoint_valid/usable/fresh`、range bytes/frames/age、pending abandon/supersede；
- recovery start/done/adopt、assembler bytes/frames/gap span、logical lane credit/HOL；
- `delivery_received_ordinal` 与 `replica_applied_event_seq` lag；
- `input_ack_ms - measured_transport_rtt_ms`、Cloud/Host local queue、Ctrl-C 到 PTY write；
- time-to-first-visible、time-to-current、fresh attach 与 same-page resync 分布。

未来候选 gate：

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

- Phase 0–4 依赖顺序通过；Recovery v3 默认启用/rollout 需独立批准，v2 fallback 在明确
  退役 gate 通过前继续保留 pause/barrier/pin invariant；
- README/总纲、wire protocol、input 与本计划使用同一 invariant、cursor 和 CURRENT/TARGET 术语；
- Phase 2 以独立 reference model/property、owner fault tests、真实 snapshot continuation fixture 与最小 wiring
  smoke 收口；wiring smoke 不证明状态机、terminal state 或性能；
- 性能、soak、production dashboard 与 SLO 保持后置，只有验证方法另行批准后才成为对应后续阶段的 gate；
- rollback 只影响新 generation，已开始 attempt 完成或明确 reset，不跨 generation 拼接；
- 同环境 Mosh/SSH 基准只陈述观测结果，不用架构推断宣称“全面超过”。
