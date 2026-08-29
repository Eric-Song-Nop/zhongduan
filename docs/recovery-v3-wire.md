# Recovery v3 Wire Contract

> 状态：P2.0 wire/capability、P2.1 pure Browser RecoveryAssembler、P2.2 pure Host
> PreparedGap/source owner 与 P2.3 Cloud durable scalar owner 为 stacked candidates；所有生产 generation
> 仍选择 protocol v2，Recovery v3 runtime 仍不可达
>
> 适用范围：Host authority data、Cloud delivery、Browser recovery 与滚动协商

本文固定 Recovery v3 的 wire 分层与 capability gate。状态机、资源 ownership 和阶段依赖见
[高性能 Snapshot 与 Recovery v3 实施计划](high-performance-snapshot-recovery-plan.md)，最高级连续前缀
不变量见[终端协议架构](terminal-protocol-architecture.md)。

## 分层

Recovery v3 不改变 canonical mutation identity：

- Host authority log 继续使用 data protocol v2；
- `PTY_OUTPUT` 与 `RESIZE_APPLIED` 仍是唯一推进 authority cursor 的 mutation；
- Cloud 到 Browser 的 v3 delivery 使用 generation-scoped envelope；
- envelope 携带 logical lane、delivery ordinal 和 cumulative encoded bytes；
- envelope 内的 mutation 是完整、未改写的 canonical v2 frame；
- delivery generation、lane、ordinal 和 receipt 都不属于 authority log。

因此，同一个 mutation 不会因发送给不同 Browser 而获得新的 canonical identity。v2 Browser 仍只收到
现有 v2 delivery frame；未确认 v3 协商的 endpoint 永远不能收到 v3 envelope 或 control frame。

## 显式协商

Endpoint 通过 `x-zhongduan-relay-capabilities` 提交有界 token offer。只有 offer 包含
`capability-negotiation-v1` 时，新 Cloud 才在 connection-set response 返回
`negotiatedCapabilities`。返回数组是服务端固定顺序的已知交集，并包含 negotiation token 本身。

滚动兼容规则：

- 旧 client 不提交 negotiation token，因此新 Cloud 不增加 response 字段；
- 新 client 连接旧 Cloud 时，未知 token 被忽略，response 不含确认字段；
- 缺少 `negotiatedCapabilities`，或数组不含 negotiation token，必须完整回退 v2；
- 历史 `selectedCapabilities` 仅为旧 DO response 的 decode shim，不能确认任何 v3 能力；
- endpoint 只有在实现对应状态机后才能 advertise 该能力，不能把预告 token 当 feature flag。

P2.0 只允许生产 endpoint advertise 已实现的：

- `capability-negotiation-v1`；
- `authority-data-v2`；
- Host 已有的 `delivery-barrier-outcome-v1`。

以下 token 只建立 schema，后续 owner PR 完成前不得由生产 endpoint advertise：

- `wire-endpoint-v3`；
- `delivery-envelope-v3`；
- `delivery-receive-credit-v1`；
- `authority-apply-progress-v1`；
- `recovery-v3-gap-fill-v1`。

新 generation 只有在 Cloud kill switch 开启、session authority data version 匹配，并且 Host 与 Browser 的
confirmed capability 分别满足完整依赖时才能选择 v3。缺少任一依赖都选择完整 v2；同一 generation 的
strategy 不可切换。

## Cursor 与 boundary

Authority cursor 不包含 delivery generation：

```text
AuthorityCursor {
  sessionEpoch
  eventSeq
  nextPtyOffset
}
```

`H` 之后的 live 起点不是一个尚未存在的 authority cursor，而是 mutation boundary：

```text
MutationBoundary {
  sessionEpoch: H.sessionEpoch
  nextEventSeq: H.eventSeq + 1
  nextPtyOffset: H.nextPtyOffset
}
```

`H.eventSeq == uint64 max` 时不存在 successor，必须 fail closed。`base R`、`committedThrough H` 与
`liveFloor` 必须属于同一 session epoch，且 `R <= H`。

## Delivery envelope v3

固定 header 为 40 bytes：

| Offset | Size | 字段                                             |
| -----: | ---: | ------------------------------------------------ |
|      0 |    4 | `ZENV` magic，network order                      |
|      4 |    1 | envelope version，固定 `3`                       |
|      5 |    1 | lane：`live=1`、`recovery=2`                     |
|      6 |    2 | flags，当前固定 `0`                              |
|      8 |    8 | delivery generation，little-endian               |
|     16 |    8 | delivery ordinal，little-endian，从 `1` 连续递增 |
|     24 |    8 | cumulative encoded bytes，little-endian          |
|     32 |    4 | stream id，little-endian                         |
|     36 |    4 | payload length，little-endian                    |

payload 是完整 canonical v2 frame。它必须满足：

- inner `deliveryGeneration=0`、`streamId=0`、`flags=0`；
- live lane 只承载 `PTY_OUTPUT` 或 `RESIZE_APPLIED`；
- recovery lane 还可承载空 payload 的 `REPLAY_COMMIT`，作为 `RecoveryDone`；
- cumulative bytes 等于本 lane 从 ordinal `1` 到当前 record 的 `header + payload` 字节总和；
- 同 lane、同 ordinal 的 retry 必须逐字节一致；任何不同 identity fail closed；
- recovery `(R,H]` 与 live `[H+1,...)` 不应重叠，跨 lane 重复视为错误 live floor 并 reset。

P2.0 codec 只证明单条 envelope 形状和显式 lane cursor 的连续 ordinal/bytes。`RecoveryDone` 是否精确位于
start 的 `H`、之后是否仍有 recovery record，以及跨 lane eventSeq 是否冲突，都必须由 P2.1
generation-owned assembler 根据完整 start state 判定；单条 codec 不能替代这些状态 oracle。

Browser 只有在完整 envelope 已校验并复制进有界内存后，才可发送对应的 `DeliveryReceived`。receipt 只能
等于已经发送的 lane cursor，不能越过 ordinal 或 cumulative bytes。`ReplicaApplied` 独立表示 Ghostty
连续 apply 的 authority cursor；两种进度不能互相推导。

## Recovery start fence

`RecoveryStartFence` 是 Host canonical data WebSocket 上的有序 marker。它复用 v2 delivery-barrier frame
header 的 `H` cursor，但使用仅 v3 endpoint 可接受的严格 payload。Cloud 在同一 Host data message queue 中：

1. 验证 client、engine、epoch、generation、base 与 source；
2. 要求 SQL committed head 精确等于 header 中的 `H`；
3. 原子安装 assembling state、immutable start identity 与精确的 successor mutation boundary；
4. 在处理下一条 Host data message 前完成上述状态写入；
5. 向 Browser 发送完整 `RecoveryStart`，并向 Host 返回初始 bounded recovery grant。

Start fence 不推进 event sequence，不进入 journal，也不转发 Browser。Host 收到 matching ready/grant 前不得
发送 PreparedGap payload；Cloud grant 使用累计 bytes，Host source 只能在已授予窗口内发送。

Cold `RecoveryStart` 原子包含完整 immutable snapshot manifest、base、H 和 live floor。不能先发 snapshot id，
再用另一条消息补 checksum、长度或 URL。

## Completion 与 ownership

`RecoveryDone` 是 recovery lane 的最后一个 record，占用稳定 ordinal 和 cumulative bytes。Browser receipt
覆盖 Done 后，closure 路径为：

```text
Browser DeliveryReceived
  -> Cloud verified receipt
  -> Host source receipt
  -> Host release-once PreparedGap
  -> RecoverySourceClosed
  -> Cloud
  -> Browser
```

Browser 只有在 matching `RecoverySourceClosed`，且本地 recovery lane receipt 同时覆盖 certificate 的
`throughRecoveryOrdinal` 与 `throughRecoveryCumulativeEncodedBytes` 时，才能释放用于 late duplicate 比较的
identity cache。Cloud 只有 matching `RecoveryAdopted` 与 `RecoverySourceClosed` 都成立时才能把 generation
标为 synced。两者允许任意到达顺序和幂等 retry。

## 当前启用状态与验证边界

### 当前保留的 stacked candidates

P2.0 保留 strict schema、codec、capability transport、authority version migration 和 generation strategy 的 v2
default。它不发送 v3 frame，也不移除 v2 pause/barrier/pin。

P2.1 只保留 `packages/session-client` 内 generation-scoped pure assembler。它接受 caller 提供的 start、完整
envelope、cold candidate、source-closed certificate、monotonic tick 和 handoff confirmation，并输出稳定的
receipt/apply/adopt/completion progress；它自己不做网络、HTTP 或真实 production WTerm wiring，只向 caller
提供的 pure target 应用已验证 mutation。保留行为包括：

- start 前有界拥有 payload copy，但 start 完整验证前不公开 receipt；matching immutable start 幂等，divergent
  start fail closed；
- per-lane ordinal/cumulative bytes 连续，同 ordinal retry 逐字节一致；recovery `(R,H]`、live
  `[H+1,...)`，错误 range、canonical identity、offset、epoch、engine、generation、stream 或跨 lane duplicate
  fail closed；
- 只按小 quantum apply 连续 authority 前缀。cold target 未安装时 receipt 与 apply 分离；warm target 必须精确
  位于 `R`，cold install 必须匹配 recovery attempt、base 与 engine；candidate 是否确由 start 中的 snapshot
  manifest restore 而来，属于后续 HTTP/WTerm wiring owner 的义务，P2.1 尚不证明；
- `RecoveryDone(H)` 是 recovery lane 的最后 record。它可早于 restore/apply 完成到达，但不能早于缺失的
  lower recovery ordinal；Done 已验证且 applied 至少到 `H` 后才 handoff-eligible；
- caller 完成真实 atomic handoff 后才 confirm，随后产生稳定 `RecoveryAdopted(K)`，其中 `K >= H`；
- handoff confirmation 只把 cold core 的 dispose/visibility ownership 交给长期 replica host；在 attempt completion
  之前，assembler 仍是该 core 唯一的 mutation ordering writer，并继续排空已经 receipt 的 live prefix；
- source closed 与 adopted 可任意顺序到达；closure certificate 被本地 recovery receipt 覆盖后才释放 late-retry
  identity cache，两者齐备且已经 receipt 的 live mutation 全部 apply 后，才输出 final authority 与 lane cursors；
- cold candidate 的 install 返回 accepted 才转移 ownership；之后 failure/reset/close 由 assembler
  dispose-once，handoff confirm 后把 dispose/visibility ownership 转给长期 replica host，completion 后才把 mutation
  delivery ownership 转给长期 live receiver。只有已经尝试 write/resize 后发生的 warm failure 才把 borrowed target
  标为 tainted；零 effect 的 start/admission conflict 保留可复用 base。所有 gap/no-progress deadline只由显式
  monotonic tick 推进。

P2.2 只保留 Host 内尚未接 production control union 或 relay connection 的 source primitives：

- `TerminalSession` 在一个 actor turn 内固定 `H`，对 `(R,H]` 先做 exact journal range/cap 检查，再拥有
  materialized frame copy，并同步执行 fence commit；callback 拒绝或抛错只返回 unavailable，不改变 authority；
- `CanonicalPublisher` 把 strict `RecoveryStartFence(H)` 当作 ordered marker 排在 `H` 与 `H+1` 之间。marker
  占有界 queue budget，但不进入 journal、不推进 canonical cursor，也不暂停 publisher；
- session-scoped `RecoverySourceManager` 在 actor commit 内预编码唯一 retained recovery envelope copy，并以
  `RecoveryDone(H)` 作为最后 record。只有 matching start-ready/grant 才允许 bounded drain；send throw 不推进
  cursor，重试仍发送逐字节一致的 retained record；
- 只有覆盖实际已发送 Done ordinal/cumulative bytes 的 receipt 才 release-once payload，并产生稳定
  `RecoverySourceClosed`。reset、deadline 与 generation replacement 立即释放 payload，同时保留有界
  owner+stream generation tombstone，阻止同代迟到 prepare 复活；
- per-source canonical cap、session-owned envelope cap、source-count cap、no-progress/absolute deadline 和
  owner-token fence 均由 caller 显式配置或驱动；这一层不拥有 socket、timer、Cloud state 或公平调度。

P2.3 只保留尚未接 production v3 control/data handler 的 Cloud durable owner：

- schema v7 的 STRICT `recovery_attempt`、`recovery_delivery_lane` 与 `recovery_control_outbox` 保存 immutable
  prepare/start、base/committed/live floor、grant/Done、每 lane sent/received cursor scalar、replica-applied、adopted、
  source-closed、deadline/reset 与 bounded control intent；它们是 hibernation-safe scalar truth；
- strict versioned hibernation attachment 完整保留 V2 legacy delivery state；V3 只保存 socket identity、
  capability、`recoveryStrategy=v3` 与可选 Browser recovery lookup key，Host lookup 必须为空，不持久化 legacy
  delivery fields，未知 version/field fail closed；production socket 目前仍只创建 V2 attachment；
- generation replacement/activation、Host fence 与 client removal 在各自 connection mutation 的同一
  `transactionSync` 中 fence recovery。reset outbox 无容量时整个 connection/recovery transition 回滚；alarm、
  snapshot cleanup 和其他外部工作只在 transaction 之后请求；
- active cold recovery snapshot pin 与已有 V2 socket pin 合并；SQL deadline 在 eviction 后重建；snapshot 与
  recovery 共用一个 persisted earliest-deadline alarm mux，首次 `initialized` marker 只接管一次 pre-mux
  snapshot alarm，stale early delivery 不提前执行 future deadline；handler failure 从完成时钟后做 bounded retry；
- Cloud 不保存 delivery mutation envelope payload 或 hash。已记录 ordinal 的 envelope retry 无法只靠 scalar
  cursor 证明 byte-identical，因此 Cloud 必须 fail closed，并由后续 runtime reset/replan；exact retry 证明仍由
  持有 retained copy/hash 的 Host source 与 Browser assembler 负责。

P2.3 的 outbox 还没有 drain/ACK owner，prepare/start/envelope/receipt/apply/adopt/source-closed 也未绑定当前
socket identity，因此这些 durable facts 当前不能使 production v3 可达。

### 当前不可达与后置 owner

P2.1 assembler 没有接入 Browser `TerminalSession` 或 v2 `SessionCoordinator`，P2.2 Host primitives 也没有
接入 `HostRelayConnection`、production control union 或 delivery scheduler；P2.3 Cloud scalar store/outbox 也
没有 production drain、socket binding 或 v3 handler。endpoint 仍不 advertise v3 capability。因此 production
attach、warm resync 和 cold recovery 都不能到达这些 primitives。当前不包含：

- HTTP snapshot download、WTerm restore/adopt 或真实 terminal handoff；
- Cloud outbox drain、current-socket binding，以及 prepare/start/envelope/receipt/apply/adopt/closure transport；
- Host source primitive 到实际 relay control/data socket 的接线和多 source 公平调度；
- WebSocket send/retry timer、generation replan，或 completion 后长期消费 live mutation 的 owner。

滚动实现顺序保持 capability-first：P2.0 保留显式 downgrade，P2.1/P2.2 冻结两端 pure owner，P2.3 冻结 Cloud
durable scalar/attachment/alarm owner；P2.4 才 drain outbox 并接 Host/Browser runtime、真实 target handoff 和长期
live，P2.5 再建立 bounded lane credit 与 multi-client fairness。完整三方 negotiated state machine、owner fault
tests、真实 snapshot continuation oracle 与 rolling downgrade/rollback gate 全部通过后，才允许对**新的**
delivery generation 一次性选择 v3，并同时切换掉该 v3 path 的 global pause、fixed-commit barrier 和 pin。不能
提前单独关闭其中任何一项；v2 generation 与 v2 fallback 保持原 invariant。性能验证继续后置，不能替代这些
correctness gates。

### 保留的直接证据

P2.0 owner tests 覆盖：

- negotiation-aware 与 legacy response shape；
- 三方 capability selector 与 disabled kill switch；
- v5 row migration 后 authority data version 为 `2`、strategy 为 `v2`；
- Browser capability 在 connection ticket、hibernation attachment 和 data-only replacement 后保持；
- authority cursor、boundary、start、receipt/apply/closure strict schema；
- envelope roundtrip、ordinal/bytes 边界、非法 inner mutation 与 unknown version/kind；
- v2 decoder 不接受 v3 envelope，v3 decoder 不把 v2 frame 误判成 envelope。

P2.1 owner tests 使用 pure target，覆盖 start-before/after-data、immutable start、lane ordinal/bytes、buffer copy
ownership、相同/冲突 retry、range/identity/offset 错误、cold target install、warm/cold failure ownership、
quantum apply、Done 相对 target readiness/apply 的时序、adoption/source-closed 两种顺序、budget/deadline 和
release-once。短 lane merge 由保持各 lane 顺序的 exhaustive cases 覆盖，并在每一步对照不调用 production
cursor/assembler helper 的独立 reference reducer；当前不引入随机 generator 或 `fast-check`，也不把 case
数量当作 correctness 证据。

P2.2 owner tests 覆盖 actor 内固定 `(R,H]`、真实 `TerminalSession -> CanonicalPublisher` 的
`H / fence(H) / H+1` 出队顺序、R=H 时唯一 Done record 的 literal 40-byte envelope/cumulative bytes、cap 与
cap+1、grant boundary、send throw 后 byte-identical retry、伪造/提前 receipt 不释放、exact Done closure retry、
generation tombstone、16-source aggregate bound、deadline、reset 与 dispose。既有 v2 delivery scheduler regression
仍单独证明 pause/barrier/pin fallback 未改变；这些测试不声称已证明 socket fairness 或 Cloud hibernation。

P2.3 owner tests 保留 v6→v7 strict migration 与 V2 default、prepare/install/outbox 原子性、独立 lane
sent/received、receipt/apply/adopt/source-closed/completion scalar、deadline/pin/fence、outbox capacity rollback、
V2/V3 attachment strict roundtrip、eviction 后 SQL deadline 重建，以及 snapshot/recovery shared earliest alarm、
one-time initialized marker、stale early delivery、active retry fact 与 completion-clock bounded retry。它们也明确
证明 Cloud 不保存 mutation payload/hash 时不能认证同 ordinal replay。

这些证据只证明 pure wire/assembler contract、Cloud scalar/attachment/alarm owner 和显式 downgrade，不等价于
production-like 网络、outbox drain、真实 WTerm/Ghostty state 或跨进程 owner。P2.4 需要 current-socket-bound
outbox send/ACK、same-ordinal reset、duplicate/gap/adopt/closure/crash fault integration 与真实
snapshot/parser-continuation/exact-tail oracle；P2.5 需要 multi-client aggregate/fair scheduling。性能、load/soak、
SLO 与 dashboard 仍后置；当前 correctness gate 不编写或推断性能验证。

P2.0–P2.3 不修改 WTerm/Ghostty。若后续 wiring 暴露真实 API 缺口，必须先在 `Eric-Song-Nop` 对应 fork 建立
正式 PR 并完成 review/验证，再由 Zhongduan 的独立 stacked PR 更新固定 submodule；不得直接改 vendor、指向
upstream 或 pin 未审 commit。
