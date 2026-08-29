# Recovery v3 Wire Contract

> 状态：P2.0–P2.4 为 stacked candidates；Recovery v3 的 Host/Cloud/Browser runtime 已在完整
> capability gate 后接通。默认 production kill switch 和 Host/Browser offers 仍选择 protocol v2，
> 所以尚未 rollout
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

默认 production endpoint 仍只 advertise v2 baseline：

- `capability-negotiation-v1`；
- `authority-data-v2`；
- Host 已有的 `delivery-barrier-outcome-v1`。

P2.4 runtime 已实现以下 v3 family，但默认 Host/Browser offer 不包含它们；只能通过
明确 rollout/test seam 提交对应 endpoint 所需的完整 family，不能只开其中一部分：

- `wire-endpoint-v3`；
- `delivery-envelope-v3`；
- `delivery-receive-credit-v1`；
- `authority-apply-progress-v1`；
- `recovery-v3-gap-fill-v1`。

新 generation 只有在 Cloud kill switch 开启、session authority data version 匹配，并且 Host 与 Browser 的
confirmed capability 分别满足完整依赖时才能选择 v3。缺少任一依赖都选择完整 v2；同一 generation 的
strategy 不可切换。Browser 侧 strategy-aware strict decoder 保证 v2 generation 不接受 v3
control/envelope，v3 generation 也不接受 v2 replay-start、snapshot-manifest、ACK 或 data frame。
Host relay pair 属于整个 session，必须同时服务 v2 与 v3 Browser generations；因此 Host 连接先严格分派
共享/legacy control，再分派 Recovery v3 source control，不能把某个 Browser generation 的 strategy
提升为整条 Host pair 的唯一 decoder。

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

### P2.0–P2.4 stacked candidates

P2.0 保留 strict schema、codec、capability transport、authority version migration 和 generation strategy 的 v2
default。P2.4 增加按 generation strategy 隔离的 Browser↔Cloud 与 Cloud↔Host strict control union；
v2 decoder 仍保持原 shape。v2 fallback 的 pause/barrier/pin 没有改变。

P2.1 保留 `packages/session-client` 内 generation-scoped pure assembler。它接受 caller 提供的 start、完整
envelope、cold candidate、source-closed certificate、monotonic tick 和 handoff confirmation，并输出稳定的
receipt/apply/adopt/completion progress；它自己不做网络、HTTP 或 WTerm wiring，只向 caller
提供的 target 应用已验证 mutation。保留行为包括：

- start 前有界拥有 payload copy，但 start 完整验证前不公开 receipt；matching immutable start 幂等，divergent
  start fail closed；
- per-lane ordinal/cumulative bytes 连续，同 ordinal retry 逐字节一致；recovery `(R,H]`、live
  `[H+1,...)`，错误 range、canonical identity、offset、epoch、engine、generation、stream 或跨 lane duplicate
  fail closed；
- 只按小 quantum apply 连续 authority 前缀。cold target 未安装时 receipt 与 apply 分离；warm target 必须精确
  位于 `R`，cold install 必须匹配 recovery attempt、base 与 engine；candidate 是否确由 start 中的 snapshot
  manifest restore 而来不属于 P2.1 pure owner 的证明边界；P2.4 接入 exact snapshot transport，
  真实 WTerm/Ghostty continuation oracle 仍属于 P2.6；
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

P2.4 `RecoveryRuntime` 现在把这个 assembler 接入 Browser `TerminalSession`：它使用
`RecoveryStart` 内的 exact manifest 执行 HTTP snapshot restore，将 detached cold candidate adopt 到
`ReplicaHost`，为 warm handoff 确认现有 target，并稳定重试 receipt/apply/adopt progress 与
source-closed 收敛。只有 assembler completion 后才把 mutation ordering ownership 交给长期 live
receiver；deadline、restore abort、dispose/adopt 不可判定结果和 tainted warm base 都有明确 owner。

P2.2 保留 Host 内 bounded source primitives：

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

P2.4 已把 source owner 接入 strict `HostRelayConnection`：Cloud prepare 在 authority actor 内建立
source 并产生 ordered fence，start-ready/grant 驱动单 record bounded drain，verified receipt 驱动后续
record 和 release-once `RecoverySourceClosed`。reset、pair replacement、send outcome uncertain 与 deadline 都
fence exact owner；Cloud 在 prepare 送达前持久化的 exact reset 会安装有界 generation tombstone，
防止同代迟到 prepare 复活，不会误关健康 Host pair。

P2.3 保留 Cloud hibernation-safe durable owner：

- schema v7 的 STRICT `recovery_attempt`、`recovery_delivery_lane` 与 `recovery_control_outbox` 保存 immutable
  prepare/start、base/committed/live floor、grant/Done、每 lane sent/received cursor scalar、replica-applied、adopted、
  source-closed、deadline/reset 与 bounded control intent；它们是 hibernation-safe scalar truth；
- strict versioned hibernation attachment 完整保留 V2 legacy delivery state；V3 只保存 socket identity、
  capability、`recoveryStrategy=v3` 与可选 Browser recovery lookup key，Host lookup 必须为空，不持久化 legacy
  delivery fields，未知 version/field fail closed；
- generation replacement/activation、Host fence 与 client removal 在各自 connection mutation 的同一
  `transactionSync` 中 fence recovery。reset outbox 无容量时整个 connection/recovery transition 回滚；alarm、
  snapshot cleanup 和其他外部工作只在 transaction 之后请求；
- active cold recovery snapshot pin 与已有 V2 socket pin 合并；SQL deadline 在 eviction 后重建；snapshot 与
  recovery 共用一个 persisted earliest-deadline alarm mux，首次 `initialized` marker 只接管一次 pre-mux
  snapshot alarm，stale early delivery 不提前执行 future deadline；handler failure 从完成时钟后做 bounded retry；
- Cloud 不保存 delivery mutation envelope payload 或 hash。已记录 ordinal 的 envelope retry 无法只靠 scalar
  cursor 证明 byte-identical，因此 Cloud 必须 fail closed 并 reset/replan；exact retry 证明仍由
  持有 retained copy/hash 的 Host source 与 Browser assembler 负责。

P2.4 Cloud runtime 在这些 durable facts 上完成：

- outbox 只向 exact current Host/Browser socket identity 发送，socket send 在 `transactionSync`
  之外；ACK 用 exact destination、kind 和 payload JSON 做 CAS，stale socket 或不可判定 send
  outcome 不能消费 durable intent；hibernation 后从 strict attachment 与 SQL 重建 exact 路由；
- Host data 严格保持 `H / fence(H) / H+1`。start fence 在处理 `H+1` 前原子安装
  attempt/start/live floor；Host 只能发 recovery-lane envelope，Cloud 只从 canonical mutation 生成
  live-lane envelope，Host 注入 live lane 会被 fence；
- prepare/start/envelope、`DeliveryReceived`、`ReplicaApplied`、`RecoveryAdopted` 和
  `RecoverySourceClosed` 全部绑定 exact recovery/client/connection/stream/generation 与 Host fence。
  writer Welcome/token、lease renew、semantic input 和 Host input ACK 也只使用 exact active v3 owner；
- P2.4 以每 lane strict stop-and-wait 代替尚未实现的 dynamic credit。当当前 client 存在
  outstanding envelope 时又来一条 canonical mutation、看到 same ordinal 或 data send outcome
  uncertain，只 reset/isolate 该 client generation；authority、v2 客户端与其他 v3 客户端继续。

### 默认生产启用状态与后置 gate

P2.4 的 runtime wiring 已经存在，但默认配置不会选中它：

- Wrangler 的 `RECOVERY_V3_ENABLED` 默认为 `"false"`；
- Host `CloudApiClient` 与 Browser `TerminalSession` 的默认 capability offer 只包含 v2 baseline；
- 只有 kill switch、已发布 snapshot、authority version 与 Host/Browser/Cloud 完整 capability family
  同时成立，新 Browser generation 才选择 v3；
- kill switch 关闭时，Cloud 在 v2 decode 或 outbox drain 前 fail closed 任何已持久化 v3
  attachment/attempt，不发送 pending v3 outbox intent。

因此当前状态是“capability-gated runtime 已接通”，而不是“Recovery v3 已上线”。v3
generation 不使用 v2 global pause/fixed-commit barrier/pin；默认 v2 generation 和 rolling fallback
仍完整保留 pause/barrier/pin invariant，不得因 P2.4 wiring 单独放宽。

P2.5 仍需建立 dynamic per-lane credit、session aggregate cap、writer/live reservation 与 multi-client
fair scheduling；P2.4 的 fixed grant/stop-and-wait 只是有界 correctness 路径。P2.6 仍需完成
three-owner deterministic crash/loss integration、真实 WTerm/Ghostty snapshot/parser continuation 与 exact-tail
continuity oracle，以及 rolling downgrade/rollback gate。性能、load/soak、SLO 和 dashboard 继续后置，
不能替代 correctness gates。

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
generation tombstone、session aggregate bound、deadline、reset 与 dispose。既有 v2 delivery scheduler regression
仍单独证明 pause/barrier/pin fallback 未改变；这些测试不声称已证明 socket fairness 或 Cloud hibernation。

P2.3 owner tests 保留 v6→v7 strict migration 与 V2 default、prepare/install/outbox 原子性、独立 lane
sent/received、receipt/apply/adopt/source-closed/completion scalar、deadline/pin/fence、outbox capacity rollback、
V2/V3 attachment strict roundtrip、eviction 后 SQL deadline 重建，以及 snapshot/recovery shared earliest alarm、
one-time initialized marker、stale early delivery、active retry fact 与 completion-clock bounded retry。它们也明确
证明 Cloud 不保存 mutation payload/hash 时不能认证同 ordinal replay。

P2.4 owner-level wiring tests 保留以下直接证据：

- Host strict control union、`H / fence(H) / H+1`、bounded source drain、partial/Done receipt、exact
  source reset/tombstone、send outcome 与 deadline fence；
- Cloud exact Host/Browser/payload outbox CAS、DO hibernation 后路由、start/live/recovery 顺序、strict
  envelope/progress/closure、writer lease activation crash cut、Host/Browser fence、kill-switch 关闭和
  per-client stop-and-wait/reset isolation；
- Browser 完整 capability family 选择、HTTP snapshot restore/adopt、warm/cold handoff、三类 progress
  retry、monotonic deadline、outcome-uncertain ownership 与 completion 后长期 live receiver；
- 既有 v2 decoder、pause/barrier/pin、connection replacement 和 input path regression 仍保持隔离。

P2.4 证据是本地 workerd/Miniflare、fake WebSocket、fake timer/clock 与 caller-provided replica seam
上的 wiring evidence。它不证明真实网络、production Cloudflare/R2、真实 WTerm/Ghostty parser
continuation 与 tail equivalence，也不证明 lane credit、multi-client fairness、性能或 SLO；这些分别
属于 P2.5/P2.6 及后续环境 gate。

P2.0–P2.4 不修改 WTerm/Ghostty。若 P2.5/P2.6 暴露真实 API 缺口，必须先在
`Eric-Song-Nop` 对应 fork 建立正式 PR 并完成 review/验证，再由 Zhongduan 的独立 stacked PR
更新固定 submodule；不得直接改 vendor、指向
upstream 或 pin 未审 commit。
