# Recovery v3 Wire Contract

> 状态：P2.0 stacked candidate；所有生产 generation 仍选择 protocol v2
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

P2.0 只交付 strict schema、codec、capability transport、authority version migration 和 generation strategy
的 v2 default。它不发送 v3 frame，不移除 v2 pause/barrier/pin，也不声称 Recovery v3 已启用。
Start-ready 与累计 recovery grant 的 Host/Cloud control shape 由 P2.2 source-owner PR 冻结；P2.0 只固定其
ownership/credit requirement，尚未声称完整 v3 runtime wire 已全部实现。

直接测试必须覆盖：

- negotiation-aware 与 legacy response shape；
- 三方 capability selector 与 disabled kill switch；
- v5 row migration 后 authority data version 为 `2`、strategy 为 `v2`；
- Browser capability 在 connection ticket、hibernation attachment和 data-only replacement 后保持；
- authority cursor、boundary、start、receipt/apply/closure strict schema；
- envelope roundtrip、ordinal/bytes边界、非法 inner mutation 与 unknown version/kind；
- v2 decoder 不接受 v3 envelope，v3 decoder 不把 v2 frame误判成 envelope。

这些测试证明本地 wire/schema contract 和协商降级逻辑，不等价于运行旧 binary 或 production-like
Cloud-first/rollback staging。它们也不证明 assembler、PreparedGap ownership、Cloud v3 state、fairness、
真实 restore/adopt 或性能。后续各 owner PR 必须提供独立 model、deterministic integration 和 fault oracle。
