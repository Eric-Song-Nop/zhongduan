# MVP 架构：CURRENT protocol v2 runtime inventory

> 状态：本文只记录 PR #24 head（`5d7511782542de511292d25908b8d92be4319636`）对应的
> **CURRENT protocol v2** 实现事实，包括现有缺陷。它不是产品 acceptance gate、future protocol
> 或实施 roadmap。CURRENT 文档与该基线代码冲突时以代码为准；稳定产品边界见
> [产品契约与协议边界](terminal-protocol-architecture.md)，阶段与完成条件见
> [MVP 路线](mvp-roadmap.md)，输入 TARGET 细节见[输入核心实施计划](input-core-plan.md)。

## 运行时范围

CURRENT runtime 在 Linux Host 上启动持续运行的 PTY 和 child process，并允许 Browser 经
Cloudflare Worker / Durable Object 附加与重连。v2 使用三条传输面：

```text
control WebSocket    attach / lease / input / ACK / resync
data WebSocket       PTY_OUTPUT / RESIZE_APPLIED / directed replay / barrier / commit
snapshot HTTP        immutable compressed Ghostty snapshot
```

```text
Host daemon
  PTY + child process
  authoritative libghostty core
  ordered journal
  reusable Ghostty snapshot
          |
          | ordered PTY/resize mutations
          v
Cloudflare Worker + TerminalSessionDO + R2
          |
          v
Browser wterm
  passive libghostty replica
```

## CURRENT owner 与生命周期

| 执行 owner                                                           | 当前拥有的状态与工作                                                                                                           | 当前串行化边界与生命周期                                                                                                                                              |
| -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Host `TerminalSession` actor                                         | PTY、authoritative Ghostty core、`eventSeq`、`nextPtyOffset`、journal append、writer fence 和 input result cache               | PTY output、semantic input、resize、query response 和 snapshot capture 共用一个同步队列；生命周期等于本地 PTY session                                                 |
| Host `HostRelayConnection` / `HostDeliveryScheduler`                 | 一对 Cloud control/data socket、canonical publisher、attach recovery queue、一个 active recovery、一个 delivery barrier waiter | control frame 共用一条 Promise chain；recovery attempt 逐个执行；relay 重连不终止 PTY                                                                                 |
| Cloud `TerminalSessionDO`                                            | session/client row、host fence、writer lease、ticket、snapshot/upload metadata、WebSocket 协调和 delivery cursor               | 同一 DO 的 Browser control、Host control 和 Host data inbound work 共用一个 `BoundedSerialQueue`；SQLite 与 Hibernation WebSocket attachment 跨 DO 唤醒保留           |
| Browser `TerminalSession` / `SessionCoordinator` / `InputDispatcher` | connection/delivery generation、live replica、detached restore candidate、recovery tail、input epoch/sequence 和 pending ACK   | 每个页面实例拥有本地状态；data-only replacement 只 fence 旧 delivery；full/control replacement 还会 detach input transport，并把未决 input 的聚合状态标为 `uncertain` |

Cloud 的 Browser data WebSocket attachment 当前会序列化 `deliveryGeneration`、`dataState`、
first/acked/sent event cursor 与 PTY offset、`replayMode`、`snapshotId` 以及 pinned
`replayCommitEventSeq` / `replayCommitPtyOffset`。这使 v2 delivery pin 和 cursor 可跨 Durable Object
hibernation 继续；它只是 CURRENT 的持久化手段，不表示未来 recovery attempt 具有相同 durability 要求。

## Authority 与 canonical cursor

Host 是唯一终端设备权威。`TerminalSession` 按同一 actor 顺序把 PTY output 应用到 authoritative
Ghostty core、处理 resize、生成 terminal query response，并把语义 input 编码后写入 PTY。Browser core
固定使用 `effects: discard`，不会回答 DA/DSR、写入 PTY，或重放 bell、clipboard 等瞬时副作用。

CURRENT cursor 是：

```text
{
  sessionEpoch,
  lastEventSeq,
  nextPtyOffset
}
```

`engineId` 在 session/attach/snapshot metadata 上独立校验。`eventSeq` 只由
`PTY_OUTPUT` 和 `RESIZE_APPLIED` 消耗；两类 mutation 共用同一序列。`PTY_OUTPUT` 同时推进
`nextPtyOffset`，resize 不推进 PTY offset。Control ACK、writer lease 和 input receipt 不进入 terminal
journal，也不改变该 cursor。

`TerminalSession` 先 transition Ghostty state，再构造并 append 连续 frame；journal append 或后续 actor
步骤抛错时 session fail closed。Cloud 只验证和转发 Host 已给出的连续 canonical cursor，不成为 terminal
authority。

## CURRENT input 路径与结果

Browser `InputDispatcher` 把 wterm semantic event 转为 control frame；`TerminalSessionDO` 验证当前
connection、writer lease 和 host fence 后转发；Host `TerminalSession` 最终决定是否调用 `pty.write()`
或 authoritative resize。CURRENT wire 使用以下 status：

| CURRENT event/status                           | 当前能证明的事实                                                            | 对产品结果分类的当前限定                                                                          |
| ---------------------------------------------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Browser 在无 sender 或 writer lease 时提前返回 | 没有分配 sequence，也没有进入 transport；但没有逐 event 的稳定本地结果      | 内部事实是 `not-sent`，当前 UI surface 未完整暴露该分类                                           |
| `written`                                      | Host commit 调用返回且没有抛错                                              | 在 result-retention 范围内是 `deterministic`；不证明 PTY slave、child 或 application 已读取或提交 |
| `duplicate`                                    | 相同 identity 的已写结果仍在 Host cache，未再次施加 PTY effect              | 只在同一 writer fence/input epoch 的保留窗口内是 `deterministic`                                  |
| `rejected`                                     | Cloud 或 Host 在调用该 attempt 的 commit 前拒绝                             | owner 能证明未施加 PTY effect 时是 `deterministic`                                                |
| `uncertain`                                    | Cloud 向 Host send 抛错、Host commit 抛错，或旧 sequence 的结果已不在 cache | 当前实现不能证明 effect 是否发生；未决 input 不会因 transport replacement 自动重发                |

Host 默认只保存当前 writer fence / input epoch 最近 4096 个 `written` 或 `uncertain` 结果。writer fence
前进时 cache 清空；已低于 high-water 但结果被淘汰的 sequence 返回 `uncertain`。CURRENT Host 检查第一个
sequence 必须为 1，并处理 cache 内 duplicate，但会接受任意大于 high-water 的 sequence，因此并不是 strict
contiguous input stream。

Browser 也尚未统一做到 validate/admit 后才分配 sequence：`#toFrame()` 先递增 sequence，再做 schema
parse 和 transport send；local validation 失败或 send 返回 false 都会留下 sequence gap。输入队列当前最多
保留 256 个 event，pending ACK map 最多保留 1024 个 identity；pending map 超限会淘汰最旧项并把聚合
status 标为 `uncertain`。

两个现有 state-intent 特例也是 CURRENT 行为：

- `latestResize` 在没有 transport 时仍会更新；新 transport attach 后会把最新尺寸作为新的
  `resize-request` 再发送。Browser 在收到相同 authoritative resize 前关闭 mouse gate。
- 只有相邻的 mouse `move` 会合并；key、press、release 等有序 input 不会被跨越。replica 不 current 或
  resize 尚未确认时，mouse event 直接返回，目前没有逐 event 的本地结果。

## 实时转发与恢复路径

健康连接直接转发有序原始 PTY bytes。`PTY_OUTPUT` 和 `RESIZE_APPLIED` 共用一个二进制 data WebSocket
和同一个 `eventSeq`，所以 resize 在 canonical mutation stream 中保留其 reflow 位置。

恢复有两条路径：

1. 页面保留相同 `engineId` / `sessionEpoch` 的 live core，且 Host journal 覆盖缺口时，使用 warm tail。
2. 页面没有可用 live core、journal gap 或慢客户端被 reset 时，加载 Host Ghostty snapshot，再应用
   snapshot cut 之后的 cold tail。

Ghostty snapshot 是 checkpoint，不是事件传输。它包含 cut 之前尚未完成的 VT/UTF-8 parser
continuation；tail 从 snapshot 的 `nextPtyOffset` 开始，不能重复 continuation bytes。Browser cold restore
在 detached candidate 中执行，CURRENT manifest 固定 `restoreThrough: finish`，通过 replay commit 后才
`adoptCore()`。

### v2 pause / barrier / pin 的实际线性交接

CURRENT warm 与 cold recovery 共用 fixed commit、delivery barrier、pinned delivery 和 global canonical
publisher pause：

```text
Host pause canonical publisher
  -> choose base R and current fixed commit C
  -> flush already-canonical frames through C to Cloud
  -> send DeliveryBarrier(stream, generation, mode, C[, snapshotId])
  -> Cloud pin Browser data attachment at C and return ready
  -> Host send directed exact tail (R, C]
  -> Host send ReplayCommit(C)
  -> Host resume canonical publisher
```

`pause()` 只停止该 relay connection 的 canonical publisher pump；PTY、Host authority 和 journal 继续
前进，新 canonical frame 堆积在 Host publisher queue。Host 在发送 barrier 前固定 `C`，Cloud 只在 barrier
等于当前 canonical head、Browser generation/current sockets 匹配且 data cursor 仍未推进时建立 pin。

pin 写入 Browser data WebSocket attachment 后，Cloud 只接受不超过 pinned commit 的 directed tail 和精确
匹配的 `ReplayCommit(C)`。只要存在 `catching-up` 的 pinned delivery，Cloud 收到新的 stream-0 canonical
frame 就会 fail current Host；因此 Host 必须保持 publisher pause。`ReplayCommit` 把该 Browser data state
推进为 `synced`，随后 Host 才 resume publisher。barrier marker 的结果若变成 uncertain，Host 会关闭整个
relay pair，而不是猜测 pin 是否已经建立。

Host 同一时间只运行一个 recovery attempt 和一个 barrier。warm admission 或现有 checkpoint 不可用时，
cold snapshot preparation 也只有一个 build in flight；pending attach 每个 Browser stream 只保留最新
delivery generation。

## CURRENT queue 与资源上限

这些数字是 PR #24 实现中的保护条件和默认值，不是 future wire compatibility 保证。

| owner / resource                            | CURRENT 默认上限                                                             | 超限或到期行为                                                                  |
| ------------------------------------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| Host journal segment                        | 256 KiB 或 250 ms                                                            | seal segment                                                                    |
| Host journal retention                      | 60 s / 8 MiB                                                                 | 按已 seal segment 淘汰；ACK 不延长 retention                                    |
| Host relay inbound control chain            | 8 MiB / 64 frames                                                            | 关闭 Host relay pair                                                            |
| Host control WebSocket send buffer          | 1 MiB                                                                        | 关闭 Host relay pair                                                            |
| Host canonical publisher queue              | 8 MiB / 1024 frames                                                          | barrier 前可中断并重排 recovery；marker/pin 后或继续超限时关闭 relay pair       |
| Warm replay 与 cold snapshot tail admission | 各 256 KiB / 512 frames                                                      | warm 转 cold，或使 checkpoint/tail 不可用后重试                                 |
| Cloud inbound serial queue（全 DO）         | 32 MiB / 2048 tasks                                                          | Host 超限会 fail current Host；Browser 超限关闭该 Browser                       |
| Cloud per-socket inbound profile            | Browser control 16 MiB / 8；Host control 1 MiB / 64；Host data 16 MiB / 1024 | 拒绝该入队并按 peer 执行上述关闭策略                                            |
| Browser delivery outstanding                | 512 KiB                                                                      | reset 该 Browser data delivery 并推进 `deliveryGeneration`；control socket 保留 |
| Browser recovery tail                       | 2 MiB / 1024 frames                                                          | 请求 resync                                                                     |
| Browser restore deadline                    | 5 s                                                                          | 放弃 candidate 并请求 resync                                                    |
| Browser connections per session             | 16                                                                           | 新 reservation 返回容量错误                                                     |
| Host input result cache                     | 4096 entries                                                                 | 淘汰最旧结果；旧 identity 只能返回 `uncertain`                                  |
| Snapshot checkpoint metadata cache          | 30 s                                                                         | 过期后重新准备 cold checkpoint                                                  |
| Snapshot recovery quiet / max quiet wait    | 250 ms / 5 s                                                                 | 延后 cold capture，但最多等待当前上限                                           |
| Snapshot encode budget / publish timeout    | 5 s / 120 s                                                                  | 本次 preparation 失败或重试                                                     |
| Snapshot build minimum interval             | 1 s                                                                          | 延后下一次 build                                                                |
| Snapshot blob                               | 32 MiB compressed / 128 MiB uncompressed                                     | 拒绝 snapshot                                                                   |
| Cloud snapshot/upload rows                  | 32 total / 4 pending；latest + active pins + 2 recent 受保护                 | maintenance 回收未受保护对象                                                    |

Host `TerminalSession` 的同步 actor queue 没有独立 bytes/count hard limit。当前调用通常在同一个 JavaScript
turn 内立即 drain，但 snapshot encode 也在这个 owner 内同步执行；表中的 5 s encode budget 是完成后
检查和外层 deadline，不能抢占正在运行的同步 Ghostty encode。

## 已知实现缺陷

以下是 CURRENT inventory，不是 future design 或完成标准：

- Cloud 的一个 `BoundedSerialQueue` 为所有 inbound socket 共用同一 Promise tail。即使 bytes/count 有界，
  Browser input、lease、ACK 和 attach 的 queue wait 仍会随排在前面的无关 Host bulk work 增长。
- recovery pause 覆盖整个 Host-to-Cloud canonical publisher，并且 recovery 逐个执行。一个 Browser recovery
  会延迟所有 Browser 的新 canonical output；期间 input 即使已写入 PTY，其 echo 也只能先进入 Host queue。
- synchronous snapshot encode 与 PTY output、resize 和 semantic input 共用 Host authority actor，可能直接
  阻塞 input hot path；当前 timeout 不能抢占同步 encode。
- Browser 的无 transport/lease、mouse gate 和 coalescing 路径没有完整的逐 event 可见结果；sequence 又在
  schema validation 和 send admission 之前分配。
- Host input 只有 high-water 与有限 result cache，不拒绝 high-water 之后的 gap，因而不能证明 strict
  contiguous input prefix。
- overload 隔离并不一致：per-Browser delivery credit 超限只 reset 该 Browser，但 Cloud global inbound、Host
  canonical queue 或 barrier marker uncertainty 会关闭整个 Host relay pair。

## 其他 CURRENT 边界

- capability token 区分 host、writer 和 observer，并绑定 session、角色与过期时间。
- writer lease 使用 fencing token；同一 session 同时只有一个 writer 可以输入和 resize。
- Cloud 是可信 relay，传输依赖 TLS；CURRENT 没有端到端加密或 zero-knowledge relay。
- `engineId` 必须精确匹配。Ghostty snapshot envelope 的 `version=1` 本身不提供跨 engine compatibility。
- journal 不保存无限 scrollback；snapshot 不恢复 shell、Vim 或其他 child process 内存，daemon crash 后也不
  恢复原 child process。
- CURRENT 不提供 CRDT、多 writer merge、recording playback、Kitty graphics recovery 或跨 `engineId`
  snapshot migration。
