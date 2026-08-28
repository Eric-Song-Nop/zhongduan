# 输入稳定性与高 RTT 交互实施计划

> 状态：方向已确认，等待实施
>
> 决策日期：2026-08-28
>
> 适用范围：Browser 输入、Cloud relay、Host session actor、PTY、Ghostty/WTerm replica 与 DOM renderer
>
> 协议总纲：[终端协议架构与核心不变量](terminal-protocol-architecture.md)
>
> 相关计划：[高性能远程终端 Snapshot 恢复实施计划](high-performance-snapshot-recovery-plan.md)

## 最高优先级不变量

> 客户端终端当前采用的状态，永远等于某个 immutable checkpoint，加上同一 authority mutation log
> 的一个连续前缀；所有输入预测都只是这个状态之上的可丢弃 presentation branch。

输入方案必须服从[协议总纲](terminal-protocol-architecture.md)定义的三个平面：

| 平面                 | 输入相关职责                                                     | 持久化与重放                                                  |
| -------------------- | ---------------------------------------------------------------- | ------------------------------------------------------------- |
| Terminal state plane | PTY output、已应用 resize 等可恢复 terminal mutation             | 进入 journal/snapshot/tail；只有这些 mutation 消耗 `eventSeq` |
| Interaction sideband | input result、validation cut、context token、writer coordination | 预测验证只 live，不进入 terminal log                          |
| Delivery plane       | connection、delivery generation/ordinal、flow control 与路由     | 不代表 terminal apply，也不改变 authority state               |

因此 input ACK、`InputValidationCut`、writer 状态和 context token 都不能仅为获得顺序而进入 canonical
terminal stream。

## 结论

Zhongduan 始终是终端，不为 Codex、Claude Code 或其他应用另做一套 RPC 客户端。真实应用继续运行在
PTY 后面，Host 的 Ghostty terminal 始终是唯一权威；Browser 只在这个边界内改善输入反馈。

输入稳定性采用三种明确模式，而不是一个对所有应用都冒险的“本地回显”开关：

| 模式       | 谁拥有待编辑内容 | 输入何时写入 PTY                    | Browser 能做什么                 | 适用范围                                                    |
| ---------- | ---------------- | ----------------------------------- | -------------------------------- | ----------------------------------------------------------- |
| `Mirrored` | 远端应用         | 每个按键立即写入                    | 非权威、可撤销的本地视觉 mirror  | MVP 为窄 ASCII shell/REPL；TUI 需后续 driver/context/opt-in |
| `Raw`      | 远端应用         | 每个事件立即写入                    | 不预测，只显示 canonical output  | 密码、审批/modal、不确定上下文、控制键和不支持状态          |
| `Owned`    | Zhongduan        | 提交时按一个受 fence 保护的批次写入 | 编辑反馈不依赖 RTT，提交仍走网络 | 仅限明确 ownership transfer/integration                     |

这三个 mode 决定 buffer ownership 和 presentation，不等于网络 delivery policy。后者与 mode 正交：

| Delivery policy   | 语义                                                                            |
| ----------------- | ------------------------------------------------------------------------------- |
| `urgent-stream`   | Ctrl-C 等必须尽快到 Host，但仍保持 authority 所需的 PTY/output 顺序             |
| `ordinary-stream` | 普通 edit 立即 pipeline，不等待 ACK                                             |
| `guarded-context` | Host 只对它已经观察到的 terminal context 做 compare-and-write；不匹配则不写 PTY |

`Raw` 只表示“不预测”，本身不能阻止 RTT 途中应用切换 modal。高风险动作在存在可靠 token 时应同时使用
`guarded-context`；没有 token 时仍存在普通远程终端固有的 context race。

这三个 mode 也不决定 catching-up 时是否发送。replica freshness 使用独立 input policy；关闭 prediction
不能冒充 context safety。

`libghostty` 的价值是精确计算“如果应用按某种方式显示这个编辑，cells、cursor、grapheme 和 wrap
会是什么”，而不是猜应用下一步的业务意图。应用是否会回显、Enter 当前是提交还是批准、Vim 当前是否
处于 INSERT mode，单靠 VT parser 都无法知道。因此：

- 普通编辑可以积极地镜像，但每个 key 仍立即流向 PTY；
- Enter、Tab、Esc、Ctrl、function key、mouse 和 approval 等高风险动作默认不预测、不延迟提交、
  不自动重放；
- 预测层只影响 presentation，清掉它之后，权威 terminal 必须和“功能完全关闭”时逐字节相同；
- AI 不进入正确性路径和输入热路径。将来可以辅助离线编写 adapter，但运行时不依赖它；
- snapshot 用于恢复和 rebase，不用于逐键 fork，也不负责降低健康连接的输入 RTT。

MVP 先让支持矩阵内的窄 ASCII shell/REPL 编辑反馈在本地一帧内完成；后续只有通过 deterministic driver、
稳定 context 和显式 opt-in 的 TUI composer 才扩大可见预测。commit、执行和 canonical confirmation 仍受
网络影响。本计划不承诺任意 TUI 动作都能在本地确定执行，也不宣称在所有维度“全面超过 Mosh”。

## “输入稳定”具体指什么

这里的稳定性不是单一延迟数字，而是以下性质同时成立：

1. **Effect safety**：在 live fence/dedup 窗口内不重复执行已知输入；结果为 `uncertain` 时系统绝不自动重写。
2. **Context safety**：对 Host 已观察到并可 fence 的状态，旧 prompt/modal/screen 上的危险输入不落到新上下文；
   对尚未输出的应用内部状态不作虚假保证。
3. **Feedback latency**：普通编辑在高 RTT 下可以立即显示；Ctrl-C 等控制输入不会排在历史输出或恢复之后。
4. **Authority isolation**：任何预测错误都只造成短暂视觉回滚，不改变 PTY、Ghostty authority、snapshot、
   journal、复制/选择、ARIA 或日志。
5. **Recovery safety**：断线或 generation 变化后不自动重放不确定输入，本地 draft 也绝不自动提交。
6. **Privacy**：已知 password/secure-input 状态不显示未经确认的文本；未知状态的不可消除边界必须显式说明；
   遥测不记录输入内容。
7. **Boundedness**：input queue、pending ACK、prediction epoch、overlay patch 和回滚时间均有上限。

## 当前输入路径与延迟下界

当前 semantic input 的真实路径是：

```text
Browser keydown
  -> Browser control WebSocket
  -> Cloud Durable Object
  -> Host control WebSocket
  -> Host session actor
  -> Ghostty encodeKey(current authority modes)
  -> pty.write(...)

PTY/application output
  -> Host Ghostty authority
  -> canonical data WebSocket
  -> Cloud commit + fanout
  -> Browser Ghostty replica
  -> DOM paint
```

没有本地预测时，第一次可见回显至少需要四个单向网络段：

```text
Browser -> Cloud -> Host -> Cloud -> Browser
```

也就是一个逻辑 Browser↔Host relay RTT，外加 Cloud/Host queue、应用处理、replica apply 和浏览器绘制。
Snapshot 无法缩短健康连接上的这个下界。

### 当前已经有的正确性基础

- 端到端经 Cloud 归一化后，Host 用 `(writerFence, inputEpoch, clientInputSeq)` 标识输入；Browser 只提交
  opaque writer lease 与 epoch/seq，不自报可信 fence。control transport 变化或新 input epoch 会把
  outstanding input 标为 `uncertain` 并清空待发送队列，不盲目重放。data-only delivery
  generation/resync 当前只把 replica 标为不 current，并不会自动更换 input epoch；prediction 层必须另行 reset。
- Cloud 每次只接受当前 writer lease，并把可信的 `connectionId/clientId/writerFence` 注入 Host control frame。
- Host 在单一 session actor 中单调检查 writer fence、client、input epoch 和 seq，保留有界 duplicate
  suppression 结果。当前 seq 允许 gap，不是 contiguous/exactly-once log。
- 已记住的输入返回 `duplicate`；太旧且已逐出 dedup window 的输入返回 `uncertain`，不会假定安全重写。
- semantic key 在 Host 依据**当前权威 Ghostty modes**编码。Browser 落后时不会用旧 cursor-key、keypad 或
  Kitty keyboard mode 生成最终 PTY bytes。
- writer fence 和 relay-pair fence 能阻止旧连接的输入或 ACK 冒充当前连接。

这些性质提供的是 live session 内有界的 at-most-once/effect-safe 输入，不是跨 Host crash 的
exactly-once。PTY 不是事务数据库；`pty.write` 返回之后 Host 若崩溃，恢复方无法证明 child 是否已消费。

### `input-ack: written` 的准确语义

当前 `written` 只表示：

1. Host 已验证 fence/epoch/seq；
2. Ghostty 完成 semantic encoding；
3. 如果 encoding 非空，同步 `pty.write(...)` 没有抛错；key release、disabled focus/mouse 等事件可能
   encoding 为空，此时根本不会调用 `pty.write`；
4. dedup 状态已记住这个结果。

因此当前 `written` 名称还混合了“写入 bytes”和“成功处理但没有 bytes”。它**不表示** PTY slave 或应用已经读取，
不表示应用产生了效果，也不表示 Browser 已经显示回显。协议演进时应拆成 `written` 与
`handled-no-bytes`，或统一改名为不暗示 I/O 的 `accepted` 并另带 byte count。
ACK 中的 `authorityEventSeq` 是写入时的 Host head，是前置 watermark，不是 echo/effect ACK。ACK 与
canonical data 又使用不同 WebSocket，因此二者没有跨通道总顺序。

当前 journal/snapshot/tail 只保存 terminal mutation（PTY output 与 resize），不保存 input，也不能在恢复后证明
应用消费过哪些 key。后文的 `InputValidationCut` 是 writer-only、live-only 的 sideband causal
certificate；它不携带 input bytes、不进入 terminal log，也不能消除 ACK 丢失或 Host crash 后的 effect
uncertainty。

预测确认必须来自后续 canonical data cut 上的兼容性验证，不能把现有 ACK 当成视觉确认或业务确认；即使
画面相同，也不一定足以提高 predictor 的置信度。

## 当前代码中已确认的风险

以下是 2026-08-28 当前实现事实，不是未来设计：

| 风险                      | 当前实现                                                                                                   | 直接影响                                           |
| ------------------------- | ---------------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| Cloud 全局 HOL            | Browser control、Host control 和 Host data 共用一个 `BoundedSerialQueue`                                   | output/SQLite/fanout 可以把 key 或 Ctrl-C 排在后面 |
| 每键续 writer lease       | 每个 input 都执行 token SHA-256、lease read/update；Browser 又每 10 秒单独 heartbeat                       | 增加 CPU、SQLite 和队列尾延迟                      |
| Host actor 同步 snapshot  | `encodeSnapshot()` 与 input、PTY output 在同一个 actor 中执行                                              | 大 snapshot 可以直接阻塞 key→`pty.write`           |
| 恢复全局 pause canonical  | warm/cold delivery 在 barrier/replay 周围 pause publisher                                                  | 一个 Browser 恢复可能延迟另一个 writer 的可见回显  |
| context fence 太弱        | 只有 key 带 `observedEventSeq`，Host 只拒绝“观察到未来”，接受 stale；paste/text 没有该字段                 | 旧 UI 上产生的输入可以落到新 modal                 |
| 无 termios/context 信号   | `PtyProcess` 没有 ECHO/ICANON generation，WTerm 也没暴露 app focus                                         | 无法可靠区分 password、shell edit、TUI raw mode    |
| renderer 无 overlay 层    | DOM renderer 只从一个 `TerminalCore` 重建 dirty rows                                                       | 预测若直接写 core 会污染 authority replica         |
| 非法首事件可卡死 epoch    | Browser 在 schema parse 前递增 seq；若首事件无效，下一条从 seq=2 发出，而 Host 建立新 epoch 要求首条 seq=1 | 一个本地坏事件可让后续合法输入持续被拒绝           |
| 无 transport 时静默吞输入 | dispatcher 没有 writer transport/lease 时直接 return，DOM sink 已 `preventDefault`，也没有逐事件失败反馈   | 用户以为已经输入，实际上事件没有排队也没有送达     |

关键源码入口：

- Browser 输入 lifecycle/seq/ACK：[`input-dispatcher.ts`](../apps/terminal-cloud/src/browser/input-dispatcher.ts#L102-L315)
- Cloud lease 与输入转发：[`terminal-session-do.ts`](../apps/terminal-cloud/src/worker/terminal-session-do.ts#L978-L1099)
- Cloud 全局串行 queue：[`relay-message-queue.ts`](../apps/terminal-cloud/src/worker/relay-message-queue.ts#L20-L64)
- Host actor、input/dedup/ACK/PTY write：[`session.ts`](../apps/host-daemon/src/session.ts#L333-L569)
- Host canonical pause/queue：[`canonical-publisher.ts`](../apps/host-daemon/src/cloud/canonical-publisher.ts#L80-L165)、
  [`delivery-scheduler.ts`](../apps/host-daemon/src/cloud/delivery-scheduler.ts#L315-L639)
- 固定 WTerm fork 的
  [Ghostty input/mode ABI](https://github.com/Eric-Song-Nop/wterm/blob/4764f3b5e81cb76cdb08d4ffbfba1257fd33efd6/packages/%40wterm/ghostty/zig/src/wasm_api.zig)、
  [DOM renderer](https://github.com/Eric-Song-Nop/wterm/blob/4764f3b5e81cb76cdb08d4ffbfba1257fd33efd6/packages/%40wterm/dom/src/renderer.ts)

## 外部方案给我们的启发

### SSH 与 `ssh + tmux`

SSH 在一条健康连接内依靠 TCP 和 SSH integrity 保护有序 byte stream，但新连接没有 Zhongduan 的
snapshot/tail、Browser replica generation 或 PTY input dedup。`ssh + tmux` 的断线连续性来自远端
tmux 持有会话并在重连时重绘，不是 SSH 自己恢复旧 channel。

SSH 给我们的基线是：输入顺序和鉴权必须可靠；它没有解决高 RTT 的本地编辑反馈。

### Mosh

Mosh 的两个核心价值是网络漫游/state synchronization，以及 epoch-based speculative echo。预测先隐藏，
远端确认过相同类型的 echo 后才显示；Left/Right 是常见可预测项，而 Up/Down、Esc、Enter 和未知控制输入会
降低或重置信心。

Zhongduan 应采用它的**可撤销 epoch 和在线验证**，但当前 Browser/Cloud 部署不照搬 UDP roaming 协议：
WSS、session identity、writer fencing、reconnect 和 snapshot/tail 解决的是本项目的会话恢复边界，并不等价于
Mosh 的 SSP、快速 path migration 或避开 TCP head-of-line。UDP roaming 对直连移动网络仍有价值，只是不是
当前部署的优先问题。我们还拥有相同版本的 Ghostty authority/replica，可以比字符级预测更准确地计算
grapheme、宽字符、wrap、cursor 和 terminal mode。

Mosh 也不能告诉我们 Codex 的 Enter 是 submit 还是 approval；这个边界仍然存在。

### Warp

Warp 的 shell prompt 输入由本地 editor 持有，提交时清理远端 line-editor buffer，再用 bracketed paste
发送完整 command，最后发送 Enter。它会依赖 shell integration 的 prompt/preexec 状态并保留一个短暂
竞态窗口；如果 `preexec` 先到，就取消写入。进入 active command 或 TUI 后则回到原始 PTY 输入。

这证明 `Owned` shell buffer 能让编辑反馈不依赖 RTT，但代价是 Zhongduan 需要承担 history、completion、
keybinding、multiline、IME 和 shell 配置兼容。它只应该在明确集成成功时启用，不能通过“看起来像 prompt”猜测。

### Ghostty / WTerm

Ghostty output parser 能精确处理 cells、cursor、SGR、margins、pending wrap、Unicode grapheme、wide cell、
alt-screen、mouse/keyboard modes、synchronized output，以及应用发出的 OSC 133 semantic prompt。

它不知道 termios ECHO/ICANON、shell/readline/Vim 的内部状态，也不知道一段 output 是内核 echo、应用 redraw
还是无关后台输出。输入编码后的 `ESC`/CSI-u/Alt/arrow bytes 是 **PTY input**，不能喂回 VT output parser。

当前 WTerm 适合做权威 replica，但还不适合逐键 fork：Ghostty snapshot encoder 包含完整 history、同步执行，
没有 READY-only encoder 或完整 `Terminal.clone/COW`；现有 `Screen.clone` 也不是完整可执行 terminal clone。

因此先做 cell presentation overlay，之后在 Ghostty fork 增加无 effects、无 history 的 lightweight active-state
shadow 或专用 `predictSemanticEdit` API，而不是每键 encode/restore 一份完整 snapshot。

### Codex、Claude Code 和其他现代 TUI

这类应用的 composer 是应用层状态机：multiline、history、Vim mode、paste burst、autocomplete、slash/file
picker、queued message、approval modal 都会改变同一个 key 的意义。Codex 自己会延迟 approval 切换，避免刚刚
输入的 typeahead 穿透；它还用 synchronized output 包住完整 Ratatui frame。Claude Code 官方文档也把
Chat、Autocomplete 和 Confirmation 划成不同 key context。

对它们最合适的是积极的 `Mirrored` 模式：

- printable/backspace/有限 cursor movement 可以立即画 overlay，同时每键立即发 PTY；
- synchronized-output begin 时冻结或隐藏 overlay，end 后只和完整 canonical frame 对账；
- popup、geometry、cursor region、mode 或 screen generation 改变时重置 epoch；
- Enter、Tab、Esc、Ctrl、permission 的 Y/N 和 mouse 默认走 `Raw`；
- 不在 Browser 攒完整消息后盲目 `paste + Enter`，因为批次到 Host 前 modal 可能已经切换。

Codex app-server、Claude SDK/Remote Control 或重做一套原生 agent frontend 都不属于本计划。它们可能是别的
产品，但会绕过用户正在运行的真实 TUI、插件、shell 环境和 PTY 行为，违背 Zhongduan 的终端边界。

## 最终架构

```text
                         authoritative path
Browser semantic input -------------------------------> Cloud -> Host -> PTY
        |                                                        |
        |                                                        v
        +-> PredictionController                           Ghostty authority
                 |                                               |
                 v                                               v
           presentation overlay <--- verifier <--- canonical Ghostty replica
                 |
                 +-- disposable; never enters core/snapshot/copy/log/network
```

系统里只能有一条 effect path：semantic input 经 Cloud/Host 写 PTY。预测分支没有 `WRITE_PTY` handler，不能
生成 terminal query response，也不能修改 active/detached replica。

### 权威层与展示层

Browser 保持三份逻辑对象：

1. `AuthoritativeReplica`：只消费 canonical output、snapshot 和 exact tail；现有 active/detached candidate
   语义保持不变。
2. `PredictionController`：保存当前 mode、epoch、输入 seq、base cursor/context、预期 cell patch 和置信状态。
3. `PresentationOverlay`：renderer 合成时最后覆盖少量 cells/cursor；不拥有 scrollback 或 terminal effects。

强制不变量：

- `clearOverlay()` 前后，`AuthoritativeReplica` 的 snapshot/hash/eventSeq/ptyOffset 完全不变；
- overlay 不出现在 selection、copy、search、screen reader、OSC hyperlink、日志、snapshot 或 replay；
- resize、engine/session epoch、delivery generation、snapshot adoption、replica swap、parser continuation 和
  sync-output generation 变化都是 reset/rebase boundary；
- overlay 有 cells、rows、bytes、inputs 和 age 上限；超限立即退回 `Raw`；
- prediction crash/timeout 只能关闭预测，不能阻断真实 input。

## 有序输入基础：先 validate/admit，再分配 seq

`InputValidationCut`、urgent lane 和预测前缀都依赖 Host 准确知道“连续处理到了哪个 input seq”。所有预测
之前，先把同一 `(writerFence, inputEpoch)` 的 semantic input 改成严格连续的 ordered stream。

Browser pipeline：

```text
DOM semantic event
  -> 完整 schema/size/policy validation
  -> 生成合法、不可变的 semantic event
  -> per-writer sequencer admission
       - 当前不允许发送：not-sent，不分配 seq
       - 允许发送：原子分配 next seq 并进入有界发送队列
  -> Cloud control connection
```

不得在 schema parse、IME normalization、paste sanitization、writer capability 或 catching-up policy 之前递增
seq。UI 已消费的每个事件都必须得到逐事件结果：

| 结果            | 定义                                                              | 自动重发                                 |
| --------------- | ----------------------------------------------------------------- | ---------------------------------------- |
| `queued`        | 已分配 seq，仍在当前 connection 的有界队列                        | 仅在同一确定性 connection 状态内继续发送 |
| `sent`          | frame 已交给当前 control transport                                | 否；等待 result                          |
| `not-sent`      | 未 admission、被取消或当前没有安全 transport；未写 PTY            | 永不                                     |
| `uncertain`     | 已可能离开 Browser，但无法得到可证明的确定结果                    | 终止 epoch，永不                         |
| `missing-input` | Host 观察到 seq gap；该 frame 未执行，但 earlier input 状态不完整 | 终止 epoch，永不                         |

Host 为每个 writer epoch 保存 `nextExpectedSeq`、有界 `seq -> deterministic result` cache 和
`active | uncertain | closed` 状态：

```text
seq == nextExpectedSeq:
  在 session actor 中处理
  记录确定性结果
  nextExpectedSeq += 1

seq < nextExpectedSeq:
  cache 内存在 -> 返回完全相同的 duplicate result
  cache 已逐出 -> uncertain，终止 epoch

seq > nextExpectedSeq:
  missing-input(expectedSeq)；不处理、不推进，终止 epoch

无法判断 PTY effect:
  uncertain；终止 epoch
```

会消费当前 seq 的确定性结果包括 `written(bytes)`、`handled-no-bytes`，以及有效 writer/epoch 下由 authority
得出的 `rejected(reason)`，例如 `stale-context`。错误 capability/fence/epoch 与 malformed envelope 尚未进入
该 ordered stream，不消费 seq。gap 是 protocol violation：Host 返回 `missing-input(expectedSeq)` 并关闭该
input epoch；Browser 将所有未决 input 视为 uncertain，不自动补发高 seq 或猜测缺失帧，只有明确放弃后才能
建立新 epoch。result 不是 child-consumed/effect ACK。

key、text、paste、focus、mouse 等 semantic input 共享单一 per-writer sequencer。urgent/ordinary 可有不同
物理 lane，但不能改变已 admission input 的顺序。Ctrl-C 可越过 snapshot、recovery、bulk fanout 和无关
storage；不能越过已分配 seq 的 earlier input。如果要取消尚未 admission 的本地 edit，必须先将其标成
`not-sent/cancelled`，再给 Ctrl-C 分配 seq。

writer lease 改为 connection-scoped capability：Cloud 在 control connection 建立时验证 token，把
`clientId/writerFence/expiresAt/selectedCapabilities` 固定到可信 WebSocket attachment；input fast path
只检查 attachment 和 cached fence，heartbeat 有界频率续租，不再每键 hash 和 storage update。无法从
attachment/storage 确认时 fail closed。

## `Mirrored`：应用仍拥有 buffer

在 `Mirrored` 中，每个输入都立即走现有 semantic PTY path；Browser 只预测视觉结果。推荐 epoch 状态机：

```text
disabled
  -> learning-hidden
  -> visible-compatible
  -> validating
  -> confirmed / reset
```

- 新 epoch 先在后台预测，不显示；canonical output 连续匹配同类编辑后才提高置信度。
- `visible-compatible` 中可即时显示普通 printable、Backspace，以及已证明安全的 Left/Right。
- 每个 patch 绑定 base event cursor、screen context、semantic input seq 和 expected cell digest。
- canonical verification cut 与预期相符则退休已确认 patch；冲突则在一帧内清 overlay、降低 driver 置信度并
  继续显示权威画面。
- application driver 只能缩小 eligible action 集，不能改变权威 input bytes 或跳过 Host。
- AI 不参与判断；driver 是版本化、确定性、可离线回放测试的规则。

第一版只支持 ASCII printable、单 cell cursor、无 pending wrap 的 Backspace。之后才逐项开启 combining mark、
wide glyph、wrap、multiline、word motion 和 app-specific region。每项都需要独立 compatibility gate。

### 确认、兼容和置信度

Control ACK 可以越过 data，`written` 又只证明 Host 处理了 input，且 encoding 非空时调用 `pty.write` 未抛错。
因此 overlay 不能看到 ACK 或任意一帧相同画面就宣布“应用已确认”。允许开始验证的是协商后的、
writer-only、live-only、non-replayable interaction sideband：

```text
InputValidationCut {
  writerFence,
  inputEpoch,
  settledThroughContiguous,
  authorityCut: {
    sessionEpoch,
    eventSeq,
    nextPtyOffset
  },
  synchronizedOutputGeneration?,
  contextGeneration? // 仅 opt-in integration 存在时
}

WriterSidebandEnvelope {
  connectionId,
  clientId,
  deliveryGeneration,  // Cloud 绑定，不接受 Browser 自报
  certificate: InputValidationCut
}
```

它有以下硬边界：

- `settledThroughContiguous` 只能是 Host 已得到确定性结果的最大连续 input prefix，不能是最高已见 seq；
- Host 在 session actor 内采样 `AuthorityCursor`，可在有界 settle window 后或 synchronized-output end 合并
  生成；不能等 publisher send 时回填；
- Cloud 只发给当前 writer connection，并绑定当前 delivery generation；fence/generation 变化就丢弃，不能
  改投新 writer；
- Browser 必须等 `AuthoritativeReplica` 连续 apply 到 cut，且 synchronized frame 已结束，才可比较；
- cut 丢失、乱序或过期没有 correctness 后果：等待后续 cut、超时或清 overlay；
- cut 不消耗 `eventSeq`，不进入 journal/snapshot/tail，不占 terminal delivery credit，不广播 observer。

它只证明 Host 已连续、确定性地处理到该 input prefix，并提供可开始比较的 authority cut。它不是 child
consumed/effect ACK，不证明某个 diff 由该 input 导致，也不需要 reconnect/replay。

Browser 不能按“一 key 一 echo”验证。一个 prediction epoch 保存 authoritative base `A`、pending inputs
`[i1..in]` 和 prefix states `Pj = predict(A, i1..ij)`。cut 到达后：

1. 只考虑 cut 覆盖的连续 prefix；
2. 在 canonical region/cursor 与 `P1..Pm` 中寻找**最大、无歧义、informative 的 compatible prefix** `Pk`；
3. 退休 `i1..ik`，以最新 canonical state 为 base，确定性重算剩余 suffix；
4. context/geometry/mode 失效或存在冲突时整 epoch reset，一帧内显示 authority；
5. 多个 prefix 同时兼容、原画面本来相同或没有可归因 change 时只能 `compatible-no-credit`：不提高置信度；
   cut-covered patch 可在有界 grace 后从展示层退休，但不能宣称应用 effect 已确认。

```text
A0 = "> hel|"
i1 = "l" -> P1 = "> hell|"
i2 = "o" -> P2 = "> hello|"  // 当前 overlay

canonical A1 = "> hell|" 且 cut 覆盖 i1
  -> 退休 i1
  -> 以 A1 为 base 重算 i2
  -> overlay 仍显示 "> hello|"
```

验证结果分成三类：

| 结果                   | 条件                                                               | 动作                                     |
| ---------------------- | ------------------------------------------------------------------ | ---------------------------------------- |
| `compatible-credit`    | 最大兼容 prefix 唯一，目标 region 有预期的非平凡变化，context 未变 | 退休 prefix，提高同类 epoch 的置信度     |
| `compatible-no-credit` | 最终 cells 相同但不 informative、无法归因，或多个 prefix 兼容      | 不学习；有界等待后隐藏/退休相关 overlay  |
| `incompatible`         | cut 后目标 cells/cursor/context 与预期冲突                         | 一帧内清 overlay、重置 epoch、降级 `Raw` |

这沿用 Mosh `CorrectNoCredit` 的关键思想：画面“碰巧正确”不能训练 predictor。即使
`compatible-credit` 也只是终端层的观察证据，不是应用 effect 的因果证明；只有显式 shell/input-region
integration 才能提供更强的语义确认。

### `libghostty` 如何参与

不要用 encoded input bytes 模拟 output。`PredictionController` 从 semantic event 构造“假想视觉 edit”，再让
无 effects 的 Ghostty shadow 复用真正的打印、grapheme、width、wrap 和 cursor 规则。最终只导出 changed cell
patch 与 predicted cursor。

在实现 lightweight shadow 前，JS/cell overlay 只允许在当前 API 能证明安全的窄条件下工作：单行、无 wrap、
无 combining/wide、无 IRM、parser ground、cursor 可见且 region 未改变。不知道就退回 `Raw`。

### synchronized output

DEC synchronized output 表示应用正在组成一个原子 frame。预测不得和半帧逐 cell 对账：

```text
sync begin  -> freeze/hide overlay
frame bytes -> update authoritative replica only
sync end    -> compare full frame, confirm/rebase/reset overlay
```

这对 Codex/Ratatui 等频繁全局 redraw 的 TUI 很重要，也能减少“刚预测就被半帧擦掉”的抖动。

### 首版真实支持矩阵

| 场景                                       | MVP 默认行为                                                                       |
| ------------------------------------------ | ---------------------------------------------------------------------------------- |
| 已知非 secure、稳定单行、ASCII edit        | hidden learning 后可显示 printable/Backspace                                       |
| 普通 shell prompt、无 integration          | 只做窄 `Mirrored`，不启用 `Owned`                                                  |
| 简单 REPL                                  | 满足相同窄条件后 opt-in/逐步开放                                                   |
| Codex/Claude Code 全屏 composer            | 默认 `Raw` 或 hidden-only；确定性 driver、稳定 context、显式 opt-in 三者齐全才可见 |
| Left/Right、Unicode、CJK、emoji、wrap      | MVP 不支持；后续逐项 compatibility gate                                            |
| Enter/Tab/Esc/Ctrl/function/mouse/approval | 不预测；是否发送由独立 policy 决定                                                 |
| password/secure                            | 永不显示 prediction                                                                |

这张表描述首版承诺，不把未来可能覆盖的 TUI 写成当前默认支持。

## `Raw`：正确性优先

以下情况默认强制 `Raw`：

- password/secure-input，或 termios/context 还无法排除敏感输入；
- approval、confirmation、popup、menu、search、Vim NORMAL/VISUAL 等 context 不确定状态；
- Enter、Tab、Esc、Ctrl、function key、mouse 和焦点改变；
- parser 非 ground、snapshot/continuation adoption、synchronized frame 未结束；
- replica catching-up、resize 未确认、writer/generation/engine/session epoch 改变；
- overlay/driver 达到资源上限或 canonical diff 不兼容。

ECHO/ICANON 是单向 veto/reset 信号，不是输入类型分类器：ECHO on 不证明应用会回显；ECHO off 也不证明是密码，
很多 line editor/TUI 会自管回显。mode generation 改变立即清当前 epoch，sampling unknown 不能放宽策略，
ECHO 本身也永不提高 confidence。

默认产品策略是：ECHO off 或 unknown 时不启用 visible prediction，除非确定性的 TUI driver、同一未变化
context 内的 informative cut validation，以及显式 `mirrored_echo_off` opt-in 三者同时成立；每个新 epoch
仍从 hidden 开始，Enter、prompt/mode/termios generation 改变后立即清空。已知 secure-input 永久禁用。

纯终端无法在应用内部状态变化但尚未输出/更新 termios 的瞬间给出形式化零泄漏保证，因此产品还需提供全局
`Secure input` 硬开关。开启后立即清空 overlay，并保持 `Raw`，直到用户显式关闭。

## 恢复期输入策略独立于 `Raw`

catching-up 时展示旧 state 或尚未 adopt 的 candidate。强制 `Raw` 只关闭预测，无法阻止旧 composer 上的
Enter 落到新 modal。

| Input class                          | catching-up 默认行为                                                |
| ------------------------------------ | ------------------------------------------------------------------- |
| resize                               | 允许走 authority resize path；确认前阻止 geometry-sensitive input   |
| Ctrl-C / Ctrl-\ 等明确 interrupt     | allowlist 发送；仍进入 ordered stream，不越 earlier input           |
| ordinary printable                   | 默认 `not-sent`；若未来支持 blind typing opt-in，必须持续显示 stale |
| Enter、Tab、Esc、approval、commit    | 默认 `not-sent`；有效 guarded context 才可例外                      |
| paste                                | 默认 `not-sent`                                                     |
| mouse、focus、geometry-sensitive key | 默认 `not-sent`，直到 current 且 resize confirmed                   |

被阻止事件不分配 seq、不缓存、不在恢复后重放。恢复的 concurrent gap-fill、delivery credit、apply cursor 与
handoff 由[协议总纲](terminal-protocol-architecture.md)和
[Snapshot 计划](high-performance-snapshot-recovery-plan.md)统一定义；本计划不再把“删除 global pause”
作为一个孤立 input phase。

## Context fence：减少 stale typeahead

当前 `observedEventSeq` 只防 Browser 声称看过 Host 未来，不防 stale input。提议为所有 semantic input 增加
显式 input class 和可选 `guarded-context` fence：

```text
GuardedContext {
  sessionEpoch,
  authorityCursor,
  contextGeneration,
  contextToken
}

InputPolicy = ordinary-stream | urgent-stream | guarded-context
InputClass  = edit | commit | control | geometry-sensitive
```

- 普通 mirrored edit 使用 `stream`：保持当前 pipeline 和 at-most-once；Host 已观察到 hard transition 时拒绝该
  prediction epoch，canonical mismatch 负责视觉回滚。
- commit/control 在有可靠 token 时使用 `guarded-context`：Host 在 `pty.write` **之前**做 compare-and-write；
  不相等返回 `stale-context`，Browser 不自动重试。
- Cloud 先验证当前 connection/delivery/writer，并像现有 writer fence 一样向 Host 注入可信
  `deliveryGeneration`；Host-issued token 只绑定 session epoch、authority event/context generation，以及按场景
  定义的 rows/cols、cursor、alt-screen、sync/termios generation 和 region digest。Browser 只回显 opaque
  token，不能自报一个看似正确的 cursor/region 取得放行；Host actor 把 Cloud fence 与 token 一起
  compare-and-write。
- Browser replica 不 current 时不允许构造 `guarded-context` 动作；UI 明确显示 catching up。
- 第一版只有显式 Owned prompt 和 mouse geometry 有足够可靠的窄 context。Generic TUI 最多使用保守的
  whole-screen terminal fence；没有可靠 token 时不能把它称为 context-safe。

这个 fence 是 optimistic concurrency guard，不是应用状态安全边界。它能阻止“Browser 已看到 modal 切换，
但旧输入还在队列里”的大部分事故；它不能看到应用尚未写到 PTY output 的内部状态变化。也就是说，纯 PTY 下
无法严格证明某个 Enter 仍属于 Codex composer 而不是刚出现但尚未 render 的 approval。默认把这类动作留在
`Raw`，是产品边界，不是待优化的小 bug。

## `Owned`：显式 shell command buffer（后置）

`Owned` 的实施晚于 `Mirrored`，只对显式安装/allowlist 的 shell integration 或协商后的 input-region
ownership transfer 启用。Host 必须持有 integration assertion，并在 submit
时同时核对 foreground shell、prompt epoch 和 authority context。OSC 133 可以作为正向信号，但普通 child 也能
打印 OSC，因此单独使用它不是授权或安全边界；side channel/session nonce 主要防 stale 或 accidental spoof，
也不能天然隔离能继承环境或控制同一 PTY 的恶意 child。

建议状态机：

```text
inactive
  -> prompt-ready(promptEpoch, capabilities, remoteBufferState)
  -> editing-local(promptEpoch, draftRevision)
  -> committing(commitId, guardedContextFence)
  -> accepted | preexec-race | stale-context | uncertain
  -> inactive
```

提交协议的目标形态：

```text
CommitCommand {
  promptEpoch,
  commitId,
  writerFence,
  inputEpoch,
  clientInputSeq,
  guardedContextToken,
  text,
  submitAction: "enter"
}
```

`commitId` 是 command-level 幂等键，不替代现有 transport input identity。

Host 在 session actor 的同一个 turn 中：

1. 验证 writer fence、prompt epoch 和 guarded context；
2. 确认 shell 仍处于 integration 声明的 editable state；
3. 只在 integration 报告 `known-empty`，或提供了确定性的 buffer sync action 时继续；绝不能猜测 Ctrl-U、
   kill-line 或其他 shell binding；
4. Host authority 当前确认 bracketed-paste mode 时，才发送 bracketed paste 包围的完整 command 和 Enter；
   mode 未开启时只允许不含 CR/LF/control 的 single-line safe subset，否则拒绝并保留 draft；
5. 记住 `commitId` 的有界结果。

Commit 实现必须复用 authority 的 `encodePaste(text)`，继承既有 1 MiB 上限、line-ending translation、
bracketed framing 和 control/ESC sanitization；禁止手拼 bracketed-paste delimiter。terminator 也必须是经过
authority semantic key encoder 的固定提交动作，不能接受调用方提供的任意 control bytes。

这个 batch 只在 Zhongduan relay/Host actor 排序上连续，不是 app transaction；PTY/application 仍可能逐字节消费。
`promptEpoch` 和 preexec fence 降低竞态，但不能把 Unix PTY 变成原子 commit API。

短暂 transport reconnect 可以保留内存中的**未提交 draft**，但必须显示给用户确认，绝不自动提交。跨 page
reload 的 draft persistence 是单独 opt-in：只能 local-only（或用户明确配置的加密存储）、有 TTL、可一键清除，
永不进入 terminal snapshot/tail、Cloud telemetry 或日志。若 commit 结果为 `uncertain`，也不能自动重试；
跨 Host crash 无法提供 exactly-once。

保留 shell-native fallback。只要 completion/history/keymap 不兼容、integration 消失或 prompt epoch 改变，就把
当前 draft 交还/取消并退回 `Raw`，不能让本地 editor 偷偷改变 shell 语义。

## Ghostty/WTerm 需要补齐的能力

按依赖顺序：

1. **只读观测 API**
   - parser 是否 ground、continuation 是否为空；
   - pending wrap、IRM、margins、charset、cursor pen；
   - OSC 133 semantic content / 当前 cell input-region hint；
   - 复用现有 synchronized-output generation，并新增 stable visible-region digest。
2. **Renderer overlay**
   - 合成 cell patch 和 predicted cursor；
   - 与 copy/selection/search/ARIA/dirty-row core 隔离；
   - 一次原子清除和 feature kill switch。
3. **Lightweight prediction state**
   - 只包含 active screen 执行所需状态，不包含 history/images；
   - side effects 永远 discard；
   - semantic edit API，而不是 raw input/output byte 回灌；
   - 有界 clone/COW 或专用 `predictSemanticEdit`。
4. **Host termios sampler**
   - 至少暴露 ECHO/ICANON 及相关 generation；
   - 采样失败视为 unknown，不放宽预测；
   - termios 只作安全/置信信号，不能代替 app context。

现有 cell ABI 的两个保留字节可以评估承载 semantic hint，但优先考虑独立、版本化 API，避免把实验性字段固化成
难以演进的 ABI。

## 先修输入快路径，再增加预测

本地预测只能隐藏网络 RTT，不能掩盖 Ctrl-C 被内部队列阻塞。因此实施顺序必须先处理权威路径：

### Cloud

- 为 Browser control、Host control 和 Host data 建立独立 lane，保持每 socket/per-session 必需顺序；
- control 可以越过**Browser 尚未观察到**的 bulk data，但 lease、writer transfer、barrier 和 input fence 必须有
  明确 ordering rule，不能简单做无条件优先级队列；
- 连接建立时绑定已验证 writer identity，input fast path 只检查 cached fence/token/expiry；
- writer lease 由现有 heartbeat 或 rate limit 更新，不再每键 hash + SQLite UPDATE；
- 记录各 lane queue wait/depth、SQLite time 和 fanout time。

### Host

- 缩短并约束 key accept→Ghostty encode→`pty.write` 的 actor 延迟，但绝不能越过先到达的 PTY output/resize；
  否则 Ghostty 可能用旧 keyboard mode 编码 key；
- 先测量当前同步 snapshot blocking；交付严格有界 immutable/COW cut 后，才启用可执行的 actor-pause hard
  guard，并把可安全的 encode/compress 工作迁出 actor，而不是把可变 Terminal 直接丢给另一个线程；
- recovery 的 concurrent gap-fill、handoff 与 flow control 服从[协议总纲](terminal-protocol-architecture.md)，
  不在 input fast path 中孤立删除现有 correctness barrier；
- 在 actor 内生成有界合并的 `InputValidationCut`，走 negotiated writer-only sideband，不写 journal。

### Browser

- keydown 不等待上一条 ACK，保持 pipeline；
- control transport/input epoch 变化继续将 outstanding 标成 uncertain，绝不自动 resend；data delivery
  generation、resync 和 snapshot adoption 至少清 prediction epoch，不能误写成当前已经清 input pending；
- 控制输入和 ordinary edit 分队列、分指标；高风险 input queue 超限时 fail closed；
- dispatcher 对 `sent/queued/not-sent/uncertain` 给出逐事件结果；没有 writer transport 时不再静默吞掉已被
  DOM `preventDefault` 的输入；
- paint 和 verifier 不阻塞真实 WebSocket send。

## 能力协商与滚动部署

feature flag 不能代替 wire compatibility。Browser、Cloud、Host 在握手时 advertisement，Cloud 选择三方交集：

```text
input-contiguous-v1
input-result-v1
input-validation-cut-v1
guarded-context-v1
presentation-overlay-v1
input-region-v1
```

- selected capability 绑定 session/input epoch 或 connection/delivery generation，生命周期内不可静默改变；
- Cloud 只发送接收方已选择的 sideband/status，strict old decoder 永远看不到未知 kind；
- 不支持 `input-contiguous-v1` 就不能启用 validation cut 或 visible Mirrored；
- 长生命周期旧 session 保持旧协议；升级必须经过新协商边界，必要时新建 input epoch/delivery generation；
- writer-only input capability 可以按当前 writer connection 协商，terminal state plane 仍保持兼容版本；
- negotiation mismatch fail closed 到现有 `Raw` PTY path。

`InputValidationCut` 不进入 data protocol，因此无需把 terminal data v2 升级为 mutation/metadata 混合 log。

## 分阶段实施

### Phase A：input correctness 与 hot path

实施进度（2026-08-29）：`observability/terminal-latency-pipeline` 只先建立 Host 侧事实。它把 control callback→
ACK send decision、session actor wait、authority encode、`pty.write`/`pty.resize` call 和双 relay socket RTT
拆开，并把 input/control 字节数分桶。TerminalSession actor 不调用 telemetry sink；Relay 尝试发送 ACK 后才向
共享有界 deferred buffer enqueue。该切片不修改 input seq、dedup、lease、wire 或 recovery ordering，因此不代表
Phase A correctness 已完成。

`observability/cloud-relay-latency` 这个 Cloud Phase 0b 切片只增加 Durable Object 的 relay queue
admission/wait/depth/capacity/completion、Browser input 中现有 lease verify/renew outcome 与 Host send decision、
recovery barrier，以及 attach/delivery-reset transition 本地事实。Workers runtime clock 只在 I/O 边界推进；从
Queue 从本地 admission 起算，input/attach/barrier 从 JavaScript message callback admission 起算；reset 从可被
socket close 等路径调用的 `resetBrowserDelivery()` 操作入口起算。到 decision/handling 或 `WebSocket.send()` 返回
的 duration 都只是 Cloud local lower-bound，同步 CPU 工作可能不可见，也不证明 Host/Browser 已接收。该切片不
覆盖 Host ACK 转发或 Host data fanout，不单列 lease acquire/heartbeat，不拆 lane、不移除每键 hash/SQLite
renewal，也不修改 wire、fence、dedup、journal、snapshot、replay 或 WebSocket attachment，所以同样不代表
Phase A 已完成。

`observability/cloud-delivery-facts` 这个 Cloud Phase 0c 切片只补齐上述缺失的 Cloud owner 事实，并把 Cloud event
升级为 `workers-logs-v2`：

- `cloud.input.ack-forward` 从 Host ACK 进入 relay queue 量到 Browser control target missing、`send()` returned
  或 send failed，同时保留 ACK status；这不是 Browser receipt、canonical paint 或 app-effect ACK。
- `cloud.data.fanout` 由 `TerminalSessionDO` 为每个 Host canonical/directed data frame 产生一条聚合事实，而不是
  每个 Browser 一条。它用闭集 outcome/reason 和计数覆盖 selected、send-returned、stale、sequence-error、
  credit-reset、send-uncertain-reset targets，并只记录最大 credit utilization bucket。Reset 计数是本地 decision/
  request；既有 reset transition 另记 issuance/Host notify decision，两者都不是完成证明。
- `cloud.writer.lease` 记录 writer attach acquire 与 heartbeat verify-renew，并区分 acquired/renewed、unavailable/
  inactive、current/stale 和 uncertain outcome；它观察现有 lease authority，不移动或替换它。

这个切片仍不拆 lane，不改变 input sequence/fence/dedup/wire/recovery，也没有移除每次 semantic input 上的 writer
token SHA-256 与 SQLite lease renewal。Phase A 的 connection-scoped lease 和 input correctness/hot-path 工作仍未
完成。

`observability/browser-recovery-latency` 继续补 Browser 本机事实，但仍不修改 input correctness。它用独立于
票据 wall clock 的 monotonic clock 记录 control/data socket RTT、成功送入 control socket 到 input ACK 的时间、
attach matching/timeout/cancelled 终点，以及 snapshot load/restore/buffer apply/adopt 与最终 recovery outcome。内部 pairing 可以使用
input epoch/seq，但事件不得带出这些标识或 key/text/paste；所有记录只进入 256 条有界内存 ring，不写 console、
storage 或网络。这里的 ACK 仍不是 child read/app effect，socket RTT 也不是纯网络 RTT。

- Browser 完整 validate/admit 后才分配 seq；
- Host 使用 strict `nextExpectedSeq`；确定性 reject 消费 seq，`uncertain` 终止 epoch；
- 建立单一 per-writer sequencer，明确 urgent cancellation 与 resize ordering；
- 完成 `sent/queued/not-sent/uncertain` 逐事件反馈；
- writer lease 改为 connection-scoped capability；
- 拆除 input 热路径上的每键 storage/hash/lease renewal；
- 建立 capability negotiation 和分段 monotonic telemetry；
- 测量并约束 snapshot actor pause，但 recovery 协议改造由 snapshot/总纲计划负责。

完成门槛：预测关闭时，schema failure、断连、writer transfer、urgent input 和 duplicate injection 都不会制造
silent loss、seq gap 或重复 PTY effect；本地 input latency 不被 bulk work 线性放大。

### Phase B：authority-isolated overlay

- WTerm renderer 增 `PresentationOverlay`；
- 补最小 parser/wrap/mode/cursor/sync generation/region digest 只读 API；
- hidden-only predictor 运行 authority isolation property test；
- 完成 reset、resource bound 和 kill switch。

完成门槛：任意时刻清 overlay 后 core 与 canonical replay 一致；speculative content 在
snapshot/copy/selection/ARIA/log/network 中出现次数为 0。

### Phase C：ASCII `Mirrored` MVP

- 协商 `input-validation-cut-v1`；
- Host 生成 writer-only live cut，Cloud 绑定 delivery generation；
- 实现 authoritative base、pending inputs、prefix states 与 maximal compatible prefix；
- 实现 credit/no-credit/incompatible、suffix rebase 和 sync-output full-frame validation；
- 只开放支持矩阵中的 ASCII printable/Backspace。

完成门槛：eligible edit 的 predicted paint p95 在一个本地 frame 内；conflict 到 overlay 清除 p95 在一个
frame 内；coalesced redraw 不会重复 overlay 或错误退休 suffix。

### Phase D：扩大 Mirrored 与 input-region

input-region 是长期核心抽象，现在定义、后置实现；ASCII MVP 不依赖它：

```text
InputRegionBegin {
  id, revision,
  ownership: application | terminal,
  secure, geometry, cursor, capabilities, contextToken
}
InputRegionUpdate {
  id, revision, geometry, cursor, contentDigest, contextToken
}
InputRegionEnd { id, revision, reason }
```

- Left/Right、Unicode、wide/combining、wrap、multiline 使用独立 compatibility gate；
- 实现 versioned input-region、nonce/revision/context fence 和 fail-closed；
- 可增加 deterministic Codex/Claude/readline driver，但 app-specific 规则只能缩小 eligibility；
- 根据 hidden-match 数据决定 ECHO-off context 的 opt-in。

协议仍通过 PTY，未知应用忽略；普通 child 可打印 OSC/DCS，所以它不是抵御同 PTY 恶意进程的授权边界。

### Phase E：显式 `Owned`

- 只在 shell integration/input-region 明确 ownership transfer 后启用；
- 实现 revisioned draft/commit、context token 和 known-empty/buffer sync；
- 保留 shell history、IME、completion、keymap 与 native fallback；
- uncertain commit 永不自动 retry。

Recovery v3、FrozenTerminalCut 和 rolling checkpoint 是并行工作流，phase owner 见相应计划。

## 指标与 SLO

分布式节点只记录本机 monotonic duration；不能直接相减不同机器的时钟。用
`(writerFence,inputEpoch,clientInputSeq)` 关联事件，并区分网络段和本地处理段。

### 必须记录

- Browser：keydown→WS send、keydown→ACK、keydown→predicted paint、keydown→matching canonical paint；
- Cloud：各 lane queue wait/depth、lease verify/renew、Browser receive→Host send、Host data/ACK receive→Browser send；
- Host：control queue、session actor wait、Ghostty encode、`pty.write`、snapshot blocking、canonical paused time；
- 体验：Ctrl-C→`pty.write`、Ctrl-C→output quiet、Ctrl-C→prompt paint；
- 预测：eligible coverage、hidden/visible epoch 数、match/mismatch、rollback duration、context reject、secure reset；
- 资源：overlay cells/bytes/age、pending ACK、input queue、driver CPU/frame time。

遥测只能记录枚举、大小、时延、digest 和计数，不能记录 text、paste、command、cell 内容或 key 明文。
输入大小属于敏感行为元数据：Host input/control 事实只记录 bucket，不记录精确文本长度、writer/client/session
标识或异常字符串。`host.input.apply` 中的 ACK send 只表示本地 WebSocket `send()` 返回；`written` 只表示现有
Host input result，不证明 child read、应用 effect、canonical output 或 Browser paint。Host relay RTT 也只是该
socket 到 Cloud auto-responder 的本机观测 RTT，不是 Browser↔Host 或纯网络 RTT。

Cloud 自定义 event payload 同样不得包含 terminal/input/frame payload、text/paste/command/cell/key、ticket/
capability/writer lease，原始 session/client/connection/stream/generation/epoch/sequence、journal/snapshot ID 或
cursor、URL、异常或 error string，以及上述敏感值的 hash/digest；input/control/frame 大小只允许 bucket。
Cloudflare 自动 invocation logs 和 tracing 必须保持显式关闭，因为 WebSocket ticket 位于 query、session ID 位于
path；这个 payload 保证不延伸为“平台 envelope 匿名”，Workers Logs 仍可能附加平台 request、invocation 或
Durable Object 标识。

Cloud queue wait 只从 `webSocketMessage` callback/local queue admission 起算；Browser input→Host send、Host
ACK→Browser control、每 Host data frame fanout 和 barrier result send 只量到本地 decision/`send()` 返回。
Workers runtime clock 只在 I/O 边界推进，同步 CPU 工作可能不可见，所以这些字段全部是 Cloud local lower-bound，
不能标成网络传输、对端接收、端到端 latency 或 CPU time。每个 v2 event 都有 `sampleWeight`；ACK 与常见
heartbeat outcome 会采样；canonical frame 在 Browser loop 前以 weight 64 固定选择，未选中时不安装
per-Browser observer，选中的正常或异常 outcome 使用同一权重。Weight 1 也仍是 best-effort；原始 event count
不是完整流量计数，聚合必须使用 `sampleWeight`。

Cloud pending event、drop 状态和 sampling phase/counter 只驻每个 DO instance 的有界内存，不进入 SQLite 或
WebSocket attachment；capacity、collector/runtime failure、版本切换、eviction/hibernation 可以丢弃或重置它们，
没有 backfill/replay。生产 query 在 rollout/rollback 期间必须同时接受 schema v1/v2；代码和精确 mode 分步发布或
回滚不匹配会形成不可恢复的 telemetry blind window，因此必须成对发布并验证 v2 到达后再退休 v1 query。

Browser 本机切片已经补齐 input send-return→ACK、socket RTT 与 snapshot load/restore/buffer apply/adopt，但还没有
keydown、canonical match 或真实 paint 信号。Cloud Phase 0c 只补事实层；生产 query/aggregation/dashboard、
Browser paint、跨节点 E2E/SLO，以及启用插桩后 input latency/canonical throughput `<=5%` 回归 gate 仍开放。

### 初始发布门槛

- predicted paint p95 不超过一个 60 Hz frame；
- conflicting canonical output 到 overlay 清除 p95 不超过一个 frame；
- supported-load 下 Cloud+Host 非网络 input overhead p99 不超过 25 ms，Phase A 后按实测校准；
- end-to-end result/ACK 目标写成“实测 relay 网络 RTT + Browser/Cloud/Host 本地预算”，不能在注入
  600 ms RTT 时要求物理上不可能的固定 500 ms；
- recovery/snapshot 开启时本地 control→`pty.write` p99 不得超过无恢复 baseline 的 2 倍，并单独报告网络段；
- fault-injection 中 live dedup 窗口内的重复 PTY write、Host 已观察到 invalidation 后的 stale guarded write、
  已声明或已被 Host 观察到的 secure state visible prediction 均为 0；
- authority isolation property test 在所有 fuzz case 中成立。

## 测试矩阵

### 网络与负载

- Browser↔Cloud、Cloud↔Host 分别注入 20/100/300/600 ms RTT、jitter、packet loss 和断连；
- 10/30/60 keys/s，单键、burst、paste、IME composition；
- idle shell、持续 PTY output、全屏 repaint、output flood + Ctrl-C；
- 同时发生 cold attach、snapshot capture/upload、journal replay、writer transfer 和 resize。

### 应用

- bash/zsh/fish + readline/zle；
- Python/Node/Postgres 等 REPL；
- Vim/Neovim/Emacs、less、htop；
- Codex、Claude Code 的 composer、autocomplete、queue 和 approval；
- password prompt、`sudo`、SSH password、no-echo reader；
- tmux/screen 内外和 alternate screen 切换。

### Terminal mechanics

- ASCII、CJK wide、emoji、combining、ZWJ、RTL fallback；
- right-margin pending wrap、IRM、scroll margins、resize/reflow；
- application cursor keys、Kitty keyboard、bracketed paste、focus、mouse；
- partial UTF-8/CSI/OSC/DCS continuation；
- synchronized-output begin/end 和半帧断线；
- snapshot restore/replica swap 期间输入。

### 必须成立的 property

1. 对 `Mirrored/Raw`，相同 semantic input 必须产生相同 PTY bytes；给定相同 PTY output + resize stream，
   prediction feature on/off 的 Host authority snapshot 完全相同。`Owned` 因批次/时序本来就不同，单独验证。
2. authority state 始终是 checkpoint 加同一 mutation log 的连续前缀；interaction sideband 不消耗 `eventSeq`。
3. 清空 overlay 后 Browser active replica 与 canonical replay 结果完全相同。
4. speculative cells 永不进入 snapshot、tail、copy、selection、search、ARIA、log 或 network payload。
5. 同一 writer epoch 不存在已处理的 seq gap；确定性 duplicate 返回完全相同的 result。
6. `uncertain` 后该 input epoch 不再接收新 input，也不自动重放。
7. `InputValidationCut` 只发当前 writer；旧 fence/generation cut 不验证新 overlay。
8. maximal compatible prefix 后，未退休 suffix 从最新 canonical base 重算。
9. catching-up 中被阻止的 `not-sent` 输入在恢复后 `pty.write` 次数仍为 0。
10. Host 已观察到的 guarded context mismatch 发生时，`pty.write` 调用次数为 0。
11. writer transfer 后旧 fence 的 input/result/cut 都不能影响新 writer。
12. prediction failure、driver exception 和资源超限只关闭预测，真实 ordered input path 保持可用。

## Rollout

所有能力独立 feature flag：

```text
input_contiguous_stream
connection_scoped_writer
input_fast_path_lanes
input_validation_cut
input_guarded_context_fence
presentation_overlay
mirrored_ascii_edit
mirrored_unicode_edit
tui_deterministic_drivers
mirrored_echo_off
input_region_protocol
shell_owned_buffer
```

发布顺序是 capability negotiation → hidden measurement → internal visible → opt-in → 支持矩阵内默认开启。
feature flag 不替代 wire negotiation。Presentation/driver kill switch 可以立即清 overlay、回到 `Raw`；改变
ordered-input 或 sideband wire 语义的 kill switch 只能 fence/结束当前 input epoch 或 control connection，
再按已协商 fallback 重连，不能在未决 input 中途静默切换协议。两者都不需要重建 terminal snapshot。

## 明确不做

- 不把 input validation metadata 写入 canonical journal/snapshot/tail；
- 不把 Codex app-server、Claude SDK/Remote Control 当作终端输入方案；
- 不让 Browser 成为任意 TUI 的权威业务状态机；
- 不用 AI 决定某个按键是否安全、是否提交或是否批准；
- 不把 input bytes 当作 terminal output 喂给 Ghostty parser；
- 不用完整 Ghostty snapshot 做 per-key clone；
- 不因预测命中而跳过真实 PTY input 或 canonical output；
- 不自动重放 uncertain Enter、Ctrl、paste 或 command commit；
- 不自动重放 catching-up 时被阻止的 input；
- 不让 urgent lane 越过 earlier semantic input；
- 不把 `Raw` 表述成 context-safe；
- 不让 input-region implementation 阻塞 ASCII Mirrored MVP；
- 不在没有 capability negotiation 时仅靠 feature flag 发送新 wire kind；
- 不承诺在纯 PTY 下识别应用尚未输出的内部 modal 状态；
- 不把 Mosh 的 UDP roaming 当成现代 Browser 部署必须照搬的架构。

## 参考资料

### 项目源码

- Browser input dispatcher：[`apps/terminal-cloud/src/browser/input-dispatcher.ts`](../apps/terminal-cloud/src/browser/input-dispatcher.ts)
- Cloud session relay：[`apps/terminal-cloud/src/worker/terminal-session-do.ts`](../apps/terminal-cloud/src/worker/terminal-session-do.ts)
- Host session authority：[`apps/host-daemon/src/session.ts`](../apps/host-daemon/src/session.ts)
- 固定 WTerm fork：
  [Ghostty adapter](https://github.com/Eric-Song-Nop/wterm/blob/4764f3b5e81cb76cdb08d4ffbfba1257fd33efd6/packages/%40wterm/ghostty/zig/src/wasm_api.zig)、
  [DOM renderer](https://github.com/Eric-Song-Nop/wterm/blob/4764f3b5e81cb76cdb08d4ffbfba1257fd33efd6/packages/%40wterm/dom/src/renderer.ts)
- 固定 Ghostty fork 的 [semantic prompt](https://github.com/Eric-Song-Nop/ghostty/blob/fe317f850c3ab212f6638122c459b9b48b99a016/src/terminal/Terminal.zig#L2078-L2204)、
  [cell semantic API](https://github.com/Eric-Song-Nop/ghostty/blob/fe317f850c3ab212f6638122c459b9b48b99a016/src/terminal/c/cell.zig#L40-L95) 和
  [key encoding](https://github.com/Eric-Song-Nop/ghostty/blob/fe317f850c3ab212f6638122c459b9b48b99a016/src/input/key_encode.zig#L45-L75)

### 一手外部资料

- Mosh：[官网](https://mosh.org/)、[论文](https://mosh.org/mosh-paper.pdf)、
  [源码与设计说明](https://github.com/mobile-shell/mosh)、
  [`CorrectNoCredit` 判定](https://github.com/mobile-shell/mosh/blob/decd9b705eb81626f694335b8d5940538beb06da/src/frontend/terminaloverlay.cc#L72-L111)、
  [50 ms late echo ACK](https://github.com/mobile-shell/mosh/blob/decd9b705eb81626f694335b8d5940538beb06da/src/statesync/completeterminal.cc#L127-L176)
- Warp：[输入架构说明](https://www.warp.dev/blog/why-is-the-terminal-input-so-weird)、
  [command submit](https://github.com/warpdotdev/warp/blob/061318ff7fc424e41fbd77e30432995d483c99e4/app/src/terminal/writeable_pty/pty_controller.rs#L775-L826)、
  [line-editor status](https://github.com/warpdotdev/warp/blob/061318ff7fc424e41fbd77e30432995d483c99e4/app/src/terminal/line_editor_status.rs#L71-L134)
- OpenSSH：[client configuration](https://man.openbsd.org/ssh_config)、
  [server configuration](https://man.openbsd.org/sshd_config)
- Codex 固定源码：
  [ChatComposer](https://github.com/openai/codex/blob/94311d447587411789533c47601fd8bc9d81eb48/codex-rs/tui/src/bottom_pane/chat_composer.rs#L1-L107)、
  [TextArea](https://github.com/openai/codex/blob/94311d447587411789533c47601fd8bc9d81eb48/codex-rs/tui/src/bottom_pane/textarea.rs#L549-L678)、
  [synchronized draw](https://github.com/openai/codex/blob/94311d447587411789533c47601fd8bc9d81eb48/codex-rs/tui/src/tui.rs#L973-L1049)、
  [approval protection](https://github.com/openai/codex/blob/94311d447587411789533c47601fd8bc9d81eb48/codex-rs/tui/src/bottom_pane/mod.rs#L599-L643)
- Claude Code 官方文档：[interactive mode](https://code.claude.com/docs/en/interactive-mode)、
  [key contexts](https://code.claude.com/docs/en/keybindings#available-actions)、
  [fullscreen rendering](https://code.claude.com/docs/en/fullscreen)
