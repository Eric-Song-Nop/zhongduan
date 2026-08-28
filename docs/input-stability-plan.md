# 输入稳定性与高 RTT 交互实施计划

> 状态：方向已确认，等待实施
>
> 决策日期：2026-08-28
>
> 适用范围：Browser 输入、Cloud relay、Host session actor、PTY、Ghostty/WTerm replica 与 DOM renderer
>
> 相关计划：[高性能远程终端 Snapshot 恢复实施计划](high-performance-snapshot-recovery-plan.md)

## 结论

Zhongduan 始终是终端，不为 Codex、Claude Code 或其他应用另做一套 RPC 客户端。真实应用继续运行在
PTY 后面，Host 的 Ghostty terminal 始终是唯一权威；Browser 只在这个边界内改善输入反馈。

输入稳定性采用三种明确模式，而不是一个对所有应用都冒险的“本地回显”开关：

| 模式       | 谁拥有待编辑内容 | 输入何时写入 PTY                    | Browser 能做什么                 | 适用范围                                                    |
| ---------- | ---------------- | ----------------------------------- | -------------------------------- | ----------------------------------------------------------- |
| `Owned`    | Zhongduan        | 提交时按一个受 fence 保护的批次写入 | 编辑反馈不依赖 RTT，提交仍走网络 | 有明确 shell integration 或终端输入区协议的 prompt          |
| `Mirrored` | 远端应用         | 每个按键立即写入                    | 非权威、可撤销的本地视觉 mirror  | REPL、普通 TUI、Codex/Claude Code composer 的稳定编辑 epoch |
| `Raw`      | 远端应用         | 每个事件立即写入                    | 不预测，只显示 canonical output  | 密码、审批/modal、不确定上下文、控制键和不支持状态          |

这三个 mode 决定 buffer ownership 和 presentation，不等于网络 delivery policy。后者与 mode 正交：

| Delivery policy   | 语义                                                                            |
| ----------------- | ------------------------------------------------------------------------------- |
| `urgent-stream`   | Ctrl-C 等必须尽快到 Host，但仍保持 authority 所需的 PTY/output 顺序             |
| `ordinary-stream` | 普通 edit 立即 pipeline，不等待 ACK                                             |
| `guarded-context` | Host 只对它已经观察到的 terminal context 做 compare-and-write；不匹配则不写 PTY |

`Raw` 只表示“不预测”，本身不能阻止 RTT 途中应用切换 modal。高风险动作在存在可靠 token 时应同时使用
`guarded-context`；没有 token 时仍存在普通远程终端固有的 context race。

`libghostty` 的价值是精确计算“如果应用按某种方式显示这个编辑，cells、cursor、grapheme 和 wrap
会是什么”，而不是猜应用下一步的业务意图。应用是否会回显、Enter 当前是提交还是批准、Vim 当前是否
处于 INSERT mode，单靠 VT parser 都无法知道。因此：

- 普通编辑可以积极地镜像，但每个 key 仍立即流向 PTY；
- Enter、Tab、Esc、Ctrl、function key、mouse 和 approval 等高风险动作默认不预测、不延迟提交、
  不自动重放；
- 预测层只影响 presentation，清掉它之后，权威 terminal 必须和“功能完全关闭”时逐字节相同；
- AI 不进入正确性路径和输入热路径。将来可以辅助离线编写 adapter，但运行时不依赖它；
- snapshot 用于恢复和 rebase，不用于逐键 fork，也不负责降低健康连接的输入 RTT。

这能让 shell prompt 的编辑反馈在本地一帧内完成，让大量 REPL/TUI 文本编辑接近相同的视觉反馈；commit、
执行和 canonical confirmation 仍受网络影响。它不承诺任意 TUI 动作都能在本地确定执行，也不宣称在
所有维度“全面超过 Mosh”。

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

- Browser/Cloud/Host 用 `(writerFence, inputEpoch, clientInputSeq)` 标识输入；control transport 变化或新
  input epoch 会把 outstanding input 标为 `uncertain` 并清空待发送队列，不盲目重放。data-only delivery
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
应用消费过哪些 key。后文提议的 `InputSettle` 只增加有序验证 metadata，永远不携带或重放 input bytes，不能
消除 ACK 丢失或 Host crash 后的 effect uncertainty。

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
- WTerm Ghostty input/mode ABI：[`wasm_api.zig`](../vendor/wterm/packages/@wterm/ghostty/zig/src/wasm_api.zig#L808-L852)
- 当前 DOM renderer：[`renderer.ts`](../vendor/wterm/packages/@wterm/dom/src/renderer.ts#L294-L506)

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

## `Owned`：显式 shell command buffer

`Owned` 只对显式安装/allowlist 的 shell integration 启用。Host 必须持有 integration assertion，并在 submit
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
因此 overlay 不能看到 ACK 或任意一帧相同画面就宣布“应用已确认”。普通 ACK 可携带 `writtenThrough` 供
诊断；真正允许开始验证的是按 writer epoch 单调、data-ordered 的 `InputSettle`：

```text
InputSettle payload {
  writerFence,
  inputEpoch,
  throughSeq
}
```

每次 `pty.write`/handled-no-bytes 记录 `writtenAt`。初始等待 50 ms 后，Host 把到期 input 合并成最多 20 个
marker/s 的 actor message。第一版把 `InputSettle` 定义为新的 canonical metadata `DataFrameKind`：它消耗一个
`eventSeq`、保持 `ptyOffset` 不变、进入 journal/tail，但不送入 Ghostty parser。当前 data protocol v2 和
CanonicalPublisher 只接受 PTY output/resize，因此这是需要 protocol version bump 的明确 wire change，不是
现有能力。完成后，连续 eventSeq、
CanonicalPublisher、Cloud commit 和 replay ordering 仍只有一套规则；snapshot/adopt 本来就清 prediction，
重放的旧 marker 只需按 fence/epoch 忽略。以后有数据证明成本值得优化，再考虑 piggyback/live-only lane。

Host 必须在 session actor 处理 marker 时建立 cut，不能等 publisher send 时再补；否则更早产生、只是在队列里
更晚发送的 output 会被错误标成 input 之后。Browser 只在 data stream 上看到覆盖目标 input seq 的
`InputSettle`、且 synchronized frame 已结束后，才开始比较。它只证明“Host 已处理 input，等待了最小 settle
delay，并已应用 marker 前当时可见的 PTY output”，仍然**不证明** child 已消费，也不证明某个 diff 是该 input
导致的。`InputSettle` 是 validation eligibility watermark，不是 application-effect acknowledgement。

如果 marker 时 screen 仍是 base，candidate 可以保持 pending 到后续 output 或有界 expiration；不能因为固定
50 ms 到期就声称应用不会 echo。最终未出现 informative change 时回滚/退休且不给 confidence。

验证结果分成三类：

| 结果                   | 条件                                                                                          | 动作                                       |
| ---------------------- | --------------------------------------------------------------------------------------------- | ------------------------------------------ |
| `compatible-credit`    | cut 已覆盖 input，目标 region 从 base 发生了预期的非平凡变化，context 未变                    | 退休 patch，并提高同类 epoch 的置信度      |
| `compatible-no-credit` | 最终 cells 相同，但原画面本来就相同、replacement 是空白/unknown，或没有可归因的 region change | 可以退休已 settle 的 patch，但不提高置信度 |
| `incompatible`         | cut 后目标 cells/cursor/context 与预期冲突                                                    | 一帧内清 overlay、重置 epoch、降级 `Raw`   |

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
context 内的 informative settle validation，以及显式 `mirrored_echo_off` opt-in 三者同时成立；每个新 epoch
仍从 hidden 开始，Enter、prompt/mode/termios generation 改变后立即清空。已知 secure-input 永久禁用。

纯终端无法在应用内部状态变化但尚未输出/更新 termios 的瞬间给出形式化零泄漏保证，因此产品还需提供全局
`Secure input` 硬开关。开启后立即清空 overlay，并保持 `Raw`，直到用户显式关闭。

## Context fence：减少 stale typeahead

当前 `observedEventSeq` 只防 Browser 声称看过 Host 未来，不防 stale input。提议为所有 semantic input 增加
显式 input class 和可选 `guarded-context` fence：

```text
GuardedContext {
  sessionEpoch,
  deliveryGeneration,
  observedEventSeq,
  contextGeneration,
  contextToken
}

InputPolicy = stream | guarded-context
InputClass  = edit | commit | control
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

## Ghostty/WTerm 需要补齐的能力

按依赖顺序：

1. **只读观测 API**
   - parser 是否 ground、continuation 是否为空；
   - pending wrap、IRM、margins、charset、cursor pen；
   - OSC 133 semantic content / 当前 cell input-region hint；
   - stable visible-region digest 和 synchronized-output generation。
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
- snapshot capture 增 blocking-duration hard guard；随后只在 actor 内原子取得 immutable/COW cut，把可安全的
  encode/compress 工作迁出 actor，而不是把可变 Terminal 直接丢给另一个线程；
- recovery delivery 改成 per-client gating，不能在 barrier/replay 期间全局暂停其他 synced client 的 canonical
  output；
- 实现有界合并的 canonical `InputSettle`，作为 prediction verification eligibility cut；明确它仍不是
  app-effect ACK，并让 journal/snapshot cursor 正确处理这个 metadata event。

### Browser

- keydown 不等待上一条 ACK，保持 pipeline；
- control transport/input epoch 变化继续将 outstanding 标成 uncertain，绝不自动 resend；data delivery
  generation、resync 和 snapshot adoption 至少清 prediction epoch，不能误写成当前已经清 input pending；
- 控制输入和 ordinary edit 分队列、分指标；高风险 input queue 超限时 fail closed；
- dispatcher 对 `sent/queued/not-sent/uncertain` 给出逐事件结果；没有 writer transport 时不再静默吞掉已被
  DOM `preventDefault` 的输入；
- paint 和 verifier 不阻塞真实 WebSocket send。

## 分阶段实施

### Phase 0：测量并消除现有 HOL

- 建立端到端 correlation id 与分段 monotonic timestamps；
- 修复 schema parse 失败先消耗 seq=1 导致整个新 epoch 卡死的问题，并补 Browser→Host 接入测试；
- 为 no-transport/no-lease 输入提供明确的未送达 UI/事件结果；
- 拆 Cloud queue、停止每键 lease renewal；
- 量化并限制 snapshot capture actor pause；
- 消除 recovery 对全局 canonical output 的 pause；
- 为 `Ctrl-C -> pty.write -> output quiet -> prompt paint` 建独立指标。

完成门槛：关闭所有预测时，高输出、并发 cold recovery 和 600 ms RTT 下无重复输入、无健康连接静默卡住，
control 的本地处理尾延迟不被 bulk output 线性放大。

### Phase 1：协议与 context hardening

- 为 key/text/paste/control 统一 input identity 与 observed cursor；
- 区分 `written`、`handled-no-bytes`，增 canonical `InputSettle` 并验证 journal/tail/cursor 连续性；
- 同步更新 [Wire protocol](wire-protocol.md) 和 snapshot/tail 不变量，明确 metadata event 不进入 Ghostty、
  不重放 input effect；
- 增 `InputClass/InputPolicy/GuardedContext` 和 `stale-context` 结果；
- guarded input 在 Host authority 上 compare-and-write；
- 把 reconnect、writer transfer、generation change 和 uncertain commit 行为写成协议测试。

完成门槛：所有 invalidating mutation 已在 Host actor 观察到的 stale guarded action 都在 `pty.write` 前被拒绝，
且一次也不自动重试；文档和 API 不把它表述成通用 app-state safety。

### Phase 2：纯展示 overlay 基础

- 给 WTerm renderer 增独立 `PresentationOverlay`；
- 补 parser-ground、pending-wrap、mode、semantic-region 和 digest 观测；
- 先运行 hidden prediction，验证 feature on/off 的 authority state 完全一致；
- 只为单行 ASCII printable/Backspace 小范围开放 visible prediction。

完成门槛：overlay 任意时刻清除后画面立即等于 canonical replica；snapshot、copy、selection、ARIA、日志和
network trace 中都找不到 speculative 内容。

### Phase 3：显式 shell `Owned` buffer

- 实现 bash/zsh/fish integration 的 prompt epoch 与 preexec fence；
- 本地 editor 支持基础 history、multiline、IME、completion fallback；
- Host 实现有界 `commitId` dedup 和 bracketed-paste batch；
- integration 不完整时保持 shell-native `Raw`。

完成门槛：高 RTT 下编辑反馈不依赖 RTT、在本地一帧内完成；commit、执行和 canonical confirmation 仍走网络。
prompt/preexec race、刷新、断线和 ACK 丢失均不会重复或自动提交 command。

### Phase 4：REPL/TUI `Mirrored` driver

- 增 lightweight Ghostty prediction state；
- 从 printable/backspace/left/right 扩到 Unicode、wide/combining、wrap 和 multiline；
- 用在线 compatibility epoch，而不是应用名硬编码，决定是否显示；
- 可增加确定性的 Codex/Claude/readline driver，但 app-specific 规则只能缩小 eligibility；
- synchronized output 按完整 frame 验证。

完成门槛：支持矩阵内 eligible edit 的视觉反馈在一帧内出现；任何 divergence 一帧内回到 canonical，危险动作
从不由 predictor 代执行。

### Phase 5：可选的终端输入区协议

如果纯 PTY heuristics 的覆盖率不足，定义可被 shell/TUI 主动采用的 OSC/DCS input-region 协议：

```text
input-region begin(id, revision, capabilities, secure)
input-region update(id, revision, cursor, digest)
input-region end(id, revision, reason)
```

它仍然是终端协议：应用继续读写 PTY，Zhongduan 不变成 Codex/Claude 专用客户端。协议必须带 nonce/fence、
版本协商和 fail-closed 语义；未知实现忽略它即可。它表达的是应用 opt-in capability，不是抵御同一 PTY 上
恶意应用的授权边界。

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

### 初始发布门槛

- predicted paint p95 不超过一个 60 Hz frame；
- conflicting canonical output 到 overlay 清除 p95 不超过一个 frame；
- supported-load 下 Cloud+Host 非网络 input overhead p99 不超过 25 ms，Phase 0 后按实测校准；
- recovery/snapshot 开启时 control→`pty.write` p99 不得超过无恢复 baseline 的 2 倍；
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
2. 清空 overlay 后 Browser active replica 与 canonical replay 结果完全相同。
3. speculative cells 永不进入 snapshot、tail、copy、selection、ARIA、log 或 network payload。
4. Host 已观察到的 guarded context mismatch 发生时，`pty.write` 调用次数为 0。
5. reconnect/generation change 后旧 seq 不自动重放；uncertain command 不自动提交。
6. writer transfer 后旧 fence 的任何 input/ACK 都不能影响新 writer。
7. prediction failure、driver exception 和资源超限只关闭预测，真实 input path 保持可用。

## Rollout

所有能力独立 feature flag：

```text
input_fast_path_lanes
input_guarded_context_fence
presentation_overlay
shell_owned_buffer
mirrored_ascii_edit
mirrored_unicode_edit
tui_deterministic_drivers
mirrored_echo_off
```

发布顺序是 hidden measurement → internal visible → opt-in → supported app/mode 默认开启。任何层都有全局 kill
switch；关闭时立刻回到当前 raw semantic PTY path，而不需要重启 session 或重建 snapshot。

## 明确不做

- 不把 Codex app-server、Claude SDK/Remote Control 当作终端输入方案；
- 不让 Browser 成为任意 TUI 的权威业务状态机；
- 不用 AI 决定某个按键是否安全、是否提交或是否批准；
- 不把 input bytes 当作 terminal output 喂给 Ghostty parser；
- 不用完整 Ghostty snapshot 做 per-key clone；
- 不因预测命中而跳过真实 PTY input 或 canonical output；
- 不自动重放 uncertain Enter、Ctrl、paste 或 command commit；
- 不承诺在纯 PTY 下识别应用尚未输出的内部 modal 状态；
- 不把 Mosh 的 UDP roaming 当成现代 Browser 部署必须照搬的架构。

## 参考资料

### 项目源码

- Browser input dispatcher：[`apps/terminal-cloud/src/browser/input-dispatcher.ts`](../apps/terminal-cloud/src/browser/input-dispatcher.ts)
- Cloud session relay：[`apps/terminal-cloud/src/worker/terminal-session-do.ts`](../apps/terminal-cloud/src/worker/terminal-session-do.ts)
- Host session authority：[`apps/host-daemon/src/session.ts`](../apps/host-daemon/src/session.ts)
- Ghostty WTerm adapter：[`vendor/wterm/packages/@wterm/ghostty/zig/src/wasm_api.zig`](../vendor/wterm/packages/@wterm/ghostty/zig/src/wasm_api.zig)
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
