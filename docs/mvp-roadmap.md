# Zhongduan 当前实施 Roadmap

> 状态：当前唯一实施路线
>
> 上位约束：[产品契约与协议边界](terminal-protocol-architecture.md)
>
> CURRENT 运行时事实：[MVP 架构](mvp-architecture.md)与 [Wire Protocol V2](wire-protocol.md)
>
> 既有验收证据：[Phase 0 验收契约](phase-0-acceptance-contract.md)
>
> E1–E4 专项实施契约：[输入核心实施计划](input-core-plan.md)

本文把产品契约转换为可执行的 PR 依赖、证据基线和阶段完成条件。产品不变量或 protocol admission
规则发生冲突时，以产品契约为准；CURRENT 行为发生冲突时，以冻结基线及其实现事实文档为准。

## 执行规则

- A1 先固定产品与协议边界；E0 再建立 raw semantic PTY journey baseline。
- E0 完成前不开始 recovery replacement。后续阶段必须引用 E0 的 workload、oracle 和测量结果，
  不能用未测量的绝对阈值替代基线。
- E1–E4 先收口 input correctness、input latency 和 background-work isolation；E4b 只有在测量证明
  E4a 仍不能满足 gate 时才执行。
- CURRENT protocol v2 在 R4 destructive cutover 完成前是唯一生产运行时。它的 recovery behavior
  保持冻结，只接受 blocker、安全问题和验证既有行为的测试修复；E1–E4 可以收口 input/hot-path gate，
  但不得扩展 recovery 功能或局部删除 pause、barrier、pin。
- R1–R3 可以在测试和隔离环境中构建 replacement，但不能形成第二套并行生产 runtime。R4 必须在完整
  replacement 通过 gate 后一次切换并删除 v2，不能留下长期兼容矩阵。
- E5 是真实应用、Cloudflare staging 和长会话的最终发布 gate，不是用来补做前面缺失的 correctness
  或 latency 设计。

## 证据基线

E0 建立真实 raw semantic PTY path 的 journey harness。第一轮只记录 baseline，不凭空设置绝对 SLO；
baseline 完成后，相关相对阈值必须写入 source-controlled benchmark contract。后续每个阶段都必须说明
自己影响哪些 workload、oracle 和指标，并提供可复现结果。

### 网络与故障矩阵

Journey harness 至少覆盖以下链路档位和故障条件，并在结果中记录实际组合、环境与负载：

| 维度                   | 覆盖值或场景                           |
| ---------------------- | -------------------------------------- |
| Browser ↔ Cloud RTT    | 20 / 100 / 300 / 600 ms                |
| Cloud ↔ Host RTT       | 20 / 100 / 300 / 600 ms                |
| 网络扰动               | jitter、disconnect、reconnect          |
| 负载与恢复             | output flood、cold attach              |
| 基础设施生命周期与替换 | DO hibernation、Host relay replacement |

单一健康路径通过不能代替矩阵证据。每个阶段只需重跑受其变更影响的组合，但 R4 和 E5 必须覆盖声明支持的
完整矩阵。

### 正确性 gate

以下结果是累计发布 gate；E0 必须先为它们建立可执行 oracle，并如实记录 CURRENT baseline 的失败项。
后续阶段逐项收口，R4 前必须全部通过：

1. 被 UI 消费但结果为 silent loss 的 input：0。
2. dedupe window 内 duplicate PTY effect：0。
3. uncertain input 自动重发：0。
4. output flood 下 Ctrl-C 准确写入一次。
5. writer transfer 后旧 writer input 成功次数：0。
6. cold candidate 在验证完成前 visible 次数：0。
7. snapshot/recovery 开启与关闭时，Host authority state 仍由相同 canonical input/output 决定。
8. secure-input 场景启用 speculative presentation 的次数：0。

### 延迟 gate

E0 至少采集以下 span：

- Browser keydown 到 send decision；
- Cloud Browser receive 到 Host send；
- Host receive 到 `pty.write`；
- input 到 matching Browser render；
- PTY output 到 Browser useful render；
- Ctrl-C 到 `pty.write`；
- Ctrl-C 到应用 quiet 或 prompt 恢复。

baseline 完成后，source-controlled benchmark contract 至少必须约束：

```text
在 supported load 内，Cloud input queue wait / latency
  不随无关 Host bulk/data backlog 线性增长
snapshot/recovery 开启后的 Host local input p99
  不超过关闭时 baseline 的约定倍数
output flood 中 Ctrl-C 仍有有界完成时间
```

若 control/input 继续与 bulk work 共享串行执行 owner，必须用 benchmark 证明等价隔离。防挂死 deadline
只负责终止测试，不能作为 latency SLO 的通过证据。

## 主线依赖顺序

```text
A1  产品与协议边界
 |
 v
E0  journey baseline
 |
 v
E1 -> E2 -> E3 -> E4a -> E4b decision
 |
 v
R0 -> R1 -> R2 -> R3 -> R4
 |
 v
E5  staging release gate
```

E4b decision 是强制检查点，但 E4b 实现是条件项：测量证明 E4a 已满足 gate 时，记录“不实施”的证据
并进入 R0；否则完成 E4b 后再进入 R0。

## 阶段契约

### A1 — 产品与协议边界

**依赖：** PR #24 的 CURRENT baseline。

**工作：** 固定三个同级产品 gate、adopted-state reference model、input outcome model、四类对象及其
生命周期、新提案准入规则和 v2 freeze/cutover 边界；文档职责分别落到产品契约、CURRENT 架构、
active roadmap 和专项实现计划。

**完成条件：** 文档不改变 source、runtime、wire schema、capability 或部署行为；后续 PR 能明确引用
稳定产品不变量和本路线的阶段 gate，而无需从 CURRENT v2 的实现反推产品要求。

### E0 — Terminal journey baseline

**依赖：** A1。

**工作：** 为 raw semantic PTY path 建立可复现的真实 journey harness，覆盖本页网络与故障矩阵；为全部
正确性 gate 建立 oracle，采集全部 latency span，并记录 workload、环境、样本和 CURRENT baseline。

**完成条件：** harness 与 baseline 可在 source control 中复跑；当前失败项被显式记录；由测量得到的
相对阈值进入 benchmark contract。没有这些产物，不得开始 R0–R4。

### E1 — Browser validate/admit

**依赖：** E0。

**工作：** Browser 在分配 sequence 前完成 semantic normalization、schema/size/policy validation 和
有界队列 admission；每个被 UI 消费的事件都产生本地可观察结果，并区分 queued、sent 与 uncertain。
可合并 state intent 必须有明确的 supersede 与 identity 规则。

**完成条件：** sequence 只在成功 admission 后分配；未离开 Browser 的事件不会在重连后自动发送；
UI-consumed input 不会静默丢失，所有本地 queue 都有 bytes/count/age 上限及 overload 行为。

### E2 — Cloud input lanes and connection-scoped writer

**依赖：** E1。

**工作：** 明确 control/input 与 bulk data 的执行 owner 和 ordering；writer capability 绑定 connection，
由 heartbeat 续租；input fast path 不执行 per-key storage/hash，并把 overload 隔离到具体连接或任务。

**完成条件：** writer replacement 后旧 connection 不能成功输入；input queue 有界；在 E0 workload 下，
Cloud input queue wait / latency 不随无关 Host bulk/data backlog 线性增长，或以 benchmark 证明共享 owner
具有等价隔离。

### E3 — Host contiguous input epoch

**依赖：** E2。

**工作：** Host 以 `nextExpectedSeq` 执行严格连续 input prefix，定义 deterministic result cache、duplicate、
missing input、uncertain epoch termination、消耗 sequence 的 rejection，以及 `pty.write()` ACK 的准确语义。

**完成条件：** dedupe window 内 duplicate PTY effect 为 0；gap 不会被跳过；无法证明 effect 的 input
终止当前 epoch 且不自动重发；所有确定结果在声明的 retention window 内可重复返回。

### E4a — Finalized background snapshots

**依赖：** E3。

**工作：** attach 只选择 finalized checkpoint；snapshot refresh 不进入 attach/input hot path；明确 authority
mutation 与 encode/compress/upload 的 owner，并测量 authority actor pause 及所有 background queue 上限。

**完成条件：** 未 finalized 的 checkpoint 不可见；snapshot/recovery 开关不改变 authority canonical
state；background work 不造成无界阻塞，并满足 E0 后冻结的 Host local input p99 和 Ctrl-C gate。

### E4b — Immutable Ghostty cut（条件项）

**依赖：** E4a 及其测量结果。

**工作：** 只有测量证明 E4a 无法在现有 cut 模型下满足 gate 时，才引入 immutable/COW Ghostty cut；
否则明确记录跳过原因，不为假设性性能问题增加 owner 或 state machine。

**完成条件：** 若实施，authority 同步 pause 有明确上限，encode/compress/upload 全部在 cut 之外完成，
并通过 E0 benchmark；若跳过，E4a 的通过证据和“不需要 E4b”的决策进入 source control。

### R0 — Freeze v2 behavioral contract

**依赖：** E0–E4a 完成，以及 E4b 已通过或有证据地跳过。

**工作：** 把 A1 的 v2 freeze 政策与 CURRENT pause/barrier/pinned delivery/fixed commit 行为固化为
executable tests，为 replacement 提供可比较的输入、adoption、liveness 与故障 oracle。

**完成条件：** v2 既有 correctness contract 可复现且不再扩展；replacement 的测试必须引用同一组产品
gate。R0 不改变 v2 的唯一运行时地位，也不局部拆除 legacy recovery。

### R1 — Host ordered generation stream

**依赖：** R0。

**工作：** 在 Host 建立有界、有序、带 authority lineage 的 generation stream；recovery attempt 可以终止、
丢弃并从新 generation 重新规划，不要求把 attempt 本身持久化成 terminal truth。

**完成条件：** canonical mutation identity 连续且可验证；gap、replacement 和 outcome uncertainty 都会
fail closed；流与队列有明确上限，abort/restart 不阻塞 authority input/output hot path。

### R2 — Browser stream runtime

**依赖：** R1。

**工作：** Browser 只向 detached cold candidate 应用 checkpoint/suffix，并在验证后原子 adopt；warm
recovery 只原地应用已验证的连续 suffix，cursor 不得越过实际 apply prefix。

**完成条件：** restore + canonical suffix 与 uninterrupted authority engine 通过同一 normalized-state 和
continuation oracle；失败 candidate 不可见、不污染 adopted replica，并可被完整释放后重新 attach。

### R3 — Ephemeral Cloud relay

**依赖：** R2。

**工作：** Cloud 只中继当前 generation 所需的有界 control/data，不把 recovery attempt 提升为长期 durable
owner；DO hibernation、relay replacement 或 transport uncertainty 可以结束 attempt，并由 Browser/Host
从仍有效的 identity 重新规划。

**完成条件：** hibernation、reconnect、cold attach 和 Host relay replacement 通过对应 fault matrix；
丢弃 attempt 不会采用错误 state、重复 input effect 或无限期阻塞 live traffic。

### R4 — Destructive cutover and delete v2

**依赖：** R1–R3 完成，且 replacement 通过全部正确性与延迟 gate。

**工作：** 以明确 capability、rollout、rollback-before-cutover 和 cleanup 边界切换到 replacement；删除 v2
runtime、pause/barrier/pin 路径及其专用 schema、状态和兼容分支。

**完成条件：** replacement 成为唯一 runtime；仓库中不再存在双协议兼容矩阵；声明支持的完整网络与故障
矩阵通过。R4 完成前，CURRENT v2 始终是冻结且唯一的运行时。

### E5 — Real applications and Cloudflare staging release gate

**依赖：** R4。

**工作：** 在真实 Cloudflare staging、真实 TUI/交互应用和长会话中验证完整 journey，包括 output flood、
Ctrl-C、disconnect/reconnect、cold attach、DO hibernation、relay replacement 和资源压力。

**完成条件：** source-controlled staging suite 在声明支持的矩阵内通过全部 correctness/latency gate；queue、
bytes、frames、age、concurrency 和资源预算都有观测证据；发布、回滚和 destructive cleanup procedure 已演练。

## 当前阶段性 scope exclusions

在 E5 完成前，本轮路线不进入以下主题：

- Mirrored prediction；
- Owned shell command buffer；
- Codex/Claude application driver；
- input-region protocol；
- rolling/delta snapshot；
- history pages；
- recovery fairness 调参；
- multi-writer；
- Recovery v3 compatibility；
- terminal state delta 网络格式；
- Mosh 式 UDP roaming。

这些主题只有在重新通过产品契约中的 proposal admission rules 后，才能形成新的 roadmap 项；不能插入
A1–E5/R0–R4 主线，也不能作为扩展 CURRENT v2 的理由。
