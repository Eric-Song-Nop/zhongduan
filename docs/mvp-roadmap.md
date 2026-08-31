# MVP Roadmap

本文是 Zhongduan 当前路线图与完成状态的唯一事实源。系统 owner 与不变量见
[终端协议架构](terminal-protocol-architecture.md)，当前 wire 和 Recovery 状态机分别见
[Wire protocol](wire-protocol.md)与[Recovery 协议](recovery-protocol.md)。已经退役的实现只记录在
[Recovery 实现沿革](recovery-history.md)。

> 状态基线：2026-08-31，全部历史 stacked PR 已合入 `main`。当前产品只有一套无版本名称的
> Recovery 实现，不保留旧协议、旧 Durable Object schema、协商、fallback 或混合提交部署。

## 计划恢复依据

本页恢复的是 Git 历史中正式修订后的计划，而不是按当前代码反向编造的新阶段：

- `d91fb06`（原 stacked commit `158f1c9`）首次记录 Snapshot/Recovery 与高 RTT 输入的完整草案；
- `6ed9a15`（原 `f838a0a`）正式重排 Recovery 阶段为 R0–R5，并把输入路线修订为 A–E；
- `1e485c4`（原 `064a08b`）记录 R0/R1 candidates 与 R2 实施切片的状态，同时保留 R3–R5 target；
- `b1ce18f`（原 `fba2008`，PR #41）在删除旧 Recovery runtime 时压缩计划文档，移除了 R3–R5 标题，
  但没有实现或明确取消它们。

因此本次还原采用 `6ed9a15` / `1e485c4` 的阶段定义，再用当前 `main` 审计实际完成度。输入路线写成
I-A–I-E，避免与 Recovery 的 R0–R5 冲突。被删除的旧 runtime 只保留历史描述，不恢复兼容代码或旧实践。

## 状态词

本路线图把“实现”“证据”和“发布范围”分开，避免把单元测试通过写成生产完成。

| 维度           | 状态                                                                      |
| -------------- | ------------------------------------------------------------------------- |
| Implementation | `absorbed`、`code-complete`、`partial`、`not-started`                     |
| Evidence       | `none`、`unit`、`owner-integration`、`local-e2e`、`staging`、`production` |
| Release scope  | `satisfied`、`MVP-blocker`、`conditional-MVP`、`post-MVP`                 |

- `absorbed`：阶段原有实现已经被后续架构替换，但它建立的正确性要求仍由当前 owner 持有；
- `code-complete`：计划内代码与本地 correctness gate 已完成，不代表 staging 或 production；
- `partial`：已有可复用基础，但该阶段的关键 exit gate 尚未通过；
- `none`：尚无能证明该阶段产品行为的证据；底层 primitive 不等于产品实现；
- `satisfied`：该阶段不再有 MVP release work；
- `conditional-MVP`：必须实现，或用批准的 staging 数据明确证明当前方案已满足 release envelope；
- `post-MVP`：不阻塞首次 MVP 发布，但不能因此把它描述成已经实现。

## 总体结论

当前 `main` 已完成基础终端栈、Recovery R0–R2 和 pre-MVP 单实现迁移：

- Host authority、PTY、journal、snapshot capture 与 immutable publish；
- Cloudflare Worker、SQLite Durable Object、R2 和双 WebSocket relay；
- Browser warm/cold restore、atomic replica adoption 与长期 live receiver；
- Host retained source、Cloud no-payload ledger、shared ring 和公平调度；
- writer lease、input epoch、semantic input、dedup 与 outcome-uncertain fencing；
- committed Ghostty WASM continuation 与真实 `WTermReplicaHost` adoption gates；
- 旧恢复 runtime、wire、schema、strategy、fallback 和命名清理。

项目仍处于 MVP 开发阶段。MVP 前仍需解决 R3 blocker、R4 条件 gate、I-A–I-C、production-like 验证、长期负载、
观测、安全和上线批准；R5 与 I-D/I-E 是已经明确记录的 post-MVP backlog。

## 已完成的 MVP 基础栈

| 基础栈               | 预期交付                                                                            | 当前结果                                                                |
| -------------------- | ----------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| Protocol             | strict control schema、ordered binary mutation、continuity invariant                | 完成；Recovery wire 已收敛为唯一协议                                    |
| Host live session    | 真实 PTY、Ghostty authority、semantic input、resize/query response、有界 journal    | 完成                                                                    |
| Cloud relay          | 认证双 WebSocket、hibernating DO、writer lease、fence、慢 client 隔离               | 完成；早期 direct delivery 已被 durable ledger/ring/scheduler 取代      |
| WTerm/Ghostty        | snapshot encode/passive restore、continuation、ownership、atomic adoption           | 完成；固定 fork 和 committed WASM 继续使用                              |
| Reconnect/checkpoint | warm reconnect、Cloudflare R2 snapshot、cold restore、journal gap、generation fence | 完成；早期 pause/barrier/replay 方案已被 concurrent gap-fill 取代       |
| Operational gate     | 真实 staging、TUI、hibernate、fault、资源和长期负载                                 | 只完成本地 correctness/fault 基础，production-like 部分仍是 MVP blocker |

## Recovery 路线 R0–R5

R0–R5 属于 Recovery/Snapshot 主路线。高 RTT 输入使用独立的 I-A–I-E 编号，不能覆盖这套编号。

| ID  | 目标                                       | Implementation  | Evidence                     | Release scope     |
| --- | ------------------------------------------ | --------------- | ---------------------------- | ----------------- |
| R0  | Recovery 活性、deadline 与事实边界         | `absorbed`      | `owner-integration`          | `satisfied`       |
| R1  | Checkpoint ownership 与 serviceability     | `code-complete` | `owner-integration`          | `satisfied`       |
| R2  | Concurrent gap-fill Recovery               | `code-complete` | `local-e2e`                  | `satisfied`       |
| R3  | 严格有界 immutable cut                     | `partial`       | `unit` / `owner-integration` | `MVP-blocker`     |
| R4  | Rolling snapshot 与 snapshot-aware planner | `partial`       | `unit`                       | `conditional-MVP` |
| R5  | Execution checkpoint 与 history pages      | `not-started`   | `none`                       | `post-MVP`        |

### R0：Recovery 活性与事实边界

原目标是关闭早期 fixed-commit 恢复中的无限等待和无 owner outcome：所有 warm/cold/refresh/drop 路径必须
收敛，matching generation 必须有 deadline，不能靠没有 producer 的状态猜测完成。

当前状态：早期 pause/barrier/replay runtime 已删除；deadline、generation/full-pair fencing、progress retry、
outcome uncertainty 和 fail-closed 原则已经被当前 `RecoveryRuntime`、Host source owner 与 DO socket owner 吸收。

偏差：R0 的旧协议对象不再存在，因此这是 `absorbed`，不是需要保留的兼容层。
正式初稿还把性能事实与 telemetry 放在 R0；后续验收契约把 R0 收窄为活性 correctness。本路线图只把后者记为
完成，未完成的观测和性能工作分别进入 G6/G7，不能由 `absorbed` 状态代替。

### R1：Checkpoint ownership 与 Host-local serviceability

原目标：

- session 而不是 attach/connection 拥有 latest immutable checkpoint；
- 先计算 journal range 的 cursor、bytes 和 frames，再 materialize；
- capture/publish/install 每个可变边界都重新检查 serviceability；
- snapshot body、pending upload、ambiguity cleanup 与 waiter cancellation 严格有界；
- 多 waiter 共享 single-flight，单个 waiter 离开不能取消公共工作。

当前状态：`SnapshotCheckpointManager`、`SnapshotPublisher` 和 `EventJournal.planRangeThrough()` 保留
checkpoint/body/range/single-flight owner 与 identity/lineage/minimum-cut 校验。动态 journal gap/capacity
serviceability 不再由 attach 或 publish 生命周期重查：`SnapshotRefreshOwner` 无条件主动 prime/refresh，
`TerminalSession.prepareRecoveryGap()` 在冻结 recovery gap 时才重新判断是否可服务。

### R2：Concurrent gap-fill Recovery

R2 的目标是让 Host authority 在 Browser 恢复期间继续前进：

```text
warm: visible replica@R + retained gap (R,H] + live [H+1,...]
cold: immutable snapshot@R + retained gap (R,H] + live [H+1,...]
```

完成内容按实施切片记录如下：

| Slice | 完成内容                                                                                         |
| ----- | ------------------------------------------------------------------------------------------------ |
| R2.0  | strict envelope/control、authority cursor、lane ordinal、累计字节与 start fence                  |
| R2.1  | Browser bounded assembler、warm/cold owner、exact retry、deadline、taint、handoff                |
| R2.2  | Host `(R,H]` retained source、`H / fence(H) / H+1`、grant、receipt、deadline、tombstone          |
| R2.3  | DO attempt/lane/outbox/deadline/pin scalar truth、strict socket identity、shared alarm           |
| R2.4  | Host/Cloud/Browser runtime wiring、HTTP restore、atomic adopt、progress retry、long-lived live   |
| R2.5a | no-payload `queued -> sending -> sent` ledger、exact sent-prefix receipt、wake fencing           |
| R2.5b | Host multi-outstanding、prefix release、byte-DRR、per-record yield、source deadline isolation    |
| R2.5c | Cloud shared live ring、session capacity、writer reserve、observer-first reconcile、weighted DRR |
| R2.6  | local three-owner continuity、unsafe wake、committed-WASM continuation、WTerm atomic adoption    |

R2 完成后又发生一次有意的 pre-MVP 破坏性收口：过渡期 capability negotiation、strategy、fallback、kill switch、
双 decoder/attachment、旧 scheduler 与旧 schema migration 全部删除，R2 状态机成为唯一 Recovery 实现。

证据边界：R2 的 `local-e2e` 使用本地 workerd、可控 fault seam 和部分 literal test owner；它不证明真实
Cloudflare/R2、跨进程网络、OS crash、物理 send cut、性能、soak 或 SLO。

### R3：严格有界 immutable cut

目标：制造 snapshot 的 authority pause 本身必须有严格上限，不能让同步 WASM encode 卡住 PTY output、输入或
Ctrl-C。

已完成的前置：

- `TerminalSession` 在一个 actor turn 内原子采样 `eventSeq/nextPtyOffset`；
- snapshot bytes 在返回前 `.slice()`，不别名 authority backing memory；
- encode 后的压缩、hash、上传和 immutable publish 在 actor 外执行；
- Ghostty/WTerm 有 snapshot + exact tail continuation fixtures。

未完成的关键 gate：

- `TerminalAuthority.encodeSnapshot()` 和 Ghostty WASM snapshot encode 仍是完整同步调用；
- JavaScript timer 不能在同步 WASM 执行中抢占，当前 5 秒 budget 只是 Promise race 加事后 `encodeMs` 检查；
- 尚无 COW、bounded copy/off-actor、incremental/yield 或 immutable frozen-cut owner/API；
- 尚无可执行 capture hard max、`capture_actor_pause_p99` 或真实大 snapshot input/output 并发 gate；
- encoder throw/empty 当前会使 authority session fail，而目标行为是只让 snapshot unavailable。

Exit criteria：

1. 选择并验证 COW、bounded copy/off-actor 或 incremental/yield 中至少一种；
2. cut/cursor 原子，encode 后续不占用可变 authority ownership；
3. capture pause hard max 可执行，超限只报告 snapshot unavailable；
4. 真实 Ghostty/WTerm 大 snapshot 下 input/output 仍满足批准的本地 latency envelope；
5. 此 gate 未通过前不得声称 rolling hard deadline 安全。

### R4：Rolling snapshot 与 snapshot-aware planner

已提前完成的基础：

- `SnapshotRefreshOwner` 启动时 prime，成功或失败后按固定 30 秒周期再尝试；
- checkpoint manager 有 latest/high-water、single-flight、minimum cut、follow-up 和 cancellation owner；
- publisher 有 build interval、failure backoff、cursor-ahead deadline、body bounds；
- Cloud retention union latest、active recovery pins、recent snapshots 与 pending uploads。

原计划仍未完成：

- quiet opportunity 与独立 hard freshness deadline；
- 根据 tail bytes/frames/age、restore cost、dirty state 和 attach pressure 做选择的 planner；
- 新 checkpoint 只 rebase 尚未 start attempt 的规划；
- 持续输出下 bounded freshness、input/control SLO 与 snapshot-storm gate；
- rolling/delta snapshot、bounded chain、chain integrity、cancellation 和 GC reference model。

当前实现采用“固定周期 full snapshot”，不是 snapshot-aware planner。这是明确的实现偏差，不应把周期 refresh
描述成 R4 完成。

Exit criteria：

1. R3 先通过；
2. 持续 50–100 ms output、从不 quiet 时，cut 仍有界前进；
3. input/control latency 不回归，无 snapshot storm；
4. 旧 tail 只有在新 immutable checkpoint 吸收后才能删除；
5. 若 MVP 不实现 rolling/delta，必须用批准的 staging 数据明确记录 full snapshot 方案满足 release envelope。

### R5：Execution checkpoint 与 history pages

目标是先恢复可执行 terminal state，再在 adoption 后后台 hydrate scrollback/history：

- execution checkpoint 和 history page 使用独立 schema、integrity、owner 与 cancellation；
- execution + exact tail 达到 adoption gate 后允许 history background hydrate；
- page dedupe、retention、merge 不改变 authority cursor；
- 独立 benchmark 后才决定默认启用。

当前 WTerm fork 已有 `READY/HISTORY/FINISH`、逐页 decode/yield、abandon 与 `takeCore()` 等底层 primitive；但产品
协议仍强制 restore through `FINISH`，Browser 等完整 history 完成后才 adopt。独立 page storage、dedupe、retention、
background hydrate 和 scrollback merge 均未实现，因此 R5 状态是 `not-started`。

R5 不阻塞 R2、R3、高 RTT Mirrored 或首次 MVP 发布。

## 高 RTT 输入路线 I-A–I-E

高 RTT 输入是与 Recovery 并行的 presentation/interaction 路线。完整设计见
[输入稳定性与高 RTT 交互计划](input-stability-plan.md)。

| ID  | 目标                                    | Implementation | Evidence            | Release scope |
| --- | --------------------------------------- | -------------- | ------------------- | ------------- |
| I-A | Input correctness 与 hot path           | `partial`      | `owner-integration` | `MVP-blocker` |
| I-B | Authority-isolated presentation overlay | `not-started`  | none                | `MVP-blocker` |
| I-C | ASCII `Mirrored`                        | `not-started`  | none                | `MVP-blocker` |
| I-D | Unicode/TUI Mirrored 与 input-region    | `not-started`  | none                | `post-MVP`    |
| I-E | 显式 `Owned` shell buffer               | `not-started`  | none                | `post-MVP`    |

I-A 已有 semantic input、writer/input identity、Host dedup、ACK、bounded Browser queue 和 uncertain 不重发基础；
但 strict contiguous sequence、逐事件结果、connection-cached writer hot path、独立 control lane、urgent ordering、
recovery/catching-up input policy、telemetry 和 snapshot-pause measurement 尚未完成。

I-B–I-E 没有产品实现。当前没有 `PresentationOverlay`、`InputValidationCut`、predictor、input-region、shell
ownership transfer 或 local draft/commit。

## 跨路线依赖

```text
R0 -> R1 -> R2                  complete
             |
             +-> R3 -> R4      remaining Recovery/Snapshot work

R5                               independent post-MVP

I-A -> I-B -> I-C -> I-D -> I-E
       |      |
       +------+-- R3 is shared only when snapshot pause exceeds the approved input envelope
```

- I-C 不依赖 R4/R5；
- R3 是 snapshot actor-pause 与高 RTT input SLO 的共享条件；
- R4 是否阻塞 MVP 由 full snapshot staging 数据决定，不能靠单元测试或固定 30 秒 timer 猜测；
- R5 明确独立后置。

## 与原计划的偏差

| 原计划或早期实现                                       | 当前结果                                                                  |
| ------------------------------------------------------ | ------------------------------------------------------------------------- |
| 并发 Recovery 与旧 fixed-commit runtime 并存、协商选择 | pre-MVP 决定只保留并发 Recovery；旧 runtime、wire、schema 全部删除        |
| 通过 capability/strategy/kill switch 渐进 rollout      | 改为 Host、Cloud、Browser 同提交部署；不支持 mixed commit 或协议 fallback |
| Snapshot refresh 由 attach/serviceability 请求驱动     | 收敛为 session-scoped owner 主动 prime 和固定周期 refresh                 |
| R3 先解决严格 bounded cut，再启用周期/rolling capture  | 当前固定周期 refresh 已先落地；同步 encode hard-bound 仍未解决            |
| R4 使用动态 snapshot-aware planner                     | 当前只有固定周期 full snapshot                                            |
| R5 READY-first 后台 history                            | fork 有 primitive，产品仍坚持完整 `FINISH` 后 adopt                       |
| 输入最初使用数字 Phase 0–5                             | 为避免覆盖 Recovery R0–R5，修订为 I-A–I-E，并把 ASCII Mirrored 提前       |
| 输入 wire 计划保留旧 session 协商/fallback             | 单实现决策取消兼容；未来 input wire 只定义当前 schema 和显式 feature gate |
| R0 初稿同时包含性能事实与 telemetry                    | R0 correctness 已吸收；未完成的 observability/release limits 移到 G6/G7   |

这些偏差必须保留在路线图中，直到对应实现完成或通过显式 ADR 修改目标；不能靠删除旧标题把未完成工作变成已完成。

## MVP release gates

以下是唯一的全局 MVP gate。其他文档引用本节，不再维护不同版本的清单。

1. **G1 — R3 snapshot pause**：完成严格有界 immutable cut；同步 capture 必须有可执行 hard max，超限只让
   snapshot unavailable，不能阻塞或终止 authority；
2. **G2 — 高 RTT 输入 I-A–I-C**：完成 input hot path、authority-isolated overlay 和窄 ASCII Mirrored；所有
   不支持、secure 或 context-uncertain 场景保持 Raw；
3. **G3 — Production-like Cloudflare**：验证真实 WebSocket、R2、SQLite DO hibernation、alarm、response loss、
   Host replacement 与 failure sequence；
4. **G4 — 真实终端 E2E 与 soak**：真实 PTY/Ghostty/WTerm、Vim/tmux/htop/Codex 类 TUI、长会话、16 client、
   slow client、output flood、writer input 和 Ctrl-C；
5. **G5 — Snapshot lifecycle**：上传/下载失败、checksum、retention、pin、GC、长期 orphan cleanup；
6. **G6 — Observability**：部署观测、capacity alert、可操作 session diagnostics、分段 monotonic latency 与
   不记录输入内容的 telemetry；
7. **G7 — Release limits 与体验**：把代码 hard caps 区分为 release-approved limits，冻结最低支持负载、
   reconnect UX 和最小 latency envelope；完整 benchmark/dashboard 优化可以后置；
8. **G8 — Security 与上线**：安全审查、secret/capability threat review、部署/回滚/DR 演练、运维文档和显式
   launch approval。

## Post-MVP

- R4 中经数据证明非首次发布必需的 rolling/delta 优化；
- R5 execution checkpoint/history pages；
- I-D Unicode/TUI Mirrored 与 input-region；
- I-E shell-owned editor、completion、context integration；
- 更细 client priority、动态 credit 调参、性能 dashboard 与持续 SLO 优化；
- Host daemon 崩溃后的 PTY/Unix child 恢复；
- 端到端加密与不信任 Cloud 的部署模式。

## 文档职责

- 本页：阶段、状态、偏差、依赖与全局 MVP gate；
- [Recovery 技术计划](high-performance-snapshot-recovery-plan.md)：R0–R5 的技术设计和 exit criteria；
- [输入稳定性计划](input-stability-plan.md)：I-A–I-E 的输入安全、预测与测试矩阵；
- [Recovery 协议](recovery-protocol.md)：当前唯一 Recovery runtime 的规范；
- [Recovery 实现沿革](recovery-history.md)：已经删除的旧实现，仅作历史说明。
