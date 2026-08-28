# 高性能远程终端 Snapshot 恢复实施计划

> 状态：方向已确认，等待实施
>
> 决策日期：2026-08-28
>
> 适用范围：Host authority、snapshot 发布、journal、Cloud delivery 和 Browser 恢复

## 产品目标

Zhongduan 的目标是高性能、稳定、browser-native 的远程终端体验。Ghostty 完整状态的精确
恢复不是产品目标；它只是 snapshot handoff 正确时应满足的底层实现条件之一。

本计划不采用 Mosh 式“为了画面新鲜度任意跳过中间 presentation frame”的协议。我们使用
滚动、不可变 snapshot 压缩已经发生的历史，只传所选 snapshot cut 之后仍然必要的有序
terminal mutation。

```text
健康连接：       canonical mutation stream
小型同源缺口：   short exact tail
页面刷新/长缺口：latest usable snapshot@R + required tail(R,C]
tail 变得过长：  构建 snapshot@R'，丢弃已被 R' 吸收的 (R,R']
```

这里“丢弃不需要的东西”有严格含义：

- `eventSeq <= R` 的状态已经包含在 `snapshot@R` 中，不再传输；
- `eventSeq > R` 且不在更新 snapshot 中的 mutation 仍然需要按顺序传输；
- 可以在 transport 中批量编码相邻 mutation，但不能改变其语义顺序或漏掉 resize/PTY bytes；
- 一旦换到 `snapshot@R'`，`(R,R']` 才变成不需要的历史；
- 不允许在同一个旧 snapshot 后任意截断 tail。

## 能达成的体验效果

实施完成后：

1. 页面刷新和长时间断线的恢复成本主要取决于最新 snapshot 的大小与短 tail，而不是断线期间
   产生的全部输出量。
2. Snapshot 已经存在时，fresh Browser 可以立即开始下载；多个客户端复用同一构建。
3. 持续 TUI 刷新时，snapshot cut 仍受限地前进，不再依赖 250 ms quiet 才能重建 baseline。
4. Browser 在 detached core 中恢复，旧画面保持可见，追上后原子替换，不闪空白或采用半状态。
5. 小型网络缺口仍走 warm tail，避免不必要的 snapshot 压缩、上传、下载和 restore。
6. Ctrl-C、input ACK、writer lease 和 resync 不被 cold recovery 或历史输出阻塞。
7. Snapshot、journal、tail、WebSocket queue、R2 retention、worker 和 WASM ownership 都有界。
8. 失败路径明确 retry、rebase、reset 或 stale；健康 socket 不会无声冻结在 catching-up。

## 正确性边界

### Snapshot 覆盖什么

Snapshot 需要自洽地恢复后续 terminal execution 所需的状态。当前 Ghostty snapshot 包含主/备用
screen、配置范围内的 history、cursor/modes 和未完成的 UTF-8/CSI/OSC/DCS continuation。

这不等于产品承诺无限 transcript。已经超出 scrollback 限额的历史、Unix child/PTY 进程状态、
Kitty graphics，以及 bell/clipboard/query 等瞬时 effect 不属于默认恢复承诺。

### Tail 覆盖什么

当前 data protocol v2 中，所选 cut 之后直到 commit 的 tail 必须包含每一个 canonical terminal mutation：

- `PTY_OUTPUT`
- `RESIZE_APPLIED`

两者共用严格递增的 `eventSeq`，并以 `ptyOffset` 验证 PTY byte 位置。Snapshot continuation 是
cut 之前的 parser 材料，不能当作新的 PTY output 重复发送。

[输入稳定性计划](input-stability-plan.md)后续提议的 `InputSettle` 是 canonical metadata event：它会消耗
`eventSeq`、保持 `ptyOffset` 不变并进入 journal/tail，但不送入 Ghostty parser，也不重放 input effect。
引入该 wire change 时，replay cursor 必须保留它的顺序，terminal-state oracle 则把它当作 no-op。

### Rebase 与 barrier

Barrier 发出前可以 latest-wins 地更新 baseline：

```text
waiting / ready(snapshot@R) / pre-marker(snapshot@R)
  -> ready(snapshot@R'), R' > R
```

Barrier 一旦可能发出，同一 `deliveryGeneration` 的 `{snapshotId, R, C}` 就不可更换：

```text
marker-uncertain / pinned(snapshot@R, commit=C)
  -X-> snapshot@R'
```

此后只能完成原来的 `tail(R,C] + ReplayCommit(C)`，或者 reset 并增加 generation 后重新选择
snapshot。这个 fence 是为了避免把新 snapshot、旧 tail 和旧 commit 错拼，不是产品层追求
byte-for-byte state 的目的。

## 高性能设计原则

### 恢复成本按最新 baseline 计算

`RecoveryPlanner` 不按“还欠多少历史输出”规划，而是比较：

- Browser 是否仍有同 engine/epoch 的 live core；
- journal 是否连续覆盖当前缺口；
- warm tail 的 bytes、frames 和预计发送时间；
- 最新 checkpoint 的 age、cut 和 restore 成本；
- 从 checkpoint 到当前 head 的必要 tail 成本。

规则：小缺口优先 warm tail；其余使用最新可服务 snapshot。Snapshot tail 超过预算时不发送截断
tail，而是在 barrier 前请求更新 checkpoint，使旧 mutation 被新 snapshot 吸收。

### Snapshot 构建不绑定单个客户端

`SnapshotPublisher` 和 `SnapshotCheckpointCache` 已由 `HostCloudRelay` 持有并跨 relay reconnect
复用。需要新增 session-owned `SnapshotCheckpointManager`，收口当前仍在 scheduler/request 中的
capture、`ColdPreparation`、`#coldBuildInFlight` 和 waiter 生命周期。

Manager 同时持有：

```text
latestReady       一个可立即复用的 immutable checkpoint
refreshInFlight   至多一个后台构建
needsNewerCut     合并后的后续刷新需求
```

职责：

- 每 session 最多一个 capture/compress/publish pipeline in flight；
- 多个 cold waiter 共享 build，单个 generation 被替换只移除 waiter；
- 只安装同 engine、同 epoch 且 cut 单调前进的 checkpoint；
- 刷新过程中 `latestReady` 继续可服务，不能暂时清空；
- pending upload 以相同 snapshot ID 和 immutable body 幂等重试；
- `cursor-ahead` 等待 Cloud canonical head 后重试同一 body，不触发 recapture storm；
- session dispose、epoch 变化和 authority failure 恰好释放一次资源。

### Quiet 是机会，不是正确性门槛

当前 cold retry 依赖 trailing quiet；持续输出可能永久不触发新 snapshot。目标策略：

```text
有 quiet opportunity -> 尽早 capture
达到 hard capture deadline -> 即使 output 持续也 capture
```

同时保留 single-flight、最小构建间隔、失败 backoff 和 capture pause 保护。多次 cold waiter、age、
tail pressure 触发合并到同一个 `needsNewerCut`，而不是并发构建。

### 输入和控制优先

本计划不需要另造 presentation-state 协议，但必须保证恢复流量不损害交互：

- control 与 data WebSocket 继续分离；
- input、Ctrl-C、lease、heartbeat、resync 和 ACK 使用独立有界队列；
- snapshot 压缩/upload/download/history page 不持有 control 所需资源；
- canonical paused queue 超限时只中断可安全重启的 pre-marker recovery；
- pinned/marker-uncertain attempt fail closed，不跨 generation 拼接；
- 慢客户端只 reset 自己的 data delivery，不影响其他客户端。

输入 ACK、context fence、高 RTT 视觉反馈和 PTY/TUI prediction 的完整设计见
[输入稳定性与高 RTT 交互实施计划](input-stability-plan.md)。

### Browser passive restore

继续使用 detached Ghostty core：

1. 旧 core 保持挂载和可见；
2. worker 下载、校验和解压 snapshot；
3. passive handler 禁止 bell/clipboard/query 等副作用重放；
4. candidate 恢复 snapshot 并应用必要 tail；
5. 只有 cursor 到达 pinned commit 才原子 `adoptCore()`；
6. 被 supersede 的 decoder、history page、worker 和 core 恰好释放一次。

精确 cursor 是防止采用损坏 candidate 的内部检查；用户体验指标仍然是恢复延迟、画面连续和资源
稳定，而不是暴露 Ghostty 内部状态等价性。

## Snapshot 触发与预算

### 强制触发

- cold attach 没有可服务 checkpoint；
- journal 从 checkpoint 到 head 已有 gap；
- 必要 tail 的 bytes、frames 或预计耗时超预算；
- snapshot upload 已确认不可服务；
- checkpoint engine/epoch 不再匹配。

### 机会性触发

- checkpoint age 超过软目标且 authority 已变化；
- cut 后累计 mutation 成本超过软目标；
- 活跃或近期活跃 session 预计会发生页面刷新/新 observer attach；
- staging 数据表明主动构建能显著降低 P95/P99 recovery latency。

Idle 且状态未变化时不能重复上传等价 snapshot。具体软阈值先 shadow 计算，再根据 capture pause、
snapshot size、R2 cost 和 recovery latency 确定。

### 当前运行时硬上限

这些是不同层的保护条件，不是一个可以互换的“tail budget”：

| 层级/用途                         | 当前值                                                                   | 源码入口                                                 |
| --------------------------------- | ------------------------------------------------------------------------ | -------------------------------------------------------- |
| Journal segment / retention       | 256 KiB 或 250 ms；60 s / 8 MiB                                          | `apps/host-daemon/src/journal.ts`                        |
| Host warm replay admission        | 256 KiB / 512 frames                                                     | `apps/host-daemon/src/cloud/delivery-scheduler.ts`       |
| Host cold snapshot tail admission | 256 KiB / 512 frames                                                     | `apps/host-daemon/src/cloud/delivery-scheduler.ts`       |
| Relay delivery outstanding        | 512 KiB                                                                  | `packages/protocol/src/delivery-credit.ts`               |
| Host paused canonical queue       | 8 MiB / 1024 frames                                                      | `apps/host-daemon/src/cloud/canonical-publisher.ts`      |
| Browser recovery tail             | 2 MiB / 1024 frames；5 s restore deadline                                | `packages/session-client/src/session-coordinator.ts`     |
| Checkpoint cache / build cadence  | 30 s TTL；1 s minimum build interval                                     | `snapshot-checkpoint-cache.ts` / `snapshot-publisher.ts` |
| Snapshot blob                     | 32 MiB compressed / 128 MiB uncompressed                                 | `packages/protocol/src/snapshot.ts`                      |
| Cloud retention                   | 32 snapshot/upload rows；4 reservations；latest + active pins + 2 recent | `snapshot-store.ts`                                      |

迁移初期不顺手放宽这些上限。Phase 3 可以按实测调整 policy，但必须保留独立 hard cap、故障测试
和 rollback threshold。

## 不可避免的容量边界

合理使用 snapshot 可以大幅缩短 tail，但不能违反吞吐的物理边界。设：

- `lambda`：authority 产生 mutation 的速率；
- `T_publish`：capture、compress、publish/finalize 的 p99 时间；
- `T_restore`：download、decompress、restore 的 p99 时间；
- `mu`：Browser apply tail 的速率；
- `B_host`、`B_browser`：Host admission 与 Browser buffer 上限。

一次 exact handoff 容易收敛的条件是：

```text
lambda * T_publish < B_host
lambda * T_restore < B_browser
mu > lambda
```

如果短时间不满足，planner 应更新 snapshot baseline，而不是发送旧 backlog。若长期不满足，系统
必须保持资源有界、保留 control 并明确重试/reset；不能靠无限增加 snapshot 频率或 buffer 假装
解决。后续只有数据证明需要时，才评估降低 encode/restore latency、提高预算或有界 PTY 背压。

## 当前已确认差距

| 领域          | 当前基础                                         | 待改进                                                   |
| ------------- | ------------------------------------------------ | -------------------------------------------------------- |
| Authority cut | actor 内同步捕获 snapshot、seq、offset           | 建立 capture pause SLO 和保护模式                        |
| Snapshot      | Ghostty full state、zstd、R2、passive restore    | 构建/preparation 仍偏 request-owned                      |
| Rebase        | tail gap/超限会废弃旧 checkpoint                 | 重建依赖 250 ms trailing quiet                           |
| Delivery      | barrier、directed tail、explicit commit          | `rejected` 被当成 `stale` complete，可能冻结 Browser     |
| Browser       | detached restore、generation fence、atomic adopt | 缺 attach-start watchdog 和真实画面 E2E gate             |
| Multi-client  | writer/observer、独立 generation                 | 需要共享 build 且隔离慢客户端成本                        |
| Performance   | 各层已有硬上限                                   | 缺 snapshot age、tail cost、rebase 和 phase latency 指标 |
| Documentation | 当前架构已记录主要保护                           | 历史 2/8 MiB 与实际 256 KiB/512 frames 漂移              |

## 实施阶段

### Phase 0：修复现有活性并建立性能事实

- Delivery 返回 `committed | stale | rejected`；只有 committed/stale 才 queue `complete()`。
- Warm rejected 转 cold requeue；cold rejected 使不可服务 checkpoint 失效并有界 requeue/reset。
- 增加 Browser attach-start watchdog，防止健康 socket 永久没有 manifest/replay/reset。
- 区分 journal retention、Host admission、relay credit、Browser buffer 和 canonical queue 指标。
- 增加 capture/publish/download/restore/tail/barrier 分段 latency 与 snapshot size 指标。

验收：snapshot 在 publish 到 barrier 间不可服务时，Browser 最终 retry/live 或明确 reset；不能
无声冻结。Phase 0 独立发布，不受后续策略 feature flag 回滚。

### Phase 1：Session-owned checkpoint manager

- 抽取 `SnapshotCheckpointManager`，复用已有 relay-scope publisher/cache。
- Scheduler 只规划 recovery、选择 immutable checkpoint 并执行 barrier/tail/commit。
- 多 waiter coalesce；request supersede 不取消公共 build。
- 保持 `latestReady + refreshInFlight`；pending body 幂等 resume。

验收：每 session 最多一个 build；16 个并发 cold attach 只 encode/upload 一次；relay reconnect、
cursor-ahead、响应丢失和 dispose 不重复 capture、不泄漏资源。

### Phase 2：Rolling snapshot 与安全 rebase

- 去掉 quiet 作为重建正确性 gate，加入 hard capture deadline。
- `ColdTailUnavailable` 改为 checkpoint-stale 决策，不算基础设施失败。
- 新 checkpoint 发布后，所有 pre-marker waiter 重新计算并选择更近 cut。
- marker-uncertain/pinned attempt 不换 baseline；需要时 reset generation。
- Cloud retention 保留 latest/recent/active pins，不删除仍在 delivery 中的旧 snapshot。

验收：每 50–100 ms 持续 output、从不 quiet 时，snapshot cut 仍受 rate limit 地前进；旧 tail 被
新 snapshot 吸收；新 cut 后第一条 mutation seq/offset 精确相邻；无 snapshot storm。

### Phase 3：主动维护与 snapshot-aware planner

- Planner 根据 live core、gap、tail cost、checkpoint age 和预计总恢复时间选择 warm/snapshot。
- 使用 shadow 数据确定 snapshot age、dirty bytes 和 cold waiter 触发阈值。
- 活跃 session 机会性维护新 checkpoint；idle/unchanged 不重复构建。
- Fresh page 优先复用已有 checkpoint，不默认现建。
- 所有 policy 参数集中配置并在启动时输出快照。

验收：小 gap 仍走 warm tail；页面刷新/长 gap 的 P95/P99 tail 和恢复时间显著低于 legacy；主动
snapshot 的 CPU/R2 成本处于批准预算。

### Phase 4：Snapshot pipeline 与必要 tail 优化

按测量结果依次评估：

1. 降低 authority 同步 encode pause，必要时设计 copy-on-write/off-thread encode；
2. Browser 下载/restore 时有界接收 tail，减少串行等待；
3. 对相邻 PTY mutation 做不改变语义的 transport batching；
4. history page dedupe、增量 upload 或压缩优化；
5. 只有输入/控制仍受输出影响时，再调整 lane priority 和 credit。

不引入任意 presentation-frame 丢弃，也不把完整 transcript 放入关键路径。

### Phase 5：Shadow、灰度和默认启用

使用按 session epoch 固定的 `recoveryStrategy=legacy | shadow | rolling-snapshot`：

1. Shadow 只计算 build/rebase/selection 决策与预计成本，不新增 upload。
2. Staging 运行持续输出、慢 R2、DO hibernation、relay reconnect 和多客户端故障注入。
3. Internal observer/writer 各运行 24 小时。
4. 依次灰度 1%/24 小时、10%/48 小时、50%/48 小时和 100%。
5. 回滚只影响新 attempt；已 marker/pin 的 delivery 按原 generation 完成或 fence。

## 验证矩阵

### Snapshot/rebase 正确性

- 对固定和随机序列选择多个 `R/R'/C`，验证更新 snapshot 确实吸收 `(R,R']`。
- `snapshot@R' + required tail(R',C]` 的最终可见画面、cursor、modes 和后续 parser 行为与
  uninterrupted authority 一致。
- 这是一项内部安全 oracle，不是产品层要求 Browser 暴露完整 Ghostty 状态等价。
- 覆盖 UTF-8、CSI、OSC、DCS continuation、primary/alternate、resize/reflow 和 scrollback。
- Tail gap、重复、乱序、错误 offset/epoch/engine/generation 一律 fail closed。

### 生命周期与故障

- Single-flight、16 waiters、supersede、relay reconnect、pending upload、cursor-ahead。
- Encode throw/empty/timeout、zstd failure、upload 5xx/response loss、R2 404/checksum mismatch。
- Barrier ready/stale/rejected、ACK 丢失、marker 后断线、DO hibernation。
- Browser download/restore timeout、generation bump、candidate abort、history cancel、dispose once。

### 性能和体验

- 页面刷新、warm gap、cold gap、持续 htop/vim/tmux、高速日志、`yes` + Ctrl-C。
- RTT 20/100/300/600 ms，loss 0/1/5/10%，慢 Browser 和 16 observers。
- 测量 capture pause、snapshot size、publish/download/restore、tail bytes/frames、time-to-visible/live。
- 30 分钟 soak 验证 Host RSS/CPU、Browser heap/WASM、R2 rows/bytes 和 build cadence 无持续增长。

## 指标与上线 gate

核心指标：

- `snapshot_capture_ms`、`snapshot_publish_ms`、`snapshot_download_ms`、`snapshot_restore_ms`；
- `snapshot_age_ms`、`snapshot_bytes`、`snapshot_builds{reason,result}`、`build_inflight`；
- `recovery_mode{warm,snapshot}`、`rebase_count{reason}`、`tail_bytes/frames`；
- `recovery_to_visible_ms`、`recovery_to_commit_ms`；
- `input_ack_ms`、`ctrl_c_to_effect_ms`、canonical/control queue pressure；
- Browser tail peak、worker/WASM live ownership、Cloud retention/upload rows。

初始 staging gate：

- 可服务负载内 cold recovery 成功率至少 99.9%；P95 commit 不高于 5 s、P99 不高于 15 s；
- 输出持续但处于支持 envelope 内时，无 quiet 也能完成 recovery；
- `yes` 过载时队列/内存有界，Ctrl-C ACK P99 不高于 500 ms；负载降低后 10 s 内恢复；
- 每 session build in flight 始终为 0 或 1，多客户端不重复 build；
- 无错误 snapshot adoption、tail hole、跨 generation 拼接、重复 effect 或 ownership leak；
- 无 recovery 时 input latency、canonical throughput 和 Host CPU 相比 legacy 回归不超过 5%；
- 30 分钟 soak 后 Host RSS、Browser heap 和 R2 retention 不存在持续正斜率或超硬上限。

## 与 Mosh 的准确比较

本方案与 Mosh 都利用“状态可以覆盖过去输出”的思想，但边界不同：

```text
Mosh:       同步最新可见 screen state，可跳过任意中间 screen frame
Zhongduan:  更新完整 checkpoint 吸收旧 mutation，只传新 cut 后必要 tail
```

计划落地后，Zhongduan 有机会在页面刷新、fresh Browser attach、多 observer、writer fencing、有限
history 和现代 Ghostty TUI 兼容方面提供更好的产品体验。

但本恢复子系统不实现 Mosh 的 UDP roaming、predictive local echo 或 visible-state frame-rate control；
predictive input 的独立方向见[输入稳定性计划](input-stability-plan.md)。项目也仍信任 Cloud relay。因此不能
称为在所有网络、输入延迟、安全和成熟度维度“全面超过 Mosh”。
更准确的目标是：在 Zhongduan 面向的 browser/multi-client 场景中，用更合理的 snapshot 管理
获得高性能稳定体验，而不是复刻 Mosh 的全部协议选择。

## 非目标

- 任意丢弃尚未被 snapshot 吸收的 mutation；
- Mosh 式 visible-screen state protocol；
- 本恢复子系统内的 predictive local echo、UDP roaming 或完整 SSH 替代；输入预测另见
  [输入稳定性计划](input-stability-plan.md)；
- 无限 transcript 或审计 archive；
- daemon 崩溃后复活 Unix child/PTY；
- 跨 `engineId` snapshot 迁移；
- 默认不信任 Cloud 的端到端加密；
- 没有数据证明前重写 Ghostty snapshot schema。

## 完成定义

- Phase 0–3 默认启用，Phase 4 的必要优化完成或被数据证明不需要；
- 所有 rebase、持续输出、故障注入、多客户端和 soak gate 通过；
- Dashboard 能解释恢复慢在 capture、publish、download、restore、tail 还是 barrier；
- 文档预算与代码一致，rollback 在 staging 验证；
- 用同环境 Mosh/SSH 基准描述各自优势，不以架构推断宣称“全面超过”。
