# Phase 0 验收契约

Phase 0 只收口 CURRENT protocol v2 的 recovery 活性。它不实现 TARGET Recovery v3，也不以测试数量、
分支存在或笼统的 E2E 标签代替具体状态转换证据。

本契约按 revision 判定：只有该 revision 同时包含实现和下列验收测试，且测试通过，才能称为 Phase 0
candidate complete；只有进入 `main` 后才能称为主线完成。每个 Phase 0 PR 必须在正文中写明它翻转的
sub-gate、翻转前后的状态、直接观察该状态的测试以及测试局限。只增加基础设施而不翻转 sub-gate 的 PR
不能声称完成度前进。

## 本轮保留的执行教训

- 先冻结顶层 gate，再开始实现；每个 PR 必须让一个预先定义的 gate 从 fail 变为 pass。
- 测试名称和结论必须等于 oracle 直接观察到的事实。ACK、socket send、DOM mutation、RAF、timeout 或
  最终 `live` 不能替代它们没有观察到的 app effect、paint、性能或中间 recovery 状态。
- supporting infrastructure、测试数量和绿色 CI 本身不代表阶段进度；stack review 必须重新检查顶层 gate，
  不能只检查每层 diff 是否安全。
- 无法可靠观察的目标保持为明确非目标或后续工作；不为了填满验收表而增加 proxy test。
- 性能验证使用独立、预先批准的 workload、环境和统计方法；在该方法存在前，不把 correctness smoke 当作
  latency、SLO 或 overhead 证据。
- WTerm 或 Ghostty 的任何变更必须先在 `Eric-Song-Nop` 自己的 fork 上通过正式 PR，再更新本仓库固定的
  submodule 指针；不得直接把未审 fork 工作混入 Zhongduan 的阶段 PR。

## 必须通过的 gate

### P0.1：保留 protocol v2 correctness invariant

- canonical output 在 recovery marker/barrier 未确定前继续遵守既有 pause、fixed commit 和 pinned replay
  顺序；
- 只有精确匹配 delivery identity 的 `ready` 才能产生 `ReplayCommit`；
- timeout、错误 identity、错误 generation、错误 canonical head 或不确定 marker 继续 fail closed，不能被
  当作成功。

### P0.2：non-ready outcome 必须收敛

- `missing-live-seed/same-generation` 与 legacy warm rejection 转为同 generation 的 cold recovery；
- rejected cold checkpoint 在 bounded backoff 后 refresh，连续 canonical output 不能无限延后 refresh；
- `drop-client` 只结束已经隔离的目标 delivery，不发布 commit，也不关闭健康 Host pair；
- stale/supersede 只结束对应 delivery generation；显式 generation reset 由既有
  `replay-unavailable`/delivery-reset owner 发起，不能由 barrier rejection 猜测；旧 generation 不能删除或
  提交新 generation 的工作；
- snapshot 暂时不可用仍是 publisher 的 retryable 状态，Host offline/unavailable 仍是连接状态；两者都不是
  可以静默结束 recovery 的 terminal outcome；
- non-ready outcome 不得进入无后续动作的 catching-up 状态。

### P0.3：CURRENT recovery completion 必须有界

- Browser 在 CURRENT 最大合法 cold-service window 内仍未收到 recovery start 时，必须关闭该 connection
  set 并进入 reconnect；
- warm `replay-start` 已收到但 matching `ReplayCommit` 一直不到时，同一 deadline 仍然生效；只有真正进入
  `live` 才取消它；
- snapshot restore 使用 SessionCoordinator 自己的 restore deadline，不能因 attach-start watchdog 的存在而
  改变 restore ownership。

这些 deadline 只是活性上界，不是 latency SLO。

### P0.4：enriched outcome 可以滚动发布

`reason + retryScope` 只对协商了 `delivery-barrier-outcome-v1` 的 Host 启用；旧 Host 继续收到 legacy shape，
未知 future capability 被忽略，协商结果跨 Durable Object hibernation 保留。新旧组件 skew 不能改变上述
correctness 行为。

## 可复现诊断报告

Phase 0 的本地诊断报告入口是：

```bash
pnpm exec vp run verify-phase0
```

该任务固定运行下列 gate owner 的直接测试：protocol/capability、Host barrier waiter/recovery
queue/scheduler、Cloud relay/runtime，以及 Browser TerminalSession。测试 runner 输出的用例名、明确状态断言和
pass/fail 结果构成 revision 对应的可复现报告；它不依赖生产 telemetry、外部 dashboard 或网络服务。

该报告只回答 P0.1–P0.4 的 correctness 问题。它没有 latency 数值，不区分纯 network RTT，不观察通用 app
effect 或 pixel paint，也不能被用于性能、SLO 或 production rollout 结论。

## 当前验收证据

以下测试直接检查 Phase 0 所声明的状态转换：

- `apps/host-daemon/src/cloud/delivery-scheduler.test.ts` 检查 warm rejection 转 cold、cold rejection refresh、
  continuous-output hard deadline、isolated-client drop、uncertain marker fail-close、generation supersede，以及
  `ReplayCommit` 只在 ready 后出现；
- `apps/host-daemon/src/cloud/delivery-recovery-queue.test.ts` 检查 quiet deferral 的 hard bound、per-delivery
  backoff 和旧 generation 不影响新 generation；
- `apps/terminal-cloud/src/browser/terminal-session.test.ts` 检查 start 完全缺失和 warm start 后缺失 matching
  commit 都会在 deadline 后关闭两个 socket 并进入 reconnect；matching commit 在 deadline 前进入 `live`
  后不会被旧 watchdog 关闭；
- `apps/terminal-cloud/test/relay.test.ts` 检查 snapshot missing、metadata mismatch、Browser control isolation、
  client gone、generation fence、hibernation 和 legacy/enriched rollout 下的精确 barrier outcome；
- `packages/protocol/src/control-frame.test.ts` 与 `packages/protocol/src/cloud-api.test.ts` 检查严格 outcome
  schema、retry scope 和 bounded capability intersection。

这些测试的 oracle 是 scheduler、queue、Browser session、Durable Object relay 和 protocol decoder 的明确
状态、frame 顺序、socket 状态与 terminal outcome。Phase 0 不使用页面出现某段文本、单次 wall-clock
耗时或间接 telemetry 作为上述 correctness 的替代证据。

## 局限

这些是受控的状态机、组件和本地 Worker runtime 测试。它们不证明：

- production Cloudflare Durable Objects、R2、真实网络或真实滚动发布的行为；
- snapshot restore/adoption、tail continuity 或 TARGET Recovery v3 的 gap-fill correctness；
- Browser、Host 与 Cloud 之间的纯 network RTT；
- 通用 shell/TUI application effect、WTerm/Ghostty pixel paint 或 composite；
- latency percentile、99.9% 成功率、throughput、资源上限或 instrumentation overhead；
- 多客户端公平性、writer transfer、output flood、load、soak 或完整输入矩阵。

因此，Phase 0 完成只表示 CURRENT v2 的上述 recovery 活性边界有直接状态机证据，不表示终端恢复、输入
稳定性或性能项目整体完成。

## 后续需要的验证

后续阶段需要分别设计，而不是扩张 Phase 0 的 correctness tests：

- Recovery v3 的 state model、property/fuzz、generation/retry/reset/adopt 和 snapshot tail-continuity oracle；
- 两条链路的 fault injection，以及 production-like staging 对 DO/R2 与滚动发布的验证；
- 多客户端、writer transfer、output flood、load/soak 和资源有界性；
- 能区分 Cloud、Host、PTY 与受支持 workload effect 的运维查询或 dashboard；
- 有明确 workload、link profile、warm-up、sample size、统计方法和资源隔离的可复现性能验证。

性能、production dashboard、纯 network 分解与通用 application-effect 验证仍然有价值，但它们属于后续
独立工作，当前不实现，也不阻止 Phase 0 按本契约收口。任何 timeout 都只能作为防挂死或状态机 deadline，
不能被解释为性能证据。
