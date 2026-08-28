# Stacked PR 路线

> 历史文档：这里记录 MVP 最初的 stacked PR 规划。相关实现已于 2026-08-28 合并到
> `main`，不再用本页判断当前功能或部署状态。当前入口见[项目 README](../README.md)和
> [部署指南](deployment.md)。当前演进的最高级协议不变量见[终端协议架构](terminal-protocol-architecture.md)，
> 具体工作分别见[高性能远程终端 Snapshot 恢复实施计划](high-performance-snapshot-recovery-plan.md)与
> [输入稳定性与高 RTT 交互实施计划](input-stability-plan.md)。

每层 PR 都基于前一层分支，并提供一个可运行或可验证的纵向能力。后续 PR 只在其直接依赖合并后改 base，不压平历史。

| Stack | 分支                          | 可验证交付                                                                                                | 依赖                      |
| ----- | ----------------------------- | --------------------------------------------------------------------------------------------------------- | ------------------------- |
| 1     | `mvp/protocol-contract`       | Vite+ workspace、严格控制协议、二进制有序 mutation codec、连续性测试和 CI                                 | 无                        |
| 2     | `mvp/host-live-session`       | 本地 daemon 启动真实 shell；PTY output、semantic input、resize、query response 和有界 journal 端到端工作  | Stack 1                   |
| 3     | `mvp/cloudflare-relay`        | Host 和浏览器通过认证的双 WebSocket 与 Hibernation DO 交互；单 writer lease、fencing 和慢客户端隔离可测试 | Stack 2                   |
| 4     | `mvp/wterm-snapshot-recovery` | 固定 engine 的 snapshot encode、passive restore、continuation + tail、显式 ownership 和原子 core adoption | Stack 3；wterm fork stack |
| 5     | `mvp/reconnect-checkpoint`    | warm tail-only reconnect、R2 snapshot publish、cold attach、journal gap 和 delivery generation reset      | Stack 4                   |
| 6     | `mvp/operational-gate`        | staging E2E 覆盖 Vim/tmux/htop、DO hibernation、output spew、故障注入和资源预算                           | Stack 5                   |

## wterm Fork Stack

| Stack | 分支                       | 可验证交付                                                                                                | 依赖        |
| ----- | -------------------------- | --------------------------------------------------------------------------------------------------------- | ----------- |
| W1    | `mvp/core-lifecycle`       | `TerminalCore.dispose()`、集中 effects policy、幂等销毁与 ownership 测试                                  | fork `main` |
| W2    | `mvp/ghostty-snapshot`     | 固定 Ghostty SHA、WASM snapshot encode/decode、`GhosttyRuntime`、passive restore 和 continuation fixtures | W1          |
| W3    | `mvp/atomic-core-adoption` | `takeCore()` 和 `WTerm.adoptCore()`，真实浏览器无空白帧、无重复 effects                                   | W2          |

## 完成门槛

- 普通交互走 raw fast path，网络正常时不周期性传输全量状态。
- warm reconnect 不需要 snapshot；cold attach 恢复 snapshot + 完整有序 tail。
- 半截 CSI 和 UTF-8 snapshot 与不切流 baseline 的终端状态相同。
- 浏览器 replica 不产生任何 PTY response；Host 对 TUI query 只响应一次。
- `yes` 等持续输出不会导致无界内存或 snapshot storm，control 仍可传递 Ctrl-C。
- PTY、journal、发送队列、snapshot 构建和 passive restore 均有明确上限与指标。
- Host 与浏览器 `engineId` 不同会 fail closed 并请求兼容客户端，不尝试猜测解码。
- restore、abort、adopt 和 destroy 的每条路径都恰好释放一次 WASM state。
