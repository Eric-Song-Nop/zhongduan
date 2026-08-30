# MVP Roadmap

## 已完成的本地 correctness 基础

- Host authority、journal、snapshot capture和 immutable publish；
- 唯一 Recovery wire/control/runtime；
- Host retained source、multi-outstanding receipt、byte-DRR与deadline隔离；
- Cloud no-payload durable ledger、shared ring、writer reserve与weighted delivery scheduling；
- Browser assembler、snapshot restore、atomic adopt与long-lived live receiver；
- writer lease、input epoch、semantic input和exact Host ACK；
- Durable Object hibernation、outbox replay、generation/Host fence与fault gates；
- committed Ghostty WASM continuation和 WTermReplicaHost adoption gates；
- pre-MVP旧恢复代码、策略、兼容schema和命名清理。

## MVP 前剩余工作

1. 在 production-like Cloudflare环境验证真实 WebSocket/R2/hibernate/failure序列；
2. 加入部署观测、容量告警与可操作的session诊断；
3. 运行真实 PTY/Ghostty/WTerm端到端continuity、长会话和多client soak；
4. 冻结MVP的resource limits与用户可见重连行为；
5. 完成安全审查、部署演练与显式上线批准。

## 后置方向

- rolling/delta snapshot与history pages；
- presentation/history后台构建；
- 更细粒度的client优先级与动态credit调参；
- 性能benchmark、dashboard和SLO；
- shell integration、completion/context与更丰富的terminal sideband。

历史实现和为什么在MVP前删除，见 [Recovery 实现沿革](recovery-history.md)。
