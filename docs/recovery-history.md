# Recovery 实现沿革

本文只记录已经退役的设计，不能作为当前实现规范、兼容承诺或部署指南。当前规范见
[Recovery 协议](recovery-protocol.md)和[终端协议架构](terminal-protocol-architecture.md)。

## 固定提交恢复

最早完成的远程恢复实现曾在代码与文档中称为 **Version 2**。它在恢复期间暂停 Host 的全局
canonical 发布，把某个 commit 固定为交付边界，再通过 barrier、定向 replay 和 snapshot pin 把
Browser 追到该边界。该实现解决了早期 MVP 的顺序问题，但有几个结构性限制：

- 一个 Browser 的恢复会影响整个 session 的 live 发布；
- snapshot、tail 和 live 依赖暂停与固定提交拼接；
- hibernated socket attachment 保存了大量投递游标与恢复状态；
- 数据通道替换、barrier outcome 和 replay reset 形成了第二套状态机。

这套实现及其 wire frame、capability、scheduler、attachment 字段和测试已经从产品代码中删除。

## 并发 gap-fill 候选

Phase 2 期间，并发恢复实现曾称为 **Version 3**。它引入 generation-scoped recovery、两条 delivery
lane、Host retained source、Cloud durable scalar ledger、Browser assembler、atomic adopt、live handoff、
byte-credit 与公平调度。它通过了 Host、Durable Object、Browser、committed Ghostty WASM 和 WTerm
adoption 的本地 correctness gates。

在 MVP 发布前，项目决定不再把它当作可选的第三版协议，也不再保留旧实现作为 fallback。它的状态机
和故障语义被提升为唯一 Recovery 实现；所有代码、类型、文件、配置和运行时字段因此改用无版本名称。

## 破坏性切换原则

本次切换发生在 MVP 发布前，因此没有向后兼容承诺：

- 不协商或选择恢复策略；
- 不接受旧 control/data frame；
- 不读取旧 socket attachment；
- 不续接旧 ticket、writer lease、delivery generation 或 recovery attempt；
- 发现旧 Durable Object schema 时直接重建当前 schema；
- 不提供原地降级或混合运行模式。

HTTP 路由、对象键、认证 token 或引擎标识中独立存在的格式编号不属于上述 Recovery 实现名称；它们由
各自的 API/存储契约管理。
