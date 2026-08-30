# Wire protocol

Zhongduan 只有一套终端 wire protocol。恢复状态机见 [Recovery 协议](recovery-protocol.md)，更高层 owner
边界见[终端协议架构](terminal-protocol-architecture.md)。历史协议只记录在
[Recovery 实现沿革](recovery-history.md)，不是兼容契约。

## Control channel

Control channel 使用 strict JSON object。每个方向都有独立 schema，unknown type、unknown field、方向错误或
身份不完整都视为 protocol error：

- Browser → Cloud：attach、writer lease renew、semantic input、delivery receipt、replica apply progress、
  recovery adopted；
- Cloud → Browser：welcome、input acknowledgement、Host/status/resync、recovery start、source closed；
- Host → Cloud：host ready、input acknowledgement、source rejected、source closed；
- Cloud → Host：host ready acknowledgement、forwarded input、prepare、start-ready、grant、receipt、reset。

控制消息只能影响 socket attachment 与 SQL 都证明的 exact current owner。裸 recovery id、client id 或
connection id 不能单独授权状态变更。

## Canonical data frame

Host authority 输出固定长度 header 加 bounded payload。Header 包含 magic、format、kind、session epoch、
event sequence、PTY offset，以及仅 start-fence 使用的 routing identity。普通 canonical mutation 的 routing
字段必须为零。

当前 kind：

- `PtyOutput`：payload 是 exact PTY bytes；offset 按 payload 长度连续；
- `ResizeApplied`：payload 是 strict rows/columns；
- `RecoveryDone`：只允许 recovery lane，payload 为空；
- `RecoveryStartFence`：只允许 Host ordered stream，payload 是 strict fence identity。

Flags 必须为零。未知 format、kind、flags、长度或 cursor gap 都 fail closed。

## Delivery envelope

Cloud→Browser data 消息始终是 delivery envelope，而不是裸 canonical frame。Envelope header 包含 magic、
format、lane、delivery generation、ordinal、cumulative encoded bytes、stream 和 payload length。Payload 是
完整、未重写的 canonical frame。

每条 lane 独立连续；首条 ordinal 为 1，首条累计字节等于该 envelope 的完整 wire bytes。Receipt 必须命中
Cloud durable sent ledger 中的 exact prefix。

## Recovery start fence

Start fence 与 canonical data 共用 Host data socket和同一个 per-socket FIFO。它必须出现在 canonical `H`
之后、`H+1` 之前。Cloud 只有在 durable authority head 恰好为 `H` 时才能安装 start；事务提交后才允许
处理后续 canonical frame。

## Limits

Frame、control JSON、socket message、queue、outbox、source、lane、generation 和 snapshot metadata 都有独立
硬上限。编码器和解码器必须执行相同的长度与 scalar 范围检查；任何截断、额外字节或非 canonical decimal
都不得进入 owner 状态机。
