# Wire Protocol V2

> 本文只描述当前已实现、已经冻结的 v2 wire behavior。产品级不变量与允许的变更边界见
> [产品契约与协议边界](terminal-protocol-architecture.md)，cutover 顺序见 [MVP 路线](mvp-roadmap.md)。v2 的 global canonical
> pause、barrier 与 pinned commit 是一个整体 correctness contract，不能在 v2 内单独删除。

## 通道

每个参与者建立两条 WebSocket：

- `control`：UTF-8 JSON，承载 attach、writer lease、语义 input、ACK、host 状态和 resync。
- `data`：默认一条 WebSocket message 对应一个二进制 frame，承载 PTY output、resize、replay
  commit，以及 Host 到 relay 的 recovery barrier。协商 `host-data-batch-v1` 后，Host 到 Cloud 的
  message 可以是最多 64 个原始 v2 frame 的直接拼接；Browser 单独协商
  `browser-data-batch-v1` 后，Cloud 也可把同一 canonical batch 直接拼接为一个 Browser message。
- `snapshot`：受权 HTTP response 从私有 R2 对象直接流向浏览器，不经过 DO WebSocket。

所有重建 Host authority 所必需的 terminal mutation 必须在 data 通道内排序并消耗 `eventSeq`；当前只有
`PTY_OUTPUT` 与 `RESIZE_APPLIED`。input ACK、writer lease 和其他临时交互 metadata 不属于 terminal
mutation，不能推进 replica cursor。control 消息不能直接改变 replica。

`host-data-batch-v1` 不增加第二层 envelope，也不改变 logical frame identity、bytes 或顺序。单个 batch
最多 256 KiB；Host 同时只能发送一个未获 credit 的 batch。Cloud 完成该 batch 的验证、canonical commit
与 Browser delivery 后，在 Host data socket 上返回固定文本 `data-ack`，Host 才发送下一批。未协商的
peer 不发送拼接 batch，也不发送或等待该 ACK。

`browser-data-batch-v1` 同样不增加 envelope。它只用于 sole synced Browser 的 canonical live
delivery；directed recovery、mixed batch、多 Browser fan-out 或未协商的 Browser 仍保持一条 message
一个 v2 frame。Browser 必须按拼接顺序解析和应用全部 logical frame，ACK cursor 语义不变。

## Data Header

所有整数除 magic 外均为 little-endian。固定 header 为 48 bytes。

| Offset | Size | 字段                                           |
| -----: | ---: | ---------------------------------------------- |
|      0 |    4 | ASCII `ZTRM` magic，以 network order 读取      |
|      4 |    1 | protocol version，当前为 2                     |
|      5 |    1 | frame kind                                     |
|      6 |    2 | flags                                          |
|      8 |    8 | `sessionEpoch`                                 |
|     16 |    8 | `deliveryGeneration`                           |
|     24 |    8 | `eventSeq`                                     |
|     32 |    8 | PTY `startOffset` 或当前 `nextPtyOffset`       |
|     40 |    4 | relay 分配的 `streamId`，0 表示 live broadcast |
|     44 |    4 | payload length                                 |

Kinds：

```text
1 PTY_OUTPUT
2 RESIZE_APPLIED
3 REPLAY_COMMIT
4 RESET
5 DELIVERY_BARRIER
```

`DELIVERY_BARRIER` 由 Host 发给 relay，用于在目标 browser delivery 上固定 warm replay 或
snapshot recovery 的 commit watermark。relay 接受 barrier 后才发送 `replay-start` 或
`snapshot-manifest`，并在 `REPLAY_COMMIT` 到达前阻止该 delivery 越过固定 commit。

`PTY_OUTPUT` 与 `RESIZE_APPLIED` 的 `eventSeq` 必须严格加一。两者的 `ptyOffset` 都必须等于客户端当前 `nextPtyOffset`；只有 `PTY_OUTPUT` 会按 payload 长度推进 offset。这样 resize 在 PTY byte stream 中的位置可被确定且验证。

## Epoch 与 Generation

`sessionEpoch` 标识同一个 PTY/child process 生命周期。daemon 无法继续原进程时必须创建新 epoch。

`deliveryGeneration` 属于单个浏览器 data delivery。慢客户端、gap 或 restore 失败只增加该连接的 generation；旧 data WebSocket 上残留的 frame 会被拒绝，不影响其他客户端。

## 输入幂等

key、text、paste、resize request、focus 与 mouse 共享 `(inputEpoch, clientInputSeq)` 序列。`inputEpoch` 由浏览器 controller 在一个输入序列开始时生成；data-only delivery reconnect 不改变它，但 control WebSocket replacement 会把 outstanding 输入标为 uncertain 并创建新 epoch。收到明确 ACK 后才能丢弃对应输入。DO 不信任客户端声明的身份，转发给 Host 时附加 capability 已认证的稳定 `clientId` 和单调 `writerFence`。Host 以 `(writerFence, clientId, inputEpoch, clientInputSeq)` 去重；更高 fence 会永久封住旧 writer，ACK 丢失后的同一 fence 重试不会再次写入 PTY。换 controller 或明确放弃 uncertain 输入时必须创建新的 `inputEpoch`。

## Snapshot Cut

control 通道的 snapshot manifest 声明：

- snapshot 和精确 `engineId`；
- `cutSeq`，表示 snapshot 已包含所有 `eventSeq <= cutSeq` 的 mutation；
- `nextPtyOffset`，表示 snapshot 已逻辑消费 `[0, nextPtyOffset)`；
- compression、压缩前后长度、SHA-256 和受权下载路径。

浏览器流式下载到有上限的 `Uint8Array`，校验声明长度与 SHA-256 后才交给 decoder。MVP 不把异步网络暴露成 Ghostty reader；临时 0-byte read 会被 decoder 解释成永久 EOF。

snapshot continuation 是 parser 恢复材料，不是新的 PTY output。snapshot 后的第一条 PTY frame 必须从声明的 `nextPtyOffset` 开始。

## 限制

- 单 data payload 最大 16 MiB；Host 正常 PTY batch 目标为 16 KiB / 4 ms。
- 协商的 Host transport batch 最大 256 KiB / 64 logical frames，单次 in-flight 为 1；Host 等待 credit
  时最多保留 8 MiB / 8192 logical frames。
- paste 最大 1 MiB；Host 在写入前再次执行会话策略限制。
- control decoder 拒绝未知字段，所有 uint64 在 JSON 中用十进制字符串。
- 任意 magic、version、长度、epoch、generation、seq 或 offset 错误都会终止当前 delivery，并请求新 generation。
