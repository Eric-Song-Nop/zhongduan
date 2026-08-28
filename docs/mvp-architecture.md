# MVP 架构

## 目标

MVP 交付一个 Linux Host 上持续运行、可从浏览器附加和重连的远程终端。它必须正确运行 Vim、tmux、htop 等 TUI，并在网络抖动、浏览器刷新、Cloudflare Durable Object 休眠和持续高速输出下保持有界资源占用。

## 权威模型

```text
Host daemon
  PTY + child process
  authoritative libghostty core
  ordered journal
  reusable Ghostty snapshot
          |
          | ordered PTY/resize mutations
          v
Cloudflare Worker + TerminalSessionDO + R2
          |
          v
Browser wterm
  passive libghostty replica
```

Host 是唯一终端设备权威。它按同一 actor 顺序执行 PTY 输出、resize、终端 query response 和语义输入编码。浏览器 core 永久采用 `effects: discard`，不会回答 DA/DSR、写入 PTY 或重放 bell、clipboard 等瞬时副作用。

## 实时与恢复

健康连接直接转发有序的原始 PTY bytes。`PTY_OUTPUT` 和 `RESIZE_APPLIED` 共用一个二进制数据 WebSocket 和同一个 `eventSeq`，因此 resize 的 reflow 位置不会丢失。

恢复有两条路径：

1. 页面仍保留相同 engine 的 live core，且 Host journal 覆盖缺口时，只重放 tail。
2. 页面刷新、journal gap 或慢客户端重置时，恢复 Host 的 Ghostty snapshot，再严格应用 snapshot cut 之后的 tail。

Ghostty snapshot 只承担 checkpoint，不承担事件传输。它的 continuation 恢复 snapshot cut 之前未完成的 VT/UTF-8 parser 状态；tail 从 `nextPtyOffset` 开始，不能重复 continuation bytes。

## 进程边界

### Host daemon

- Node.js 24 + TypeScript，由 Vite+ 构建和测试。
- `node-pty` 持有真实 PTY 和 child process。
- 复用 wterm fork 的 Ghostty WASM 作为权威 core，使 Host 和浏览器天然共享相同 codec。
- relay-agent 是 daemon 内的可重启连接组件；网络断开不终止 PTY。
- journal 只保留 warm replay 和 snapshot tail，不因客户端 ACK 无限延长。

### Cloudflare

- 一个 Cloudflare Vite 应用同时包含 React SPA、Worker 和 SQLite-backed `TerminalSessionDO`。
- 两条 Hibernation WebSocket：control 负责 attach、lease、input、ACK 和 resync；data 只负责有序终端 mutation 与 journal tail。
- DO 只做连接协调、fencing 和短暂转发，不保存高吞吐 journal。
- R2 保存不可变压缩 snapshot；blob 成功写入后才发布 DO metadata。浏览器通过受权 HTTP `ReadableStream` 下载，以获得背压、取消和零 DO 缓冲。

### wterm fork

- Ghostty SHA、snapshot schema、Zig toolchain 和 patchset 共同形成精确 `engineId`。
- 增加 snapshot encode、detached passive restore、显式 `takeCore()` ownership transfer、`dispose()` 和原子 `adoptCore()`。
- READY 前同步恢复当前画面；history 每页解码一次并在 JS event loop 间 yield。
- MVP 在 adopt 前选择 FINISH，或在 READY 后明确放弃剩余 history；不跨 owner 保留 decoder。

## 有界策略

| 项目                     |        MVP 默认值 |
| ------------------------ | ----------------: |
| Journal segment          | 256 KiB 或 250 ms |
| Warm replay              |      60 s / 8 MiB |
| Snapshot 理想 tail       |             2 MiB |
| Snapshot 最大可服务 tail |             8 MiB |
| 每客户端发送队列         |           512 KiB |
| Passive restore tail     |             2 MiB |
| Passive restore deadline |               5 s |
| Snapshot idle debounce   |            100 ms |
| Snapshot 最小构建间隔    |               2 s |

客户端 ACK 只决定能否 warm replay，不会 pin journal。发送队列超限时只重置该客户端的 data WebSocket 并增加其 `deliveryGeneration`；control WebSocket 保持可用，因此 Ctrl-C 和重同步控制不会被历史输出阻塞。

## MVP 安全边界

- capability token 明确区分 host、writer 和 viewer；token 绑定 session、角色和过期时间。
- writer lease 带 fencing token；同一 session 同时只有一个控制者可以输入和 resize。
- 所有网络帧在分配前检查长度；snapshot 同时限制压缩后和解压后长度。
- Cloudflare 在 MVP 中是可信 relay，传输依赖 TLS；端到端加密不在本阶段。
- `engineId` 必须精确相等。Ghostty snapshot envelope 的 `version=1` 不构成兼容保证。

## 非目标

- CRDT、多 writer 合并和共享输入草稿。
- 录制回放、无限 scrollback、Kitty graphics 恢复。
- 跨 `engineId` snapshot 迁移。
- daemon 崩溃后恢复 shell 进程。
- 端到端加密和零知识 relay。
