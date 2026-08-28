# Zhongduan

Zhongduan 是一个自托管的浏览器远程终端 MVP。Linux Host 上的 daemon 持有真实
PTY 和唯一的 Ghostty 终端权威状态，Cloudflare Worker 负责鉴权、连接协调与快照存储，
浏览器通过 wterm 渲染并控制终端。

```text
Linux Host                         Cloudflare                         Browser
PTY + Host daemon  <->  Worker + SQLite Durable Object + R2  <->  wterm replica
```

它面向需要在网络中断、页面刷新和 Durable Object 休眠后继续使用 Vim、tmux、htop
等 TUI 的场景。正常连接传输有序 PTY mutation；恢复时优先重放短 journal，必要时恢复
Ghostty snapshot 再接续 tail。

## 当前状态

这是可部署、可验证的 MVP，不是已经打包发布的通用远程运维产品。

- 支持一个 Linux Host、最多一个活动 writer 和多个 observer。
- writer 可以输入和调整尺寸；observer 只能查看。
- Host 与浏览器使用同一个固定版本的 Ghostty WASM 和严格 `engineId`。
- Host 网络 relay 可以重连；Cloud 网络故障不会主动终止仍在运行的 PTY。
- Cloudflare 是可信 relay，传输依赖 HTTPS/WSS；当前没有端到端加密。
- Host daemon 退出仍会结束它持有的 PTY，尚不支持 daemon 崩溃后的进程恢复。

## 组成

| 组件                        | 职责                                                                            |
| --------------------------- | ------------------------------------------------------------------------------- |
| `@zhongduan/host-daemon`    | 持有 PTY、Ghostty authority、journal、snapshot publisher 和 Cloud relay         |
| `@zhongduan/terminal-cloud` | React SPA、Worker API、双 WebSocket relay、SQLite Durable Object 和 R2 snapshot |
| `@zhongduan/protocol`       | 严格的 control schema、二进制 data frame 和 Cloud API contract                  |
| `@zhongduan/session-client` | 浏览器 attach、warm/cold recovery 和 replica commit 协调                        |
| `vendor/wterm`              | 固定 fork，提供 Ghostty runtime、snapshot restore 和 DOM renderer               |

## 环境要求

- Linux Host
- Node.js `>=24.11.0`
- pnpm `10.33.2`
- Git submodule
- 部署 Cloud 时需要 Cloudflare Workers、SQLite Durable Objects 和 R2

Ghostty WASM 已提交在固定的 wterm submodule 中。普通构建不需要安装 Zig；只有重新构建
Ghostty WASM 的维护者需要对应 Zig toolchain。

## 获取与验证

```bash
git clone --recurse-submodules "https://github.com/Eric-Song-Nop/zhongduan.git"
cd "zhongduan"
corepack enable
pnpm install --frozen-lockfile
pnpm exec vp run verify
pnpm exec vp run verify-clean-host
pnpm exec vp run verify-clean-browser
```

构建 Cloud SPA/Worker 和 Host CLI：

```bash
pnpm exec vp run build-browser
pnpm exec vp run build
```

Host CLI 产物位于 `apps/host-daemon/dist/cli.mjs`。它仍依赖工作区安装的原生
`node-pty`，当前不能把 `dist` 单独复制到另一台机器运行。

## 部署概览

1. 用 Wrangler 登录 Cloudflare，并创建配置中声明的 R2 bucket。
2. 生成彼此独立的 `BOOTSTRAP_TOKEN` 和 `CAPABILITY_SIGNING_KEY`。
3. 构建 Cloud 应用，再用 Wrangler 部署生成的 Worker 配置和静态资源。
4. 在 Linux Host 构建并启动 `zhongduan-host cloud`。
5. 从 Host 写出的 session JSON 组装 writer 或 observer capability 链接。

完整命令、首次部署、升级和本地联调见[部署指南](docs/deployment.md)。

## 访问与安全

Zhongduan 不使用传统登录密码。浏览器链接携带有角色和有效期的 bearer capability：

```text
https://<worker-origin>/sessions/<session-id>#capability=<writer-or-observer-capability>
```

URL fragment 不会发送给 HTTP 服务器，页面加载后也会立即从地址栏移除；但完整链接仍然
等同于临时凭据。不要把它写入公开日志、聊天记录或版本库。Host 生成的 session JSON
同时包含 writer 和 observer capability，也必须按 secret 文件保护。

`BOOTSTRAP_TOKEN` 是整个部署创建 session 和回收 Host authority 的高权限凭据，不是浏览器
密码。它只应通过权限受限的文件或 secret manager 提供，不能放进命令行参数。

## 文档

- [部署指南](docs/deployment.md)
- [MVP 架构](docs/mvp-architecture.md)
- [高性能远程终端 Snapshot 恢复实施计划](docs/high-performance-snapshot-recovery-plan.md)
- [输入稳定性与高 RTT 交互实施计划](docs/input-stability-plan.md)
- [Wire protocol](docs/wire-protocol.md)
- [设计记录与资源约束](resource.md)

## License

[Apache License 2.0](LICENSE)
