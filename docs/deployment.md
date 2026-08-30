# 部署指南

本文描述当前 MVP 的最小自托管部署：Cloudflare 承载 SPA、Worker、SQLite Durable
Object 和 R2；一台 Linux Host 从源码工作区运行 daemon 并创建远程终端 session。

## 1. 前置条件

- Linux Host，安装 Git、OpenSSL 和 Node.js `>=24.11.0`
- Corepack 与 pnpm `10.33.2`
- Cloudflare 账号，能够使用 Workers、SQLite Durable Objects 和 R2
- 已安装依赖的源码工作区；Host 产物当前不是独立发行包

首次检出：

```bash
git clone --recurse-submodules "https://github.com/Eric-Song-Nop/zhongduan.git"
cd "zhongduan"
corepack enable
pnpm install --frozen-lockfile
```

如果源码已经存在，仍要确认固定 submodule 完整：

```bash
git submodule update --init --recursive
node "scripts/verify-wterm-submodule.mjs"
```

## 2. 准备 Cloudflare

登录并检查当前账号：

```bash
pnpm --filter "@zhongduan/terminal-cloud" exec wrangler login
pnpm --filter "@zhongduan/terminal-cloud" exec wrangler whoami
```

首次部署创建生产 R2 bucket：

```bash
pnpm --filter "@zhongduan/terminal-cloud" exec wrangler r2 bucket create "zhongduan-terminal-snapshots"
```

只有使用远程 preview binding 时才需要 preview bucket：

```bash
pnpm --filter "@zhongduan/terminal-cloud" exec wrangler r2 bucket create "zhongduan-terminal-snapshots-preview"
```

不需要创建 D1，也不需要手工执行 SQL migration。首次部署会根据 Worker 配置声明创建
SQLite-backed `TerminalSessionDO` namespace；每个对象在首次启动时安装当前内部 schema。

## 3. 创建 Secrets

创建部署外的私有目录和两个独立的高熵值：

```bash
install -d -m "700" "$HOME/.config/zhongduan"
openssl rand -hex "32" > "$HOME/.config/zhongduan/bootstrap-token"
openssl rand -hex "32" > "$HOME/.config/zhongduan/capability-signing-key"
chmod "600" "$HOME/.config/zhongduan/bootstrap-token"
chmod "600" "$HOME/.config/zhongduan/capability-signing-key"
```

Worker 需要：

| Secret                   | 用途                                           |
| ------------------------ | ---------------------------------------------- |
| `BOOTSTRAP_TOKEN`        | 创建 session、Host capability 失效后的身份回收 |
| `CAPABILITY_SIGNING_KEY` | 签发和验证 host、writer、observer capability   |

两个值必须不同，且都至少 32 bytes。不要把它们写进仓库、命令行参数或公开日志。

首次部署可以使用仓库外、权限为 `0600` 的 `.env` 或 JSON 文件，通过 Wrangler
`--secrets-file` 和代码一起上传。文件示例：

```dotenv
BOOTSTRAP_TOKEN=<bootstrap-token 文件的完整内容>
CAPABILITY_SIGNING_KEY=<capability-signing-key 文件的完整内容>
```

例如保存为 `$HOME/.config/zhongduan/cloud.env` 后执行：

```bash
chmod "600" "$HOME/.config/zhongduan/cloud.env"
```

也可以在 Worker 已存在后使用 `wrangler secret put` 交互式配置。不要把 secret 值作为
命令行参数传入。

## 4. 构建和部署 Cloud

先运行 Vite build，再运行 Wrangler deploy：

```bash
pnpm exec vp run build-browser
pnpm --filter "@zhongduan/terminal-cloud" exec wrangler deploy --secrets-file "$HOME/.config/zhongduan/cloud.env"
```

Vite build 会生成供 Wrangler 使用的重定向配置，同时打包 Worker、SPA 和 Ghostty WASM。
Wrangler 输出的 HTTPS origin 是下一步的 `CLOUD_URL`，通常形如：

```text
https://zhongduan-terminal-cloud.<account-subdomain>.workers.dev
```

后续部署不需要重新创建 bucket。Cloudflare 会保留已配置的 secrets。MVP 前的构建不提供跨提交互通；
停止旧 Host 后，从同一个提交验证、构建并部署 Cloud 与 Host：

```bash
git pull --ff-only
git submodule update --init --recursive
pnpm install --frozen-lockfile
pnpm exec vp run verify
pnpm exec vp run build-browser
pnpm --filter "@zhongduan/terminal-cloud" exec wrangler deploy
```

当前代码只有一套 Recovery wire和runtime，不执行协议协商、策略选择、降级或混合generation。
当前 correctness gates 已覆盖 Browser assembler、Host source owner、Cloud durable state、receipt/closure/
fairness，以及本地 three-owner continuity、committed-WASM Ghostty continuation与WTerm adoption；这些仍只是
本地证据，不代表production Cloudflare/R2、真实跨进程网络、性能或上线批准。

本次切换发生在MVP发布前，不提供旧开发态协议或Durable Object schema的向后兼容。对象发现非当前
schema marker时会重建内部SQL/KV；旧socket、ticket、lease和generation不可续接。部署该变更前应接受
开发环境session数据失效。R2中的开发态孤儿对象可以删除bucket后重建，或按运维策略清理。

应用级回滚必须回退整个Host、Cloud和Browser构建，并重建开发态session；不能让不同提交的endpoint
继续共享现有连接。正式上线后的发布契约属于MVP剩余工作，不能通过恢复已删除的实现来规避。

部署前可用以下命令只验证生成的 Worker、Assets、DO 和 R2 binding，不上传：

```bash
pnpm --filter "@zhongduan/terminal-cloud" exec wrangler deploy --dry-run
```

## 5. 构建和启动 Host

在实际持有 shell 的 Linux Host 上运行：

```bash
pnpm exec vp run build
```

准备一个不存在的 session 输出路径。CLI 会以 `0600` 和排他创建方式写入该文件，拒绝
覆盖已有文件：

```bash
install -d -m "700" "$HOME/.local/state/zhongduan"
```

启动一个登录 shell：

```bash
node "apps/host-daemon/dist/cli.mjs" cloud \
  --url "https://<worker-origin>" \
  --bootstrap-token-file "$HOME/.config/zhongduan/bootstrap-token" \
  --session-info-file "$HOME/.local/state/zhongduan/session.json" \
  -- "/bin/bash" "-l"
```

该进程必须保持运行。生产 Cloud URL 必须使用 HTTPS；只有 `localhost`、`127.0.0.1` 和
`::1` 允许 HTTP。网络或 Cloud 凭据临时失效时 relay 会退避重连，仍在运行的 PTY 不会
因此被主动终止。

也可以通过以下环境变量提供配置：

- `ZHONGDUAN_CLOUD_URL`
- `ZHONGDUAN_BOOTSTRAP_TOKEN_FILE`
- `ZHONGDUAN_SESSION_INFO_FILE`

虽然也支持 `ZHONGDUAN_BOOTSTRAP_TOKEN`，生产环境优先使用 token file 或 secret manager。

## 6. 打开浏览器终端

session JSON 包含 `sessionId`、`writerCapability` 和 `observerCapability`。按角色组装链接：

```text
Writer:   <CLOUD_URL>/sessions/<sessionId>#capability=<writerCapability>
Observer: <CLOUD_URL>/sessions/<sessionId>#capability=<observerCapability>
```

Writer 可以输入和 resize；observer 只能查看。浏览器首次读取 capability 后会立即清除 URL
fragment，并把凭据放进当前站点的 `sessionStorage`。完整链接和 session JSON 仍然是 bearer
凭据，分享时必须按 secret 处理。

浏览器 capability 初始有效期为 8 小时，在线客户端会在半寿命附近自动刷新。Host
capability 初始有效期为 24 小时，并由 daemon 自动刷新或使用 bootstrap token 回收。

## 7. 本地联调

本地 Vite/workerd 可以模拟 Durable Object 和 R2。创建被 Git 忽略的
`apps/terminal-cloud/.dev.vars`：

```dotenv
BOOTSTRAP_TOKEN=local-bootstrap-token-with-at-least-32-bytes
CAPABILITY_SIGNING_KEY=local-capability-key-with-at-least-32-bytes
```

启动 Cloud 应用：

```bash
pnpm --filter "@zhongduan/terminal-cloud" run dev -- --host "127.0.0.1" --port "5173"
```

另一个终端构建 Host，并将相同 bootstrap token 放入权限为 `0600` 的文件，然后使用
`http://127.0.0.1:5173` 作为 `--url` 启动 Host。

Recovery 的开发验收范围、状态机和证据边界见
[Recovery 协议](recovery-protocol.md)与[高性能 Snapshot 与 Recovery 计划](high-performance-snapshot-recovery-plan.md)。
本地完整回归使用：

```bash
pnpm exec vp run verify
```

测试数量本身不是正确性证据。本地部署步骤和这组测试也不提供真实跨进程网络、性能或生产行为证明。

## 8. 运维边界

- Cloudflare 是可信 relay，当前没有端到端加密。
- capability 和 bootstrap token 都是 bearer secret；泄露后应立即轮换相应 secret。
- `CAPABILITY_SIGNING_KEY` 轮换会使所有现有 capability 失效。
- `BOOTSTRAP_TOKEN` 轮换后，需要同步更新 Host 使用的 token file。
- Host daemon 退出会结束它持有的 PTY；当前没有 daemon crash recovery。
- 当前仓库不提供 systemd unit、容器镜像或独立 npm/binary 发行包。
