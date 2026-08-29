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
SQLite-backed `TerminalSessionDO` namespace；每个对象的内部 schema 在实例启动时迁移。

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

后续部署不需要重新创建 bucket。Cloudflare 会保留已配置的 secrets，因此常规升级只需：

```bash
git pull --ff-only
git submodule update --init --recursive
pnpm install --frozen-lockfile
pnpm exec vp run verify
pnpm exec vp run build-browser
pnpm --filter "@zhongduan/terminal-cloud" exec wrangler deploy
```

部署前可用以下命令只验证生成的 Worker、Assets、DO 和 R2 binding，不上传：

```bash
pnpm --filter "@zhongduan/terminal-cloud" exec wrangler deploy --dry-run
```

### Cloud 诊断与日志隐私

生产 Worker 只有在 `CLOUD_TELEMETRY_MODE` 精确为 `workers-logs-v2` 时才把自定义诊断写入
Workers Logs；缺失、`off` 或未知值都关闭该出口，可作为无需更改协议或存储的 kill switch。仓库生产配置显式启用
该版本，测试环境显式关闭；修改 mode 后需重新部署 Worker，DO 重启时允许丢弃尚未 drain 的 best-effort 诊断。

`workers-logs-v2` 是事实 schema 的不兼容版本边界，不是 terminal wire 版本。生产查询和 dashboard 在发布与回滚
窗口内必须同时接受 `schemaVersion=1` 和 `schemaVersion=2`，并按版本解释字段；确认 v2 持续到达后才能移除 v1
查询。仓库的 `CloudTelemetryWriteEventSchema` 只允许 v2 producer；`CloudTelemetryReadEventSchema` 则保留四种
Phase 0b v1 shape 并同时接受 v2，供兼容窗口的 ingestion 使用。`CloudTelemetryReadLogRecordSchema` 严格核验
flattened `type=zhongduan.telemetry`、`runtime=cloud-do` wrapper、event payload 与版本化 producer
`sampleWeight`；`CloudflareCloudTelemetryLogEventSchema` 只从 Workers Logs envelope 投影 `event.source`，不会把
平台 metadata 带入应用查询结果。新 Phase 0c event name 不允许伪装成 v1，改变 producer sampling policy 也必须
升级 event schema version。当前
Worker 只识别与自身代码配套的精确 mode，若代码与 binding 分步发布或回滚成不匹配组合，自定义诊断会
静默关闭并形成 telemetry blind window。代码和 binding 应作为同一版本发布，同时对事件缺口告警；该窗口不改变
terminal relay、authority 或恢复行为，也不能靠重放补回。

生产配置显式打开 custom-log persistence、把 Workers Logs head sampling 固定为 1，同时关闭 Cloudflare 自动
invocation logs 和 automatic tracing。公开 WebSocket 握手当前把一次性 ticket
放在 query string，session ID 也出现在 API path；自动 request log/trace 会把这些 URL 元数据带出 Zhongduan
自定义诊断 schema。不要在 Dashboard 中重新开启它们，也不要在升级 compatibility date 时删除仓库中的显式关闭项。

Cloud 自定义诊断 payload 只允许封闭枚举、分桶后的大小、计数和本机 monotonic duration。禁止写入 terminal、
input 或 frame payload，text/paste/command/cell/key，ticket/capability/writer lease，session/client/connection ID，
stream、generation、input epoch/sequence，journal/snapshot ID 或 cursor，URL，以及 error/exception string；
input/control 大小也只能分桶，上述敏感值的 hash/digest 同样禁止。这个保证只约束 Zhongduan 提交给日志系统的
payload；Cloudflare 仍可能在平台 envelope 中附加 invocation/request/Durable Object 等平台标识符。因此
Workers Logs 仍是敏感运行元数据，访问、保留与导出权限必须按生产日志管理，不能把它当作匿名数据。

本层的 Cloud 自定义事件覆盖 relay queue、Browser input 中现有 lease verify/renew 与 Host send decision、
recovery barrier、attach/delivery-reset transition，以及以下 Phase 0c 本地事实：

- `cloud.input.ack-forward` 从 Host ACK 进入当前 relay queue 起，记录 ACK status 和 Browser control target missing、
  `send()` returned 或 send failed 的终点；它不证明 Browser 收到、处理或绘制结果。
- `cloud.data.fanout` 由 `TerminalSessionDO` 的 canonical/directed data owner 为每个 Host data frame 产生一条聚合
  事实，记录 frame kind、选中目标数、send-return/stale/sequence-error 目标数、credit 超限或 send 不确定所触发的
  reset 目标数，以及最大 credit utilization bucket。它不是逐 Browser event；reset 字段只表示本地决策/发起，
  不证明 reset 完成，既有 `cloud.recovery.transition` reset 事实仍负责记录 reset issuance 与 Host notify decision。
- `cloud.writer.lease` 分开记录 writer attach acquire 与 heartbeat verify-renew，并区分 current、stale、inactive/
  unavailable 和 uncertain outcome。它只是既有 lease 路径的观察，不改变 lease ownership 或持久化。

后续平台 envelope hardening 还应在部署工具支持并完成真实日志验证后启用 query-string redaction；它不替代当前
对 invocation logs/tracing 的显式关闭，也不扩大本层自定义 payload 的允许字段。

Queue duration 从本地 queue admission 起算；input、Host ACK、Host data、attach 和 barrier duration 从 Durable Object
JavaScript message callback admission 起算；reset duration 从 `resetBrowserDelivery()` 操作入口起算，因为它也可由
socket close 等非 message callback 路径触发。各字段终点是对应的本地 decision/handling 或
`WebSocket.send()` 返回。Workers runtime 的 clock 只在 I/O 边界推进，因此这些字段即使位于同一 runtime 也只是
Cloud local lower-bound：同步 CPU 工作可能不可见，也不包括各自起点前的 edge/唤醒/调度；`send()` 返回不证明
帧已离开 socket queue 或被 Browser/Host 收到。不要把不同机器的时间戳相减，也不要把这些事件标成网络 RTT、
端到端 latency 或 CPU time。

事件是有界、best-effort 的；高频事件采用每个 DO/key 随机起始相位的系统采样，聚合估算必须使用
`sampleWeight`，原始 event count 不是完整流量计数。ACK 转发与常见 heartbeat lease outcome 会采样；canonical
frame 在进入 Browser fanout loop 前按 weight 64 固定选择，未选中的 frame 不安装 per-Browser observer，选中的正常
或异常 outcome 使用同一权重。Directed 或其他 weight 1 事实仍可能因有界 producer、collector/runtime failure 而
丢失。安全随机相位无法取得时直接丢弃该采样序列，不使用有偏的固定首样本。每个 DO instance 最多暂存 64 条
Cloud 事件；pending event、drop 状态和 sampling phase/counter 都只驻内存，不写 SQLite 或 WebSocket attachment，
可在队列满、版本切换、eviction/hibernation 或 runtime failure 时丢失或重置，也不 backfill/replay。

`observability/cloud-telemetry-query` 在这些事实之上只增加 source-controlled 查询契约与本地工具：所有查询都固定
过滤 Cloudflare service/log type 和 Zhongduan wrapper，并在执行前通过 keys API 核验 exact direct key/type；流量
估算使用 `SUM(sampleWeight)`，raw row count 只表示被保存/查询的行数。任何 percentile 查询都必须额外固定一个
producer `sampleWeight`，不得把不同采样率的成功/失败路径混在同一个 p95/p99。若查询结果的
`sampleInterval != 1` 或 `abr_level != 1`，报告必须标为 approximate，不能据此通过 rollout/SLO hard gate，也不能
盲目再次乘 `sampleInterval`。

本地 `validate` 不需要凭据；只读意图的 smoke/report 使用独立的
`CLOUDFLARE_OBSERVABILITY_TOKEN` 与 `CLOUDFLARE_OBSERVABILITY_ACCOUNT_ID`，不得复用或输出 deploy token。
Cloudflare 当前 run-query API 仍要求 account-scoped Observability Write permission，因此 token 必须按写权限管理。
Saved-query provisioning 只有显式 `--apply` 才能执行：缺失项可创建、同名同内容 no-op、同名漂移 fail closed，
不得自动覆盖或删除 account-level state。上线前必须在隔离 staging 窗口验证真实 Workers Logs source shape、exact
keys/types、v1/v2 strict parse 与非 approximate 聚合；仓库中的 synthetic/sanitized fixture 不能替代这个 gate。

本地契约检查不会读取凭据或访问网络：

```bash
node scripts/cloud-telemetry-observability.mjs validate
node scripts/cloud-telemetry-observability.mjs fixture-gate
```

当前仓库 fixture 明确标记为 synthetic，第二条命令会以非零状态要求 staging-captured sanitized replacement。取得
专用 observability token 后，可对指定毫秒或 ISO 时间窗运行 Cloud-local report 和 arrival smoke：

```bash
export CLOUDFLARE_OBSERVABILITY_ACCOUNT_ID="<account-id>"
export CLOUDFLARE_OBSERVABILITY_TOKEN="<dedicated-token>"
node scripts/cloud-telemetry-observability.mjs report --from "<from>" --to "<to>"
node scripts/cloud-telemetry-observability.mjs arrival-smoke --from "<from>" --to "<to>"
```

`arrival-smoke` 输出固定包含 `performanceGate: false`；exact key/type 缺失、无 v2 arrival、或平台采样使结果
approximate 时都不能算 rollout gate 通过。只有人工确认 manifest 与 account-level state 后才执行：

```bash
node scripts/cloud-telemetry-observability.mjs provision --apply
```

这一层只完成 Cloud-local query contract。`observability/browser-presentation-facts` 只在 Browser 本地补上
input lifecycle 与 canonical presentation opportunity 事实。本地 pre-live input/recovery-liveness smoke 另见
第 7 节；它不是 Phase 0 完成判据。诊断不得写入 terminal wire、journal、snapshot、replay、SQLite schema 或
WebSocket attachment。

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

可选设置 `ZHONGDUAN_TELEMETRY=stderr`，让 Host 输出一行一个 JSON 的 snapshot/recovery、control queue、
input apply 与 relay RTT 诊断事件。默认关闭；事件 schema 只允许本机 monotonic duration、分桶后的 input/control
大小、snapshot/journal bytes/frames 和封闭 outcome，不包含 terminal/input 内容、凭据、原始标识符或异常文本。
诊断先进入一个 Host 进程拥有的有界 deferred queue；队列满或 stderr backpressure 时丢弃诊断，不阻塞 input
或 recovery，也不无限累积输出。该 Host 出口尚不等于跨 Host/Cloud/Browser 的统一 dashboard。

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

Browser 诊断由 session URL 上的 `browserDiagnostics` 参数选择，它与 `Raw`/`Mirrored`/`Owned`
输入模式正交：

- 参数缺失、空值或精确为 `memory-v2` 时，开启本地内存诊断；
- 精确为 `off` 时关闭；任何未知值也 fail closed 到 `off`。

例如 Writer URL 可写为
`<CLOUD_URL>/sessions/<sessionId>?browserDiagnostics=off#capability=<writerCapability>`。`off` 路径不创建
diagnostics tracker/ring、telemetry-only clock、map、page-visibility listener、WTerm render-commit hook、
presentation timer 或 `requestAnimationFrame`；WebSocket heartbeat correctness 自身的 monotonic clock 与
deadline timer 仍保留。它不关闭 terminal，只是关闭这组观测。

`memory-v2` 维护最多 256 条的本机诊断 ring，并保留原有 control/data socket RTT、attach 终点和
snapshot/recovery 事实。新增两条只观测现有路径的有界 lifecycle：

- input 从 semantic dispatch 入口经 send decision 到 matching ACK；未发出、不确定、epoch/
  transport/session 终止等路径记录其本地 terminal reason；
- canonical `PTY_OUTPUT`/`RESIZE_APPLIED` 只选择完成 frame decode 后、data callback 进入时已经处于
  `live` 或 warm `replaying` 的帧，再经 visible active replica 的精确 cursor 推进、WTerm 同步 DOM render
  commit，到下一次 animation-frame callback opportunity。Snapshot `restoring` 期间的 detached/buffered
  work 不是这条 visible presentation 事实。

两个序列都在知道 outcome 前使用随机起始相位的固定系统采样，`sampleWeight=64`；安全随机相位
无法取得时丢弃该序列，不退回固定首样本。Tracker 合计最多保留 64 个 pending probe，每个
deadline 为 2 秒；正常与 terminal outcome 使用同一权重。Ring 只驻内存，不进入 console、
`sessionStorage`/`localStorage`、terminal snapshot 或网络；容量满时覆盖最旧事件并累计 drop
count。`TerminalSession.diagnostics` 返回按时间顺序复制的只读快照，主要供本机排障和测试使用。

这些 duration 使用 `performance.now()` 一类本机 monotonic clock；票据 `expiresAt` 仍只能与 wall clock
比较。`load-total` 包含 HTTP、完整性校验、解压和 worker transfer，不能称为纯下载；`adopt
call-returned` 不证明浏览器已经 paint；socket RTT 包含 Browser event loop/socket queue 与 Cloud
auto-response，也不是纯网络 RTT；input ACK 仍不证明 child/app effect。WTerm render commit 只证明
同步 DOM/scroll/title/response 工作已提交，下一帧 opportunity 只证明 Browser 调度了后续
animation-frame callback；两者都绝不是 pixel paint/composite 证明。事件 schema 严格且 content-free：
不包含 session/client/connection、epoch/seq/generation、snapshot ID/cursor、URL、输入/帧/cell 内容、key、异常文本
或这些值的 hash/digest，大小只允许 bucket。Browser 当前没有生产上传出口，跨节点聚合与
dashboard 属于后续显式 opt-in 的独立阶段。

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

本地 synthetic pre-live input/recovery-liveness smoke 使用真实 Vite/workerd、Host relay 和 Chromium，
但 Host child 是仓库内固定字节协议的 synthetic PTY。先在仓库外创建临时 venv，安装 source-controlled 的
Playwright 直接依赖版本与其 managed Chromium；不要直接安装未锁版本：

```bash
BROWSER_E2E_VENV="$(mktemp -d)/venv"
python3 -m venv "$BROWSER_E2E_VENV"
"$BROWSER_E2E_VENV/bin/python3" -m pip install -r scripts/requirements-browser-e2e.txt
"$BROWSER_E2E_VENV/bin/python3" -m playwright install chromium
PATH="$BROWSER_E2E_VENV/bin:$PATH" pnpm exec vp run verify-browser-recovery-smoke
```

脚本会自行创建并删除 `apps/terminal-cloud/.dev.vars`，因此运行前必须停止本地 dev server，并临时移走已有的
`.dev.vars`。不需要 Playwright 的 helper/contract 测试可单独运行，并且已进入普通根 `verify`：

```bash
pnpm exec vp run verify-browser-recovery-smoke-contract
```

该本地 smoke 只通过 `vite dev --mode browser-e2e` 选择一个拒绝非 loopback 请求的 alternate Worker/DO entry，
并把本次 DO/R2 状态放入运行级临时目录。该 entry 只在自己的 coordinator 实例上接受当前 pinned Miniflare
生成的 171 字符 opaque multipart part ETag；流式长度、SHA-256、最终对象大小、metadata 与 multipart
object ETag 约束仍保持。默认 `wrangler.jsonc` 的 `main` 继续指向 production `index.ts`，production R2
part ETag 仍必须精确匹配 MD5。禁止将 `browser-e2e` mode 用于远端、staging 或 production；Miniflare
恢复 production MD5 语义后应删除这层兼容 entry，而不是放宽 production matcher。

该 smoke 只验证以下固定本地链路：Browser 发起 cold snapshot GET 后，测试暂时 hold 该请求以固定 pre-live
窗口；Playwright 通过真实 Chromium 的键盘事件路径在 `attaching` 或 `restoring` phase 发出 `Control+KeyC`，synthetic
PTY 精确观察到一次 `0x03` effect。放行 snapshot GET 后 Browser
最终进入 `live`；ready/interrupt sentinel 唯一可见后，固定 probe 只执行一次、synthetic result 只进入 terminal
DOM 一次。最终 PTY capture 必须精确为 interrupt+probe，三个 sentinel 唯一且有序，Browser/Host/workerd 不得
提前退出。Bounded retry/resync/fallback 本身不使 smoke 失败，只要最终恢复为 `live` 且上述 effect 不重复。
成功输出只有固定 `browser-recovery-smoke-v1/passed`；失败
诊断只保留固定 stage/phase/status label 与 count，不输出 URL、session/capability/ticket、input identity、payload、
异常文本、process output、trace、video 或 screenshot。该结果只表示 pending-recovery 输入可达且系统最终恢复
活性，不提供 latency、throughput、rendering 或 SLO 证据。

边界必须按 smoke 解读：它只在单机 loopback 和带 local-only multipart ETag 兼容层的 Miniflare 上执行一个固定
场景的一次运行；snapshot GET hold 是测试控制点，不是产品时序。不提供 Browser↔Cloud 与 Cloud↔Host 两条
链路可独立控制的 RTT、jitter、loss，也不证明
p95、p99、99.9% 成功率、`<=5%` 插桩开销、throughput 或资源上限。固定 synthetic child/result 不是通用 shell/TUI
的 application effect，DOM result 也不是 pixel paint/composite。它不能替代 model/property/fuzz、故障注入、
多客户端、公平性/load/soak 或 production staging 验收，也不覆盖 IME、Unicode、CapsLock、mouse、paste 等输入
矩阵。local-only ETag 兼容层只让 Miniflare 能运行该 smoke，不验证生产 Cloudflare Durable Objects/R2；各 timeout
只是防挂死预算，不是产品 latency 或 recovery SLO。观察到 snapshot GET 不证明该 snapshot 被 restore/adopt；
因为 fallback 被允许，这个 smoke 只证明 recovery pending 时输入仍可达 synthetic child，且系统最终恢复活性。

后续若要扩大验收，需要分别设计而不是叠加到这个 smoke：状态模型/property/fuzz 覆盖 generation、retry、reset、
adopt；故障注入覆盖两条网络、断连和恢复分支；多客户端/writer transfer/output flood/load/soak 覆盖公平性与资源；
真实 staging 覆盖生产 DO/R2；另需 snapshot adoption/tail continuity 与 ACK identity/status/dedup 的专门测试。
性能验证需要另行设计受控且可复现的测试；当前暂不实现，也不把
单次 smoke 的 timeout 或运行时间当作性能证据。

## 8. 运维边界

- Cloudflare 是可信 relay，当前没有端到端加密。
- capability 和 bootstrap token 都是 bearer secret；泄露后应立即轮换相应 secret。
- `CAPABILITY_SIGNING_KEY` 轮换会使所有现有 capability 失效。
- `BOOTSTRAP_TOKEN` 轮换后，需要同步更新 Host 使用的 token file。
- Host daemon 退出会结束它持有的 PTY；当前没有 daemon crash recovery。
- 当前仓库不提供 systemd unit、容器镜像或独立 npm/binary 发行包。
