> 设计记录：本文不是协议规范、roadmap 或实施门槛，其中多轮讨论的结论可能彼此冲突或已经失效。
> 当前产品决策以[产品契约与协议边界](docs/terminal-protocol-architecture.md)为准，实施顺序以
> [MVP 路线](docs/mvp-roadmap.md)为准；当前行为与常量仍以代码、[MVP 架构](docs/mvp-architecture.md)和
> [Wire Protocol V2](docs/wire-protocol.md)为准。

可以。先把方案压缩成一个明确的复制模型：

```text
权威状态：
  Host 上的 PTY + libghostty Terminal

客户端副本：
  同版本 libghostty Terminal

稳定链路：
  发送原始 PTY bytes

恢复链路：
  发送 libghostty snapshot@R
  再发送 R 之后的全序事件
```

这里所谓“事件”不要设计成 `CellChanged`、`CursorMoved`，只保留真正会驱动 libghostty 状态机的输入：

```text
PTY_OUTPUT(bytes)
RESIZE(cols, rows, pixel_width, pixel_height)
少数会改变终端配置的 session 事件
```

当前 libghostty snapshot 已经是：

```text
TERMINAL
SCREEN + PAGE
CONTINUATION
READY
HISTORY + PAGE
FINISH
```

`READY` 之前包含当前可渲染状态和未完成的 VT/UTF-8 parser continuation；`READY` 后是较老 scrollback。解码到 `READY` 后，终端已经可以显示和继续接收新的 PTY 数据。

---

# 一、先定义外层协议

libghostty snapshot 是内层格式。网络还需要一层 session 协议。

```ts
type U64 = bigint;

interface Common {
  sessionEpoch: U64;
  deliveryGeneration: U64;
}

type ServerFrame =
  | (Common & {
      type: "welcome";
      engineId: string;
      headEventSeq: U64;
      nextPtyOffset: U64;
    })
  | (Common & {
      type: "replay-start";
      streamId: number;
      baseEventSeq: U64;
      basePtyOffset: U64;
      commitEventSeq: U64;
      commitPtyOffset: U64;
    })
  | (Common & {
      type: "snapshot-manifest";
      snapshotId: string;
      streamId: number;
      engineId: string;

      // Snapshot 包含所有 eventSeq <= cutEventSeq 的状态。
      cutEventSeq: U64;

      // Snapshot 已经消费了 [0, nextPtyOffset) 的 PTY 输出。
      nextPtyOffset: U64;

      // Host 在收到 ready barrier result 后，从 cut 补到这个 pinned head。
      commitEventSeq: U64;
      commitPtyOffset: U64;

      compressedLength: U64;
      uncompressedLength: U64;
      compression: "none" | "zstd";
      sha256: string;
      downloadPath: string;
      restoreThrough: "finish";
    })
  | (Common & {
      type: "pty-output";
      eventSeq: U64;
      startOffset: U64;
      data: Uint8Array;
    })
  | (Common & {
      type: "resize-applied";
      eventSeq: U64;
      cols: number;
      rows: number;
      widthPx: number;
      heightPx: number;
    })
  | {
      type: "writer-lease-status";
      active: true;
      expiresAt: number;
    }
  | {
      type: "writer-lease-status";
      active: false;
    }
  | {
      type: "input-ack";
      inputEpoch: string;
      clientInputSeq: U64;
      status: "written" | "duplicate" | "rejected" | "uncertain";
      authorityEventSeq: U64;
    }
  | (Common & {
      type: "resync-required";
      reason:
        | "journal-gap"
        | "slow-client"
        | "engine-mismatch"
        | "epoch-changed"
        | "data-disconnected"
        | "host-reconnect";

      // replacement credential 必须成对出现，并绑定当前 connection set/generation。
      dataTicket?: string;
      expiresAt?: number;
    });

type ClientFrame =
  | {
      type: "attach";
      engineId: string;
      deliveryGeneration: U64;
      hasLiveReplica: false;
    }
  | {
      type: "attach";
      engineId: string;
      deliveryGeneration: U64;

      // 三个 cursor 来自发送 attach 这一刻的 active libghostty replica。
      hasLiveReplica: true;
      lastSessionEpoch: U64;
      lastEventSeq: U64;
      nextPtyOffset: U64;
    }
  | {
      type: "writer-lease-renew";
      writerLease: string;
    }
  | {
      type: "key";
      writerLease: string;
      inputEpoch: string;
      clientInputSeq: U64;
      observedEventSeq: U64;
      code: string;
      key: string;
      text?: string;
      modifiers: number;
      action: "press" | "repeat" | "release";
      altGraph: boolean;
      composing: boolean;
      consumedModifiers: number;
      unshiftedCodepoint?: number;
    }
  | {
      type: "text";
      writerLease: string;
      inputEpoch: string;
      clientInputSeq: U64;
      data: string;
    }
  | {
      type: "paste";
      writerLease: string;
      inputEpoch: string;
      clientInputSeq: U64;
      data: string;
    }
  | {
      type: "focus";
      writerLease: string;
      inputEpoch: string;
      clientInputSeq: U64;
      focused: boolean;
    }
  | {
      type: "mouse";
      writerLease: string;
      inputEpoch: string;
      clientInputSeq: U64;
      action: "press" | "release" | "move" | "wheel";
      button: 0 | 1 | 2 | 3 | 4 | null;
      buttons: number;
      modifiers: number;
      altGraph: boolean;
      surface: { x: number; y: number };
      deltaX?: number;
      deltaY?: number;
      deltaMode?: "pixel" | "line" | "page";
    }
  | {
      type: "resize-request";
      writerLease: string;
      inputEpoch: string;
      clientInputSeq: U64;
      cols: number;
      rows: number;
      widthPx: number;
      heightPx: number;
    }
  | {
      type: "ack";
      sessionEpoch: U64;
      deliveryGeneration: U64;
      eventSeq: U64;
      nextPtyOffset: U64;
    };
```

Binary data frame 当前固定为 protocol v2。MVP 的 `flags` 必须为 `0`；任何非零 flags 都是未实现语义，Relay 必须 fail 当前 Host，不能把声明 compressed/final 的 payload 当 raw bytes 转发。v2 data kind 为 `PtyOutput`、`ResizeApplied`、`ReplayCommit`、`Reset` 和 Host-only `DeliveryBarrier`；`Reset` 不接受 Host 入站。

`modifiers` 和 `consumedModifiers` 固定采用 Ghostty 位布局：

```text
Shift=1, Control=2, Alt=4, Super=8, CapsLock=16, NumLock=32
```

`unshiftedCodepoint` 如果存在，必须是 Unicode scalar，不能落在 surrogate 区间。
两个 modifier 字段只能使用 `0x3f` 内的位，并且 `consumedModifiers` 必须是 `modifiers` 的子集；AltGraph 合成出的 Ctrl/Alt 不应伪装成真实 modifier。

`text.data` 与 `paste.data` 是 UTF-8 文本，分别最多 1 MiB；上限按编码后的 UTF-8 byte length 计算，不能按 JavaScript UTF-16 `length` 计算。`focus.focused` 只表达浏览器焦点状态。mouse 的 `buttons` 限于 `0..31`，`modifiers` 限于 `0x3f`，`surface.{x,y}` 是不超过 `1_000_000` 的非负整数 CSS px。press/release 的 `button` 必须为 `0..4`，move/wheel 必须为 `null`；wheel 至少携带一个有限、非零且绝对值不超过 `1_000_000` 的 delta，并必须携带 `deltaMode`，非 wheel 禁止携带 delta。cell、viewport 和 surface width/height 只作为 WTerm DOM adapter 的本地提示，不进入 wire；Host 只信自己的权威 geometry。Relay 不把这些输入编码成终端 bytes，只在现有 writer lease/input fence 验证后从可信 socket attachment 注入 `{connectionId, clientId, writerFence}` 并转发给 Host。Browser 不能声明或覆盖 `writerFence`。

`ack` 只表示该 delivery generation 的 frame 已可靠到达，不代表 libghostty 已应用。共享 credit 公式固定为 `(sentPtyOffset - ackedPtyOffset) + (sentEventSeq - ackedEventSeq) * 64`，上限为 512 KiB；Cloud 和 Host scheduler 必须复用 protocol package 的同一常量与公式。ACK cursor 只保存在当前 data WebSocket 的 hibernation attachment 中，不写入 SQL，也不作为下一 connection set 的单调下界；full reconnect 必须以新 `attach` 的 live replica cursor 重新建立 credit baseline。Host 重连或浏览器 data 断线时，DO 会 bump generation、关闭旧 data、签发同一 connection set 的 30 秒单次 data ticket，并发送一个 `resync-required`；已激活 control 保持 `active`，只同步新的 delivery generation，writer lease fence 不变。浏览器建立新 data 后，必须从当前 active replica 读取 cursor 并重新 `attach`；DO 不缓存 cursor 来自动 replay，从而消除“frame 已 apply、progress 尚未上报”的重复应用窗口。replacement ticket 过期只使 data 恢复失败，不撤销 control 或 lease；浏览器可改走 full connection set replacement。

Control activation 与 data delivery activation 是两个独立门闩。新 full control 只在首次成功 `attach` 时执行 `awaiting-attach -> active` 并决定是否授予 writer lease；每个新建或 replacement data socket 都从 `awaiting-attach` 开始，Client 的 `attach.deliveryGeneration` 必须显式声明它要提交的 generation，只有等于 SQL/current control/data generation 的一次成功 `attach` 才原子提交 baseline 并执行 `awaiting-attach -> catching-up`，`ReplayCommit` 再进入 `synced`。低于、高于或因跨通道重排而不再是 current generation 的 attach 一律静默丢弃，不能关闭 control 或释放 lease；active control 的 current-generation attach 如果已失去 matching data 也静默等待下一次恢复。只有首次尚未激活的 current-generation control 缺少 matching data 才 fail closed。重复 attach 不得重写 baseline 或 snapshot pin，只隔离该 browser connection set；data recovery attach 不得重新获取 writer lease，也不再发送第二个 `welcome`，只向 Host 发送新的 `attach-request`。`welcome` 只属于首次 control activation，用来确认身份、generation、session head 与初始 writer authority，本身不启动 directed delivery。

Data 缺失、等待 attach 或 cold snapshot 恢复期间，已激活 writer control 仍按原 lease/input fence 转发 key、text、paste、focus 和 resize，因此持续输出导致的 slow reset 不能阻断 `Ctrl-C`。ACK 仍严格绑定当前 generation 已 attach 的 data socket 及其 sent cursor；旧 generation ACK 被忽略，当前 generation 在 baseline 提交前也不能伪造进度。Mouse 由 Browser 在 Ghostty 权威 mouse mode 与可信 surface geometry 就绪时本地放行；Relay 不使用 browser 派生 cell 做恢复期判断，Host 继续以自己的权威 geometry 校验。

所有 Relay 到当前 Host control 的写入都经过同一个 fenced sink。若 socket 已有证据未处于可写 current 状态，Relay fail 该 Host pair，并把对应 semantic input 回为 `rejected`；若 `WebSocket.send()` 同步抛错，无法证明 frame 未进入运行时队列，Relay fail Host、广播 `host-offline`，并把 semantic input 回为 `uncertain`。两种情况都不能让异常冒回 Browser 入站队列，不能关闭 Browser control 或释放 writer lease；失败的 recovery `attach-request` 保留 Browser，等待新 Host 上线后由新的 generation 重试。Browser 不自动重试 `uncertain` input。

Writer lease 是需要显式维持的 control-plane liveness，而不是 data progress。Browser 只在持有 token 且 control 已激活时每 10 秒发送一次 `writer-lease-renew`；DO 只允许相同 `{clientId, leaseFence, token digest}` 的 active writer control 把 30 秒 deadline 向后推进，并返回 `writer-lease-status {active: true, expiresAt}`。renew 不能获取 lease、改变 fence、转发 Host 或替代 semantic input；data reset/reattach 也不能隐式续租。token 已过期、被新 fence 取代、control 尚未激活或角色不是 writer 时返回 `{active: false}`，Browser 必须立即清除 token 并进入 waiting。control close 继续按 fence 释放 lease；WebSocket ping/pong 的运行时自动响应不构成续租，也不能依赖它唤醒 DO。

Browser 的 session HTTP 边界不能依赖底层取消一定生效：connection-set 创建与 capability refresh 各自有 10 秒独立 deadline，并在外层结束等待；迟到响应在安装 client identity 或新 capability 前必须再次检查 abort/lifecycle。snapshot dedicated worker 的 HTTP 恢复上限为 30 秒，主线程再以 35 秒 watchdog 终止失去响应的 worker；这些 deadline 与 snapshot 失败的 2/4/8/16/30 秒重连退避相互独立。

`writer_lease` singleton 同时是 session 内 writer fence 的持久 high-water。lease 过期或 client LRU 回收只能把 deadline 置为失效并删除 client row，不能删除 singleton；下一次成功获取必须在旧 fence 上加一，达到 `u64::MAX` 后 fail closed，绝不能回绕。Browser 每次收到携带新 `writerLease` 的 writer `welcome`，必须生成新的随机 `inputEpoch`，把 `clientInputSeq` 从 `1` 开始；data-only recovery 不发送第二个 welcome，继续使用原 fence、input epoch 和序列。

DO 对所有 WebSocket 入站使用同一全局串行队列，因此 lease acquisition、fence 注入和向同一 Host control WebSocket 的 semantic input 发送保持同一顺序。Host 分开保存最高 `writerFence` 与该 fence 已绑定的 `{clientId, inputEpoch, highWater, bounded results}`：低 fence 一律 `rejected`；首次看到高 fence 时先原子推进 fence、清空旧 dedup 并保持 identity 未绑定，再校验输入。即使首帧的 seq、cursor 或纯 payload 校验失败，旧 fence 也已经永久失效，同时坏帧不会抢占新 fence；只有合法的 seq=1 可以完成 `{clientId, inputEpoch}` 绑定。绑定后，同 fence 改变 `clientId` 或 `inputEpoch` 一律 `rejected`。这使 Host 内存与 session 生命周期内累计 client/epoch 数量解耦，同时不会把被淘汰 client 的旧重传再次写入 PTY。

`hasLiveReplica: false` 不提供 delivery baseline；`hasLiveReplica: true` 只采样完整三元 cursor。两者在 DO 接受同 Host data WebSocket 上的 `DeliveryBarrier` 前都不允许 directed mutation 或 `ReplayCommit`。warm barrier 以 attach cursor 为 base 并向 browser control 发送 `replay-start {streamId, generation, base, pinnedCommit}`；snapshot barrier 可以在尚未 pin/尚未发送 start 时覆盖 live attach baseline，以 finalize 后的 snapshot cut seed attachment 并发送 manifest。

barrier 接受后，directed mutation 的上界和 `ReplayCommit` 的精确终点都使用 attachment 中不可变的 pinned commit，不再读取会继续变化的 session head。barrier 到 commit 之间出现 canonical frame、commit 不等于 pin、或当前 generation 的 delivery 被另一 barrier 冲突覆盖，都是 Host data 顺序违约并 fail closed。MVP 不接受 Host 发来的 directed `Reset`；合法 reset 只能走 DO 的 generation bump、关闭旧 data、签发 replacement ticket、control `resync-required` 这一条显式路径。

connection set 遵循 `reserved -> control-open/data-open -> paired -> ready -> replaced/closed`。reservation 必须先冻结 activation：未注册 client 使用当前 generation，已注册 client 使用 `current + 1`；HTTP 响应、control ticket 与 data ticket 都直接携带这一相同值。control claim 只允许以预留值执行 CAS 激活，不得在 WebSocket open 时重新选择 generation；data ticket 自身不能激活 client，且必须等待同 connection set 的 control。相同 browser identity 的新 pending set 会撤销旧票，但在旧 set 尚未 claim 时复用相同 activation。每次转换都同时校验 `{peer, connectionSetId, connectionId, hostFence | (clientId, deliveryGeneration)}`。未消费 reservation 与已注册 browser identity 共用 16-client 原子 quota；同一 browser identity 或 Host subject 最多保留一个 pending set，新签会撤销旧票。quota 满时只按 LRU 回收同时满足“无 open/hibernating socket、无 pending ticket、无有效 writer lease”的断连 identity。这样“只签票不连 WS”和“批量签完再消费”都不能绕过限额，也不会把 16 变成 session 终身 clientId 上限。

Hibernation socket 的 Error 事件直接执行同一套 fenced close lifecycle，不能假设 workerd 随后还会派发 Close。Host data error 会使当前 pair 下线；browser data error 会 bump generation 并签发 replacement ticket。后到的 Close 只会命中已推进的 fence/generation 并被幂等忽略。

Host 的 control/data 是两条独立 WebSocket，不存在跨通道顺序保证。Host 发送 `host-ready` 后必须等待 relay 在同一 control socket 返回 `host-ready-ack { sessionEpoch, headEventSeq, nextPtyOffset }`，再放行 data；DO 只在校验并提交 session head、将当前 fenced pair 切到 ready、完成现有 browser delivery reset 后返回 ACK。当前 fenced Host 在 barrier 前发送 data 会 fail closed，不能静默丢弃或在 DO 内隐式缓存。

Host 在 control 和 data 两条连接上分别用原始文本 `ping`/`pong` 检测 silent half-open，不能为此新增 JSON control frame。DO 通过 `setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"))` 在 hibernation 下直接应答，不唤醒 handler；Host 为两条连接分别记录精确原始 `pong`，任一连接约 45 秒无响应就 fence 整个 pair 并重连。heartbeat 只替换网络 pair，不改变 PTY、authority 或 `sessionEpoch`。

Browser 在 full connection set 激活后也分别维护 control/data 的 `pong` 时间戳，并每 15 秒向两条 socket 发送同一原始 `ping`；任一通道 45 秒无响应、不可写或超过发送队列上限都会 fence 整个 Browser pair。data-only replacement 期间 heartbeat 暂停并在 replacement data open 后按新 generation 重启，writer control 与 10 秒 lease renewal 保持独立可用。

入站消息在进入串行处理队列前完成 channel/type/size 校验。队列全局限制为 2048 条/32 MiB；Host data 单 socket 为 1024 条/16 MiB，browser control 为 8 条/16 MiB。Host 超限会 fail 当前 fenced pair，browser 超限只隔离对应客户端。

## Cloud session 创建必须可幂等恢复

Host 在启动 PTY 后、第一次 Cloud 请求前生成一次稳定 `sessionId`，并在所有超时、断网和凭据轮换重试中复用：

```http
POST /api/v1/sessions
Authorization: Bearer <bootstrap project token>
Content-Type: application/json

{ "sessionId": string, "engineId": string, "sessionEpoch": U64 }
```

Worker 直接以 `v1:${sessionId}` 命名 DO。首次精确 identity 初始化返回 `201`；同一 `{sessionId, engineId, sessionEpoch}` 重试命中同一 DO、返回新的 capabilities 和 `200`；同 ID 的 engine 或 epoch 冲突返回 `409`。因此即使 DO 已创建但 HTTP response 丢失，Host 也不会不断制造 orphan session。Host 必须校验响应 identity 与请求精确一致。

bootstrap secret 不接受 argv 明文，只能来自 `ZHONGDUAN_BOOTSTRAP_TOKEN` 或 `--bootstrap-token-file` / `ZHONGDUAN_BOOTSTRAP_TOKEN_FILE`。create 和 reclaim 每次尝试都重新读取来源；401/403、文件轮换和临时 Cloud 故障进入有上限 degraded backoff，不能终止 PTY。

Host capability 的半寿命+jitter 续期是 session 级 single-flight。每次共享续期有独立 10 秒 deadline，bootstrap provider、refresh 和 reclaim 都接收同一个 abort signal；Host 还必须在外层 race deadline，不能假设文件系统、fetch 或 response body 一定响应取消。单个调用者取消只退出自己的等待，不能取消其他调用者共享的续期。续期返回后必须再次校验 manager 生命周期、deadline 和 generation，过期操作不能迟到覆盖新 capability；旧 capability 尚未过期时可以短暂 fallback，已经过期则进入 degraded retry。

## Snapshot blob 使用私有 R2，由 session DO 协调 multipart 生命周期

snapshot 二进制采用统一公开契约：

```text
Content-Type: application/vnd.ghostty.snapshot
最大压缩长度:   32 MiB
最大解压后长度: 128 MiB

x-zhongduan-compression: none | zstd
x-zhongduan-compressed-length: canonical decimal u64
x-zhongduan-uncompressed-length: canonical decimal u64
x-zhongduan-sha256: lowercase hex SHA-256
x-zhongduan-engine-id
x-zhongduan-session-epoch
x-zhongduan-cut-event-seq
x-zhongduan-next-pty-offset
```

Host 用 Host capability 上传：

```http
PUT /api/v1/sessions/:sessionId/snapshots/:snapshotId
```

从 zstd body 生成到收到精确发布响应之间，Host session 级 publisher 最多持有一个不超过 32 MiB 的 owned pending body，以及与它绑定的 immutable metadata/snapshotId。transport error、timeout、5xx、409、400、无效 2xx body 或 identity mismatch 都不能证明 Cloud 已清理对象，因此跨 delivery scheduler 和 Host pair 重连必须先用同一 ID/body 重试，禁止重新 capture/compress。只有精确匹配的 `200/201`，或 Cloud 在完成 multipart abort 与 object existence 收敛检查后返回的 `422 snapshot-checksum-mismatch`，才允许释放 pending。成功 checkpoint cache 仍只保留 metadata 和 `R` cursor，不保留 body。

请求必须带规范的 `Content-Length`，且精确等于 `x-zhongduan-compressed-length`；不接受 chunked 或缺失长度上传。公开 Worker 只做 capability、path 和 metadata 校验，然后把同一个 request body stream 转交该 session DO。snapshot bytes 不进入 SQLite，也不在内存中聚合。

R2 写入只由 session DO 发起，流程固定为：

```text
取得 snapshotId 的 per-ID exclusive owner
  -> 持久化 recovery alarm
  -> SQLite INSERT preparing reservation
  -> 为本次 reservation 持久化 144-bit attempt-scoped object key
  -> 有界 R2 HEAD exact attempt key
  -> 有界 createMultipartUpload
  -> SQLite preparing -> uploading，并持久化 uploadId
  -> 取得每 session 唯一 body permit
  -> 单次 pull-through stream：length + SHA-256 + MD5 -> uploadPart(1)
  -> 校验 DigestStream.bytesWritten、声明 SHA-256、part ETag/MD5
  -> SQLite uploading -> completing，并持久化 part ETag
  -> 释放 body permit
  -> 有界 complete multipart
  -> 校验 version/etag/size/httpMetadata/customMetadata
  -> SQLite completing -> completed，并持久化 R2 identity
  -> SQLite transaction: completed -> servable，更新 latest 并 retire 超出保留集的 pointer
```

owner 在任何 `await` 之前同步取得；alarm 必须在 reservation 之前成功持久化，alarm 返回后又在下一个 R2 await 之前同步插入 reservation。任何 R2 `HEAD/create/uploadPart/complete` 都发生在 reservation 之后；alarm 写失败时没有 ledger row，也不触碰 R2。DO 为每个 `snapshotId` 使用同一把内存 owner，使 HTTP retry 与 maintenance 在任一 R2 await 前互斥；不同 ID 不互相阻塞。

正常 body 上传另有每 session 一个 permit，但它只在 `uploadId` 已持久化后覆盖 inbound body、`uploadPart` 与 SHA-256/MD5 摘要阶段。initial HEAD/create、complete/finalize、published/completed HEAD、abort 与 GC 都不占这个 permit；因此任一非 body R2 Promise 永久 pending 也不会阻止不同 ID 处理 body。permit 已被占用时，新请求保留其 `uploading(uploadId)` fence、cancel body 并返回 503，由 maintenance abort，不释放为无账本对象。owner 最后释放时会再次驱动 maintenance，且已到期但仍 active 的 row 仍保留 watchdog alarm。

公开 HTTP 上的 R2 HEAD/create/abort/complete 使用同一个 30 秒 typed deadline；超时 Promise 只挂 rejection observer，迟到结果绝不继续写 SQL 或执行 delete。HEAD timeout 可安全删除尚未触碰 R2 的 preparing row；create timeout 保留 preparing（最多遗留零 part MPU）；abort timeout 保留原 row；complete timeout 保留 completing。`uploadPart` 与 body pipeline 使用 10 分钟 reservation deadline。part/body 失败时主动 abort 本地 pipeline、digest writer 并观察所有迟到 rejection，但不等待可能永不 settle 的 FixedLengthStream、part 或 inline MPU abort；持久 `uploading(uploadId)` 交给 maintenance。这样失败请求有界返回并释放 body permit，同时所有可能存在的 part 仍有 durable fence。

body 通过一个 `TransformStream` 顺序写入 SHA-256 与 MD5 `DigestStream`，再进入 `FixedLengthStream` 和单个 multipart part，不使用可能缓存整份 body 的 `tee()`。complete 前同时要求观测长度、两个 `bytesWritten` 精确等于声明长度、SHA-256 等于 manifest，并要求 R2 part ETag 等于本地 MD5。R2 multipart complete object 通常没有 SHA-256 checksum，所以新 row 持久标记为 `multipart-verified`；只有这个 kind 允许 object checksum 缺失，并仍强制 multipart `-1` ETag、size、HTTP metadata、完整 custom metadata、version 与 etag。旧的 `single-put-verified` row 和没有 `uploadKind` custom metadata 的历史对象继续按原契约读取，但必须具有匹配的 R2 SHA-256。GET 后 Browser 仍对下载 bytes 计算 manifest SHA-256，再交给 Ghostty restore。

同一 `snapshotId`、同一 metadata 的 retry 幂等，不同 metadata 返回冲突。已处于 `published` 或 `completed` 的 retry 只允许 HEAD 并验证已持久化的 exact object identity，绝不能重建或覆盖 serving key。`cursor-ahead` 是 Host data WebSocket 与 HTTP 跨通道乱序的正常瞬态：complete object 和 `completed` row 保持不变；Host 等 head 追上后用同一 ID/body 重试，路径只做 HEAD + finalize，不再次 create/uploadPart。finalize 的 transport 或响应不确定同样保留 exact row/object。

每次新 reservation 使用 `.../:snapshotId/attempts/:random144.bin`；同一 ledger row 的 retry 复用该 key，但 row 被安全删除后，同一 `snapshotId` 的下一次 reservation 必须生成新 key。`aborted` recovery 也只在 abort 成功且有界 HEAD 明确为 null 后，以 exact CAS 把旧 `{key, uploadId}` 轮换成 fresh attempt key，再允许任何新 R2 调用。旧 maintenance task 永远只操作它捕获的旧 key，所以跨 DO replacement 的迟到 delete 不能删除后续 attempt。schema 3 的 deterministic key 只用于读取和回收既有 `single-put-verified` pointer，不再用于新上传。

公开 HTTP retry 不执行可能迟到的 destructive R2 delete。`uploading/completing` recovery 的 abort 必须在 deadline 内成功；随后有界 HEAD=null 才能把 `aborted` row 以 exact CAS 轮换到 fresh preparing attempt 并继续同一 body。abort/HEAD timeout、错误或 HEAD 非空都返回 503 并保留 ledger。声明 SHA-256 与实际 body 不符时也先释放 body permit，再在 per-ID owner 下执行有界 abort；只有 abort 成功、再次有界强一致 HEAD 明确为 null、且 exact aborted row 被同步删除后仍不存在同 ID replacement/published pointer，才返回 cleanup-safe 422 `snapshot-checksum-mismatch`。任一步失败都返回 503；HTTP 从不启动一个 timeout 后可能误删后续对象的 delete。

上传 ledger 状态为：

```text
preparing -> uploading -> completing -> completed -> servable
                     +-> aborted -> preparing(fresh attempt key)
                     +-> uncertain
preparing/completed --expiry--> retired
```

`preparing` 尚未上传 part；exact 同 ID retry 在 reservation 的 10 分钟期限内 fail closed，最坏需要等待该期限后才能重新上传，同时占用一个 upload 槽。到期后它先转 `retired`，再做 exact-key HEAD/delete/HEAD 并删除 row。如果 `createMultipartUpload` 已返回但 runtime 在 uploadId 持久化前终止，可能留下一个零 part MPU；它不含 snapshot bytes，不计入 1 GiB 对象上界，并由 R2 默认 7 天的 incomplete multipart auto-abort 回收，但相关 MPU metadata/Class A 操作不受 32-row 账本的绝对计数保证。Host 改用随机新 ID 也只能使用其余 upload 槽，不能绕过 4-row 上限。

maintenance 先做只读 preflight；发现 expired/retired/recoverable/可淘汰候选后，必须在任何 SQLite 状态迁移或 R2 调用前持久化 `now + 30s` watchdog。alarm await 返回后重新采样 `now`、per-ID owners 与所有 active/hibernating attachment pins，再在同一无 await 调用栈内重验候选、执行 retired/reconcile 并抢占 candidate owners。alarm 失败时 SQL 不变且 R2 调用数为零。每次 constructor/request/alarm 触发的 `maintain()` 只启动一个最多 32 个 candidate 的批次；运行期间出现的新请求只合并并持久化下一次 alarm，不在当前 activation 递归续跑另一批。

每个 retired 或 multipart recovery candidate 运行成独立 per-ID maintenance task。主调度器不等待其 R2 Promise，某个 ID 的 HEAD/delete/abort 永久 pending 只持续占该 ID owner/ledger，其他 ID 的 GC、recovery 与新上传继续运行。destructive HEAD/delete/HEAD task 不能 timeout 后脱离 owner；它必须持有 owner 到真实 settle，避免迟到 delete 删除同 key 的后续对象。task 异常在 watchdog 与 durable row 已存在的前提下收敛为有界 alarm retry，不泄漏为后台 unhandled rejection。

对已持久化 uploadId 的 `uploading/completing`，maintenance 抢相同 per-ID owner 后重读 SQL identity，之后才允许 resume/abort/HEAD。abort 成功后仍执行 exact-key HEAD/delete/HEAD，确认 key 为空才删除 row；普通 R2 abort/delete 错误保留 row，并用 alarm backoff 重试。若 abort 返回 `NoSuchUpload` 且 HEAD 得到完全匹配的 object，先把 exact `{version, etag}` 持久化为 `completed`，随后统一走 completed retention/finalize；不能从 completing 直接跨 R2 与 SQLite 删除。

若 abort 返回 `NoSuchUpload` 且强一致 HEAD 仍为 null 或 mismatch，DO 无法证明旧 complete 是否还可能落盘，必须持久转为 `uncertain` 并 fail closed。`uncertain` 继续计入 upload/总账本上限，不由 alarm 每 30 秒轮询；只有 Host 保留同一 `snapshotId` 和 body 的显式 retry 或运维动作重新 HEAD。HEAD exact 时可记录 completed 并 finalize，null/mismatch 时仍保留，不得 create、uploadPart 或释放槽。即使 abort 实际成功，runtime 在 R2 成功响应与 SQL `aborted` 提交之间终止，也可能永久消耗一个 upload 槽；这是跨系统事务无法消除的安全侧结果。MVP 不宣称所有异常都最终 GC，但任何路径都不能因此绕过硬上限或产生第 5 个 upload row。

`snapshot_upload` 最多 4 行；`snapshot + snapshot_upload` 共用每 session 32 行硬上限。reservation 满或总账本满时必须在任何 R2 操作前返回 503。每个登记对象或 part 最多 32 MiB，所以除上述零 part MPU metadata 外，升级后由该 session ledger 拥有的 snapshot bytes 最多 1 GiB；该承诺不追溯扫描未发布旧实现可能遗留、且不在 SQLite ledger 中的 R2 key。极端终止最多让 4 个 upload 槽进入永久 `uncertain`；服务会拒绝新随机 ID，而不是释放未证明安全的 fence。Host 对除 cleanup-safe 422 以外的全部失败状态保留同一 ID/body；只有 200/201 成功，或 DO 已完成 abort、强一致 HEAD 明确旧 key 不存在且 exact ledger CAS 删除成功后返回的 422 `snapshot-checksum-mismatch`，才允许释放 pending body。

Servable 保留集精确定义为：`latest_snapshot_id`、所有当前或 hibernating browser data WebSocket attachment 中非空的 `snapshotId`、正在验证 published pointer 的 public upload owners，以及 2 个额外 unpinned grace snapshot。grace recency 是持久语义：成功 finalize 或 exact published retry 会提升对应 snapshot；pin/owner 释放时，上一轮实际保留项进入候选，当前事务再选两个额外项。published retry 只有在 HEAD exact 后同步重读并校验完整 pointer、持久刷新 grace，才允许返回 200，覆盖 HTTP 成功到 Host delivery barrier 的间隙。`ReplayCommit` 只把 data attachment 切到 `synced`，不能清除 snapshot pin；data reset/close 通过 attachment 生命周期自然释放 pin，不增加 Browser release 协议。

两个 grace 槽与最多 23 项的 previous-retained 候选集都持久化；容量对应 16 个 browser pins、最多 4 个 servable public upload owners、latest 与两个 grace。steady state 已有 32-row 硬界，每次 reconciliation 最多读取 33 行，按上一轮实际保留集、当前 pins/owners 和 finalize/refresh touch 重算两个槽并压缩候选集；因此槽被新 pin 吞掉或暂时为 null 后，刚释放的保护项仍能接替 grace。schema 3 大账本迁移使用同一 retained-union 语义，但通过单调 `rowid` 游标每轮只扫描 32 行；动态 pin/owner 变化不重置游标。真正的保护集始终只有当前 pins、servable public owners、latest 和选出的两个 grace，不会把全部候选都永久保留。latest 的比较仍只使用不可变的 `cutEventSeq`、`nextPtyOffset`、`createdAt`、`snapshotId`，前两个用 `BigInt`；grace refresh 不改写 latest 排序。

schema 迁移只有基线 `v3 -> final v4`：给现有 snapshot pointer 增加默认 `single-put-verified` kind，给单行 session state 增加 bounded retention 状态，并创建空 multipart upload ledger/index。迁移不复制、不排序、不回填大表；cold constructor 在并发门内检查 backlog 并可靠持久化 alarm，首次 alarm 写失败会让 activation 失败，下一次 cold wake 重新尝试。迁移期间新 reservation fail closed，GET 仍可读取 servable pointer。

Retention 在 finalize 的同一个 SQLite transaction 中先把最老非保留行改为 `retired`；从 transaction 提交起新的 GET 必须 404。R2 delete 在事务后异步执行，失败 tombstone 保持在 32-row 上限内并由 alarm 重试，成功后才按 exact row identity 删除 SQLite。GET 已取得 pointer 后与 GC 并发时允许 fail closed 为 503，但绝不能返回错误对象或让 retired pointer 重新可见。乱序 finalize 不允许 latest pointer 回退；比较顺序是 `cutEventSeq`、`nextPtyOffset`、`createdAt`、`snapshotId`，前两个必须用 `BigInt`。DO hibernation 后 pin 从 serialized WebSocket attachment 重建，ledger 与 alarm 来自持久化存储。

Host、writer、observer 使用各自 capability 下载：

```http
GET /api/v1/sessions/:sessionId/snapshots/:snapshotId
```

Worker 先从 DO 读取已发布 pointer，再从 R2 读取 object。version、etag、size、checksum policy、HTTP metadata 或 custom metadata 任一不匹配时，先 cancel R2 body，再返回 503；绝不能返回部分或错误对象。成功响应把 R2 `ReadableStream` 直接返回，并固定 `Cache-Control: private, no-store` 与 `X-Content-Type-Options: nosniff`。

Capability 是 session-scoped HMAC bearer，不引入 IdP。Host capability 默认 24 小时，writer/observer 默认 8 小时；share URL 可以自然过期，但活跃 Host/Browser 应在 capability 半寿命附近加入 jitter 主动刷新：

```http
POST /api/v1/sessions/:sessionId/capabilities
Authorization: Bearer <host capability>
Content-Type: application/json

{ "role": "writer" | "observer" }

POST /api/v1/sessions/:sessionId/capabilities/refresh
Authorization: Bearer <still-valid host | writer | observer capability>
Content-Type: application/json

{}
```

mint 只接受 Host bearer，subject 必须由服务端生成，浏览器不能指定 subject 或签发其他 capability。refresh 必须保持原来的 `{sessionId, role, subject}`，只生成新的 `tokenId`、`issuedAt` 和 `expiresAt`；过期、伪造、跨 session 的 token 都不能 refresh，也不能通过 body 改 role。统一响应为 `{ capability, expiresAt, role }`，其中 `expiresAt` 是 Unix epoch seconds。Host 可以用有效 Host capability 随时生成新的短期分享链接。

Host sleep 超过 24 小时后只允许 bootstrap project token 恢复 Host authority：

```http
POST /api/v1/sessions/:sessionId/capabilities/host/reclaim
Authorization: Bearer <bootstrap project token>
Content-Type: application/json

{ "engineId": string, "sessionEpoch": U64 }
```

Worker 必须向对应 DO 只读校验已存在 session 的 `{sessionId, engineId, sessionEpoch}` 精确相等后，签发新的 Host capability；这个 endpoint 不创建 session、不接受 browser capability，也不返回实际 session identity。capability 和 bootstrap secret 不进入日志。

Warm replay 与 snapshot recovery 共用同一个 data-plane barrier。Host 暂停新的 canonical frame，把 marker 排在此前 canonical 之后，并在同一 Host data WebSocket 发送：

```ts
type DeliveryBarrierPayload =
  | { mode: "warm"; connectionId: string }
  | { mode: "snapshot"; connectionId: string; snapshotId: string };

type DeliveryBarrier = {
  kind: "DeliveryBarrier";
  sessionEpoch: U64;
  streamId: number;
  deliveryGeneration: U64;
  commitEventSeq: U64;
  commitPtyOffset: U64;
  payload: DeliveryBarrierPayload;
};

type DeliveryBarrierResult = {
  type: "delivery-barrier-result";
  status: "ready" | "stale" | "rejected";
  mode: "warm" | "snapshot";
  connectionId: string;
  snapshotId?: string;
  streamId: number;
  deliveryGeneration: U64;
  commitEventSeq: U64;
  commitPtyOffset: U64;
};
```

marker header 的 commit cursor 必须精确等于 DO 处理该 data frame 时的 canonical head。这样 ACK 虽然返回 Host control WebSocket，入站顺序仍由同一个 data WebSocket 的 marker 保证。snapshot payload 不携带 engine、长度、checksum 等可重复伪造的元数据；DO 只从已 finalize 的 SQLite row 构造 manifest，并校验 snapshot epoch/engine 与 `cut <= pinned commit`。成功路径固定为：

```text
1. 在 browser data attachment 原子 pin commit；snapshot mode 同时以 cut seed cursor
2. 向 browser control 发送 replay-start 或 snapshot-manifest
3. 消息成功入队后，向 matching/current Host control 发送 status=ready 的 result
4. Host 收到 ready 后，在同一 data WebSocket 严格发送 tail -> ReplayCommit(pin)
5. ReplayCommit 之后才恢复 queued canonical frame
```

目标 browser 已关闭、data generation 已 bump 或 full connection 已替换时，旧 marker 是合法陈旧工作：DO 返回 `status=stale`，不 seed、不发送 start，也不伤 Host/其他 browser。snapshot 不可服务或该 delivery 无合适 baseline 时返回 `status=rejected`。browser control send 失败也只隔离该 browser 并返回 rejected。Host 对 stale/rejected 必须丢弃对应 tail 并恢复 queued canonical。

同一个 marker 只有在 attachment 仍精确停在该 seed、没有发送任何 tail 时才可幂等重发 start/result；cursor 一旦推进，重复 marker 是当前 delivery 的顺序冲突，必须 fail 当前 Host，不能把 delivery 倒退到 base/cut。pin 之前收到 directed mutation/commit 同样 fail Host。

Browser 的 control/data WebSocket 也没有跨通道顺序保证。即使 DO 先发 start/manifest 再向 Host 返回 ready，directed data 仍可能先于 browser control message 到达。SessionClient 必须按 connection/stream/generation 暂存 pre-control binary frame，队列严格限制为 2 MiB 或 1024 帧，先到任一上限就关闭当前 data 并请求 resync；收到匹配的 `replay-start`/`snapshot-manifest` 后才按 base/cut 应用，`welcome` 不能释放 queued data，旧 generation 直接丢弃。

connection-set 响应给出新 generation 后，SessionClient 必须先 fence 并停止旧 data handler，再从当时的 active libghostty replica 采样完整 attach cursor、发送 `attach`，最后才允许新 generation 的 queued frame 进入 apply 路径。不能等待“首个新 generation frame”再切 fence 或采样 cursor，否则旧 handler 与新 frame 的竞态会把重复/遗漏固化为新的 baseline。

## 为什么既要 `eventSeq`，又要 `ptyOffset`

`ptyOffset` 只能排序 PTY bytes，无法表达 resize 在哪里发生。

例如：

```text
event 100: PTY_OUTPUT offset 5000..5100
event 101: RESIZE 120x40
event 102: PTY_OUTPUT offset 5100..5300
```

只看 byte offset，会不知道 resize 应该放在两段输出之间还是之后。

因此：

```text
eventSeq
  = 所有会改变 canonical terminal state 的全序

ptyOffset
  = 只用于验证 PTY byte stream 是否连续、重复或丢失
```

---

# 二、最普通的 raw fast path

假设程序输出：

```text
ESC[?2004h
user@host:~$ 
```

真实字节：

```text
1b 5b 3f 32 30 30 34 68
75 73 65 72 40 68 6f 73 74 3a 7e 24 20
```

Host 收到：

```text
headEventSeq = 18
nextPtyOffset = 4200
```

处理：

```text
1. Host libghostty.apply(bytes)
2. eventSeq = 19
3. journal append:
   PTY_OUTPUT {
     eventSeq: 19
     startOffset: 4200
     length: 21
   }
4. nextPtyOffset = 4221
5. 广播给客户端
```

网络帧：

```text
PTY_OUTPUT
  epoch             = 7
  deliveryGeneration = 3
  eventSeq           = 19
  startOffset        = 4200
  data               =
    1b 5b 3f 32 30 30 34 68
    75 73 65 72 40 68 6f 73 74 3a 7e 24 20
```

客户端验证：

```ts
function applyPtyFrame(frame: PtyOutputFrame): void {
  if (frame.sessionEpoch !== replica.sessionEpoch) {
    throw new Error("wrong session epoch");
  }

  if (frame.eventSeq !== replica.lastEventSeq + 1n) {
    requestResync("event gap");
    return;
  }

  if (frame.startOffset !== replica.nextPtyOffset) {
    requestResync("PTY offset gap or duplicate");
    return;
  }

  ghosttyTerminalVtWrite(replica.terminal, frame.data);

  replica.lastEventSeq = frame.eventSeq;
  replica.nextPtyOffset += BigInt(frame.data.length);
}
```

此时 Host 和 Client 是相同输入驱动的两个 libghostty 状态机：

```text
T_host'   = apply(T_host, bytes)
T_client' = apply(T_client, bytes)
```

只要起点相同：

```text
T_host == T_client
```

---

# 三、在半截 SGR 序列中取 snapshot

这是最能体现 continuation 价值的例子。

PTY 先输出：

```text
ESC [ 3 1
```

即：

```text
1b 5b 33 31
```

它还没有输出 `m`，所以 parser 正处在半截 CSI 中。

Ghostty 官方 C example 现在就故意写入 `"\x1b[31"` 后立刻 snapshot，用于验证这种情况。

## Host 当前状态

```text
eventSeq     = 441
ptyOffset    = 9012

PTY 历史最后四字节：
  offset 9008..9012 = 1b 5b 33 31

libghostty parser：
  CSI parameter state
  parameter = 31
  尚未 dispatch SGR
```

此时 snapshot：

```text
snapshotId        = snap-88
cutEventSeq       = 441
nextPtyOffset     = 9012
```

简化后的 snapshot 内容：

```text
47 48 4f 53 54 53 4e 50 01 00
^^^^^^^^^^^^^^^^^^^^^^^ ^^^^^
GHOSTSNP             v1

TERMINAL
  cols=120 rows=40
  modes...
  palette...

SCREEN
PAGE
  当前 screen/cursor/attributes...

CONTINUATION
  payload = 1b 5b 33 31

READY

HISTORY
PAGE...
FINISH
```

当前记录编号中 `CONTINUATION = 7`。

然后程序继续输出：

```text
mERROR ESC[0m
```

字节：

```text
6d 45 52 52 4f 52 1b 5b 30 6d
```

它是 snapshot 之后的 tail：

```text
PTY_OUTPUT
  eventSeq    = 442
  startOffset = 9012
  data        = 6d 45 52 52 4f 52 1b 5b 30 6d
```

客户端过程：

```text
1. decode snapshot
2. CONTINUATION 把 parser 恢复为“已经收到 ESC[31”
3. 到 READY
4. apply tail 的第一个字节 6d
5. parser dispatch SGR 31
6. ERROR 被渲染成红色
7. ESC[0m 恢复样式
```

最关键的不变量：

```text
snapshot 已经逻辑消费了 offset < 9012 的所有字节
tail 必须从 offset 9012 开始
```

**绝不能因为 CONTINUATION 里出现了 `1b 5b 33 31`，又从 offset 9008 开始重发。**

否则客户端会看到：

```text
ESC[31 ESC[31 m...
```

continuation 是 parser 恢复材料，不是需要再次计入输出 offset 的新输出。

---

# 四、在半个 UTF-8 字符中 snapshot

例如 `中` 的 UTF-8：

```text
e4 b8 ad
```

Host 只收到前两个字节：

```text
PTY_OUTPUT
  eventSeq    = 500
  startOffset = 12000
  data        = e4 b8
```

此时：

```text
nextPtyOffset = 12002

screen state：
  尚未出现“中”

UTF-8 decoder：
  已保存 e4 b8
  等待第三字节
```

snapshot continuation 中包含：

```text
e4 b8
```

之后 tail：

```text
PTY_OUTPUT
  eventSeq    = 501
  startOffset = 12002
  data        = ad
```

客户端恢复后：

```text
snapshot continuation: e4 b8
tail:                  ad
结果:                   中
```

没有 continuation 时，常见错误会是：

```text
�
```

或者直接丢失字符。

---

# 五、snapshot 切点如何和新输出保持一致

Host authority actor 串行处理 VT write、resize、语义输入和 snapshot encode。snapshot writer 是同步调用，因此在一个 actor turn 内捕获 snapshot cut `R`；返回后 authority 立即继续工作，Cloud canonical pump 也继续发送：

```ts
function captureSnapshot(): Promise<Snapshot> {
  return actor.enqueue(() => {
    return {
      cutEventSeq: eventSeq,
      nextPtyOffset,
      bytes: ghostty.encodeSnapshot(),
    };
  });
}
```

`snapshot@R` 在 authority actor 外异步执行 level-1 zstd 和 immutable PUT；这段慢路径不占用 delivery barrier，也不暂停 canonical，因此其他已同步 TUI 和 semantic input 不受 R2 时延影响。发布完成后，scheduler 才短暂停 canonical pump，读取当前 cursor `C`，并要求 journal 的 `R..C` 连续且 directed tail 同时不超过 256 KiB / 512 帧。随后严格执行：

```text
flush canonical through C
DeliveryBarrier(snapshot R, pinned commit C)
directed tail R+1..C
ReplayCommit(C)
resume canonical C+1
```

成功发布的 checkpoint 在 Host session 内只短暂缓存 30 秒，并可跨 Host relay pair 重连复用。缓存只包含 immutable snapshot metadata、engine 和 `R` cursor，不保留 Ghostty 原始 snapshot bytes 或 zstd body。每个 cold delivery 仍必须在发送 marker 前重新读取自己的 `C` 并验证 `R..C` 的 journal continuity 与 256 KiB / 512 帧预算；因此多个同时 cold 的 client 可以共享同一 snapshotId，但不能共享未经复核的 commit。短 TTL 也避免在 Browser 尚无 `snapshot-restore-failed` 回报时无限复用损坏对象。

snapshot encode 的 5 秒 authority budget、HTTP publish deadline 和 marker ACK 的 5 秒 deadline 相互独立，不能用一个总 deadline 覆盖慢链路。如果发布后发现 journal gap 或 tail 超预算，Host 在发送 marker 前恢复 canonical、失效该 checkpoint 引用，并给失败 delivery 保留独立 backoff；session 级 quiet gate 只约束下一次 cold capture，输出安静后从更新的 `R` 单次重捕。其他可 warm delivery 可以越过这个 cold gate。marker 一旦发送而结果不确定，必须 fence 整个 Host pair，不能按 pre-marker 路径继续。

错误实现是：

```text
锁住 terminal
    ↓
ghostty_snapshot_encode(
  writer = 直接发 WebSocket
)
```

网络一旦阻塞，PTY 也被锁住。

正确实现是：

```text
锁住 terminal
    ↓
encode 到本地内存
    ↓
解锁 terminal
    ↓
压缩
    ↓
异步上传 / 发送
```

---

# 六、snapshot 期间 resize 到达

假设：

```text
event 510:
  PTY_OUTPUT "当前是 80 列布局"

开始 snapshot：
  cutEventSeq = 510

snapshot encode 期间：
  客户端请求 resize 120x40
```

不能让 resize 修改一半 snapshot。

应当排队：

```text
snapshot@510
  geometry = 80x24

snapshot 完成

event 511:
  RESIZE 120x40

event 512:
  PTY_OUTPUT 应用收到 SIGWINCH 后的重绘
```

客户端恢复：

```text
decode snapshot@510
  得到 80x24

apply event 511
  resize 为 120x40

apply event 512
  应用新布局重绘
```

一个 resize 应当被看成一个逻辑事件：

```text
RESIZE {
  terminal.resize(...)
  ioctl(pty_fd, TIOCSWINSZ, ...)
  notify child / SIGWINCH
}
```

它和 PTY bytes 必须由同一个 session actor 排序。

Host 不需要猜“这些 output 是 resize 前生成的还是之后生成的”。只要 Host 自己按照：

```text
resize -> process following PTY bytes
```

处理，客户端重复相同顺序即可。

---

# 七、多个客户端的 resize 怎么办

不能让每个 observer 都改变 PTY 大小。

推荐语义：

```text
一个 writer lease
writer 的窗口大小 = canonical PTY size

observer：
  不改变 canonical size
  本地缩放、裁剪或留白显示
```

例如：

```text
Alice writer:   120x40
Bob observer:    80x24
Web observer:   100x30
```

canonical libghostty state 始终是：

```text
120x40
```

Bob 可以只显示其中一部分，但不能把自己的 80x24 发给 PTY。

否则：

```text
Alice resize 120x40
Bob resize 80x24
Alice resize 120x40
Bob resize 80x24
```

TUI 会不断收到 SIGWINCH 并重绘。

---

# 八、慢客户端发生 output spew

假设客户端已确认：

```text
ACK eventSeq=8000
ACK nextPtyOffset=4 MiB
```

程序开始：

```bash
yes
```

Host 很快推进到：

```text
eventSeq=100000
ptyOffset=80 MiB
```

客户端网络只有 50 KiB/s。

绝对不能：

```text
把 76 MiB 全部排进 WebSocket
```

否则它会一直播放过去，最新 Ctrl-C 的视觉反馈要几分钟后才出现。

## 每个客户端维护有界发送队列

```ts
interface ClientDeliveryState {
  generation: bigint;

  mode:
    | "synced"
    | "desynced"
    | "sending-snapshot";

  queuedBytes: number;
  lastAckEventSeq: bigint;
  nextAckPtyOffset: bigint;
}
```

超过阈值：

```ts
const MAX_QUEUED_OUTPUT = 512 * 1024;

function enqueueOutput(
  client: ClientDeliveryState,
  frame: PtyOutputFrame,
): void {
  if (client.mode !== "synced") {
    return;
  }

  if (client.queuedBytes + frame.data.length > MAX_QUEUED_OUTPUT) {
    client.mode = "desynced";
    client.generation++;
    resetDataTransport(client);
    scheduleLatestSnapshot(client);
    return;
  }

  enqueue(client, frame);
}
```

新的恢复数据：

```text
SNAPSHOT_MANIFEST (control)
  deliveryGeneration = 9
  cutEventSeq         = 100000
  nextPtyOffset       = 80 MiB

HTTP GET snapshot blob from private R2

DIRECTED PTY_OUTPUT (data)
  deliveryGeneration = 9
  eventSeq            = 100001
  startOffset         = 80 MiB
```

客户端丢弃所有：

```text
deliveryGeneration < 9
```

## WebSocket 的一个现实问题

TCP/WebSocket 中已经写进 socket buffer 的旧数据无法撤回。

所以不能只是：

```text
旧数据还堵在前面
然后发送 SNAPSHOT
```

因为 snapshot 仍然排在 76 MiB 旧数据后面。

WebSocket fallback 最好拆成两个连接：

```text
control WebSocket
  input
  resize
  lease
  ACK
  reconnect

data WebSocket
  PTY output
  directed tail
  DeliveryBarrier
  ReplayCommit

private HTTP + R2
  snapshot blob
```

慢客户端时：

```text
只关闭 data WebSocket
control WebSocket 保持
```

重新建立 data WebSocket 后，control 发送最新 snapshot manifest；浏览器通过私有 HTTP GET 流式恢复 R2 blob，再应用新 generation 的 directed tail。

使用 WebTransport/QUIC 时更自然：

```text
reset old output stream
open a new live stream
```

这才是真正能“跳过中间状态”。

---

# 九、snapshot 和 tail 同时到达

假设 snapshot：

```text
cutEventSeq   = 2000
nextPtyOffset = 400000
```

生成和传输 snapshot 的过程中，Host 已经继续运行到：

```text
headEventSeq   = 2050
nextPtyOffset  = 410000
```

客户端可能交错收到：

```text
snapshot HTTP body chunks
tail event 2001
tail event 2002
...
tail event 2050
```

客户端在 snapshot 到 `READY` 前不能应用 tail；如果 tail 甚至早于 control manifest 到达，则先进入上一节的 pre-control 有界队列。

```ts
interface PendingRestore {
  cutEventSeq: bigint;
  nextPtyOffset: bigint;
  tail: ServerFrame[];
}

function receiveTailDuringSnapshot(frame: ServerFrame): void {
  pendingRestore.tail.push(frame);
}
```

snapshot decode 完成后：

```ts
function finishReady(
  restoredTerminal: GhosttyTerminal,
  restore: PendingRestore,
): void {
  replica.terminal = restoredTerminal;
  replica.lastEventSeq = restore.cutEventSeq;
  replica.nextPtyOffset = restore.nextPtyOffset;

  restore.tail.sort((a, b) =>
    Number(a.eventSeq - b.eventSeq));

  for (const frame of restore.tail) {
    applyOrderedFrame(frame);
  }
}
```

通常不应该真的 `sort`；tail channel 应当有序，排序只是示意。发现 gap 就重新同步，不应长期等待。

---

# 十、READY 后立即吃 tail，还是先恢复完整 history

这是一个真实的取舍。

当前 decoder 明确支持：

```text
decoder_ready()
render
在 decoder_next() 之间：
  resize
  apply live PTY input
```

但如果 live terminal 已经变化到无法安全 prepend 某个历史 PAGE，该 PAGE 仍会被消费和验证，只是报告恢复了 0 行。

因此有两种模式。

## 模式 A：严格恢复完整 scrollback

```text
decoder_ready()
立即显示当前屏幕

tail 暂存在内存

while decoder_next() == SUCCESS:
  恢复所有 history

FINISH

应用暂存 tail
进入 live
```

优点：

```text
完整 scrollback
严格确定性
```

缺点：

```text
如果历史有 500 MiB，live 更新需要等待很久
```

## 模式 B：优先 live

```text
decoder_ready()
立即显示

立即应用 tail
同时 decoder_next() 尝试补 history
```

优点：

```text
立刻追上当前状态
```

缺点：

```text
部分旧 history 可能无法安全插入
```

## 我更建议第三种产品设计

```text
snapshot:
  当前 screen
  alternate screen
  最近有限行 scrollback
  parser continuation

archive:
  更老的历史独立存储和查询
```

例如：

```text
snapshot 保留最近 2,000～10,000 行
更老内容进入 terminal transcript/archive
```

这样：

```text
READY 很快
完整 snapshot 也有明确上限
tail 可以很快开始
旧历史不需要插回正在变化的 libghostty PageList
```

这可能是你们真正需要给 libghostty 增加的 API：

```c
typedef struct {
  uint64_t max_history_rows;
  uint64_t max_history_bytes;
  bool include_alternate;
} GhosttySnapshotEncodeOptions;
```

当前公开 C API主要是完整 snapshot；若需要 bounded reconnect snapshot，可能要用内部 Zig 层能力或扩展 C API。

---

# 十一、终端 effect 怎么处理

PTY stream 不只有状态变化。

例如：

```text
07                        BEL
ESC ] 2 ; title BEL       设置标题
ESC [ 6 n                 查询光标位置
ESC ] 52 ; ...            剪贴板
```

libghostty 把 bell、title、PWD、clipboard、notification、设备查询和 `WRITE_PTY` 等暴露为同步 effect callback；回调发生在 VT write 内部，不应阻塞或重入同一个 terminal。

Host 权威实例：

```text
WRITE_PTY              开启
SIZE                   开启
DEVICE_ATTRIBUTES      开启
XTVERSION              开启
CLIPBOARD              根据策略
BELL / NOTIFICATION    转成产品 effect
```

客户端 replica：

```text
WRITE_PTY              必须关闭
SIZE query response    必须关闭
DEVICE_ATTRIBUTES      必须关闭

BELL                    可开启，但 catch-up 时抑制
TITLE/PWD               可更新本地 UI
CLIPBOARD               必须权限检查
```

否则一次：

```text
CSI 6 n
```

会变成：

```text
Host 回答一次
Client A 回答一次
Client B 回答一次
```

远端程序收到多个 cursor-position report。

## Host callback 也不要直接做阻塞 IO

```c
static void write_pty_callback(
    GhosttyTerminal terminal,
    const uint8_t* data,
    size_t len,
    void* userdata
) {
  Session* session = userdata;

  // 不要在回调里进行阻塞 write。
  // 复制进本地小队列，vt_write 返回后由 actor flush。
  queue_internal_pty_reply(session, data, len);
}
```

处理一段输出：

```c
ghostty_terminal_vt_write(session->terminal, bytes, len);

// 现在再向 PTY 写 terminal query replies。
flush_internal_pty_replies(session);
```

## catch-up 期间抑制瞬时 effect

客户端断线期间有：

```text
BEL
BEL
notification
title changed
```

恢复时通常应该：

```text
BEL              不重放
notification     不重放
clipboard write  不自动重放
title            最终状态保留
PWD              最终状态保留
```

客户端可以维护：

```ts
type ApplyMode = "snapshot" | "catch-up" | "live";
```

```ts
function onBell(): void {
  if (applyMode === "live") {
    ringBell();
  }
}
```

当客户端追到 Host 提供的 live watermark 后：

```text
applyMode = live
```

---

# 十二、输入为什么最好传 semantic key，而不是客户端编码后的 bytes

假设远端程序刚刚启用了 Kitty keyboard protocol：

```text
PTY_OUTPUT:
  CSI > 1 u
```

Host 已经应用，但客户端还没收到这一帧。

用户此时按：

```text
Shift+Enter
```

如果客户端按自己落后的 terminal state 编码，可能发出错误字节。

因此：

```text
Client:
  KEY {
    code = Enter
    key = Enter
    modifiers = Shift
    action = press
    altGraph = false
    composing = false
    consumedModifiers = 0
  }

Host:
  根据 authoritative libghostty keyboard mode 编码
  写入 PTY
```

Paste 同理：

```text
Host authoritative terminal 决定当前是否 bracketed paste
```

客户端不要自行包：

```text
ESC[200~
...
ESC[201~
```

除非协议明确保证客户端 replica 已追到当前 authoritative revision。

---

# 十三、输入 ACK 的崩溃窗口

客户端发送：

```text
KEY
  clientInputSeq = 77
  key = Enter
```

Host：

```text
1. 写入 PTY
2. 发送 ACK 77
```

如果发生：

```text
写入 PTY成功
    ↓
Agent 崩溃
    ↓
ACK 尚未发送
```

客户端无法判断 Enter 是否执行。

不能自动重发，否则可能：

```text
命令执行两次
paste 两次
Ctrl-C 两次
```

协议应明确：

```text
同一 writerFence + inputEpoch：
  如果 Host 仍有 dedup 记录，重发 seq=77 只返回 duplicate ACK
  如果逐条结果已被有界缓存淘汰，但 seq 不高于 high-water，则返回 uncertain

更高 writerFence：
  必须使用新 inputEpoch，并从 seq=1 开始
  Host 原子清除旧 fence 状态；旧 fence 的任何迟到输入都返回 rejected

Host authority epoch 已变化：
  未确认输入返回 uncertain，客户端不自动重发
```

伪代码：

```ts
function handleInput(input: ForwardedClientInput): void {
  if (input.writerFence < currentWriterFence) {
    sendInputAck(input.clientInputSeq, "rejected");
    return;
  }

  if (input.writerFence > currentWriterFence) {
    currentWriterFence = input.writerFence;
    currentWriter = undefined;
    inputDedup.clear();
    inputHighWater = 0;
  }

  if (currentWriter === undefined) {
    if (input.clientInputSeq !== 1 || !validatePureInput(input)) {
      sendInputAck(input.clientInputSeq, "rejected");
      return;
    }
    currentWriter = {
      clientId: input.clientId,
      inputEpoch: input.inputEpoch,
    };
  } else if (
    input.clientId !== currentWriter.clientId ||
    input.inputEpoch !== currentWriter.inputEpoch
  ) {
    sendInputAck(input.clientInputSeq, "rejected");
    return;
  }

  const previous = inputDedup.get(input.clientInputSeq);

  if (previous) {
    sendInputAck(input.clientInputSeq, "duplicate");
    return;
  }

  const bytes = authoritativeKeyEncoder.encode(input);

  // 记录“准备写入”不能带来 exactly-once；
  // PTY 本身无法参与事务。
  const written = writePty(bytes);

  if (!written) {
    sendInputAck(input.clientInputSeq, "rejected");
    return;
  }

  inputDedup.set(input.clientInputSeq, "written");
  sendInputAck(input.clientInputSeq, "written");
}
```

这种场景最多做到：

```text
正常断线：dedup + ACK
Host crash boundary：at-most-once 优先
```

---

# 十四、warm reconnect 根本不需要 snapshot

浏览器网络断开，但页面和客户端 `GhosttyTerminal` 都还活着：

```text
client:
  epoch         = 7
  lastEventSeq  = 5000
  nextPtyOffset = 800000
```

重新 attach：

```text
ATTACH
  hasLiveReplica  = true
  lastEpoch       = 7
  lastEventSeq    = 5000
  nextPtyOffset   = 800000
```

Host journal 仍然保存：

```text
event 5001..5200
```

那么直接：

```text
PTY_OUTPUT 5001
PTY_OUTPUT 5002
RESIZE     5003
...
```

不需要 snapshot。

只有这些情况才 snapshot：

```text
客户端页面刷新，terminal 已丢失
journal 已经淘汰 event 5001
engine build 不匹配
session epoch 改变
客户端主动要求完整重置
客户端因为太慢被标记 desynced
```

所以大多数短暂移动网络断线：

```text
reconnect + tail
```

成本很低。

---

# 十五、`sessionEpoch` 什么时候必须变化

以下情况保持 epoch：

```text
Browser 断线
DO connection 重建
Agent 到 DO 的 WebSocket 重建
客户端切换 Wi-Fi
```

以下情况应改变 epoch：

```text
PTY 被重新创建
shell/process 被重新启动
Host 无法证明 output journal 连续
authoritative libghostty 被重新初始化且没有可靠 snapshot+tail
Agent crash 导致输入/输出边界无法确定
```

Host 创建 Cloud session 时必须显式提交一个随机、非零的 u64 `sessionEpoch`。Cloud API 不提供默认值；缺失字段直接返回 400，避免两个 PTY 生命周期意外复用相同 epoch。

客户端看到 epoch 改变：

```text
立即销毁旧 replica
清空所有旧 tail
请求新 snapshot
```

不能尝试把新 session 的 output 接在旧 terminal 上。

---

# 十六、单个 authoritative terminal 的完整 actor 示例

```ts
class TerminalSession {
  readonly sessionId: string;
  readonly sessionEpoch: bigint;

  eventSeq = 0n;
  nextPtyOffset = 0n;

  terminal: GhosttyTerminal;
  journal = new EventJournal();
  clients = new Map<string, ClientState>();

  private snapshotBarrier = false;
  private ingress: IngressEvent[] = [];

  onPtyData(data: Uint8Array): void {
    if (this.snapshotBarrier) {
      this.ingress.push({ type: "pty-output", data: data.slice() });
      return;
    }

    this.commitPtyData(data);
  }

  private commitPtyData(data: Uint8Array): void {
    const startOffset = this.nextPtyOffset;

    ghosttyTerminalVtWrite(this.terminal, data);

    const eventSeq = ++this.eventSeq;
    this.nextPtyOffset += BigInt(data.length);

    const event = {
      type: "pty-output" as const,
      eventSeq,
      startOffset,
      data: data.slice(),
    };

    this.journal.append(event);
    this.broadcastReplaceable(event);
  }

  onResizeRequest(request: ResizeRequest): void {
    if (!this.validateWriterLease(request.writerLease)) {
      this.rejectInput(request.clientInputSeq);
      return;
    }

    if (this.snapshotBarrier) {
      this.ingress.push({ type: "resize", request });
      return;
    }

    this.commitResize(request);
  }

  private commitResize(request: ResizeRequest): void {
    // 具体 API 名称省略；这两个动作属于一个 session event。
    resizeGhosttyTerminal(
      this.terminal,
      request.cols,
      request.rows,
      request.widthPx,
      request.heightPx,
    );

    resizePty(
      request.cols,
      request.rows,
      request.widthPx,
      request.heightPx,
    );

    const eventSeq = ++this.eventSeq;

    const event = {
      type: "resize-applied" as const,
      eventSeq,
      cols: request.cols,
      rows: request.rows,
      widthPx: request.widthPx,
      heightPx: request.heightPx,
    };

    this.journal.append(event);
    this.broadcastReplaceable(event);
    this.ackInput(request.clientInputSeq);
  }

  takeSnapshot(): SnapshotArtifact {
    this.snapshotBarrier = true;

    const cutEventSeq = this.eventSeq;
    const nextPtyOffset = this.nextPtyOffset;

    try {
      const bytes = ghosttySnapshotEncodeAlloc(this.terminal);

      return {
        snapshotId: crypto.randomUUID(),
        sessionEpoch: this.sessionEpoch,
        cutEventSeq,
        nextPtyOffset,
        engineId: ENGINE_ID,
        bytes,
      };
    } finally {
      this.snapshotBarrier = false;
      this.drainIngress();
    }
  }

  private drainIngress(): void {
    const queued = this.ingress;
    this.ingress = [];

    for (const event of queued) {
      switch (event.type) {
        case "pty-output":
          this.commitPtyData(event.data);
          break;
        case "resize":
          this.commitResize(event.request);
          break;
        case "key":
          this.commitKey(event.request);
          break;
      }
    }
  }
}
```

这只是第一版。它会让输入在 snapshot encode 的几毫秒内排队。

---

# 十七、完全不阻塞 live terminal 的双实例版本

需要极低输入延迟时，可以维护两个相同实例：

```text
PTY bytes ───────┬────▶ T_live
                 │
                 └────▶ T_checkpoint
```

两者输入相同，但：

```text
T_live:
  effects 开启
  WRITE_PTY 开启
  真正权威实例

T_checkpoint:
  所有效果关闭
  永远不写 PTY
  只用于 snapshot
```

snapshot：

```text
1. T_checkpoint 停在 event R
2. 后续事件继续应用到 T_live
3. 后续事件保存到 checkpointTail
4. worker encode T_checkpoint
5. snapshot 完成
6. checkpointTail replay 到 T_checkpoint
7. T_checkpoint 再次追上 T_live
```

伪代码：

```ts
class DualTerminalSession {
  live: GhosttyTerminal;
  checkpoint: GhosttyTerminal;

  checkpointFrozen = false;
  checkpointTail: TerminalEvent[] = [];

  commit(event: TerminalEvent): void {
    applyEvent(this.live, event);

    if (this.checkpointFrozen) {
      this.checkpointTail.push(cloneEvent(event));
    } else {
      applyEvent(this.checkpoint, event);
    }

    appendJournal(event);
    broadcast(event);
  }

  async takeSnapshot(): Promise<SnapshotArtifact> {
    this.checkpointFrozen = true;

    const cutEventSeq = this.eventSeq;
    const nextPtyOffset = this.nextPtyOffset;

    try {
      const bytes = await encodeSnapshotOnWorker(this.checkpoint);

      return {
        cutEventSeq,
        nextPtyOffset,
        bytes,
      };
    } finally {
      for (const event of this.checkpointTail) {
        applyEvent(this.checkpoint, event);
      }

      this.checkpointTail = [];
      this.checkpointFrozen = false;
    }
  }
}
```

优点：

```text
snapshot 不阻塞输入
snapshot 不阻塞 Host PTY parsing
```

代价：

```text
两份 screen + scrollback 内存
两次 VT parsing
```

注意这不是 tmux 那种双重串行解析：

```text
parse -> 重新生成 VT -> parse
```

而是相同输入并行喂两个同构状态机。

---

# 十八、客户端 snapshot 恢复代码

严格 history 模式：

```c
typedef struct {
  uint64_t cut_event_seq;
  uint64_t next_pty_offset;
  GhosttyTerminal terminal;
} RestoredReplica;

static RestoredReplica restore_snapshot_exact(
    const uint8_t* bytes,
    size_t len,
    uint64_t cut_event_seq,
    uint64_t next_pty_offset
) {
  GhosttySnapshotDecoder decoder = NULL;
  GhosttyTerminal terminal = NULL;

  assert(
      ghostty_snapshot_decoder_new_buf(
          NULL,
          &decoder,
          bytes,
          len) == GHOSTTY_SUCCESS);

  // 先到 READY；此时已经可以渲染。
  assert(
      ghostty_snapshot_decoder_ready(
          decoder,
          &terminal) == GHOSTTY_SUCCESS);

  render_terminal(terminal);

  // 为完整 scrollback，暂时不应用 live tail。
  GhosttyResult result;
  while (
      (result = ghostty_snapshot_decoder_next(decoder))
      == GHOSTTY_SUCCESS
  ) {
    render_terminal(terminal);
  }

  assert(result == GHOSTTY_NO_VALUE);

  ghostty_snapshot_decoder_free(decoder);

  return (RestoredReplica) {
    .cut_event_seq = cut_event_seq,
    .next_pty_offset = next_pty_offset,
    .terminal = terminal,
  };
}
```

优先 live 模式：

```c
assert(
    ghostty_snapshot_decoder_ready(
        decoder,
        &terminal) == GHOSTTY_SUCCESS);

install_replica(
    terminal,
    cut_event_seq,
    next_pty_offset);

// 立即应用缓存 tail。
apply_buffered_live_tail();

// 后台逐页补 history；允许某些 PAGE 因当前 terminal 已变化而无法插入。
while (
    ghostty_snapshot_decoder_next(decoder)
    == GHOSTTY_SUCCESS
) {
  maybe_refresh_scrollback();
}
```

---

# 十九、一次完整断线恢复时序

```text
Client                    Worker / Durable Object              Host Agent / R2
  │                                  │                                  │
  │── ATTACH live@8000 ─────────────▶│── attach-request ───────────────▶│
  │                                  │                                  │
  │                                  │◀─ replay-unavailable ───────────│
  │◀─ RESYNC_REQUIRED ───────────────│   gap: 8001..8499 missing        │
  │                                  │                                  │
  │                                  │◀─ PUT snapshot@9000 to R2 ─────│
  │                                  │   blob-first + finalize pointer  │
  │                                  │                                  │
  │                                  │◀─ SNAPSHOT_OFFER commit@9002 ──│
  │                                  │   seed browser cursor@9000       │
  │◀─ SNAPSHOT_MANIFEST ─────────────│                                  │
  │                                  │── SNAPSHOT_READY_ACK ──────────▶│
  │                                  │                                  │
  │── authenticated HTTP GET ───────▶│── R2 GET stream ───────────────▶│
  │◀─ snapshot body stream ──────────│◀────────────────────────────────│
  │◀─ directed event 9001（先缓存）──│◀─ after ACK: tail 9001 ─────────│
  │◀─ directed event 9002（先缓存）──│◀─ tail 9002 + ReplayCommit ─────│
  │                                  │                                  │
  │ decode to READY                  │                                  │
  │ install terminal@9000            │                                  │
  │ apply 9001, 9002                 │                                  │
  │── ACK event=9002 ───────────────▶│                                  │
```

---

# 二十、这个设计仍然没有解决什么

## Snapshot 不保存 shell 进程

它保存：

```text
terminal emulator state
```

不保存：

```text
shell address space
open files
PTY fd
child process state
```

如果 Host Agent 就是 PTY owner，Agent 崩溃后 PTY 可能一起消失。

更稳的 Host 结构应当是：

```text
pty-session-daemon
  真正持有 PTY 和 child process

relay-agent
  连接 DO
  管理网络复制
```

relay-agent 重启时可以重新连接本地 daemon。

## 不能保证跨 Agent crash 的 exactly-once 输入

PTY 没有事务接口，这一点只能选择安全语义，不能靠 snapshot 消除。

## Snapshot v1 还不能作为长期稳定存储 ABI

当前格式仍明确没有 v1 binary compatibility guarantee。

每个 session 至少固定：

```text
engineId
  = libghostty commit
  + build options
  + snapshot schema digest
  + Unicode/width policy
```

第一版直接要求：

```text
client.engineId == host.engineId
```

不要做“应该兼容”的猜测。

## Kitty graphics 等大资源最好独立传输

终端 snapshot 保存状态，图片 payload 更适合：

```text
content-addressed resource:
  sha256 -> bytes

snapshot:
  image metadata / placement / hash

resource channel:
  按需传真实 payload
```

否则一个图片密集 session 会让每次 snapshot 都非常大。

---

# 最小可实现版本

第一版可以只实现：

```text
1. Host 一个 authoritative libghostty Terminal
2. 一 session 一个单线程 actor
3. PTY_OUTPUT + RESIZE 全序
4. 每客户端 512 KiB 有界 output queue
5. warm reconnect 优先 journal tail
6. gap 或慢客户端使用 snapshot + tail
7. snapshot encode 到内存，不直接写网络
8. 输入走独立 control WebSocket
9. snapshot/output 走 data WebSocket
10. 客户端 WRITE_PTY callback 永远关闭
11. catch-up 阶段抑制 BEL/notification/clipboard
12. engineId 精确匹配
```

这已经可以得到一个非常接近理想状态的性质：

```text
正常网络：
  raw PTY 延迟，和普通 SSH 类似

短暂断线：
  补 journal tail

客户端严重落后：
  丢弃中间状态，snapshot 跳到当前

页面刷新：
  snapshot 恢复完整 libghostty state

半截 ESC / UTF-8：
  continuation 精确恢复

Ctrl-C：
  独立 control channel，不排在历史输出之后
```

真正最需要实验验证的不是 terminal correctness 了，而是三个工程参数：

```text
snapshot encode 锁定时间
snapshot 常见/极端大小
READY 之后 history 与 live tail 的产品取舍
```

从协议角度看，这套 `snapshot@R + ordered PTY/resize tail` 已经足够形成一个可实现、可证明不会重复或漏接终端状态的基础版本。

我有一个问题不太清楚，snapshot什么时候会构建，会在哪一端构建

**结论很明确：**

> **Snapshot 应当在主机端构建。**
> 更准确地说，是在持有 PTY 和 authoritative libghostty `Terminal` 的 Host Agent 上构建。
> Durable Object 负责触发、索引、缓存位置和转发；客户端负责解码，不负责生成权威 snapshot。

```text
Host Agent                    Durable Object                    Client
──────────                    ──────────────                    ──────
PTY owner
authoritative libghostty
     │
     ├── build Snapshot@R ───────▶ 保存 metadata / blob pointer
     │                           │
     ├── event tail R+1... ─────▶│──────── Snapshot@R ─────────▶ decode
     │                           │──────── tail R+1... ─────────▶ apply
```

## 为什么一定是在 Host 构建

只有 Host 同时掌握：

```text
PTY 输出的完整顺序
resize 与输出的相对顺序
authoritative libghostty state
未完成的 VT / UTF-8 parser continuation
终端向 PTY 写回的 query response
session epoch
```

客户端可能已经落后或断线；Durable Object 则根本没有 PTY 和完整 terminal state。

当前 libghostty snapshot 的 `READY` 前缀已经包含可渲染终端状态以及未完成的 parser continuation，较老的 scrollback 放在 `READY` 后面。客户端解码到 `READY` 后，就可以渲染并继续接收新的 PTY 数据。

另外，当前 snapshot encoder 要求编码期间禁止对同一个 terminal 做并发 VT write、resize 或其他 mutation，所以 snapshot 必须在能够控制 authoritative terminal 生命周期的一端生成。

---

# Snapshot 不是每次有输出都构建

正常运行时应该完全不构建 snapshot：

```text
PTY output
   ↓
Host libghostty
   ↓
PTY_DATA frame
   ↓
Client libghostty
```

Snapshot 只是一个 **重新建立复制基线的 checkpoint**。

常见触发条件如下：

| 场景                                |                   是否需要新建 Snapshot |
| --------------------------------- | --------------------------------: |
| 正常实时连接                            |                               不需要 |
| 短暂断线，客户端 terminal 还活着，journal 也完整 |                       不需要，只补 tail |
| 页面刷新、新设备、新客户端加入                   |        需要 snapshot，或复用已有 snapshot |
| 客户端落后太多，output queue 超限           |                需要 snapshot，跳过中间输出 |
| reconnect 时 journal 已经淘汰缺失事件      |                       需要 snapshot |
| 现有 snapshot 太旧，后续 tail 太大         |                            建议构建新的 |
| 为 relay-agent 重启保存 terminal state |                            可周期性构建 |
| PTY 本身被重新创建                       | 旧 snapshot 通常失效，session epoch 应改变 |

因此它不是：

```text
每隔一帧 snapshot
```

也不是：

```text
每次 WebSocket 断开立即 snapshot
```

而是：

```text
正常走 event tail
无法经济地补 tail 时才切换到 snapshot
```

---

# 三种具体情况

## 情况一：热重连，不构建 Snapshot

客户端断线前：

```text
Client:
  epoch = 7
  lastEventSeq = 1000
  nextPtyOffset = 50000
```

Host 当前：

```text
Host:
  epoch = 7
  headEventSeq = 1030
  journal contains 1001..1030
```

重连后：

```text
Host → Client:
  event 1001
  event 1002
  ...
  event 1030
```

客户端原来的 libghostty terminal 还在，所以只补缺失事件。

```text
Snapshot：不需要
```

这应该是 Wi-Fi 切换、短暂休眠、移动网络抖动时的主要路径。

---

## 情况二：新客户端加入，复用已有 Snapshot

Host 已经有：

```text
Snapshot S17:
  cutEventSeq = 900
  nextPtyOffset = 44000
```

当前 session：

```text
headEventSeq = 1030
```

新客户端加入：

```text
Host/DO → Client:
  Snapshot S17 @ event 900
  event 901
  event 902
  ...
  event 1030
```

不需要为了每个客户端重新编码一次。

Snapshot 是：

> **session checkpoint，不是 client checkpoint。**

同一份 snapshot 可以同时服务：

```text
新打开的桌面客户端
浏览器刷新后的客户端
observer
协作客户端
```

只要它们使用相同的：

```text
session epoch
libghostty engine build
terminal profile
```

---

## 情况三：已有 Snapshot 太旧，构建新的

已有：

```text
Snapshot @ event 900
```

但 Host 已经到了：

```text
headEventSeq = 500000
tail = 200 MiB
```

这时发送：

```text
Snapshot@900 + 200 MiB tail
```

已经失去 snapshot 的意义。

Host 应当新建：

```text
Snapshot@500000
```

然后只发送很短的 tail：

```text
Snapshot@500000
event 500001...
```

因此 snapshot 刷新通常由这些阈值决定：

```text
snapshot 之后累计的 PTY bytes
snapshot 之后累计的 event 数
snapshot 年龄
客户端 attach / resync 需求
journal 即将压缩或淘汰
```

具体阈值需要压测。概念上可以先从：

```text
tail 超过数 MiB
或
snapshot 已经数十秒未更新且当前有 attach 需求
```

开始实验，但不应把这些数字当作协议保证。

---

# 推荐使用“混合策略”

纯按需生成：

```text
新客户端来了
    ↓
这时才 build snapshot
```

实现最简单，但新客户端必须等待 encode。

纯周期生成：

```text
每 5 秒生成一次
```

可能浪费 CPU 和内存，特别是无人连接的 session。

更合适的是：

```text
平时保留一个 latest snapshot
           +
没有 snapshot 时按需生成
           +
snapshot 过旧时在空闲期刷新
           +
journal 即将超过阈值时刷新
```

例如：

```text
Session:
  latestSnapshot = S@10000
  journal = events 10001..12000
```

新客户端加入：

```text
tail 只有 2000 events
→ 直接复用 S@10000
```

如果变成：

```text
journal = events 10001..900000
```

则：

```text
→ 后台生成 S@900000
→ 原子替换 latestSnapshot
```

旧 snapshot 不能仅因 latest 更新就立即删除，因为可能仍被 data attachment pin 住，或处于 finalize 到 marker 之间的 recent grace 窗口。

```text
S_old
  保留到所有引用它的 data socket reset/close
  ReplayCommit 后仍保留 pin

S_new
  成为新的 latestSnapshot
```

---

# Snapshot 的精确构建时刻

构建 Snapshot 时需要定义一个切点：

```text
R = cutEventSeq
P = nextPtyOffset
```

Snapshot 表示：

```text
所有 eventSeq <= R 的 terminal state

所有 PTY bytes [0, P) 已经包含在该状态中
```

Snapshot 之后发送：

```text
eventSeq > R
PTY offset >= P
```

例如：

```text
event 100: PTY_OUTPUT offset 4000..4100
event 101: RESIZE 120x40
event 102: PTY_OUTPUT offset 4100..4300
```

在 event 101 后切 snapshot：

```text
Snapshot:
  cutEventSeq = 101
  nextPtyOffset = 4100
  terminal size = 120x40

Tail:
  event 102
  PTY offset 4100..4300
```

客户端：

```text
decode Snapshot@101
apply event 102
```

---

# 单实例 Host 怎么构建

最简单的第一版：

```text
1. 暂停向 authoritative libghostty 应用新 mutation
2. 记录 cutEventSeq 和 nextPtyOffset
3. encode snapshot 到本地内存
4. 恢复处理
5. 把暂停期间的事件作为 tail
```

注意：

> 可以继续从 PTY fd 读取数据，防止 PTY kernel buffer 堵塞，但暂时放进 ingress queue，不要在 snapshot 过程中写入 libghostty。

```text
                         snapshot barrier
                               │
PTY read ───────────────▶ ingress queue
                               │
                               ▼
                    authoritative libghostty
```

伪代码：

```ts
function takeSnapshot(session: Session): SnapshotArtifact {
  session.snapshotBarrier = true;

  const cutEventSeq = session.eventSeq;
  const nextPtyOffset = session.nextPtyOffset;

  try {
    // 只写本地内存，绝不能直接把 writer 接到慢速网络。
    const bytes = ghosttySnapshotEncodeAlloc(session.terminal);

    return {
      sessionEpoch: session.epoch,
      cutEventSeq,
      nextPtyOffset,
      engineId: ENGINE_ID,
      bytes,
    };
  } finally {
    session.snapshotBarrier = false;
    session.drainQueuedEvents();
  }
}
```

错误做法：

```text
锁住 terminal
    ↓
snapshot encoder
    ↓
writer 直接发送 WebSocket
```

如果客户端很慢，terminal mutation 就会被整个网络传输时间阻塞。

正确做法：

```text
encode 到本地 buffer
    ↓
解除 terminal barrier
    ↓
异步压缩、上传或发送
```

---

# 生产版更适合双实例

如果 snapshot 编码会明显影响输入延迟，可以在 Host 维护两个同步的 libghostty instance：

```text
                   ┌────▶ T_live
PTY output ────────┤
                   └────▶ T_checkpoint
```

其中：

```text
T_live
  真正 authoritative
  处理 query response 和 effects
  始终保持实时

T_checkpoint
  effects 全部关闭
  只用于 snapshot
```

生成 snapshot 时：

```text
1. T_checkpoint 停在 revision R
2. T_live 继续运行
3. R 之后的事件进入 checkpointTail
4. worker 对 T_checkpoint 编码 snapshot
5. 编码完成
6. checkpointTail replay 到 T_checkpoint
7. T_checkpoint 重新追上 T_live
```

```text
                snapshot encode
                      │
T_checkpoint@R ───────┤
                      │
T_live@R+1 ───────────┼──▶ continues
T_live@R+2 ───────────┤
                      │
                encode complete
                      │
               replay R+1, R+2
                      ▼
             T_checkpoint catches up
```

这样 snapshot 可以完全不阻塞 live PTY 和用户输入。

代价是：

```text
两份 terminal state
两份 scrollback
PTY bytes 解析两次
```

但这是同一输入的并行状态机复制，不是 tmux 那种串行的：

```text
parse → 重新生成 VT → 再 parse
```

---

# Durable Object 到底做什么

DO 不构建 snapshot，因为它没有 authoritative terminal。

它保存：

```ts
interface SnapshotMetadata {
  sessionEpoch: bigint;
  snapshotId: string;
  cutEventSeq: bigint;
  nextPtyOffset: bigint;

  engineId: string;
  compression: "zstd";
  length: bigint;

  objectKey?: string;
  hostAgentId: string;
}
```

DO 的职责：

```text
客户端 attach
    ↓
检查是否能 tail-resume
    ↓
如果不能：
    ├── 已有合适 snapshot → 返回 snapshot + tail
    └── 没有合适 snapshot → 请求 Host Agent 构建
```

Snapshot bytes 可以放在：

```text
Host 内存
Host 本地磁盘
对象存储
独立 blob 服务
```

DO 最好只保存 pointer 和 metadata，而不是无限把大型 snapshot 塞进自身存储。

---

# 客户端能不能自己构建 Snapshot

可以，但只能作为**本地缓存优化**，不能成为权威来源。

例如桌面客户端或浏览器可以把自己的 replica snapshot 保存到：

```text
本地磁盘
IndexedDB
```

页面刷新后：

```text
1. 从本地 snapshot 恢复
2. 告诉 Host：
   epoch=7
   restoredEventSeq=1000
3. Host 验证：
   epoch 匹配
   engineId 匹配
   journal 仍覆盖 1001..head
4. 补 tail
```

如果 Host 无法验证：

```text
客户端本地 snapshot 无效
→ 使用 Host snapshot
```

客户端 snapshot 不能自行声称：

```text
“我现在就是 session 的正确状态”
```

因为它可能：

* 少了一段 PTY output；
* 少了 resize；
* 在旧 epoch；
* 使用了不同 libghostty build；
* 在错误 parser continuation 上；
* 对终端 query 使用了不同响应。

所以：

```text
Host snapshot = authoritative checkpoint
Client snapshot = speculative cache
```

第一版完全可以不做客户端 snapshot。

---

# Session 创建时是否立即构建

不一定。

## 第一个客户端从 session 开始就在线

```text
PTY 创建
Client 同时连接
从初始空 terminal 开始接收所有事件
```

不需要 snapshot。

## Session 允许后台运行，之后才 attach

这时需要：

```text
attach 时按需构建
```

或者：

```text
Host 提前维护 rolling snapshot
```

## 需要 relay-agent 崩溃恢复

那么即使没有客户端，也可能需要周期性落盘：

```text
Snapshot@R
+
本地 PTY output journal after R
```

但它只能恢复 terminal emulator state，不能仅凭 snapshot 恢复 shell 进程。真正的 PTY 和 child process 仍需由独立的 session daemon 持有。

---

# 我建议的最终策略

```text
Snapshot 构建端：
  Host Agent 或 Host 上的 checkpoint worker

Snapshot 使用端：
  Client libghostty decoder

Snapshot 协调端：
  Durable Object

构建触发：
  1. 第一次 cold attach 且没有缓存
  2. journal gap
  3. 客户端严重落后
  4. latest snapshot 后的 tail 超过阈值
  5. 可选的空闲期 checkpoint
  6. journal 压缩前
```

完整路径：

```text
Host authoritative terminal
          │
          ├── Snapshot@R
          │       │
          │       └── immutable reusable blob
          │
          └── ordered tail R+1...
                    │
                    ▼
             Durable Object
                    │
                    ▼
                 Client
          decode snapshot to READY
          apply tail
          enter live mode
```

所以最重要的一句话是：

> **Snapshot 不属于某次连接，它属于 Host 上的 terminal session。它由 Host 在某个确定 revision 上构建，可被多个客户端复用；连接恢复时优先补 tail，只有 tail 不存在或不值得补时才使用或刷新 snapshot。**
