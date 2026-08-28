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
  | (Common & {
      type: "input-ack";
      clientInputSeq: U64;
      status: "written" | "duplicate" | "rejected" | "uncertain";
    })
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
      hasLiveReplica: false;
    }
  | {
      type: "attach";
      engineId: string;

      // 三个 cursor 来自发送 attach 这一刻的 active libghostty replica。
      hasLiveReplica: true;
      lastSessionEpoch: U64;
      lastEventSeq: U64;
      nextPtyOffset: U64;
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
      type: "paste";
      writerLease: string;
      inputEpoch: string;
      clientInputSeq: U64;
      data: Uint8Array;
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

`ack` 只表示该 delivery generation 的 frame 已可靠到达，用于 512 KiB credit 计算，不代表 libghostty 已应用。ACK cursor 只保存在当前 data WebSocket 的 hibernation attachment 中，不写入 SQL，也不作为下一 connection set 的单调下界；full reconnect 必须以新 `attach` 的 live replica cursor 重新建立 credit baseline。Host 重连或浏览器 data 断线时，DO 会 bump generation、关闭旧 data、签发同一 connection set 的 30 秒单次 data ticket，并发送一个 `resync-required`。浏览器建立新 data 后，必须从当前 active replica 读取 cursor 并重新 `attach`；DO 不缓存 cursor 来自动 replay，从而消除“frame 已 apply、progress 尚未上报”的重复应用窗口。

每个 delivery generation 只允许一次 `attach`，且只允许 `awaiting-attach -> active`。重复 attach 不得重写 baseline 或 snapshot pin，只隔离该 browser connection set；writer lease 的重新获取不能借重复 attach 偷渡。`welcome` 只确认身份、generation 和 session head，本身不启动 directed delivery。

`hasLiveReplica: false` 不提供 delivery baseline；`hasLiveReplica: true` 只采样完整三元 cursor。两者在 DO 接受同 Host data WebSocket 上的 `DeliveryBarrier` 前都不允许 directed mutation 或 `ReplayCommit`。warm barrier 以 attach cursor 为 base 并向 browser control 发送 `replay-start {streamId, generation, base, pinnedCommit}`；snapshot barrier 可以在尚未 pin/尚未发送 start 时覆盖 live attach baseline，以 finalize 后的 snapshot cut seed attachment 并发送 manifest。

barrier 接受后，directed mutation 的上界和 `ReplayCommit` 的精确终点都使用 attachment 中不可变的 pinned commit，不再读取会继续变化的 session head。barrier 到 commit 之间出现 canonical frame、commit 不等于 pin、或当前 generation 的 delivery 被另一 barrier 冲突覆盖，都是 Host data 顺序违约并 fail closed。MVP 不接受 Host 发来的 directed `Reset`；合法 reset 只能走 DO 的 generation bump、关闭旧 data、签发 replacement ticket、control `resync-required` 这一条显式路径。

connection set 遵循 `reserved -> control-open/data-open -> paired -> ready -> replaced/closed`。每次转换都同时校验 `{peer, connectionSetId, connectionId, hostFence | (clientId, deliveryGeneration)}`。未消费 reservation 与已注册 browser identity 共用 16-client 原子 quota；同一 browser identity 或 Host subject 最多保留一个 pending set，新签会撤销旧票。quota 满时只按 LRU 回收同时满足“无 open/hibernating socket、无 pending ticket、无有效 writer lease”的断连 identity。这样“只签票不连 WS”和“批量签完再消费”都不能绕过限额，也不会把 16 变成 session 终身 clientId 上限。

Hibernation socket 的 Error 事件直接执行同一套 fenced close lifecycle，不能假设 workerd 随后还会派发 Close。Host data error 会使当前 pair 下线；browser data error 会 bump generation 并签发 replacement ticket。后到的 Close 只会命中已推进的 fence/generation 并被幂等忽略。

Host 的 control/data 是两条独立 WebSocket，不存在跨通道顺序保证。Host 发送 `host-ready` 后必须等待 relay 在同一 control socket 返回 `host-ready-ack { sessionEpoch, headEventSeq, nextPtyOffset }`，再放行 data；DO 只在校验并提交 session head、将当前 fenced pair 切到 ready、完成现有 browser delivery reset 后返回 ACK。当前 fenced Host 在 barrier 前发送 data 会 fail closed，不能静默丢弃或在 DO 内隐式缓存。

入站消息在进入串行处理队列前完成 channel/type/size 校验。队列全局限制为 2048 条/32 MiB；Host data 单 socket 为 1024 条/16 MiB，browser control 为 8 条/16 MiB。Host 超限会 fail 当前 fenced pair，browser 超限只隔离对应客户端。

## Snapshot blob 使用私有 R2 HTTP，不进入 WebSocket 或 DO

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

请求必须带规范的 `Content-Length`，且精确等于 `x-zhongduan-compressed-length`；不接受 chunked/缺失长度上传。Worker 将 request body 的 `ReadableStream` 直接传给 R2，并把 Host 声明的 SHA-256 交给 R2 校验，不在 Worker 或 Durable Object 中聚合 blob。R2 object key 是 immutable 的，写入使用 `etagDoesNotMatch: "*"`。流程固定为：

```text
R2 conditional put 成功
  -> 校验 size/checksum/httpMetadata/customMetadata
  -> DO SQLite transaction 幂等 finalize metadata
  -> 更新 latest_snapshot_id
```

finalize 失败时 object 只是私有 orphan，没有 DO serving pointer，因此浏览器不可见。乱序 finalize 不允许 latest pointer 回退；比较顺序是 `cutEventSeq`、`nextPtyOffset`、`createdAt`、`snapshotId`，前两个必须用 `BigInt`，不能依赖 SQLite TEXT 字典序。

Host、writer、observer 使用各自 capability 下载：

```http
GET /api/v1/sessions/:sessionId/snapshots/:snapshotId
```

Worker 先从 DO 读取已发布 pointer，再从 R2 读取 object。version、etag、size、checksum、HTTP metadata 或 custom metadata 任一不匹配时，先 cancel R2 body，再返回 503；绝不能返回部分对象。成功响应把 R2 `ReadableStream` 直接返回，并固定 `Cache-Control: private, no-store` 与 `X-Content-Type-Options: nosniff`。

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

最简单的第一版可以短暂停止 terminal mutation。

```c
typedef struct {
  uint64_t session_epoch;
  uint64_t event_seq;
  uint64_t pty_offset;

  GhosttyTerminal terminal;

  bool snapshot_barrier;
  EventQueue ingress_queue;
  Journal journal;
} Session;
```

正常 PTY 输出：

```c
static void commit_pty_output(
    Session* session,
    const uint8_t* bytes,
    size_t len
) {
  assert(!session->snapshot_barrier);

  const uint64_t start_offset = session->pty_offset;

  // Host authoritative terminal 先应用。
  ghostty_terminal_vt_write(session->terminal, bytes, len);

  const uint64_t event_seq = ++session->event_seq;
  session->pty_offset += len;

  journal_append_pty_output(
      &session->journal,
      event_seq,
      start_offset,
      bytes,
      len);

  publish_pty_output(
      session->session_epoch,
      event_seq,
      start_offset,
      bytes,
      len);
}
```

生成 snapshot：

```c
typedef struct {
  uint64_t cut_event_seq;
  uint64_t next_pty_offset;
  uint8_t* bytes;
  size_t len;
} Snapshot;

static Snapshot take_snapshot(Session* session) {
  // 此 barrier 期间：
  // - PTY reader 可以继续从 fd 读，防止 kernel PTY buffer 填满；
  // - 但读出的数据只能放 ingress_queue，不能写 terminal；
  // - 用户输入和 resize 也排队。
  session->snapshot_barrier = true;

  Snapshot result = {
    .cut_event_seq = session->event_seq,
    .next_pty_offset = session->pty_offset,
    .bytes = NULL,
    .len = 0,
  };

  GhosttyResult rc = ghostty_snapshot_encode_alloc(
      session->terminal,
      NULL,
      &result.bytes,
      &result.len);

  if (rc != GHOSTTY_SUCCESS) {
    session->snapshot_barrier = false;
    abort_snapshot(rc);
  }

  session->snapshot_barrier = false;

  // 现在再按 ingress_queue 的全序继续处理事件。
  drain_ingress_queue(session);

  return result;
}
```

这与当前 API 的约束一致：snapshot encode 期间调用方必须阻止 concurrent VT write、resize 和其他 terminal mutation，并且 writer 是同步调用。

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
同一 Agent epoch：
  如果 Host 仍有 dedup 记录，重发 seq=77 只返回 duplicate ACK

Agent epoch 已变化：
  未确认输入返回 uncertain
  客户端不自动重发
```

伪代码：

```ts
function handleInput(input: ClientInput): void {
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

旧 snapshot 不能立即删除，因为可能仍有客户端正在下载它。

```text
S_old
  保留到所有引用它的 delivery generation 结束

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
