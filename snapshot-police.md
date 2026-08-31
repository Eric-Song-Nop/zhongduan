> 历史评审草稿：本文保留早期 snapshot 方案的审查记录，其中部分前提已经失效。当前项目
> 入口和实现状态以 [README](README.md)、[MVP 架构](docs/mvp-architecture.md)、
> [部署指南](docs/deployment.md)与 [Wire Protocol V2](docs/wire-protocol.md)为准；产品与恢复边界见
> [产品契约与协议边界](docs/terminal-protocol-architecture.md)，实施顺序见
> [MVP 路线](docs/mvp-roadmap.md)。本文中的后续方案没有路线约束力。

可以。先把两个容易混在一起的概念拆开：

> **Host 侧的 snapshot 构建暂停**，和 **wterm 客户端的 passive snapshot 恢复**是两件事。

* Host passive checkpoint：解决“生成 snapshot 时是否暂停权威 terminal”。
* wterm passive restore：解决“客户端恢复 snapshot 时是否卡住、闪屏、触发副作用”。

wterm 的 passive restore **不能减少 Host 的 snapshot encode 暂停时间**；它只能让客户端继续显示旧 terminal，同时在一个未挂载的新 core 中恢复，追上后原子替换。

下面给出一个可以直接作为 V1 默认值的收敛方案。

---

# 一、最终默认参数

| 参数                      |               V1 默认值 | 作用                           |
| ----------------------- | -------------------: | ---------------------------- |
| Journal segment         | `256 KiB` 或 `250 ms` | 任一达到就封段                      |
| Warm replay 时间          |               `60 s` | 短暂断线优先补 tail                 |
| Warm replay 字节上限        |              `8 MiB` | 防止 output spew 占满内存          |
| Snapshot 理想 tail        |            `≤ 2 MiB` | 可以直接复用已有 snapshot            |
| Snapshot 最大可服务 tail     |              `8 MiB` | 超过后 snapshot 退休              |
| 客户端发送队列                 |            `512 KiB` | 超过即停止 raw replay，转 resync    |
| Passive restore tail 缓冲 |              `2 MiB` | 超过则放弃旧 snapshot，重新取最新        |
| Passive restore 最长追赶    |                `5 s` | 防止客户端永远追不上                   |
| Snapshot idle debounce  |             `100 ms` | 尽量在输出暂歇时生成                   |
| Snapshot 软年龄            |               `30 s` | 仅在已有至少 `256 KiB` tail 时刷新    |
| Snapshot 最小构建间隔         |                `2 s` | 防止反复生成                       |
| Host encode P50 目标      |             `≤ 2 ms` | 正常单实例构建                      |
| Host encode P99 目标      |             `≤ 4 ms` | 可接受交互暂停                      |
| Host encode 迁移阈值        |          单次 `> 8 ms` | 后续切换 Host passive checkpoint |

这些不是协议常量，可以遥测后调整；但 V1 应该先固定下来，避免一开始就做过度自适应。

---

# 二、Journal 只保留两种用途

Journal 不应该无限为最慢客户端保留。它只服务：

1. **warm reconnect**：页面和客户端 libghostty core 仍然存在，只需要补缺失事件。
2. **snapshot tail**：客户端从某个 Snapshot `S@R` 恢复后，需要应用 `R+1..head`。

事件仍然只有：

```ts
type TerminalEvent =
  | {
      seq: bigint;
      type: "pty-output";
      startOffset: bigint;
      bytes: Uint8Array;
    }
  | {
      seq: bigint;
      type: "resize";
      cols: number;
      rows: number;
      widthPx: number;
      heightPx: number;
    };
```

## 保留边界

假设：

```text
warmFloor
  = 同时满足“最近 60 秒”和“最近 8 MiB”的最早事件

snapshotFloor
  = latestSnapshot.cutSeq + 1
```

实际保留边界：

```text
retainFloor = min(warmFloor, snapshotFloor)
```

意思是：

* snapshot 很旧时，为了让它仍然可用，需要保留更老的 tail；
* snapshot 很新时，仍额外保留最近 60 秒的旧事件，让已有 core 可以 tail-only reconnect；
* 没有可服务 snapshot 时，只保留 warm replay ring。

伪代码：

```ts
function journalRetainFloor(session: Session): bigint {
  const warmFloor = laterOf(
    session.journal.seqAtTime(Date.now() - 60_000),
    session.journal.seqAtRecentBytes(8 * MiB),
  );

  const snapshotFloor =
    session.latestSnapshot?.state === "servable"
      ? session.latestSnapshot.cutSeq + 1n
      : session.headSeq + 1n;

  return earlierOf(warmFloor, snapshotFloor);
}
```

## 客户端 ACK 不 pin Journal

这是重要约束：

```text
客户端 ACK
  只用于判断能否 warm replay
  只用于 backpressure
  不能阻止 journal 淘汰
```

一个离线三小时的客户端不能让 Host 保留三小时输出。

如果客户端请求的 seq 已经早于 `retainFloor`：

```text
tail-only resume 失败
→ snapshot resync
```

## Journal 放在 Host，不放在 Durable Object

最终建议：

```text
Host Agent
  authoritative journal
  snapshot producer
  PTY owner

Durable Object
  latest snapshot metadata
  client ACK
  delivery generation
  writer lease
  route/control
```

DO 可以有很小的转发队列，但不要承担每 session 数 MiB 的高吞吐 Journal。

---

# 三、Snapshot 不必始终保持“可服务”

这里应当避免一个陷阱：

> 不能为了确保永远存在 `snapshot + complete tail`，在 `yes`、编译日志或模型输出期间疯狂构建 snapshot。

Snapshot 状态定义成：

```ts
type SnapshotState =
  | "building"
  | "servable"
  | "retired";
```

一个 snapshot 只有在下面条件满足时才是 `servable`：

```text
Snapshot blob 完整构建成功
+
从 cutSeq + 1 到 head 的所有事件仍在 Journal
+
engineId / sessionEpoch 仍匹配
```

当：

```text
bytesAfterSnapshot > 8 MiB
```

不要立即疯狂重建，而是：

```text
snapshot.state = retired
journal 回到只保留 warm replay ring
```

之后：

* 没有冷连接需求：继续正常跑，不生成 snapshot；
* 出现 cold attach / resync：按需构建当前状态的新 snapshot；
* 输出进入 idle：机会性构建新 snapshot。

这样连续 output spew 不会造成：

```text
snapshot
snapshot
snapshot
snapshot
```

---

# 四、Snapshot 触发状态机

## 1. 强制按需触发

以下情况必须请求新 snapshot：

```text
cold attach 且没有 servable snapshot
journal gap
客户端页面刷新，没有 live replica
engineId 不匹配
当前 snapshot 已 retired
```

## 2. 机会性触发

满足任一条件，并且已经 idle `100 ms`：

```text
tail >= 2 MiB
```

或者：

```text
snapshot age >= 30 s
且 tail >= 256 KiB
```

这会把构建尽量移到 shell prompt、命令结束、TUI 暂停刷新等时刻。

## 3. 复用还是重建

Cold attach 时已有 snapshot：

### Tail ≤ 2 MiB

直接复用：

```text
snapshot + tail
```

### Tail 在 2–8 MiB

看 Host 的预测暂停：

```text
predicted encode pause <= 4 ms
  → 构建新 snapshot

predicted encode pause > 4 ms
  → 复用旧 snapshot + tail
```

### Tail > 8 MiB

旧 snapshot 已退休：

```text
必须构建新 snapshot
```

## 伪代码

```ts
function maybeRequestSnapshot(session: Session, reason: Reason): void {
  if (session.snapshotBuild) {
    session.snapshotBuild.needsNewerCut = true;
    return;
  }

  if (Date.now() - session.lastSnapshotStartedAt < 2_000) {
    return;
  }

  const snapshot = session.latestSnapshot;
  const tailBytes = snapshot
    ? session.journal.bytesAfter(snapshot.cutSeq)
    : Infinity;

  if (reason === "cold-attach" || reason === "journal-gap") {
    if (!snapshot || snapshot.state !== "servable") {
      startSnapshot(session);
      return;
    }

    if (tailBytes <= 2 * MiB) {
      serveExistingSnapshot(session, snapshot);
      return;
    }

    if (
      tailBytes <= 8 * MiB &&
      session.snapshotPauseEstimator.p99 <= 4
    ) {
      startSnapshot(session);
      return;
    }

    serveExistingSnapshot(session, snapshot);
    return;
  }

  const idleFor = Date.now() - session.lastOutputAt;

  if (
    idleFor >= 100 &&
    (
      tailBytes >= 2 * MiB ||
      (
        snapshot &&
        Date.now() - snapshot.createdAt >= 30_000 &&
        tailBytes >= 256 * KiB
      )
    )
  ) {
    startSnapshot(session);
  }
}
```

---

# 五、Host 侧暂停预算

当前 libghostty snapshot encoder 要求调用方在 encode 期间阻止同一 terminal 的并发 VT write、resize 和其他 mutation；writer 也是同步调用。

因此单实例构建时：

```text
进入 mutation fence
记录 cutSeq / ptyOffset
encode 到本地内存
退出 mutation fence
压缩
上传或发网络
```

绝不能：

```text
持有 terminal fence
→ snapshot writer 直接写 WebSocket
```

## 精确暂停范围

```ts
function buildSnapshot(session: Session): Snapshot {
  session.terminalMutationFence.enter();

  const cutSeq = session.headSeq;
  const nextPtyOffset = session.nextPtyOffset;
  const startedAt = performance.now();

  try {
    // 只写本地内存。
    const bytes = libghosttyEncodeSnapshot(session.terminal);

    return {
      cutSeq,
      nextPtyOffset,
      bytes,
      encodeMs: performance.now() - startedAt,
    };
  } finally {
    session.terminalMutationFence.leave();
    session.drainPendingTerminalEvents();
  }
}
```

在 fence 期间：

* PTY fd 仍应继续读取；
* 读到的 bytes 放入 ingress ring；
* resize 排队；
* semantic key input 最多排队几毫秒；
* compression、CRC 外层处理、对象存储上传全部在 fence 外。

## 暂停阈值

我会定成：

```text
P50 <= 2 ms
P99 <= 4 ms
单次 > 8 ms：判定单实例 snapshot 不再适合该 session
```

`8 ms` 是迁移阈值，不是可中断 deadline。当前 encode 通常不能在中途安全取消，所以第一次超时只能记录；后续改用 passive checkpoint。

入口队列可以预留：

```text
1 MiB 初始
8 MiB hard burst capacity
```

如果一次 snapshot 期间 ingress 超过 1 MiB，已经说明输出率或暂停时间过高，应立即把该 session 标成：

```text
snapshotMode = passive-checkpoint
```

---

# 六、Host passive checkpoint 什么时候启用

再强调一次，这和 wterm passive restore 不同。

默认只跑一个权威 terminal：

```text
PTY → T_live
```

如果 encode 暂停超预算，变成：

```text
               ┌──▶ T_live
PTY output ────┤
               └──▶ T_snapshot
```

其中：

```text
T_live
  权威实例
  effects/query response 开启
  永不因 snapshot 暂停

T_snapshot
  effects 全部关闭
  只用于 snapshot encode
```

生成 snapshot：

```text
T_snapshot 暂停在 R
T_live 继续处理
R 后事件进入 checkpointTail
encode T_snapshot
完成后 checkpointTail replay 到 T_snapshot
```

启用条件：

```text
最近 3 次 encode EWMA > 4 ms
或
任意一次 encode > 8 ms
或
snapshot ingress > 1 MiB
```

不建议默认双实例，因为它会：

* 双倍解析 PTY；
* 接近双倍 terminal/scrollback 内存；
* 增加状态一致性验证成本。

---

# 七、wterm 当前其实还没有这个 passive restore 接口

截至当前 `vercel-labs/wterm` main：

* `TerminalCore` 只有 `init`、`resize`、`writeRaw`、grid 和 scrollback 查询，没有 snapshot restore、已初始化 core adoption 或 dispose 接口。
* `WTerm` 虽然接受预构建的 `core`，但 `WTerm.init()` 随后会调用 `core.init(cols, rows)`，这会覆盖一个已经从 snapshot 恢复的 core；它也没有公开的 core swap 方法。
* WASM binding 已经暴露 `deinit(ptr)`，但当前 `GhosttyCore` 和 `WTerm.destroy()` 没有把它形成完整的 core ownership/disposal contract。
* 当前开放的 `@wterm/serialize` PR #36 是把状态编码成 ANSI stream 再 replay，并不是 libghostty binary snapshot，也不包含精确 parser continuation；它不应作为这个远程同步协议的基础。

所以需要给 wterm 增加一个真正的 **passive Ghostty restore API**。

---

# 八、wterm passive restore 的准确语义

我建议把 `passive` 定义为：

```text
未挂载到 DOM
不触发 RAF render
不调用 onData
不发送 terminal query response
不执行 bell / clipboard / notification
可以解码 snapshot
可以直接吃 PTY tail 和 resize
追到 commit watermark 后可以原子挂载
```

## 建议 API

```ts
export interface PassiveGhosttyRestore {
  readonly core: GhosttyCore;
  readonly phase: "ready" | "finish";

  /**
   * 恢复下一页 history。
   * 返回 false 表示 FINISH 已完成。
   */
  decodeNextHistory(): boolean;

  dispose(): void;
}

export interface PassiveRestoreOptions {
  through: "ready" | "finish";

  /**
   * 客户端 replica 不能回答远端程序的 terminal query。
   */
  responses: "discard";
}

export class GhosttyCore implements TerminalCore {
  static restorePassive(
    runtime: GhosttyRuntime,
    snapshot: Uint8Array,
    options: PassiveRestoreOptions,
  ): PassiveGhosttyRestore;

  dispose(): void;
}
```

另给 `WTerm` 添加：

```ts
export interface AdoptCoreOptions {
  initialized: true;

  preserveScroll:
    | "bottom"
    | "distance-from-bottom";

  emitResize: false;
  drainResponses: false;
  disposePrevious: true;
}

export class WTerm {
  adoptCore(
    core: TerminalCore,
    options: AdoptCoreOptions,
  ): void;
}
```

`initialized: true` 的意义是：

```text
绝不能再次调用 core.init()
```

---

# 九、Passive restore 的客户端流程

## 模式 A：完整恢复

适合：

```text
页面刷新
cold attach
snapshot 较小
需要完整 bounded scrollback
```

流程：

```text
旧 core 继续显示（如果存在）

在 detached core 中：
  decode snapshot 到 FINISH
  应用 cutSeq 后的 tail
  追到 commitSeq H

原子 adoptCore()
旧 core dispose
```

伪代码：

```ts
async function restoreComplete(
  wterm: WTerm,
  snapshot: SnapshotEnvelope,
  tail: AsyncIterable<TerminalEvent>,
): Promise<void> {
  const passive = GhosttyCore.restorePassive(runtime, snapshot.bytes, {
    through: "finish",
    responses: "discard",
  });

  let seq = snapshot.cutSeq;
  let offset = snapshot.nextPtyOffset;

  for await (const event of tail) {
    validateNext(event, seq, offset);

    applyDirectly(passive.core, event);

    seq = event.seq;
    if (event.type === "pty-output") {
      offset += BigInt(event.bytes.length);
    }

    if (seq >= snapshot.commitSeq) break;
  }

  wterm.adoptCore(passive.core, {
    initialized: true,
    preserveScroll: "distance-from-bottom",
    emitResize: false,
    drainResponses: false,
    disposePrevious: true,
  });
}
```

注意这里必须：

```text
passive.core.writeRaw(...)
```

不能调用：

```text
wterm.write(...)
```

当前 `WTerm.write()` 会安排渲染并主动 drain terminal responses 到 `onData`；这与 passive 恢复语义冲突。

## 模式 B：快速追赶

适合：

```text
客户端严重落后
持续 output spew
snapshot history 较大
目标是尽快回到 live
```

流程：

```text
decode 到 READY
立即开始向 passive core 应用 tail
达到 commit watermark 后 adopt
history 后续 best-effort 恢复或直接放弃
```

libghostty 当前 decoder 允许在 `READY` 后、`next()` 之间渲染、resize 和喂入 live PTY；如果历史页因 terminal 已继续变化而无法安全插入，会消费并验证该页但不应用。

因此：

```ts
const mode =
  reason === "slow-client"
    ? "ready"
    : "finish";
```

---

# 十、Passive restore 也需要追赶上限

即使客户端在后台恢复，Host 可能持续高速输出。

为一次 restore 设定：

```text
最大 buffered tail：2 MiB
最大 restore age：5 s
```

超过任一：

```text
dispose passive core
增加 deliveryGeneration
reset data stream
请求更新的 snapshot
```

```ts
if (
  restore.bufferedTailBytes > 2 * MiB ||
  performance.now() - restore.startedAt > 5_000
) {
  restore.dispose();
  requestSnapshot({ newerThan: restore.cutSeq });
}
```

否则可能出现：

```text
客户端一直在恢复旧 snapshot
但永远追不上 head
```

在 `READY` 后可以直接把新 tail 喂给 passive core，避免继续堆积。

---

# 十一、原子 adopt 的细节

`WTerm.adoptCore()` 应在一个 JS task 内：

```text
1. cancel 当前 RAF
2. cancel synchronized-output timer
3. 暂停 data frame handler
4. 验证新 core cols/rows
5. bridge = nextCore
6. reset scrollback bookkeeping
7. renderer.setup()
8. render exactly once
9. 恢复 frame handler
10. dispose old core
```

不应：

```text
触发 onResize
drain snapshot 期间产生的 response
播放旧 BEL
重新执行 clipboard
```

滚动位置：

* 用户原本在 bottom：保持 bottom；
* 用户原本查看历史：保存“距 bottom 的行数”，在新 scrollback 中 clamp；
* 不保存绝对 scrollTop，因为新 snapshot 的 history 长度可能不同。

---

# 十二、最终状态机

```text
                    ┌──────────────┐
                    │ no snapshot  │
                    └──────┬───────┘
                           │ cold demand / idle
                           ▼
                    ┌──────────────┐
                    │   building   │
                    └──────┬───────┘
                           │ complete + atomically publish
                           ▼
                    ┌──────────────┐
                    │   servable   │
                    └──────┬───────┘
                           │ tail > 8 MiB
                           ▼
                    ┌──────────────┐
                    │   retired    │
                    └──────┬───────┘
                           │ next demand / idle
                           └──────────────▶ building
```

Client：

```text
live replica available + journal hit
  → tail replay

no replica / journal miss + servable snapshot
  → passive restore + tail + atomic adopt

no servable snapshot
  → Host on-demand snapshot
  → passive restore

restore lag > 2 MiB or > 5 s
  → abort + newer snapshot
```

---

# 最终 V1 决策

可以直接定成下面这组规则：

```text
1. Host Journal：60 秒 / 8 MiB segmented ring。
2. 客户端 ACK 永远不 pin Journal。
3. Snapshot tail ≤ 2 MiB 时直接复用。
4. Tail > 8 MiB 时 snapshot 退休，不在持续输出中强制重建。
5. Cold attach、journal gap 或 idle 100 ms 时按需/机会性构建。
6. 单实例 encode 目标 P99 4 ms，单次超过 8 ms 后切 Host passive checkpoint。
7. Host encode 只写内存；压缩和网络完全在 fence 外。
8. wterm 在 detached GhosttyCore 中 passive restore。
9. Cold attach 默认恢复到 FINISH；慢客户端 resync 默认只到 READY。
10. Passive core 追到 commit watermark 后原子 adopt。
11. Passive restore tail 超过 2 MiB 或持续 5 秒则丢弃并重新基线。
12. 不使用 ANSI serialize 作为网络 snapshot。
```

这套策略的核心不是“始终维护最新 snapshot”，而是：

> **始终维护一个有界的 warm Journal；有便宜且完整的 snapshot 时复用，没有时按需构建。Host 的暂停通过明确预算控制，客户端的恢复通过 wterm passive core 完全隐藏。**
