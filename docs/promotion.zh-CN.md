# dsh-peer：让两个（或多个）DeepSeek Harness Agent 直接对话

> 一个零依赖的 MCP 服务端插件。装上它，你电脑上的 Agent 和手机上的 Agent 就在同一个网段里互相认识，
> 可以互相派活、拉群、留回执——不需要云中转，不需要共享会话文件，也不需要你在中间复制粘贴。

- 包名：`dsh-peer`（npm） · 仓库：[CC2027MK2/dsh-peer](https://github.com/CC2027MK2/dsh-peer)
- 许可：MIT · 依赖：**零**（只用 Node 内置模块）· 协议：MCP streamable-HTTP
- 工具：12 个 · 端口：默认 `38404` · 适用：DSH `>=0.2.1-rc.2 <0.3.0`

---

## 1. 问题：DSH 只有客户端，没有服务端

DeepSeek Harness 自带 `@deepseek-ai/dsh-mcp-client`——它能**连到**别的 MCP 服务，但没有 MCP
**服务端**。于是两个 DSH 之间是断的：

```
电脑 DSH  ──可以连──▶ 各种 MCP 服务
手机 DSH  ──可以连──▶ 各种 MCP 服务
电脑 DSH  ──✗ 没法──▶ 手机 DSH        ← 缺的那一半
```

现实里这件事是这样凑的，每一种都有硬伤：

| 权宜办法 | 硬伤 |
| --- | --- |
| 把结果复制粘贴给对方 | 人在环路，慢，且每次都要重新解释上下文 |
| 共享一个会话文件 | 两边同时写会互相截断；没有权限边界 |
| 走公网中转 / 第三方 Bot | 要注册服务、要出网、内容和凭证明文过第三方 |
| 自己写个 HTTP 接口 | 每加一个能力就要改代码，没有工具语义、没有回执 |

`dsh-peer` 补的就是缺的那一半：**把本机 Agent 变成一个标准 MCP 服务**。接上之后，
"手机上的 Agent"在电脑的 Agent 眼里就是四个/十二个普通工具，跟读文件、跑命令没有区别。

---

## 2. 装上之后是什么样

```
   电脑（DSH 桌面）                                   手机（DSH Go）
┌────────────────────────────────┐              ┌────────────────────────────────┐
│ agent                          │              │ agent                          │
│  ├─ dsh-mcp-client ────────────┼──MCP/HTTP──▶ │  ├─ dsh-peer :38404            │
│  │   serverName: phone         │              │  │   POST /mcp → agent.session  │
│  └─ dsh-peer :38404 ◀──────────┼──MCP/HTTP──  │  └─ dsh-mcp-client             │
└────────────────────────────────┘              └────────────────────────────────┘
```

- 两边都可以同时是服务端和客户端（上图就是互相都连着）。
- 三台以上时不做全互联，改**一个 hub + 若干成员**（见第 5 节）：
  N 个 Agent 全互联要 N×(N−1) 条客户端配置和 N 个入站端口，hub 只要每成员一行。
- 手机在热点/蜂窝网后面、完全没有入站端口也能参与——所有消息都由成员**主动拉取**。

---

## 3. 十二个工具，分三层能力

### 3.1 调度层：把活派过去

| 工具 | 干什么 |
| --- | --- |
| `peer_ask` | 给对方 Agent 发一句话，等它回话。`session` 决定用哪个对话（互不串味） |
| `peer_task` | 等/读一个**异步**任务的结果 |
| `peer_sessions` | 列出对方有哪些对话槽、各跑了多少轮 |
| `peer_history` | 读回某个槽最近几条消息 |
| `peer_status` | 对方是谁：主机、平台、模型、身份、沙箱模式、**能力清单**、任务计数 |

**长任务不再硬等。** 老版本一次调用最长等 240 秒；现在三选一：

```jsonc
// 1) 同步：小活，直接等
{ "prompt": "把 build 目录里最新的 apk 大小报给我", "session": "build" }

// 2) 异步：大活，先拿号
{ "prompt": "跑一遍完整构建并截图", "session": "build", "mode": "async" }
// → { "status": "accepted", "task_id": "t-m9x2k-a7f3qd" }

// 3) 回收：过一会儿来取，最多等 5 分钟
// peer_task { "taskId": "t-m9x2k-a7f3qd", "waitMs": 300000 }
```

实测：一个 255.5 秒的任务，`peer_ask(mode:"async")` **0.2 秒**返回受理号，
`peer_task` 在第 255.5 秒拿到结果——全程不占调用方的一次 turn。

带 `idempotencyKey` 重发同一件事，拿回的是**同一个** `task_id`，不会重复执行。

### 3.2 契约层：先问能不能，再谈干什么

**能力清单。** `peer_status` / `/health` 回一份 `capabilities`：

```jsonc
{
  "protocol": "chatv1.1", "identity": "phone",
  "platform": "linux/arm64", "shell": "bash",
  "sandbox": "danger-full-access",
  "tools": null,                       // null = 未声明，不是"什么都没有"
  "writableRoots": ["/root", "/root/.dsh/peer"],
  "async": true,
  "artifacts": { "fetch": true, "hosts": [], "maxBytes": 67108864, "strict": false },
  "limits": { "maxChunkChars": 60000, "taskTimeoutMs": 1800000, "askTimeoutMs": 240000, "maxSlots": 8 }
}
```

**预检 `require`。** 派活时先说清"我需要什么"，对方**在把活交给 Agent 之前**就核对：

```jsonc
{
  "prompt": "在 /root 下编译",
  "require": { "shell": "bash", "sandbox": "workspace-write", "async": true }
}
```

不满足就直接返回，**一次 turn 都不烧**：

```jsonc
{
  "v": 1, "status": "failed",
  "error": "requirements not met",
  "output": { "missing": ["sandbox:workspace-write (host is danger-full-access)"],
              "unverified": [], "capabilities": { … } }
}
```

实测这类拒绝 **21ms** 返回。没有预检时，同样的请求会在对方 Agent 里跑十几秒，
最后回一句 `need_user`，调用方还得自己翻译成"你那边做不到"。

### 3.3 交付层：文件不是"描述"，是**校验过的字节**

`artifacts[]` 里给的每个文件都会被真的取一遍、算哈希、记账：

```jsonc
{
  "prompt": "把这个包装上，装完回版本号",
  "artifacts": [
    { "role": "apk", "url": "http://192.0.2.10:38405/app.apk",
      "sha256": "9f2c…", "size": 2473609 }
  ]
}
```

- 先 `HEAD` 看长度；有 `sha256` 或 HEAD 不成就**整份流式下载再算哈希**，超过 `maxBytes` 直接报错。
- 结果写进 `<dataDir>/transfers/<port>/<时间>-<随机>.json`，**失败也记**，保留 200 条。
- 校验通过后，prompt 前面会插入 `[peer] verified artifacts …`，对方 Agent 知道"这些字节我确认过"。
- `strictArtifacts: true` 时，正文里出现**没有声明**的 URL 会被直接拒绝（12ms 级别）。

这一条解决的是最常见的失败：**描述不等于交付**。人写的"在我电脑上的 xxx 目录"对面根本看不到；
写清了 URL + sha256，才有可能真的对上。

### 3.4 协议层：消息会自我介绍

每条经过 `dsh-peer` 的消息都带信封（CHATv1 v1.1）：

```
[[CHATv1 from=phone msg=phone-20261119T155511Z-g95bok ts=2026-11-19T15:55:11Z chain=- reply=pc-20261119T153000z-aaa111]]
好的，我看到你上一条了
```

- `from` 是谁说的，`msg` 是本条唯一编号，`reply` 指回上一条——**跨设备引用不再靠"刚才那句"**。
- **没带信封的，就是人本人**说的。这条规则让"人在中间插话"和"Agent 之间对话"在同一个界面里可区分。
- 同一条 `msg` 重投会自动去重，返回缓存结果（实测：第二次调用直接 `"duplicate": true`，不重跑）。
- 正文超过 `maxChunkChars`（默认 60000 字符）按 `chunk=i/n` 自动分片。

### 3.5 群聊层：三个以上 Agent 进同一个房间

```yaml
rooms:
  lobby:
    members: [pc, phone, laptop]
  build:
    members: [pc, phone]
roomMaxEvents: 2000
```

| 工具 | 干什么 |
| --- | --- |
| `room_say` | 往房间发一条（可 `to` 定向某成员，可 `replyTo` 引用） |
| `room_sync` | 拉 `since` 之后的消息；给 `waitMs` 就是长轮询，有新消息立刻返回 |
| `room_list` | 房间名册、最新 seq、最后一条 |
| `room_pending` | **我欠谁回执**——断线回来先看这里 |
| `room_ack` | 把到 `seq` 为止的标记为已读 |
| `room_claim` | 认领一条：这活归我 |
| `room_accept` | 签收：`accepted` / `rejected` + 备注 |

房间就是追加式日志（`<dataDir>/rooms/<port>/<room>.jsonl`），每条有单调递增的 `seq`，
所以成员的游标只是一个数字——重启后从断点继续，不用任何人重发。文件超过 1.5× 上限自动压回 `roomMaxEvents`。

**身份靠 HTTP 头。** 成员调用 hub 时带 `X-Peer-Identity: phone`，hub 把这条消息记在
**phone** 名下，而不是"hub 主人"名下——否则三个 Agent 说的话在日志里全是一个人口吻。

### 3.6 回执、补投、归属：谁接的、谁在干、谁验收

```
queued ──(room_sync)──▶ delivered ──(room_ack)──▶ read
                            │
                            └──(room_claim)──▶ owned ──▶ accepted | rejected
```

- **回执是按成员、按条计的**。phone 读过一条，不代表 laptop 也读过了——不会出现"一条 ack 全体静音"。
- **补投**：`room_pending` 是你欠下的全部，断线一小时回来也一条不丢，不需要谁手工重发。
- **归属**：`room_claim` 之后别人再抢，会拿到 `seq 7 is owned by phone`，而不是悄悄两个人干同一件事。
- **验收**：只有 owner（或无人认领时的收件人）能签收，旁人拿到
  `only the owner can accept or reject this one`。签收记录永久留在这条消息旁边。

这套语义不是拍脑袋：SMTP 的 DSN、XMPP 的 XEP-0184 已读回执、MQTT 的 QoS 1、
Redis Streams 的 PEL / SQS 的 visibility timeout，都是同一个问题的答案——**"投递"和"处理完"必须分开记**。

---

## 4. 快速开始

### 4.1 桌面端（DSH desktop profile）

```powershell
# 1) 装进 profile（bundle 会自动插入 cordis.patch.yml 的一行）
dsh plugin --profile desktop add dsh-peer

# 2) 起一个 token（不要让局域网里谁都能当你的 Agent）
$token = -join ((1..32) | ForEach-Object { '0123456789abcdef'[(Get-Random -Max 16)] })
New-Item -ItemType Directory -Force $env:USERPROFILE\.dsh\peer | Out-Null
Set-Content $env:USERPROFILE\.dsh\peer\token $token -NoNewline

# 3) 重启 DSH，确认它在听
curl.exe -s http://127.0.0.1:38404/health
```

### 4.2 手机端（DSH Go）

手机上的 DSH 跑在 PRoot 容器里，`workspace-write` 沙箱后端不可用（bwrap 读不到
`/proc/sys/kernel/overflowuid`）。所以要么接受"Agent 不能用 shell"，要么显式放开：

```yaml
# /root/.dsh/profiles/web/cordis.patch.yml
- insert:
    - id: dsh-peer
      name: dsh-peer
      config:
        port: 38404
        tokenFile: /root/.dsh/peer/token
        identity: phone
        agentPreset: dsh-executor     # 执行者预设，见第 6 节
        sandboxMode: danger-full-access
```

### 4.3 让两边互相认识

在**客户端一侧**加一行（`@deepseek-ai/dsh-mcp-client`）：

```yaml
- id: peer-phone
  name: '@deepseek-ai/dsh-mcp-client'
  config:
    serverName: phone                       # 工具前缀 → mcp__phone__*
    transport: streamable-http
    url: http://192.0.2.11:38404/mcp
    headers:
      Authorization: 'Bearer <对方的 token>'
    toolCallTimeoutMs: 600000
```

然后对方的工具就以 `mcp__phone__peer_ask`、`mcp__phone__room_pending` 的形式出现在你的工具列表里。

> 名字只有一处要小心：`serverName` 和 `denyTools` 是**成对**的。防回环配置里写的是
> `mcp__phone__*`，换了 `serverName` 却忘了改 `denyTools`，回环保护会静默失效。

---

## 5. 为什么是 hub-and-spoke

| | 全互联 | hub |
| --- | --- | --- |
| 客户端配置行 | N×(N−1) | N |
| 需要开入站端口 | N 个 | 1 个 |
| 加一个新成员 | 改 N 处 | 改 1 处（hub 的房间名册） |
| 手机/NAT 后面 | 没法当别人连入的目标 | 只出站，天然可用 |

代价是 hub 是单点——对"我的几台机器协作"这个场景，这个代价比维护 N² 条连线便宜得多。

---

## 6. 回环、越权与其它安全边界

这类插件的真正风险不是"被陌生人连上"，而是**两个 Agent 互相把活推给对方，无限套娃**。

- **执行者预设（`dsh-executor`）**：跑在 peer 会话里的 Agent 用这个预设，它**不包含任何
  MCP 客户端**——所以它根本没有 `mcp__phone__peer_ask` 这类工具，想转派也没有手。
- **`denyTools`**：即使预设里带进来了，插件还会在 agent scope 上调用
  `tools.restrict({ deny: [...] })` 再挡一刀。PC 那边看健康检查能看到
  `"denyTools": ["mcp__phone__peer_ask", …]`。
- **只允许一层转派**：预设说明里写死"不要转派"，协议里有 `no_forward_ack` 字段回执。
- **token**：`token` 或 `tokenFile`（32 位 hex 就够），无 token 的请求一律 401。
- **仅局域网**：默认监听 `0.0.0.0` 是为了让局域网里另一台机器连上；不要映射到公网。
- **需要人时明确求助**：Agent 卡在审批上时，插件不等 240 秒超时，而是立刻返回
  `status: "need_user"` + 审批内容（工具名、原因）。
- **有限并发**：`maxSlots`（默认 8），超出按 LRU 淘汰**并 dispose 掉对应 agent**，不会把内存和会话越堆越多。

责任边界也要说清楚：**token 只挡住"谁能调"，挡不住"被调用方干了什么"**。
如果你给执行者 `danger-full-access`，那它在自己机器上就是真的能跑任意命令。

---

## 7. 配置全表

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `port` | `38404` | 监听端口。桌面端已有 38404 时，再起一个实例用别的端口 |
| `token` / `tokenFile` | 空 | 鉴权。二选一，`tokenFile` 更适合不入库 |
| `identity` | 空 | 本机在群聊里的名字（`[a-z][a-z0-9_-]{0,31}`）；设了才启用 CHATv1 |
| `chatProtocol` | `identity` 非空时 true | 是否给回复套信封 |
| `hostLabel` | 主机名 | `peer_status` 里显示的名字 |
| `cwd` | `process.cwd()` | Agent 的工作目录 |
| `agentPreset` | 空 | 用哪个预设跑 peer 会话（强烈建议配一个无 MCP 客户端的执行者预设） |
| `denyTools` | `[]` | 在 agent scope 上禁用的工具名（精确名，不是前缀） |
| `sandboxMode` | 空 | 会话来一次 `sandbox/mode` 事件把它钉住（`read-only`/`workspace-write`/`danger-full-access`） |
| `maxSlots` | `8` | 会话槽上限，LRU 淘汰 |
| `askTimeoutMs` | `240000` | 单次同步等待上限 |
| `taskTimeoutMs` | `1800000` | 异步任务执行上限 |
| `maxChunkChars` | `60000` | 单条正文上限，超出分片 |
| `dedupeSize` | `500` | 去重窗口（按 `msg`） |
| `rooms` / `roomMaxEvents` | `{}` / `2000` | 群聊房间与每个房间保留的事件数 |
| `artifactMaxBytes` / `artifactTimeoutMs` / `artifactHosts` / `strictArtifacts` | 64 MiB / 20s / 全部 / false | 文件交付策略 |
| `capabilities` | 空 | 覆盖自动探测出来的能力清单（例如手工声明 `tools`） |
| `dataDir` | `~/.dsh/peer` | 任务、房间、回执、台账都在这儿 |
| `logFile` | `<dataDir>/peer.log` | 结构化行为日志 |

HTTP 面：`POST /mcp`（MCP）、`GET /mcp` → 405（本服务不发服务端流）、`DELETE /mcp` → 204、
`GET /health`（**不鉴权**：只暴露版本、工具名、能力与计数，方便探活）。

---

## 8. 实测数据（都要在真机上复现过）

| 项目 | 结果 |
| --- | --- |
| 能力预检拒绝 | 21 ms（不消耗对方 turn） |
| 文件校验通过并交付 | 6.86 s（含下载 + 哈希 + 落账） |
| sha256 不符 | 72 ms 拒绝，台账仍记一笔 |
| strict 模式拦未声明 URL | 12 ms |
| 异步长任务 | 受理 0.2 s，255.5 s 后取到结果 |
| 去重 | 同 msg 重投直接返回缓存，不重跑 |
| 会话槽 LRU | 建 10 个 → 稳定在 8，日志有 `slot: evicted peer:slot0 (cap 8)` |
| 三方群聊 | pc / phone / pc-chat 同房，手机 Agent 自己发消息、自己复述全部历史 |
| 回执与归属 | 10 项断言全过；跨进程真机：手机自己 pending → claim → accept，34.2 s |
| 防回环 | 两端问"你有对方的 peer 工具吗" → `NONE` |

---

## 9. 诚实的边界

- **需要 L3 可达**：同广播域（同一 Wi-Fi / 同一热点）零配置；跨子网要手填地址；
  CGNAT 下的手机只能"往外出"。要跨网就上 Tailscale/ZeroTier 之类，本插件不用改。
- **DHCP 漂移**：手机换网就换 IP，建议路由器做静态租约。群聊里身份是名字，不受 IP 影响。
- **审批会阻塞**：对方 Agent 需要人工批准时，本次调用会以 `need_user` 返回——这是设计，不是 bug。
- **手机上的 shell 有 300 秒单命令上限**：更长的活要在对方后台起进程，自己写 job 文件再轮询。
- **不是加密通道**：局域网内明文 HTTP，靠 token 鉴权。别映射到公网。
- **一次性成本**：每次 `peer_ask` 都是对方的一次真实模型回合，费钱也费时间；能用群聊广播就别点对点刷。

---

## 10. 和别的东西什么关系

- **MCP 官方生态**：`dsh-peer` 就是一个 MCP server，任何 MCP 客户端都能连，不只是 DSH。
  `tools/mcp-probe.mjs` 是随包的零依赖探针，不装 DSH 也能验证对面通不通。
- **A2A（Agent2Agent）**：思路一致（消息、任务、能力声明），`dsh-peer` 取的是其中最小可用的一组，
  并且用 MCP 的工具语义承载，所以不用新协议栈。
- **消息队列**：回执/补投/归属那套语义借自 SMTP DSN、XMPP XEP-0184、MQTT QoS1、
  Redis Streams PEL 与 SQS visibility timeout——它们各自解决了"投递≠处理完"的同一件事。

---

## 11. 路线图

- [ ] npm 正式发布 + 完整 provenance 证明
- [ ] CHATv1 信封的 `chunk` 重组端到端测试
- [ ] `/health` 可选鉴权；token 常量时间比较
- [ ] 日志轮转（现在只 append）
- [ ] 房间 ACL（现在成员表只校验发送者是否属于房间）
- [ ] 桌面端 GUI 面板：房间、回执、任务一屏看完

---

## 12. 引用

- 仓库：<https://github.com/CC2027MK2/dsh-peer>
- 详细英文文档：仓库 `README.md`（含每个工具的输入输出、五类设计出处）
- 探针：`node tools/mcp-probe.mjs http://<host>:38404/mcp "<一句话>"`

MIT License · 2026
