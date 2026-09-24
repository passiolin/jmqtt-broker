# 配置详解

> 返回 [README](../README.md)

完整配置样例见仓库 `src/main/resources/application.yml`,前缀 `jmqtt.broker`。
工作目录(或其 `config/` 子目录)下放 `application-local.yml` 可覆盖仓库默认配置
(外部文件优先级高于 jar 内配置;`spring.profiles.include: local` 已内置激活)。

## 配置

**本地/部署环境定制**：工作目录(或其 `config/` 子目录)下放 `application-local.yml`
即可覆盖仓库默认配置(外部文件优先级高于 jar 内配置；`spring.profiles.include: local`
已内置激活)。该文件已加入 `.gitignore` —— 密码、内网地址、真实 topic 只写在里面,
不会入库。注意:`mvn test` 会加载它,跑测试前可临时移开。


完整配置见 `src/main/resources/application.yml`，前缀 `jmqtt.broker`。

常用项：

```yaml
jmqtt:
  broker:
    id: jmqtt                    # Broker 标识
    port: 1883                   # MQTT/TCP 端口
    websocket-enabled: true
    websocket-port: 8083
    use-epoll: true              # 无 native 库时自动降级 NIO
    auth-enabled: true
    auth-username: jmqtt
    auth-password: jmqtt         # 建议改为 sha256:<hex>
    default-keep-alive: 60
    session-expiry-seconds: 7200 # 会话保留时长; v5 下同时是 Session Expiry Interval 的封顶值
    topic-alias-maximum: 0       # v5: 接受的入站 topic alias 上限。0 = 不支持(会在 CONNACK 声明)
    server-keep-alive: 0         # v5: 服务端强制心跳(秒)。0 = 不干预, 沿用客户端协商值
    max-payload-size: 10485760   # 单报文上限 10MB
    cluster-enabled: false       # 集群开关
    kafka:
      enabled: false             # 消息总线开关(集群能力依赖它)
      # ---- 消息面: 广播范围 ----
      # 只有「其他节点上的 MQTT 订阅者也需要这条消息」时才需要广播。
      # 若业务上只有设备与服务端通信(设备与其他端之间走 P2P 信令), 让每个节点消费
      # 全量消息就纯粹是成本。★ 配错的后果是「跨节点消息静默不到」而不是报错
      broadcast-enabled: true
      # 只有匹配这些过滤器的主题才进集群广播; 留空 = 全部广播。
      # 混合场景: 只留端间信令与下行指令, 遥测就不必让 N 个节点各读一遍
      broadcast-filters: []
    # 管理面: 把节点概要/客户端注册表/订阅过滤器视图发布到 Redis, 供 jmqtt-admin 读取
    admin:
      enabled: true
      heartbeat-interval-ms: 5000
      state-ttl-seconds: 30      # 心跳停更后键自然过期 —— 这是判活的唯一依据
      max-evict-per-task: 5000   # ★ 单次驱逐硬上限(安全阀, 不是性能参数)
```


### TLS / MQTTS：在 LB 层终结

Broker **有意不内置 TLS** —— 只监听明文 MQTT/TCP 与 MQTT/WebSocket，
SSL 终结交给前面的负载均衡层（NLB / ALB / HAProxy / Nginx 等）：

- 证书的安装与轮换集中在 LB 一处完成，broker 不感知证书，换证书不需要动 broker；
- MQTTS（8883）、WSS（443）等对外端口与限流、访问控制统一在接入层收口；
- broker 与 LB 之间通常同处内网 / 安全组，明文转发不增加暴露面。

### HTTP 认证与 ACL（EMQX 兼容）

鉴权采用可插拔设计，契约兼容 EMQX 5.x 的 HTTP authenticator / HTTP authorization：**认证**（能不能连）与 **ACL 授权**（连上后能 pub/sub 哪些 topic）是两套独立机制 —— 已经为 EMQX 写好的认证服务端可以直接对接。

**认证**（`jmqtt.broker.http-auth`，默认关）。CONNECT 时 broker POST：

```json
{"clientid":"dev-1", "username":"app", "password":"...", "peerhost":"10.0.0.7"}
```

（`password-hash: sha256` 时发摘要，明文不出 broker；GET 方式走查询参数。）服务端回：

```json
{"result": "allow", "superuser": true}
```

- `allow` 放行；`deny` 拒绝（v5 0x86 / v3 code 4）；**`ignore` 落到认证链下一环** —— 内置静态鉴权（`auth-username`/`auth-password`），即链为 `[http, 内置]`
- `superuser: true`：该连接**跳过后续全部 ACL 检查**
- 超时/不可达/响应不合法：按 `on-error` 处理 —— `reject`（默认，fail-closed）或 `ignore`（回落内置）
- 故障后进入 `cooldown-ms` 熔断冷却，期间不再发请求，防止认证中心故障拖垮连接建立

**ACL**（`jmqtt.broker.http-acl`，默认关）。每次订阅（逐过滤器）与每条 PUBLISH 前询问：

```json
{"clientid":"dev-1", "username":"app", "peerhost":"10.0.0.7", "action":"publish", "topic":"cmd/dev/1"}
```

响应 `{"result":"allow"|"deny"|"ignore"}`；`ignore` 与故障按 `on-fail`（默认 `deny`，fail-closed）。实现上的三条硬约束：

- **决策缓存是可行性前提**（默认 60s TTL / 每客户端 32 条 LRU）：ACL 挂在每条 PUBLISH 上，没有缓存等于把投递路径接进 HTTP RTT。缓存命中零外部调用；断连即释放
- **保序**：判定挂起期间同连接的后续报文经每连接串行门**按序排队**（有界 64，超限关连接）—— MQTT 要求同一连接保序，异步判定不能破坏它
- **拒绝语义按版本**：SUBSCRIBE 被拒在 SUBACK 里逐项回 0x87（v3 0x80），不断连接；PUBLISH 被拒 v5 回 PUBACK/PUBREC 0x87，v3 静默丢弃 + 日志（v3 没有逐条错误通道，关连接只会制造重连风暴）

**认证服务端示例**（Spring）：

```java
@PostMapping("/mqtt/auth")
Map<String, Object> auth(@RequestBody Map<String, String> req) {
    boolean ok = deviceService.checkPassword(req.get("username"), req.get("password"));
    return Map.of("result", ok ? "allow" : "deny");
}

@PostMapping("/mqtt/acl")
Map<String, Object> acl(@RequestBody Map<String, String> req) {
    boolean ok = aclService.can(req.get("username"), req.get("action"), req.get("topic"));
    return Map.of("result", ok ? "allow" : "deny");
}
```

两个开关都关闭时（默认）：认证走内置静态鉴权，ACL 完全不生效，行为与引入本功能之前完全一致（同步内联路径，零额外开销）。`enabled: true` 但未配 `url` 时启动即失败 —— 不静默放行。

### 集群（Kafka）

```yaml
jmqtt:
  broker:
    cluster-enabled: true
    kafka:
      enabled: true
      bootstrap-servers: 127.0.0.1:9092
      cluster-topic: jmqtt-cluster
      group-id:              # 留空则自动为 jmqtt-<brokerId>
      auto-offset-reset: latest
      uplink-topic: ""       # 留空表示不开数据面
      uplink-filters: []
      uplink-exclusive: false
      uplink-key: topic      # 分区键: topic(键=主题) 或 device(键=设备标识)
```

**多节点部署时，每个节点必须在同一 Kafka topic 上使用不同的 `group.id`。**
相同会退化成组内分摊（队列语义），跨节点投递直接失效。留空时按
`{client-id-prefix}-{brokerId}` 自动生成，天然满足这一点。

启动日志会打印实际使用的 `groupId`，部署后建议直接核对这一行。

几条实现上的硬约束（都写在代码注释里，改动时请勿绕过）：

| 约束 | 原因 |
|---|---|
| `groupId` 每节点不同 | 广播语义的来源 |
| record 的 **key = MQTT 主题**（消息面恒定） | Kafka 只保证同一 partition 内有序；key 为空时默认 sticky 分区, 同主题消息会散落不同 partition, 消费端看到乱序。key 同时也是消费端还原主题的唯一途径。数据面的 `uplink-key`（按设备分区）只作用于上行 topic, 不得影响消息面 |
| **跳过 `brokerId` 等于自身的消息** | 发布节点已在发布瞬间完成本地投递, 再从总线取回会重复投递 |
| **ingress 不再出站** | 每节点消费全量的拓扑下, 出站回环会呈指数级重发 |
| 出站走有界队列, 满则丢弃 | 位于 MQTT 发布路径上, 绝不能被 Kafka 故障阻塞 |
| `max-block-ms` 必须设小 | 默认 60s 会直接卡住发布路径 |

**消费并行度**（`consumer-threads`，默认 1）：poller 线程只做 poll / 分派 / 提交，
记录按**分区**哈希到固定 worker 并行处理。顺序不受影响 —— 上面「key = 主题 ⇒
同主题必落同分区」保证了同分区（即同主题）固定由同一个 worker 串行处理，
并行只发生在不同分区之间。位点提交只领先于「已连续完成」的部分，至少一次语义
与串行消费等价；worker 队列满时背压 poller 而不是丢消息。有效上限 = topic 分区数。

### 数据面（上行出口）：服务端接入的正规路径

**平台侧消费设备数据应当读 Kafka，而不是订阅 MQTT。** 这不是优化建议，而是架构定位：
用 MQTT 订阅（含共享订阅）做服务端消费，在数据量大时会丢，而且丢得静默 ——
broker 侧的发送队列有界、满了丢最旧且不通知（本实现的 `max-mqueue-len` 就是这道闸门），
QoS 1 覆盖不到这个窗口，MQTT 里更没有位点与重放。
**完整论证、以及下游需要的记录格式与消费约定，见
[`server-side-ingestion.md`](server-side-ingestion.md)。**

Broker 在这里的角色是**协议适配器**：MQTT 进、Kafka 出。

| 配置 | 作用 |
|---|---|
| `routes` | **多路由上行（推荐）**：每条路由一组 `filters` + 目标 `topic` + `key`（topic\|device），一条消息命中多条路由就各发一份；未命中任何路由不发 Kafka、只走集群广播。配置了 `routes` 时下面三个单路由项被取代 |
| `uplink-topic` | 单路由形态（兼容保留）：服务端读的 topic，与集群总线 topic 分开 |
| `uplink-filters` | 哪些主题算上行。建议显式列出，用「全部」会把下行指令也写进去 |
| `uplink-exclusive` | 命中上行的消息**只**进数据面、不进集群广播。服务端直连 Kafka 时**应当打开** —— 集群里已经没有 MQTT 订阅者在等这些消息，不打开就是白白付出 N 倍扇出 |
| `uplink-key` | 分区键。**遥测必须用 `device`** —— 见下 |

**`uplink-key` 是这里最容易埋雷的一个配置。** Kafka 只保证分区内有序，键决定顺序边界：

- `topic`（默认）：键 = MQTT 主题，保证同一主题有序，与集群广播一致。
- `device`：键 = 设备标识，保证**每个设备自己的时序**有序，并按设备把流量散到各分区。

遥测主题往往高度集中（典型情况是**只有一个**，设备身份在 payload 里）。
此时按主题分区会把**全量流量压进同一个分区** —— 下游加再多消费者，能并行读的也只有
持有那个分区的实例。**这个分区就是整条数据管道的吞吐上限**，而且是个很隐蔽的天花板：
消费者数量上去了、CPU 却很闲。按设备分区则天然散开。

记录格式（**只转发 PUBLISH 消息**；value 为 JSON 信封，设备身份不用解析 payload）：

```
key      = 分区键(按路由的 key 决定: topic 或设备标识)
value    = JSON 信封(见下)
headers  = jmqtt-kind = routed-publish
```

```json
{"username":"app-user","topic":"demo/LCU-P1/abc123def/event/up","timestamp":1789381061831,
 "qos":1,"payload":"{\"cmd\":3041,\"requestId\":\"rpt-1\",\"data\":{}}",
 "node":"broker-1","clientid":"device:LCU-P1:abc123def"}
```

- `payload` 是字符串化的消息体（二进制 payload 会被 UTF-8 有损解码；需要无损二进制请订阅 MQTT）
- `node` 是接入节点标识（brokerId），`username` 缺失时省略该字段
- 同 key（`key: device` 时即同设备）分区内严格有序

### 连接事件（设备生命周期）

`connection-event-topic`（留空关闭）：客户端上线/下线发布到专用 topic，
后台按 key 追踪设备的完整在线状态机。

```
key      = clientid          # 同一客户端的事件严格有序
value    = JSON
headers  = jmqtt-kind = client-event
```

```json
{"proto_ver":4,"proto_name":"MQTT","peername":"10.0.0.9:47414","node":"broker-1",
 "connected_at":1788989660509,"clientid":"app:PAD-x:abc123def","action":"connected"}

{"username":"app-user","reason":"tcp_closed","proto_ver":4,"proto_name":"MQTT",
 "peername":"10.0.0.9:51166","node":"broker-1","disconnected_at":1788982685781,
 "clientid":"app:PAD-x:abc123def","action":"disconnected"}
```

`reason`：`closed` = 客户端主动 DISCONNECT；`tcp_closed` = 连接断开 / 心跳超时 / 被接管。
被接管时旧连接的关闭**不**产生 disconnected 事件（客户端马上以新连接出现在事件流里），
一个 clientid 在同一时刻只有一条有效状态。发布走有界队列 ——
重启引发的重连风暴不会阻塞连接路径。

### 下行通道（后台 → 设备）

`downlink`（留空关闭）：broker 消费这些 topic 并按**本地订阅**投递为 MQTT 消息。
这是后台给设备下发指令的 Kafka 通道，与集群消息面完全隔离。

```json
{"topic":"demo/LCU-P1/abc123def/property/down","qos":1,
 "payload":"{\"cmd\":3011,\"requestId\":\"req-1\",\"data\":{}}"}
```

`topic` 必填；`qos` 缺省 1（消息级，以每条消息为准）；`payload` 字符串按 UTF-8 编码为
MQTT 消息体；record 的 key 仅用于分区。

三条结构性保证：

- **每节点独立 group**（`<groupId>-downlink`）：持有订阅者的节点投递，其他节点空转 ——
  设备连在哪个节点，指令就能从哪个节点下去
- **只做本地投递，绝不回写出站**：后台从上行拿到消息再下发（或设备侧回执）不会绕回 Kafka，
  回环从结构上不存在
- **至少一次**：处理完一批才提交位点，与集群消息面同一语义

> 注意：主题现在走 header（`jmqtt-topic`），**不再由 key 承载** ——
> 因为按设备分区时 key 已经不是主题了。消费端优先读 header，读不到才回退到 key。

**`uplink-exclusive` 是互斥的**：打开之后 broker 内部的 MQTT 订阅者也收不到这些消息。
若确实有 MQTT 侧消费者（设备回显、运维工具），需要保持 `false` 或把两类主题拆开。
建议先用真实流量量出「一条消息平均需要投递到几个节点」（`E` 指标）再决定 ——
见 [`cluster-broadcast-rationale.md`](cluster-broadcast-rationale.md)。

启动后可通过 HTTP API 观察总线状态：

```bash
curl -s http://127.0.0.1:8922/open/api/jmqtt/info | jq .data.clusterBus
# {"published":..., "dropped":..., "received":..., "skippedSelf":..., "uplink":..., "outboxSize":...}
```

`dropped` 上升说明写总线跟不上（或 Kafka 不可用）；`outboxSize` 持续接近容量说明
即将开始丢弃。这两个指标能在「跨节点投递正在悄悄变差」彻底暴露之前给出预警。

### 会话持久化（Redis）

三种部署模式（`redis.mode`），任选其一：

```yaml
# 1) 单机（默认）
jmqtt:
  broker:
    redis:
      enabled: true
      mode: standalone
      host: 127.0.0.1
      port: 6379
      database: 0
      key-prefix: jmqtt
      command-timeout-ms: 1000   # 这些命令都在握手路径上, 必须设小
      health-interval-ms: 5000

# 2) 哨兵（主从 + 自动故障转移, 推荐的生产形态）
jmqtt:
  broker:
    redis:
      enabled: true
      mode: sentinel
      master-id: mymaster        # sentinel monitor 配置的主节点名称
      nodes:                     # 哨兵进程地址, 任一可达即可; 主从切换后自动跟随
        - 10.0.0.6:26379
        - 10.0.0.7:26379
        - 10.0.0.8:26379
      key-prefix: jmqtt

# 3) 集群（分片）
jmqtt:
  broker:
    redis:
      enabled: true
      mode: cluster
      nodes:                     # 种子节点, 任一可达即可
        - 10.0.0.6:6379
        - 10.0.0.7:6379
        - 10.0.0.8:6379
      # 集群只有 db0: database 必须为 0, 非 0 启动即失败
```

选型：本项目 Redis 只存会话元数据（属性 + 订阅 + 在途镜像 + 管理面状态），
不在投递路径上 —— **哨兵解决高可用即可满足绝大多数部署**；集群（分片）面向
单机内存确实不够的超大规模。三种模式共享同一套惰性连接、健康检查与冷却降级，
配置错误（哨兵缺 master-id、集群配了 database、节点格式非法）启动即失败。

键结构：

```
jmqtt:session:{clientId}   Hash  node / clean / expire / created / touched / will
jmqtt:subs:{clientId}      Hash  topicFilter -> qos
```

`{clientId}` 是 Redis Cluster 的 **hash tag**：两个键必须落在同一 slot，
否则原子接管的 Lua 脚本会被集群直接拒绝。

几条必须遵守的约束：

| 约束 | 原因 |
|---|---|
| **会话记录必须包含订阅关系** | MQTT 规范中订阅是 session state 的一部分。只存会话属性不存订阅, 恢复后只能回 `sessionPresent=false` —— 否则客户端会以为订阅仍有效而消息永远到不了, 属最难排查的静默故障 |
| **只在连接 / 重连 / 接管 / 订阅变更路径读写** | 一旦接进投递路径就退化成「每次投递一次 Redis 往返」 |
| **`acquireOwner` 必须原子（Lua）** | 「先读 owner 再写 owner」分两步的话, 两个节点同时接管会双双读到空 owner, 于是都认为自己是归属方 → 两份路由、重复投递 |
| **只有 `cleanSession=0` 的会话落盘** | 规范里 `cleanSession=1` 明确要求丢弃既有会话, 没有持久化价值。IoT 设备绝大多数用 `cleanSession=1` —— 这个判断直接把 Redis 写入量砍掉一大截 |
| **Redis 不可用时降级为新会话** | 丢会话的代价是一轮重新订阅, 拒绝接入的代价是设备全部掉线。一律选可用性 |
| **不存消息** | 消息的归宿是 Kafka。放进 Redis 等于把投递路径接到中心化存储上 |

Redis 不可用与恢复都会打日志，恢复后自动重新生效、无需重启 broker。
broker 启动时<b>不会</b>连接 Redis —— Redis 挂掉不应该导致 broker 起不来。

**健康状态的语义**（`RedisConnectionManager`）：「只知道故障、不假定健康」。
初始视为可用，失败后进入一段冷却期（长度取 `health-interval-ms`），冷却结束自动重试，
任何一次成功立刻清除故障标记。

由此得到一条调用约定：

| `available()` 回答的是「现在值不值得尝试」，**不是**「Redis 是否一定可用」 |
|---|
| 它只适合用在**可以安全跳过**的写路径上（跳过的代价只是这一次不落盘） |
| **恢复 / 读取路径不做前置判断** —— 跳过一次恢复的代价是消息真的丢了，而直接尝试一次的代价只是一次命令超时（`command-timeout-ms` 兜底）。两个方向的代价不对称，判断就不该对称 |

在途消息（已发送未确认的 QoS 1/2）的持久镜像见 [在途消息持久化](#在途消息持久化)。

**订阅与会话分开存两个 Hash** 而不是塞进一个字段：订阅增删是 O(1) 的 `HSET`/`HDEL`；
若序列化成一整个列表存单字段，每次变更都要读-改-写整份列表，N 条订阅就退化成 O(N²)。

### 会话过期清理

`SessionExpiryReaper` 每 30 秒回收过期会话，并**级联清理订阅、在途消息、持久层记录**。

只删会话不删订阅的话，订阅会永远留在主题树里指向一个已不存在的客户端 ——
主题树节点数与订阅计数因此单调增长，长期运行后匹配成本与内存都会被历史订阅拖垮。

注意与遗嘱消息的区别：遗嘱在**异常断连**时触发，与会话过期是两件互不相关的事。

### 跨节点连接接管

MQTT 规范 [MQTT-3.1.4-2]：同一 clientId 已有连接时，服务端**必须**断开既有连接。
单节点内这件事由 `ConnectionRegistry.register` 顺手完成；跨节点时必须显式通知 ——
否则旧节点上的连接会一直存活到它自己的心跳超时（可能几分钟），
这期间同一个 clientId 在两个节点上同时在线，表现为共享订阅重复投递、
下行指令被处理两次等难以复现的问题。

实现走集群总线的**定向控制消息**（不是广播）：

```
CONNECT 到节点 B → acquireOwner 返回旧 owner = nodeA
                 → publishTakeover(clientId, "nodeA")
                 → nodeA 消费到该消息, 关闭本地连接并清理本地状态
```

同一条 `cluster-topic` 上混跑两类记录，用 `jmqtt-kind` 头区分
（`publish` / `takeover`）；接管消息带 `jmqtt-target-node`，
非目标节点直接跳过。

| 约束 | 原因 |
|---|---|
| **定向而非广播** | 只有真正持有该连接的节点需要动作, 其他节点收到也无事可做 |
| **key 用 clientId** | 与该客户端的普通消息落在同一 partition, 顺序一致 |
| **先移除会话, 再关连接** | 反过来的话, `channelInactive` 会读到仍然存在的会话并发布遗嘱 —— 客户端只是换了节点, 并没有死 |
| **绝不删除持久层记录** | 此时会话已归属新节点, 删掉等于把新节点刚建立的会话摧毁。`SessionTakeoverService` 刻意不注入 `SessionPersistence`, 从结构上杜绝 |

**已发送未确认消息的去向**：接管清理只释放本节点内存，PUBLISH 在途的**持久镜像刻意保留**
（`detach` 而非 `removeByClient`）—— 客户端在接管方节点重连时镜像被加载回来，
以 `dup=1` 且原报文标识符重发（跨节点在途恢复见上文「在途消息持久化」）。
留下的缺口是 PUBREL 半程状态：它不进镜像，接管后服务端不会再主动重发未送达的 PUBREL ——
流程靠客户端超时重发 PUBREC/PUBREL 收敛（新节点对两者都幂等），不是服务端补发。
接收方向 QoS 2 标识符集合同样不随会话迁移，见 `reports/10-qos2-inbound-dedup.md` 的遗留说明。

**共用同一个 Redis 但未启用集群总线**时，接管无法通知。这种配置会打 ERROR 日志
明确指出问题（而不是静默降级），因为它的症状是极难排查的偶发重复投递。

### 离线消息队列

```yaml
jmqtt:
  broker:
    # 每个持久会话最多缓存多少条离线消息, 超出丢弃最旧的
    # 0 表示不缓存(对 cleanSession=0 的客户端离线即丢, 即 QoS 1/2 语义上的破损)
    max-offline-queue-len: 1000
```

**它补的是哪个洞**：MQTT 规范里，对 `cleanSession=0` 的客户端，服务端在它离线期间
必须为 QoS 1/2 的订阅缓存消息并在重连后投递。在此之前本实现对这一场景是**直接丢弃** ——
QoS 1/2 的投递保证在最需要它的场景（客户端掉线）下根本没有兑现。

**三条丢弃口径**（决定了队列的写入量，也决定哪些场景下保证成立）：

| 场景 | 行为 | 原因 |
|---|---|---|
| QoS 0 | 不入队 | 它本来就没有投递保证 |
| `cleanSession=1` 的会话 | 不入队 | 断开即消失，缓存没有意义 |
| 持久会话 + QoS > 0 + 离线 | **入队** | 这是唯一需要兑现保证的场景 |

这三条把队列的写入量限制在一个很小的子集上。若前两条写成「都入队」，
在百万连接下就是给每一个瞬时断开的设备都堆一份无人领取的消息。

键结构（与 session / subs 共用 `{clientId}` hash tag 以落在同一 slot）：

```
jmqtt:queue:{clientId}   List  按入队顺序保存序列化后的待投递消息
```

两个 Lua 脚本，都是原子操作：**入队**（RPUSH → 超限 LTRIM 丢最旧 → 按会话 expire 续 TTL）
与**取出**（LRANGE + LTRIM，不原子的话并发取队列会拿到重复消息）。
TTL 取自会话哈希的 `expire` 字段 —— 队列的存活期本就该跟会话一致。

**为什么是每客户端一条队，而不是丢进 Kafka**：Kafka 擅长「一份数据多方消费」；
而这里是「一批消息只欠给某一个确定的客户端，且必须保序」。Kafka 的日志压缩保留的是
每个 key 的最新值，不是队列语义；用它做每客户端队列需要自己维护逐客户端 offset，
复杂度远高于收益。

**分批投递**：重连后按 50 条一批取，而不是一次取空。客户端在投递过程中再次断开时，
尚未取出的部分仍留在队列里，下次重连还能拿到。

**订阅是本节点私有的**，因此离线消息只能由**持有该订阅的那个节点**入队。
这是「订阅不复制」这个架构选择的直接推论，客户端重连到哪个节点都能领到积压，
但入队必须发生在订阅所在的节点上。

监控：`/open/api/jmqtt/info` 的 `metrics.offlineMessagesDropped` ——
**非零就在丢应答给离线客户端的 QoS 1/2 消息，是投递保证的实际破损点，必须能看见。**

### 发送窗口与背压

```yaml
jmqtt:
  broker:
    # 单个 clientId 允许的最大在途(QoS 1/2 已发未确认)消息数 —— MQTT 流控
    max-inflight: 32
    # 单连接发送队列上限, 超出丢弃最旧的
    max-mqueue-len: 1000
```

**它解决的是可用性问题，不是性能问题。** 对一个连上就不再读取 socket 的客户端，
Netty 的写缓冲会无限增长 —— **一个坏客户端就足以把整个 broker 的堆吃光**。

两道闸门，职责不同：

| 闸门 | 作用 | 性质 |
|---|---|---|
| **在途窗口**（`max-inflight`） | 未确认的 QoS 1/2 达到上限后不再发新的，剩下的排队等确认 | **规范行为**，也是慢消费者的主要保护 |
| **队列上限**（`max-mqueue-len`） | 排队长度到顶后丢弃最旧的，保证单连接内存有硬上限 | 内存保护 |
| QoS 0 写缓冲满 | 直接丢弃并计数 | QoS 0 本就没有投递保证 |

**为什么主要靠窗口而不是队列**：队列兜的是「已经决定要发」的消息，只能吸收瞬时抖动；
而窗口让整个投递节奏跟随客户端的确认速度 —— 慢客户端自然就慢下来，不会堆积。
单靠队列的话，内存上限一到就必然开始丢消息。

**流控的闭环在 PUBACK / PUBREC** —— 这两处才是「客户端确实收到了 PUBLISH」的语义点。
收到确认后释放窗口，并把排队中的消息继续发出去。**没有这个闭环，窗口一旦用满就永久卡死。**

发送缓冲挂在 **Channel** 上（`ChannelAttributes.SEND_BUFFER`），不是客户端映射表里 ——
它的生命周期就是这条 TCP 连接，连接一断自动回收，不需要任何清理逻辑。

注意与**离线队列**的区别：离线队列属于**会话**，要跨连接、跨节点存活；
发送缓冲属于**连接**，只在连接存活期内有效。两者共用「有界 + 丢最旧 + 计数」的口径，
但生命周期完全不同，不能混为一谈。

监控：`/open/api/jmqtt/info` 的 `backpressure` 段。

| 指标 | 含义 | 排查方向 |
|---|---|---|
| `qos0NotWritableDropped` | QoS 0 因写缓冲满被丢 | 客户端读取慢。QoS 0 无保证，丢弃符合预期 |
| `sendQueueEnqueued` | 因窗口用尽而排队的消息数 | 反映流控在多大程度上生效 |
| **`sendQueueFullDropped`** | **发送队列超限丢最旧** | **投递速度持续超过客户端消费速度，需要排查该客户端** |

`sendQueueFullDropped` 持续增长是最需要关注的 —— 它意味着有客户端的确认速度跟不上，
而窗口机制已经在生效（否则不会积压到队列上限）。

### 在途消息持久化

```yaml
jmqtt:
  broker:
    redis:
      # off   = 不落盘(节点故障丢在途消息, 即原有行为)
      # sync  = 每次变更同步写 Redis。不丢, 但每条 QoS 1/2 多一次 Redis 往返, 抬高投递延迟
      # async = 定时批量刷盘(默认)。投递路径不等 Redis, 崩溃时丢最近一个刷盘周期
      inflight-mode: async
      inflight-flush-interval-ms: 100
      inflight-max-pending-clients: 10000
```

**它补的是哪个窗口。** 在途消息（已发送未确认的 QoS 1/2）原本只存在于进程内存。
节点崩溃或发生跨节点接管时，这些「已经答应要投递给客户端」的消息就消失了 ——
客户端重连后拿不到，**QoS 1/2 的保证在节点故障场景下破损**。

**为什么是整份快照，而不是逐条操作日志。** 每个客户端的在途集合有硬上限
（`max-inflight`，默认 32），所以「重写这一份」成本可控，换来两个关键好处：

| 好处 | 说明 |
|---|---|
| **天然幂等、无顺序问题** | 逐条 `HSET`/`HDEL` 的日志在异步模式下必须保证顺序，否则一条迟到的 `HSET` 会把已确认的消息永久留在镜像里。整份覆盖是「最后写入者胜」 |
| **实现简单** | 不需要维护操作队列、不需要回放 |

写入用 `DEL` + `HSET` 的 Lua 脚本一次往返完成。不用「`HSET` 新值 + `HDEL` 缺失值」，
是因为后者需要记住上一次写了哪些字段，一旦刷盘失败就失去基准；`DEL`+`HSET` 在 Lua 里原子，没有中间空窗。

**异步模式用单线程刷盘**：不是为了让 Redis 操作串行（Lettuce 本身异步），
而是让「提交」有序 —— 同一客户端的两份快照不会因为线程调度而倒序落盘。
配合「只保留最新快照」，即使偶尔乱序也只是短暂不一致，下次刷盘即纠正。

**镜像的存活期跟会话一致**：写入时读会话 Hash 的 `expire` 字段作为 TTL ——
会话过期了，欠它的在途消息也没有意义。

**`detach` 与 `removeByClient` 是两件事**（接管语义的关键）：

| 方法 | 行为 | 用在哪 |
|---|---|---|
| `detach(clientId)` | **只清内存** | 跨节点接管 —— 镜像必须留给接管方节点加载 |
| `removeByClient(clientId)` | 内存与镜像一起清 | 会话**真正销毁**（`cleanSession=1` 断开、会话过期） |

监控：`/open/api/jmqtt/info` 的 `inflight` 段。

| 指标 | 含义 | 排查方向 |
|---|---|---|
| `inflightFlushCount` | 成功刷盘的次数 | 与投递量对照，判断刷盘是否跟上 |
| `inflightFlushedEntries` | 累计刷盘的消息条数 | |
| `inflightPendingClients` | 待刷盘客户端数 | 持续高位说明刷盘跟不上或执行器队列积压 |
| **`inflightFlushFailed`** | **刷盘失败次数** | Redis 抖动或不可用。持续增长意味着崩溃可恢复性在下降 |

**注意：这个组件不按 `redis.enabled` 条件装配。** Bean 始终存在，
是否落盘由 `inflight-mode` 控制（`off` = 不落盘）。

