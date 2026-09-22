# jmqtt-broker

基于 **Netty + Spring Boot** 的 MQTT Broker，单模块 Maven 工程，支持 MQTT 3.1.1 与 5.0。

设计的出发点是一件事：**投递路径上不能有任何外部依赖**。
路由匹配的成本应该是「主题有多深」的函数，而不是「订阅有多少条」的函数；
集群的复杂度应该落在「谁有事做」上，而不是「谁来同意谁有事做」。
下面两节分别展开。

## 技术栈

| 项 | 版本 / 说明 |
|---|---|
| Java | 21（Zulu 21） |
| Spring Boot | 2.7.18（官方支持 Java 8 ~ 21） |
| 构建 | Maven，**单模块** |
| 网络 | Netty 4.1.x（版本由 Spring Boot 的 netty-bom 统一管理） |
| 编解码 | `io.netty:netty-codec-mqtt`（官方库，支持 v3.1 / v3.1.1 / v5.0） |
| 协议 | MQTT 3.1.1 / 5.0 双栈（按 CONNECT 自动分流） |
| 许可 | Apache License 2.0 |

## 快速开始

### 1. 构建

```bash
mvn clean package
```

### 2. 运行

```bash
java -jar target/jmqtt-broker.jar
```

或开发期直接：

```bash
mvn spring-boot:run
```

默认监听：

| 端口 | 用途 |
|---|---|
| 1883 | MQTT over TCP |
| 8083 | MQTT over WebSocket（路径 `/mqtt`） |
| 8922 | HTTP API / Actuator |

### 3. 连接

MQTT 3.1.1，默认开启认证：

```
username: jmqtt
password: jmqtt
```

> 生产环境请把 `jmqtt.broker.auth-password` 改成 SHA-256 摘要形式。
> 生成方式：`java -cp target/jmqtt-broker.jar online.ipuff.jmqtt.auth.util.PwdUtil <明文密码>`

### 4. HTTP API

```bash
# 查看运行状态(连接数、会话数、订阅数、主题树节点数、背压触发次数)
curl http://127.0.0.1:8922/open/api/jmqtt/info

# 服务端主动下发消息
curl -X POST http://127.0.0.1:8922/open/api/jmqtt/send \
     -H 'Content-Type: application/json' \
     -d '{"topic":"device/123/cmd","qos":1,"message":"hello"}'
```

## 目录结构

```
src/main/java/online/ipuff/jmqtt/
├── JmqttBrokerApplication.java      # 启动入口
├── config/
│   ├── BrokerProperties.java        # 配置绑定(构造器绑定, record)
│   ├── AsyncConfig.java             # 异步线程池(显式 TaskExecutor)
│   └── RedisConfig.java             # RedisClient(惰性连接)
├── server/
│   ├── BrokerServer.java            # Netty 接入层 / SmartLifecycle
│   └── codec/MqttWebSocketCodec.java
├── handler/
│   ├── MqttBrokerHandler.java       # 报文入口与分发
│   └── ChannelAttributes.java       # CLIENT_ID / SEND_BUFFER
├── protocol/
│   ├── ProtocolProcessor.java       # 处理器聚合
│   ├── ReplyFactory.java            # ★ 按连接版本构造全部回包 + 出站 PUBLISH
│   ├── ConnectOptions.java          # ★ 把 v3.1.1/v5 的 CONNECT 语义拍平成统一模型
│   ├── ConnectHandler.java          # CONNECT(会话恢复 / 接管 / 在途重发)
│   ├── PublishHandler.java          # PUBLISH
│   ├── SubscribeHandler.java        # SUBSCRIBE
│   ├── UnsubscribeHandler.java      # UNSUBSCRIBE
│   ├── PubAckHandler.java           # PUBACK / QoS1
│   ├── PubRecHandler.java           # PUBREC / QoS2
│   ├── PubRelHandler.java           # PUBREL / QoS2
│   ├── PubCompHandler.java          # PUBCOMP / QoS2
│   ├── PingReqHandler.java          # PINGREQ
│   └── DisconnectHandler.java       # DISCONNECT
├── router/                          # ★ 内存主题树路由
│   ├── MqttTopic.java               # 主题解析与校验
│   └── TopicTrie.java               # 主题树(前缀树, 双向遍历)
├── message/                         # 领域模型
│   ├── SubscribeStore.java / WillMessage.java
│   └── DupPublishMessageStore.java / DupPubRelMessageStore.java
│   └── RetainMessageStore.java
├── session/
│   ├── SessionStore.java            # 会话(不含连接标识)
│   ├── ISessionStoreService.java / SessionStoreService.java
│   ├── ConnectionRegistry.java      # ★ clientId -> Channel 本地注册表
│   ├── SessionExpiryReaper.java     # 会话过期的级联清理
│   ├── SessionTakeoverService.java  # 跨节点接管(接收侧)
│   ├── SendBuffer.java              # 在途窗口 + 有界发送队列
│   ├── BackpressureMetrics.java / QosMetrics.java
│   └── persistence/                 # 会话持久层(策略层 + 接口 + Noop)
│       ├── ISessionRepository.java / NoopSessionRepository.java
│       ├── PersistedSession.java / SessionPersistence.java
│       └── redis/RedisSessionRepository.java
├── subscribe/
│   ├── ISubscribeStoreService.java
│   └── SubscribeStoreService.java   # 基于 TopicTrie
├── store/
│   ├── IRetainMessageStoreService.java / RetainMessageStoreService.java
│   ├── IDupPublishMessageStoreService.java / DupPublishMessageStoreService.java
│   ├── IDupPubRelMessageStoreService.java  / DupPubRelMessageStoreService.java
│   ├── IMessageIdService.java / MessageIdService.java
│   ├── InflightPersistence.java     # 在途消息的 Redis 镜像(整份快照, 三种模式)
│   ├── IPendingMessageStore.java / InMemoryPendingMessageStore.java / PendingMessage.java
│   └── redis/RedisPendingMessageStore.java   # 离线队列(Lua 原子入队/取出)
├── redis/
│   └── RedisConnectionManager.java  # 共享连接 + 统一降级(冷却期语义)
├── cluster/
│   ├── InternalMessage.java         # 内部转发消息
│   ├── ClusterBus.java / LocalClusterBus.java / ClusterBusStats.java
│   ├── TakeoverListener.java        # 接管通知的接收侧
│   ├── InternalCommunication.java   # 出站转发(消息面/数据面分流)
│   ├── InternalSendServer.java      # 本节点投递
│   └── kafka/                       # ★ 集群总线实现
│       ├── ClusterRecords.java      # 编解码(key = MQTT 主题)
│       └── KafkaClusterBus.java     # 每节点独立 group.id = 广播
├── auth/
│   ├── IAuthService.java
│   ├── AuthService.java             # 配置化凭据校验
│   └── util/PwdUtil.java            # 生成密码摘要
└── web/
    └── OpenApiController.java       # HTTP API(指标全部读内存计数器)
```

## 关键设计决策

### 1. 主题路由：内存主题树，而不是每次发布去外部存储捞订阅

把订阅关系放在外部存储、发布时把匹配的订阅捞出来，是一种很自然的做法，但它有两个
**随规模放大**的代价：匹配复杂度是 O(通配订阅总数)，而且每一条都带一次网络往返。
在十万级连接、百万级订阅量下，这条路径本身就构成了吞吐上限。

改为纯内存前缀树：

| | 朴素做法（订阅在外部存储） | 本实现 |
|---|---|---|
| 订阅匹配 | O(通配订阅总数) + 每条一次往返 | **O(主题层数)**，零外部调用 |
| retain 通配 | O(全部保留消息数) | O(匹配结果数) |
| 剪枝 | 手动判断外部结构是否为空 | 回溯自动回收 |
| 幂等 | 依赖外部结构的 key 覆盖 | 树节点值覆盖 |

**代价说清楚**：订阅表因此只在节点本地，不复制、不共享。
节点宕机时它随进程一起消失 —— 这正是广播模式成立的前提，也是它恢复快的根因。

`TopicTrie` 同时承载两种**方向相反**的查询，这一点必须区分清楚：

- `matchTopic()` —— **主题驱动**。树里存过滤器，输入具体主题名。用于订阅路由。
- `scanByFilter()` —— **过滤器驱动**。树里存具体主题名，`+` / `#` 表现为枚举子节点。用于保留消息索引。

### 2. 投递路径：一次 map get，不做间接层

「先查会话拿到连接标识，再按标识查连接」这种两级设计看起来层次清晰，
实际把一条本可以 O(1) 的本地操作变成了三步：

```java
SessionStore s = sessionStoreService.get(clientId);                          // ① 可能是一次外部往返
ChannelId cid = channelIdMap.get(s.getBrokerId() + "_" + s.getChannelId());  // ② 拼 key 再查
Channel ch = channelGroup.find(cid);                                         // ③ 遍历
```

而这发生在**每条消息、每个订阅者**上。更麻烦的是中间那层标识里带 broker 前缀是为
「远程连接」准备的 —— 但连接对象只存在于本 JVM，远程永远查不到，这层间接性是纯粹的成本。

现在由 `ConnectionRegistry` 承担：clientId → Channel 单层映射，投递一次 get 结束。
连接信息**只存在于进程内**，不进任何外部存储。

`unregister` 使用 `remove(key, value)` 语义 —— 否则「旧连接断开」的延迟回调
会误删刚刚重建的新连接（重连场景的典型竞态）。

### 3. `sessionPresent` 必须问服务端自己

一个很自然但错误的写法是按客户端的标志位反推：

```java
boolean sessionPresent = !msg.variableHeader().isCleanSession();   // 只看客户端标志位
```

它的后果是：以 `cleanSession=0` 连到一个**并没有**该会话的 broker，客户端会收到
`sessionPresent=true`，于是**不重新订阅**；而服务端一条订阅都没有 →
**静默丢消息，不报错也不断连**，客户端完全无法察觉。

正确做法是按「服务端是否真的恢复了会话（且订阅也一并恢复）」计算，
只有两者都成立才回 `true`。

### 4. 其他细节取舍

| 项 | 朴素做法 | 本实现 |
|---|---|---|
| SUBSCRIBE 校验 | 一个过滤器非法就关整条连接 | 逐项返回失败码 |
| 过滤器校验规则 | 只查「非空 + 无非法字符」，漏了 `sport/tennis#` 这类「`#` 未独立占层」 | 完整校验 |
| 保留消息的 retain 标志 | 投递时置 0 | 置 1（规范要求） |
| 背压 | 设了 `WRITE_BUFFER_WATER_MARK` 却无人检查 `isWritable()` | QoS 0 背压丢弃，QoS 1/2 计入重传 |
| 会话 TTL | 用 `keepAlive × 1.5` 兼作会话过期 | 会话过期独立配置 |
| 会话中的连接信息 | 存连接标识 | 移除，连接由 `ConnectionRegistry` 管 |
| 认证 | 固定私钥「加密」用户名再比对密文 | 常量时间凭据校验，支持 SHA-256 |
| 报文标识符 | 共享存储自增 | 进程内 `AtomicInteger` |
| 集群广播 | 共享存储 Pub/Sub | 消息总线差异化消费组（见 `docs/cluster-broadcast-rationale.md`） |

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
[`docs/server-side-ingestion.md`](docs/server-side-ingestion.md)。**

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
{"topic":"demo/LCU-P1/abc123def/property/down",
 "payload":"{\"cmd\":3011,\"requestId\":\"req-1\",\"data\":{}}"}
```

`topic` 必填；`payload` 字符串按 UTF-8 编码为 MQTT 消息体；record 的 key 仅用于分区。
**投递 QoS 是通道级运维策略**（`downlink` 项的 `qos`，默认 1 —— 指令类消息要 PUBACK
确认送达，高频可容忍丢失的通道显式配 0），消息体不携带；历史消息里的 `qos` 字段
仍可解析但不再生效。

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
见 [`docs/cluster-broadcast-rationale.md`](docs/cluster-broadcast-rationale.md)。

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

> 这里踩过两个坑，都不是理论问题：一是标记从 `false` 起步，而端口在容器就绪时已监听、
> 健康检查还要晚一点 —— 这段启动窗口内到达的 CONNECT 会看到 `available=false`，
> 于是会话该恢复的不恢复、该落盘的不落盘；二是标记全局共享，
> 在途镜像写入抛一次异常，同一时刻的「取得归属」和「写入会话」就被一起跳过。
> 详见 [`docs/reports/07-inflight-persistence.md`](docs/reports/07-inflight-persistence.md)。

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
接收方向 QoS 2 标识符集合同样不随会话迁移，见 `docs/reports/10-qos2-inbound-dedup.md` 的遗留说明。

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

**注意：这个组件不按 `redis.enabled` 条件装配。** 它曾经是
`@ConditionalOnProperty(redis.enabled=true)`，而 `ConnectHandler` 又直接注入它 ——
结果是**默认配置（`redis.enabled=false`）下整个应用起不来**。
根子在于用「Bean 存不存在」表达「功能开不开」：条件 Bean 一旦被直接注入就是启动期的定时炸弹。
现在 Bean 始终存在，用 `inflight-mode: off` 表达「不落盘」。

## MQTT 5.0

**v3.1.1 与 v5 双栈共存**：同一个端口按 CONNECT 里的协议级别自动分流，无需分别配置。

### 支持的范围

| 能力 | 说明 |
|---|---|
| **会话生命周期** | `cleanStart` + Session Expiry Interval（含 DISCONNECT 时改写，只能减小）。**`cleanStart=1` 且 `SEI>0` 这种组合也被正确处理** —— 会话从零开始建立、但建立之后按 SEI 保留 |
| **全套原因码** | CONNACK / SUBACK / UNSUBACK / PUBACK / PUBREC / PUBREL / PUBCOMP / DISCONNECT。注意**同一语义在两版本下码值不同**（未授权: v3 0x05 / v5 0x87） |
| **Receive Maximum** | 映射到发送窗口：`min(服务端 max-inflight, 客户端 Receive Maximum)` |
| **CONNACK 能力声明** | Maximum QoS=2、Retain Available=1、Wildcard Subscription Available=1、Shared Subscription Available=0、Subscription Identifiers Available=0、Topic Alias Maximum=0 |
| **Retain Handling** | 支持 v5 的三种取值（每次发 / 仅新订阅时发 / 从不发） |
| **空 clientId** | v5 由服务端分配并回传 `Assigned Client Identifier` |
| **Server Keep Alive** | 可配置的服务端强制心跳（默认不干预） |
| **遗嘱延迟** | Will Delay Interval 解析并落盘，**不做延迟投递**（见下） |

### 明确不支持的，以及为什么

| 项 | 处理方式 |
|---|---|
| **共享订阅** `$share/{group}/{filter}` | 返回 `0x9E` **明确拒绝**。两个理由: ① 它语法合法但**永远匹配不到任何发布**（`$share` 是保留前缀）—— 照常接受就是静默丢消息; ② **平台侧消费本就该读 Kafka 而不是订阅 MQTT**，服务端接入这条路上不该出现共享订阅（见 [`docs/server-side-ingestion.md`](docs/server-side-ingestion.md)）。它真正的服务对象是「需要分摊的 MQTT 侧消费者」，当前没有这种需求 |
| **入站 Topic Alias** | CONNACK 声明 `Topic Alias Maximum=0`；收到别名按协议错误 `0x82` 断开。**声明不支持然后明确拒绝**，好过默不作声地忽略 |
| **Subscription Identifier** | CONNACK 声明不支持 |
| **出站 Topic Alias / User Properties / 增强认证(AUTH)** | 见阶段报告，各有独立设计成本 |

### 三条反直觉的编解码事实（踩过才知道）

Netty 的 `MqttEncoder` 是 `@Sharable` 的**无状态单例**，它靠**Channel 属性**里的协议版本决定编包格式，
而那个 key（`MqttCodecUtil.MQTT_VERSION_KEY`）是**包级私有**的 —— 应用代码设不了，
只能由 `MqttDecoder` 在解出 CONNECT 后自己写入。由此推出三条必须遵守的约束：

1. **服务端不用改 pipeline**，v5 编包会在收到 v5 CONNECT 后自动生效。
2. **给 v5 连接构造报文时 `MqttProperties` 不能省**（哪怕空）。
   `encodePropertiesIfNeeded` 在 v5 下**无条件**编码 properties —— 空也会写出长度 0；
   但版本一旦没被正确识别，properties 会被整个丢掉，客户端把 payload 首字节当成长度而**整体错位**。
   **出站 `PUBLISH` 也在此列**，这是最容易被忽略的一处。
3. **属性的线宽由属性 id 决定，不由 Java 承载类型决定。**
   `MqttProperties` 只有 `IntegerProperty` / `StringProperty` / `BinaryProperty` 几种载体，
   没有按字节宽度区分的类；编码器按规范表推出每个 id 占几字节
   （SEI 4 字节 / Receive Maximum 2 字节 / Maximum QoS 1 字节）。

另外：`MqttProperty.reasonCode()` 返回**有符号 byte**，`0x8E` 读出来是 `-114`，比较时必须 `& 0xFF`。

### 出站报文统一走 `ReplyFactory`

所有回包与出站 PUBLISH 都经 `ReplyFactory` 构造，处理器只表达**语义**（成功 / 过滤器非法 / 未授权），
由它决定字节。这不是为了整洁 —— 是因为 **UNSUBACK 在两个版本下的要求正好相反**：
v3.1.1 必须**没有** payload，v5 必须**有** payload（逐项原因码）。
这种判断散到十来个处理器里，迟早漏一处，而漏了就是客户端解包错位。

### 「要不要保留会话」只有一个判据

```java
// SessionStore
public boolean isPersistent() { return expireSeconds > 0; }
```

不要用 `!cleanSession` 代劳。v5 存在 `cleanStart=1 && SEI>0` 的组合 ——
用 `cleanStart` 判会把它误判成「不保留」。v3.1.1 下 `cleanSession` 会被翻译成保留时长
（`=1` → 0，`=0` → 配置默认值），所以两版本在同一个判据下等价。

### 调试验证方式

`tools/mqtt5/MqttV5E2ETest.java` 是**裸 TCP** 实现的 v5 客户端（手工构造/解析字节），
`tools/run-mqtt5.sh` 一键运行。之所以不用 Netty 写测试客户端，除了「用不了」（见事实 1），
更重要的是**不该用**：若客户端复用被测服务端同一套编解码假设，两边对 v5 线格式的理解错了
也会一起错、测试照样通过。

> 用 `EmbeddedChannel` 做编解码时注意：它**没有**协议版本属性，
> 必须先把一条 v5 CONNECT 喂进 `MqttDecoder`，属性才会被写上；
> 直接 `writeOutbound` 得到的是 v3 字节。

## 当前状态与已知边界

**已完成**：

- **单节点 MQTT 3.1.1 broker** —— 接入、内存主题树路由、内存会话、QoS 0/1/2、
  保留消息、遗嘱消息、连接接管、WebSocket、HTTP API
- **Kafka 集群总线** —— 消息面（跨节点 pub/sub，每节点独立 group.id 的广播）
  与数据面（上行出口：服务端直连 Kafka 消费，可选按设备分区、可选独占以消除 N 倍扇出）
- **Redis 会话持久化** —— write-back 模式，会话属性 + 订阅关系一并存，
  客户端重连到任意节点都能完整恢复会话
- **跨节点连接接管通知** —— 会话归属转移时定向通知旧节点释放连接，
  补齐 MQTT 规范 [MQTT-3.1.4-2] 的跨节点一致性
- **离线消息队列** —— 持久会话的客户端离线期间，QoS 1/2 消息入队而非丢弃；
  重连到任意节点后按序投递（队列经 Redis 跨节点共享）
- **发送窗口与背压** —— MQTT 流控（未确认的 QoS 1/2 不超过 `max-inflight`）+
  每连接有界发送队列 + QoS 0 写缓冲保护
- **在途消息持久化** —— 已发送未确认的 QoS 1/2 消息落 Redis 镜像；
  节点崩溃重启或跨节点接管后，客户端重连时以 `dup=1` 重发且报文标识符保持不变
- **集群广播范围可配置** —— `broadcast-enabled` / `broadcast-filters` 三态：
  全部广播 / 按过滤器广播 / 不广播。用于「业务上只有设备与服务端通信」这类场景，
  同时守住两条边界：数据面上行不受影响、连接接管通道不受影响
- **管理面（Redis 控制平面）** —— broker 把节点概要、客户端注册表、订阅过滤器视图
  发布到 Redis；控制台直接读取，不直连任何 broker。另有命令通道供驱逐/踢下线，
  命令自带签发时间与最大时效
- **接收方向 QoS 2 去重** —— 客户端在拿到 PUBREC 前带 `dup=1` 重发是合法行为，
  服务端只投递一次 [MQTT-4.3.3-2]；PUBREL 后释放报文标识符，使同一标识符可再次使用。
  每客户端上限取服务端声明的 Receive Maximum 的两倍，防止「不发 PUBREL 的客户端」做内存放大
- **MQTT 5.0 与 3.1.1 双栈** —— 按 CONNECT 的协议级别自动分流；v5 的会话生命周期
  (`cleanStart` + Session Expiry Interval，含 DISCONNECT 改写)、全套原因码、
  Receive Maximum 映射到发送窗口、CONNACK 能力声明、Retain Handling、空 clientId 分配

**有意留待后续的项**：

| 项 | 说明 |
|---|---|
| **ACL** | 仅有认证，无授权。 |
| **Kafka 真实集群验证** | 代码与逻辑已单测覆盖，但**未在真实 Kafka 上跑过跨节点投递与接管**（见下方说明）。 |

**配套项目**：[`../jmqtt-admin`](../jmqtt-admin) —— 集群管理台（节点概览 / 客户端列表 /
主题列表 / 单条踢下线 / 节点排水）。后端 Spring Boot 2.7.18 + 前端 Vue 3，
构建为单 jar；它**只读 Redis、不直连任何 broker**。设计说明见
[`docs/admin-console.md`](docs/admin-console.md)。

**构建与运行已验证**（OpenJDK 21.0.12.1 + Maven 3.9.16）：

- `mvn clean package` → **BUILD SUCCESS**，零错误，**67 个单元测试通过**
- 启动实测：Tomcat 8922 + MQTT 1883 + WebSocket 8083 全部监听，**epoll 传输生效**
- 端到端协议测试 **11 / 11 通过**（`e2e/MqttE2ECheck.java`）
- 集群总线逻辑 **25 / 25 通过**（`cluster/ClusterBusTest.java`，含记录编解码、分区键选择、向前兼容回退、广播策略）
- **集群广播策略 6 / 6 通过**（`cluster/BroadcastPolicyTest.java`，含「接管通道不受广播开关影响」）
- **管理台与排水流程端到端 35 / 35 通过**（`tools/run-admin-drain.sh`：真实 Redis + 两节点 +
  控制台 + 12 个真实客户端；覆盖鉴权、节点发现、客户端/主题列表、踢下线、排水四步、
  会话与订阅跨节点恢复）
- **接收方向 QoS 2 去重 8 / 8 通过**（`store/InboundQos2StoreTest.java`，含「释放后可重用」与「大量重复不占配额」）
- **QoS 2 去重端到端通过**（`tools/run-qos2-dedup.sh`：重复 PUBLISH 不重复投递、PUBREC 照补、
  PUBREL 后同一标识符可重用）
- 会话持久化策略 **7 / 7 通过**（`session/persistence/SessionPersistenceTest.java`）
- 连接接管 **7 / 7 通过**（`session/SessionTakeoverServiceTest.java`）
- 投递与背压策略 **12 / 12 通过**（`cluster/InternalSendServerTest.java`）
- 启动冒烟 **2 / 2 通过**（默认配置启动 + Redis 不可达时仍能启动，见下）
- **跨节点会话恢复 7 / 7 通过**（`e2e/SessionRestoreCheck.java`，真实 Redis + 两节点）
- **离线消息跨节点投递 7 / 7 通过**（`e2e/OfflineQueueCheck.java`，真实 Redis + 两节点）
- **MQTT 流控窗口 3 / 3 通过**（`e2e/BackpressureCheck.java`，真实 broker + 只收不回的客户端）
- **在途消息崩溃恢复通过**（`e2e` 两阶段：投递未确认 → `kill -9` → 重启 → 重连收到 `dup=1` 且 packetId 不变）
- **MQTT 5.0 编解码往返 12 / 12 通过**（`tools/mqtt5/MqttV5CodecProbe.java`，含本 broker 实际发出的全部 10 个 CONNACK 属性）
- **MQTT 5.0 端到端 37 / 37 通过**（`tools/mqtt5/MqttV5E2ETest.java`，裸 TCP 手工线格式，
  覆盖 CONNACK 能力声明 / SUBACK / **UNSUBACK 带 payload** / 失败码细分 / 会话恢复 /
  空 clientId / 错误凭据 / **出站 PUBLISH 的 v5 线格式** / QoS2 全流程 / **Receive Maximum 窗口**）

**启动冒烟测试（`BrokerStartupDefaultConfigTest` / `BrokerStartupRedisUnreachableTest`）**：

| 用例 | 结果 |
|---|---|
| 用 `application.yml` **原样**配置（`redis.enabled=false`）把上下文拉起来 | PASS |
| Redis 启用但指向一个确定没有服务的端口，上下文仍能启动 | PASS |
| 该场景下 `InflightPersistence` 仍是可注入的 Bean、`mode()` 读得出配置 | PASS |
| 该场景下 `load()` 优雅降级为空列表，不把异常抛到 MQTT 处理路径上 | PASS |

这两条的存在理由很直接：前者校验「默认配置能启动」，后者校验
「Redis 挂掉不应该导致 broker 起不来」—— 这一类回归（条件 Bean 被硬注入、
必需属性缺失、循环依赖）**只有真正 refresh 一次上下文才看得见**，
而平时的 E2E 都显式启用了 Redis，覆盖不到。


| 用例 | 结果 |
|---|---|
| 3 客户端 CONNECT 并收到 CONNACK | PASS |
| 全新会话（含 cleanSession=0）的 `sessionPresent` 全为 false | PASS |
| 通配符订阅 `sensor/+/temp` 返回 QoS 1 | PASS |
| 非法过滤器 `sport/tennis#` 逐项返回 0x80 且合法项放行 | PASS |
| 含非法过滤器后连接保持存活 | PASS |
| 通配符订阅收到 `sensor/room1/temp` | PASS |
| 不匹配的主题未被投递 | PASS |
| 晚到的订阅者收到保留消息且 retain=1 | PASS |
| 异常断连触发遗嘱消息 | PASS |
| 正常 DISCONNECT 不触发遗嘱 | PASS |

**跨节点会话恢复**（`e2e/SessionRestoreCheck.java`，两节点共享 Redis）：

| 用例 | 结果 |
|---|---|
| 新会话在节点 A 上 `sessionPresent=false` | PASS |
| 节点 A 订阅 `restore/+/data` 完成 | PASS |
| `cleanSession=0` 断开后会话保留 | PASS |
| **重连到节点 B 时 `sessionPresent=true`（会话从持久层恢复）** | PASS |
| **订阅已被恢复：节点 B 发布后客户端收到消息** | PASS |
| `cleanSession=1` 的新会话 `sessionPresent=false` | PASS |
| `cleanSession=1` 断开后重连仍为 `sessionPresent=false`（未持久化） | PASS |

Redis 侧的实际数据印证了每一个环节：

```
jmqtt:session:{sess-restore}  →  node=nodeB     ← 归属已从 nodeA 原子转移到 nodeB
                                 clean=0, expire=7200, TTL=7196   ← 自动过期生效
jmqtt:subs:{sess-restore}     →  restore/+/data = 1              ← 订阅已持久化
```

另外已验证**降级行为**：Redis 不可用时 broker 照常启动、正常接入、返回
`sessionPresent=false`，本地投递不受影响。

**跨节点连接接管**（`session/SessionTakeoverServiceTest.java`）：

| 用例 | 结果 |
|---|---|
| 接管本节点未持有的 clientId 时不做任何事 | PASS |
| 接管时关闭连接并清理本地运行时状态 | PASS |
| **会话必须在 `channel.close()` 之前被移除**（否则误发遗嘱） | PASS |
| `close()` 时该 clientId 已不在注册表中 | PASS |
| 在途未确认消息被清理而不是泄漏 | PASS |

接管通知的代码路径也已在两节点测试中实际走到（日志可见），
且无集群总线时给出明确 ERROR：

```
已从持久层恢复会话: clientId=sess-restore 订阅数=1 原归属=nodeA
ERROR ConnectHandler - clientId=sess-restore 的会话归属在节点 [nodeA], 但集群总线未启用,
                      无法通知其释放旧连接。... 请同时开启 cluster-enabled 与
                      jmqtt.broker.kafka.enabled
```

**离线消息跨节点投递**（`e2e/OfflineQueueCheck.java`，两节点共享 Redis）：

| 用例 | 结果 |
|---|---|
| 节点 A 建立持久会话 | PASS |
| 节点 A 订阅 `offline/+/data` 完成 | PASS |
| 客户端离线（`cleanSession=0`，会话与订阅保留） | PASS |
| 离线期间在节点 A 发布 3 条 QoS 1 消息 | PASS |
| **重连到节点 B 时 `sessionPresent=true`** | PASS |
| **重连后收到全部 3 条离线消息** | PASS |
| **离线消息保持入队顺序** | PASS |

这条链路跨越了两个节点：消息在 **nodeA** 发布并由 nodeA 入队（因为订阅在 nodeA），
客户端重连到 **nodeB** 后从共享的 Redis 队列里领走 —— 证明队列确实跨节点生效。

**MQTT 流控窗口**（`e2e/BackpressureCheck.java`，`max-inflight=4`，客户端刻意不回 PUBACK）：

| 用例 | 结果 |
|---|---|
| 不确认时收到的条数**恰好等于窗口上限**（4 条） | PASS |
| 确认一批后应继续投递下一批 | PASS |
| 持续确认后应收完全部 20 条（队列未丢消息） | PASS |

broker 侧的指标同时印证了流控在工作：

```json
"backpressure": {
    "qos0NotWritableDropped": 0,
    "sendQueueEnqueued": 16,      // 20 条发布 − 窗口内 4 条 = 16 条排队
    "sendQueueFullDropped": 0
}
```

`sendQueueEnqueued=16` 与窗口大小 4 精确吻合 —— 这是流控确实生效的直接证据。

**在途消息崩溃恢复**（真实 `kill -9` + 重启，`inflight-mode=sync`）：

| 用例 | 结果 |
|---|---|
| 阶段一：收到 QoS 1 投递且 `dup=false`，刻意不 PUBACK 后断开 | PASS |
| 崩溃前 Redis 中已存在会话（`node` / `clean=0` / `expire=7200`）、订阅、在途镜像 | PASS |
| `kill -9` 模拟节点崩溃 | PASS |
| 阶段二：**重启后立即重连（不等健康检查）`sessionPresent=true`** | PASS |
| 阶段二：**以 `dup=1` 重发在途消息，且 `packetId` 与首次投递一致** | PASS |
| 两阶段日志均无 `ClassCastException`、无「Redis 不可用」降级 | PASS |

崩溃前后 Redis 里的镜像始终是同一份：

```
jmqtt:inflight:{if-client} → 1 = {"topic":"if/room1/data","qos":1,"payload":"aW4tZmxpZ2h0LXBheWxvYWQ="}
```

指标指纹在修复前后的对比（同一个用例）：

```
修前: inflightFlushCount=0  inflightFlushedEntries=0  inflightFlushFailed=3   ← 镜像写进去了, 计数却说失败
修后: inflightFlushCount=2  inflightFlushedEntries=1  inflightFlushFailed=0
```

> 修前那个矛盾是 **Lettuce 的 `ScriptOutputType.INTEGER` 解出的是 `Long` 而不是 `Integer`**
> 导致的 `ClassCastException`。最阴的地方在于 `eval` 是**先执行完脚本、再解码返回值**的 ——
> `DEL`+`HSET` 的副作用真的发生了，只有返回值在解码时炸掉，
> 于是「数据写进去了，调用方却认为失败」。这个异常还会顺手把全局健康标记打成不可用，
> 连带跳过同一时刻的会话落盘 —— 两个独立功能之间出现耦合。

**MQTT 5.0 端到端**（`MqttV5E2ETest`，裸 TCP 手工构造/解析字节）：

| 用例组 | 结果 |
|---|---|
| CONNACK 形态 + 7 项能力声明（含回传 Session Expiry Interval=7200） | PASS |
| SUBACK 形态 / 授予 QoS / 报文标识符回显 | PASS |
| **UNSUBACK 带 payload 且原因码 0x00**；未订阅过时回 0x11 | PASS |
| 失败码细分：共享订阅 0x9E、非法过滤器 0x8F | PASS |
| DISCONNECT 后重连 `sessionPresent=true` 且保留时长一致 | PASS |
| 空 clientId → `Assigned Client Identifier` | PASS |
| 错误密码 → 0x86 | PASS |
| **出站 PUBLISH 的 v5 线格式（topic/payload 解析无错位）** + PUBACK 原因码 | PASS |
| QoS 2 全流程（PUBREC → PUBREL → PUBCOMP，双向） | PASS |
| **Receive Maximum=1 → 只投递 1 条未确认消息**，确认后窗口闭环 | PASS |

类名为 `MqttE2ECheck` / `SessionRestoreCheck` / `OfflineQueueCheck` / `BackpressureCheck`
而非 `*Test`，因此**不会**进入 surefire 的默认执行集 —— 它们依赖真实运行的 broker。
（`MqttV5E2ETest` 同样放在 `tools/mqtt5/`，由 `tools/run-mqtt5.sh` 驱动。）

**尚未验证的部分（需要你在真实 Kafka 上跑）**：

集群总线的**逻辑**已被单元测试覆盖（出站分流、上行独占、record key 约定、
编解码往返、拒绝缺来源标识的记录）。但「两个 broker 节点经真实 Kafka 完成
跨节点投递」这一步**没有跑过** —— 生成环境的沙箱有 4GB 虚拟内存硬上限，
Kafka 4.x 在这一限制下无法启动（`unable to create native thread`）。

验证方法（两节点，同一 Kafka）：

```bash
# 节点 1
java -jar target/jmqtt-broker.jar --jmqtt.broker.id=node1 \
     --jmqtt.broker.port=1883 --server.port=8922 \
     --jmqtt.broker.cluster-enabled=true --jmqtt.broker.kafka.enabled=true

# 节点 2 (同机改端口即可)
java -jar target/jmqtt-broker.jar --jmqtt.broker.id=node2 \
     --jmqtt.broker.port=1884 --server.port=8923 \
     --jmqtt.broker.websocket-port=8084 \
     --jmqtt.broker.cluster-enabled=true --jmqtt.broker.kafka.enabled=true
```

然后在 node2 上订阅、在 node1 上发布，确认消息到达。重点核对两件事：

1. **跨节点到达**（订阅者连 node2，发布者连 node1）
2. **同节点恰好一次**（订阅者与发布者都连 node1，消息不能收到两次）
   —— 第二次到达就说明「跳过自身消息」失效了

## 许可

[Apache License, Version 2.0](LICENSE)

MQTT 报文的编解码使用 [Netty](https://netty.io) 官方的 `netty-codec-mqtt`
（v3.1.1 与 v5.0 均已支持）。
