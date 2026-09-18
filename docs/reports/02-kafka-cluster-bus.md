# jmqtt-broker 第二阶段：Kafka 集群总线

> 时间：2026-09-11
> 前置：`jmqtt-broker-init-report.md`（单节点基线，已构建+启动+11 项协议测试通过）
> 项目路径：`/home/passio/WorkBuddy/mqtt-broker/jmqtt-broker`

---

## 一、本阶段交付

在单节点基线上接入 Kafka，把集群能力落成两条独立链路：

| 链路 | topic | 语义 | 消费方 |
|---|---|---|---|
| **消息面** | `jmqtt-cluster` | 跨节点 pub/sub，**每个节点都要读** | 各 broker 节点，每个用独立 `group.id` |
| **数据面** | `jmqtt-uplink` | 上行数据，**只需下游消费一次** | 数仓 / 业务系统 |

新增/改动文件：

```
cluster/
├── ClusterBus.java              改：增加数据面方法与设计约定说明
├── ClusterBusStats.java         新：总线指标接口(可选注入, 不污染 ClusterBus 语义)
├── InternalCommunication.java   改：出站按数据面/消息面分流 + uplink-exclusive 判断
└── kafka/
    ├── ClusterRecords.java      新：InternalMessage ↔ Kafka record 编解码
    └── KafkaClusterBus.java     新：总线实现(生产者队列 + 独立消费组 + 生命周期)
```

构成：**51 个主源文件**（原 48 + 3），测试 14 个（12 单测 + 2 端到端）。

---

## 二、实现上的六条硬约束

这些不是调优项，绕过去就会出事。每条都写在代码注释里。

### 1. 每节点独立 `group.id`

广播语义的唯一来源。Kafka 官方文档：

> If all the consumer instances have **different** consumer groups,
> then each record will be **broadcast to all the consumer processes**.

相同则退化为组内分摊（队列语义），跨节点投递直接失效。
留空时按 `{client-id-prefix}-{brokerId}` 自动生成，天然满足。
启动日志会打印实际使用的 `groupId`，部署后可直接核对。

### 2. record 的 key 必须是 MQTT 主题

两个理由，缺一不可：

- **分区顺序。** Kafka 只保证同一 partition 内有序。key 为空时默认是 sticky 分区器，
  同一主题的消息会散落不同 partition，消费端看到的就是乱序
  —— 对上行遥测可容忍，对下行指令（开关、控制、OTA 触发）是事故。
- **主题还原。** key 是消费端唯一能拿到 MQTT 主题的地方。

用 MQTT 主题做 key 后，默认分区器对 key 取 hash，同一主题恒定落到同一 partition。

### 3. 跳过 `brokerId` 等于自身的消息

发布节点在发布瞬间**已经完成过一次本地投递**。若再从总线把自己的消息取回来，
本节点订阅者会收到两次。

这是最容易漏的一条：漏了之后症状是「消息重复」，而不是「消息不到」，
在功能测试里很容易被放过。

### 4. ingress 不再出站（回环防护）

每节点消费全量的拓扑下，如果 ingress 注入的消息再被出站，会呈指数级重发：

```
节点1 发布 → Kafka 1 条
  节点2/3 各消费 1 条 → 各再出站 1 条 = Kafka 2 条
    三节点各消费 2 条 → 各再出站 2 条 = Kafka 6 条 → 18 → 54 → …
```

本实现从**结构上**排除：消费线程只调用 `InternalSendServer.sendPublishMessage()`
（纯本地投递），不经过任何出站路径。这是设计保证，不是运行时检查。

> 一个同类缺陷：桥接层如果在 ingress 侧触发发布钩子，
> 而 egress 侧对来源不做任何判断，一旦具备 ingress/egress 成对配置就会回环放大。

### 5. 出站走有界队列，满则丢弃

`publish()` 位于 MQTT 发布路径上，**绝不能被 Kafka 阻塞**。
所以它只往有界队列里塞，立刻返回；队列满时丢弃并计数。

这里的选择是明确的：**宁可丢跨节点消息，也不能让 Kafka 故障卡住本节点投递。**
本节点投递必须始终畅通 —— 那是 broker 的核心职责。

### 6. `max-block-ms` 必须设小

kafka-clients 的默认值 60s。如果生产者缓冲满或元数据不可用，
`send()` 会阻塞到该时长。放在发布路径上就是 60 秒的停顿。
本实现设为 1000ms，并同时收紧 `delivery.timeout.ms` / `request.timeout.ms`。

---

## 三、数据面：把「上行不走集群」落成开关

`uplink-exclusive=true` 时，命中 `uplink-filters` 的消息**只**进数据面、
不再进集群广播。

这不是调优参数，而是把第一轮设计里那条架构判断落成配置：

> 当上行订阅者集中在云端（而非分布在各 broker）时，`C × f × (N-1)` 的扇出
> **无法避免，且纯属浪费** —— 数据最终要进数据管道，绕一圈 MQTT 集群没有任何收益。

打开它可把扇出从 N 倍降到 1 倍。

**决定是否打开的正确方式**：先用真实流量测出「一条消息平均需要投递到几个节点」
（即第一阶段提出的 `E` 指标）。`E ≈ N` → 广播必要；`E ≈ 1` → 应该打开独占。

过滤器匹配复用了第一阶段已验证的 `TopicTrie`（把过滤器插进一棵树，用主题去匹配），
没有另写一套字符串比对。

---

## 四、可观测性

`/open/api/jmqtt/info` 新增 `clusterBus` 段：

| 指标 | 含义 | 什么时候该紧张 |
|---|---|---|
| `published` | 已入队的总数 | 基线参考 |
| **`dropped`** | 队列满而丢弃的条数 | **非零就在丢跨节点消息** |
| `received` | 从总线收到的条数 | 与各节点发布量之和比对, 偏差大说明有节点掉队 |
| `skippedSelf` | 被跳过的自身消息数 | 应约等于 `published`。远低于它说明来源标识有问题 |
| `uplink` | 数据面投递数 | 与上行流量比对 |
| **`outboxSize`** | 当前队列深度 | **持续接近容量说明马上要开始丢** |

`dropped` 与 `outboxSize` 这两个指标的价值在于：它们能在
「跨节点投递正在悄悄变差」彻底暴露成业务问题**之前**给出预警。
否则症状只会表现为「部分设备收不到指令」，排查成本极高。

---

## 五、验证结果

### 已通过

| 项 | 结果 |
|---|---|
| `mvn clean package` | **BUILD SUCCESS**，51 个主源文件零错误 |
| 单元测试 `ClusterBusTest` | **12 / 12 PASS** |
| 端到端协议测试 `MqttE2ECheck` | **11 / 11 PASS**（第一阶段，本次回归仍通过） |

单元测试覆盖的正是最容易出错的逻辑：

| 用例 | 验证内容 |
|---|---|
| 总线未启用时出站不产生投递 | 单机模式无副作用 |
| 未开数据面时只进集群广播 | 默认路径正确 |
| `uplink-exclusive=false` 时同时进两条链路 | 非独占语义 |
| **`uplink-exclusive=true` 时只进数据面** | **N 倍扇出降到 1 倍的关键判断** |
| 独占时未命中上行的消息仍走广播 | 下行指令不受影响 |
| 空主题消息被丢弃 | 防御性 |
| `fromLocal` 正确携带 `brokerId` | 回环防护的前提 |
| **record 的 key 是 MQTT 主题** | 分区顺序与主题还原 |
| 编解码往返不丢字段 | 含 qos/retain/dup/payload 二进制 |
| `clientId` 为空时解码为 null | 服务端主动下发场景 |
| 拒绝缺 `brokerId` 的记录 | 无法做回环防护时必须丢弃 |
| 拒绝 key 为空的记录 | 无法还原主题时必须丢弃 |

### 未通过：真实 Kafka 上的跨节点投递

**这一步没跑成。** 生成环境的沙箱有 **4GB 虚拟内存硬上限**（`ulimit -v 4194304`，
且作为 root 也无法解除）。Kafka 4.x 在该限制下启动即失败：

```
java.lang.OutOfMemoryError: unable to create native thread
  at kafka.server.KafkaRequestHandlerPool.createHandler
```

已尝试的缓解手段（减小堆、`-Xss512k`、SerialGC、把 network/io/background 线程数
降到 2）都不足以绕过 —— 瓶颈是虚拟地址空间，不是堆大小。

**这不影响代码正确性判断，但确实留了一个必须在真实环境验证的点。**
README 里给了两节点验证的具体命令和两个核对项：

1. **跨节点到达**（订阅者连 node2，发布者连 node1）
2. **同节点恰好一次**（订阅者与发布者都连 node1，消息不能收到两次）
   —— 收到两次说明「跳过自身消息」失效

---

## 六、下一步建议

1. **先在真实环境跑通上述两节点验证**（最重要，它验证的是本阶段全部设计的落点）
2. 然后按第一阶段报告排的顺序推进：Redis 会话持久化 → QoS 1/2 消息持久化
   → 每客户端有界队列 → MQTT 5.0
3. **部署前建议做的容量核对**：按 `group.id` 每节点独立的前提下，确认
   partition 数 ≥ 峰值 msg/s ÷ 30k（单 partition 吞吐量级），
   Kafka `retention.ms` 设 1~4 小时（消费端是实时的，不必留 7 天）
