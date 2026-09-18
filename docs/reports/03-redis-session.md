# jmqtt-broker 第三阶段：Redis 会话持久化

> 时间：2026-09-11
> 前置：`jmqtt-broker-init-report.md`（单节点基线）、`jmqtt-broker-kafka-phase-report.md`（集群总线）
> 项目路径：`/home/passio/WorkBuddy/mqtt-broker/jmqtt-broker`

---

## 一、本阶段交付

会话持久化（write-back 模式）+ 一个顺带修掉的真实缺陷。

**58 个源文件**（原 51 + 7），测试 **19 个单测 + 2 个端到端**。

新增/改动：

```
session/persistence/
├── PersistedSession.java            新  从持久层恢复出来的会话(必带订阅)
├── ISessionRepository.java          新  持久层接口(含"不得进投递路径"的硬约束说明)
├── NoopSessionRepository.java       新  未启用时的空实现
├── SessionPersistence.java          新  策略层: 集中判断"什么时候该落盘"
└── redis/
    └── RedisSessionRepository.java  新  Redis 实现(Lua 原子接管 + 降级 + 健康检查)

session/
├── SessionExpiryReaper.java         新  过期会话的级联清理(修掉订阅泄漏)
├── ISessionStoreService.java        改  purgeExpired() → drainExpired()
└── SessionStoreService.java         改  去掉 @Scheduled, 只负责取出

config/
├── BrokerProperties.java            改  + RedisProperties
└── RedisConfig.java                 新  RedisClient(惰性连接)

protocol/
├── ConnectHandler.java              改  恢复会话 + 原子取归属 + 落盘
├── SubscribeHandler.java            改  订阅新增落盘
├── UnsubscribeHandler.java          改  订阅删除落盘
└── DisconnectHandler.java           改  cleanSession=1 时清持久层
```

---

## 二、键结构

```
{prefix}:session:{clientId}   Hash  node / clean / expire / created / touched / will
{prefix}:subs:{clientId}      Hash  topicFilter -> qos
```

**`{clientId}` 是 Redis Cluster 的 hash tag。** 两个键必须落在同一 slot ——
原子接管的 Lua 脚本要同时操作它们，不同 slot 会被集群直接拒绝。

**订阅单独一个 Hash**，而不是塞进会话的某个字段：订阅增删是 O(1) 的
`HSET`/`HDEL`；若序列化成一整个列表存单字段，每次变更都要读-改-写整份列表，
N 条订阅就退化成 O(N²)。

---

## 三、六条硬约束

这几条是设计约束而非调优项，改变它们会破坏语义。都写在代码注释与 README 里。

### 1. 会话记录必须包含订阅关系

MQTT 规范中 session state 包括「客户端的全部订阅」。只存会话属性不存订阅，
恢复后 broker 手里没有任何路由，却只能回 `sessionPresent=false` 让客户端重新订阅
—— 否则客户端会以为订阅仍有效，而消息永远到不了。**这是最难排查的静默故障：
不报错、不断连、只是消息不再到达。**

### 2. 只在低频路径读写，绝不进投递路径

允许：连接、重连、接管、订阅变更。
禁止：消息匹配、消息投递。

判断标准一句话：**这个方法会被每条消息调用吗？会，就不能放这里。**
一旦接进去就退化成「每次投递一次 Redis 往返」—— 那正是本架构要避免的陷阱。

### 3. `acquireOwner` 必须原子

拆成「先 `HGET` 再 `HSET`」的话，两个节点同时接管会双双读到空 owner，
于是都认为自己是归属方 → 两份路由、重复投递。用 Lua 在 Redis 里单线程执行天然互斥：

```lua
local prev = redis.call('HGET', KEYS[1], 'node')
redis.call('HSET', KEYS[1], 'node', ARGV[1], 'touched', ARGV[3])
if tonumber(ARGV[2]) > 0 then
  redis.call('EXPIRE', KEYS[1], ARGV[2])
  redis.call('EXPIRE', KEYS[2], ARGV[2])
end
if prev == false then return '' end
return prev
```

### 4. 只有 `cleanSession=0` 的会话落盘

规范里 `cleanSession=1` 明确要求丢弃既有会话，没有持久化价值。
**IoT 设备绝大多数用 `cleanSession=1`** —— 这个判断直接把 Redis 写入量砍掉一大截。
策略集中在 `SessionPersistence.shouldPersist()` 一处，处理器只表达意图。

### 5. Redis 不可用时降级为新会话

丢失会话的代价是客户端多发一轮 SUBSCRIBE（在本架构里那是**本地操作，零网络**）；
拒绝接入的代价是设备全部掉线。所以一律选可用性。

实现上：所有操作异常只记录并返回空结果，不抛给调用方；
`available()` 反映真实可达性，健康检查同时负责发现故障**与发现恢复**，
恢复后自动重新生效、无需重启 broker。

broker 启动时**不会**连接 Redis —— Redis 挂掉不应该导致 broker 起不来。

### 6. 不存消息

消息的归宿是 Kafka。放进 Redis 等于把投递路径接到中心化存储上。

---

## 四、顺带修掉的一个真实缺陷：会话过期导致订阅泄漏

实现过程中发现的，**与 Redis 无关，在单机模式下同样存在**：

原来 `SessionStoreService.purgeExpired()` 只把过期会话从表里删掉，
**不会清理它的订阅关系**。后果是订阅永远留在主题树里，指向一个已不存在的客户端：

- 主题树节点数与订阅计数单调增长，长期运行后匹配成本与内存被历史订阅拖垮
- 持久层记录虽然靠 TTL 自己过期，但期间任何节点重连该 clientId 都会
  把它当成有效会话恢复 —— 等于「复活了一个应该被遗忘的会话」

修法：把过期清理抽成独立的 `SessionExpiryReaper`（每 30 秒），
级联清理**会话 + 订阅 + 在途消息 + 持久层记录**。

不能放在 `SessionStoreService` 里，因为它需要清除订阅与持久层，
而 `SessionPersistence` 又依赖 `ISessionStoreService` —— 放进去会形成循环依赖。

---

## 五、验证结果

### 构建与单元测试

| 项 | 结果 |
|---|---|
| `mvn clean package` | **BUILD SUCCESS**，58 个源文件零错误 |
| `SessionPersistenceTest` | **7 / 7 PASS** |
| `ClusterBusTest` | **12 / 12 PASS**（回归） |
| 端到端协议 `MqttE2ECheck` | **11 / 11 PASS**（回归） |

`SessionPersistenceTest` 覆盖的是**策略**而非 Redis 本身：

| 用例 | 验证内容 |
|---|---|
| 持久层不可用时任何操作都不落盘 | 降级路径不留副作用 |
| **`cleanSession=1` 的会话不落盘** | **写入量的大头是否被正确砍掉** |
| `cleanSession=0` 的会话订阅变更与建立都落盘 | 正常路径完整 |
| 进程内无该会话时不落盘 | 不给幽灵会话写数据 |
| `restore` 透传持久层结果 / 无记录返回 null | 恢复语义 |
| 会话销毁时清除持久层记录 | 清理路径 |
| `acquireOwnership` 透传旧 owner | 接管决策的数据来源 |

### 跨节点会话恢复（真实 Redis + 两节点）

**7 / 7 PASS。** 环境：Redis 8.0.6，两个 broker 节点（nodeA:1883、nodeB:1884）共享该 Redis。

```
[PASS] 新会话在节点 A 上 sessionPresent=false
[PASS] 节点 A 订阅 restore/+/data 完成
[PASS] 已发送 DISCONNECT(cleanSession=0, 会话应保留)
[PASS] 重连到另一个节点 B 时 sessionPresent=true(会话从持久层恢复)
[PASS] 订阅已被恢复: 节点 B 发布后客户端收到消息
[PASS] cleanSession=1 的新会话 sessionPresent=false
[PASS] cleanSession=1 断开后重连仍为 sessionPresent=false(未持久化)
```

第 5 项是本次最有价值的一条 —— 它验证的不只是「回了一个 true」，
而是**订阅真的被写回了节点 B 的本地主题树**（通过实际投递证明）。
只回 `sessionPresent=true` 而不恢复订阅，正是规范陷阱的形态。

Redis 侧实际数据：

```
jmqtt:session:{sess-restore}
  node      nodeB          ← 归属已从 nodeA 原子转移到 nodeB
  clean     0
  expire    7200
  TTL       7196           ← 自动过期生效
jmqtt:subs:{sess-restore}
  restore/+/data  1        ← 订阅已持久化
```

`node=nodeB` 这一行同时证明了第 3 条约束（原子接管）确实生效。

### 降级行为

已意外验证：某次测试运行时 Redis 未存活（后台进程被回收），
结果是 broker **照常启动、正常接入、返回 `sessionPresent=false`**，
本地投递不受任何影响。这正是第 5 条约束期望的行为。

### 未验证

- **跨节点接管通知**：归属字段已正确写入，但旧节点上的连接要等它自己的
  心跳超时才释放。这是当前唯一已知的 MQTT 规范偏差，缺的是节点间通道
  —— 应当复用集群总线发一条控制消息。
- Kafka 真实集群的跨节点投递（见第二阶段报告，沙箱虚拟内存限制）

---

## 六、下一步建议

按优先级：

1. **跨节点接管通知** —— 唯一的已知规范偏差。归属数据已就位，
   只需在集群总线上加一条控制消息（按目标 nodeId 定向、旧节点收到后
   关闭本地连接并清理）。工作量不大，但能把规范一致性补齐。
2. **QoS 1/2 消息持久化** —— 注意与本次会话持久化的边界：
   消息**不要**放进 Redis 的会话记录。
3. **每客户端有界发送队列** —— 背压的最后一环。
4. **MQTT 5.0** —— 官方 codec 已支持，主要是补状态机分支。

**部署时留意**：Redis 应当与 Kafka 同 DC，**不跨 DC 复制** ——
跨 DC 的话又会把「跨区数据同步」引回来，与既定路线冲突。
跨 DC 场景下每个 DC 各自一套 Redis，会话归属也只在 DC 内成立。
