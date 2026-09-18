# jmqtt-broker 第四阶段：跨节点连接接管通知

> 时间：2026-09-11
> 前置：`jmqtt-broker-init-report.md`、`jmqtt-broker-kafka-phase-report.md`、`jmqtt-broker-redis-session-phase-report.md`
> 项目路径：`/home/passio/WorkBuddy/mqtt-broker/jmqtt-broker`

---

## 一、本阶段交付

补齐第三阶段留下的**唯一已知 MQTT 规范偏差**。

**60 个源文件**（原 58 + 2），测试 **28 个单测 + 2 个端到端**。

```
cluster/
├── ClusterBus.java            改  + publishTakeover(定向控制消息)
├── TakeoverListener.java      新  接管接收侧接口(含三条实现约束的说明)
└── kafka/
    ├── ClusterRecords.java    改  + kind 区分(publish/takeover)与目标节点
    └── KafkaClusterBus.java   改  + publishTakeover + 消费侧分发

session/
└── SessionTakeoverService.java 新  接管接收侧实现(关闭连接 + 清理本地状态)

protocol/
└── ConnectHandler.java        改  归属转移时发出接管通知
```

---

## 二、问题

MQTT 规范 [MQTT-3.1.4-2]：

> 如果 ClientId 代表的客户端已经连接到服务端，服务端**必须**断开既有连接。

单节点内这件事由 `ConnectionRegistry.register` 顺手完成 —— 它返回被替换掉的旧连接，
调用方关闭即可。

跨节点则完全没有机制：客户端从 nodeA 重连到 nodeB 时，
**nodeA 上那条连接对此一无所知**，会一直存活到它自己的心跳超时（可能几分钟）。

这期间的后果是同一个 clientId 在两个节点上同时在线：

- 共享订阅消息可能被投递两次
- 下行指令被执行两次
- 两个节点各有一份订阅指向同一个客户端

症状是**偶发、难复现、且不报错**——典型的「只有出问题才知道存在」的那类缺陷。

---

## 三、设计

### 定向控制消息，而非广播

复用同一条 `cluster-topic`，用 `jmqtt-kind` 头区分两类记录：

| kind | 用途 | 频率 |
|---|---|---|
| `publish` | 普通消息转发 | 高 |
| `takeover` | 连接接管指令 | 极低 |

接管消息带 `jmqtt-target-node`，**只有目标节点会执行**，其他节点直接跳过。

为什么不广播：只有真正持有该连接的节点需要动作，其他节点收到也无事可做。
广播会让 N 个节点都去做一次「查注册表 → 没查到 → 打日志」的空转。

### 流程

```
客户端 CONNECT 到节点 B (cleanSession=0)
  │
  ├─ restore()           ← 从 Redis 恢复会话与订阅
  ├─ acquireOwner()      ← 原子读到旧 owner = "nodeA", 并写入 "nodeB"
  │                        (顺序关键: 必须在此之前不能 saveSession,
  │                         否则 node 字段已是 nodeB, 读不出旧 owner)
  ├─ publishTakeover(clientId, "nodeA")
  ├─ saveSession()
  └─ CONNACK sessionPresent=true

节点 A 消费到 takeover 记录
  ├─ kind == TAKEOVER 且 target == 本节点? 是 → 继续
  ├─ sessionStoreService.remove(clientId)      ← 必须最先
  ├─ connectionRegistry.unregister(clientId, channel)
  ├─ channel.close()
  └─ 清理订阅 + 在途消息
     (绝不触碰 SessionPersistence)
```

---

## 四、四条实现约束

### 1. 必须先移除会话，再关连接

反过来的话，`channel.close()` 触发 `channelInactive`，
而那里会读取会话里的遗嘱消息并发布 —— **但客户端只是换了节点，并没有死。**

先移除会话后，`channelInactive` 读不到会话，自然不会发遗嘱。
这条约束用测试直接钉住了：在 `close()` 被调用的那一刻检查会话是否还在。

### 2. 绝不删除持久层记录

收到接管通知时，该会话**已经归属新节点** —— 新节点刚把它写进 Redis。
这里删掉等于把新节点刚建立的会话摧毁，客户端下次重连会拿到
`sessionPresent=false`，订阅全部丢失。

为了从**结构上**杜绝这种错误，`SessionTakeoverService` 刻意不注入
`SessionPersistence` —— 想写错都没有入口。这一点写在了类的 javadoc 里。

### 3. key 用 clientId

与该客户端的普通消息落在同一 partition，保持顺序一致。

### 4. 定向而非广播

见上文。

---

## 五、已知限制

> **（2026-09-12 更新）** 本节写于第 7 阶段（在途消息持久化）之前。PUBLISH 在途的
> **持久镜像现在会在接管时保留**（`detach` 而非 `removeByClient`），客户端在接管方节点
> 重连时镜像被加载回来，以 `dup=1` 且原报文标识符重发 —— 见
> `docs/reports/07-inflight-persistence.md`。剩下的缺口是 PUBREL 半程状态：
> 它不进镜像，接管后服务端不会再主动重发未送达的 PUBREL，流程靠客户端超时重发
> PUBREC/PUBREL 收敛（新节点对两者都幂等）；接收方向 QoS 2 标识符集合的遗留
> 见 `docs/reports/10-qos2-inbound-dedup.md`。以下为原始记录，留作背景。

**旧节点上该客户端的在途未确认消息（QoS 1/2）会被丢弃。**

它们只存在于旧节点的进程内存里，而连接已经不在那里了 —— 无法投递，也不该继续留着。
接管时会打 `warn` 说明丢弃条数，而不是静默清理：

```
接管清理丢弃了 3 条在途未确认消息: clientId=device-1。
这些消息只存在于本节点内存, 连接已不在本节点。要做到不丢需要 QoS 1/2 消息持久化(待办)
```

要做到不丢，需要 QoS 1/2 消息持久化 —— 已经在待办列表里，
**注意不要把消息放进 Redis 的会话记录**，那是两条不同的链路。

### 一个配置陷阱：共用 Redis 但未启用集群总线

这种配置下接管无法通知。实现会打 ERROR 明确指出问题，而不是静默降级 ——
因为它的症状是极难排查的偶发重复投递，静默是最差的选择：

```
clientId=sess-restore 的会话归属在节点 [nodeA], 但集群总线未启用, 无法通知其释放旧连接。
旧连接会存活到自身心跳超时, 期间该客户端实际处于双连状态。
请同时开启 cluster-enabled 与 jmqtt.broker.kafka.enabled。
```

---

## 六、验证结果

### 构建与单元测试

| 项 | 结果 |
|---|---|
| `mvn clean package` | **BUILD SUCCESS**，60 个源文件零错误 |
| `SessionTakeoverServiceTest` | **5 / 5 PASS**（新增） |
| `ClusterBusTest` | **16 / 16 PASS**（12 → 16，新增 4 条控制消息用例） |
| `SessionPersistenceTest` | 7 / 7 PASS（回归） |

新增的 5 条接管用例，重点钉住两条最容易写错的约束：

| 用例 | 验证内容 |
|---|---|
| 接管未持有的 clientId 时不做任何事 | 幂等, 不误伤其他会话 |
| 接管时关闭连接并清理本地运行时状态 | 正常路径完整 |
| **会话必须在 `channel.close()` 之前被移除** | **防止误发遗嘱** |
| `close()` 时该 clientId 已不在注册表中 | 关连接时已不可被投递 |
| 在途未确认消息被清理而不是泄漏 | 已知限制的行为被固化 |

新增的 4 条控制消息用例：

| 用例 | 验证内容 |
|---|---|
| 普通消息被识别为 `PUBLISH` 类别 | kind 判别 |
| 接管指令往返能还原 clientId / 目标节点 / 发起方 | 编解码完整 |
| 缺失目标节点的接管指令被拒绝 | 否则所有节点都会去关连接 |
| key 用 clientId（与普通消息同 partition） | 顺序一致性 |

### 端到端回归与路径验证

**跨节点会话恢复 7 / 7 PASS**（真实 Redis 8.0.6 + 两节点）。

同时**接管通知的代码路径已在真实两节点上被实际走到**，日志可见：

```
DEBUG SessionPersistence - 已从持久层恢复会话: clientId=sess-restore 订阅数=1 原归属=nodeA
ERROR ConnectHandler     - clientId=sess-restore 的会话归属在节点 [nodeA],
                           但集群总线未启用, 无法通知其释放旧连接。...
```

这两行同时证明了三件事：会话恢复带回了订阅、`acquireOwner` 正确读出了旧 owner、
接管通知的决策分支被执行且给出了可操作的错误信息。

### 未验证

**接管消息经 Kafka 的实际传输与旧节点的实际响应** —— 与第二阶段同样的原因：
沙箱有 4GB 虚拟内存硬上限，Kafka 4.x 起不来。

需要在真实 Kafka 上补的验证（两节点 + Redis + Kafka）：

1. 客户端连 nodeA（cleanSession=0），订阅、断开
2. 客户端重连 nodeB（cleanSession=0）
3. 确认 nodeB 日志出现「已通知节点 [nodeA] 释放 ...」
4. **确认 nodeA 日志出现「收到跨节点接管通知」，且该连接确实被关闭**
5. 通过 nodeA 的 HTTP API 确认连接数与订阅数都已回落

第 4 步是这次要验证的核心 —— 前三步在无 Kafka 的环境下已经能走到。

---

## 七、下一步

按优先级：

1. **QoS 1/2 消息持久化** —— 同时解决两个问题：节点重启丢在途消息、
   接管时旧节点丢弃在途消息。注意与会话持久化的边界：消息**不要**放进 Redis 会话记录。
2. **每客户端有界发送队列** —— 背压的最后一环。
3. **MQTT 5.0** —— 官方 codec 已支持，主要是补状态机分支。

至此 MQTT 3.1.1 的规范一致性已经没有已知偏差。剩下的都是可靠性增强与新协议版本。
