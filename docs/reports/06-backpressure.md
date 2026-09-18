# jmqtt-broker 第六阶段：发送窗口与背压

> 时间：2026-09-11
> 前置：`jmqtt-broker-init-report.md`、`-kafka-`、`-redis-session-`、`-takeover-`、`-offline-queue-phase-report.md`
> 项目路径：`/home/passio/WorkBuddy/mqtt-broker/jmqtt-broker`

---

## 一、本阶段修的是可用性问题

不是性能问题。**一个坏客户端就足以把整个 broker 的堆吃光。**

原来的实现：

```java
boolean writable = channel.isWritable();
if (!writable) {
    if (qos == MqttQoS.AT_MOST_ONCE.value()) { return; }   // QoS 0 丢掉
    // QoS 1/2: 继续写
}
channel.writeAndFlush(publishMessage, ...);
```

对一个连上就不再读取 socket 的客户端，QoS 1/2 消息会**持续写入 Netty 的写缓冲**，
而 Netty 的写缓冲没有上限。只要有一个这样的客户端订阅了一个高频主题，
堆就会一直涨到 OOM —— 影响的是整个 broker 上所有客户端。

同时，`max-inflight` 与 `max-mqueue-len` 两个参数**在配置里存在但代码从未使用** ——
这一阶段把这两个缺口一起闭合。

**67 个源文件**（原 65 + 2），测试 **42 个单测 + 4 个端到端**。

---

## 二、两道闸门，职责不同

| 闸门 | 参数 | 作用 | 性质 |
|---|---|---|---|
| **在途窗口** | `max-inflight` | 未确认的 QoS 1/2 达到上限后不再发新的，剩下的排队等确认 | **规范行为**（MQTT 流控），也是慢消费者的主要保护 |
| **队列上限** | `max-mqueue-len` | 排队长度到顶后丢弃最旧的 | 内存保护，保证单连接内存有硬上限 |
| QoS 0 写缓冲满 | — | 直接丢弃并计数 | QoS 0 本就没有投递保证 |

### 为什么主要靠窗口，而不是靠队列

队列兜的是「已经决定要发」的消息，只能吸收**瞬时抖动**；
而窗口让整个投递节奏**跟随客户端的确认速度** —— 慢客户端自然就慢下来，不会堆积。

单靠队列的话，内存上限一到就必然开始丢消息；有窗口在，堆积本身就很难发生。
**两者不是替代关系，但主次必须清楚。**

---

## 三、流控的闭环在 PUBACK / PUBREC

这是最容易漏的一点：**窗口必须有释放路径，否则一旦用满就永久卡死。**

```java
// PubAckHandler —— QoS 1
dupPublishMessageStoreService.remove(clientId, messageId);
internalSendServer.releaseSendWindow(channel, messageId);

// PubRecHandler —— QoS 2
dupPublishMessageStoreService.remove(clientId, messageId);
dupPubRelMessageStoreService.put(new DupPubRelMessageStore(clientId, messageId));
internalSendServer.releaseSendWindow(channel, messageId);   // ← 在 PUBREC 就闭环
```

**为什么 QoS 2 的窗口在 PUBREC 释放而不是 PUBCOMP**：PUBREC 表示客户端已确认收到
PUBLISH，PUBLISH 的传输已经完成；之后的 PUBREL/PUBCOMP 走的是另一个流程，
不再占用「发送 PUBLISH」的窗口。

`releaseSendWindow` 内部做两件事：`buffer.release(packetId)` + `drainSendBuffer(channel)`。
后者把队列里能发的消息发出去；**队列清空后若仍有窗口，继续从离线积压里取** ——
否则一次重连领不完的积压会一直等到下次重连。

---

## 四、为什么把发送缓冲挂在 Channel 上

```java
channel.attr(ChannelAttributes.SEND_BUFFER).set(new SendBuffer(...));
```

它的生命周期**就是这条 TCP 连接**。挂在 Channel 上意味着连接一断自动回收，
不需要任何清理逻辑、不需要维护一张并发的 `clientId → buffer` 表、
也不需要在接管/断连/过期三条路径上分别记得清理它。

### 与离线队列的区别（重要）

| | 发送缓冲 | 离线队列 |
|---|---|---|
| 归属 | **连接** | **会话** |
| 存活期 | 连接存活期内 | 跨连接、跨节点 |
| 存储 | 进程内，挂 Channel | Redis（或进程内） |
| 触发条件 | 窗口用尽 / 客户端慢 | 客户端离线 |

两者共用「有界 + 丢最旧 + 计数」的口径，但**生命周期完全不同，不能混为一谈**。
一个体现是：`deliverPendingMessages` 会在窗口满时**停下**，把剩余积压留在持久队列里 ——
若继续取出来塞进连接级的发送队列，一旦连接断开这部分就没了，
反而丢掉了离线队列「跨连接存活」的意义。

---

## 五、可观测性

`/open/api/jmqtt/info` 新增 `backpressure` 段：

| 指标 | 含义 | 排查方向 |
|---|---|---|
| `qos0NotWritableDropped` | QoS 0 因写缓冲满被丢 | 客户端读取慢。QoS 0 无保证，丢弃符合预期 |
| `sendQueueEnqueued` | 因窗口用尽而排队的消息数 | 反映流控在多大程度上生效 |
| **`sendQueueFullDropped`** | **发送队列超限丢最旧** | **投递速度持续超过客户端消费速度，需要排查该客户端** |

`sendQueueFullDropped` 持续增长是最需要关注的 —— 它意味着有客户端的确认速度跟不上，
而窗口机制已经在生效（否则不会积压到队列上限）。换句话说：
**这个指标非零，说明窗口这层保护都兜不住了。**

---

## 六、验证结果

### 构建与单元测试

| 项 | 结果 |
|---|---|
| `mvn clean package` | **BUILD SUCCESS**，67 个源文件零错误 |
| `InternalSendServerTest` | **12 / 12 PASS** |
| `SessionTakeoverServiceTest` | 7 / 7 PASS（回归） |
| `SessionPersistenceTest` | 7 / 7 PASS（回归） |
| `ClusterBusTest` | 16 / 16 PASS（回归） |

`InternalSendServerTest` 用 Netty 的 **`EmbeddedChannel`** 而不是 mock ——
它能真实提供 `attr()` / `writeAndFlush()` / `isWritable()`，并且可以直接读回写出的报文。
**断言的是「真的写出去了什么」，而不是「调用过某个方法」。**

| 用例 | 验证内容 |
|---|---|
| 在线时直接投递，不入队 | 正常路径 + 真的写出了 PUBLISH |
| 离线 + 持久会话 + QoS 1 → 入队 | QoS 保证的核心场景 |
| 离线 + QoS 0 / `cleanSession=1` → 不入队 | 写入量控制 |
| 重连后投递积压并清空队列 | 核心链路 + 两条都写出 |
| 连接已断时停止投递，积压留下 | 中途断线不丢 |
| 离线队列超限丢最旧并计数 | 边界 + 可观测性 |
| **窗口用尽后不再发送，消息进入队列** | **MQTT 流控** |
| **收到确认后释放窗口并继续投递** | **流控闭环（最容易漏的一点）** |
| **QoS 0 不占用发送窗口** | 规范细节 |
| **发送队列超限丢最旧并计数** | 内存闸门 |

### MQTT 流控窗口（真实 broker + 只收不回的客户端）

**3 / 3 PASS。** 以 `max-inflight=4` 启动，订阅者刻意不回 PUBACK：

```
[PASS] 不确认时收到的条数恰好等于窗口上限(4 条)
[PASS] 确认一批后应继续投递下一批
[PASS] 持续确认后应收完全部 20 条(队列未丢消息)
```

第一条是本次最有价值的断言：**「只收不回」时收到的条数恰好等于窗口大小** ——
这正是 MQTT 流控的定义，也是朴素实现完全不具备的行为。

broker 侧的指标同时印证：

```json
"backpressure": {
    "qos0NotWritableDropped": 0,
    "sendQueueEnqueued": 16,      // 20 条发布 − 窗口内 4 条 = 16 条排队
    "sendQueueFullDropped": 0
}
```

**`sendQueueEnqueued=16` 与窗口大小 4 精确吻合** —— 这是流控确实生效的直接证据。

### 未验证

- 真实 Kafka 上的跨节点投递与接管（沙箱 4GB 虚拟内存上限，Kafka 4.x 起不来）
- 在途消息持久化（刻意未做，见下）

---

## 七、下一步

按优先级：

1. **量 QoS 1/2 的实际占比**，然后定在途消息持久化的方案（同步写 vs 写回）——
   这一项的决策依据在上一份报告里
2. **MQTT 5.0** —— 官方 codec 已支持，主要是补状态机分支
3. **ACL** —— 目前只有认证没有授权
4. **真实 Kafka 环境**补跑跨节点投递与接管的验证

至此 broker 的**可用性防护**（慢消费者 / 内存上限 / 流控）已经闭合，
剩下的是可靠性增强（在途持久化）、安全（ACL）与新协议版本。
