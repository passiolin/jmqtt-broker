# jmqtt-broker Phase 8：MQTT 5.0 支持 —— 阶段报告

> 目标：让 v5 客户端能正确接入、订阅、发布、接收、断开，并支持 v5 的会话生命周期语义。
>
> 本轮最重要的产出不是「加了 v5 分支」，而是**先钉死了 Netty MQTT 编解码器的三个反直觉事实**——
> 猜错任意一条，出站报文的字节都会是坏的，而且症状是客户端整体错位解析，很难从日志看出来。

---

## 一、先把编解码的地基钉死

`MqttEncoder.INSTANCE` 是 `@Sharable` 的<b>无状态单例</b>。它凭什么区分 v3.1.1 与 v5？
这个问题的答案决定了整个实现的形态，所以先用反编译 + 编解码往返探针把它查清。

### 事实 1：版本存在 Channel 属性里，而那个 key 是包级私有的

```java
// MqttCodecUtil（包级私有类）
static final AttributeKey<MqttVersion> MQTT_VERSION_KEY;

static MqttVersion getMqttVersion(ChannelHandlerContext ctx) {
    MqttVersion v = ctx.channel().attr(MQTT_VERSION_KEY).get();
    return v == null ? MqttVersion.MQTT_3_1_1 : v;   // ← 读不到就默认 3.1.1
}
```

而 `MqttDecoder` 在解出 CONNECT 的协议级别后会调用 `setMqttVersion(ctx, …)` 写进去。

**结论**：服务端**不需要改 pipeline**，v5 编包会在收到 v5 CONNECT 后自动生效。
但反过来说，**我们自己设不了这个属性**——这直接决定了测试客户端不能用 Netty 写（见第五节）。

### 事实 2：v5 下 properties 是无条件编码的；v3 下一个字节都不写

```java
private static ByteBuf encodePropertiesIfNeeded(MqttVersion v, ByteBufAllocator a, MqttProperties p) {
    if (v == MqttVersion.MQTT_5) return encodeProperties(a, p);   // 空 properties 也会写出长度 0
    return Unpooled.EMPTY_BUFFER;                                 // v3: 什么都不写
}
```

**结论**：给 v5 连接构造报文时**必须**带上 `MqttProperties`（哪怕空）。
用两参构造（`NO_PROPERTIES` 哨兵）仍然会写出长度 0，这是对的；但如果**版本没被正确识别**，
properties 会被整个丢掉，客户端把 payload 首字节当成 properties 长度 —— **整条报文解析错位**。

出站 `PUBLISH` 也在此列，这是最容易被忽略的一处：v3 形态的 PUBLISH 发给 v5 客户端，字节就是坏的。

### 事实 3：属性的线宽由**属性 id** 决定，不由 Java 承载类型决定

`MqttProperties` 只有 `IntegerProperty` / `StringProperty` / `BinaryProperty` 这几种载体，
没有 `ByteProperty` / `TwoByteIntegerProperty`。编码器按规范表推出每个 id 该占几个字节：

| 线宽 | 属性 |
|---|---|
| 1 字节 | Maximum QoS(0x24)、Retain Available(0x25)、Topic Alias Maximum(0x22)… |
| 2 字节 | Receive Maximum(0x21)、Server Keep Alive(0x13)、Topic Alias(0x23) |
| 4 字节 | Session Expiry Interval(0x11)、Maximum Packet Size(0x27)、Will Delay(0x18) |
| UTF-8 / 二进制 | Assigned Client Identifier(0x12)、Content Type(0x03)… |

**结论**：用 `IntegerProperty` 装 Receive Maximum 是安全的（编码器会按 id 写 2 字节），
但这件事必须**实测验证**而不是推断 —— 用一个 4 字节载体去表达一个 2 字节属性，
一旦编码器真的按载体走，就会多写两个字节。所以加了
`MqttV5CodecProbe` 把本 broker 实际会发出的**全部 10 个 CONNACK 属性**做编解码往返。

### 附带确认的两个 API 细节

- `reasonCode()` 返回**有符号 byte**：`0x8E` 读出来是 `-114`。所有比较必须 `& 0xFF`。
  这个坑在本轮真的出现过一次（探针里把 `0x8E` 和有符号值直接比，误报失败）。
- `RetainedHandlingPolicy` 的常量名是 `DONT_SEND_AT_SUBSCRIBE`（不是 `DO_NOT_…`）；
  `MqttPropertyType.SUBSCRIPTION_IDENTIFIER_AVAILABLE` 是单数。

---

## 二、两个抽象：把版本差异收在一处

### `ReplyFactory` —— 回包字节只在一个文件里成形

v3.1.1 与 v5 的差别不是「多几个字段」，而是**同一语义在两个版本下字节完全不同**：

| 回包 | v3.1.1 | v5 |
|---|---|---|
| CONNACK | 2 字节可变头 | 追加 properties 长度字段 |
| PUBACK/PUBREC/PUBREL/PUBCOMP | 只有报文标识符 | 追加原因码 + properties 长度 |
| SUBACK | 返回码 / 失败码 0x80 | 原因码 / 细分失败码 0x8F、0x9E… |
| **UNSUBACK** | **必须没有 payload** | **必须有 payload**，逐项原因码 |
| DISCONNECT | 服务端不发，直接关 TCP | 可带原因码 + properties |

**`UNSUBACK` 是唯一两个方向都错的回包**：v5 漏 payload 会让客户端解包错位；
v3.1.1 多 payload 是规范明确禁止的。这种事如果散在十来个处理器里，
每处都要重新回答一次「v5 要不要带 properties」，迟早漏一处。

处理器现在只表达**语义**（成功 / 过滤器非法 / 不支持共享订阅），字节由工厂决定：

```java
ReplyFactory.pubAck(channel, packetId, ReplyFactory.SUCCESS)
ReplyFactory.subAck(channel, msgId, grantedQos)
ReplyFactory.unsubAck(channel, msgId, reasonCodes)
ReplyFactory.closeWithReason(channel, ReplyFactory.PROTOCOL_ERROR)   // v3 自动退化为直接关连接
ReplyFactory.publish(channel, topic, qos, packetId, payload, retain, dup)
```

`ConnAckReason` 用枚举表达语义，因为**同一语义在两版本下码值不同**：

| 语义 | v3.1.1 | v5 |
|---|---|---|
| 未授权 | 0x05 | 0x87 |
| 用户名/密码错误 | 0x04 | 0x86 |
| clientId 无效 | 0x02 | 0x85 |
| 协议版本不支持 | 0x01 | 0x84 |
| 服务端不可用 | 0x03 | 0x88 |

### `ConnectOptions` —— 握手时一次性拍平

v3.1.1 与 v5 表达同一件事的方式完全不同（会话时长、丢弃既有会话、客户端侧流控、最大报文）。
在握手时统一成一个模型，之后所有代码不再关心版本：

```java
public record ConnectOptions(
        int protocolVersion, boolean cleanStart, long sessionExpirySeconds,
        int receiveMaximum, int maximumPacketSize, int topicAliasMaximum,
        boolean requestProblemInformation, long willDelaySeconds) { … }
```

**两个缺省值陷阱**：

1. **Receive Maximum 的缺省值是 65535，不是 0。** 照字面取 0 会把发送窗口压成 1，
   吞吐直接塌掉且**不会报任何错**。
2. **Session Expiry Interval 必须封顶。** v5 允许 `0xFFFFFFFF` 表示「永不过期」，
   而 Netty 是按 `int` 承载的，读出来是 **-1** —— 直接拿去 `Math.min` 会把保留时长算成负数。
   这里归一为「服务端上限」，并且**把实际采用的值回在 CONNACK 里**（规范要求）。
   顺带的好处是不需要引入「永不过期」哨兵值，也就不会撞上「Redis `EXPIRE` 传天文数字报错」
   这类衍生问题。

---

## 三、会话生命周期的统一：「要不要保留」只有一个判据

v3.1.1 用 `cleanSession` 一个布尔同时表达「丢弃既有会话」和「断开后留不留」。
v5 把它拆成了两件事：`cleanStart`（怎么开始）+ Session Expiry Interval（留多久）。

于是出现了一个在两版本下都容易写错的组合：

```
v5: cleanStart=1 且 SEI > 0   →  会话从零开始建立, 但建立之后要按 SEI 保留
```

**只要判断条件是 `!cleanSession`，这个组合就会被误判成「不保留」。**
修复方式是把判据换成保留时长：

```java
/* SessionStore */
public boolean isPersistent() {
    return expireSeconds > 0;
}
```

v3.1.1 下 `cleanSession=1` 被翻译成保留时长 0、`=0` 被翻译成配置默认时长，所以两版本等价。
所有「要不要落盘 / 要不要缓存离线消息 / 断开时清不清」的判断统一改用它：

| 位置 | 原判据 | 现判据 |
|---|---|---|
| `SessionPersistence.shouldPersist` | `!isCleanSession()` | `isPersistent()` |
| `SessionPersistence.saveSession` | `isCleanSession()` | `!isPersistent()` |
| `InternalSendServer.enqueueOffline` | `isCleanSession()` | `!isPersistent()` |
| `DisconnectHandler` | `isCleanSession()` | `!isPersistent()` |
| `ConnectHandler` 恢复投递门控 | `!cleanSession` | `isPersistent()` |

**DISCONNECT 时的会话时长改写**（[MQTT-3.14.2-2]）：v5 允许客户端在断开时用同名属性改写它，
常见用法是「连的时候给大值以便断线重连、主动下线时改成 0 让服务端立刻清掉」。
规范限定**只能减小** —— 从 0 调大意味着客户端想多留一段自己都没承诺过的时间，按协议错误断开。

---

## 四、v5 的其他落地点

| 能力 | 实现要点 |
|---|---|
| **Receive Maximum → 发送窗口** | `SendBuffer` 窗口 = `min(broker.max-inflight, client ReceiveMaximum)`。v3.1.1 无此概念，缺省 65535 即不构成约束 |
| **CONNACK 能力声明** | Maximum QoS=2、Retain Available=1、Wildcard Subscription Available=1、**Shared Subscription Available=0**、**Subscription Identifiers Available=0**、Topic Alias Maximum=0。用声明代替让客户端试错 |
| **空 clientId** | v5 允许服务端分配并必须回 `Assigned Client Identifier`；v3.1.1 仍拒绝（0x02 / 0x85） |
| **共享订阅** | `$share/{group}/{filter}` 语法合法但**永远匹配不到任何发布**（`$share` 是保留前缀）。照常接受会变成静默丢消息 —— 明确返回 `0x9E` |
| **Retain Handling** | 支持 v5 的三种取值。默认行为会把所有保留消息重放一遍，对「只关心当前状态」的设备是纯浪费 |
| **Server Keep Alive** | 配置项（默认 0 = 不干预）。把心跳下限握在服务端手里，否则客户端声明 `keepAlive=600` 就能让死连接占着资源十分钟 |
| **入站 Topic Alias** | 声明 `Topic Alias Maximum=0`（明确不接受），收到别名按协议错误 `0x82` 断开 —— 而不是当成普通报文处理（那会把「空主题名 + 别名」误判为空主题名）|
| **Will Delay Interval** | 解析并落盘保存，**不做延迟投递**（见下）|

### 顺带修掉的一个行为问题

v5 的探针跑到「非法过滤器」这一条时连接被 broker 主动断开了。原因是
`SubscribeHandler` 在「全部过滤器都非法」时会 `channel.close()`。

这在 v3.1.1 下还算可辩解，但在 v5 下**与原因码机制自相矛盾**：
v5 专门提供 `0x8F`/`0x9E` 逐项失败码，设计意图就是让客户端知道问题在哪而**不必拆掉整条连接**；
断了连接，客户端拿不到 SUBACK，只能看到一次莫名其妙的断开。
而且订阅失败本来就不该升级成连接级事件 —— 同一连接上可能有别的正常订阅。

已改为：**只回逐项失败码，不断连接**。v3.1.1 的既有 E2E 断言（含非法过滤器后连接保持存活）不受影响。

---

## 五、测试：为什么客户端是裸 TCP 实现的

`MqttV5E2ETest` 用 `java.net.Socket` **手工构造与解析每一个字节**，完全不用 Netty 编解码器。

两个理由，后者更重要：

1. **用不了。** Netty 的版本属性 key 是包级私有的（见第一节）。服务端侧由解码器自动写入，
   客户端侧没有任何时机写它 —— 编解码器会一直按 3.1.1 处理，SUBSCRIBE / PUBACK 全都会编错。
2. **不该用。** 若测试客户端复用被测服务端**同一套编解码假设**，两边对「v5 报文长什么样」的
   理解错了也会一起错，测试照样通过。手工字节才是对线格式的**独立验证**。

测试里最关键的断言是第八组：若服务端给 v5 连接发的是 v3 形态的 PUBLISH
（可变头缺 properties 长度字段），订阅者解出的 topic 与 payload 会直接错位 ——
这条断言就是那个错位的探针。

### 验证结果

| 套件 | 用例数 | 结果 |
|---|---|---|
| 编解码往返探针（`MqttV5CodecProbe`） | 12 | 12 / 12 通过 |
| **MQTT 5.0 端到端（`MqttV5E2ETest`）** | **37** | **37 / 37 通过** |
| 单元测试（`mvn test`） | 44 | 44 / 44 通过 |
| 基础协议 v3.1.1（`MqttE2ETest`） | 11 | 11 / 11 通过 |
| 背压流控（`BackpressureTest`） | 3 | 3 / 3 通过 |
| 跨节点会话恢复（`SessionRestoreTest`） | 7 | 7 / 7 通过 |
| 离线消息跨节点投递（`OfflineQueueTest`） | 7 | 7 / 7 通过 |
| 在途消息崩溃恢复（`InflightRecoveryTest`） | 2 阶段 | 通过 |

v5 端到端覆盖的 10 组断言：

1. CONNACK 形态与 7 项能力声明（含回传 SEI=7200）
2. SUBACK 形态 + 授予 QoS + 报文标识符回显
3. **UNSUBACK 带 payload 且原因码 0x00**、未订阅过时回 **0x11**
4. 失败路径细分：共享订阅 0x9E、非法过滤器 0x8F
5. DISCONNECT 后重连 `sessionPresent=true` 且保留时长一致
6. 空 clientId → `Assigned Client Identifier`
7. 错误密码 → 0x86
8. **出站 PUBLISH 的 v5 线格式（topic/payload 解析无错位）** + PUBACK 原因码
9. QoS 2 全流程（PUBREC → PUBREL → PUBCOMP，双向）
10. **Receive Maximum=1 → 只投递 1 条未确认消息**，确认后窗口闭环继续投递

---

## 六、明确没做的项，以及原因

| 项 | 不做的原因 |
|---|---|
| **出站 Topic Alias** | 需要为每条连接维护 alias→topic 映射并处理回收。收益是省带宽，但在带宽不紧张时优先级低于正确性。已在 CONNACK 声明不支持 |
| **Will Delay Interval 的实际延迟投递** | 需要独立定时器，且要与「会话过期回收」划清职责边界（会话到期与遗嘱延迟到期是两件事）。**值已解析并落盘**，不会无声丢失语义 |
| **User Properties 透传** | 需要决定在哪些报文上透传、是否跨节点携带，牵扯集群记录格式。属独立课题 |
| **Subscription Identifier** | 意义在于区分同一条消息的多次投递（多订阅命中）。已在 CONNACK 声明不支持 |
| **共享订阅** | 正确实现涉及集群路由（订阅分摊到多节点，需要与 Kafka 总线的分区策略一起设计），不是单节点层面的改动。已声明不支持并明确拒绝 |
| **MQTT 5 的 AUTH 报文（增强认证）** | 与现有「用户名/密码 + 配置化凭据校验」是并列的另一套体系，且与 LDAP 等外部认证源一起才有意义 |
| **`emitProperties` 类的零拷贝优化** | 当前所有出站报文用 `NO_PROPERTIES`，没有引入额外开销，无需优化 |

---

## 七、代码改动清单

### 新增

| 文件 | 职责 |
|---|---|
| `protocol/ReplyFactory.java` | **核心抽象**：按连接版本构造全部回包（CONNACK/PUBACK/PUBREC/PUBREL/PUBCOMP/SUBACK/UNSUBACK/DISCONNECT/PUBLISH），并把 `ConnAckReason` 语义映射成版本相关的码值 |
| `protocol/ConnectOptions.java` | 把两版本的 CONNECT 语义拍平成统一模型（含 SEI 封顶、Receive Maximum 缺省值归一） |
| `test/…/BrokerStartupDefaultConfigTest` 等 | （上一阶段）启动冒烟 |

### 修改

| 文件 | 改动 |
|---|---|
| `handler/ChannelAttributes.java` | 新增 `PROTOCOL_VERSION`、`CONNECT_OPTIONS` 两个 Channel 属性 |
| `protocol/ConnectHandler.java` | 版本写入 Channel、`ConnectOptions` 解析、v5 会话生命周期、空 clientId 分配、CONNACK 能力声明、窗口取 min、按版本拒绝 |
| `protocol/SubscribeHandler.java` | SUBACK 按版本、失败码细分（0x8F/0x9E）、Retain Handling、**全量非法时不再断连** |
| `protocol/UnsubscribeHandler.java` | UNSUBACK 按版本（v5 带 payload）、原因码 0x00/0x11/0x8F |
| `protocol/PublishHandler.java` | PUBACK/PUBREC 按版本、拒绝未协商的入站 topic alias、v5 主题名错误码 |
| `protocol/PubRecHandler.java` / `PubRelHandler.java` | 回包改走 `ReplyFactory` |
| `protocol/DisconnectHandler.java` | v5 原因码解析、Session Expiry Interval 改写（只能减小）、`0x04 Disconnect with Will` 保留遗嘱 |
| `handler/MqttBrokerHandler.java` | DISCONNECT 传递整条报文（否则 v5 的原因码与属性丢失） |
| `session/SessionStore.java` | 新增 `isPersistent()`；`expireSeconds` 改为可变（DISCONNECT 可改写） |
| `session/persistence/SessionPersistence.java` | 落盘判据改用 `isPersistent()` |
| `session/persistence/redis/RedisSessionRepository.java` | 遗嘱延迟字段落盘 |
| `message/WillMessage.java` | 新增 `willDelaySeconds`（保留 4 参构造兼容 v3） |
| `cluster/InternalSendServer.java` | 出站 PUBLISH 改走 `ReplyFactory`；离线入队判据改用 `isPersistent()` |
| `config/BrokerProperties.java` | 新增 `topic-alias-maximum`、`server-keep-alive` |
| `resources/application.yml` | 上述两项配置及说明 |
| 两个测试类 | `BrokerProperties` 构造补两个新参数 |

### 驱动脚本

| 脚本 | 用途 |
|---|---|
| `tools/ssltest/MqttV5CodecProbe.java` | 编解码往返探针（12 项），钉死第一节的三个事实 |
| `tools/ssltest/MqttV5E2ETest.java` | 裸 TCP 的 v5 端到端客户端（37 项断言） |
| `tools/run-mqtt5.sh` | 一键跑 v5 端到端验证 |

---

## 八、沙箱约束备忘（跑这些验证时会撞到的）

- 所有 JVM（broker / `javac` / 测试客户端 / `javap`）都必须显式约束 metaspace
  （`-XX:CompressedClassSpaceSize` + `-XX:MaxMetaspaceSize`），否则报
  `Failed to reserve memory for metaspace`。**`javap` 也不例外**，要用 `-J` 传参。
- 12 核默认线程数会撞上 4GB 虚拟内存上限（`unable to create native thread`），
  需 `-XX:ActiveProcessorCount=2` + `--jmqtt.broker.worker-threads=2` + 收窄 Tomcat 线程池。
- 默认 charset 是 POSIX/ASCII，中文输出需 `-Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8`。
- `redis-cli` 依赖 `liblzf.so.1`，必须带 `LD_LIBRARY_PATH`；拼成字符串再加引号会被当成单个命令名，要用函数。
- 后台进程随所在 Bash 命令结束被回收 —— 整条链路必须放在同一个命令里。
- 多节点用例必须逐节点指定 `websocket-port`（默认 8083 会让第二个节点 bind 失败）。
- Netty `EmbeddedChannel` 里**没有**协议版本属性，所以 v5 报文的编解码必须先把一条 v5 CONNECT
  喂进 `MqttDecoder`，属性才会被写上。直接 `writeOutbound` 得到的是 v3 字节。

---

## 九、下一步

1. **ACL 授权**。目前只有认证没有授权：任何通过认证的设备都能订阅 `#`
   读到全部设备遥测、或往其他设备的指令主题发布。多租户与出海场景下这是实打实的缺口。
2. **服务端接收方向的 QoS 2 去重**。未保存「已接收未确认的 QoS 2」状态，重复 PUBLISH 会被重复投递。
3. **真实 Kafka 集群验证**（沙箱 4GB 虚拟内存上限导致 Kafka 起不来，跨节点投递与接管通知只有单测覆盖）。
4. 本轮明确留下的：出站 topic alias、遗嘱延迟投递、User Properties 透传、共享订阅、v5 增强认证。
