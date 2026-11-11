# jmqtt-broker（v3-lite）

面向 **IoT 设备 ↔ 服务端通信** 的 MQTT Broker。基于 Netty + Spring Boot，单模块 Maven 工程。

## 这个分支解决什么问题

标准 MQTT 的能力集合里，有一大类是给「客户端之间互相订阅」用的：主题通配符、
共享订阅、订阅标识符、集群内的全量广播扇出。而 IoT 平台的真实形态通常是另一种：

```
设备  --上行-->  broker  --Kafka-->  服务端（业务系统 / 数仓 / 规则引擎）
设备  <--下行--  broker  <--Kafka--  服务端
```

**没有设备订阅另一台设备的主题**，也没有 APP 订阅设备。设备只做两件事：上报，
以及接收针对自己的指令。所有跨端逻辑都在服务端完成，服务端连的是 Kafka，不是 MQTT。

这个判断一旦成立，集群里最贵的那一项就可以整块去掉——

### 被去掉的成本：跨节点广播扇出

常规集群做法是「每个节点都消费全量消息，再各自对本地订阅树匹配」。它能保证无论
订阅者连在哪个节点都能收到，代价是 **N 个节点 = N 倍消息复制**：一条设备上报，
三个节点各自反序列化、各自查一次订阅树、各自丢弃。

当订阅者只有服务端（而服务端连 Kafka）时，这 N-1 份工作全是浪费。所以本分支的默认
部署形态是：

| 流量 | 走哪 | 是否扇出 |
|---|---|---|
| 设备上行 | 直接进 Kafka，服务端消费 | **1 份** |
| 服务端下行指令 | Kafka → broker → 目标设备 | 1 份 |
| 连接接管通知 | 独立的定向通知通道 | 只发给旧节点 |

上行独占由 `uplink-exclusive: true` 落地：命中上行过滤器的消息写一次 Kafka 就不再
进集群广播。实测收益（双节点 4C8G、30 万连接、含回显）：

| | 广播全开 | 上行独占 |
|---|---|---|
| broker CPU 峰值 | 98.0% / 95.3% | 85.8% / 91.2% |
| PUBACK P50 / P99 | 1174 / 25,233 ms | **684 / 2,970 ms** |
| 下行丢包 | 472,142（30.8%） | **0** |
| 建连失败 | 1,811 | 911 |

CPU 只降了 10~14%，但 P99 快了 8.5 倍、下行从丢三成变成零丢。原因是 98% 那条线
已经越过了良性边界：节点忙着消费别的节点的消息，自己的设备确认反而处理不过来，
形成正反馈。**那 12% 不是省下来的资源，是压垮系统的最后一点负载。**

> 如果将来确实出现「必须有跨节点 MQTT 订阅」的场景（比如 APP 直连 MQTT 订阅设备状态），
> 把 `broadcast-enabled` 打开即可，代价就是上面那张表的左列。两条路都存在，
> 本分支把默认值选在更常见的这一侧。

### 顺带的裁剪

协议只保留 **3.1.1（兼容 3.1）**。原因不是性能——实测 v5 分支根本不在热路径上——
而是**每处回包只需维护一种字节形态**：

| 报文 | 3.1 / 3.1.1 | v5 |
|---|---|---|
| CONNACK | `20 02 00 00` | `20 03 00 00 00` |
| UNSUBACK | `B0 02 00 07` | `B0 03 00 07 00` |
| SUBACK | `90 03 00 07 01` | `90 04 00 07 00 01` |

双栈意味着 CONNACK / PUBACK / SUBACK / UNSUBACK / DISCONNECT 每处都要按版本分支。
v5 客户端在握手阶段被明确拒绝，而不是接受后静默降级——放行它只会让客户端收到
结构错位的坏字节。

3.1 与 3.1.1 在报文层面等价（CONNECT payload 结构相同，各类回包字节布局相同），
实测两者编出的回包逐字节一致，因此走同一条路径。差别只有两条：3.1 的 clientId
限 1~23 字符（解码器直接拒），以及 3.1 的 CONNACK bit0 是保留位而非 Session Present
（本实现这一位只在会话不存在时为 true，两种解读下都不产生误判）。

## 性能

### 单机容量

| 场景 | 规格 | 结果 |
|---|---|---|
| 纯心跳（零业务消息） | 8C16G | **40 万连接**，零失败零重连，CPU 峰值 48%，内存 6.4G（≈11KB/连接） |
| 业务闭环（上行 + 回显） | 8C16G | 30 万连接 / 5,505 TPS，上行零丢失零重复 |
| 业务闭环（上行 + 回显） | 2×4C8G 集群 | 30 万连接 / 5,412 TPS（广播关闭） |
| 纯 MQTT 层（无 Kafka/Redis） | 4C8G | 30 万连接 / 6,371 TPS，PUBACK P99 **186ms** |

### 与同类 broker 的对照

同机同场景（4C8G、10/20/30 万连接、每设备 60s 一条 QoS1、不回显）：

| | jmqtt-broker | EMQX 5.8.9 | RMQTT 0.24.0 |
|---|---|---|---|
| 连接建立 | 300,000 / fail 0 | 255,036 / fail 5,554 | 300,000 / fail 0 |
| TPS @30 万 | 6,371 | 5,438（仅 54% 确认） | 6,375 |
| PUBACK P99 | 186.6 ms | 52,076 ms | 29.9 ms |
| CPU 峰值 | 45.7% | 99.5% | — |

> 口径说明：EMQX 用默认配置（未调 process_limit / vm.args），jmqtt 用调优后配置。
> 这个对比不能直接对外引用，只能作为量级参考。

### 容量规划参考

| 指标 | 数值 |
|---|---|
| 每 vCPU 承载连接（纯心跳） | ≈ 13 万 |
| 每 vCPU 承载连接（业务闭环） | ≈ 3.5 万（保守 3 万） |
| 每 vCPU 闭环指令 TPS | ≈ 600 |
| 每连接内存 | ≈ 11~23 KB |
| 上行独占 + 3×32C64G 广播模式 | ≈ **100~120 万设备**（每 2 分钟双向一条） |

压测方法与完整数据见 [docs/benchmark.md](docs/benchmark.md)，
工具在 [passiolin/jmqtt-bench](https://github.com/passiolin/jmqtt-bench)（Go 单二进制，
自研 MQTT 3.1.1 协议栈 + 开环加压 + 三集合差丢失归因）。

## 存储选型：建议用 Valkey

Redis 在本项目里只做**持久会话与在线归属**，不承载消息投递——投递路径全部走进程内存。
用到的命令只有基础几类：

```
HSET / HGET / HGETALL / HDEL    会话属性、订阅关系
LPUSH / LTRIM                   离线消息队列（有界）
INCR / EXPIRE / DEL             报文标识符、TTL
SET / GET                       在途镜像快照
EVAL                            少量原子操作（归属转移）
```

**这些 Valkey 全部兼容，零代码改动即可替换。**

推荐 Valkey 的理由：

1. **许可**：Valkey 是 Linux 基金会下的 BSD-3 项目，源自 Redis 7.2.4 的分支。
   Redis 在 7.4 起改为 RSALv2 / SSPLv1 双许可，自建对外提供服务属于受限场景。
   Valkey 没有这个问题，也不会有后续许可变动。
2. **性能与内存**：延续 Redis 的 IO 多线程与数据结构，同等负载下表现一致；
   本项目实测 Valkey 9.0.4 在压测中 CPU 峰值 45%、内存 16%。
3. **协议与客户端完全兼容**：RESP 协议、命令语义、Lettuce 客户端全部无需改动，
   切换只是改一个 host/port。

配置示例（`application.yml`）：

```yaml
jmqtt:
  broker:
    redis:
      enabled: true
      host: 127.0.0.1        # 换成 Valkey 地址即可
      port: 6379
      mode: standalone       # standalone / sentinel / cluster 三种都支持
```

部署建议：

- **关闭 RDB / AOF**。这里存的是会话元数据与在途镜像，不是业务数据；
  broker 侧已有 `write-back` 与过期回收，落盘只会带来 fsync 抖动。
  `valkey.conf` 里 `save ""` + `appendonly no`。
- **`maxmemory-policy` 用 `noeviction`**。会话被静默驱逐会导致客户端重连后
  收不到离线消息，且不会有任何报错——宁可让写入失败暴露出来。
- **别把它当缓存用**。如果同时还要拿它做业务缓存，请分实例，不要和 broker 共用。

## 设计

每条 PUBLISH 从收包、路由到写给订阅者，只接触进程内存。Redis 只出现在低频路径
（连接、重连、接管、订阅变更），且都是 write-back 镜像；Kafka 只在集群总线与
上行数据面上，同样不在路由路径上。收益有两层：外部往返不进单条消息的延迟，
外部故障也不拖垮本地投递。

### 路由

订阅关系放在内存前缀树里，匹配 `a/b/c` 就是沿树走三层，与订阅总数无关——
连接从 1 万涨到 40 万，匹配步数不变。读路径无锁（弱一致迭代，新订阅最多晚一次
投递生效），写路径单把锁只串行化本节点的订阅变更。

### 集群

节点完全对称、没有 leader，也没有「先协调再投递」的环节：

- 跨节点 pub/sub 走 Kafka，消费端先拿主题对本地订阅树匹配一次，
  没有本地订阅者的消息在反序列化之前就丢弃；
- 会话归属是一次 Redis 原子操作（读出旧 owner、写入本节点），
  归属转移时定向通知旧节点释放连接，而不是仲裁或投票。

设计细节见 [docs/cluster-broadcast-rationale.md](docs/cluster-broadcast-rationale.md)。

## 技术栈

| 项 | 版本 / 说明 |
|---|---|
| Java | 21 |
| Spring Boot | 2.7.18 |
| 构建 | Maven，**单模块** |
| 网络 | Netty 4.1.x（版本由 Spring Boot 的 netty-bom 管理） |
| 编解码 | `io.netty:netty-codec-mqtt` |
| Redis 客户端 | Lettuce（不用 spring-data-redis，避免多余抽象层） |
| 协议 | **MQTT 3.1.1，兼容 3.1**；v5 在握手阶段拒绝 |
| 会话存储 | Redis / **Valkey**（协议兼容） |
| 集群总线 | Kafka（可选） |
| 许可 | Apache License 2.0 |

## 快速开始

### 构建与运行

```bash
mvn clean package
java -jar target/jmqtt-broker.jar
```

开发期也可以 `mvn spring-boot:run`。默认监听：

| 端口 | 用途 |
|---|---|
| 1883 | MQTT over TCP |
| 8083 | MQTT over WebSocket（路径 `/mqtt`） |
| 8922 | HTTP API / Actuator |

### 连接

默认开启认证：

```
username: jmqtt
password: jmqtt
```

> 生产环境把 `jmqtt.broker.auth-password` 改成 SHA-256 摘要形式：
> `java -cp target/jmqtt-broker.jar online.ipuff.jmqtt.auth.util.PwdUtil <明文密码>`

### HTTP API

> **仅限内网/受信网络**：`/open/api/jmqtt/send` 不做鉴权，可向任意 topic 注入消息。
> 请用防火墙把 8922 限制在内网或运维网段。

```bash
# 运行状态（连接数、会话数、订阅数、主题树节点数、背压触发次数）
curl http://127.0.0.1:8922/open/api/jmqtt/info

# 服务端主动下发
curl -X POST http://127.0.0.1:8922/open/api/jmqtt/send \
     -H 'Content-Type: application/json' \
     -d '{"topic":"device/123/cmd","qos":1,"message":"hello"}'
```

## 能力清单

- **接入与路由** —— 内存前缀树、QoS 0/1/2、保留消息、遗嘱消息、连接接管、
  WebSocket、TCP 与 WS 可分别关闭
- **持久会话** —— write-back 到 Redis/Valkey，会话属性与订阅关系一并存，
  重连到任意节点都能完整恢复
- **离线消息队列** —— 持久会话的客户端离线期间 QoS 1/2 入队（有界，超出丢最旧），
  重连后按序投递
- **发送窗口与背压** —— 未确认 QoS 1/2 不超过 `max-inflight`，每连接有界发送队列，
  QoS 0 受写缓冲保护
- **在途消息持久化** —— 已发未确认的 QoS 1/2 落镜像，节点崩溃或跨节点接管后
  以 `dup=1` 重发且报文标识符不变
- **接收方向 QoS 2 去重** —— 拿到 PUBREC 前带 `dup=1` 重发是合法的，服务端只投递一次
- **集群** —— Kafka 总线、跨节点连接接管通知、广播范围三态可配
  （全部 / 按过滤器 / 不广播）
- **管理面** —— 节点概要、客户端注册表、订阅视图发布到 Redis；
  控制台只读 Redis，不直连 broker。另有命令通道做驱逐/踢下线
- **可观测** —— `/actuator/prometheus`，指标覆盖连接、投递、丢弃、背压、集群总线

## 已知边界

这个分支为了场景匹配做了取舍，以下是需要提前知道的：

1. **v5 客户端连不上** —— 收到「协议版本不支持」。若存在发 v5 CONNECT 的固件版本，
   需先确认。
2. **v5 的拒绝没有 CONNACK** —— Netty 解码层对 v5 报文结构不匹配时抛的异常不在
   `handleDecodeFailure` 的识别范围内，表现为连接被直接关闭。排查时容易误判成网络问题。
3. **ACL 拒绝的 QoS 1/2 不回确认** —— 客户端会超时重发直到 ACL 变更或断开。
   这是 3.1.1 没有逐条错误通道的固有限制。
4. **SUBACK 失败码只有 0x80** —— 无法区分「过滤器非法」与「未授权」。
5. **UNSUBACK 不带逐项结果** —— 逐项成败只进日志。
6. **广播关闭时跨节点订阅静默收不到** —— `broadcast-enabled: false` 的前提是
   「集群内不存在跨节点 MQTT 订阅」。若判断错了，症状是消息不报错地丢失。
7. **无配额体系** —— 没有全局/单 IP 最大连接数、单客户端发布速率限制、
   单客户端订阅数上限。需要靠前置 LB 与设备侧约束。
8. **Spring Boot 2.7.18 已 EOL** —— 基线无安全补丁，对公网暴露需评估。

## 与 main 分支的差异

`main` 保留 3.1.1 + 5.0 双栈，本分支只保留 3.1.1（兼容 3.1）。

| 项 | main | v3-lite |
|---|---|---|
| 协议 | 3.1.1 + 5.0 双栈 | 3.1.1，兼容 3.1；v5 握手阶段拒绝 |
| `ReplyFactory` | 324 行，9 处版本分支 | **191 行，无版本分支** |
| `ConnectOptions` | 8 个字段（含 SEI / Receive Maximum / Packet Size / Alias / Will Delay） | **2 个字段**（cleanSession / sessionExpiry） |
| 出站回包 | 按连接版本构造两套形态 | 只有一种形态 |
| 订阅处理 | 含 Retain Handling 与 v5 细分失败码 | 只有 0x80 一个失败码 |
| ACL 拒绝 | v5 回 0x87 原因码 | 只有静默丢弃 |

其余能力（QoS 0/1/2、保留消息、遗嘱、连接接管、持久会话、离线队列、发送窗口与背压、
在途持久化、Kafka 集群总线、Redis 会话持久化、管理面）两个分支一致。

## 文档

| 文档 | 内容 |
|---|---|
| [docs/configuration.md](docs/configuration.md) | 配置详解：集群/数据面/下行通道/Redis 持久化/背压/离线队列/ACL/TLS |
| [docs/cluster-broadcast-rationale.md](docs/cluster-broadcast-rationale.md) | 集群为什么选广播而不是复制元数据 |
| [docs/cluster-broadcast-assessment.md](docs/cluster-broadcast-assessment.md) | Kafka 差异化消费组做集群广播的方案评估 |
| [docs/benchmark.md](docs/benchmark.md) | 压测方法、完整数据与容量结论 |
| [docs/monitoring/](docs/monitoring/) | Prometheus 告警规则 + Grafana 面板 |
| [docs/server-side-ingestion.md](docs/server-side-ingestion.md) | 平台侧为什么读 Kafka 而不是订阅 MQTT |
| [docs/admin-console.md](docs/admin-console.md) | 管理台（jmqtt-admin）设计说明 |
| [docs/reports/](docs/reports/) | 各能力的设计与实现报告 |

**配套项目**：[passiolin/jmqtt-admin](https://github.com/passiolin/jmqtt-admin) ——
集群管理台（节点概览 / 客户端列表 / 单条踢下线 / 节点排水）。
Spring Boot 2.7.18 + Vue 3，构建为单 jar，**只读 Redis、不直连任何 broker**。

## 许可

[Apache License, Version 2.0](LICENSE)

MQTT 报文编解码使用 [Netty](https://netty.io) 官方的 `netty-codec-mqtt`。
