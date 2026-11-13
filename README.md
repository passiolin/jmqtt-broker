# jmqtt-broker（v3-lite）

面向 **IoT 设备 ↔ 服务端通信** 的 MQTT Broker。Netty + Spring Boot，Java 21，单模块 Maven 工程。

## 场景与取舍

```
设备  --上行-->  broker  --Kafka-->  服务端（业务 / 数仓 / 规则引擎）
设备  <--下行--  broker  <--Kafka--  服务端
```

设备只做两件事：上报，以及接收发给自己的指令。没有设备订阅另一台设备，也没有 APP 订阅设备；跨端逻辑在服务端完成，服务端读 Kafka，不订阅 MQTT。

端与端的实时交互（音视频、信令、大文件）走 WebRTC / P2P：服务端只做信令与会话协调，NAT 打洞失败才回落 TURN 中继。这条流量不进 broker，比「消息灌进 broker 再扇出到 N 个节点」少一整条服务端链路。

订阅者集中在服务端之后，集群里最贵的一项成本可以整块去掉——跨节点全量广播扇出。常规集群让每个节点消费全量消息、各自对本地订阅树匹配一遍，N 个节点就是 N 倍复制。本分支默认：

| 流量 | 路径 | 份数 |
| --- | --- | --- |
| 设备上行 | 直连 Kafka，服务端消费 | 1 |
| 服务端下行指令 | Kafka → broker → 目标设备 | 1 |
| 连接事件 | Kafka 连接事件 topic，各节点各读一遍 | N（各节点只处理自己持有的 clientId） |

上行独占由 `uplink-exclusive: true` 落地：命中上行过滤器的消息写一次 Kafka，不再进集群广播。广播有三态（全部 / 按过滤器 / 关闭），语义与取舍见 [docs/cluster-broadcast-rationale.md](docs/cluster-broadcast-rationale.md)。

> 广播三态的档间对比数据已撤回：采集在超分宿主上进行，后半程受资源争抢影响出现停摆，绝对值与档间差异都不可稳定复现。需要对比时应在独占硬件上重测。

## 能力

| 域 | 能力 |
| --- | --- |
| 接入 | MQTT 3.1.1，兼容 3.1；v5 在握手阶段拒绝。WebSocket 可单独关闭 |
| 路由 | 内存前缀树、QoS 0/1/2、保留消息、遗嘱消息、同 clientId 连接接管 |
| 会话 | 持久会话（`cleanSession=0`）、有界离线队列（超出丢最旧）、`max-inflight` 发送窗口 |
| 可靠性 | 在途 QoS 1/2 镜像落 Redis（off / sync / async），崩溃或接管后以 `dup=1`、原报文标识符重发；入站 QoS 2 去重 |
| 背压 | 每连接有界发送队列，超出按 QoS 分级丢弃；QoS 0 受写缓冲水位保护 |
| 认证授权 | 内置静态账号；HTTP 认证与 HTTP ACL（EMQX 契约），带决策缓存与故障熔断 |
| 集群 | Kafka 广播三态、上行多路由、下行通道、连接事件流、跨节点接管 |
| 管理 | 节点 / 客户端 / 订阅视图发布到 Redis，控制台只读不直连 broker；命令通道做驱逐、踢下线与主题抓取 |
| 可观测 | `/actuator/prometheus`，覆盖连接、投递、丢弃、背压、集群总线 |

## 设计

每条 PUBLISH 从收包、路由到写给订阅者，只接触进程内存。Redis 出现在连接、重连、跨节点接管、订阅变更、离线入队这些低频路径，且都是 write-back 镜像；Kafka 只在集群总线与上行数据面上，不在路由路径上。外部往返不进单条消息的延迟，外部故障也不拖垮本地投递。

持久会话接入要串行做 restore、归属写入、会话保存几次 Redis 往返。这些阻塞调用按 clientId 分片到独立线程池，同一客户端保序、不同客户端并行，不占 Netty event loop——连接风暴时 event loop 不会集体卡在等 Redis 上。

### 路由

订阅关系在内存前缀树里，匹配 `a/b/c` 沿树走三层，与订阅总数无关：连接从 1 万涨到 40 万，匹配步数不变。读路径无锁（弱一致迭代，新订阅最晚在下一次投递生效），写路径一把锁只串行化本节点的订阅变更。

### 集群

节点完全对称，没有 leader，也没有「先协调再投递」的环节：

- 跨节点 pub/sub 走 Kafka；消费端先拿主题对本地订阅树匹配，没有本地订阅者的消息在反序列化前丢弃；
- 会话归属是 Redis 里的一次原子写（读出旧节点、写入本节点），不是仲裁或投票；
- 跨节点接管复用连接事件流：每个节点用独立 group 消费 `connected` 事件，发现同一 clientId 已在别的节点上线、且本地连接明显更老时释放本地旧连接。比较依据是事件里的 `connected_at`，时钟差落在 2s 容差内就不动，交给 keepalive 兜底——漏踢晚收敛几十秒，误踢会打断正常连接。

会话跨节点恢复依赖 Redis：不开 Redis 时集群仍有广播与接管，会话状态留在各节点内存。

## 性能

### 口径

方法见 [docs/benchmark.md](docs/benchmark.md):全链路闭环——设备上行 → broker → Kafka → backend 消费 → 逐条回显下行 → 设备收回,seq 三集合差做丢失归因。有两轮口径:

- **2026-11-12 同宿主轮(baseline)**:8C16G 被测 VM(JVM 12G)与压测机、Kafka、Valkey 同跑一台 16 核 32 线程的 PVE 宿主(2698B v3)。宿主平时还有十余台业务 VM,vCPU : 物理线程 ≈ 2.3 : 1;该轮趁宿主空闲采集(load1 < 1),更早的轮次在争抢中拿到过明显不同的结果。
- **2026-11-13/14 分离宿主轮(当前口径)**:broker 独占宿主(dell-pve-0,2683 v4)上的 8C16G VM;压测机 ×3、backend、Kafka、Valkey 在另一台宿主(2698B v3)。每档前重启 broker 并清 Valkey/topics;结果取 300s soak 稳态(目标 TPS = 连接数/60)。

- **绝对值受虚拟机规格限制。** 实例能用的只有自己那几核，宿主有闲置算力不等于被测实例拿得到。容量上限由规格给出，本文不做容量承诺。
- **看 CPU 要拆 `iowait`。** 在 EMQX 那轮实测里，CPU 显示 99.6% 时只有 38% 在计算，55% 挂在等盘上。CPU 高 + `iowait` 低才是算力瓶颈，加核有效；`iowait` 高先查 IO。
- **轮次间抖动大。** 同配置同脚本，PUBACK P99 在不同轮次能差出数倍，跨轮次只做方向性比较。
- **大规模压测先检查负载端余量。** 50 万连接档曾用 2 台压测机失败:接入 ~45 万后 dev VM CPU 100%、宿主 load 43,重连与下行积压全来自负载端;加第三台压测机后同场景干净通过。broker 无压力 ≠ 数字可信,资源表要整张看。

### 分离宿主轮(当前口径)

8C16G(12G 堆,G1 50ms),持久会话,256B,QoS 1,逐条回显,300s soak:

| 连接 | soak TPS(目标=连接数/60) | 上行/回显丢失 | ACK RTT P50/P95 | 全环 RTT P50/P95 | broker CPU 均/峰 |
| --- | --- | --- | --- | --- | --- |
| 10 万 | 1,664 / 1,667 | **0 / 0** | 0.5 / 1.3 ms | 54.8 / 73.4 ms | 10.2% / 45.0% |
| 20 万 | 3,330 / 3,333 | **0 / 0** | 0.5 / 4.9 ms | 47.7 / 72.9 ms | 18.4% / 48.1% |
| 30 万 | 4,998 / 5,000 | **0 / 0** | 0.5 / 12.1 ms | 52.2 / 86.0 ms | 25.0% / 55.0% |
| 40 万 | 6,657 / 6,667 | **0 / 0** | 0.5 / 17.6 ms | 58.7 / 222.3 ms | 32.3% / 59.6% |
| 50 万 | 8,328 / 8,333 | **0 / 0** | 0.7 / 20.6 ms | 64.5 / 331.4 ms | 41.4% / 68.6% |

五档守恒均 100.0000%(发送 = PUBACK = backend 消费 = 回显 = 设备收到;50 万档 4,739,759 条),连接零失败零重连。

- **50 万连接 / 8.3k TPS 闭环下 broker 仍有余量**:CPU 均值 41%、峰值 68.6%(8C 口径),GC Young 停顿 30–48ms、无 Full GC。
- **内存远低于旧估算**:50 万连接 RSS ≈ 4.7G(≈10KB/连接;接入路径 Redis 异步化后),16G 规格下内存不再是先顶到的配置项。旧轮的 28KB/连接估算不适用于当前构建。
- **下一个边界信号是下行投递尾延迟**:下行腿 P95 随连接数从 ~20ms 涨到 281ms(50 万档 × 8.3k/s 回显),全环 P95 相应 73→331ms。评估容量时把下行算进去。

### 同宿主轮(baseline,2026-11-12)

| 场景 | 规模 | 结果 |
| --- | --- | --- |
| 业务闭环（上行 + 逐条回显，256B，QoS 1，持久会话） | 8C16G，10 万连接 | 1,799 TPS；上行零丢失；回显丢 13 条；PUBACK P50/P95 0.5/5.8 ms；端到端 P50/P95 48/76 ms |
| 业务闭环（同上） | 8C16G，20 万连接 | 3,751 TPS；上行零丢失；回显丢 213 条（0.03%）；PUBACK 0.8/15.8 ms；端到端 58/97 ms |
| 纯心跳（仅 PINGREQ，60s 一条） | 8C16G，42 万连接 | 接入零失败、零重连；CPU 峰值 67.5%；内存峰值 75%（12G/16G）。稳态只维持约 35s，不是长稳 |
| 上行纯吞吐（无回显） | 8C16G，1 万连接 | 16 / 24 / 32k msg/s：丢 1 / 2 / 2 条，PUBACK P50 0.6–0.8 ms |

业务闭环的先饱和点是回显 / 下行投递链路：双向 ladder 全程上行丢失 0.55%，回显从 16k/s 起积压。评估容量时把下行算进去。该轮内存按约 28KB/连接估算（42 万档实测 12G/16G）；当前构建接入路径 Redis 异步化后，50 万档实测 RSS ≈ 4.7G（≈10KB/连接）。

消费分派用共享队列 + worker 争抢，并行度只由 worker 数决定，与 topic 分区数解耦。同场景与「分区固定 worker」对比（200k 档）：

| 指标 | 分区固定 worker | 共享队列争抢 |
| --- | --- | --- |
| 指令 TPS | 3,739 | 3,751 |
| ACK RTT P95 / P99 | 218 / 810 ms | 16 / 221 ms |
| 端到端 RTT P95 / P99 | 2,450 / 8,288 ms | 97 / 529 ms |
| broker CPU 均值 / 峰值 | 29.7% / 74.4% | 24.1% / 59.7% |

慢分区不再独占 worker，尾延迟与 CPU 都降下来；代价是同一分区内不再保证顺序（业务不需要）。

集群档间对比在本宿主上不可信，语义见 [docs/cluster-broadcast-rationale.md](docs/cluster-broadcast-rationale.md)。

### 与同类 broker 的对照

三台被测 VM 同宿主、同脚本、各 4 vCPU，每设备 60s 一条 QoS 1、不回显，只取 10 / 20 万档的 PUBACK P99：

| | jmqtt-broker | EMQX 5.8.9（调优后） | RMQTT 0.24.0 |
| --- | --- | --- | --- |
| 10 万档 P99 | 53.7 ms | 7.6 ms | 数 ms |
| 20 万档 P99 | 54.3 ms | 21.6 ms | 数 ms |

EMQX 的延迟控制更好（Erlang 轻量进程 + 抢占式调度），jmqtt 在同档可用。权威对比需要独占物理机、双方各自推荐配置、多轮取中位数。

### 压测方法与完整数据

见 [docs/benchmark.md](docs/benchmark.md)；工具在 [passiolin/jmqtt-bench](https://github.com/passiolin/jmqtt-bench)（Go 单二进制，自研 MQTT 3.1.1 协议栈 + 开环加压 + 三集合差丢失归因）。

## 存储：建议 Valkey

Redis / Valkey 在本项目只做持久会话与运行视图：会话属性、订阅关系、离线队列、在途镜像、管理面视图。消息投递路径全部走进程内存。用到的命令就这几类：

```
HSET / HGET / HGETALL / HDEL    会话属性、订阅关系
LPUSH / LTRIM                   离线消息队列（有界）
INCR / EXPIRE / DEL             报文标识符、TTL
SET / GET                       在途镜像快照
EVAL                            会话归属的原子操作
```

这些命令 Valkey 全部兼容，换过去只需改 host / port。推荐 Valkey 的理由是许可与兼容性：Valkey 是 Linux 基金会下的 BSD-3 项目，源自 Redis 7.2.4 分支；Redis 从 7.4 起改为 RSALv2 / SSPLv1 双许可，自建对外提供服务属于受限场景。RESP 协议、命令语义、Lettuce 客户端都不用改。压测中 Valkey 9.0.4 的 CPU 峰值 45%、内存占用 16%。

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

- **关闭 RDB / AOF**。这里存的是会话元数据与在途镜像，不是业务数据；broker 侧已有 write-back 与过期回收，落盘只带来 fsync 抖动。`valkey.conf` 里 `save ""` + `appendonly no`。
- **`maxmemory-policy` 用 `noeviction`**。会话被静默驱逐的后果是客户端重连后收不到离线消息，且没有任何报错——宁可让写入失败暴露出来。
- **不要和业务缓存共用实例**。同时拿它做业务缓存时，请分实例。

## 技术栈

| 项 | 版本 / 说明 |
| --- | --- |
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

```bash
mvn clean package
java -jar target/jmqtt-broker.jar
```

开发期也可以 `mvn spring-boot:run`。默认单机：Redis 与 Kafka 都关着，会话、订阅、离线队列在进程内存。持久会话开 `jmqtt.broker.redis.enabled`；集群开 `jmqtt.broker.cluster-enabled` + `jmqtt.broker.kafka.enabled`，会话要跨节点恢复再加 Redis。

| 端口 | 用途 |
| --- | --- |
| 1883 | MQTT over TCP |
| 8083 | MQTT over WebSocket（路径 `/mqtt`） |
| 8922 | HTTP API / Actuator |

默认开启认证：

```
username: jmqtt
password: jmqtt
```

生产环境把 `jmqtt.broker.auth-password` 改成 SHA-256 摘要：

```bash
java -cp target/jmqtt-broker.jar online.ipuff.jmqtt.auth.util.PwdUtil <明文密码>
```

### HTTP API

> **仅限内网 / 受信网络**：`/open/api/jmqtt/send` 不做鉴权，可向任意 topic 注入消息。请用防火墙把 8922 限制在内网或运维网段。

```bash
# 运行状态（连接数、会话数、订阅数、主题树节点数、背压触发次数）
curl http://127.0.0.1:8922/open/api/jmqtt/info

# 服务端主动下发
curl -X POST http://127.0.0.1:8922/open/api/jmqtt/send \
     -H 'Content-Type: application/json' \
     -d '{"topic":"device/123/cmd","qos":1,"message":"hello"}'
```

## 已知边界

1. **v5 客户端连不上** —— 收到「协议版本不支持」。若存在发 v5 CONNECT 的固件版本，需先确认。
2. **v5 的拒绝没有 CONNACK** —— Netty 解码层对 v5 报文结构不匹配时抛的异常不在 `handleDecodeFailure` 的识别范围内，表现为连接被直接关闭，排查时容易误判成网络问题。
3. **ACL 拒绝的 QoS 1/2 不回确认** —— 客户端会超时重发直到 ACL 变更或断开。这是 3.1.1 没有逐条错误通道的固有限制。
4. **SUBACK 失败码只有 0x80** —— 无法区分「过滤器非法」与「未授权」。
5. **UNSUBACK 不带逐项结果** —— 逐项成败只进日志。
6. **广播关闭时跨节点订阅静默收不到** —— `broadcast-enabled: false` 的前提是「集群内不存在跨节点 MQTT 订阅」。判断错了，症状是消息不报错地丢失。
7. **无配额体系** —— 没有全局 / 单 IP 最大连接数、单客户端发布速率限制、单客户端订阅数上限。需要靠前置 LB 与设备侧约束。
8. **跨节点接管收敛依赖连接事件 topic** —— 未配置 `connection-event-topic` 时，客户端切换节点后旧连接会存活到 keepalive 超时（60s × 1.5），期间同一 clientId 双在线；事件丢失时同样退化到这条兜底路径，不会报错。
9. **不终结 TLS** —— 只监听明文 1883 / 8083，TLS 在 LB 层终结，见 [docs/configuration.md](docs/configuration.md)。
10. **Spring Boot 2.7.18 已 EOL** —— 基线无安全补丁，对公网暴露需评估。

## 与 main 分支的差异

`main` 保留 3.1.1 + 5.0 双栈，本分支只保留 3.1.1（兼容 3.1）。

| 项 | main | v3-lite |
| --- | --- | --- |
| 协议 | 3.1.1 + 5.0 双栈 | 3.1.1，兼容 3.1；v5 握手阶段拒绝 |
| `ReplyFactory` | 324 行，9 处版本分支 | **191 行，无版本分支** |
| `ConnectOptions` | 8 个字段（含 SEI / Receive Maximum / Packet Size / Alias / Will Delay） | **2 个字段**（cleanSession / sessionExpiry） |
| 出站回包 | 按连接版本构造两套形态 | 只有一种形态 |
| 订阅处理 | 含 Retain Handling 与 v5 细分失败码 | 只有 0x80 一个失败码 |
| ACL 拒绝 | v5 回 0x87 原因码 | 只有静默丢弃 |

其余能力（QoS 0/1/2、保留消息、遗嘱、连接接管、持久会话、离线队列、发送窗口与背压、在途持久化、Kafka 集群总线、Redis 会话持久化、管理面）两个分支一致。

## 文档

| 文档 | 内容 |
| --- | --- |
| [docs/configuration.md](docs/configuration.md) | 配置详解：集群 / 数据面 / 下行通道 / 连接事件 / Redis 持久化 / 背压 / 离线队列 / ACL |
| [docs/cluster-broadcast-rationale.md](docs/cluster-broadcast-rationale.md) | 集群为什么选广播而不是复制元数据 |
| [docs/cluster-broadcast-assessment.md](docs/cluster-broadcast-assessment.md) | Kafka 差异化消费组做集群广播的方案评估 |
| [docs/benchmark.md](docs/benchmark.md) | 压测方法、完整数据与容量结论 |
| [docs/monitoring/](docs/monitoring/) | Prometheus 告警规则 + Grafana 面板 |
| [docs/server-side-ingestion.md](docs/server-side-ingestion.md) | 平台侧为什么读 Kafka 而不是订阅 MQTT |
| [docs/admin-console.md](docs/admin-console.md) | 管理台（jmqtt-admin）设计说明 |
| [docs/reports/](docs/reports/) | 各能力的设计与实现报告 |

**配套项目**：[passiolin/jmqtt-admin](https://github.com/passiolin/jmqtt-admin) —— 集群管理台（节点概览 / 客户端列表 / 单条踢下线 / 节点排水）。Spring Boot 2.7.18 + Vue 3，构建为单 jar，**只读 Redis、不直连任何 broker**。

## 许可

[Apache License, Version 2.0](LICENSE)

MQTT 报文编解码使用 [Netty](https://netty.io) 官方的 `netty-codec-mqtt`。
