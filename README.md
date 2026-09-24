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


## 文档

| 文档 | 内容 |
|---|---|
| [docs/configuration.md](docs/configuration.md) | 配置详解:集群/数据面/下行通道/Redis 持久化/背压/离线队列/ACL/TLS |
| [docs/benchmark.md](docs/benchmark.md) | 压测方法、完整数据与容量结论 |
| [docs/server-side-ingestion.md](docs/server-side-ingestion.md) | 平台侧为什么读 Kafka 而不是订阅 MQTT |
| [docs/admin-console.md](docs/admin-console.md) | 管理台(jmqtt-admin)设计说明 |
| [passiolin/jmqtt-bench](https://github.com/passiolin/jmqtt-bench) | 压测工具(Go) |

## 压测：容量规格与结果

压测工具在独立仓库 [passiolin/jmqtt-bench](https://github.com/passiolin/jmqtt-bench)（Go，单二进制），模型为全链路闭环:
设备 MQTT 上行 → broker → Kafka → backend 消费 → 逐条回显下行 → 设备收回。
seq 三集合差做丢失归因,自适应错峰接入,持久会话(Redis 参与接入路径)。
以下为最新一轮实测数据。

### 压测环境(PVE 10.10.10.44,CPU: Intel Xeon E5-2698B v3 @ 2.00GHz,16 核 32 线程,2.0GHz)

| VM | 名称 | 规格 | 地址 | 角色 |
|---|---|---|---|---|
| 105 | jmqtt-broker | 8C / 16G / 60G NVMe(JVM 12G) | 10.10.10.104 | **被测对象**,单节点 |
| 106/111 | bench-device ×2 | 各 8C / 16G / 40G NVMe,各 7-8 个源 IP | 10.10.10.105-.111 / .162-.169 | 模拟终端(合计 ≤90 万连接) |
| 107 | bench-backend | 4C / 8G / 40G NVMe | 10.10.10.130 | 模拟后台(Kafka 收发 + report) |
| 108 | bench-kafka | 4C / 8G / 60G NVMe | 10.10.10.128 | Kafka 4.1.2 KRaft 单节点 |
| 110 | bench-redis | 2C / 4G / 40G NVMe | 10.10.10.131 | Redis(会话持久化) |
| 109 | bench-monitor | 2C / 4G | 10.10.10.129 | Prometheus + Grafana |

Kafka topics: `jmqtt-uplink` 16 分区 / `jmqtt-downlink` 8 分区,retention 1h。
broker 基线:单节点,`consumer-threads=0`(2×核=16,含下行 worker 池)、
`max-inflight=32`、`max-mqueue-len=1000`、Kafka producer `acks=1/linger 5ms/16KB`、
上行路由 `bench/+/event/up → key=device`、下行通道 `jmqtt-downlink`。

### 业务闭环实测(指令 + 逐条回显,持久会话,256B,QoS1)

| 稳态连接数 | 指令TPS | broker CPU 均值/峰值 | broker 内存占用 | JVM 堆峰值 | PUBACK RTT P50/P99 | 全环 RTT P50/P99 | 异常 |
|---|---|---|---|---|---|---|---|
| 100,000 | 1,438 | 35.7% / 60.7% | 6.9G | 3.9G | 0.9 / 2,550 ms | 20 / 5,268 ms | 无,回显全部送达 |
| 200,000 | 3,100 | 49.5% / 63.5% | 8.3G | 5.8G | 1.7 / 6,443 ms | 128 / 12,482 ms | 无,回显全部送达 |
| 300,000 | 4,833 | 55.3% / 60.4% | 9.1G | 3.8G | 2.0 / 7,281 ms | 78,383 / 136,365 ms | **有:下行回显积压 75 万条**,全环 RTT 分钟级;上行零丢失 |

- 下行回显投递能力实测:20 万连接 ≈3,100 msg/s(恰好跟上);30 万连接 ≈2,400-2,700 msg/s
  (需求 5,000,积压不可收敛)—— **闭环瓶颈,需 broker 侧 profile 投递路径**
- 上行全程无损:累计 400 万条指令,上行丢失 0 条,确认率 99.9981%

### 纯连接实测(仅 keepalive,零业务消息,PINGREQ 1 条/60s)

| 稳态连接数 | PING 处理 | broker CPU 均值/峰值 | broker 内存占用 | 异常 |
|---|---|---|---|---|
| 100,000 | ≈1,667/s | 9.6% / 34.2% | 3.2G | 无 |
| 200,000 | ≈3,333/s | 19.1% / 50.5% | 5.6G | 无 |
| 300,000 | ≈5,000/s | 10.3%* / 15.7% | 8.0G | 无 |
| **420,000** | ≈7,000/s | 28.3% / 52.9% | 9.3G | 无;接入零失败零重连 |

\* 含爬坡窗口,CPU 均值被摊薄。每连接内存成本 ≈23KB;42 万连接下 broker CPU
仅 28%,CPU 未构成限制。

### 折算参考(按实测线性内插,取舍自行判断)

- 每 vCPU:纯连接 ≈13 万条;业务闭环 ≈3 ~ 3.5 万条 ≈ 600 条/分钟指令(含等量回显)
- 每 1 万连接:业务闭环 ≈0.3 vCPU + ≈23KB×1 万 内存;纯连接 ≈0.08 vCPU + ≈230MB 内存
- 上行纯吞吐参考(无回显,10k 连接):≈15k msg/s

完整数据与口径见 [docs/benchmark.md](docs/benchmark.md);报告与原始产物:[jmqtt-bench/results](https://github.com/passiolin/jmqtt-bench/tree/main/results)(仅保留最新一轮)。


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


**配套项目**:[passiolin/jmqtt-admin](https://github.com/passiolin/jmqtt-admin) —— 集群管理台(节点概览 / 客户端列表 / 单条踢下线 / 节点排水)。后端 Spring Boot 2.7.18 + 前端 Vue 3,构建为单 jar;它**只读 Redis、不直连任何 broker**。设计说明见 [docs/admin-console.md](docs/admin-console.md)。

## 许可

[Apache License, Version 2.0](LICENSE)

MQTT 报文的编解码使用 [Netty](https://netty.io) 官方的 `netty-codec-mqtt`
（v3.1.1 与 v5.0 均已支持）。
