# 压测报告

> 返回 [README](../README.md)
> 压测工具:[passiolin/jmqtt-bench](https://github.com/passiolin/jmqtt-bench)

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

报告与原始产物:[jmqtt-bench/results](https://github.com/passiolin/jmqtt-bench/tree/main/results)(仅保留最新一轮)。
