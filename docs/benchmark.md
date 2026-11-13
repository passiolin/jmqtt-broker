# 压测报告

> 返回 [README](../README.md)
> 压测工具:[passiolin/jmqtt-bench](https://github.com/passiolin/jmqtt-bench)

## 压测：容量规格与结果

压测工具在独立仓库 [passiolin/jmqtt-bench](https://github.com/passiolin/jmqtt-bench)（Go，单二进制），模型为全链路闭环:
设备 MQTT 上行 → broker → Kafka → backend 消费 → 逐条回显下行 → 设备收回。
seq 三集合差做丢失归因,自适应错峰接入,持久会话(Redis 参与接入路径)。

以下为 **2026-11-12 标准压测轮**（8C16G 标准规格、Kafka JSON 结构契约、
producer `acks=0/linger 50ms/lz4`、`KafkaClusterBus` 拆分后的构建）。

### 压测环境(PVE 10.10.12.254,CPU: Intel Xeon E5-2698B v3 @ 2.00GHz,16 核 32 线程)

| VM | 名称 | 规格 | 地址 | 角色 |
|---|---|---|---|---|
| 111 | jmqtt-broker | **8C / 16G(JVM 12G)** | 10.10.12.111 | **被测对象**,单节点 |
| 113/114 | bench-device ×2 | 各 8C / 16G,各 8 个源 IP | 10.10.12.113/114 | 模拟终端 |
| 115 | bench-backend | 4C / 8G | 10.10.12.115 | 模拟后台(Kafka 收发 + report) |
| 116 | bench-kafka | 4C / 8G | 10.10.12.116 | Kafka 4.1.2 KRaft 单节点 |
| 117 | bench-redis | 2C / 4G | 10.10.12.117 | Valkey(会话持久化) |

Kafka topics: `jmqtt-uplink`(16 分区)/`jmqtt-downlink`/`jmqtt-cluster`/`jmqtt-client-events`,RF=1。
broker 基线:单节点,`consumer-threads=0`(2×核=16,含下行 worker 池)、`max-inflight=32`、
producer `acks=0/linger 50ms/16KB/lz4`、上行路由 `bench/+/event/up → key=device`、
下行通道 `jmqtt-downlink`、连接事件 `jmqtt-client-events`。

> **每轮前必须重启 broker 并清空 valkey/topics**:`clean_session=false` 的会话在 JVM 内
> 保留 2h,跨轮累积会撑满 12G 堆并把测量拖进 GC/超时噪声(本轮踩过:70 万会话残留时
> 16k/s 档出现 37s 级 PUBACK 停摆)。宿主同时跑其他业务 VM 时数据不可信,本轮均在
> 宿主空闲时采集(load1 < 1)。

### 业务闭环实测(指令 + 逐条回显,持久会话,256B,QoS1)

| 稳态连接数 | 指令TPS | 上行丢失 | 回显丢失 | PUBACK RTT P50/P95 | 全环 RTT P50/P95 |
|---|---|---|---|---|---|
| 100,000 | 1,799 | **0** | 13 | 0.5 / 5.8 ms | 48 / 76 ms |
| 200,000 | 3,751 | **0** | 213 | 0.8 / 15.8 ms | 58 / 97 ms |

- 10/20 万档:上行零丢失;回显丢失 13 / 213 条(≤0.03%);PUBACK 到达率 **100.0000%**
- 消费分派为**共享队列 + worker 争抢**: 并行度只由 worker 数决定, 与 topic 分区数解耦。
  同场景对比「分区固定 worker」:

| 200k 档 | 分区固定 worker | **共享队列争抢** |
|---|---|---|
| 指令 TPS | 3,739 | 3,751 |
| ACK RTT P95 / P99 | 218 / 810 ms | **16 / 221 ms** |
| 全环 RTT P95 / P99 | 2,450 / 8,288 ms | **97 / 529 ms** |
| 回显丢失 | 164 | 213(0.03%) |
| broker CPU 均值/峰值 | 29.7% / 74.4% | 24.1% / 59.7% |

慢分区不再独占 worker(空闲 worker 可分担热点分区), 尾延迟大幅下降、CPU 更低;
代价是同一分区内不再保证顺序(业务明确不需要)。
- **30 万连接档已从本报告删除**:该量级在超分宿主上不可稳定复现
  (压测期间宿主同时运行 50+ vCPU 的其他业务 VM,曾出现数十秒级停摆与重连风暴),
  不能作为容量结论

### 12C24G / 扩线程复测(已撤回)

> 12C24G 与「扩线程(Netty worker 48 / Kafka 消费 worker 32 / 分区 32)」的 30 万档
> 对比已撤回:该量级在本超分宿主上不可稳定复现(曾出现 45s 级停摆与连接重连风暴),
> 线程/分区的相对结论(尾延迟小幅改善、TPS 基本不变)来自被污染的轮次, 不作为口径。
> 需要这些对比时应在独占硬件上重测。

### 纯连接实测(仅 keepalive,零业务消息,PINGREQ 1 条/60s)

| 稳态连接数 | broker CPU 均值/峰值 | broker 内存峰值 | 结果 |
|---|---|---|---|
| **420,000** | 35.9% / 67.5% | 75%(≈12G / 16G) | 接入零失败零重连 |

注:bench 的 connect 时长按聚合速率簿记,420k 稳态 soak 被提前结束(实际维持约 35s);
连接数与资源数据为实测。CPU 有大量余量,内存是 16G 规格下更先逼近上限的配置项。

### 上行纯吞吐(无回显,10k 连接)

| 实测速率 | 上行丢失 | PUBACK P50 |
|---|---|---|
| 16k msg/s | 1 | 0.6 ms |
| 24k msg/s | 2 | 0.7 ms |
| 32k msg/s | 2 | 0.8 ms |

累计发送 8.70M 条、PUBACK 100.0000%、上行 gap 5 条。旧参考值 ≈15k msg/s,
当前构建在 32k msg/s 下接近零丢(PUBACK P99 < 4ms)。

### 双向 ladder(echo 开,总线流量 ×2;标称 1k→32k,两实例聚合实测 2k→64k)

| 实测速率 | 上行丢失 | 回显丢失 | PUBACK P50 | 全环 P50 |
|---|---|---|---|---|
| 2k–8k msg/s | 0–1 | 0–441 | 0.4–1.7 ms | 49–78 ms |
| 16k msg/s | 583 | 1.35M | 327 ms | 89 ms |
| 32k msg/s | 42,052 | 积压回流 | 566 ms | 36 s |
| 64k msg/s | 5,749 | 1.67M | 2.3 s | 115 s |

全局:消费 8.81M、上行 gap 48,387(0.55%)、下行丢 1.78M(20%)。
**拐点:上行到 64k/s 都基本无损(0.55%),先饱和的是回显/下行投递链路(16k/s 起积压)。**

### 折算参考(按实测线性内插,取舍自行判断)

- 纯连接(连接-only):8C16G ≈**42 万连接**,内存峰值 ≈12G(≈28KB/连接),CPU 峰值 67%
- 业务闭环(可复现区间):10 万连接 1,775 TPS、20 万连接 3,807 TPS,上行零丢失
- 上行纯吞吐:32k msg/s 近零丢(无回显),PUBACK P99 < 4ms
- **30 万级容量外推已撤回**:本宿主为超分环境,该量级数据不可稳定复现, 不作为承诺

报告与原始产物:[jmqtt-bench/results](https://github.com/passiolin/jmqtt-bench/tree/main/results)
(本轮: `std-closed-loop-8c16g` / `std-idle-420k-8c16g` / `std-ladder2-8c16g` / `std-ladder1-8c16g`)。
