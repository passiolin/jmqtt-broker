# jmqtt-broker Phase 7：在途消息持久化 —— 阶段报告

> 目标：把「已发送但未确认的 QoS 1/2 消息」从纯内存状态变成可恢复状态，
> 使节点崩溃或跨节点接管后，客户端重连仍能以 `dup=1` 收到这些消息。
>
> 本轮除了把这个能力做出来，还排查出 **3 个真实缺陷**（其中 1 个会让默认配置完全起不来）。
> 这三个缺陷都不是理论推演，是日志与回归用例抓出来的。

---

## 一、实现

核心是新增 `store/InflightPersistence.java`，作为 `DupPublishMessageStoreService`
（进程内 `ConcurrentHashMap`）的**持久镜像**。

### 1.1 为什么是「整份快照」而不是「逐条操作日志」

每个客户端的在途集合有硬上限（`max-inflight`，默认 32），所以「重写这一份」成本可控，换来两个关键好处：

| 好处 | 说明 |
|---|---|
| **天然幂等、无顺序问题** | 逐条 `HSET`/`HDEL` 的日志在异步模式下必须保证顺序，否则一条迟到的 `HSET` 会把已确认的消息永久留在镜像里。整份覆盖是「最后写入者胜」，不存在这个问题 |
| **实现简单** | 不需要维护操作队列、不需要回放 |

`onChanged` 只保留每个客户端**最新**的一份快照，未刷盘期间的连续变更被 `ConcurrentHashMap.put` 自然合并。

写入用 `DEL` + `HSET` 的 Lua 脚本一次往返完成（而不是「`HSET` 新值 + `HDEL` 缺失值」）——
后者需要记住上一次写了哪些字段，一旦刷盘失败就失去基准；`DEL`+`HSET` 在 Lua 里原子，不存在中间空窗。

### 1.2 三种模式，代价明确

| 模式 | 行为 | 代价 |
|---|---|---|
| `off` | 不落盘 | 节点故障丢在途消息（原有行为） |
| `sync` | 每次变更同步写 Redis | **每条 QoS 1/2 消息多一次 Redis 往返，直接抬高投递延迟** |
| `async` | 标记脏客户端，单线程定时批量刷盘（默认 100ms） | 崩溃时丢最近一个刷盘周期 |

**异步模式用单线程刷盘**：不是为了让 Redis 操作串行（Lettuce 本身异步），而是让「提交」有序——
同一客户端的两份快照不会因线程调度而倒序落盘。

### 1.3 与其它部件的边界

- **镜像的存活期跟会话一致**：写入时读会话 Hash 的 `expire` 字段作为 TTL。会话过期了，欠它的在途消息也没有意义。
- **`detach` 与 `removeByClient` 的区别**（接管语义的关键）：
  - `detach(clientId)`：**只清内存**。跨节点接管时用——镜像必须留给接管方节点加载。
  - `removeByClient(clientId)`：内存与镜像一起清。会话**真正销毁**时用（`cleanSession=1` 断开、会话过期）。
- **Redis 里只放消息的「恢复副本」，不放路由**。投递路径始终是纯内存。

---

## 二、验证结果

### 2.1 在途恢复端到端（两阶段，真实 kill -9）

驱动：`tools/ssltest/InflightRecoveryTest.java` + `tools/run-inflight-recovery.sh`

| 阶段 | 断言 | 结果 |
|---|---|---|
| 一 | 订阅 `if/+/data`，收到 QoS 1 投递，`dup=false` | PASS |
| 一 | 刻意不回 PUBACK 后断开，broker 侧留下在途记录 | PASS |
| — | Redis 中存在 `jmqttif:session:{if-client}`（`node=ifnode`、`clean=0`、`expire=7200`） | PASS |
| — | Redis 中存在 `jmqttif:subs:{if-client}`（`if/+/data = 1`） | PASS |
| — | Redis 中存在 `jmqttif:inflight:{if-client}` → 字段 `1` = `{"topic":"if/room1/data","qos":1,"payload":"aW4tZmxpZ2h0LXBheWxvYWQ="}` | PASS |
| — | `kill -9` 模拟节点崩溃 | PASS |
| 二 | **重启后立即重连（不等健康检查）`sessionPresent=true`** | PASS |
| 二 | **以 `dup=1` 重发在途消息，且 `packetId` 与首次投递一致（=1）** | PASS |
| — | 两个阶段的 broker 日志均无 `ClassCastException`、无「Redis 不可用」降级 | PASS |

崩溃前后 Redis 中的镜像始终为同一份，重连后被重写为新的副本。

**指标指纹的修复痕迹**（同一个用例，修前 vs 修后）：

```
修前: inflightFlushCount=0  inflightFlushedEntries=0  inflightFlushFailed=3   ← 镜像写进去了, 计数却说失败
修后: inflightFlushCount=2  inflightFlushedEntries=1  inflightFlushFailed=0
```

### 2.2 全量回归

| 套件 | 用例数 | 结果 |
|---|---|---|
| 单元测试（`mvn test`，含新增 2 个启动冒烟测试） | 44 | 44 / 44 通过 |
| 基础协议（`MqttE2ETest`，真实 broker） | 11 | 11 / 11 通过 |
| 背压流控（`BackpressureTest`，`max-inflight=3`） | 3 | 3 / 3 通过 |
| 跨节点会话恢复（`SessionRestoreTest`，两节点共享 Redis） | 7 | 7 / 7 通过 |
| 离线消息跨节点投递（`OfflineQueueTest`，两节点共享 Redis） | 7 | 7 / 7 通过 |
| 在途恢复（`InflightRecoveryTest`，kill -9 + 重启） | 2 阶段 | 通过 |

驱动脚本（可重复执行，全部落在 `/home/passio/.workbuddy/tools/`）：
`run-regression.sh`、`run-offline-queue.sh`、`run-inflight-recovery.sh`

---

## 三、本轮排查出的缺陷（本轮最重要的部分）

### 缺陷 1：Lettuce `ScriptOutputType.INTEGER` 解出的是 `Long`，不是 `Integer`

**症状**：镜像**确实写进了 Redis**，但指标显示 `inflightFlushFailed=3 / inflightFlushCount=0`。

**根因**：

```
java.lang.ClassCastException: class java.lang.Long cannot be cast to class java.lang.Integer
    at InflightPersistence.lambda$persist$3(InflightPersistence.java:277)
    at InflightPersistence.persist(InflightPersistence.java:277)
    at DupPublishMessageStoreService.mirror(DupPublishMessageStoreService.java:127)
```

Lettuce 的 `IntegerOutput` 继承的是 `CommandOutput<K,V,Long>`，返回 `Long`。
把它赋给 `Integer` 会在调用点插入 `checkcast Integer`，运行期抛 `ClassCastException`。

**最阴的地方**：`eval` 是**先在服务端执行完脚本、再解码返回值**的。
所以 `DEL`+`HSET` 的副作用是真的发生了，只有返回值在解码时炸掉 ——
表现就是「数据写进去了，调用方却认为失败」。

**同一处错误在 `RedisPendingMessageStore.enqueue` 也存在**，后果更糟：
入队已经发生，调用方却按「入队失败」处理，甚至打日志说「离线消息无法缓存」。
它之所以在 Phase 5 的 E2E 里没暴露，是因为脚本副作用生效、消息确实进了队列，
端到端行为看起来是对的 —— **只有返回值语义被破坏**。

**修复**：两处接收类型改为 `Long`，并在代码里注明这个坑。

### 缺陷 2：全局健康标记把「一个功能的失败」放大成「所有功能停摆」

`RedisConnectionManager` 原本用一个从 `false` 起步的布尔标记：

```java
private volatile boolean available;   // 初始 false

public <T> T execute(...) {
    try { T r = action.get(); available = true; return r; }
    catch (Exception e) { available = false; return fallback; }
}
```

这带来两个问题：

**(a) 启动窗口内所有 Redis 功能被跳过。** 端口在 Spring 容器就绪时就已监听，
而健康检查的第一轮探活还要晚一点。这段时间到达的 CONNECT 会看到 `available=false`。

**(b) 一个功能的失败会熄掉所有功能。** 上游日志给出了完整因果链：

```
33- 连接建立: /127.0.0.1:32872
34- SessionPersistence - 持久层无会话: clientId=if-client
35- ERROR RedisConnectionManager - Redis 不可用... op=inflight.persist
36: java.lang.ClassCastException: Long cannot be cast to Integer
43-   at ConnectHandler.clearDanglingState(ConnectHandler.java:307)
44-   at ConnectHandler.processConnect(ConnectHandler.java:162)
```

看 `processConnect` 的顺序就明白了：

```
162 行  clearDanglingState  → removeByClient → mirror → persist → CCE → available=false
185 行  acquireOwnership    → !available() → 静默跳过
190 行  saveSession         → !available() → 静默跳过
```

**所以 Phase 7 的第一次验证里，会话压根没有写进 Redis** —— 阶段二的
`sessionPresent=false` 不是「恢复逻辑坏了」，是「根本没存」。两个独立功能之间
出现了耦合：一个镜像写入的异常，把同一时刻的会话持久化一起带走了。

**修复**（`RedisConnectionManager`）：

1. **语义改为「只知道故障、不假定健康」**：初始即可用，失败后进入一段冷却期
   （长度取 `health-interval-ms`），冷却结束自动重新尝试，任何一次成功立刻清除故障标记。
2. **明确调用约定**：`available()` 回答的是「现在值不值得尝试」，不是「Redis 是否一定可用」。
   它只适合用在**可以安全跳过**的写路径上。
3. **恢复/读取路径不再做前置判断**：`SessionPersistence.restore()` 与
   `InflightPersistence.load()` 去掉了 `available()` 门禁。
   理由是两个方向的代价不对称 —— 跳过一次恢复的代价是**消息真的丢了**，
   而直接尝试一次的代价只是一次命令超时（`command-timeout-ms` 兜底）。

### 缺陷 3：条件 Bean 被硬注入 —— 默认配置下应用完全起不来

回归用例（一个不启用 Redis 的场景）直接把启动打挂了：

```
Parameter 11 of constructor in online.ipuff.jmqtt.protocol.ConnectHandler
required a bean of type 'online.ipuff.jmqtt.store.InflightPersistence' that could not be found.
```

`InflightPersistence` 是 `@ConditionalOnProperty(redis.enabled=true)` 的，
而 `application.yml` 里 `jmqtt.broker.redis.enabled` **默认就是 `false`** ——
也就是说 Phase 7 合入后，**用默认配置根本起不来**。之前一路全绿，
是因为所有 E2E 脚本都显式带了 `--jmqtt.broker.redis.enabled=true`。

同类引用点里 `DupPublishMessageStoreService` 与 `OpenApiController` 用的是
`ObjectProvider`，只有 `ConnectHandler` 是硬注入 —— 同一个类有两种写法，迟早写错一处。

**修复**：`InflightPersistence` 改为**始终注册**，用 `Mode.OFF` 表达「没启用 / 没有 Redis」，
Redis 连接管理器通过 `ObjectProvider` 可选获取。

> 根子在于**用「Bean 存不存在」表达「功能开不开」**。条件 Bean 一旦被直接注入，
> 就成了启动期的定时炸弹。这个类本身已经有 `OFF` 模式，用它表达比用 Bean 的有无更清晰。

### 顺带修正：`clearDanglingState` 把「读不到」当成了「可以删」

`processConnect` 在 `restore()` 返回 null 时会调 `clearDanglingState`。
它原本用 `removeByClient`，会**连带删掉 Redis 里的在途镜像**。

但「本进程内存里没有该会话」有两个成因：会话确实不存在，或者持久层这一次没读到。
后者会把一次 Redis 抖动翻译成「删掉别的节点上那个会话的在途镜像」——
客户端重连后既没有会话也没有待重发的消息，**QoS 1/2 静默破损**。

**修复**：改用 `detach`，只清本地内存；真正该销毁时由 `clearSessionState` 处理。

---

## 四、代码改动清单

| 文件 | 改动 |
|---|---|
| `store/InflightPersistence.java` | **新增**；随后去掉条件装配、Redis 依赖改 `ObjectProvider`、`mode()` 在无 Redis 时返回 `OFF`、结果类型 `Integer`→`Long`、`load()` 去掉健康门禁 |
| `redis/RedisConnectionManager.java` | 健康状态由布尔改为「冷却期」三态语义；失败只影响冷却窗口，成功后立刻恢复 |
| `store/redis/RedisPendingMessageStore.java` | `enqueue` 结果类型 `Integer`→`Long`（同一 CCE 的第二个位置） |
| `session/persistence/SessionPersistence.java` | `restore()` 去掉 `available()` 前置判断 |
| `protocol/ConnectHandler.java` | `clearDanglingState` 改用 `detach`（不再误删持久层在途镜像） |
| `store/DupPublishMessageStoreService.java` | 带 `ObjectProvider` 的构造函数补 `@Autowired`（多构造函数下 Spring 会静默选无参构造，导致镜像静默不写） |

> `@Autowired` 那条也值得单独记一笔：两个构造函数且无注解时，Spring **不报错**，
> 只是静默选无参构造，结果是 `persistence` 永远为 null、镜像静默不写。
> 这个坑是 E2E 测出「镜像为空」才发现的。

### 新增测试

| 文件 | 用途 |
|---|---|
| `BrokerStartupDefaultConfigTest` | **用 application.yml 原样配置把上下文拉起来**。只校验「默认配置能启动」——条件 Bean 被硬注入、必需属性缺失、循环依赖这一类回归，只有真正 refresh 一次上下文才看得见 |
| `BrokerStartupRedisUnreachableTest` | Redis 启用了但**指向一个确定没有服务的端口**，验证上下文仍能启动、`InflightPersistence` 仍是可注入的 Bean、`load()` 优雅降级为空而不是抛异常。这条覆盖的是「Redis 挂掉不应该导致 broker 起不来」这个承诺，平时的用例连得上 Redis，覆盖不到 |

### 顺带修正的测试缺陷

`InflightRecoveryTest` 原先断言重发消息的 `packetId == 500`（发布方报文里的值）。
但 broker 给下行报文分配**自己的** packetId 空间，首次投递实测是 1 ——
这个断言即使恢复逻辑正确也会失败。改为由阶段一把实际观测到的 packetId
写入交接文件、阶段二读出来比对，这样「报文标识符保持不变」才真的被验证。

---

## 五、遗留与下一步

1. **`sync` 模式每条 QoS 1/2 有两次 Redis 往返**（一次读会话 `expire`、一次写镜像）。
   会话的 `expire` 完全可以由调用方传入或按 clientId 缓存，属于热路径上的可优化点。
2. **`available()` 语义变化的落点**：`RedisPendingMessageStore.persistent()` 与
   info 接口的 `offlineQueuePersistent` 现在在冷却窗口内也会报 `true`。
   它只用于上报，不影响行为，但语义上应区分「配置启用了」与「此刻连得上」。
3. **先量 QoS 1/2 占比再定模式**：从 `/open/api/jmqtt/info` 的 `qos` 段读
   `deliveredQos12Permille`。占比低 → `sync` 代价可忽略；上行也用 QoS 1 → 必须 `async`。
4. **真实 Kafka 环境验证**仍是空缺（沙箱 4GB 虚拟内存上限导致 Kafka 起不来），
   跨节点投递与接管通知只有单测覆盖。
5. 其余既有待办：MQTT 5.0、ACL、服务端接收方向的 QoS 2 去重。

### 沙箱约束备忘（跑这些验证时会撞到的）

- 所有 JVM（broker / `javac` / 测试客户端）都必须显式约束 metaspace
  （`-XX:CompressedClassSpaceSize` + `-XX:MaxMetaspaceSize`），否则报
  `Failed to reserve memory for metaspace`。
- 12 核机器上默认线程数会撞上虚拟内存上限（`unable to create native thread`），
  需要 `-XX:ActiveProcessorCount=2` + `--jmqtt.broker.worker-threads=2` + 收窄 Tomcat 线程池。
- 默认 charset 是 POSIX/ASCII，中文输出需要
  `-Dfile.encoding=UTF-8 -Dstdout.encoding=UTF-8`。
- `redis-cli` 依赖 `liblzf.so.1`，必须带 `LD_LIBRARY_PATH`。
- 后台进程会随所在 Bash 命令结束被回收 —— **整条链路必须放在同一个命令里**。
- `rm` 受安全删除守卫影响，改用「不删 + `flushall`」；`pkill -f` 匹配过宽（曾误杀 pid 1），
  改用 `ps -eo pid,args | grep -F <精确路径>`。
- 多节点用例必须逐节点指定 `websocket-port`，默认 8083 会让第二个节点 `bind` 失败。
