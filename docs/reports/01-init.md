# jmqtt-broker 初始化报告

> 时间：2026-09-11
> 目标：Spring Boot 2.7.18 + Netty，单模块 Maven，Apache 2.0
> 项目路径：`/home/passio/WorkBuddy/mqtt-broker/jmqtt-broker`

---

## 一、交付结果

**48 个 Java 文件 / 4224 行，构建通过，启动通过，端到端协议测试 11/11 通过。**

| 验证项 | 结果 |
|---|---|
| `mvn clean package` | **BUILD SUCCESS**，零编译错误 |
| 构建产物 | `target/jmqtt-broker-0.1.0-SNAPSHOT.jar`（24MB 可执行 fat jar） |
| 启动 | Tomcat 8922 + MQTT 1883 + WebSocket 8083 全部监听，**epoll 传输生效** |
| Actuator | `/actuator/health` → `{"status":"UP"}` |
| HTTP API | `/open/api/jmqtt/info` 返回连接数/会话数/订阅数/主题树节点数 |
| 端到端协议 | **11 / 11 PASS** |

技术栈：Java 21（Temurin 21.0.12.1）、Spring Boot 2.7.18、Spring 5.3.31、Netty 4.1.101.Final、
`io.netty:netty-codec-mqtt`。包名 `online.ipuff.jmqtt`，groupId `online.ipuff`。

---

## 二、包结构

单模块 Maven 工程，按职责分层：

| 层 | 包 |
|---|---|
| 领域模型 | `message` / `session` / `subscribe` / `store`（接口） |
| 接入与协议 | `server` / `handler` / `protocol` / `web` / `config` |
| 存储实现 | `store` / `subscribe` / `session`（实现） |
| 认证 | `auth` |
| 路由 | `router`（内存主题树） |
| 集群 | `cluster`（集群总线抽象） |

---

## 三、已落地的修正

| # | 项 | 朴素做法 | 本实现 |
|---|---|---|---|
| 1 | 主题路由 | 把订阅放外部存储，发布时全量拉取通配订阅 + 逐条字符串拼装比对，O(通配订阅总数) | 内存 `TopicTrie`，O(主题层数)，投递路径零外部调用 |
| 2 | retain 匹配 | 全量扫描全部保留消息 | 第二棵主题树的**反向遍历** `scanByFilter()`，O(匹配结果数) |
| 3 | 投递查找 | 会话查询 + 拼 key + 全量遍历，共三次查表 | `ConnectionRegistry` 单层 `clientId → Channel`，一次 map get |
| 4 | `sessionPresent` | `= !cleanSession`，不看服务端是否真有会话 → **静默丢消息** | 按「服务端是否真的恢复了会话」计算 |
| 5 | SUBSCRIBE 校验 | 一个过滤器非法就 close 整条连接；漏检 `sport/tennis#` | 逐项返回 0x80；完整校验规则 |
| 6 | 保留消息 retain 标志 | 投递时置 0 | 置 1（规范要求） |
| 7 | 背压 | 设了水位线但全代码无人检查 `isWritable()` | QoS 0 背压丢弃、QoS 1/2 计入重传，并统计触发次数 |
| 8 | 会话过期 | `keepAlive × 1.5` 兼作会话 TTL | `session-expiry-seconds` 独立配置 |
| 9 | 会话中的连接信息 | 存 `channelId`（连接与会话耦合） | 移除；连接由 `ConnectionRegistry` 独立管理 |
| 10 | 认证 | 固定 RSA 私钥「加密」用户名后比对（PKCS#1 填充带随机性） | 常量时间凭据校验，支持 `sha256:` 摘要 |
| 11 | 报文标识符 | 共享存储自增（每条 QoS 1/2 投递一次网络往返） | 进程内 `AtomicInteger` |
| 12 | IoC | 手写 `volatile` 双重检查锁 | Spring 构造器注入 |
| 13 | 主题层数 | 无限制 → 递归匹配可被超长主题打爆线程栈 | `MAX_TOPIC_LEVELS = 128` |

---

## 四、端到端测试结果（11/11）

```
[PASS] 3 个客户端 CONNECT 成功
[PASS] 全部 3 个客户端收到 CONNACK
[PASS] 全新会话的 sessionPresent 全部为 false (含 cleanSession=0)
[PASS] 通配符订阅 sensor/+/temp 返回 QoS 1
[PASS] 非法过滤器 sport/tennis# 逐项返回 0x80, 合法项放行
[PASS] 含非法过滤器后连接保持存活
[PASS] 通配符订阅收到 sensor/room1/temp
[PASS] 不匹配 sensor/+/temp 的主题未被投递
[PASS] 晚到的订阅者收到保留消息且 retain=1
[PASS] 异常断连触发遗嘱消息
[PASS] 正常 DISCONNECT 不触发遗嘱
```

测试类：`src/test/java/online/ipuff/jmqtt/e2e/MqttE2ECheck.java`
命名为 `*Check` 而非 `*Test`，**不会**进入 surefire 默认执行集（它需要真实 broker）。

---

## 五、有意留待后续的项

| 优先级 | 项 | 现状 |
|---|---|---|
| 高 | **集群总线（Kafka）** | `ClusterBus` 接口 + `LocalClusterBus` 单机实现已就位。接入 Kafka 时用**每节点独立 group.id** 实现广播；必须处理**回环防护**（ingress 注入的消息不得再出站，否则指数放大） |
| 高 | **消息键** | Kafka record 的 key 必须是 MQTT 主题 —— 既保证同主题落同一 partition（顺序），也是消费端还原主题的唯一途径 |
| 高 | **上行数据面分离** | 上行数据只做 egress（云端从 Kafka 消费），不参与集群广播，扇出从 N 倍降到 1 倍 |
| 中 | **会话持久化（Redis）** | `ISessionStoreService` 接口不变可替换。两条硬约束：**会话记录必须包含订阅关系**（否则只能回 `sessionPresent=false`）；只在连接/重连/接管路径读写，**绝不进入投递路径** |
| 中 | **QoS 1/2 消息持久化** | 当前在途消息是进程内状态，节点重启即丢 |
| 中 | **每客户端有界发送队列** | 当前 QoS 1/2 在客户端不可写时仍入队 |
| 低 | MQTT 5.0 | 官方 codec 已支持，需补 v5 分支 |
| 低 | ACL、共享订阅、服务端接收方向 QoS 2 去重 | 未实现 |

---

## 六、运行环境说明

构建验证在受限沙箱中完成，其中几处需要说明（**与你的本机环境无关**）：

- 沙箱无 JDK/Maven，通过 Debian 软件池 + 阿里云镜像解包 OpenJDK 21 与 Maven 3.9.16 完成验证；
  Debian 包的 `conf/security/**` 与 `lib/security/**` 全是指向 `/etc` 的符号链接，
  需将这些链接替换为真实文件后 JVM 才能启动
- 沙箱 `ulimit -v` 为 4GB，JVM 默认堆（1/4 物理内存）+ 1GB 压缩类空间必然超出，
  构建时用 `-Xmx640m -XX:CompressedClassSpaceSize=128m` 收紧
- 沙箱内 TLS 信任库为空，Maven 用 `-Dmaven.resolver.transport=wagon -Dmaven.wagon.http.ssl.insecure=true` 绕过

**你的本机（Zulu 21）直接 `mvn clean package` 即可，不需要上述任何 workaround。**

---

## 七、常用命令

```bash
# 构建
mvn clean package

# 运行
java -jar target/jmqtt-broker-0.1.0-SNAPSHOT.jar

# 查看状态
curl http://127.0.0.1:8922/open/api/jmqtt/info

# 服务端下发消息
curl -X POST http://127.0.0.1:8922/open/api/jmqtt/send \
     -H 'Content-Type: application/json' \
     -d '{"topic":"device/123/cmd","qos":1,"message":"hello"}'

# 生成密码摘要
java -cp target/jmqtt-broker-0.1.0-SNAPSHOT.jar online.ipuff.jmqtt.auth.util.PwdUtil <明文密码>
```
