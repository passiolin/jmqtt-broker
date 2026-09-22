# 容器部署

镜像以 [azul/zulu-openjdk-debian:21](https://hub.docker.com/r/azul/zulu-openjdk-debian)(Zulu JDK 21,
amd64/arm64)为父镜像,非 root 运行,内置容器内存感知与 MQTT 端口健康检查。

## 构建

```bash
deploy/build.sh               # mvn package(含测试) + docker build
deploy/build.sh --skip-tests  # 跳过测试
```

产出 `jmqtt-broker:<version>` 与 `jmqtt-broker:latest` 两个 tag。
受限网络(空 TLS 信任库)下追加 Maven 参数:

```bash
MAVEN_ARGS="-Dmaven.resolver.transport=wagon -Dmaven.wagon.http.ssl.insecure=true" deploy/build.sh
```

## 运行

```bash
deploy/run.sh                            # 单容器, 1883/8083 对外, 8922 只绑本机
deploy/run.sh -e JMQTT_AUTH_PASSWORD=sha256:<hex>
```

或 compose(在本目录):

```bash
docker compose up -d
```

## 端口

| 端口 | 用途 | 暴露建议 |
|---|---|---|
| 1883 | MQTT/TCP | 不直接暴露公网,由 LB 终结 TLS 后转发 |
| 8083 | MQTT/WebSocket | 同上(LB 上做 WSS 443) |
| 8922 | HTTP API / 健康检查 | 只绑本机或内网 |

**TLS 在 LB 层终结**(见主 README「TLS / MQTTS」):镜像内只有明文端口,
MQTTS(8883)/WSS(443) 的证书、轮换、限流统一在负载均衡层完成。

## 环境变量

application.yml 对基础设施配置内置了 `${ENV:默认}` 占位符 —— 设置环境变量即覆盖,
不设置则用内置默认值(与仓库配置完全一致)。常用变量:

| 环境变量 | 对应配置 | 默认值 |
|---|---|---|
| `SERVER_PORT` | HTTP API / 健康检查端口 | 8922 |
| `JMQTT_ID` | broker 标识(集群每节点不同) | jmqtt |
| `JMQTT_HOST` | 监听地址(空 = 全网卡) | 空 |
| `JMQTT_PORT` | MQTT/TCP 端口 | 1883 |
| `JMQTT_WEBSOCKET_ENABLED` / `JMQTT_WEBSOCKET_PORT` | WebSocket 开关/端口 | true / 8083 |
| `JMQTT_AUTH_ENABLED` / `JMQTT_AUTH_USERNAME` / `JMQTT_AUTH_PASSWORD` | 连接认证 | true / jmqtt / jmqtt |
| `JMQTT_CLUSTER_ENABLED` | 集群总开关 | false |
| `JMQTT_KAFKA_ENABLED` | Kafka 总线开关 | false |
| `JMQTT_KAFKA_BOOTSTRAP_SERVERS` | Kafka 地址 | 127.0.0.1:9092 |
| `JMQTT_KAFKA_CONSUMER_THREADS` | 集群消费并行度 | 1 |
| `JMQTT_REDIS_ENABLED` / `JMQTT_REDIS_HOST` / `JMQTT_REDIS_PORT` / `JMQTT_REDIS_PASSWORD` / `JMQTT_REDIS_DATABASE` | 会话持久化 Redis | false / 127.0.0.1 / 6379 / 空 / 0 |

**自定义路径的配置文件**(docker 挂载场景, 文件名可自定义, 与 `JMQTT_*` 环境变量并存且优先级更高):

```bash
docker run -e SPRING_CONFIG_ADDITIONAL_LOCATION=file:/etc/jmqtt/overrides.yml \
           -v ./prod-config.yml:/etc/jmqtt/overrides.yml:ro jmqtt-broker:latest
```

其余配置(含 HTTP 认证/ACL)仍可用 Spring Boot 原生松散绑定:
`JMQTT_BROKER_HTTP_AUTH_URL` → `jmqtt.broker.http-auth.url`,规则是
`JMQTT_BROKER_` + 大写下划线形式。JVM 参数通过 `JAVA_OPTS` 覆盖,
默认 `-XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError`。

```bash
docker run -e JMQTT_KAFKA_ENABLED=true -e JMQTT_KAFKA_BOOTSTRAP_SERVERS=10.0.0.5:9092 \
           -e JMQTT_REDIS_ENABLED=true -e JMQTT_REDIS_HOST=10.0.0.6 jmqtt-broker:latest
```

## 注意

- **健康检查**写死了 1883;若用 `JMQTT_PORT` 改端口,需在 compose 里覆盖 `healthcheck`。
- **epoll**:镜像内可用(pom 已带 `netty-transport-native-epoll`,Linux 容器原生支持)。
- **集群模式**:开 Kafka/Redis 前先读主 README 的集群章节,`JMQTT_BROKER_ID` 每节点必须不同。
