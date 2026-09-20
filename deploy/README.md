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
deploy/run.sh -e JMQTT_BROKER_AUTH_PASSWORD=sha256:<hex>
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

## 配置覆盖

环境变量走 Spring Boot 松散绑定,`JMQTT_BROKER_XXX` 对应 `jmqtt.broker.xxx`(嵌套配置用 `_` 连接,
如 `JMQTT_BROKER_KAFKA_BOOTSTRAP_SERVERS`)。完整配置项见主 README 的配置章节。

JVM 参数通过 `JAVA_OPTS` 覆盖,默认 `-XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError`。

## 注意

- **健康检查**写死了 1883;若用 `JMQTT_BROKER_PORT` 改端口,需在 compose 里覆盖 `healthcheck`。
- **epoll**:镜像内可用(pom 已带 `netty-transport-native-epoll`,Linux 容器原生支持)。
- **集群模式**:开 Kafka/Redis 前先读主 README 的集群章节,`JMQTT_BROKER_ID` 每节点必须不同。
