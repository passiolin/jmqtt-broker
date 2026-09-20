# jmqtt-broker 运行镜像。
# 构建方式: deploy/build.sh —— 它会先把可执行 jar 暂存为 deploy/app.jar,
# 再以 deploy/ 为构建上下文执行 docker build, 因此这里直接 COPY app.jar。
#
# 镜像内只有明文端口: TLS 终结在 LB 层(NLB/ALB/HAProxy/Nginx), 见 README「TLS / MQTTS」。
FROM azul/zulu-openjdk-debian:21

# 日志时间戳时区, 运行时可覆盖: docker run -e TZ=UTC ...
ENV TZ=UTC

# 非 root 运行; 端口都大于 1024, 无需特权
RUN groupadd -r jmqtt && useradd -r -g jmqtt -d /app -s /usr/sbin/nologin jmqtt

WORKDIR /app
COPY --chown=jmqtt:jmqtt app.jar /app/app.jar
USER jmqtt

# MQTT/TCP, MQTT/WebSocket, HTTP API(健康检查与管理接口)
EXPOSE 1883 8083 8922

# 容器内存感知: 堆最多占容器内存的 75%; OOM 时直接退出, 交给重启策略拉起
# (Spring Boot 2.7 + JDK 21 默认 MaxRAMPercentage=25%, 对 broker 偏保守)
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError"

# MQTT 监听端口可建立 TCP 连接即视为健康。
# 注意: 若运行时用 JMQTT_BROKER_PORT 改了端口, 这里要跟着改(compose 里覆盖 healthcheck)。
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/1883' || exit 1

# sh -c 是为了运行时展开 $JAVA_OPTS
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
