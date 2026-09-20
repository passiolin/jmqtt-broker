#!/usr/bin/env bash
#
# 运行 jmqtt-broker 容器。额外参数原样透传给 docker run(在镜像名之前)。
#
# 用法示例:
#   ./run.sh                                              # 默认端口
#   ./run.sh -e JMQTT_BROKER_AUTH_PASSWORD=sha256:xxx     # 覆盖配置(Spring Boot 松散绑定)
#   ./run.sh --memory 1g                                  # 限制内存
#   ./run.sh -p 10.0.0.5:1883:1883                        # 绑定到指定网卡
#   IMAGE=jmqtt-broker:0.1.0-SNAPSHOT ./run.sh            # 指定镜像 tag
#
# 端口约定:
#   1883 / 8083 正常发布 —— 生产上应由 LB 终结 TLS 后转发到这两个端口, 不直接暴露公网;
#   8922(HTTP API/健康检查)只绑本机, 远程访问走 SSH 隧道或内网。
set -euo pipefail
cd "$(dirname "$0")"

IMAGE="${IMAGE:-jmqtt-broker:latest}"
NAME="${NAME:-jmqtt-broker}"

if docker ps -a --format '{{.Names}}' | grep -qx "$NAME"; then
    echo "容器 $NAME 已存在, 先删除(如需保留日志请自行导出)" >&2
    docker rm -f "$NAME"
fi

exec docker run -d \
    --name "$NAME" \
    --restart unless-stopped \
    -p 1883:1883 \
    -p 8083:8083 \
    -p 127.0.0.1:8922:8922 \
    -e TZ="${TZ:-Asia/Shanghai}" \
    "$@" \
    "$IMAGE"
