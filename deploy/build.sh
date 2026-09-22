#!/usr/bin/env bash
#
# 构建 jmqtt-broker Docker 镜像:
#   1. mvn package 打出可执行 jar(默认含测试)
#   2. 把 jar 暂存为 deploy/app.jar —— 构建上下文只有 deploy/, 不必把整个仓库发给 daemon
#   3. docker build, 同时打 <name>:<version> 与 <name>:latest 两个 tag
#
# 用法:
#   ./build.sh               # 完整构建(含测试)
#   ./build.sh --skip-tests  # 跳过测试
#
# 环境变量:
#   IMAGE_NAME  镜像名, 默认 jmqtt-broker
#   MAVEN_ARGS  追加的 maven 参数。例如受限网络(空 TLS 信任库)下:
#               MAVEN_ARGS="-Dmaven.resolver.transport=wagon -Dmaven.wagon.http.ssl.insecure=true" ./build.sh
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE_NAME="${IMAGE_NAME:-jmqtt-broker}"
MAVEN_ARGS="${MAVEN_ARGS:-}"

# pom.xml 里本项目自己的 <version> 紧跟在 <artifactId>jmqtt-broker</artifactId> 之后
VERSION="$(sed -n '/<artifactId>jmqtt-broker<\/artifactId>/{n;p}' pom.xml | sed -e 's:.*<version>\(.*\)</version>.*:\1:')"
if [[ -z "$VERSION" ]]; then
    echo "无法从 pom.xml 解析版本号" >&2
    exit 1
fi

MVN_ARGS=($MAVEN_ARGS package)
if [[ "${1:-}" == "--skip-tests" ]]; then
    MVN_ARGS+=( -DskipTests )
fi

echo "==> mvn ${MVN_ARGS[*]}"
mvn "${MVN_ARGS[@]}"

JAR="target/jmqtt-broker.jar"
if [[ ! -f "$JAR" ]]; then
    echo "找不到 ${JAR}" >&2
    exit 1
fi

echo "==> 暂存 ${JAR} -> deploy/app.jar"
cp -f "$JAR" deploy/app.jar

echo "==> docker build (${IMAGE_NAME}:${VERSION} / ${IMAGE_NAME}:latest)"
docker build -t "${IMAGE_NAME}:${VERSION}" -t "${IMAGE_NAME}:latest" -f deploy/Dockerfile deploy/

echo "==> 完成: ${IMAGE_NAME}:${VERSION}"
