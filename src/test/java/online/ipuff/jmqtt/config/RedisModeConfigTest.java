/*
 * Copyright (c) 2026 ipuff.online
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package online.ipuff.jmqtt.config;

import online.ipuff.jmqtt.TestBrokerProperties;
import online.ipuff.jmqtt.config.BrokerProperties.RedisProperties;
import online.ipuff.jmqtt.redis.RedisConnectionSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Redis 三种部署模式(单机 / 哨兵 / 集群)的装配与 fail-fast 校验。
 *
 * <p>客户端构造是惰性连接的(设计约定: Redis 挂掉不应导致 broker 起不来),
 * 因此三种模式都能在无 Redis 的测试环境里完成装配 —— 但<b>配置错误必须在
 * 启动时立刻失败</b>: 集群没有 db 选择、哨兵必须知道 master 与哨兵地址,
 * 这些错配如果静默通过, 表现都是「连不上」而不是「配错了」。
 */
class RedisModeConfigTest {

    private static final RedisConfig CONFIG = new RedisConfig();

    private static BrokerProperties propsWith(RedisProperties redis) {
        // 复用完整默认配置, 只替换 redis 段
        BrokerProperties base = TestBrokerProperties.create();
        return new BrokerProperties(
                base.id(), base.host(), base.port(), base.websocketEnabled(),
                base.websocketPort(), base.websocketPath(),
                base.bossThreads(), base.workerThreads(), base.useEpoll(), base.connectRatePerSecond(),
                base.authEnabled(), base.authUsername(), base.authPassword(),
                base.defaultKeepAlive(), base.sessionExpirySeconds(), base.topicAliasMaximum(),
                base.serverKeepAlive(), base.maxInflight(), base.maxMqueueLen(), base.maxOfflineQueueLen(),
                base.clusterEnabled(), base.kafka(), redis,
                base.soBacklog(), base.soKeepAlive(), base.tcpNoDelay(), base.maxPayloadSize(),
                base.writeBufferLowWaterMark(), base.writeBufferHighWaterMark());
    }

    private static RedisProperties redis(String mode, String masterId, List<String> nodes,
                                         String host, int port, int database) {
        return new RedisProperties(true, host, port, null, database, "jmqtt",
                1000, 5000, "off", 100, 10000, mode, masterId, nodes);
    }

    @Test
    @DisplayName("单机模式(默认): 装配成功")
    void standaloneMode() {
        RedisConnectionSource source = CONFIG.redisConnectionSource(
                propsWith(redis("standalone", null, null, "127.0.0.1", 6379, 0)));
        assertNotNull(source);
    }

    @Test
    @DisplayName("哨兵模式: 配置齐全装配成功")
    void sentinelMode() {
        RedisConnectionSource source = CONFIG.redisConnectionSource(propsWith(
                redis("sentinel", "mymaster", List.of("10.0.0.6:26379", "10.0.0.7:26379"),
                        "ignored-host", 6379, 2)));
        assertNotNull(source);
    }

    @Test
    @DisplayName("集群模式: 配置齐全装配成功")
    void clusterMode() {
        RedisConnectionSource source = CONFIG.redisConnectionSource(propsWith(
                redis("cluster", null, List.of("10.0.0.6:6379", "10.0.0.7:6379"),
                        "ignored-host", 6379, 0)));
        assertNotNull(source);
    }

    @Test
    @DisplayName("未知模式: 启动即失败")
    void unknownModeFailsFast() {
        assertThrows(IllegalStateException.class, () -> CONFIG.redisConnectionSource(
                propsWith(redis("sharded", null, null, "127.0.0.1", 6379, 0))));
    }

    @Test
    @DisplayName("哨兵缺 master-id 或 nodes: 启动即失败")
    void sentinelMissingMasterOrNodesFailsFast() {
        assertThrows(IllegalStateException.class, () -> CONFIG.redisConnectionSource(
                propsWith(redis("sentinel", null, List.of("10.0.0.6:26379"), "h", 6379, 0))),
                "没有 master-id 无法定位主节点");
        assertThrows(IllegalStateException.class, () -> CONFIG.redisConnectionSource(
                propsWith(redis("sentinel", "mymaster", List.of(), "h", 6379, 0))),
                "没有哨兵地址无法问询");
    }

    @Test
    @DisplayName("集群模式 database != 0: 启动即失败(集群只有 db0)")
    void clusterWithDatabaseFailsFast() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> CONFIG.redisConnectionSource(propsWith(
                        redis("cluster", null, List.of("10.0.0.6:6379"), "h", 6379, 2))));
        assertTrue(e.getMessage().contains("db0"), "报错要说明原因: " + e.getMessage());
    }

    @Test
    @DisplayName("集群缺 nodes: 启动即失败")
    void clusterMissingNodesFailsFast() {
        assertThrows(IllegalStateException.class, () -> CONFIG.redisConnectionSource(
                propsWith(redis("cluster", null, null, "h", 6379, 0))));
    }

    @Test
    @DisplayName("nodes 格式非法(缺端口): 启动即失败并指明是哪个节点")
    void malformedNodeFailsFast() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> CONFIG.redisConnectionSource(propsWith(
                        redis("cluster", null, List.of("10.0.0.6-not-a-port"), "h", 6379, 0))));
        assertTrue(e.getMessage().contains("10.0.0.6-not-a-port"), "报错带上出错的节点: " + e.getMessage());
    }
}
