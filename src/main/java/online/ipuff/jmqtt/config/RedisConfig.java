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

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import online.ipuff.jmqtt.redis.RedisConnectionSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Redis 客户端配置, 支持三种部署模式({@code redis.mode}):
 *
 * <ul>
 *   <li><b>standalone</b>(默认)—— 单机, {@code host/port/password/database}</li>
 *   <li><b>sentinel</b> —— 哨兵主从, {@code master-id} + 哨兵地址列表 {@code nodes};
 *       连接由哨兵指向当前主节点, 主从切换后自动跟随</li>
 *   <li><b>cluster</b> —— 分片集群, 种子节点列表 {@code nodes};
 *       {@code database} 必须为 0(集群只有 db0)</li>
 * </ul>
 *
 * <p>三种模式都<b>刻意不在启动时建立连接</b>: Redis 挂掉不应该导致 broker 起不来。
 * 但<b>配置错误必须在启动时立刻失败</b> —— 缺 master-id、集群配了 db、节点格式非法
 * 这类错配如果静默通过, 运行期的表现是「连不上」而不是「配错了」, 排查方向全错。
 */
@Configuration
@ConditionalOnProperty(prefix = "jmqtt.broker.redis", name = "enabled", havingValue = "true")
public class RedisConfig {

    private static final Logger log = LoggerFactory.getLogger(RedisConfig.class);

    @Bean(destroyMethod = "close")
    public RedisConnectionSource redisConnectionSource(BrokerProperties properties) {
        BrokerProperties.RedisProperties redis = properties.redis();

        // 单机/哨兵是 RedisClient, 集群是 RedisClusterClient —— 共同父类上设默认超时
        io.lettuce.core.AbstractRedisClient client = createClient(redis);
        // 命令超时必须设小: 这些命令都在连接建立路径上, Redis 卡住会直接拖慢握手
        client.setDefaultTimeout(Duration.ofMillis(redis.commandTimeoutMs()));
        log.info("Redis 客户端已创建(惰性连接): mode={} {}",
                modeOf(redis), describeTargets(redis));
        return new RedisConnectionSource(client);
    }

    private static io.lettuce.core.AbstractRedisClient createClient(BrokerProperties.RedisProperties redis) {
        if (redis.clusterMode()) {
            requireNodes(redis, "cluster");
            if (redis.database() != 0) {
                throw new IllegalStateException(
                        "redis.mode=cluster 但 database=" + redis.database()
                                + " —— Redis Cluster 只有 db0, 请将 database 置 0 或改用 standalone/sentinel 模式。");
            }
            List<RedisURI> seeds = new ArrayList<>();
            for (String node : redis.nodes()) {
                String[] hostPort = parseNode(node);
                seeds.add(uri(hostPort[0], Integer.parseInt(hostPort[1]), 0, redis.password()));
            }
            return RedisClusterClient.create(seeds);
        }
        if (redis.sentinelMode()) {
            if (redis.masterId() == null || redis.masterId().isBlank()) {
                throw new IllegalStateException(
                        "redis.mode=sentinel 但未配置 master-id(哨兵 monitor 的主节点名称)。");
            }
            requireNodes(redis, "sentinel");
            RedisURI.Builder builder = RedisURI.Builder.sentinel(redis.masterId())
                    .withDatabase(redis.database());
            applyPassword(builder, redis.password());
            for (String node : redis.nodes()) {
                String[] hostPort = parseNode(node);
                builder.withSentinel(hostPort[0], Integer.parseInt(hostPort[1]));
            }
            return RedisClient.create(builder.build());
        }
        if (!"standalone".equalsIgnoreCase(redis.mode())) {
            throw new IllegalStateException("未知的 redis.mode: " + redis.mode()
                    + " —— 支持 standalone / sentinel / cluster。");
        }
        RedisURI.Builder builder = RedisURI.Builder
                .redis(redis.host(), redis.port())
                .withDatabase(redis.database());
        applyPassword(builder, redis.password());
        return RedisClient.create(builder.build());
    }

    /** 构造带密码的节点 URI(集群种子) */
    private static RedisURI uri(String host, int port, int database, String password) {
        RedisURI.Builder builder = RedisURI.Builder.redis(host, port).withDatabase(database);
        applyPassword(builder, password);
        return builder.build();
    }

    private static void applyPassword(RedisURI.Builder builder, String password) {
        if (password != null && !password.isBlank()) {
            builder.withPassword(password);
        }
    }

    private static void requireNodes(BrokerProperties.RedisProperties redis, String mode) {
        if (redis.nodes() == null || redis.nodes().isEmpty()) {
            throw new IllegalStateException(
                    "redis.mode=" + mode + " 但未配置 nodes(节点地址列表, 形如 10.0.0.6:6379)。");
        }
    }

    /** 解析 host:port; 非法时启动即失败并指明是哪个节点 */
    private static String[] parseNode(String node) {
        if (node == null) {
            throw badNode("null");
        }
        String[] parts = node.split(":", 2);
        if (parts.length != 2 || parts[0].isBlank() || !parts[1].matches("\\d{1,5}")) {
            throw badNode(node);
        }
        return parts;
    }

    private static IllegalStateException badNode(String node) {
        return new IllegalStateException(
                "redis.nodes 中的节点地址非法: \"" + node + "\" —— 应为 host:port 形式(如 10.0.0.6:6379)。");
    }

    private static String modeOf(BrokerProperties.RedisProperties redis) {
        return redis.mode() == null ? "standalone" : redis.mode();
    }

    private static String describeTargets(BrokerProperties.RedisProperties redis) {
        if (redis.clusterMode() || redis.sentinelMode()) {
            return (redis.clusterMode() ? "seeds=" : "sentinels=") + redis.nodes();
        }
        return "target=" + redis.host() + ":" + redis.port();
    }
}
