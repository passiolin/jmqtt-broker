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
package online.ipuff.jmqtt.redis;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.util.Map;

/**
 * Redis 连接来源: 屏蔽单机 / 哨兵 / 集群的客户端差异。
 *
 * <p>哨兵与单机用 {@link RedisClient}(只是 URI 构造不同), 集群用
 * {@link RedisClusterClient} —— 两者的连接与命令类型都是平行层次, 所以
 * {@link #open} 返回「连接本体 + {@link RedisSyncCommands} 命令视图」的对子,
 * 命令视图由对应的薄适配器委托实现。连接管理层({@code RedisConnectionManager})
 * 只依赖这个统一形态, 不感知模式。
 *
 * <p>客户端本身<b>惰性连接</b>: 构造本对象不建连, 首次 {@link #open} 才连。
 */
public final class RedisConnectionSource implements AutoCloseable {

    private final AbstractRedisClient client;

    public RedisConnectionSource(AbstractRedisClient client) {
        this.client = client;
    }

    /** 打开一条连接。线程安全(多路复用), 但通常只由连接管理层单点调用 */
    public Handle open() {
        if (client instanceof RedisClusterClient cluster) {
            StatefulRedisClusterConnection<String, String> connection = cluster.connect();
            return new Handle(connection, new ClusterCommands(connection.sync()));
        }
        StatefulRedisConnection<String, String> connection = ((RedisClient) client).connect();
        return new Handle(connection, new StandaloneCommands(connection.sync()));
    }

    @Override
    public void close() {
        client.shutdown();
    }

    /**
     * 一条已打开的连接: 本体(用于 isOpen 判断与生命周期)与同步命令视图。
     */
    public record Handle(StatefulConnection<String, String> connection, RedisSyncCommands commands) {
    }

    /** 单机/哨兵命令的委托适配 */
    private record StandaloneCommands(RedisCommands<String, String> c) implements RedisSyncCommands {
        @Override
        public String ping() {
            return c.ping();
        }

        @Override
        public Long del(String... keys) {
            return c.del(keys);
        }

        @Override
        public Boolean expire(String key, long seconds) {
            return c.expire(key, seconds);
        }

        @Override
        public Long hdel(String key, String... fields) {
            return c.hdel(key, fields);
        }

        @Override
        public String hget(String key, String field) {
            return c.hget(key, field);
        }

        @Override
        public Map<String, String> hgetall(String key) {
            return c.hgetall(key);
        }

        @Override
        public Boolean hset(String key, String field, String value) {
            return c.hset(key, field, value);
        }

        @Override
        public Long hset(String key, Map<String, String> map) {
            return c.hset(key, map);
        }

        @Override
        public Long sadd(String key, String... members) {
            return c.sadd(key, members);
        }

        @Override
        public Long llen(String key) {
            return c.llen(key);
        }

        @Override
        public String rpop(String key) {
            return c.rpop(key);
        }

        @Override
        public <T> T eval(String script, ScriptOutputType type, String[] keys, String... values) {
            return c.eval(script, type, keys, values);
        }
    }

    /** 集群命令的委托适配 */
    private record ClusterCommands(RedisAdvancedClusterCommands<String, String> c)
            implements RedisSyncCommands {
        @Override
        public String ping() {
            return c.ping();
        }

        @Override
        public Long del(String... keys) {
            return c.del(keys);
        }

        @Override
        public Boolean expire(String key, long seconds) {
            return c.expire(key, seconds);
        }

        @Override
        public Long hdel(String key, String... fields) {
            return c.hdel(key, fields);
        }

        @Override
        public String hget(String key, String field) {
            return c.hget(key, field);
        }

        @Override
        public Map<String, String> hgetall(String key) {
            return c.hgetall(key);
        }

        @Override
        public Boolean hset(String key, String field, String value) {
            return c.hset(key, field, value);
        }

        @Override
        public Long hset(String key, Map<String, String> map) {
            return c.hset(key, map);
        }

        @Override
        public Long sadd(String key, String... members) {
            return c.sadd(key, members);
        }

        @Override
        public Long llen(String key) {
            return c.llen(key);
        }

        @Override
        public String rpop(String key) {
            return c.rpop(key);
        }

        @Override
        public <T> T eval(String script, ScriptOutputType type, String[] keys, String... values) {
            return c.eval(script, type, keys, values);
        }
    }
}
