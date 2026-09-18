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
 */
package online.ipuff.jmqtt.session.persistence.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.redis.RedisConnectionManager;
import online.ipuff.jmqtt.message.SubscribeStore;
import online.ipuff.jmqtt.message.WillMessage;
import online.ipuff.jmqtt.session.persistence.ISessionRepository;
import online.ipuff.jmqtt.session.persistence.PersistedSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Redis 会话持久层。
 *
 * <h2>键结构</h2>
 * <pre>
 *   {prefix}:session:{{clientId}}   Hash  node / clean / expire / created / touched / will
 *   {prefix}:subs:{{clientId}}      Hash  topicFilter -> qos
 * </pre>
 *
 * <p>两个键都用 <b>hash tag {@code {clientId}}</b> 包裹, 保证在 Redis Cluster 下
 * 落在同一个 slot —— {@code acquireOwner} 的 Lua 脚本要同时操作这两个键,
 * 不同 slot 会直接被集群拒绝。
 *
 * <p>订阅单独放一个 Hash 而不是塞进会话的某个字段: 订阅的增删是 O(1) 的
 * {@code HSET}/{@code HDEL}; 若序列化成一整个列表存一个字段, 每次订阅变更都要
 * 读-改-写整份列表, N 条订阅就会退化成 O(N²)。
 *
 * <h2>降级策略</h2>
 * 任何一步失败都只记录并返回空结果, <b>不抛异常给调用方</b>。协调层会把
 * 「拿不到会话」等同为「新会话」, 客户端的 SIG 是收到 {@code sessionPresent=false}
 * 并重新订阅。丢会话的代价是一轮重新订阅, 拒绝接入的代价是设备全部掉线 ——
 * 所以这里一律选择可用性。
 *
 * <p>Redis 不可用与恢复都会打日志; 恢复后自动重新生效, 无需重启 broker。
 */
@Component
@ConditionalOnProperty(prefix = "jmqtt.broker.redis", name = "enabled", havingValue = "true")
public class RedisSessionRepository implements ISessionRepository {

    private static final Logger log = LoggerFactory.getLogger(RedisSessionRepository.class);

    private static final String KEY_SEGMENT_SESSION = ":session:";
    private static final String KEY_SEGMENT_SUBS = ":subs:";

    private static final String FIELD_NODE = "node";
    private static final String FIELD_CLEAN = "clean";
    private static final String FIELD_EXPIRE = "expire";
    private static final String FIELD_CREATED = "created";
    private static final String FIELD_TOUCHED = "touched";
    private static final String FIELD_WILL = "will";

    /**
     * 原子接管: 读旧 owner + 写新 owner + 续期。
     *
     * <p>拆成「先 HGET 再 HSET」两步的话, 两个节点同时接管会双双读到空 owner,
     * 于是都认为自己是归属方 —— 结果就是两份路由、重复投递。
     * Lua 在 Redis 里单线程执行, 天然互斥。
     */
    private static final String ACQUIRE_SCRIPT =
            "local prev = redis.call('HGET', KEYS[1], 'node') "
                    + "redis.call('HSET', KEYS[1], 'node', ARGV[1], 'touched', ARGV[3]) "
                    + "local ttl = tonumber(ARGV[2]) "
                    + "if ttl > 0 then "
                    + "  redis.call('EXPIRE', KEYS[1], ttl) "
                    + "  redis.call('EXPIRE', KEYS[2], ttl) "
                    + "end "
                    + "if prev == false then return '' end "
                    + "return prev";

    private final BrokerProperties properties;
    private final RedisConnectionManager redis;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public RedisSessionRepository(BrokerProperties properties, RedisConnectionManager redis) {
        this.properties = properties;
        this.redis = redis;
        BrokerProperties.RedisProperties redisProperties = properties.redis();
        log.info("会话持久化已启用: keyPrefix={} (连接由 RedisConnectionManager 惰性建立)",
                redisProperties.keyPrefix());
    }

    // ------------------------------------------------------------------
    // ISessionRepository
    // ------------------------------------------------------------------

    @Override
    public boolean available() {
        return redis.available();
    }

    @Override
    public PersistedSession load(String clientId) {
        Map<String, String> sessionFields = execute("load.session",
                () -> commands().hgetall(sessionKey(clientId)), Collections.emptyMap());
        if (sessionFields.isEmpty() || !sessionFields.containsKey(FIELD_NODE)) {
            return null;
        }
        Map<String, String> subFields = execute("load.subs",
                () -> commands().hgetall(subsKey(clientId)), Collections.emptyMap());

        List<SubscribeStore> subscriptions = new ArrayList<>(subFields.size());
        subFields.forEach((filter, qos) -> subscriptions.add(
                new SubscribeStore(clientId, filter, parseQos(qos))));

        return new PersistedSession(
                clientId,
                sessionFields.get(FIELD_NODE),
                !"0".equals(sessionFields.get(FIELD_CLEAN)),
                parseLong(sessionFields.get(FIELD_EXPIRE), 0L),
                parseLong(sessionFields.get(FIELD_CREATED), System.currentTimeMillis()),
                decodeWill(sessionFields.get(FIELD_WILL)),
                subscriptions);
    }

    @Override
    public String acquireOwner(String clientId, String nodeId, long expireSeconds) {
        String previous = execute("acquireOwner", () -> commands().eval(
                ACQUIRE_SCRIPT,
                ScriptOutputType.VALUE,
                new String[]{sessionKey(clientId), subsKey(clientId)},
                nodeId,
                Long.toString(expireSeconds),
                Long.toString(System.currentTimeMillis())), "");
        return (previous == null || previous.isEmpty()) ? null : previous;
    }

    @Override
    public void saveSession(String clientId, String nodeId, boolean cleanSession,
                            long expireSeconds, WillMessage willMessage) {
        Map<String, String> fields = new HashMap<>();
        fields.put(FIELD_NODE, nodeId);
        fields.put(FIELD_CLEAN, cleanSession ? "1" : "0");
        fields.put(FIELD_EXPIRE, Long.toString(expireSeconds));
        fields.put(FIELD_CREATED, Long.toString(System.currentTimeMillis()));
        fields.put(FIELD_TOUCHED, Long.toString(System.currentTimeMillis()));
        String will = encodeWill(willMessage);
        if (will != null) {
            fields.put(FIELD_WILL, will);
        }
        execute("saveSession", () -> {
            RedisCommands<String, String> cmd = commands();
            String key = sessionKey(clientId);
            cmd.hset(key, fields);
            if (expireSeconds > 0) {
                cmd.expire(key, expireSeconds);
            }
            return Boolean.TRUE;
        }, Boolean.FALSE);
    }

    @Override
    public void saveSubscription(String clientId, String topicFilter, int qos) {
        execute("saveSubscription", () -> {
            RedisCommands<String, String> cmd = commands();
            String key = subsKey(clientId);
            cmd.hset(key, topicFilter, Integer.toString(qos));
            long expireSeconds = sessionExpireSeconds(clientId);
            if (expireSeconds > 0) {
                cmd.expire(key, expireSeconds);
            }
            return Boolean.TRUE;
        }, Boolean.FALSE);
    }

    @Override
    public void removeSubscription(String clientId, String topicFilter) {
        execute("removeSubscription", () -> {
            commands().hdel(subsKey(clientId), topicFilter);
            return Boolean.TRUE;
        }, Boolean.FALSE);
    }

    @Override
    public void removeSubscriptions(String clientId) {
        execute("removeSubscriptions", () -> {
            commands().del(subsKey(clientId));
            return Boolean.TRUE;
        }, Boolean.FALSE);
    }

    @Override
    public void remove(String clientId) {
        execute("remove", () -> {
            commands().del(sessionKey(clientId), subsKey(clientId));
            return Boolean.TRUE;
        }, Boolean.FALSE);
    }

    // ------------------------------------------------------------------
    // 内部
    // ------------------------------------------------------------------

    /**
     * 委托给共享的 {@link RedisConnectionManager} 执行。
     *
     * <p>降级策略(失败只记录不抛、状态变化才打日志、恢复后自动生效)
     * 集中在那里 —— 复制到每个组件里迟早会有一处写得不一样,
     * 而「有一处没降级」就意味着 Redis 抖动会直接抛到 MQTT 处理路径上。
     */
    private <T> T execute(String op, Supplier<T> action, T fallback) {
        return redis.execute(op, action, fallback);
    }

    private RedisCommands<String, String> commands() {
        return redis.commands();
    }

    private String sessionKey(String clientId) {
        // {clientId} 是 hash tag: 让 session 与 subs 两个键落在同一 slot,
        // acquireOwner 的 Lua 才能同时操作它们(Redis Cluster 要求)
        return properties.redis().keyPrefix() + KEY_SEGMENT_SESSION + "{" + clientId + "}";
    }

    private String subsKey(String clientId) {
        return properties.redis().keyPrefix() + KEY_SEGMENT_SUBS + "{" + clientId + "}";
    }

    private long sessionExpireSeconds(String clientId) {
        String value = execute("readExpire",
                () -> commands().hget(sessionKey(clientId), FIELD_EXPIRE), null);
        return parseLong(value, 0L);
    }

    private static int parseQos(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static long parseLong(String value, long fallback) {
        if (value == null) {
            return fallback;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /**
     * 遗嘱消息序列化。只存领域字段, 不序列化 Netty 报文对象。
     */
    private String encodeWill(WillMessage will) {
        if (will == null) {
            return null;
        }
        try {
            WillJson json = new WillJson(
                    will.topic(),
                    will.payload() == null ? "" : Base64.getEncoder().encodeToString(will.payload()),
                    will.qos(),
                    will.retain(),
                    will.willDelaySeconds());
            return objectMapper.writeValueAsString(json);
        } catch (Exception e) {
            log.warn("遗嘱消息序列化失败, 将不持久化", e);
            return null;
        }
    }

    private WillMessage decodeWill(String json) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            WillJson decoded = objectMapper.readValue(json, WillJson.class);
            byte[] payload = decoded.payload() == null || decoded.payload().isEmpty()
                    ? new byte[0]
                    : Base64.getDecoder().decode(decoded.payload());
            return new WillMessage(decoded.topic(), payload, decoded.qos(), decoded.retain(),
                    decoded.willDelaySeconds());
        } catch (Exception e) {
            log.warn("遗嘱消息反序列化失败, 按无遗嘱处理", e);
            return null;
        }
    }

    /**
     * 遗嘱消息的持久化形态。
     */
    private record WillJson(String topic, String payload, int qos, boolean retain,
                            long willDelaySeconds) {
    }
}
