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
package online.ipuff.jmqtt.store.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.message.RetainMessageStore;
import online.ipuff.jmqtt.redis.RedisConnectionManager;
import online.ipuff.jmqtt.redis.RedisSyncCommands;
import online.ipuff.jmqtt.router.MqttTopic;
import online.ipuff.jmqtt.router.TopicTrie;
import online.ipuff.jmqtt.store.IRetainMessageStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Redis 支撑的保留消息存储(启用 {@code jmqtt.broker.redis.enabled} 时装配)。
 *
 * <h2>为什么保留消息必须离开纯内存</h2>
 * 保留消息是<b>数据</b>而不是路由状态(订阅才是路由状态, 留在本地主题树)。
 * 纯内存实现下, 设备把 retain 发到节点 A, 应用在节点 B 订阅同一个主题 ——
 * B 的保留库里什么都没有, 订阅者永远收不到这条状态; 节点重启同样清零。
 *
 * <h2>读写路径</h2>
 * <ul>
 *   <li><b>写</b>(发布 retain / 遗嘱 retain): 写 Redis(HASH) + 立即更新本地视图,
 *       并自增版本号</li>
 *   <li><b>精确读</b>(无通配符的过滤器): 直接 HGET, 恒读到最新</li>
 *   <li><b>通配读</b>: 本地主题树扫描, 但先比对版本号 —— 版本变了才全量刷新。
 *       版本比较是 O(1) 一次往返, 全量拉取只发生在「别的节点确实改过保留库」时,
 *       订阅风暴不会打爆 Redis</li>
 * </ul>
 *
 * <h2>生命周期</h2>
 * 按 MQTT 语义, 保留消息<b>不设 TTL</b> —— 只在被覆盖或显式清除(空 payload)时消失。
 * 这是全库唯一长期驻留的 key, 运维需关注其容量; 不适用派生 key 的 TTL 口径。
 */
@Service
@ConditionalOnProperty(prefix = "jmqtt.broker.redis", name = "enabled", havingValue = "true")
public class RedisRetainMessageStore implements IRetainMessageStoreService {

    private static final Logger log = LoggerFactory.getLogger(RedisRetainMessageStore.class);

    private static final String KEY_SEGMENT_RETAINED = ":retained";
    private static final String KEY_SEGMENT_RETAINED_VER = ":retained:ver";

    private final BrokerProperties properties;
    private final RedisConnectionManager redis;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** 本地视图: 通配扫描用主题树(重建式刷新, 无锁读) */
    private volatile TopicTrie<RetainMessageStore> snapshot = new TopicTrie<>();
    /** 本地视图对应的 Redis 版本号 */
    private volatile long snapshotVersion = -1;

    public RedisRetainMessageStore(BrokerProperties properties, RedisConnectionManager redis) {
        this.properties = properties;
        this.redis = redis;
        log.info("保留消息存储已启用 Redis 后端: key={}", retainedKey());
    }

    @Override
    public void put(RetainMessageStore retain) {
        String topic = retain.topic();
        String json = encode(retain);
        if (json == null) {
            return;
        }
        Long version = redis.execute("retain.put", () -> {
            RedisSyncCommands cmd = commands();
            cmd.hset(retainedKey(), topic, json);
            return cmd.incr(verKey());
        }, null);
        TopicTrie<RetainMessageStore> local = snapshot;
        local.insert(MqttTopic.levels(topic), topic, retain);
        if (version != null) {
            // 本节点自己的写已反映在本地视图里, 版本号跟上, 避免下一次读白刷新一轮
            snapshotVersion = version;
        }
    }

    @Override
    public RetainMessageStore get(String topic) {
        String json = redis.execute("retain.get",
                () -> commands().hget(retainedKey(), topic), null);
        if (json == null) {
            // Redis 不可用或确无此主题: 退本地视图
            Map<String, RetainMessageStore> matched = snapshot.scanByFilter(MqttTopic.levels(topic));
            return matched.get(topic);
        }
        return decode(topic, json);
    }

    @Override
    public boolean remove(String topic) {
        Long removed = redis.execute("retain.remove", () -> {
            RedisSyncCommands cmd = commands();
            long n = cmd.hdel(retainedKey(), topic);
            cmd.incr(verKey());
            return n;
        }, null);
        boolean localRemoved = snapshot.remove(MqttTopic.levels(topic), topic);
        if (removed == null) {
            return localRemoved;
        }
        return removed > 0;
    }

    @Override
    public boolean containsKey(String topic) {
        return get(topic) != null;
    }

    @Override
    public List<RetainMessageStore> search(String topicFilter) {
        if (topicFilter == null || topicFilter.isEmpty()) {
            return List.of();
        }
        if (!topicFilter.contains("+") && !topicFilter.contains("#")) {
            // 精确主题: 一次 HGET 恒读到最新, 不依赖本地视图
            RetainMessageStore exact = get(topicFilter);
            return exact == null ? List.of() : List.of(exact);
        }
        refreshIfChanged();
        Map<String, RetainMessageStore> matched = snapshot.scanByFilter(MqttTopic.levels(topicFilter));
        return matched.isEmpty() ? List.of() : new ArrayList<>(matched.values());
    }

    @Override
    public int size() {
        refreshIfChanged();
        return snapshot.valueCount();
    }

    // ------------------------------------------------------------------
    // 内部
    // ------------------------------------------------------------------

    /**
     * 版本号变了才全量刷新本地视图。版本比较一次 O(1) 往返, 订阅风暴的代价恒定;
     * 全量 HGETALL 只发生在别的节点真的改过保留库之后。
     *
     * <p>版本号取<b>刷新前</b>的值记入本地 —— 刷新期间到达的写会把版本推得更高,
     * 下一次读会再刷新, 宁可多刷一次也不把旧数据当新数据。
     */
    private void refreshIfChanged() {
        String rawVersion = redis.execute("retain.ver", () -> commands().get(verKey()), null);
        if (rawVersion == null) {
            // Redis 不可用: 用本地视图(可能滞后), 可用性优先于新鲜度
            return;
        }
        long version;
        try {
            version = Long.parseLong(rawVersion);
        } catch (NumberFormatException e) {
            version = -1;
        }
        if (version == snapshotVersion) {
            return;
        }
        Map<String, String> all = redis.execute("retain.scan",
                () -> commands().hgetall(retainedKey()), Collections.emptyMap());
        TopicTrie<RetainMessageStore> fresh = new TopicTrie<>();
        for (Map.Entry<String, String> entry : all.entrySet()) {
            RetainMessageStore retain = decode(entry.getKey(), entry.getValue());
            if (retain != null) {
                fresh.insert(MqttTopic.levels(entry.getKey()), entry.getKey(), retain);
            }
        }
        snapshot = fresh;
        snapshotVersion = version;
        log.debug("保留消息本地视图已刷新: 条数={}", fresh.valueCount());
    }

    private RedisSyncCommands commands() {
        return redis.commands();
    }

    private String retainedKey() {
        return properties.redis().keyPrefix() + KEY_SEGMENT_RETAINED;
    }

    private String verKey() {
        return properties.redis().keyPrefix() + KEY_SEGMENT_RETAINED_VER;
    }

    private String encode(RetainMessageStore retain) {
        try {
            return objectMapper.writeValueAsString(new RetainJson(
                    retain.qos(),
                    retain.payload() == null ? "" : Base64.getEncoder().encodeToString(retain.payload())));
        } catch (Exception e) {
            log.warn("保留消息序列化失败, 跳过: topic={}", retain.topic(), e);
            return null;
        }
    }

    private RetainMessageStore decode(String topic, String json) {
        try {
            RetainJson decoded = objectMapper.readValue(json, RetainJson.class);
            byte[] payload = decoded.payload() == null || decoded.payload().isEmpty()
                    ? new byte[0]
                    : Base64.getDecoder().decode(decoded.payload());
            return new RetainMessageStore(topic, payload, decoded.qos());
        } catch (Exception e) {
            log.warn("保留消息反序列化失败, 跳过: topic={}", topic, e);
            return null;
        }
    }

    private record RetainJson(int qos, String payload) {
    }
}
