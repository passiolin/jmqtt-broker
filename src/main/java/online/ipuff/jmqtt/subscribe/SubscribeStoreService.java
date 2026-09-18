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
package online.ipuff.jmqtt.subscribe;

import online.ipuff.jmqtt.message.SubscribeStore;
import online.ipuff.jmqtt.router.MqttTopic;
import online.ipuff.jmqtt.router.TopicTrie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;

/**
 * 订阅存储的进程内实现, 基于 {@link TopicTrie}。
 *
 * <p>朴素做法是订阅关系存外部存储、每次发布把全部通配订阅拉出来做字符串拼装比对
 * —— 匹配成本随订阅总数增长, 且投递路径上挂着一次网络往返。这里改为纯内存主题树:
 * <ul>
 *   <li>匹配复杂度 O(主题层数), 与订阅总数无关</li>
 *   <li>投递路径完全不访问外部存储</li>
 *   <li>剪枝由主题树在删除时自动完成</li>
 * </ul>
 *
 * <p>额外维护 {@code clientId -> (过滤器 -> 订阅)} 的倒排索引, 用于
 * {@link #removeForClient} 与 {@link #subscriptionsOf} —— 主题树无法按 key 反查。
 */
@Service
public class SubscribeStoreService implements ISubscribeStoreService {

    private static final Logger log = LoggerFactory.getLogger(SubscribeStoreService.class);

    /** 同一客户端被多条过滤器命中时, 取最大 QoS(规范要求按最大 QoS 投递) */
    private static final BinaryOperator<SubscribeStore> MAX_QOS =
            (a, b) -> a.qos() >= b.qos() ? a : b;

    /** 订阅树: 层级 = 过滤器, 节点值的 key = clientId */
    private final TopicTrie<SubscribeStore> trie = new TopicTrie<>();

    /** 倒排索引: clientId -> (topicFilter -> 订阅) */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, SubscribeStore>> clientIndex =
            new ConcurrentHashMap<>();

    /**
     * 订阅变更版本号。管理面用它跳过「订阅没变却要重算过滤器视图」的无用功。
     *
     * <p>只增不减、不做回绕处理: 累加到 long 溢出需要几百年, 而回绕带来的后果
     * 仅仅是「多算一次视图」—— 不值得为它引入取模逻辑。
     */
    private final AtomicLong mutationVersion = new AtomicLong();

    @Override
    public void put(SubscribeStore subscribeStore) {
        String clientId = subscribeStore.clientId();
        String topicFilter = subscribeStore.topicFilter();

        trie.insert(MqttTopic.levels(topicFilter), clientId, subscribeStore);
        clientIndex.computeIfAbsent(clientId, k -> new ConcurrentHashMap<>()).put(topicFilter, subscribeStore);
        mutationVersion.incrementAndGet();

        log.debug("SUBSCRIBE clientId={} topicFilter={} qos={}", clientId, topicFilter, subscribeStore.qos());
    }

    @Override
    public boolean remove(String clientId, String topicFilter) {
        boolean removed = trie.remove(MqttTopic.levels(topicFilter), clientId);

        Map<String, SubscribeStore> filters = clientIndex.get(clientId);
        if (filters != null) {
            filters.remove(topicFilter);
            if (filters.isEmpty()) {
                clientIndex.remove(clientId, filters);
            }
        }
        mutationVersion.incrementAndGet();
        log.debug("UNSUBSCRIBE clientId={} topicFilter={} removed={}", clientId, topicFilter, removed);
        return removed;
    }

    @Override
    public int removeForClient(String clientId) {
        Map<String, SubscribeStore> filters = clientIndex.remove(clientId);
        if (filters == null || filters.isEmpty()) {
            return 0;
        }
        int removed = 0;
        for (String topicFilter : filters.keySet()) {
            if (trie.remove(MqttTopic.levels(topicFilter), clientId)) {
                removed++;
            }
        }
        mutationVersion.incrementAndGet();
        log.debug("removeForClient clientId={} removed={}", clientId, removed);
        return removed;
    }

    @Override
    public List<SubscribeStore> search(String topic) {
        if (topic == null || topic.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, SubscribeStore> matched = trie.matchTopic(MqttTopic.levels(topic), MAX_QOS);
        if (matched.isEmpty()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(matched.values());
    }

    @Override
    public Collection<SubscribeStore> subscriptionsOf(String clientId) {
        Map<String, SubscribeStore> filters = clientIndex.get(clientId);
        if (filters == null || filters.isEmpty()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(filters.values());
    }

    @Override
    public int subscriptionCount() {
        return trie.valueCount();
    }

    @Override
    public int topicNodeCount() {
        return trie.nodeCount();
    }

    @Override
    public long mutationVersion() {
        return mutationVersion.get();
    }

    @Override
    public Map<String, Integer> filterHistogram() {
        Map<String, Integer> histogram = new HashMap<>();
        // 主题树上「存有值的节点」正好对应一个过滤器, 其值的条数就是订阅者数 ——
        // 因此一次深度优先遍历即可得到直方图, 不需要按 clientId 再聚合一遍
        trie.forEachValue((levels, values) -> {
            if (!values.isEmpty()) {
                histogram.put(String.join("/", levels), values.size());
            }
            return true;
        });
        return histogram;
    }
}
