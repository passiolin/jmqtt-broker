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
package online.ipuff.jmqtt.store;

import online.ipuff.jmqtt.message.RetainMessageStore;
import online.ipuff.jmqtt.router.MqttTopic;
import online.ipuff.jmqtt.router.TopicTrie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 保留消息存储的进程内实现。
 *
 * <p>朴素做法是把保留消息存进外部存储, 匹配时把<b>全部保留消息</b>拉出来逐条做字符串拼装比对
 * —— 复杂度 O(全部保留消息数), 且每条都带网络往返。
 *
 * <p>这里用一棵主题树索引, 走 <b>过滤器驱动</b> 的 {@link TopicTrie#scanByFilter}:
 * 树里存具体主题名, 过滤器中的 {@code +} / {@code #} 表现为枚举子节点,
 * 复杂度降到 O(匹配结果数)。
 *
 * <p>注意: 这与订阅路由的 {@link TopicTrie#matchTopic} 是<b>相反方向</b>的遍历,
 * 不能复用同一个方法 —— 把过滤器当成主题去走主题驱动的匹配, 是这里最容易犯的错。
 */
@Service
public class RetainMessageStoreService implements IRetainMessageStoreService {

    private static final Logger log = LoggerFactory.getLogger(RetainMessageStoreService.class);

    /** 保留消息树: 层级 = 具体主题名, 节点值的 key = 主题名 */
    private final TopicTrie<RetainMessageStore> trie = new TopicTrie<>();

    @Override
    public void put(RetainMessageStore retain) {
        String topic = retain.topic();
        trie.insert(MqttTopic.levels(topic), topic, retain);
        log.debug("RETAIN put topic={} qos={} size={}", topic, retain.qos(),
                retain.payload() == null ? 0 : retain.payload().length);
    }

    @Override
    public RetainMessageStore get(String topic) {
        // 主题树没有按 key 的单点取值, 用精确路径扫描(无通配符时只走一条分支)
        Map<String, RetainMessageStore> matched = trie.scanByFilter(MqttTopic.levels(topic));
        return matched.get(topic);
    }

    @Override
    public boolean remove(String topic) {
        boolean removed = trie.remove(MqttTopic.levels(topic), topic);
        log.debug("RETAIN remove topic={} removed={}", topic, removed);
        return removed;
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
        Map<String, RetainMessageStore> matched = trie.scanByFilter(MqttTopic.levels(topicFilter));
        if (matched.isEmpty()) {
            return List.of();
        }
        return new ArrayList<>(matched.values());
    }

    @Override
    public int size() {
        return trie.valueCount();
    }
}
