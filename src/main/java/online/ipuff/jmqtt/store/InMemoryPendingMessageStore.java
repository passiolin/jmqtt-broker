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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 离线消息队列的进程内实现。
 *
 * <p>在未启用会话持久化({@code jmqtt.broker.redis.enabled=false})时装配。
 * 队列仍然可用 —— 客户端短暂掉线期间的消息会被缓存并在重连后投递,
 * 只是节点重启会丢。这比「直接丢弃」已经是实质改善。
 *
 * <p>每个客户端一条 {@link ArrayDeque}, 单条队列内部同步。
 * 不同客户端之间无竞争, 因此这把锁的粒度足够细。
 */
@Component
@ConditionalOnProperty(prefix = "jmqtt.broker.redis", name = "enabled",
        havingValue = "false", matchIfMissing = true)
public class InMemoryPendingMessageStore implements IPendingMessageStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryPendingMessageStore.class);

    private final ConcurrentHashMap<String, Deque<PendingMessage>> queues = new ConcurrentHashMap<>();

    private final AtomicLong droppedCount = new AtomicLong();

    public InMemoryPendingMessageStore() {
        log.info("离线消息队列使用进程内实现(节点重启会丢失积压消息)");
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public int enqueue(String clientId, PendingMessage message, int maxLen) {
        if (maxLen <= 0) {
            recordDrop(clientId, 1, "队列长度上限配置为 0, 不缓存离线消息");
            return 1;
        }
        Deque<PendingMessage> queue = queues.computeIfAbsent(clientId, k -> new ArrayDeque<>());
        int dropped = 0;
        synchronized (queue) {
            queue.addLast(message);
            // 超限时丢弃最旧的: IoT 场景下「最近的数据」通常更有价值
            while (queue.size() > maxLen) {
                queue.pollFirst();
                dropped++;
            }
        }
        if (dropped > 0) {
            recordDrop(clientId, dropped, "超过上限 " + maxLen);
        }
        return dropped;
    }

    @Override
    public List<PendingMessage> drainBatch(String clientId, int limit) {
        Deque<PendingMessage> queue = queues.get(clientId);
        if (queue == null || limit <= 0) {
            return List.of();
        }
        List<PendingMessage> batch = new ArrayList<>(Math.min(limit, 16));
        synchronized (queue) {
            while (batch.size() < limit) {
                PendingMessage message = queue.pollFirst();
                if (message == null) {
                    break;
                }
                batch.add(message);
            }
            if (queue.isEmpty()) {
                queues.remove(clientId, queue);
            }
        }
        return batch;
    }

    @Override
    public int size(String clientId) {
        Deque<PendingMessage> queue = queues.get(clientId);
        if (queue == null) {
            return 0;
        }
        synchronized (queue) {
            return queue.size();
        }
    }

    @Override
    public void removeAll(String clientId) {
        queues.remove(clientId);
    }

    @Override
    public long droppedCount() {
        return droppedCount.get();
    }

    /**
     * 累计队列总长度, 用于监控整体积压水位。
     */
    public int totalSize() {
        int total = 0;
        for (Deque<PendingMessage> queue : queues.values()) {
            synchronized (queue) {
                total += queue.size();
            }
        }
        return total;
    }

    private void recordDrop(String clientId, int count, String reason) {
        long total = droppedCount.addAndGet(count);
        if (total == count || total % 1000 < count) {
            log.warn("离线消息被丢弃: clientId={} 本次 {} 条, 累计 {} 条, 原因: {}。"
                            + "这是 QoS 1/2 投递保证的实际破损点",
                    clientId, count, total, reason);
        }
    }
}
