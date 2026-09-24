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
import org.springframework.beans.factory.annotation.Autowired;
import online.ipuff.jmqtt.config.BrokerProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 离线入队的异步缓冲: 投递线程只做内存追加, 后台单线程批量刷入存储。
 *
 * <h2>为什么需要它</h2>
 * 离线入队发生在<b>下行投递线程</b>上(设备不在线的回显/指令都要缓存)。
 * 直接同步写 Redis 时, 每条消息一次 EVAL 往返; 高峰期 Redis 被 30 万会话的
 * 在途镜像刷盘挤占, 单条延迟可达秒级 —— 16 个下行 worker 会被队头阻塞,
 * 整条下行投递归零(压测实测: 300k 连接 + 5k TPS 时投递停滞, lag 46 万)。
 *
 * <h2>语义与边界</h2>
 * <ul>
 *   <li>每客户端 FIFO 有界(同 max-offline-queue-len), 超限丢最旧并计数</li>
 *   <li>全局缓冲总量有界({@code maxBuffered}), 超限丢最新并计数 —— 内存不失控</li>
 *   <li>后台线程批量刷入(每客户端一次 EVAL 携带全部积压消息), Redis IO 次数
 *       从「每消息一次」降为「每客户端每轮一次」</li>
 *   <li>崩溃丢失窗口 = 未刷盘的缓冲尾部; 对离线缓存语义可接受(在线投递零影响)</li>
 * </ul>
 */
@Component
public class OfflineEnqueueBuffer {

    private static final Logger log = LoggerFactory.getLogger(OfflineEnqueueBuffer.class);

    private final IPendingMessageStore store;
    private final int maxLen;
    private final int maxBuffered;
    private final boolean synchronous;

    /** 全局缓冲上限(条): 超限丢最新, 内存不失控。 */
    public static final int DEFAULT_MAX_BUFFERED = 100_000;

    private final Object lock = new Object();
    /** clientId -> FIFO(保序) */
    private final Map<String, ArrayDeque<PendingMessage>> buffered = new HashMap<>();
    private final AtomicLong bufferedCount = new AtomicLong();
    private final AtomicLong droppedBuffered = new AtomicLong();

    @Autowired
    public OfflineEnqueueBuffer(IPendingMessageStore store, BrokerProperties properties) {
        this(store, properties.maxOfflineQueueLen(), DEFAULT_MAX_BUFFERED, false);
    }

    /** 测试与降级用: 同步直写模式。 */
    public OfflineEnqueueBuffer(IPendingMessageStore store, int maxLen, int maxBuffered, boolean synchronous) {
        this.store = store;
        this.maxLen = maxLen;
        this.maxBuffered = maxBuffered;
        this.synchronous = synchronous;
    }

    /** 投递线程调用: 永不阻塞。synchronous 模式直接写存储(测试/降级用)。 */
    public void submit(String clientId, PendingMessage message) {
        if (synchronous) {
            store.enqueue(clientId, message, maxLen);
            return;
        }
        boolean overflow = false;
        synchronized (lock) {
            ArrayDeque<PendingMessage> dq = buffered.computeIfAbsent(clientId, k -> new ArrayDeque<>());
            // 每客户端有界: 同存储口径, 超限丢最旧
            while (dq.size() >= maxLen) {
                dq.pollFirst();
                droppedBuffered.incrementAndGet();
            }
            if (bufferedCount.get() < maxBuffered) {
                dq.offerLast(message);
                bufferedCount.incrementAndGet();
            } else {
                overflow = true;
            }
        }
        if (overflow) {
            log.warn("离线入队缓冲超限({}), 丢弃最新消息: clientId={}", maxBuffered, clientId);
        }
    }

    /** 把滞留消息放回该客户端缓冲的头部(保序), 等待窗口/下次投递。 */
    public void requeueFront(String clientId, List<PendingMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }
        synchronized (lock) {
            ArrayDeque<PendingMessage> dq = buffered.computeIfAbsent(clientId, k -> new ArrayDeque<>());
            for (int i = messages.size() - 1; i >= 0; i--) {
                dq.offerFirst(messages.get(i));
            }
            bufferedCount.addAndGet(messages.size());
        }
    }

    /** 设备重连投递离线积压前, 先同步取走该客户端仍滞留在缓冲里的消息(保序入库)。 */
    public List<PendingMessage> drainClient(String clientId) {
        synchronized (lock) {
            ArrayDeque<PendingMessage> dq = buffered.remove(clientId);
            if (dq == null || dq.isEmpty()) {
                return List.of();
            }
            bufferedCount.addAndGet(-dq.size());
            return new ArrayList<>(dq);
        }
    }

    /** 后台刷盘线程: 每 tick 把缓冲整体批量落存储。 */
    public void flushTick() {
        Map<String, List<PendingMessage>> batch;
        synchronized (lock) {
            if (buffered.isEmpty()) {
                return;
            }
            batch = new HashMap<>(buffered.size() * 2);
            for (Map.Entry<String, ArrayDeque<PendingMessage>> e : buffered.entrySet()) {
                if (!e.getValue().isEmpty()) {
                    batch.put(e.getKey(), new ArrayList<>(e.getValue()));
                }
            }
            buffered.clear();
            bufferedCount.set(0);
        }
        long dropped = 0;
        for (Map.Entry<String, List<PendingMessage>> e : batch.entrySet()) {
            dropped += store.enqueueBatch(e.getKey(), e.getValue(), maxLen);
        }
        if (dropped > 0) {
            log.warn("离线缓冲批量刷盘丢弃 {} 条(超限丢最旧)", dropped);
        }
    }

    public long bufferedCount() {
        return bufferedCount.get();
    }

    public long droppedBuffered() {
        return droppedBuffered.get();
    }
}
