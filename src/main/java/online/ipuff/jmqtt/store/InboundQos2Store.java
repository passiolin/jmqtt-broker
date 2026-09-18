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

import online.ipuff.jmqtt.config.BrokerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 接收方向 QoS 2 去重状态的进程内实现。
 *
 * <h2>为什么有一个每客户端的上限</h2>
 * 这个集合的条目只会因为 PUBREL 或会话销毁而减少。也就是说, <b>一个不发 PUBREL 的客户端
 * 可以让它的集合无限增长</b> —— 每次发一个递增的报文标识符即可, 不需要任何越权行为。
 * 这是典型的内存放大攻击: 攻击者用很小的代价让服务端持有大量状态。
 *
 * <p>上限取「服务端在 CONNACK 里声明的 Receive Maximum」的两倍: 超过它的客户端
 * 本来就已经违反了协议 —— Receive Maximum 的含义正是「你同时最多能有这么多条未确认的
 * QoS 1/2」。所以这里不是武断地设一个阈值, 而是<b>拿服务端自己声明的承诺去校验客户端</b>。
 *
 * <p>保留两倍余量而不是一倍, 是因为「声明值」与「客户端实际遵守值」之间总有偏差,
 * 而这一层的目的是防滥用、不是卡死正常客户端。
 */
@Component
public class InboundQos2Store implements IInboundQos2Store {

    private static final Logger log = LoggerFactory.getLogger(InboundQos2Store.class);

    /** 每客户端未完成标识符的上限下限, 避免把 max-inflight 配成 1 时过于苛刻 */
    private static final int MIN_PENDING = 16;

    private final ConcurrentHashMap<String, Set<Integer>> pending = new ConcurrentHashMap<>();
    private final int maxPendingPerClient;

    public InboundQos2Store(BrokerProperties properties) {
        this.maxPendingPerClient = Math.max(MIN_PENDING, properties.maxInflight() * 2);
    }

    @Override
    public Result mark(String clientId, int packetId) {
        if (clientId == null) {
            // 理论上不该出现(CONNECT 之前不会有 PUBLISH), 但真出现时按首次处理 ——
            // 宁可重复投递一条, 也不要因为一个空 key 把正常消息丢掉
            return Result.FIRST;
        }
        Set<Integer> ids = pending.computeIfAbsent(clientId, key -> ConcurrentHashMap.newKeySet());
        // 先判断再插入: 顺序反过来的话重复报文会先把计数推上去, 长连接上的正常重发
        // 会逐渐逼近上限
        if (ids.contains(packetId)) {
            return Result.DUPLICATE;
        }
        if (ids.size() >= maxPendingPerClient) {
            log.warn("客户端 {} 的未完成 QoS 2 标识符已达上限 {} —— 它在 CONNECT 里声明的 "
                            + "Receive Maximum 是 {}。这属于协议违规, 断开连接",
                    clientId, ids.size(), maxPendingPerClient / 2);
            return Result.OVERFLOW;
        }
        ids.add(packetId);
        return Result.FIRST;
    }

    @Override
    public void release(String clientId, int packetId) {
        if (clientId == null) {
            return;
        }
        Set<Integer> ids = pending.get(clientId);
        if (ids == null) {
            return;
        }
        ids.remove(packetId);
        // 空集合立即摘掉, 否则「曾经用过 QoS 2 的客户端」会一直留一个空 Set
        if (ids.isEmpty()) {
            pending.remove(clientId, ids);
        }
    }

    @Override
    public void clearClient(String clientId) {
        if (clientId != null) {
            pending.remove(clientId);
        }
    }

    @Override
    public int clientCount() {
        return pending.size();
    }
}
