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

import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntPredicate;

/**
 * 报文标识符生成器的进程内实现。
 *
 * <p>一种常见做法是把计数器放到共享存储上(如用 Redis 的 {@code INCR} 生成全局序号),
 * 目的是让多个 broker 实例之间不重复。但报文标识符的<b>唯一性只需在单个客户端会话内保证</b>
 * (MQTT 3.1.1 §2.3.1): 它标识的是「这条连接上的一次投递」, 跨实例共享没有意义,
 * 反而给每条 QoS 1/2 投递增加一次网络往返, 并让共享存储成为投递路径上的依赖。
 *
 * <p><b>为什么不能是一个全局计数器</b>: 全局绕回 65535 次后, 某个慢客户端
 * (仍有未确认消息)会再次拿到同一个标识符 —— 在途镜像按标识符覆盖, 旧消息从
 * 恢复副本里消失, 客户端还会收到两条同标识符的不同消息。计数器按会话隔离后,
 * 绕回时跳过本会话在用的标识符(见 {@link #nextMessageId})就不可能撞。
 *
 * <p>跨节点接管后计数器不迁移: 新节点从在途镜像加载的消息带着各自的标识符,
 * 分配时的 {@code inUse} 判定会把它们跳过 —— 镜像就是在途集合的真源,
 * 比「把计数值也存一份」更准也更省。
 */
@Service
public class MessageIdService implements IMessageIdService {

    private static final int MAX_MESSAGE_ID = 65535;

    /** clientId -> 上次分配的标识符(单元素数组, 供同步块内自增) */
    private final ConcurrentHashMap<String, int[]> lastIds = new ConcurrentHashMap<>();

    @Override
    public int nextMessageId(String clientId, IntPredicate inUse) {
        int[] state = lastIds.computeIfAbsent(clientId, k -> new int[]{0});
        synchronized (state) {
            int candidate = state[0];
            for (int i = 0; i < MAX_MESSAGE_ID; i++) {
                candidate = candidate >= MAX_MESSAGE_ID ? 1 : candidate + 1;
                if (!inUse.test(candidate)) {
                    state[0] = candidate;
                    return candidate;
                }
            }
        }
        throw new IllegalStateException("会话 " + clientId + " 的报文标识符已耗尽(在途消息过多)");
    }

    @Override
    public void release(String clientId) {
        lastIds.remove(clientId);
    }
}
