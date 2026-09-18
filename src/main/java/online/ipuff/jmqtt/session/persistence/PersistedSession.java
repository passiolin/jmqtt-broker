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
package online.ipuff.jmqtt.session.persistence;

import online.ipuff.jmqtt.message.SubscribeStore;
import online.ipuff.jmqtt.message.WillMessage;

import java.util.List;

/**
 * 从持久层恢复出来的会话。
 *
 * <p><b>为什么必须带订阅关系。</b>MQTT 规范里 session state 包括「客户端的全部订阅」。
 * 如果持久层只存会话属性不存订阅, 恢复后 broker 手里没有任何路由,
 * 却只能回 {@code sessionPresent=false} 让客户端重新订阅 —— 否则客户端会以为
 * 订阅仍然有效, 而消息永远到不了, 属于最难排查的静默故障。
 *
 * <p>所以 {@link #subscriptions} 允许为空, 但**不允许缺失**:
 * 一个真实的非 clean 会话如果没有订阅, 那也必须是「确实没有订阅」,
 * 而不是「我们没存」。
 *
 * @param clientId      客户端标识
 * @param nodeId        归属节点(owner)。重连到其他节点时, 据此判断是否需要通知旧节点释放
 * @param cleanSession  连接时的 cleanSession 标志
 * @param expireSeconds 会话过期时长(秒)
 * @param createdAt     创建时间(epoch millis)
 * @param willMessage   遗嘱消息, 可为 null
 * @param subscriptions 订阅关系, 不可为 null(可为空列表)
 */
public record PersistedSession(
        String clientId,
        String nodeId,
        boolean cleanSession,
        long expireSeconds,
        long createdAt,
        WillMessage willMessage,
        List<SubscribeStore> subscriptions
) {

    /**
     * 是否持有订阅关系。
     */
    public boolean hasSubscriptions() {
        return subscriptions != null && !subscriptions.isEmpty();
    }
}
