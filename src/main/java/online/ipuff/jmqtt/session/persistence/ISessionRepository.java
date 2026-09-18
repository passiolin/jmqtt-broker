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

import online.ipuff.jmqtt.message.WillMessage;

/**
 * 会话持久层。
 *
 * <p><b>调用时机是硬约束</b>: 只允许出现在连接 / 重连 / 接管 / 订阅变更这类低频路径上。
 * 一旦有人把它接进消息投递路径, 就会退化成「每次投递一次 Redis 往返」——
 * 那正是本架构要避免的性能陷阱。判断标准很简单:
 * <b>这个方法会被每条消息调用吗? 会, 就不能放这里。</b>
 *
 * <p>所有方法在持久层不可用时都必须安静降级, 不抛异常给调用方 ——
 * 协调层已经按「拿不到会话就当新会话」处理了。
 */
public interface ISessionRepository {

    /**
     * 持久层当前是否可用。不可用时调用方应按「无会话」处理。
     */
    boolean available();

    /**
     * 加载会话(含订阅)。
     *
     * @return 会话; 不存在或不可用时返回 {@code null}
     */
    PersistedSession load(String clientId);

    /**
     * 原子地取得会话归属: 读取旧 owner 并写入新 owner。
     *
     * <p><b>必须原子。</b>「先读再写」分两步的话, 两个节点同时接管会双双读到
     * 空 owner, 于是都认为自己是归属方 —— 结果是两份路由、重复投递。
     *
     * @return 旧的 owner 节点 id; 无旧 owner(新会话)返回 {@code null}; 不可用时也返回 null
     */
    String acquireOwner(String clientId, String nodeId, long expireSeconds);

    /**
     * 写入/覆盖会话属性(不含订阅)。
     */
    void saveSession(String clientId, String nodeId, boolean cleanSession,
                     long expireSeconds, WillMessage willMessage);

    /**
     * 保存一条订阅。
     */
    void saveSubscription(String clientId, String topicFilter, int qos);

    /**
     * 删除一条订阅。
     */
    void removeSubscription(String clientId, String topicFilter);

    /**
     * 删除该客户端的全部订阅。
     */
    void removeSubscriptions(String clientId);

    /**
     * 删除会话及其全部附属数据。
     */
    void remove(String clientId);
}
