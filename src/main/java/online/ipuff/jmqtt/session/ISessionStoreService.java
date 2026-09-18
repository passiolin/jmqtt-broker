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
package online.ipuff.jmqtt.session;

import java.util.Collection;
import java.util.List;

/**
 * 会话存储服务。
 *
 * {@code put(clientId, sessionStore, expire)} —— 过期时间与 clientId 同时传入,
 * 但过期时间本来就在 {@link SessionStore} 里, 属于冗余参数, 这里去掉。
 *
 * <p>实现可替换: P0 为进程内实现; 后续接入 Redis 时保持本接口不变,
 * 由外部存储承担跨节点恢复。
 */
public interface ISessionStoreService {

    /**
     * 存储或覆盖会话。
     */
    void put(SessionStore session);

    /**
     * 获取会话, 不存在或已过期返回 {@code null}。
     */
    SessionStore get(String clientId);

    /**
     * 会话是否存在且未过期。
     */
    boolean containsKey(String clientId);

    /**
     * 删除会话。
     */
    void remove(String clientId);

    /**
     * 当前会话总数。
     */
    int size();

    /**
     * 全部会话快照。
     */
    Collection<SessionStore> all();

    /**
     * 取出并移除所有已过期的会话, 返回其 clientId。
     *
     * <p>刻意只做「取出并移除」, 不在这里做任何级联清理 ——
     * 会话过期还要连带清掉订阅、在途消息、持久层数据, 那是协调层的职责
     * (见 {@code SessionExpiryReaper})。放在这里会形成循环依赖。
     */
    List<String> drainExpired();
}
