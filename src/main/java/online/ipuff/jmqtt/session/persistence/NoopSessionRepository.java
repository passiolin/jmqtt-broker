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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 会话持久层的空实现(`jmqtt.broker.redis.enabled=false` 时装配)。
 *
 * <p>会话只存在于进程内。节点宕机后其客户端的会话不可恢复 ——
 * 客户端重连会拿到 {@code sessionPresent=false} 并重新订阅。
 *
 * <p>{@link #available()} 返回 false, 协调层因此会一致地按「无持久层」处理,
 * 不必在业务代码里到处判断开关。
 */
@Component
@ConditionalOnProperty(prefix = "jmqtt.broker.redis", name = "enabled",
        havingValue = "false", matchIfMissing = true)
public class NoopSessionRepository implements ISessionRepository {

    private static final Logger log = LoggerFactory.getLogger(NoopSessionRepository.class);

    public NoopSessionRepository() {
        log.info("会话持久化未启用, 会话仅存在于进程内(节点重启后不可恢复)");
    }

    @Override
    public boolean available() {
        return false;
    }

    @Override
    public PersistedSession load(String clientId) {
        return null;
    }

    @Override
    public String acquireOwner(String clientId, String nodeId, long expireSeconds) {
        return null;
    }

    @Override
    public void saveSession(String clientId, String nodeId, boolean cleanSession,
                            long expireSeconds, WillMessage willMessage) {
        // 不持久化
    }

    @Override
    public void saveSubscription(String clientId, String topicFilter, int qos) {
        // 不持久化
    }

    @Override
    public void removeSubscription(String clientId, String topicFilter) {
        // 不持久化
    }

    @Override
    public void removeSubscriptions(String clientId) {
        // 不持久化
    }

    @Override
    public void remove(String clientId) {
        // 不持久化
    }
}
