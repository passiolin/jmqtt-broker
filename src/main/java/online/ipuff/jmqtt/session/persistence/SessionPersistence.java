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

import online.ipuff.jmqtt.session.ISessionStoreService;
import online.ipuff.jmqtt.session.SessionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 会话持久化的策略层。
 *
 * <p>把「什么时候该落盘」集中在一处, 协议处理器只需要表达意图
 * （订阅加了、订阅删了、会话建立了), 不必各自去判断 cleanSession 与开关状态。
 *
 * <h2>核心策略: 只有 cleanSession=0 的会话才落盘</h2>
 * MQTT 规范里 {@code cleanSession=1} 明确要求服务端丢弃既有会话,
 * 这类会话没有持久化的意义, 落盘只会白白增加 Redis 写入。
 * 绝大多数 IoT 设备用的正是 cleanSession=1 —— 所以这个判断
 * 直接把 Redis 的写入量砍掉一大截。
 */
@Component
public class SessionPersistence {

    private static final Logger log = LoggerFactory.getLogger(SessionPersistence.class);

    private final ISessionRepository repository;
    private final ISessionStoreService runtimeSessions;

    public SessionPersistence(ISessionRepository repository, ISessionStoreService runtimeSessions) {
        this.repository = repository;
        this.runtimeSessions = runtimeSessions;
    }

    /**
     * 持久层是否可用。
     */
    public boolean available() {
        return repository.available();
    }

    // ------------------------------------------------------------------
    // 连接路径
    // ------------------------------------------------------------------

    /**
     * 尝试加载已持久化的会话。
     *
     * <p>返回非 null 意味着<b>会话及其订阅都被完整恢复</b>, 调用方据此回
     * {@code sessionPresent=true}。两者必须一致: 只有会话没有订阅时回 true,
     * 客户端会以为订阅仍有效而消息永远到不了。
     *
     * <p><b>刻意不做 {@code available()} 前置判断。</b>恢复属于「不能省」的一步:
     * 跳过它, 客户端会收到 {@code sessionPresent=false}、在途镜像也不会被加载,
     * 代价是真实的消息丢失; 而直接尝试一次, 最坏情况只是一次命令超时。
     * 两个方向的代价不对称, 判断就不该对称 —— 健康标记只留给可以安全跳过的写路径。
     */
    public PersistedSession restore(String clientId) {
        PersistedSession persisted = repository.load(clientId);
        if (persisted == null) {
            log.debug("持久层无会话: clientId={}", clientId);
            return null;
        }
        if (persisted.hasSubscriptions()) {
            log.debug("已从持久层恢复会话: clientId={} 订阅数={} 原归属={}",
                    clientId, persisted.subscriptions().size(), persisted.nodeId());
        } else {
            log.debug("已从持久层恢复会话(无订阅): clientId={}", clientId);
        }
        return persisted;
    }

    /**
     * 原子取得会话归属。
     *
     * @return 旧的 owner; 新会话返回 {@code null}
     */
    public String acquireOwnership(String clientId, String nodeId, long expireSeconds) {
        if (!repository.available()) {
            return null;
        }
        return repository.acquireOwner(clientId, nodeId, expireSeconds);
    }

    /**
     * 会话建立后写入持久层。
     */
    public void saveSession(SessionStore session) {
        if (session == null || !session.isPersistent() || !repository.available()) {
            return;
        }
        repository.saveSession(session.getClientId(), session.getBrokerId(), false,
                session.getExpireSeconds(), session.getWillMessage());
    }

    // ------------------------------------------------------------------
    // 订阅变更
    // ------------------------------------------------------------------

    public void subscriptionAdded(String clientId, String topicFilter, int qos) {
        if (!shouldPersist(clientId)) {
            return;
        }
        repository.saveSubscription(clientId, topicFilter, qos);
    }

    public void subscriptionRemoved(String clientId, String topicFilter) {
        if (!shouldPersist(clientId)) {
            return;
        }
        repository.removeSubscription(clientId, topicFilter);
    }

    public void subscriptionsCleared(String clientId) {
        if (!repository.available()) {
            return;
        }
        repository.removeSubscriptions(clientId);
    }

    // ------------------------------------------------------------------
    // 会话销毁
    // ------------------------------------------------------------------

    /**
     * 会话销毁时清除持久化数据(客户端以 cleanSession=1 重连, 或断开时声明 clean)。
     */
    public void sessionDestroyed(String clientId) {
        if (!repository.available()) {
            return;
        }
        repository.remove(clientId);
    }

    private boolean shouldPersist(String clientId) {
        if (!repository.available() || clientId == null) {
            return false;
        }
        SessionStore session = runtimeSessions.get(clientId);
        // 判据是「是否保留」而不是「是否 cleanSession」—— v5 下两者不等价, 见 SessionStore#isPersistent
        return session != null && session.isPersistent();
    }
}
