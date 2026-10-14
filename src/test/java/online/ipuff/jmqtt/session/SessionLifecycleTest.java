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

import online.ipuff.jmqtt.TestBrokerProperties;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.message.SubscribeStore;
import online.ipuff.jmqtt.session.persistence.ISessionRepository;
import online.ipuff.jmqtt.session.persistence.PersistedSession;
import online.ipuff.jmqtt.session.persistence.SessionPersistence;
import online.ipuff.jmqtt.store.DupPubRelMessageStoreService;
import online.ipuff.jmqtt.store.DupPublishMessageStoreService;
import online.ipuff.jmqtt.store.InMemoryPendingMessageStore;
import online.ipuff.jmqtt.store.InboundQos2Store;
import online.ipuff.jmqtt.store.MessageIdService;
import online.ipuff.jmqtt.store.OfflineEnqueueBuffer;
import online.ipuff.jmqtt.store.PendingMessage;
import online.ipuff.jmqtt.subscribe.SubscribeStoreService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 会话销毁的级联与归属校验。
 */
class SessionLifecycleTest {

    private static final String CLIENT = "device-1";

    private SessionStoreService sessions;
    private SubscribeStoreService subscriptions;
    private InMemoryPendingMessageStore pending;
    private OfflineEnqueueBuffer buffer;
    private RecordingRepository repository;
    private SessionLifecycle lifecycle;

    @BeforeEach
    void setUp() {
        BrokerProperties props = TestBrokerProperties.create();
        sessions = new SessionStoreService();
        subscriptions = new SubscribeStoreService();
        pending = new InMemoryPendingMessageStore();
        buffer = new OfflineEnqueueBuffer(pending, 1000, 10_000, false);
        repository = new RecordingRepository();
        lifecycle = new SessionLifecycle(sessions, subscriptions,
                new DupPublishMessageStoreService(), new DupPubRelMessageStoreService(),
                new InboundQos2Store(props), pending, buffer, new MessageIdService(),
                new SessionPersistence(repository, sessions));
    }

    @Test
    @DisplayName("归属本节点时彻底销毁: 会话/订阅/离线积压(含缓冲)全清")
    void destroyClearsEverythingWhenOwned() {
        givenClientWithState();
        repository.owned = true;
        buffer.submit(CLIENT, PendingMessage.of("t/1", 1, new byte[]{1}, false));

        lifecycle.destroy(CLIENT);

        assertNull(sessions.get(CLIENT));
        assertEquals(0, subscriptions.subscriptionCount());
        assertEquals(0, pending.size(CLIENT), "存储里的离线积压要清掉");
        assertEquals(0, buffer.bufferedCount(), "缓冲尾巴也要清掉");
    }

    @Test
    @DisplayName("归属已迁走时只清本地副本 —— 队列留给新归属方")
    void destroyKeepsSharedDataWhenNotOwner() {
        givenClientWithState();
        repository.owned = false;
        buffer.submit(CLIENT, PendingMessage.of("t/1", 1, new byte[]{1}, false));

        lifecycle.destroy(CLIENT);

        assertNull(sessions.get(CLIENT), "本地会话副本清掉");
        assertEquals(0, subscriptions.subscriptionCount());
        // 缓冲被刷进存储而不是丢弃 —— 会话归别人了, 这些消息是欠新归属方投递的。
        // 原有 1 条 + 缓冲刷入 1 条, 一条都不能少
        assertEquals(2, pending.size(CLIENT), "离线积压不能删, 归属已迁走");
        assertEquals(0, buffer.bufferedCount());
    }

    @Test
    @DisplayName("cleanStart 的无条件销毁: 即使归属是别人也清干净")
    void forceDestroyIgnoresOwnership() {
        givenClientWithState();
        repository.owned = false;

        lifecycle.destroy(CLIENT, true);

        assertNull(sessions.get(CLIENT));
        assertEquals(0, pending.size(CLIENT), "cleanStart 是客户端明确要求丢弃");
    }

    @Test
    @DisplayName("持久会话的异常断连: 保留会话并记录关闭时刻")
    void persistentSessionSurvivesAbnormalClose() {
        sessions.put(new SessionStore("node-1", CLIENT, false, 7200));

        lifecycle.connectionClosed(CLIENT);

        SessionStore session = sessions.get(CLIENT);
        assertNotNull(session, "持久会话要保留");
        assertTrue(session.isExpired() == false);
        assertTrue(repository.calls.contains("saveSession:" + CLIENT), "要落库");
    }

    @Test
    @DisplayName("非持久会话的异常断连: 即刻销毁(不再泄漏)")
    void nonPersistentSessionDestroyedOnAbnormalClose() {
        sessions.put(new SessionStore("node-1", CLIENT, true, 0));
        subscriptions.put(new SubscribeStore(CLIENT, "t/1", 1));

        lifecycle.connectionClosed(CLIENT);

        assertNull(sessions.get(CLIENT), "clean session 随连接结束, 之前这里会永久泄漏");
        assertEquals(0, subscriptions.subscriptionCount(), "订阅不能残留在主题树里");
    }

    private void givenClientWithState() {
        sessions.put(new SessionStore("node-1", CLIENT, false, 7200));
        subscriptions.put(new SubscribeStore(CLIENT, "t/1", 1));
        pending.enqueue(CLIENT, PendingMessage.of("t/0", 1, new byte[]{0}, false), 100);
    }

    /** 记录调用的最小仓储替身 */
    private static class RecordingRepository implements ISessionRepository {

        final java.util.List<String> calls = new java.util.ArrayList<>();
        boolean owned = true;

        @Override
        public boolean available() {
            return true;
        }

        @Override
        public PersistedSession load(String clientId) {
            return null;
        }

        @Override
        public String acquireOwner(String clientId, String nodeId, long expireSeconds) {
            calls.add("acquireOwner:" + clientId);
            return null;
        }

        @Override
        public void saveSession(String clientId, String nodeId, boolean cleanSession,
                                long expireSeconds, online.ipuff.jmqtt.message.WillMessage willMessage) {
            calls.add("saveSession:" + clientId);
        }

        @Override
        public void saveSubscription(String clientId, String topicFilter, int qos) {
            calls.add("saveSubscription:" + clientId);
        }

        @Override
        public void removeSubscription(String clientId, String topicFilter) {
            calls.add("removeSubscription:" + clientId);
        }

        @Override
        public void removeSubscriptions(String clientId) {
            calls.add("removeSubscriptions:" + clientId);
        }

        @Override
        public void remove(String clientId) {
            calls.add("remove:" + clientId);
        }

        @Override
        public boolean removeIfOwner(String clientId) {
            calls.add("removeIfOwner:" + clientId);
            return owned;
        }
    }
}
