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
import online.ipuff.jmqtt.session.SessionStore;
import online.ipuff.jmqtt.session.SessionStoreService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 会话持久化策略测试。
 *
 * <p>这里不连 Redis —— 被测的是<b>策略</b>: 什么情况下才该写持久层。
 * 政策错了会很贵: 把 cleanSession=1 的会话也写进去, 在百万连接下
 * 就是平白多出百万次 Redis 写入, 而这些会话本来就没有持久化价值。
 */
class SessionPersistenceTest {

    private static final String CLIENT = "device-1";

    @Test
    @DisplayName("持久层不可用时, 任何操作都不落盘")
    void unavailableRepositoryPersistsNothing() {
        RecordingRepository repo = new RecordingRepository(false);
        SessionPersistence persistence = new SessionPersistence(repo, runtimeWithSession(false));

        persistence.subscriptionAdded(CLIENT, "a/b", 1);
        persistence.subscriptionRemoved(CLIENT, "a/b");
        persistence.saveSession(session(false));
        persistence.sessionDestroyed(CLIENT);
        persistence.acquireOwnership(CLIENT, "node-1", 7200);

        assertTrue(repo.calls.isEmpty(), "持久层不可用时不应有任何调用, 实际=" + repo.calls);
        assertNull(persistence.restore(CLIENT));
        assertFalse(persistence.available());
    }

    @Test
    @DisplayName("cleanSession=1 的会话不落盘 —— 这是写入量的大头")
    void cleanSessionIsNotPersisted() {
        RecordingRepository repo = new RecordingRepository(true);
        SessionPersistence persistence = new SessionPersistence(repo, runtimeWithSession(true));

        persistence.subscriptionAdded(CLIENT, "a/b", 1);
        persistence.subscriptionRemoved(CLIENT, "a/b");
        persistence.saveSession(session(true));

        assertTrue(repo.calls.isEmpty(),
                "cleanSession=1 的会话没有持久化价值, 不应写 Redis。实际=" + repo.calls);
    }

    @Test
    @DisplayName("cleanSession=0 的会话: 订阅变更与建立都落盘")
    void durableSessionIsPersisted() {
        RecordingRepository repo = new RecordingRepository(true);
        SessionPersistence persistence = new SessionPersistence(repo, runtimeWithSession(false));

        persistence.subscriptionAdded(CLIENT, "sensor/+/temp", 1);
        persistence.subscriptionRemoved(CLIENT, "sensor/old");
        persistence.saveSession(session(false));

        assertTrue(repo.calls.contains("saveSubscription:" + CLIENT + ":sensor/+/temp:1"),
                "订阅新增应落盘, 实际=" + repo.calls);
        assertTrue(repo.calls.contains("removeSubscription:" + CLIENT + ":sensor/old"),
                "订阅删除应落盘, 实际=" + repo.calls);
        assertTrue(repo.calls.contains("saveSession:" + CLIENT),
                "会话建立应落盘, 实际=" + repo.calls);
    }

    @Test
    @DisplayName("进程内无该会话时不落盘(避免给幽灵会话写数据)")
    void unknownClientIsNotPersisted() {
        RecordingRepository repo = new RecordingRepository(true);
        SessionPersistence persistence = new SessionPersistence(repo, new SessionStoreService());

        persistence.subscriptionAdded(CLIENT, "a/b", 1);

        assertTrue(repo.calls.isEmpty(), "无会话时不应落盘, 实际=" + repo.calls);
    }

    @Test
    @DisplayName("restore 透传持久层结果; 无记录时返回 null")
    void restoreDelegates() {
        RecordingRepository repo = new RecordingRepository(true);
        repo.loadResult = new PersistedSession(CLIENT, "node-9", false, 7200, 1L, null,
                List.of(new SubscribeStore(CLIENT, "a/b", 1)));
        SessionPersistence persistence = new SessionPersistence(repo, new SessionStoreService());

        PersistedSession restored = persistence.restore(CLIENT);
        assertTrue(restored != null && restored.hasSubscriptions(), "应带回订阅");
        assertEquals("node-9", restored.nodeId(), "应带回原归属节点");

        repo.loadResult = null;
        assertNull(persistence.restore(CLIENT));
    }

    @Test
    @DisplayName("会话销毁时清除持久层记录")
    void destroyRemovesFromRepository() {
        RecordingRepository repo = new RecordingRepository(true);
        SessionPersistence persistence = new SessionPersistence(repo, new SessionStoreService());

        persistence.sessionDestroyed(CLIENT);

        assertEquals(List.of("remove:" + CLIENT), repo.calls);
    }

    @Test
    @DisplayName("acquireOwnership 透传旧 owner")
    void acquireOwnershipReturnsPreviousOwner() {
        RecordingRepository repo = new RecordingRepository(true);
        repo.previousOwner = "node-7";
        SessionPersistence persistence = new SessionPersistence(repo, new SessionStoreService());

        assertEquals("node-7", persistence.acquireOwnership(CLIENT, "node-1", 7200));
    }

    // ------------------------------------------------------------------
    // 测试替身
    // ------------------------------------------------------------------

    static SessionStore session(boolean cleanSession) {
        return new SessionStore("node-1", CLIENT, cleanSession, cleanSession ? 0 : 7200);
    }

    static SessionStoreService runtimeWithSession(boolean cleanSession) {
        SessionStoreService runtime = new SessionStoreService();
        runtime.put(session(cleanSession));
        return runtime;
    }

    static class RecordingRepository implements ISessionRepository {

        final List<String> calls = new ArrayList<>();
        private final boolean available;

        String previousOwner;
        PersistedSession loadResult;

        RecordingRepository(boolean available) {
            this.available = available;
        }

        @Override
        public boolean available() {
            return available;
        }

        @Override
        public PersistedSession load(String clientId) {
            calls.add("load:" + clientId);
            return loadResult;
        }

        @Override
        public String acquireOwner(String clientId, String nodeId, long expireSeconds) {
            calls.add("acquireOwner:" + clientId + ":" + nodeId);
            return previousOwner;
        }

        @Override
        public void saveSession(String clientId, String nodeId, boolean cleanSession,
                                long expireSeconds, WillMessage willMessage) {
            calls.add("saveSession:" + clientId);
        }

        @Override
        public void saveSubscription(String clientId, String topicFilter, int qos) {
            calls.add("saveSubscription:" + clientId + ":" + topicFilter + ":" + qos);
        }

        @Override
        public void removeSubscription(String clientId, String topicFilter) {
            calls.add("removeSubscription:" + clientId + ":" + topicFilter);
        }

        @Override
        public void removeSubscriptions(String clientId) {
            calls.add("removeSubscriptions:" + clientId);
        }

        @Override
        public void remove(String clientId) {
            calls.add("remove:" + clientId);
        }
    }
}
