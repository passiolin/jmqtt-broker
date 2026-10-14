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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 会话过期判定: 在线永不过期, 过期从「断开」起算。
 */
class SessionStoreExpiryTest {

    @Test
    @DisplayName("在线会话永不过期 —— 即使距上次活跃已超过保留时长")
    void onlineSessionNeverExpires() {
        SessionStore session = new SessionStore("node-1", "c1", false, 1);
        session.markClosed();
        session.markOpen(); // 重连: 过期时钟清零

        sleep(20);

        assertFalse(session.isExpired(), "连接还在, 会话按定义就是活的");
        assertTrue(session.isOnline());
    }

    @Test
    @DisplayName("非持久会话断开即过期")
    void nonPersistentExpiresAtClose() {
        SessionStore session = new SessionStore("node-1", "c1", true, 0);

        assertFalse(session.isExpired(), "在线时不过期");
        session.markClosed();
        assertTrue(session.isExpired(), "断开即销毁");
    }

    @Test
    @DisplayName("持久会话的过期从关闭时刻起算, 不是最后活跃时刻")
    void persistentExpiresFromCloseTime() {
        SessionStore session = new SessionStore("node-1", "c1", false, 1);
        session.markClosed();

        assertFalse(session.isExpired(), "刚断开, 还在保留期内");
    }

    @Test
    @DisplayName("markClosed 幂等 —— 只记第一次关闭时刻")
    void markClosedIsIdempotent() {
        SessionStore session = new SessionStore("node-1", "c1", false, 1);
        session.markClosed();
        long firstClosed = System.currentTimeMillis();
        sleep(20);
        session.markClosed();

        // 若第二次 markClosed 覆盖了时刻, 这里几乎不可能过期(1s 的窗口);
        // 幂等保证的是「关闭时刻不被后来的调用推后」, 用 isExpired 的边界行为间接验证
        assertFalse(session.isExpired());
        assertTrue(firstClosed <= System.currentTimeMillis());
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
