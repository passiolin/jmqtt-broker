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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 离线入队缓冲: 刷盘、销毁清理、上限口径。
 */
class OfflineEnqueueBufferTest {

    @Test
    @DisplayName("flushTick 把缓冲批量落进存储 —— 缓冲只是暂存, 存储是真源")
    void flushTickPersistsBufferedMessages() {
        InMemoryPendingMessageStore store = new InMemoryPendingMessageStore();
        OfflineEnqueueBuffer buffer = new OfflineEnqueueBuffer(store, 1000, 10_000, false);
        buffer.submit("c1", PendingMessage.of("t/1", 1, new byte[]{1}, false));
        buffer.submit("c1", PendingMessage.of("t/2", 1, new byte[]{2}, false));

        buffer.flushTick();

        assertEquals(0, buffer.bufferedCount());
        assertEquals(2, store.size("c1"), "曾经这里永远是 0 —— flushTick 根本没被接线");
    }

    @Test
    @DisplayName("discard 丢弃缓冲消息 —— 会话销毁不能留下尾巴")
    void discardClearsBuffer() {
        InMemoryPendingMessageStore store = new InMemoryPendingMessageStore();
        OfflineEnqueueBuffer buffer = new OfflineEnqueueBuffer(store, 1000, 10_000, false);
        buffer.submit("c1", PendingMessage.of("t/1", 1, new byte[]{1}, false));

        buffer.discard("c1");

        assertEquals(0, buffer.bufferedCount());
        buffer.flushTick();
        assertEquals(0, store.size("c1"), "被丢弃的不能又冒出来");
    }

    @Test
    @DisplayName("flushClient 把缓冲刷进存储 —— 接管时消息要留给新归属方")
    void flushClientMovesBufferToStore() {
        InMemoryPendingMessageStore store = new InMemoryPendingMessageStore();
        OfflineEnqueueBuffer buffer = new OfflineEnqueueBuffer(store, 1000, 10_000, false);
        buffer.submit("c1", PendingMessage.of("t/1", 1, new byte[]{1}, false));
        buffer.submit("c1", PendingMessage.of("t/2", 1, new byte[]{2}, false));

        buffer.flushClient("c1");

        assertEquals(0, buffer.bufferedCount());
        assertEquals(2, store.size("c1"));
    }

    @Test
    @DisplayName("每客户端上限: 超限丢最旧, 计数同步")
    void perClientCapDropsOldest() {
        InMemoryPendingMessageStore store = new InMemoryPendingMessageStore();
        OfflineEnqueueBuffer buffer = new OfflineEnqueueBuffer(store, 2, 10_000, false);
        buffer.submit("c1", PendingMessage.of("t/1", 1, new byte[]{1}, false));
        buffer.submit("c1", PendingMessage.of("t/2", 1, new byte[]{2}, false));
        buffer.submit("c1", PendingMessage.of("t/3", 1, new byte[]{3}, false));

        assertEquals(2, buffer.bufferedCount(), "丢最旧时计数要跟着减");
        assertEquals(1, buffer.droppedBuffered());
    }

    @Test
    @DisplayName("全局上限: 超限丢最新, 不留空队列残渣")
    void globalCapDropsNewest() {
        InMemoryPendingMessageStore store = new InMemoryPendingMessageStore();
        OfflineEnqueueBuffer buffer = new OfflineEnqueueBuffer(store, 1000, 2, false);
        buffer.submit("c1", PendingMessage.of("t/1", 1, new byte[]{1}, false));
        buffer.submit("c2", PendingMessage.of("t/2", 1, new byte[]{2}, false));
        buffer.submit("c3", PendingMessage.of("t/3", 1, new byte[]{3}, false));

        assertEquals(2, buffer.bufferedCount());
    }

    @Test
    @DisplayName("requeueFront 整段回填保序 —— 逐条回填会把顺序倒过来")
    void requeueFrontPreservesOrder() {
        InMemoryPendingMessageStore store = new InMemoryPendingMessageStore();
        OfflineEnqueueBuffer buffer = new OfflineEnqueueBuffer(store, 1000, 10_000, true);
        List<PendingMessage> rest = List.of(
                PendingMessage.of("t/6", 1, new byte[]{6}, false),
                PendingMessage.of("t/7", 1, new byte[]{7}, false));

        buffer.requeueFront("c1", rest);
        List<PendingMessage> drained = buffer.drainClient("c1");

        assertEquals(2, drained.size());
        assertEquals("t/6", drained.get(0).topic(), "整段回填后仍按原顺序");
        assertEquals("t/7", drained.get(1).topic());
    }
}
