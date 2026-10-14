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

import java.util.HashSet;
import java.util.Set;
import java.util.function.IntPredicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 报文标识符分配: 每会话独立计数 + 跳过在用值。
 */
class MessageIdServiceTest {

    private final MessageIdService service = new MessageIdService();

    @Test
    @DisplayName("计数器按会话隔离 —— 不同会话各自从 1 开始")
    void countersArePerSession() {
        assertEquals(1, service.nextMessageId("a", id -> false));
        assertEquals(2, service.nextMessageId("a", id -> false));
        assertEquals(1, service.nextMessageId("b", id -> false), "另一会话不受影响");
    }

    @Test
    @DisplayName("跳过在用标识符 —— 全局计数器绕回撞在途消息的场景")
    void skipsInUseIds() {
        Set<Integer> inUse = new HashSet<>(Set.of(1, 2, 3));
        IntPredicate used = inUse::contains;

        assertEquals(4, service.nextMessageId("a", used));

        // 模拟「计数器绕回到 1 附近」: 释放一批, 只留 2 在用
        inUse.retainAll(Set.of(2));
        // 逼到绕回: 分配 65535 - 4 次把计数器推到顶
        for (int i = 0; i < 65535 - 4; i++) {
            service.nextMessageId("a", id -> false);
        }
        int wrapped = service.nextMessageId("a", used);
        assertNotEquals(2, wrapped, "绕回后不能撞上仍在途的标识符");
        assertTrue(wrapped >= 1 && wrapped <= 65535);
    }

    @Test
    @DisplayName("取值范围 1..65535, 绕回后重复使用")
    void wrapsWithinRange() {
        int first = service.nextMessageId("a", id -> false);
        assertEquals(1, first);
        for (int i = 0; i < 65533; i++) {
            service.nextMessageId("a", id -> false);
        }
        assertEquals(65535, service.nextMessageId("a", id -> false));
        assertEquals(1, service.nextMessageId("a", id -> false), "65535 之后绕回 1");
    }

    @Test
    @DisplayName("在途占满整个取值空间时抛出异常, 而不是死循环")
    void throwsWhenExhausted() {
        assertThrows(IllegalStateException.class,
                () -> service.nextMessageId("a", id -> true));
    }

    @Test
    @DisplayName("release 之后计数器重新开始")
    void releaseResetsCounter() {
        service.nextMessageId("a", id -> false);
        service.nextMessageId("a", id -> false);
        service.release("a");
        assertEquals(1, service.nextMessageId("a", id -> false));
    }
}
