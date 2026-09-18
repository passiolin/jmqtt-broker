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

import online.ipuff.jmqtt.TestBrokerProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * 接收方向 QoS 2 去重状态。
 *
 * <h2>两个方向都要测</h2>
 * 这个组件的价值在于「不重复投递」, 但它有一个<b>相反的</b>失效方式同样严重:
 * 只记不释放会让标识符永久卡住, 于是后续消息被静默丢弃。
 * 重复投递看得见(业务侧会发现指令执行了两次), 而静默丢弃看不见 ——
 * 所以「释放后能重用」这条用例比「能去重」更重要。
 */
class InboundQos2StoreTest {

    private static InboundQos2Store store() {
        // maxInflight=4 -> 每客户端上限 max(16, 8) = 16
        return new InboundQos2Store(TestBrokerProperties.create(4, 1000));
    }

    @Test
    @DisplayName("首次收到为 FIRST, 同一标识符再来为 DUPLICATE")
    void firstThenDuplicate() {
        InboundQos2Store store = store();

        assertEquals(IInboundQos2Store.Result.FIRST, store.mark("c1", 7));
        assertEquals(IInboundQos2Store.Result.DUPLICATE, store.mark("c1", 7));
        assertEquals(IInboundQos2Store.Result.DUPLICATE, store.mark("c1", 7),
                "连续重发都应判为重复, 不能因为次数多就变回首次");
        assertEquals(1, store.clientCount(), "重复标记不应产生新的客户端条目");
    }

    @Test
    @DisplayName("★ PUBREL 释放后, 同一标识符可以再用于新一轮流程")
    void releaseAllowsReuse() {
        InboundQos2Store store = store();

        assertEquals(IInboundQos2Store.Result.FIRST, store.mark("c1", 7));
        store.release("c1", 7);
        // 这是最关键的一条: 若释放没生效, 客户端后续用 id=7 发的消息会被静默丢弃,
        // 而协议层看不出任何异常
        assertEquals(IInboundQos2Store.Result.FIRST, store.mark("c1", 7),
                "PUBREL 之后标识符必须回到可用状态");
    }

    @Test
    @DisplayName("全部释放后客户端条目被摘掉(不留下空集合)")
    void entryRemovedWhenEmpty() {
        InboundQos2Store store = store();

        store.mark("c1", 1);
        store.mark("c1", 2);
        assertEquals(1, store.clientCount());

        store.release("c1", 1);
        assertEquals(1, store.clientCount(), "还有未完成的标识符, 条目应当保留");
        store.release("c1", 2);
        assertEquals(0, store.clientCount(),
                "条目必须被摘掉, 否则「曾经用过 QoS 2 的客户端」会永久留一个空集合");
    }

    @Test
    @DisplayName("不同客户端之间互不干扰")
    void clientsAreIndependent() {
        InboundQos2Store store = store();

        assertEquals(IInboundQos2Store.Result.FIRST, store.mark("c1", 7));
        assertEquals(IInboundQos2Store.Result.FIRST, store.mark("c2", 7),
                "报文标识符的唯一性只在单个客户端会话内成立, 跨客户端必须互不影响");
        assertEquals(2, store.clientCount());

        store.release("c1", 7);
        assertEquals(IInboundQos2Store.Result.DUPLICATE, store.mark("c2", 7),
                "释放 c1 的标识符不应把 c2 的记录一起清掉");
    }

    @Test
    @DisplayName("clearClient 清除该客户端全部在途标识符")
    void clearClientDropsEverything() {
        InboundQos2Store store = store();

        store.mark("c1", 1);
        store.mark("c1", 2);
        store.clearClient("c1");

        assertEquals(0, store.clientCount());
        assertEquals(IInboundQos2Store.Result.FIRST, store.mark("c1", 1),
                "会话销毁后, 旧标识符不应再被当成重复");
    }

    @Test
    @DisplayName("超出上限返回 OVERFLOW, 调用方据此断开连接")
    void overflowRejects() {
        // maxInflight=4 -> 上限 max(16, 8) = 16
        InboundQos2Store store = store();

        for (int i = 1; i <= 16; i++) {
            assertEquals(IInboundQos2Store.Result.FIRST, store.mark("c1", i),
                    "上限内的第 " + i + " 个标识符应当被接受");
        }
        assertEquals(IInboundQos2Store.Result.OVERFLOW, store.mark("c1", 17),
                "超出上限必须返回 OVERFLOW —— 悄悄丢弃会让客户端以为发送成功");
    }

    @Test
    @DisplayName("重复标记不占用配额(先判重再插入)")
    void duplicatesDoNotConsumeCapacity() {
        InboundQos2Store store = store();

        store.mark("c1", 1);
        // 同一个标识符重发很多次: 若实现写成「先插入再判重」, 计数会先被推上去,
        // 长连接上的正常重发会逐渐逼近上限并最终误判为协议违规
        for (int i = 0; i < 200; i++) {
            assertEquals(IInboundQos2Store.Result.DUPLICATE, store.mark("c1", 1));
        }
        for (int i = 2; i <= 16; i++) {
            assertNotEquals(IInboundQos2Store.Result.OVERFLOW, store.mark("c1", i),
                    "大量重复之后仍应有余量容纳新的标识符");
        }
    }

    @Test
    @DisplayName("clientId 为空时不抛出异常, 且按首次处理")
    void nullClientIdIsSafe() {
        InboundQos2Store store = store();

        // 理论上不该出现(CONNECT 之前不会有 PUBLISH), 但真出现时
        // 宁可重复投递一条, 也不要因为空 key 把正常消息丢掉
        assertEquals(IInboundQos2Store.Result.FIRST, store.mark(null, 1));
        assertEquals(0, store.clientCount(), "空 clientId 不应产生条目");
        store.release(null, 1);
        store.clearClient(null);
    }
}
