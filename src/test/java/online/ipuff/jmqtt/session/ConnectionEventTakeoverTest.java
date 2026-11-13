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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package online.ipuff.jmqtt.session;

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.netty.util.Attribute;
import online.ipuff.jmqtt.TestBrokerProperties;
import online.ipuff.jmqtt.cluster.ConnectionEvent;
import online.ipuff.jmqtt.cluster.TakeoverListener;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 连接事件触发跨节点接管的判定测试。
 *
 * <p>核心约束: <b>迟到的事件不能踢掉新连接</b>。客户端 B → A 快速切换时,
 * A 可能先建好连接、后收到 B 的 connected 事件; 只有本地连接明显更老才允许释放。
 */
class ConnectionEventTakeoverTest {

    private static final String CLIENT = "device-1";
    private static final String OTHER_NODE = "node-b";
    private static final long NOW = 1_800_000_000_000L;

    private ConnectionRegistry connectionRegistry;
    private SessionStoreService sessionStoreService;
    private TakeoverListener takeoverListener;
    private ConnectionEventTakeover takeover;

    @BeforeEach
    void setUp() {
        connectionRegistry = new ConnectionRegistry();
        sessionStoreService = new SessionStoreService();
        takeoverListener = mock(TakeoverListener.class);
        takeover = new ConnectionEventTakeover(TestBrokerProperties.create(),
                connectionRegistry, sessionStoreService, takeoverListener);
    }

    @Test
    @DisplayName("预过滤: 本节点既无连接也无会话时, 不解析事件体")
    void unrelatedClientSkippedByKey() {
        assertFalse(takeover.shouldProcess(CLIENT));
        assertEquals(1, takeover.skippedNotLocal());
    }

    @Test
    @DisplayName("预过滤: 有活跃连接或残留会话都必须继续处理")
    void localConnectionOrSessionIsRelevant() {
        registerConnection(NOW);
        assertTrue(takeover.shouldProcess(CLIENT));

        ConnectionRegistry emptyRegistry = new ConnectionRegistry();
        ConnectionEventTakeover withSession = new ConnectionEventTakeover(
                TestBrokerProperties.create(), emptyRegistry, sessionStoreService, takeoverListener);
        sessionStoreService.put(new SessionStore("node-old", "offline-device", false, 7200));
        assertTrue(withSession.shouldProcess("offline-device"),
                "会话残留需要被接管清理, 不能因无连接而跳过");
    }

    @Test
    @DisplayName("本节点自己发出的 connected 事件忽略(本地接管已由注册表完成)")
    void selfEventIgnored() {
        boolean applied = takeover.onEvent(connected(CLIENT, "node-1", NOW));

        assertFalse(applied);
        verify(takeoverListener, never()).onTakeover(CLIENT, "node-1");
    }

    @Test
    @DisplayName("disconnected 事件不触发接管")
    void disconnectedEventIgnored() {
        assertFalse(takeover.onEvent(new ConnectionEvent(
                ConnectionEvent.ACTION_DISCONNECTED, CLIENT, null, 4, "MQTT",
                "10.0.0.9:1", OTHER_NODE, ConnectionEvent.REASON_TCP_CLOSED, null, NOW)));
        verify(takeoverListener, never()).onTakeover(CLIENT, OTHER_NODE);
    }

    @Test
    @DisplayName("本节点无该连接时仍委托清理(可能是残留的会话/订阅副本)")
    void noLocalConnectionDelegates() {
        boolean applied = takeover.onEvent(connected(CLIENT, OTHER_NODE, NOW));

        assertTrue(applied);
        verify(takeoverListener).onTakeover(CLIENT, OTHER_NODE);
        assertEquals(1, takeover.applied());
    }

    @Test
    @DisplayName("本地连接更老 → 释放(正常的跨节点接管)")
    void olderLocalConnectionReleased() {
        registerConnection(NOW - 10_000);

        boolean applied = takeover.onEvent(connected(CLIENT, OTHER_NODE, NOW));

        assertTrue(applied);
        verify(takeoverListener).onTakeover(CLIENT, OTHER_NODE);
    }

    @Test
    @DisplayName("本地连接更新 → 忽略(迟到事件不能打断刚重连上来的连接)")
    void newerLocalConnectionKept() {
        registerConnection(NOW);

        boolean applied = takeover.onEvent(connected(CLIENT, OTHER_NODE, NOW - 10_000));

        assertFalse(applied);
        verify(takeoverListener, never()).onTakeover(CLIENT, OTHER_NODE);
        assertEquals(1, takeover.skippedStale());
    }

    @Test
    @DisplayName("差距在时钟容差内 → 视为同时发生, 不踢")
    void withinClockSkewKept() {
        registerConnection(NOW - ConnectionEventTakeover.CLOCK_SKEW_TOLERANCE_MS + 1);

        boolean applied = takeover.onEvent(connected(CLIENT, OTHER_NODE, NOW));

        assertFalse(applied);
        verify(takeoverListener, never()).onTakeover(CLIENT, OTHER_NODE);
    }

    @Test
    @DisplayName("缺少接入时间 → 无法判断, 不踢(由 keepalive 超时兜底)")
    void missingTimestampKept() {
        registerConnection(null);

        boolean applied = takeover.onEvent(connected(CLIENT, OTHER_NODE, NOW));

        assertFalse(applied);
        verify(takeoverListener, never()).onTakeover(CLIENT, OTHER_NODE);
    }

    private void registerConnection(Long connectedAt) {
        Channel channel = mock(Channel.class);
        ChannelId channelId = mock(ChannelId.class);
        when(channelId.asShortText()).thenReturn("test-ch");
        when(channel.id()).thenReturn(channelId);
        when(channel.isActive()).thenReturn(true);
        @SuppressWarnings("unchecked")
        Attribute<Long> attr = mock(Attribute.class);
        when(attr.get()).thenReturn(connectedAt);
        when(channel.attr(ChannelAttributes.CONNECTED_AT)).thenReturn(attr);
        connectionRegistry.register(CLIENT, channel);
    }

    private static ConnectionEvent connected(String clientId, String node, long at) {
        return new ConnectionEvent(ConnectionEvent.ACTION_CONNECTED, clientId, "u", 4, "MQTT",
                "10.0.0.9:47414", node, null, at, null);
    }
}
