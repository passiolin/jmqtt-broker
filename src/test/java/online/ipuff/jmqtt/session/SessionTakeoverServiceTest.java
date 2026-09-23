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

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import online.ipuff.jmqtt.message.SubscribeStore;
import online.ipuff.jmqtt.store.DupPubRelMessageStoreService;
import online.ipuff.jmqtt.store.IPendingMessageStore;
import online.ipuff.jmqtt.store.InMemoryPendingMessageStore;
import online.ipuff.jmqtt.TestBrokerProperties;
import online.ipuff.jmqtt.store.InboundQos2Store;
import online.ipuff.jmqtt.store.PendingMessage;
import online.ipuff.jmqtt.store.DupPublishMessageStoreService;
import online.ipuff.jmqtt.subscribe.SubscribeStoreService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 跨节点连接接管测试。
 *
 * <p>重点验证两条最容易写错的约束:
 * <ol>
 *   <li><b>会话必须在关连接之前被移除。</b>反过来的话, 连接关闭触发的
 *       {@code channelInactive} 会读到仍然存在的会话, 从而错误地发布遗嘱 ——
 *       客户端只是换了节点, 并没有死。</li>
 *   <li><b>绝不删除持久层的会话记录。</b>收到接管通知时该会话已归属新节点,
 *       删掉等于把新节点刚建立的会话摧毁。本类刻意不注入
 *       {@code SessionPersistence}, 从结构上杜绝这种错误。</li>
 * </ol>
 */
class SessionTakeoverServiceTest {

    private static final String CLIENT = "device-1";

    private ConnectionRegistry connectionRegistry;
    private SessionStoreService sessionStoreService;
    private SubscribeStoreService subscribeStoreService;
    private DupPublishMessageStoreService dupPublishStore;
    private DupPubRelMessageStoreService dupPubRelStore;
    private InMemoryPendingMessageStore pendingStore;
    private InboundQos2Store inboundQos2Store;
    private SessionTakeoverService service;

    @BeforeEach
    void setUp() {
        connectionRegistry = new ConnectionRegistry();
        sessionStoreService = new SessionStoreService();
        subscribeStoreService = new SubscribeStoreService();
        dupPublishStore = new DupPublishMessageStoreService();
        dupPubRelStore = new DupPubRelMessageStoreService();
        pendingStore = new InMemoryPendingMessageStore();
        inboundQos2Store = new InboundQos2Store(TestBrokerProperties.create());
        service = new SessionTakeoverService(connectionRegistry, sessionStoreService,
                subscribeStoreService, inboundQos2Store, dupPublishStore, dupPubRelStore,
                pendingStore, online.ipuff.jmqtt.TestNodeMetrics.create());

        sessionStoreService.put(new SessionStore("node-old", CLIENT, false, 7200));
        subscribeStoreService.put(new SubscribeStore(CLIENT, "sensor/+/temp", 1));
    }

    @Test
    @DisplayName("接管本节点未持有的 clientId 时不做任何事")
    void unknownClientIsNoOp() {
        service.onTakeover("not-connected", "node-new");

        assertEquals(1, sessionStoreService.size(), "不应影响其他会话");
        assertEquals(1, subscribeStoreService.subscriptionCount(), "不应影响其他订阅");
    }

    @Test
    @DisplayName("接管时关闭连接并清理本地运行时状态")
    void takeoverClosesChannelAndCleansLocalState() {
        Channel channel = registerConnectedChannel();

        service.onTakeover(CLIENT, "node-new");

        verify(channel).close();
        assertFalse(sessionStoreService.containsKey(CLIENT), "本地会话应被移除");
        assertFalse(connectionRegistry.isOnline(CLIENT), "连接应已注销");
        assertEquals(0, subscribeStoreService.subscriptionCount(), "本地订阅应被清除");
    }

    @Test
    @DisplayName("会话必须在 channel.close() 之前被移除 —— 否则会误发遗嘱")
    void sessionRemovedBeforeChannelClosed() {
        Channel channel = registerConnectedChannel();

        // 在 close() 被调用的那一刻, 检查会话是否还在
        AtomicBoolean sessionPresentAtClose = new AtomicBoolean(true);
        when(channel.close()).thenAnswer(invocation -> {
            sessionPresentAtClose.set(sessionStoreService.containsKey(CLIENT));
            return null;
        });

        service.onTakeover(CLIENT, "node-new");

        verify(channel).close();
        assertFalse(sessionPresentAtClose.get(),
                "close() 时会话必须已移除; 否则 channelInactive 会发布遗嘱(客户端只是换了节点)");
    }

    @Test
    @DisplayName("关连接后调用方不会重复注销(ConnectionRegistry 已先移除)")
    void registryEntryRemovedBeforeClose() {
        Channel channel = registerConnectedChannel();

        AtomicBoolean onlineAtClose = new AtomicBoolean(true);
        when(channel.close()).thenAnswer(invocation -> {
            onlineAtClose.set(connectionRegistry.isOnline(CLIENT));
            return null;
        });

        service.onTakeover(CLIENT, "node-new");

        assertFalse(onlineAtClose.get(), "close() 时该 clientId 不应再出现在注册表中");
    }

    @Test
    @DisplayName("在途未确认消息被清理(已知限制: 这些消息会被丢弃)")
    void inflightMessagesAreCleared() {
        Channel channel = registerConnectedChannel();
        dupPublishStore.put(new online.ipuff.jmqtt.message.DupPublishMessageStore(
                CLIENT, "sensor/a/temp", 1, 42, new byte[]{1}));

        service.onTakeover(CLIENT, "node-new");

        verify(channel).close();
        assertEquals(0, dupPublishStore.size(), "在途消息无法再投递, 应被清理而不是泄漏");
    }

    @Test
    @DisplayName("非持久队列在接管时被清理(客户端已迁走, 本节点再也投不出去)")
    void nonPersistentQueueClearedOnTakeover() {
        Channel channel = registerConnectedChannel();
        pendingStore.enqueue(CLIENT, PendingMessage.of("sensor/a/temp", 1, new byte[]{1}, false), 100);

        service.onTakeover(CLIENT, "node-new");

        verify(channel).close();
        assertEquals(0, pendingStore.size(CLIENT), "非持久队列应清理, 否则会永久占用本节点内存");
    }

    @Test
    @DisplayName("持久队列在接管时必须保留 —— 否则会销毁新节点该投递的消息")
    void persistentQueueKeptOnTakeover() {
        RecordingPersistentQueue persistentQueue = new RecordingPersistentQueue();
        ServiceWithPersistentQueue holder = new ServiceWithPersistentQueue(persistentQueue);

        holder.service.onTakeover(CLIENT, "node-new");

        assertEquals(0, persistentQueue.removeAllCalls,
                "持久队列已随会话归属新节点, 接管时绝不能删");
    }

    /** 用于验证「持久队列不被删除」的测试替身 */
    static class RecordingPersistentQueue extends InMemoryPendingMessageStore {
        int removeAllCalls;

        @Override
        public boolean persistent() {
            return true;
        }

        @Override
        public void removeAll(String clientId) {
            removeAllCalls++;
        }
    }

    /** 只关心队列行为的最小装配 */
    class ServiceWithPersistentQueue {
        final SessionTakeoverService service;

        ServiceWithPersistentQueue(IPendingMessageStore store) {
            service = new SessionTakeoverService(connectionRegistry, sessionStoreService,
                    subscribeStoreService, inboundQos2Store, dupPublishStore, dupPubRelStore,
                    store, online.ipuff.jmqtt.TestNodeMetrics.create());
        }
    }

    /**
     * 注册一条本节点持有的连接。
     *
     * <p>channel.id() 必须打桩: ConnectionRegistry 在发生接管时会在日志里读它。
     */
    private Channel registerConnectedChannel() {
        Channel channel = mock(Channel.class);
        ChannelId channelId = mock(ChannelId.class);
        when(channelId.asShortText()).thenReturn("test-ch");
        when(channel.id()).thenReturn(channelId);
        when(channel.isActive()).thenReturn(true);
        connectionRegistry.register(CLIENT, channel);
        return channel;
    }
}
