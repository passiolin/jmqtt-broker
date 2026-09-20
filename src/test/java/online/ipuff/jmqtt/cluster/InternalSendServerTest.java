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
package online.ipuff.jmqtt.cluster;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.message.SubscribeStore;
import online.ipuff.jmqtt.session.BackpressureMetrics;
import online.ipuff.jmqtt.session.ConnectionRegistry;
import online.ipuff.jmqtt.session.QosMetrics;
import online.ipuff.jmqtt.session.SendBuffer;
import online.ipuff.jmqtt.session.SessionStore;
import online.ipuff.jmqtt.session.SessionStoreService;
import online.ipuff.jmqtt.store.DupPublishMessageStoreService;
import online.ipuff.jmqtt.store.InMemoryPendingMessageStore;
import online.ipuff.jmqtt.store.MessageIdService;
import online.ipuff.jmqtt.store.PendingMessage;
import online.ipuff.jmqtt.subscribe.SubscribeStoreService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 投递路径的离线队列、发送窗口与背压策略测试。
 *
 * <p>用 Netty 的 {@link EmbeddedChannel} 而不是 mock: 它能真实提供
 * {@code attr()} / {@code writeAndFlush()} / {@code isWritable()},
 * 并且可以直接读回写出的报文 —— 断言的是「真的写出去了什么」,
 * 而不是「调用过某个方法」。
 *
 * <p>核心要钉住的三组约束:
 * <ol>
 *   <li><b>离线队列的丢弃口径</b> —— 只有「持久会话 + QoS&gt;0 + 离线」才入队</li>
 *   <li><b>发送窗口</b> —— 未确认的 QoS 1/2 达到上限后不再发新的, 这就是 MQTT 流控</li>
 *   <li><b>确认后必须继续投递</b> —— 否则窗口一旦用满就永久卡死</li>
 * </ol>
 */
class InternalSendServerTest {

    private static final String TOPIC = "sensor/room1/temp";
    private static final String PUBLISHER = "publisher";
    private static final String SUBSCRIBER = "subscriber";

    private SubscribeStoreService subscribeStoreService;
    private ConnectionRegistry connectionRegistry;
    private SessionStoreService sessionStoreService;
    private InMemoryPendingMessageStore pendingStore;
    private DupPublishMessageStoreService dupStore;
    private BackpressureMetrics metrics;
    private InternalSendServer server;

    @BeforeEach
    void setUp() {
        subscribeStoreService = new SubscribeStoreService();
        connectionRegistry = new ConnectionRegistry();
        sessionStoreService = new SessionStoreService();
        pendingStore = new InMemoryPendingMessageStore();
        dupStore = new DupPublishMessageStoreService();
        metrics = new BackpressureMetrics();
        server = buildServer(propertiesWithOfflineQueueLen(1000));
    }

    // ------------------------------------------------------------------
    // 离线队列
    // ------------------------------------------------------------------

    @Test
    @DisplayName("订阅者在线时直接投递, 不入队")
    void onlineSubscriberIsDeliveredDirectly() {
        subscribe("sensor/+/temp", 1);
        sessionStoreService.put(durableSession());
        EmbeddedChannel channel = connect(32, 1000);

        int delivered = server.sendPublishMessage(message(1));

        assertEquals(1, delivered);
        assertEquals(0, pendingStore.size(SUBSCRIBER), "在线时不应入队");
        assertNotNull(channel.readOutbound(), "应真的写出了 PUBLISH");
    }

    @Test
    @DisplayName("离线 + 持久会话 + QoS 1 → 入队")
    void offlineDurableSubscriberQueuesQos1() {
        subscribe("sensor/+/temp", 1);
        sessionStoreService.put(durableSession());

        server.sendPublishMessage(message(1));

        assertEquals(1, pendingStore.size(SUBSCRIBER), "这是 QoS 1/2 投递保证的核心场景");
    }

    @Test
    @DisplayName("离线 + QoS 0 → 不入队")
    void offlineQos0IsNotQueued() {
        subscribe("sensor/+/temp", 0);
        sessionStoreService.put(durableSession());

        server.sendPublishMessage(message(0));

        assertEquals(0, pendingStore.size(SUBSCRIBER), "QoS 0 没有投递保证, 离线即丢");
    }

    @Test
    @DisplayName("离线 + cleanSession=1 → 不入队")
    void offlineCleanSessionIsNotQueued() {
        subscribe("sensor/+/temp", 1);
        sessionStoreService.put(new SessionStore("node-1", SUBSCRIBER, true, 0));

        server.sendPublishMessage(message(1));

        assertEquals(0, pendingStore.size(SUBSCRIBER), "cleanSession=1 的会话断开即消失");
    }

    @Test
    @DisplayName("本地无该会话 → 不入队")
    void unknownSessionIsNotQueued() {
        subscribe("sensor/+/temp", 1);

        server.sendPublishMessage(message(1));

        assertEquals(0, pendingStore.size(SUBSCRIBER));
    }

    @Test
    @DisplayName("重连后投递积压并清空队列")
    void reconnectDrainsPendingMessages() {
        subscribe("sensor/+/temp", 1);
        sessionStoreService.put(durableSession());
        server.sendPublishMessage(message(1));
        server.sendPublishMessage(message(1));
        assertEquals(2, pendingStore.size(SUBSCRIBER));

        EmbeddedChannel channel = connect(32, 1000);
        int delivered = server.deliverPendingMessages(SUBSCRIBER, channel);

        assertEquals(2, delivered);
        assertEquals(0, pendingStore.size(SUBSCRIBER), "投递后队列应清空");
        assertNotNull(channel.readOutbound());
        assertNotNull(channel.readOutbound(), "两条都应写出");
    }

    @Test
    @DisplayName("连接已断时停止投递, 剩余积压留在队列里")
    void drainStopsWhenChannelInactive() {
        subscribe("sensor/+/temp", 1);
        sessionStoreService.put(durableSession());
        server.sendPublishMessage(message(1));

        EmbeddedChannel channel = connect(32, 1000);
        channel.close();   // 模拟投递过程中连接断开

        int delivered = server.deliverPendingMessages(SUBSCRIBER, channel);

        assertEquals(0, delivered, "连接不可用时不投递");
        assertEquals(1, pendingStore.size(SUBSCRIBER), "积压必须留在队列里等下次重连");
    }

    @Test
    @DisplayName("离线队列超限时丢弃最旧的, 保留最新的")
    void offlineQueueOverflowDropsOldest() {
        server = buildServer(propertiesWithOfflineQueueLen(2));

        subscribe("sensor/+/temp", 1);
        sessionStoreService.put(durableSession());

        server.sendPublishMessage(messageWithPayload(1, "first"));
        server.sendPublishMessage(messageWithPayload(1, "second"));
        server.sendPublishMessage(messageWithPayload(1, "third"));

        List<PendingMessage> queued = pendingStore.drainBatch(SUBSCRIBER, 10);
        assertEquals(2, queued.size(), "应被限制在上限内");
        assertEquals("second", new String(queued.get(0).payload(), StandardCharsets.UTF_8),
                "丢最旧的: 第一条应已被丢弃");
        assertEquals(1, pendingStore.droppedCount(), "丢弃必须被计数, 不能静默");
    }

    // ------------------------------------------------------------------
    // 发送窗口(背压)
    // ------------------------------------------------------------------

    @Test
    @DisplayName("窗口用尽后不再发送, 消息进入有界发送队列")
    void windowExhaustedQueuesInsteadOfSending() {
        subscribe("sensor/+/temp", 1);
        sessionStoreService.put(durableSession());
        EmbeddedChannel channel = connect(1, 100);   // 窗口只有 1

        server.sendPublishMessage(messageWithPayload(1, "first"));
        server.sendPublishMessage(messageWithPayload(1, "second"));

        assertNotNull(channel.readOutbound(), "第一条应写出");
        assertNull(channel.readOutbound(), "窗口用尽后不应再写第二条");
        assertEquals(1, sendQueueSize(channel), "第二条应排队等待确认");
    }

    @Test
    @DisplayName("收到确认后释放窗口并继续投递排队中的消息 —— 这是流控的闭环")
    void ackReleasesWindowAndDrainsQueue() {
        subscribe("sensor/+/temp", 1);
        sessionStoreService.put(durableSession());
        EmbeddedChannel channel = connect(1, 100);

        server.sendPublishMessage(messageWithPayload(1, "first"));
        server.sendPublishMessage(messageWithPayload(1, "second"));

        MqttPublishMessage first = channel.readOutbound();
        assertNotNull(first);
        assertEquals(1, sendQueueSize(channel), "第二条应在队列里");

        // 客户端确认第一条
        server.releaseSendWindow(channel, first.variableHeader().packetId());

        MqttPublishMessage second = channel.readOutbound();
        assertNotNull(second, "释放窗口后应把排队中的第二条发出去");
        assertEquals("second", second.payload().toString(StandardCharsets.UTF_8));
        assertEquals(0, sendQueueSize(channel), "队列应已清空");
    }

    @Test
    @DisplayName("QoS 0 不占用发送窗口")
    void qos0DoesNotConsumeWindow() {
        subscribe("sensor/+/temp", 0);
        sessionStoreService.put(durableSession());
        EmbeddedChannel channel = connect(1, 100);   // 窗口只有 1

        server.sendPublishMessage(messageWithPayload(0, "a"));
        server.sendPublishMessage(messageWithPayload(0, "b"));
        server.sendPublishMessage(messageWithPayload(0, "c"));

        assertNotNull(channel.readOutbound());
        assertNotNull(channel.readOutbound());
        assertNotNull(channel.readOutbound(), "QoS 0 不占窗口, 三条都应发出");
        assertEquals(0, sendQueueSize(channel));
    }

    @Test
    @DisplayName("发送队列超限时丢弃最旧的, 且计数")
    void sendQueueOverflowDropsOldest() {
        subscribe("sensor/+/temp", 1);
        sessionStoreService.put(durableSession());
        EmbeddedChannel channel = connect(1, 2);   // 窗口 1, 队列 2

        for (int i = 1; i <= 5; i++) {
            server.sendPublishMessage(messageWithPayload(1, "m" + i));
        }

        assertNotNull(channel.readOutbound(), "第一条占用窗口直接写出");
        assertEquals(2, sendQueueSize(channel), "队列应被限制在上限内");

        Long dropped = metrics.stats().get("sendQueueFullDropped");
        assertNotNull(dropped);
        assertEquals(2L, dropped, "超出的 2 条应被丢弃并计数");
    }

    // ------------------------------------------------------------------
    // 辅助
    // ------------------------------------------------------------------

    private int sendQueueSize(EmbeddedChannel channel) {
        SendBuffer buffer = channel.attr(ChannelAttributes.SEND_BUFFER).get();
        return buffer == null ? 0 : buffer.queuedCount();
    }

    /** 建立一条带发送缓冲的连接(模拟 CONNECT 之后的连接) */
    private EmbeddedChannel connect(int maxInflight, int maxQueueLen) {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.attr(ChannelAttributes.CLIENT_ID).set(SUBSCRIBER);
        channel.attr(ChannelAttributes.SEND_BUFFER)
                .set(new SendBuffer(maxInflight, maxQueueLen, metrics));
        connectionRegistry.register(SUBSCRIBER, channel);
        return channel;
    }

    private void subscribe(String filter, int qos) {
        subscribeStoreService.put(new SubscribeStore(SUBSCRIBER, filter, qos));
    }

    private static SessionStore durableSession() {
        return new SessionStore("node-1", SUBSCRIBER, false, 7200);
    }

    private static InternalMessage message(int qos) {
        return messageWithPayload(qos, "23.5");
    }

    private static InternalMessage messageWithPayload(int qos, String body) {
        return new InternalMessage((PROP_NODE), PUBLISHER, TOPIC, qos,
                body.getBytes(StandardCharsets.UTF_8), false, false);
    }

    private static final String PROP_NODE = "node-1";

    private InternalSendServer buildServer(BrokerProperties props) {
        return new InternalSendServer(subscribeStoreService, connectionRegistry, new MessageIdService(),
                dupStore, sessionStoreService, pendingStore, props, metrics, new QosMetrics());
    }

    private static BrokerProperties propertiesWithOfflineQueueLen(int offlineQueueLen) {
        BrokerProperties.KafkaProperties kafka = new BrokerProperties.KafkaProperties(
                false, "127.0.0.1:9092", "jmqtt", "jmqtt-cluster", null,
                true, List.of(),
                "", List.of(), false, 1000, 1000, 200, "none", "latest", "topic");
        BrokerProperties.RedisProperties redis = new BrokerProperties.RedisProperties(
                false, "127.0.0.1", 6379, null, 0, "jmqtt", 1000, 5000, "off", 100, 10000);
        return new BrokerProperties(
                PROP_NODE, null, 1883, true, 8083, "/mqtt",
                1, 2, false,
                false, "jmqtt", "jmqtt",
                60, 7200, 0, 0, 32, 1000, offlineQueueLen,
                false, kafka, redis,
                511, true, true, 10485760, 32768, 65536);
    }
}
