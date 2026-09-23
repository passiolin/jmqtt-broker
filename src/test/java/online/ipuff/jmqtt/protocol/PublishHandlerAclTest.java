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
package online.ipuff.jmqtt.protocol;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import online.ipuff.jmqtt.TestBrokerProperties;
import online.ipuff.jmqtt.authz.IAclService;
import online.ipuff.jmqtt.cluster.InternalCommunication;
import online.ipuff.jmqtt.cluster.InternalSendServer;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.message.SubscribeStore;
import online.ipuff.jmqtt.session.BackpressureMetrics;
import online.ipuff.jmqtt.session.ConnectionRegistry;
import online.ipuff.jmqtt.session.QosMetrics;
import online.ipuff.jmqtt.session.SendBuffer;
import online.ipuff.jmqtt.session.SessionStoreService;
import online.ipuff.jmqtt.store.DupPublishMessageStoreService;
import online.ipuff.jmqtt.store.InboundQos2Store;
import online.ipuff.jmqtt.store.InMemoryPendingMessageStore;
import online.ipuff.jmqtt.store.MessageIdService;
import online.ipuff.jmqtt.store.RetainMessageStoreService;
import online.ipuff.jmqtt.subscribe.SubscribeStoreService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PUBLISH 路径的 ACL 门控: 检查挂起期间同连接的后续 PUBLISH 必须<b>按序排队</b>
 * (MQTT 对同一连接的报文顺序有保证), 拒绝要按版本正确回码, superuser 整体跳过,
 * 排队超限关连接(不能为等 ACL 无限吃内存)。
 */
class PublishHandlerAclTest {

    private static final String PUB = "publisher";
    private static final String SUB = "subscriber";

    /** 可控 ACL 桩: 按 topic 挂起或直接给结论, 统计命中 */
    private static final class StubAcl implements IAclService {
        final Map<String, CompletableFuture<Boolean>> pending = new ConcurrentHashMap<>();
        final AtomicInteger hits = new AtomicInteger();
        final AtomicInteger offlineCalls = new AtomicInteger();

        void hold(String topic) {
            pending.put("publish\n" + topic, new CompletableFuture<>());
        }

        void release(String topic, boolean allow) {
            pending.get("publish\n" + topic).complete(allow);
        }

        @Override
        public CompletableFuture<Boolean> check(String clientId, String username, String peerhost,
                                                String action, String topic) {
            hits.incrementAndGet();
            CompletableFuture<Boolean> gate = pending.get(action + "\n" + topic);
            return gate != null ? gate : CompletableFuture.completedFuture(Boolean.TRUE);
        }

        @Override
        public void onClientOffline(String clientId) {
            offlineCalls.incrementAndGet();
        }
    }

    private BrokerProperties props;
    private SubscribeStoreService subscribeStoreService;
    private PublishHandler handler;
    private StubAcl acl;
    private ConnectionRegistry registry;

    @BeforeEach
    void setUp() {
        props = TestBrokerProperties.create();
        subscribeStoreService = new SubscribeStoreService();
        registry = new ConnectionRegistry();
        SessionStoreService sessionStoreService = new SessionStoreService();
        BackpressureMetrics metrics = new BackpressureMetrics();
        InternalSendServer sendServer = new InternalSendServer(
                subscribeStoreService, registry, new MessageIdService(),
                new DupPublishMessageStoreService(), sessionStoreService,
                new InMemoryPendingMessageStore(), props, metrics, new QosMetrics(),
                new online.ipuff.jmqtt.admin.TopicCaptureService(
                        online.ipuff.jmqtt.TestObjectProviders.empty(),
                        new online.ipuff.jmqtt.admin.AdminRedisKeys(props), props));
        InternalCommunication communication = new InternalCommunication(
                new online.ipuff.jmqtt.cluster.LocalClusterBus(props), props);
        acl = new StubAcl();
        handler = new PublishHandler(communication, sendServer,
                new RetainMessageStoreService(), new QosMetrics(),
                new InboundQos2Store(props), acl,
                new online.ipuff.jmqtt.admin.TopicCaptureService(
                        emptyProvider(),
                        new online.ipuff.jmqtt.admin.AdminRedisKeys(props), props));
    }

    private EmbeddedChannel publisherChannel(boolean v5) {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.attr(ChannelAttributes.CLIENT_ID).set(PUB);
        channel.attr(ChannelAttributes.PROTOCOL_VERSION)
                .set(v5 ? MqttVersion.MQTT_5 : MqttVersion.MQTT_3_1_1);
        return channel;
    }

    private EmbeddedChannel subscriberChannel() {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.attr(ChannelAttributes.CLIENT_ID).set(SUB);
        channel.attr(ChannelAttributes.PROTOCOL_VERSION).set(MqttVersion.MQTT_5);
        channel.attr(ChannelAttributes.SEND_BUFFER).set(new SendBuffer(32, 1000, new BackpressureMetrics()));
        subscribeStoreService.put(new SubscribeStore(SUB, "sensor/#", 0));
        // 投递按 ConnectionRegistry 找通道 —— 没注册就等于订阅者离线
        registry.register(SUB, channel);
        return channel;
    }

    private static MqttPublishMessage publish(String topic, int qos, int packetId, String body) {
        MqttPublishVariableHeader header = qos > 0
                ? new MqttPublishVariableHeader(topic, packetId, MqttProperties.NO_PROPERTIES)
                : new MqttPublishVariableHeader(topic, packetId, MqttProperties.NO_PROPERTIES);
        return new MqttPublishMessage(new MqttFixedHeader(
                MqttMessageType.PUBLISH, false, MqttQoS.valueOf(qos), false, 0),
                header, Unpooled.wrappedBuffer(body.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    @DisplayName("缓存内直接放行: 正常投递, 不回错误码")
    void allowedPublishDelivers() {
        EmbeddedChannel subscriber = subscriberChannel();
        EmbeddedChannel publisher = publisherChannel(true);
        handler.processPublish(publisher, publish("sensor/temp", 0, 0, "23.5"));
        assertNotNull(subscriber.readOutbound(), "放行的消息必须投递");
        assertNull(publisher.readOutbound(), "QoS 0 不回 PUBACK");
    }

    @Test
    @DisplayName("ACL 拒绝(v5): 不投递, 回 PUBACK 0x87")
    void deniedV5RepliesNotAuthorized() {
        EmbeddedChannel subscriber = subscriberChannel();
        EmbeddedChannel publisher = publisherChannel(true);
        acl.hold("sensor/temp");
        handler.processPublish(publisher, publish("sensor/temp", 1, 7, "23.5"));
        acl.release("sensor/temp", false);
        publisher.runPendingTasks();

        assertNull(subscriber.readOutbound(), "拒绝的消息不得投递");
        io.netty.handler.codec.mqtt.MqttMessage ack = publisher.readOutbound();
        assertNotNull(ack, "QoS 1 拒绝也要回 PUBACK(带原因码), 否则客户端会一直重发");
        io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader header =
                (io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader) ack.variableHeader();
        assertEquals(0x87, header.reasonCode() & 0xFF, "v5 Not authorized = 0x87");
    }

    @Test
    @DisplayName("ACL 拒绝(v3): 不投递不回码(v3 没有逐条错误通道)")
    void deniedV3DropsSilently() {
        EmbeddedChannel subscriber = subscriberChannel();
        EmbeddedChannel publisher = publisherChannel(false);
        acl.hold("sensor/temp");
        handler.processPublish(publisher, publish("sensor/temp", 1, 7, "23.5"));
        acl.release("sensor/temp", false);
        publisher.runPendingTasks();

        assertNull(subscriber.readOutbound());
        assertNull(publisher.readOutbound(), "v3 拒绝只能静默丢弃");
    }

    @Test
    @DisplayName("ACL 挂起期间后续 PUBLISH 按序排队: 先到的先投递")
    void pendingCheckPreservesOrder() {
        EmbeddedChannel subscriber = subscriberChannel();
        EmbeddedChannel publisher = publisherChannel(true);

        acl.hold("sensor/a");
        handler.processPublish(publisher, publish("sensor/a", 0, 0, "first"));
        handler.processPublish(publisher, publish("sensor/b", 0, 0, "second"));

        assertNull(subscriber.readOutbound(), "第一条还在等 ACL, 第二条不得越过它先投递");

        acl.release("sensor/a", true);
        publisher.runPendingTasks();

        Object first = subscriber.readOutbound();
        assertNotNull(first, "放行后两条都要投递");
        Object second = subscriber.readOutbound();
        assertNotNull(second);
        assertTrue(payloadOf(first).contains("first"), "顺序必须先 first: " + payloadOf(first));
        assertTrue(payloadOf(second).contains("second"), "后 second");
    }

    @Test
    @DisplayName("superuser 连接完全跳过 ACL(不发请求)")
    void superuserSkipsAcl() {
        EmbeddedChannel subscriber = subscriberChannel();
        EmbeddedChannel publisher = publisherChannel(true);
        publisher.attr(ChannelAttributes.SUPERUSER).set(Boolean.TRUE);

        handler.processPublish(publisher, publish("sensor/temp", 0, 0, "23.5"));

        assertNotNull(subscriber.readOutbound());
        assertEquals(0, acl.hits.get(), "superuser 不得触发 ACL 请求");
    }

    @Test
    @DisplayName("排队超上限: 关闭连接而不是无限吃内存")
    void queueOverflowClosesChannel() {
        EmbeddedChannel publisher = publisherChannel(true);
        acl.hold("sensor/a");
        handler.processPublish(publisher, publish("sensor/a", 0, 0, "hold"));

        // 门容量 64: 再写 64 条已完成检查的 PUBLISH 不触发关闭, 第 65 条触发
        for (int i = 0; i < 64; i++) {
            handler.processPublish(publisher, publish("sensor/x" + i, 0, 0, "q"));
        }
        assertTrue(publisher.isActive(), "64 条排队仍在容量内");

        handler.processPublish(publisher, publish("sensor/overflow", 0, 0, "q"));
        assertFalse(publisher.isActive(), "超出容量必须关连接");
    }

    private static String payloadOf(Object message) {
        return new String(((MqttPublishMessage) message).payload().array(), StandardCharsets.UTF_8);
    }

    private static <T> ObjectProvider<T> emptyProvider() {
        return new ObjectProvider<>() {
            @Override
            public T getObject() {
                throw new java.util.NoSuchElementException();
            }

            @Override
            public T getObject(Object... args) {
                throw new java.util.NoSuchElementException();
            }

            @Override
            public T getIfAvailable() {
                return null;
            }

            @Override
            public T getIfUnique() {
                return null;
            }
        };
    }
}
