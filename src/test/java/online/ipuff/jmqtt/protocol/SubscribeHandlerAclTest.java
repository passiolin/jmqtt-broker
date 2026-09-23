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

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttVersion;
import online.ipuff.jmqtt.TestBrokerProperties;
import online.ipuff.jmqtt.authz.IAclService;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.session.SessionStoreService;
import online.ipuff.jmqtt.session.persistence.NoopSessionRepository;
import online.ipuff.jmqtt.session.persistence.SessionPersistence;
import online.ipuff.jmqtt.store.DupPublishMessageStoreService;
import online.ipuff.jmqtt.store.MessageIdService;
import online.ipuff.jmqtt.store.RetainMessageStoreService;
import online.ipuff.jmqtt.subscribe.ISubscribeStoreService;
import online.ipuff.jmqtt.subscribe.SubscribeStoreService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SUBSCRIBE 路径的 ACL: 逐过滤器判定, 拒绝的过滤器回 0x87/0x80 而其余照常接受
 * (复用既有的「失败码逐项返回」机制), 挂起期间 SUBACK 等全部判定完成。
 */
class SubscribeHandlerAclTest {

    private static final class StubAcl implements IAclService {
        final ConcurrentHashMap<String, CompletableFuture<Boolean>> pending = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, Boolean> decisions = new ConcurrentHashMap<>();
        final AtomicInteger hits = new AtomicInteger();

        @Override
        public CompletableFuture<Boolean> check(String clientId, String username, String peerhost,
                                                String action, String topic) {
            hits.incrementAndGet();
            String key = action + "\n" + topic;
            CompletableFuture<Boolean> gate = pending.get(key);
            if (gate != null) {
                return gate;
            }
            Boolean decision = decisions.get(key);
            return CompletableFuture.completedFuture(decision == null || decision);
        }

        @Override
        public void onClientOffline(String clientId) {
            // 本测试不关注
        }
    }

    private SubscribeStoreService subscribeStoreService;
    private SubscribeHandler handler;
    private StubAcl acl;

    @BeforeEach
    void setUp() {
        BrokerProperties props = TestBrokerProperties.create();
        subscribeStoreService = new SubscribeStoreService();
        SessionStoreService sessionStoreService = new SessionStoreService();
        acl = new StubAcl();
        handler = new SubscribeHandler(
                subscribeStoreService, new RetainMessageStoreService(), new MessageIdService(),
                new DupPublishMessageStoreService(),
                new SessionPersistence(new NoopSessionRepository(), sessionStoreService), acl);
    }
    private EmbeddedChannel clientChannel(boolean v5) {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.attr(ChannelAttributes.CLIENT_ID).set("client-1");
        channel.attr(ChannelAttributes.PROTOCOL_VERSION)
                .set(v5 ? MqttVersion.MQTT_5 : MqttVersion.MQTT_3_1_1);
        return channel;
    }

    private static MqttSubscribeMessage subscribe(int packetId, String... filters) {
        List<MqttTopicSubscription> subs = java.util.Arrays.stream(filters)
                .map(f -> new MqttTopicSubscription(f, MqttQoS.AT_LEAST_ONCE))
                .toList();
        return new MqttSubscribeMessage(new MqttFixedHeader(
                MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader.from(packetId),
                new MqttSubscribePayload(subs));
    }

    private static List<Integer> reasonCodes(EmbeddedChannel channel) {
        MqttSubAckMessage subAck = channel.readOutbound();
        assertNotNull(subAck, "必须回 SUBACK");
        return subAck.payload().reasonCodes();
    }

    @Test
    @DisplayName("默认放行: 授予请求的 QoS")
    void allowedFilterGranted() {
        EmbeddedChannel channel = clientChannel(true);
        handler.processSubscribe(channel, subscribe(1, "sensor/#"));
        assertEquals(List.of(1), reasonCodes(channel));
        assertEquals(1, subscribeStoreService.subscriptionsOf("client-1").size());
    }

    @Test
    @DisplayName("拒绝的过滤器回 0x87, 同报文其他过滤器照常接受")
    void deniedFilterGetsNotAuthorizedOthersGranted() {
        EmbeddedChannel channel = clientChannel(true);
        acl.decisions.put("subscribe\nsecret/#", false);

        handler.processSubscribe(channel, subscribe(1, "secret/#", "sensor/#"));

        assertEquals(List.of(0x87, 1), reasonCodes(channel), "逐项失败码: 拒绝 0x87, 放行授予 qos");
        List<String> stored = subscribeStoreService.subscriptionsOf("client-1").stream()
                .map(online.ipuff.jmqtt.message.SubscribeStore::topicFilter).toList();
        assertFalse(stored.contains("secret/#"), "拒绝的过滤器不得进订阅表");
        assertTrue(stored.contains("sensor/#"));
    }

    @Test
    @DisplayName("v3.1.1 拒绝回 0x80(通用失败码)")
    void deniedFilterV3Gets0x80() {
        EmbeddedChannel channel = clientChannel(false);
        acl.decisions.put("subscribe\nsecret/#", false);

        handler.processSubscribe(channel, subscribe(1, "secret/#"));

        assertEquals(List.of(0x80), reasonCodes(channel));
    }

    @Test
    @DisplayName("判定挂起期间不回 SUBACK, 全部完成后才回")
    void subackWaitsForAllChecks() {
        EmbeddedChannel channel = clientChannel(true);
        CompletableFuture<Boolean> gate = new CompletableFuture<>();
        acl.pending.put("subscribe\nsensor/#", gate);

        handler.processSubscribe(channel, subscribe(1, "sensor/#"));
        assertNull(channel.readOutbound(), "判定未完成不得回 SUBACK");

        gate.complete(true);
        channel.runPendingTasks();
        assertEquals(List.of(1), reasonCodes(channel));
    }

    @Test
    @DisplayName("superuser 连接整体跳过 ACL")
    void superuserSkipsAcl() {
        EmbeddedChannel channel = clientChannel(true);
        channel.attr(ChannelAttributes.SUPERUSER).set(Boolean.TRUE);

        handler.processSubscribe(channel, subscribe(1, "sensor/#"));

        assertEquals(List.of(1), reasonCodes(channel));
        assertEquals(0, acl.hits.get(), "superuser 不得触发 ACL 请求");
    }
}
