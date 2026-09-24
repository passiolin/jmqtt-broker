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
package online.ipuff.jmqtt.auth;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import online.ipuff.jmqtt.TestBrokerProperties;
import online.ipuff.jmqtt.admin.AdminProperties;
import online.ipuff.jmqtt.admin.AdminRedisKeys;
import online.ipuff.jmqtt.admin.AdminStatePublisher;
import online.ipuff.jmqtt.cluster.ClusterBus;
import online.ipuff.jmqtt.cluster.InternalSendServer;
import online.ipuff.jmqtt.cluster.LocalClusterBus;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.protocol.ConnectHandler;
import online.ipuff.jmqtt.session.BackpressureMetrics;
import online.ipuff.jmqtt.session.ConnectionRegistry;
import online.ipuff.jmqtt.session.QosMetrics;
import online.ipuff.jmqtt.session.SessionStoreService;
import online.ipuff.jmqtt.session.persistence.NoopSessionRepository;
import online.ipuff.jmqtt.session.persistence.SessionPersistence;
import online.ipuff.jmqtt.store.DupPubRelMessageStoreService;
import online.ipuff.jmqtt.store.DupPublishMessageStoreService;
import online.ipuff.jmqtt.store.InboundQos2Store;
import online.ipuff.jmqtt.store.InflightPersistence;
import online.ipuff.jmqtt.store.InMemoryPendingMessageStore;
import online.ipuff.jmqtt.store.MessageIdService;
import online.ipuff.jmqtt.subscribe.SubscribeStoreService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CONNECT 认证挂起语义: 认证改为异步(HTTP 鉴权)后, CONNACK 必须等认证结果,
 * 挂起期间连接断开必须安全放弃, superuser 标志要落到 Channel 上。
 *
 * <p>用 EmbeddedChannel + 全真实内存组件(与 InternalSendServerTest 同一套路),
 * IAuthService 用受控 future 代替 —— 测的是 ConnectHandler 的挂起/续接逻辑,
 * 不是认证本身。
 */
class ConnectHandlerAuthTest {

    private CompletableFuture<AuthResult> authGate;
    private ConnectHandler handler;
    private ConnectionRegistry connectionRegistry;

    @BeforeEach
    void setUp() {
        BrokerProperties props = TestBrokerProperties.create(true, "jmqtt", "jmqtt");
        authGate = new CompletableFuture<>();

        SessionStoreService sessionStoreService = new SessionStoreService();
        SubscribeStoreService subscribeStoreService = new SubscribeStoreService();
        DupPublishMessageStoreService dupPublishStore = new DupPublishMessageStoreService();
        DupPubRelMessageStoreService dupPubRelStore = new DupPubRelMessageStoreService();
        connectionRegistry = new ConnectionRegistry();
        SessionPersistence sessionPersistence =
                new SessionPersistence(new NoopSessionRepository(), sessionStoreService);
        ClusterBus clusterBus = new LocalClusterBus(props);
        BackpressureMetrics backpressureMetrics = new BackpressureMetrics();
        InflightPersistence inflightPersistence =
                new InflightPersistence(props, emptyProvider(), Runnable::run);
        AdminStatePublisher adminStatePublisher = new AdminStatePublisher(
                new AdminProperties(false, 5000, 30, 200000, 5000, 16,
                        5000, 200, 50, 500, 60000, 3600),
                props, new AdminRedisKeys(props), emptyProvider(), connectionRegistry,
                sessionStoreService, subscribeStoreService, new InMemoryPendingMessageStore(),
                backpressureMetrics, new QosMetrics(), clusterBus, emptyProvider(), emptyProvider());
        InternalSendServer internalSendServer = new InternalSendServer(
                subscribeStoreService, connectionRegistry, new MessageIdService(),
                dupPublishStore, sessionStoreService, new InMemoryPendingMessageStore(),
                new online.ipuff.jmqtt.store.OfflineEnqueueBuffer(new InMemoryPendingMessageStore(),
                        props.maxOfflineQueueLen(),
                        online.ipuff.jmqtt.store.OfflineEnqueueBuffer.DEFAULT_MAX_BUFFERED, true),
                props, backpressureMetrics, new QosMetrics(),
                new online.ipuff.jmqtt.admin.TopicCaptureService(
                        online.ipuff.jmqtt.TestObjectProviders.empty(),
                        new online.ipuff.jmqtt.admin.AdminRedisKeys(props), props));

        handler = new ConnectHandler(props,
                (clientId, username, password, peerhost) -> authGate,
                sessionStoreService, subscribeStoreService, dupPublishStore, dupPubRelStore,
                connectionRegistry, sessionPersistence, clusterBus, internalSendServer,
                backpressureMetrics, inflightPersistence, adminStatePublisher,
                new InboundQos2Store(props), online.ipuff.jmqtt.TestNodeMetrics.create());
    }

    private static MqttConnectMessage connect(boolean v5, String clientId,
                                              String username, String password) {
        byte[] passwordBytes = password == null ? null : password.getBytes(StandardCharsets.UTF_8);
        MqttConnectPayload payload = v5
                ? new MqttConnectPayload(clientId, MqttProperties.NO_PROPERTIES,
                        null, null, username, passwordBytes)
                : new MqttConnectPayload(clientId, null, (byte[]) null, username, passwordBytes);
        MqttConnectVariableHeader header = new MqttConnectVariableHeader(
                "MQTT", v5 ? 5 : 4, username != null, password != null,
                false, 0, false, true, 30,
                v5 ? MqttProperties.NO_PROPERTIES : null);
        return new MqttConnectMessage(new MqttFixedHeader(
                MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0), header, payload);
    }

    @Test
    @DisplayName("认证未完成时不回 CONNACK, 完成后回 CONNECTION_ACCEPTED")
    void connackWaitsForAuthResult() {
        EmbeddedChannel channel = new EmbeddedChannel();
        handler.processConnect(channel, connect(true, "client-1", "jmqtt", "jmqtt"));

        assertNull(channel.readOutbound(), "认证挂起期间不得回 CONNACK");
        assertEquals(Boolean.TRUE, channel.attr(ChannelAttributes.AUTH_PENDING).get(),
                "挂起标志必须可见(入口据此忽略后续报文)");

        authGate.complete(AuthResult.ALLOW);
        channel.runPendingTasks();

        MqttConnAckMessage connAck = channel.readOutbound();
        assertNotNull(connAck, "认证通过后必须回 CONNACK");
        assertEquals(MqttConnectReturnCode.CONNECTION_ACCEPTED, connAck.variableHeader().connectReturnCode());
        assertNull(channel.attr(ChannelAttributes.AUTH_PENDING).get(), "完成后清除挂起标志");
        assertEquals("client-1", channel.attr(ChannelAttributes.CLIENT_ID).get());
        assertTrue(connectionRegistry.isOnline("client-1"), "连接必须注册");
    }

    @Test
    @DisplayName("认证拒绝回 0x86 并关闭连接(v5)")
    void authDenyRepliesBadCredentials() {
        EmbeddedChannel channel = new EmbeddedChannel();
        handler.processConnect(channel, connect(true, "client-1", "jmqtt", "wrong"));

        authGate.complete(AuthResult.DENY);
        channel.runPendingTasks();

        MqttConnAckMessage connAck = channel.readOutbound();
        assertNotNull(connAck);
        assertEquals(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD,
                connAck.variableHeader().connectReturnCode());
        assertFalse(channel.isActive(), "拒绝后必须关闭连接");
        assertNull(channel.attr(ChannelAttributes.CLIENT_ID).get(), "不得注册 clientId");
    }

    @Test
    @DisplayName("v3.1.1 认证拒绝回 code 4(用户名或密码错误)")
    void authDenyV3RepliesCode4() {
        EmbeddedChannel channel = new EmbeddedChannel();
        handler.processConnect(channel, connect(false, "client-1", "jmqtt", "wrong"));

        authGate.complete(AuthResult.DENY);
        channel.runPendingTasks();

        MqttConnAckMessage connAck = channel.readOutbound();
        assertNotNull(connAck);
        assertEquals(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD,
                connAck.variableHeader().connectReturnCode());
    }

    @Test
    @DisplayName("superuser=true 落到 Channel 属性, 供 ACL 跳过判定")
    void superuserLandsOnChannel() {
        EmbeddedChannel channel = new EmbeddedChannel();
        handler.processConnect(channel, connect(true, "client-1", "jmqtt", "jmqtt"));

        authGate.complete(new AuthResult(true, true));
        channel.runPendingTasks();

        assertNotNull(channel.readOutbound());
        assertEquals(Boolean.TRUE, channel.attr(ChannelAttributes.SUPERUSER).get());
    }

    @Test
    @DisplayName("挂起期间连接断开: 认证结果晚到也不得写 CONNACK 或注册")
    void channelClosedDuringPendingAuthIsAbandoned() {
        EmbeddedChannel channel = new EmbeddedChannel();
        handler.processConnect(channel, connect(true, "client-1", "jmqtt", "jmqtt"));

        channel.close();
        authGate.complete(AuthResult.ALLOW);
        channel.runPendingTasks();

        assertNull(channel.readOutbound(), "连接已断, 晚到的认证结果不得回包");
        assertFalse(connectionRegistry.isOnline("client-1"), "不得注册");
    }

    private static <T> ObjectProvider<T> emptyProvider() {
        return new ObjectProvider<>() {
            @Override
            public T getObject() {
                throw new NoSuchElementException();
            }

            @Override
            public T getObject(Object... args) {
                throw new NoSuchElementException();
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
