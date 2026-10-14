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
package online.ipuff.jmqtt.handler;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import online.ipuff.jmqtt.admin.AdminStatePublisher;
import online.ipuff.jmqtt.authz.IAclService;
import online.ipuff.jmqtt.cluster.ClusterBus;
import online.ipuff.jmqtt.cluster.InternalCommunication;
import online.ipuff.jmqtt.cluster.InternalSendServer;
import online.ipuff.jmqtt.metrics.NodeMetricsService;
import online.ipuff.jmqtt.protocol.ProtocolProcessor;
import online.ipuff.jmqtt.protocol.PublishHandler;
import online.ipuff.jmqtt.session.ConnectionRegistry;
import online.ipuff.jmqtt.session.ISessionStoreService;
import online.ipuff.jmqtt.session.SessionLifecycle;
import online.ipuff.jmqtt.store.IRetainMessageStoreService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 首包必须是 CONNECT —— 否则裸 TCP 可以绕过认证直接注入消息。
 */
class MqttBrokerHandlerFirstPacketTest {

    private ProtocolProcessor protocolProcessor;
    private PublishHandler publishHandler;
    private MqttBrokerHandler handler;
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        protocolProcessor = mock(ProtocolProcessor.class);
        publishHandler = mock(PublishHandler.class);
        when(protocolProcessor.publish()).thenReturn(publishHandler);
        handler = new MqttBrokerHandler(protocolProcessor, mock(ConnectionRegistry.class),
                mock(ISessionStoreService.class), mock(InternalCommunication.class),
                mock(InternalSendServer.class), mock(IRetainMessageStoreService.class),
                mock(AdminStatePublisher.class), mock(IAclService.class),
                mock(ClusterBus.class), mock(NodeMetricsService.class),
                mock(SessionLifecycle.class));
        channel = new EmbeddedChannel(handler);
    }

    @Test
    @DisplayName("CONNECT 之前的 PUBLISH 直接断开, 不进入任何处理逻辑")
    void publishBeforeConnectIsRejected() {
        channel.writeInbound(publish());

        verify(publishHandler, never()).processPublish(any(), any());
        assertFalse(channel.isOpen(), "未握手的业务报文要断开连接");
    }

    @Test
    @DisplayName("握手后的 PUBLISH 正常分发")
    void publishAfterConnectIsProcessed() {
        channel.attr(ChannelAttributes.CONNECT_SEEN).set(Boolean.TRUE);

        channel.writeInbound(publish());

        verify(publishHandler).processPublish(any(), any());
        assertTrue(channel.isOpen());
    }

    private static MqttPublishMessage publish() {
        return (MqttPublishMessage) MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttPublishVariableHeader("t/1", 0),
                Unpooled.wrappedBuffer(new byte[]{1}));
    }
}
