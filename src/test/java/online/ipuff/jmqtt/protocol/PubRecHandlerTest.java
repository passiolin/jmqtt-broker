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
package online.ipuff.jmqtt.protocol;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import online.ipuff.jmqtt.cluster.InternalSendServer;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.message.DupPublishMessageStore;
import online.ipuff.jmqtt.store.DupPubRelMessageStoreService;
import online.ipuff.jmqtt.store.DupPublishMessageStoreService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * PUBREC 处理: v5 拒收原因码与未知报文标识符。
 */
class PubRecHandlerTest {

    private static final String CLIENT = "device-1";

    private DupPublishMessageStoreService dupPublish;
    private DupPubRelMessageStoreService dupPubRel;
    private InternalSendServer sendServer;
    private PubRecHandler handler;
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        dupPublish = new DupPublishMessageStoreService();
        dupPubRel = new DupPubRelMessageStoreService();
        sendServer = mock(InternalSendServer.class);
        handler = new PubRecHandler(dupPublish, dupPubRel, sendServer);
        channel = new EmbeddedChannel();
        channel.attr(ChannelAttributes.CLIENT_ID).set(CLIENT);
    }

    @Test
    @DisplayName("v5 客户端拒收(reason>=0x80): 释放标识符, 不发 PUBREL, 不留状态")
    void v5RejectionReleasesWithoutPubRel() {
        dupPublish.put(new DupPublishMessageStore(CLIENT, "t/1", 2, 7, new byte[]{1}));

        handler.processPubRec(channel,
                new MqttPubReplyMessageVariableHeader(7, (byte) 0x87, MqttProperties.NO_PROPERTIES));

        assertEquals(0, dupPublish.get(CLIENT).size(), "在途要释放");
        assertEquals(0, dupPubRel.get(CLIENT).size(), "不能建幽灵 PUBREL 状态");
        verify(sendServer).releaseSendWindow(any(), anyInt());
    }

    @Test
    @DisplayName("未知报文标识符的 PUBREC 被忽略 —— 不建幽灵状态")
    void unknownMessageIdIsIgnored() {
        handler.processPubRec(channel, MqttMessageIdVariableHeader.from(42));

        assertEquals(0, dupPubRel.get(CLIENT).size());
        verify(sendServer, never()).releaseSendWindow(any(), anyInt());
    }

    @Test
    @DisplayName("正常 PUBREC: 记 PUBREL 状态并释放窗口")
    void normalPubRecRecordsPubRelState() {
        dupPublish.put(new DupPublishMessageStore(CLIENT, "t/1", 2, 7, new byte[]{1}));

        handler.processPubRec(channel, MqttMessageIdVariableHeader.from(7));

        assertEquals(0, dupPublish.get(CLIENT).size());
        assertEquals(1, dupPubRel.get(CLIENT).size(), "待确认的 PUBREL 要记下");
        verify(sendServer).releaseSendWindow(any(), anyInt());
    }
}
