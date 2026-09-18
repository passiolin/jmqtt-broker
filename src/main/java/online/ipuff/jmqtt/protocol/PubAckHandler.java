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

import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import online.ipuff.jmqtt.cluster.InternalSendServer;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.store.IDupPublishMessageStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * PUBACK 处理: 客户端确认已收到 QoS 1 的 PUBLISH, 清除重发记录。
 */
@Component
public class PubAckHandler {

    private static final Logger log = LoggerFactory.getLogger(PubAckHandler.class);

    private final IDupPublishMessageStoreService dupPublishMessageStoreService;
    private final InternalSendServer internalSendServer;

    public PubAckHandler(IDupPublishMessageStoreService dupPublishMessageStoreService,
                         InternalSendServer internalSendServer) {
        this.dupPublishMessageStoreService = dupPublishMessageStoreService;
        this.internalSendServer = internalSendServer;
    }

    public void processPubAck(Channel channel, MqttMessageIdVariableHeader variableHeader) {
        String clientId = channel.attr(ChannelAttributes.CLIENT_ID).get();
        if (clientId == null) {
            return;
        }
        int messageId = variableHeader.messageId();
        dupPublishMessageStoreService.remove(clientId, messageId);
        // 客户端确实收到了这条 PUBLISH: 释放发送窗口, 并把排队中的消息继续发出去。
        // 这一步是 MQTT 流控的闭环 —— 没有它, 窗口一旦用满就再也不会往前推进。
        internalSendServer.releaseSendWindow(channel, messageId);
        log.trace("PUBACK clientId={} messageId={}", clientId, messageId);
    }
}
