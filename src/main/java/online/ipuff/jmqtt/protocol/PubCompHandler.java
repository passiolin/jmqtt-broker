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
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.store.IDupPubRelMessageStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * PUBCOMP 处理: QoS 2 流程收尾, 清除待确认的 PUBREL。
 */
@Component
public class PubCompHandler {

    private static final Logger log = LoggerFactory.getLogger(PubCompHandler.class);

    private final IDupPubRelMessageStoreService dupPubRelMessageStoreService;

    public PubCompHandler(IDupPubRelMessageStoreService dupPubRelMessageStoreService) {
        this.dupPubRelMessageStoreService = dupPubRelMessageStoreService;
    }

    public void processPubComp(Channel channel, MqttMessageIdVariableHeader variableHeader) {
        String clientId = channel.attr(ChannelAttributes.CLIENT_ID).get();
        if (clientId == null) {
            return;
        }
        int messageId = variableHeader.messageId();
        dupPubRelMessageStoreService.remove(clientId, messageId);
        log.trace("PUBCOMP clientId={} messageId={}", clientId, messageId);
    }
}
