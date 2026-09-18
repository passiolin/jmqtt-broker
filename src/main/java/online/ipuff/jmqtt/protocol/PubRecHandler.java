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
import online.ipuff.jmqtt.message.DupPubRelMessageStore;
import online.ipuff.jmqtt.store.IDupPubRelMessageStoreService;
import online.ipuff.jmqtt.store.IDupPublishMessageStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * PUBREC 处理: QoS 2 流程第二步。
 *
 * <p>客户端确认收到 PUBLISH 后, 服务端不再需要重发 PUBLISH,
 * 但要记录一条待确认的 PUBREL(收到 PUBCOMP 才清除), 并立即下发 PUBREL。
 */
@Component
public class PubRecHandler {

    private static final Logger log = LoggerFactory.getLogger(PubRecHandler.class);

    private final IDupPublishMessageStoreService dupPublishMessageStoreService;
    private final IDupPubRelMessageStoreService dupPubRelMessageStoreService;
    private final InternalSendServer internalSendServer;

    public PubRecHandler(IDupPublishMessageStoreService dupPublishMessageStoreService,
                         IDupPubRelMessageStoreService dupPubRelMessageStoreService,
                         InternalSendServer internalSendServer) {
        this.dupPublishMessageStoreService = dupPublishMessageStoreService;
        this.dupPubRelMessageStoreService = dupPubRelMessageStoreService;
        this.internalSendServer = internalSendServer;
    }

    public void processPubRec(Channel channel, MqttMessageIdVariableHeader variableHeader) {
        String clientId = channel.attr(ChannelAttributes.CLIENT_ID).get();
        if (clientId == null) {
            return;
        }
        int messageId = variableHeader.messageId();

        dupPublishMessageStoreService.remove(clientId, messageId);
        dupPubRelMessageStoreService.put(new DupPubRelMessageStore(clientId, messageId));

        // QoS 2 的流控在 PUBREC 就闭环 —— 此刻客户端已确认收到 PUBLISH,
        // 之后的 PUBREL/PUBCOMP 走的是另一个流程, 不再占用发送窗口。
        internalSendServer.releaseSendWindow(channel, messageId);

        // 回包形态按连接协商的版本决定(v5 要带原因码 + properties)
        channel.writeAndFlush(ReplyFactory.pubRel(channel, messageId, ReplyFactory.SUCCESS));
        log.trace("PUBREC clientId={} messageId={}", clientId, messageId);
    }
}
