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
import online.ipuff.jmqtt.store.IInboundQos2Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * PUBREL 处理: 结束一轮<b>接收方向</b>的 QoS 2 流程。
 *
 * <h2>为什么必须在这里释放报文标识符</h2>
 * 除了回 PUBCOMP, 这里还要把该标识符从「已接收未确认」集合里摘掉。
 * 只记不释放的后果很具体: 同一个标识符将永远无法再用于新一轮流程 ——
 * 客户端后续用它发消息会被当成重复而丢弃, 而且没有任何报错。
 *
 * <p>这比重复投递更糟: <b>重复看得见, 丢弃看不见</b>。所以释放这一步放在回包
 * <b>之前</b> —— 若回包失败, 客户端会重发 PUBREL, 而重发是幂等的;
 * 反过来先回包再释放而释放失败, 那个标识符就永久卡住了。
 */
@Component
public class PubRelHandler {

    private static final Logger log = LoggerFactory.getLogger(PubRelHandler.class);

    private final IInboundQos2Store inboundQos2Store;

    public PubRelHandler(IInboundQos2Store inboundQos2Store) {
        this.inboundQos2Store = inboundQos2Store;
    }

    public void processPubRel(Channel channel, MqttMessageIdVariableHeader variableHeader) {
        String clientId = channel.attr(ChannelAttributes.CLIENT_ID).get();
        int messageId = variableHeader.messageId();
        inboundQos2Store.release(clientId, messageId);
        channel.writeAndFlush(ReplyFactory.pubComp(channel, messageId, ReplyFactory.SUCCESS));
        log.trace("PUBREL clientId={} messageId={}", clientId, messageId);
    }
}
