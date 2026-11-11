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
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.router.MqttTopic;
import online.ipuff.jmqtt.session.persistence.SessionPersistence;
import online.ipuff.jmqtt.subscribe.ISubscribeStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * UNSUBSCRIBE 处理。
 *
 * <h2>UNSUBACK 的形态</h2>
 * 3.1.1 的 UNSUBACK <b>必须没有 payload</b>, 只带报文标识符。
 * 带上 payload 属于协议错误, 严格的客户端会直接断开。
 * 逐项取消结果在 3.1.1 的报文里没有位置, 这里只记日志。
 */
@Component
public class UnsubscribeHandler {

    private static final Logger log = LoggerFactory.getLogger(UnsubscribeHandler.class);

    private final ISubscribeStoreService subscribeStoreService;
    private final SessionPersistence sessionPersistence;

    public UnsubscribeHandler(ISubscribeStoreService subscribeStoreService,
                              SessionPersistence sessionPersistence) {
        this.subscribeStoreService = subscribeStoreService;
        this.sessionPersistence = sessionPersistence;
    }

    public void processUnsubscribe(Channel channel, MqttUnsubscribeMessage msg) {
        String clientId = channel.attr(ChannelAttributes.CLIENT_ID).get();
        List<String> topicFilters = msg.payload().topics();
        if (clientId == null || topicFilters == null) {
            channel.close();
            return;
        }

        // 3.1.1 的 UNSUBACK 不带逐项结果 —— 只能带报文标识符。
        // 无效过滤器仍然逐条剔除, 但结果只进日志, 不进报文。
        int removedCount = 0;
        for (String topicFilter : topicFilters) {
            if (!MqttTopic.isValidFilter(topicFilter)) {
                continue;
            }
            if (subscribeStoreService.remove(clientId, topicFilter)) {
                removedCount++;
            }
            sessionPersistence.subscriptionRemoved(clientId, topicFilter);
        }

        channel.writeAndFlush(ReplyFactory.unsubAck(msg.variableHeader().messageId()));
        log.debug("UNSUBSCRIBE clientId={} topicFilters={} 已移除={}", clientId, topicFilters, removedCount);
    }
}
