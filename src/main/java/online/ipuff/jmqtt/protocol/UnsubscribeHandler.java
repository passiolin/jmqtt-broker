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

import java.util.ArrayList;
import java.util.List;

/**
 * UNSUBSCRIBE 处理。
 *
 * <h2>UNSUBACK 在两个版本下形态相反, 这是最容易漏的一处</h2>
 * <ul>
 *   <li><b>v3.1.1</b>: UNSUBACK <b>必须没有 payload</b>。带上 payload 属于协议错误,
 *       严格的客户端会直接断开。</li>
 *   <li><b>v5</b>: UNSUBACK <b>必须有 payload</b>, 每个过滤器一个原因码,
 *       顺序与请求一致。漏掉会让客户端解包错位。</li>
 * </ul>
 * 这里把形态交给 {@link ReplyFactory#unsubAck} 决定, 本类只负责算原因码。
 *
 * <h2>v5 的原因码不是「成功/失败」二值</h2>
 * 取消一个本来就不存在的订阅, 规范上要用 {@code 0x11 No subscription existed}
 * 而不是成功 —— 客户端据此可以区分「确实退掉了」与「本来就没有」。
 * 所以 {@code subscribeStoreService.remove} 的返回值在这里有了实际用途。
 */
@Component
public class UnsubscribeHandler {

    private static final Logger log = LoggerFactory.getLogger(UnsubscribeHandler.class);

    /** v5: 该过滤器本来就没有订阅 */
    private static final short NO_SUBSCRIPTION_EXISTED = 0x11;

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

        List<Short> reasonCodes = new ArrayList<>(topicFilters.size());
        for (String topicFilter : topicFilters) {
            if (!MqttTopic.isValidFilter(topicFilter)) {
                reasonCodes.add((short) ReplyFactory.TOPIC_FILTER_INVALID);
                continue;
            }
            boolean removed = subscribeStoreService.remove(clientId, topicFilter);
            sessionPersistence.subscriptionRemoved(clientId, topicFilter);
            reasonCodes.add(removed ? (short) ReplyFactory.SUCCESS : NO_SUBSCRIPTION_EXISTED);
        }

        channel.writeAndFlush(ReplyFactory.unsubAck(channel, msg.variableHeader().messageId(), reasonCodes));
        log.debug("UNSUBSCRIBE clientId={} topicFilters={} 结果={}", clientId, topicFilters, reasonCodes);
    }
}
