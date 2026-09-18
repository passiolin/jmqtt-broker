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
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.session.ISessionStoreService;
import online.ipuff.jmqtt.session.SessionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * PINGREQ 处理: 刷新会话活跃时间并回 PINGRESP。
 *
 * <p>纯内存操作 —— 只更新会话的活跃时间戳。
 *
 * <p>这里刻意<b>不</b>再去判断一次「会话归属节点是否等于本节点」: 连接只可能属于本节点,
 * 在自己的不变量上再做一次校验只会增加分支。真正的归属判断发生在 CONNECT ——
 * 那才是它唯一有意义的位置, 而且是靠一次原子操作完成的。
 */
@Component
public class PingReqHandler {

    private static final Logger log = LoggerFactory.getLogger(PingReqHandler.class);

    private final ISessionStoreService sessionStoreService;

    public PingReqHandler(ISessionStoreService sessionStoreService) {
        this.sessionStoreService = sessionStoreService;
    }

    public void processPingReq(Channel channel, MqttMessage msg) {
        String clientId = channel.attr(ChannelAttributes.CLIENT_ID).get();
        if (clientId == null) {
            channel.close();
            return;
        }

        SessionStore session = sessionStoreService.get(clientId);
        if (session != null) {
            session.touch();
        }

        MqttMessage pingResp = MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0),
                null, null);
        channel.writeAndFlush(pingResp);
        log.trace("PINGREQ clientId={}", clientId);
    }
}
