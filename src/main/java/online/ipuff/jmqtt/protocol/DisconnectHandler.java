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
import io.netty.handler.codec.mqtt.MqttMessage;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.session.ISessionStoreService;
import online.ipuff.jmqtt.session.SessionLifecycle;
import online.ipuff.jmqtt.session.SessionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * DISCONNECT 处理。
 *
 * <ul>
 *   <li>会话需要保留 → 保留会话与订阅, 等待客户端重连</li>
 *   <li>会话不需要保留 → 会话及相关状态全部清除</li>
 * </ul>
 * 「需不需要保留」用 {@link SessionStore#isPersistent()}。
 *
 * <p><b>正常断开不触发遗嘱</b>: 这里先把会话里的 will 清空, 再关闭连接,
 * 使随后的 {@code channelInactive} 判定为非遗嘱场景。
 * 遗嘱只在<b>异常断连</b>(心跳超时、连接被重置)时触发 —— 3.1.1 没有
 * 「断开并发布遗嘱」这类原因码, 主动 DISCONNECT 一律视为正常下线。
 */
@Component
public class DisconnectHandler {

    private static final Logger log = LoggerFactory.getLogger(DisconnectHandler.class);

    private final ISessionStoreService sessionStoreService;
    private final SessionLifecycle sessionLifecycle;
    private final online.ipuff.jmqtt.session.ConnectionRegistry connectionRegistry;

    public DisconnectHandler(ISessionStoreService sessionStoreService,
                             SessionLifecycle sessionLifecycle,
                             online.ipuff.jmqtt.session.ConnectionRegistry connectionRegistry) {
        this.sessionStoreService = sessionStoreService;
        this.sessionLifecycle = sessionLifecycle;
        this.connectionRegistry = connectionRegistry;
    }

    public void processDisconnect(Channel channel, MqttMessage msg) {
        String clientId = channel.attr(ChannelAttributes.CLIENT_ID).get();
        if (clientId == null) {
            channel.close();
            return;
        }
        // 归属校验: 会话若已被新连接接管(本连接是即将关闭的旧连接), 一切会话处置
        // 都归新连接 —— 这里动手会把新连接刚建立的会话与积压一起销毁
        if (connectionRegistry.get(clientId) != channel) {
            log.debug("DISCONNECT 来自已被接管的旧连接, 只关连接不动会话: clientId={}", clientId);
            channel.close();
            return;
        }
        // 标记优雅离线: channelInactive 据此把下线原因写成 disconnect 而不是 closed
        channel.attr(ChannelAttributes.GRACEFUL_DISCONNECT).set(Boolean.TRUE);

        SessionStore session = sessionStoreService.get(clientId);
        boolean persistent = session != null && session.isPersistent();

        if (!persistent) {
            // 不保留的会话没有持久化价值, 连同内存与持久层一起清掉
            sessionLifecycle.destroy(clientId);
        } else {
            // 保留会话与订阅。正常断开不该发遗嘱, 先把 will 清掉再落库
            session.setWillMessage(null);
            // 记录关闭时刻(过期从现在起算)并落库
            sessionLifecycle.connectionClosed(clientId);
        }

        log.debug("DISCONNECT clientId={} 保留会话={}", clientId, persistent);
        channel.close();
    }

}
