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
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.session.ISessionStoreService;
import online.ipuff.jmqtt.session.SessionStore;
import online.ipuff.jmqtt.session.persistence.SessionPersistence;
import online.ipuff.jmqtt.store.IDupPubRelMessageStoreService;
import online.ipuff.jmqtt.store.IDupPublishMessageStoreService;
import online.ipuff.jmqtt.store.IPendingMessageStore;
import online.ipuff.jmqtt.subscribe.ISubscribeStoreService;
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
 * 「需不需要保留」用 {@link SessionStore#isPersistent()}, 不按版本分支 —— v5 下
 * {@code cleanStart} 与「断开后留不留」不是同一件事。
 *
 * <p><b>正常断开不触发遗嘱</b>: 这里先把会话里的 will 清空, 再关闭连接,
 * 使随后的 {@code channelInactive} 判定为非遗嘱场景。
 * 遗嘱只在<b>异常断连</b>(心跳超时、连接被重置)时触发。
 * 例外是 v5 的原因码 {@code 0x04 Disconnect with Will Message} ——
 * 客户端明确要求「断开并发布遗嘱」, 这时的正确行为恰好相反: <b>保留 will</b>。
 *
 * <h2>v5 的会话保留时长可以在断开时改写</h2>
 * CONNECT 里给的 Session Expiry Interval 不是最终值 —— v5 允许客户端在 DISCONNECT
 * 时用一个同名属性把它改掉（常见用法: 连的时候给一个较大的值以便断线重连,
 * 主动下线时改成 0, 让服务端立刻清掉会话）。
 *
 * <p>但规范限定只能<b>减小</b>（[MQTT-3.14.2-2]: 连接时是 0 就不允许在断开时调大）——
 * 调大意味着客户端想让服务端多留一段自己都没承诺过的时间。这里按协议错误断开。
 */
@Component
public class DisconnectHandler {

    private static final Logger log = LoggerFactory.getLogger(DisconnectHandler.class);

    /** v5: Disconnect with Will Message —— 断开并发布遗嘱 */
    private static final int REASON_DISCONNECT_WITH_WILL = 0x04;

    private final ISessionStoreService sessionStoreService;
    private final ISubscribeStoreService subscribeStoreService;
    private final IDupPublishMessageStoreService dupPublishMessageStoreService;
    private final IDupPubRelMessageStoreService dupPubRelMessageStoreService;
    private final SessionPersistence sessionPersistence;
    private final IPendingMessageStore pendingMessageStore;

    public DisconnectHandler(ISessionStoreService sessionStoreService,
                             ISubscribeStoreService subscribeStoreService,
                             IDupPublishMessageStoreService dupPublishMessageStoreService,
                             IDupPubRelMessageStoreService dupPubRelMessageStoreService,
                             SessionPersistence sessionPersistence,
                             IPendingMessageStore pendingMessageStore) {
        this.sessionStoreService = sessionStoreService;
        this.subscribeStoreService = subscribeStoreService;
        this.dupPublishMessageStoreService = dupPublishMessageStoreService;
        this.dupPubRelMessageStoreService = dupPubRelMessageStoreService;
        this.sessionPersistence = sessionPersistence;
        this.pendingMessageStore = pendingMessageStore;
    }

    public void processDisconnect(Channel channel, MqttMessage msg) {
        String clientId = channel.attr(ChannelAttributes.CLIENT_ID).get();
        if (clientId == null) {
            channel.close();
            return;
        }

        int reasonCode = reasonCode(msg);
        boolean publishWill = reasonCode == REASON_DISCONNECT_WITH_WILL;

        SessionStore session = sessionStoreService.get(clientId);

        // v5: 断开时可改写会话保留时长(只能减小)
        Long requestedExpiry = requestedSessionExpiry(msg);
        if (requestedExpiry != null && session != null) {
            if (session.getExpireSeconds() == 0 && requestedExpiry > 0) {
                log.warn("v5 DISCONNECT 试图把会话保留时长从 0 调大, clientId={} 请求={}s, 按协议错误处理",
                        clientId, requestedExpiry);
                ReplyFactory.closeWithReason(channel, ReplyFactory.PROTOCOL_ERROR);
                return;
            }
            long previous = session.getExpireSeconds();
            long capped = requestedExpiry < 0
                    ? previous
                    : Math.min(requestedExpiry, previous);
            session.setExpireSeconds(capped);
            log.debug("v5 DISCONNECT 改写会话保留时长: clientId={} {}s -> {}s",
                    clientId, previous, capped);
        }

        boolean persistent = session != null && session.isPersistent();

        if (!persistent) {
            subscribeStoreService.removeForClient(clientId);
            dupPublishMessageStoreService.removeByClient(clientId);
            dupPubRelMessageStoreService.removeByClient(clientId);
            sessionStoreService.remove(clientId);
            // 不保留的会话没有持久化价值, 连同持久层一起清掉
            pendingMessageStore.removeAll(clientId);
            sessionPersistence.sessionDestroyed(clientId);
        } else {
            // 保留会话与订阅。正常情况下清掉遗嘱(正常断开不该发遗嘱),
            // 除非客户端明确要求「断开并发布遗嘱」
            if (!publishWill) {
                session.setWillMessage(null);
            }
            sessionPersistence.saveSession(session);
        }

        log.debug("DISCONNECT clientId={} reasonCode=0x{} 保留会话={}",
                clientId, Integer.toHexString(reasonCode), persistent);
        channel.close();
    }

    /**
     * DISCONNECT 的原因码。v3.1.1 的 DISCONNECT 没有可变头, 视作 0x00。
     */
    private int reasonCode(MqttMessage msg) {
        if (msg.variableHeader() instanceof MqttReasonCodeAndPropertiesVariableHeader v5Header) {
            // Netty 用有符号 byte 承载, 必须按无符号解读
            return v5Header.reasonCode() & 0xFF;
        }
        return ReplyFactory.SUCCESS;
    }

    /**
     * DISCONNECT 里的 Session Expiry Interval 改写请求; 未声明时返回 null。
     */
    private Long requestedSessionExpiry(MqttMessage msg) {
        if (!(msg.variableHeader() instanceof MqttReasonCodeAndPropertiesVariableHeader v5Header)) {
            return null;
        }
        MqttProperties properties = v5Header.properties();
        if (properties == null || properties == MqttProperties.NO_PROPERTIES) {
            return null;
        }
        MqttProperties.MqttProperty property = properties.getProperty(
                MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value());
        if (property instanceof MqttProperties.IntegerProperty integer) {
            // 0xFFFFFFFF 会以 -1 的形式读出来, 表示「永不过期」→ 取服务端上限
            return integer.value() < 0 ? Long.MAX_VALUE : (long) integer.value();
        }
        return null;
    }
}
