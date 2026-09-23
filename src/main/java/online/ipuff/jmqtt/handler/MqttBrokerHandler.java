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
package online.ipuff.jmqtt.handler;

import io.netty.channel.Channel;

import java.util.List;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttIdentifierRejectedException;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnacceptableProtocolVersionException;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import online.ipuff.jmqtt.admin.AdminStatePublisher;
import online.ipuff.jmqtt.cluster.InternalCommunication;
import online.ipuff.jmqtt.cluster.InternalMessage;
import online.ipuff.jmqtt.cluster.InternalSendServer;
import online.ipuff.jmqtt.message.RetainMessageStore;
import online.ipuff.jmqtt.message.WillMessage;
import online.ipuff.jmqtt.protocol.ProtocolProcessor;
import online.ipuff.jmqtt.session.ConnectionRegistry;
import online.ipuff.jmqtt.util.Payloads;
import online.ipuff.jmqtt.session.ISessionStoreService;
import online.ipuff.jmqtt.session.SessionStore;
import online.ipuff.jmqtt.store.IRetainMessageStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * MQTT 报文入口处理器。
 *
 * <ol>
 *   <li>解码失败时按异常类型回对应的 CONNACK 拒绝码</li>
 *   <li>按报文类型分发到 {@link ProtocolProcessor}</li>
 *   <li>空闲超时判定</li>
 *   <li>连接上下线时维护 {@link ConnectionRegistry}</li>
 *   <li>异常断连时发布遗嘱消息</li>
 * </ol>
 *
 * <p>标了 {@link ChannelHandler.Sharable}: 无状态, 所有连接共用一个实例。
 */
@Component
@ChannelHandler.Sharable
public class MqttBrokerHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private static final Logger log = LoggerFactory.getLogger(MqttBrokerHandler.class);

    private final ProtocolProcessor protocolProcessor;
    private final ConnectionRegistry connectionRegistry;
    private final ISessionStoreService sessionStoreService;
    private final InternalCommunication internalCommunication;
    private final InternalSendServer internalSendServer;
    private final IRetainMessageStoreService retainMessageStoreService;
    private final AdminStatePublisher adminStatePublisher;
    private final online.ipuff.jmqtt.authz.IAclService aclService;
    private final online.ipuff.jmqtt.cluster.ClusterBus clusterBus;
    private final online.ipuff.jmqtt.metrics.NodeMetricsService nodeMetricsService;

    public MqttBrokerHandler(ProtocolProcessor protocolProcessor,
                             ConnectionRegistry connectionRegistry,
                             ISessionStoreService sessionStoreService,
                             InternalCommunication internalCommunication,
                             InternalSendServer internalSendServer,
                             IRetainMessageStoreService retainMessageStoreService,
                             AdminStatePublisher adminStatePublisher,
                             online.ipuff.jmqtt.authz.IAclService aclService,
                             online.ipuff.jmqtt.cluster.ClusterBus clusterBus,
                             online.ipuff.jmqtt.metrics.NodeMetricsService nodeMetricsService) {
        this.protocolProcessor = protocolProcessor;
        this.connectionRegistry = connectionRegistry;
        this.sessionStoreService = sessionStoreService;
        this.internalCommunication = internalCommunication;
        this.internalSendServer = internalSendServer;
        this.retainMessageStoreService = retainMessageStoreService;
        this.adminStatePublisher = adminStatePublisher;
        this.aclService = aclService;
        this.clusterBus = clusterBus;
        this.nodeMetricsService = nodeMetricsService;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        if (log.isDebugEnabled()) {
            log.debug("连接建立: {}", ctx.channel().remoteAddress());
        }
        connectionRegistry.addChannel(ctx.channel());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) {
        Channel channel = ctx.channel();

        if (msg.decoderResult().isFailure()) {
            handleDecodeFailure(channel, msg);
            return;
        }

        // 入站报文日志: 所有客户端报文必经的分发点, 一处打印保证格式统一。
        // 只有 DEBUG 级才构造描述字符串(preview 截断, 见 Payloads)
        if (log.isDebugEnabled()) {
            log.debug("{}", describeInbound(channel, msg));
        }

        // CONNECT 认证挂起中(HTTP 鉴权等外部响应): 忽略一切后续报文。
        // 规范要求客户端必须等 CONNACK 才能继续, 此时到来的都是异常流量;
        // 且连接状态尚未初始化(clientId/发送缓冲都还没写入), 处理它们必然读到空值。
        if (Boolean.TRUE.equals(channel.attr(ChannelAttributes.AUTH_PENDING).get())) {
            log.debug("认证挂起期间收到报文, 忽略: {}", msg.fixedHeader().messageType());
            return;
        }

        switch (msg.fixedHeader().messageType()) {
            case CONNECT -> protocolProcessor.connect()
                    .processConnect(channel, (MqttConnectMessage) msg);
            case PUBLISH -> protocolProcessor.publish()
                    .processPublish(channel, (MqttPublishMessage) msg);
            case SUBSCRIBE -> protocolProcessor.subscribe()
                    .processSubscribe(channel, (MqttSubscribeMessage) msg);
            case UNSUBSCRIBE -> protocolProcessor.unsubscribe()
                    .processUnsubscribe(channel, (MqttUnsubscribeMessage) msg);
            case PUBACK -> protocolProcessor.pubAck()
                    .processPubAck(channel, (MqttMessageIdVariableHeader) msg.variableHeader());
            case PUBREC -> protocolProcessor.pubRec()
                    .processPubRec(channel, (MqttMessageIdVariableHeader) msg.variableHeader());
            case PUBREL -> protocolProcessor.pubRel()
                    .processPubRel(channel, (MqttMessageIdVariableHeader) msg.variableHeader());
            case PUBCOMP -> protocolProcessor.pubComp()
                    .processPubComp(channel, (MqttMessageIdVariableHeader) msg.variableHeader());
            case PINGREQ -> protocolProcessor.pingReq().processPingReq(channel, msg);
            // DISCONNECT 必须把整条报文传下去: v5 的可变头里带原因码,
            // 以及可选的 Session Expiry Interval 改写请求 —— 只看类型会把这些丢掉
            case DISCONNECT -> protocolProcessor.disconnect().processDisconnect(channel, msg);
            default -> log.debug("忽略报文类型: {}", msg.fixedHeader().messageType());
        }
    }

    /**
     * 入站报文的统一日志描述(规范: 前缀 INBOUND, key=value, 单行)。
     *
     * <p>格式: {@code INBOUND clientId=... type=... <类型专属字段>} ——
     * CONNECT 带 username/proto/keepAlive/cleanSession(<b>绝不带密码</b>),
     * PUBLISH 带 topic/qos/packetId/retain/size/payload(截断预览),
     * SUBSCRIBE/UNSUBSCRIBE 逐过滤器带 QoS, 确认类只有 packetId。
     * 静态且无副作用, 便于对格式本身做单测。
     */
    static String describeInbound(Channel channel, MqttMessage msg) {
        String clientId = channel.attr(ChannelAttributes.CLIENT_ID).get();
        if ((clientId == null || clientId.isEmpty()) && msg instanceof MqttConnectMessage connect) {
            clientId = connect.payload().clientIdentifier();
        }
        if (clientId == null || clientId.isEmpty()) {
            clientId = "unknown";
        }
        StringBuilder sb = new StringBuilder(96)
                .append("INBOUND clientId=").append(clientId)
                .append(" type=").append(msg.fixedHeader().messageType());
        switch (msg.fixedHeader().messageType()) {
            case CONNECT -> {
                MqttConnectMessage connect = (MqttConnectMessage) msg;
                sb.append(" username=").append(connect.payload().userName())
                        .append(" proto=").append(connect.variableHeader().version())
                        .append(" keepAlive=").append(connect.variableHeader().keepAliveTimeSeconds())
                        .append(" cleanSession=").append(connect.variableHeader().isCleanSession())
                        .append(" will=").append(connect.variableHeader().isWillFlag());
            }
            case PUBLISH -> {
                MqttPublishMessage publish = (MqttPublishMessage) msg;
                sb.append(" topic=").append(publish.variableHeader().topicName())
                        .append(" qos=").append(publish.fixedHeader().qosLevel().value())
                        .append(" packetId=").append(publish.variableHeader().packetId())
                        .append(" retain=").append(publish.fixedHeader().isRetain())
                        .append(" dup=").append(publish.fixedHeader().isDup())
                        .append(" size=").append(publish.payload().readableBytes())
                        .append(" payload=").append(Payloads.preview(publish.payload()));
            }
            case SUBSCRIBE -> {
                MqttSubscribeMessage subscribe = (MqttSubscribeMessage) msg;
                sb.append(" packetId=").append(subscribe.variableHeader().messageId());
                sb.append(" filters=[");
                List<MqttTopicSubscription> topics = subscribe.payload().topicSubscriptions();
                for (int i = 0; i < topics.size(); i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append(topics.get(i).topicName())
                            .append(':').append(topics.get(i).qualityOfService().value());
                }
                sb.append(']');
            }
            case UNSUBSCRIBE -> {
                MqttUnsubscribeMessage unsubscribe = (MqttUnsubscribeMessage) msg;
                sb.append(" packetId=").append(unsubscribe.variableHeader().messageId())
                        .append(" topics=").append(unsubscribe.payload().topics());
            }
            case PUBACK, PUBREC, PUBREL, PUBCOMP -> {
                if (msg.variableHeader() instanceof MqttMessageIdVariableHeader header) {
                    sb.append(" packetId=").append(header.messageId());
                }
            }
            case DISCONNECT -> {
                if (msg.variableHeader() instanceof MqttReasonCodeAndPropertiesVariableHeader header) {
                    sb.append(" reason=").append(header.reasonCode());
                }
            }
            default -> {
                // PINGREQ 等无附加字段
            }
        }
        return sb.toString();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
        String clientId = channel.attr(ChannelAttributes.CLIENT_ID).get();

        if (clientId != null) {
            // 使用 remove(key, value) 语义: 若该 clientId 已被新连接接管, 这里不会误删新连接
            boolean removed = connectionRegistry.unregister(clientId, channel);
            // 管理面状态同步必须挂在同一个判断上。否则「旧连接的延迟关闭」回调会把
            // 刚接管它的新连接从视图里删掉 —— 客户端明明在线, 控制台上却消失了。
            if (removed) {
                adminStatePublisher.clientOffline(clientId);
                // 断连计数: 优雅(DISCONNECT 主动断开)与异常(超时/TCP 断开)分开 ——
                // 异常断连速率是弱网/设备故障的直接信号
                nodeMetricsService.connectionClosed(Boolean.TRUE.equals(
                        channel.attr(ChannelAttributes.GRACEFUL_DISCONNECT).get()));
                // 连接事件(下线): 只在「该 clientId 的在线状态真正结束」时发布 ——
                // 被接管时旧连接的关闭不构成下线(客户端马上以新连接出现在事件流里)
                clusterBus.publishConnectionEvent(
                        online.ipuff.jmqtt.cluster.ConnectionEvent.disconnected(
                                clientId,
                                channel.attr(ChannelAttributes.USERNAME).get(),
                                Boolean.TRUE.equals(
                                        channel.attr(ChannelAttributes.PROTOCOL_VERSION).get()
                                                == io.netty.handler.codec.mqtt.MqttVersion.MQTT_5) ? 5 : 4,
                                ChannelAttributes.peernameOf(channel),
                                null,
                                Boolean.TRUE.equals(
                                        channel.attr(ChannelAttributes.GRACEFUL_DISCONNECT).get())
                                        ? online.ipuff.jmqtt.cluster.ConnectionEvent.REASON_CLOSED
                                        : online.ipuff.jmqtt.cluster.ConnectionEvent.REASON_TCP_CLOSED));
            }
            // ACL 决策缓存随连接释放(无论是否被接管 —— 新连接有自己的缓存条目)
            aclService.onClientOffline(clientId);
            publishWillIfNeeded(channel, clientId);
        }

        if (log.isDebugEnabled()) {
            log.debug("连接关闭: clientId={} remote={}", clientId, channel.remoteAddress());
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent idleEvent && idleEvent.state() == IdleState.ALL_IDLE) {
            Channel channel = ctx.channel();
            String clientId = channel.attr(ChannelAttributes.CLIENT_ID).get();
            log.debug("空闲超时, 关闭连接 clientId={}", clientId);
            // 遗嘱发布统一放在 channelInactive 里, 避免两处重复触发
            channel.close();
            return;
        }
        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof IOException) {
            // 客户端强制断开属于常态, 不打错误栈
            log.debug("连接异常关闭: {}", cause.getMessage());
        } else {
            log.warn("连接处理异常, clientId={}", ctx.channel().attr(ChannelAttributes.CLIENT_ID).get(), cause);
        }
        ctx.close();
    }

    private void handleDecodeFailure(Channel channel, MqttMessage msg) {
        Throwable cause = msg.decoderResult().cause();
        MqttConnAckVariableHeader variableHeader;
        if (cause instanceof MqttUnacceptableProtocolVersionException) {
            variableHeader = new MqttConnAckVariableHeader(
                    MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION, false);
        } else if (cause instanceof MqttIdentifierRejectedException) {
            variableHeader = new MqttConnAckVariableHeader(
                    MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED, false);
        } else {
            log.debug("报文解码失败: {}", cause == null ? "unknown" : cause.getMessage());
            channel.close();
            return;
        }
        MqttConnAckMessage connAck = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                variableHeader, null);
        channel.writeAndFlush(connAck).addListener(future -> channel.close());
    }

    /**
     * 异常断连时发布遗嘱消息。
     *
     * <p>正常 DISCONNECT 的路径上, {@code DisconnectHandler} 已经先把 will 清空,
     * 所以这里不会重复发布。
     *
     * <p><b>驱逐(排水)场景例外</b>: 服务端主动断开连接时, 客户端会在秒级内重连到其他节点。
     * 按规范这属于「非正常断开」, 遗嘱应当发布 —— 但一次排水会在一分钟内触发上万条遗嘱,
     * 它们涌进路由、集群总线与保留存储, 而设备并没有真的下线。所以驱逐会先在连接上
     * 置 {@link ChannelAttributes#SUPPRESS_WILL}, 这里据此跳过发布。
     *
     * <p>跳过发布时仍会清掉会话上的遗嘱: 否则这条遗嘱会一直挂在会话里,
     * 等到这个客户端下次<em>真正</em>异常断开时被触发 —— 那是一个更晚、更意外的时刻。
     * 客户端重连时会重新在 CONNECT 里声明遗嘱, 不存在丢失。
     */
    private void publishWillIfNeeded(Channel channel, String clientId) {
        if (Boolean.TRUE.equals(channel.attr(ChannelAttributes.SUPPRESS_WILL).get())) {
            SessionStore suppressed = sessionStoreService.get(clientId);
            if (suppressed != null) {
                suppressed.setWillMessage(null);
            }
            log.info("驱逐断连, 已跳过遗嘱发布 clientId={}", clientId);
            return;
        }

        SessionStore session = sessionStoreService.get(clientId);
        if (session == null) {
            return;
        }
        WillMessage will = session.getWillMessage();
        if (will == null) {
            return;
        }
        // 先清空, 防止后续重复触发
        session.setWillMessage(null);

        try {
            InternalMessage message = internalCommunication.fromLocal(
                    null, will.topic(), will.qos(), will.payload(), false, false,
                    channel.attr(ChannelAttributes.USERNAME).get());
            internalSendServer.sendPublishMessage(message);
            internalCommunication.internalSend(message);

            if (will.retain()) {
                if (will.payload() == null || will.payload().length == 0) {
                    retainMessageStoreService.remove(will.topic());
                } else {
                    retainMessageStoreService.put(
                            new RetainMessageStore(will.topic(), will.payload(), will.qos()));
                }
            }
            log.info("已发布遗嘱消息 clientId={} topic={}", clientId, will.topic());
        } catch (Exception e) {
            log.warn("发布遗嘱消息失败 clientId={}", clientId, e);
        }
    }
}
