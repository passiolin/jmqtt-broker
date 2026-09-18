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

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import online.ipuff.jmqtt.cluster.InternalCommunication;
import online.ipuff.jmqtt.cluster.InternalMessage;
import online.ipuff.jmqtt.cluster.InternalSendServer;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.message.RetainMessageStore;
import online.ipuff.jmqtt.router.MqttTopic;
import online.ipuff.jmqtt.session.QosMetrics;
import online.ipuff.jmqtt.store.IInboundQos2Store;
import online.ipuff.jmqtt.store.IRetainMessageStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * PUBLISH 处理。
 *
 * <p>流程:
 * <ol>
 *   <li>本地投递(内存主题树匹配, 零外部调用)</li>
 *   <li>出站到集群总线(启用时)</li>
 *   <li>按 QoS 回 PUBACK / PUBREC</li>
 *   <li>处理 retain 标志</li>
 * </ol>
 *
 * <p><b>投递路径上不做任何外部调用。</b>本地投递全部交给 {@link InternalSendServer},
 * 在那里一次拿到订阅者列表、一次拿到 Channel; QoS 用参数表达而不是按 QoS 复制三份逻辑。
 */
@Component
public class PublishHandler {

    private static final Logger log = LoggerFactory.getLogger(PublishHandler.class);

    private final InternalCommunication internalCommunication;
    private final InternalSendServer internalSendServer;
    private final IRetainMessageStoreService retainMessageStoreService;
    private final QosMetrics qosMetrics;
    private final IInboundQos2Store inboundQos2Store;

    public PublishHandler(InternalCommunication internalCommunication,
                          InternalSendServer internalSendServer,
                          IRetainMessageStoreService retainMessageStoreService,
                          QosMetrics qosMetrics,
                          IInboundQos2Store inboundQos2Store) {
        this.internalCommunication = internalCommunication;
        this.internalSendServer = internalSendServer;
        this.retainMessageStoreService = retainMessageStoreService;
        this.qosMetrics = qosMetrics;
        this.inboundQos2Store = inboundQos2Store;
    }

    public void processPublish(Channel channel, MqttPublishMessage msg) {
        String clientId = channel.attr(ChannelAttributes.CLIENT_ID).get();
        String topic = msg.variableHeader().topicName();
        int qos = msg.fixedHeader().qosLevel().value();
        boolean v5 = ReplyFactory.isV5(channel);

        // v5 的 Topic Alias 在 CONNACK 里被我们声明为 0(明确不接受入站别名),
        // 客户端仍然发来就是协议错误 —— 必须显式拒绝, 而不是当成普通报文处理,
        // 否则「topic 为空 + 别名」会被当作空主题名, 处置方式是错的。
        if (v5 && msg.variableHeader().properties() != null
                && msg.variableHeader().properties().getProperty(
                        MqttProperties.MqttPropertyType.TOPIC_ALIAS.value()) != null) {
            log.warn("v5 客户端使用了未协商的 topic alias, clientId={}, 按协议错误断开", clientId);
            ReplyFactory.closeWithReason(channel, ReplyFactory.PROTOCOL_ERROR);
            return;
        }

        if (!MqttTopic.isValidTopicName(topic)) {
            log.warn("收到非法主题名的 PUBLISH, clientId={} topic={}", clientId, topic);
            if (v5) {
                // v5 有专门的原因码; v3.1.1 只能直接关连接
                ReplyFactory.closeWithReason(channel, ReplyFactory.TOPIC_NAME_INVALID);
            } else {
                channel.close();
            }
            return;
        }

        int packetId = msg.variableHeader().packetId();

        // QoS 2 的接收方向去重。必须在投递<b>之前</b>判断, 否则去重就没有意义了。
        //
        // 客户端在拿到 PUBREC 之前重发同一条 PUBLISH(dup=1) 是合法行为 ——
        // PUBREC 可能丢在路上。但服务端只能投递一次 [MQTT-4.3.3-2]:
        // 重复投递在业务侧表现为「同一条指令被执行了两次」, 而发布方
        // 从协议层看到的是两次成功确认, 完全无从察觉。
        if (qos == MqttQoS.EXACTLY_ONCE.value()) {
            IInboundQos2Store.Result inbound = inboundQos2Store.mark(clientId, packetId);
            if (inbound == IInboundQos2Store.Result.DUPLICATE) {
                // 重复: 不投递、不上总线、不重放 retain, 只补一个 PUBREC。
                // 客户端据此继续走 PUBREL 流程, 与第一次收到时完全一致
                channel.writeAndFlush(ReplyFactory.pubRec(channel, packetId, ReplyFactory.SUCCESS));
                log.debug("QoS 2 重复 PUBLISH, 已丢弃(只回 PUBREC) clientId={} packetId={}",
                        clientId, packetId);
                return;
            }
            if (inbound == IInboundQos2Store.Result.OVERFLOW) {
                ReplyFactory.closeWithReason(channel, ReplyFactory.RECEIVE_MAXIMUM_EXCEEDED);
                return;
            }
        }

        // 必须复制: SimpleChannelInboundHandler 会在 channelRead0 返回后释放报文
        byte[] payload = ByteBufUtil.getBytes(msg.payload());
        qosMetrics.published(qos);

        // 投递给本地订阅者时 retain 置 false ——
        // 规范要求只有「因新订阅而投递的保留消息」才置 retain=1
        InternalMessage internalMessage =
                internalCommunication.fromLocal(clientId, topic, qos, payload, false, false);

        int delivered = internalSendServer.sendPublishMessage(internalMessage);

        // 出站: 交给其他节点(启用集群时)
        internalCommunication.internalSend(internalMessage);

        // 按 QoS 应答。
        //
        // 原因码恒为 SUCCESS, 这里刻意不使用 v5 的 0x10 No matching subscribers:
        // 本方法只知道<b>本地</b>投递了几条, 而消息还会经集群总线发往其他节点。
        // 在集群模式下报「无匹配订阅者」会是错的信号 —— 客户端据此可能以为消息没人要。
        // 只有在能确定「全局都没有订阅者」时才谈得上用这个码, 那需要总线回执, 属后续项。
        if (qos == MqttQoS.AT_LEAST_ONCE.value()) {
            channel.writeAndFlush(ReplyFactory.pubAck(channel, packetId, ReplyFactory.SUCCESS));
        } else if (qos == MqttQoS.EXACTLY_ONCE.value()) {
            channel.writeAndFlush(ReplyFactory.pubRec(channel, packetId, ReplyFactory.SUCCESS));
        }

        // retain 处理
        if (msg.fixedHeader().isRetain()) {
            if (payload.length == 0) {
                retainMessageStoreService.remove(topic);
            } else {
                retainMessageStoreService.put(new RetainMessageStore(topic, payload, qos));
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("PUBLISH clientId={} topic={} qos={} size={} 本地投递={}",
                    clientId, topic, qos, payload.length, delivered);
        }
    }
}
