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
package online.ipuff.jmqtt.cluster;

import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttQoS;
import online.ipuff.jmqtt.message.DupPublishMessageStore;
import online.ipuff.jmqtt.message.SubscribeStore;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.protocol.ReplyFactory;
import online.ipuff.jmqtt.session.BackpressureMetrics;
import online.ipuff.jmqtt.session.ConnectionRegistry;
import online.ipuff.jmqtt.session.QosMetrics;
import online.ipuff.jmqtt.session.ISessionStoreService;
import online.ipuff.jmqtt.session.SendBuffer;
import online.ipuff.jmqtt.session.SessionStore;
import online.ipuff.jmqtt.store.IDupPublishMessageStoreService;
import online.ipuff.jmqtt.store.IMessageIdService;
import online.ipuff.jmqtt.store.IPendingMessageStore;
import online.ipuff.jmqtt.store.PendingMessage;
import online.ipuff.jmqtt.subscribe.SubscribeStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 本节点消息投递。
 *
 * 复制了三份, 且每次投递都要「查 Redis 会话 → 拼 key → 查 channelIdMap → 遍历
 * ChannelGroup」四步。这里改为:
 * <ul>
 *   <li>一次 {@link SubscribeStoreService#search} 拿到订阅者(内存主题树)</li>
 *   <li>一次 {@link ConnectionRegistry#get} 拿到 Channel</li>
 *   <li>投递逻辑按 QoS 参数化, 不再三分支复制</li>
 * </ul>
 *
 * <p>本类同时被三条路径调用: 本节点客户端发布时、集群总线 ingress 注入时、
 * 以及客户端重连后投递离线积压时。
 */
@Service
public class InternalSendServer {

    private static final Logger log = LoggerFactory.getLogger(InternalSendServer.class);

    /** 离线积压投递的分批大小: 分批而非一次取空, 保证中途断线时剩余部分不丢 */
    private static final int DRAIN_BATCH_SIZE = 50;

    private final SubscribeStoreService subscribeStoreService;
    private final ConnectionRegistry connectionRegistry;
    private final IMessageIdService messageIdService;
    private final IDupPublishMessageStoreService dupPublishMessageStoreService;
    private final ISessionStoreService sessionStoreService;
    private final IPendingMessageStore pendingMessageStore;
    private final BrokerProperties properties;
    private final online.ipuff.jmqtt.admin.TopicCaptureService captureService;
    private final BackpressureMetrics metrics;
    private final QosMetrics qosMetrics;

    public InternalSendServer(SubscribeStoreService subscribeStoreService,
                             ConnectionRegistry connectionRegistry,
                             IMessageIdService messageIdService,
                             IDupPublishMessageStoreService dupPublishMessageStoreService,
                             ISessionStoreService sessionStoreService,
                             IPendingMessageStore pendingMessageStore,
                             BrokerProperties properties,
                             BackpressureMetrics metrics,
                             QosMetrics qosMetrics,
                             online.ipuff.jmqtt.admin.TopicCaptureService captureService) {
        this.subscribeStoreService = subscribeStoreService;
        this.connectionRegistry = connectionRegistry;
        this.messageIdService = messageIdService;
        this.dupPublishMessageStoreService = dupPublishMessageStoreService;
        this.sessionStoreService = sessionStoreService;
        this.pendingMessageStore = pendingMessageStore;
        this.properties = properties;
        this.captureService = captureService;
        this.metrics = metrics;
        this.qosMetrics = qosMetrics;
    }

    /**
     * 把消息投递给本节点命中的订阅者。
     *
     * <p>订阅者不在线时: 若其会话是持久的({@code cleanSession=0})且 QoS &gt; 0,
     * 消息进入离线队列, 客户端重连后投递; 否则丢弃(MQTT 语义如此)。
     *
     * @param message 内部消息; {@code clientId} 只作来源标识, 不参与投递判定
     * @return 实际投递的客户端数(不含入队数)
     */
    public int sendPublishMessage(InternalMessage message) {
        String topic = message.topic();
        byte[] payload = message.payload();

        List<SubscribeStore> subscribers = subscribeStoreService.search(topic);
        if (subscribers.isEmpty()) {
            return 0;
        }

        int delivered = 0;
        for (SubscribeStore sub : subscribers) {
            String subscriberId = sub.clientId();

            // 不排除发布者自身: loopback(自发自收)是 MQTT 的合法且必须投递的语义。
            // "No Local" 是 v5 的订阅选项且默认关闭 —— 此前按 clientId 全局排除发布者,
            // 导致订阅了自己发布主题的客户端永远收不到消息(违背规范)。
            // 集群下也不会双投: 本节点投完后, 总线回环由消费端的 brokerId 自查拦住。

            // 投递 QoS 取发布 QoS 与订阅 QoS 的较小值
            int deliveryQos = Math.min(message.qos(), sub.qos());

            Channel channel = connectionRegistry.get(subscriberId);
            if (channel == null || !channel.isActive()) {
                enqueueOffline(subscriberId, topic, deliveryQos, payload, message.retain());
                continue;
            }

            // 消息抓取(客户端维度, direction=sub): 记录该订阅者收到了什么。
            // 零活跃任务时一次空表判断即返回
            captureService.onDeliver(message, subscriberId);

            deliver(channel, subscriberId, topic, payload, deliveryQos, message.retain(), message.dup());
            delivered++;
        }
        return delivered;
    }

    /**
     * 客户端重连后, 把欠它的离线消息投递出去。
     *
     * <p>分批投递而不是一次取空: 客户端在投递过程中再次断开时,
     * 尚未取出的部分仍留在队列里, 下次重连还能拿到。
     *
     * @return 本次投递条数
     */
    public int deliverPendingMessages(String clientId, Channel channel) {
        int delivered = 0;
        while (channel.isActive()) {
            // 窗口已满就停: 尚未取出的积压继续留在持久队列里, 等确认腾出窗口后再取。
            // 若在这里一次取空, 多出来的部分会退化成连接级的发送队列 ——
            // 连接一断就没了, 反而丢掉了离线队列「跨连接存活」的意义。
            SendBuffer buffer = channel.attr(ChannelAttributes.SEND_BUFFER).get();
            if (buffer != null && !buffer.hasWindow()) {
                break;
            }
            List<PendingMessage> batch = pendingMessageStore.drainBatch(clientId, DRAIN_BATCH_SIZE);
            if (batch.isEmpty()) {
                break;
            }
            for (PendingMessage message : batch) {
                // 队列里的消息是「正常投递」而非「因订阅触发的保留消息」, retain 恒为 false
                deliver(channel, clientId, message.topic(), message.payload(),
                        message.qos(), message.retain(), false);
                delivered++;
            }
        }
        if (delivered > 0) {
            log.info("已投递离线消息: clientId={} 条数={}", clientId, delivered);
        }
        return delivered;
    }

    /**
     * 离线消息入队。
     *
     * <p>两条明确的丢弃口径:
     * <ul>
     *   <li><b>QoS 0 不入队</b> —— 它本来就没有投递保证, 离线即丢符合语义</li>
     *   <li><b>非持久会话不入队</b> —— {@code cleanSession=1} 的会话断了就没了,
     *       缓存消息没有意义</li>
     * </ul>
     * 这两条把队列的写入量限制在「持久会话 + QoS 1/2 + 客户端离线」这个很小的子集上。
     */
    private void enqueueOffline(String clientId, String topic, int qos, byte[] payload, boolean retain) {
        if (qos == MqttQoS.AT_MOST_ONCE.value()) {
            return;
        }
        SessionStore session = sessionStoreService.get(clientId);
        // 判据是「会话是否保留」而不是「是否 cleanSession」——
        // v5 允许 cleanStart=1 且 Session Expiry Interval>0, 那种会话要保留、也该缓存离线消息
        if (session == null || !session.isPersistent()) {
            return;
        }
        pendingMessageStore.enqueue(clientId, PendingMessage.of(topic, qos, payload, retain),
                properties.maxOfflineQueueLen());
    }

    /**
     * 投递一条消息, 受发送窗口与队列上限约束。
     *
     * <p>QoS 0 不占窗口(MQTT 只对 QoS 1/2 做流控), 写缓冲满时直接丢弃。
     * QoS 1/2 先申请窗口: 窗口未满立即发送, 满了则进入有界队列等确认 ——
     * 这一步就是 MQTT 的流控, 也是慢消费者的主要保护。
     */
    private void deliver(Channel channel, String clientId, String topic, byte[] payload,
                         int qos, boolean retain, boolean dup) {

        qosMetrics.delivered(qos);

        if (qos == MqttQoS.AT_MOST_ONCE.value()) {
            deliverQos0(channel, topic, payload, retain, dup);
            return;
        }

        SendBuffer buffer = channel.attr(ChannelAttributes.SEND_BUFFER).get();
        if (buffer == null || buffer.tryAcquireWindow()) {
            // 有窗口, 或未装配发送缓冲(退化路径): 立即发送
            sendNew(channel, buffer, clientId, topic, payload, qos, retain, dup);
            return;
        }

        // 窗口用尽 —— 不确认就不再发新的, 剩下的排队
        metrics.sendQueueEnqueued();
        buffer.enqueue(new SendBuffer.Outbound(topic, qos, payload, retain, dup));
    }

    /**
     * QoS 0: 不占窗口, 但写缓冲满时丢弃。
     *
     * <p>QoS 0 本来就没有投递保证, 丢弃符合语义; 继续写入只会让 Netty 的写缓冲
     * 无限增长, 一个不读 socket 的客户端就足以吃光整个堆。
     */
    private void deliverQos0(Channel channel, String topic, byte[] payload, boolean retain, boolean dup) {
        if (!channel.isWritable()) {
            connectionRegistry.recordHighWaterMark();
            metrics.qos0NotWritableDropped();
            log.debug("写缓冲已满, 丢弃 QoS 0 消息: topic={}", topic);
            return;
        }
        writePublish(channel, topic, payload, 0, MqttQoS.AT_MOST_ONCE.value(), retain, dup);
    }

    /**
     * 分配报文标识符、登记在途、写出、确认窗口占位。
     */
    private void sendNew(Channel channel, SendBuffer buffer, String clientId,
                         String topic, byte[] payload, int qos, boolean retain, boolean dup) {
        int packetId = messageIdService.nextMessageId();
        dupPublishMessageStoreService.put(
                new DupPublishMessageStore(clientId, topic, qos, packetId, payload));
        try {
            writePublish(channel, topic, payload, packetId, qos, retain, dup);
        } catch (Exception e) {
            if (buffer != null) {
                buffer.cancelReservation();
            }
            throw e;
        }
        if (buffer != null) {
            buffer.confirm(packetId);
        }
    }

    /**
     * 写出 PUBLISH。<b>必须经 {@link ReplyFactory}</b> —— v5 连接的可变头里要有
     * properties 长度字段, 少了它客户端会把 payload 首字节当成 properties 长度而整体错位。
     */
    private void writePublish(Channel channel, String topic, byte[] payload, int packetId,
                              int qos, boolean retain, boolean dup) {
        channel.writeAndFlush(
                ReplyFactory.publish(channel, topic, qos, packetId, payload, retain, dup),
                channel.voidPromise());
    }

    /**
     * 释放一个发送窗口, 并把队列里能发的继续发出去。
     *
     * <p>由 PUBACK(QoS 1)与 PUBREC(QoS 2)触发 —— 这两处才是「客户端确实收到了 PUBLISH」
     * 的语义点。PUBCOMP 完成的是 PUBREL, 不占窗口。
     */
    public void releaseSendWindow(Channel channel, int packetId) {
        SendBuffer buffer = channel.attr(ChannelAttributes.SEND_BUFFER).get();
        if (buffer == null) {
            return;
        }
        buffer.release(packetId);
        drainSendBuffer(channel);
    }

    /**
     * 把发送队列里能发的消息发出去; 队列清空后若仍有窗口, 继续从离线积压里取。
     */
    int drainSendBuffer(Channel channel) {
        SendBuffer buffer = channel.attr(ChannelAttributes.SEND_BUFFER).get();
        if (buffer == null) {
            return 0;
        }
        String clientId = channel.attr(ChannelAttributes.CLIENT_ID).get();
        int sent = 0;
        while (channel.isActive()) {
            SendBuffer.Outbound next = buffer.pollIfWindow();
            if (next == null) {
                break;
            }
            sendNew(channel, buffer, clientId, next.topic(), next.payload(),
                    next.qos(), next.retain(), next.dup());
            sent++;
        }
        // 发送队列已清空且仍有窗口: 继续领取离线积压, 否则积压会一直等到下次重连
        if (sent > 0 && channel.isActive() && buffer.hasWindow() && clientId != null) {
            deliverPendingMessages(clientId, channel);
        }
        return sent;
    }
}
