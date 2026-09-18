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
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import online.ipuff.jmqtt.handler.ChannelAttributes;
import online.ipuff.jmqtt.message.DupPublishMessageStore;
import online.ipuff.jmqtt.message.RetainMessageStore;
import online.ipuff.jmqtt.message.SubscribeStore;
import online.ipuff.jmqtt.router.MqttTopic;
import online.ipuff.jmqtt.session.persistence.SessionPersistence;
import online.ipuff.jmqtt.store.IDupPublishMessageStoreService;
import online.ipuff.jmqtt.store.IMessageIdService;
import online.ipuff.jmqtt.store.IRetainMessageStoreService;
import online.ipuff.jmqtt.subscribe.ISubscribeStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SUBSCRIBE 处理。
 *
 * <h2>两处容易做错的地方</h2>
 * <ol>
 *   <li><b>失败码要逐项返回。</b>一个过滤器非法就把整条连接关掉, 是过重的处置:
 *       规范允许在 SUBACK 里为每个过滤器单独返回失败码, 同一连接上可能存在别的正常订阅。
 *       这里逐项处理 —— v5 还提供了细分原因码(0x8F 过滤器非法 / 0x9E 不支持共享订阅),
 *       更不该升级成连接级事件。</li>
 *   <li><b>保留消息的 retain 标志。</b>规范要求「因新订阅而投递的保留消息」
 *       必须置 {@code retain=1}, 否则客户端无法区分「这是历史状态」还是「这是一条新消息」,
 *       在状态类主题上会做出错误判断。</li>
 * </ol>
 */
@Component
public class SubscribeHandler {

    private static final Logger log = LoggerFactory.getLogger(SubscribeHandler.class);

    private final ISubscribeStoreService subscribeStoreService;
    private final IRetainMessageStoreService retainMessageStoreService;
    private final IMessageIdService messageIdService;
    private final IDupPublishMessageStoreService dupPublishMessageStoreService;
    private final SessionPersistence sessionPersistence;

    public SubscribeHandler(ISubscribeStoreService subscribeStoreService,
                            IRetainMessageStoreService retainMessageStoreService,
                            IMessageIdService messageIdService,
                            IDupPublishMessageStoreService dupPublishMessageStoreService,
                            SessionPersistence sessionPersistence) {
        this.subscribeStoreService = subscribeStoreService;
        this.retainMessageStoreService = retainMessageStoreService;
        this.messageIdService = messageIdService;
        this.dupPublishMessageStoreService = dupPublishMessageStoreService;
        this.sessionPersistence = sessionPersistence;
    }

    public void processSubscribe(Channel channel, MqttSubscribeMessage msg) {
        String clientId = channel.attr(ChannelAttributes.CLIENT_ID).get();
        List<MqttTopicSubscription> subscriptions = msg.payload().topicSubscriptions();
        if (clientId == null || subscriptions == null || subscriptions.isEmpty()) {
            channel.close();
            return;
        }

        boolean v5 = ReplyFactory.isV5(channel);
        // 一次性取出该客户端现有的过滤器: v5 的 Retain Handling=1 需要判断「本次是否新订阅」
        Set<String> existingFilters = subscribeStoreService.subscriptionsOf(clientId).stream()
                .map(SubscribeStore::topicFilter)
                .collect(Collectors.toSet());
        List<Integer> grantedQos = new ArrayList<>(subscriptions.size());
        List<MqttTopicSubscription> accepted = new ArrayList<>(subscriptions.size());
        List<Boolean> sendRetain = new ArrayList<>(subscriptions.size());

        for (MqttTopicSubscription subscription : subscriptions) {
            String topicFilter = subscription.topicName();
            int qos = subscription.qualityOfService().value();

            // 共享订阅($share/{group}/{filter})能把消息分摊到多个订阅者。
            // 我们不支持, 而 $share/... 恰好是一个语法合法的过滤器 —— 若照常接受,
            // 订阅会成功但<b>永远匹配不到任何发布</b>($share 是保留前缀, 不会被当作普通主题层),
            // 表现为静默丢消息。v5 有专用原因码, 必须明确拒绝。
            if (topicFilter != null && topicFilter.startsWith("$share/")) {
                log.debug("不支持共享订阅, clientId={} topicFilter={}", clientId, topicFilter);
                grantedQos.add(v5 ? ReplyFactory.SHARED_SUBSCRIPTIONS_NOT_SUPPORTED
                        : ReplyFactory.V3_SUBSCRIBE_FAILURE);
                continue;
            }

            if (!MqttTopic.isValidFilter(topicFilter)) {
                log.debug("非法主题过滤器, clientId={} topicFilter={}", clientId, topicFilter);
                // 失败码按版本选: v3.1.1 只有 0x80, v5 有专门的「过滤器非法」0x8F
                grantedQos.add(v5 ? ReplyFactory.TOPIC_FILTER_INVALID : ReplyFactory.V3_SUBSCRIBE_FAILURE);
                continue;
            }

            MqttSubscriptionOption option = subscription.option();
            boolean alreadySubscribed = existingFilters.contains(topicFilter);

            subscribeStoreService.put(new SubscribeStore(clientId, topicFilter, qos));
            // 只有需要保留的会话才真正落盘, 判断集中在 SessionPersistence 里
            sessionPersistence.subscriptionAdded(clientId, topicFilter, qos);
            grantedQos.add(qos);
            accepted.add(subscription);
            sendRetain.add(shouldSendRetain(option, alreadySubscribed));
        }

        // 即使全部过滤器都非法也<b>不断连接</b>, 只回逐项失败码。
        //
        // 这里原先会在「全量非法」时 close channel, 理由是「通常意味着客户端异常」。
        // 但 v5 专门为此提供了细分的订阅失败码(0x8F 过滤器非法 / 0x9E 不支持共享订阅 …),
        // 其设计意图正是让客户端能<b>逐项</b>知道问题出在哪, 而不必拆掉整条连接 ——
        // 断了连接, 客户端就拿不到 SUBACK, 只能看到一次莫名其妙的断开。
        // 订阅失败本来也不该升级成连接级事件: 同一连接上可能有别的正常订阅,
        // 而且 v3.1.1 的规范同样只要求「为每个过滤器回一个返回码」。
        channel.writeAndFlush(ReplyFactory.subAck(channel, msg.variableHeader().messageId(), grantedQos));

        // 投递命中的保留消息
        for (int i = 0; i < accepted.size(); i++) {
            if (!sendRetain.get(i)) {
                continue;
            }
            MqttTopicSubscription subscription = accepted.get(i);
            sendRetainMessages(channel, clientId, subscription.topicName(),
                    subscription.qualityOfService().value());
        }
    }

    /**
     * 是否应为本次订阅投递保留消息。v5 的 Retain Handling 选项控制这件事:
     * <ul>
     *   <li>{@code 0} 每次订阅都发(也是 v3.1.1 的行为)</li>
     *   <li>{@code 1} 仅当这个过滤器是<b>新</b>的才发 —— 重连恢复订阅时不会重放一遍保留消息</li>
     *   <li>{@code 2} 从不发</li>
     * </ul>
     * 这个选项在设备重连场景下很实用: 默认行为会把所有保留消息重放一遍,
     * 对「只关心当前状态」的设备是纯浪费。
     */
    private boolean shouldSendRetain(MqttSubscriptionOption option, boolean alreadySubscribed) {
        if (option == null) {
            return true;
        }
        return switch (option.retainHandling()) {
            case SEND_AT_SUBSCRIBE -> true;
            case SEND_AT_SUBSCRIBE_IF_NOT_YET_EXISTS -> !alreadySubscribed;
            case DONT_SEND_AT_SUBSCRIBE -> false;
        };
    }

    /**
     * 投递该过滤器命中的保留消息。
     *
     * <p>注意这里是<b>过滤器驱动</b>的查询(输入过滤器, 输出匹配合适的具体主题),
     * 与发布时的主题驱动匹配方向相反。
     */
    private void sendRetainMessages(Channel channel, String clientId, String topicFilter, int subscribeQos) {
        List<RetainMessageStore> retains = retainMessageStoreService.search(topicFilter);
        if (retains.isEmpty()) {
            return;
        }
        for (RetainMessageStore retain : retains) {
            int deliveryQos = Math.min(retain.qos(), subscribeQos);
            int packetId = 0;
            if (deliveryQos > 0) {
                packetId = messageIdService.nextMessageId();
                dupPublishMessageStoreService.put(new DupPublishMessageStore(
                        clientId, retain.topic(), deliveryQos, packetId, retain.payload()));
            }
            // retain=1: 因新订阅而投递的保留消息(规范要求置位)。
            // 必须走 ReplyFactory —— v5 连接的可变头需要 properties 长度字段, 否则客户端整体错位
            channel.write(ReplyFactory.publish(channel, retain.topic(), deliveryQos,
                    packetId, retain.payload(), true, false));
        }
        channel.flush();
        log.debug("投递保留消息 clientId={} topicFilter={} 数量={}", clientId, topicFilter, retains.size());
    }
}
