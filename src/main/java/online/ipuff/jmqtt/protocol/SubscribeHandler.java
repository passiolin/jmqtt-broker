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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package online.ipuff.jmqtt.protocol;

import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscriptionOption;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import online.ipuff.jmqtt.authz.AclGate;
import online.ipuff.jmqtt.authz.IAclService;
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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * SUBSCRIBE 处理。
 *
 * <h2>三处容易做错的地方</h2>
 * <ol>
 *   <li><b>失败码要逐项返回。</b>一个过滤器非法就把整条连接关掉, 是过重的处置:
 *       规范允许在 SUBACK 里为每个过滤器单独返回失败码, 同一连接上可能存在别的正常订阅。
 *       这里逐项处理 —— v5 还提供了细分原因码(0x8F 过滤器非法 / 0x9E 不支持共享订阅 /
 *       0x87 ACL 拒绝), 更不该升级成连接级事件。</li>
 *   <li><b>ACL 判定是异步的, SUBACK 必须等全部过滤器的判定完成。</b>判定期间该连接的
 *       后续报文经 {@link AclGate} 按序排队; 全部命中缓存时同步完成, 零挂起。</li>
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
    private final IAclService aclService;

    public SubscribeHandler(ISubscribeStoreService subscribeStoreService,
                            IRetainMessageStoreService retainMessageStoreService,
                            IMessageIdService messageIdService,
                            IDupPublishMessageStoreService dupPublishMessageStoreService,
                            SessionPersistence sessionPersistence,
                            IAclService aclService) {
        this.subscribeStoreService = subscribeStoreService;
        this.retainMessageStoreService = retainMessageStoreService;
        this.messageIdService = messageIdService;
        this.dupPublishMessageStoreService = dupPublishMessageStoreService;
        this.sessionPersistence = sessionPersistence;
        this.aclService = aclService;
    }

    public void processSubscribe(Channel channel, MqttSubscribeMessage msg) {
        String clientId = channel.attr(ChannelAttributes.CLIENT_ID).get();
        List<MqttTopicSubscription> subscriptions = msg.payload().topicSubscriptions();
        if (clientId == null || subscriptions == null || subscriptions.isEmpty()) {
            channel.close();
            return;
        }

        boolean v5 = ReplyFactory.isV5(channel);
        int messageId = msg.variableHeader().messageId();
        // 一次性取出该客户端现有的过滤器: v5 的 Retain Handling=1 需要判断「本次是否新订阅」
        Set<String> existingFilters = subscribeStoreService.subscriptionsOf(clientId).stream()
                .map(SubscribeStore::topicFilter)
                .collect(Collectors.toSet());

        // 第一遍(同步): 静态校验; codes[i] 为 null 表示「待 ACL/可接受」, 非 null 为失败码
        Integer[] codes = new Integer[subscriptions.size()];
        List<Integer> aclPending = new ArrayList<>();
        for (int i = 0; i < subscriptions.size(); i++) {
            String topicFilter = subscriptions.get(i).topicName();

            // 共享订阅($share/{group}/{filter})能把消息分摊到多个订阅者。
            // 我们不支持, 而 $share/... 恰好是一个语法合法的过滤器 —— 若照常接受,
            // 订阅会成功但<b>永远匹配不到任何发布</b>($share 是保留前缀, 不会被当作普通主题层),
            // 表现为静默丢消息。v5 有专用原因码, 必须明确拒绝。
            if (topicFilter != null && topicFilter.startsWith("$share/")) {
                log.debug("不支持共享订阅, clientId={} topicFilter={}", clientId, topicFilter);
                codes[i] = v5 ? ReplyFactory.SHARED_SUBSCRIPTIONS_NOT_SUPPORTED
                        : ReplyFactory.V3_SUBSCRIBE_FAILURE;
                continue;
            }

            if (!MqttTopic.isValidFilter(topicFilter)) {
                log.debug("非法主题过滤器, clientId={} topicFilter={}", clientId, topicFilter);
                // 失败码按版本选: v3.1.1 只有 0x80, v5 有专门的「过滤器非法」0x8F
                codes[i] = v5 ? ReplyFactory.TOPIC_FILTER_INVALID : ReplyFactory.V3_SUBSCRIBE_FAILURE;
                continue;
            }

            aclPending.add(i);
        }

        // superuser / 全部过滤器已在静态校验拒绝 → 无 ACL 待判定, 直接走同步路径
        if (aclPending.isEmpty()
                || Boolean.TRUE.equals(channel.attr(ChannelAttributes.SUPERUSER).get())) {
            acceptSubscriptions(channel, clientId, v5, messageId, subscriptions, codes, existingFilters);
            return;
        }

        gateOf(channel).offer(channel, () -> checkThenAccept(
                channel, clientId, v5, messageId, subscriptions, codes, existingFilters, aclPending));
    }

    /**
     * 门内执行: 对全部待判定过滤器发 ACL 检查(缓存命中的同步完成),
     * 全部完成后合并结果并回 SUBACK。异步结果 hop 回连接的 EventLoop。
     */
    private void checkThenAccept(Channel channel, String clientId, boolean v5, int messageId,
                                 List<MqttTopicSubscription> subscriptions, Integer[] codes,
                                 Set<String> existingFilters, List<Integer> aclPending) {
        AclGate gate = channel.attr(ChannelAttributes.ACL_GATE).get();
        String username = channel.attr(ChannelAttributes.USERNAME).get();
        String peerhost = ChannelAttributes.peerhostOf(channel);

        @SuppressWarnings("unchecked")
        CompletableFuture<Boolean>[] checks = aclPending.stream()
                .map(i -> aclService.check(clientId, username, peerhost,
                        "subscribe", subscriptions.get(i).topicName()))
                .toArray(CompletableFuture[]::new);

        Runnable finish = () -> {
            for (int k = 0; k < aclPending.size(); k++) {
                int idx = aclPending.get(k);
                if (!join(checks[k])) {
                    log.debug("订阅被 ACL 拒绝 clientId={} topicFilter={}",
                            clientId, subscriptions.get(idx).topicName());
                    codes[idx] = v5 ? ReplyFactory.NOT_AUTHORIZED : ReplyFactory.V3_SUBSCRIBE_FAILURE;
                }
            }
            acceptSubscriptions(channel, clientId, v5, messageId, subscriptions, codes, existingFilters);
            gate.release();
        };

        boolean allDone = true;
        for (CompletableFuture<Boolean> check : checks) {
            allDone &= check.isDone();
        }
        if (allDone) {
            finish.run();
            return;
        }
        CompletableFuture.allOf(checks)
                .whenComplete((r, e) -> channel.eventLoop().execute(finish));
    }

    private static boolean join(CompletableFuture<Boolean> future) {
        try {
            return Boolean.TRUE.equals(future.join());
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * 第二遍: 按逐项结果接受订阅、回 SUBACK、投递保留消息。
     */
    private void acceptSubscriptions(Channel channel, String clientId, boolean v5, int messageId,
                                     List<MqttTopicSubscription> subscriptions, Integer[] codes,
                                     Set<String> existingFilters) {
        List<Integer> grantedQos = new ArrayList<>(subscriptions.size());
        List<MqttTopicSubscription> accepted = new ArrayList<>(subscriptions.size());
        List<Boolean> sendRetain = new ArrayList<>(subscriptions.size());

        for (int i = 0; i < subscriptions.size(); i++) {
            if (codes[i] != null) {
                grantedQos.add(codes[i]);
                continue;
            }
            MqttTopicSubscription subscription = subscriptions.get(i);
            String topicFilter = subscription.topicName();
            int qos = subscription.qualityOfService().value();
            MqttSubscriptionOption option = subscription.option();

            subscribeStoreService.put(new SubscribeStore(clientId, topicFilter, qos));
            // 只有需要保留的会话才真正落盘, 判断集中在 SessionPersistence 里
            sessionPersistence.subscriptionAdded(clientId, topicFilter, qos);
            grantedQos.add(qos);
            accepted.add(subscription);
            sendRetain.add(shouldSendRetain(option, existingFilters.contains(topicFilter)));
        }

        // 即使全部过滤器都失败也<b>不断连接</b>, 只回逐项失败码 —— 与静态校验同一口径。
        channel.writeAndFlush(ReplyFactory.subAck(channel, messageId, grantedQos));

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

    private static AclGate gateOf(Channel channel) {
        AclGate gate = channel.attr(ChannelAttributes.ACL_GATE).get();
        if (gate == null) {
            gate = new AclGate();
            channel.attr(ChannelAttributes.ACL_GATE).set(gate);
        }
        return gate;
    }
}
