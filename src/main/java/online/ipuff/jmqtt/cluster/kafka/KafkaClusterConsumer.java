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
package online.ipuff.jmqtt.cluster.kafka;

import online.ipuff.jmqtt.cluster.InternalMessage;
import online.ipuff.jmqtt.cluster.InternalSendServer;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.subscribe.ISubscribeStoreService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 集群广播消费者: 每节点独立 group 消费 {@code cluster-topic},
 * 收到的消息只做本节点投递, 绝不回写出站。
 *
 * <ul>
 *   <li>早期过滤: 本地订阅树无匹配则跳过反序列化与投递(跨节点记录大多与本节点无关)</li>
 *   <li>自回环: 结构里的 node 就是发布节点 id, 与自身相同则跳过
 *       (发布路径已完成过一次本地投递)</li>
 * </ul>
 */
final class KafkaClusterConsumer extends PartitionedConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaClusterConsumer.class);

    private final InternalSendServer internalSendServer;
    private final ISubscribeStoreService subscribeStoreService;

    private final AtomicLong receivedCount = new AtomicLong();
    private final AtomicLong skippedSelfCount = new AtomicLong();
    private final AtomicLong skippedNoLocalSubCount = new AtomicLong();

    KafkaClusterConsumer(BrokerProperties properties,
                         InternalSendServer internalSendServer,
                         ISubscribeStoreService subscribeStoreService) {
        super(properties);
        this.internalSendServer = internalSendServer;
        this.subscribeStoreService = subscribeStoreService;
    }

    @Override
    protected String threadName() {
        return "jmqtt-kafka-consumer";
    }

    @Override
    protected String workerNamePrefix() {
        return "jmqtt-kafka-worker-";
    }

    @Override
    protected String groupIdSuffix() {
        return "";
    }

    @Override
    protected String clientIdSuffix() {
        return "-consumer";
    }

    @Override
    protected List<String> topics() {
        return List.of(properties.kafka().clusterTopic());
    }

    @Override
    protected void handle(ConsumerRecord<String, byte[]> record) {
        // 早期过滤: 广播 record 的 key 即 MQTT 主题。先查本地订阅树,
        // 无匹配订阅者则跳过反序列化和投递 —— 30 万连接下大多数消息
        // 只涉及本节点 1~2 个订阅者, 远端节点的无效消费占了大量 CPU。
        String topic = record.key();
        if (topic != null && !subscribeStoreService.hasSubscribers(topic)) {
            skippedNoLocalSubCount.incrementAndGet();
            return;
        }
        InternalMessage message = ClusterRecords.fromRecord(record);
        if (message == null) {
            log.debug("丢弃无法解码的集群记录, offset={}", record.offset());
            return;
        }
        // 跳过本节点自己发出的消息: 发布路径已完成过一次本地投递,
        // 再取回来会重复投递。来源标识在结构的 node 字段里。
        if (properties.id().equals(message.brokerId())) {
            skippedSelfCount.incrementAndGet();
            return;
        }
        receivedCount.incrementAndGet();
        // 只做本节点投递。绝不调用出站路径 —— 这是回环防护的结构性保证。
        int delivered = internalSendServer.sendPublishMessage(message);
        if (log.isDebugEnabled()) {
            log.debug("总线消息投递: topic={} 本节点投递={}", message.topic(), delivered);
        }
    }

    long received() {
        return receivedCount.get();
    }

    long skippedSelf() {
        return skippedSelfCount.get();
    }

    long skippedNoLocalSub() {
        return skippedNoLocalSubCount.get();
    }
}
