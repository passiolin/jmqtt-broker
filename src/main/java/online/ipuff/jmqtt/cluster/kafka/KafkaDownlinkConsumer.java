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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 后台下行消费者: 每节点独立 group({@code <groupId>-downlink})消费下行 topic,
 * 只做本节点投递, 绝不回写出站 —— 回环防护是结构性的。
 *
 * <p>worker 池与集群广播同构: 按分区固定 worker, 同 key 恒落同分区 ⇒
 * 分区内投递严格有序, 并行只发生在不同分区之间。
 */
final class KafkaDownlinkConsumer extends PartitionedConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaDownlinkConsumer.class);

    private final InternalSendServer internalSendServer;
    private final List<String> topics;

    KafkaDownlinkConsumer(BrokerProperties properties,
                          InternalSendServer internalSendServer,
                          List<String> topics) {
        super(properties);
        this.internalSendServer = internalSendServer;
        this.topics = List.copyOf(topics);
    }

    /** 过滤出合法的下行通道 topic; 未配置时返回空列表(调用方不启动本消费者)。 */
    static List<String> topicsOf(BrokerProperties.KafkaProperties kafka) {
        List<String> topics = new ArrayList<>();
        if (kafka.downlink() == null) {
            return topics;
        }
        for (BrokerProperties.KafkaProperties.Downlink downlink : kafka.downlink()) {
            if (downlink.topic() == null || downlink.topic().isBlank()) {
                log.warn("忽略未配置 topic 的下行通道: {}", downlink);
                continue;
            }
            topics.add(downlink.topic());
        }
        return topics;
    }

    @Override
    protected String threadName() {
        return "jmqtt-kafka-downlink";
    }

    @Override
    protected String workerNamePrefix() {
        return "jmqtt-kafka-downlink-worker-";
    }

    @Override
    protected String groupIdSuffix() {
        return "-downlink";
    }

    @Override
    protected String clientIdSuffix() {
        return "-downlink";
    }

    @Override
    protected List<String> topics() {
        return topics;
    }

    @Override
    protected void handle(ConsumerRecord<String, byte[]> record) {
        InternalMessage message = ClusterRecords.downlinkMessage(record);
        if (message == null) {
            log.debug("丢弃无法解码的下行记录: topic={} offset={}", record.topic(), record.offset());
            return;
        }
        if (internalSendServer == null) {
            return;
        }
        int delivered = internalSendServer.sendPublishMessage(message);
        if (log.isDebugEnabled()) {
            // 与入站报文日志同一规范: DOWNLINK 前缀 + key=value + payload 截断预览
            log.debug("DOWNLINK clientId=unknown type=PUBLISH topic={} qos={} size={} 本地投递={} payload={}",
                    message.topic(), message.qos(), message.payload().length, delivered,
                    online.ipuff.jmqtt.util.Payloads.preview(message.payload()));
        }
    }
}
