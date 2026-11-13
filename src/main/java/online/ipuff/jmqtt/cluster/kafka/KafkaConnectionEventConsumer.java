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
package online.ipuff.jmqtt.cluster.kafka;

import online.ipuff.jmqtt.cluster.ConnectionEvent;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.session.ConnectionEventTakeover;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 连接事件消费者: 每节点独立 group 读取连接事件, 发现「别的节点上线了同一 clientId」
 * 时释放本节点上的旧连接(跨节点接管)。
 *
 * <p>事件量只跟连接/断开次数相关, 与消息吞吐无关, 所以固定单 worker:
 * 抢占 worker 池没有收益, 反而会把每节点几十个线程浪费在一个低频 topic 上。
 *
 * <p>位点起点固定 latest: 历史事件回放没有意义 —— 当时的连接早已不在,
 * 用陈旧时间戳做接管判断只会制造噪声。丢一条事件的后果也只是晚到 keepalive 超时,
 * 不是正确性损失。
 */
final class KafkaConnectionEventConsumer extends PartitionedConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConnectionEventConsumer.class);

    private final ConnectionEventTakeover takeover;
    private final List<String> topics;

    KafkaConnectionEventConsumer(BrokerProperties properties,
                                 ConnectionEventTakeover takeover,
                                 String topic) {
        super(properties);
        this.takeover = takeover;
        this.topics = List.of(topic);
    }

    @Override
    protected int workerPoolSize() {
        return 1;
    }

    @Override
    protected String autoOffsetReset() {
        return "latest";
    }

    @Override
    protected String threadName() {
        return "jmqtt-kafka-events";
    }

    @Override
    protected String workerNamePrefix() {
        return "jmqtt-kafka-events-worker-";
    }

    @Override
    protected String groupIdSuffix() {
        return "-events";
    }

    @Override
    protected String clientIdSuffix() {
        return "-events";
    }

    @Override
    protected List<String> topics() {
        return topics;
    }

    @Override
    protected void handle(ConsumerRecord<String, byte[]> record) {
        // key=clientId: 先用它做一次本节点相关性预过滤, 绝大多数事件属于别的节点,
        // 不反序列化 JSON 就能丢掉
        String clientId = record.key();
        if (!takeover.shouldProcess(clientId)) {
            return;
        }
        ConnectionEvent event = ClusterRecords.connectionEvent(record);
        if (event == null) {
            log.debug("丢弃无法解码的连接事件: topic={} offset={}", record.topic(), record.offset());
            return;
        }
        takeover.onEvent(event);
    }
}
