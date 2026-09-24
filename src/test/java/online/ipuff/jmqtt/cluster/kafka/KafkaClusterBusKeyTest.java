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

import online.ipuff.jmqtt.TestBrokerProperties;
import online.ipuff.jmqtt.config.BrokerProperties;
import online.ipuff.jmqtt.cluster.InternalMessage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 集群总线的 record key 约定:
 *
 * <ul>
 *   <li><b>消息面(集群广播) key 恒为 MQTT 主题</b> —— 这是「同主题必落同分区,
 *       分区内有序 ⇒ 同主题有序」这条链的起点, 也是消费侧按分区分派 worker
 *       能够保序的前提。数据面的 uplink-key 配置(按设备分区)只属于数据面,
 *       不得泄漏进来 —— 否则改一个数据面参数会悄悄破坏消息面的顺序保证。</li>
 *   <li><b>数据面(上行出口) key 按 uplink-key 配置</b>(默认主题, 可选设备)。</li>
 * </ul>
 */
class KafkaClusterBusKeyTest {

    private static KafkaClusterBus bus(BrokerProperties.KafkaProperties kafka) {
        return new KafkaClusterBus(TestBrokerProperties.create(
                        "node-1", 32, 1000, kafka),
                null, (clientId, fromNodeId) -> { }, new online.ipuff.jmqtt.subscribe.SubscribeStoreService());
    }

    private static InternalMessage message(String clientId, String topic) {
        return new InternalMessage("node-1", clientId, topic, 1,
                "x".getBytes(StandardCharsets.UTF_8), false, false, null);
    }

    private static org.apache.kafka.clients.producer.ProducerRecord<String, byte[]>
    take(KafkaClusterBus bus) throws InterruptedException {
        return bus.outbox.poll(2, TimeUnit.SECONDS);
    }

    @Test
    @DisplayName("消息面 key 恒为 MQTT 主题 —— 即使数据面配了按设备分区")
    void clusterBroadcastKeyIsAlwaysTopic() throws InterruptedException {
        BrokerProperties.KafkaProperties deviceKey = new BrokerProperties.KafkaProperties(
                true, "127.0.0.1:9092", "jmqtt", "jmqtt-cluster", null,
                true, List.of(), "", List.of(), false, 1000, 1000, 200, 1,
                "none", "latest", "device", null, null, null);
        KafkaClusterBus bus = bus(deviceKey);

        bus.publish(message("device-42", "sensor/room1/temp"));

        org.apache.kafka.clients.producer.ProducerRecord<String, byte[]> record = take(bus);
        assertEquals("sensor/room1/temp", record.key(),
                "消息面 key 必须是 MQTT 主题, uplink-key=device 只作用于数据面");
    }

    @Test
    @DisplayName("数据面 key 按 uplink-key 配置: device 模式下键是设备标识")
    void uplinkKeyFollowsConfig() throws InterruptedException {
        BrokerProperties.KafkaProperties deviceKey = new BrokerProperties.KafkaProperties(
                true, "127.0.0.1:9092", "jmqtt", "jmqtt-cluster", null,
                true, List.of(), "jmqtt-uplink", List.of(), false, 1000, 1000, 200, 1,
                "none", "latest", "device", null, null, null);
        KafkaClusterBus bus = bus(deviceKey);

        bus.publishUplink(message("device-42", "sensor/room1/temp"));

        org.apache.kafka.clients.producer.ProducerRecord<String, byte[]> record = take(bus);
        assertEquals("device-42", record.key(), "数据面按设备分区时 key 是设备标识");
    }

    @Test
    @DisplayName("数据面默认(topic 模式)key 是 MQTT 主题")
    void uplinkKeyDefaultIsTopic() throws InterruptedException {
        BrokerProperties.KafkaProperties topicKey = new BrokerProperties.KafkaProperties(
                true, "127.0.0.1:9092", "jmqtt", "jmqtt-cluster", null,
                true, List.of(), "jmqtt-uplink", List.of(), false, 1000, 1000, 200, 1,
                "none", "latest", "topic", null, null, null);
        KafkaClusterBus bus = bus(topicKey);

        bus.publishUplink(message("device-42", "sensor/room1/temp"));

        org.apache.kafka.clients.producer.ProducerRecord<String, byte[]> record = take(bus);
        assertEquals("sensor/room1/temp", record.key());
    }
}
