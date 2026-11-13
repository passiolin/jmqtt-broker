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

import com.fasterxml.jackson.databind.ObjectMapper;
import online.ipuff.jmqtt.TestBrokerProperties;
import online.ipuff.jmqtt.cluster.InternalMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 异步编码器的顺序约束: 同一分区 key 的消息必须按提交顺序进入 outbox。
 *
 * <p>多分片并行编码时, 顺序保证来自「同 key 固定同一分片 + 单线程 FIFO」;
 * 这条测试就是防止后续把分片改成随机/轮询。
 */
class KafkaRecordSerializerTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    @DisplayName("同一 key 的消息跨分片编码后仍保持提交顺序")
    void preservesOrderPerKey() throws Exception {
        KafkaMessageProducer producer = new KafkaMessageProducer(TestBrokerProperties.create());
        KafkaRecordSerializer serializer = new KafkaRecordSerializer(producer, 4);
        serializer.start();
        try {
            for (int i = 0; i < 200; i++) {
                serializer.publishRouted("jmqtt-uplink", "device-1", message(i));
            }
            serializer.flush();

            List<Integer> order = new ArrayList<>();
            ProducerRecord<String, byte[]> record;
            while ((record = producer.outbox.poll()) != null) {
                assertEquals("device-1", record.key());
                ClusterRecords.BrokerMessage body =
                        JSON.readValue(record.value(), ClusterRecords.BrokerMessage.class);
                order.add(Integer.parseInt(new String(body.payload(), StandardCharsets.UTF_8)));
            }
            assertEquals(200, order.size(), "所有消息都应被编码");
            for (int i = 0; i < order.size(); i++) {
                assertEquals(i, order.get(i), "同 key 顺序不得改变");
            }
        } finally {
            serializer.stop();
        }
    }

    @Test
    @DisplayName("时间戳在提交时捕获, 不受编码排队影响")
    void timestampCapturedAtSubmit() throws Exception {
        KafkaMessageProducer producer = new KafkaMessageProducer(TestBrokerProperties.create());
        KafkaRecordSerializer serializer = new KafkaRecordSerializer(producer, 1);
        serializer.start();
        try {
            long before = System.currentTimeMillis();
            serializer.publishBroadcast("jmqtt-cluster", message(1));
            serializer.flush();
            ProducerRecord<String, byte[]> record = producer.outbox.poll();
            assertNotNull(record);
            ClusterRecords.BrokerMessage body =
                    JSON.readValue(record.value(), ClusterRecords.BrokerMessage.class);
            assertTrue(body.timestamp() >= before);
        } finally {
            serializer.stop();
        }
    }

    private static InternalMessage message(int seq) {
        return new InternalMessage("node-1", "device-1", "bench/device-1/event/up", 1,
                String.valueOf(seq).getBytes(StandardCharsets.UTF_8), false, false, "u");
    }
}
