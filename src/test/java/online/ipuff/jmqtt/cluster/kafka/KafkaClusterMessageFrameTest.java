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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import online.ipuff.jmqtt.cluster.InternalMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Kafka 消息结构: JSON, 零 Kafka header。
 *
 * <p>broker → Kafka: {username, topic, timestamp, qos, payload(string), node, clientid}
 * —— 集群广播与数据面上行同一份; 后台 → broker: {topic, qos, payload(string)}。
 * payload 为 UTF-8 文本直接承载(new String(bytes, UTF_8)), 不再 base64;
 * 契约要求 payload 为 UTF-8 文本, 非 UTF-8 字节编码时按 U+FFFD 替换。
 */
class KafkaClusterMessageFrameTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    @DisplayName("集群广播: key=MQTT主题, 零 header, value=完整 JSON 结构")
    void broadcastStructure() throws Exception {
        byte[] payload = ("{\"cmd\":3011,\"requestId\":\"win-device-list-1790651506999-120\","
                + "\"ts\":1790651506999,\"data\":{\"deviceId\":\"bba2cf1a203a43eb870ad944f523ec54\","
                + "\"params\":{\"dplist\":[1310976]}}}").getBytes(StandardCharsets.UTF_8);
        InternalMessage msg = new InternalMessage("node-1", "device-42",
                "device/42/telemetry", 1, payload, false, false, "app-user");

        ProducerRecord<String, byte[]> record = ClusterRecords.toRecord("jmqtt-cluster", msg);
        assertEquals("device/42/telemetry", record.key(), "广播按主题分区");
        assertFalse(record.headers().iterator().hasNext(), "零 Kafka header");

        // value 是 JSON, 字段齐全
        JsonNode node = JSON.readTree(record.value());
        assertEquals("app-user", node.get("username").asText());
        assertEquals("device/42/telemetry", node.get("topic").asText());
        assertTrue(node.get("timestamp").asLong() > 0);
        assertEquals(1, node.get("qos").asInt());
        assertEquals("node-1", node.get("node").asText(), "node = jmqtt-broker-id");
        assertEquals("device-42", node.get("clientid").asText());
        assertEquals(new String(payload, StandardCharsets.UTF_8), node.get("payload").asText(),
                "payload 以 UTF-8 文本承载(原文转义, 无 base64)");

        // 往返
        InternalMessage decoded = ClusterRecords.fromRecord(toConsumer(record));
        assertNotNull(decoded);
        assertEquals("app-user", decoded.username());
        assertEquals("device/42/telemetry", decoded.topic());
        assertEquals(1, decoded.qos());
        assertEquals("node-1", decoded.brokerId());
        assertEquals("device-42", decoded.clientId());
        assertArrayEquals(payload, decoded.payload());
    }

    @Test
    @DisplayName("数据面上行: 同一 JSON 结构, key 可以是设备标识(topic 在 JSON 里)")
    void uplinkUsesSameStructure() throws Exception {
        byte[] payload = "{\"seq\":7,\"t0\":42,\"did\":\"d1\"}".getBytes(StandardCharsets.UTF_8);
        InternalMessage msg = new InternalMessage("node-9", "device-7",
                "telemetry/raw", 2, payload, false, false, "u");

        ProducerRecord<String, byte[]> record =
                ClusterRecords.toRoutedRecord("jmqtt-uplink", "device-7", msg);
        assertEquals("device-7", record.key(), "上行按路由配置分区");
        assertFalse(record.headers().iterator().hasNext(), "零 Kafka header");
        JsonNode node = JSON.readTree(record.value());
        assertEquals("telemetry/raw", node.get("topic").asText(), "topic 在 JSON 里, 不依赖 key");
        assertEquals(new String(payload, StandardCharsets.UTF_8), node.get("payload").asText(),
                "payload 文本承载");

        InternalMessage decoded = ClusterRecords.fromRecord(toConsumer(record));
        assertNotNull(decoded);
        assertEquals("telemetry/raw", decoded.topic());
        assertEquals(2, decoded.qos());
        assertEquals("node-9", decoded.brokerId());
        assertEquals("device-7", decoded.clientId());
        assertEquals("u", decoded.username());
        assertArrayEquals(payload, decoded.payload());
    }

    @Test
    @DisplayName("后台下行: JSON {topic, qos, payload(string)}")
    void downlinkUsesJsonStructure() throws Exception {
        String payloadText = "{\"cmd\":3011,\"ts\":1790651506999}";
        String body = "{\"topic\":\"cmd/device-42\",\"qos\":2,\"payload\":"
                + JSON.writeValueAsString(payloadText) + "}";
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "jmqtt-downlink", 0, 0L, "partition-key-only", body.getBytes(StandardCharsets.UTF_8));

        InternalMessage decoded = ClusterRecords.downlinkMessage(record);
        assertNotNull(decoded);
        assertEquals("cmd/device-42", decoded.topic());
        assertEquals(2, decoded.qos());
        assertArrayEquals(payloadText.getBytes(StandardCharsets.UTF_8), decoded.payload(),
                "payload 文本经 JSON 转义往返无损");
        assertNull(decoded.clientId(), "下行消息无来源客户端, 不得排除任何订阅者");

        // qos 缺省 1
        ConsumerRecord<String, byte[]> noQos = new ConsumerRecord<>(
                "jmqtt-downlink", 0, 0L, "k",
                "{\"topic\":\"cmd/device-42\",\"payload\":\"\"}".getBytes(StandardCharsets.UTF_8));
        assertEquals(1, ClusterRecords.downlinkMessage(noQos).qos());
    }

    @Test
    @DisplayName("非 UTF-8 payload 按 U+FFFD 替换(契约要求 payload 为 UTF-8 文本)")
    void nonUtf8PayloadIsReplaced() {
        byte[] raw = {0x00, 0x22, (byte) 0xFF, 0x5C, 0x0A};
        InternalMessage msg = new InternalMessage("node-1", "device-42",
                "device/42/telemetry", 1, raw, false, false, "app-user");

        InternalMessage decoded = ClusterRecords.fromRecord(
                toConsumer(ClusterRecords.toRecord("jmqtt-cluster", msg)));
        byte[] replaced = new String(raw, StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(replaced, decoded.payload(),
                "0xFF 替换为 U+FFFD(EF BF BD); 引号/反斜杠/NUL/换行经 JSON 转义保留");
    }

    @Test
    @DisplayName("非法 JSON / 缺 topic → 丢弃")
    void invalidStructureDropped() {
        assertNull(ClusterRecords.downlinkMessage(new ConsumerRecord<>(
                "jmqtt-downlink", 0, 0L, "k", "not-json".getBytes(StandardCharsets.UTF_8))));
        assertNull(ClusterRecords.downlinkMessage(new ConsumerRecord<>(
                "jmqtt-downlink", 0, 0L, "k", "{\"qos\":1}".getBytes(StandardCharsets.UTF_8))));
        assertNull(ClusterRecords.fromRecord(new ConsumerRecord<>(
                "jmqtt-cluster", 0, 0L, "k", null)));
        assertNull(ClusterRecords.fromRecord(new ConsumerRecord<>(
                "jmqtt-cluster", 0, 0L, "k", "{}".getBytes(StandardCharsets.UTF_8))));
    }

    private static ConsumerRecord<String, byte[]> toConsumer(ProducerRecord<String, byte[]> record) {
        ConsumerRecord<String, byte[]> consumer = new ConsumerRecord<>(
                record.topic(), 0, 0L, record.key(), record.value());
        record.headers().forEach(h -> consumer.headers().add(h));
        return consumer;
    }
}
