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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import online.ipuff.jmqtt.cluster.InternalMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Kafka 消息结构: JSON, 零 Kafka header。
 *
 * <p>broker → Kafka: {username, topic, timestamp, qos, payload(byte[]), node, clientid}
 * —— 集群广播与数据面上行同一份; 后台 → broker: {topic, qos, payload(byte[])}。
 * payload 为 byte[], JSON 中以 base64 承载(Jackson 缺省), 二进制无损。
 */
class KafkaClusterMessageFrameTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    @DisplayName("集群广播: key=MQTT主题, 零 header, value=完整 JSON 结构")
    void broadcastStructure() throws Exception {
        byte[] payload = {0x00, 0x22, (byte) 0xFF, 0x5C, 0x0A};
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
        assertArrayEquals(payload, Base64.getDecoder().decode(node.get("payload").asText()),
                "payload 以 base64 承载, 二进制无损");

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
        byte[] payload = {0x01, 0x02};
        InternalMessage msg = new InternalMessage("node-9", "device-7",
                "telemetry/raw", 2, payload, false, false, "u");

        ProducerRecord<String, byte[]> record =
                ClusterRecords.toRoutedRecord("jmqtt-uplink", "device-7", msg);
        assertEquals("device-7", record.key(), "上行按路由配置分区");
        assertFalse(record.headers().iterator().hasNext(), "零 Kafka header");
        assertEquals("telemetry/raw", JSON.readTree(record.value()).get("topic").asText(),
                "topic 在 JSON 里, 不依赖 key");

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
    @DisplayName("后台下行: JSON {topic, qos, payload(byte[])}")
    void downlinkUsesJsonStructure() {
        byte[] payload = {0x7F, 0x00, (byte) 0xFE};
        String body = "{\"topic\":\"cmd/device-42\",\"qos\":2,\"payload\":\""
                + Base64.getEncoder().encodeToString(payload) + "\"}";
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "jmqtt-downlink", 0, 0L, "partition-key-only", body.getBytes(StandardCharsets.UTF_8));

        InternalMessage decoded = ClusterRecords.downlinkMessage(record);
        assertNotNull(decoded);
        assertEquals("cmd/device-42", decoded.topic());
        assertEquals(2, decoded.qos());
        assertArrayEquals(payload, decoded.payload(), "二进制 payload 无损");
        assertNull(decoded.clientId(), "下行消息无来源客户端, 不得排除任何订阅者");

        // qos 缺省 1
        ConsumerRecord<String, byte[]> noQos = new ConsumerRecord<>(
                "jmqtt-downlink", 0, 0L, "k",
                "{\"topic\":\"cmd/device-42\",\"payload\":\"\"}".getBytes(StandardCharsets.UTF_8));
        assertEquals(1, ClusterRecords.downlinkMessage(noQos).qos());
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
