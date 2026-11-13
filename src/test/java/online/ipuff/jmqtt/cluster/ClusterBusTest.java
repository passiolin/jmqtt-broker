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

import online.ipuff.jmqtt.cluster.kafka.ClusterRecords;
import online.ipuff.jmqtt.config.BrokerProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 集群总线的路由决策与 record 编解码测试。
 *
 * <p>这里刻意绕开真实 Kafka: 被测的是<b>我们的逻辑</b>——出站分流、
 * 上行独占、以及统一 JSON 记录结构(MQTT 主题在 JSON 里, key 仅用于分区, 零 header)。
 *
 * <p>其中「上行独占时不进集群广播」这一条最关键: 它决定了上行数据是否要
 * 承受 N 倍扇出。这条判断错了, 百万连接下就是 N 倍的冤枉带宽。
 */
class ClusterBusTest {

    // ------------------------------------------------------------------
    // 出站分流
    // ------------------------------------------------------------------

    @Test
    @DisplayName("总线未启用时, 出站不产生任何投递")
    void disabledBusPublishesNothing() {
        RecordingBus bus = new RecordingBus(false, false, null);
        InternalCommunication communication = new InternalCommunication(bus, props(kafka(false, "", List.of(), false)));

        communication.internalSend(message("sensor/room1/temp"));

        assertTrue(bus.published.isEmpty(), "未启用总线时不应有出站投递");
    }

    @Test
    @DisplayName("未开启数据面时, 消息只进集群广播")
    void onlyClusterWhenUplinkDisabled() {
        RecordingBus bus = new RecordingBus(true, false, null);
        InternalCommunication communication = new InternalCommunication(bus, props(kafka(true, "", List.of(), false)));

        communication.internalSend(message("sensor/room1/temp"));

        assertEquals(1, bus.published.size(), "应进集群广播");
        assertEquals(0, bus.uplinkPublished.size(), "未开启数据面不应有上行投递");
    }

    @Test
    @DisplayName("uplink-exclusive=false: 命中上行的消息同时进数据面与集群广播")
    void uplinkAndClusterWhenNotExclusive() {
        RecordingBus bus = new RecordingBus(true, true, null);
        InternalCommunication communication =
                new InternalCommunication(bus, props(kafka(true, "jmqtt-uplink", List.of(), false)));

        communication.internalSend(message("sensor/room1/temp"));

        assertEquals(1, bus.uplinkPublished.size(), "应进数据面");
        assertEquals(1, bus.published.size(), "非独占时也应进集群广播");
    }

    @Test
    @DisplayName("uplink-exclusive=true: 命中上行的消息只进数据面, 不进集群广播")
    void exclusiveUplinkSkipsCluster() {
        RecordingBus bus = new RecordingBus(true, true, null);
        InternalCommunication communication =
                new InternalCommunication(bus, props(kafka(true, "jmqtt-uplink", List.of(), true)));

        communication.internalSend(message("sensor/room1/temp"));

        assertEquals(1, bus.uplinkPublished.size(), "应进数据面");
        assertEquals(0, bus.published.size(), "独占时不应进集群广播(避免 N 倍扇出)");
    }

    @Test
    @DisplayName("uplink-exclusive=true 时, 未命中上行的消息仍走集群广播")
    void exclusiveUplinkStillBroadcastsNonUplink() {
        // 只有 sensor/# 算上行, 所以 device/cmd 走集群广播
        RecordingBus bus = new RecordingBus(true, true, "sensor/");
        InternalCommunication communication =
                new InternalCommunication(bus, props(kafka(true, "jmqtt-uplink", List.of(), true)));

        communication.internalSend(message("device/abc/cmd"));
        assertEquals(1, bus.published.size(), "下行指令应走集群广播");
        assertEquals(0, bus.uplinkPublished.size(), "下行指令不应进数据面");

        bus.published.clear();
        communication.internalSend(message("sensor/room1/temp"));
        assertEquals(0, bus.published.size(), "上行数据不应走集群广播");
        assertEquals(1, bus.uplinkPublished.size(), "上行数据应进数据面");
    }

    @Test
    @DisplayName("广播被策略拒绝时: 不进消息面, 但数据面上行照发")
    void broadcastDeniedStillSendsUplink() {
        // 这正是「设备只与服务端通信」的部署形态: 遥测要落 Kafka 给服务端消费,
        // 但没有任何其他节点在对它做 MQTT 订阅, 于是集群广播纯属多余开销。
        // 关键在于: 关掉的只能是消息面, 数据面必须照常 —— 否则就是把「省成本」
        // 做成了「丢数据」。
        RecordingBus bus = new RecordingBus(true, true, null, false);
        InternalCommunication communication =
                new InternalCommunication(bus, props(kafka(true, "jmqtt-uplink", List.of(), false)));

        communication.internalSend(message("sensor/room1/temp"));

        assertEquals(0, bus.published.size(), "广播被拒时不应进消息面");
        assertEquals(1, bus.uplinkPublished.size(), "数据面上行不应受影响");
    }

    @Test
    @DisplayName("广播被策略拒绝时: 不抛异常, 也不影响本节点投递链路")
    void broadcastDeniedIsSilent() {
        RecordingBus bus = new RecordingBus(true, false, null, false);
        InternalCommunication communication =
                new InternalCommunication(bus, props(kafka(true, "", List.of(), false)));

        // 出站被静默跳过是刻意的: 这个判断在每条消息的出站路径上,
        // 若改成抛异常或打日志, 高频遥测会把异常栈与日志写满
        communication.internalSend(message("sensor/room1/temp"));
        communication.internalSend(message("sensor/room1/temp"));

        assertEquals(0, bus.published.size());
    }

    @Test
    @DisplayName("广播被策略拒绝时: 总线未启用与广播关闭是两种不同原因, 都不产生消息面投递")
    void broadcastDeniedAndBusDisabled() {
        RecordingBus denied = new RecordingBus(true, false, null, false);
        new InternalCommunication(denied, props(kafka(true, "", List.of(), false)))
                .internalSend(message("a/b"));
        RecordingBus disabled = new RecordingBus(false, false, null, true);
        new InternalCommunication(disabled, props(kafka(false, "", List.of(), false)))
                .internalSend(message("a/b"));

        assertEquals(0, denied.published.size());
        assertEquals(0, disabled.published.size());
    }

    @Test
    @DisplayName("空主题消息被丢弃, 不进任何链路")
    void emptyTopicDropped() {
        RecordingBus bus = new RecordingBus(true, true, null);
        InternalCommunication communication =
                new InternalCommunication(bus, props(kafka(true, "jmqtt-uplink", List.of(), false)));

        communication.internalSend(new InternalMessage("node-1", "c1", "", 0, new byte[0], false, false, null));

        assertEquals(0, bus.published.size());
        assertEquals(0, bus.uplinkPublished.size());
    }

    @Test
    @DisplayName("fromLocal 使用 broker id 作为来源标识(消费端据此跳过自身消息)")
    void fromLocalCarriesBrokerId() {
        BrokerProperties properties = props(kafka(false, "", List.of(), false));
        InternalCommunication communication = new InternalCommunication(new RecordingBus(false, false, null), properties);

        InternalMessage message = communication.fromLocal("c1", "a/b", 1, new byte[]{1}, true, false, "u1");

        assertEquals(properties.id(), message.brokerId());
        assertEquals("c1", message.clientId());
        assertEquals("a/b", message.topic());
        assertEquals(1, message.qos());
        assertTrue(message.retain());
        assertFalse(message.dup());
    }

    // ------------------------------------------------------------------
    // record 编解码
    // ------------------------------------------------------------------

    @Test
    @DisplayName("record 的 key 必须是 MQTT 主题, 零 header, value = JSON 结构")
    void recordKeyIsMqttTopic() {
        InternalMessage message = new InternalMessage("node-1", "c1", "sensor/room1/temp", 1,
                "23.5".getBytes(StandardCharsets.UTF_8), false, false, null);

        ProducerRecord<String, byte[]> record = ClusterRecords.toRecord("jmqtt-cluster", message);

        assertEquals("jmqtt-cluster", record.topic());
        assertEquals("sensor/room1/temp", record.key(), "key 必须是 MQTT 主题");
        assertFalse(record.headers().iterator().hasNext(), "零 Kafka header");
        InternalMessage decoded = ClusterRecords.fromRecord(toConsumerRecord(record));
        assertNotNull(decoded);
        assertEquals("sensor/room1/temp", decoded.topic(), "主题在结构里");
        assertEquals(1, decoded.qos());
        assertArrayEquals("23.5".getBytes(StandardCharsets.UTF_8), decoded.payload(),
                "payload 原样字节无损");
    }

    @Test
    @DisplayName("编解码往返不丢字段")
    void codecRoundTrip() {
        InternalMessage original = new InternalMessage("node-7", "device-42", "sensor/room1/temp", 2,
                new byte[]{0x01, 0x02, 0x03}, true, true, null);

        ConsumerRecord<String, byte[]> record = toConsumerRecord(
                ClusterRecords.toRecord("jmqtt-cluster", original));
        InternalMessage decoded = ClusterRecords.fromRecord(record);

        assertNotNull(decoded);
        assertEquals(original.brokerId(), decoded.brokerId(), "node 字段往返");
        assertEquals(original.clientId(), decoded.clientId());
        assertEquals(original.username(), decoded.username());
        assertEquals(original.topic(), decoded.topic());
        assertEquals(original.qos(), decoded.qos());
        assertArrayEquals(original.payload(), decoded.payload());
        assertFalse(decoded.retain(), "结构不携带 retain 标志");
        assertFalse(decoded.dup(), "结构不携带 dup 标志");
    }

    @Test
    @DisplayName("uplink-key 配置: 默认按主题, device 时按设备, 设备缺失时回退为主题")
    void uplinkPartitionKeySelection() {
        BrokerProperties.KafkaProperties byTopic =
                kafka(true, "jmqtt-uplink", List.of(), true, "topic");
        assertEquals("telemetry/raw", byTopic.uplinkPartitionKey("device-a", "telemetry/raw"),
                "默认按主题分区");

        BrokerProperties.KafkaProperties byDevice =
                kafka(true, "jmqtt-uplink", List.of(), true, "device");
        assertEquals("device-a", byDevice.uplinkPartitionKey("device-a", "telemetry/raw"),
                "device 模式按设备分区");
        // 服务端主动下发的消息没有来源客户端 —— 必须回退而不是发 null key:
        // null key 会让 Kafka 在各分区轮转, 同一设备的消息散开, 有序性直接消失
        assertEquals("telemetry/raw", byDevice.uplinkPartitionKey(null, "telemetry/raw"),
                "设备缺失时回退为主题");
        assertEquals("telemetry/raw", byDevice.uplinkPartitionKey("  ", "telemetry/raw"),
                "空白设备标识同样回退");
    }

    @Test
    @DisplayName("value 缺失或非法 JSON 的记录被拒绝")
    void codecRejectsEmptyValue() {
        assertNull(ClusterRecords.fromRecord(
                new ConsumerRecord<>("jmqtt-cluster", 0, 0L, "a/b", new byte[0])));
        assertNull(ClusterRecords.fromRecord(
                new ConsumerRecord<>("jmqtt-cluster", 0, 0L, "a/b", null)));
    }

    @Test
    @DisplayName("key 不影响解码 —— 主题在 JSON 里, key 只是分区键")
    void codecIgnoresKey() {
        ProducerRecord<String, byte[]> producerRecord = ClusterRecords.toRecord(
                "jmqtt-cluster", new InternalMessage("node-1", "c1", "a/b", 0,
                        new byte[0], false, false, null));
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "jmqtt-cluster", 0, 0L, null, producerRecord.value());
        InternalMessage decoded = ClusterRecords.fromRecord(record);
        assertNotNull(decoded, "主题在 value 里, key 为空不影响解码");
        assertEquals("a/b", decoded.topic());
    }

    // ------------------------------------------------------------------
    // 控制消息: 跨节点连接接管
    // ------------------------------------------------------------------

    // ------------------------------------------------------------------
    // 测试替身
    // ------------------------------------------------------------------

    /** 记录所有出站调用的假总线 */
    static class RecordingBus implements ClusterBus {

        final List<InternalMessage> published = new ArrayList<>();
        final List<InternalMessage> uplinkPublished = new ArrayList<>();

        private final boolean enabled;
        private final boolean uplinkEnabled;
        /** null 表示所有主题都算上行 */
        private final String uplinkPrefix;
        private final boolean broadcastAllowed;

        RecordingBus(boolean enabled, boolean uplinkEnabled, String uplinkPrefix) {
            this(enabled, uplinkEnabled, uplinkPrefix, true);
        }

        RecordingBus(boolean enabled, boolean uplinkEnabled, String uplinkPrefix,
                     boolean broadcastAllowed) {
            this.enabled = enabled;
            this.uplinkEnabled = uplinkEnabled;
            this.uplinkPrefix = uplinkPrefix;
            this.broadcastAllowed = broadcastAllowed;
        }

        @Override
        public boolean enabled() {
            return enabled;
        }

        @Override
        public void publish(InternalMessage message) {
            published.add(message);
        }

        @Override
        public boolean uplinkEnabled() {
            return uplinkEnabled;
        }

        @Override
        public boolean isUplink(String topic) {
            if (!uplinkEnabled || topic == null) {
                return false;
            }
            return uplinkPrefix == null || topic.startsWith(uplinkPrefix);
        }

        @Override
        public boolean isBroadcastAllowed(String topic) {
            return broadcastAllowed;
        }

        @Override
        public void publishUplink(InternalMessage message) {
            uplinkPublished.add(message);
        }
    }

    // ------------------------------------------------------------------
    // 构造辅助
    // ------------------------------------------------------------------

    static InternalMessage message(String topic) {
        return new InternalMessage("node-1", "c1", topic, 1, "payload".getBytes(StandardCharsets.UTF_8), false, false, null);
    }

    static ConsumerRecord<String, byte[]> toConsumerRecord(ProducerRecord<String, byte[]> producerRecord) {
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                producerRecord.topic(), 0, 0L, producerRecord.key(), producerRecord.value());
        producerRecord.headers().forEach(header -> record.headers().add(header));
        return record;
    }

    static BrokerProperties.KafkaProperties kafka(boolean enabled, String uplinkTopic,
                                                  List<String> uplinkFilters, boolean uplinkExclusive) {
        return kafka(enabled, uplinkTopic, uplinkFilters, uplinkExclusive, "topic");
    }

    static BrokerProperties.KafkaProperties kafka(boolean enabled, String uplinkTopic,
                                                  List<String> uplinkFilters, boolean uplinkExclusive,
                                                  String uplinkKey) {
        return new BrokerProperties.KafkaProperties(
                enabled,
                "127.0.0.1:9092",
                "jmqtt",
                "jmqtt-cluster",
                null,
                true, List.of(),
                uplinkTopic,
                uplinkFilters,
                uplinkExclusive,
                1000,
                1000,
                200,
                1,
                "none",
                "latest",
                uplinkKey,
                null, null, null);
    }

    /** 广播策略相关的配置: 直接暴露开关与过滤器 */
    static BrokerProperties.KafkaProperties kafkaBroadcast(boolean broadcastEnabled,
                                                          List<String> broadcastFilters) {
        return new BrokerProperties.KafkaProperties(
                true,
                "127.0.0.1:9092",
                "jmqtt",
                "jmqtt-cluster",
                null,
                broadcastEnabled,
                broadcastFilters,
                "",
                List.of(),
                false,
                1000,
                1000,
                200,
                1,
                "none",
                "latest",
                "topic",
                null, null, null);
    }

    static BrokerProperties props(BrokerProperties.KafkaProperties kafka) {
        return new BrokerProperties(
                "node-1",
                null,
                1883,
                true,
                8083,
                "/mqtt",
                1,
                2,
                false,
                4096,
                false,
                "jmqtt",
                "jmqtt",
                60,
                7200,
                32,
                1000,
                1000,
                false,
                kafka,
                new BrokerProperties.RedisProperties(false, "127.0.0.1", 6379, null, 0, "jmqtt", 604800, 1000, 5000, "off", 100, 10000, "standalone", null, null),
                511,
                true,
                true,
                10485760,
                32768,
                65536);
    }
}
