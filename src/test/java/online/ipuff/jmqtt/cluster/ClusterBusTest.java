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
 * 上行独占、以及 record 的 key/header 约定。Kafka 本身的行为不需要我们验证。
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

        communication.internalSend(new InternalMessage("node-1", "c1", "", 0, new byte[0], false, false));

        assertEquals(0, bus.published.size());
        assertEquals(0, bus.uplinkPublished.size());
    }

    @Test
    @DisplayName("fromLocal 使用 broker id 作为来源标识(消费端据此跳过自身消息)")
    void fromLocalCarriesBrokerId() {
        BrokerProperties properties = props(kafka(false, "", List.of(), false));
        InternalCommunication communication = new InternalCommunication(new RecordingBus(false, false, null), properties);

        InternalMessage message = communication.fromLocal("c1", "a/b", 1, new byte[]{1}, true, false);

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
    @DisplayName("record 的 key 必须是 MQTT 主题 —— 分区顺序与主题还原都依赖它")
    void recordKeyIsMqttTopic() {
        InternalMessage message = new InternalMessage("node-1", "c1", "sensor/room1/temp", 1,
                "23.5".getBytes(StandardCharsets.UTF_8), false, false);

        ProducerRecord<String, byte[]> record = ClusterRecords.toRecord("jmqtt-cluster", message);

        assertEquals("jmqtt-cluster", record.topic());
        assertEquals("sensor/room1/temp", record.key(), "key 必须是 MQTT 主题");
        assertArrayEquals("23.5".getBytes(StandardCharsets.UTF_8), record.value(),
                "value 应保持原始字节, 不做 base64");
    }

    @Test
    @DisplayName("编解码往返不丢字段")
    void codecRoundTrip() {
        InternalMessage original = new InternalMessage("node-7", "device-42", "sensor/room1/temp", 2,
                new byte[]{0x01, 0x02, 0x03}, true, true);

        ConsumerRecord<String, byte[]> record = toConsumerRecord(
                ClusterRecords.toRecord("jmqtt-cluster", original));
        InternalMessage decoded = ClusterRecords.fromRecord(record);

        assertNotNull(decoded);
        assertEquals(original.brokerId(), decoded.brokerId());
        assertEquals(original.clientId(), decoded.clientId());
        assertEquals(original.topic(), decoded.topic());
        assertEquals(original.qos(), decoded.qos());
        assertTrue(decoded.retain());
        assertTrue(decoded.dup());
        assertArrayEquals(original.payload(), decoded.payload());
    }

    @Test
    @DisplayName("clientId 为空时解码为 null(服务端主动下发场景)")
    void codecHandlesNullClientId() {
        InternalMessage original = new InternalMessage("node-1", null, "device/cmd", 0,
                new byte[]{9}, false, false);

        InternalMessage decoded = ClusterRecords.fromRecord(
                toConsumerRecord(ClusterRecords.toRecord("jmqtt-cluster", original)));

        assertNotNull(decoded);
        assertNull(decoded.clientId());
        assertEquals("device/cmd", decoded.topic());
    }

    @Test
    @DisplayName("记录始终携带主题 header —— 主题不能再依赖 key 承载")
    void recordAlwaysCarriesTopicHeader() {
        InternalMessage message = new InternalMessage("node-1", "c1", "sensor/room1/temp", 1,
                new byte[0], false, false);

        ProducerRecord<String, byte[]> record = ClusterRecords.toRecord("jmqtt-cluster", message);

        assertEquals("sensor/room1/temp",
                headerValue(record, ClusterRecords.HEADER_TOPIC), "主题必须在 header 里");
    }

    @Test
    @DisplayName("数据面可按设备分区: key=设备标识, 主题仍能从 header 还原")
    void uplinkRecordKeyedByDevice() {
        InternalMessage message = new InternalMessage("node-1", "device-42", "telemetry/raw", 1,
                "23.5".getBytes(StandardCharsets.UTF_8), false, false);

        // 按设备分区: 键是设备, 不是主题
        ProducerRecord<String, byte[]> record =
                ClusterRecords.toRecord("jmqtt-uplink", "device-42", message);

        assertEquals("device-42", record.key(), "数据面的 key 应是设备标识");
        InternalMessage decoded = ClusterRecords.fromRecord(toConsumerRecord(record));
        assertNotNull(decoded, "主题在 header 里, 必须仍能还原");
        assertEquals("telemetry/raw", decoded.topic(),
                "主题取自 header, 不能因为 key 不是主题就丢失");
        assertEquals("device-42", decoded.clientId(), "设备标识不能丢");
    }

    @Test
    @DisplayName("同一设备的消息落同一分区, 不同设备散开")
    void uplinkKeySpreadsByDevice() {
        // 遥测集中的一个主题: 若按主题分区, 全部设备都会挤进同一个分区
        String sharedTopic = "telemetry/raw";

        ProducerRecord<String, byte[]> a1 = ClusterRecords.toRecord("jmqtt-uplink", "device-a",
                new InternalMessage("node-1", "device-a", sharedTopic, 1, new byte[0], false, false));
        ProducerRecord<String, byte[]> a2 = ClusterRecords.toRecord("jmqtt-uplink", "device-a",
                new InternalMessage("node-1", "device-a", sharedTopic, 1, new byte[0], false, false));
        ProducerRecord<String, byte[]> b1 = ClusterRecords.toRecord("jmqtt-uplink", "device-b",
                new InternalMessage("node-1", "device-b", sharedTopic, 1, new byte[0], false, false));

        assertEquals(a1.key(), a2.key(), "同一设备必须落同一分区, 否则它自己的时序会错乱");
        assertNotEquals(a1.key(), b1.key(), "不同设备应散到不同分区, 否则吞吐被单分区锁死");
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
    @DisplayName("没有主题 header 的历史记录仍能用 key 还原主题(向前兼容)")
    void fromRecordFallsBackToKey() {
        // 模拟「主题即分区键」时期写下的记录: 只有 key, 没有主题 header
        ConsumerRecord<String, byte[]> legacy = new ConsumerRecord<>(
                "jmqtt-cluster", 0, 0L, "sensor/room1/temp", new byte[]{1});
        legacy.headers().add(ClusterRecords.HEADER_BROKER_ID, "node-1".getBytes(StandardCharsets.UTF_8));

        InternalMessage decoded = ClusterRecords.fromRecord(legacy);

        assertNotNull(decoded, "历史记录不应因为缺 header 而整条丢弃");
        assertEquals("sensor/room1/temp", decoded.topic());
    }

    @Test
    @DisplayName("缺少来源标识的记录被拒绝(无法做回环防护)")
    void codecRejectsMissingBrokerId() {
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("jmqtt-cluster", 0, 0L, "a/b", new byte[]{1});
        assertNull(ClusterRecords.fromRecord(record));
    }

    @Test
    @DisplayName("key 为空但主题 header 存在时不丢消息")
    void codecAcceptsEmptyKeyWithTopicHeader() {
        ProducerRecord<String, byte[]> producerRecord =
                ClusterRecords.toRecord("jmqtt-cluster", new InternalMessage("node-1", "c1", "a/b", 0, new byte[0], false, false));
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>("jmqtt-cluster", 0, 0L, null, producerRecord.value());
        producerRecord.headers().forEach(h -> record.headers().add(h));

        // 不变式已经变了: 主题现在走 header, 所以「key 为空」不再等于「主题无法还原」。
        // 这条记录仍然可以被正确处理 —— 拒绝它反而是白白丢消息。
        InternalMessage decoded = ClusterRecords.fromRecord(record);
        assertNotNull(decoded, "主题在 header 里, key 为空不应导致丢消息");
        assertEquals("a/b", decoded.topic());
    }

    @Test
    @DisplayName("主题与 key 都取不到时才拒绝(否则无从投递)")
    void codecRejectsRecordWithoutAnyTopic() {
        // 既没有主题 header, 也没有 key —— 这才是真正无法还原主题的记录
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "jmqtt-cluster", 0, 0L, null, new byte[]{1});
        record.headers().add(ClusterRecords.HEADER_BROKER_ID, "node-1".getBytes(StandardCharsets.UTF_8));

        assertNull(ClusterRecords.fromRecord(record));
    }

    // ------------------------------------------------------------------
    // 控制消息: 跨节点连接接管
    // ------------------------------------------------------------------

    @Test
    @DisplayName("普通消息被识别为 PUBLISH 类别")
    void publishRecordKind() {
        ConsumerRecord<String, byte[]> record = toConsumerRecord(
                ClusterRecords.toRecord("jmqtt-cluster", message("a/b")));
        assertEquals(ClusterRecords.Kind.PUBLISH, ClusterRecords.kind(record));
    }

    @Test
    @DisplayName("接管指令被识别为 TAKEOVER 类别, 且能还原出目标节点")
    void takeoverRecordRoundTrip() {
        ConsumerRecord<String, byte[]> record = toConsumerRecord(
                ClusterRecords.takeoverRecord("jmqtt-cluster",
                        new ClusterRecords.Takeover("device-42", "node-old", "node-new")));

        assertEquals(ClusterRecords.Kind.TAKEOVER, ClusterRecords.kind(record));
        assertEquals("device-42", record.key(), "key 用 clientId: 同一客户端的接管指令按序生效");

        ClusterRecords.Takeover takeover = ClusterRecords.takeover(record);
        assertNotNull(takeover);
        assertEquals("device-42", takeover.clientId());
        assertEquals("node-old", takeover.targetNodeId(), "接管是定向消息, 必须带目标节点");
        assertEquals("node-new", takeover.fromNodeId());
    }

    @Test
    @DisplayName("缺失目标节点的接管指令被拒绝(否则所有节点都会去关连接)")
    void takeoverRejectsMissingTarget() {
        ProducerRecord<String, byte[]> producerRecord = ClusterRecords.takeoverRecord(
                "jmqtt-cluster", new ClusterRecords.Takeover("device-42", "", "node-new"));
        assertNull(ClusterRecords.takeover(toConsumerRecord(producerRecord)));
    }

    @Test
    @DisplayName("接管指令不会被误当作普通消息投递")
    void takeoverIsNotDeliveredAsPublish() {
        ConsumerRecord<String, byte[]> record = toConsumerRecord(
                ClusterRecords.takeoverRecord("jmqtt-cluster",
                        new ClusterRecords.Takeover("device-42", "node-old", "node-new")));

        // 接管记录没有 payload / qos, fromRecord 会因 key 非空而返回对象 ——
        // 因此消费端 MUST 先判 kind 再决定解码方式, 本用例固化这一约定
        assertEquals(ClusterRecords.Kind.TAKEOVER, ClusterRecords.kind(record));
    }

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
        return new InternalMessage("node-1", "c1", topic, 1, "payload".getBytes(StandardCharsets.UTF_8), false, false);
    }

    /** 取一个 header 的字符串值(不存在时返回 null) */
    static String headerValue(ProducerRecord<String, byte[]> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
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
                uplinkKey);
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
                "topic");
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
                false,
                "jmqtt",
                "jmqtt",
                60,
                7200,
                // v5 新增: 不接受入站 topic alias / 不强制心跳
                0,
                0,
                32,
                1000,
                1000,
                false,
                kafka,
                new BrokerProperties.RedisProperties(false, "127.0.0.1", 6379, null, 0, "jmqtt", 1000, 5000, "off", 100, 10000),
                511,
                true,
                true,
                10485760,
                32768,
                65536);
    }
}
