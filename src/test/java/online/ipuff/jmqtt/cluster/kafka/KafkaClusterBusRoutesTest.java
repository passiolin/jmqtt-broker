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
import online.ipuff.jmqtt.cluster.ConnectionEvent;
import online.ipuff.jmqtt.cluster.InternalMessage;
import online.ipuff.jmqtt.config.BrokerProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 数据面与事件面的外部契约:
 * <ol>
 *   <li><b>多路由上行</b> —— 命中即发一份 raw 记录, 一条消息命中多条路由就发多份,
 *       每条路由有自己的分区键; 未命中不发 Kafka、集群广播不受影响</li>
 *   <li><b>上行 raw 契约</b> —— value = 原始 payload 字节(零编解码), 零 header,
 *       timestamp 用 Kafka 内建 record timestamp; 设备标识由分区键(key: device)承载,
 *       MQTT 主题不随记录携带(key: topic 路由时即 key)</li>
 *   <li><b>下行契约</b> —— JSON 结构: {"topic","qos","payload"}(payload 为
 *       byte[], JSON 中 base64 承载); key 仅用于分区</li>
 *   <li><b>连接事件</b> —— connected/disconnected 字段集, key=clientId 有序</li>
 * </ol>
 */
class KafkaClusterBusRoutesTest {

    private static InternalMessage message(String clientId, String topic) {
        return new InternalMessage("node-1", clientId, topic, 0,
                "x".getBytes(StandardCharsets.UTF_8), false, false, "app-user");
    }

    private static BrokerProperties.KafkaProperties kafka(String connectionEventTopic,
                                                          List<BrokerProperties.KafkaProperties.Route> routes) {
        return new BrokerProperties.KafkaProperties(
                true, "127.0.0.1:9092", "jmqtt", "jmqtt-cluster", null,
                true, List.of(), "", List.of(), false, 1000, 1000, 200, 1,
                "none", "latest", "topic", connectionEventTopic, routes, null);
    }

    private static KafkaClusterBus bus(BrokerProperties.KafkaProperties kafka) {
        return new KafkaClusterBus(TestBrokerProperties.create("node-1", 32, 1000, kafka),
                null, new online.ipuff.jmqtt.subscribe.SubscribeStoreService());
    }

    private static ProducerRecord<String, byte[]> take(KafkaClusterBus bus) throws InterruptedException {
        return bus.outbox().poll(2, TimeUnit.SECONDS);
    }

    private static ConsumerRecord<String, byte[]> toConsumerRecord(ProducerRecord<String, byte[]> record) {
        ConsumerRecord<String, byte[]> consumerRecord = new ConsumerRecord<>(
                record.topic(), 0, 0L, record.key(), record.value());
        record.headers().forEach(header -> consumerRecord.headers().add(header));
        return consumerRecord;
    }

    @Test
    @DisplayName("命中多条路由: 每条路由各发一份完整结构记录, 分区键按各自配置")
    void multipleRoutesFanOut() throws InterruptedException {
        List<BrokerProperties.KafkaProperties.Route> routes = List.of(
                new BrokerProperties.KafkaProperties.Route(
                        List.of("device/+/telemetry"), "telemetry-raw", "device"),
                new BrokerProperties.KafkaProperties.Route(
                        List.of("device/#"), "device-all", "topic"));
        KafkaClusterBus bus = bus(kafka(null, routes));

        bus.publishUplink(message("device-42", "device/42/telemetry"));

        ProducerRecord<String, byte[]> first = take(bus);
        ProducerRecord<String, byte[]> second = take(bus);
        assertEquals("telemetry-raw", first.topic());
        assertEquals("device-42", first.key(), "该路由配 key=device");
        assertEquals("device-all", second.topic());
        assertEquals("device/42/telemetry", second.key(), "该路由配 key=topic");
        assertNull(take(bus), "只有命中数量的记录");

        // 上行记录零 header, value = 完整结构(集群广播同一份)
        assertFalse(first.headers().iterator().hasNext(), "上行记录不带任何 jmqtt header");
        assertNotNull(first.timestamp(), "record timestamp 由 Kafka 内建承载");
        InternalMessage decoded = ClusterRecords.fromRecord(toConsumerRecord(first));
        assertNotNull(decoded);
        assertEquals("device/42/telemetry", decoded.topic(), "topic 在结构里");
        assertEquals("x", new String(decoded.payload(), StandardCharsets.UTF_8));
        assertEquals("device-42", decoded.clientId());
        assertEquals("node-1", decoded.brokerId(), "node = jmqtt-broker-id");
        assertEquals("app-user", decoded.username());
        assertEquals(0, decoded.qos());

        assertFalse(second.headers().iterator().hasNext(), "第二路由同样零 header");
        InternalMessage decodedSecond = ClusterRecords.fromRecord(toConsumerRecord(second));
        assertNotNull(decodedSecond);
        assertEquals("device/42/telemetry", decodedSecond.topic());
    }

    @Test
    @DisplayName("不命中任何路由: 不发 Kafka, 且不影响集群广播判定")
    void unmatchedSkipsUplinkButBroadcastUnaffected() throws InterruptedException {
        List<BrokerProperties.KafkaProperties.Route> routes = List.of(
                new BrokerProperties.KafkaProperties.Route(
                        List.of("device/+/telemetry"), "telemetry-raw", "device"));
        KafkaClusterBus bus = bus(kafka(null, routes));

        assertFalse(bus.isUplink("app/x/signal"), "未命中任何路由");
        assertTrue(bus.isBroadcastAllowed("app/x/signal"),
                "未命中路由的消息照常走集群广播(不命中就不发 Kafka)");
        bus.publishUplink(message("device-42", "app/x/signal"));
        assertNull(take(bus), "未命中不得发任何 Kafka 记录");
    }

    @Test
    @DisplayName("旧的单路由配置(uplink-topic)同样产出 raw 记录")
    void legacySingleUplinkProducesRaw() throws InterruptedException {
        BrokerProperties.KafkaProperties kafka = new BrokerProperties.KafkaProperties(
                true, "127.0.0.1:9092", "jmqtt", "jmqtt-cluster", null,
                true, List.of(), "jmqtt-uplink", List.of("device/#"), false,
                1000, 1000, 200, 1, "none", "latest", "topic", null, null, null);
        KafkaClusterBus bus = bus(kafka);

        assertTrue(bus.isUplink("device/42/telemetry"));
        bus.publishUplink(message("device-42", "device/42/telemetry"));
        ProducerRecord<String, byte[]> record = take(bus);
        assertEquals("jmqtt-uplink", record.topic());
        assertFalse(record.headers().iterator().hasNext(), "上行记录零 header");
        InternalMessage decoded = ClusterRecords.fromRecord(toConsumerRecord(record));
        assertNotNull(decoded);
        assertEquals("x", new String(decoded.payload(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("上行 raw: 二进制 payload 逐位原样, 不经 UTF-8/JSON 编解码")
    void binaryPayloadSurvivesUplinkRaw() throws InterruptedException {
        BrokerProperties.KafkaProperties kafka = new BrokerProperties.KafkaProperties(
                true, "127.0.0.1:9092", "jmqtt", "jmqtt-cluster", null,
                true, List.of(), "jmqtt-uplink", List.of("device/#"), false,
                1000, 1000, 200, 1, "none", "latest", "topic", null, null, null);
        KafkaClusterBus bus = bus(kafka);

        byte[] payload = {0x22, 0x5C, (byte) 0xFF, 0x00, 0x0A};
        bus.publishUplink(new InternalMessage("node-1", "device-42",
                "device/42/telemetry", 1, payload, false, false, "app-user"));

        ProducerRecord<String, byte[]> record = take(bus);
        InternalMessage decoded = ClusterRecords.fromRecord(toConsumerRecord(record));
        assertNotNull(decoded);
        assertArrayEquals(payload, decoded.payload(), "引号/反斜杠/非法 UTF-8/NUL/换行 都不得被改动");
    }

    @Test
    @DisplayName("连接事件(disconnected): 发到专用 topic, key=clientId, 字段完整可解码")
    void connectionEventPublished() throws InterruptedException {
        KafkaClusterBus bus = bus(kafka("jmqtt-client-events", null));

        bus.publishConnectionEvent(ConnectionEvent.disconnected(
                "device-42", "app-user", 4, "10.0.0.7:51166", "node-1",
                ConnectionEvent.REASON_TCP_CLOSED));

        ProducerRecord<String, byte[]> record = take(bus);
        assertEquals("jmqtt-client-events", record.topic());
        assertEquals("device-42", record.key(), "同一客户端的事件必须落同分区(有序)");
        assertFalse(record.headers().iterator().hasNext(), "连接事件零 header, action 在 JSON 体里");
        ConnectionEvent decoded = ClusterRecords.connectionEvent(toConsumerRecord(record));
        assertEquals(ConnectionEvent.ACTION_DISCONNECTED, decoded.action());
        assertEquals("device-42", decoded.clientid());
        assertEquals("app-user", decoded.username());
        assertEquals(4, decoded.protoVersion());
        assertEquals("MQTT", decoded.protoName());
        assertEquals("10.0.0.7:51166", decoded.peername());
        assertEquals("node-1", decoded.node());
        assertEquals(ConnectionEvent.REASON_TCP_CLOSED, decoded.reason());
        assertNotNull(decoded.disconnectedAt());
        assertNull(decoded.connectedAt(), "disconnected 事件不带 connected_at");
    }

    @Test
    @DisplayName("连接事件(disconnected/keepalive_timeout): reason 如实透传")
    void connectionEventKeepaliveTimeout() throws InterruptedException {
        KafkaClusterBus bus = bus(kafka("jmqtt-client-events", null));

        bus.publishConnectionEvent(ConnectionEvent.disconnected(
                "device-42", null, 5, "10.0.0.7:51166", "node-1",
                ConnectionEvent.REASON_KEEPALIVE_TIMEOUT));

        ProducerRecord<String, byte[]> record = take(bus);
        ConnectionEvent decoded = ClusterRecords.connectionEvent(toConsumerRecord(record));
        assertEquals(ConnectionEvent.REASON_KEEPALIVE_TIMEOUT, decoded.reason());
        assertEquals(5, decoded.protoVersion());
    }

    @Test
    @DisplayName("连接事件(connected): username 缺失也可表达")
    void connectedEventWithoutUsername() throws InterruptedException {
        KafkaClusterBus bus = bus(kafka("jmqtt-client-events", null));

        bus.publishConnectionEvent(ConnectionEvent.connected(
                "anon-client", null, 5, "10.0.0.8:40001", "node-1"));

        ProducerRecord<String, byte[]> record = take(bus);
        ConnectionEvent decoded = ClusterRecords.connectionEvent(toConsumerRecord(record));
        assertEquals(ConnectionEvent.ACTION_CONNECTED, decoded.action());
        assertEquals(5, decoded.protoVersion());
        assertNull(decoded.reason(), "connected 事件不带 reason");
        String body = new String(record.value(), StandardCharsets.UTF_8);
        assertFalse(body.contains("username"), "username 为 null 时序列化省略该字段: " + body);
    }

    @Test
    @DisplayName("未配置 connection-event-topic: 事件静默丢弃(不入队)")
    void connectionEventDisabledWhenTopicAbsent() throws InterruptedException {
        KafkaClusterBus bus = bus(kafka(null, null));
        bus.publishConnectionEvent(ConnectionEvent.connected(
                "device-42", "app-user", 4, "10.0.0.7:40001", "node-1"));
        assertNull(take(bus));
    }

    @Test
    @DisplayName("下行契约: JSON {topic, qos, payload(byte[]→base64)}, 零 header")
    void downlinkRawContract() {
        byte[] payload = {0x00, 0x22, (byte) 0xFF};
        String body = "{\"topic\":\"cmd/device-42\",\"qos\":1,\"payload\":\""
                + java.util.Base64.getEncoder().encodeToString(payload) + "\"}";
        ConsumerRecord<String, byte[]> raw = new ConsumerRecord<>(
                "backend-commands", 0, 0L, "partition-key-only", body.getBytes(StandardCharsets.UTF_8));

        InternalMessage decoded = ClusterRecords.downlinkMessage(raw);
        assertNotNull(decoded);
        assertEquals("cmd/device-42", decoded.topic(), "主题取自 JSON");
        assertEquals(1, decoded.qos());
        assertArrayEquals(payload, decoded.payload(), "二进制 payload 无损");
        assertNull(decoded.clientId(), "下行消息无来源客户端, 不得排除任何订阅者");
    }
}
