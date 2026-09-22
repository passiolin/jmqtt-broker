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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 数据面与事件面的外部契约:
 * <ol>
 *   <li><b>多路由上行</b> —— 命中即发 JSON 信封, 一条消息命中多条路由就发多份,
 *       每条路由有自己的分区键; 未命中不发 Kafka、集群广播不受影响</li>
 *   <li><b>上行 JSON 信封</b> —— {username, topic, timestamp, qos, payload, node, clientid}</li>
 *   <li><b>下行 JSON 契约</b> —— {topic, qos?, payload}, qos 缺省用通道默认值</li>
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
                null, (clientId, fromNodeId) -> { });
    }

    private static ProducerRecord<String, byte[]> take(KafkaClusterBus bus) throws InterruptedException {
        return bus.outbox.poll(2, TimeUnit.SECONDS);
    }

    private static ConsumerRecord<String, byte[]> toConsumerRecord(ProducerRecord<String, byte[]> record) {
        ConsumerRecord<String, byte[]> consumerRecord = new ConsumerRecord<>(
                record.topic(), 0, 0L, record.key(), record.value());
        record.headers().forEach(header -> consumerRecord.headers().add(header));
        return consumerRecord;
    }

    @Test
    @DisplayName("命中多条路由: 每条路由各发一份 JSON 信封, 分区键按各自配置")
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

        // 价值是 JSON 信封, 不是裸 payload
        ClusterRecords.RoutedEnvelope envelope = ClusterRecords.routedEnvelope(first);
        assertEquals("device/42/telemetry", envelope.topic());
        assertEquals("x", envelope.payload());
        assertEquals("device-42", envelope.clientid());
        assertEquals("app-user", envelope.username());
        assertEquals("node-1", envelope.node());
        assertEquals(0, envelope.qos());
        assertTrue(envelope.timestamp() > 0);
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
    @DisplayName("旧的单路由配置(uplink-topic)同样产出 JSON 信封")
    void legacySingleUlinkUsesEnvelope() throws InterruptedException {
        BrokerProperties.KafkaProperties kafka = new BrokerProperties.KafkaProperties(
                true, "127.0.0.1:9092", "jmqtt", "jmqtt-cluster", null,
                true, List.of(), "jmqtt-uplink", List.of("device/#"), false,
                1000, 1000, 200, 1, "none", "latest", "topic", null, null, null);
        KafkaClusterBus bus = bus(kafka);

        assertTrue(bus.isUplink("device/42/telemetry"));
        bus.publishUplink(message("device-42", "device/42/telemetry"));
        ProducerRecord<String, byte[]> record = take(bus);
        assertEquals("jmqtt-uplink", record.topic());
        assertNotNull(ClusterRecords.routedEnvelope(record), "单路由同样是 JSON 信封");
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
    @DisplayName("下行 JSON 契约: {topic, payload} 走通道默认 QoS, qos 字段可覆盖")
    void downlinkJsonContract() {
        // 最小契约: 只给 topic 和 payload
        ConsumerRecord<String, byte[]> minimal = new ConsumerRecord<>(
                "backend-commands", 0, 0L, "unused-key",
                "{\"topic\":\"cmd/device-42\",\"payload\":\"hello\"}".getBytes(StandardCharsets.UTF_8));
        InternalMessage decoded = ClusterRecords.downlinkMessage(minimal, 1);
        assertEquals("cmd/device-42", decoded.topic());
        assertEquals(1, decoded.qos(), "qos 缺省用通道配置的默认值");
        assertEquals("hello", new String(decoded.payload(), StandardCharsets.UTF_8));
        assertNull(decoded.clientId(), "下行消息无来源客户端, 不得排除任何订阅者");

        // qos 显式覆盖
        ConsumerRecord<String, byte[]> withQos = new ConsumerRecord<>(
                "backend-commands", 0, 0L, "unused-key",
                "{\"topic\":\"cmd/device-42\",\"qos\":2,\"payload\":\"hi\"}".getBytes(StandardCharsets.UTF_8));
        assertEquals(2, ClusterRecords.downlinkMessage(withQos, 1).qos());

        // 非法 JSON / 缺 topic → 丢弃
        ConsumerRecord<String, byte[]> bad = new ConsumerRecord<>(
                "backend-commands", 0, 0L, "k", "not-json".getBytes(StandardCharsets.UTF_8));
        assertNull(ClusterRecords.downlinkMessage(bad, 1));
        ConsumerRecord<String, byte[]> noTopic = new ConsumerRecord<>(
                "backend-commands", 0, 0L, "k", "{\"payload\":\"x\"}".getBytes(StandardCharsets.UTF_8));
        assertNull(ClusterRecords.downlinkMessage(noTopic, 1));
    }
}
