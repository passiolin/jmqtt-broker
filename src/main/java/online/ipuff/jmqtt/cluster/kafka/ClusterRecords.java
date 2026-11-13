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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import online.ipuff.jmqtt.cluster.ConnectionEvent;
import online.ipuff.jmqtt.cluster.InternalMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Kafka 消息结构: <b>JSON, 零 Kafka header</b>。payload 为 {@code byte[]},
 * JSON 中以 base64 承载(Jackson 缺省行为), 二进制无损。
 *
 * <h2>broker → Kafka(集群广播与数据面上行同一份)</h2>
 * <pre>
 * {
 *   "username": string,
 *   "topic":    string,
 *   "timestamp": long,
 *   "qos":       int,
 *   "payload":   byte[],
 *   "node":      string,   // jmqtt-broker-id
 *   "clientid":  string
 * }
 * </pre>
 * 集群广播消费所需的字段(topic/qos/payload/node)全在这份结构里;
 * username/timestamp/clientid 是给后台系统用的附加字段。
 * key 仅用于分区: 广播按 topic, 上行按路由配置(topic / device)。
 *
 * <h2>后台 → broker(下行)</h2>
 * <pre>
 * { "topic": string, "qos": int, "payload": byte[] }
 * </pre>
 */
public final class ClusterRecords {

    private static final ObjectMapper JSON = new ObjectMapper();

    private ClusterRecords() {
    }

    /** broker 产出的完整消息结构。 */
    public record BrokerMessage(String username, String topic, long timestamp,
                                int qos, byte[] payload, String node, String clientid) {
    }

    /** 后台下行的最小消息结构。 */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record DownlinkMessage(String topic, Integer qos, byte[] payload) {
    }

    // ------------------------------------------------------------------
    // broker → Kafka: 集群广播与数据面上行共用同一结构
    // ------------------------------------------------------------------

    /** 编码集群广播记录: key = MQTT 主题(同主题同分区保序), value = JSON。 */
    public static ProducerRecord<String, byte[]> toRecord(String kafkaTopic, InternalMessage message) {
        long timestamp = System.currentTimeMillis();
        return new ProducerRecord<>(kafkaTopic, null, timestamp,
                message.topic(), brokerJson(message, timestamp));
    }

    /**
     * 编码数据面上行记录: key 由路由决定(topic 或设备标识), value = 同一个 JSON 结构。
     *
     * <p>分区键与结构分离: 上行按设备分区时 key 是设备标识, topic 仍在 JSON 里。
     */
    public static ProducerRecord<String, byte[]> toRoutedRecord(
            String kafkaTopic, String partitionKey, InternalMessage message) {
        long timestamp = System.currentTimeMillis();
        return new ProducerRecord<>(kafkaTopic, null, timestamp,
                partitionKey, brokerJson(message, timestamp));
    }

    /** 解码 broker 产出的记录(广播/上行同一结构); JSON 非法或缺 topic 返回 {@code null}。 */
    public static InternalMessage fromRecord(ConsumerRecord<String, byte[]> record) {
        BrokerMessage m = parse(record.value(), BrokerMessage.class);
        if (m == null || m.topic() == null || m.topic().isEmpty()) {
            return null;
        }
        return new InternalMessage(
                m.node(),
                m.clientid(),
                m.topic(),
                m.qos(),
                m.payload() == null ? new byte[0] : m.payload(),
                false,
                false,
                m.username());
    }

    // ------------------------------------------------------------------
    // 后台 → broker(下行)
    // ------------------------------------------------------------------

    /**
     * 解码下行记录; JSON 非法或缺 topic 时返回 {@code null}。
     *
     * <p>clientId 恒为 null —— 下行消息没有来源客户端, 不得因此排除任何订阅者。
     */
    public static InternalMessage downlinkMessage(ConsumerRecord<String, byte[]> record) {
        DownlinkMessage m = parse(record.value(), DownlinkMessage.class);
        if (m == null || m.topic() == null || m.topic().isEmpty()) {
            return null;
        }
        return new InternalMessage(
                "",
                null,
                m.topic(),
                m.qos() != null ? m.qos() : 1,
                m.payload() == null ? new byte[0] : m.payload(),
                false,
                false,
                null);
    }

    // ------------------------------------------------------------------
    // 连接事件(JSON 体, 零 header)
    // ------------------------------------------------------------------

    /**
     * 编码连接事件: key = clientId(同客户端事件保序), value = JSON(自带 action)。
     *
     * <p>key 是消费端唯一的低成本索引: 跨节点接管消费者先按 key 查本地连接/会话,
     * 无关的事件不必反序列化 value。
     */
    public static ProducerRecord<String, byte[]> connectionEventRecord(
            String kafkaTopic, ConnectionEvent event) {
        return new ProducerRecord<>(kafkaTopic, event.clientid(), json(event, "连接事件序列化失败"));
    }

    /**
     * 解码连接事件。
     *
     * @return 事件; body 不是合法事件时返回 {@code null}
     */
    public static ConnectionEvent connectionEvent(ConsumerRecord<String, byte[]> record) {
        return parse(record.value(), ConnectionEvent.class);
    }

    // ------------------------------------------------------------------
    // JSON 编解码
    // ------------------------------------------------------------------

    private static byte[] brokerJson(InternalMessage message, long timestamp) {
        return json(new BrokerMessage(
                message.username(),
                message.topic(),
                timestamp,
                message.qos(),
                message.payload(),
                message.brokerId(),
                message.clientId()), "Kafka 消息序列化失败");
    }

    private static byte[] json(Object body, String error) {
        try {
            return JSON.writeValueAsBytes(body);
        } catch (Exception e) {
            throw new IllegalStateException(error, e);
        }
    }

    private static <T> T parse(byte[] body, Class<T> type) {
        if (body == null) {
            return null;
        }
        try {
            return JSON.readValue(body, type);
        } catch (Exception e) {
            return null;
        }
    }
}
