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

import com.fasterxml.jackson.databind.ObjectMapper;
import online.ipuff.jmqtt.cluster.ConnectionEvent;
import online.ipuff.jmqtt.cluster.InternalMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;

/**
 * {@link InternalMessage} 与 Kafka record 之间的编解码。
 *
 * <h2>设计要点</h2>
 * <ul>
 *   <li><b>key = MQTT 主题。</b>这是唯一正确的选择:
 *       <ul>
 *         <li>Kafka 默认分区器对 key 取 hash → 同一主题恒定落到同一 partition,
 *             从而维持该主题的顺序。若 key 为空, 默认是 sticky 分区,
 *             同一主题的消息会散落不同 partition, 消费端看到的就是乱序。</li>
 *         <li>主题本身也就随消息传递过去了, 无需在 value 里再编码一遍。</li>
 *       </ul>
 *   </li>
 *   <li><b>元数据放 header, payload 放 value。</b>payload 保持原始字节不做 base64,
 *       header 的键在同一批次内高度重复, 压缩后开销很小。</li>
 * </ul>
 */
public final class ClusterRecords {

    /** 消息来源 broker 标识。消费端据此跳过自己发出的消息(回环防护) */
    /**
     * MQTT 主题。
     *
     * <p><b>主题必须显式放在 header 里, 不能从 record 的 key 反推。</b>
     * 原因见 {@link #toRecord(String, String, InternalMessage)}: 广播用的分区键是主题,
     * 而数据面(服务端接入)用的分区键是设备标识 —— 两者不同, 主题就不能再由 key 承载。
     */
    public static final String HEADER_TOPIC = "jmqtt-topic";

    public static final String HEADER_BROKER_ID = "jmqtt-broker-id";
    public static final String HEADER_CLIENT_ID = "jmqtt-client-id";
    public static final String HEADER_QOS = "jmqtt-qos";
    public static final String HEADER_RETAIN = "jmqtt-retain";
    public static final String HEADER_DUP = "jmqtt-dup";

    /**
     * 记录类别。同一条 topic 上混跑两类记录:
     * <ul>
     *   <li>{@link Kind#PUBLISH} —— 普通消息转发(占绝大多数)</li>
     *   <li>{@link Kind#TAKEOVER} —— 连接接管控制消息(极低频)</li>
     * </ul>
     * 不复用 `Message` 类型是因为控制消息没有 payload / qos 语义,
     * 硬塞进同一个结构会产生一堆「该字段对本次无意义」的空洞。
     */
    public static final String HEADER_KIND = "jmqtt-kind";

    /** 接管消息的目标节点。只有该节点会处理; 其他节点直接跳过 */
    public static final String HEADER_TARGET_NODE = "jmqtt-target-node";

    public static final String KIND_PUBLISH = "publish";
    public static final String KIND_TAKEOVER = "takeover";
    public static final String KIND_CLIENT_EVENT = "client-event";
    public static final String KIND_ROUTED_PUBLISH = "routed-publish";

    /** 连接事件的 JSON 编解码器(事件体是给外部系统消费的, 走 JSON 而不是 header 编码) */
    private static final ObjectMapper JSON = new ObjectMapper();

    private ClusterRecords() {
    }

    /**
     * 记录类别。
     */
    public enum Kind {
        PUBLISH,
        TAKEOVER
    }

    /**
     * 跨节点连接接管指令。
     *
     * @param clientId     需要被释放的客户端
     * @param targetNodeId 目标节点 —— 只通知真正持有该连接的那一个, 而不是广播给所有节点
     * @param fromNodeId   发起方节点, 用于日志
     */
    public record Takeover(String clientId, String targetNodeId, String fromNodeId) {
    }

    // ------------------------------------------------------------------
    // 编码
    // ------------------------------------------------------------------

    /**
     * 编码普通消息为 Kafka record。
     *
     * @param kafkaTopic 目标 Kafka topic
     */
    /**
     * 按「主题即分区键」构造记录。用于集群广播 —— 同一主题必须落在同一分区, 以保证顺序。
     */
    public static ProducerRecord<String, byte[]> toRecord(String kafkaTopic, InternalMessage message) {
        return toRecord(kafkaTopic, message.topic(), message);
    }

    /**
     * 构造记录, 分区键由调用方指定。
     *
     * <h2>为什么分区键要可指定</h2>
     * Kafka 保证「同一分区内有序」, 所以键的选择决定了下游能依赖哪种顺序:
     * <ul>
     *   <li><b>集群广播: 键 = MQTT 主题。</b>跨节点投递要保证同一主题的消息按序到达
     *       —— 订阅者看到的消息顺序不该因为经过了一次总线而错乱。</li>
     *   <li><b>数据面(服务端接入): 键 = 设备标识。</b>下游是数据管道, 关心的顺序是
     *       <b>每个设备自己的时序</b>, 而不是跨设备的主题顺序。
     *       更要紧的是: 设备遥测往往集中在少数几个主题上(甚至只有一个) ——
     *       此时按主题分区会把全部流量压进一个分区, 下游无论加多少个消费者都只能串行读,
     *       这个分区就是整条管道的吞吐上限。按设备分区则天然散开。</li>
     * </ul>
     *
     * <p>正因为键不再等于主题, 主题必须随记录一起走 header
     * （{@link #HEADER_TOPIC}）, 否则消费端无从还原。
     */
    public static ProducerRecord<String, byte[]> toRecord(String kafkaTopic, String partitionKey,
                                                          InternalMessage message) {
        ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(kafkaTopic, partitionKey, message.payload());
        Headers headers = record.headers();
        headers.add(HEADER_TOPIC, utf8(nullToEmpty(message.topic())));
        headers.add(HEADER_KIND, utf8(KIND_PUBLISH));
        headers.add(HEADER_BROKER_ID, utf8(nullToEmpty(message.brokerId())));
        headers.add(HEADER_CLIENT_ID, utf8(nullToEmpty(message.clientId())));
        headers.add(HEADER_QOS, new byte[]{(byte) message.qos()});
        headers.add(HEADER_RETAIN, new byte[]{(byte) (message.retain() ? 1 : 0)});
        headers.add(HEADER_DUP, new byte[]{(byte) (message.dup() ? 1 : 0)});
        return record;
    }

    /**
     * 编码连接接管指令。
     *
     * <p>key 用 clientId —— 同一客户端的多次接管指令落在同一分区, 按序生效。
     * (普通消息按约定以 MQTT 主题为 key, 控制指令与数据消息之间没有顺序要求。)
     */
    public static ProducerRecord<String, byte[]> takeoverRecord(String kafkaTopic, Takeover takeover) {
        ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(kafkaTopic, takeover.clientId(), new byte[0]);
        Headers headers = record.headers();
        headers.add(HEADER_KIND, utf8(KIND_TAKEOVER));
        headers.add(HEADER_TARGET_NODE, utf8(nullToEmpty(takeover.targetNodeId())));
        headers.add(HEADER_BROKER_ID, utf8(nullToEmpty(takeover.fromNodeId())));
        headers.add(HEADER_CLIENT_ID, utf8(nullToEmpty(takeover.clientId())));
        return record;
    }

    /**
     * 数据面上行信封 —— 后台消费的 JSON 契约(只含 PUBLISH 消息)。
     * 字段名与既有后台的消费格式对齐; username 缺失时省略。
     */
    public record RoutedEnvelope(
            String username,
            String topic,
            long timestamp,
            int qos,
            String payload,
            String node,
            String clientid
    ) {
    }

    /**
     * 编码数据面上行记录: value 为 JSON 信封, key 由路由决定(topic 或设备标识)。
     * payload 以 UTF-8 字符串承载 —— 二进制 payload 会被有损解码(替换字符),
     * 需要无损传输二进制的场景应直接订阅 MQTT 而不是走数据面。
     */
    public static ProducerRecord<String, byte[]> toRoutedRecord(
            String kafkaTopic, String partitionKey, InternalMessage message) {
        RoutedEnvelope envelope = new RoutedEnvelope(
                message.username(),
                message.topic(),
                System.currentTimeMillis(),
                message.qos(),
                message.payload() == null ? "" : new String(message.payload(), StandardCharsets.UTF_8),
                message.brokerId(),
                message.clientId());
        byte[] body;
        try {
            body = JSON.writeValueAsString(envelope).getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("上行信封序列化失败", e);
        }
        ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(kafkaTopic, partitionKey, body);
        record.headers().add(HEADER_KIND, utf8(KIND_ROUTED_PUBLISH));
        return record;
    }

    /**
     * 下行记录的 JSON 契约: {topic, qos?, payload}。
     * 投递 QoS 以消息体的 qos 字段为准(缺省 1); 宽容未知字段。
     */
    @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
    public record DownlinkEnvelope(
            String topic,
            Integer qos,
            String payload
    ) {
    }

    /**
     * 编码连接事件。value 是 JSON(后台直接反序列化), key=clientId。
     */
    public static ProducerRecord<String, byte[]> connectionEventRecord(
            String kafkaTopic, ConnectionEvent event) {
        byte[] body;
        try {
            body = JSON.writeValueAsString(event).getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("连接事件序列化失败", e);
        }
        ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(kafkaTopic, event.clientid(), body);
        record.headers().add(HEADER_KIND, utf8(KIND_CLIENT_EVENT));
        return record;
    }

    /**
     * 解码上行 JSON 信封(测试与诊断用; 后台直接反序列化同结构 JSON)。
     *
     * @return 信封; value 不是合法信封时返回 {@code null}
     */
    public static RoutedEnvelope routedEnvelope(ProducerRecord<String, byte[]> record) {
        if (record.value() == null) {
            return null;
        }
        try {
            return JSON.readValue(record.value(), RoutedEnvelope.class);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 解码连接事件。
     *
     * @return 事件; body 不是合法事件时返回 {@code null}
     */
    public static ConnectionEvent connectionEvent(ConsumerRecord<String, byte[]> record) {
        if (record.value() == null) {
            return null;
        }
        try {
            return JSON.readValue(record.value(), ConnectionEvent.class);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 解码下行记录(后台 → broker)。
     *
     * <p>JSON 契约: {@code {topic, qos?, payload}} —— 投递 QoS 以消息体的
     * qos 字段为准, 缺省 1; payload 字符串按 UTF-8 编码为 MQTT 消息体。
     * clientId 恒为 null —— 下行消息没有来源客户端, 不得因此排除任何订阅者。
     *
     * @return 内部消息; JSON 非法或 topic 缺失时返回 {@code null}, 调用方应丢弃该记录
     */
    public static InternalMessage downlinkMessage(ConsumerRecord<String, byte[]> record) {
        if (record.value() == null) {
            return null;
        }
        DownlinkEnvelope envelope;
        try {
            envelope = JSON.readValue(record.value(), DownlinkEnvelope.class);
        } catch (Exception e) {
            return null;
        }
        if (envelope == null || envelope.topic() == null || envelope.topic().isEmpty()) {
            return null;
        }
        return new InternalMessage(
                "",
                null,
                envelope.topic(),
                envelope.qos() != null ? envelope.qos() : 1,
                envelope.payload() == null ? new byte[0] : envelope.payload().getBytes(StandardCharsets.UTF_8),
                false,
                false,
                null);
    }

    // ------------------------------------------------------------------
    // 解码
    // ------------------------------------------------------------------

    /**
     * 判断记录类别。缺失 kind 头时按 {@link Kind#PUBLISH} 处理(向前兼容)。
     */
    public static Kind kind(ConsumerRecord<String, byte[]> record) {
        String kind = string(record.headers(), HEADER_KIND);
        return KIND_TAKEOVER.equals(kind) ? Kind.TAKEOVER : Kind.PUBLISH;
    }

    /**
     * 解码普通消息。
     *
     * <p>主题优先取 header; 取不到时回退到 record 的 key —— 回退是为了兼容
     * 「主题即分区键」时期写下的历史数据, 以及手工投递到 topic 的调试消息。
     *
     * @return 解码结果; 主题与来源标识缺一时返回 {@code null}, 调用方应丢弃该记录
     */
    public static InternalMessage fromRecord(ConsumerRecord<String, byte[]> record) {
        String topic = string(record.headers(), HEADER_TOPIC);
        if (topic == null || topic.isEmpty()) {
            topic = record.key();
        }
        String brokerId = string(record.headers(), HEADER_BROKER_ID);
        if (topic == null || topic.isEmpty() || brokerId == null) {
            return null;
        }
        String clientId = string(record.headers(), HEADER_CLIENT_ID);
        return new InternalMessage(
                brokerId,
                (clientId == null || clientId.isEmpty()) ? null : clientId,
                topic,
                unsignedByte(record.headers(), HEADER_QOS),
                record.value() == null ? new byte[0] : record.value(),
                unsignedByte(record.headers(), HEADER_RETAIN) == 1,
                unsignedByte(record.headers(), HEADER_DUP) == 1, null);
    }

    /**
     * 解码接管指令。
     *
     * @return 指令; 缺失 clientId 或目标节点时返回 {@code null}
     */
    public static Takeover takeover(ConsumerRecord<String, byte[]> record) {
        String clientId = string(record.headers(), HEADER_CLIENT_ID);
        String targetNode = string(record.headers(), HEADER_TARGET_NODE);
        if (clientId == null || clientId.isEmpty() || targetNode == null || targetNode.isEmpty()) {
            return null;
        }
        return new Takeover(clientId, targetNode, string(record.headers(), HEADER_BROKER_ID));
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    private static String string(Headers headers, String key) {
        Header header = headers.lastHeader(key);
        return header == null || header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static int unsignedByte(Headers headers, String key) {
        Header header = headers.lastHeader(key);
        if (header == null || header.value() == null || header.value().length == 0) {
            return 0;
        }
        return header.value()[0] & 0xFF;
    }

    private static boolean hasHeader(Headers headers, String key) {
        Header header = headers.lastHeader(key);
        return header != null && header.value() != null && header.value().length > 0;
    }
}
