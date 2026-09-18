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
package online.ipuff.jmqtt.protocol;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageIdAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckPayload;
import io.netty.handler.codec.mqtt.MqttVersion;
import online.ipuff.jmqtt.handler.ChannelAttributes;

import java.util.List;

/**
 * 按连接协商的协议版本构造回包。
 *
 * <h2>为什么必须集中在一处</h2>
 * v3.1.1 与 v5 的差别不是「多几个字段」, 而是<b>同一种语义在两个版本下的字节完全不同</b>:
 *
 * <table>
 *   <tr><th>回包</th><th>v3.1.1</th><th>v5</th></tr>
 *   <tr><td>CONNACK</td><td>2 字节可变头</td><td>追加 properties 长度字段</td></tr>
 *   <tr><td>PUBACK/PUBREC/PUBREL/PUBCOMP</td><td>只有报文标识符</td>
 *       <td>追加原因码 + properties 长度</td></tr>
 *   <tr><td>SUBACK</td><td>返回码列表(0/1/2/0x80)</td><td>原因码列表(语义不同, 见下)</td></tr>
 *   <tr><td><b>UNSUBACK</b></td><td><b>必须没有 payload</b></td>
 *       <td><b>必须有 payload</b>, 每个过滤器一个原因码</td></tr>
 *   <tr><td>DISCONNECT</td><td>服务端不发, 直接关 TCP</td><td>可带原因码 + properties</td></tr>
 * </table>
 *
 * <p>把这些判断散到十来个处理器里, 每加一处回包都要重新回答一次
 * 「v5 要不要带 properties」—— 其中 UNSUBACK 那一条是<b>两个方向都错</b>的
 * （v5 漏 payload 会让客户端解包错位; v3.1.1 多 payload 是规范明确禁止的）。
 * 收在这里之后, 处理器只表达「语义」(成功 / 非法过滤器 / 未授权), 由本类决定字节。
 *
 * <h2>两个反直觉的细节</h2>
 * <ol>
 *   <li><b>「同一语义」在两个版本下码值不同。</b>例如未授权: v3.1.1 是 0x05, v5 是 0x87;
 *       报文太大: v3.1.1 是 0x02 家族, v5 是 0x95。所以 CONNACK 用
 *       {@link ConnAckReason} 枚举表达语义, 由本类做映射, 调用方不写裸码。</li>
 *   <li><b>SUBACK 的 0x00/0x01/0x02 在 v5 里恰好等于 QoS 值</b>, 看起来能直接复用 ——
 *       但失败码不是: v3.1.1 只有 0x80, v5 细分出 0x8F(过滤器非法)/0x87(未授权)/
 *       0x9E(不支持共享订阅)等。所以订阅失败要按版本选码。</li>
 * </ol>
 *
 * <h2>出站 PUBLISH 也必须过这里</h2>
 * 这条最容易被忽略: v5 的 PUBLISH 可变头里<b>必须</b>有 properties 长度字段。
 * Netty 的编码器按 Channel 上的协议版本属性决定要不要写它 —— 所以给 v5 连接
 * 发一条「v3 形态」的 PUBLISH, 客户端会把 payload 的第一个字节当成 properties 长度,
 * <b>整条报文解析错位</b>。因此所有出站 PUBLISH 统一走
 * {@link #publish(Channel, String, int, int, byte[], boolean, boolean)}.
 */
public final class ReplyFactory {

    // ------------------------------------------------------------------
    // 原因码(v5)
    // ------------------------------------------------------------------

    public static final int SUCCESS = 0x00;
    public static final int NO_MATCHING_SUBSCRIBERS = 0x10;
    public static final int UNSPECIFIED_ERROR = 0x80;
    public static final int MALFORMED_PACKET = 0x81;
    public static final int PROTOCOL_ERROR = 0x82;
    public static final int IMPLEMENTATION_SPECIFIC_ERROR = 0x83;
    public static final int NOT_AUTHORIZED = 0x87;
    public static final int SERVER_BUSY = 0x89;
    public static final int SERVER_SHUTTING_DOWN = 0x8B;
    public static final int KEEP_ALIVE_TIMEOUT = 0x8D;
    public static final int SESSION_TAKEN_OVER = 0x8E;
    public static final int TOPIC_FILTER_INVALID = 0x8F;
    public static final int TOPIC_NAME_INVALID = 0x90;
    public static final int PACKET_ID_IN_USE = 0x91;
    public static final int QUOTA_EXCEEDED = 0x97;
    /** 客户端发来的未确认 QoS 1/2 条数超过服务端在 CONNACK 里声明的 Receive Maximum */
    public static final int RECEIVE_MAXIMUM_EXCEEDED = 0x93;
    public static final int TOPIC_ALIAS_INVALID = 0x94;
    public static final int PACKET_TOO_LARGE = 0x95;
    public static final int SHARED_SUBSCRIPTIONS_NOT_SUPPORTED = 0x9E;
    public static final int SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED = 0xA1;
    public static final int WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED = 0xA2;

    /** v3.1.1 的 SUBACK 只有这一个失败码 */
    public static final int V3_SUBSCRIBE_FAILURE = 0x80;

    /**
     * CONNACK 拒绝原因。<b>用语义而不是裸码</b>来表达, 因为同一语义在两个版本下码值不同。
     */
    public enum ConnAckReason {
        ACCEPTED,
        UNSUPPORTED_PROTOCOL_VERSION,
        CLIENT_IDENTIFIER_NOT_VALID,
        SERVER_UNAVAILABLE,
        BAD_CREDENTIALS,
        NOT_AUTHORIZED,
        SERVER_BUSY,
        UNSPECIFIED_ERROR
    }

    private ReplyFactory() {
    }

    /**
     * 该连接协商的协议版本。
     *
     * <p>由 Netty 的 {@code MqttDecoder} 在解出 CONNECT 的协议级别后写入 Channel,
     * 这里再读回来。读不到时默认 3.1.1 —— 与 Netty 编码器的缺省行为保持一致,
     * 否则我们按 v5 构造报文、编码器却按 v3 编码, 出来的字节是坏的。
     */
    public static MqttVersion versionOf(Channel channel) {
        MqttVersion version = channel.attr(ChannelAttributes.PROTOCOL_VERSION).get();
        return version == null ? MqttVersion.MQTT_3_1_1 : version;
    }

    public static boolean isV5(Channel channel) {
        return versionOf(channel) == MqttVersion.MQTT_5;
    }

    // ------------------------------------------------------------------
    // CONNACK
    // ------------------------------------------------------------------

    /**
     * 把语义映射成当前版本下的 CONNACK 返回码。
     */
    public static MqttConnectReturnCode connAckCode(MqttVersion version, ConnAckReason reason) {
        boolean v5 = version == MqttVersion.MQTT_5;
        return switch (reason) {
            case ACCEPTED -> MqttConnectReturnCode.CONNECTION_ACCEPTED;
            case UNSUPPORTED_PROTOCOL_VERSION -> v5
                    ? MqttConnectReturnCode.CONNECTION_REFUSED_UNSUPPORTED_PROTOCOL_VERSION
                    : MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION;
            case CLIENT_IDENTIFIER_NOT_VALID -> v5
                    ? MqttConnectReturnCode.CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID
                    : MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED;
            case SERVER_UNAVAILABLE -> v5
                    ? MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE_5
                    : MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE;
            case BAD_CREDENTIALS -> v5
                    ? MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD
                    : MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD;
            case NOT_AUTHORIZED -> v5
                    ? MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED_5
                    : MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED;
            case SERVER_BUSY -> MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_BUSY;
            case UNSPECIFIED_ERROR -> MqttConnectReturnCode.CONNECTION_REFUSED_UNSPECIFIED_ERROR;
        };
    }

    /**
     * CONNACK。
     *
     * @param v5Properties 仅 v5 生效; v3.1.1 传 null 即可
     */
    public static MqttMessage connAck(MqttVersion version, MqttConnectReturnCode code,
                                     boolean sessionPresent, MqttProperties v5Properties) {
        MqttConnAckVariableHeader variableHeader;
        if (version == MqttVersion.MQTT_5) {
            variableHeader = new MqttConnAckVariableHeader(code, sessionPresent,
                    v5Properties == null ? MqttProperties.NO_PROPERTIES : v5Properties);
        } else {
            variableHeader = new MqttConnAckVariableHeader(code, sessionPresent);
        }
        return MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                variableHeader, null);
    }

    // ------------------------------------------------------------------
    // QoS 1/2 的确认回包
    // ------------------------------------------------------------------

    public static MqttMessage pubAck(Channel channel, int packetId, int reasonCode) {
        return pubReply(channel, MqttMessageType.PUBACK, packetId, reasonCode, false);
    }

    public static MqttMessage pubRec(Channel channel, int packetId, int reasonCode) {
        return pubReply(channel, MqttMessageType.PUBREC, packetId, reasonCode, false);
    }

    public static MqttMessage pubRel(Channel channel, int packetId, int reasonCode) {
        // PUBREL 的固定头有保留位: 低 4 位必须是 0b0010
        return pubReply(channel, MqttMessageType.PUBREL, packetId, reasonCode, true);
    }

    public static MqttMessage pubComp(Channel channel, int packetId, int reasonCode) {
        return pubReply(channel, MqttMessageType.PUBCOMP, packetId, reasonCode, false);
    }

    private static MqttMessage pubReply(Channel channel, MqttMessageType type, int packetId,
                                        int reasonCode, boolean reservedFlag) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(type, false,
                type == MqttMessageType.PUBREL ? MqttQoS.AT_LEAST_ONCE : MqttQoS.AT_MOST_ONCE,
                reservedFlag, 0);
        if (isV5(channel)) {
            return MqttMessageFactory.newMessage(fixedHeader,
                    new MqttPubReplyMessageVariableHeader(packetId, (byte) reasonCode,
                            MqttProperties.NO_PROPERTIES), null);
        }
        return MqttMessageFactory.newMessage(fixedHeader,
                MqttMessageIdVariableHeader.from(packetId), null);
    }

    // ------------------------------------------------------------------
    // SUBACK / UNSUBACK
    // ------------------------------------------------------------------

    /**
     * SUBACK。
     *
     * @param reasonCodes 逐项结果。<b>由调用方按版本给出正确的码</b> ——
     *                    失败码在 v5 下是细分的(0x8F/0x87/0x9E…), v3.1.1 下只有 0x80
     */
    public static MqttMessage subAck(Channel channel, int packetId, List<Integer> reasonCodes) {
        if (isV5(channel)) {
            return new MqttSubAckMessage(
                    new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                    new MqttMessageIdAndPropertiesVariableHeader(packetId, MqttProperties.NO_PROPERTIES),
                    new MqttSubAckPayload(reasonCodes));
        }
        return (MqttSubAckMessage) MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(packetId),
                new MqttSubAckPayload(reasonCodes));
    }

    /**
     * UNSUBACK。
     *
     * <p><b>这是两个方向都不能错的回包</b>: v5 必须带逐项原因码,
     * v3.1.1 必须不带 payload。v3.1.1 若带上 payload, 严格实现会直接判协议错误。
     *
     * @param reasonCodes 仅 v5 使用
     */
    public static MqttMessage unsubAck(Channel channel, int packetId, List<Short> reasonCodes) {
        if (isV5(channel)) {
            List<Short> codes = (reasonCodes == null || reasonCodes.isEmpty())
                    ? List.of((short) SUCCESS) : reasonCodes;
            return new MqttUnsubAckMessage(
                    new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                    new MqttMessageIdAndPropertiesVariableHeader(packetId, MqttProperties.NO_PROPERTIES),
                    new MqttUnsubAckPayload(codes));
        }
        return (MqttUnsubAckMessage) MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(packetId), null);
    }

    // ------------------------------------------------------------------
    // DISCONNECT
    // ------------------------------------------------------------------

    /**
     * 服务端主动断开。
     *
     * <p>v3.1.1 没有「服务端带原因码的 DISCONNECT」这回事 —— 断开就是关 TCP。
     * 所以这里只在 v5 下写出报文, v3.1.1 直接关连接。调用方不需要分版本。
     */
    public static void closeWithReason(Channel channel, int reasonCode) {
        if (!channel.isActive()) {
            channel.close();
            return;
        }
        if (isV5(channel)) {
            MqttMessage disconnect = MqttMessageFactory.newMessage(
                    new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0),
                    new MqttReasonCodeAndPropertiesVariableHeader((byte) reasonCode,
                            MqttProperties.NO_PROPERTIES), null);
            channel.writeAndFlush(disconnect).addListener(ChannelFutureListener.CLOSE);
        } else {
            channel.close();
        }
    }

    // ------------------------------------------------------------------
    // PUBLISH(出站)
    // ------------------------------------------------------------------

    /**
     * 构造出站 PUBLISH。<b>所有出站投递都必须走这里</b>, 原因见类注释。
     *
     * <p>v5 下可变头必须带 properties 长度字段。这里传 {@code NO_PROPERTIES},
     * Netty 会写出长度 0 —— 字节上就是多一个 {@code 0x00}, 但少了它客户端会整体错位。
     */
    public static MqttPublishMessage publish(Channel channel, String topic, int qos, int packetId,
                                            byte[] payload, boolean retain, boolean dup) {
        MqttPublishVariableHeader variableHeader = isV5(channel)
                ? new MqttPublishVariableHeader(topic, packetId, MqttProperties.NO_PROPERTIES)
                : new MqttPublishVariableHeader(topic, packetId);
        return new MqttPublishMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, dup, MqttQoS.valueOf(qos), retain, 0),
                variableHeader,
                // wrappedBuffer 只做包装不复制数据, 每帧一个独立 ByteBuf
                Unpooled.wrappedBuffer(payload));
    }
}
