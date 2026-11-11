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
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;

import java.util.List;

/**
 * 出站报文构造 —— <b>只支持 MQTT 3.1.1</b>。
 *
 * <h2>这个分支为什么没有 v5</h2>
 * 本项目服务的终端设备固件只实现 3.1.1，v5 的 properties 解析、原因码映射、
 * 会话生命周期语义在本场景中永远不会被执行。保留它们不是"零成本的能力储备"，
 * 而是每条回包构造路径上的一次版本判断，以及十几处需要同步维护的语义映射。
 *
 * <p>同一语义在两个版本下的字节完全不同（CONNACK 的 properties 长度字段、
 * PUBACK 的原因码、UNSUBACK 在 v5 必须有 payload 而 v3.1.1 必须没有），
 * 废掉一个版本意味着这些差异全部消失 —— 代码只剩一种形态，不存在"改一处忘一处"。
 *
 * <p>v5 客户端发来的 CONNECT 在 {@link ConnectHandler} 入口即被拒绝
 * （CONNACK 返回「协议版本不支持」），不会走到本类。
 *
 * <h2>出站 PUBLISH 也必须过这里</h2>
 * 报文标识符、QoS、dup/retain 三个标志的正确组合只在这一处生成。
 * 直接 {@code channel.write} 拼装会绕过 QoS 与标识符的一致性约束。
 */
public final class ReplyFactory {

    /** v3.1.1 的 SUBACK 只有这一个失败码 */
    public static final int V3_SUBSCRIBE_FAILURE = 0x80;

    /**
     * CONNACK 拒绝原因。
     *
     * <p>保留语义枚举而不是裸码：调用方表达"为什么拒绝"，映射成字节是本类的职责。
     * v5 的细分码（0x87/0x95/0x9E…）已随 v5 一并移除。
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

    // ------------------------------------------------------------------
    // CONNACK
    // ------------------------------------------------------------------

    public static MqttConnectReturnCode connAckCode(ConnAckReason reason) {
        return switch (reason) {
            case ACCEPTED -> MqttConnectReturnCode.CONNECTION_ACCEPTED;
            case UNSUPPORTED_PROTOCOL_VERSION ->
                    MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION;
            case CLIENT_IDENTIFIER_NOT_VALID ->
                    MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED;
            case SERVER_UNAVAILABLE ->
                    MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE;
            case BAD_CREDENTIALS ->
                    MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD;
            case NOT_AUTHORIZED ->
                    MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED;
            case SERVER_BUSY -> MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_BUSY;
            case UNSPECIFIED_ERROR ->
                    MqttConnectReturnCode.CONNECTION_REFUSED_UNSPECIFIED_ERROR;
        };
    }

    public static MqttMessage connAck(MqttConnectReturnCode code, boolean sessionPresent) {
        return MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttConnAckVariableHeader(code, sessionPresent), null);
    }

    // ------------------------------------------------------------------
    // QoS 1/2 的确认回包（v3.1.1: 可变头只有报文标识符）
    // ------------------------------------------------------------------

    public static MqttMessage pubAck(int packetId) {
        return pubReply(MqttMessageType.PUBACK, packetId, false);
    }

    public static MqttMessage pubRec(int packetId) {
        return pubReply(MqttMessageType.PUBREC, packetId, false);
    }

    public static MqttMessage pubRel(int packetId) {
        // PUBREL 的固定头有保留位: 低 4 位必须是 0b0010
        return pubReply(MqttMessageType.PUBREL, packetId, true);
    }

    public static MqttMessage pubComp(int packetId) {
        return pubReply(MqttMessageType.PUBCOMP, packetId, false);
    }

    private static MqttMessage pubReply(MqttMessageType type, int packetId, boolean reservedFlag) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(type, false,
                type == MqttMessageType.PUBREL ? MqttQoS.AT_LEAST_ONCE : MqttQoS.AT_MOST_ONCE,
                reservedFlag, 0);
        return MqttMessageFactory.newMessage(fixedHeader,
                MqttMessageIdVariableHeader.from(packetId), null);
    }

    // ------------------------------------------------------------------
    // SUBACK / UNSUBACK
    // ------------------------------------------------------------------

    /**
     * SUBACK。{@code reasonCodes} 直接是授予的 QoS（0/1/2）或 0x80 失败。
     */
    public static MqttMessage subAck(int packetId, List<Integer> reasonCodes) {
        return (MqttSubAckMessage) MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(packetId),
                new MqttSubAckPayload(reasonCodes));
    }

    /**
     * UNSUBACK。
     *
     * <p><b>v3.1.1 的 UNSUBACK 必须没有 payload。</b>这是与 v5 最容易写错的一处
     * （v5 必须带逐项原因码），严格实现的客户端收到带 payload 的 UNSUBACK 会判协议错误。
     */
    public static MqttMessage unsubAck(int packetId) {
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
     * <p>v3.1.1 没有「服务端带原因码的 DISCONNECT」—— 断开就是关 TCP。
     * 调用方语义不变，只是不再有写报文这一步。
     */
    public static void closeWithReason(Channel channel, String reason) {
        channel.close();
    }

    // ------------------------------------------------------------------
    // PUBLISH(出站)
    // ------------------------------------------------------------------

    /**
     * 构造出站 PUBLISH。所有出站投递都必须走这里。
     */
    public static MqttPublishMessage publish(String topic, int qos, int packetId,
                                            byte[] payload, boolean retain, boolean dup) {
        return new MqttPublishMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, dup, MqttQoS.valueOf(qos), retain, 0),
                new MqttPublishVariableHeader(topic, packetId),
                // wrappedBuffer 只做包装不复制数据, 每帧一个独立 ByteBuf
                Unpooled.wrappedBuffer(payload));
    }
}
