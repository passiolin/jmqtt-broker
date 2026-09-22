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
package online.ipuff.jmqtt.handler;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 入站报文日志的格式规范 —— 集中在 {@code MqttBrokerHandler} 分发点打印,
 * 每条一行、前缀 INBOUND、key=value 风格, 便于 grep 与采集解析。
 */
class MqttBrokerHandlerLogTest {

    private static String describe(MqttMessage msg) {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.attr(ChannelAttributes.CLIENT_ID).set("device-42");
        return MqttBrokerHandler.describeInbound(channel, msg);
    }

    @Test
    @DisplayName("CONNECT: 带 clientId/username/版本/心跳, 绝不打印密码")
    void connectDescription() {
        MqttConnectMessage msg = new MqttConnectMessage(
                new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttConnectVariableHeader("MQTT", 4, true, true, false, 0, false, true, 60),
                new MqttConnectPayload("device-42", null, null, "app-user",
                        "secret-password".getBytes(StandardCharsets.UTF_8)));
        // CONNECT 时 channel 上还没有 clientId, 应取报文里的
        EmbeddedChannel channel = new EmbeddedChannel();
        String line = MqttBrokerHandler.describeInbound(channel, msg);

        assertTrue(line.startsWith("INBOUND "), "统一前缀: " + line);
        assertTrue(line.contains("type=CONNECT"), line);
        assertTrue(line.contains("clientId=device-42"), line);
        assertTrue(line.contains("username=app-user"), line);
        assertTrue(line.contains("proto=4"), line);
        assertTrue(line.contains("keepAlive=60"), line);
        assertTrue(line.contains("cleanSession=true"), line);
        assertFalse(line.contains("secret-password"), "密码绝不出现在日志: " + line);
    }

    @Test
    @DisplayName("PUBLISH: 带 topic/qos/packetId/retain/size/payload")
    void publishDescription() {
        MqttPublishMessage msg = new MqttPublishMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, true, 0),
                new MqttPublishVariableHeader("sensor/1/temp", 7, MqttProperties.NO_PROPERTIES),
                Unpooled.wrappedBuffer("{\"temp\":23.5}".getBytes(StandardCharsets.UTF_8)));

        String line = describe(msg);
        assertTrue(line.contains("type=PUBLISH"), line);
        assertTrue(line.contains("topic=sensor/1/temp"), line);
        assertTrue(line.contains("qos=1"), line);
        assertTrue(line.contains("packetId=7"), line);
        assertTrue(line.contains("retain=true"), line);
        assertTrue(line.contains("size=13"), line);
        assertTrue(line.contains("payload={\"temp\":23.5}"), line);
    }

    @Test
    @DisplayName("PUBLISH 长 payload 截断并标注总大小")
    void longPayloadTruncated() {
        byte[] big = ("x").repeat(5000).getBytes(StandardCharsets.UTF_8);
        MqttPublishMessage msg = new MqttPublishMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttPublishVariableHeader("t", 0, MqttProperties.NO_PROPERTIES),
                Unpooled.wrappedBuffer(big));

        String line = describe(msg);
        assertTrue(line.contains("total=5000"), "标注总大小: " + line);
        assertFalse(line.contains("x".repeat(1000)), "不得整段打印超长 payload");
    }

    @Test
    @DisplayName("SUBSCRIBE: 逐过滤器带 QoS")
    void subscribeDescription() {
        MqttSubscribeMessage msg = new MqttSubscribeMessage(
                new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader.from(2),
                new MqttSubscribePayload(List.of(
                        new MqttTopicSubscription("sensor/#", MqttQoS.AT_LEAST_ONCE),
                        new MqttTopicSubscription("cmd/+/x", MqttQoS.AT_MOST_ONCE))));

        String line = describe(msg);
        assertTrue(line.contains("type=SUBSCRIBE"), line);
        assertTrue(line.contains("packetId=2"), line);
        assertTrue(line.contains("sensor/#:1"), line);
        assertTrue(line.contains("cmd/+/x:0"), line);
    }

    @Test
    @DisplayName("PUBACK: 只有 packetId; 二进制 payload 不炸")
    void ackAndBinaryPayload() {
        MqttMessage pubAck = new MqttPubAckMessage(
                new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader.from(9));
        String ackLine = describe(pubAck);
        assertTrue(ackLine.contains("type=PUBACK") && ackLine.contains("packetId=9"), ackLine);

        byte[] binary = {0x00, 0x01, 0x02, (byte) 0xFF, (byte) 0xFE};
        MqttPublishMessage binMsg = new MqttPublishMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttPublishVariableHeader("t", 0, MqttProperties.NO_PROPERTIES),
                Unpooled.wrappedBuffer(binary));
        String binLine = describe(binMsg);
        assertFalse(binLine.isEmpty(), "二进制 payload 也要能安全打印(替换不可见字符)");
    }

    @Test
    @DisplayName("未注册 clientId 的连接上报文, clientId 为 unknown 而不是 null")
    void unknownClientId() {
        MqttMessage ping = new MqttMessage(new MqttFixedHeader(
                MqttMessageType.PINGREQ, false, MqttQoS.AT_MOST_ONCE, false, 0));
        String line = MqttBrokerHandler.describeInbound(new EmbeddedChannel(), ping);
        assertTrue(line.contains("clientId=unknown"), line);
        assertTrue(line.contains("type=PINGREQ"), line);
    }

    @Test
    @DisplayName("已注册连接取 channel 上的 clientId(报文里不再携带)")
    void clientIdFromChannelAttr() {
        MqttVersion v = MqttVersion.MQTT_5;
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.attr(ChannelAttributes.CLIENT_ID).set("app:PAD-1");
        channel.attr(ChannelAttributes.PROTOCOL_VERSION).set(v);
        MqttMessage ping = new MqttMessage(new MqttFixedHeader(
                MqttMessageType.PINGREQ, false, MqttQoS.AT_MOST_ONCE, false, 0));
        String line = MqttBrokerHandler.describeInbound(channel, ping);
        assertTrue(line.contains("clientId=app:PAD-1"), line);
    }
}
