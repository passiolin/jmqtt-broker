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
package online.ipuff.jmqtt.protocol;

import io.netty.handler.codec.mqtt.MqttConnectMessage;
import online.ipuff.jmqtt.config.BrokerProperties;

/**
 * 从 CONNECT 报文里解析出连接选项。
 *
 * <h2>服务 3.1 与 3.1.1 两个协议级别</h2>
 * 两者在报文层面几乎一致：CONNECT 的 payload 结构相同（3.1 没有 Will Properties，
 * 那本来就是 v5 才有的），PUBLISH/PUBACK/PUBREC/PUBREL/PUBCOMP/SUBSCRIBE/UNSUBSCRIBE
 * 的字节布局完全一样。实测（Netty 4.1.77 编码器）两者编出的 SUBACK / PUBACK /
 * UNSUBACK / CONNACK 逐字节相同，差别只在两条规范细节上：
 *
 * <ul>
 *   <li><b>3.1 的 CONNACK 里 bit0 是「Topic Name Compression Response」保留位</b>，
 *       不是 3.1.1 的 Session Present。Netty 对 3.1 也按 Session Present 写，
 *       我们这里恒回 {@code false}（会话不存在时才可能为 true，见下方说明），
 *       因此两种解读下都是 0，不会造成误判。</li>
 *   <li><b>3.1 的 clientId 限定 1~23 个字符</b>，超长由解码器直接判
 *       {@code MqttIdentifierRejectedException}。这条不需要在业务层重复校验。</li>
 * </ul>
 *
 * <p>两个版本都不存在 Session Expiry Interval / Receive Maximum / Maximum Packet Size /
 * Topic Alias / Will Delay —— 那些是 v5 才有的字段。所以这里的两项全部来自
 * 报文里的 {@code cleanSession} 位与服务端配置。
 *
 * @param cleanSession          是否丢弃既有会话
 * @param sessionExpirySeconds  服务端采用的会话保留秒数；0 表示断开即销毁
 */
public record ConnectOptions(
        boolean cleanSession,
        long sessionExpirySeconds
) {

    /** 协议级别 3 = MQTT 3.1（协议名 MQIsdp） */
    public static final int VERSION_3_1 = 3;

    /** 协议级别 4 = MQTT 3.1.1（协议名 MQTT） */
    public static final int VERSION_3_1_1 = 4;

    /**
     * 会话是否需要保留（需要落盘、需要在断开后继续存在）。
     */
    public boolean persistent() {
        return !cleanSession && sessionExpirySeconds > 0;
    }

    /**
     * 从 CONNECT 报文解析。两个版本都只有 {@code cleanSession} 一个开关，其余取服务端配置。
     */
    public static ConnectOptions parse(MqttConnectMessage msg, BrokerProperties brokerProperties) {
        boolean cleanSession = msg.variableHeader().isCleanSession();
        long serverMaxExpiry = brokerProperties.sessionExpirySeconds();
        // cleanSession=0 的会话保留时长由服务端决定; =1 则断开即销毁
        long expiry = cleanSession ? 0L : serverMaxExpiry;
        return new ConnectOptions(cleanSession, expiry);
    }
}
