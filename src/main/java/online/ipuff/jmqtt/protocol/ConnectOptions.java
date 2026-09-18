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

import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttProperties;
import online.ipuff.jmqtt.config.BrokerProperties;

/**
 * 从 CONNECT 报文里解析出的、与协议版本无关的连接选项。
 *
 * <h2>为什么要有这一层</h2>
 * v3.1.1 与 v5 表达同一件事的方式完全不同:
 * <table>
 *   <tr><th>语义</th><th>v3.1.1</th><th>v5</th></tr>
 *   <tr><td>会话保留多久</td><td>没有这个概念, 服务端自己定</td>
 *       <td>Session Expiry Interval(客户端给, 秒)</td></tr>
 *   <tr><td>丢弃既有会话</td><td>{@code cleanSession=1}</td><td>{@code cleanStart=1}</td></tr>
 *   <tr><td>客户端能收多少未确认 QoS1/2</td><td>无, 只有服务端单方面限制</td>
 *       <td>Receive Maximum</td></tr>
 *   <tr><td>能收多大的报文</td><td>无</td><td>Maximum Packet Size</td></tr>
 * </table>
 *
 * <p>如果让上层处理器各自去 {@code if (version == 5)} 分支, 这个判断会散落到
 * 十几个地方, 而且每一处都要重新想一遍缺省值。这里在握手时<b>一次性拍平</b>:
 * 之后所有代码只面对这个模型, 不再关心版本。
 *
 * <h2>两个缺省值陷阱</h2>
 * <ol>
 *   <li><b>Receive Maximum 的缺省值是 65535, 不是 0。</b>规范规定不声明时等同 65535;
 *       若照字面取到 0, 发送窗口会被压成 1 —— 吞吐直接塌掉, 而且不会有任何报错。</li>
 *   <li><b>Session Expiry Interval 必须封顶。</b>v5 允许 0xFFFFFFFF 表示「永不过期」,
 *       但服务端有权施加自己的上限, 并且<b>必须把实际采用的值在 CONNACK 里告知客户端</b>
 *       （[MQTT-3.1.2-21]）。这里直接封顶到配置的会话保留时长,
 *       于是不需要「永不过期」这个哨兵值, 也就不会撞上「Redis EXPIRE 传天文数字报错」这类衍生问题。</li>
 * </ol>
 *
 * @param protocolVersion   4 = MQTT 3.1.1, 5 = MQTT 5.0
 * @param cleanStart        是否丢弃既有会话(v3 的 cleanSession / v5 的 cleanStart)
 * @param sessionExpirySeconds 服务端<b>实际采用</b>的会话保留秒数; 0 表示断开即销毁
 * @param receiveMaximum    客户端声明的在途窗口上限(已归一, 至少 1)
 * @param maximumPacketSize 客户端能接收的最大报文字节数; 0 表示未声明(无限制)
 * @param topicAliasMaximum 客户端允许服务端使用的 topic alias 数量; 0 表示不接受
 * @param requestProblemInformation 是否允许服务端在回包里带 Reason String / User Property
 * @param willDelaySeconds  遗嘱延迟投递秒数(v5; v3 恒为 0)
 */
public record ConnectOptions(
        int protocolVersion,
        boolean cleanStart,
        long sessionExpirySeconds,
        int receiveMaximum,
        int maximumPacketSize,
        int topicAliasMaximum,
        boolean requestProblemInformation,
        long willDelaySeconds
) {

    /** v5 未声明 Receive Maximum 时的规范缺省值 */
    public static final int DEFAULT_RECEIVE_MAXIMUM = 65535;

    public static final int VERSION_3_1_1 = 4;
    public static final int VERSION_5 = 5;

    public boolean isV5() {
        return protocolVersion == VERSION_5;
    }

    /**
     * 会话是否需要保留(需要落盘、需要在断开后继续存在)。
     *
     * <p>v3.1.1 下等价于 {@code !cleanSession} —— 因为 {@link #parse} 会把
     * cleanSession=1 的会话保留时长设为 0。所以上层不必再分版本判断。
     */
    public boolean persistent() {
        return sessionExpirySeconds > 0;
    }

    /**
     * 从 CONNECT 报文解析。v3.1.1 的字段用配置缺省值补齐, 使上层无需感知版本。
     *
     * @param brokerProperties 用于取服务端允许的最大会话保留时长(v5 的 SEI 封顶值)
     */
    public static ConnectOptions parse(MqttConnectMessage msg, BrokerProperties brokerProperties) {
        int version = msg.variableHeader().version();
        boolean cleanStart = msg.variableHeader().isCleanSession();
        long serverMaxExpiry = brokerProperties.sessionExpirySeconds();

        if (version != VERSION_5) {
            // v3.1.1: cleanSession=0 的会话保留时长由服务端决定; =1 则断开即销毁
            return new ConnectOptions(VERSION_3_1_1, cleanStart, cleanStart ? 0L : serverMaxExpiry,
                    DEFAULT_RECEIVE_MAXIMUM, 0, 0, true, 0L);
        }

        MqttProperties properties = msg.variableHeader().properties();
        long requestedExpiry = intProperty(properties,
                MqttProperties.MqttPropertyType.SESSION_EXPIRY_INTERVAL.value(), 0);
        // 0xFFFFFFFF(永不过期)在 Netty 里是按 int 承载的, 读出来是 -1。
        // 直接拿去 Math.min 会把保留时长算成负数 —— 归一为「服务端上限」。
        if (requestedExpiry < 0) {
            requestedExpiry = serverMaxExpiry;
        }
        // 0 是「断开即销毁」, 必须原样保留; 其余一律封顶到服务端上限。
        // 注意: cleanStart=1 且 SEI>0 是合法组合 —— 会话从零开始, 但建立之后要保留 SEI 秒。
        long effectiveExpiry = requestedExpiry == 0 ? 0L : Math.min(requestedExpiry, serverMaxExpiry);

        int receiveMaximum = intProperty(properties,
                MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value(), DEFAULT_RECEIVE_MAXIMUM);
        if (receiveMaximum < 1) {
            receiveMaximum = DEFAULT_RECEIVE_MAXIMUM;
        }
        int maximumPacketSize = intProperty(properties,
                MqttProperties.MqttPropertyType.MAXIMUM_PACKET_SIZE.value(), 0);
        int topicAliasMaximum = intProperty(properties,
                MqttProperties.MqttPropertyType.TOPIC_ALIAS_MAXIMUM.value(), 0);
        int requestProblemInformation = intProperty(properties,
                MqttProperties.MqttPropertyType.REQUEST_PROBLEM_INFORMATION.value(), 1);
        long willDelay = willDelaySeconds(msg);

        return new ConnectOptions(VERSION_5, cleanStart, effectiveExpiry, receiveMaximum,
                maximumPacketSize, topicAliasMaximum, requestProblemInformation != 0, willDelay);
    }

    /**
     * 遗嘱延迟投递间隔。位于 will properties 里, 与连接属性分属两处。
     */
    private static long willDelaySeconds(MqttConnectMessage msg) {
        if (!msg.variableHeader().isWillFlag()) {
            return 0L;
        }
        MqttProperties willProperties = msg.payload().willProperties();
        if (willProperties == null || willProperties == MqttProperties.NO_PROPERTIES) {
            return 0L;
        }
        return intProperty(willProperties,
                MqttProperties.MqttPropertyType.WILL_DELAY_INTERVAL.value(), 0);
    }

    /**
     * 取一个整型属性。缺失时返回缺省值 —— 注意这里的缺省值是调用方给的,
     * 不能一律用 0, 见类注释里的 Receive Maximum 陷阱。
     */
    private static int intProperty(MqttProperties properties, int type, int defaultValue) {
        if (properties == null || properties == MqttProperties.NO_PROPERTIES) {
            return defaultValue;
        }
        MqttProperties.MqttProperty property = properties.getProperty(type);
        if (property instanceof MqttProperties.IntegerProperty integer) {
            return integer.value();
        }
        return defaultValue;
    }
}
