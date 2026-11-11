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
import online.ipuff.jmqtt.config.BrokerProperties;

/**
 * 从 CONNECT 报文里解析出、与协议版本无关的连接选项。
 *
 * <h2>这个分支只处理 3.1.1</h2>
 * 3.1.1 的 CONNECT 里没有 Session Expiry Interval、Receive Maximum、Maximum Packet Size、
 * Topic Alias、Will Delay 这些字段, 因此这里的三项全部来自两条输入:
 * 报文里的 {@code cleanSession} 位, 以及服务端配置。
 *
 * <p>保留的两项：
 * <ul>
 *   <li>{@code cleanSession} —— 丢弃既有会话</li>
 *   <li>{@code sessionExpirySeconds} —— 服务端决定的会话保留时长（3.1.1 无此字段，
 *       由 {@code jmqtt.broker.session-expiry-seconds} 统一给出）</li>
 * </ul>
 *
 * @param cleanSession          是否丢弃既有会话
 * @param sessionExpirySeconds  服务端采用的会话保留秒数；0 表示断开即销毁
 */
public record ConnectOptions(
        boolean cleanSession,
        long sessionExpirySeconds
) {

    /** 协议级别 4 = MQTT 3.1.1 */
    public static final int VERSION_3_1_1 = 4;

    /**
     * 会话是否需要保留（需要落盘、需要在断开后继续存在）。
     */
    public boolean persistent() {
        return !cleanSession && sessionExpirySeconds > 0;
    }

    /**
     * 从 CONNECT 报文解析。3.1.1 只有 cleanSession 一个开关，其余取服务端配置。
     */
    public static ConnectOptions parse(MqttConnectMessage msg, BrokerProperties brokerProperties) {
        boolean cleanSession = msg.variableHeader().isCleanSession();
        long serverMaxExpiry = brokerProperties.sessionExpirySeconds();
        // cleanSession=0 的会话保留时长由服务端决定; =1 则断开即销毁
        long expiry = cleanSession ? 0L : serverMaxExpiry;
        return new ConnectOptions(cleanSession, expiry);
    }
}
