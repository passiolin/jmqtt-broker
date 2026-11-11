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
 * v5 独有的一组字段（Session Expiry Interval、Receive Maximum、Maximum Packet Size、
 * Topic Alias Maximum、Request Problem Information、Will Delay Interval）在这里全部不存在。
 * 它们原本的作用是「把两个版本的差异在握手时一次性拍平，让上层不必分版本」——
 * 只剩一个版本后，这层拍平不再需要，字段直接退化为服务端配置。
 *
 * <p>留在模型里的三项都是 3.1.1 自身就有的语义：
 * <ul>
 *   <li>{@code cleanSession} —— 丢弃既有会话</li>
 *   <li>{@code sessionExpirySeconds} —— 服务端决定的会话保留时长（v3.1.1 无此字段，
 *       由 {@code jmqtt.broker.session-expiry-seconds} 统一给出）</li>
 *   <li>{@code receiveMaximum} —— 服务端的发送窗口上限（v3.1.1 无客户端协商）</li>
 * </ul>
 *
 * @param cleanSession          是否丢弃既有会话
 * @param sessionExpirySeconds  服务端采用的会话保留秒数；0 表示断开即销毁
 * @param receiveMaximum        服务端的发送窗口上限（归一后至少 1）
 */
public record ConnectOptions(
        boolean cleanSession,
        long sessionExpirySeconds,
        int receiveMaximum
) {

    /** v5 未声明 Receive Maximum 时的规范缺省值; 这里作为服务端窗口的缺省上限 */
    public static final int DEFAULT_RECEIVE_MAXIMUM = 65535;

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
        int window = Math.max(1, brokerProperties.maxInflight());
        return new ConnectOptions(cleanSession, expiry, window);
    }
}
