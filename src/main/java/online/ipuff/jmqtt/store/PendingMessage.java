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
package online.ipuff.jmqtt.store;

/**
 * 一条待投递给离线客户端的消息。
 *
 * <p>只存领域字段, 不含报文标识符 —— 标识符在真正发送时才分配,
 * 因为同一条消息可能被多次尝试投递, 每次都需要新的标识符语义。
 *
 * @param topic     主题
 * @param qos       入队时确定的投递 QoS(已按订阅 QoS 取过较小值)
 * @param payload   消息体
 * @param retain    是否作为保留消息投递
 * @param createdAt 入队时间(epoch millis), 用于排查积压时长
 */
public record PendingMessage(String topic, int qos, byte[] payload, boolean retain, long createdAt) {

    public static PendingMessage of(String topic, int qos, byte[] payload, boolean retain) {
        return new PendingMessage(topic, qos, payload, retain, System.currentTimeMillis());
    }
}
