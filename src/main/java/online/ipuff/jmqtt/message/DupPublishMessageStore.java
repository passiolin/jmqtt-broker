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
package online.ipuff.jmqtt.message;

/**
 * QoS 1/2 已发送但未确认的 PUBLISH, 用于重连后重发(DUP=1)。
 *
 *
 * @param clientId 接收方 clientId
 * @param topic    主题
 * @param qos      投递时使用的 QoS
 * @param messageId 报文标识符
 * @param payload  消息体
 */
public record DupPublishMessageStore(String clientId, String topic, int qos, int messageId, byte[] payload) {
}
