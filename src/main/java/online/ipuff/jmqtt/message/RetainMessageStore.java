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
 * 保留消息(Retain)。
 *
 * @param topic   具体主题名(不含通配符)
 * @param payload 消息体
 * @param qos     发布时的 QoS
 */
public record RetainMessageStore(String topic, byte[] payload, int qos) {
}
