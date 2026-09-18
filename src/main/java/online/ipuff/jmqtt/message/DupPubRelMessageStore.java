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
 * QoS 2 流程中已发送但未收到 PUBCOMP 的 PUBREL, 用于重连后重发。
 *
 *
 * @param clientId  接收方 clientId
 * @param messageId 报文标识符
 */
public record DupPubRelMessageStore(String clientId, int messageId) {
}
