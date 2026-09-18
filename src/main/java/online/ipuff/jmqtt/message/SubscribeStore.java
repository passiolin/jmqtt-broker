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
 * 订阅关系。
 *
 * <p>去除可变 setter, 改为不可变记录。
 *
 * @param clientId    订阅方 clientId
 * @param topicFilter 订阅主题过滤器, 可含 {@code +} / {@code #} 通配符
 * @param qos         订阅时协商的 QoS (0/1/2)
 */
public record SubscribeStore(String clientId, String topicFilter, int qos) {

    public static SubscribeStore of(String clientId, String topicFilter, int qos) {
        return new SubscribeStore(clientId, topicFilter, qos);
    }
}
