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

import online.ipuff.jmqtt.message.DupPubRelMessageStore;

import java.util.List;

/**
 * QoS 2 未收到 PUBCOMP 的 PUBREL 存储服务。
 *
 */
public interface IDupPubRelMessageStoreService {

    void put(DupPubRelMessageStore message);

    List<DupPubRelMessageStore> get(String clientId);

    void remove(String clientId, int messageId);

    void removeByClient(String clientId);

    int size();
}
